"""One output per business ID: native checkpoint and sink-gap faults."""
import argparse
from collections import Counter
import json
from pathlib import Path
import subprocess
import time
import uuid

from latency import Experiment, business_rows, file_info, jsonl
from run_lab import JAVA, wait_for, redis

COUNT, RATE = 9000, 3000
CASES = (
    ('redis-after-normal', 'redis-after', 'none'),
    ('redis-after-checkpoint', 'redis-after', 'checkpoint'),
    ('redis-before-gap', 'redis-before', 'after-redis'),
    ('redis-after-gap', 'redis-after', 'after-ack'),
    ('state-alo-checkpoint', 'state-alo', 'checkpoint'),
    ('state-eos-checkpoint', 'state-eos', 'checkpoint'),
)


def native_json(directory, pattern):
    values = []
    for path in sorted(directory.glob(pattern)):
        try: values.append(json.loads(path.read_text()))
        except (OSError, ValueError): pass
    return values


def audit_business(inputs, outputs):
    """Expect the first source-partition arrival, exactly once per business ID."""
    first, sequences = {}, set()
    source = business_rows(inputs)
    for row in source:
        value = json.loads(row['value'])
        assert row['key'] == value['event_id']
        assert type(value['input_seq']) is int and value['input_seq'] not in sequences
        sequences.add(value['input_seq'])
        first.setdefault(value['event_id'], value)
    counts, payload_errors = Counter(), 0
    for row in outputs:
        value = json.loads(row['value']); event_id = value['event_id']
        counts[event_id] += 1
        if row['key'] != event_id or value != first.get(event_id): payload_errors += 1
    missing = sorted(set(first)-set(counts))
    unexpected = sorted(set(counts)-set(first))
    duplicates = {k:n for k,n in counts.items() if n > 1}
    return {'source_arrivals': len(source), 'expected_business_events': len(first),
        'output_records': len(outputs), 'missing_business_ids': missing,
        'unexpected_business_ids': unexpected,
        'duplicate_records': sum(n-1 for n in counts.values()),
        'duplicated_business_ids': duplicates, 'payload_or_first_arrival_mismatches': payload_errors,
        'business_contract_pass': not (missing or unexpected or duplicates or payload_errors)}


class Deduplication(Experiment):
    def tag(self, args, directory):
        if args[0] != 'deduplication-flink': return super().tag(args,directory)
        self.serial += 1
        out=(directory/f'dedup-{self.serial}.stdout').open('w')
        err=(directory/f'dedup-{self.serial}.stderr').open('w')
        self.streams.extend([out,err])
        worker=subprocess.Popen(JAVA[:-1]+['lab.DeduplicationDemo']+list(map(str,args[1:])),
            stdout=out,stderr=err,start_new_session=True)
        worker.lab_output,worker.lab_error=Path(out.name),Path(err.name)
        self.processes.append(worker)
        return worker

    def scenario(self, name, mode, fault):
        directory=self.root/name; directory.mkdir()
        source=self.root.name+'-'+name+'-in'; output=self.root.name+'-'+name+'-out'
        self.create(source);self.create(output)
        observer=self.tag(['observe',output,directory],directory)
        for view in ('read_uncommitted','read_committed'):
            wait_for(lambda:(directory/('observer-ready-'+view)).exists(),35,'observer assignment',observer)
        worker_dir=directory/'flink';worker_dir.mkdir()
        self.tag_call(['seed',source,0,0],directory)
        worker=self.tag(['deduplication-flink',source,output,mode,worker_dir,fault],worker_dir)
        wait_for(lambda:(worker_dir/'worker-ready').exists(),120,'Flink warmup',worker)
        wait_for(lambda:any(c['processed']==0 for c in native_json(worker_dir,'completed-*.json')),
                 180,'completed initial empty checkpoint',worker)
        initial=[c for c in native_json(worker_dir,'completed-*.json') if c['processed']==0][-1]
        generation=json.loads(self.tag_call(['seed',source,COUNT,RATE],directory,timeout=90))
        if fault != 'none':
            pattern='checkpoint-fault.json' if fault=='checkpoint' else 'sink-gap.json'
            wait_for(lambda:bool(native_json(worker_dir,pattern)),600,'injected native failure witness',worker)
            wait_for(lambda:any(a['attempt']>=1 and a['is_restored'] for a in native_json(worker_dir,'attempt-*-initialized.json')),
                     180,'automatic native restored attempt',worker)
        wait_for(lambda:any(c['processed']>=COUNT and (fault=='none' or c['attempt']>=1)
                            for c in native_json(worker_dir,'completed-*.json')),
                 900,'successful checkpoint after full input processing',worker)
        assert worker.poll() is None, 'Flink JVM must remain alive through native recovery'
        native_output=json.loads(self.tag_call(['read',output,'read_committed'],directory))
        wait_for(lambda:self.progress(directory,'read_committed')>=len(native_output),120,'observer drain',observer)
        self.kill(worker)  # Cleanup after successful full processing, never the fault trigger.
        (directory/'observer-stop').touch();self.finish(observer,15)
        inputs=json.loads(self.tag_call(['read',source,'read_uncommitted'],directory))
        input_path=directory/'input.jsonl'
        input_path.write_text(''.join(r['value']+'\n' for r in business_rows(inputs)))
        assert len(business_rows(inputs))==COUNT
        case={'name':name,'mode':mode,'fault_kind':fault,'generation':generation,
            'source_topic':source,'output_topic':output,'checkpoint_interval_ms':30000,
            'deduplication_ttl_ms':3600000,'initial_checkpoint':initial,
            'controller_kill_during_failure':False,'worker_directory':str(worker_dir),
            'initialized_attempts':native_json(worker_dir,'attempt-*-initialized.json'),
            'completed_checkpoints':native_json(worker_dir,'completed-*.json'),
            'checkpoint_fault':native_json(worker_dir,'checkpoint-fault.json'),
            'sink_gap':native_json(worker_dir,'sink-gap.json'),
            'input_file':file_info(input_path),'views':{}}
        for view in ('read_uncommitted','read_committed'):
            rows=json.loads(self.tag_call(['read',output,view],directory))
            sink=directory/('sink-'+view+'.jsonl');observations=jsonl(directory/('observations-'+view+'.jsonl'))
            assert jsonl(sink)==[json.loads(r['value']) for r in rows]
            assert [(r['offset'],r['value']) for r in observations]==[(r['offset'],r['value']) for r in rows]
            case['views'][view]={'audit':audit_business(inputs,rows),'sink_file':file_info(sink)}
        if case['sink_gap']:
            event_id=case['sink_gap'][0]['event_id']
            case['native_redis_fault_id_value']=redis('GET',output+':'+event_id)
        assert_outcome(case)
        print(json.dumps({'case':name,'attempts':case['initialized_attempts'],
                          'committed_audit':case['views']['read_committed']['audit']}),flush=True)
        return case

    def run(self):
        self.root.mkdir(parents=True)
        deadline=time.monotonic()+120
        while True:
            try:
                if int(self.call('ready',timeout=20).strip())==3:break
            except (RuntimeError,TimeoutError):pass
            if time.monotonic()>deadline:raise TimeoutError('Kafka readiness')
            time.sleep(.5)
        assert redis('PING')=='PONG'
        return {'experiment_valid':True,'run_id':self.root.name,
            'contract':'first source-partition arrival per business ID exactly once; skip repeated IDs',
            'source_arrivals_per_case':COUNT,'target_rate_per_sec':RATE,'producer_threads':4,
            'checkpoint_interval_ms':30000,'deduplication_ttl_ms':3600000,
            'ttl_rule':'rolling processing time, refreshed on each processed arrival',
            'versions':{'kafka_broker':'3.7.0','kafka_client':'3.4.0','flink':'1.20.4',
                        'kafka_connector':'3.4.0-1.20','redis':'7.2.6','python':'3.10.12'},
            'limitations':['single machine, one input/output partition, parallelism one',
                'synthetic JSON, four producer threads; cases run sequentially',
                'controlled snapshot/sink exceptions, no controller kill used as fault',
                'not a measured natural failure probability or throughput ceiling',
                'one-hour expiry, Redis data loss and concurrent writers not exercised'],
            'cases':[self.scenario(*spec) for spec in CASES]}


def assert_outcome(case):
    audit=case['views']['read_committed']['audit'];name=case['name']
    assert not audit['unexpected_business_ids'] and audit['payload_or_first_arrival_mismatches']==0
    if name=='redis-before-gap':
        assert audit['missing_business_ids']==[case['sink_gap'][0]['event_id']]
        assert audit['duplicate_records']==0
    elif name=='redis-after-gap':
        assert not audit['missing_business_ids']
        assert audit['duplicated_business_ids']=={case['sink_gap'][0]['event_id']:2}
    elif name=='state-alo-checkpoint':
        assert not audit['missing_business_ids'] and audit['duplicate_records']>0
    else:assert audit['business_contract_pass'] is True


if __name__=='__main__':
    parser=argparse.ArgumentParser();parser.add_argument('--output',required=True);args=parser.parse_args()
    experiment=Deduplication(Path('/app/deduplication-runs')/('dedup-'+uuid.uuid4().hex[:10]))
    try:Path(args.output).write_text(json.dumps(experiment.run(),indent=2)+'\n')
    finally:experiment.close()
