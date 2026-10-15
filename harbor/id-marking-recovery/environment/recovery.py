"""Observe real checkpoint aborts and native automatic Flink recovery."""
import argparse
from collections import Counter
import json
from pathlib import Path
import subprocess
import time
import uuid

from latency import Experiment, TAG_JAVA, COUNT, RATE, business_rows, jsonl, file_info
from run_lab import JAVA, wait_for, redis

CASES = (
    ('state-alo-timeout', 'alo', 'timeout'),
    ('state-eos-timeout', 'eos', 'timeout'),
    ('state-alo-restart', 'alo', 'snapshot-error'),
    ('redis-alo-restart', 'redis', 'snapshot-error'),
    ('streams-after-alo-restart', 'bridge', 'snapshot-error'),
    ('state-eos-restart', 'eos', 'snapshot-error'),
)


def native_json(directory, pattern):
    values = []
    for path in sorted(directory.glob(pattern)):
        try: values.append(json.loads(path.read_text()))
        except (OSError, ValueError): pass
    return values


def audit_arrivals(inputs, outputs):
    expected = {}
    for row in business_rows(inputs):
        value = json.loads(row['value']); seq = value['input_seq']
        assert seq not in expected and row['key'] == value['event_id']
        expected[seq] = value
    assert set(expected) == set(range(COUNT))
    counts, first_flags, output_ids = Counter(), Counter(), set()
    payload_errors, marker_errors, invalid_flags = 0, 0, 0
    false_count, true_count = 0, 0
    for row in outputs:
        value = json.loads(row['value']); seq = value['input_seq']
        counts[seq] += 1
        if seq not in expected:
            payload_errors += 1; continue
        if row['key'] != value['event_id']: payload_errors += 1
        if ({k:v for k,v in value.items() if k not in ('is_duplicate','marked_at_ms')} != expected[seq]):
            payload_errors += 1
        flag = value.get('is_duplicate')
        if type(flag) is not bool:
            invalid_flags += 1
        elif flag: true_count += 1
        else:
            false_count += 1; first_flags[value['event_id']] += 1
        if type(flag) is not bool or flag != (value['event_id'] in output_ids):
            marker_errors += 1
        output_ids.add(value['event_id'])
    source_ids = {v['event_id'] for v in expected.values()}
    result = {'source_arrivals': len(expected), 'output_arrivals': len(outputs),
        'missing_arrivals': len(set(expected)-set(counts)),
        'unexpected_arrivals': sum(n for seq,n in counts.items() if seq not in expected),
        'replayed_arrival_records': sum(max(0,n-1) for n in counts.values()),
        'payload_mismatches': payload_errors, 'flag_mismatches_in_output_order': marker_errors,
        'invalid_flag_types': invalid_flags, 'false_markers': false_count, 'true_markers': true_count,
        'business_ids_without_false': sum(first_flags[k] == 0 for k in source_ids),
        'business_ids_with_multiple_false': sum(first_flags[k] > 1 for k in source_ids),
        'maximum_input_sequence_multiplicity': max(counts.values(), default=0)}
    result['arrival_contract_pass'] = all(result[k] == 0 for k in (
        'missing_arrivals','unexpected_arrivals','replayed_arrival_records','payload_mismatches',
        'flag_mismatches_in_output_order','invalid_flag_types'))
    return result


class Recovery(Experiment):
    def tag(self, args, directory):
        if args[0] != 'recovery-flink':
            return super().tag(args,directory)
        self.serial += 1
        out = (directory/f'fault-{self.serial}.stdout').open('w')
        err = (directory/f'fault-{self.serial}.stderr').open('w')
        self.streams.extend([out,err])
        p = subprocess.Popen(JAVA[:-1]+['lab.CheckpointFailureDemo']+list(map(str,args[1:])),
            stdout=out,stderr=err,start_new_session=True)
        p.lab_output,p.lab_error = Path(out.name),Path(err.name)
        self.processes.append(p)
        return p

    def scenario(self, name, mode, kind):
        directory=self.root/name; directory.mkdir()
        source=self.root.name+'-'+name+'-in'; output=self.root.name+'-'+name+'-out'
        self.create(source); self.create(output)
        staging=None
        if mode=='bridge': staging=output+'-staging'; self.create(staging)
        observer=self.tag(['observe',output,directory],directory)
        for view in ('read_uncommitted','read_committed'):
            wait_for(lambda:(directory/('observer-ready-'+view)).exists(),35,'observer assignment',observer)
        worker_dir=directory/'flink'; worker_dir.mkdir()
        self.tag_call(['seed',source,0,0],directory)
        stream_worker=None
        if staging:
            self.tag_call(['seed',staging,0,0],directory)
            stream_worker=self.tag(['streams',staging,output,directory],directory)
            wait_for(lambda:(directory/'worker-ready').exists(),120,'Streams warmup',stream_worker)
        worker=self.tag(['recovery-flink',source,staging or output,mode,worker_dir,kind],worker_dir)
        wait_for(lambda:(worker_dir/'worker-ready').exists(),120,'Flink warmup',worker)
        wait_for(lambda:any(x['processed']==0 for x in native_json(worker_dir,'completed-*.json')),
                 180,'actual initial completed checkpoint',worker)
        initial=[x for x in native_json(worker_dir,'completed-*.json') if x['processed']==0][-1]
        generation=json.loads(self.tag_call(['seed',source,COUNT,RATE],directory,timeout=90))
        wait_for(lambda:(worker_dir/'checkpoint-fault.json').exists(),240,'actual snapshot fault callback',worker)
        wait_for(lambda:bool(native_json(worker_dir,'checkpoint-fault.json')),15,'complete fault witness',worker)
        if kind=='snapshot-error':
            wait_for(lambda:any(x['attempt']>=1 and x['is_restored'] for x in native_json(worker_dir,'attempt-*-initialized.json')),
                     180,'native restored attempt in same JVM',worker)
            wait_for(lambda:any(x['attempt']>=1 and x['processed']>=COUNT for x in native_json(worker_dir,'completed-*.json')),
                     900,'completed data checkpoint after native restart',worker)
        else:
            wait_for(lambda:bool(native_json(worker_dir,'aborted-*.json')),120,'native checkpoint abort notification',worker)
            failed=native_json(worker_dir,'checkpoint-fault.json')[0]['checkpoint_id']
            wait_for(lambda:any(x['checkpoint_id']>failed and x['processed']>=COUNT for x in native_json(worker_dir,'completed-*.json')),
                     180,'next successful checkpoint after tolerated timeout',worker)
        assert worker.poll() is None, 'JVM must stay alive during automatic job recovery'
        if staging:
            target_rows=business_rows(json.loads(self.tag_call(['read',staging,'read_uncommitted'],directory)))
            target=len(target_rows)
        else:
            wait_for(lambda:self.progress(directory,'read_committed')>=COUNT,120,'committed full source coverage',worker)
            target_rows=json.loads(self.tag_call(['read',output,'read_committed'],directory))
            target=len(target_rows)
        wait_for(lambda:self.progress(directory,'read_committed')>=target,120,'observer committed drain',observer)
        # Shutdown only after native checkpoint recovery has already completed.
        self.kill(worker)
        if stream_worker:self.kill(stream_worker)
        (directory/'observer-stop').touch();self.finish(observer,15)
        inputs=json.loads(self.tag_call(['read',source,'read_uncommitted'],directory))
        input_path=directory/'input.jsonl'
        with input_path.open('w') as stream:
            for row in business_rows(inputs):stream.write(row['value']+'\n')
        case={'name':name,'mode':mode,'failure_kind':kind,'source_topic':source,'output_topic':output,
            'staging_topic':staging,'generation':generation,'checkpoint_interval_ms':30000,
            'checkpoint_timeout_ms':5000 if kind=='timeout' else 120000,
            'tolerable_checkpoint_failures':1 if kind=='timeout' else 0,
            'deduplication_ttl_ms':3600000,'flink_jvm_pid':worker.pid,
            'failure_action':'native snapshot stall and coordinator timeout' if kind=='timeout' else 'native snapshot IOException; automatic job restart',
            'controller_kill_during_failure':False,'initial_checkpoint':initial,
            'fault':native_json(worker_dir,'checkpoint-fault.json')[0],
            'initialized_attempts':native_json(worker_dir,'attempt-*-initialized.json'),
            'aborted_checkpoints':native_json(worker_dir,'aborted-*.json'),
            'completed_checkpoints':native_json(worker_dir,'completed-*.json'),
            'worker_directory':str(worker_dir),'input_file':file_info(input_path),'views':{}}
        for view in ('read_uncommitted','read_committed'):
            rows=json.loads(self.tag_call(['read',output,view],directory))
            sink_path=directory/('sink-'+view+'.jsonl')
            assert jsonl(sink_path)==[json.loads(r['value']) for r in rows]
            observed=jsonl(directory/('observations-'+view+'.jsonl'))
            assert [(r['offset'],r['value']) for r in observed]==[(r['offset'],r['value']) for r in rows]
            case['views'][view]={'audit':audit_arrivals(inputs,rows),'sink_file':file_info(sink_path),
                'examples':[json.loads(r['value']) for r in rows[:6]]}
        committed=case['views']['read_committed']['audit']
        assert committed['missing_arrivals']==committed['payload_mismatches']==0
        if mode=='eos' or kind=='timeout':
            assert committed['arrival_contract_pass'] is True, 'Reference/control did not preserve all arrivals correctly'
        else:
            assert committed['replayed_arrival_records']>0, 'Expected replay fault was not exercised'
        print(json.dumps({'case':name,'failure':kind,'attempts':case['initialized_attempts'],
                          'committed_audit':committed}),flush=True)
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
        cases=[self.scenario(*spec) for spec in CASES]
        return {'experiment_valid':True,'run_id':self.root.name,
            'contract':'preserve all JSON arrivals and correct ID markers through checkpoint failure/recovery',
            'target_rate_per_sec':RATE,'source_arrivals_per_case':COUNT,'producer_threads':4,
            'checkpoint_interval_ms':30000,'deduplication_ttl_ms':3600000,'streams_commit_interval_ms':100,
            'versions':{'kafka_broker':'3.7.0','kafka_client':'3.4.0','kafka_streams':'3.4.0','flink':'1.20.4','redis':'7.2.6','python':'3.10.12'},
            'limitations':['single-machine single partition, synthetic JSON input',
                'snapshot timeout/IOException injected once; not broker or disk-loss testing',
                'no manual JVM kill/relaunch used to cause failure; final process kill is cleanup only',
                'one-hour TTL expiry not observed by waiting an hour'], 'cases':cases}


if __name__=='__main__':
    parser=argparse.ArgumentParser();parser.add_argument('--output',required=True);args=parser.parse_args()
    experiment=Recovery(Path('/app/recovery-runs')/('recovery-'+uuid.uuid4().hex[:10]))
    try:Path(args.output).write_text(json.dumps(experiment.run(),indent=2)+'\n')
    finally:experiment.close()
