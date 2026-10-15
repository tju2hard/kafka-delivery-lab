"""Preserve all arrivals: native normal/recovery comparison with exact JSON audits."""
import argparse
from collections import Counter
import json
from pathlib import Path
import subprocess
import time
import uuid
from latency import Experiment,TAG_JAVA,RATE,business_rows,jsonl,file_info,distribution
from run_lab import JAVA,wait_for,redis
COUNT=9000
STREAMS_JAVA=JAVA[:-1]+['lab.ArrivalStreamsDemo']
CASES=tuple((name,mode,fault) for fault in ('none','snapshot-error') for name,mode in (
 ('flink-state-alo','alo'),('redis-sink-alo','redis'),('streams-direct-eos','streams'),
 ('flink-alo-streams-eos','bridge'),('flink-state-eos','eos')))
CASE_NAMES=[name+('-normal' if fault=='none' else '-recovery') for name,mode,fault in CASES]

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
    first_seq={}
    for row in business_rows(inputs):
        v=json.loads(row['value']);first_seq.setdefault(v['event_id'],v['input_seq'])
    input_flag_errors=sum(type((v:=json.loads(row['value'])).get('is_duplicate')) is not bool
        or v['is_duplicate'] != (v['input_seq']!=first_seq[v['event_id']]) for row in outputs)
    for row in outputs:
        v=json.loads(row['value']);seq=v['input_seq']
        if seq in expected:
            if json.dumps({k:x for k,x in v.items() if k not in ('is_duplicate','marked_at_ms')},sort_keys=True,separators=(',',':')) != json.dumps(expected[seq],sort_keys=True,separators=(',',':')):
                payload_errors+=1
    assert not outputs or max(json.loads(r['value'])['marked_at_ms'] for r in outputs)-min(v['ingested_at_ms'] for v in expected.values()) < 3600000

    result = {'source_arrivals': len(expected), 'unique_business_ids':len(source_ids), 'output_unique_business_ids':len(output_ids), 'output_arrivals': len(outputs),
        'missing_arrivals': len(set(expected)-set(counts)),
        'unexpected_arrivals': sum(n for seq,n in counts.items() if seq not in expected),
        'replayed_arrival_records': sum(max(0,n-1) for n in counts.values()),
        'payload_mismatches': payload_errors, 'flag_mismatches_in_output_order': marker_errors,
        'flag_mismatches_against_source':input_flag_errors, 'invalid_flag_types': invalid_flags, 'false_markers': false_count, 'true_markers': true_count,
        'business_ids_without_false': sum(first_flags[k] == 0 for k in source_ids),
        'business_ids_with_multiple_false': sum(first_flags[k] > 1 for k in source_ids),
        'maximum_input_sequence_multiplicity': max(counts.values(), default=0)}
    result['arrival_contract_pass'] = all(result[k] == 0 for k in (
        'missing_arrivals','unexpected_arrivals','replayed_arrival_records','payload_mismatches',
        'flag_mismatches_in_output_order','flag_mismatches_against_source','invalid_flag_types'))
    return result


def observations(case):
    directory=Path(case['input_file']['path']).parent
    for view in ('read_uncommitted','read_committed'):
        path=directory/('observations-'+view+'.jsonl');rows=jsonl(path)
        case['views'][view]['observation_file']=file_info(path)
        case['views'][view]['visibility_ms']=distribution([
            row['observed_at_ms']-json.loads(row['value'])['ingested_at_ms'] for row in rows])
    return case


class ArrivalExperiment(Experiment):
    def tag(self, args, directory):
        if args[0]=='arrival-streams':
            self.serial+=1
            out=(directory/f'streams-{self.serial}.stdout').open('w');err=(directory/f'streams-{self.serial}.stderr').open('w')
            self.streams.extend([out,err])
            worker=subprocess.Popen(STREAMS_JAVA+list(map(str,args[1:])),stdout=out,stderr=err,start_new_session=True)
            worker.lab_output,worker.lab_error=Path(out.name),Path(err.name);self.processes.append(worker)
            return worker
        if args[0] != 'recovery-flink':
            return super().tag(args,directory)
        self.serial += 1
        out = (directory/f'fault-{self.serial}.stdout').open('w')
        err = (directory/f'fault-{self.serial}.stderr').open('w')
        self.streams.extend([out,err])
        p = subprocess.Popen(JAVA[:-1]+['lab.ArrivalCheckpointDemo']+list(map(str,args[1:])),
            stdout=out,stderr=err,start_new_session=True)
        p.lab_output,p.lab_error = Path(out.name),Path(err.name)
        self.processes.append(p)
        return p

    def flink_case(self, name, mode, kind):
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
            stream_worker=self.tag(['arrival-streams',staging,output,directory,'none'],directory)
            wait_for(lambda:(directory/'worker-ready').exists(),120,'Streams warmup',stream_worker)
        worker=self.tag(['recovery-flink',source,staging or output,mode,worker_dir,kind],worker_dir)
        wait_for(lambda:(worker_dir/'worker-ready').exists(),120,'Flink warmup',worker)
        wait_for(lambda:any(x['processed']==0 for x in native_json(worker_dir,'completed-*.json')),
                 180,'actual initial completed checkpoint',worker)
        initial=[x for x in native_json(worker_dir,'completed-*.json') if x['processed']==0][-1]
        generation=json.loads(self.tag_call(['seed',source,COUNT,RATE],directory,timeout=90))
        if kind=='snapshot-error':
            wait_for(lambda:bool(native_json(worker_dir,'checkpoint-fault.json')),240,'actual CK snapshot IOException',worker)
            wait_for(lambda:any(x['attempt']>=1 and x['is_restored'] for x in native_json(worker_dir,'attempt-*-initialized.json')),
                     180,'native restored attempt in same JVM',worker)
        required_attempt=1 if kind=='snapshot-error' else 0
        wait_for(lambda:any(x['attempt']>=required_attempt and x['processed']>=COUNT for x in native_json(worker_dir,'completed-*.json')),
                 900,'completed all-data checkpoint',worker)
        assert worker.poll() is None, 'JVM must stay alive during automatic job recovery'
        if staging:
            target_rows=business_rows(json.loads(self.tag_call(['read',staging,'read_uncommitted'],directory)))
            target=len(target_rows)
        else:
            wait_for(lambda:self.progress(directory,'read_committed')>=COUNT,120,'committed full source coverage',worker)
            target_rows=json.loads(self.tag_call(['read',output,'read_committed'],directory))
            target=len(target_rows)
        wait_for(lambda:self.progress(directory,'read_committed')>=target,120,'observer committed drain',observer)
        for view in ('read_uncommitted','read_committed'):
            rows=json.loads(self.tag_call(['read',output,view],directory))
            wait_for(lambda:self.progress(directory,view)>=len(rows),60,'observer full drain',observer)
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
            'failure_action':'none' if kind=='none' else 'native snapshot IOException; automatic job restart',
            'controller_kill_during_failure':False,'initial_checkpoint':initial,
            'fault':native_json(worker_dir,'checkpoint-fault.json')[0] if kind!='none' else None,
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
        if mode=='eos' or kind=='none':
            assert committed['arrival_contract_pass'] is True, 'Reference/control did not preserve all arrivals correctly'
        else:
            assert committed['replayed_arrival_records']>0, 'Expected replay fault was not exercised'
        print(json.dumps({'case':name,'failure':kind,'attempts':case['initialized_attempts'],
                          'committed_audit':committed}),flush=True)
        return observations(case)

    def committed_offset(self,application,source):
        result=subprocess.run(STREAMS_JAVA+['offset',application,source],capture_output=True,text=True,timeout=30)
        assert result.returncode==0,result.stderr[-1000:]
        return int(result.stdout.strip())

    def streams_case(self,name):
        directory=self.root/name;directory.mkdir()
        source=self.root.name+'-'+name+'-in';output=self.root.name+'-'+name+'-out'
        self.create(source);self.create(output)
        observer=self.tag(['observe',output,directory],directory)
        for view in ('read_uncommitted','read_committed'):
            wait_for(lambda:(directory/('observer-ready-'+view)).exists(),35,'observer assignment',observer)
        worker_dir=directory/'streams';worker_dir.mkdir()
        self.tag_call(['seed',source,0,0],directory)
        fault='ack-crash' if name.endswith('recovery') else 'none'
        args=['arrival-streams',source,output,worker_dir,fault]
        worker=self.tag(args,worker_dir);pids=[worker.pid]
        wait_for(lambda:(worker_dir/f'lifecycle-{worker.pid}-RUNNING.json').exists(),180,'Streams native RUNNING',worker)
        wait_for(lambda:(worker_dir/'worker-ready').exists(),120,'Streams warmup',worker)
        generation=json.loads(self.tag_call(['seed',source,COUNT,RATE],directory,timeout=90))
        failure=None
        if fault=='ack-crash':
            wait_for(lambda:bool(native_json(worker_dir,'ack-before-commit.json')),120,'actual output ACK callback',worker)
            witness=native_json(worker_dir,'ack-before-commit.json')[0]
            raw=json.loads(self.tag_call(['read',output,'read_uncommitted'],directory))
            committed=json.loads(self.tag_call(['read',output,'read_committed'],directory))
            assert committed==[], 'Crash must occur before first data transaction commit'
            assert any(r['offset']==witness['offset'] and json.loads(r['value'])==witness['value'] for r in raw)
            assert witness['pid']==worker.pid and worker.poll() is None
            self.kill(worker)
            assert worker.returncode==-9
            failure={'kind':'SIGKILL after native output ACK, before transaction commit',
                'witness':witness,'killed_pid':worker.pid,'exit_code':worker.returncode,
                'raw_records_before_kill':len(raw),'committed_records_before_kill':0}
            worker=self.tag(args,worker_dir);pids.append(worker.pid)
            wait_for(lambda:(worker_dir/f'lifecycle-{worker.pid}-RUNNING.json').exists(),180,'recovered Streams RUNNING',worker)
        wait_for(lambda:self.progress(directory,'read_committed')>=COUNT,240,'all input arrivals committed',worker)
        config=json.loads((worker_dir/'configuration.json').read_text())
        deadline=time.monotonic()+120
        while True:
            offset=self.committed_offset(config['application_id'],source)
            if offset>=COUNT+1:break
            if time.monotonic()>deadline:raise TimeoutError('full source offset/state/output transaction commit')
            assert worker.poll() is None
            time.sleep(1)
        for view in ('read_uncommitted','read_committed'):
            rows=json.loads(self.tag_call(['read',output,view],directory))
            wait_for(lambda:self.progress(directory,view)>=len(rows),60,'full observer drain',observer)
        assert worker.poll() is None
        self.kill(worker)
        (directory/'observer-stop').touch();self.finish(observer,15)
        inputs=json.loads(self.tag_call(['read',source,'read_uncommitted'],directory))
        path=directory/'input.jsonl';path.write_text(''.join(r['value']+'\n' for r in business_rows(inputs)))
        case={'name':name,'engine':'Kafka Streams','source_topic':source,'output_topic':output,
            'generation':generation,'configuration':config,'worker_directory':str(worker_dir),
            'worker_pids':pids,'failure':failure,'source_committed_offset':offset,
            'lifecycle':native_json(worker_dir,'lifecycle-*.json'),'restore':native_json(worker_dir,'restore-*.json'),
            'input_file':file_info(path),'views':{}}
        for view in ('read_uncommitted','read_committed'):
            rows=json.loads(self.tag_call(['read',output,view],directory))
            sink=directory/('sink-'+view+'.jsonl')
            assert jsonl(sink)==[json.loads(r['value']) for r in rows]
            observed=jsonl(directory/('observations-'+view+'.jsonl'))
            assert [(r['offset'],r['value']) for r in observed]==[(r['offset'],r['value']) for r in rows]
            case['views'][view]={'audit':audit_arrivals(inputs,rows),'sink_file':file_info(sink)}
        assert case['views']['read_committed']['audit']['arrival_contract_pass'] is True
        if failure:assert case['views']['read_uncommitted']['audit']['replayed_arrival_records']>0
        observations(case)
        print(json.dumps({'case':name,'committed_audit':case['views']['read_committed']['audit'],
            'committed_visibility_ms':case['views']['read_committed']['visibility_ms']}),flush=True)
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
        cases=[]
        for (prefix,mode,fault),name in zip(CASES,CASE_NAMES):
            cases.append(self.streams_case(name) if mode=='streams' else self.flink_case(name,mode,fault))
        return {'experiment_valid':True,'run_id':self.root.name,
            'contract':'all source arrivals preserved and marked; business-ID duplicates are NEVER filtered',
            'source_arrivals_per_case':COUNT,'unique_business_ids':COUNT//3,'producer_threads':4,
            'target_rate_per_sec':RATE,'deduplication_ttl_ms':3600000,'flink_checkpoint_interval_ms':30000,
            'streams_commit_interval_ms':100,
            'versions':{'kafka_broker':'3.7.0','kafka_streams':'3.4.0','flink':'1.20.4','redis':'7.2.6','python':'3.10.12'},
            'limitations':['single machine, single partition, synthetic 3-second load, no production SLA',
                'warmup allocates workers; first business state/output path is not separately preheated',
                'Flink: actual CK IOException with automatic task restore in same JVM; Streams: ACK-before-commit process kill',
                'Redis exact seen keys in sink, updated before send; not an atomic Redis/Kafka transaction',
                'one-hour logical expiry configured, not observed for an actual hour; no broker/disk/rescale tests'],
            'cases':cases}

if __name__=='__main__':
    parser=argparse.ArgumentParser();parser.add_argument('--output',required=True);args=parser.parse_args()
    experiment=ArrivalExperiment(Path('/app/arrival-runs')/('arrival-'+uuid.uuid4().hex[:10]))
    try:Path(args.output).write_text(json.dumps(experiment.run(),indent=2)+'\n')
    finally:experiment.close()
