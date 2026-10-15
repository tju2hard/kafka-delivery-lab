"""Native Kafka Streams ID filtering, transaction crash and CK30 reference."""
import argparse
import json
from pathlib import Path
import subprocess
import time
import uuid

from deduplication import Deduplication, COUNT, RATE, audit_business, native_json
from latency import business_rows, distribution, file_info, jsonl
from run_lab import JAVA, wait_for

CASES=('streams-eos-filter-100ms','streams-eos-filter-ack-crash','flink-eos-filter-30s')
STREAMS_JAVA=JAVA[:-1]+['lab.StreamsDeduplicationDemo']


def observations(case):
    directory=Path(case['input_file']['path']).parent
    for view in ('read_uncommitted','read_committed'):
        path=directory/('observations-'+view+'.jsonl');rows=jsonl(path)
        case['views'][view]['observation_file']=file_info(path)
        case['views'][view]['visibility_ms']=distribution([
            row['observed_at_ms']-json.loads(row['value'])['ingested_at_ms'] for row in rows])
    return case


class StreamsExperiment(Deduplication):
    def tag(self,args,directory):
        if args[0]!='streams-filter':return super().tag(args,directory)
        self.serial+=1
        out=(directory/f'streams-{self.serial}.stdout').open('w')
        err=(directory/f'streams-{self.serial}.stderr').open('w')
        self.streams.extend([out,err])
        worker=subprocess.Popen(STREAMS_JAVA+list(map(str,args[1:])),stdout=out,stderr=err,start_new_session=True)
        worker.lab_output,worker.lab_error=Path(out.name),Path(err.name)
        self.processes.append(worker)
        return worker

    def committed_offset(self,application,source):
        result=subprocess.run(STREAMS_JAVA+['offset',application,source],capture_output=True,text=True,timeout=30)
        assert result.returncode==0,result.stderr[-1000:]
        return int(result.stdout.strip())

    def scenario(self,name):
        if name=='flink-eos-filter-30s':
            return observations(super().scenario(name,'state-eos','none'))
        directory=self.root/name;directory.mkdir()
        source=self.root.name+'-'+name+'-in';output=self.root.name+'-'+name+'-out'
        self.create(source);self.create(output)
        observer=self.tag(['observe',output,directory],directory)
        for view in ('read_uncommitted','read_committed'):
            wait_for(lambda:(directory/('observer-ready-'+view)).exists(),35,'observer assignment',observer)
        worker_dir=directory/'streams';worker_dir.mkdir()
        self.tag_call(['seed',source,0,0],directory)
        fault='ack-crash' if name.endswith('ack-crash') else 'none'
        args=['streams-filter',source,output,worker_dir,fault]
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
        wait_for(lambda:self.progress(directory,'read_committed')>=COUNT//3,240,'all unique business outputs committed',worker)
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
            case['views'][view]={'audit':audit_business(inputs,rows),'sink_file':file_info(sink)}
        assert case['views']['read_committed']['audit']['business_contract_pass'] is True
        if failure:assert case['views']['read_uncommitted']['audit']['duplicate_records']>0
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
        return {'experiment_valid':True,'run_id':self.root.name,'contract':'one complete first-arrival JSON per business ID',
            'source_arrivals_per_case':COUNT,'producer_threads':4,'target_rate_per_sec':RATE,
            'deduplication_ttl_ms':3600000,'streams_commit_interval_ms':100,'flink_checkpoint_interval_ms':30000,
            'versions':{'kafka_broker':'3.7.0','kafka_streams':'3.4.0','flink':'1.20.4','connector':'3.4.0-1.20','python':'3.10.12'},
            'limitations':['single machine, single input/output partition, one processing thread',
                'synthetic inputs; source keys must equal event_id',
                '100ms commit configuration is not a latency SLA; cold start excluded through warmup',
                'ACK-before-commit crash is SIGKILL of an owned JVM, not a Flink CK failure',
                'transaction timeout10s for bounded crash recovery; production setting needs separate sizing',
                'one-hour TTL expiry, broker/disk failures and rebalancing under rescale not tested'],
            'cases':[self.scenario(name) for name in CASES]}


if __name__=='__main__':
    parser=argparse.ArgumentParser();parser.add_argument('--output',required=True);args=parser.parse_args()
    experiment=StreamsExperiment(Path('/app/streams-filter-runs')/('streams-filter-'+uuid.uuid4().hex[:10]))
    try:Path(args.output).write_text(json.dumps(experiment.run(),indent=2)+'\n')
    finally:experiment.close()
