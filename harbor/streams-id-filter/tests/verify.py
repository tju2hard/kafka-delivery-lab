"""Fresh native consumers verify transactions, filtering and observed visibility."""
import gzip
import hashlib
import json
from pathlib import Path
import subprocess
import sys

sys.path.insert(0,'/app')
from streams_filter import CASES, COUNT, STREAMS_JAVA
from deduplication import audit_business, native_json
from latency import TAG_JAVA, business_rows, distribution, jsonl

destination=Path('/logs/verifier');checks=[];errors=[]
try:
    report=json.loads(Path('/app/streams-filter-report.json').read_text())
    assert report['experiment_valid'] is True
    assert [c['name'] for c in report['cases']]==list(CASES)
    for case in report['cases']:
        def read(topic,view):
            p=subprocess.run(TAG_JAVA+['read',topic,view],capture_output=True,text=True,timeout=60)
            assert p.returncode==0,p.stderr[-1000:]
            return json.loads(p.stdout)
        def check_file(info):
            p=Path(info['path']);assert p.stat().st_size==info['bytes']
            assert hashlib.sha256(p.read_bytes()).hexdigest()==info['sha256']
            return jsonl(p)
        all_inputs=read(case['source_topic'],'read_uncommitted');inputs=business_rows(all_inputs)
        values=[json.loads(r['value']) for r in inputs]
        assert len(values)==COUNT and {v['input_seq'] for v in values}==set(range(COUNT))
        assert check_file(case['input_file'])==values
        first={}
        for row,v in zip(inputs,values):
            assert row['key']==v['event_id']
            seq=v['input_seq'];assert v['payload']=={'source':'synthetic','value':seq,'worker':seq%4}
            first.setdefault(v['event_id'],json.dumps(v,sort_keys=True,separators=(',',':')))
        assert len(first)==COUNT//3
        worker=Path(case['worker_directory']);is_streams=case['name'].startswith('streams-')
        if is_streams:
            config=case['configuration']
            assert json.loads((worker/'configuration.json').read_text())==config
            assert config['commit_interval_ms']==100 and config['processing_guarantee']=='exactly_once_v2'
            assert config['filter_duplicates'] is True and config['ttl_ms']==3600000
            assert native_json(worker,'lifecycle-*.json')==case['lifecycle']
            assert native_json(worker,'restore-*.json')==case['restore']
            for pid in case['worker_pids']:assert any(s['pid']==pid and s['state']=='RUNNING' for s in case['lifecycle'])
            p=subprocess.run(STREAMS_JAVA+['offset',config['application_id'],case['source_topic']],capture_output=True,text=True,timeout=30)
            assert p.returncode==0,p.stderr[-1000:]
            assert int(p.stdout)==case['source_committed_offset']==max(r['offset'] for r in all_inputs)+1
            if case['failure']:
                failure=case['failure'];witness=failure['witness']
                assert native_json(worker,'ack-before-commit.json')==[witness]
                assert failure['exit_code']==-9 and failure['killed_pid']==case['worker_pids'][0]==witness['pid']
                assert len(case['worker_pids'])==2 and len(set(case['worker_pids']))==2
                assert failure['committed_records_before_kill']==0 and failure['raw_records_before_kill']>0
            else:assert len(case['worker_pids'])==1 and not native_json(worker,'ack-before-commit.json')
        else:
            assert case['mode']=='state-eos' and case['fault_kind']=='none'
            assert case['checkpoint_interval_ms']==30000
            assert native_json(worker,'completed-*.json')==case['completed_checkpoints']
            assert any(c['processed']>=COUNT for c in case['completed_checkpoints'])
            assert all(a['attempt']==0 for a in case['initialized_attempts'])
            assert case['generation']['started_at_ms']>case['initial_checkpoint']['completed_at_ms']
        exports=[]
        def export(stem,values):
            path=destination/'data'/case['name']/(stem+'.jsonl.gz');path.parent.mkdir(parents=True,exist_ok=True)
            digest=hashlib.sha256()
            with gzip.open(path,'wt',encoding='utf-8') as stream:
                for value in values:
                    line=json.dumps(value,sort_keys=True,separators=(',',':'))+'\n';stream.write(line);digest.update(line.encode())
            exports.append({'path':str(path.relative_to(destination)),'rows':len(values),
                'uncompressed_sha256':digest.hexdigest(),'gzip_sha256':hashlib.sha256(path.read_bytes()).hexdigest()})
        export('input',values)
        for view in ('read_uncommitted','read_committed'):
            rows=read(case['output_topic'],view);actual=[json.loads(r['value']) for r in rows]
            info=case['views'][view];assert check_file(info['sink_file'])==actual
            assert audit_business(inputs,rows)==info['audit']
            assert all(json.dumps(v,sort_keys=True,separators=(',',':'))==first[v['event_id']] for v in actual)
            observed=check_file(info['observation_file'])
            assert [(r['offset'],r['value']) for r in observed]==[(r['offset'],r['value']) for r in rows]
            assert distribution([r['observed_at_ms']-json.loads(r['value'])['ingested_at_ms'] for r in observed])==info['visibility_ms']
            if is_streams and case['failure']:
                matches=[r for r in rows if r['offset']==witness['offset']]
                if view=='read_uncommitted':assert len(matches)==1 and json.loads(matches[0]['value'])==witness['value']
                else:assert not matches, 'Crashed transaction became committed'
            export('output-'+view,actual);export('observations-'+view,observed)
        assert case['views']['read_committed']['audit']['business_contract_pass'] is True
        if is_streams and case['failure']:assert case['views']['read_uncommitted']['audit']['duplicate_records']>0
        case['exported_jsonl']=exports
        checks.append({'name':case['name'],'passed':True,'committed_audit':case['views']['read_committed']['audit'],
            'committed_visibility_ms':case['views']['read_committed']['visibility_ms']})
    (destination/'streams-filter-comparison.json').write_text(json.dumps(report,indent=2)+'\n')
    reward=1
except (AssertionError,OSError,ValueError,KeyError,TypeError,subprocess.TimeoutExpired) as error:
    reward=0;errors.append(type(error).__name__+': '+str(error))
(destination/'checks.json').write_text(json.dumps({'report_validation_reward':reward,'checks':checks,'errors':errors},indent=2)+'\n')
(destination/'reward.txt').write_text(str(reward)+'\n')
print(json.dumps({'report_validation_reward':reward,'cases':len(checks),'errors':errors}))
