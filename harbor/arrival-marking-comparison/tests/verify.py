"""Independently reread Kafka, recompute arrival/flag/latency metrics and export JSONL."""
import gzip
import hashlib
import json
from pathlib import Path
import subprocess
import sys
sys.path.insert(0,'/app')
from arrival_comparison import CASE_NAMES,COUNT,TAG_JAVA,STREAMS_JAVA,audit_arrivals,native_json
from latency import business_rows,distribution,jsonl
out=Path('/logs/verifier');checks=[];errors=[]
try:
    report=json.loads(Path('/app/arrival-report.json').read_text())
    assert report['experiment_valid'] is True
    assert [c['name'] for c in report['cases']]==CASE_NAMES
    for case in report['cases']:
        def read(topic,view):
            p=subprocess.run(TAG_JAVA+['read',topic,view],capture_output=True,text=True,timeout=60)
            assert p.returncode==0,p.stderr[-1200:]
            return json.loads(p.stdout)
        def file(info):
            p=Path(info['path']);assert p.stat().st_size==info['bytes']
            assert hashlib.sha256(p.read_bytes()).hexdigest()==info['sha256']
            return jsonl(p)
        inputs=read(case['source_topic'],'read_uncommitted');source=business_rows(inputs)
        values=[json.loads(r['value']) for r in source]
        assert len(values)==COUNT and {v['input_seq'] for v in values}==set(range(COUNT))
        assert len({v['event_id'] for v in values})==COUNT//3
        assert file(case['input_file'])==values
        for row,value in zip(source,values):
            seq=value['input_seq'];assert row['key']==value['event_id']
            assert value['payload']=={'source':'synthetic','value':seq,'worker':seq%4}
        worker=Path(case['worker_directory']);direct=case['name'].startswith('streams-direct')
        recovered=case['name'].endswith('recovery')
        if direct:
            config=case['configuration'];assert json.loads((worker/'configuration.json').read_text())==config
            assert config['filter_duplicates'] is False and config['ttl_ms']==3600000
            assert config['commit_interval_ms']==100 and config['processing_guarantee']=='exactly_once_v2'
            assert native_json(worker,'lifecycle-*.json')==case['lifecycle']
            assert native_json(worker,'restore-*.json')==case['restore']
            assert len(case['worker_pids'])==(2 if recovered else 1)
            assert len(set(case['worker_pids']))==len(case['worker_pids'])
            for pid in case['worker_pids']:assert any(v['pid']==pid and v['state']=='RUNNING' for v in case['lifecycle'])
            p=subprocess.run(STREAMS_JAVA+['offset',config['application_id'],case['source_topic']],capture_output=True,text=True,timeout=30)
            assert p.returncode==0,p.stderr[-1200:]
            assert int(p.stdout)==case['source_committed_offset']==max(r['offset'] for r in inputs)+1
            if recovered:
                failure=case['failure'];witness=failure['witness']
                assert native_json(worker,'ack-before-commit.json')==[witness]
                assert failure['exit_code']==-9 and failure['killed_pid']==case['worker_pids'][0]==witness['pid']
                assert failure['committed_records_before_kill']==0 and failure['raw_records_before_kill']>0
            else:assert case['failure'] is None and not native_json(worker,'ack-before-commit.json')
        else:
            assert case['checkpoint_interval_ms']==30000 and case['deduplication_ttl_ms']==3600000
            assert case['controller_kill_during_failure'] is False
            assert native_json(worker,'attempt-*-initialized.json')==case['initialized_attempts']
            assert native_json(worker,'completed-*.json')==case['completed_checkpoints']
            assert native_json(worker,'aborted-*.json')==case['aborted_checkpoints']
            assert case['initial_checkpoint']['processed']==0
            assert case['generation']['started_at_ms']>case['initial_checkpoint']['completed_at_ms']
            if recovered:
                assert native_json(worker,'checkpoint-fault.json')==[case['fault']]
                assert case['fault']['checkpoint_id']>case['initial_checkpoint']['checkpoint_id']
                assert case['fault']['processed_before_failure']>0
                assert any(v['attempt']>=1 and v['is_restored'] for v in case['initialized_attempts'])
            else:
                assert case['fault'] is None and not native_json(worker,'checkpoint-fault.json')
                assert all(v['attempt']==0 for v in case['initialized_attempts'])
            assert any(v['processed']>=COUNT and v['attempt']>=(1 if recovered else 0) for v in case['completed_checkpoints'])
        exports=[]
        def export(stem,values):
            p=out/'data'/case['name']/(stem+'.jsonl.gz');p.parent.mkdir(parents=True,exist_ok=True)
            digest=hashlib.sha256()
            with gzip.open(p,'wt',encoding='utf-8') as stream:
                for value in values:
                    line=json.dumps(value,sort_keys=True,separators=(',',':'))+'\n';stream.write(line);digest.update(line.encode())
            exports.append({'path':str(p.relative_to(out)),'rows':len(values),'uncompressed_sha256':digest.hexdigest(),
                'gzip_sha256':hashlib.sha256(p.read_bytes()).hexdigest()})
        export('input',values)
        for view in ('read_uncommitted','read_committed'):
            rows=read(case['output_topic'],view);actual=[json.loads(r['value']) for r in rows];info=case['views'][view]
            assert file(info['sink_file'])==actual
            assert audit_arrivals(inputs,rows)==info['audit']
            observed=file(info['observation_file'])
            assert [(r['offset'],r['value']) for r in observed]==[(r['offset'],r['value']) for r in rows]
            assert distribution([r['observed_at_ms']-json.loads(r['value'])['ingested_at_ms'] for r in observed])==info['visibility_ms']
            if direct and recovered:
                matches=[r for r in rows if r['offset']==witness['offset']]
                if view=='read_uncommitted':assert len(matches)==1 and json.loads(matches[0]['value'])==witness['value']
                else:assert not matches
            export('output-'+view,actual);export('observations-'+view,observed)
        a=case['views']['read_committed']['audit']
        assert a['missing_arrivals']==a['payload_mismatches']==a['unexpected_arrivals']==0
        assert a['output_unique_business_ids']==COUNT//3
        should_pass=not recovered or direct or case['mode']=='eos'
        assert a['arrival_contract_pass'] is should_pass
        if should_pass:assert a['output_arrivals']==COUNT and a['false_markers']==COUNT//3 and a['true_markers']==COUNT*2//3
        else:assert a['replayed_arrival_records']>0
        if case.get('staging_topic'):
            assert len(business_rows(read(case['staging_topic'],'read_uncommitted')))==a['output_arrivals']
        case['exported_jsonl']=exports
        checks.append({'name':case['name'],'passed':True,'committed_audit':a,'visibility_ms':case['views']['read_committed']['visibility_ms']})
    (out/'arrival-comparison.json').write_text(json.dumps(report,indent=2)+'\n');reward=1
except (AssertionError,OSError,ValueError,KeyError,TypeError,subprocess.TimeoutExpired) as error:
    reward=0;errors.append(type(error).__name__+': '+str(error))
(out/'checks.json').write_text(json.dumps({'report_validation_reward':reward,'checks':checks,'errors':errors},indent=2)+'\n')
(out/'reward.txt').write_text(str(reward)+'\n')
print(json.dumps({'report_validation_reward':reward,'cases':len(checks),'errors':errors}))
