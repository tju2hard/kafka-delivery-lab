"""Reconsume Kafka and verify the one-record-per-business-ID contract."""
from collections import Counter
import gzip
import hashlib
import json
from pathlib import Path
import subprocess
import sys

sys.path.insert(0,'/app')
from deduplication import CASES, COUNT, assert_outcome, audit_business, native_json
from latency import TAG_JAVA, business_rows, jsonl
from run_lab import redis

destination=Path('/logs/verifier');checks=[];errors=[]
try:
    report=json.loads(Path('/app/deduplication-report.json').read_text())
    assert report['experiment_valid'] is True
    assert [c['name'] for c in report['cases']]==[c[0] for c in CASES]
    assert report['contract']=='first source-partition arrival per business ID exactly once; skip repeated IDs'
    for case in report['cases']:
        def read(topic,view):
            result=subprocess.run(TAG_JAVA+['read',topic,view],capture_output=True,text=True,timeout=60)
            assert result.returncode==0,result.stderr[-1500:]
            return json.loads(result.stdout)
        def check_file(info):
            path=Path(info['path']);assert path.stat().st_size==info['bytes']
            assert hashlib.sha256(path.read_bytes()).hexdigest()==info['sha256']
            return jsonl(path)
        inputs=business_rows(read(case['source_topic'],'read_uncommitted'))
        assert len(inputs)==COUNT
        values=[json.loads(r['value']) for r in inputs]
        assert {v['input_seq'] for v in values}==set(range(COUNT))
        assert set(Counter(v['event_id'] for v in values).values())=={3}
        assert check_file(case['input_file'])==values
        first={}
        for row,value in zip(inputs,values):
            assert row['key']==value['event_id']
            assert set(value)=={'event_id','input_seq','payload','ingested_at_ms'}
            seq=value['input_seq']
            assert value['payload']=={'source':'synthetic','value':seq,'worker':seq%4}
            first.setdefault(value['event_id'],value)
        worker=Path(case['worker_directory'])
        for field,pattern in (('initialized_attempts','attempt-*-initialized.json'),
                              ('completed_checkpoints','completed-*.json'),
                              ('checkpoint_fault','checkpoint-fault.json'),('sink_gap','sink-gap.json')):
            assert native_json(worker,pattern)==case[field]
        assert case['controller_kill_during_failure'] is False
        assert case['checkpoint_interval_ms']==30000 and case['deduplication_ttl_ms']==3600000
        assert case['initial_checkpoint']['processed']==0
        assert case['initial_checkpoint'] in case['completed_checkpoints']
        assert case['generation']['records']==COUNT and case['generation']['target_rate_per_sec']==3000
        assert case['generation']['started_at_ms']>case['initial_checkpoint']['completed_at_ms']
        assert any(c['processed']>=COUNT for c in case['completed_checkpoints'])
        fault=case['fault_kind']
        if fault=='none':
            assert all(a['attempt']==0 for a in case['initialized_attempts'])
            assert not case['sink_gap'] and not case['checkpoint_fault']
        else:
            assert any(a['attempt']>=1 and a['is_restored'] for a in case['initialized_attempts'])
            assert any(c['attempt']>=1 and c['processed']>=COUNT for c in case['completed_checkpoints'])
            if fault=='checkpoint':
                witness,=case['checkpoint_fault']
                assert witness['checkpoint_id']>case['initial_checkpoint']['checkpoint_id']
                assert witness['processed_before_failure']>0 and not case['sink_gap']
            else:
                witness,=case['sink_gap'];assert not case['checkpoint_fault']
                assert redis('GET',case['output_topic']+':'+witness['event_id'])==case['native_redis_fault_id_value']
                assert witness['stage']==fault and witness['attempt']==0
                assert witness['input']==first[witness['event_id']]
                assert witness['at_ms']>case['initial_checkpoint']['completed_at_ms']
                if fault=='after-ack':
                    assert witness['kafka_acked'] is True and witness['kafka_offset']>=0
                    assert witness['redis_value'] is None
                    assert case['native_redis_fault_id_value']=='sent'
                else:
                    assert witness['kafka_acked'] is False and witness['kafka_offset']==-1
                    assert witness['redis_value']==case['native_redis_fault_id_value']=='reserved'
        exports=[]
        def export(stem,rows):
            path=destination/'data'/case['name']/(stem+'.jsonl.gz');path.parent.mkdir(parents=True,exist_ok=True)
            digest=hashlib.sha256()
            with gzip.open(path,'wt',encoding='utf-8') as stream:
                for row in rows:
                    line=json.dumps(json.loads(row['value']),sort_keys=True,separators=(',',':'))+'\n'
                    stream.write(line);digest.update(line.encode())
            exports.append({'path':str(path.relative_to(destination)),'rows':len(rows),
                'uncompressed_sha256':digest.hexdigest(),'gzip_sha256':hashlib.sha256(path.read_bytes()).hexdigest()})
        export('input',inputs)
        for view in ('read_uncommitted','read_committed'):
            rows=read(case['output_topic'],view);actual=[json.loads(r['value']) for r in rows]
            assert check_file(case['views'][view]['sink_file'])==actual
            assert audit_business(inputs,rows)==case['views'][view]['audit']
            # Independent conservation probe: same total can conceal one missing ID + one duplicate.
            counts=Counter(v['event_id'] for v in actual)
            exact=(counts==Counter({event_id:1 for event_id in first})
                   and all(v==first.get(v['event_id']) and r['key']==v['event_id'] for r,v in zip(rows,actual)))
            assert exact==case['views'][view]['audit']['business_contract_pass']
            if fault=='after-ack':
                matching=[r for r in rows if r['offset']==witness['kafka_offset']]
                assert len(matching)==1 and json.loads(matching[0]['value'])==witness['input']
            export('output-'+view,rows)
        assert_outcome(case)
        case['exported_jsonl']=exports
        checks.append({'name':case['name'],'passed':True,
            'physical_audit':case['views']['read_uncommitted']['audit'],
            'committed_audit':case['views']['read_committed']['audit']})
    (destination/'deduplication-comparison.json').write_text(json.dumps(report,indent=2)+'\n')
    reward=1
except (AssertionError,OSError,ValueError,KeyError,TypeError,subprocess.TimeoutExpired) as error:
    reward=0;errors.append(type(error).__name__+': '+str(error))
(destination/'checks.json').write_text(json.dumps({'report_validation_reward':reward,'checks':checks,'errors':errors},indent=2)+'\n')
(destination/'reward.txt').write_text(str(reward)+'\n')
print(json.dumps({'report_validation_reward':reward,'cases':len(checks),'errors':errors}))
