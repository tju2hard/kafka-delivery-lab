"""Fresh native consumers audit recovery, payload conservation and markers."""
import gzip
import hashlib
import json
from pathlib import Path
import subprocess
import sys

sys.path.insert(0,'/app')
from recovery import CASES, COUNT, TAG_JAVA, audit_arrivals, native_json, business_rows, jsonl

output=Path('/logs/verifier');checks=[];errors=[]
try:
    report=json.loads(Path('/app/recovery-report.json').read_text())
    assert report['experiment_valid'] is True
    assert [c['name'] for c in report['cases']]==[s[0] for s in CASES]
    for case in report['cases']:
        def read(topic,view):
            p=subprocess.run(TAG_JAVA+['read',topic,view],capture_output=True,text=True,timeout=60)
            assert p.returncode==0,p.stderr[-1500:]
            return json.loads(p.stdout)
        def check_file(info):
            p=Path(info['path']);assert p.stat().st_size==info['bytes']
            assert hashlib.sha256(p.read_bytes()).hexdigest()==info['sha256']
            return jsonl(p)
        inputs=read(case['source_topic'],'read_uncommitted')
        assert check_file(case['input_file'])==[json.loads(r['value']) for r in business_rows(inputs)]
        worker=Path(case['worker_directory'])
        assert native_json(worker,'checkpoint-fault.json')==[case['fault']]
        assert native_json(worker,'attempt-*-initialized.json')==case['initialized_attempts']
        assert native_json(worker,'completed-*.json')==case['completed_checkpoints']
        assert native_json(worker,'aborted-*.json')==case['aborted_checkpoints']
        assert case['controller_kill_during_failure'] is False
        assert case['checkpoint_interval_ms']==30000 and case['deduplication_ttl_ms']==3600000
        assert case['initial_checkpoint']['processed']==0
        assert case['fault']['checkpoint_id']>case['initial_checkpoint']['checkpoint_id']
        assert case['fault']['processed_before_failure']>0
        assert case['generation']['started_at_ms']>case['initial_checkpoint']['completed_at_ms']
        if case['failure_kind']=='snapshot-error':
            assert any(a['attempt']>=1 and a['is_restored'] for a in case['initialized_attempts'])
            assert any(c['attempt']>=1 and c['processed']>=COUNT for c in case['completed_checkpoints'])
        else:
            assert case['checkpoint_timeout_ms']==5000 and case['tolerable_checkpoint_failures']==1
            assert any(a['checkpoint_id']==case['fault']['checkpoint_id'] for a in case['aborted_checkpoints'])
            assert all(a['attempt']==0 for a in case['initialized_attempts']), 'Tolerated timeout unexpectedly restarted task'
            assert any(c['checkpoint_id']>case['fault']['checkpoint_id'] and c['processed']>=COUNT for c in case['completed_checkpoints'])
        dest=output/'data'/case['name'];dest.mkdir(parents=True)
        exports=[]
        def export(stem,rows):
            p=dest/(stem+'.jsonl.gz');digest=hashlib.sha256()
            with gzip.open(p,'wt',encoding='utf-8') as stream:
                for row in rows:
                    line=json.dumps(json.loads(row['value']),sort_keys=True,separators=(',',':'))+'\n'
                    stream.write(line);digest.update(line.encode())
            exports.append({'path':str(p.relative_to(output)),'rows':len(rows),'uncompressed_sha256':digest.hexdigest(),
                            'gzip_sha256':hashlib.sha256(p.read_bytes()).hexdigest()})
        export('input',business_rows(inputs))
        for view in ('read_uncommitted','read_committed'):
            rows=read(case['output_topic'],view)
            assert check_file(case['views'][view]['sink_file'])==[json.loads(r['value']) for r in rows]
            assert audit_arrivals(inputs,rows)==case['views'][view]['audit']
            export('output-'+view,rows)
        committed=case['views']['read_committed']['audit']
        assert committed['missing_arrivals']==committed['payload_mismatches']==0
        if case['mode']=='eos' or case['failure_kind']=='timeout':
            assert committed['arrival_contract_pass'] is True
        else:
            assert committed['replayed_arrival_records']>0
        if case['staging_topic']:
            staged=business_rows(read(case['staging_topic'],'read_uncommitted'))
            assert len(staged)==committed['output_arrivals']
            assert committed['flag_mismatches_in_output_order']==0
        case['exported_jsonl']=exports
        checks.append({'name':case['name'],'passed':True,'committed_audit':committed})
    (output/'recovery-comparison.json').write_text(json.dumps(report,indent=2)+'\n')
    reward=1
except (AssertionError,OSError,ValueError,KeyError,TypeError,subprocess.TimeoutExpired) as error:
    reward=0;errors.append(type(error).__name__+': '+str(error))
(output/'checks.json').write_text(json.dumps({'report_validation_reward':reward,'checks':checks,'errors':errors},indent=2)+'\n')
(output/'reward.txt').write_text(str(reward)+'\n')
print(json.dumps({'report_validation_reward':reward,'cases':len(checks),'errors':errors}))
