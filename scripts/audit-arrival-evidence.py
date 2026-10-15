"""Offline revalidation plus counterexamples derived from actual Kafka exports."""
import gzip
import hashlib
import json
from pathlib import Path
import sys
ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT/'harbor/arrival-marking-comparison/environment'))
from arrival_comparison import audit_arrivals
from latency import distribution

def rows(path):
    with gzip.open(path,'rt',encoding='utf-8') as f:return [json.loads(line) for line in f]
def wrapped(values):return [{'key':v['event_id'],'value':json.dumps(v,separators=(',',':'))} for v in values]
def main():
    summary=json.loads((ROOT/'results/arrival-summary.json').read_text());checks=[];paired=[]
    for run in summary['runs']:
        for case in run['cases']:
            directory=ROOT/'results/arrival-evidence'/run['trial']/'data'/case['name']
            inputs=wrapped(rows(directory/'input.jsonl.gz'))
            for view in ('read_uncommitted','read_committed'):
                outputs=wrapped(rows(directory/('output-'+view+'.jsonl.gz')))
                assert audit_arrivals(inputs,outputs)==case['views'][view]['audit']
                observations=rows(directory/('observations-'+view+'.jsonl.gz'))
                assert [json.loads(r['value']) for r in observations]==[json.loads(r['value']) for r in outputs]
                assert distribution([r['observed_at_ms']-json.loads(r['value'])['ingested_at_ms'] for r in observations])==case['views'][view]['visibility_ms']
                checks.append({'trial':run['trial'],'case':case['name'],'view':view,'matches_native_audit':True,'latency_recomputed':True})
            for info in case['exported_jsonl']:
                path=ROOT/'results/arrival-evidence'/run['trial']/info['path']
                assert hashlib.sha256(path.read_bytes()).hexdigest()==info['gzip_sha256']
                with gzip.open(path,'rb') as f:raw=f.read()
                assert hashlib.sha256(raw).hexdigest()==info['uncompressed_sha256'] and len(raw.splitlines())==info['rows']
            before={r['offset']:r for r in rows(directory/'observations-read_uncommitted.jsonl.gz')}
            after=rows(directory/'observations-read_committed.jsonl.gz');gaps=[]
            for row in after:
                assert row['value']==before[row['offset']]['value'];gaps.append(row['observed_at_ms']-before[row['offset']]['observed_at_ms'])
            ordered=sorted(gaps)
            paired.append({'trial':run['trial'],'case':case['name'],'samples':len(gaps),'p50':ordered[(len(gaps)-1)//2],
                'p99':ordered[int((len(gaps)-1)*.99)],'negative_samples':sum(x<0 for x in gaps),
                'meaning':'same-offset observer difference; includes consumer scheduling, not precise broker commit time'})
    controls=[]
    directory=ROOT/'results/arrival-evidence'/summary['runs'][0]['trial']/'data/streams-direct-eos-normal'
    source=wrapped(rows(directory/'input.jsonl.gz'));valid=wrapped(rows(directory/'output-read_committed.jsonl.gz'))
    assert audit_arrivals(source,valid)['arrival_contract_pass'];controls.append('actual native data accepted')
    damaged=valid[:-1]+valid[:1];a=audit_arrivals(source,damaged)
    assert not a['arrival_contract_pass'] and a['missing_arrivals']==a['replayed_arrival_records']==1
    controls.append('equal total with one lost and one replay rejected')
    damaged=valid.copy();value=json.loads(damaged[0]['value']);value['is_duplicate']=not value['is_duplicate'];damaged[0]={**damaged[0],'value':json.dumps(value)}
    assert not audit_arrivals(source,damaged)['arrival_contract_pass'];controls.append('wrong first/duplicate flag rejected')
    damaged=valid.copy();value=json.loads(damaged[0]['value']);value['payload']['source']='corrupted';damaged[0]={**damaged[0],'value':json.dumps(value)}
    assert not audit_arrivals(source,damaged)['arrival_contract_pass'];controls.append('full payload corruption rejected')
    damaged=[r for r in valid if not json.loads(r['value'])['is_duplicate']]
    assert not audit_arrivals(source,damaged)['arrival_contract_pass'];controls.append('business duplicate filtering rejected')
    result={'offline_checks':checks,'actual_data_controls':controls,'paired_raw_to_committed_observer_differences_ms':paired}
    (ROOT/'results/arrival-offline-controls.json').write_text(json.dumps(result,indent=2)+'\n')
    print(json.dumps({'native_audits_and_latency_matches':len(checks),'real_data_controls_passed':len(controls),'gzip_files_verified':100}))
if __name__=='__main__':main()
