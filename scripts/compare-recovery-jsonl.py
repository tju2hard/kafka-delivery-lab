"""Offline audit of this lab's 30000-arrival recovery JSONL exports.

Unlike compare-jsonl.py (normal one-to-one load), flags are checked in final
output order, while input_seq multiplicity independently exposes source replay.
Kafka keys/offsets and native checkpoint witnesses are verified by Harbor.
"""
import argparse
import gzip
import json
from pathlib import Path
import sys

sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'harbor/id-marking-recovery/environment'))
from recovery import audit_arrivals


def rows(path):
    opener=gzip.open if str(path).endswith('.gz') else open
    with opener(path,'rt',encoding='utf-8') as stream:
        values=[json.loads(line) for line in stream if line.strip()]
    return [{'key':v['event_id'],'value':json.dumps(v,separators=(',',':'))} for v in values]


if __name__=='__main__':
    parser=argparse.ArgumentParser()
    parser.add_argument('--input',required=True)
    parser.add_argument('--output',required=True)
    parser.add_argument('--report')
    args=parser.parse_args()
    try:
        inputs,outputs=rows(args.input),rows(args.output)
        timestamps=[json.loads(r['value'])['ingested_at_ms'] for r in inputs]
        marked=[json.loads(r['value'])['marked_at_ms'] for r in outputs]
        assert timestamps and marked and max(marked)-min(timestamps)<3600000, 'Run must fit inside the 1h horizon'
        result=audit_arrivals(inputs,outputs)
    except (AssertionError,OSError,ValueError,KeyError,TypeError) as error:
        result={'arrival_contract_pass':False,'error':type(error).__name__+': '+str(error)}
    if args.report:Path(args.report).write_text(json.dumps(result,ensure_ascii=False,indent=2)+'\n')
    print(json.dumps(result,ensure_ascii=False))
    raise SystemExit(0 if result['arrival_contract_pass'] else 1)
