"""Audit business-ID filtering; exit 1 means missing/duplicate/incorrect output."""
import argparse
import gzip
import json
from pathlib import Path
import sys

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT/'harbor/id-deduplication/environment'))
from deduplication import audit_business


def read(path):
    opener=gzip.open if str(path).endswith('.gz') else open
    with opener(path,'rt',encoding='utf-8') as stream:
        values=[json.loads(line) for line in stream]
    # Offline JSON-only exports omit native Kafka keys; the native verifier checks keys.
    return [{'key':v['event_id'],'value':json.dumps(v)} for v in values]


if __name__=='__main__':
    parser=argparse.ArgumentParser();parser.add_argument('--input',required=True)
    parser.add_argument('--output',required=True);args=parser.parse_args()
    result=audit_business(read(args.input),read(args.output))
    print(json.dumps(result,ensure_ascii=False,indent=2))
    raise SystemExit(0 if result['business_contract_pass'] else 1)
