"""Compare full JSON input/output files, including every duplicate marker.

Input order is Kafka partition order. This load-test checker requires the whole
run to fit inside the one-hour horizon; it does not certify expiry behavior.
For recovery diagnostics (markers in final output order), use
compare-recovery-jsonl.py instead.
"""
import argparse
from collections import Counter
import gzip
import json
from pathlib import Path


def records(path):
    opener = gzip.open if str(path).endswith('.gz') else open
    with opener(path, 'rt', encoding='utf-8') as stream:
        for line in stream:
            if line.strip():
                yield json.loads(line)


def compare(source, sink):
    expected, seen = {}, set()
    timestamps = []
    for row in records(source):
        seq = row['input_seq']
        if seq in expected:
            raise ValueError('Input input_seq must be unique')
        expected[seq] = {**row, 'is_duplicate': row['event_id'] in seen}
        seen.add(row['event_id'])
        timestamps.append(row['ingested_at_ms'])
    if not timestamps or max(timestamps)-min(timestamps) >= 3600000:
        raise ValueError('This checker supports nonempty load runs within one hour')
    minimum_timestamp = min(timestamps)
    counts, flags, payloads, types = Counter(), 0, 0, 0
    false_count, true_count = 0, 0
    for row in records(sink):
        seq = row['input_seq']; counts[seq] += 1
        if seq not in expected:
            continue
        reference = expected[seq]
        if type(row.get('is_duplicate')) is not bool:
            types += 1
        elif row['is_duplicate']:
            true_count += 1
        else:
            false_count += 1
        if type(row.get('is_duplicate')) is not bool or row['is_duplicate'] != reference['is_duplicate']:
            flags += 1
        if ({k:v for k,v in row.items() if k not in ('is_duplicate','marked_at_ms')}
                != {k:v for k,v in reference.items() if k != 'is_duplicate'}):
            payloads += 1
        if row.get('marked_at_ms', -1) < row['ingested_at_ms']:
            types += 1
        if row.get('marked_at_ms', minimum_timestamp)-minimum_timestamp >= 3600000:
            raise ValueError('Processing crossed the one-hour horizon; use an expiry-aware reference')
    result = {'input_arrivals': len(expected), 'output_arrivals': sum(counts.values()),
        'unique_business_ids': len(seen), 'first_markers': false_count, 'duplicate_markers': true_count,
        'missing_arrivals': len(set(expected)-set(counts)),
        'unexpected_arrivals': sum(n for seq,n in counts.items() if seq not in expected),
        'repeated_arrival_sequences': sum(max(0,n-1) for n in counts.values()),
        'payload_mismatches': payloads, 'flag_mismatches': flags, 'schema_errors': types}
    result['passed'] = all(result[k] == 0 for k in (
        'missing_arrivals','unexpected_arrivals','repeated_arrival_sequences',
        'payload_mismatches','flag_mismatches','schema_errors'))
    return result


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--input', required=True)
    parser.add_argument('--output', required=True)
    parser.add_argument('--report')
    args = parser.parse_args()
    try:
        result = compare(args.input, args.output)
    except (ValueError, KeyError, TypeError, OSError) as error:
        result = {'passed': False, 'error': type(error).__name__+': '+str(error)}
    if args.report:
        Path(args.report).write_text(json.dumps(result, ensure_ascii=False, indent=2)+'\n')
    print(json.dumps(result, ensure_ascii=False))
    raise SystemExit(0 if result['passed'] else 1)
