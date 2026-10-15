"""Reconsume all records independently; export actual JSONL sinks and metrics."""
import gzip
import hashlib
import json
from pathlib import Path
import subprocess
import sys

sys.path.insert(0, '/app')
from latency import TAG_JAVA, CASES, COUNT, RATE, validate, distribution, business_rows, jsonl

output = Path('/logs/verifier')
checks, errors = [], []
try:
    report = json.loads(Path('/app/tagging-report.json').read_text())
    assert report['experiment_valid'] is True
    assert [c['name'] for c in report['cases']] == list(CASES)
    boundary = subprocess.run(TAG_JAVA+['ttl-policy-check'],capture_output=True,text=True,timeout=30)
    assert boundary.returncode == 0 and json.loads(boundary.stdout)['boundary_policy_pass'] is True
    for case in report['cases']:
        def read(topic, view):
            p = subprocess.run(TAG_JAVA+['read', topic, view], capture_output=True, text=True, timeout=60)
            assert p.returncode == 0, p.stderr[-1500:]
            return json.loads(p.stdout)
        def checked_file(info):
            p = Path(info['path'])
            assert p.stat().st_size == info['bytes']
            assert hashlib.sha256(p.read_bytes()).hexdigest() == info['sha256']
            return jsonl(p)
        inputs = read(case['source_topic'], 'read_uncommitted')
        data = business_rows(inputs)
        assert checked_file(case['input_file']) == [json.loads(r['value']) for r in data]
        timestamps = [json.loads(r['value'])['ingested_at_ms'] for r in data]
        case['independent_input_rate_per_sec'] = COUNT / ((max(timestamps)-min(timestamps))/1000)
        assert case['generation']['records'] == COUNT
        assert case['generation']['target_rate_per_sec'] == RATE
        assert case['generation']['producer_threads'] == 4
        assert case['ttl_configuration'] == {'ttl_ms':3600000,'refresh':'every_arrival','time_basis':'processing'}
        if case['name'] == 'redis-marker':
            assert 3000000 < case['native_redis_remaining_ttl_ms'] <= 3600000
        dest = output/'data'/case['name']; dest.mkdir(parents=True)
        exported = []
        def export(name, values):
            path = dest/(name+'.jsonl.gz')
            digest = hashlib.sha256()
            with gzip.open(path, 'wt', encoding='utf-8') as stream:
                for value in values:
                    line = json.dumps(value, sort_keys=True, separators=(',',':'))+'\n'
                    stream.write(line); digest.update(line.encode())
            exported.append({'path': str(path.relative_to(output)), 'rows': len(values),
                             'uncompressed_sha256': digest.hexdigest(),
                             'gzip_sha256': hashlib.sha256(path.read_bytes()).hexdigest()})
        export('input', [json.loads(r['value']) for r in data])
        for view in ('read_uncommitted', 'read_committed'):
            rows = read(case['output_topic'], view)
            checked = validate(inputs, rows)
            reported = case['views'][view]
            assert checked == reported['flags']
            assert checked_file(reported['sink_file']) == [json.loads(r['value']) for r in rows]
            live = checked_file(reported['observation_file'])
            assert len(live) == COUNT
            assert [(r['key'],r['value'],r['offset']) for r in live] == [(r['key'],r['value'],r['offset']) for r in rows]
            assert distribution([r['observed_at_ms']-json.loads(r['value'])['ingested_at_ms'] for r in live]) == reported['visibility_ms']
            assert distribution([json.loads(r['value'])['marked_at_ms']-json.loads(r['value'])['ingested_at_ms'] for r in live]) == reported['input_to_mark_ms']
            # Export the final committed JSON sink; raw view has been fully compared above.
            if view == 'read_committed': export('output', [json.loads(r['value']) for r in rows])
            export('observations-'+view, [{'input_seq': json.loads(r['value'])['input_seq'],
                'offset': r['offset'], 'ingested_at_ms': json.loads(r['value'])['ingested_at_ms'],
                'marked_at_ms': json.loads(r['value'])['marked_at_ms'], 'observed_at_ms': r['observed_at_ms']} for r in live])
        if case['name'].startswith('streams'):
            assert case['commit_interval_ms'] == 100
        elif case['name'].startswith('flink'):
            assert case['checkpoint_interval_ms'] == 30000
        if case['name'] == 'flink-alo-to-streams-eos':
            assert case['upstream_ttl_configuration'] == case['ttl_configuration']
            assert case['commit_interval_ms'] == 100
            staging = business_rows(read(case['staging_topic'], 'read_uncommitted'))
            assert [(r['key'],r['value']) for r in staging] == [(r['key'],r['value']) for r in data]
        if case['name'] == 'flink-eos-30s':
            assert case['early_observation']['raw_records'] == COUNT
            assert case['early_observation']['committed_records'] == 0
            assert any(n['processed_at_notification'] == COUNT for n in case['checkpoint_notifications'])
            initial = case['initial_periodic_checkpoint']
            assert initial['processed_at_notification'] == 0
            assert case['generation']['started_at_ms'] > initial['completed_at_ms']
            assert any(n['processed_at_notification'] == COUNT and n['completed_at_ms']-initial['completed_at_ms'] >= 25000
                       for n in case['checkpoint_notifications'])
            actual_notifications = [json.loads(p.read_text()) for p in sorted(Path(case['input_file']['path']).parent.glob('checkpoint-*'))]
            assert actual_notifications == case['checkpoint_notifications']
        case['exported_jsonl'] = exported
        checks.append({'name': case['name'], 'passed': True,
                       'flags': case['views']['read_committed']['flags'],
                       'input_rate_per_sec': case['independent_input_rate_per_sec'],
                       'committed_visibility_ms': case['views']['read_committed']['visibility_ms']})
    (output/'tagging-comparison.json').write_text(json.dumps(report, indent=2)+'\n')
    reward = 1
except (AssertionError, OSError, ValueError, KeyError, TypeError, subprocess.TimeoutExpired) as error:
    reward = 0
    errors.append(type(error).__name__+': '+str(error))
(output/'checks.json').write_text(json.dumps({'report_validation_reward': reward, 'checks': checks, 'errors': errors}, indent=2)+'\n')
(output/'reward.txt').write_text(str(reward)+'\n')
print(json.dumps({'report_validation_reward': reward, 'checks': len(checks), 'errors': errors}))
