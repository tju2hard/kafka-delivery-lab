"""Audit the experimental report with independent fresh Kafka consumers.

Harbor reward certifies a complete truthful experiment, NOT a universally
exactly-once implementation. Strategy-specific contract flags are separate.
"""
from collections import Counter
import json
from pathlib import Path
import subprocess
import sys

sys.path.insert(0, '/app')
from run_lab import JAVA, summarize

output = Path('/logs/verifier')
checks = []
errors = []
expected_names = {
    'plain-normal', 'idempotent-normal', 'plain-ack-drop', 'idempotent-ack-drop',
    'application-replay', 'compacted-key', 'idempotent-restart',
    'redis-before-restart', 'redis-after-restart', 'outbox-restart',
    'reconcile-acked-restart', 'transaction-external-restart',
    'redis-expired-owner', 'reconcile-late-old-request',
    'atomic-offsets-before-commit', 'atomic-offsets-after-commit',
    'transaction-visibility', 'flink-at-least-once-normal',
    'flink-exactly-once-normal', 'flink-at-least-once-restore',
    'flink-exactly-once-restore',
}
try:
    report = json.loads(Path('/app/report.json').read_text())
    assert report['experiment_valid'] is True
    assert {x['name'] for x in report['cases']} == expected_names
    assert len(report['cases']) == len(expected_names)
    assert report['topology']['replication_factor'] == 3
    assert report['topology']['min_insync_replicas'] == 2
    assert report['topology']['acks'] == 'all'
    for case in report['cases']:
        expected = case['expected_ids']
        count = 1 if case['name'] in {'redis-expired-owner','reconcile-late-old-request'} else 8
        assert expected == [f'event-{i:04d}' for i in range(count)]
        for isolation in ('read_uncommitted', 'read_committed'):
            process = subprocess.run(JAVA + ['read', case['topic'], isolation],
                                     capture_output=True, text=True, timeout=45)
            assert process.returncode == 0, 'Independent consumer failed'
            rows = json.loads(process.stdout)
            actual = summarize(rows, expected)
            assert actual == case[isolation], 'Reported counters differ from native Kafka'
            assert [(r['id'], r['offset'], r['value']) for r in rows] == [(r['id'], r['offset'], r['value']) for r in case[isolation + '_rows']]
            good = not (actual['duplicate_records'] or actual['missing_ids'] or actual['unexpected_ids'])
            flag = 'physical_contract_pass' if isolation == 'read_uncommitted' else 'committed_view_contract_pass'
            assert case[flag] is good, 'Incorrect contract classification'
        if case['fault'] != 'none':
            assert case['fault_witnesses'], 'Missing fault witness'
        if case['name'].startswith('atomic-offsets-'):
            process = subprocess.run(JAVA+['group-offset',case['topic'],case['source_topic']],capture_output=True,text=True,timeout=30)
            assert process.returncode == 0 and int(process.stdout.strip()) == case['source_committed_offset_after_recovery'] == 8
        checks.append({'name': case['name'], 'passed': True,
                       'physical_contract_pass': case['physical_contract_pass'],
                       'committed_view_contract_pass': case['committed_view_contract_pass']})
    by_name = {c['name']: c for c in report['cases']}
    assert by_name['idempotent-ack-drop']['physical_contract_pass'] is True
    assert by_name['idempotent-restart']['physical_contract_pass'] is False
    assert by_name['redis-before-restart']['read_uncommitted']['missing_ids']
    assert by_name['redis-after-restart']['read_uncommitted']['duplicate_records'] == 1
    for name in ('atomic-offsets-before-commit', 'flink-exactly-once-restore'):
        assert by_name[name]['physical_contract_pass'] is False
        assert by_name[name]['committed_view_contract_pass'] is True
    assert by_name['flink-at-least-once-restore']['committed_view_contract_pass'] is False
    assert by_name['transaction-visibility']['fault_witnesses'][0]['committed_before_release'] == 0
    (output/'comparison.json').write_text(json.dumps(report, indent=2)+'\n')
    reward = 1.0
except (AssertionError, OSError, ValueError, KeyError, TypeError, subprocess.TimeoutExpired) as error:
    errors.append(type(error).__name__ + ': ' + str(error))
    reward = 0.0
(output/'checks.json').write_text(json.dumps({'report_validation_reward': reward,
    'meaning': 'truthful complete engineering experiment; not strategy exactly-once certification',
    'checks': checks, 'errors': errors}, indent=2)+'\n')
(output/'reward.txt').write_text(str(reward)+'\n')
print(json.dumps({'report_validation_reward': reward, 'cases_independently_checked': len(checks), 'errors': errors}))
