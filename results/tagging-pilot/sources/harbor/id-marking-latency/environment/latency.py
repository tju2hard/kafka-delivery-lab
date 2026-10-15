"""Real 3000-arrival/s JSON marking experiment; no fabricated timing."""
import argparse
import hashlib
import json
from pathlib import Path
import subprocess
import time
import uuid

from run_lab import Matrix, JAVA, wait_for, redis

TAG_JAVA = JAVA[:-1] + ['lab.TaggingDemo']
CASES = ('local-marker', 'redis-marker', 'streams-eos-100ms', 'flink-alo-180s',
         'flink-alo-to-streams-eos', 'flink-eos-180s')
COUNT = 30000
RATE = 3000


def business_rows(rows):
    return [r for r in rows if not json.loads(r['value']).get('control', False)]


def validate(input_rows, output_rows):
    expected, seen = {}, set()
    for row in business_rows(input_rows):
        value = json.loads(row['value'])
        seq, event_id = value['input_seq'], value['event_id']
        assert type(seq) is int and seq not in expected
        assert row['key'] == event_id
        assert set(value) == {'event_id', 'input_seq', 'payload', 'ingested_at_ms'}
        assert value['payload'] == {'source': 'synthetic', 'value': seq, 'worker': seq % 4}
        expected[seq] = {**value, 'is_duplicate': event_id in seen}
        seen.add(event_id)
    assert len(expected) == COUNT and set(expected) == set(range(COUNT))
    actual = {}
    for row in output_rows:
        value = json.loads(row['value'])
        seq = value['input_seq']
        assert seq in expected and seq not in actual, 'One output per input arrival required'
        assert row['key'] == value['event_id']
        assert type(value['is_duplicate']) is bool
        assert set(value) == set(expected[seq]) | {'marked_at_ms'}
        assert {k: value[k] for k in expected[seq]} == expected[seq], f'Bad data or flag for input_seq={seq}'
        assert value['marked_at_ms'] >= value['ingested_at_ms']
        actual[seq] = value
    assert set(actual) == set(expected), 'Missing or unexpected arrival'
    return {'arrivals': len(actual), 'unique_business_ids': len(seen),
            'first_arrivals': sum(not v['is_duplicate'] for v in actual.values()),
            'duplicate_arrivals': sum(v['is_duplicate'] for v in actual.values()),
            'missing_arrivals': 0, 'extra_arrivals': 0, 'payload_mismatches': 0, 'flag_mismatches': 0}


def distribution(values):
    ordered = sorted(values)
    assert ordered and ordered[0] >= 0
    return {'samples': len(ordered), 'min': ordered[0],
            'p50': ordered[int((len(ordered)-1)*.50)],
            'p95': ordered[int((len(ordered)-1)*.95)],
            'p99': ordered[int((len(ordered)-1)*.99)], 'max': ordered[-1],
            'within_100ms': sum(x <= 100 for x in ordered)}


def jsonl(path):
    with Path(path).open() as stream:
        return [json.loads(line) for line in stream]


def file_info(path):
    path = Path(path)
    return {'path': str(path), 'bytes': path.stat().st_size,
            'sha256': hashlib.sha256(path.read_bytes()).hexdigest()}


class Experiment(Matrix):
    def tag(self, args, directory):
        self.serial += 1
        out = (directory / f'tag-{self.serial}.stdout').open('w')
        err = (directory / f'tag-{self.serial}.stderr').open('w')
        self.streams.extend([out, err])
        p = subprocess.Popen(TAG_JAVA + list(map(str, args)), stdout=out, stderr=err, start_new_session=True)
        p.lab_output, p.lab_error = Path(out.name), Path(err.name)
        self.processes.append(p)
        return p

    def tag_call(self, args, directory, timeout=60):
        return self.finish(self.tag(args, directory), timeout)

    @staticmethod
    def progress(directory, view):
        path = directory/('progress-'+view)
        return int(path.read_text()) if path.exists() else 0

    def scenario(self, name):
        directory = self.root / name
        directory.mkdir()
        source, output = self.root.name+'-'+name+'-in', self.root.name+'-'+name+'-out'
        self.create(source); self.create(output)
        bridge = None
        if name == 'flink-alo-to-streams-eos':
            bridge = output + '-staging'; self.create(bridge)
        observer = self.tag(['observe', output, directory], directory)
        for view in ('read_uncommitted', 'read_committed'):
            wait_for(lambda: (directory/('observer-ready-'+view)).exists(), 35, 'observer ready', observer)
        self.tag_call(['seed', source, 0, 0], directory)
        started_at = int(time.time()*1000)
        upstream = None
        if bridge:
            upstream_dir = directory/'flink-upstream'; upstream_dir.mkdir()
            self.tag_call(['seed', bridge, 0, 0], directory)
            upstream = self.tag(['flink', source, bridge, 'bridge', upstream_dir], upstream_dir)
            wait_for(lambda: (upstream_dir/'worker-ready').exists(), 120, 'Flink warmup', upstream)
        if name in ('local-marker', 'redis-marker'):
            args = ['direct', source, output, 'redis' if name == 'redis-marker' else 'local', directory]
        elif name.startswith('streams') or bridge:
            args = ['streams', bridge or source, output, directory]
        else:
            args = ['flink', source, output, 'eos' if '-eos-' in name else 'alo', directory]
        worker = self.tag(args, directory)
        wait_for(lambda: (directory/'worker-ready').exists() or (directory/'worker-error').exists(), 120, 'worker warmup', worker)
        assert not (directory/'worker-error').exists(), (directory/'worker-error').read_text() if (directory/'worker-error').exists() else ''
        ready_at = int(time.time()*1000)
        generation = json.loads(self.tag_call(['seed', source, COUNT, RATE], directory, timeout=90))
        assert generation['records'] == COUNT and generation['target_rate_per_sec'] == RATE
        wait_for(lambda: self.progress(directory, 'read_uncommitted') >= COUNT, 600, 'raw drain', worker)
        early_at = int(time.time()*1000)
        early_raw = self.progress(directory, 'read_uncommitted')
        early_committed = self.progress(directory, 'read_committed')
        if name == 'flink-eos-180s':
            assert early_committed == 0, 'Unexpected commit before first checkpoint'
        wait_for(lambda: self.progress(directory, 'read_committed') >= COUNT, 600, 'committed drain', worker)
        if name == 'flink-eos-180s':
            wait_for(lambda: bool(list(directory.glob('checkpoint-*'))), 30, 'native checkpoint completion', worker)
        self.kill(worker)
        if upstream:
            self.kill(upstream)
        (directory/'observer-stop').touch(); self.finish(observer, 15)
        input_rows = json.loads(self.tag_call(['read', source, 'read_uncommitted'], directory))
        inputs_path = directory/'input.jsonl'
        with inputs_path.open('w') as stream:
            for r in business_rows(input_rows): stream.write(r['value']+'\n')
        case = {'name': name, 'source_topic': source, 'output_topic': output, 'staging_topic': bridge,
                'checkpoint_interval_ms': 180000 if name.startswith('flink') else None,
                'commit_interval_ms': 100 if name.startswith('streams') or bridge else None,
                'worker_started_at_ms': started_at, 'warmup_ready_at_ms': ready_at,
                'generation': generation,
                'ttl_configuration': json.loads((directory/'ttl-config.json').read_text()),
                'early_observation': {'at_ms': early_at, 'raw_records': early_raw, 'committed_records': early_committed},
                'checkpoint_notifications': [json.loads(p.read_text()) for p in sorted(directory.glob('checkpoint-*'))],
                'input_file': file_info(inputs_path), 'views': {}}
        if bridge:
            case['upstream_ttl_configuration'] = json.loads((directory/'flink-upstream/ttl-config.json').read_text())
        if name == 'redis-marker':
            case['native_redis_remaining_ttl_ms'] = redis('PTTL', output+':event-00000')
        for view in ('read_uncommitted', 'read_committed'):
            rows = json.loads(self.tag_call(['read', output, view], directory))
            flags = validate(input_rows, rows)
            observed_path = directory/('observations-'+view+'.jsonl')
            observed = jsonl(observed_path)
            sink_path = directory/('sink-'+view+'.jsonl')
            assert jsonl(sink_path) == [json.loads(r['value']) for r in rows]
            assert len(observed) == COUNT
            assert [(r['offset'],r['value']) for r in observed] == [(r['offset'],r['value']) for r in rows]
            latencies = [r['observed_at_ms']-json.loads(r['value'])['ingested_at_ms'] for r in observed]
            processing = [json.loads(r['value'])['marked_at_ms']-json.loads(r['value'])['ingested_at_ms'] for r in rows]
            case['views'][view] = {'flags': flags, 'visibility_ms': distribution(latencies),
                'input_to_mark_ms': distribution(processing), 'sink_file': file_info(sink_path),
                'observation_file': file_info(observed_path),
                'examples': [json.loads(r['value']) for r in rows[:6]]}
        print(json.dumps({'case': name, 'achieved_send_rate': generation['achieved_send_rate_per_sec'],
                          'flags': flags, 'committed_visibility_ms': case['views']['read_committed']['visibility_ms']}), flush=True)
        return case

    def run(self):
        self.root.mkdir(parents=True)
        deadline = time.monotonic()+120
        while True:
            try:
                if int(self.call('ready', timeout=20).strip()) == 3: break
            except (RuntimeError, TimeoutError): pass
            if time.monotonic() > deadline: raise TimeoutError('Kafka readiness')
            time.sleep(.5)
        assert redis('PING') == 'PONG'
        cases = [self.scenario(name) for name in CASES]
        return {'experiment_valid': True, 'run_id': self.root.name,
            'contract': 'one annotated JSON output per input arrival; duplicate business IDs are retained',
            'versions': {'kafka_broker': '3.7.0', 'kafka_streams': '3.4.0', 'kafka_client': '3.4.0',
                         'flink': '1.20.4', 'flink_kafka_connector': '3.4.0-1.20', 'python': '3.10.12'},
            'topology': {'brokers': 3, 'replication_factor': 3, 'min_insync_replicas': 2,
                         'acks': 'all', 'input_partitions': 1, 'output_partitions': 1},
            'measurement': {'arrivals_per_case': COUNT, 'target_rate_per_sec': RATE, 'producer_threads': 4,
                'start': 'timestamp immediately before JSON input send',
                'end': 'first output observation by preassigned native consumer',
                'workers_run_sequentially': True, 'cold_start_excluded': True,
                'first_arrival_order': 'input Kafka partition offset, not concurrent input_seq order',
                'state_ttl_ms': 3600000, 'ttl_rule': 'rolling processing-time expiry, refreshed on every arrival', 'fault_injection': False},
            'limitations': ['single-machine, one partition; descriptive load experiment, not a production SLA',
                'no failure/rebalance/restart test in this marking experiment',
                '100ms commit interval does not guarantee 100ms end-to-end latency',
                'the 1h expiry boundary was not observed by waiting an actual hour',
                'Streams/local markers enforce expiry on access; expired key cleanup is not benchmarked',
                'checkpoint period is independent of nontransactional output visibility'], 'cases': cases}


if __name__ == '__main__':
    parser = argparse.ArgumentParser(); parser.add_argument('--output', required=True)
    args = parser.parse_args()
    experiment = Experiment(Path('/app/tagging-runs')/('tag-'+uuid.uuid4().hex[:12]))
    try:
        Path(args.output).write_text(json.dumps(experiment.run(), indent=2)+'\n')
    finally:
        experiment.close()
