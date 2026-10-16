"""Real-process fault matrix; all messages are synthetic laboratory inputs."""
import argparse
from collections import Counter
import json
import os
from pathlib import Path
import signal
import socket
import sqlite3
import subprocess
import time
import uuid

from wire_proxy import WireProxy

JAVA = ['/opt/java/openjdk/bin/java', '-Xmx1536m',
        '--add-opens=java.base/java.lang=ALL-UNNAMED',
        '--add-opens=java.base/java.util=ALL-UNNAMED',
        '--add-opens=java.base/java.io=ALL-UNNAMED',
        '--add-opens=java.base/java.nio=ALL-UNNAMED',
        '--add-opens=java.base/sun.nio.ch=ALL-UNNAMED',
        '-Dorg.slf4j.simpleLogger.defaultLogLevel=warn',
        '-cp', '/app/classes:/app/lib/*', 'lab.Lab']


def event(n):
    return f'event-{n:04d}|synthetic-payload-{n:04d}'


def wait_for(predicate, seconds, label, process=None):
    end = time.monotonic() + seconds
    while time.monotonic() < end:
        if predicate():
            return
        if process is not None and process.poll() is not None:
            raise RuntimeError(f'{label}: process exited {process.returncode}')
        time.sleep(.02)
    raise TimeoutError(label)


def redis(*args):
    with socket.create_connection((os.getenv('REDIS_HOST', 'redis'), int(os.getenv('REDIS_PORT', '6379'))), 5) as connection:
        connection.sendall(f'*{len(args)}\r\n'.encode() + b''.join(f'${len(str(x).encode())}\r\n'.encode() + str(x).encode() + b'\r\n' for x in args))
        stream = connection.makefile('rb')
        line = stream.readline()
        if line[:1] == b':':
            return int(line[1:])
        if line[:1] == b'+':
            return line[1:-2].decode()
        if line[:1] == b'$':
            size = int(line[1:-2])
            if size < 0:
                return None
            data = stream.read(size)
            if len(data) != size or stream.read(2) != b'\r\n':
                raise RuntimeError('Incomplete Redis bulk response')
            return data.decode()
        raise RuntimeError('Unexpected Redis control response')


def summarize(rows, expected):
    counts = Counter(x['id'] for x in rows)
    return {'records': len(rows), 'unique_ids': len(counts),
            'duplicate_records': sum(max(0, n - 1) for n in counts.values()),
            'missing_ids': sorted(set(expected) - set(counts)),
            'unexpected_ids': sorted(set(counts) - set(expected)),
            'counts': dict(sorted(counts.items()))}


class Matrix:
    def __init__(self, directory):
        self.root = directory
        self.processes = []
        self.streams = []
        self.serial = 0
        self.proxy = WireProxy()
        self.results = []

    def launch(self, args, folder=None):
        folder = folder or self.root
        folder.mkdir(parents=True, exist_ok=True)
        self.serial += 1
        output = (folder / f'process-{self.serial:03d}.stdout').open('w')
        error = (folder / f'process-{self.serial:03d}.stderr').open('w')
        self.streams.extend([output, error])
        p = subprocess.Popen(JAVA + list(map(str, args)), stdout=output, stderr=error, start_new_session=True)
        p.lab_output = Path(output.name)
        p.lab_error = Path(error.name)
        self.processes.append(p)
        return p

    def finish(self, p, timeout=50):
        try:
            code = p.wait(timeout)
        except subprocess.TimeoutExpired:
            self.kill(p)
            raise TimeoutError(f'Native operation timeout; log: {p.lab_error}')
        if code:
            lines = p.lab_error.read_text(errors='replace').splitlines()
            raise RuntimeError(f'Native process exit{code}; {p.lab_error}: ' + '\n'.join(lines[-14:]))
        return p.lab_output.read_text()

    def call(self, *args, timeout=50):
        return self.finish(self.launch(args), timeout)

    def kill(self, p):
        if p.poll() is None:
            os.killpg(p.pid, signal.SIGKILL)
            p.wait(10)

    def create(self, topic, compact=False):
        self.call('create', topic, 'compact' if compact else 'delete')

    def read(self, topic, isolation):
        return json.loads(self.call('read', topic, isolation))

    def observer(self, topic, folder):
        p = self.launch(['observe', topic, folder], folder)
        for view in ('read_uncommitted', 'read_committed'):
            wait_for(lambda: (folder / ('observer-ready-' + view)).exists(), 35, 'observer assignment ' + view, p)
        return p

    def observed(self, folder, view):
        path = folder / 'observations.jsonl'
        if not path.exists():
            return []
        rows = []
        for line in path.read_text().splitlines():
            try:
                row = json.loads(line)
            except ValueError:
                continue  # writer may be in the final line; reread on next condition check
            if row['isolation'] == view:
                rows.append(row)
        return rows

    def write_case(self, name, strategy, count=8, fault='none', observer=False, compact=False):
        folder = self.root / name
        folder.mkdir()
        topic = self.root.name + '-' + name
        self.create(topic, compact)
        case = {'name': name, 'strategy': strategy, 'topic': topic, 'expected_ids': [event(i).split('|')[0] for i in range(count)], 'fault': fault, 'fault_witnesses': []}
        observing = self.observer(topic, folder) if observer else None
        return folder, topic, case, observing

    def collect(self, case, folder, observing=None):
        if observing:
            wait_for(lambda: set(x['id'] for x in self.observed(folder, 'read_committed')) >= set(case['expected_ids']), 30, 'committed observer drain', observing)
            (folder / 'observer-stop').touch()
            self.finish(observing, 10)
            observations = []
            for view in ('read_uncommitted', 'read_committed'):
                rows = self.observed(folder, view)
                observations.extend(rows)
                latencies = sorted(x['observed_at_ms'] - x['producer_timestamp_ms'] for x in rows)
                case[view + '_visibility_ms'] = {'samples': len(latencies), 'p50': latencies[len(latencies)//2] if latencies else None, 'p95': latencies[min(len(latencies)-1, int(len(latencies)*.95))] if latencies else None}
            case['live_observations'] = observations
        for view in ('read_uncommitted', 'read_committed'):
            rows = self.read(case['topic'], view)
            case[view] = summarize(rows, case['expected_ids'])
            case[view + '_rows'] = rows
            if any(x['value'] != event(int(x['id'].split('-')[1])) for x in rows):
                raise AssertionError('Payload corruption')
        raw, committed = case['read_uncommitted'], case['read_committed']
        case['physical_contract_pass'] = not (raw['duplicate_records'] or raw['missing_ids'] or raw['unexpected_ids'])
        case['committed_view_contract_pass'] = not (committed['duplicate_records'] or committed['missing_ids'] or committed['unexpected_ids'])
        self.results.append(case)
        print(json.dumps({'case': case['name'], 'raw_records': raw['records'], 'raw_duplicates': raw['duplicate_records'], 'raw_missing': len(raw['missing_ids']), 'committed_records': committed['records'], 'physical_contract_pass': case['physical_contract_pass']}), flush=True)

    def forward(self, strategy, topic, count, folder, fault='none', server=None):
        args = ['forward', strategy, topic, count, folder, fault]
        if server:
            args.append(server)
        return self.launch(args, folder)

    def crash_case(self, name, strategy, fault, expected, count=8):
        d, topic, case, _ = self.write_case(name, strategy, count, fault)
        if strategy == 'outbox':
            subprocess.run(['python3', '/app/state.py', 'init', str(d/'outbox.db'), str(count)], check=True)
        p = self.forward(strategy, topic, count, d, fault)
        witness = d / ('witness-' + fault)
        wait_for(witness.exists, 45, name + ' fault point', p)
        case['fault_witnesses'].append({'stage': fault, 'writer_pid': p.pid, 'observed_at_ms': int(time.time()*1000), 'worker_witness': witness.read_text()})
        self.kill(p)
        case['fault_witnesses'][-1]['terminated_exit'] = p.returncode
        self.finish(self.forward(strategy, topic, count, d))
        self.collect(case, d)
        self.assert_shape(case, *expected)

    @staticmethod
    def assert_shape(case, duplicates, missing, committed_duplicates=None):
        r = case['read_uncommitted']
        assert r['duplicate_records'] == duplicates and len(r['missing_ids']) == missing, case['name'] + ': unexpected native raw outcome'
        if committed_duplicates is not None:
            assert case['read_committed']['duplicate_records'] == committed_duplicates, case['name'] + ': unexpected committed outcome'

    def basic(self):
        for mode in ('plain', 'idempotent'):
            d,t,c,o = self.write_case(mode+'-normal', mode, observer=True)
            self.finish(self.forward(mode,t,8,d))
            self.collect(c,d,o)
            self.assert_shape(c,0,0,0)
        for mode in ('plain', 'idempotent'):
            self.proxy.configure('drop_ack')
            d,t,c,_ = self.write_case(mode+'-ack-drop', mode, fault='successful ACK dropped')
            self.finish(self.forward(mode,t,8,d,server='main:19091'))
            assert self.proxy.triggered and self.proxy.witnesses and not self.proxy.errors, self.proxy.errors
            c['fault_witnesses'] = list(self.proxy.witnesses)
            self.collect(c,d)
            self.assert_shape(c, 1 if mode=='plain' else 0,0)
            self.proxy.configure('pass')
        for mode in ('application-replay','compacted-key'):
            d,t,c,_ = self.write_case(mode,mode,compact=mode=='compacted-key')
            self.finish(self.forward(mode,t,8,d))
            self.collect(c,d)
            self.assert_shape(c,1,0)
        for name,mode,fault,shape in [
            ('idempotent-restart','idempotent','after-append',(1,0)),
            ('redis-before-restart','redis-before','reserved',(0,1)),
            ('redis-after-restart','redis-after','after-append',(1,0)),
            ('outbox-restart','outbox','after-append',(1,0)),
            ('reconcile-acked-restart','reconcile','after-append',(0,0)),
            ('transaction-external-restart','transaction-external','after-commit',(1,0)),
        ]:
            self.crash_case(name,mode,fault,shape)

    def lease(self):
        d,t,c,_ = self.write_case('redis-expired-owner','redis-lease',1,'lease expires after final ownership check')
        a = self.forward('redis-lease',t,1,d/'owner-a','lease-checked')
        wait_for(lambda: (d/'owner-a/witness-lease-checked').exists(),45,'lease ownership check',a)
        wait_for(lambda: redis('PTTL',t+':event-0000') == -2,5,'native Redis lease expiry')
        self.finish(self.forward('redis-lease',t,1,d/'owner-b'))
        (d/'owner-a/release-lease-checked').touch()
        self.finish(a)
        c['fault_witnesses']=[{'stage':'final lease check then actual Redis expiry then replacement publish then old-owner publish','expired_key_pttl':-2}]
        self.collect(c,d)
        self.assert_shape(c,1,0)

    def delayed_request(self):
        self.proxy.configure('hold_request')
        d,t,c,_ = self.write_case('reconcile-late-old-request','reconcile',1,'old received request delivered after restart and scan')
        old = self.forward('idempotent',t,1,d/'old',server='main:19091')
        wait_for(self.proxy.held.is_set,45,'buffered old native ProduceRequest',old)
        self.kill(old)
        self.finish(self.forward('reconcile',t,1,d/'replacement'))
        self.proxy.release.set()
        wait_for(self.proxy.ack.is_set,20,'late old request successful broker ACK')
        assert not self.proxy.errors, self.proxy.errors
        c['fault_witnesses']=list(self.proxy.witnesses)+[{'old_writer_terminated_exit':old.returncode}]
        self.collect(c,d)
        self.assert_shape(c,1,0)
        self.proxy.configure('pass')

    def transactions(self):
        for fault in ('before-commit','after-commit'):
            d,t,c,_ = self.write_case('atomic-offsets-'+fault,'kafka-transaction-offsets',8,fault)
            source=t+'-input'
            self.create(source)
            self.call('seed',source,8)
            p=self.launch(['transaction',source,t,8,d,fault],d)
            wait_for(lambda: (d/('witness-'+fault)).exists(),50,'transaction boundary',p)
            before_raw=self.read(t,'read_uncommitted')
            before_committed=self.read(t,'read_committed')
            self.kill(p)
            self.finish(self.launch(['transaction',source,t,8,d,'none'],d))
            c['source_topic']=source
            c['source_committed_offset_after_recovery']=int(self.call('group-offset',t,source).strip())
            assert c['source_committed_offset_after_recovery']==8
            c['fault_witnesses']=[{'stage':fault,'pre_kill_raw':len(before_raw),'pre_kill_committed':len(before_committed),'terminated_exit':p.returncode}]
            self.collect(c,d)
            self.assert_shape(c,8 if fault=='before-commit' else 0,0,0)
        d,t,c,o=self.write_case('transaction-visibility','kafka-transaction-offsets',8,'explicit commit barrier',observer=True)
        source=t+'-input';self.create(source);self.call('seed',source,8)
        p=self.launch(['transaction',source,t,8,d,'before-commit'],d)
        wait_for(lambda: (d/'witness-before-commit').exists(),50,'precommit barrier',p)
        wait_for(lambda:len(self.observed(d,'read_uncommitted'))==8,20,'raw transaction observation',p)
        before=self.read(t,'read_committed');assert not before
        held_at=time.monotonic_ns()
        time.sleep(.25)  # deliberately measured commit delay; not a readiness condition
        (d/'release-before-commit').touch();self.finish(p)
        c['fault_witnesses']=[{'stage':'raw records visible while committed view empty','committed_before_release':len(before),'minimum_explicit_hold_ms':250,'hold_started_at_ns':held_at}]
        self.collect(c,d,o);self.assert_shape(c,0,0,0)

    def flink(self, mode, restore):
        name='flink-'+mode+('-restore' if restore else '-normal')
        d,t,c,o=self.write_case(name,'flink-keyed-state-'+mode,8,'kill after Kafka append before completed checkpoint' if restore else 'none',observer=True)
        if restore:
            (d/'block-checkpoint').touch()
        first=self.launch(['flink',t,mode,'first' if restore else 'normal',d,'none',8],d)
        wait_for(lambda:any(d.glob('*-checkpoint-*-index-0')),100,'initial native completed checkpoint',first)
        initial=sorted(d.glob('*-checkpoint-*-index-0'))[-1];checkpoint=int(initial.read_text());metadata=list((d/'checkpoints').rglob(f'chk-{checkpoint}/_metadata'));assert len(metadata)==1
        (d/'go').touch()
        if restore:
            wait_for(lambda:len(self.observed(d,'read_uncommitted'))>=1,35,'native Kafka output before checkpoint',first)
            wait_for(lambda:(d/'checkpoint-blocked').exists(),15,'actual Flink snapshot boundary',first)
            c['fault_witnesses']=[{'stage':'native append then blocked next checkpoint','restored_metadata':str(metadata[0]),'checkpoint_id':checkpoint,'raw_records_before_kill':len(self.observed(d,'read_uncommitted'))}]
            self.kill(first);c['fault_witnesses'][0]['terminated_exit']=first.returncode
            (d/'block-checkpoint').unlink()
            worker=self.launch(['flink',t,mode,'recovery',d,metadata[0],8],d)
            wait_for(lambda:(d/'recovery-restored').exists(),100,'native Flink restore initialization',worker)
            assert (d/'recovery-restored').read_text()=='true', 'Job did not restore the actual checkpoint'
            c['fault_witnesses'][0]['native_is_restored']=True
        else:
            worker=first
        wait_for(lambda:any(d.glob(('*' if not restore else 'recovery')+'-checkpoint-*-index-8')),120,'completed checkpoint including all source events',worker)
        wait_for(lambda:len(self.observed(d,'read_committed'))>=8,40,'native sink committed delivery',worker)
        self.kill(worker)
        self.collect(c,d,o)
        self.assert_shape(c,1 if restore else 0,0,0 if mode=='exactly-once' else (1 if restore else 0))

    def run(self):
        self.root.mkdir(parents=True)
        end=time.monotonic()+120
        while True:
            try:
                if int(self.call('ready',timeout=20).strip())==3:
                    break
            except (RuntimeError,TimeoutError):
                pass
            if time.monotonic()>end:
                raise TimeoutError('Three native Kafka brokers were not ready')
            time.sleep(.5)
        assert redis('PING')=='PONG'
        self.proxy.start()
        self.basic();self.lease();self.delayed_request();self.transactions()
        for restore in (False,True):
            for mode in ('at-least-once','exactly-once'):
                self.flink(mode,restore)
        return {'schema_version':1,'experiment_valid':True,'run_id':self.root.name,
                'contract':'one physical Kafka payload record per accepted business event, no loss',
                'versions':{'kafka_broker':'3.7.0','kafka_java_client':'3.4.0','redis':'7.2.6','flink':'1.20.4','flink_kafka_connector':'3.4.0-1.20','python':'3.10.12'},
                'topology':{'brokers':3,'replication_factor':3,'min_insync_replicas':2,'acks':'all','output_partitions_per_case':1},
                'limitations':['bounded deterministic fault matrix, not an impossibility proof or a general correctness proof','synthetic ordered event batches; no production throughput claim','broker outages, disk loss, partition expansion and retention expiry not tested'],
                'cases':self.results}

    def close(self):
        self.proxy.close()
        for p in self.processes:
            self.kill(p)
        for stream in self.streams:
            stream.close()


def main():
    p=argparse.ArgumentParser();p.add_argument('--output',default='/app/report.json');args=p.parse_args()
    matrix=Matrix(Path('/app/runs')/('run-'+uuid.uuid4().hex[:12]))
    try:
        report=matrix.run()
        Path(args.output).write_text(json.dumps(report,indent=2)+'\n')
    finally:
        matrix.close()


if __name__=='__main__':
    main()
