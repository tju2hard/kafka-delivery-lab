"""Publish native state-store diagnostic evidence separately from the 9000-row report."""
import argparse,gzip,hashlib,json,shutil,sys
from pathlib import Path
ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT/'harbor/arrival-marking-comparison/environment'))
import arrival_comparison as comparison

def rows(path):
    with gzip.open(path,'rt') as f:return [json.loads(line) for line in f]
def stat(values):
    values=sorted(values);return {'samples':len(values),'min':values[0],'p50':values[(len(values)-1)//2],
        'p99':values[int((len(values)-1)*.99)],'max':values[-1],'negative_samples':sum(v<0 for v in values)}
def main():
    parser=argparse.ArgumentParser();parser.add_argument('--run',type=Path,required=True);args=parser.parse_args()
    report=json.loads((args.run/'report.json').read_text());dest=ROOT/'results/state-probe-evidence';dest.mkdir(exist_ok=True)
    assert len(report['cases'])==5
    checks=[]
    for case in report['cases']:
        folder=args.run/case['name'];count=9000 if case['name'].endswith('recovery') else 90000
        comparison.COUNT=count
        inputs=rows(folder/'input.jsonl.gz');wrapped=[{'key':v['event_id'],'value':json.dumps(v)} for v in inputs]
        assert len(inputs)==count
        config=case['configuration'];assert config['processing_guarantee']=='exactly_once_v2' and config['commit_interval_ms']==100
        assert config['store_backend']==('memory' if 'memory' in case['name'] else 'rocksdb')
        assert case['source_committed_offset']==count+1
        for pid in case['worker_pids']:assert any(s['pid']==pid and s['state']=='RUNNING' for s in case['lifecycle'])
        for view in ('read_uncommitted','read_committed'):
            output=rows(folder/('sink-'+view+'.jsonl.gz'));observed=rows(folder/('observations-'+view+'.jsonl.gz'))
            assert output==[json.loads(r['value']) for r in observed]
            assert comparison.audit_arrivals(wrapped,observed)==case['views'][view]['audit']
        assert case['views']['read_committed']['audit']['arrival_contract_pass'] is True
        for info in case['exports']:
            source=args.run/info['file'];target=dest/info['file'];target.parent.mkdir(parents=True,exist_ok=True)
            assert hashlib.sha256(source.read_bytes()).hexdigest()==info['sha256'] and len(rows(source))==info['rows']
            shutil.copyfile(source,target)
        raw={r['offset']:r for r in rows(folder/'observations-read_uncommitted.jsonl.gz')}
        committed=rows(folder/'observations-read_committed.jsonl.gz')
        if case['failure']:
            w=case['failure']['witness'];assert w['offset'] in raw and all(r['offset']!=w['offset'] for r in committed)
            assert case['failure']['exit_code']==-9 and len(set(case['worker_pids']))==2
        for window in case['fixed_windows']:
            begin,end=window['source_send_window_ms'];start=case['generation']['started_at_ms']
            metrics={k:[] for k in window['metrics']}
            for r in committed:
                v=json.loads(r['value'])
                if not begin<=v['ingested_at_ms']-start<end:continue
                prior=raw[r['offset']];assert prior['value']==r['value']
                metrics['send_to_mark_ms'].append(v['marked_at_ms']-v['ingested_at_ms'])
                metrics['mark_to_raw_observed_ms'].append(prior['observed_at_ms']-v['marked_at_ms'])
                metrics['raw_to_committed_observed_ms'].append(r['observed_at_ms']-prior['observed_at_ms'])
                metrics['end_to_end_ms'].append(r['observed_at_ms']-v['ingested_at_ms'])
            assert {k:stat(v) for k,v in metrics.items()}==window['metrics']
        checks.append({'case':case['name'],'native_counts_markers_and_fixed_window_metrics_verified':True})
    report['checks']=checks
    report['base_runtime_manifest_sha256']=hashlib.sha256((ROOT/'results/arrival-runtime.sha256.json').read_bytes()).hexdigest()
    runtime=ROOT/'harbor/arrival-marking-comparison/environment/runtime/classes/lab'
    report['probe_class_sha256']={p.name:hashlib.sha256(p.read_bytes()).hexdigest() for p in sorted(runtime.glob('StreamsStateProbe*.class'))}
    report['source_sha256']={name:hashlib.sha256((ROOT/name).read_bytes()).hexdigest() for name in (
        'src/main/java/lab/StreamsStateProbe.java','scripts/profile-streams-state.py','scripts/run-streams-state-probe.sh',
        'experiments/streams-state-probe/compose.yaml')}
    (ROOT/'results/state-probe-summary.json').write_text(json.dumps(report,indent=2)+'\n')
    lines=['# 状态后端与持续输入定位（附录）','',
        '主报告的9000条短批次数据保持原样，本页为额外定位：正常场景改为每秒3000条、持续30秒，共90000条。'
        '顺序为 RocksDB、内存、内存、RocksDB，各使用新应用和新 topic；EOS、100ms提交、changelog、'
        '1小时逻辑重复判断规则均保留。使用 JFR 采样，有观测开销，不能直接当作无采样生产性能。','',
        '固定按输入发送时间取0～3秒、10～20秒、20～30秒三个窗口，窗口在实验前确定。所有90000条仍做完整总量和标记核对。','',
        '| 顺序/后端 | 输入/已提交输出 | 首次/重复标记 | 最后10秒样本数 | 最后10秒端到端 p50 / p99（ms） |',
        '|---|---:|---:|---:|---:|']
    for case in report['cases']:
        if case['name'].endswith('recovery'):continue
        audit=case['views']['read_committed']['audit'];w=next(w for w in case['fixed_windows'] if w['source_send_window_ms']==[20000,30000]);d=w['metrics']['end_to_end_ms']
        lines.append(f"| {case['name']} | 90000 / {audit['output_arrivals']} | {audit['false_markers']} / {audit['true_markers']} | {d['samples']} | {d['p50']} / {d['p99']} |")
    lines += ['',
        '后跑的 RocksDB 同样能在固定后期窗口达到约200ms的 p99，因此现有结果不支持“6秒主要就是 RocksDB 导致、换内存就解决”的结论。'
        '各方案启动初期都有明显积压，持续运行后可见延迟下降；尚未定位到具体阻塞方法，不能把全部原因归给某个组件。'
        '局部窗口达到亚秒不代表整场 p99、每次启动或生产环境都有同样保证。','',
        '另做内存状态的9000条 ACK 后、事务提交前崩溃恢复：最终9000条，3000条首次、6000条重复，逐条核对通过。'
        '这验证了该受控故障下的恢复，没有测试所有故障、跨小时清理或大规模内存容量。','',
        'Flink 的旧 FsStateBackend 是内存状态与文件系统 CK 存储的组合；Kafka Streams 的内存状态仍可通过 Kafka changelog 恢复。'
        '两者的“本地状态存储”和“持久化/恢复途径”需要分别看。'
        '依据：[Flink 后端说明](https://nightlies.apache.org/flink/flink-docs-release-1.20/docs/ops/state/state_backends/#fsstatebackend)、'
        '[Streams 状态存储说明](https://kafka.apache.org/34/streams/developer-guide/processor-api/#state-stores)。','',
        '- [完整数据与观测文件](state-probe-evidence/) / [机器可读结果、逐场验证和源码哈希](state-probe-summary.json)',
        '- [主报告：9000条的总量、标记和平均/p99时间](ARRIVAL_COMPARISON.md)','',
        '复现（只管理本实验启动的服务，原始 JFR 和日志保存在 results/local/）：','',
        '```bash','bash scripts/build.sh',
        'docker build -q -t kafka-delivery-lab/streams-state-probe:local harbor/arrival-marking-comparison/environment',
        'bash scripts/run-streams-state-probe.sh','```','']
    (ROOT/'results/STATE_STORE_PROBE.md').write_text('\n'.join(lines))
    print(json.dumps({'native_cases_verified':len(checks),'data_files':sum(len(c['exports']) for c in report['cases']),
        'report':'results/STATE_STORE_PROBE.md'}))
if __name__=='__main__':main()
