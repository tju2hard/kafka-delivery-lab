"""Export native Harbor marking measurements without introducing latency gates."""
import argparse
import hashlib
import json
from pathlib import Path
import shutil
import gzip
import itertools

ROOT = Path(__file__).resolve().parents[1]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--baseline', required=True)
    parser.add_argument('--oracle', nargs='+', required=True)
    args = parser.parse_args()
    results = ROOT / 'results'
    manifest = results / 'tagging-inputs.sha256.json'
    for name, digest in json.loads(manifest.read_text()).items():
        candidates=(ROOT/name,results/'tagging-evidence/inputs'/name)
        assert any(p.is_file() and hashlib.sha256(p.read_bytes()).hexdigest()==digest for p in candidates), name
    evidence = results / 'tagging-evidence'
    evidence.mkdir(exist_ok=True)
    metadata = []
    for trial, reward in [(args.baseline, 0), *((n, 1) for n in args.oracle)]:
        source = results / 'trials' / trial
        native = json.loads((source/'result.json').read_text())
        assert native['exception_info'] is None
        assert native['verifier_result']['rewards']['reward'] == reward
        checks = json.loads((source/'verifier/checks.json').read_text())
        assert checks['report_validation_reward'] == reward
        assert len(checks['checks']) == (6 if reward else 0)
        if reward:
            assert not checks['errors']
        metadata.append({'trial': trial, **{k: native[k] for k in
                         ('started_at', 'finished_at', 'exception_info', 'verifier_result')}})
        shutil.copyfile(source/'verifier/checks.json', evidence/(trial+'-checks.json'))
        if reward:
            shutil.copyfile(source/'verifier/tagging-comparison.json', evidence/(trial+'-comparison.json'))
            shutil.copytree(source/'verifier/data', evidence/trial/'data', dirs_exist_ok=True)
    reports = [json.loads((evidence/(name+'-comparison.json')).read_text()) for name in args.oracle]
    assert len({r['run_id'] for r in reports}) == len(reports)
    examples = results / 'examples'
    examples.mkdir(exist_ok=True)
    case_name = reports[-1]['cases'][0]['name']
    for stem in ('input', 'output'):
        source = evidence/args.oracle[-1]/'data'/case_name/(stem+'.jsonl.gz')
        with gzip.open(source, 'rt', encoding='utf-8') as stream:
            (examples/('marked-'+stem+'.jsonl')).write_text(''.join(itertools.islice(stream, 6)))
    signatures=[{c['name']:c['views']['read_committed']['flags'] for c in r['cases']} for r in reports]
    assert all(s==signatures[0] for s in signatures)
    summary = {'report_reward_meaning': 'complete and independently verified marking report, not a latency SLA',
               'input_manifest_sha256': hashlib.sha256(manifest.read_bytes()).hexdigest(),
               'native_trials': metadata, 'runs': []}
    for name, report in zip(args.oracle, reports):
        summary['runs'].append({'trial': name, **{k: v for k,v in report.items() if k != 'cases'},
            'cases': [{k: v for k,v in c.items() if k not in ('input_rows', 'views')} | {
                'views': {view: {k:v for k,v in data.items() if k not in ('rows','live_observations')}
                          for view,data in c['views'].items()}} for c in report['cases']]})
    (results/'tagging-summary.json').write_text(json.dumps(summary, ensure_ascii=False, indent=2)+'\n')
    (evidence/'trials.json').write_text(json.dumps(metadata, indent=2)+'\n')
    lines = [
        '# 单机每秒3000条 JSON：1小时 ID 标记、30秒 CK 与100ms commit', '',
        '每条输入到达都写入结果 Kafka；同一 event_id 首次标记 false，后续标记 true。'
        '业务重复消息保留，input_seq 区分每一次输入。此实验不要求按业务 ID 删除重复记录。', '',
        '按处理时间执行滚动1小时去重：每次到达刷新有效期，连续1小时未出现后，下次重新标为首次。'
        '并行发送的 input_seq 不表示到达顺序；以输入 Kafka 分区 offset 定义首次。', '',
        '## 实际链路', '',
        '1. Kafka → 普通 Java Consumer＋内存状态＋非事务 Producer → Kafka。',
        '2. Kafka → Redis 原子检查/刷新1小时 TTL＋非事务 Producer → Kafka。',
        '3. Kafka → Streams（EOS，commit.interval.ms=100）→ 结果 Kafka。',
        '4. Kafka → Flink（CK=30000ms，非事务 sink）→ 结果 Kafka。',
        '5. Kafka → Flink（CK=30000ms，非事务 sink）→ 中间 Kafka → Streams（EOS，100ms）→ 结果 Kafka。',
        '6. Kafka → Flink（CK=30000ms，事务 sink）→ 结果 Kafka。', '',
        'Flink、Streams 均使用真实 Kafka 输入和输出，去重状态按 ID 分区。'
        'Streams 使用 RocksDB last-seen state/changelog；Flink 使用 keyBy(event_id)＋ValueState，'
        'TTL=1小时、OnCreateAndWrite、NeverReturnExpired，并对每次到达更新状态。'
        'Redis 用 Lua 原子读取和 PX=3600000 写入，重复到达也刷新。'
        'Streams/内存实现按 last-seen 判断逻辑过期，过期 key 的物理清理未做容量测试。', '',
        '## 原生验证', '',
        f'Harbor 基线 `{args.baseline}` 得0；正常负载运行 '+ '、'.join('`'+n+'`' for n in args.oracle)+
        f' 共{len(reports)}次，均得1。1表示每条标记、负载和观察报告经独立消费者核对正确，'
        '不代表所有消息延迟低于100ms。本报告是正常负载对照，CK失败另用 recovery 实验验证。', '',
        '每个场景30000条合成 JSON 输入，四个线程异步发送，目标3000条/秒，持续约10秒。'
        '10000个业务 ID 各到达3次，应有10000条 false、20000条 true。'
        '时间从发送输入消息前开始，到预先分配分区的输出消费者首次观察为止；'
        '串联场景包括 Flink、中间 Kafka 和 Streams 的时间。热身消息不计入样本。'
        '各场景顺序运行，排除作业冷启动。样本量和负载仅用于展示机制。', '',
        '| 运行 | 场景 | 实际输入条/秒 | 首次/重复标记 | 原始 p50/p95/p99 ms | 已提交 p50/p95/p99 ms | 已提交 ≤100ms |',
        '|---|---|---:|---:|---:|---:|---:|',
    ]
    for name, report in zip(args.oracle, reports):
        for case in report['cases']:
            raw = case['views']['read_uncommitted']['visibility_ms']
            committed = case['views']['read_committed']['visibility_ms']
            flags = case['views']['read_committed']['flags']
            triplet = lambda d: '/'.join(str(d[k]) for k in ('p50','p95','p99'))
            rate = case['independent_input_rate_per_sec']
            lines.append(f"| {name} | {case['name']} | {rate:.1f} | {flags['first_arrivals']}/{flags['duplicate_arrivals']} | {triplet(raw)} | {triplet(committed)} | {committed['within_100ms']}/30000 |")
    lines += ['', '## 输出示例（真实实验消息）', '', '```json']
    for value in reports[-1]['cases'][0]['views']['read_committed']['examples'][:5]:
        lines.append(json.dumps({k: value[k] for k in ('event_id','input_seq','payload','is_duplicate')}, ensure_ascii=False))
    lines += ['```', '', '完整消息还包含 ingested_at_ms、marked_at_ms。消息 key 为 event_id。', '',
              '## 结论边界', '',
              '- Flink 非事务 sink 的写出不以 checkpoint 完成为条件；30秒 CK 不会自动给每条输出增加30秒等待。',
              '- Flink 事务 sink 的已提交可见性与 checkpoint 提交关联；原始日志可先看到尚未提交的数据。'
              '事务对照先完成一次空的周期 checkpoint，再开始发送，观察下一次30秒周期提交；'
              '保存两个原生完成通知，避免首次调度随机偏移。没有人为 sleep 伪造提交延迟。',
              '- 串联链路允许 Flink 的恢复周期与 Streams 的事务提交周期分别配置。'
              '100ms commit 仍需加上输入、处理、副本确认及消费者观察开销；看实测分位数，不能将配置当成SLA。',
              '- 本轮没有崩溃、rebalance或重启测试。Flink 非事务上游在恢复时可重放；'
              'Streams 可以按 ID 把重放标为重复，但这里会保留这条带 true 标记的消息。'
              '这不构成整条链路在故障下按输入到达恰好一次的证明。',
              '- 本轮没有实际等待1小时验证到期。保留了 last-seen 的边界策略检查及原生 Redis 剩余 TTL；'
              '10秒负载结果不能证明跨小时到期和清理行为。',
              '- 每个输入/输出均逐条比较完整 JSON 和 input_seq，检查缺失、多出、负载和标记；'
              '不是只比较消息总数。消费者最终落盘 JSONL，独立复读导出 gzip 文件及 SHA-256。',
              '- 三 Broker、副本3、min ISR2、acks=all，单输入/输出分区；'
              '未验证生产吞吐、跨机时钟误差、多分区分布及故障恢复延迟。', '',
              'Redis 使用单连接、逐条同步 Lua，AOF appendfsync=always，未做 pipeline。'
              '输入3k条/秒不代表所有输出端都能持续处理3k条/秒；排空等待包含积压时间。'
              '这些结果不代表 Redis、Streams 或 Flink 的性能上限。'
              '机器为共享8逻辑CPU/约7.7GiB内存，主容器4CPU/4GiB；三个Broker各限768MiB，'
              'Broker健康检查也有后台开销。', '',
              '原始输入、每条输出、观察时间与 checkpoint 证据见 [tagging-evidence/](tagging-evidence/)，'
              '摘要见 [tagging-summary.json](tagging-summary.json)。', '',
              '曾有一次 Redis 排空等待120秒超时，该轮不作为丢失或标记错误证据。'
              '调整等待后重新执行完整验证；记录见 [tagging-attempts.json](tagging-attempts.json)。', '',
              '## 代码与复现', '',
              '- [Streams 标记与输入输出工具](../src/main/java/lab/TaggingDemo.java)',
              '- [Flink 标记与非事务转发](../src/main/java/lab/FlinkTagging.java)',
              '- [实验控制器](../harbor/id-marking-latency/environment/latency.py)', '',
              '```bash', 'bash scripts/build.sh',
              'harbor trials start -p harbor/id-marking-latency -a oracle \\',
              '  --trial-name my-tagging-run --trials-dir results/trials', '```', '',
              '每次原生报告目录 `verifier/data/<case>/` 包含 input.jsonl.gz、output.jsonl.gz '
              '和两种视图的逐条观察记录。可独立重验：', '',
              '```bash',
              'python3 scripts/compare-jsonl.py --input INPUT/input.jsonl.gz \\',
              '  --output INPUT/output.jsonl.gz', '```', '',
              '无需解压即可查看的小样本：[输入 JSONL](examples/marked-input.jsonl)、'
              '[带标记输出 JSONL](examples/marked-output.jsonl)。完整 gzip 文件仍是逐行 JSON。', '',
              'Kafka Streams/Client3.4.0、Kafka Broker3.7.0、Flink1.20.4、Kafka Connector3.4.0-1.20。'
              '这些是实验固定版本。', '',
              '官方机制依据：[Kafka Streams 配置](https://kafka.apache.org/34/streams/developer-guide/config-streams/)、'
              '[Flink KafkaSink](https://nightlies.apache.org/flink/flink-docs-release-1.20/docs/connectors/datastream/kafka/)。', '']
    (results/'TAGGING.md').write_text('\n'.join(lines))
    print(json.dumps({'cases_per_run': 6, 'runs': args.oracle, 'report': 'results/TAGGING.md'}))


if __name__ == '__main__':
    main()
