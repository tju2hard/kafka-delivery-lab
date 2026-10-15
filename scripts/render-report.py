"""Produce a compact comparison from independently verified native Harbor reports."""
import argparse
import hashlib
import json
from pathlib import Path
import shutil

ROOT = Path(__file__).resolve().parents[1]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--baseline', required=True)
    parser.add_argument('--oracle', nargs=2, required=True)
    args = parser.parse_args()
    results = ROOT / 'results'
    manifest = results / 'validation-inputs.sha256.json'
    for filename, digest in json.loads(manifest.read_text()).items():
        current = ROOT / filename
        archived = results / 'evidence/delivery-matrix-inputs' / filename
        assert any(p.is_file() and hashlib.sha256(p.read_bytes()).hexdigest() == digest
                   for p in (current, archived)), filename
    baseline = json.loads((results / 'trials' / args.baseline / 'verifier/checks.json').read_text())
    assert baseline['report_validation_reward'] == 0 and not baseline['checks']
    evidence = results / 'evidence'
    evidence.mkdir(exist_ok=True)
    trial_metadata = []
    for name, expected_reward in [(args.baseline, 0), *((n, 1) for n in args.oracle)]:
        native = json.loads((results / 'trials' / name / 'result.json').read_text())
        assert native['exception_info'] is None
        assert native['verifier_result']['rewards']['reward'] == expected_reward
        trial_metadata.append({'trial': name, **{k: native[k] for k in
                              ('started_at', 'finished_at', 'exception_info', 'verifier_result')}})
    (evidence / 'trials.json').write_text(json.dumps(trial_metadata, indent=2) + '\n')
    shutil.copyfile(results / 'trials' / args.baseline / 'verifier/checks.json', evidence / 'baseline-checks.json')
    reports = []
    for name in args.oracle:
        trial = results / 'trials' / name / 'verifier'
        checks = json.loads((trial / 'checks.json').read_text())
        assert checks['report_validation_reward'] == 1 and len(checks['checks']) == 21 and not checks['errors']
        report = json.loads((trial / 'comparison.json').read_text())
        assert report['experiment_valid'] is True
        shutil.copyfile(trial / 'comparison.json', evidence / (name + '-comparison.json'))
        shutil.copyfile(trial / 'checks.json', evidence / (name + '-checks.json'))
        reports.append(report)
    assert reports[0]['run_id'] != reports[1]['run_id']
    signatures = [{c['name']: (c['read_uncommitted'], c['read_committed']) for c in r['cases']} for r in reports]
    assert signatures[0] == signatures[1], 'Native count outcomes differ across clean runs'
    report = reports[-1]
    summary = {
        'contract': report['contract'],
        'report_reward_meaning': 'complete truthful report, not universal exactly-once certification',
        'validation': {'baseline': args.baseline, 'baseline_reward': 0,
                       'oracle_runs': args.oracle, 'oracle_rewards': [1, 1],
                       'independently_verified_cases_per_run': 21,
                       'count_outcomes_identical': True,
                       'native_trials': trial_metadata,
                       'input_manifest_sha256': hashlib.sha256(manifest.read_bytes()).hexdigest()},
        'versions': report['versions'], 'topology': report['topology'],
        'limitations': report['limitations'],
        'cases': [{k: v for k, v in c.items() if not k.endswith('_rows') and k != 'live_observations'} for c in report['cases']],
        'visibility_runs': [{
            'trial': name,
            'cases': [{k: c[k] for k in ('name', 'read_uncommitted_visibility_ms', 'read_committed_visibility_ms')}
                      for c in r['cases'] if 'read_committed_visibility_ms' in c],
        } for name, r in zip(args.oracle, reports)],
    }
    (results / 'summary.json').write_text(json.dumps(summary, ensure_ascii=False, indent=2) + '\n')
    lines = [
        '# Kafka 交付实验结果', '',
        '要求：每个已接受事件在 Kafka 原始数据日志中恰好一条，并且恢复后不丢事件。', '',
        '**结论：当前实现的非事务方案都存在故障反例，不能满足该完整要求。** '
        'Kafka 输入 offset 与输出同事务、Flink 事务 sink 在已测恢复场景中保证已提交视图唯一，'
        '但中止事务的数据仍留在原始日志。', '',
        '三类主实现是直接 Producer、Flink 状态去重、Redis 去重；Kafka 事务作为对照。'
        'Outbox、相同 key、日志扫描恢复是补充实验。', '',
        f'全新 Harbor 环境：基线 `{args.baseline}` 得0；完整实验 `{args.oracle[0]}`、'
        f'`{args.oracle[1]}` 均得1，每次21个场景由独立消费者重读核对，计数一致。'
        '**这里的1仅表示报告完整且与 Kafka 一致，不表示所有方案满足要求。**', '',
        '## 原始日志与已提交视图', '',
        '每个场景8个不同业务事件；租约过期、迟到旧请求场景各1个。重复数表示多出的记录数。', '',
        '| 场景 | 原始条数 | 原始重复 | 原始缺失 | 已提交重复 | 已提交缺失 | 原始唯一且不丢 |',
        '|---|---:|---:|---:|---:|---:|---|',
    ]
    for c in report['cases']:
        raw, committed = c['read_uncommitted'], c['read_committed']
        lines.append(f"| {c['name']} | {raw['records']} | {raw['duplicate_records']} | {len(raw['missing_ids'])} | {committed['duplicate_records']} | {len(committed['missing_ids'])} | {'通过该场景' if c['physical_contract_pass'] else '不满足'} |")
    lines += [
        '', '通过单个场景不构成一般正确性证明。日志扫描在 ACK 后重启能恢复；'
        '旧请求晚于扫描到达时仍会重复。Redis 租约实验真实等待 Redis TTL 到期，'
        '新 writer 发布后再放行旧 writer；Redis 的 owner token 没有被 Kafka 用来拒绝追加。', '',
        'Kafka 事务恢复实验使用原生 `sendOffsetsToTransaction`，恢复后独立查询到源 offset=8。'
        'Flink 使用真实 KafkaSink、真实完成的 checkpoint 和原生恢复状态；'
        '不是通过重置模拟计数来推断结果。Kafka 事务输入使用单个分区的手动 assignment，'
        '未验证 consumer group rebalance。', '',
        '## 可见延迟观察', '',
        '以下是从 Producer record timestamp 到同容器消费者首次观察的时间；每格仅8个样本，'
        '不能当作生产 p95 或吞吐基准。Flink checkpoint 间隔1000ms，两个正常 Flink 场景'
        '都先完成初始 checkpoint、再放行输入；未计入作业冷启动。', '',
        '| 运行 | 场景 | 原始视图 p50/p95 ms | 已提交视图 p50/p95 ms |',
        '|---|---|---:|---:|',
    ]
    for name, r in zip(args.oracle, reports):
        for c in r['cases']:
            if 'read_committed_visibility_ms' not in c or c['name'].endswith('-restore'):
                continue
            raw, committed = c['read_uncommitted_visibility_ms'], c['read_committed_visibility_ms']
            lines.append(f"| {name} | {c['name']} | {raw['p50']}/{raw['p95']} | {committed['p50']}/{committed['p95']} |")
    lines += [
        '', '`transaction-visibility` 故意在 commit 前暂停，包含独立检查和至少250ms的受控等待；'
        '该行用于证明未提交数据已经追加但对 read_committed 不可见，不能用于评价事务性能。'
        'Redis 和 Outbox 的正常吞吐、不同批次/提交周期下的公平性能比较尚未实施。', '',
        '## 选择建议', '',
        '- 保留 Kafka 且要求原始日志业务唯一：这些标准客户端组合尚不能交付目标；'
        '需要目标侧原子去重追加或专门的身份恢复与 fencing 协议，不能只增加外部缓存。',
        '- 接受已提交视图唯一：Kafka→Kafka 可使用 offset/output 同事务；Flink 可使用事务 sink。'
        'Kafka 事务不自动覆盖外部数据库进度，实验中的“事务＋外部游标”同样重复。',
        '- 放宽到下游副作用唯一：Outbox＋至少一次投递＋消费者 inbox/唯一约束是另一种设计，'
        '但不满足当前原始日志要求。',
        '- 允许更换队列：下一轮优先验证 Pulsar broker dedup 或 RabbitMQ Streams 发布去重。'
        '需要稳定 producer 身份、事件序号和接管规则；当前仅有设计，未实现或测过。', '',
        '实验固定 Kafka Broker3.7.0、Java Client3.4.0、Redis7.2.6、Flink1.20.4。'
        '三节点、副本3、min ISR2、acks=all；每场景单输出分区。'
        'Broker 宕机、磁盘丢失、扩分区、保留期过期及生产吞吐不在本轮结论内。', '',
        '原始独立核对报告见 [evidence/](evidence/)，摘要见 [summary.json](summary.json)，'
        '构建和运行输入哈希见 [validation-inputs.sha256.json](validation-inputs.sha256.json)。'
        '机制解释和官方来源见 [设计文档](../docs/design.md)。', '',
    ]
    (results / 'COMPARISON.md').write_text('\n'.join(lines))
    print(json.dumps({'runs': args.oracle, 'cases': len(report['cases']), 'summary': str(results / 'summary.json')}))


if __name__ == '__main__':
    main()
