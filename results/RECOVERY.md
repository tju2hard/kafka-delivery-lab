# Checkpoint 失败后：消息完整性与 ID 标记

每个场景四线程目标每秒3000条，输入30000条 JSON，10000个业务 ID 各出现三次。保留每次到达，按滚动1小时处理时间标记 is_duplicate。Flink 周期 CK=30秒，Streams commit=100ms。

**主实验是 checkpoint 失败，而不是人工杀进程。** 先完成真实空 checkpoint，再开始业务输入。场景分为：

- `*-timeout`：真实 snapshotState 阶段暂停8秒，超过5秒 CK 超时；容忍一次失败。要求原生 abort 通知，作业保持 attempt0，下一次 CK 成功。
- `*-restart`：真实 snapshotState 中抛一次 IOException，容忍次数0；Flink 自动恢复作业。要求原生 attempt≥1、isRestored=true 和恢复后的成功 CK。JVM 始终存活，controller 没有用 kill/relaunch 触发故障。

最终完成后强杀本实验进程仅用于清理，不算故障注入。

Harbor 基线 `recovery-baseline-01` 得0；`recovery-oracle-01`、`recovery-oracle-02` 两次完整实验均得1。1表示报告经过独立 Kafka 消费者逐条核对正确，不表示所有方案满足交付要求。

## 已提交视图：逐条比较源输入与最终 JSON

| 运行 | 场景 | 输入 | 已提交输出 | 多出的重放记录 | 缺失 | 负载错误 | 标记错误 | 每次输入恰好一次且标记正确 |
|---|---|---:|---:|---:|---:|---:|---:|---|
| recovery-oracle-01 | state-alo-timeout | 30000 | 30000 | 0 | 0 | 0 | 0 | 通过该场景 |
| recovery-oracle-01 | state-eos-timeout | 30000 | 30000 | 0 | 0 | 0 | 0 | 通过该场景 |
| recovery-oracle-01 | state-alo-restart | 30000 | 60000 | 30000 | 0 | 0 | 10000 | 不满足 |
| recovery-oracle-01 | redis-alo-restart | 30000 | 36973 | 6973 | 0 | 0 | 0 | 不满足 |
| recovery-oracle-01 | streams-after-alo-restart | 30000 | 60000 | 30000 | 0 | 0 | 0 | 不满足 |
| recovery-oracle-01 | state-eos-restart | 30000 | 30000 | 0 | 0 | 0 | 0 | 通过该场景 |
| recovery-oracle-02 | state-alo-timeout | 30000 | 30000 | 0 | 0 | 0 | 0 | 通过该场景 |
| recovery-oracle-02 | state-eos-timeout | 30000 | 30000 | 0 | 0 | 0 | 0 | 通过该场景 |
| recovery-oracle-02 | state-alo-restart | 30000 | 60000 | 30000 | 0 | 0 | 10000 | 不满足 |
| recovery-oracle-02 | redis-alo-restart | 30000 | 36973 | 6973 | 0 | 0 | 0 | 不满足 |
| recovery-oracle-02 | streams-after-alo-restart | 30000 | 60000 | 30000 | 0 | 0 | 0 | 不满足 |
| recovery-oracle-02 | state-eos-restart | 30000 | 30000 | 0 | 0 | 0 | 0 | 通过该场景 |

标记错误按最终输出的实际顺序核对：同一业务 ID 第一次应为 false，后续为 true。重复写入同一个 input_seq 仍是重放缺陷，即使它被正确标成 true。源 input_seq 唯一，payload、event_id、ingested_at_ms 必须完整保留。

## 原始视图与事务可见性

| 运行 | 场景 | 原始数据条数 | 已提交数据条数 | false 标记 | true 标记 | 多次 false 的 ID 数 | 从未 false 的 ID 数 |
|---|---|---:|---:|---:|---:|---:|---:|
| recovery-oracle-01 | state-alo-timeout | 30000 | 30000 | 10000 | 20000 | 0 | 0 |
| recovery-oracle-01 | state-eos-timeout | 30000 | 30000 | 10000 | 20000 | 0 | 0 |
| recovery-oracle-01 | state-alo-restart | 60000 | 60000 | 20000 | 40000 | 10000 | 0 |
| recovery-oracle-01 | redis-alo-restart | 36973 | 36973 | 10000 | 26973 | 0 | 0 |
| recovery-oracle-01 | streams-after-alo-restart | 60000 | 60000 | 10000 | 50000 | 0 | 0 |
| recovery-oracle-01 | state-eos-restart | 60000 | 30000 | 10000 | 20000 | 0 | 0 |
| recovery-oracle-02 | state-alo-timeout | 30000 | 30000 | 10000 | 20000 | 0 | 0 |
| recovery-oracle-02 | state-eos-timeout | 30000 | 30000 | 10000 | 20000 | 0 | 0 |
| recovery-oracle-02 | state-alo-restart | 60000 | 60000 | 20000 | 40000 | 10000 | 0 |
| recovery-oracle-02 | redis-alo-restart | 36973 | 36973 | 10000 | 26973 | 0 | 0 |
| recovery-oracle-02 | streams-after-alo-restart | 60000 | 60000 | 10000 | 50000 | 0 | 0 |
| recovery-oracle-02 | state-eos-restart | 60000 | 30000 | 10000 | 20000 | 0 | 0 |

事务恢复以 read_committed 检查输入/输出一致性。原始视图包含中止事务的数据，不能把这些原始数据与已提交业务消息混成一个计数。

## 如何解释不同方案

- CK 超时且没有状态回滚时，不能仅凭 CK 失败就推断非事务输出重复。这就是两个 tolerated-timeout 对照的作用。
- Flink state＋非事务 sink：恢复点之后的输出不能随状态一起回滚；重放可能多写记录，回滚后的 seen 状态也可能再次产生 false。
- Flink 内 Redis 标记＋非事务 sink：这是 sink 前登记 seen 的旧标记方案，不代表 Kafka 已成功输出；Redis 状态不随 CK 回滚。重放与输出的标记可能不同。需分别看额外记录、重复 false，以及有没有 ID 从未获得 false。当前 Redis 是逐条同步 Lua＋AOF/always，吞吐与排空速度不是方案上限。
- 非事务 Flink→Streams EOS：下游事务保护中间 Kafka 的读取与输出，上游重放已经形成新的中间记录。当前实现保留这些记录并标记，不会自动删除它们。
- Flink 事务 sink：恢复时将未完成事务排除在已提交视图之外，配合源进度和状态恢复，核对每个 input_seq 是否仅有一个正确结果。

机制解释不能替代上面的实测数字。当前只覆盖单机、单分区、一次受控 CK 故障；没有验证 Broker 断电、磁盘损坏、rebalance 或实际等待1小时到期。

## 代码与复现

- [真实 CK 异常与自动恢复](../src/main/java/lab/CheckpointFailureDemo.java)
- [controller 与逐条审计](../harbor/id-marking-recovery/environment/recovery.py)
- [独立 Kafka 复读验证](../harbor/id-marking-recovery/tests/verify.py)
- [全部输入/输出 JSONL、native witness 与哈希](recovery-evidence/)

```bash
bash scripts/build.sh
harbor trials start -p harbor/id-marking-recovery -a oracle \
  --trial-name my-recovery --trials-dir results/trials
```

每个场景的 `verifier/data/<case>/` 包含 input.jsonl.gz、output-read_committed.jsonl.gz、output-read_uncommitted.jsonl.gz。这些文件来自实际消费者，已逐条核对完整 JSON。

离线检查恢复数据（退出码1表示发现交付或标记缺陷，非报告无效）：

```bash
python3 scripts/compare-recovery-jsonl.py --input DATA/input.jsonl.gz \
  --output DATA/output-read_committed.jsonl.gz
```

正常一对一负载使用 compare-jsonl.py；恢复检查按最终输出顺序判断标记，同时独立统计源序号重放。两种标记统计口径不能混用。
