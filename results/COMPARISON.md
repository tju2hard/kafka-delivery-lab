# Kafka 交付实验结果

要求：每个已接受事件在 Kafka 原始数据日志中恰好一条，并且恢复后不丢事件。

**结论：当前实现的非事务方案都存在故障反例，不能满足该完整要求。** Kafka 输入 offset 与输出同事务、Flink 事务 sink 在已测恢复场景中保证已提交视图唯一，但中止事务的数据仍留在原始日志。

三类主实现是直接 Producer、Flink 状态去重、Redis 去重；Kafka 事务作为对照。Outbox、相同 key、日志扫描恢复是补充实验。

全新 Harbor 环境：基线 `closure-baseline` 得0；完整实验 `development-01`、`closure-oracle-01` 均得1，每次21个场景由独立消费者重读核对，计数一致。**这里的1仅表示报告完整且与 Kafka 一致，不表示所有方案满足要求。**

## 原始日志与已提交视图

每个场景8个不同业务事件；租约过期、迟到旧请求场景各1个。重复数表示多出的记录数。

| 场景 | 原始条数 | 原始重复 | 原始缺失 | 已提交重复 | 已提交缺失 | 原始唯一且不丢 |
|---|---:|---:|---:|---:|---:|---|
| plain-normal | 8 | 0 | 0 | 0 | 0 | 通过该场景 |
| idempotent-normal | 8 | 0 | 0 | 0 | 0 | 通过该场景 |
| plain-ack-drop | 9 | 1 | 0 | 1 | 0 | 不满足 |
| idempotent-ack-drop | 8 | 0 | 0 | 0 | 0 | 通过该场景 |
| application-replay | 9 | 1 | 0 | 1 | 0 | 不满足 |
| compacted-key | 9 | 1 | 0 | 1 | 0 | 不满足 |
| idempotent-restart | 9 | 1 | 0 | 1 | 0 | 不满足 |
| redis-before-restart | 7 | 0 | 1 | 0 | 1 | 不满足 |
| redis-after-restart | 9 | 1 | 0 | 1 | 0 | 不满足 |
| outbox-restart | 9 | 1 | 0 | 1 | 0 | 不满足 |
| reconcile-acked-restart | 8 | 0 | 0 | 0 | 0 | 通过该场景 |
| transaction-external-restart | 9 | 1 | 0 | 1 | 0 | 不满足 |
| redis-expired-owner | 2 | 1 | 0 | 1 | 0 | 不满足 |
| reconcile-late-old-request | 2 | 1 | 0 | 1 | 0 | 不满足 |
| atomic-offsets-before-commit | 16 | 8 | 0 | 0 | 0 | 不满足 |
| atomic-offsets-after-commit | 8 | 0 | 0 | 0 | 0 | 通过该场景 |
| transaction-visibility | 8 | 0 | 0 | 0 | 0 | 通过该场景 |
| flink-at-least-once-normal | 8 | 0 | 0 | 0 | 0 | 通过该场景 |
| flink-exactly-once-normal | 8 | 0 | 0 | 0 | 0 | 通过该场景 |
| flink-at-least-once-restore | 9 | 1 | 0 | 1 | 0 | 不满足 |
| flink-exactly-once-restore | 9 | 1 | 0 | 0 | 0 | 不满足 |

通过单个场景不构成一般正确性证明。日志扫描在 ACK 后重启能恢复；旧请求晚于扫描到达时仍会重复。Redis 租约实验真实等待 Redis TTL 到期，新 writer 发布后再放行旧 writer；Redis 的 owner token 没有被 Kafka 用来拒绝追加。

Kafka 事务恢复实验使用原生 `sendOffsetsToTransaction`，恢复后独立查询到源 offset=8。Flink 使用真实 KafkaSink、真实完成的 checkpoint 和原生恢复状态；不是通过重置模拟计数来推断结果。Kafka 事务输入使用单个分区的手动 assignment，未验证 consumer group rebalance。

## 可见延迟观察

以下是从 Producer record timestamp 到同容器消费者首次观察的时间；每格仅8个样本，不能当作生产 p95 或吞吐基准。Flink checkpoint 间隔1000ms，两个正常 Flink 场景都先完成初始 checkpoint、再放行输入；未计入作业冷启动。

| 运行 | 场景 | 原始视图 p50/p95 ms | 已提交视图 p50/p95 ms |
|---|---|---:|---:|
| development-01 | plain-normal | 19/115 | 19/116 |
| development-01 | idempotent-normal | 13/354 | 14/354 |
| development-01 | transaction-visibility | 8/58 | 1339/1401 |
| development-01 | flink-at-least-once-normal | 56/57 | 57/58 |
| development-01 | flink-exactly-once-normal | 58/79 | 685/707 |
| closure-oracle-01 | plain-normal | 32/186 | 32/183 |
| closure-oracle-01 | idempotent-normal | 12/295 | 12/295 |
| closure-oracle-01 | transaction-visibility | 13/58 | 1780/1843 |
| closure-oracle-01 | flink-at-least-once-normal | 76/85 | 76/86 |
| closure-oracle-01 | flink-exactly-once-normal | 40/50 | 1053/1063 |

`transaction-visibility` 故意在 commit 前暂停，包含独立检查和至少250ms的受控等待；该行用于证明未提交数据已经追加但对 read_committed 不可见，不能用于评价事务性能。Redis 和 Outbox 的正常吞吐、不同批次/提交周期下的公平性能比较尚未实施。

## 选择建议

- 保留 Kafka 且要求原始日志业务唯一：这些标准客户端组合尚不能交付目标；需要目标侧原子去重追加或专门的身份恢复与 fencing 协议，不能只增加外部缓存。
- 接受已提交视图唯一：Kafka→Kafka 可使用 offset/output 同事务；Flink 可使用事务 sink。Kafka 事务不自动覆盖外部数据库进度，实验中的“事务＋外部游标”同样重复。
- 放宽到下游副作用唯一：Outbox＋至少一次投递＋消费者 inbox/唯一约束是另一种设计，但不满足当前原始日志要求。
- 允许更换队列：下一轮优先验证 Pulsar broker dedup 或 RabbitMQ Streams 发布去重。需要稳定 producer 身份、事件序号和接管规则；当前仅有设计，未实现或测过。

实验固定 Kafka Broker3.7.0、Java Client3.4.0、Redis7.2.6、Flink1.20.4。三节点、副本3、min ISR2、acks=all；每场景单输出分区。Broker 宕机、磁盘丢失、扩分区、保留期过期及生产吞吐不在本轮结论内。

原始独立核对报告见 [evidence/](evidence/)，摘要见 [summary.json](summary.json)，构建和运行输入哈希见 [validation-inputs.sha256.json](validation-inputs.sha256.json)。机制解释和官方来源见 [设计文档](../docs/design.md)。
