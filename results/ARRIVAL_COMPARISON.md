# 七种方案：首次标记、消息交付与可见时间

每场输入9000条，3000个业务 ID 各出现三次。**全部到达都要保留：正确输出是9000条，首次3000、重复6000。** 本文“去重判断”只决定首次/重复标记；sink 的发送登记只尝试跳过同一次输入的恢复重放。

以下是1轮真实 Kafka／Redis／Flink／Streams 进程实验，由独立消费者复读并逐条审计。恢复行包含故障等待；不同方案的故障位置见后文，不能把它们当成同一种随机故障。

| 方案 | 场景 | 输出总数 | 首次标记数 | 重复标记数 | 平均可见延迟（秒） | p99可见延迟（秒） | 结果 |
|---|---|---:|---:|---:|---:|---:|---|
| state 标记＋事务输出（CK＝30秒） | 正常 | 9000 | 3000 | 6000 | 27.65 | 29.05 | 本次正确 |
| state 标记＋事务输出（CK＝30秒） | 故障恢复 | 9000 | 3000 | 6000 | 34.81 | 36.23 | 本次已提交结果正确 |
| state 标记＋非事务输出 | 正常 | 9000 | 3000 | 6000 | 0.18 | 0.52 | 本次正确 |
| state 标记＋非事务输出 | 故障恢复 | 18000 | 6000 | 12000 | 14.60 | 30.12 | 恢复重发，首次也多输出 |
| 直接 Streams EOS | 正常 | 9000 | 3000 | 6000 | 2.14 | 2.76 | 本次正确 |
| 直接 Streams EOS | 故障恢复 | 9000 | 3000 | 6000 | 15.63 | 16.25 | 本次已提交结果正确 |
| 纯 Redis 标记：先登记，再输出 | 正常 | 9000 | 3000 | 6000 | 28.48 | 56.85 | 本次正确 |
| 纯 Redis 标记：先登记，再输出 | 故障恢复 | 9000 | 2999 | 6001 | 33.32 | 61.18 | 首次少标1；消息未漏 |
| 纯 Redis 标记：先输出，再登记 | 正常 | 9000 | 3000 | 6000 | 27.91 | 56.53 | 本次正确 |
| 纯 Redis 标记：先输出，再登记 | 故障恢复 | 9001 | 3001 | 6000 | 34.91 | 66.07 | 首次多输出1；重发1条 |
| state 标记＋Redis 发送登记：先登记 | 正常 | 9000 | 3000 | 6000 | 29.34 | 60.14 | 本次正确 |
| state 标记＋Redis 发送登记：先登记 | 故障恢复 | 8999 | 2999 | 6000 | 34.33 | 63.15 | 首次少1；漏发1条 |
| state 标记＋Redis 发送登记：后登记 | 正常 | 9000 | 3000 | 6000 | 29.23 | 60.07 | 本次正确 |
| state 标记＋Redis 发送登记：后登记 | 故障恢复 | 9001 | 3001 | 6000 | 35.15 | 66.72 | 首次多输出1；重发1条 |

**先登记 Redis 的两种问题不同：** 纯 Redis 标记方案保住了9000条消息，但其中一次首次被改成重复；state 标记＋Redis 发送登记方案保留了 state 算出的标记值，却漏掉了整条首次消息。**后登记的两种方案都在 ACK 后、登记前故障，恢复后多输出一次首次消息。**

平均值是所有实际输出的算术平均；计时从输入 send 前开始，到目标 read_committed 消费者第一次读到对应输出为止，包括积压、处理、写入、事务提交和观察等待。重发记录也纳入统计；漏掉的输入没有延迟样本，不能靠漏发让耗时显得更好。这里的输出用真实 Kafka 模拟 Talos，未调用真实 Talos 服务。Redis 使用逐条同步调用、AOF always，未做 pipeline；表中时间不是 Redis 的性能上限。

## 每种方案把判断放在哪里

| 方案 | 业务首次/重复由谁判断 | sink 的执行顺序 | Redis 已存在时 |
|---|---|---|---|
| 事务／非事务输出 | 上游 Flink keyed state，按 event_id | 输出带标记 JSON；事务输出随 CK 提交 | 无 Redis |
| 直接 Streams EOS | Streams 持久化 state，按 event_id | 状态、源 offset、输出在 Kafka 事务中提交 | 无 Redis |
| 纯 Redis：先登记 | sink 内 Redis，按 event_id | 原子检查并更新 seen → 加标记 → send/ACK | 标重复，仍然输出 |
| 纯 Redis：后登记 | sink 内 Redis，按 event_id | 查 seen → 加标记 → send/ACK → 更新 seen | 标重复，仍然输出 |
| state＋Redis：先登记 | 上游 Flink state，按 event_id | 按 input_seq 查并预约 → send/ACK | 跳过该次输入；预约不代表已发送 |
| state＋Redis：后登记 | 上游 Flink state，按 event_id | 按 input_seq 查 sent → send/ACK → 登记 sent | 跳过该次输入 |

“纯 Redis”指业务首次/重复判断完全由 Redis 完成，不使用 Flink 业务 state；仍使用 Flink source/sink 读取输入、执行发送和恢复源进度，没有中间转发 topic。

**纯 Redis 完全可以“查 Redis → 写 Kafka → 再登记 Redis”。** 本实验已单独实现这条链路；它的 seen 键代表该业务 ID 是否出现过，不承担逐次输入的发送确认。state＋Redis 方案则把两件事分开：state 算业务标记，Redis 按 input_seq 记发送进度。生产中该编号需要稳定，例如源 topic／partition／offset，不能每次重试重新生成。

以上四条 Redis 链路的检查、登记、send/ACK 均在同一个 sink.invoke 中，仍不是跨系统原子事务。后登记方案在本次单 writer 下运行；并发 writer 的检查与写入竞争、Redis 数据丢失、登记过期后重放都未覆盖。

## 如何定点暴露问题

每次 Redis 恢复实验都先完成真实空 checkpoint，再让第一条业务输入进入 sink。任务在 Redis 已更新／尚未 send，或 Kafka 已 ACK／Redis 尚未更新的位置停住。独立客户端读取 Redis 和 Kafka 原始／已提交视图，确认窗口存在后释放任务抛 IOException，Flink 在原 JVM 自动恢复。选中的是源分区内该业务 ID 的首次输入，见证文件保存了 input_seq、false 标记、Redis 值和 Kafka ACK／offset。

事务与非事务 Flink 对照仍注入真实 snapshotState IOException，CK=30秒；直接 Streams EOS 在数据 ACK 后、事务提交前 SIGKILL 自有 JVM，再恢复同一 application.id 和状态目录。100ms 是 Streams 提交配置，实测可见时间见表，不是100ms延迟保证。

## Flink 转发那条链路为什么移出

旧链路“Flink 普通转发 → 中间 Kafka → Streams EOS”用来观察 EOS 的作用范围：Flink 已多写进中间 topic 的新 offset，会被下游视为新输入。它是额外诊断，已移出当前七方案比较。原来的14402条 Redis 结果来自旧的业务标记方案和 checkpoint 故障，不能替代本次定点窗口结果。

- [逐轮数据、原始／已提交计数与故障见证](ARRIVAL_DETAILS.md)
- [旧版主报告](ARRIVAL_LEGACY_COMPARISON.md)／[旧版明细](ARRIVAL_LEGACY_DETAILS.md)
- [单独的90000条持续负载诊断](STATE_STORE_PROBE.md)
- [可复核 JSONL、原生见证与冻结源代码](arrival-v2-evidence/)／[新摘要](arrival-v2-summary.json)
