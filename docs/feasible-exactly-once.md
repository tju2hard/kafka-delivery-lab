# 不重复、不漏发：哪些路线可以实现

这里区分两个目标：每个业务 ID 在已提交输出中恰好一次；以及目标队列日志中
该 ID 物理上只保存一条。所有结论都以明确的持久化、故障和去重保留期为前提。

## 保留 Kafka：已有实测与低延迟路线

本仓库已实现 `keyBy(event_id)`＋1小时 TTL state 过滤＋Flink 事务 KafkaSink。
两次独立原生 CK 失败实验均是9000次输入、3000个业务 ID，恢复后已提交输出
3000条，缺失0、重复0、负载错误0。原始视图有6000条，包含中止事务的数据。
完整证据见 [业务 ID 实验](../results/DEDUPLICATION.md)。这不是原始日志物理不重复。

如果要减少对 Flink 30秒 CK 发布节奏的依赖，可实现：

```text
Kafka 输入 → 按业务 ID 分区 → 可恢复去重 state → 只转发首次 → Kafka Streams EOSv2
```

EOSv2 使用 Kafka 事务，协调输入 offset、state changelog 与输出。官方配置文档
说明 EOS 默认 commit.interval.ms 为100ms；这不是端到端100ms保证。消费结果需
使用 read_committed。[Kafka Streams 配置与处理保证](https://kafka.apache.org/34/streams/developer-guide/config-streams/)

旧 `TaggingDemo` Streams 原型保留到达并打标记；新的 `StreamsDeduplicationDemo`
已实现首次过滤、EOSv2 和提交前崩溃恢复实验，见 [Streams 过滤实测](../results/STREAMS_FILTER.md)。
这个过滤版本的当前端到端实测仍为秒级，尚未达到100ms；不能用提交配置代替延迟结果，
也不能将旧标记实验作为新过滤版本的证明。

## 不用分布式两阶段提交：目标 MQ 提供持久幂等追加

RabbitMQ Streams 提供原生发布去重。消息和 publishing ID 同时写入 stream，
恢复后从 stream 重建每个 producer 的最大 publishing ID。再次提交旧序号时，
Broker 确认请求但不会再次保存消息，因此不需要外部 Redis SENT 记录。
[RabbitMQ effectively-once 原理与条件](https://www.rabbitmq.com/docs/stream-effectively-once-processing)

适合先做单输入分区、单活 writer 的非事务原型：

```text
持久 Kafka 输入，携带 topic/partition/offset
  → Flink 按 event_id 维护可恢复 state，只保留首次
  → RabbitMQ Streams sink
      producer name：稳定绑定源分区和目标 stream
      publishing ID：这次首次输出对应的源 Kafka offset
      重试/恢复时沿用相同身份、序号、目标和负载
```

这条路线的关键条件：

- 输出按源分区 offset 递增发布，源与目标的映射保持稳定。四线程生成器的
  `input_seq` 不代表 Kafka offset，不能用它代替发布序号，也不能直接取业务 ID 的 hash。
- 每个 producer name 只有一个活跃 writer；故障接管必须阻止旧 writer 继续发布。
- 未确认的输出要重试；永久发送错误应停止推进。成功 CK 前，sink 必须确认此前
  输出已经持久化。可以等待 ACK 而不等待未来 CK 才让消息可见，不需要 prepare/commit。
- 恢复时同时恢复业务 ID state 和源进度，重放 CK 之后的输入。已经落入目标队列的
  结果由 Broker 去重，同时重放补齐 state。不能只从目标最大发布序号之后开始读，
  却继续使用更早 CK 中的 state，否则可能忘掉部分已见业务 ID。
- 业务 ID 首次选择和1小时窗口在重放时须保持一致；序号去重本身不是任意 JSON
  业务 ID 去重。扩分区、rescale、窗口边界、保留期及 Broker 故障需要另行验证。

因此它可以作为本项目的非事务实现方向，但**RabbitMQ Streams 尚未在本仓库安装
或实测**，上述是基于官方机制的设计推导，不是已通过 Harbor 的实现。

Pulsar 也提供服务端去重，基于 producer identity 和 sequence ID，重发同一条
消息时可以避免再次持久化；同样不能直接等同于任意业务 ID 去重。
[Pulsar 消息去重](https://pulsar.apache.org/docs/4.0.x/concepts-messaging/#message-deduplication)
本仓库同样没有完成 Pulsar 原型或故障验证。

## 外置 Redis＋普通 Kafka 的实测结论

Redis 检查、发送和登记都在 sink 内，也没有跨系统的原子提交。两次完整实验均
复现：先写 Redis 再发 Kafka 的定点故障缺失1个 ID；Kafka ACK 后再写 Redis 的
定点故障多出1条重发。普通 CK 失败时后者可以保持一致，但这不能覆盖两个写入
之间的故障。这条具体实现不能作为“不重复、不漏发”的严格保证。
