# 设计边界与替代策略

## 三个不同的目标

1. **原始日志唯一**：`read_uncommitted` 重读包含未提交/已中止事务的数据记录，
   按业务 event_id 计数。Kafka 内部控制记录不是业务消息，不计入业务事件条数。
2. **提交视图唯一**：`read_committed` 看每个事件是否只有一个已提交结果。
3. **业务副作用唯一**：消费者用 event_id 做 inbox/唯一约束后只生效一次。
   这可以是工程方案，但不满足本项目要求的原始日志唯一。

本实验同时检查不丢消息。只要出现重复或漏发就是该故障场景的反例；某个
场景通过只说明该场景通过，不能从有限实验推导一般性保证。

## 从机制推导的预期，而非预填实测结论

Kafka 幂等 Producer 的去重身份是 producer session/partition/sequence，
不是业务 event_id。Broker 能拒绝同批网络重试，应用再次 send 同一个事件会
获得新的 sequence；重启创建新 session 也不能直接继承业务去重身份。
依据：[Kafka Producer API](https://kafka.apache.org/37/javadoc/org/apache/kafka/clients/producer/KafkaProducer.html)、
[KIP-98](https://cwiki.apache.org/confluence/spaces/KAFKA/pages/66854913/KIP-98%2B-%2BExactly%2BOnce%2BDelivery%2Band%2BTransactional%2BMessaging)。

Redis 的 SET NX/Lua 能原子化 Redis 内的状态，但没有同时原子地追加 Kafka。
先记后发有漏发窗口，先发后记有重发窗口。PENDING＋TTL＋owner token＋Lua
完成确认仍有“最终检查成功后、Kafka 追加前”窗口；Kafka 不解释该 token，
旧 writer 不能仅靠 Redis 侧 CAS 被 Kafka fencing。这是两套独立提交状态的
推导，实验要用真实操作和可控暂停来验证。
依据：[Redis SET](https://redis.io/docs/latest/commands/set/)。

数据库 Outbox 保住本地已接受的事件，可靠地做至少一次发布；若 Kafka 目标
不按稳定业务身份拒绝重发，ACK 与 SENT 标记间仍存在窗口。换成 CDC 发布
并不自动消除此窗口，故本轮先验证最小 SQLite Outbox，而不把 CDC 作为已测实现。

Kafka 事务可以把 Kafka 输入 offset 和输出放在同一事务中。失败事务的记录
会从 read_committed 过滤，但曾经追加的数据不因此从物理日志回滚。若输入
进度在 Kafka 之外，单独 commit Kafka 事务并不原子更新外部进度。
依据：[Kafka Design](https://kafka.apache.org/41/design/design/)。

Flink checkpoint 管状态一致性；非幂等外部副作用仍需外部 sink 的配合。
KafkaSink 的 EXACTLY_ONCE 使用 Kafka 事务并在 checkpoint 上提交，因此需要
分开看原始日志和提交视图。不能把 Kafka Streams/Flink 的 EOS 配置当作一种
“不用事务的 Kafka 日志去重”实现。
依据：[Flink Guarantees](https://nightlies.apache.org/flink/flink-docs-release-1.20/docs/connectors/datastream/guarantees/)、
[Flink KafkaSink](https://nightlies.apache.org/flink/flink-docs-release-1.20/docs/connectors/datastream/kafka/)。

Kafka compaction 是后台保留最新 key 版本的机制，不是追加时的唯一约束。
本轮检查当前活跃 segment 中相同 key 的两次写入，不把最终清理结果当实时去重。
依据：[Kafka Log Compaction](https://kafka.apache.org/41/design/design/)。

## 可进一步实现的路径

### 保留 Kafka，调整语义到“提交视图只有一次”

Kafka→Kafka 用事务同时提交 offset/output；Flink 用事务 sink。降低 checkpoint
间隔或缩小事务批次可降低可见等待，但会增加提交/协调开销，不能预先宣称
事务一定比 Redis 慢。需在相同资源、持久性和批次条件下做另一个性能实验。

### 保留 Kafka，要求原始日志唯一

标准幂等 Producer 只能直接解决其 session 内的传输重试。更广的业务唯一性
需要让目标侧在同一持久化边界内执行“判断稳定事件身份＋追加”，或者约束
所有请求的身份、所有权和顺序，使恢复可继承目标侧去重身份。仅增加外部
Redis、Outbox 或进程锁没有建立这个原子边界；不要把这些组件叫作已证明的
严格 Kafka exactly-once。Broker 扩展/自定义协议需要单独设计、维护和验证。

### 允许更换队列：优先验证 Broker 端跨重启发布去重

这是设计候选，**本轮未实测**：

- **Pulsar**：开启 broker deduplication，固定 producer name，稳定 sequence ID，
  固定分区映射；再核对 producer ownership/fencing、去重状态持久化和过期规则。
  不能每次重启换名，不能让无关事件复用 sequence，也不能只凭开启配置宣称成立。
  [Pulsar Messaging](https://pulsar.apache.org/docs/4.0.x/concepts-messaging/)、
  [Pulsar Producer](https://pulsar.apache.org/docs/client-libraries/producers/)。
- **RabbitMQ Streams**：稳定 producer name＋严格递增 publishing ID，Broker
  记录该 producer 的最高 ID。重复发布被过滤并确认。要求同一 stream/name
  只有一个发布者，必须处理消息顺序和接管；普通 RabbitMQ 队列不等于该机制。
  [RabbitMQ Stream Dedup](https://www.rabbitmq.com/docs/streams)、
  [Effectively-once](https://www.rabbitmq.com/docs/stream-effectively-once-processing)。

这两条路径最接近“不使用跨系统2PC、恢复后仍由目标 Broker 拒绝重发”，但
应复用本仓库的 ACK 丢失、崩溃、旧请求延迟、并发接管、ID/分区映射和状态
保留期矩阵来实测。当前 Kafka 原型不会冒充它们的验证结果。
