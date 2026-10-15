# 按业务 ID 过滤：Redis 能否模拟事务的不重不漏？

目标是在1小时去重有效期内，每个业务 ID 只输出源分区中首次到达的一条完整 JSON。重复到达直接跳过 sink，不再只是标记。每场景四线程目标3000条/秒，9000条输入，3000个 ID 各三次，CK=30秒；计数和业务集合都从真实源 Kafka 复读得到。

Redis 检查和登记都在 `RedisSink.invoke()` 内。先登记再发留下漏发窗口；先发、等 ACK 再登记留下重发窗口。同一个函数不能把两次跨系统写入变成原子操作。

原生 Harbor baseline `deduplication-baseline-02` 为0；两次独立 Oracle `deduplication-oracle-02`、`deduplication-oracle-03` 均正常结束且 reward=1。reward只证明报告由独立消费者核对正确，不表示每个方案满足交付契约。

| 运行 | 场景 | 源到达数 | 业务 ID 数 | 原始输出 | 已提交输出 | 缺失 ID | 额外重复记录 | 完整负载/首次选择错误 | 已提交业务契约 |
|---|---|---:|---:|---:|---:|---:|---:|---:|---|
| deduplication-oracle-02 | redis-after-normal | 9000 | 3000 | 3000 | 3000 | 0 | 0 | 0 | 本场景通过 |
| deduplication-oracle-02 | redis-after-checkpoint | 9000 | 3000 | 3000 | 3000 | 0 | 0 | 0 | 本场景通过 |
| deduplication-oracle-02 | redis-before-gap | 9000 | 3000 | 2999 | 2999 | 1 | 0 | 0 | 不满足 |
| deduplication-oracle-02 | redis-after-gap | 9000 | 3000 | 3001 | 3001 | 0 | 1 | 0 | 不满足 |
| deduplication-oracle-02 | state-alo-checkpoint | 9000 | 3000 | 6000 | 6000 | 0 | 3000 | 0 | 不满足 |
| deduplication-oracle-02 | state-eos-checkpoint | 9000 | 3000 | 6000 | 3000 | 0 | 0 | 0 | 本场景通过 |
| deduplication-oracle-03 | redis-after-normal | 9000 | 3000 | 3000 | 3000 | 0 | 0 | 0 | 本场景通过 |
| deduplication-oracle-03 | redis-after-checkpoint | 9000 | 3000 | 3000 | 3000 | 0 | 0 | 0 | 本场景通过 |
| deduplication-oracle-03 | redis-before-gap | 9000 | 3000 | 2999 | 2999 | 1 | 0 | 0 | 不满足 |
| deduplication-oracle-03 | redis-after-gap | 9000 | 3000 | 3001 | 3001 | 0 | 1 | 0 | 不满足 |
| deduplication-oracle-03 | state-alo-checkpoint | 9000 | 3000 | 6000 | 6000 | 0 | 3000 | 0 | 不满足 |
| deduplication-oracle-03 | state-eos-checkpoint | 9000 | 3000 | 6000 | 3000 | 0 | 0 | 0 | 本场景通过 |

目标速率与实际源发送速率分开报告；源发送完成不等于 sink 已经排空：

| 运行 | 场景 | 目标到达/s | 实际源发送/s | 源全部 ACK 耗时 ms |
|---|---|---:|---:|---:|
| deduplication-oracle-02 | redis-after-normal | 3000 | 2988.8 | 3135.9 |
| deduplication-oracle-02 | redis-after-checkpoint | 3000 | 2996.2 | 3048.5 |
| deduplication-oracle-02 | redis-before-gap | 3000 | 2996.4 | 3057.8 |
| deduplication-oracle-02 | redis-after-gap | 3000 | 2997.1 | 3009.4 |
| deduplication-oracle-02 | state-alo-checkpoint | 3000 | 2994.5 | 3020.4 |
| deduplication-oracle-02 | state-eos-checkpoint | 3000 | 2976.4 | 3049.9 |
| deduplication-oracle-03 | redis-after-normal | 3000 | 2990.1 | 3227.4 |
| deduplication-oracle-03 | redis-after-checkpoint | 3000 | 2992.2 | 3043.4 |
| deduplication-oracle-03 | redis-before-gap | 3000 | 2994.8 | 3020.0 |
| deduplication-oracle-03 | redis-after-gap | 3000 | 2981.3 | 3031.8 |
| deduplication-oracle-03 | state-alo-checkpoint | 3000 | 2990.7 | 3029.7 |
| deduplication-oracle-03 | state-eos-checkpoint | 3000 | 2994.6 | 3014.4 |

## 故障含义

- `redis-after-normal`：Redis 已记录 sent 则跳过，否则发 Kafka、等待 ACK 后才登记。正常对照不注入故障。
- `redis-after-checkpoint`：相同 sink，真实 snapshotState 抛 IOException；Flink 自动从空 CK 恢复。单独这种 CK 故障可能保持一致，因为 Redis 不回滚；不能推导任意故障安全。
- `redis-before-gap`：在 Redis SET NX 成功后、Kafka send 之前抛任务异常。恢复后 Redis 仍认为该 ID 存在，因此三次到达都被跳过。
- `redis-after-gap`：在真实 Kafka ACK 后、Redis SET sent 前抛任务异常。恢复时 Redis 尚无记录，因此同一个首次事件再次写入。witness 保存 ACK offset，验证器独立找到该记录。
- `state-alo-checkpoint`：keyBy＋TTL ValueState 过滤，native KafkaSink 非事务输出。状态回滚但已发布的 Kafka 记录仍在，重放导致重复。
- `state-eos-checkpoint`：同样过滤，native KafkaSink 使用 EXACTLY_ONCE。读已提交视图检查事务恢复；原始日志可能同时含中止事务数据。

**严格要求 Kafka 原始日志每个 ID 物理上只有一条时，事务对照也不能用已提交视图冒充通过。** 上表同时保留两种真实计数。事务模拟的业务交付语义通常对应 read_committed。

所有业务数据均在真实空 CK 完成后产生。故障均由实际 snapshot 或 sink 回调抛异常，通过 Flink 自身 restart strategy 恢复；native isRestored=true、attempt≥1 及数据 CK 成功为必查项。JVM 保持存活，controller 仅在最终完成后清理自己启动的进程。

## 数据完整性与限制

验证器重新消费源和两种输出视图，逐条比较 event_id、input_seq、嵌套 payload、ingested_at_ms 及 Kafka key，并检查首次选择。相同总数不能掩盖一个 ID 缺失、另一个 ID 重复。完整源/输出 JSONL.gz、SHA256、缺失 ID、重复 ID 以及 native 故障 witness 见 [证据目录](deduplication-evidence/) 和 [结构化报告](deduplication-summary.json)。

合成输入、单机、单分区、parallelism=1；没有测试并发同 ID writer、Redis 数据丢失、Broker/磁盘故障或实际等待1小时到期。Redis 开启 AOF/always，串行同步写，未做吞吐优化。这些定点故障证明窗口的后果，不能估算生产自然故障概率，也不证明任意故障安全。

实现入口：[sink 与 state 过滤](../src/main/java/lab/DeduplicationDemo.java)、[实验 controller](../harbor/id-deduplication/environment/deduplication.py)、[独立验证器](../harbor/id-deduplication/tests/verify.py)、[9条输入/3条输出小例子](examples/deduplication.md)。

```bash
bash scripts/build.sh
harbor trials start -p harbor/id-deduplication -a oracle \
  --trial-name business-deduplication --trials-dir results/trials
python3 scripts/compare-business-jsonl.py \
  --input DATA/input.jsonl.gz --output DATA/output-read_committed.jsonl.gz
```

离线检查退出码0表示业务 ID 契约通过，1表示发现缺失、重复或负载/首次选择错误。离线 JSON 导出不含 Kafka key；原生验证器另外核对真实 Kafka key。
