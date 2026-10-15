# 异常如何模拟

`id-deduplication` 是新的业务 ID 过滤实验，重复到达直接跳过 sink。Redis 检查
及确认全部放在真实 `RichSinkFunction.invoke()` 中；先完成空 CK，再注入两种
确定性异常：Redis 占位成功但尚未调用 Kafka send；Kafka send 已收到 ACK 但
尚未登记 sent。它们分别验证漏发与重发窗口。异常由 Flink 自身自动恢复，不靠
controller 强杀作为触发；异常 witness 包含事件 JSON、Redis 值及 ACK/offset。
同一实验还包含真实 CK snapshot 异常，以及非事务/事务 KafkaSink 的对照。
这证明窗口能发生什么，不能据此估算生产环境发生概率。两种 Kafka 消费视图
分别导出完整 JSONL；首次事件以源分区 offset 顺序确定。

`id-marking-latency` 的每秒3000条 JSON 实验目前只测正常运行。它证明本轮输入、
标记和落盘结果是否一致，不证明故障恢复后也一致。1小时 TTL 同样没有通过
实际等待一小时来验证到期。

另一个独立实验 `delivery-matrix` 已执行实际故障注入，报告见
[交付结果](../results/COMPARISON.md)。其负载是每场景8个事件，并非3000条/秒。

## 已实现的故障点

| 故障 | 注入方式 | 核对方式 |
|---|---|---|
| Broker 已写入，客户端没有收到 ACK | TCP 代理解析成功 ProduceResponse 后丢弃并断开连接 | 原生 Producer 重试后重读 Kafka |
| 外部标记与 Kafka 写入之间崩溃 | 到指定业务阶段写出 witness，再由 controller 执行 SIGKILL | 从原有游标/标记恢复，检查重复和缺失 |
| 租约过期旧 writer 继续发送 | 最后一次 owner 检查后暂停，等待真实 Redis TTL 过期；新 writer 完成后放行旧 writer | 重读 Kafka 是否有两次追加 |
| 杀掉旧进程但网络请求迟到 | 代理扣留已收到的完整 ProduceRequest，替代 writer 扫描并发布后才转发 | 检查扫描是否遗漏迟到追加 |
| Flink 在下一次 CK 完成前崩溃 | 实际完成初始 CK；放行事件，看到 Kafka 原始记录；阻塞下一次 snapshot，再 SIGKILL | 使用实际 `_metadata` 恢复，检查原生 isRestored 和两种消费视图 |
| Kafka 事务提交前/后崩溃 | 在真实 sendOffsetsToTransaction/commitTransaction 边界暂停，再 SIGKILL | 独立检查源 offset、已提交输出及中止事务数据 |

`SIGKILL` 仅针对 controller 自己启动并登记的进程组。它让进程没有机会执行
正常关闭、最后一次 checkpoint 或优雅提交。杀进程是主动操作，模拟的是
进程突然崩溃；不是平滑重启。

实现入口：

- [进程登记、暂停、强杀和恢复控制](../harbor/delivery-matrix/environment/run_lab.py)
- [实际 Flink checkpoint 与恢复](../src/main/java/lab/FlinkRun.java)
- [真实 Kafka 网络故障代理](../harbor/delivery-matrix/environment/wire_proxy.py)

当前 Flink 故障实验是在 checkpoint 未完成时杀进程；它不等于测试
“checkpoint 自然超时后，原进程继续运行”的策略，也没有测试 Broker 断电。

## 新增3000条/秒 checkpoint 失败实验

`id-marking-recovery` 使用真实 checkpoint 回调制造两种不同情况：

- snapshotState 暂停8秒，超过配置的5秒 CK 超时；容忍一次失败。核对原生
  notifyCheckpointAborted，要求作业仍处于 attempt0，并完成后续 CK。
- snapshotState 抛一次 IOException，容忍失败次数为0。由 Flink 的固定延迟
  restart strategy 自动恢复任务，核对 attempt≥1 和 isRestored=true。

controller 保持同一个 JVM 存活，不调用 kill/relaunch 来制造这两种故障。
最终完成后才强杀本实验进程做清理。结果见 [恢复报告](../results/RECOVERY.md)。

快照异常恢复场景的流程：

1. 在输入业务数据前保存实际完成的恢复点。
2. 开始3万条输入；确认一部分已进入 Kafka，且下一次30秒 CK 尚未完成。
3. 在真实 snapshotState 回调抛一次异常，保留 Kafka、源输入和 Streams 状态。
4. 由 Flink 自动从恢复点恢复，保持 job/operator identity、分区映射和状态配置。
5. 同时统计 input_seq 重放、业务 ID 标记、负载守恒、已提交输出和原始记录。

非事务 Flink 上游可能重放已经发布的数据。若下游 Streams 保留重复消息并
标为 true，那么这些重放会增加输出条数；它们不能被“标记正确”掩盖。
因此恢复检查必须同时报告“每次输入到达是否完整”和“出现了多少重放”。
