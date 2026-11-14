# 数字 ID 六小时 TTL 去重：Flink heap 与 RocksDB 实测

## 结论先说

这次不是模拟 Map，也不是 Kafka Streams：在 `kafka-delivery-lab` 新增独立模块，运行真实 Flink 1.20.4。
每个大场次 **14,285,714 条输入、10,000,000 个唯一数字 ID、4,285,714 条重复**，约 30% 重复。
两个后端、RocksDB 全量/增量 checkpoint、正反顺序两轮均按同一输入验证。
独立 oracle 逐 ID 与 input_seq 核对首条输出，不只核对总数。实际结果见下表与验证文件。

**关键不是“RocksDB 也用内存，所以应该和 heap 一样快”。** Heap 直接操作 JVM 对象；RocksDB 要序列化、经过 JNI，并管理缓存、memtable、SST 与合并。
增量 checkpoint 减的是快照的新增持久化数据，不会取消日常 get/put 的这些成本。
本次同时测到 heap 的 GC 开销，不能忽略其容量和暂停代价。

实测 heap 为 **25.5–27.9 万条/秒**，RocksDB 为 **14.7–16.0 万条/秒**；前者快但进程峰值约 **2.3 GiB**，后者约 **1.1 GiB**。这是该配置的实际取舍。上述序列化/JNI/LSM 是原理上的开销来源，本次没有 CPU profiler 分解，不能把吞吐差精确归因于其中某一项。

## 相同约束下的实测

- 单机、两 CPU 亲和性、并行度 2；JVM heap 上限 2 GiB，managed 512 MiB，network 64 MiB。
- 不是 1 GiB 整机预算。`retained heap`、进程 RSS、RocksDB 文件分别计量。
- 数字 ID 采用固定可逆 64 位映射，重复随机从此前 ID 采样；固定种子 20261114。
- 状态仅 `ValueState<Boolean>`，**没有为凑 GB 填充大 payload**。
- processing-time TTL 6 小时，每次到达刷新；实验全部在 TTL 内，不等待六小时或冒充已验证到期清理。
- 每轮新 JVM/空状态；r1 为 heap→full→incremental，r2 反序。

单位：吞吐万条输入/秒；内存/目录大小 MiB。CPU、GC 为活跃阶段采样覆盖区间，覆盖率见 JSON。

| 轮次/后端 | 吞吐 | GC 后堆占用 | 峰值进程 RSS | RocksDB 目录峰值 | 平均 CPU 核数 | GC 时间秒 |
|---|---:|---:|---:|---:|---:|---:|
| r1-heap | 25.49 | 998.6 | 2376.4 | 0.0 | 1.94 | 7.61 |
| r1-rocks-full | 15.99 | 26.2 | 1092.6 | 230.4 | 1.89 | 0.14 |
| r1-rocks-incremental | 14.80 | 26.2 | 1105.8 | 241.6 | 1.90 | 0.13 |
| r2-heap | 27.88 | 998.1 | 2357.1 | 0.0 | 1.92 | 7.10 |
| r2-rocks-full | 14.67 | 26.1 | 1093.7 | 312.4 | 1.90 | 0.12 |
| r2-rocks-incremental | 15.00 | 26.2 | 1090.4 | 215.9 | 1.90 | 0.15 |

吞吐是**从空状态开始的整批增长阶段**，包括重叠 checkpoint 和 JIT，不是预热后极限 QPS。
源会受反压暂停，并非外部 Kafka 持续堆积的无限队列。进程包含 MiniCluster 的 JobManager 和 TaskManager。
RocksDB 目录为逻辑文件长度之和，可能包括硬链接的重复计量，不代表独占物理块或内存。

## 约 1 GB 能存多少 ID？

本机实际累积到 **一千万唯一 ID**。Heap 的 GC 后堆占用约 1 GB，实际进程峰值明显高于它。
不能用 `1GB ÷ 8字节` 估算：还包括 Long 对象、状态表条目、TTL 时间戳/包装、索引与框架。
25 万、100 万、一千万三个规模均有实测；应参考多规模差值，而非把小场次固定开销摊成每条成本。

| 唯一 ID 数 | heap GC 后堆 MiB | heap 基线堆 MiB | heap 差额/ID 字节（近似） |
|---|---:|---:|---:|
| 250,000 (scale250k-heap) | 48.4 | 76.1 | 不适用：启动基线不稳 |
| 1,000,000 (scale1m-heap) | 117.8 | 24.4 | 97.9 |
| 10,000,000 (scale10m-r1-heap) | 998.6 | 23.7 | 102.2 |
| 10,000,000 (scale10m-r2-heap) | 998.1 | 22.8 | 102.3 |

25万场次的启动基线高于最终值，说明启动临时对象干扰明显，不能解释成负的状态开销，也不能用这条基线差额估计容量。100万到1000万的 retained heap 差值约为 **103 字节/新增 ID**，仅是这套实现的边际估算；这里的差额含框架/类加载对象，哈希表还会阶梯扩容，不是精确对象布局。

**容量和吞吐不是同一件事。** 一千万 ID 若要留六小时，简单容量算术约为 463 个新增 ID/秒；70% 输入为新 ID 时，约 661 条输入/秒就会在六小时新增一千万 ID。这不是性能上限，而是保留窗口的容量约束。重复还会刷新 TTL，旧 ID 可能留得更久，因此不能把这个数当作保证值；也不能把短批次的几十万条/秒当成可连续六小时保持相同内存。约 1 GB 的堆状态不等于 1 GB checkpoint 文件，也不等于 1 GB 进程。
RocksDB 的 Java heap 小，不等于总内存只有这些：native cache/memtable、共享页缓存要另外考虑。
本实验只验证到一千万 ID，不外推“1 GB 内存的 RocksDB 能无限装”或宣称测到了其极限容量。

## Checkpoint：负载中与负载结束后分开

5 秒触发、最大并发 1；两种后端都写**本机文件系统**，不是 HDFS/S3。

| 轮次/后端 | 负载中触发且最终完成的 CK 数¹ | 负载中 CK 秒 | 本次新增持久化 MiB | 负载后新增持久化 MiB |
|---|---:|---:|---:|---:|
| r1-heap | 11 | 0.78–5.43 | 17.72–168.50 | 171.67–171.67 |
| r1-rocks-full | 17 | 0.34–0.97 | 18.55–165.77 | 162.94–162.94 |
| r1-rocks-incremental | 19 | 0.43–1.10 | 11.25–85.24 | 7.89–77.99 |
| r2-heap | 11 | 0.21–4.31 | 0.19–169.33 | 171.67–171.67 |
| r2-rocks-full | 19 | 0.47–0.96 | 14.11–165.27 | 156.03–174.32 |
| r2-rocks-incremental | 19 | 0.43–1.01 | 10.58–118.92 | 0.04–78.08 |

¹按触发时间落在负载区间分类，CK 可在负载结束后完成。精确每次持续时间、新增字节与完整状态见 JSON。
`state_size` 是完整可恢复快照的逻辑大小，`checkpointed_size` 是本次 checkpoint 计入的新持久化大小；都不是 JVM heap 或实际网络字节。
停流后仍可能持久化尚未 flush 或 compaction 重写的 SST，本次空闲阶段仍可达约 78 MiB；一轮后期降至约 0.04 MiB。增量按 SST 文件复用，不等于只写新增业务记录。**不能拿已稳定的空闲增量 CK 去对比持续写入时的 heap 全量 CK**。

这些结果不证明线上 40 GB/HDFS 的瓶颈是磁盘。要定位仍需分别看 start delay、alignment、sync、async，再结合实际新增持久化字节、最慢 subtask 和存储指标。

## 正确性、失败和局限

- 所有最终运行目录的 `verification.json` 保留输入/唯一/重复/输出、缺失、额外、重复输出、首条 input_seq 错误数。
- `run-statuses.json` 明示退出码；checkpoint 成功/失败/进行中计数保留在各场次 JSON。
- `scale250k-heap` 与 `scale10m-r2-rocks-incremental` 的 checkpoint 汇总各有 **failed=1**，但返回的 history/latest.failed 没有原因；不把它隐瞒，也不猜测原因。输出正确不代表所有 checkpoint 尝试都成功。关闭任务时尚在进行的 CK 不当作已完成。
- 没有 crash/recovery 验证；source offset 和文件 sink 不参与事务恢复，不能当生产 exactly-once 实现。
- 延迟记录到 state 处理或 sink callback，**不是 fsync / Kafka committed visibility**；p99 是 log2 微秒桶的上界。
- 显式 GC 在负载开始前与完全处理后，排除在吞吐区间之外。CPU/GC 每秒采样会漏掉区间两端，短批次不做性能归因。
- 没有网络存储、真实 Kafka、六小时自然过期、生产键倾斜或长期 compaction 压力测试。
- 共享云端、两轮而非统计学大样本。结果支持本场景取舍，不构成固定“多几台机器”的通用公式。


## 延迟与反压补充

| 场次 | source→sink callback 均值 ms | p99 上界 ms | source 反压均值 ms/s | 活跃 CPU 采样覆盖率 |
|---|---:|---:|---:|---:|
| r1-heap | 39.69 | 262.144 | 830.8 | 97.3% |
| r1-rocks-full | 64.85 | 524.288 | 928.0 | 98.6% |
| r1-rocks-incremental | 70.16 | 524.288 | 920.5 | 98.6% |
| r2-heap | 36.44 | 262.144 | 843.1 | 96.8% |
| r2-rocks-full | 70.77 | 524.288 | 924.6 | 98.7% |
| r2-rocks-incremental | 68.87 | 524.288 | 907.0 | 98.8% |

## 代码与证据

- [复现说明](../experiments/flink-state-backends/README.md)
- [Java 实验](../experiments/flink-state-backends/src/main/java/lab/StateBackendBench.java)
- [运行器和独立 oracle](../experiments/flink-state-backends/run.py)
- [机器可读汇总](flink-state-evidence/summary.json)、[运行状态](flink-state-evidence/run-statuses.json)
- [完整小型观测证据](flink-state-evidence/)：REST checkpoint/subtask 详情、每秒 JVM/进程/反压样本、输入及输出哈希。
- 大输入/输出原始文件保留在实验机器的忽略目录，未提交数百 MB 文件；可由固定种子复现。

原理依据：[Flink state backends](https://nightlies.apache.org/flink/flink-docs-release-1.20/docs/ops/state/state_backends/)、[Flink TTL](https://nightlies.apache.org/flink/flink-docs-release-1.20/docs/dev/datastream/fault-tolerance/state/#state-time-to-live-ttl)。
