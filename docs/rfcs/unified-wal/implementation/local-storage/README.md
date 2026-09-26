# 隔离本地存储实施

状态：EXPERIMENTAL / NON_PROMOTABLE / NO_AUTHORITY / DISCARDABLE。

施工清单仍由[上层 README](../README.md)拥有，本目录只记录步骤1开始的本地存储实现与验证。源码在现有 `bookkeeper-server` 模块的 `org.apache.bookkeeper.bookie.storage.segment` 包，不接入生产 Bookie、LedgerStorage、Journal、Add 或 ACK。

## 当前实现

步骤1：`IsolatedSegmentConfig` 固定单 Arena、4个逻辑 shard、两个16 MiB DATA文件、4096字节对齐、1 MiB payload上限、256个pending上限及 FILE_CHANNEL/OFF/SYSTEM/PARK。参数检查拒绝容量溢出、非对齐容量和未实现模式。配置合同由步骤3的文件实现消费。

执行位置为 `/Users/liusinan/apps/ideaproject/nereusstream/bookkeeper` 的 `turbo-bk`；规划输入提交为 `e75c67f3514d1627ea790d052e2c9226e593aa3c`，代码原基线为 `e8b162076fd8cfa84a11351a82caac8c5ac1f939`。完整执行目标见[执行正文](../execution-objective.md)。

步骤3：已实现[实验格式v1](format-v1.md)、DATA/控制framing writer与独立parser、固定字节向量、全容量预创建文件及必要目录同步。真实FileChannel处理短写后force，失败保留坐标并停止受影响文件写入；容量满额停止。控制文件目前只有身份块，control parser只校验framing。尚无allocator授权、逐entry成功、selector或restart服务。

步骤4的第一部分：`SegmentOwnerLoop` 用固定平台线程承载静态 shard 子集，ledger→shard 只依赖固定逻辑 shard 数，worker 数不进入持久身份。每个已准入批次预留一个 completion 槽，控制队列独立有界；循环每轮处理完成、控制和各 shard 的一个输入。PARK 遵守最老 batch deadline，后来的 deadline 不能推迟它。`SegmentIoExecutor` 在启动线程预创建固定数量服务线程，用有界队列执行阻塞 I/O，并将结果送回原 owner；提交后 handler 异常、停止准入或等待超时不提前释放在途义务。

该循环尚未接入实际 append/allocator 状态机，尚无 group 启动、CPU绑定或NUMA池。容量由构造参数显式给定；多 owner 集成时必须从同一总预算分配，不能给每个 owner 复制全局额度。

## 验证入口

```bash
scripts/unified-wal/run-local-storage-tests.sh IsolatedSegmentConfigTest,SegmentBatchFormatTest,SegmentControlFormatTest,SegmentOwnerLoopTest
mvn -o -pl bookkeeper-server -DskipTests spotless:check checkstyle:check
```

脚本为每次运行创建独立临时结果目录，保存HEAD/branch、源码及测试SHA-256、命令、平台、Maven输出、退出码和本次生成的JUnit XML。脚本不覆写历史receipt，不扫描或复制用户PDF。默认离线使用已缓存依赖；首次缺依赖时用 `UW_FETCH_DEPENDENCIES=true scripts/unified-wal/run-local-storage-tests.sh` 联网获取，后续不重复请求snapshot元数据。它通过 `-P '!cargo-zigbuild'` 禁用本实验不使用的Linux Rust交叉编译profile；CPU affinity原模块仍构建，但没有进行Linux native/NUMA运行验证。

首次探索性离线构建缺少datasketches/Vert.x依赖；未禁用上述profile的尝试因本机缺少cargo-zigbuild失败。这些是构建失败，不是测试PASS或故障注入结果。

## 当前验证

`unified-wal-local-TrhdAZFy`：20模块构建成功，`IsolatedSegmentConfigTest` 2项通过，0失败/错误/跳过；JUnit耗时0.033秒，Maven总耗时17.896秒（离线构建），两者都不是性能测量。Spotless和Checkstyle通过。source/config/run及原始输出校验见[步骤1回执](step1-receipt.json)。

2026-09-26 `unified-wal-local-Gms0z3fD`：14项通过（2配置、9 DATA/文件、3控制framing），0失败/错误/跳过，重试关闭；Spotless/Checkstyle通过。[步骤3回执](step3-receipt.json)绑定本次源码、命令和结果。覆盖固定向量、N-1/N/N+1、44字节差集、长度溢出、截断/坏CRC/未知类型/generation、短写/force失败/零进展、部分初始化、满额停止和单record读取。

文件测试的可复现工作量：默认DATA预创建2×16 MiB；4096字节批次在每次最多7字节的故障注入下需要586次write及1次force；1024字节payload的record为1220字节，先读160字节header再读record共1380字节，小于整批4096字节。16 KiB文件扣除4096字节身份块后容纳3个4096字节批次，下一批被拒绝。上述为工作量/边界测量，不是性能对照；首份局部性能报告等待步骤5～7的实际闭环。

2026-09-26 `unified-wal-local-Kjwv4bjm`：当前隔离测试20项通过，0失败/错误/跳过，Spotless/Checkstyle通过。[owner循环回执](owner-loop-receipt.json)记录本次源码和范围。新增6项测试验证满额4批仍可完成、控制队列保留容量、多shard轮转、无新输入deadline触发、实际FileChannel I/O线程与owner线程隔离、有界I/O拒绝/错误回传、异常后在途义务保留及100次空队列唤醒交错。I/O测试固定1线程＋1队列槽，第三次提交被拒绝并回原owner，不无限排队。测试工作量不证明CPU或延迟收益。

## 未覆盖范围

步骤4的拓扑/绑定/NUMA池及存储集成、步骤5条件控制日志/allocator、步骤6逐entry写入/幂等、步骤7定位/pin/restart尚未实现。性能、Linux绑定/NUMA、掉电、真实网络/MetadataStore和Gate验证均NOT_EXECUTED。配置边界测试不授予任何持久化或生产authority。
