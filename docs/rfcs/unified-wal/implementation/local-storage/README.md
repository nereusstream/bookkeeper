# 隔离本地存储实施

状态：EXPERIMENTAL / NON_PROMOTABLE / NO_AUTHORITY / DISCARDABLE。

施工清单仍由[上层 README](../README.md)拥有，本目录只记录步骤1开始的本地存储实现与验证。源码在现有 `bookkeeper-server` 模块的 `org.apache.bookkeeper.bookie.storage.segment` 包，不接入生产 Bookie、LedgerStorage、Journal、Add 或 ACK。

## 当前实现

步骤1：`IsolatedSegmentConfig` 固定单 Arena、4个逻辑 shard、两个16 MiB DATA文件、4096字节对齐、1 MiB payload上限、256个pending上限及 FILE_CHANNEL/OFF/SYSTEM/PARK。参数检查拒绝容量溢出、非对齐容量和未实现模式。配置合同由步骤3的文件实现消费。

执行位置为 `/Users/liusinan/apps/ideaproject/nereusstream/bookkeeper` 的 `turbo-bk`；规划输入提交为 `e75c67f3514d1627ea790d052e2c9226e593aa3c`，代码原基线为 `e8b162076fd8cfa84a11351a82caac8c5ac1f939`。完整执行目标见[执行正文](../execution-objective.md)。

步骤3：已实现[实验格式v1](format-v1.md)、DATA/控制framing writer与独立parser、固定字节向量、全容量预创建文件及必要目录同步。真实FileChannel处理短写后force，失败保留坐标并停止受影响文件写入；容量满额停止。控制文件目前只有身份块，control parser只校验framing。尚无allocator授权、逐entry成功、selector或restart服务。

步骤4的第一部分：`SegmentOwnerLoop` 用固定平台线程承载静态 shard 子集，ledger→shard 只依赖固定逻辑 shard 数，worker 数不进入持久身份。每个已准入批次预留一个 completion 槽，控制队列独立有界；循环每轮处理完成、控制和各 shard 的一个输入。PARK 遵守最老 batch deadline，后来的 deadline 不能推迟它。`SegmentIoExecutor` 在启动线程预创建固定数量服务线程，用有界队列执行阻塞 I/O，并将结果送回原 owner；提交后 handler 异常、停止准入或等待超时不提前释放在途义务。

该循环尚未接入实际 append/allocator 状态机，现已通过下述group启动接入绑定与SYSTEM/OWNER_TOUCH对齐池。容量由构造参数显式给定并从同一总量拆分，不能给每个owner复制全局额度。

步骤4的显式绑定API：现有 `cpu-affinity` 模块新增 `CpuBinding`，只作用于调用平台线程，不调用旧 `acquireCore`，不改 root/CPU online/sibling/文件锁策略。动态 JNI mask 支持稀疏编号（上限65536），设置后读取实际mask核对；绑定失败回滚，关闭只允许原线程执行，恢复不完整明确报错。拒绝空mask、越界、超出初始mask和虚拟线程；提供运行期mask复查。该API已接入隔离group启动；配置接受OFF/PREFER/REQUIRE启动模式，默认OFF。运行期拓扑变化处理仍未完成，不能按完整§10.5模式合同启用生产服务。

步骤4的拓扑与静态placement：`CpuTopologySnapshot` 在缩窄应用线程mask前只读捕获初始mask、online CPU、cgroup v2有效CPU/memory-node集合及可见祖先最小quota，读取package/core/SMT sibling/NUMA信息。不跨socket合并同号core，不一致/缺失信息标未知；隐藏祖先明确不可观测。DATA设备节点只消费调用者已确认的sysfs `numa_node` 路径，未提供或-1保持UNKNOWN，不自动猜测复合设备归属。

`ExecutionPlacementPlan` 按固定逻辑shard映射worker，每个append worker选一个不同物理核的CPU，并排除其整个SMT sibling组给服务线程留余量。显式配置冲突拒绝；quota未知/不足、核不足或物理拓扑未知不生成绑定计划。内存节点不在允许集合时不给pool节点偏好；该计划不设置NUMA策略，也不证明物理页属地。OFF映射不要求native拓扑，CPU/worker变化不改变逻辑shard数。该计划已接入以下group启动；运行期重检查仍是步骤4剩余工作。

步骤4的group启动与池：`SegmentExecutionGroup` 从未缩窄mask的启动线程创建固定owner/I/O线程，在线程入口绑定、分配池并报告ready，全部ready后才开放准入。初始化失败先停止准入、解除启动等待，再join所有已启动线程；pool和binding都在其原线程关闭。PREFER对拓扑/native不可用可退回OFF；绑定阶段失败只有在已验证清理成功且无其他初始化错误时才能重建OFF组，清理不确定不伪报降级成功。REQUIRE启动失败不返回可用group。

`SegmentBufferPool` 用固定数量Netty unpooled direct backing buffer提供4096对齐view，支持SYSTEM和OWNER_TOUCH，均在owner上初始化；OWNER_TOUCH不宣称页必然本地。全局pending/control/buffer数量按owner分摊，direct容量预算包含每块alignment padding；这是请求容量上限，不是整个进程RSS。每个物理buffer预留归还槽，跨线程归还回原池并唤醒owner，不占DATA/control槽。lease携带原池实例、owner和generation，重复归还被拒绝。关闭等待已准入命令和所有live lease；超时返回未终结状态，不释放在途资源。owner排空后才停止I/O服务。

当前startup模式和池尚未构成完整步骤4：运行期CPU/cpuset/memory-node/quota变化的低频检查、PREFER降级/REQUIRE停止准入编排、PREFERRED专有mapping及实际DATA批写接入仍待完成。

## 验证入口

```bash
scripts/unified-wal/run-local-storage-tests.sh IsolatedSegmentConfigTest,SegmentBatchFormatTest,SegmentControlFormatTest,SegmentOwnerLoopTest,SegmentExecutionGroupTest
mvn -o -pl bookkeeper-server -DskipTests spotless:check checkstyle:check
```

脚本为每次运行创建独立临时结果目录，保存HEAD/branch、源码及测试SHA-256、命令、平台、Maven输出、退出码和本次生成的JUnit XML。脚本不覆写历史receipt，不扫描或复制用户PDF。默认离线使用已缓存依赖；首次缺依赖时用 `UW_FETCH_DEPENDENCIES=true scripts/unified-wal/run-local-storage-tests.sh` 联网获取，后续不重复请求snapshot元数据。它通过 `-P '!cargo-zigbuild'` 禁用本实验不使用的Linux Rust交叉编译profile；该入口构建CPU affinity模块；Linux native绑定和拓扑另有下述容器验证，物理NUMA仍未验证。

首次探索性离线构建缺少datasketches/Vert.x依赖；未禁用上述profile的尝试因本机缺少cargo-zigbuild失败。这些是构建失败，不是测试PASS或故障注入结果。

## 当前验证

`unified-wal-local-TrhdAZFy`：20模块构建成功，`IsolatedSegmentConfigTest` 2项通过，0失败/错误/跳过；JUnit耗时0.033秒，Maven总耗时17.896秒（离线构建），两者都不是性能测量。Spotless和Checkstyle通过。source/config/run及原始输出校验见[步骤1回执](step1-receipt.json)。

2026-09-26 `unified-wal-local-Gms0z3fD`：14项通过（2配置、9 DATA/文件、3控制framing），0失败/错误/跳过，重试关闭；Spotless/Checkstyle通过。[步骤3回执](step3-receipt.json)绑定本次源码、命令和结果。覆盖固定向量、N-1/N/N+1、44字节差集、长度溢出、截断/坏CRC/未知类型/generation、短写/force失败/零进展、部分初始化、满额停止和单record读取。

文件测试的可复现工作量：默认DATA预创建2×16 MiB；4096字节批次在每次最多7字节的故障注入下需要586次write及1次force；1024字节payload的record为1220字节，先读160字节header再读record共1380字节，小于整批4096字节。16 KiB文件扣除4096字节身份块后容纳3个4096字节批次，下一批被拒绝。上述为工作量/边界测量，不是性能对照；首份局部性能报告等待步骤5～7的实际闭环。

2026-09-26 `unified-wal-local-Kjwv4bjm`：当前隔离测试20项通过，0失败/错误/跳过，Spotless/Checkstyle通过。[owner循环回执](owner-loop-receipt.json)记录本次源码和范围。新增6项测试验证满额4批仍可完成、控制队列保留容量、多shard轮转、无新输入deadline触发、实际FileChannel I/O线程与owner线程隔离、有界I/O拒绝/错误回传、异常后在途义务保留及100次空队列唤醒交错。I/O测试固定1线程＋1队列槽，第三次提交被拒绝并回原owner，不无限排队。测试工作量不证明CPU或延迟收益。

显式绑定验证见[cpu-binding回执](cpu-binding-receipt.json)。macOS Maven测试11项：10通过，1个Linux原生测试按平台跳过。Linux aarch64容器中以UID 65534、cpuset=`0,2`、quota=1 CPU、512 MiB运行同一Java测试字节码，现场用gcc构建当前JNI：10通过，1个非Linux测试按平台跳过。真实初始/恢复mask均为`{0,2}`，测试验证单CPU绑定、子线程继承、非法native mask拒绝和恢复。稀疏高编号、内核缩小mask、恢复失败和运行期mask变化用kernel boundary模型覆盖，不冒充硬件结果。Spotless/Checkstyle通过。

复现Java/native模块检查（Linux上会执行原生绑定测试）：

```bash
mvn -o -pl cpu-affinity -am -Dtest=CpuBindingTest,CpuInfoTest,IsolatedProcessorsTest \
  -Dsurefire.failIfNoSpecifiedTests=false -DtestRetryCount=0 test
mvn -o -pl cpu-affinity -DskipTests spotless:check checkstyle:check
```

容器的完整编译/测试命令、image ID、classpath与源码/字节码hash保存在回执中。此处是Linux JNI路径证据，不是Linux Maven全构建、NUMA硬件、吞吐或绑核收益证据。

拓扑/placement验证见[本次回执](topology-placement-receipt.json)：macOS 18项中16通过、2个Linux测试按平台跳过；Linux容器18项中17通过、1个非Linux测试按平台跳过，Spotless/Checkstyle通过。固定fixture为2 socket、3物理核、6逻辑CPU，可见祖先quota为3.5 CPU；2个append worker保留1个服务物理核。另覆盖SMT冲突、非连续集合、缺失quota、异常sibling、允许内存节点及mount路径不能解析。实际容器只读结果为CPU `{0,2}`、quota=1、memory nodes `{0}`，测试逐项对照容器约束；不能据此声称物理NUMA或性能收益。

```bash
mvn -o -pl cpu-affinity -am -Dtest=CpuTopologySnapshotTest,CpuBindingTest,CpuInfoTest,IsolatedProcessorsTest \
  -Dsurefire.failIfNoSpecifiedTests=false -DtestRetryCount=0 test
```

group启动与池验证见[本次回执](execution-startup-receipt.json)：2026-09-26 `unified-wal-local-0lK9Qwxl` 为26通过、1个Linux测试按平台跳过；Linux容器运行owner/group测试为12通过、1个非Linux测试按平台跳过，Spotless/Checkstyle通过。Linux fixture固定UID 65534、cpuset `0,2,4`、quota=3 CPU，验证REQUIRE绑定后才初始化池、main线程mask不变和部分初始化失败后全部线程退出且恢复无错误。macOS验证native不可用时PREFER fallback及REQUIRE拒绝。

本次资源测量固定2个owner、1个I/O线程、5个pending按3/2分配、2个控制槽、3个buffer按2/1分配；每个buffer可用4096字节、direct请求容量8191字节，共24573字节，没有按owner或NUMA节点复制总预算。覆盖DATA满额时归还、池耗尽、重复归还、借出buffer阻止提前关闭和返回后继续drain。未测吞吐、页分布或NUMA收益。

## 未覆盖范围

步骤4的运行期拓扑变化处理、运行期PREFER/REQUIRE编排、PREFERRED页策略及存储集成、步骤5条件控制日志/allocator、步骤6逐entry写入/幂等、步骤7定位/pin/restart尚未实现。性能、物理NUMA、掉电、真实网络/MetadataStore和Gate验证均NOT_EXECUTED。配置边界测试不授予任何持久化或生产authority。
