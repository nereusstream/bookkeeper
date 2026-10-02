# Unified WAL Implementation Wave 0

> 状态：**EXPERIMENTAL / NON-PROMOTABLE / NO AUTHORITY / DISCARDABLE**

本目录保存 Unified WAL Implementation Wave 0 的可执行实现边界与机器回执。Wave 0 只把已冻结合同变成 reference implementation、compatibility harness、形式化模型或隔离 prototype；它不授予 stable wire、stable on-disk、live shadow、Segment ACK 或生产注册 authority。

当前已实现的完整模块：

- [`ProfileDescriptor` reference implementation](profile-descriptor/README.md)
- [`Profile control/auth typed reference endpoint`](profile-control/README.md)：已完成interfaces、isolated endpoint、12项普通功能测试、机器回执与最终scope audit；仅消费transport facts与semantic store interfaces，不接入production listener/store/route/Add/ACK。
- [`Bookie startup/readiness/new-scope reference harness`](startup-readiness/README.md)：typed immutable facts、固定fail-closed startup顺序、durable local readiness、persistent CAS/response-loss重读、matching service-info、stale demotion、ephemeral writable registration、new-scope access isolation、九个crash/restart边界、17项普通测试、机器manifest/receipt/test summary与最终scope audit已完成；不提升Spike/Gate/production状态。

已冻结、延期验证的模块：

- [`Experimental Profile wire 与冻结的 old-decoder/stock-binary harness`](profile-wire/README.md)：Block D scope guard、pure codec 与610-vector corpus 已实现；真实4.14.8 decoder发现并冻结原`0x0FFE4250`反例，replacement `0x0FF04250`与authoritative corpus已通过new-codec定向回归。released-decoder/localhost stock-binary矩阵为`DEFERRED_NOT_RUN`，harness与corpus只保留、不进入当前执行，G1保持`BLOCKED_UNVERIFIED`。

输入锁见 [`manifest.json`](manifest.json)。每个模块自己的source/test/artifact范围、复现命令、证据结论和剩余BLOCK由模块目录拥有。实现证据不能自动提升四份owner RFC或三个Spike的状态。

## 下一阶段实施清单

规划基线为`origin/turbo-bk@e8b162076fd8cfa84a11351a82caac8c5ac1f939`，2026-09-22核对本地HEAD及远端引用一致；该提交时间为2026-09-22 08:29:33 UTC（北京时间16:29:33）。16步顺序现已开始实施。步骤1隔离配置、参数边界测试和可追溯构建入口已交付，2项测试通过；[本地存储状态与回执](local-storage/README.md)记录真实覆盖。步骤2的[CLIENT-1/2独立Classic补丁](client-maintenance/README.md)均已完成确定性红绿回归（当前85项测试通过），与隔离存储authority分离；步骤3已交付实验格式、独立parser与真实FileChannel I/O（14项定向测试通过）；步骤4已交付固定owner/PARK、DATA/控制独立completion及有界I/O服务份额、显式绑定、拓扑/placement及group启动与SYSTEM/OWNER_TOUCH池，低频运行期CPU/cgroup检查及PREFER/REQUIRE本地处置已接入隔离group；步骤5～7已有单在途Arena控制驱动、DATA真实write/force到连续物理候选的隔离驱动、重复payload真实点读比较、点读/文件同步、重启物理清单、单stream序号候选检查及新owner有界物理热目录重建局部切片。Bookie控制、请求级幂等、逐entry成功、完整restart、Bookie生命周期重启/PREFERRED及步骤5～16完整合同仍未完成。文件层验证不表示entry成功、restart、Spike或Gate通过。

继续复用已有ProfileDescriptor、Profile control/auth、startup/readiness reference implementation及experimental wire codec/corpus。它们提供接口、字节合同和测试依据，不能替代真实文件持久化、网络或MetadataStore。固定owner、安全显式绑核、NUMA池与独立等待策略沿用RFC-0005 §10.2—10.5、RFC-0003 §6.2；默认仍为OFF/SYSTEM/PARK。

CLIENT-1/2和UW-1～UW-7是既有职责分类，下面的步骤1～16只表示施工顺序，不新增RFC、Maven模块或另一套工作项体系。尤其不能按UW编号机械施工：UW-1的首版集群删除依赖UW-6的基础fenced-close。各阶段测试、故障注入和测量随模块交付，最后一阶段负责综合验收。

| 工作项 | 实施与完成条件 | Owner / 验证入口 |
| --- | --- | --- |
| CLIENT-1 ACK状态与切换顺序 | 复用现有completion gate，remote phase结束后继续阻塞callback；全部受影响pending/被替换slot映射及ACK/故障域状态更新、资格重算后才恢复连续回调。保留未变identity/incarnation的有效ACK，只重发必要副本；兼顾原残留集合、timeout统计及policy关闭成本 | [RFC-0001 §9.3/9.5](../RFC-0001-profile-capability-install.md)、[Spike A A29](../spikes/SPIKE-A-profile-install.md)、Model A-ACK-RESP；4/3/2先处理write-set外slot不提前成功 |
| CLIENT-2 先修对象回收 | 旧地址响应分支补安全maybeRecycle；最后旧响应/当前响应/callback未完成均不遗漏、提前或重复回收 | RFC-0001 §9.5、Spike A A30 |
| UW-1 普通删除竞争 | 初始/写期activation authority发布时持久留存target/incarnation，并与delete共享lifecycle cut；同起点membership覆盖、snapshot压缩/restart不丢清理目标。完整target/tombstone后普通logical success；delete-applied cursor/rejoin不等drain/compaction/free，屏障和physical结果独立 | [RFC-0001 §6.3/9.1](../RFC-0001-profile-capability-install.md)、[RFC-0004 §10—14](../RFC-0004-range-recovery-delete.md)、A9/A10/A16/A28、B4/B6、[Spike C D-RACE/D-LOGICAL/D-ACCESS/CD-REUSE](../spikes/SPIKE-C-no-object-tla.md) |
| UW-2 普通Add与identity | 保留CRC32C、统一maxPayload/N边界、读路径分层、逐entry结果及unknown身份，收紧新写不损害旧entry读/恢复。普通/合法恢复Add由内部设置ENTRY_DATA；存储来源不证明ACK，也不能由客户端伪造relocation身份。重启幂等成功仍须有效durability证据或恢复同步，完整status/CLIENT-1 gate保持 | [RFC-0001 §8/9/11.6](../RFC-0001-profile-capability-install.md)、[RFC-0003 §9](../RFC-0003-segment-storage-allocator.md)、[RFC-0005 §6.1/8/10.1](../RFC-0005-segment-bookie-state.md)、A26/A27/A29/B2/B9/B18/B19 |
| UW-3 最小真实写入路径 | 保留固定shard/shared block/预分配及entry的barrier→物理前缀→热定位→成功。framing持久区分ENTRY_DATA/RELOCATION_COPY及move/target身份；同一I/O的copy完成只进内部MOVE路径，不直接发布普通定位、不覆写提交位。网络固定header后预留额度再收大body；DATA准入仍保障提交/定位工作集，Arena命令仍合批durable后顺序apply；固定owner＋PARK＋安全binding/拓扑计划随第一批线程模型完成；阻塞I/O走有界服务线程，completion回原owner且预留容量，CPU/worker数不改变持久shard/stream身份。 | RFC-0001 §11.6、RFC-0003 §5.3/6.1—6.2/9/15、RFC-0005 §5.2/6.1/10.1—10.5、[Spike B B2/B3/B9/B10/B16/B18](../spikes/SPIKE-B-allocator-block.md) |
| UW-4 allocator恢复与复用 | 先验证checkpoint/完整后缀并重演metadata predicate，再验证最终selector及未完成操作仍需DATA；合法退出的历史MOVE副本不再要求存在，不因日志完整就判所有命令成功。必要恢复同步后开放服务，C坏/后缀缺失仍隔离且不回退旧位置。FREE前受保护回收/gate、applied checkpoint cut及原共享文件/reuse/compaction合同保持，不强制每FREE一次checkpoint | [RFC-0003 §5.3/10—12](../RFC-0003-segment-storage-allocator.md)、B2/B3/B4/B6/B7/B9/B10/B16、C-CKPT/C-COND/C-TAIL/C-REUSE/C-MOVE、AC-LOCAL-STORE |
| UW-5 空间耗尽与进展 | 保留Arena内部/Bookie filesystem双层预算和维护份额；按有界victim组计算释放source减实际destination占用，计framing/padding，跨source复制/group commit可合批。低水位无获益候选时背压，等待真实删除/容量恢复，不反复搬近全live块；不靠hole punch或收缩解围 | RFC-0003 §10/13.1/14/15、B10/B17/B18、C-SPACE；copied bytes与net reusable bytes gained分列 |
| UW-6 最小集群可恢复性 | 实验3/3/2与3/3/3、Bookie故障域F=1；复用Classic write-set/fencing/W-A+1/顺序write-back及逐来源否定条件。保留提示源entryId/LAC/累计length，取得匹配最终P的L后发布并重读核对CLOSED(P,L,context)；空ledger为(-1,0)，缺锚点defer、矛盾conflict，必要时只补边界点读。恢复sync不推导AQ/LAC | [RFC-0004 §7.3—7.5](../RFC-0004-range-recovery-delete.md)、RFC-0005 §6—8、A16/A26/B19、A-POINT/D-LOGICAL |
| UW-7 全量重建与总资源 | 全量重建由最终控制状态及受校验来源选择位置，未发布copy不因index/task丢失提升；保留hot-first/fresh Get与真实persisted coverage。TLS/header/read、半包/已解码未准入请求加入既有总账，交接credit不漏计/永久重复；同Profile listener保留有界独立控制连接及接收/执行份额，DATA暂停仍可达且不豁免认证/总量。原内存/双层磁盘及完成工作集保持；接入NUMA属地池初始化/首次触碰/原池有界归还；CPU quota、服务余量与内存同一边界内计量，affinity/NUMA降级不改变存储事实，B18分离pin/池/spin收益。 | RFC-0001 §11.6、RFC-0003 §6.2/9/12/13/15、RFC-0005 §8/10.1—10.5、A26/B2/B3/B9/B10/B11/B12/B17/B18、C-REBUILD，dedicated仍另验B5/B13 |

## 实施顺序

主线按“基础与执行模型 → 单机可恢复存储 → 索引与回收 → 协议与集群 → 完整验收与灰度”推进：

```text
1. 基线、实验范围与测试入口
   ├─ 2. CLIENT-1/2独立Classic维护补丁
   ├─ 3. 实验格式与文件I/O基础
   └─ 4. 固定owner、安全CPU绑定与NUMA缓冲池
                       ↓
5. 本地控制日志、启动检查与allocator
                       ↓
6. 普通Add、合批持久化与幂等
                       ↓
7. 点读与基础restart
   【第一里程碑：真实写入—点读—重启恢复】
                       ↓
8. checkpoint/rotation、异步索引与完整重建
                       ↓
9. 本地tombstone、compaction、FREE/reuse
                       ↓
10. 空间耗尽、长期负载与多Arena验证
    【第二里程碑：有界、可长期运行的本地存储】
                       ↓
11. Profile网络接入与接收背压
                       ↓
12. MetadataStore、安装激活与registration
                       ↓
13. 客户端ACK、重试与写期换组
                       ↓
14. 基础ledger recovery与AutoRecovery
                       ↓
15. 集群删除、离线追赶与rejoin
    【第三里程碑：完整分布式WAL闭环】
                       ↓
16. 综合验收、运维交付与受限canary
```

图示是集成主线。步骤2不阻塞本地文件原型，但必须在步骤13接入对应客户端路径前完成。步骤3与4可并行；步骤11与12可在接口稳定后提前并行开发，真实集群成功路径联调仍等待本地存储通过对应验证。Block H隔离存储与Block G实际启用范围的模型验证继续并行，各自锁定source/config及独立证据。

### 第一阶段：基础与执行模型

#### 1. 固定实施范围、基线和最小测试入口

固定本轮源码与配置、独立实验目录、一个Arena、shared block、固定逻辑shard及第一种真实I/O路径。暂未实现能力显式关闭，不用空方法返回成功。基准配置为：

```text
affinity = OFF
NUMA policy = SYSTEM
wait strategy = PARK
```

在现有测试及benchmark能力上准备小型可复用入口：创建实验ledger状态、追加、点读、终止进程、重启、注入短写和同步失败、读取原始文件镜像。不建设通用测试平台；实际操作随对应模块逐步接通，尚未接通的操作明确拒绝。

**完成条件：** 一条明确命令能构建并运行基线，输入参数、源码版本和结果目录可追溯；不触碰生产目录，不覆盖历史receipt。每次正式比较前冻结格式、硬件、durability、负载、阈值和停止条件，沿用[总体RFC §11](../../../turbo-bk.md#11-gate-与证据合同)的1/4/32 KiB、ledger规模、负载与quorum矩阵。

#### 2. 完成CLIENT-1/CLIENT-2独立维护补丁

依赖步骤1的基线与测试入口，复用`LedgerHandle`、`PendingAddOp`及既有测试，拆成两个小变更。合同及反例见[RFC-0001 §9.5](../RFC-0001-profile-capability-install.md)、Spike A A29/A30。

CLIENT-1先构造失败测试，再修复ACK集合、故障域状态和多slot整体completion gate。全部受影响pending/slot的映射、ACK撤销和资格重算完成后才恢复连续回调；身份未变的有效ACK保留，只重发必要副本。固定4/3/2下先处理write-set外slot的顺序，并交错新旧响应，不以最终集合正确代替中途无提前callback。

CLIENT-2修复最后旧地址响应到达时的回收路径，覆盖callback已完成/未完成、最后响应来自当前/旧Bookie的组合。回收后立即返回，不再访问已重置字段；现有timeout/callback同步保护保持。

**完成条件：** 确定性反例在修复前失败、修复后通过；没有重复回调、提前释放、漏回收或重复回收。检查timeout统计及正常单条Add的分配、CPU开销；未启用故障域检查时避免无必要集合成本。这两项属于Classic维护，不接入Segment、不扩大为客户端整体重构，也不借Wave 0证据取得新存储ACK authority。

#### 3. 实现实验格式与文件I/O基础

依赖步骤1，可与步骤2、4并行。在同一变更中交付实验格式说明、writer、独立reader/parser及固定字节向量，确定控制与DATA record的framing、长度、generation、checksum覆盖范围，持久区分`ENTRY_DATA`/`RELOCATION_COPY`，并明确batch边界、alignment、统一entry上限、溢出检查及superblock/checkpoint/日志的最小识别规则。

I/O层先选择一种明确后端，完整写入并处理短写，再执行对应持久化屏障；提供有限错误注入点。先用固定数量的预创建DATA文件，容量达到实验上限就停止，不自动破坏性回收。不同时开发普通文件、O_DIRECT、io_uring三套后端。

**完成条件：** 独立parser能读取writer输出；截断、坏长度、未知类型和错误generation都有确定结果；任意短写或同步失败都不能当作成功。文件创建及必要目录持久化纳入对应I/O合同。此时格式可随实验修改，数据可丢弃，不提前建设长期迁移框架或宣称稳定兼容。

#### 4. 实现固定owner、安全CPU绑定与属地缓冲池

依赖步骤1，与步骤3并行，随后共同接入写入。按[RFC-0005 §10.2—10.5](../RFC-0005-segment-bookie-state.md)、[RFC-0003 §6.2](../RFC-0003-segment-storage-allocator.md)分三个小交付：

1. 固定`ledger → logical shard → worker`，使用常驻平台线程，一个worker可承载多个shard。默认PARK，同时处理输入、completion、控制通知和buffer归还；新DATA不能饿死完成事件，多shard轮转不重置最老batch deadline。
2. 在现有`cpu-affinity`模块扩展拓扑读取、静态placement及只绑定自身线程的显式API，验证实际mask、部分启动失败清理与线程继承。不调用原`acquireCore`自动抢核/下线sibling策略，不改变Classic旧行为。
3. owner绑定验证后才初始化、首次触碰自有对齐池；先SYSTEM和OWNER_TOUCH，再接可选PREFERRED。buffer始终回原池，归还容量有界且预留；每个NUMA节点不能各复制一份全局预算。CPU/worker数变化不改变持久shard/stream身份。

**完成条件：** OFF与绑定模式使用同一存储逻辑；核不足、非连续cpuset、socket/SMT拓扑、native不可用、线程继承mask、部分启动失败及跨节点归还都有测试；无丢失唤醒，completion队列满载仍可推进。PREFER降级和REQUIRE失败遵守原合同；隔离owner运行期placement check抛错时停收并排空已受理I/O及排队ticket已有定向回归，NUMA受允许memory-node集合约束，不能把设置preference当作页必然本地化。

BOUNDED_SPIN最后加入并单独测量，不作为步骤5～7开工条件。亲和性从线程/池构建时就有正确接入点；未测得收益时保持OFF/SYSTEM/PARK。

### 第二阶段：单机可恢复存储

#### 5. 实现本地控制日志、启动检查与allocator

依赖步骤3，执行资源使用步骤4。保持两个语义owner，底层framing、文件I/O和group commit可以复用：

```text
Bookie控制日志：route / credential / install / activation
                fence / recovery grant / explicit LAC / tombstone
Arena控制日志： ALLOC / ALLOC_POOL / generation / MOVE / FREE / checkpoint
```

先实现真实控制记录持久化、顺序apply、重复操作解析、启动身份检查及ALLOC/ALLOC_POOL。条件命令按已选合同“合批durable → 顺序判断并apply”，不增加推测状态或结果日志。初版可完整重放且暂不删除旧日志，但必须设置日志空间上限，满额停止接收；完整checkpoint/rotation在步骤8补齐。

当前[隔离本地切片](local-storage/README.md)实现Arena ALLOC/ALLOC_POOL的文件持久化、stream首序号约束、单在途驱动、顺序apply和完整历史重放；重放状态可报告每文件成功授权block的最高结束位置，全零已授权block仍计入，但尚不能判定pool used/unused。定向真实文件测试已在DATA准入及I/O服务满额时通过独立控制份额完成force与原owner apply。Bookie控制文件现有INSTALL/ACTIVATE的有界批量force、顺序apply和重开重放，INSTALL只建立inactive route，独立ACTIVATE后才开放局部normal准入查询；FENCE受控入口先关闭新准入；在已接线的隔离DATA路径中，票据待实际I/O与owner回调均终结后才释放，随后force并持久拒绝普通写。受控TOMBSTONE先切断匹配实例normal准入及在途成功资格，再force并建立不可逆terminal route；重放后迟到激活仍被拒绝，同一DATA force中其他ledger可独立成功。局部读票据不依赖normal-active且不被fence撤销；pending/durable tombstone阻止新读，并使尚未判定结果的旧票据失败。两个控制日志现在可在Bookie文件追加后共同重开并重放fence/tombstone；Arena对对方控制文件只检查身份头和最小长度。显式DATA文件写入路径检查整段grant，停写后的只读扫描验证framing与grant。Bookie控制owner驱动已将INSTALL/ACTIVATE及FENCE的write/force交给预留控制I/O服务，durable后回原owner apply；隔离explicit LAC也已按exact route/credential与单调值执行独立write/force、顺序apply及重放，现可经控制I/O预留、force及原owner apply，执行前拒绝不跳过sequence；fence等待前序barrier，同值同body可解析响应丢失；完整请求级local result及完整read权限/selector/pin、delete-applied cursor和清理义务、实际服务生命周期及pool used/unused恢复尚未接通，步骤5完成条件仍未满足。

Bookie控制文件新增隔离 `RECOVERY_GRANT` 持久记录和重放：public payload identity绑定RepairIntent/目标/范围，局部准入只接受匹配的 `ADD_RECOVERY` tuple，tombstone撤销票据结果资格。grant与close现经预留控制I/O服务write/force，回原owner顺序apply；close先切断新准入、等待已准入票据终结，再持久并重放局部committed-readable控制事实。recovery票据现可绑定完整wire entry进入真实DATA force/前缀/热定位；受控点读比对解析运行期同坐标重试并拒绝不同payload。裸的重启扫描候选不直接报告durable duplicate；当前无MOVE/FREE的单stream受控物理候选比对另在步骤7验证，尚不授予durable duplicate成功。真实RepairIntent冷读授权、网络生命周期和完整恢复验证仍未接线，步骤5未完成。

控制日志force已移出授权状态锁，受控慢force不阻塞已apply grant查询；DATA文件write/force也移出范围预留状态锁，以同文件独立锁串行。普通Add、逐entry结果和完整DATA owner链路尚未闭合。

**完成条件：** 同批两个ALLOC竞争同一slot只有一个成功；durable未apply、apply中途崩溃和重复请求的结果一致；错误storage identity不开放写入；DATA只使用已持久化且成功apply的空间授权。本地fence/grant先用受控DATA事件验证顺序，在步骤6补真实I/O交错测试。

#### 6. 实现普通Add、合批持久化与同坐标幂等

依赖步骤4、5，对应UW-2/UW-3。真实数据路径为：

```text
统一大小与权限检查 → 取得输入、提交和完成资源 → owner检查/占用坐标
→ 合批并冻结DATA → 有界I/O backend写入与同步 → completion回原owner
→ 推进连续物理前缀 → 发布热定位 → 完成逐entry的local result
```

同时实现相同坐标相同数据重试、不同数据冲突、合法LAC封装变化、提交后timeout/取消及I/O unknown。保持CRC32C identity与统一N边界，收紧新写阈值不能损害已接受entry的读取/恢复能力。正常Add不访问MetadataStore，不逐entry写控制日志。

**完成条件：** 对应durability未完成绝不成功；源/对齐buffer及pending有界，完成事件不被DATA队列满额挡住。覆盖同批不同ledger的逐entry结果、L1 fence等待同批已准入的pre-cut Add终结后才durable且不阻断L2、batch 12先于11完成、DATA durable但定位尚未发布、请求超时后旧I/O继续完成；未解析I/O保护原坐标并暂停受影响stream/file，不以跳号或新stream绕过。当前采用drain pre-cut；只有另行启用显式终止pre-cut Add的路径，才验同批L1被fence终止、L2成功。此时local result只供隔离测试，不接管生产Bookie ACK。

当前[隔离物理驱动与热候选切片](local-storage/README.md)已将DATA坐标预留、真实I/O write/force、原owner连续stream前缀及有界可点读候选接成一条调用路径；未终结的文件预留阻止DATA文件关闭，I/O服务在执行前拒绝时仅终结预留计数而不回退坐标；一个携带两条不同entry的冻结batch在隔离group中一次force后可点读两条，失败向未决回调报错且不发布候选，搬迁副本不进入普通候选。受控normal批次先核对每条ledger的固定shard，候选发布后、票据释放前再按entry核对捕获的Bookie准入generation：仍有效者返回隔离`LOCAL_DURABLE`，过期者返回`STALE_ADMISSION`，物理失败返回`UNKNOWN`；不同ledger同批独立判定，失败entry的物理候选仍可保留。重复检查在原owner发现同坐标normal在途时，相同逻辑内容持有票据等待其物理结果，不同内容返回冲突；原批次失败使等待者得到`UNKNOWN`。已发布的运行期候选仍经有界I/O服务真实点读，完成时复核准入与候选；重启扫描先拒绝错shard记录；其候选只返回`NOT_READY`，不据此报告absence或durable成功。normal与重复请求提交后停止响应等待已有隔离真实文件切片，但尚无网络deadline/断连接线及完整请求生命周期、整体bytes预算、可恢复末端前缀、完整read selector及生产ACK，步骤6未完成。

2026-10-02 提交buffer切片已将现有owner对齐池用于真实DATA：`encodePooled`先验证完整大小再取得固定lease，池满或尺寸不足立即背压；payload集中复制后冻结，driver持有至真实I/O与owner结果处理均终结。停止响应等待、前批失败导致后批提前返回`UNKNOWN`均不提前复用buffer；验证失败、owner拒收和未启动I/O的拒绝会归还原池。封包切片自身不覆盖接收输入；同日的新输入入口见下，TLS/socket、重复点读及索引工作集总预算仍未闭合，步骤6仍未完成。

2026-10-02 普通entry输入已由执行组共享bytes额度先预留完整容量再分配，传给owner前核对统一大小/CRC、context和shard。输入转换与物理I/O共用原owner/completion ticket，复制到固定对齐池后才释放源；取消等待不提前释放。池不足返回隔离`TRANSIENT_UNAVAILABLE`且不占物理坐标，owner拒收、验证失败及排队后owner故障释放已认领输入和Bookie票据。该显式入口仍由调用方提供batch集合和授权地址；其后的逐entry自动入口见下，接收/存储credit交接仍未完成。


2026-10-02 固定driver/shard新增`NormalBatcher`逐entry入口，按framing bytes/count及最老单调deadline冻结非空batch，读取已apply grant/当前stream游标/下一序号，继续经原prefix/file入口重验与预留。沿用每条entry的完成ticket，不新增队列；shutdown封包已接纳输入，owner/回调故障仍排空资源。启用前校验固定源/池可容纳统一上限，合法大entry超过普通batch阈值时单独封包，1 MiB payload已跨同allocation两个block真实写入并点读。assembling/已冻结/已发布的同坐标重试分别比较、等待或点读；相同内容不新增DATA force，不同payload返回隔离`CONFLICT`。按需refill进展见下；grant尾部回收、网络credit/timeout交接、全读取/索引预算及服务级restart仍未完成，步骤6不标完成。

2026-10-02 pool接续使用文件层按shard/generation的单调游标，独立stream可按任意先后使用同文件各自的已apply pool；非空batch从最早能容纳整批的同stream grant选地址，短尾或其他owner范围跳过，不跨不同allocation拼包。预留后游标不回退，已跳过范围不复用；重开仍跳过全部旧授权块。运行期空间接续已接入自动合批，按需异步ALLOC_POOL refill进展见下，used/unused恢复与尾部回收仍未完成。

2026-10-02 逐entry自动路径已接入实际Arena控制owner的有界按需ALLOC_POOL refill：dispatch前合并每shard一个pool请求，一次control force后顺序apply，原DATA ticket回原owner继续封包并重验grant。等待期间源/票据有界且不占DATA坐标，新坐标背压，同坐标重试可加入；全部容量不足不写空control batch、不force或占DATA序号，控制/owner失败与shutdown可排空。普通pool内Add不访问控制日志。预取策略、used/unused恢复、尾部回收、网络/全局bytes总账及完整服务restart尚未完成，步骤6仍不标完成。

#### 7. 实现点读与基础restart

依赖步骤5、6。点读实现正确定位、allocation/generation pin、独立record校验和有界对齐读取，不为小entry强制读取整个DATA batch。restart顺序为：

```text
验证storage与控制文件 → 重演控制状态 → 枚举当前合法DATA
→ 验证当前必要记录 → 完成必要恢复同步 → 重建定位 → 开放对应实验服务
```

尚未接RocksDB时，可在声明的小规模范围内使用有界定位结构，满额明确背压；不得用持续增长的内存Map得出长期性能结论。

**完成条件：** 写入→确认本地持久化→读取→终止进程→重启→再读与重复写入可重复运行；未知覆盖不误报NoSuchEntry，不同stream恢复不截短共享文件，旧generation残留不复活。开始验证两次故障：write完成但barrier未完成时退出，重启处理重复请求后再次掉电；单次进程终止后可读不足以证明持久化保证。

当前[隔离点读及文件同步切片](local-storage/README.md)已用真实文件核对Arena grant、batch/record身份及CRC，按对齐范围读取单条record；较低新写入阈值重开后旧entry仍可读。停写后完整扫描的DATA文件可执行一次恢复`force(false)`；全目录校验同步后可在显式容量内取回普通record物理位置，并先以Arena已apply首序号核对调用方起点，再仅对匹配shard与generation的单stream batch/entry检查序号连续性；同文件其他stream按Arena授权块跳过framing解析，但仍完成文件身份检查与文件级同步；目标stream损坏、缺口、重复和容量不足不返回候选。全量清单仍校验整个DATA文件；隔离回归证明旧stream结构损坏不阻断另一stream物理候选。运行时热候选可发布，调用方选定的多个恢复物理stream候选可在新owner的有界热目录整组核对scope、容量和重复坐标后原子装入并点读，失败不发布先通过的stream；隔离测试还覆盖normal DATA真实force与durable fence后关闭、重开双控制日志并从同步候选点读；局部点读现可持Bookie读票据核对安装实例，在fence后继续读取，并于tombstone前后复核本地route；只读stream恢复也可在完整校验目标batch后，以重放的Bookie本地route排除已tombstone实例的定位容量。当前无MOVE/FREE的单Arena受控recovery重试可在只读重开后要求单一stream、连续前缀、DATA恢复同步、唯一坐标和重放的exact grant，点读比较后复核权限，仅返回物理候选比对结果；旧DATA I/O错误尚无持久分类，不能把该结果提升为durable duplicate成功。文件层仅有物理append-only重开；完整current-selector/pin、全部必要DATA选择与服务权限尚未接通，步骤7未完成。

受控normal重试也可持普通Bookie准入票据，在真实normal DATA得到隔离本地结果并重开双控制日志后，同步并比较唯一物理候选；相同payload/累计length与不同payload分别只返回匹配或冲突**候选**。同步失败释放票据，同步期间tombstone撤权返回`STALE_ADMISSION`。该入口不向DATA driver恢复热定位，不把任一候选提升为最终Add结果；完整selector和旧I/O结果证明仍缺失。

普通Add另有真实子进程回归：分别在本地结果之后和DATA barrier之前`Runtime.halt`，父进程重开双控制日志，对目标stream完成恢复同步，再持Bookie读票据点读和普通写票据比较候选。这验证普通路径跨进程恢复的物理证据，不模拟掉电，也不返回重启后的durable duplicate结果。

当前无MOVE/FREE且目标shard只有一个Arena stream时，隔离恢复点读可从已重放的Bookie本地route取得读票据，在DATA writer排他锁内完成连续stream扫描、必要同步、唯一坐标选择和独立record点读，再复核route与stream。普通/recovery重复候选比较复用同一受锁保护的选择过程；无唯一位置或出现第二个stream时返回未就绪而非确定不存在。该受限选择不包含一般current selector、allocation pin、网络读授权或可写restart。

隔离recovery DATA新增真实子进程边界测试：本地持久结果后或完整写入但barrier前直接终止进程，父进程重放双控制日志、执行DATA恢复同步并点读候选。它验证进程中断与只读恢复入口，不模拟掉电，也不将物理比对提升为durable duplicate或生产ACK。

另一个两次`Runtime.halt`回归验证进程重启后的物理追加：新进程同步旧DATA并跳过旧授权块零尾，在新grant下写另一stream；再次终止后父进程分别点读两条物理stream，旧recovery重复请求仍因同shard多stream保持`NOT_READY`。此回归不完成服务级可写重启或第二次掉电验证。

扫描的零尾页不能作为未提交或可复用证明。隔离文件层现可完整扫描并同步旧DATA后，只在全部已apply授权块的最高结束位置之后重开**物理追加**；旧块零尾全部跳过，新写仍须新apply的Arena grant。该句柄拒绝无授权原始写入，也不能构造会返回local result的DATA driver。必要DATA选择、持久空间used/unused与末端前缀事实、旧I/O错误及终结未闭合前，不开放完整服务级可写重启。

该物理重开现可选在同一次writer锁下保留容量有界的全目录普通DATA清单，并由原owner原子装入热目录；旧坐标只在经过该目录的准入路径中占位，低层物理写入尚未接入。同次扫描还可在显式stream容量内核对Arena持久起点和每个已观测stream的连续物理序号，缺口/重复拒绝该stream候选；空stream报告空前缀。重开句柄的有界读取pin允许在持续持有writer锁时，从单一Arena stream与重放Bookie exact读route中选择该清单的旧entry并点读，关闭须等待pin退出；fence后仍可读，tombstone后拒绝。恢复定位仍无local-durable标记。DATA锁入口先拒绝本进程同scope重入，再打开锁文件channel，避免失败关闭第二个channel释放原writer的POSIX锁。任一容量不足或恢复同步失败不返回清单/句柄；此受限本地读取不能替代旧I/O错误、一般selector或Profile网络读授权证明。

**第一里程碑：真实写入—点读—重启恢复。** 步骤5～7完成后，已有真实文件I/O和可重复的基础恢复测试，可出第一份局部性能报告。无真实网络的结果不称端到端提升，无长期索引/回收的结果不称可持续吞吐；按Spike切片声明实际覆盖范围。

### 第三阶段：索引、回收与长期运行

#### 8. 完善checkpoint/rotation、派生索引与完整重建

依赖步骤5～7，先控制checkpoint，再派生索引，分开提交。先完成Bookie/Arena checkpoint的applied cut、A/B发布、fallback依赖及旧控制日志回收；不能把日志fsync最大序号当作已apply位置。

随后接入真实RocksDB派生索引：

```text
DATA durable → hot locator → local result
                     ↓ 异步
              RocksDB WriteBatch
                     ↓
              flush实际覆盖内容
                     ↓
              durable coverage checkpoint
```

实现hot-first、miss后fresh Get、query-visible后淘汰hot，以及Add/MOVE/delete的必要顺序。只有真实持久覆盖才允许restart跳过对应DATA扫描。纯派生索引可关闭WAL，但必须同时实现coverage与DATA重建，不能只修改RocksDB参数。

**完成条件：** Put成功但未flush时崩溃不虚报覆盖，删除全部派生索引后能完整重建；index stall只通过预算背压新写，不阻塞append owner；checkpoint任一发布边界崩溃都能选择合法状态。query-visible只授权hot交接，不冒充persisted coverage。

#### 9. 实现本地tombstone、compaction与FREE/reuse

依赖步骤8，对应UW-4/UW-5。按本地tombstone禁止新访问→整块全死直接FREE/reuse→部分存活块compaction→多次搬迁/orphan/并发回收推进。搬迁复用DATA I/O，但使用独立内部完成分支：

```text
写RELOCATION_COPY → 副本durable → MOVE命令durable且apply成功
→ selector/pin cutover → 旧读者与I/O结束 → 整allocation FREE
```

本地测试通过受控授权入口触发删除，不代表集群删除协议已完成。按有界victim组的实际净收益选候选，扣除destination占用及framing/padding；合批复制和控制持久化，不为每条moved entry单独fsync，不要求OPEN ledger先关闭。

**完成条件：** 部分ledger删除不提前释放共享块；copy durable而MOVE未成功不成为当前数据；A→B→C且A/B已复用、没有新增checkpoint时仍能恢复C，C损坏或必要控制后缀缺失仍隔离；并发MOVE/FREE不产生两个winner，旧I/O不覆盖新owner。FREE以整allocation generation可回收为条件，不以单ledger删除原因代替。

#### 10. 完成空间耗尽、长期负载与多Arena验证

依赖步骤8、9，在真实压力下验证已有预算，不另建资源框架。覆盖Arena空闲块、filesystem余量、SST/compaction临时空间、控制checkpoint空间、内存和completion工作集。暂停再恢复下游，验证无循环等待、不突破上限、已准入请求可完成且资源归还，随后自动恢复准入；无净收益候选时背压，不反复搬近全live块。

单Arena读写回收稳定后加入第二个Arena，验证同Bookie的route/fence/tombstone一致性、required device部分失败与共享filesystem预算，不实现跨Arena搬迁。

亲和性沿用B18的分离对照，worker数、batch、I/O、TLS范围、E/W/A和维护负载保持相同：

| 对照 | 配置 |
| --- | --- |
| A | 固定owner，OFF / SYSTEM / PARK |
| B | A加CPU pin |
| C | B加NUMA属地池 |
| D | C加有界spin |

报告实际mask、页分布、CPU/entry、空闲CPU、迁移/queue wait、p99.9与quota throttling，不一次全开只报总提升。维护测量分列copied bytes和扣除destination后的net reusable bytes gained，并关联debt/p99；同时记录恢复sync的文件/次数/等待、control commands/barrier与apply lag、必要恢复边界点读数、index coverage、拒绝/retry/replacement及内存/磁盘预算。

**第二里程碑：有界、可长期运行的本地存储原型。** 持续create/write/read/delete下资源有界、有净回收、背压可解释，通过完整索引重建和所需多Arena测试；尚不代表分布式quorum、集群删除或协议兼容完成。

### 第四阶段：协议与集群生命周期

#### 11. 实现Profile transport adapter与接收背压

依赖步骤3的格式/codec、步骤4的执行模型及步骤6、7的真实接口，可在接口稳定后提前独立开发。复用reference codec/corpus，增加ByteBuf adapter、TLS/HELLO、operation分发、完整status三元组及DATA/control连接分工。

预算从socket/TLS/header/半包开始，固定header通过后先预留body额度，再向存储转移ownership；暂停前pipeline已持有的字节仍计账。控制连接使用总预算内有限保留份额，不排在暂停DATA连接的半包后，也不豁免认证。

同一listener按首个非HELLO固定头选择候选用途，不增加wire字段。连接先占有独立、有界的未分类准入、TLS与接收份额，DATA须在大body前取得DATA额度；控制请求完成实际operation、context、credential及授权校验后才取得保留控制连接名额，后续用途固定。未分类池耗尽时新控制连接无法被预先识别，验收分别检查DATA背压下新控制连接可建立，以及未分类池耗尽时资源有界、已有控制连接继续推进。TLS、HELLO和首请求等待分别限时；跨分类的同次read残留字节继续按原归属计费，长期工作集换账必须原子检查。

**完成条件：** 正常、截断、超长、错误subtype与reference corpus一致；多连接半包内存有界，关闭/取消/解析失败无泄漏；DATA暂停时控制仍可到达；普通Add无逐请求handshake或proof读取。测试可使用隔离listener，尚不发布stable wire承诺。

前置的[Classic master-key错误日志泄漏](profile-control/classic-master-key-log-receipt.json)已用真实日志事件红绿回归修复。隔离Profile明文接收切片现可在完整body到达前校验固定header并申请连接/Bookie接收额度，计入同次read中已进pipeline的字节、半包与交付后尚未释放的frame；错误subtype、容量不足、非法HELLO body及断连释放已定向验证。完整HELLO在交付前复用`ProfileHello.decodeClient`，小额解析副本单独计费。`EmbeddedChannel`联测验证真实TLS1.3/mTLS握手后的HELLO与DATA帧通过解码器；新增localhost NIO socket测试使用带SAN的服务端测试证书，验证客户端主机名校验、双端TLS握手、服务端提取已验证的客户端证书身份、HELLO与DATA framing，以及`channelInactive`后连接名额和明文额度归零。复现命令：`mvn -o -P '!cargo-zigbuild' -pl bookkeeper-server -am -Dtest=ProfileFrameCodecTest,ProfileHelloStatusTest,ProfileWireCorpusTest,ProfileReceiveDecoderTest,ProfileTlsContextFactoryTest,ProfileInboundPipelineSocketTest -Dsurefire.failIfNoSpecifiedTests=false -DtestRetryCount=0 test`；本轮common 13项、server 17项通过。

接收器现已补充下游同步关闭后的同次read终止，以及`FENCE_LEDGER`固定100字节长度的额度申请前校验。三份额明文预算使每条连接先占未分类额度；首个非HELLO固定头将DATA基础额度原子转账，控制候选保留未分类额度，待下游对该frame显式报告授权后原子转入保留CONTROL额度。`FENCE_LEDGER`虽在实验wire枚举中属于DATA，接收用途上仍归控制；已分类的DATA连接在后续fence固定头阶段拒绝，不接收其body。跨HELLO/首请求的同次read残留字节不随分类改账；转账失败保持原账，待frame实际释放才归还。`ProfileInboundPipeline`先取得未分类连接名额再创建`SslHandler`，连接名额与明文额度在同一账本中跨用途原子交接。握手、HELLO和首请求分别设10/10/30秒绝对期限；后续帧从首字节起设不续期的30秒接收期限，完整交付后取消，空闲连接和已交付frame不受该期限约束。未授权的首个控制frame后收到下一帧字节即关闭，不能通过流水线半包延长未分类占用。虚拟时间测试覆盖慢HELLO、首请求及后续DATA/CONTROL半包不续期、名额耗尽在TLS安装前拒绝、关闭后释放及重新准入。这里的下游授权标记是测试注入，尚无真实operation/credential授权；管线也未接入Bookie listener，TLS/socket实际buffer计费和存储接管未完成，不能算作B18控制可达性验收。

2026-09-29 执行`mvn -o -P '!cargo-zigbuild' -pl bookkeeper-server -am -Dtest=ProfileReceiveDecoderTest,ProfileInboundPipelineSocketTest -Dsurefire.failIfNoSpecifiedTests=false -DtestRetryCount=0 test -q`：接收器18项与隔离socket 1项通过，0失败/错误/跳过；`bookkeeper-server` Spotless/Checkstyle通过。此范围仅验证明文计费、定时器与隔离TLS socket路径，不证明真实listener或完整TLS/socket内存计费。

同日修正已分类DATA连接后续接受`FENCE_LEDGER`的问题：在固定头阶段与首请求分类共用控制用途判断，未接收body即关闭并释放额度。`mvn -o -P '!cargo-zigbuild' -pl bookkeeper-server -am -Dtest=ProfileReceiveDecoderTest -Dsurefire.failIfNoSpecifiedTests=false -DtestRetryCount=0 test -q`执行19项，0失败/错误/跳过；`mvn -o -pl bookkeeper-server -DskipTests spotless:check checkstyle:check -q`通过。该回归只证明接收用途隔离，不证明真实控制授权或Step 11完整预算。

现有`TLSContextFactory.forProfileServer()`单独复用Bookie证书/信任库配置，固定`startTls(false)`、TLS1.3与`ClientAuth.REQUIRE`，Classic默认工厂行为不变。握手成功后可从已验证的叶子证书提取`trust-domain/issuer/subject`身份；这个方法不声称能从任意`SslHandler`证明listener的首字节TLS顺序。上述2项TLS定向测试验证真实双端握手、证书身份提取、应用数据非明文、缺信任材料拒绝及Classic对照。该模式尚未接入Bookie listener，身份事实与HELLO能力/operation处理、真实TLS/socket内存计费仍未接通；匿名/越权拒绝和完整secret-leak矩阵未完成，步骤11仍未闭合。

#### 12. 实现真实MetadataStore adapter、安装激活与registration

依赖步骤5、11的接口，可与步骤11并行开发，真实集群联调等待本地存储验证。先选一种实际MetadataStore后端实现`ProfileControlStore`、`ProfileRegistrationStore`，不同时开发所有驱动。

范围包括immutable instance backlink、sidecar单记录CAS、有界child/page发布、READY、activation target留存、RepairIntent admission、delete cut及必要snapshot/retention。初始writer创建顺序为：

```text
分配instance/准备sidecar → 全E inactive install → 标准LedgerMetadata
→ READY与目标留存 → 全E activation → 返回writer
```

同phase请求并发，不逐个串行安装E个节点。只读打开不激活；恢复打开另走fence/grant。将startup/readiness reference接到真实文件与registration：本地恢复和所需delete catch-up完成后，先持久readiness，再发布匹配的ephemeral registration；完整delete追赶在步骤15闭合，未实现状态不能默认就绪。

**完成条件：** 每个CAS响应丢失都可查询同一结果；未知状态不放行，metadata不复制第二份membership权威；普通Add不查远端控制状态；断点重启不扩大权限。复用已有reference契约测试，不重新建设同名内存框架。

#### 13. 接入客户端普通写、ACK、背压重试和写期换组

依赖步骤2、11、12及本地存储验收。先跑通实验3/3/2、3/3/3和Bookie故障域F=1范围的普通写，再接背压、unknown与replacement。

逻辑entry identity保持不变，区分当前投递目标；当前尝试的NONE不能清除前次UNKNOWN。短暂资源拒绝进入有界退避，不直接触发换组风暴；达到真实故障条件时仍可正式换组。replacement顺序为：

```text
新目标inactive install → membership CAS → activation authority及历史目标留存
→ local activation → pending ACK/mapping整体切换
→ 必要副本resend → 恢复连续回调
```

**完成条件：** 迟到/重复ACK、旧incarnation、同起点连续换组、多slot更新和响应丢失不提前成功；保留未变副本有效ACK；无无限请求副本、无限退避或跳过逻辑空洞。保留一般E>W的客户端兼容回归，其通过不代表首批Segment全配置性能已验证。

#### 14. 实现基础ledger recovery与AutoRecovery

依赖步骤13，使用步骤5的grant/fence和步骤12的RepairIntent，分两部分交付。

先做open-ledger recovery：OPEN→IN_RECOVERY、fence、LAC/required-frontier取证、点读、合法recovery Add、匹配的`CLOSED(P,length,context)`和响应丢失解析。复用Classic的write-set、fencing read、W-A+1聚合、顺序推进及write-back，适配Profile身份与错误分类，不重建基础quorum算法。

再做fragment AutoRecovery：先持久化并admit RepairIntent，再授予recovery-only grant；复制后CAS membership、关闭grant、保留清理历史。修复完成不自动授予normal writer，也不触发延期的strong loss reset。

**完成条件：** 缺失证据来自正确且已fence的副本，timeout/unknown不算absence；LAC落后及fragment起点提升时P与length匹配，空ledger为(-1,0)，缺锚点defer、矛盾conflict；失败不伪造close，协调器重启不丢目标和清理义务。Model A随实现验证基础point recovery/durable close，Model E高级Range延期不豁免基础模型。

#### 15. 实现集群删除、离线投递与rejoin

依赖步骤14，首版删除要求fenced-close。将步骤9的本地能力接入集群流程：

```text
验证fenced-close → DELETE_INTENT关闭新admission → membership freeze
→ 枚举完整历史目标 → durable manifest/cluster tombstone
→ 普通逻辑删除成功 → 异步投递本地删除
```

目标包含初始/写期activation历史及RepairIntent，不只取最终ensemble map。随后实现delete stream、no-hole applied cursor、snapshot/catch-up、离线重返与incarnation校验；startup/readiness接入真实删除追赶结果。

**完成条件：** 离线Bookie不阻塞普通logical success，但清理义务不丢；applied cursor不等compaction；游标缺口、snapshot缺页、身份不符时不注册writable；coordinator崩溃后能继续同一删除。强访问撤销仍关闭，不把普通logical success宣称为全部旧reader立即失效。

**第三里程碑：完整分布式WAL闭环。** 创建→写入→读取→换组→恢复→关闭→删除→离线重返均有对应实现与验证；集群跑通仍须经过步骤16的综合验收才能用于真实业务。

### 第五阶段：完整验收与受限灰度

#### 16. 完成全链路故障、性能、兼容和运维交付

依赖前述各模块及其持续验证，在此组合执行，不把测试留到此时才补。

故障验收覆盖process crash、power loss、响应丢失、媒体损坏、永久副本损失、MetadataStore暂不可用、空间耗尽与重启交错；在声明的故障预算内验证不丢成功数据、不产生重复owner、不错误close、不复活旧instance。

模型验收完成实际启用A/C、必要D及组合边界，保留E>W、response loss、offline/rejoin、generation reuse和逐CAS/local durability边界。PASS只证明声明的有限模型范围，不替代真实fsync、TLS或资源测试；未执行/不适用范围在锁定manifest中明确说明。

性能按同durability、CPU/内存预算和负载比较stock Classic、修复后Classic、Segment，分别报告无网络局部性能、真实TLS/quorum性能及带索引/回收的可持续性能。沿用既定Gate，不在看到结果后调整阈值；格式、统一N、工作集、维护及预分配上限同样预先冻结。

兼容验收覆盖真实decoder、旧binary、新scope隔离与格式拒绝。当前延期的raw old-decoder/stock-binary验证，必须在发布对应stable面之前按既定流程恢复执行并通过；不能长期DEFERRED却宣称兼容。本次文档更新不执行该矩阵，G1仍为BLOCKED_UNVERIFIED；原地same-scope迁移不作为首个new-scope canary的额外功能。

运维至少交付配置校验、实际CPU/NUMA placement、只读状态/文件诊断、恢复失败原因、背压/磁盘水位指标及drain/停止流程；诊断不增加“忽略校验强制恢复”。最后在配置明确的独立小cohort做受限canary。退出方案为停止新写、保留新引擎读取/恢复能力并按已验证流程处理已有数据，不能让旧binary打开Segment文件。

**完成条件：** 实际启用能力的安全、恢复、资源、兼容、性能及canary证据闭合后，才提升对应RFC/feature状态与部署范围。完整重建、维护进展、基础恢复和普通删除的必要Gate都须在可保留数据canary前完成，不能用Add/ACK实验或Classic/DirectJournal基线替代。

## 并行工作与集成依赖

| 工作线 | 可并行内容 | 必须等待的节点 |
| --- | --- | --- |
| Classic维护 | CLIENT-1/2、基线与低风险调优 | 步骤13客户端接入前完成修复 |
| 执行与内存 | owner、安全binding、NUMA池，与格式/I/O并行 | DATA写入集成前完成基础ownership及预算 |
| 存储基础 | 格式、I/O及独立测试入口 | 控制日志/allocator→写入→点读/restart依次集成 |
| 网络与控制后端 | codec adapter、TLS、MetadataStore adapter | 真实集群联调等待本地存储验证与身份接口稳定 |
| 模型与故障测试 | 跟随模块同步展开，Block G与Block H并行 | authority/canary前完成实际启用范围的必要Gate |
| 性能验证 | 每个里程碑测量，亲和性分项对照 | 总收益结论等待索引、回收和真实网络全部加入 |

单人或小团队优先做成可运行闭环，避免同时展开许多未接通的框架。当前先交付步骤1～4，紧接步骤5～7取得第一份真实写入—点读—重启恢复结果，再加入索引/回收，最后接网络/集群；可选spin或延期增强不阻塞主线。

## DirectJournal与延期能力

DirectJournal是独立对照/优化支线，不是Segment开工前置。它保留Journal＋LedgerStorage结构，用独立cohort验证I/O、合批和线程收益，可与主线并行或等局部数据形成后开展；不宣称一份payload持久化，也不替代Segment安全证据。

首个完整WAL版本之后，按需求或测量选择扩展：

| 后续模块 | 前置条件 |
| --- | --- |
| 更换I/O backend、进一步barrier合并 | 已有稳定基线，可同条件比较 |
| Dedicated extent、简单lifetime segregation | 已证明shared-block碎片或搬迁成本不可接受；dedicated另验B5/B13 |
| Streaming Range、TailSummary、BatchRecoveryAdd | 基础点读恢复已完成，再验与point oracle等价 |
| rack/AZ故障域 | placement、ACK判断和恢复模型共同扩展 |
| 强访问撤销、在线删除 | 普通删除、fenced-close与rejoin稳定；强撤权独立验证全目标屏障 |
| strong completion/loss reset | 整ledger fenced+CLOSED，通过独立证明和竞争协议 |
| 跨Arena搬迁、设备evacuation、原地迁移 | 单Arena回收及多Arena失败处理成熟，随后单独接受 |

无实际需求或证据时继续disabled，不把这些能力列为首版必须补齐的欠项。基础close/membership不创建pendingPublication、不自动续期loss budget，旧未resolve token仍按原协议解析，不得超时强删。

## 每个模块的完成标准与证据边界

每个模块交付实际实现、对应测试、可重复测量、状态与未覆盖范围说明。接口定义或内存测试不能作为真实存储模块完成证据；未运行范围仍为NOT_EXECUTED。

热路径review检查是否新增逐entry fsync、MetadataStore往返、整包复制、线程交接或无界对象；后台review检查实际净回收、前台阻塞及故障后必要状态重建。每次正式比较锁定同一环境与阈值，旧恢复/网络/预算反例继续有效。

`manifest.json`中的`sourceCommit`、`designInputsAtSourceCommit`和各模块receipt继续绑定原历史源码，不能改成当前文档hash来伪造新证据。`plannedWork`只导航本清单；正式实施时必须以新的source/config/run identity记录本轮合同验证，旧12/17项reference测试、wire corpus和codec clone oracle不证明上述协议已经实现。

CLIENT-1/2是单独列出的Classic修复计划，不纳入Wave 0历史scope/receipt；该计划不豁免Segment安全门槛。Wave 0仍禁止触碰：

- `BookieImpl` 普通 Add 成功路径；
- 通过`PendingAddOp`接入Profile/Segment ACK决策；上述独立Classic缺陷修复不借Wave 0证据取得authority；
- `LedgerStorage`、`Journal`、`EntryLog` 的生产 authority 或格式；
- 现有 `bookie-rpc` fallback；
- 生产 Cookie、registration、AutoRecovery 和 delete；
- 任何真实 producer 的 ACK authority。
