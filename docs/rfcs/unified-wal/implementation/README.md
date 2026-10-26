# Unified WAL Implementation Wave 0

> 状态：**EXPERIMENTAL / NON-PROMOTABLE / NO AUTHORITY / DISCARDABLE**

本目录保存 Unified WAL Implementation Wave 0 的可执行实现边界与机器回执。Wave 0 只把已冻结合同变成 reference implementation、compatibility harness、形式化模型或隔离 prototype；它不授予 stable wire、stable on-disk、live shadow、Segment ACK 或生产注册 authority。

当前已实现的完整模块：

- [`ProfileDescriptor` reference implementation](profile-descriptor/README.md)
- [`Profile control/auth typed reference endpoint`](profile-control/README.md)：已完成interfaces、isolated endpoint、12项普通功能测试、机器回执与最终scope audit；仅消费transport facts与semantic store interfaces，不接入production listener/store/route/Add/ACK。
- [`Bookie startup/readiness/new-scope reference harness`](startup-readiness/README.md)：typed immutable facts、固定fail-closed startup顺序、durable local readiness、persistent CAS/response-loss重读、matching service-info、stale demotion、ephemeral writable registration、new-scope access isolation、九个crash/restart边界、17项普通测试、机器manifest/receipt/test summary与最终scope audit已完成；另有步骤12隔离真实ZooKeeper persistent CAS增量，原reference回执保持冻结；不提升Spike/Gate/production状态。

已冻结、延期验证的模块：

- [`Experimental Profile wire 与冻结的 old-decoder/stock-binary harness`](profile-wire/README.md)：Block D scope guard、pure codec 与610-vector corpus 已实现；真实4.14.8 decoder发现并冻结原`0x0FFE4250`反例，replacement `0x0FF04250`与authoritative corpus已通过new-codec定向回归。2026-10-13当前fresh run的四版本665-vector（含完整实验control请求）released-decoder/localhost stock网络及JDI内部effect观测已通过，665条channel实际终结、stock文件/namespace不变、前后Classic sentinel正常，状态`PASS_SCOPED`；实际control codec请求样本及四版本resolver协议选择已验证，mixed rolling versions/auth/semantic/startup及general stable control-body Gate仍未闭合，G1保持`BLOCKED_UNVERIFIED`。历史反例与延期回执保留原绑定。

输入锁见 [`manifest.json`](manifest.json)。每个模块自己的source/test/artifact范围、复现命令、证据结论和剩余BLOCK由模块目录拥有。实现证据不能自动提升四份owner RFC或三个Spike的状态。

## 下一阶段实施清单

规划基线为 `e8b162076fd8cfa84a11351a82caac8c5ac1f939`，规划提交 `e75c67f3514d1627ea790d052e2c9226e593aa3c`；它们是历史输入锁，不表示当前 HEAD。当前仍在原 `turbo-bk` 分支持续实施16步，步骤1隔离配置、步骤2 CLIENT-1/2独立Classic红绿补丁、步骤3实验格式及真实FileChannel基础已交付。固定owner/安全binding/属地池、真实DATA—点读—restart、控制checkpoint/派生索引/MOVE/FREE/有界回收、ZK安装激活与原SDK写入/换组均有下文的隔离局部证据；各步骤的剩余验收以对应最新小节为准。当前步骤14新增显式整体recover入口，统一已有真实组件、持久身份续接与matching CLOSED；已接历史fragment copy/close、URL2双LAC证据和已知required prefix/normal-tail只读验证；最后fragment固定选择与逐target恢复写回/close也已接通；非空selected最后fragment已增加3/3/2、3/3/3 distinct terminal ACK协调、接收端持久读上界与matching标准CLOSED解析；空ledger/empty-last也已增加独立UIC6/type12的无grant关闭；matching recovery-CLOSED的独立readonly-open已接原SDK；CLOSED fragment repair已复用原copy/membership/grant-close并固定原close history cut；独立逐坐标全副本复核及最终fresh CLOSED proof已实现，显式条件化清marker已接metadata/lifecycle/lock/marker同事务cut，已接显式preconnected cohort的原worker自动循环，空缺副本grantless repair已接原worker并在实际terminal durable后发布membership，registered cohort pool、显式owned AutoRecovery service bootstrap、固定descriptor/cohort的Profile成员故障发现与CLOSED周期DATA审计均已接通；显式有界普通writer close与固定cut adoption已接原SDK及上述恢复/关闭路径；OPEN 固定 LAC 下界前缀审计已接同一周期任务且不 fence writer、不清 marker；其他required authority producer、默认SDK路由、完整 OPEN/payload/placement审计、全链路字节预算与离线可见性追赶尚未闭合。[本地存储](local-storage/README.md)、[协议/SDK](profile-wire/README.md)、[控制面](profile-control/README.md)分别拥有最新范围、命令和证据；所有生产/稳定Gate保持原未闭合状态。

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

2026-10-02 逐entry路径另已接入合法recovery Add，复用同一合批、deadline、固定池、按需refill和重复坐标状态。私有exact wire payload快照在driver claim后移交同一源bytes额度，不另复制一份大输入；格式、CRC、BK entry ID及固定shard在预留前重验。普通/recovery可同批，各自权限及grant close/tombstone在完成前独立复核；交叉重试相同内容共享物理结果，冲突不新增写入。恢复写无须normal activation，取消响应及shutdown不提前归还在途资源。wire解析/准入快照到存储的完整credit交接、全局bytes总账、旧I/O错误恢复与服务级restart仍未完成，步骤6不标完成。

#### 7. 实现点读与基础restart

2026-10-02 driver已发布候选的normal/recovery重复比较使用既有owner固定对齐池作record点读scratch，并直接读取其已拥有的输入view。池不足在读盘前临时背压，不消耗DATA坐标或force；源与scratch均等实际I/O及owner结果终结后归还。独立parser/CRC及权限/候选复核保留；通用点读、只读重启检查与无池手动入口仍为堆buffer实验路径，全读取/网络/索引bytes总账尚未闭合，步骤6/7不标完成。

依赖步骤5、6。点读实现正确定位、allocation/generation pin、独立record校验和有界对齐读取，不为小entry强制读取整个DATA batch。restart顺序为：

```text
验证storage与控制文件 → 重演控制状态 → 枚举当前合法DATA
→ 验证当前必要记录 → 完成必要恢复同步 → 重建定位 → 开放对应实验服务
```

尚未接RocksDB时，可在声明的小规模范围内使用有界定位结构，满额明确背压；不得用持续增长的内存Map得出长期性能结论。

2026-10-02 本地点读已另接入既有owner/I/O/固定池：live读选择已发布候选，重开读选择启动清单的唯一连续stream位置，I/O排队前保持有界文件pin。回owner复核Bookie权限与候选/Arena cut；未知位置不报absence。成功结果持有池lease，消费者最后使用后关闭；慢消费会背压读/DATA并使shutdown等待，取消、撤权、读盘/owner/回调失败释放实际终结的资源。重开池不足以容纳保留旧entry时拒绝reader初始化，较低新写N不截断旧读能力。此为隔离本地读入口，仍无真实Profile读授权、响应credit接线、一般MOVE/FREE selector、旧I/O错误恢复或服务级可写restart，步骤7未完成。

**完成条件：** 写入→确认本地持久化→读取→终止进程→重启→再读与重复写入可重复运行；未知覆盖不误报NoSuchEntry，不同stream恢复不截短共享文件，旧generation残留不复活。开始验证两次故障：write完成但barrier未完成时退出，重启处理重复请求后再次掉电；单次进程终止后可读不足以证明持久化保证。

当前[隔离点读及文件同步切片](local-storage/README.md)已用真实文件核对Arena grant、batch/record身份及CRC，按对齐范围读取单条record；较低新写入阈值重开后旧entry仍可读。停写后完整扫描的DATA文件可执行一次恢复`force(false)`；全目录校验同步后可在显式容量内取回普通record物理位置，并先以Arena已apply首序号核对调用方起点，再仅对匹配shard与generation的单stream batch/entry检查序号连续性；同文件其他stream按Arena授权块跳过framing解析，但仍完成文件身份检查与文件级同步；目标stream损坏、缺口、重复和容量不足不返回候选。全量清单仍校验整个DATA文件；隔离回归证明旧stream结构损坏不阻断另一stream物理候选。运行时热候选可发布，调用方选定的多个恢复物理stream候选可在新owner的有界热目录整组核对scope、容量和重复坐标后原子装入并点读，失败不发布先通过的stream；隔离测试还覆盖normal DATA真实force与durable fence后关闭、重开双控制日志并从同步候选点读；局部点读现可持Bookie读票据核对安装实例，在fence后继续读取，并于tombstone前后复核本地route；只读stream恢复也可在完整校验目标batch后，以重放的Bookie本地route排除已tombstone实例的定位容量。当前无MOVE/FREE的单Arena受控recovery重试可在只读重开后要求单一stream、连续前缀、DATA恢复同步、唯一坐标和重放的exact grant，点读比较后复核权限，仅返回物理候选比对结果；旧DATA I/O错误尚无持久分类，不能把该结果提升为durable duplicate成功。文件层仅有物理append-only重开；完整current-selector/pin、全部必要DATA选择与服务权限尚未接通，步骤7未完成。

受控normal重试也可持普通Bookie准入票据，在真实normal DATA得到隔离本地结果并重开双控制日志后，同步并比较唯一物理候选；相同payload/累计length与不同payload分别只返回匹配或冲突**候选**。同步失败释放票据，同步期间tombstone撤权返回`STALE_ADMISSION`。该入口不向DATA driver恢复热定位，不把任一候选提升为最终Add结果；完整selector和旧I/O结果证明仍缺失。

普通Add另有真实子进程回归：分别在本地结果之后和DATA barrier之前`Runtime.halt`，父进程重开双控制日志，对目标stream完成恢复同步，再持Bookie读票据点读和普通写票据比较候选。这验证普通路径跨进程恢复的物理证据，不模拟掉电，也不返回重启后的durable duplicate结果。

当前无MOVE/FREE且目标shard只有一个Arena stream时，隔离恢复点读可从已重放的Bookie本地route取得读票据，在DATA writer排他锁内完成连续stream扫描、必要同步、唯一坐标选择和独立record点读，再复核route与stream。普通/recovery重复候选比较复用同一受锁保护的选择过程；无唯一位置或出现第二个stream时返回未就绪而非确定不存在。该受限选择不包含一般current selector、allocation pin、网络读授权或可写restart。

隔离recovery DATA新增真实子进程边界测试：本地持久结果后或完整写入但barrier前直接终止进程，父进程重放双控制日志、执行DATA恢复同步并点读候选。它验证进程中断与只读恢复入口，不模拟掉电，也不将物理比对提升为durable duplicate或生产ACK。

另一个两次`Runtime.halt`回归验证进程重启后的物理追加：新进程同步旧DATA并跳过旧授权块零尾，在新grant下写另一stream；再次终止后父进程分别点读两条物理stream，旧recovery重复请求仍因同shard多stream保持`NOT_READY`。此回归不完成服务级可写重启或第二次掉电验证。

扫描的零尾页不能作为未提交或可复用证明。隔离文件层现可完整扫描并同步旧DATA后，只在全部已apply授权块的最高结束位置之后重开**物理追加**；旧块零尾全部跳过，新写仍须新apply的Arena grant。该句柄拒绝无授权原始写入，也不能构造会返回local result的DATA driver。必要DATA选择、持久空间used/unused与末端前缀事实、旧I/O错误及终结未闭合前，不开放完整服务级可写重启。

该物理重开现可选在同一次writer锁下保留容量有界的全目录普通DATA清单，并由原owner原子装入热目录；旧坐标只在经过该目录的准入路径中占位，低层物理写入尚未接入。同次扫描还可在显式stream容量内核对Arena持久起点和每个已观测stream的连续物理序号，缺口/重复拒绝该stream候选；空stream报告空前缀。重开句柄的有界读取pin允许在持续持有writer锁时，从启动清单跨stream选择唯一坐标，核对所在stream连续前缀、重放Bookie exact读route和扫描绑定的Arena applied cut，再点读旧entry；新grant使旧清单失效，关闭须等待pin退出；fence后仍可读，tombstone后拒绝。恢复定位仍无local-durable标记。DATA锁入口先拒绝本进程同scope重入，再打开锁文件channel，避免失败关闭第二个channel释放原writer的POSIX锁。任一容量不足或恢复同步失败不返回清单/句柄；此受限本地读取不能替代旧I/O错误、一般selector或Profile网络读授权证明。

2026-10-02 全目录启动清单另可按已重放Bookie本地route/tombstone筛选定位，定位容量只计当前可读record，stream容量仍计全部已观测stream。扫描前后控制cut必须稳定、无未决控制或entry义务；已观测stream缺口/重复和当前坐标歧义均拒绝整个交接。筛选不豁免任何已扫描DATA的结构/CRC验证。两次真实进程终止后的多stream唯一旧坐标可在同一重开句柄读取；独立单stream重复候选比较尚未扩大，旧I/O错误判定、一般MOVE/FREE selector、动态写入定位和可写服务restart仍未闭合，步骤7不标完成。

2026-10-02 Arena `DEVICE_STATE`已实现单调DATA文件隔离子集。实际绑定的DATA路径在I/O前共享预留每文件通知ticket及逻辑日志容量，保留普通控制槽；预留不足在占用DATA坐标前背压，控制配置不足则拒绝绑定。已知write/force失败立即停止该文件新写、点读及重复成功；控制忙时原通知等待当前批次，停收后仍排空，不重新申请完成槽。记录按序apply，原grant保留，重放与后续sync不解除隔离；普通成功没有新增控制日志/fsync。实际控制I/O或owner故障、落盘前退出仍未闭合跨重启判定，未见记录不能证明旧错误不存在，clear/reset及服务级可写restart未启用。步骤5～7仍为局部实现。

2026-10-02 写入会话切片在首次ALLOC/DATA前由隔离writer显式`startSession`，复用Arena `DEVICE_STATE` OPEN/CLOSE与单调epoch，启动时预留正常关闭记录容量。关闭DATA准入、排空实际I/O和关闭descriptor后，仍持有目录锁并检查控制/故障通知无未决，再记录CLOSE；实际DATA错误、执行前取消、关闭失败或无CLOSE的进程退出保持不可写。真实子进程回归覆盖DATA force失败、没有隔离记录就退出；重放在DATA恢复sync前拒绝重开，后次sync不改变结果。正常CLOSE可重放并开下一物理writer epoch，不复用旧授权尾部，不新增正常Add force。legacy已分配目录不能补认历史正常关闭；append-only仍无local-result authority，自动生命周期接线、未正常关闭会话恢复解析及可写restart仍未完成。

2026-10-02 正常关闭会话的隔离restart已接回既有DATA driver：新入口要求持久CLOSE及正epoch、每shard唯一未退役stream，完整扫描/恢复sync并按Bookie route选择后开新writer epoch。原owner bootstrap在稳定Bookie cut下恢复连续through及本地持久热候选，每shard一次；校验stream或容量失败不改前缀，cut变化不装入。旧entry同内容重试返回本地持久结果，冲突不增加DATA force；新entry经现有refill在fresh pool接续原stream下一序号，旧授权尾部仍跳过。新JVM正常写入/关闭、两轮重开/重复/新写已验证；plain物理扫描、legacy和未正常关闭会话仍无相同资格。网络/MetadataStore/readiness、未正常关闭恢复、一般selector及可持续空间回收仍未完成，步骤5～7不标整体完成。

2026-10-02 隔离存储图已由`IsolatedSegmentStorage`统一持有DATA、双控制日志、执行组、各shard合批器及点读入口。调用方先构造资源owner，再启动新目录或正常关闭会话的restart；只有全部原owner bootstrap及控制票据终结后才开放请求。总热定位容量按逻辑shard拆分，源/对齐buffer仍复用执行组全局预算。关闭先停止入口和执行组准入，等待实际I/O、owner完成及消费方归还读结果buffer，再关闭DATA并记录健康CLOSE，最后关闭Bookie/Arena控制日志。超时保留同一owner及目录锁供继续排空；启动失败也走同一路径清理，不删除目录。普通/recovery交接失败只归还未被driver接管的票据，不撤销已接管义务。该入口没有接Bookie listener/readiness/registration，未正常关闭恢复、一般selector/回收及完整bytes预算仍未完成。

2026-10-03 [Spike C有限模型切片](models/README.md)取得匹配官方checksum的TLC 1.7.4并实际运行：共享/独立文件正向配置均完整探索PASS（44,007/50,859 distinct states），三个候选规则均得到指定性质的反例；回执`unified-wal-model-jqvpwfsw`绑定15个实际输入及完整输出。模型覆盖管理会话、控制/DATA barrier、连续前缀和旧错误隔离，仍不是完整Model C、A+C或Block G通过。OPEN仅靠重放和后次同步取得恢复资格会丢失旧故障历史；其故障解析证据/合同仍待确认，现有Java隔离行为未改变。

**第一里程碑：真实写入—点读—重启恢复。** 步骤5～7完成后，已有真实文件I/O和可重复的基础恢复测试，可出第一份局部性能报告。无真实网络的结果不称端到端提升，无长期索引/回收的结果不称可持续吞吐；按Spike切片声明实际覆盖范围。

### 第三阶段：索引、回收与长期运行

#### 8. 完善checkpoint/rotation、派生索引与完整重建

2026-10-03：[Bookie/Arena checkpoint局部实现](local-storage/README.md)已冻结applied状态及字节边界，复用有界image codec、各自的durable CHECKPOINT_COMMIT和独立A/B发布页，恢复匹配代并重放完整后缀。权限/幂等、发布故障、真实JVM退出、DATA重启和有限模型均有回执。两owner现均提供停写后的双恢复根物理rotation与旧prefix回收，独立锁、身份和发布页不混合；完整当前suffix仍必需。真实RocksDB冷重建、flush后覆盖页及删库后的重建已接入隔离正常restart。异步定位WriteBatch已复用既有owner/DATA I/O接入普通与恢复ENTRY_DATA；每shard最多一个在途批次，后台失败保留热结果并背压新DATA，控制预留仍独立。当前已在query-visible owner completion后淘汰hot，hot miss后fresh Get与真实DATA读/重复比较复用同一票据；占用坐标预算及全量恢复容量独立于hot容量。运行期覆盖检查点已按所有原worker冻结cut、暂停Put、真实flush及A/B force发布；同代publication持续递增，部分DATA额度拒绝先解冻后返回，取消保留实际I/O/owner终结。正常关闭restart已验证实际覆盖与当前控制/stream/grant，覆盖内仅校验固定批头/连续性，未覆盖授权tail完整解析及同步；无效覆盖仍全扫描，有效覆盖复用index代并补写tail/过滤tombstone。真实运行期storage图另在active memtable、页force前、PERSISTED owner回调内及正常CLOSE后执行JVM退出；未关闭会话即使物理重建后仍拒绝，正常关闭可用同代覆盖恢复，见[进程边界证据](local-storage/README.md#运行期索引的真实进程退出边界)。运行期容量触发轮转与[内部 checkpoint 历史压缩](local-storage/README.md#内部-checkpoint-历史的有界保留)已有局部增量；业务/会话历史压缩、一般OPEN错误恢复、完整长期容量及真实掉电未完成，步骤8保持部分进展。

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

2026-10-03 [分配控制顺序与低地址追加](local-storage/README.md#分配控制顺序与低地址追加)已消除必要的文件offset排序约束：写游标按原成功allocation控制sequence推进，进入后继grant不返回旧尾；restart跳过全部旧grant尾部而允许新的低地址grant。恢复按每allocation连续区间与原控制顺序验证stream，真正gap/duplicate仍拒绝。真实rotation、全量/覆盖辅助storage重启及有限分配顺序模型通过。该前置的历史回执不授予FREE。2026-10-04已接通[整allocation全死FREE与退役前缀](local-storage/README.md#整-allocation-全死-free-与退役前缀)：真实DATA/终态tombstone proof、先Bookie后文件准入冻结与旧I/O/read lease排空、条件durable apply/generation 1→2、原owner保护、控制rotation与全量/覆盖辅助恢复。随后已接通[健康会话整块复用](local-storage/README.md#整-allocation-复用与健康关闭末端)：现有refill使用准确FREE generation，健康CLOSED保存当前写入末端，恢复排除旧suffix而校验必要prefix；真实同块两轮FREE/写入与共享活数据恢复验证。后续已接通[运行期终态派生清理](local-storage/README.md#运行期终态索引与坐标回收)：真实native行删除、原owner坐标预算释放及持久coverage，固定小容量四轮FREE/reuse在1/4 worker下验证。后续[relocation内部完成](local-storage/README.md#relocation-副本内部持久化完成)已接通copy-only实际DATA barrier、连续前缀与原owner内部结果，copy不成为普通定位。2026-10-04已接通[条件MOVE、native覆盖与多批source组compaction](local-storage/README.md#同-arena-多批-compaction)，全部批次成功后整块FREE；按完整aligned COPY总成本判断allocation净收益。后续已接通固定shard有界轮转与池化分批捕获；逐allocation在线cut、控制历史/SST长期净空间及持续循环验收仍未完成，不标步骤9完成。

依赖步骤8，对应UW-4/UW-5。按本地tombstone禁止新访问→整块全死直接FREE/reuse→部分存活块compaction→多次搬迁/orphan/并发回收推进。搬迁复用DATA I/O，但使用独立内部完成分支：

```text
写RELOCATION_COPY → 副本durable → MOVE命令durable且apply成功
→ selector/pin cutover → 旧读者与I/O结束 → 整allocation FREE
```

本地测试通过受控授权入口触发删除，不代表集群删除协议已完成。按有界victim组的实际净收益选候选，扣除destination占用及framing/padding；合批复制和控制持久化，不为每条moved entry单独fsync，不要求OPEN ledger先关闭。

**完成条件：** 部分ledger删除不提前释放共享块；copy durable而MOVE未成功不成为当前数据；A→B→C且A/B已复用、没有新增checkpoint时仍能恢复C，C损坏或必要控制后缀缺失仍隔离；并发MOVE/FREE不产生两个winner，旧I/O不覆盖新owner。FREE以整allocation generation可回收为条件，不以单ledger删除原因代替。

当前[固定shard有界选择](local-storage/README.md#固定-shard-的有界回收选择)先测量真实DATA，零current live组先于快照和destination分配处理；保留独立整allocation证明。该入口每次查看最多16个同文件grant；当前[有界轮转](local-storage/README.md#有界回收窗口轮转与有限容量循环)固定本轮控制上限并推进进程内游标，新grant留至下轮，随后回访旧候选。后续已接通健康重启后的最新current COPY tail续写；一般partial任务收敛、维护调度或长期净容量验收未完成。

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

2026-10-06 步骤11增量：[真实隔离普通 Add 接线](profile-wire/README.md#真实隔离普通-add-接线)把 mTLS/HELLO、固定布局与本地 N 上限、credential/context/CRC/entry 坐标、既有有界同步输入复制、真实 DATA 结果和完整 status 三元组接通。声明的 N+1 在 body 分配前关闭；force 前不回 OK，实际 UNKNOWN 不回 NONE；断连只停止等待。排队 completion 和下游持有的响应在真实消费/write promise 终结前继续计费。八个真实 socket 场景（含取消额度红绿）与相关14类383项中378通过、5项macOS上的Linux亲和性跳过；Spotless/Checkstyle通过。有限矩阵39正向完整PASS/122指定反例。HELLO identity来自实际存储，readiness generation仍为显式调用输入（测试为0）；生产listener、控制操作授权、TLS/socket及存储总账、完整startup/readiness、quorum和性能对照仍未完成，步骤11/B18与生产Gate保持OPEN。

2026-10-06 控制网络增量：[真实隔离FENCE与控制额度](profile-wire/README.md#真实隔离-fence-与控制额度)复用原context/credential校验、Bookie控制driver与实际write/force/owner apply，首次授权后把连接/frame/解析响应workspace原子转入CONTROL；后续逐次鉴权。pre-cut等待返回原操作transient/NONE、force/apply不确定性保留UNKNOWN，断连不提前释放已复制控制请求或实际I/O资源。同一port的真实DATA半包/第二DATA额度拒绝、UNCLASSIFIED满额下已有CONTROL推进及零effect拒绝已验证；14类391项为386通过/5平台跳过，style通过；有限矩阵40正向完整PASS/128指定反例。生产listener、完整control-plane、TLS/socket/native/storage总账、fresh startup/readiness及步骤11/Gate仍未完成。

2026-10-06 恢复写入增量：[真实隔离ADD_RECOVERY](profile-wire/README.md#真实隔离-add_recovery-接线)沿用144字节固定prefix、精确本地OPEN grant与原DATA驱动，按实际源额度先预留再复制完整BK entry；reference网络整包克隆不进入server路径。恢复成功/重复校验不激活normal writer，grant close晚于DATA写入的过期结果与force失败保持UNKNOWN；断连仅停等待，实际I/O/owner排空及native锁保留。七个真实socket用例已通过；最终相关14类398项为393通过/5平台跳过，style及41正向/134指定反例通过，详细源码绑定见上述实现记录。READ_ENTRY响应/确定missing、真实RepairIntent及grant控制面、客户端quorum/完整恢复、生产listener/readiness与TLS/socket/storage总预算尚未闭合，步骤11/14和生产Gate保持OPEN。

2026-10-06 [Netty backing buffer额度接线](profile-wire/README.md#netty-backing-buffer-额度接线)已把隔离channel的heap/direct capacity、扩缩容时旧加新backing、derived view最终释放、TLS/socket通过channel allocator分配的ByteBuf及响应workspace拆分加入原连接/Bookie账本。已有read backing跨用途保留原lane；断连归还名额但不提前归还实际仍持有的backing。真实socket漏记红绿、六项allocator回归、TLS分配拒绝及原22项socket场景已验证。JDK/native TLS内部对象、kernel buffers及存储统一总账仍未闭合，生产listener/readiness、READ_ENTRY、完整模型/长期/掉电/性能与步骤11/Gate仍OPEN。

2026-10-06 [接收至存储源副本交接](profile-wire/README.md#接收至存储源副本交接)使隔离normal/recovery同步entry复制在原source子上限之外，复用借用frame连接的exact heap backing额度；frame/workspace/source共存均计费，断连后源保留至owner实际用完，复制到预分配对齐池后才归还源，实际I/O仍持有原pool lease。明确credit拒绝回滚source并沿用原未接纳结果；其他分配错误传播，手动/pooled调用者保留独立unpooled副本合同。两条真实socket路径及额度拒绝/恢复单测通过，未改wire或存储格式。完整TLS/kernel/object/index/storage总账、生产listener/readiness、READ_ENTRY及步骤6/11/Gate仍未闭合。

#### 12. 实现真实MetadataStore adapter、安装激活与registration

依赖步骤5、11的接口，可与步骤11并行开发，真实集群联调等待本地存储验证。先选一种实际MetadataStore后端实现`ProfileControlStore`、`ProfileRegistrationStore`，不同时开发所有驱动。

范围包括immutable instance backlink、sidecar单记录CAS、有界child/page发布、READY、activation target留存、RepairIntent admission、delete cut及必要snapshot/retention。初始writer创建顺序为：

```text
分配instance/准备sidecar → 全E inactive install → 标准LedgerMetadata
→ READY与目标留存 → 全E activation → 返回writer
```

同phase请求并发，不逐个串行安装E个节点。只读打开不激活；恢复打开另走fence/grant。将startup/readiness reference接到真实文件与registration：本地恢复和所需delete catch-up完成后，先持久readiness，再发布匹配的ephemeral registration；完整delete追赶在步骤15闭合，未实现状态不能默认就绪。

**完成条件：** 每个CAS响应丢失都可查询同一结果；未知状态不放行，metadata不复制第二份membership权威；普通Add不查远端控制状态；断点重启不扩大权限。复用已有reference契约测试，不重新建设同名内存框架。


2026-10-05 persistent CAS 增量：[真实 ZooKeeper readiness CAS](startup-readiness/README.md#步骤-12真实-zookeeper-readiness-cas-增量)直接实现已有接口，fresh-read 校验完整 envelope、store mzxid/readiness generation，原子 birth 检查防止删除重建的 dataVersion ABA。真实双 client/server 验证单 winner、response-loss 查询、ACL/损坏和提交后超时；25 项相关测试全部通过，Spotless/Checkstyle 通过，当前有限模型 33 正向完整通过/96 指定反例。历史 Wave 0 scope guard 对后续累计路径返回 BLOCK，原 reference 回执及脚本未改。仅完成隔离 persistent CAS，未接 service startup、ephemeral、完整 control store/metadata/安装激活；步骤 12 与生产 Gate 仍未完成。


2026-10-05 registration 增量：[session 绑定的真实 ephemeral 发布](startup-readiness/README.md#步骤-12真实-ephemeral-发布增量)实现原接口，persistent birth/dataVersion 条件后创建 owned info/marker，再以 info 版本递增原子发布 writable。retired nonce 不复用，旧 demote 不能删除同版本新注册；unknown/foreign/corrupt 不认领成功。真实 server 的 publication 交错、丢回复、parent ACL 拒绝、session expiration/新 attempt 与原 harness 的 ZK store 组合已验证，37 项相关测试及 style 通过；模型 34 正向完整通过/100 指定反例。仅为独立实验 namespace，typed local recovery 输入未变真实文件事实；生产 BookieServiceInfo/available registry、真实 startup/local receipt/delete catch-up、listener/placement/local admission 的持续失连降级、完整控制 metadata 与安装激活仍未接线，步骤 12/Gate 保持未完成。

2026-10-05 本地 readiness 增量：[真实文件 receipt](startup-readiness/README.md#步骤-12真实本地-readiness-receipt)实现原 LocalReadinessStore，复用实际 writer 锁与 FileChannel；完整 temp write/force→原子替换→目录 force 后才 APPLIED，UNKNOWN 实例拒绝读成功，新 attempt 严格解析并同步已发布 receipt，未发布 temp 不恢复。100 代替换保持固定文件集合，短写、两个 barrier 故障、scope/格式损坏、跨进程互斥及两轮退出/重开已验证；原 harness 已组合真实文件与两个 ZK stores。48 项相关测试及 style 通过，有限本地 publication 与既有模型为 35 正向完整通过/105 指定反例。输入的 recovery/device/delete facts 仍非真实恢复，pre-replay startup、生产 registry、持续失连准入、control store/安装激活及完整模型/掉电/性能未完成，步骤 12 与生产 Gate 保持 PARTIAL。

2026-10-05 控制读取增量：[真实 ZK 控制权威读取](profile-control/README.md#步骤-12真实-zk-控制权威读取)直接实现原 ProfileControlStore，在独立 provisioned namespace 内按 derived key 执行一次 sync/get；只有真实 NONODE 才是不存在，无效/不完整/ephemeral view 隔离，错误/超时/过期为 unavailable，payload 与实际 Stat.mzxid 来自同一 snapshot。73 项相关测试及 style 通过，有限读取和全部既有模型为 36 正向完整通过/109 指定反例。它只读取单份 experimental authority view，未实现完整 sidecar/backlink/fanout/发布 CAS/安装激活；真实 startup/delete catch-up、生产 registry/持续准入及步骤 12/Gate 仍未完成。

2026-10-05 准入增量：[session 绑定的本地 DATA gate](startup-readiness/README.md#步骤-12session-绑定的本地-data-准入)在首次 cold 操作前绑定实际 ZK watcher manager；匹配 owned registration 后开放，观察到失连、过期、close/demote 即永久退休原 attempt，同 session 重连及迟到读取不重开。隔离 graph 的普通/恢复 DATA 按本地 Bookie/incarnation 检查，热路径不查 metadata；已提交 I/O 与原池资源正常排空。八类 296 项及 style 通过，有限矩阵 37 正向完整 PASS/113 指定反例。真实 startup/fresh recovery/delete facts、生产 listener/registry、持续远端 authority、完整安装激活、quorum、完整模型与步骤 12/Gate 仍未完成。

2026-10-05 启动排他增量：[本地恢复 prerequisite](local-storage/README.md#restart-的-data-排他与控制恢复顺序)先取得真实 DATA 目录 writer lock，再恢复控制及 index，把同一 native lock 交给 files，保留原全部 header 校验/恢复同步与 CLOSED 限制。实际子进程外部锁红绿、控制打开/DATA 恢复失败清理及成功移交/读回验证完成；340 项中 335 通过、5 项 macOS 上的 Linux 亲和性跳过，style 通过。有限矩阵 38 正向完整 PASS/117 指定反例。只补本地 ownership 前置，compatibility/device/delete 权威、真实 startup/readiness、生产接线及完整模型仍待完成。

2026-10-06 [标准metadata backlink创建](profile-control/README.md#步骤-12标准-metadata-backlink-创建)已将固定52-byte instance/opaque sidecar reference写入原标准metadata custom entry，并经真实ZK LongHierarchicalLedgerManager create-if-absent及每次创建结果后的实际read解析同内容重试与回复丢失；instance/reference/初始内容冲突不覆盖，实际创建后删除并读到NONODE仍保持UNKNOWN。新9项真实server回归及相关验证见该记录。它只提供全E安装前置已验证之后的创建原语；真正sidecar/全E receipts/READY/activation、后续birth-bound mutation CAS与完整步骤12仍OPEN，未返回normal writer或提升生产Gate。

2026-10-06 [真实PREPARING reservation与INSTALL冷读](profile-control/README.md#步骤-12真实-preparing-reservation-与-install-冷读)已将最多32个计划目标及完整instance/backlink/descriptor/operation/payload写入真实ZK不可覆盖root，并为指定本地Bookie投影原ProfileControlStore的INSTALL authority；真实双client竞争、同identity冲突不覆盖、响应丢失/提交后timeout及scope/ACL/坏root拒绝已有10项回归。该root不授予READY/normal activation，计划集合不替代标准membership；all-E local receipts、READY/delete cut与birth-bound mutation CAS、activation/coordinator及完整步骤12仍OPEN。

2026-10-06 [PREPARING到真实local inactive install](profile-control/README.md#步骤-12preparing-接线真实-local-inactive-install)已让原typed endpoint及实际ZK reservation冷读复用原Bookie控制driver/write/force/owner apply，只有durable结果成功，提交后timeout不取消实际任务或提前释放writer isolation。6项新验证含真实3个目标同phase并发force、全部durable install后仍normal-inactive；transport仍为typed facts，INSTALL真实TLS/wire、全局receipt/coordinator与metadata前置门槛、READY/CAS/activation及完整步骤12仍OPEN。

2026-10-06 [all-E后的初始metadata协调门槛](profile-control/README.md#步骤-12all-e-后的初始-metadata-协调门槛)已固定descriptor/E/W/A/有序计划/credential与peer binding，先真实PREPARING，再并发收齐全部E的matching durable INSTALL及稳定birth-generation receipts，fresh-read原root snapshot后才调用标准metadata create/actual read；A个成功、UNKNOWN、错binding或root变化均不推进。冻结caller custom bytes及receipt capability digest，无secret字符串展开。返回metadata/receipts，尚无persistent retention、READY/delete cut CAS、actual INSTALL TLS/wire、全E activation或normal writer，步骤12仍PARTIAL。

2026-10-06 [initial READY与DELETE_INTENT CAS](profile-control/README.md#步骤-12initial-ready目标留存与-delete_intent-cas)已真实持久化有界initial activation page，再由同一birth/semantic/version-bound lifecycle head CAS发布READY并与DELETE排序；旧root/page dataVersion0不能接受旧CAS，READY后的DELETE保留全部E及activation授权。actual READY/metadata冷读接入原typed endpoint和reserved physical driver，3个目标durable activation后本地normal admission开放；transport仍typed facts，没有实际control wire、normal writer返回或物理删除，完整步骤12/Gate仍PARTIAL。

2026-10-06 [可恢复初始协调与all-E availability](profile-control/README.md#步骤-12可恢复初始协调与-all-e-availability-snapshot)已从fresh/PREPARING/candidate/READY恢复同一初始操作；持久page证明metadata曾存在，恢复只读校验并隔离缺失，不重建未知record。全部E当前matching durable activation及最后metadata/READY取证后返回AVAILABLE snapshot，历史fenced activation返回STALE不计数。实际新coordinator/ZK session及Bookie文件replay、partial/UNKNOWN/DELETE门槛已验证；无actual control TLS/wire、LedgerHandle/writer或production authority，步骤12仍PARTIAL。

#### 13. 接入客户端普通写、ACK、背压重试和写期换组

依赖步骤2、11、12及本地存储验收。先跑通实验3/3/2、3/3/3和Bookie故障域F=1范围的普通写，再接背压、unknown与replacement。

逻辑entry identity保持不变，区分当前投递目标；当前尝试的NONE不能清除前次UNKNOWN。短暂资源拒绝进入有界退避，不直接触发换组风暴；达到真实故障条件时仍可正式换组。replacement顺序为：

```text
新目标inactive install → membership CAS → activation authority及历史目标留存
→ local activation → pending ACK/mapping整体切换
→ 必要副本resend → 恢复连续回调
```

**完成条件：** 迟到/重复ACK、旧incarnation、同起点连续换组、多slot更新和响应丢失不提前成功；保留未变副本有效ACK；无无限请求副本、无限退避或跳过逻辑空洞。保留一般E>W的客户端兼容回归，其通过不代表首批Segment全配置性能已验证。

当前隔离增量见[原 SDK 写期换组与整体回调门](profile-wire/README.md#原-sdk-写期换组与整体回调门)：预连接 bounded pool、3/3/2 两 slot 换组及3/3/3普通写/同起点连续两次换组已有实际验证；同 ID rejoin、controller restart、其他配置及生产验收保持 OPEN。

#### 14. 实现基础ledger recovery与AutoRecovery

依赖步骤13，使用步骤5的grant/fence和步骤12的RepairIntent，分两部分交付。

先做open-ledger recovery：OPEN→IN_RECOVERY、fence、LAC/required-frontier取证、点读、合法recovery Add、匹配的`CLOSED(P,length,context)`和响应丢失解析。复用Classic的write-set、fencing read、W-A+1聚合、顺序推进及write-back，适配Profile身份与错误分类，不重建基础quorum算法。

再做fragment AutoRecovery：先持久化并admit RepairIntent，再授予recovery-only grant；复制后CAS membership、关闭grant、保留清理历史。修复完成不自动授予normal writer，也不触发延期的strong loss reset。

**完成条件：** 缺失证据来自正确且已fence的副本，timeout/unknown不算absence；LAC落后及fragment起点提升时P与length匹配，空ledger为(-1,0)，缺锚点defer、矛盾conflict；失败不伪造close，协调器重启不丢目标和清理义务。Model A随实现验证基础point recovery/durable close，Model E高级Range延期不豁免基础模型。

2026-10-07 [READ_LAC源取证](profile-wire/README.md#read_lac-源取证)已接独立URL1/schema1、完整source entryId/piggyback LAC/sourceLength与真实Get/read/loan路径；EMPTY要求整ledger完整稳定fenced proof且无explicit LAC。源length不授权CLOSED前缀；原SDK LAST_ADD_CONFIRMED/quorum、explicit LAC、length锚点、fencing read与matching CLOSED仍OPEN，步骤14保持PARTIAL。

2026-10-07 [原SDK的LAC覆盖与length锚定](profile-wire/README.md#原sdk的lac覆盖与length锚定)新增显式readLacEvidence：原coverage/write-set与digest校验收集source样本，再取得同一handle metadata快照下的prefix边界length；源length不冒充prefix length，缺锚点或矛盾返回non-OK，不改writer状态。它不是fresh远端RecoveryContext、fencing coverage或CLOSED事实；完整恢复/AutoRecovery及步骤14保持OPEN。

2026-10-07 [恢复上下文与fencing前置](profile-wire/README.md#恢复上下文与fencing前置)已实现真实ZK同记录OPEN→IN_RECOVERY、固定UWRC184恢复token、metadata/lifecycle物理birth guard和同操作重试/重开；显式coordinator复用已验证CONTROL peers及原coverage，只计durable FENCE并重新核对同一上下文。FENCED_CONTEXT不是RECOVERED_AND_CLOSED，完整frontier、恢复Add/close及步骤14仍OPEN。

2026-10-07 [fresh-context只读取证](profile-wire/README.md#fresh-context只读取证)将同一IN_RECOVERY token下的fence/LAC/length与原SDK recovery点读串联；每个missing来源先完成本instance的durable fence，预算终止不等于tail。readonly session与coordinator不写Add/CLOSED，observed boundary不替代完整required authority或恢复完成；步骤14仍OPEN。

2026-10-07 [RepairIntent admission与实际恢复写回](profile-wire/README.md#repairintent-admission与实际恢复写回)新增固定32槽的immutable inert计划、actual inactive INSTALL后的同birth/context admission、UIC3 recovery grant及原DATA client单entry恢复Add；admitted目标在DELETE后仍留存。真实ZK/mTLS/files、9类151项Java、Spotless/Checkstyle及有限341配置（67正向PASS/274指定反例）通过；恢复权限不激活normal writer。grant close、membership/replica-readable publication、required frontier与匹配CLOSED、完整AutoRecovery及步骤14/生产Gate仍OPEN。

2026-10-08 [UIC4 recovery grant close接线](profile-wire/README.md#uic4-recovery-grant-close接线)已将独立exact close authority接到实际CONTROL write/force/owner apply，pre-cut DATA未终结不发布readable；timeout保留UNKNOWN并可显式重连解析同操作，重复close无额外force，不激活normal writer。现有ZK repair reader仍拒绝仅凭admission/grant授权close；membership/copy结果与真实close-authority发布器、matching CLOSED和完整AutoRecovery仍OPEN。

2026-10-08 [历史fragment repair membership与close授权](profile-wire/README.md#历史fragment-repair-membership与close授权)已用同一ZK事务发布标准membership和immutable repair结果，实际close authority随后接入UIC4；bounded head/digest/inverse校验继续采用同一恢复token，physical admission guard防止同UUID/version0重建通过旧cut。真实copy/socket/files与ZK连续commit/reply-loss/delete竞争已有定向验证。最后开放fragment仍需terminal/frontier proof；完整copy coordinator、AutoRecovery和matching CLOSED保持OPEN，不授予normal或strong loss reset。

2026-10-08 [历史fragment完整copy协调与同操作恢复](profile-wire/README.md#历史fragment完整copy协调与同操作恢复)已将原SDK recovery read/digest、单entry actual recovery Add、ZK publication及UIC4 close串成bounded协调入口；取消保留在途permit，copy失败不发布membership，published intent按原close ID解析且不recopy。换组fencing来源补齐未变成员，已验证repair-head metadata版本可继续只读。underreplication worker/scheduling、最后开放fragment/frontier和matching CLOSED仍OPEN。

2026-10-08 [URL2 explicit LAC 双证据](profile-wire/README.md#url2-explicit-lac-双证据与前缀锚点)已在 opt-in schema2 中分别返回原 DATA source 与 durable explicit LAC digest，SDK 校验两份证据后沿用原 coverage，并重新锚定 exact P 的应用 length；损坏 endpoint 排除，timeout/unknown不计 absence。真实 mTLS/ZK/files/SDK、9类176项Java、Spotless/Checkstyle及相关34配置有限模型通过；URL1字节与历史回执保持原绑定。网络 WRITE_LAC、完整 required-frontier/normal-tail oracle、最后开放fragment恢复/CLOSED与AutoRecovery仍OPEN，步骤14及生产Gate不提升。

2026-10-08 [required prefix 与 normal-tail 只读取证](profile-wire/README.md#required-prefix-与-normal-tail-只读取证)已复用原SDK write-set/AckSet/fencing transport，对 required 坐标收齐来源、检查连续性/有效冲突并区分有限exhaustion与unknown；empty、未提交suffix、count/deadline/cancel与context变化已有真实验证。历史copy同步消费完整来源且保留rich失败分类。广度180项与最终4方法、style及相关57配置有限模型通过；root RAT仍有443个既有问题。VERIFIED_NORMAL_TAIL尚不授予CLOSED，最后开放fragment write-back/visibility/匹配关闭、AutoRecovery与生产Gate保持OPEN。

2026-10-08 [固定prefix与真实last-fragment写回](profile-wire/README.md#固定最后-prefix-与真实-last-fragment-写回)已将原读predicate固定为单份UWRF节点及标准metadata的出生绑定UWRS引用；原RepairIntent/UIC3/UIC4支持selected last range的新target复制和current成员SELF recovery Add/close，history穿插/重建/回复丢失/取消已有真实验证。189项广度与最终10项、style及相关79模型配置通过，旧run不重绑；root RAT仍443个既有问题。整体quorum、正常DATA终态visibility、标准matching CLOSED与AutoRecovery仍OPEN，步骤14及生产Gate不提升。

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


2026-10-04 当前后续增量：[受保护的条件 MOVE 与当前位置](local-storage/README.md#受保护的条件-move-与当前位置)已复用现有全局entry/file排空与CONTROL service，接通实际copy字节验证、最多256行条件durable/apply、Arena selector、旧定位业务读/幂等与metadata-only控制重放。旧coverage不能跳过MOVE后的必要DATA，当前target缺失拒绝恢复；未退役历史allocation仍解析CRC。MOVE后源/orphan FREE、原owner native/coverage维护与部分存活compaction、A/B已复用后的C恢复、长期净容量及生产Gate仍未完成。当前六项真实/固定向量回归及16正向/36指定反例见该切片；此前receipt保持各自历史范围。


2026-10-04 最新后续：[搬迁源与orphan整块回收](local-storage/README.md#搬迁源与-orphan-的整块回收)已接通受保护当前后继字节验证、FREE v2 selector applied cut、源/orphan及准确T当前COPY退役与原整块generation复用；纯普通T v1字节不变。真实A→B→C、A/B均复用且无新增checkpoint、storage全量与rotation恢复保留C，坏/缺失C和陈旧proof拒绝。当前四项Java/固定向量及17正向/40指定反例保持局部范围；原owner在线native MOVE/coverage、合批victim compaction、长期净容量与生产Gate仍未完成，之前receipt不重绑。


2026-10-04 最新增量：[当前原owner native MOVE/coverage增量](local-storage/README.md#native-move-索引维护与覆盖恢复)已复用现有checkpoint暂停全部原owner索引、Bookie entry/control及全文件I/O排空、原owner scratch池、256行native更新、flush与A/B coverage发布。当前MOVE目标实际字节校验失败不回退selector；旧native覆盖被拒绝，匹配覆盖恢复仍点读当前必要副本。取消保持保护到实际I/O和全owner终结，页发布失败可继续读并重发。四项新增真实回归及七类271通过/5平台跳过、18正向/44指定反例为当前局部证据；合批victim compaction请求、实际净回收循环、逐allocation在线cut、长期native/control/SST容量、多Arena、OPEN错误恢复和掉电/生产Gate仍未完成。


2026-10-04 当前增量：[同Arena有界合批compaction](local-storage/README.md#同-arena-有界合批-compaction)已组合显式1..16个同stream source的实际扫描、allocation粒度净收益判断、独立destination、一批COPY/条件MOVE、原owner native覆盖维护及一次CONTROL barrier的合批整块FREE。各v2 FREE绑定自身前序cut，共享保护到全部claimed成员终结；取消/发布失败不提前释放或伪造净回收，后续新operation可完成保留source的回收。真实共享块、零净收益拒绝、两块减一块的净容量、全T无destination、复用恢复及失败/取消重启由当前回归验证。仍为全图暂停/一个COPY/显式组，自动选组、逐allocation在线cut、长期control/SST/文件系统净预算、多Arena、OPEN错误恢复、掉电和生产ACK未完成，步骤9/10和完整Gate不提升。


2026-10-04 当前增量：[同Arena多批compaction](local-storage/README.md#同-arena-多批-compaction)在既有显式source组内按owner buffer与256行上限划分COPY/MOVE，所有批次完成后统一native覆盖及合批FREE。净成本包括各批framing/padding总和再按destination整allocation向上取整；后批失败保留已成功selector与所有source，不伪造整组回收。40/402坐标的跨批读取、覆盖/全量恢复及source复用、后批force unknown隔离、零净收益和源预算拒绝已由真实路径验证。源snapshot总预算和既有source batch扫描buffer仍有显式上限；自动选组、逐allocation在线cut、长期净容量、多Arena、OPEN错误恢复和生产Gate保持未完成。


2026-10-04 当前选择增量：[固定shard有界回收](local-storage/README.md#固定-shard-的有界回收选择)按成功ALLOC顺序查看同文件/唯一stream最多16个当前grant；真实DATA测量先于payload捕获，优先直接回收零current live组，最终仍独立whole-allocation后继证明与合批FREE。满容量且无关current live超过源输入预算的实际回收、source generation2复用及17个grant窗口保留年轻共享活数据已验证；既有共享T及多批COPY/MOVE回归改经shard入口。七类283通过/5平台跳过及20正向/51指定反例仅为局部证据。窗口外公平选择、partial任务自动收敛、逐allocation在线cut、长期完整净容量、多Arena、OPEN错误恢复与生产Gate仍未完成。


2026-10-04 当前轮转增量：[有界回收窗口与容量循环](local-storage/README.md#有界回收窗口轮转与有限容量循环)复用固定shard/file映射，按固定本轮ALLOC上限推进进程内游标；新grant留至下轮，首窗口拒绝后可轮到更晚dead并恢复destination，完成轮次后回访旧T。真实17个满容量grant、后续普通refill、1/4 worker及四shard四轮自动FREE/generation1→5、重启读取已验证。七类285通过/5平台跳过及21正向/53指定反例保持局部范围；跨窗口最优组合、部分任务收敛、维护调度、per-allocation cut、长期control/SST/文件系统完整净预算、多Arena、OPEN错误恢复与生产Gate未完成。


2026-10-04 当前池化增量：[池化分批捕获](local-storage/README.md#池化分批-compaction-捕获)复用原owner两个buffer逐批点读/复制，不持整组payload；有限计划以既有坐标上限约束，并在每批重新复核source hash、selector、Bookie cut与测量长度。所有COPY/MOVE、native覆盖及真实整块proof后仍才FREE；池不足或后批身份变化返回PARTIAL并保留实际占用。真实40条/三批超过源输入预算、单buffer拒绝及有效CRC源变化/实际控制变化、恢复验证通过。七类287通过/5平台跳过、22正向/55指定反例保持局部范围；原source batch扫描buffer、全graph暂停和单连续destination限制保留，长期完整净容量、多Arena、OPEN错误恢复、掉电/性能及生产Gate未完成。


2026-10-04 当前部分搬迁增量：[满容量保留tail续写](local-storage/README.md#满容量部分-move-的保留-tail-续写)在新grant无空间时，验证含current COPY的最新同stream grant并保留为目的地，剩余COPY总量须适配其实际空闲tail；只为受保护证明建立最新tail cursor，不回更旧grant，目的地排除FREE。真实PARTIAL后健康重启、1/4 worker、gen1/gen2旧残留tail、21条/两批续写、两源净FREE2 MiB与普通source复用/重启、tail不足及坏字节0COPY已验证。复用新增destination容量为0仍报告实际ID。七类289通过/5平台跳过及23正向/57指定反例保持局部范围；任意partial自动收敛、调度、长期完整容量、多Arena、OPEN错误恢复、掉电/性能及生产Gate仍未完成。

2026-10-04 当前空间准入增量：[前台refill维护容量预留](local-storage/README.md#前台-refill-的维护容量预留)把最大compaction目的地whole blocks与每DATA文件内部预留绑定，单compaction并发下同文件合批refill共同扣减，超过声明目的地上限在ALLOC前拒绝；预算从已apply grant/FREE重建。真实前台拒绝→COPY/MOVE/native/FREE→净恢复→健康重启/gen2再准入及两block工作集已验证。当前仅内部容量，extent碎片、filesystem/SST/control/checkpoint临时空间、低水位/hysteresis、长期负载与多Arena仍未闭合；不关闭B17、步骤9/10或生产Gate。历史回执保留原绑定。

2026-10-04 当前控制维护增量：[运行期控制日志轮转](local-storage/README.md#运行期控制日志轮转)已接入显式graph入口、原CONTROL票据/I/O及两个driver排他名额。在Bookie admission与全DATA文件保护下依次完成双owner rotation，保留真实OPEN session/epoch/pool/游标及CLOSE余量；取消只撤回复，保护留到原owner终结，已尝试失败报告UNKNOWN并停止准入。1/4 worker满额后两轮原pool续写、读/幂等/健康重启、真实pin与force暂停/失败验证通过。七类295通过/5平台跳过及25正向/65指定反例为局部证据；自动调度、filesystem/SST/checkpoint临时工作集、长期负载、多Arena与OPEN错误恢复仍未完成，不关闭步骤8/10、B17或生产Gate。

2026-10-04 当前自动恢复增量：[控制容量压力的自动轮转](local-storage/README.md#控制容量压力的自动轮转)把可fresh-fit的容量拒绝接入既有owner pump和受保护CONTROL rotation；pressure期间暂停新graph准入，已接受entry/I/O/pin排空后再尝试，同步quarantine拒绝也唤醒维护。新journal与原owner终结后恢复原pool续写；超大请求不触发无收益轮转，自动UNKNOWN保留cause并停止准入。真实红/绿、1/4 worker两轮满额/健康restart、单Arena拒绝唤醒及错误验证通过；七类298通过/5平台跳过、26正向/68指定反例为局部证据。提前低水位/一般调度、filesystem/SST/checkpoint临时工作集、长期负载、多Arena与OPEN错误恢复仍未完成，不关闭步骤8/10、B17或生产Gate。

2026-10-04 冷启动容量增量：[满控制日志的健康冷启动](local-storage/README.md#满控制日志的健康冷启动)在真实恢复扫描/必要同步与同一DATA锁之后、下一OPEN之前，仅对确为CLOSED且新journal能容纳OPEN及实际CLOSE/双root预留的Arena执行既有轮转，保留Bookie恢复cut。健康重启、写者排他和失败边界的证据见该节；一般OPEN错误恢复、filesystem临时预算、长期负载、多Arena及步骤8/10/B17/生产Gate仍未完成。


2026-10-04 空间压力维护增量：[数据空间压力的自动回收](local-storage/README.md#数据空间压力的自动回收)把前台pool/refill的内部容量拒绝接到既有owner pump；控制rotation先行，同文件固定shard按冻结cut和16-grant窗口检查，实际pin/Bookie/buffer条件满足后复用native cleanup/COPY/MOVE/FREE。无净收益轮停下等待新控制事实，已知预写入控制容量拒绝先rotation再重新保护，真实UNKNOWN保留cause并停止图。旧生产红、新生产1/4worker自动恢复/当前COPY tail/健康restart、跨shard资源等待、全live与T、控制满额及COPY force失败的局部证据为七类306通过/5平台跳过、28正向/75指定反例。单buffer或长期资源占用没有自动进展保证；低水位/维护份额、filesystem/SST/control-checkpoint临时预算、长期负载、多Arena、全部partial收敛、一般OPEN错误恢复及性能/掉电仍待验证，步骤9/10、B17和生产Gate未完成。

2026-10-04 内部 checkpoint 历史增量：[有界内部保留](local-storage/README.md#内部-checkpoint-历史的有界保留)仅压缩已被后续根取代的成功 CHECKPOINT_COMMIT；当前双 root 身份与最新两个内部 commit 合计至多四项，原业务首次结果、grant/credential/T 与失败条件仍保留，实际物理回收完成后才清理进程内表。[image v2](local-storage/format-v1.md#内部-checkpoint-历史压缩与-image-v2)明确允许该内部 generation 间隔，v1 仍要求原完整历史，准确 predecessor 与必要 suffix 检查保留。真实旧生产预算耗尽红、新生产同 16384-byte 预算固定状态 96 次双 owner 轮转、健康 restart、旧 v1、独立新 v2 向量与单 image 损坏回退已验证；七类 309 通过/5 平台跳过、29 正向/79 指定反例保持局部范围。业务/会话历史压缩、持续创建删除、filesystem/SST/checkpoint 临时总账、长期负载、多 Arena、一般 OPEN 错误恢复、掉电/性能和完整模型仍待完成，步骤 8/10、B17 与生产 Gate 不关闭。

2026-10-05 原 journal checkpoint 恢复增量：[统一恢复入口](local-storage/README.md#原-journal-的压缩-checkpoint-恢复)使 Bookie/Arena 在未物理轮转时也按合法 v2 内部代际间隔恢复，复用轮转路径已有的准确 predecessor、原条件结果和完整必要 suffix 验证；无格式或预算变化。真实旧生产两项红，新生产八次 checkpoint 后选择最高 image、最新 image 损坏后选择上一代并重放 suffix、幂等/失败/冲突/T 拒绝已验证，七类 311 通过/5 平台跳过及 style 通过。本轮扩展有限模型 29 正向完整通过、80 指定反例符合预期，保持局部输入假设；业务/会话历史压缩、长期负载、多 Arena、完整模型及步骤 8/10、B17 和生产 Gate 仍未完成。


2026-10-05 退役 DATA 区间增量：[合并恢复投影](local-storage/README.md#退役-data-序号区间的合并)仅合并准确 stream、连续物理序号且不跨 current grant 原控制顺序的成功退役事实，包括未写 grant 的边界；全部 FREE 的最大必要 Bookie cut 含空退役，缺少时拒绝恢复。真实 24 次同 block FREE/reuse、第 25 次当前 DATA、双 owner rotation/健康 restart、未写中间 grant 与空 FREE 缺少 cut 的反例已验证。七类 314 通过/5 平台跳过、style 通过，有限模型 30 正向完整通过/84 指定反例；历史表与排序工作集仍增长，未实现请求代、完整业务状态裁剪、长期负载或多 Arena，步骤 8/10、B17 与生产 Gate 仍未完成。


2026-10-05 Arena 当前索引增量：[当前授权与历史身份分离](local-storage/README.md#arena-当前授权与历史身份的分离)由 current grant 保存原成功顺序，已用 ID 闭区间与成功高水位保留空洞及 anti-ABA；成功 FREE 增量折叠退役范围，空 FREE 移除当前边界后也重新合并并保留必要 Bookie cut。恢复读者不再排序历史 FREE，但 image 仍保存原业务命令/首次结果并重放，原 UUID、session、MOVE/Bookie T 历史未裁剪，实验格式不变。真实 ID 空洞/失败高 ID/退役身份拒绝、最高 image 与损坏后 fallback、原 grant 顺序、空边界合并和 gen2 DATA 双 owner rotation/健康 restart 已验证；七类 315 通过/5 平台跳过、style 通过，有限模型 31 正向完整通过/89 指定反例。未实现请求代、完整业务 image、总工作集有界、长期负载、多 Arena、掉电/性能或完整模型，步骤 8/10、B17 与生产 Gate 仍未完成。


2026-10-05 当前 COPY 状态增量：[COPY 证明与 selector](local-storage/README.md#当前-copy-证明与-selector-状态)把当前 allocation 内逐条成功 COPY 的 location/UUID/generation/digest 与当前 selector 的 target/身份/原序号/必要 Bookie cut 从历史 MOVE 查询中分离。失去 selector 的成功 COPY 在 allocation 当前时仍保留证明，失败 MOVE 不发布，FREE 删除该 allocation 的全部证明；selector 不持有原整批 MOVE。原历史结果仍保留，image v1/v2 与实验字节不变。真实成功/失败 COPY、最高 image/损坏后 fallback、物理 rotation、FREE/复用/restart 及 force UNKNOWN 回归通过；七类 315 通过/5 平台跳过、style 通过，有限模型 32 正向完整通过/93 指定反例。请求代、独立紧凑业务 image、UUID/session/MOVE/T 历史裁剪、总工作集、长期负载、多 Arena、掉电/性能及完整模型尚未完成，步骤 8/10、B17 和生产 Gate 保持未完成。


2026-10-06 [初始control的真实隔离socket](profile-wire/README.md#真实隔离初始-control-接线)增量：显式 opt-in 实验 capability/schema1 已把 INSTALL/ACTIVATE_INITIAL 接到真实隔离 mTLS、原 endpoint 与physical driver；默认能力及通用control-body冻结状态不变。真实principal、cold authority与exact AuthZ通过后，event loop原子转账；cold timeout/UNKNOWN/断连和实际I/O终结分开，副本持续计费至最后使用。七项socket回归含实际ZK PREPARING/DELETE cut和迟到EL授权零写入。非阻塞实际client、完整READY/metadata reader与all-E coordinator经socket组合、生产listener/readiness和统一总预算仍待完成，步骤11/12与stable/G1/生产Gate保持PARTIAL/OPEN。 相关17类412通过/5平台跳过、style通过；有限矩阵49正向完整PASS/184指定反例，完整源码及receipt绑定见实现记录。


2026-10-06 [初始all-E真实socket组合](profile-control/README.md#可恢复初始协调器的真实-all-e-socket-组合)增量：实际非阻塞initial client借用已有event loops/TLS context，验证actual HELLO/target/incarnation及完整request/reply关联，pending quota在EL排队前取得并等wire/write两种终结，取消/断连不释放仍使用的副本；无自动重试或重连。可恢复初始coordinator现直接经3个mTLS socket与实际ZK PREPARING/READY/标准metadata/原Bookie文件driver完成全E两phase，再返回AVAILABLE snapshot；actual activation reply丢失后显式新client/新coordinator解析同一READY/metadata/op。缺必需metadata仍不重建。8项组合及输出额度RED/GREEN、18类420通过/5平台跳过、style、50正向完整PASS/191指定反例已验证。尚无LedgerHandle/生产writer、普通DATA quorum SDK、replacement/recovery/full delete、完整server-process启动/readiness、统一总预算或production authority，步骤11/12与stable/G1/生产Gate仍PARTIAL/OPEN。 源码与回执绑定见实现记录。


2026-10-06 [普通DATA client](profile-wire/README.md#普通-data-client-与-sdk-接线边界)增量：actual ordinary DATA client复用shared TLS/HELLO/correlation及write/wire quota，借用原DigestManager/v3 ByteBuf/ByteBufList并保留完整status，initial-control公共API不变。真实三目标force后的status进入原AckSet，A=2不计重复target，ordinary路径无authority cold-read。取消、quota/N/context/credential、timeout同坐标解析、force UNKNOWN和reply破坏通过；这是transport/AckSet组合，尚无完整LedgerHandle/PendingAddOp SDK、有界退避/UNKNOWN累积、Profile换组或fenced-close。步骤11/13及stable/G1/生产Gate不关闭。 18类426通过/5平台跳过、style及51正向完整PASS/196指定反例，绑定见实现记录。

2026-10-06 [固定初始 ensemble 的原 SDK 写入接线](profile-wire/README.md#固定初始-ensemble-的原-sdk-写入接线)增量：AVAILABLE snapshot 经 metadata/page、全 E matching activation 与实际 DATA HELLO incarnation 校验后构造实验 ProfileLedgerHandle，复用原 LedgerHandle/PendingAddOp 的坐标、digest、quorum 和连续回调。每目标在原 worker/scheduler 上对同一坐标及保留字节做有限退避；当前 NONE 不清除先前 UNKNOWN，in-flight 超时/本地停止保留不确定性。前台 entry 和目标投递分别有界。真实 3/3/2 的后 entry ACK 先到、slot 背压/前台拒绝、deadline、force UNKNOWN→NONE 和 scheduler 拒绝已验证。Profile 换组、fenced-close、点读/recovery 尚未接通，当前停止只排空本地资源，metadata 保持 OPEN；关闭接口返回 IllegalOp，不能误入 Classic metadata 变更。步骤 13 仍 PARTIAL；步骤 11/12、stable/G1 与生产 Gate 不关闭。新回执与有限模型范围见实现记录，历史快照不重绑定。

2026-10-06 [标准 membership 的出生绑定 CAS](profile-control/README.md#标准-membership-的出生绑定-cas)增量：原 ProfileLedgerMetadataStore 的显式借用 ZK context 现在提供 cold readMembership/replaceEnsemble。一次操作冻结实际父记录 czxid、creator token、immutable backlink、完整 metadata/version 与 LAC+1/mapping；只读 birth child 与 parent version 在同一 ZK multi 中检查，防止 precheck 后删除重建的 version0 ABA。保留 user/custom/credential/state/E/W/A/ctime/creator token，拒绝 recovery/CLOSED、membershipFrozen、pendingPublication 和未知 reserved key；UNKNOWN 不重建缺失 record，不重新捕获新出生身份，不发布 activation/writer authority。九项真实 ZK 验证与 marker guard RED 已覆盖；这仍是步骤13冷路径前置，post-membership authority/历史目标留存、实际 replacement INSTALL/ACTIVATE 与原 SDK gated resend 尚未闭合。步骤11/12/13和生产Gate保持PARTIAL/OPEN，回执见实现记录。

2026-10-06 [replacement 授权与目标留存](profile-control/README.md#replacement-授权与目标留存)增量：现有 lifecycle head增加experimental REPLACEMENT kind4，最多32个immutable predecessor-linked pages；post-membership权限与本次目标引用由同一次head CAS发布，并在同一ZK multi校验标准metadata birth marker/version。DELETE先赢不授权；授权先赢后的DELETE保留initial与全部replacement目标，历史结果不等于当前可写。最新页及fresh matching metadata可投影原POST_MEMBERSHIP_REPLACEMENT冷读；actual reserved file driver现在支持typed ACTIVATE_REPLACEMENT。独立page向量、真实ZK竞争/保留/上限及typed endpoint+真实文件force/apply已验证。新目标的完整inactive INSTALL准备、replacement wire/TLS client、原SDK whole-pending gate与resend、fenced-close/recovery/delete/GC/legacy ACL仍待闭合，步骤11/12/13及production Gate仍PARTIAL/OPEN；回执范围见实现记录。

2026-10-06 [new-peer inactive INSTALL与replacement wire](profile-control/README.md#new-peer-inactive-install-与-replacement-wire)增量：same sidecar持久化bounded next-slot准备页，绑定frozen metadata physical birth/version、旧/新fragment digest、完整INSTALL scope；原metadata birth guard用于准备事务，DELETE/freeze竞争不扩大授权。verified初始或匹配prepare的receipt才能发布replacement。UIC1/schema1原bytes保留，ACTIVATE_REPLACEMENT新增独立experimental UIC2/schema2协商，复用CONTROL连接/TLS/HELLO/correlation/实际write与wire终结。四节点真实ZK、mTLS、文件driver已完成new peer inactive INSTALL→membership CAS→目标留存/授权→replacement activation→durable DATA；lost ACTV response用显式新client解析同operation。普通Add没有逐entrymetadata。SDK自动replacement、whole-pending gate/resend、recovery/fenced-close/delete/GC/legacy ACL与生产Gate仍OPEN，步骤13仍PARTIAL，实际回执见实现记录。

2026-10-07 [原 SDK 写期换组与整体回调门](profile-wire/README.md#原-sdk-写期换组与整体回调门)接通预连接 bounded pool 的真实 inactive INSTALL、membership CAS、retained authority、全新目标 activation 和原 PendingAddOp 多 slot ACK 撤销/必要重发。五节点、两个 pending entry、响应丢失及停止窗口已有真实定向证据；跳过 activation 不能报告 AVAILABLE，UNKNOWN 只能解析原操作，不复活失败 writer。同 ID/incarnation rejoin、registration discovery、controller restart、恢复/删除/GC、生产和完整验收仍待完成，步骤13保持 PARTIAL；本段为当前增量，前述日期回执保留当时范围。

2026-10-07 [3/3/3及同起点连续换组](profile-wire/README.md#333-与同起点连续换组验证)新增实际SDK证据：普通写必须等第三个durable副本，两个pending entry在起点0经历socket1→socket3→socket4两次实际失败/换组后连续回调；未变副本ACK保留、不重发，两个replacement页及全部历史目标留存。仍是隔离有限范围，更多故障交错、同ID incarnation/rejoin、恢复/删除和生产验收保持OPEN。

2026-10-07，[READ_ENTRY工作区进展](profile-wire/README.md#read_entry-接线与验收范围)已完成实际点读terminal observer与候选reply codec；仍在接线，未广告capability、未开放网络读、未产生确定missing。网络授权/覆盖证明/响应ownership/原SDK读取及完整恢复继续OPEN；现有回执保持原绑定。

2026-10-07当前[READ_ENTRY接线工作区](profile-wire/README.md#read_entry-接线与验收范围)已接实际TLS server/client、显式scope权限、fenced完整覆盖的MISSING以及原SDK flags0普通读；三项真实组合通过，native Get/读的取消资源保留已验证。仍未提交，扩大回归/模型、READ_LAC、独立readonly open、fencing read及完整恢复/fenced-close尚未闭合。

2026-10-07 [READ_ENTRY接线与验收范围](profile-wire/README.md#read_entry-接线与验收范围)已接显式读scope权限、URD1/schema1、实际Get/read/pin/loan/response ownership、fenced完整覆盖的MISSING和原SDK flags0普通读。77项相关Java和新增有限读取分发模型通过；没有normal activation或逐entry控制持久化增量。READ_LAC/fencing read/独立readonly open、完整恢复/CLOSED/AutoRecovery/删除及生产验收仍OPEN，步骤11/14保持PARTIAL，历史回执不重绑。

2026-10-07当前[CONTROL客户端FENCE前置](profile-wire/README.md#真实control客户端fence前置)已接真实网络FENCE/同operation幂等/UNKNOWN解析，3项定向通过。原LAC/quorum取证和fencing read仍未接通，source entryId与prefix length锚点必须保留；步骤14保持OPEN。

2026-10-07 [CONTROL客户端FENCE前置](profile-wire/README.md#真实control客户端fence前置)复用冻结100字节body/8字节status、verified peer与原quota/write/wire custody，实际force/同ID ALREADY/credential拒绝及UNKNOWN后同ID重连解析通过。相关59项Java和既有296配置模型矩阵通过；LAC/source/length、fencing read及matching CLOSED仍未接通，步骤14保持PARTIAL/OPEN。

2026-10-08 [非空恢复terminal cut与matching CLOSED](profile-wire/README.md#非空恢复-terminal-cut-与-matching-closed)已接原UIC4语义能力UIC5、接收端native type11持久P上界、全部pre-cut normal/recovery I/O排空、distinct原quorum ACK及标准CLOSED(P,length)同出生/版本/lifecycle/selection CAS。旧type9同操作升级在checkpoint/rotation后仍保留cut；真实回复丢失/new coordinator/restart、3/3/2和3/3/3已有验证。当前完成状态只代表matching CLOSED元数据事实与已接收节点的本地cut；empty-last close、独立readonly-open、离线追赶/rejoin、AutoRecovery和完整步骤14/生产Gate仍OPEN，历史回执不重绑。

2026-10-08 [空最后fragment的独立终态关闭](profile-wire/README.md#空最后-fragment-的独立终态关闭)新增同一实验CONTROL内的UIC6/schema5/op0x0108、P/L/selection digest绑定与Bookie type12；仅既有activated当前成员、P=lastStart-1，复用cold reader/原close driver/distinct ACK及matching CLOSED CAS，不制造grant/copy。A2/A3的(-1,0)和历史CLOSED(2,48)、回复丢失/cancel/restart已有真实验证；非空路径保持UIC4/type11。readonly-open、AutoRecovery/离线追赶等仍OPEN，步骤14与生产Gate不提升。

2026-10-09 [独立CLOSED readonly-open](profile-wire/README.md#独立-closed-readonly-open-与原-sdk-读取)已从实际matching recovery-CLOSED独立核验context/selection/birth/历史与live delete gate，复用原ReadOnlyLedgerHandle/digest/point read。构造后最后cold复核、固定P/L、confirmed/unconfirmed上界、bounded read quota与cancel custody、本地close无metadata/control写已有真实SDK/TLS/ZK/files验证；默认BookKeeper.open routing、普通writer closed adoption、AutoRecovery/离线追赶等仍OPEN，步骤14及生产Gate不提升。

2026-10-09 [整体fenced point recovery](profile-wire/README.md#整体-fenced-point-recovery-入口与持久身份续接)已把现有metadata/fence/SDK point oracle/selection/SELF write-back/terminal quorum/matching CLOSED串成单attempt入口，采用已发布context/selection/intent/close ID续接，cancel/deadline保留actual gate；A2/A3空/非空及历史末段、A3不足quorum后resume、required hole/limit已有真实验证。RECOVERED_AND_CLOSED只消费matching close，不清marker或做strong reset；AutoRecovery、普通writer close adoption、离线catch-up等仍OPEN，步骤14及生产Gate不提升。

2026-10-09 [CLOSED fragment repair](profile-wire/README.md#closed-fragment-repair-与固定-close-history-cut)新增同一次standard CLOSED CAS的UWCF80 close-history reference，关闭后copy/membership publication沿用原context/P/L及birth/inverse验证，目标inactive/grant/Add/close复用。真实A2/A3连续新目标换组、回复丢失/fresh snapshot零recopy及target restart通过；原readRecovery拒绝CLOSED、不清marker，ReplicationWorker route/最终健康清理及生产Gate仍OPEN，步骤14保持PARTIAL。

2026-10-09 [CLOSED 全副本健康复核](profile-wire/README.md#closed-全副本健康复核)新增独立 matching CLOSED 全副本复核：逐坐标要求所有 exact write-set DATA digest/payload/累计 length 合法，再重验完整 metadata birth/version/context/head/sources 与 DELETE。空 ledger/末段要求所有当前目标匹配 incarnation 和 durable FENCE；closed fence 已包含已发布 inactive repair 目标。真实 historical hole、CLOSED repair 后完整、取消/deadline实际 gate 和 DELETE/recreation 验证见该记录。私有 proof 只绑定该 snapshot，未消费 worker lock/marker 同事务清理条件，ReplicationWorker 接线与 marker 清理、步骤14及生产 Gate仍PARTIAL/OPEN。

2026-10-09 [Profile marker 条件化清理](profile-wire/README.md#profile-marker-条件化清理)新增显式 completeReplication：领取在health read前绑定原task的coordinator/deadline/单次健康proof，再fresh读取CLOSED/DELETE，以同一个ZK multi消费metadata/lifecycle和原worker session witness、marker physical birth/version，并删除原task/lock。默认OFF，UNKNOWN保留义务，受控release/session终止不删除replacement lock；强管理reset仍disabled。原ReplicationWorker自动路由、候选/intent续接与hierarchy/page GC尚未接通，步骤14及生产Gate仍PARTIAL/OPEN。

2026-10-09 [原ReplicationWorker的Profile自动循环](profile-wire/README.md#原-replicationworker-的-profile-自动循环)新增显式默认OFF cohort constructor，复用原thread/factory/backoff/stats与恢复、真实COPY、完整W健康proof及原task guarded removal；A3在fixed selection下先修last再close，published close丢回复用原ID零recopy，stop/shutdown等actual child排空才release。实际A2/A3 OPEN/CLOSED/空健康与历史fragment、重启/后续损失及Classic两项回归见该记录。空缺副本grantless repair、service bootstrap/动态pool/rejoin/delete/GC及综合Gate仍OPEN，步骤14保持PARTIAL。

2026-10-09 [空副本grantless membership repair](profile-wire/README.md#空副本的-grantless-membership-repair)复用原固定32槽/INSTALL/admission及UIC6，实际terminal force/apply后同事务发布membership与hash绑定UWEB124 receipt；空UWRP/UWRM显式schema2/kind1，DATA schema1向量不变。A2/A3空ledger与历史空末段、目标文件restart、reply丢失同ID续接、stop/DELETE及force失败/receipt损坏均有真实验证；零DATA/grant/normal，后续完整W健康与原task guarded removal沿用。步骤14仍PARTIAL，service bootstrap/动态pool/rejoin/delete/GC及综合生产Gate仍OPEN。

2026-10-09 [registration消费者发现与再次复核](startup-readiness/README.md#消费者发现与再次复核)新增既有ZK store的独立reader读取，核验持久READY/birth、两份ephemeral projection、实际publisher owner/nonce marker和末次物理身份；同字节重建/更新/expiry等真实ZK验证见该记录。只读观察不授予transport或ledger authority，后续仍须mTLS/HELLO及再次冷复核。service bootstrap/动态pool尚未接通，步骤12/14及生产Gate仍PARTIAL/OPEN。

2026-10-11 [注册观察驱动的连接与实际本地排空](profile-wire/README.md#注册观察驱动的连接与实际本地排空)接既有consumer观察、真实mTLS/HELLO和最后cold复核；CONTROL/DATA client保持原RPC超时，closeAsync等待cold/channel/HELLO输出及实际RPC借用与promise terminal。发现/关闭不授予ledger authority或remote abort/NONE/FREE；动态pool、service bootstrap和总预算尚未接线，步骤12/14及生产Gate仍PARTIAL/OPEN。

2026-10-11 [worker/service关闭边界](profile-wire/README.md#worker-与服务关闭的中断及并发边界)修复join中断和重复shutdown提前关manager/SDK、self-join及启动前关闭；caller signal在procedure结束后恢复，AutoRecoveryMain不把继承signal送进SDK等待。真实READ/task与真实metadata client RED/GREEN见记录；动态pool/Profile bootstrap尚未接通，步骤14与生产Gate保持PARTIAL/OPEN。

2026-10-11 [registered cohort pool与worker lease](profile-wire/README.md#注册-cohort-peer-pool-与实际-worker-lease)消费真实READY/mTLS/HELLO创建固定snapshot，建立/借用/排空共同保守计费connection和RPC-slot预算；原worker在实际lease drain后才清原task/marker。实际缺副本修复、held promise/cold及episode/incarnation验证见记录；service bootstrap、全registry/字节预算与生产Gate仍OPEN，步骤12/14保持PARTIAL。

2026-10-11 [显式AutoRecovery service bootstrap](profile-wire/README.md#显式-autorecovery-service-bootstrap-与-owned-资源)由服务自身SDK/ZK创建原coordinator/pool并消费真实任务；owned manager/SDK/cold/group在实际worker/lease排空后关闭，失败也收尾，默认入口OFF。Classic periodic payload/replica/placement audit非0明确拒绝；完整Profile availability/audit、字节预算与综合生产Gate仍OPEN，步骤12/14保持PARTIAL。

2026-10-11 [SDK关闭期间的新中断](profile-wire/README.md#sdk-关闭过程中的新中断)已真实RED/GREEN验证并修复：原Main使用独立一次性SDK closer，caller中断只记账并等实际procedure结束，随后回收cold/group并恢复signal。48项Java与相关54模型配置得到预期结果；Profile availability/audit、全字节预算及步骤12/14/生产Gate仍PARTIAL/OPEN。

2026-10-11 [Auditor实际排空与中断](profile-wire/README.md#auditor-排空与-owned-sdk-关闭)已真实RED/GREEN验证原audit executor等待中的interrupt不再提前进入SDK关闭；Auditor/elector ownBkc复用one-shot closer。51项Java（含Classic auditor关闭/连续换选举）与相关55模型配置得到预期结果，runner绑定实际owner源码；Profile availability/audit及步骤12/14/生产Gate仍PARTIAL/OPEN。

2026-10-11 [Profile成员故障发现](profile-wire/README.md#profile-成员故障发现与原-auditor-接线)已接原auditor启动/cohort watch/periodic扫描与原任务生成，按descriptor/instance/retained incarnation和fresh cut核验；Classic同名registry不掩盖Profile缺失，UNKNOWN/CORRUPT不计缺失。真实自动任务→COPY/成员替换→全W复核/排空/marker清理通过；注册观察不授予repair/normal权限。周期DATA审计、总预算、rejoin/delete/综合Gate仍OPEN，步骤12/14保持PARTIAL。

2026-10-11 [共享索引与legacy退役拒绝](profile-wire/README.md#共享引用索引与-legacy-退役拒绝)修复Profile过滤影响共享BookKeeperAdmin的问题：公开索引恢复全引用，仅Classic auditor过滤Profile；legacy退役对已知ensemble/marker中的Profile引用明确拒绝，避免无marker时提前成功。真实退役RED/GREEN与公开index/Classic实际任务范围验证通过；完整Profile退役/retained-history清理仍未实现，生产Gate不提升。

2026-10-11 [CLOSED周期DATA审计](profile-wire/README.md#profile-closed-data-周期审计)已复用原replica-check CLOSED范围、streamed metadata与真实全W验证，共用单peer pool/coordinator预算，actual health/lease drain与最后fresh cut后只生成检查任务。真实历史DATA hole→原worker修复/全W/marker闭环、健康/权限拒绝不clear、deadline/关闭保留actual资源通过。OPEN/正常writer close、payload/placement、总字节预算、离线/rejoin/delete与生产Gate仍OPEN，步骤12/14保持PARTIAL。


2026-10-11 [SDK writer stop 与实际 DATA 排空](profile-wire/README.md#sdk-writer-stop-与实际-data-排空)已真实 RED/GREEN 修复逻辑 delivery 完成早于排队源/output promise 终结的边界；stop 等自身全部普通 DATA attempts 的实际终态，caller cancellation 不污染共享 completion，借用 socket 保留，UNKNOWN/OPEN 不变。35 个相关 SDK/socket/recovery/worker 用例零失败/skip，17 个有限模型配置得到预期结果，style 通过；RAT 保留 443 父 HEAD 历史问题、无新增。此为普通 close 的必要排空依赖；冻结 P/length 与 durable terminal/metadata close、adoption 尚未接通，步骤 12/14/15 和生产 Gate 仍 PARTIAL/OPEN。


2026-10-11 [普通 writer close 与固定 cut 续接](profile-wire/README.md#普通-writer-close-与固定-cut-续接)在显式 cold 依赖下已接原 SDK close：冻结确认P/L、actual DATA排空、同CAS writer cut/UWRC schema2、固定prefix验证与既有SELF/terminal/标准CLOSED。A2/A3空/非空物理tail、健康文件restart、caller cancel/output、enter回复窗口新session续接、cut损坏拒绝、历史replacement与丢CLOSED回复均有真实证据；旧schema1向量不变，旧严格reader拒schema2为源码检查，不计二进制兼容PASS。阶段广度102/73/74与最终8项、27模型配置各按原source绑定保留，style通过，RAT443历史问题无新增。该有界close/adoption切片不闭合默认SDK路由、其他required authority、OPEN/payload/placement、full-bytes、删除/离线/rejoin/GC与综合Gate，步骤12/14/15仍PARTIAL/OPEN。


2026-10-11 [Fenced-CLOSED 集群逻辑删除与目标义务](profile-wire/README.md#fenced-closed-集群逻辑删除与目标义务)已显式接matching CLOSED前置、原root delete cut、标准metadata不可逆seal、完整initial/replacement/admitted-repair targets及出生绑定cluster tombstone。同起点覆盖目标/离线incarnation保留，三段回复丢失续接，cancel等actual cold completion，manifest缺失/重建拒绝；新open拒绝、旧reader未本地tombstone前可读，未冒充physical/access barrier。76与14项Java、27有限模型配置各按当前source绑定通过，style通过，RAT443历史问题无新增。实际delivery/wire/delete-applied/cursor/catch-up/rejoin/回收与GC/默认SDK routing及综合Gate仍未闭合，步骤12/14/15保持PARTIAL/OPEN。


2026-10-12 [初始共享 delete delivery journal](profile-wire/README.md#初始共享-delete-delivery-journal)已复用logical tombstone/完整target，实际ZK同事务提交event/index/连续head；一个Bookie/incarnation跨两真实ledger共用initial stream，旧绑定不可盲建新stream，丢回复/并发seq赢家/缺失与同字节重建拒绝已有证据。阶段79与最终7项Java、15配置有限模型各保留原source，style通过，RAT443历史问题无新增。4096-event有限prototype无snapshot/GC扩容，不授予本地cursor/registration authority；实际TOMBSTONE/apply、catch-up/rejoin/physical与综合Gate仍OPEN，步骤12/14/15保持PARTIAL。


2026-10-12 [UIC7 本地 delete-applied 与连续 cursor](profile-wire/README.md#uic7-本地-delete-applied-与连续-cursor)已将committed event/实际cold authority接到mTLS CONTROL与原Bookie type13 write/force/owner apply，TOMBSTONE和连续cursor同record；gap写盘前可重试，旧seq不回退，UNKNOWN不发布cursor，absent ledger仍有durable effect坐标。实际非空/空两ledger、旧读取/目标拒绝、held pin、丢目标回复/fresh重试、force失败、checkpoint rotation/restart有定向证据。158项相关Java、23个有限模型配置（3正向/20指定反例）及style通过，RAT443历史问题无新增；原history receipt保留；registration required-through cut、startup/catch-up/rejoin、snapshot/handoff/physical/GC及综合Gate仍OPEN，步骤12/14/15保持PARTIAL。


2026-10-12 [初始 required-through cut 与真实 rejoin 追赶](profile-wire/README.md#初始-required-through-cut-与真实-rejoin-追赶)已绑定原恢复control cursor与实际shared assignment/head，把head版本/birth/assignment检查加入原writable multi；delete先赢需继续追赶，普通head增长不demote既有registration。原graph实际restart、bounded LIMIT/UNKNOWN/独立completion、真实seq2注册竞争、DLR force、held pin和丢注册回复同session解析已有证据。101项相关Java、20个有限模型配置（3正向/17指定反例）及style通过，RAT443历史问题无新增。仅explicit initial gen1；在线delivery/default bootstrap、其他required facts、snapshot/handoff/GC/decommission与综合Gate未闭合，步骤12/14/15保持PARTIAL/OPEN。


2026-10-12 [全目标 publication 与在线节点消费](profile-wire/README.md#全目标-publication-与在线节点消费)已把每target event/index/head与publication前缀同事务记录，全部target就绪才发布logical tombstone，消除logical success后journal尚未可见的窗口；PREPARED不授予local delete。schema2 reference绑定prefix birth，原schema1 bytes保留；原注册graph借用bounded cold/scheduler持续消费，UNKNOWN/stop等actual local completion，close未排空保留锁。真实自动投递/注册race/cancel续接/required prefix loss/held force已有证据；阶段176与当前10项Java、31个有限模型配置分别保留原source，style通过，RAT443历史问题无新增。default bootstrap、snapshot/handoff/GC/退役、其余facts/full-bytes/综合Gate仍OPEN，步骤12/14/15保持PARTIAL。


2026-10-12 [实际恢复 graph 与原 startup 发布桥接](startup-readiness/README.md#实际恢复-graph-与原-startup-发布桥接)已将actual device/allocator/route/delete facts接回原Harness、DLR/ZK发布/session admission及既有consumer，manifest/client不匹配、consumer scheduler拒绝会拒绝或退休/demote。广度77项与最终1项真实组合、20配置有限模型按阶段source保留，style通过，RAT443父HEAD历史问题无新增。兼容性fence与credential/new-scope真实issuer、默认bootstrap/SDK routing、snapshot/handoff/GC、full-bytes及综合验收仍OPEN；步骤12/14/15保持PARTIAL，生产Gate不提升。


2026-10-12 [持久startup requirements与冻结发布条件](startup-readiness/README.md#持久-startup-requirements-与冻结发布条件)已接独立immutable ZK兼容格式/单Arena expected set、显式构造前检查及writable同事务frozen source guard；caller自报未来runtime不能改变本实现version1。阶段85/最终26项Java、相关18/最终4配置模型分别保留source，style通过、RAT443历史问题无新增。required声明仍不能证明stock fence或物理credential隔离，default bootstrap/SDK routing、snapshot/handoff/GC/full-bytes/综合验收未闭合，步骤12/14/15与生产Gate保持PARTIAL/OPEN。


2026-10-12 [实际Unix credential new-scope快照](startup-readiness/README.md#实际-unix-credential-的-new-scope-access-快照)已验证native双UID访问完整bounded现有文件及可读marker，真实0711/0644反例修复，access/canonical roots导入原actual restart/source/readiness组合。最终31项及style通过，RAT443历史问题无新增；这只是ACCESS_SNAPSHOT_ONLY，old-service disposition/privilege/stock binary/default bootstrap与完整fallback和综合验收仍OPEN，步骤12/14/15及生产Gate保持PARTIAL/OPEN。


2026-10-12 [实际old Classic episode与new-scope replay组合](startup-readiness/README.md#实际旧-classic-episode-的有序关闭与-new-scope-组合)已真实SDK写读/原stack与driver关闭/actual JVM退出/原session registration终态，owned observation接入actual Unix access与原startup组合；closed-live和abrupt-expired absence不等于DRAINED。最终34项/style通过，RAT443历史问题无新增。当前构建与受控episode证明不覆盖外部service authority、released stock矩阵、永久退役或default hook，步骤12/14/15/生产Gate与综合验收仍PARTIAL/OPEN。


2026-10-12 [显式Profile listener与actual网络credit drain](profile-wire/README.md#显式-profile-listener-与实际网络-credit-排空)已管理native parent/child、worker交接及receive ledger；retire新连接不提前归还frame/cold/backing，caller copy completion不污染actual终态，borrowed graph/loops/cold保持原owner。46项相关Java/13配置模型得到预期结果，style通过、RAT443历史问题无新增。完整startup owner/default hook/SDK routing、full-bytes、stock binary及综合验收仍OPEN，步骤11/12/14/15和生产Gate保持PARTIAL/OPEN。

2026-10-12 [显式 Profile 服务 owner](startup-readiness/README.md#显式-profile-服务启动与-owner-排空)已连接 requirements/pre-open Arena/Index 根绑定、实际 native bind endpoint、原恢复/Harness/registration/consumer、注册发现 SDK 和 listener→storage→DLR 实际排空。held DATA force 与 publication stop 保留 owner/locks，actual old Classic drain/Unix access 接入 existing restart；最终58项 Java、style通过，RAT443父 HEAD 路径无新增。default EmbeddedServer/完整外部 authority、stock矩阵、snapshot/handoff/GC/decommission/full-bytes/性能/运维和综合验收继续 OPEN，步骤11/12/14/15及生产 Gate保持 PARTIAL/OPEN。

2026-10-12 [EmbeddedServer 显式 Profile hook](startup-readiness/README.md#embeddedserver-显式-profile-分支与-lifecycle)已在 Classic 构造前选择隔离 owner 分支，原 stack 驱动实际 startup/SDK/restart；关闭失败保留非终态并阻止 stack 释放更早资源，handler 返回不伪造 STARTED。最终28项/1100源码输入及 style通过，独立RAT443父 HEAD 路径无新增；Classic 默认保留。配置/bootstrap factory、完整外部 authority、stock矩阵及其余综合验收仍 OPEN，步骤11/12/14/15和生产 Gate保持 PARTIAL/OPEN。

2026-10-12 [配置转换与显式 bootstrap](startup-readiness/README.md#配置转换与显式-bootstrap)已冻结原有 bounded layout/execution/batch 参数并公开原 typed 环境装配；actual properties load 后复用原 EmbeddedServer/SDK/restart。unknown/multi-value/non-integer/越界及 ISOLATED 无 owner 在 Classic/storage open 前拒绝，真实 prefix enumeration red/green保留；最终30项/1100源码输入与style通过，RAT443父 HEAD 问题无新增。Main/external authority/owned environment 和其余16步验收仍 OPEN，生产 Gate 不提升。

2026-10-12 [同一 episode owned资源](startup-readiness/README.md#同一服务-episode-的-owned-资源初始化与终止)已接 deferred native ZK/原metadata driver-LM、bounded cold/scheduler/loops，原服务实际drain后才确认各终态；metadata scheduler held任务、初始化失败部分资源、三节点SDK/restart得到最终33项/1103输入与style证据，RAT443父HEAD无新增。TLS/physical issuer仍借用，完整Main/外部authority/stock矩阵/full-bytes及其他综合验收继续OPEN，生产Gate不提升。

2026-10-12 [OPEN 固定 LAC 前缀审计](profile-wire/README.md#open-固定-lac-前缀审计)已复用原 SDK LAC、flags=0 普通读、全W扫描和周期任务共享池，只观察冻结下界P/L，不fence writer或授予CLOSED proof/marker清理。实际writer并行增长、损坏/权限/metadata变化、UNKNOWN与稳定缺失、deadline/actual drain及共享恢复回归共28项通过；20模型配置得到2完整正向/18指定反例，style通过、RAT443父HEAD无新增。其余required authority、完整审计/总预算、Main/外部authority/stock矩阵、GC/退役/性能/运维和综合验收仍OPEN，生产Gate不提升。

2026-10-12 [网络 WRITE_LAC 与确认前缀生产](profile-wire/README.md#网络-write_lac-与确认前缀生产)已接原网络WRITE_LAC、durable control force/apply与SDK确认prefix的distinct AQ，fence后只解析同值，UNKNOWN/cancel不提前归还actual output/wire custody；既有URL2/required-prefix实际消费新producer。7类337项Java和29有限模型配置（5完整正向/24指定反例）通过；其他required authority/自动策略/默认routing/总预算/Main/stock/GC/退役/性能/运维及全16步综合验收仍OPEN，生产Gate不提升。

2026-10-12 [本地删除空间终结](profile-wire/README.md#本地删除空间终结)已将实际terminal TOMBSTONE接到原native index/coordinate/coverage及COPY/MOVE/FREE，按实际allocated DATA/旧COPY复核本地REUSABLE；混合块可零净容量增益退休，普通维护仍要求正净增益。旧IO/owner credit与deadline/cancel独立排空，其他ledger保留、corrupt/limit不伪造成功。3类282项Java与20有限模型配置（4完整正向/16指定反例）通过；网络physical receipt/全目标/retention/GC/退役及其他16步验收仍OPEN，生产Gate不提升。


2026-10-12 [网络删除空间查询与全目标观察](profile-wire/README.md#网络删除空间查询与全目标观察)已接独立UIC8/OPERATION_STATUS、同一持久UIC7事件和原mTLS/direct/exact权威检查，实际FREE后fresh查询才报告REUSABLE；完整历史manifest逐target/incarnation观察，缺peer保留PENDING，全部REUSABLE仍是ephemeral结果。durable terminal receipts/retention/GC/退役及其他16步验收继续OPEN；默认OFF/SYSTEM/PARK和disabled能力不变，生产Gate不提升。
本里程碑349项原阶段Java/70项最终源码复核与24有限模型配置通过，style通过；独立RAT443父HEAD无新增。证据与最终1874源码输入由上述profile-wire章节拥有，阶段结果不移绑。


2026-10-12 [删除物理终结记录与持久接续](profile-wire/README.md#删除物理终结记录与持久接续)已把实际UIC8 REUSABLE绑定完整事件/request/target/incarnation，条件化存入原ZK不可变row与birth guard；新await入口按完整历史targets重读全部记录，支持coordinator重建及已有记录的实际离线目标。原观察入口保持只读，PENDING/UNKNOWN无记录，不删除历史义务或授予GC权限。最终6类105项Java/31有限模型配置（4正向/27指定反例）及style通过；retention、accepted decommission/unrecoverable、snapshot/handoff/history GC和其他16步验收仍OPEN，生产Gate不提升。
原105项阶段保留source绑定；最终还原同bytes/version/guards的metadata birth ABA补验2项通过，31模型与1884当前输入一致，style通过，RAT443父HEAD无新增。


2026-10-12 [删除流完整 snapshot 与真实启动追赶](profile-wire/README.md#删除流完整-snapshot-与真实启动追赶)已将原generation1 shared stream的完整prefix保存为有界immutable pages/root＋conditional publication witness，绑定scope/原event birth并接入实际TOMBSTONE/cursor启动追赶。真实缺prefix＋suffix、65-ledger两page、missing/recreate、取消/回复丢失及并发有111项Java/23有限模型配置（3正向/20指定反例）证据；无旧journal删除、assignment handoff或GC权限。retention/terminal退役/history GC及其他16步验收继续OPEN，生产Gate不提升。


2026-10-12 [Assignment handoff 的 PREPARED 与投递 cut](profile-wire/README.md#assignment-handoff-的-prepared-与投递-cut)已把完整native snapshot/COMMITTED effects接到原ZK admission/head同multi排序，两个实际delete publisher路径都不能跨冻结cut；旧G1 DATA与已记录effect仍有效，计划不生成local G2或EFFECTIVE事实。114项原阶段/最终6项Java和23有限模型配置（3正向/20指定反例）通过；local adoption/ACK/EFFECTIVE/route retirement、retention/GC及其他16步验收仍OPEN，生产Gate不提升。


2026-10-12 [Assignment handoff 的本地持久 adoption](profile-wire/README.md#assignment-handoff-的本地持久-adoption)已将实际PREPARED Plan接入原Bookie type14 force/owner apply/checkpoint/replay；必须先完成原G1 cut，空cut不生成effect，重复相同Plan不再force，普通G1 DATA保持有效。等待超时不释放实际I/O，fresh native admission ABA拒绝重新确认。最终4类27项Java、20有限模型配置（3正向/17指定反例）及style通过；native catch-up ACK/EFFECTIVE CAS/old-route retirement、retention/GC及其他16步验收仍OPEN，生产Gate不提升。


2026-10-12 [Assignment handoff 的 native ACK](profile-wire/README.md#assignment-handoff-的-native-ack)已将原storage graph实际force/apply及owner终态产生的不可公开构造凭据，接到原ZK conditional immutable ACK row/birth guard。完整Plan、local control sequence与native PREPARED/cut绑定，丢回复/取消后重建读取首条原记录，已关闭owner不能新确认；ACK保持G1 cursor/assignment与原义务。最终6类32项Java、26有限模型配置（4正向/22指定反例）、1919源码输入及style通过；conditional EFFECTIVE/G2 journal与registration/old-route retirement、retention/GC及其他16步验收仍OPEN，生产Gate不提升。


2026-10-12 [Assignment handoff 的 EFFECTIVE 与 G2/G3 接续](profile-wire/README.md#assignment-handoff-的-effective-与-g2g3-接续)已把原graph gate退役、owned registration删除、NON_WRITABLE投影与native assignment/head更新放进原publisher同一ZK multi；原type15 force/apply/checkpoint/replay接续effective progress，原事件身份保留。三目标真实G2/G3 READY、SDK Add/close、新旧事件与混合snapshot、健康文件旋转重启、native丢回复/held callback与type15 UNKNOWN已有验证。79项原阶段Java（server 71/common codec 8）/最终21项补验及35有限模型配置通过，1929个最终源码输入核验一致，style通过、RAT443父HEAD无新增；完整Gate不提升。retention/offline/backup-restore、accepted terminal decommission/unrecoverable/history GC、其他required authority/default routing/full-bytes/兼容/正式性能/运维/全16步验收继续OPEN。


2026-10-12 [删除 retention policy 与原子 receipt](profile-wire/README.md#删除-retention-policy-与原子-receipt)已将显式offline/rejoin、metadata propagation、backup/restore窗口绑定到原逻辑删除的native同事务metadata/immutable receipt，保留策略历史与全部target义务。策略竞争整体拒绝，旧窗口不随新策略缩短，无策略历史继续受保护。6类38项真实回归、29有限模型配置及1938冻结source通过，style exit0；RAT443父HEAD路径无新增。BOUND不授权GC，实际backup/restore/rejoin接受、历史GC与全16步其余验收继续OPEN；默认OFF/SYSTEM/PARK和生产Gate不提升。


2026-10-12 [删除 journal prefix 的实际 compaction](profile-wire/README.md#删除-journal-prefix-的实际-compaction)已接完整snapshot与同incarnation EFFECTIVE cut后的bounded native duplicate-row删除，并接通删后physical receipt持久化。真实G2/G3、snapshot publication竞争、回复丢失/held cancel、rotation/restart有6类34项原阶段及最终1项补验、31有限模型配置和1946冻结source证据。全部effect/index/Manifest/tombstone/retention/physical及handoff义务保留；snapshot/page/lineage/target GC、backup restore接受和16步其余验收仍OPEN，默认OFF/SYSTEM/PARK及生产Gate不提升。


2026-10-13 [客户端 transport backing 与实际关闭](profile-wire/README.md#客户端-transport-backing-与实际关闭)已把原SDK/cohort客户端TLS/帧及普通/恢复Add、WRITE_LAC私有snapshot接入现有backing ledger，实际close等待最后view归还；cohort原slot上限之外可观察其transport backing与固定quota。最终4类69项Java、24有限模型配置及1946当前source/style通过。仅transport ByteBuf计量，opaque TLS/codec/caller数组及存储/index/control等统一总账和正式性能仍OPEN；默认OFF/SYSTEM/PARK、disabled与完整生产Gate不提升。


2026-10-13 [客户端 codec workspace 与有界 reply 解析](profile-wire/README.md#客户端-codec-workspace-与有界-reply-解析)已在原backing账本中预留临时codec byte数组工作集，并删除fallback full-frame多层副本；真实CONTROL发送前拒绝、已协商read reply拒绝与最后owner drain得到验证。8类93项Java（common21/server72）、24有限模型配置、1946当前source及style通过。只完成客户端暂存工作集，caller/长期对象、server/control/cold/storage/index及opaque TLS等总账和全16步Gate仍OPEN，默认与disabled能力不提升。

2026-10-13 [接收端 workspace 分类与响应 backing 转交](profile-wire/README.md#接收端-workspace-分类与响应-backing-转交)已复用原connection/lane/Credit区分接收端解析、冷任务及响应workspace，按原quota在响应分配前原子转成backing；CONTROL转交失败不改账，caller/transport终结不提前结束actual cold/force/last view。4类47项原阶段Java及1项定向补验、7个原backing有限模型配置有分阶段输入证据。这里完成预算分类与owner转交，未补齐MetadataStore内部、长期对象、caller/opaque TLS/kernel或storage/index统一总账；默认OFF/SYSTEM/PARK、disabled和全16步生产Gate保持OPEN。

2026-10-13 [固定存储池与网络共享 DATA 预算](startup-readiness/README.md#固定存储池与网络共享-data-预算)已在原ProfileReceiveBudget中于storage open前保留固定对齐池全部root容量，网络背压不能侵占这份完成工作集；listener只等待实际connection/cold/output排空，service在实际storage owner释放池后归还reservation。最终5类67项中62项实际通过、5项Linux条件跳过，17原有限模型配置结果符合预期；未闭合索引/控制/长期对象/opaque依赖/cohort总账、正式性能与16步综合Gate，默认OFF/SYSTEM/PARK及disabled不变。

2026-10-13 [派生 index block cache 与共享 DATA 预算](local-storage/README.md#派生-index-block-cache-与共享-data-预算)已将原1 MiB index block cache启用strict capacity，并在ProfileService storage open前与固定池合并预留于原DATA额度；原缓存大小与协议/格式不变。旧service只预留池的真实quota红、当前拒绝绿及最终2类34项真实Java通过；memtable/WBM并非hard native上限，所有native/对象总账与正式性能仍OPEN。本轮不重跑不覆盖该native政策的TLC，旧模型保持原绑定，默认OFF/SYSTEM/PARK及16步生产Gate不提升。

2026-10-13 [全目标终结屏障与单调 retention 窗口](profile-wire/README.md#全目标终结屏障与单调-retention-窗口)已在全部历史target的原native REUSABLE receipts后原子建立retention barrier，保留原policy/receipt出生关系；coordinator使用opaque本地单调观察，重启重新等待完整窗口，每次检查重验native事实。6类30项Java与9有限模型配置有分阶段证据；elapsed不授权GC，backup/restore接受、accepted decommission/unrecoverable、剩余history GC及全16步Gate继续OPEN，默认OFF/SYSTEM/PARK及disabled不变。

2026-10-13 [默认 SDK open 的 Profile 拒绝边界](profile-wire/README.md#默认-sdk-open-的-profile-拒绝边界)已在原LedgerOpenOp密码/digest验证后、Classic handle/listener/recovery创建前识别mandatory Profile backlink，返回原IllegalOpException，避免当前SDK误用Classic路径。真实CLOSED旧行为红、新OPEN/CLOSED/valid-CRC future marker拒绝绿及三Bookie Classic兼容，最终2类17项通过；Main/外部issuer、默认Profile routing、released stock矩阵及全16步Gate仍OPEN，默认OFF/SYSTEM/PARK与disabled不变。

2026-10-13 [恢复执行的 released compatibility 矩阵](profile-wire/README.md#恢复执行的-released-compatibility-矩阵)：父源码ea98f8b20bebcf639c3ddf07effdeae2aebfd7e3上四个released decoder和真实私有loopback stock Main各跑610向量，原probe Oracle均通过；Classic sentinel成功、stock文件/native namespace快照不变、原旧magic控制在四个decoder均预期FAIL。636个输入/实际JAR/JDK/class-load与312 raw文件归档核验，所有自有进程终结/端口释放。当前PASS_SCOPED，内部handle/master-key/allocation无直接观测使完整G1仍BLOCKED_UNVERIFIED；步骤11/12/14/15、生产Gate与16步验收仍OPEN，默认OFF/SYSTEM/PARK及disabled不变。继续其余兼容effect、Main/default routing/required authority、full-bytes、backup/restore/accepted退役/GC、正式性能与运维；不重绑历史回执。

2026-10-13 [released stock内部effect与corpus补齐](profile-wire/README.md#released-stock-内部-effect-与-corpus-补齐)：父源码55388d6da0上新增外部JDI方法/channel观测，四版本最终611向量的Classic effect为零、611连接实际终结，前后Classic控制触发实际路径。当前生成器要求RECOVERY_TERMINAL_CLOSE而committed corpus缺失的三项RED已用现有生成器补资源，原610 raw bytes和14补充fixtures不变；当前Profile五类37项通过，observer定向Checkstyle通过。639 stock/corpus及1116 Java输入/三阶段raw/checker归档见profile-wire记录，旧610结果不移绑。PASS_SCOPED完成固定reference corpus的内部effect观察；完整current control-body/mixed-version/auth/semantic/startup与16步其余验收仍OPEN，G1 BLOCKED_UNVERIFIED、生产Gate OPEN，默认OFF/SYSTEM/PARK及disabled不变。继续其余兼容、Main/default routing/required authority、full-bytes、backup/restore/accepted退役/GC、正式性能与运维；目标active。

2026-10-13 [当前control请求与released resolver](profile-wire/README.md#当前-control-请求与-released-resolver)：父365dcb802e上将actual client两个control codec的9完整请求及45 magic/truncation变体接入原corpus，665-vector原611 bytes不变；七类47项Profile测试PASS，四版decoder/真实stock各665连接与actual channel drain/内部zero effect/native快照均通过，old CE仍各expected FAIL。四个released resolver的四种endpoint选择/拒绝输入也通过pure logic，registration I/O/publication不因此获证。815 stock与1170 Java输入、独立resolver source、1185 unique SHA/raw/RED/GREEN/checker证据由profile-wire记录拥有；common及resolver style通过，独立RAT仍父443无新增。支持的实验control schema样本已覆盖，mixed rolling versions/auth/semantic/startup及general stable control-body、Main/default route/其他required authority/full-bytes/backup-restore/accepted退役/GC/性能/运维与16步综合验收继续OPEN，G1 BLOCKED_UNVERIFIED、生产Gate OPEN、目标active。默认OFF/SYSTEM/PARK及disabled不变，原610/611结果不移绑。

2026-10-13 [owned lifecycle实际self-drain拒绝](startup-readiness/README.md#owned-lifecycle-的实际-self-drain-拒绝)：父54800228d8上将同一owned服务cold/delete/metadata/ZK event contexts及原Netty loops按实际Thread identity拦在退休/demotion/shutdown前；原metadata明确await API也先拒绝self，legacy非阻塞close及合法外部timeout/actual drain不变。实际旧cold自等待RED、四种managed contexts/直接metadata self-await及同名外部caller正常关闭GREEN，最终3类44 registered/42 PASS/2既有hierarchical条件skip，1170 inputs/style通过、独立RAT443父已有无新增。旧source/receipt保留原绑定，本次native caller identity未移绑TLC；没有新线程/队列/格式或authority issuer。继续Main/default SDK/其他required producer/full-bytes/backup-restore/accepted退役/GC/mixed兼容/性能/运维及16步验收；目标active，步骤11/12/14/15和生产Gate OPEN，默认OFF/SYSTEM/PARK及disabled不变。

2026-10-13 [CLOSED readonly open独立actual-drain](profile-wire/README.md#closed-readonly-open-的独立-actual-drain-信号)：父9cbeb5e2e7上原coordinator新增beginOpenReadOnly/ReadOnlyOperation分开caller completion/cancel与实际cold/未交付handle终结；原openReadOnly兼容委托，peers继续borrowed，合法交接先claim再释放cold quota，callback可继续open。真实held最后metadata、cancel仍占quota、delete/birth拒绝、success callback/nested open及其他调用方最终8项通过，相关11有限模型1正向/10指定反例、730仅catalog；2005 unique source输入冻结、style通过、独立RAT父443无新增。默认SDK公开Profile入口选择仍待明确，本增量不等同已完成默认路由。继续完整Main/required producer/full-bytes/backup-restore/accepted退役/GC/mixed/性能/运维和16步验收；目标active、生产Gate OPEN、默认OFF/SYSTEM/PARK及disabled不变。


2026-10-13 [SDK 读响应的 backing 所有权交接](profile-wire/README.md#sdk-读响应的-backing-所有权交接)：父 eda1d7d760 上把原 READ_ENTRY response backing 通过 AutoCloseable/retained slice 交给标准 SDK ReadOp，取消丢弃和 ordered executor 拒绝均释放未交付 custody，最后 caller view 归还前 closeAsync 继续等待。父 SDK adapter 的真实 retained-entry RED 与最终四类45项 GREEN、style通过；原 backing 模型7配置按自己的阶段源码保留，2005 unique输入分别核验。独立RAT父443无新增；full-bytes总预算、默认SDK公开入口及全16步验收仍OPEN，目标active、生产Gate不提升，默认OFF/SYSTEM/PARK与disabled不变。


2026-10-13 [SDK LAC evidence 的 backing 所有权](profile-wire/README.md#sdk-lac-evidence-的-backing-所有权)：父73b67bc6e9上将 URL1/URL2 的 source/explicit payload 和 matching READ_ENTRY anchor 交给原已计费 frame，fan-in/ordered 等待期间保留，最终 evidence 前或失败/拒绝时释放。真实父consumer RED、最终三类24项与style通过；原17配置模型3正向/14指定反例按独立stage保留，2005 unique输入核验。独立RAT父443无新增；该切片不闭合 full-bytes 总账、默认SDK公开入口或全16步Gate，目标active、生产Gate OPEN，默认OFF/SYSTEM/PARK与disabled不变。


2026-10-13 [Recovery candidate 的 backing 所有权](profile-wire/README.md#recovery-candidate-的-backing-所有权)：父2719623ab6后新增显式owned point-evidence入口，原source ByteBuf延续至candidate/标准packed payload最后引用释放；原worker/prefix/health/repair内部调用已交接并关闭，旧heap接口兼容。真实legacy分支RED、最终21项Java/style与40有限模型5正向/35指定反例通过，2005 unique输入SHA核验一致；独立RAT父443无新增。legacy/observation/header/caller及统一full-bytes、默认SDK/Main/其余16步验收仍OPEN，目标active、生产Gate不提升，默认OFF/SYSTEM/PARK及disabled不变。


2026-10-13 [Repair packing header 的 DATA 预算](profile-wire/README.md#repair-packing-header-的-data-预算)：父58b942ee53上标准V3新增explicit header allocator，原内部repair使用目标DATA预算，source payload credit保持原归属；quota不足DEFERRED、不写DATA/发布membership，最后header view释放后target close才drain。真实原Unpooled分支RED、最终两类17项（含Classic digest6）/style与17有限模型2正向/15指定反例通过，2016 unique输入SHA核验；Java/model/style终态后的独立RAT exit1，443 unapproved路径全部存在父HEAD，无新增；不称CI全绿。默认/legacy/observation/caller及full-bytes总账、默认SDK/Main/其余16步仍OPEN，目标active、生产Gate不提升，默认OFF/SYSTEM/PARK及disabled不变。


2026-10-13 [Normal quorum probe 的 owned candidate](profile-wire/README.md#normal-quorum-probe-的-owned-candidate)：父af791ffb69上新增原Listener/fencing/quorum算法的retained candidate入口，caller关闭或取消未交付时释放，legacy接口保持。真实heap分支RED、最终10项Java/style与16有限模型3正向/13指定反例通过，2005 unique输入核验；Java/model/style终态后的独立RAT exit1，443 unapproved全部存在父HEAD，无新增；不称CI全绿。此为observation接线的基础，observe()列表本身尚未切换，释放约定按AGENTS公共接口要求待答复；统一full-bytes、默认SDK/Main和全16步仍OPEN，目标active、生产Gate不提升，默认OFF/SYSTEM/PARK及disabled不变。


2026-10-13 [派生 index 的有界 Java 读取](local-storage/README.md#派生-index-的有界-java-读取)：父69699cd886上将原RocksDB完整数组Get/iterator改为fixed-output并检查完整长度，binding/key/location格式64/77/160不变，坏value保持IOException。真实父源码8MiB→8,390,664B分配RED、新Get2144B与最终18项storage/index/style通过，1170 stage输入及format-only source分开归档；Java/style终态后的独立RAT exit1，443 unapproved均存在父HEAD、无新增；不称CI全绿。未改/重跑不覆盖JNI分配的TLC，native/object/统一full-bytes、默认SDK/observe公共约定及其余16步仍OPEN，目标active、生产Gate不提升，默认OFF/SYSTEM/PARK及disabled不变。


[声明模型目录与现有 Java 测试整轮验证](profile-wire/README.md#声明模型目录与现有-java-测试整轮验证)：2026-10-13父fd3b1be724上的声明目录完整验证：730配置全部终结，118正向完整搜索PASS/612指定反例；现有Unified WAL Java43类整轮866项原exit1（2个遗留契约断言失败、0错误、5个Linux条件跳过），已修正两处测试并独立补验失败方法2项通过。四类补验首次被会话中断、回执为INCONCLUSIVE；新阶段253项全部通过，style通过；终态后独立RAT exit1/443父HEAD路径无新增，不称CI全绿，原失败/模型回执和修正stage分别保留，不改称整轮全绿。完整A+C+D组合、全字节、默认SDK/observe公共约定、Main/required producer、backup-restore/accepted退役/GC/mixed/正式性能/运维和16步验收继续OPEN，目标active、生产Gate不提升，默认OFF/SYSTEM/PARK及disabled不变。


[C+D local FREE 组合与实际边界验证](profile-wire/README.md#cd-local-free-组合与实际边界验证)：2026-10-13父36163d9908后在原WholeAllocationFree新增CDSpec，直接组合两个原ProfileLogicalDelete实例与共享allocation的tombstone force/apply、旧I/O/reader drain、FREE force/apply；新四个指定反例覆盖早投递、无force、丢邻居和部分effect伪FREE。最终10配置2完整正向/8指定反例与实际两类6项Java通过，2010 unique source匹配；原Spec五配置保持原计数，catalog735尚未全跑、原730回执不移绑。style通过，实际终态后的独立RAT exit1/443父HEAD路径无新增，不称CI全绿。该单target/两ledger组合不冒充完整A+C+D，继续全16步及其余OPEN范围，目标active、生产Gate不提升，默认OFF/SYSTEM/PARK及disabled不变。


[A+C local publication 到 SDK ACK 的实际验证](profile-wire/README.md#ac-local-publication-到-sdk-ack-的实际验证)：2026-10-22父30377492f4后在原ProfileSdkBridge新增ACSpec，直接由两个独立SegmentAllocator实例的local publication供给原A2 SDK ACK，slot2保留验证过的remote响应输入；已发请求可在caller超时后进入owner，取消不取消accepted I/O，恢复同步不mint原live ACK。最终17配置4完整正向/13指定反例、旧11配置计数不变，两阶段8+2原Java与最终新增1项真实队列/独立force测试通过，2016 unique路径按stage冻结，1项测试强化差异分别保留；catalog741未全跑，历史receipt不移绑。Spotless/Checkstyle通过；Java/model/style实际终结后独立RAT exit1，443父HEAD路径无新增，不称CI全绿。该单entry/每episode一个batch/pair内一次crash组合仍未闭合同批fence/冲突重试/跨batch UNKNOWN/多Arena/连续两次故障与完整A+C+D；继续全16步，目标active、生产Gate不提升，默认OFF/SYSTEM/PARK及disabled不变。


[Physical prefix 的实际 I/O 排空验证](profile-wire/README.md#physical-prefix-的实际-io-排空验证)：2026-10-22父098838a574后在原C中分开file durability与逐batch actual I/O return，只有完整actual-return prefix才可local publication，未返回任务继续保留inflight；原A+C实例直接消费该事实。新增suffix提前发布反例精确命中“两批字节durable、仅第二批返回、第一批仍在途”。最终18配置4完整正向/14指定反例及现有4项真实排空/fence/mixed测试通过，2017 unique路径与重叠SHA核验一致；C/A+C状态计数细化，旧SDK6配置计数未变，catalog742未全跑、历史结果不移绑。Spotless/Checkstyle通过；Java/model/style实际终结后的独立RAT exit1，443父HEAD路径无新增，不称CI全绿。Java/wire/持久格式未改，分布式双batch/multi-Arena/冲突/完整A+C+D及其他16步验收仍OPEN，目标active、生产Gate不提升，默认OFF/SYSTEM/PARK及disabled不变。


[跨 batch suffix 的真实 quorum](profile-wire/README.md#跨-batch-suffix-的真实-quorum)：2026-10-22父85716ef7f3后在原A+C新增suffix pending操作的分布式ACK窗口，完整DATA prefix durable仍必须等待predecessor实际返回；错误suffix DURABLE一直传播到SDK success的反例已命中。最终20配置5完整正向/15指定反例、旧18配置计数不变；两项新增真实A2/A3双batch与两项默认夹具回归通过，2019 unique路径/重叠SHA一致。两轮fixture未进入目标窗口的失败保留，当前hold在原Obligation终态交接，生产代码/wire/持久格式与旧fixture默认不变。catalog744未全跑，历史receipt不移绑。Spotless/Checkstyle通过；Java/model/style实际终结后独立RAT exit1，443父HEAD路径无新增，不称CI全绿。完整多peer/多SDK操作/multi-Arena/fence/冲突/恢复/两次故障/A+C+D及其余16步仍OPEN，目标active、生产Gate不提升，默认OFF/SYSTEM/PARK及disabled不变。


[超时后同坐标重试的身份验证](profile-wire/README.md#超时后同坐标重试的身份验证)：2026-10-22父c60271c0f7上加强已有真实socket测试：原force在途且caller已超时，payload-only/length-only改变都冲突，LAC-only变化续接原操作、只发生一次force；该项及assembling/pending/index三项现有回归全部通过。原A+C增加同坐标cut与四个指定反例，最终25配置6完整正向/19指定反例、原20计数不变，2024 unique source/重叠SHA一致；owner为坐标保护oracle，不宣称第二个Y物理写、hot/index或restart冲突恢复已建模。Spotless/Checkstyle通过；终态后独立RAT exit1/443父HEAD路径无新增。catalog749未全跑，历史receipt保持绑定；生产/wire/格式未改，完整冲突恢复/multi-Arena/fence/两次故障/A+C+D和其余16步仍OPEN，目标active、生产Gate不提升，默认OFF/SYSTEM/PARK及disabled不变。


[Checkpoint record 的流式摘要与写入](local-storage/README.md#checkpoint-record-的流式摘要与写入)：2026-10-22父830918cbb2上去掉原双owner image摘要/写入阶段的完整record临时复制，改为一个16-byte prefix和原record只读view，字节/SHA/force与publication合同不变。相同约8MiB workload旧digest/write分配8,407,640/8,421,296 bytes触发真实RED，新为12,992/29,704 bytes；最终两类20项全部通过，含向量、partial body失败、force/损坏回退/子进程退出及DATA图restart。1170输入按stage冻结，style通过；终态后独立RAT exit1/443父HEAD路径无新增。额外分段write的I/O成本未测，不宣称性能/full-bytes闭合；模型无变化不重跑。其他required范围与全16步仍OPEN，目标active、生产Gate不提升，默认OFF/SYSTEM/PARK及disabled不变。


[Recovery reader 的本地 RPC 实际排空](profile-wire/README.md#recovery-reader-的本地-rpc-实际排空)：2026-10-22父f115cd7d22上修复reader已回逻辑结果却未等自身READ/FENCE/LAC/anchor output终态即关闭的真实缺口；复用原request drain锁/集合，parent coordinator六处等待close完成才归还名额，borrowed peers保持可用。真实旧源码RED、最终五窗口及相关23项零failure/error/skip，15原有限模型配置符合预期；模型没有新reader组合，2024路径按stage冻结、import-only排序单列。style通过；终态后独立RAT exit1/443父HEAD路径无新增。observe结果公共约定/默认SDK/Main/required producer/full-bytes/backup-restore/accepted退役/GC/mixed/完整模型/正式性能/运维与全16步仍OPEN，目标active、生产Gate不提升，默认OFF/SYSTEM/PARK及disabled不变。


[LAC collector 排队期间的关闭排空](profile-wire/README.md#lac-collector-排队期间的关闭排空)：2026-10-22父36eb57665d后补原RPC终态之外的collector排队窗口；LAC内部result在原request drain登记，caller取消不提前释放，executor关闭兜底也等待owned reply清理。真实旧源码RED、新窗口通过；24项相关回归原23通过/1旧hold清理失败，已按actual-drain合同修正该方法并定向补验两项通过，原失败保留、不改称整轮全绿。1170输入按四stage冻结，style通过，终态后独立RAT443父HEAD路径无新增。模型未改不重跑，其他16步验收/生产Gate仍OPEN，目标active，默认OFF/SYSTEM/PARK及disabled不变。


[Checkpoint 流式写入的本地 JMH 对照](local-storage/README.md#checkpoint-流式写入的本地-jmh-对照)：2026-10-22父bc2f189186上新增原microbenchmarks入口，以相同有效1/64条ALLOC image、digest/open/truncate/write/file force/目录同步对照830918旧codec与当前流式codec。旧→新→新→旧的8个fork均核验所加载class及完整image字节一致。64条Java分配约50–52KB/op→25.8–25.9KB/op，write66→130/op，worker CPU上升；短运行延迟波动不支持稳定延迟/吞吐提升结论。141相关输入、冻结jar/旧class、原始JMH/环境与复现脚本归档，package/style通过，独立终态RAT443父HEAD路径无新增。无生产改动，不提升B18/正式性能或其他Gate；全16步目标active，生产Gate OPEN，Linux NUMA/掉电/正式性能/canary仍NOT_EXECUTED，默认OFF/SYSTEM/PARK及disabled不变。


[Backup/restore 首版接受语义](profile-wire/README.md#backuprestore-的首版接受语义)已按用户指定规划会话的2026-10-22裁决冻结：只支持新 BookieId/incarnation/目录的 verified export/rebuild，由当前未回滚集群授予 repair/membership 与新目标 bootstrap；旧控制/cursor/grant 不恢复为权限，旧 incarnation 删除及物理义务原样保留。原地 restore/同 BookieId 切换/MetadataStore 回滚继续 disabled；恢复接受下界未实现前不据窗口到期 GC。owner RFC 已同步，导出/导入及恢复专属验收仍 OPEN；后续据此实施，非语义完成即实现完成，完整目标 active、生产 Gate OPEN。


[SDK ACK→CLOSED删除→共享FREE组合](models/README.md#sdk-ack-到-closed-删除及共享-free-的生命周期组合)：2026-10-22父b0710fd09c上直接组合原SDK/两个storage episode与两logical-delete/原allocation。最终五配置为1完整正向（529798/99442、queue0）、3指定mutation反例及1完整成功路径witness；只合并closed且drained后冻结SDK历史的显式TLC VIEW，原动作/性质保留，无state constraint或symmetry。1131输入/全部早期求值失败、未完成搜索及最终源码各自冻结；Python/TLC通过，独立RAT443父HEAD既有无新增。无生产Java改动，不重复Java测试，不将原receipt升级本组合native验收；catalog754未全跑，完整A+C+D及16步/生产Gate仍OPEN、目标active、默认OFF/SYSTEM/PARK与disabled不变。


[离线 backup image 与不可服务输入](local-storage/README.md#离线-backup-image-与不可服务输入)：2026-10-22父4b1ba0889d上复用原DATA/双control锁和完整恢复扫描，建立有界manifest/外部SHA reference、全native文件复制/force/重读及借用scratch的instance点读；marked input在服务replay前拒绝，旧权限不作新授权。新启动拒绝RED，最终两类六项0 failure/error/skip、style通过；1171输入各stage冻结，独立RAT443父HEAD既有无新增。仅完成离线输入层，repair来源/新目标导入/membership/bootstrap及恢复专属组合仍OPEN，未称restore完成；旧目标义务保留，全16步目标active、生产Gate OPEN，默认OFF/SYSTEM/PARK和disabled不变。


[离线镜像接入当前 CLOSED repair-copy](profile-wire/README.md#离线镜像接入当前-closed-repair-copy)：2026-10-23父b7c9e32bc6上，镜像仅作附加source，经current CLOSED/context/birth/retained source校验，复用inactive install/admit/grant/copy/membership/close；缺项取当前副本，冲突隔离，不降frontier或授normal。原copy同时等待reply与actual output终结，caller cancel保持实际资源；新增七场景及既有路径共12方法全部通过，reply-only敏感性RED保留。repair模型12配置2正向/10指定反例，catalog756未全跑；1171 Java/1134模型输入去重2033，各stage保持原绑定。style通过；RAT补两新cfg许可证后回到443父HEAD既有路径，无新增。新目标assignment/snapshot/no-hole suffix/required-through/readiness/registration、restore reuse/crash/lost reply及完整接受仍OPEN，旧目标义务不解除。完整16步目标active、生产Gate OPEN、默认OFF/SYSTEM/PARK与disabled不变，Linux NUMA/真实掉电/正式性能/canary未执行。


[恢复目标的首次 service bootstrap 组合验证](profile-wire/README.md#恢复目标的首次-service-bootstrap-组合验证)：2026-10-23父389b49fada上，仅补原真实restore fixture的组合覆盖。新目标copy完成后可经当前verified-zero/head重启注册并mTLS读出原数据；首次注册前删除则用该目标自己的完整snapshot追赶，publication实际提交前已有cursor1/tombstone且尚无注册，旧Manifest/目标义务不变。最终三方法0 failure/error/skip（扩展restore方法九场景＋既有service drain/restart及snapshot/suffix），1171输入和早期零cursor测试假设失败各自冻结，无生产Java/TLA改动、不重跑TLC。完整restore的reuse/crash/lost reply/接受下界及其余16步验收仍OPEN，目标active、生产Gate OPEN、默认OFF/SYSTEM/PARK和disabled不变。


[旧恢复输入与当前 metadata 身份隔离](profile-wire/README.md#旧恢复输入与当前-metadata-身份隔离)：2026-10-23父7021519bb3上，仅补真实restore当前metadata身份覆盖：相同body/version重建但native birth不同、同ledgerId非法替换instance/backlink，两者均QUARANTINED且零DATA/零copy/metadata不变/不授normal；既有镜像context隔离和wrong-instance tombstone原native/replay验证复用。最终两类三方法通过，restore方法11场景；两轮fixture/API与分类假设失败分别保留，1171输入冻结，无生产Java/TLA变更、不重跑TLC。该结果不授予合法reuse/原地restore；恢复crash/lost reply/接受下界/运维及完整16步继续OPEN，目标active、生产Gate OPEN、默认OFF/SYSTEM/PARK和disabled不变。


[镜像导入丢回复后的同 intent 续接](profile-wire/README.md#镜像导入丢回复后的同-intent-续接)：2026-10-23父796582579f上，真实首条recovery DATA force后丢回复/断DATA，目标经正常停止和service重启、新coordinator同intent续接仅为缺项新增一次force；membership后grant-close丢回复/断CONTROL，新coordinator续接copy0/零新增DATA force。13场景restore方法与原historical lost-close两方法通过，原CLOSED及权限保持；1171输入/两轮fixture假设失败分别冻结，无生产Java/TLA改动、不重跑TLC。仅验证正常restart及上述回复丢失，非进程kill/掉电；恢复crash/cursor/readiness/registration丢回复/接受下界/运维与完整16步仍OPEN，目标active、生产Gate OPEN、默认OFF/SYSTEM/PARK和disabled不变。


[Owned service 的 repair INSTALL 与崩溃目标隔离](profile-wire/README.md#owned-service-的-repair-install-与崩溃目标隔离)：2026-10-23父3e233f324b上，修复owned PREPARING_INSTALL漏查repair authority：仅initial NOT_FOUND时查原repair reader。真实owned/ZK/mTLS INSTALL NOT_FOUND RED已复现；修复后子JVM首条recovery DATA OK/APPLIED前halt23、无Java cleanup，原target因unclosed DATA session拒绝重开/注册；另新身份可完成copy/member/close，随后删除仍保留崩溃及新target义务。最终两方法通过（restore14场景及原owned SDK/drain），1171输入/五阶段保持原绑定，TLA未改不移绑旧receipt。**同target/同intent的unclean-session crash resume及新target4注册闭环尚未验收，不能以新身份重建替代该完整目标**；cursor/readiness/registration恢复、接受下界/运维及全16步仍OPEN、目标active、生产Gate OPEN，默认OFF/SYSTEM/PARK与disabled不变，真实掉电/Linux NUMA/正式性能/canary未执行。


[崩溃后新身份目标的注册与在线删除闭环](profile-wire/README.md#崩溃后新身份目标的注册与在线删除闭环)：2026-10-23父807f78579a上，只扩展原restore测试。真实child halt后原target3保持隔离，新身份target4完成当前owned startup/registration/fresh peer+mTLS点读，再由原follower投递实际删除到cursor1/tombstone并拒绝后续读；Manifest完整字节与旧、新target义务保持。原14场景方法通过，1171 native输入及最终format-only换行分开保存；生产Java/TLA未改，不重跑TLC。补齐上一阶段新target4注册缺口，**不替代原target/原intent的unclean-session恢复**；相关新增数据语义已提请用户选择，未答复不放行。完整16步仍OPEN、目标active、生产Gate OPEN、默认OFF/SYSTEM/PARK与disabled不变，Linux NUMA/真实掉电/正式性能/canary未执行。


[派生 index 的 write-buffer 配置容量预留](local-storage/README.md#派生-index-的-write-buffer-配置容量预留)：2026-10-23父a08d52884a上，原shared DataWorkingSet启动前预留扩展为固定pool＋1 MiB cache＋两份1 MiB write-buffer配置容量，实际RocksDB选项值不变。真实quota缺口RED已复现；最终三方法通过，含原owned SDK/drain及恢复14场景，1171输入/red-green/同版依赖源码分别冻结。测试fixture显式4 MiB total，生产quota不自动增大；reservation保持至实际storage/index关闭。只是配置容量预留，不宣称memtable/native/RSS硬上限或full-bytes闭合。TLA转移未改；统一预算、同身份unclean-session恢复待决语义及其余16步继续OPEN，目标active、生产Gate OPEN、默认OFF/SYSTEM/PARK和disabled不变，Linux NUMA/真实掉电/正式性能/canary未执行。

[只读磁盘状态诊断](local-storage/README.md#只读磁盘状态诊断)：2026-10-23父4907120bff上，实验 main 复用原 Arena replay，以既有三类排他锁和READ-only内容channel报告观察到的身份/布局、session/quarantine与分配状态，不执行恢复sync或授予服务/restore权限；原正常恢复与unclosed gate保持。真实文件/独立JVM/锁、fallback与坏suffix验证由该节拥有。这里只交付离线诊断，完整运维/同身份crash接受语义/16步验收仍OPEN，目标active、生产Gate OPEN，默认OFF/SYSTEM/PARK及disabled不变。

[恢复 bootstrap 的 snapshot suffix 与发布丢回复](profile-wire/README.md#恢复-bootstrap-的-snapshot-suffix-与发布丢回复)：2026-10-23父d4a3e38114上，补实际恢复目标snapshot cut1＋真实第二ledger删除suffix/head2；完整时cursor2/两tombstone后注册，缺event2时保持cursor1且无writable。persistent readiness与writable两次实际ZK multi后分别回CONNECTIONLOSS，同session精确确认成功，Manifest/旧目标义务保持。仅测试fixture与既有文档变化，原14场景保持；不替代同incarnation unclosed crash恢复或完整接受，16步目标active、生产Gate OPEN、默认OFF/SYSTEM/PARK及disabled不变。

[显式 SDK writer 的 generated header 字节额度](profile-wire/README.md#显式-sdk-writer-的-generated-header-字节额度)：2026-10-23父9a9aa9cd79上，普通V3 header在entryId前取得原DATA额度，WRITE_LAC header同样计费；额度不足不分配entry、不发送DATA，取消/回收与source→socket实际交接保持。仅绕过计费的敏感性RED已复现，最终三类37项通过，Classic三digest实际读写及原SDK/close/替换/恢复排空保持。这里是显式writer生成buffer的单handle额度，不关闭全字节/性能/默认SDK及其他16步范围，目标active、生产Gate OPEN，默认OFF/SYSTEM/PARK及disabled不变。

[三副本 SDK 到本地删除 FREE 的组合](models/README.md#三副本-sdk-到本地删除-free-的组合)：2026-10-23父f5fc64ace4上，原SDK/两个storage与delete/FREE保持，第三slot新增自己的原allocator publication/actual I/O return；close输入还须SDK停止及三请求/source/I/O排空。六配置为1完整正向（41770502/6170077、queue0）、4指定反例和1三副本成功路径witness，1142输入相同、显式3600秒预算；762目录未全跑。仍只有一个本地allocation的D/FREE，metadata/fenced-CLOSED为输入，旧slot迟到回复的全部close路径与完整A+C+D/16步验收继续OPEN；无Java变更，目标active、生产Gate OPEN、默认OFF/SYSTEM/PARK及disabled不变。

[SDK 停止后丢弃回复的实际请求结算](models/README.md#sdk-停止后丢弃回复的实际请求结算)：2026-10-23父1fef8057fc上，组合模型补原slot0/1在SDK停止且实际I/O返回后的sent结算，不生成ACK。九配置为1完整正向（44131126/6740125、queue0）、6指定反例、2个49步A=2成功后停止/丢弃回复/local FREE witness；1146模型输入相同，native1172未变、不重跑Java。具体witness的I/O在stop前返回；仍非全目标物理完成、全部timeout/故障或完整A+C+D。766目录未全跑，16步目标active、生产Gate OPEN，默认OFF/SYSTEM/PARK及disabled不变。

[SDK 迟到 DATA 到删除复用及健康重启的原生组合](profile-wire/README.md#sdk-迟到-data-到删除复用及健康重启的原生组合)：2026-10-23父bf9c813280上，新增一方法串联实际3/3/2 SDK ACK、第三副本force跨越stop、固定P/L普通close、三目标delete/本地FREE、ledger45复用generation2及逐节点健康restart/mTLS点读。最终1 PASS、1172输入匹配、生产Java/TLA未改、style通过；旧Manifest/三target义务保留。仅单Arena顺序复用的有限组合，非crash、多Arena、持久physical receipt/GC或完整16步验收；目标active、生产Gate OPEN，默认OFF/SYSTEM/PARK及disabled不变。

[Profile 启动恢复扫描的 DATA 字节额度](local-storage/README.md#profile-启动恢复扫描的-data-字节额度)：2026-10-23父66e860ea32上，将原 service DATA quota 传给 restart scan，在分配 header/page/batch/covered MOVE scratch 前计费，实际 scope 结束归还；额度不足不启动新 DATA session。真实大 batch 拒绝、caller cancel/实际 force、CRC 失败回收及原 covered MOVE/owned SDK restart 共4方法通过，1172输入匹配、最终style通过；绕过 budget 转发的敏感性RED独立保留。只闭合启动DATA buffer计费，全字节、同身份crash/多Arena待决语义和完整16步仍OPEN，目标active、生产Gate OPEN，默认OFF/SYSTEM/PARK及disabled不变。

[共享 allocation 存活邻居的搬迁与复用](profile-wire/README.md#共享-allocation-存活邻居的搬迁与复用)：2026-10-23父c8483d3549上，原三副本SDK生命周期加入同块未删除ledger45；第三COPY force未返回时无MOVE/FREE，实际搬迁后邻居可读，ledger46的256条普通Add经合法尾部耗尽后复用旧块generation2。三节点健康restart后邻居/新首末entry仍可读、44拒绝、历史义务保持。三方法通过，1172输入/三轮fixture假设失败/最终纯换行分别保留，style通过；生产Java/TLA未改，不重跑模型。仅闭合该有限组合，完整16步目标active、生产Gate OPEN，默认OFF/SYSTEM/PARK及disabled不变。

[控制 checkpoint 的流式读回校验](local-storage/README.md#控制-checkpoint-的流式读回校验)：2026-10-23父b03297b919上，Bookie/Arena 写后验证复用原 parser，用最多64 KiB record scratch逐条比较冻结状态；恢复仍返回独立record，格式/force/commit/publication不变。8 MiB有限image的旧比较额外Java分配8,535,328 bytes，新版本最终185,200 bytes；最终两类48方法通过、1172输入匹配、style通过，旧算法RED独立保留。不是RSS/吞吐或统一CONTROL总账证明；完整16步目标active、生产Gate OPEN，默认OFF/SYSTEM/PARK及disabled不变。

[Profile checkpoint 校验的 CONTROL 容量预留](local-storage/README.md#profile-checkpoint-校验的-control-容量预留)：2026-10-23父a533d249d2上，Service在storage open前从原CONTROL额度保留69776 bytes，保持至实际storage关闭；未接线启动缺口及同步drain回调重入重复归还两次RED分别保留，最终先移走token引用再归还。最终两类19方法通过、1172输入匹配、style通过，覆盖真实held checkpoint force、停止准入后verification、原SDK/Embedded/owned restart与bind cleanup。测试只增加固定容量，原DATA/网络空间保持；统一CONTROL/full-bytes和其余16步仍OPEN，目标active、生产Gate OPEN，默认OFF/SYSTEM/PARK及disabled不变。

[Arena checkpoint 的借用记录恢复](local-storage/README.md#arena-checkpoint-的借用记录恢复)：2026-10-23父8579d1e211上，原parser以一份record scratch同步回调Arena解码，完整digest成功前只形成私有候选，失败清空后fallback；Bookie retained read与写后verify保持各自ownership。4 MiB有限image额外Java分配4,269,832→最终135,536 bytes，晚digest错误不遗留ghost allocation/id。最终三类56方法通过、1172输入匹配、style通过；启动streaming复用既有CONTROL预留，未改格式/配置/TLA。typed/frozen/Bookie状态及完整full-bytes/16步仍OPEN，目标active、生产Gate OPEN，默认OFF/SYSTEM/PARK及disabled不变。


[Arena checkpoint 的按需编码与持有态](local-storage/README.md#arena-checkpoint-的按需编码与持有态)：2026-10-23父e1fb23a0fa上，Arena只冻结原不可变命令/状态/序号，摘要、写出及精确读回比较时按需编码单条record；Bookie retained原始字节保持。实际两个JVM在同16,000控制结果/首次image force前的full-GC histogram中，只读buffer增量16,000→0、byte[]增量3,393,904→66,064，完整image字节相同且各自重开；非RSS/峰值/性能结论，重复编码成本未测。Service原CONTROL预留扩为200848 bytes并保持至实际storage关闭，生产quota不自动增加；不足启动的敏感性RED和原始失败分层保留；最终四类72项通过、1172输入一致、style通过，TLA未改不重跑TLC。typed/Bookie/其他full-bytes及完整16步仍OPEN，目标active、生产Gate OPEN，默认OFF/SYSTEM/PARK及disabled不变。


[Bookie control 原始记录的容量预留与关闭释放](local-storage/README.md#bookie-control-原始记录的容量预留与关闭释放)：2026-10-23父8b245c0ce5上，原Service CONTROL working-set在storage open前增加3倍journal的Bookie canonical backing容量，合计200848＋3×controlLogBytes，实际storage终结后归还；原raw map在close路径清空，durable字节/cut/closed拒绝保持。父版2RED、同选择修复2PASS；同4096实际INSTALL文件的两JVM在保留closed对象时，父版仍4096个只读buffer，新版降至0，磁盘字节相同且重开成功；最终三类37项通过、1172输入一致、style通过，TLA未改不重跑TLC。仅raw backing预留/生命周期与有限full-GC持有态，typed/JDK/native等完整full-bytes未闭合；全部16步目标active、生产Gate OPEN，默认OFF/SYSTEM/PARK及disabled不变。


[控制批次的逐条编码与完整预检](local-storage/README.md#控制批次的逐条编码与完整预检)：2026-10-23父ab44b475a5上，双owner在任何I/O前逐条完整预检/累计整批长度，预留原坐标后逐条编码writeFully、整批一次force及原逐序apply；去掉整批serialized列表，写者排他/UNKNOWN/partial前缀恢复保持。新两项后段拒绝/第二write失败与既有定向共6项通过；相同observer的四JVM在首次write前只读buffer增量均4096→1，Arena journal字节相同，Bookie逐项重开核对context/credential。codec预留331920＋既有3倍journal的raw backing，实际inline值核对；最终四类82项通过、1172输入一致、style通过，TLA未改不重跑TLC。CPU/累计分配未测，typed/JDK/native等完整full-bytes未闭合。全部16步目标active、生产Gate OPEN，默认OFF/SYSTEM/PARK及disabled不变。


[控制批次流式编码的本地 JMH 对照](local-storage/README.md#控制批次流式编码的本地-jmh-对照)：2026-10-23父ed036afd83上，原microbench新增固定256批append/force/apply入口；每批1/64条，Arena同块竞争/Bookie新INSTALL。两版相同release17编译、16有效fork按父→新→新→父完成真实force及每轮重开/class校验；初轮major65/61混用的16 fork保留且不作性能对照。累计分配上升已实测（64条Arena59.5→78.2–96.6KB，Bookie约700→803KB），CPU/wall波动不支持稳定收益。生产Java未改，后续复用原codec校验/尺寸以消除预检frame额外构造；完整full-bytes/正式性能与16步目标仍active、生产Gate OPEN，默认OFF/SYSTEM/PARK及disabled不变。


[控制批次预检的校验与尺寸复用](local-storage/README.md#控制批次预检的校验与尺寸复用)：2026-10-24父3c99a1f92c上，双owner整批预检改为原codec共享字段校验/尺寸，移除预检frame/CRC构造；实际完整编码、整批坐标预留、write/一次force/顺序apply及UNKNOWN不变。两项codec边界与既有五类最终87项、style通过，独立RAT443父路径无新增。同一benchmark/依赖、同release17编译67 class并核对实际加载/Maven指令流；16有效fork实测64条分配Arena96.6→63.3 KB、Bookie803.7–808.3→747.4–750.0 KB，CPU/wall波动仍不支持稳定收益。未改格式/模型或缩减CONTROL预留，完整full-bytes/正式性能与16步继续OPEN，目标active、生产Gate OPEN，默认OFF/SYSTEM/PARK及disabled不变。


[当前 Java 整轮与注册池排空观察](profile-wire/README.md#当前-java-整轮与注册池排空观察)：2026-10-24父5a16759da1上的43类整轮实际897项，891通过/1 error/5个Linux条件skip；唯一旧pool测试在hold输出promise时提前等channel close，独立单方法亦复现。按原reader actual-drain先于lease关闭合同，仅修正该方法的观察顺序并加强task拒绝提前释放；最终四项回归通过，其他方法字节及生产Java/TLA不变。原整轮exit1和全部输入/回执保留，不把补验改称整轮全绿；style通过、独立RAT443父路径无新增。其余16步/完整验收继续OPEN，目标active、生产Gate OPEN，默认OFF/SYSTEM/PARK及disabled不变。


[跨 batch quorum 的 durable 回复观察](profile-wire/README.md#跨-batch-quorum-的-durable-回复观察)：2026-10-25父85d158c432上，原43类整轮暴露回复总数观察错误；真实input额度拒绝产生NONE后，旧总数断言的两个受控反例失败。最终仅统计durable结果，先完成第三副本entry0再对suffix拒绝，保留前两副本physicalTerminal与A2/A3屏障，三个节点使用原子force调用计数并结合实际终态。原尝试数3/退避5ms/deadline10s保持。最终同一43类整轮897项为892通过/0 failure/error/5个Linux条件skip，用例身份及1172输入一致；style通过、独立RAT443父路径无新增。所有先前失败分阶段保留；仅macOS Java PASS_SCOPED，生产/TLA未改，完整16步目标active、生产Gate OPEN，默认OFF/SYSTEM/PARK及disabled不变。


[控制 owner 关闭后的历史状态释放](local-storage/README.md#控制-owner-关闭后的历史状态释放)：2026-10-25父8d2b448a0b上，Bookie/Arena在原close guard之后替换9/10个历史容器并释放pending，保留rollback/delete/session/quarantine/cut诊断；Arena关闭后的历史查询明确拒绝，原真实排空与protected settlement不变。扩展6个既有方法，9项定向及最终4类114项全部通过、输入/用例身份匹配、style通过；独立RAT仍443父路径无新增。相同冻结native输入与同JDK的两JVM持续持有closed owner：Bookie LedgerState4096→0、Arena AppliedOperation16001→0、被观察HashMap table57392→0槽位，重开及磁盘SHA不变；不是峰值/RSS或完整full-bytes。未改格式/模型或减少CONTROL预留，默认SDK/observe、同身份unclean-session恢复、多Arena及其余16步仍OPEN，目标active、生产Gate OPEN，默认OFF/SYSTEM/PARK及disabled不变。


[按 allocation 索引当前与退休区间](local-storage/README.md#按-allocation-索引当前与退休区间)：2026-10-25父f51c412490上，Arena current/retired位置索引从逐block展开改为每allocation单项区间，保留exact FREE/reuse、跨grant拒绝、generation和实际保护终结；有ID的grant查询继续O(1)，容量/Inspection仍按block计数。新增1方法及扩展真实2-block FREE/reuse方法；fixture仅补header占用的4096B后7项定向通过，原容量失败独立保留。最终5类590项零failure/error/skip，用例身份和1172输入匹配，style通过；独立RAT仍443父路径无新增。四JVM真实control对照中65536-block索引/Slot从65536降为1，control字节及replay一致；仅control metadata稀疏DATA夹具，不是完整预分配或服务证明。全部生成DATA已按终态/句柄检查清理，证据保留。完整full-bytes/B11/其余16步仍OPEN，目标active、生产Gate OPEN，默认OFF/SYSTEM/PARK及disabled不变。


[Bookie checkpoint 的 typed 按需编码与 100k 驻留](local-storage/README.md#bookie-checkpoint-的-typed-按需编码与-100k-驻留)：2026-10-25父e227679ff2上，Bookie common checkpoint记录改为冻结Command/sequence/status的typed按需编码，保留原sealed、首次结果及terminal成功覆盖，special raw和CONTROL预留不变；checkpointId仅解析commit。新journal/image精确bytes及最终五类591项全部通过、source/用例身份匹配，style通过、独立RAT443父路径无新增。真实100k native-control驻留中raw backing降至0；metadata保守下界append146.1→69.3MB、replay152.5→75.7MB，整进程live增量约下降87MiB。下界不代替上界，完整B11/full-bytes仍OPEN；fixture不含MetadataStore/SDK/service/RocksDB。原始测量、失败准备和cwd误复制清理独立保留，未修改原PDF。完整16步目标active、生产Gate OPEN，默认OFF/SYSTEM/PARK及disabled不变。


[Bookie common 结果与 checkpoint 的单一索引](local-storage/README.md#bookie-common-结果与-checkpoint-的单一索引)：2026-10-25父bd3beb346f上，common AppliedOperation同时拥有原结果与checkpoint tuple，保留失败terminal upgrade的少量历史override及原bytes/status/sequence。首次592项出现TLS close操作promise清理error，原失败保留；只修正测试helper的actual closeFuture/最终drain观察，额度与生产allocator不变。最终同一五类592项全部通过、输入/用例身份匹配、style通过，独立RAT443父路径无新增。相同100k seed重放中Map entries50万→30万、UUID40万→20万，整进程live增量减少19,845,032 B；当前九metadata根去重对象图171,946,208 B包含数组头/包装/String，但不是独占retained size或B11 PASS。完整full-bytes/16步目标active、生产Gate OPEN，默认OFF/SYSTEM/PARK及disabled保持。


[Bookie 私有 scope 的紧凑驻留](local-storage/README.md#bookie-私有-scope-的紧凑驻留)：2026-10-25父a0f6d8217a上，私有StoredScope共享准确context/固定target，保留原domain String等值、外部target/incarnation检查、原sealed与canonical bytes。新增2项真实文件回归及最终五类594项全部通过、source/用例身份匹配，style通过；独立RAT443父路径无新增。两独立JVM同100k native seed的九根去重对象图171,946,208→122,346,384 B（163.98→116.68 MiB），减少约140万对象；这是reachable shallow，未证明独占retained/完整Bookie B11。CONTROL预留和格式不变，完整full-bytes/16步目标active、生产Gate OPEN，默认OFF/SYSTEM/PARK及disabled保持。


[Managed storage 的 100k 账本与 Arena 短记录预留缺口](local-storage/README.md#managed-storage-的-100k-账本与-arena-短记录预留缺口)：2026-10-25源码8a81b08fb4上，新增正常startNew→原CONTROL driver填充100k→真实healthy close→fresh restart的完整isolated storage计量；200000 write/1563 force、全部route与drain通过，重放included图122,370,392 B（116.70 MiB）、整进程live134,196,304 B，FD/线程实际终结。另以原requestPool生成10000短记录、rotation和force冻结，Arena十根可达图3,812,536 B，高于整个storage CONTROL公式3,477,648 B，栈上snapshot另列；不是独占retained超额或portable bound证明。当前预留未单列Arena/快照成本，下一项按record/rows/high-water和真实并发重算，不以任意倍数替代。未改Java/TLA，原594/style PASS保持原绑定；B11/full-bytes及16步目标active、生产Gate OPEN，OFF/SYSTEM/PARK与disabled保持。


[CONTROL typed 状态与工作集的启动预留](local-storage/README.md#control-typed-状态与工作集的启动预留)：2026-10-25父0e63840b99上，以支持布局的typed/history/index、快照/recovery及预检前pending模型替代旧3L金额，仍在原service open前单次预留、实际close后释放，生产quota不自动扩大。支持64位HotSpot17/21与8..256对齐，其他布局拒绝启用实验服务；默认OFF/Classic不变。旧额度可启动的真实RED保留；8项定向和六类597项全部通过，source/用例匹配、style通过。四JVM实测所选类型shallow≤模型；1MiB日志/P16/S4保守预留96,518,568B，128MiB对应11,534,644,136B，不等于实际驻留。模型不闭合任意Throwable/JCA/native或完整状态计数证明；B11/full-bytes及16步目标active、生产Gate OPEN，OFF/SYSTEM/PARK与disabled保持。


[显式离线 backup/restore 运维入口](local-storage/README.md#显式离线-backuprestore-运维入口)：2026-10-25父86c9259dd7上，公开 offlineBackup factory 复用原 ISOLATED 配置冻结历史身份/bounds，capture 交付独立 CREATE_NEW/force 的56 B外部reference；import 复用当前 CLOSED/RepairIntent 授权与原复制/bootstrap，私有 image 只在实际 completion 后关闭。17个既有恢复场景改走公开入口，覆盖源目录丢失、坏reference、同步失败清理、取消及外部完成future不能提前释放native锁；定向5方法及扩大三类215项全部通过，输入/用例身份匹配。最终三处换行有非空白相同证明，style通过、独立RAT仍443父路径无新增；原失败与源码绑定保留，详细证据由上述 canonical 节拥有。仍为显式Java运维API，默认SDK/Main、完整恢复接受下界、同incarnation unclosed-session接受、B11/full-bytes及其余16步继续OPEN，目标active、生产Gate OPEN，默认OFF/SYSTEM/PARK和disabled保持。


[显式 SDK CLOSED 只读入口与依赖关闭顺序](profile-wire/README.md#显式-sdk-closed-只读入口与依赖关闭顺序)：2026-10-25父cc7b8cdd82上，BookKeeper显式enable/open复用原registered pool与CLOSED coordinator，单个open/handle持有lease到实际read/output终结；SDK close先排空Profile再关闭原metadata/executors，中断可继续，self-drain提前拒绝。三项真实服务定向与相关两类23项全部通过、输入匹配，style通过；新增有限模型1正向/3指定反例符合预期，旧Spec/11cfg保持原绑定；独立RAT仍443父路径无新增，原证据由上述canonical节拥有。默认builder拒绝保持，writer create/默认routing/Main可信issuer/full-bytes及其余16步继续OPEN，目标active、生产Gate OPEN，OFF/SYSTEM/PARK与disabled保持。


[Writer close 的实际终态与排队关闭](profile-wire/README.md#writer-close-的实际终态与排队关闭)：2026-10-25父4910d16933上，beginCloseWriter分开bounded result与actualCompleted；包内writer owner将已接受但尚未freeze的close纳入实际排空，local-stop先赢不发布CLOSED，成功callback等待lease、失败/deadline保持原回复。三项真实定向与相关14项全部通过、style通过，新增有限模型1正向/3指定反例符合预期，旧Spec/8cfg保持原绑定；独立RAT仍443既有路径无新增，原证据由上述canonical节拥有。用户已选择显式create/open；public create下一项需要原子fresh reservation归属，不能以matching READY重发writer。该入口仍待接入，完整目标active、生产Gate OPEN，OFF/SYSTEM/PARK与disabled保持。


[显式 SDK fresh create 与 writer 归属](profile-wire/README.md#显式-sdk-fresh-create-与-writer-归属)：2026-10-25父9ef30669e4上，公开enableProfile/createProfileLedger复用单owner、原all-E initializer和writer close；只有本次原生CREATE2 birth回执可取得fresh claim，matching READY/metadata不重发writer。同SDK只续接未交付的原完整请求，handoff消费origin；cancel/global stop等待实际init/config/DATA/lease，SDK stop保留OPEN。额外standby不安装/激活/DATA，CLOSED只读仅过滤非retained成员。定向11项及扩大四类68项全部通过、输入/用例身份匹配，style通过；新增有限模型1正向/4指定反例符合预期，旧Spec/8cfg保留，独立RAT仍443既有路径无新增。实际源码/回执由canonical节拥有，历史证据不移绑。默认builder/可信Main输入/full-bytes及其余16步边界保持，完整目标active、生产Gate OPEN、OFF/SYSTEM/PARK与disabled保持。


[共享 cohort 的多 ledger SDK 与子视图排空](profile-wire/README.md#共享-cohort-的多-ledger-sdk-与子视图排空)：2026-10-26父c5ec73163f上，显式SDK增加有界maxOpenLedgers、共享单Epoch/原6连接与独立CONTROL/DATA子视图；READ/LAC derived views按回复分离，最后子操作才关闭父lease/pool。首CONTROL复用已有pending等待实际授权，取消ready观察前不归还名额；原NONE背压与fresh归属保持。六项定向与五类275项全部通过、用例/输入匹配；最后一行纯换行有6个owner class指令一致证明且六项补验通过，style通过。新增有限模型1正向/5指定反例符合预期、原Spec/15cfg保持；独立RAT仍443既有路径无新增。各阶段源码/回执由canonical节拥有。首版默认builder继续Classic；完整目标active、生产Gate OPEN、OFF/SYSTEM/PARK与disabled保持。
