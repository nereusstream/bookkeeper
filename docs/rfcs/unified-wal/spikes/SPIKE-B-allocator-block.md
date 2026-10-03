# Spike B：Allocator、Block 与 100k Ledger 资源模型否证规范

> 状态：**Planned / Not Executed**<br>
> 对应 RFC：[RFC-0003](../RFC-0003-segment-storage-allocator.md)<br>
> 性质：否证型；PASS最多解锁不会污染Classic cohort的isolated/discardable shadow prototype，不解锁live shadow

2026-09-26实现进展：[步骤3文件基础](../implementation/local-storage/README.md)的14项定向测试覆盖真实FileChannel、framing和有限故障注入；尚未运行本Spike的allocator、恢复或性能切片，状态不提升。

## 1. 要回答的问题

本Spike先验证`ArenaControlLog + shared block`切片；dedicated extent为后续有条件启用配置。各已启用scope必须满足：

- crash 后无 double allocation；
- generation reuse 后无 stale read；
- 已允许参与 ACK 的本地 payload 不因 allocator authority 恢复而丢失；
- 100k idle ledger 不产生固定 extent 或 per-ledger block buffer；
- allocator checkpoint、reclaim 和 compaction 对 foreground p99 的影响可控。

该 Spike 的目的不是证明生产性能，而是尽早否证 allocator authority 和资源模型。

## 2. 非目标

- 不实现完整 BookKeeper quorum 或 AutoRecovery；
- 不证明 Profile install；
- 不决定最终生产 extent size；
- 不把 page cache 或 tmpfs 结果当作 NVMe direct-I/O 证据；
- 不把 shadow-writer PASS 当作 Segment ACK authority。

## 3. 运行前锁定 manifest

以下字段不得为 `TBD`：

首个isolated/discardable性能切片按§4锁定实际启用scope；只有属于未启用路径的字段可标`NOT_APPLICABLE`并写明原因/后续Gate，不能伪造完整Spike输入。same-scope与released-old-binary实验维持原DEFERRED，不因本轮规划自动执行。

```text
sourceCommit
RFC0003Revision
RFC0005Revision
prototypeCommit
supported stock old Bookie commits/exact built artifacts/startup modes/tools
sameScopeCandidate=BKPF1 metadata/local Cookie sentinel exact bytes
newScopeFallback=BookieId/roots/incarnation/OS-service-credential-ACL manifest
Cookie/layout/version and auto-stamp policy
storage incarnation/device-manifest schema
Arena superblock/mandatory-feature/migration schema
persistent ProfileRegistrationStore + ephemeral registration adapter revision
migration/rollback tooling revision
hardwareHostId
CPU and NUMA topology
initial deployment affinity, online/effective cpuset CPUs and memory nodes, quota and applicable parent limits
physical package/core/SMT siblings, DATA-device NUMA node or UNKNOWN, service CPU reserve
logical shard-to-owner-worker mapping, worker count and requested/actual per-thread CPU masks
affinity mode/native API revision and verified fallback/cleanup, thread-creation inheritance policy
owned aligned-pool mapping, NUMA policy/fallback, initialization/first-touch and bounded home-pool returns
PARK or BOUNDED_SPIN, spin budget, wakeup primitive and oldest-batch deadline handling
JVM/GC/JIT, NIC RSS/IRQ/irqbalance, NVMe completion placement and measurable interference scope
memory size
NVMe model, firmware and namespace
filesystem and mount options
I/O API and alignment
JDK and JVM flags
shard count
enabled paths and separately deferred feature/test scopes
transport adapter/refcount ownership and reference-corpus revision
logical identity byte ranges, LAC/digest rules and duplicate slow path
normal/recovery/read-response/Segment payload capacities, unified N and overflow/alignment rules
batch byte/count/wait limits and actual durability-barrier definition
derived-index RocksDB version/options, disableWAL=true, enabled column families and flush proof
hot-to-query-visible handoff, bounded async index credits and persisted coverage/checkpoint cuts
immutable CRC32C capability/layout binding and outer/inner coordinate checks
DATA freeze point, stream/batch framing and contiguous-prefix replay/publication
full-write/short-write handling, file barrier coverage and cold directory durability
fixed shard/physical-stream/DATA-file mapping, file count and rotation bounds
logical stream-tail exclusion without shared-file truncate/collapse/punch and old-generation residual classification
Arena-to-filesystem mapping, DATA preallocation cap, real SST/control/checkpoint reserves and concurrent temporary-space peak
physical batch result versus per-entry admission/cancellation/response result
submitted-coordinate retention and stream/file I/O-error isolation/recovery rules
record envelope integrity, bounded aligned point-read ranges and cache/pin lifetime
confirmed/unconfirmed/recovery read boundaries and LAC update/barrier accounting
shared-only first slice and separately deferred dedicated buffer-pool scope
end-to-end request/byte/batch/waiter credit ownership and release points
pre-admission submission/locator working set, shared-buffer reserve and downstream pause/resume schedule
oldest-request monotonic deadline, maximum entry path and reserved control capacity
delete-applied cursor versus access barrier and physical reclaim predicates
allocation/copy/CPU per-entry instrumentation and thread/queue/lock counters
control-log/checkpoint region sizes
Bookie-level protected control-log record/batch/checkpoint and credential protection
control-tail fault model and required durable-prefix classification oracle
pool owner/shard generation and old writer I/O quiescence mechanism
per-Arena foreground/maintenance reserves and throttle/reject/resume thresholds
bounded live-set/churn rate, maintenance scheduling share and debt-drain deadline
immutable compaction source eligibility, one normal task per source/generation, bounded victim-group net-gain threshold
whole-allocation FREE reasons/references and Bookie-only route/tombstone ownership
normal/full-index-rebuild coverage, scan/I/O/memory/readiness limits
conditional API revision
durable-command group then ordered apply, applied checkpoint cut and deterministic authority references
recovery synchronization files/dependencies, independent durability evidence and process-crash/power-loss distinction
hot-first locator lookup and fresh current-state fallback view
control-history replay before validation of final required DATA, including retired move intermediates
checked DATA origin and relocation operation/target binding, with separate upper-layer completions
connection/TLS/header/read and partial-frame credits, ownership handoff and bounded control connections
sequencer queue/waiter/idempotency hard caps
block sizes under test
extent sizes under test
ledger state implementation
fault injector version
workload generator version
warmup, duration and repetitions
baseline definition
artifact output directory
```

正式测试使用独占或可证明隔离的设备区域；不得指向生产数据盘。输出目录必须全新且 immutable。

## 4. 最小原型

第一批先完成可测切片，与实际启用路径的Model A/C及必要D子集并行；不把完整stock binary、强撤权、strong reset和多Arena扩展全部作为第一份性能数据的前置。执行阶段为：

| 切片 | 实施范围与输出 | 仍未取得的结论 |
| --- | --- | --- |
| 初始写入切片 | 隔离新storage scope、一个Arena、Bookie/Arena必要控制记录、durable预分配、ByteBuf受控view、固定shard/常驻owner共享DATA block/文件，默认OFF/SYSTEM/PARK且接入安全binding/池初始化；逐entry结果、独立record点读与基础restart；跑B18/B19适用项 | 无dedicated收益、生产listener/ACK、完整恢复、完整Spike或集群性能结论 |
| 写入与本地回收 | 同一原型增加已启用的tombstone/drain/free/reuse、shared-block搬迁及维护预算，跑写入与回收并行、B17和所需fault cuts | 不证明cluster logical delete、强访问撤销或完整多Arena |
| 完整存储/集群入口 | 补全所需tail/IO/rebuild/资源/多Arena矩阵和实际replacement/基础恢复；兼容Gate按既有边界独立闭合 | 仍需实际启用scope全部证据及canary-specific接受 |

每份局部结果列出已实现/未执行路径，只报告测得性能和已覆盖故障，不称完整Spike PASS。无网络局部bench与真实transport bench分开；production adapter要走同一reference corpus验证。先复用现有测试/指标能力与小型workload runner，不为此次测量新建通用压测平台。

完整Spike候选能力清单如下；按启用scope实现真实可崩溃恢复路径，以下清单不是初始切片的全部前置：

- superblock A/B；
- Round 7 `BKPF1` metadata/local Cookie sentinel candidate及atomic local publication；same-scope仍BLOCK，原型结果不能写成accepted format；
- new BookieId/new journal/ledger/index/Arena roots/new incarnation/new OS-service credential/ACL scope fallback，且旧service无法打开或写新scope；
- storage incarnation、required-device manifest、migration generation与versioned registration readiness；
- compatibility hook位于`EmbeddedServer`创建任何可能触碰Profile/Segment storage的component之前；
- `ArenaControlLog`；
- RFC-0005 §5.2选定的Bookie级protected control log及A/B checkpoint；route/fence/grant/tombstone不分散到Arena；
- allocator checkpoint A/B 与 rotation；
- `ALLOC/ALLOC_POOL`；
- shared active slab blocks；
- dedicated extents（后续manifest显式启用，首个切片DISABLED/DEFERRED）；
- block/record framing 与 checksums；
- locator 的 generation + ledger instance 校验；
- durable `DELETE_TOMBSTONE`；
- durable `FREE_AND_BUMP`；
- same-Arena conditional durable `MOVE_COMMIT`；
- per-Arena条件命令合批durable后顺序apply/result、operation identity、assigned control sequence，以及独立的`durableThrough/appliedThrough`；
- Bookie/shard restart replay；
- 100k idle ledger state；
- 可删除并重建的最小 derived index。

内存模拟 control log 或跳过 fsync 的原型只能用于开发，不能产生正式 PASS。

## 5. Authority Oracle

每次运行保留三份独立事实：

1. workload intent/ACK journal：哪些 record 被允许视为 local durable success；
2. 原始 device image 或故障时快照：control、checkpoint、data bytes；
3. restart 后 allocator/rebuild 输出。

离线 checker 必须验证：

```text
one live owner per slot/generation
monotonic generation per slot
every local-success record has durable allocation and valid payload
no locator crosses ledger instance or generation
free list excludes every still-live allocation
checkpoint + suffix replay is deterministic
every committed move chain has one unique authoritative successor
complete control replay does not require legally retired intermediate payload bytes
current required DATA is validated and synchronized before serving its recovered location
checkpoint current selector + anti-ABA state equals full-chain oracle
condition failure changed no authority state
durable apply result covered its own control sequence
duplicate operation produced no second winner/generation bump
uncommitted relocation copy never becomes authoritative
relocation origin and identity survive task/index loss without a mutable DATA commit bit
old allocation is not reused before move cutover and reader drain
orphan new location has no lookup/local-success authority dependency,
while the logical entry may retain its existing success at the current selector
```

checker 与在线 allocator 使用不同代码路径或至少独立解析实现，避免同一 bug 自证正确。

## 6. 功能场景

### B1：Control-first allocation

在 ALLOC append 前、append 后 fsync 前、fsync 后、空间 publish 前注入 crash。

Oracle：未 durable allocation 的空间不能包含被视为 local success 的 DATA；durable allocation 可以在 restart 后成为 allocated 或安全回收，但不能被两个 owner 使用。

### B2：DATA durability 与 local success

在block header、payload、checksum/commit marker、data fsync、readable locator publication、local success及response各边界crash；并发注入同坐标相同/不同payload、normal/recovery重试和fence/tombstone。

扩充原场景：batch 11短写/未durable而12先完成、共享文件的barrier返回时另一批仍在飞行、普通write CQE先于barrier、barrier response loss、DATA submitted后误改header/count/checksum/padding、第二批重写含已成功record的对齐块。不同ledger共用一批；在write、durability、physical-prefix和locator各cut crash，并分别重建index/重启扫描。

Oracle：local-success journal中的每条record可恢复且成功后授权点读立即可定位；不同payload不覆盖pending/durable winner，相同payload幂等、waiter有界。DATA durable但locator未发布不能成功；index缺失期间未覆盖坐标返回not-ready，不能伪造确定absence。

追加oracle：每份completion只证明其实际覆盖范围；12不能跨11的物理缺口成功，未完成短写、物理I/O错误或取消后结果不明不算durable，单条RPC取消不改变物理完成。后续DATA不覆盖旧成功范围。restart保留所有已允许成功的record，不能凭最大completion/entryId截断或跳洞。模型中的成功journal仅为外部测试oracle，不加入生产逐entry ACK日志。

两次故障按顺序执行：允许buffered I/O的路径在完整write后、barrier前终止旧进程但保留未同步可读bytes；新进程扫描并处理重复Add，只有相关control/DATA独立持久证据或必要恢复同步已成立才允许durable success；随后模拟掉电丢弃未同步bytes，再恢复核对所有已允许成功的DATA。恢复同步前的重复请求不得因CRC/locator正确便成功，也不得另写冲突winner。同步其他文件、index/readiness持久化均是负向替代方案。已知I/O error、缺失/损坏依赖不能被后来sync成功抹除；单次kill后读到数据不能作为本项PASS。文件级恢复barrier次数、等待及证据范围单列，不新增逐entry日志。

同一batch封入L1/100与L2/200，submission后fence L1并明确终止其Add；完整write/barrier后batch进入前缀、L2可成功，L1不成功且不制造物理gap。注入单条cancel/disconnect/callback失败、封包前全部取消和分配sequence后submission失败：不修改submitted record/buffer，空assembling不占序号，已分配的失败边界不得任意跳过。未回成功的有效DATA保留作恢复候选，tombstone另按删除合同处理。

延迟RocksDB WriteBatch/flush/compaction，DATA进入前缀且有界热定位发布后，符合权限的entry可local success；append shard不等待本次index入库。热定位移交实际query-visible索引时立即点读，无临时NoSuchEntry；未flush不阻止运行时接管，积压仍占总预算。该场景与B9的crash恢复cut独立核对。

同一预分配文件中，低offset的S0/batch 11留不完整尾部，高offset的S1/batch 50完整durable并已local success，随后crash。恢复先读完整allocator，按stream/generation/sequence识别范围：不因S0逻辑后缀排除而ftruncate、collapse、punch、覆盖或忽略S1；也不因S1完整而越过S0缺口。B9独立恢复oracle验证所有成功DATA及其selector仍可发现，允许回收仅经既有I/O终结与conditional FREE/bump。

### B3：ALLOC pool refill

反复refill、部分使用、shard crash、Bookie crash、未使用pool回收；延迟真实write submission/completion，覆盖旧shard退出、pool转交及generation bump后才到达的completion。

Oracle：pool/shard generation ownership不重叠；restart后unused/live/unknown分类可独立重放。旧写I/O未终结或可靠隔离前不能转交/reuse；仅取消future、忽略late callback或reader drain的方案必须被否证，旧I/O不能改写新owner数据。

同一group记录T1将free slot分给A、T2将同slot分给B；barrier前两者不授予owner，barrier后按sequence apply，T1成功、T2条件失败但消费序号。同一barrier可覆盖两条命令，不能退化成两次独立fsync。分别在durable未apply、T1已apply而T2未apply时crash，重放结果一致；重复FREE不重复generation bump。引用/时间/网络变化不改判既有命令。

复用残留场景联动B6/B9：generation 7有合法header/CRC，durable FREE_AND_BUMP后重分配为8，分别在未写、部分写新DATA时crash，保留原磁盘字节。旧7不进入8的index、不恢复旧owner；按完整分配/stream/batch/selector依赖和故障模型区分unused与unknown，证据不足保持不可写，不能把旧generation本身当作当前媒体损坏。无需每次复用整块清零，记录实际复用写放大；模型/测试不得让FREE隐式擦掉旧字节来消除反例。

### B4：Shared slab 多 ledger

多个 ledger 交错写同一 block，分别删除一个、部分、全部 ledger。

同一block混合L1/L2，按同一delete stream连续发送delete L1、delete L2。L1 tombstone durable但L2仍live导致block不可free时，L1 applied cursor先推进，L2仍可应用；加入checkpoint、清理派生队列丢失与restart/rejoin。不能以等待L1物理释放作为读取下一delete的前置。

Oracle：单ledger logical delete不影响其他record；一个ledger的deleteRequestId不能证明shared block可free。高live比例block中只删少量ledger，再组合部分move/orphan与旧pin/写I/O，仍须等全部occupants不再live、selector及清理条件满足、I/O/pin终结，最后durable generation bump才回收；FREE以allocation/generation为主语，不要求每条记录复制完整occupant名单。

### B5：Hot promotion（dedicated启用时）

首个shared切片标NOT_APPLICABLE/DEFERRED，不纳入其性能结论。后续启用时在阈值边界晋升dedicated extent，crash于promotion决策、allocation、首条dedicated write各点；专属范围不混写其他ledger，共享barrier不要求共享block，不能产生每热ledger永久buffer/文件/线程。

Oracle：shared 旧数据和 dedicated 新数据可组成唯一 ledger history；不要求迁移，不重复/丢失 local-success entry。

### B6：Delete、reader drain 与 reuse

保持reader/pin和未完成writer I/O，触发delete；在admission close、durable terminal tombstone/可重建义务、applied cursor/result、access-barrier receipt、invalidate、drain、FREE_AND_BUMP、physical result及新owner allocation各点crash。覆盖effect-before-cursor、cursor-before-free和tombstone+cursor group commit。

Oracle：cursor只依赖durable delete-applied effect，不等待drain/free；缺effect不能推进，队列丢失可重建义务。reader/pin未drain或旧writer I/O未终结时slot不复用；tombstone后不产生新local success，屏障完成后不再服务；旧locator在新generation失败。applied、barrier、physical result分别验证，restart catch-up不等待compaction。

### B7：Checkpoint A/B rotation

在 control cut `S`、bounded checkpoint chunks、fsync、`CHECKPOINT_COMMIT(generation,S,identity)`、inactive superblock update/fsync、active generation switch、fallback-dependency verification和 old-log reclaim各点 crash。把 `MOVE_COMMIT` 放在 `S-1/S/S+1`，并覆盖 current selector已保存但old source尚未free、old free后historic chain被压缩、A/B fallback仍依赖旧suffix等状态。

Oracle：restart选择一个完整authority，`checkpoint through S + complete suffix >S`与full-chain replay产生相同allocation/current selector/retiring/anti-ABA状态；不得选择损坏的较新checkpoint或回收任一fallback仍依赖的唯一suffix。

制造durableThrough领先appliedThrough，checkpoint只取连续applied cut（含条件失败no-op），不能跳过已持久未应用命令；apply中途crash后从S及完整后缀确定性重演，不追加一份逐命令结果日志。FREE提交前的pin/I/O gate须已建立并保持，重启pin=0不能为无合法前置的旧FREE补证；新FREE重新走本次quiescence。

### B8：Control authority corruption

分别损坏单个superblock、单个checkpoint、可证明未提交的末尾、必需committed prefix、unknown mandatory完整记录、边界无法分类的tail，以及A/B全部authority；同时覆盖durable但response丢失的完整batch。

Oracle：按RFC-0003 §5.4独立分类；只截断可证明未提交的末尾，完整durable transition即使未回ACK也重放；必需prefix损坏/unknown/无法分类保持non-writable。判定不能仅依赖文件末尾、checksum失败或客户端未记录ACK。

DATA部分另按§6.1验证物理前缀：从verified checkpoint/cut恢复，batch缺口后不得丢弃已允许成功的数据；required corruption保持fail closed，control已证明的free/retired区间不是未知gap。control durable-through、DATA physical durable-through与ledger LAC各自比较，不混用序号。

注入direct I/O错误且范围已部分修改、fdatasync错误后再次sync返回成功、11 unknown/12完整，以及同文件多个stream。Oracle：至少暂停受影响stream，无法缩小错误范围则暂停共享文件相关stream；必要时升级既有设备/Bookie non-writable。后续DATA不跨缺口成功、不因RPC超时丢弃、不任意跳号/新建stream或覆写绕过；重复sync成功不清除未解析失败。实际I/O终结后才释放无使用者buffer，未知坐标由pending或不可写范围接管，恢复前不接受冲突写；异常对象保持上限。

### B9：Derived index deletion

分别运行保留有效index/checkpoint的正常restart，以及完整删除RocksDB/locator index的full rebuild；首批数据包含active/sealed shared block与relocated record，dedicated启用后必须增加sealed dedicated extent，不能借首批延期漏扫已经启用的布局。

Oracle：full rebuild枚举全部需要重建的live allocation/有效DATA范围，结果与独立全量authority oracle一致；normal restart只能在有覆盖证明时缩小扫描范围。stale generation不进入index，未验证范围不返回确定absence；分别记录scan bytes/I/O、peak memory、read-only/可写时间及前台竞争。

复用B10的A→B→C控制后缀：A/B已FREE并由新generation覆盖，checkpoint仍引用A，删除全部派生索引后重启。先重演成功/失败metadata predicate得到C，再验证和按需同步最终必要DATA；不得因A/B已合法退出而隔离，也不得为了通过测试在FREE前偷偷生成checkpoint。C坏/缺失或必要控制后缀缺口必须隔离，不回退A/B；旧checkpoint作为fallback时仍须保留完整后缀并得到同一结果。

另外在K的原位置A已有效时，将带受校验RELOCATION_COPY身份的B写完整并durable，MOVE尚无完整命令就crash并清空索引。分别取无MOVE、可证明未提交的尾部、完整但条件失败的MOVE，以及成功MOVE；前几种仅A为当前位置，最后一种由控制选B。未知/损坏的必需控制后缀仍fail closed，不当无MOVE。交换扫描顺序、offset/generation大小、删除任务对象均不改winner；来源/operation/target身份损坏拒绝，客户端伪造来源无效。MOVE前后DATA字节不修改提交位，正常/恢复ENTRY_DATA不因来源类型就被视作已ACK。

重放B2共享S0/S1不相邻offset与B3 generation 7→8残留镜像，normal/full rebuild均保持其他stream成功DATA及当前owner；不能把文件高offset当跨stream后缀或把旧合法CRC当当前DATA。文件长度保持首批共享布局合同，逻辑排除不直接产生filesystem free bytes。

包含乱序完成batch、已回收区间和搬迁目标；独立checker验证§6.1成功前缀/可发现性。内存physical durable-through丢失后，先从authority/framing找合法候选，再以独立证据或§12必要恢复同步确认durability，不能把扫描完成当barrier；MOVE_COMMIT成功选择的新DATA不得落在该stream逻辑排除后缀。

按RFC-0003 §15关闭纯派生索引WAL，注入DATA durable、Put/WriteBatch成功未flush、flush完成未发布coverage、coverage发布及热定位淘汰各cut的crash。重启仅跳过实际持久且generation匹配的连续覆盖；marker缺失/损坏、index generation变化和多CF启用时部分flush均不能错误跳过DATA。Bookie/Arena权威控制日志保持durable，完整selector/tombstone控制后缀仍恢复。

并发交错旧Add索引update、MOVE新selector和delete清理，验证陈旧update不能复活旧定位；点读从当前selector重新pin/解析，不把旧缓存失败当作payload丢失。热定位淘汰只等实际query-visible接管，不等flush；runtime可见、persisted cut和物理durable-through各自有独立观测，不能以最后Put/回调当coverage。

固定点读为hot-first，命中保护对象、miss后才fresh Get当前DB。精确交错DB变query-visible与hot淘汰，命中后再交错selector/pin切换；negative fixture采用DB miss→接管/淘汰→hot miss，以及早于hot查询的snapshot/iterator/negative-cache参与fallback，必须被否证。两份locator始终至少存一份不足以让错误查询顺序PASS；不得靠跨索引全局锁或持ledger锁等DB I/O规避。B18复用此交错和资源观测。

补充大batch内小entry点读，分别命中/未命中cache；只读目标record所需对齐范围，独立验证envelope/坐标/generation及BK CRC，不强制整批读取。注入locator与header坏length/溢出/越界、外层身份损坏但BK CRC正确、pin取得时selector切换、慢reader/断连；范围与buffer有界，未恢复覆盖不返回确定absence。记录真实I/O及pin，不以模型计数冒充磁盘测量。

### B10：Compaction copy

对部分死亡shared block执行same-Arena compact；source须已停止追加、原始写入已解析、内容/占用可确定。保留OPEN ledger在其他物理范围继续写入，证明搬旧不可变范围无需ledger close/fence/seal；仍接收record或I/O未解析的source不得入选。对同source/generation的正常任务调度去重，异常竞争另行注入。在新allocation、copy、DATA durability、conditional `MOVE_COMMIT` append/group durability/顺序apply/response loss、原子selector发布并关闭old-pin admission、reader/writer quiescence、old free各点crash；不能把selector发布与new-old-pin阻断实现为两个独立cut。FREE提交前建立并保持quiescence gate直到结果解析，含unknown；只持久化但未成功apply的MOVE/FREE命令不授予cutover或reuse。

必须覆盖：

- 异常重试/迟到任务形成同一predecessor的两个concurrent moves；正常调度不主动重复搬同一source；
- move chain `A -> B -> C`，control 101 MOVE A→B、102 FREE A、103 MOVE B→C、104 FREE B后A/B复用覆盖，未生成新checkpoint即crash；重演不读取已退出旧payload，最终C或必要控制缺失则隔离；
- batch/group-commit torn tail；
- new payload digest mismatch；
- 每个边界删除 derived index 后重建；
- 只迁移部分 records 时尝试 whole-block free；
- old reader/pin 跨越 cutover；
- checkpoint 覆盖/未覆盖 `MOVE_COMMIT` 时 rotation crash。
- checkpoint后stale old operation retry；
- logical entry已有local-success，但uncommitted new location从未成为success/lookup location；
- RELOCATION_COPY durable completion只形成内部copy结果，不直接调用普通Add定位发布；MOVE成功后才切换selector，record来源不覆写。与B9联合删除全部索引并改变扫描顺序，完整copy不能自行提升；
- orphan GC与late commit在同/不同group竞争，分别覆盖free先赢与commit先赢；
- shared allocation一条orphan、一条live，以及writer-generation/pin quiescence race。
- 同一operation在enqueue前、append后force前、force后response loss重试；
- group内独立condition一成一败、`durableThrough < ownSequence`、middle gap/torn/unknown mandatory record；
- idempotency summary compact后的extremely-late retry，以及queue/waiter/future cap压力。

按有界victim组测试低/高live比例和allocation粒度：20%/90%/99%仅作成本对照输入，不是固定准入阈值。包含单source搬到同样大destination而零净收益、多个source打包后有净收益，以及padding/framing使预估失效；搬迁/FREE前复核selector/tombstone/generation。跨source复制与控制group commit应能合批，不要求每victim/entry fsync。报告实际copied bytes、释放source容量、新占destination容量和net reusable bytes gained；pin未释放前不得记已实现收益。

Oracle：无成功commit时old authoritative、new copy只是orphan；MOVE命令durable且顺序条件apply成功后new authoritative且index可重建；同一predecessor只有一个winning successor。每个live record至少一个authoritative lookup locator（允许cut前old reader pin），不能有两个new lookup winners；old block只有在全部live records moved/dead、new pin被阻断、既有reader drain，以及FREE命令durable且成功apply后回收。relocation不新增local-success fact；清理orphan new location不删除logical entry在current location承载的既存success。pending/admitted append或条件失败的durable命令均不授予authority；失败只推进applied sequence而不改业务状态，duplicate只返回same durable result或stale/conflict。

## 7. 资源规模场景

### B11：100k idle ledger

创建并安装原型级 100k ledger state，不写 payload或仅每 ledger 写极低速记录。

必须测量：

```text
Java heap retained bytes
direct memory
native allocator bytes
thread stacks
RocksDB memtable/block cache
open file descriptors
active block buffers
reserved data-arena bytes
control metadata bytes
```

硬 Gate：

```text
fixed extent reservation per idle ledger = 0
per-ledger block buffer                  = 0
shared assembling block buffers         = O(shards), plus bounded inflight/read/cache pools
idle ledger state                       <= 1 KiB per ledger
total ledger metadata resident memory   <= 128 MiB per Bookie
```

目标值：idle ledger state 不高于 512 bytes。目标未达但硬 Gate 达到可以记录为待优化；硬 Gate 失败即 FAIL。

测量必须给出 accounting equation，不能用 RSS 单值同时代表 ledger metadata。

### B12：Cold shared write

1k、10k、100k low-rate ledgers交错写，再混入大量hot ledger和慢点读者；首批仍shared block。验证assembling与全阶段buffer/cache/pin不随ledger无限增长，不停止回收来维持p99；记录group commit、fragmentation、dead bytes及父buffer驻留。小slice长期保留大父buffer时验证预算内复制与最后使用者释放。

### B13：Hot dedicated write（dedicated启用时）

首个shared切片标NOT_APPLICABLE/DEFERRED。后续以1、10、100及manifest资源上限的hot ledger比较allocation rate、tail waste、write amplification、point-read与recovery scan；按需dedicated assembling/buffer池、文件/线程数量和峰值内存单列，上限后新allocation回共享布局或背压，不混写既有专属范围或增加无界buffer。

此场景用于选择 RFC 参数，不设“某个 extent size 必须胜出”的事后 Gate。

### B14：Stock old binary pre-replay downgrade fence

步骤：用manifest锁定的每个真实stock old binary/release/commit和启动模式/tool/storage-expansion入口，打开分别包含metadata/local `BKPF1` sentinel、仅Cookie version bump、仅Cookie optional property、仅new superblock/file、仅unknown negative Journal meta-entry、仅registration property的scope；覆盖data-integrity enabled/disabled、Cookie auto-stamp、旧进程已运行/未完全退出、stale exclusive lock、BookieId reuse、Journal replay与writable registration。instrument old process在退出前触碰的每个file/path/byte和write/open动作。

Oracle：唯一可接受same-scope candidate必须让每个supported old binary在任何Profile storage open、Journal replay、Arena/data writer、handle/lazy storage、registration与write前确定退出，且不stamp over sentinel。仅“最终未注册”不足；data-integrity路径先创建LedgerStorage时任何不允许的file touch都使candidate FAIL。Cookie optional/version、registration、new file/superblock或unknown Journal record被忽略/restamp/skip必须记录为否证。若无candidate可证明，正式结果锁new BookieId/new roots/new credentials/ACL fallback，原scope不可写且旧service无access。

### B15：Partial migration、device manifest 与 rollback

步骤：按drain/connection close → exclusive storage lock → persistent PREPARED/Cookie sentinel CAS → 每required directory atomic local sentinel → device superblock → control/route recovery → delete catch-up → durable local readiness → persistent `ProfileRegistrationStore` CAS → matching BookieServiceInfo/ephemeral registration，在每个边界crash/response loss；覆盖多device子集、missing/corrupt/unknown mandatory、device replacement、stale registration、rollback/reverse/wipe/new-incarnation，以及new BookieId/new-scope fallback的access-denied与placement/readiness。

Oracle：任何partial/mismatch/unknown/corrupt状态整个Bookie non-writable并重试同一migration generation；persistent readiness CAS先于ephemeral registration，response loss重读两层且generation/incarnation mismatch demote。存在任一local success/route/activation/fence/grant/tombstone/Arena authority/durability unknown时same-scope old-binary rollback拒绝；恢复只可roll-forward、verified export/rebuild、irreversible wipe/decommission或new incarnation。new-scope fallback只在旧BookieId drained/readonly/decommissioned且旧credential不能访问时可writable。

Wave 0已完成一个不访问真实filesystem、OS权限、registration backend或外部目标的typed reference implementation与immutable receipt；17项普通测试机械覆盖上述ordering、CAS/response loss、九个crash cut、device负向状态、stale demotion、rollback拒绝和new-scope access-isolation语义。它只证明reference state machine和内存adapter满足当前合同，不能替代本节要求的真实stock binary、file-touch、multi-device、physical durability、startup raw metrics或formal Spike运行；Spike状态继续是`Planned / Not Executed`。

### B16：Bookie控制日志与多Arena部分持久化

一个ledger跨至少两个Arena，组合route/install/activation、fence、grant close/tombstone、DATA durability及各日志A/B checkpoint/rotation，在每个相邻持久化边界注入crash/response loss。保存Bookie控制日志和所有Arena原始镜像，使用独立parser比较replay。

Oracle：Bookie级权限在所有Arena一致；部分成功不能扩张接受集合；required store不完整时不注册writable。route/profile binding、activation/fence/grant/tombstone只由Bookie控制日志持有，Arena仅消费必要引用，不要求逐Arena重复tombstone durable；Arena FREE按allocation/generation与整单元条件接受。normal Add无控制日志fsync或远程read，DATA不重复写入Bookie控制日志。报告真实fsync/bytes、cold/warm恢复时间及保护credential的非泄漏检查。

联动B2两次故障检查启动依赖：Bookie/Arena控制候选及DATA各自缺少持久证据时按文件有序恢复同步，durable条件命令按序apply；readiness/registration不得先开放。仅同步某一control文件而目标DATA仍未同步时，禁止重复Add或恢复证据路径发布durable结果，已知写回错误仍隔离。

### B17：空间耗尽与长期回收进展

在manifest锁定的有界live set与admitted写入速率下持续create/write/delete，使大量shared block仅剩少量live records；另运行超过可持续能力的压力矩阵。分别耗尽Arena前台whole-free blocks、内部compaction目标预算，以及实际filesystem的SST/flush/compaction、Bookie/Arena控制日志/checkpoint预算；在maintenance与full-disk状态重启。构造Arena仍有free slot但filesystem低余量，以及多个Arena已启用时共享同一filesystem的预算竞争，不能重复预留同一余量。

Oracle：前台在侵占维护保留量前限流/拒绝，queue/memory有界，维护有最低调度份额；恢复空间后按hysteresis开放。在声明负载、可获益候选与维护I/O下，debt/dead bytes不持续增长，停止新写后在deadline内达到可行目标水位。另构造近乎全live且无净收益的候选：低水位不无限重复复制，须有界背压，待实际删除形成可获益victim组或容量恢复后继续。不能靠暂停必要compaction通过p99，也不能以复制量冒充回收量。候选分布、净收益阈值和保留量先于run冻结，不要求无限容量或所有高live布局立即排空。

追加Oracle：DATA预分配/增长上限保留真实filesystem维护工作集，低水位先停增长/限制新DATA，SST flush/必要compaction及tombstone/checkpoint在声明保留量内继续推进。仅FREE内部slot而filesystem available未变时，不解除索引空间压力；真正I/O error/authority unknown走错误恢复。分别报告Arena reusable、filesystem available、derived-index disk bytes、maintenance temporary-space peak，普通删除不靠hole punch/缩短文件回空间，关闭index WAL不当作零SST空间成本。

### B18：ByteBuf、固定shard与批量DATA路径

按RFC-0001 §11.6及RFC-0005 §10.1—10.5实现网络接收到固定shard owner的路径，NUMA池消费RFC-0003 §6.2。reference immutable byte-array codec保留作oracle，以相同valid/invalid/truncated/oversize corpus验证解析和拒绝等价；增量接收测试独立组织，不重写历史corpus/receipt。跟踪socket/TLS、header/半包、完整未准入请求至readable publication的实际buffer/预留与每次复制/ownership交接；允许集中对齐复制，禁止未计量的逐层整包clone。异步retain/release覆盖normal、拒绝、取消、断连、retry和I/O失败；源view最后使用后释放，已提交I/O的buffer直到真实I/O终结才可复用。

多连接同时发送合法但不完整的大frame，另覆盖header/body同一read、TLS累积/解密、长度/阶段/subtype错误、超时/close和解析失败。大body累积前先通过固定header检查并取得连接/Bookie credit；初始连接/TLS/read工作集与暂停前pipeline残留均受总量限制，暂停本身不释放bytes。向存储交接不得漏计、重复永久扣款或挤占提交/定位工作集，无额度时非阻塞暂停/关闭而不是收完整包才拒绝。恢复读取与失败清理后credit/实际held资源一致；1,000连接各持4 MiB约3.9 GiB仅为容量反例，不是实测参数或结果。

令DATA连接在未完成body时暂停，再发合法fence/tombstone；首批同一Profile listener上的有界独立控制连接仍完成TLS/HELLO、真实operation/身份/授权检查并进入保留执行资源。负向fixture把控制排在暂停DATA body后，应显露不可达；仅保留线程/日志buffer不能算PASS。覆盖控制连接/接收总额、DATA冒充控制、错误授权及资源归还；控制通路不绕过总账、不新增listener/每ledger连接。分列TLS/decoder retained bytes、partial-frame数、receive reserved/held峰值、暂停/恢复次数及控制接收/完成等待；只注入完整请求的无网络run对此标NOT_EXECUTED。

多个ledger合批，固定shard，bytes/count/wait均有上限；注入低负载等待、队列满、pool refill、slow/failed force、fence/delete race及compaction并发。验证不持ledger锁等待I/O，普通已准入写不串行等待三层control/DATA队列或三个future。真实barrier前无local success；不能将取消future当成I/O终结。计量实际entries/barrier、force次数、thread hops、queue wait、lock hold和各层控制写；每entry force不能仅因API名为group commit就算批量化。

扩充现有B18负载：持续小请求不重置最老deadline；低负载到期padding；超过普通block但在声明entry上限内的请求走预算内大记录批次，否则明确拒绝；热ledger与低速ledger混合、重复核对慢读、locator/响应阻塞、断连/取消但I/O未终结、DATA满额时fence/tombstone到达。全阶段request/源与对齐bytes/inflight batches/waiters有界，出队不提前还credit，慢读不阻塞append主循环，控制保留容量可关闭准入。记录低负载、目标负载和过载结果，不只测queue.size()。

加入满载推进反例：分别暂停输出buffer归还、DATA completion和index接管，把入口推到准入上限；source payload不能挤占每shard已计费共享提交工作集，已准入entry有定位/完成容量，hot/index阈值在接收新DATA前生效。大entry无对应工作集时先背压。恢复下游后已接受请求须继续完成、资源最终归还，无总量超限、event loop/shard阻塞、持ledger锁等待或永久循环依赖；下游永久不可用时不要求继续收新写。禁用这些保留规则的负向fixture应显露source等destination、durable DATA等locator的循环，而非用临时超额解围。

DATA freeze和barrier/prefix沿B2 oracle，量化padding bytes、write/durability/prefix/locator wait及阶段资源峰值。相同durability与TLS范围才比较；局部无网络run不计端到端增益。

同一切片消费B2逐entry/物理结果分离及B8错误隔离；记录每个实际DATA文件的barrier次数/等待/覆盖和受影响stream，不将per-shard table当独立flush域。点读消费B9路径，孤立请求不等待凑批，相邻请求有界合并；小entry受控复制释放大父buffer/pin，读盘、cache、response和回收并行都在总预算内。

持续制造index WriteBatch/flush/compaction stall，同时新写、立即点读及fence/tombstone。append shard不能同步卡在索引调用；hot/async queue/WriteBatch/memtable/native/cache全部有界，达到统一预算后拒绝后续DATA，控制仍推进。容量恢复后按A26/A27有界重试，不产生每次拒绝都换组的风暴；先前submitted unknown不被当前NONE清除。增加index入库等待、持久覆盖落后量、资源拒绝/retry/replacement计数；真实网络未执行的部分维持NOT_EXECUTED。

消费A26/B19统一N，核对N边界的normal/recovery/read/Segment实际buffer和对齐范围，以及负值/溢出在大分配前拒绝。与B17区分内存index backlog满和filesystem空间不足：Arena仍有free slot时也可因真实磁盘维护预算拒绝，分别计量四项磁盘指标，不增加层层资源管理器。

**固定owner、CPU与NUMA的分离验证（仍属B18）。** 固定线程模型先交付，安全显式binding与拓扑测试归现有`cpu-affinity`模块；以下均为待实现/待运行，不用当前Classic acquireCore/busy-wait路径代替。先以相同实现运行四组：

| B18内对照组 | 配置 | 单独解释的变化 |
| --- | --- | --- |
| A | 固定owner，affinity OFF，SYSTEM池，PARK | 固定线程模型基线 |
| B | A＋CPU pin，仍SYSTEM/PARK | CPU绑定；同时记录页分布，不能假设默认分配完全相同 |
| C | B＋指定OWNER_TOUCH或PREFERRED池，仍PARK | 内存属地策略；记录实际分布与fallback |
| D | C＋BOUNDED_SPIN | 有界spin的增量收益与CPU成本 |

四组保持worker数、逻辑shard数、batch/队列/内存预算、I/O/barrier、TLS、E/W/A、索引与compaction负载一致，锁定同一CPU/quota资源与机器环境。C如比较两种池策略分别命名run，D复用同一C策略；不把多开worker、停后台维护或更高资源额度算成affinity收益。保留低负载、目标负载、过载、维护压力和多ledger混合负载；支持环境不足时报告NOT_EXECUTED/降级，不伪造B/C/D成功。

| 验证场景 | 必须得到的结果 |
| --- | --- |
| 8 shards/4 workers；恢复后改worker/CPU数量 | 每shard仍只有一个owner，CPU取模不重写ledger/stream身份；正常、恢复、搬迁completion消费原合同 |
| 输入满额时completion、fence和跨线程归还到达 | 完成/控制/归还保留容量仍可用，I/O线程不并发改pending/前缀，原池最终收回buffer；不阻塞owner/Netty等盘 |
| 双socket同号core、非连续cpuset、仅一个SMT sibling、可见16 CPU但quota约2 | 不跨socket误分组、不把编号当数量、不要求关闭sibling；服务余量与quota计入计划，不增加一批持续spin线程 |
| 非root、native缺失、权限拒绝、无可用CPU、部分绑定失败 | 不写cpu online/宿主隔离/IRQ配置、不走文件锁抢核；PREFER清理并报告实际降级，REQUIRE服务开放前失败；非法列表/冲突仍明确报配置错 |
| owner已绑单核后创建服务线程 | 服务线程入口取得其计划mask，不意外整池继承单核；OFF使用原部署mask创建路径；JVM内部线程不被强行改绑 |
| 绑定后再创建/触碰池、预先被启动线程触碰的反例、设备node=-1、mbind拒绝 | 只对自有完整新映射设置策略，未知不当node 0，实际页分布/回退可观察；不把Netty slice再触碰当硬归属证明 |
| 跨节点I/O/网络释放，cpuset/memory-node/quota运行时变化 | buffer回原池且有界，飞行I/O内存不迁移/早释放；PREFER原owner继续并关闭spin，REQUIRE绑定失效时停准入、drain/退出；不污染磁盘authority |
| enqueue与park竞争，低负载deadline到期，持续DATA/多shard混合 | 无丢失唤醒，最老deadline不重置，spin有界，completion/控制不饥饿；PARK不无限等待下一条请求 |

绑定fixture保留请求mask、实际mask、核/SMT/节点与设备映射、失败及清理结果，验证新接口不触发CPU上下线或宿主文件锁。拓扑/配置/作用域清理使用确定性单测；native绑定、真实线程继承/返回路径、NUMA页分布及干扰成本使用同一隔离存储切片，不另建模型或压测平台。启动探测不进入每Add，实例数量增大不出现每ledger线程/池；开关优化及失败降级后的ACK条件、fence顺序、重复/unknown结果和恢复DATA保持一致。

报告吞吐、p99/p99.9、CPU time/entry、空闲CPU消耗、调度迁移、各阶段queue wait、quota throttling与服务线程占用；自有池记录映射/页分布、跨节点归还与fallback，NIC/IRQ/NVMe及JVM干扰单列。`numastat -p`可辅助观察进程节点分配，但`numa_hit/numa_miss`是分配统计，不等于实际远程访问率；访问成本须结合该硬件支持的计数器，不能测时标未知。依据[numastat(8)](https://man7.org/linux/man-pages/man8/numastat.8.html)。没有可比收益就保留OFF/SYSTEM/PARK，不以消耗更多核隐去成本，也不把优化开关作为正确性前置。

### B19：同坐标identity与基础恢复重写

用现有DigestManager与LedgerRecoveryOp语义构造同instance/entryId/应用bytes/累计length、不同合法piggyback LAC及digest的normal/recovery重写，应幂等且不覆盖不同数据；另测不同应用payload、累计length冲突、坏digest、跨instance、并发pending、乱序/hole、`E>W`、restart及derived-index丢失。每份输入完整性与authority都验证，不以整包bytes或全BK digest判定逻辑冲突。

首批向量固定32-byte BK metadata + 4-byte CRC32C + opaque payload，验证CRC覆盖、长度/截断、outer/inner ledger/entry mismatch、未知或非CRC32C安装拒绝及不改变60-byte context。构造不同bytes但相同CRC32C的碰撞向量，证明checksum命中不能跳过真实bytes核对；相同业务bytes而LAC/digest不同仍幂等。不得从20-byte ledger credential推导HMAC能力、下发password/MAC key或为每entry新增SHA-256。布局向量属于后续新run，历史frame/operation corpus保持原证据身份。

统一大小向量由四条实际路径共同锁定N：N-1/N正常写、点读与single-entry recovery均可表达且内容一致；N+1首次准入拒绝。另覆盖normal单项允许但recovery超限的44-byte区间，不能因normal codec可编码而接受；read/Segment更小则相应收紧N。降低runtime新写阈值后旧N entry仍可读/恢复，临时预算不足只背压，不改写大小能力或拆分entry。此为待实现的新验证，不修改历史corpus/receipt。

K/X提交后timeout，再到K/Y：原坐标不变为空，Y不能成为新winner；分别令X迟到完成、部分写入、barrier unknown、restart重建，相同X重试只能复用已解析且权限有效的结果或暂不可用。未提交请求可原子撤销但不得与封包竞争后误释放；fenced原调用失败不抹除有效恢复候选。另按A16/RFC-0004 §7.5运行LAC=99/entry 100点读，confirmed/unconfirmed/恢复各守边界，DATA完成不伪造quorum LAC，explicit LAC合批不新增每Add控制fsync。

恢复否定证据联动A16/A26及Model A-POINT：fence durable但坐标pre-cut I/O未解析、热索引miss或rebuild覆盖未恢复均不得返回definitive absence；未fenced来源先missing再收到旧写时不能提前close。身份/incarnation/上下文重验、重复响应去重和已持久fence跨entry复用沿Classic算法的Profile适配检查，不另建quorum实现或逐entry控制fsync。

恢复长度沿RFC-0004 §7.4验证：entry 99的length=1000、entry 100携带LAC=99/length=1100；在合法终点P=99的向量中不能发布(99,1100)。覆盖fragment起点抬升但hint length未匹配、没有新tail、空ledger(-1,0)、相邻应用长度不连续/溢出、P的边界读暂不可用，以及close响应丢失后同P不同L。复用已读P或matching closed authority，否则一次必要P点读；不从0重扫、不把frame/padding算进长度，不新增LAC→length表。与真实恢复联调未执行仍单列NOT_EXECUTED。

独立oracle比较不可变字段及应用数据，重建后核对每个坐标；`entryId <= localLastEntryId`不能充当存在证明。正常新entry、pending retry、已有durable命中、冲突和rebuild慢路径分别计量索引reads、hash invocations、allocation/copy bytes；不强制每新entry一次RocksDB查询和独立SHA-256，不建立全量去重库。与恢复代码的真实端到端联调未运行时单独标NOT_EXECUTED。

## 8. 性能场景

首批固定硬件、durability和E/W/A（集群先3/3/2、3/3/3；局部shard测试注明quorum未执行），优先测正常写、实际换组故障、写入与回收并行。局部切片未实现的场景留作明确后续项，不用模拟吞吐宣称集群收益。除吞吐/p99外，每项同时记录：

```text
allocated heap/direct/native bytes per logical entry (with ownership scope)
payload copied bytes per entry, broken down by layer
CPU time per entry, including the declared background share
entries per completed durability barrier and force count by log
DATA-file barrier count/wait/coverage and affected-stream scope
device bytes written / host application payload bytes
point-read bytes / returned bytes, I/O count, cache hit rate and copy bytes
read pin duration, shared parent-buffer retention and bounded dedicated-pool peak
compaction debt/dead bytes over time and stop-write drain
queue wait/depth, thread hops and ledger lock hold time
B18 A/B/C/D p99.9, idle CPU, scheduling migrations, quota throttling and service CPU share
requested/actual affinity masks, owned-pool page distribution and NUMA fallback/return costs
```

分母区分logical entries与delivery attempts，重试放大不可被隐藏；明确process CPU/JVM allocation测量窗口、后台归因、device counter或I/O trace来源。与Classic在相同durability/offered load下比较；没有硬件/原始数据时只报告静态复制风险，不声称性能回归幅度或收益。数值预算先于正式run锁定，初始探索值不能追认成PASS。

entry sizes：

```text
1 KiB
4 KiB
32 KiB
```

负载：锁定 baseline saturation 的 30%、60%、80%。至少包含：

- no-background baseline；
- control pool refill；
- checkpoint rotation；
- shared-block reclaim；
- compaction；
- full derived-index rebuild。

另设独立startup/read-amplification矩阵，不把它混入Add热路径：

```text
cold and warm Classic-only startup baseline
cold and warm Segment startup
1 and manifest-maximum required-device counts, plus intermediate points
compatibility-fence read bytes and I/O count
pre-storage-open hook latency and files/bytes touched
device-manifest + per-Arena superblock read bytes and I/O count
allocator/route/delete recovery and readiness/registration phase latency
persistent readiness CAS + ephemeral registration latency
total time to read-only and writable readiness
```

每个phase报告原始样本、分布、device-count scaling与matched baseline；exact latency/read-amplification threshold保持OPEN，不能看到结果后补Gate。最低证据要求是所有计数可独立归因，Classic-only startup不会执行Profile/Arena验证或连接handshake，且format/readiness检查只发生在startup/migration/registration，normal Add中的相关read/remote-I/O计数为0。

核心硬 Gate：

```text
allocator background activity foreground p99 regression <= 5%
```

比较必须在 matched offered load、相同 durability、相同 shard 和重复次数下进行，并报告 confidence interval/原始样本。无法稳定复现为 INCONCLUSIVE，不按 PASS。

## 9. 写放大与空间指标

必须记录：

```text
host payload bytes
device bytes written
control-log bytes
checkpoint bytes
data padding bytes
compaction copied bytes
net reusable bytes gained (source capacity freed minus destination capacity charged)
move control-log bytes and durability barriers
sequencer queue depth and waiter/token count
conditional retries and condition-failure counts
dead but unreclaimed bytes
dedicated tail waste
```

本 Spike 不把 `<= 1.25x` 作为所有原型 workload 的唯一 Gate，但必须证明计量方法可用于后续 production-candidate Gate。不得把 control/checkpoint/compaction bytes 排除后宣称总写放大。

## 10. 故障矩阵

每个 B1-B10 场景至少执行：

- deterministic crash at every named boundary；
- process kill；
- simulated torn sector/block where injector supports；
- response/local-success publication loss；
- `MOVE_COMMIT` durability/response loss、concurrent move 与 reader-pin cutover；
- checkpoint current-selector compaction、orphan free/late commit与durable-through gap；
- conditional enqueue/append/force/result、duplicate response loss、unknown mandatory record与selector/pin acquire cut；
- repeated restart；
- fixed-seed random operation/fault sequences。

B14/B15必须另执行完整stock binary/boot/migration matrix；模拟parser或mock registration不能替代正式结果。

B16必须运行完整Bookie-control/多Arena crash矩阵；B17必须完成空间耗尽、maintenance restart、持续churn与stop-write drain矩阵。不能只运行原B1-B10后声称新增要求PASS。

B18/B19按锁定切片验证buffer生命周期、实际合批与LAC/digest兼容；局部PASS不覆盖后续真实transport、集群replacement或recovery场景，也不替代B16/B17/完整资源Gate。

最低随机矩阵：

```text
seeds >= 1000 before formal run lock, or a reviewed equivalent budget
operations per seed fixed in manifest
all failing seeds preserved and replayable
```

预算可以在运行前评审调整；不能看到失败后减少 seed 数并宣称 PASS。

## 11. 硬 Safety Gate

PASS 必须满足：

```text
double allocation                             = 0
stale-generation successful read              = 0
cross-ledger-instance successful read          = 0
local-success payload lost after recovery      = 0
other-stream DATA removed by logical tail recovery in a shared file = 0
old-generation residual accepted as current DATA or owner = 0
entry first accepted but unrepresentable by required read/recovery/storage = 0
filesystem maintenance reserve consumed based solely on Arena free slots = 0
same filesystem capacity reserved independently in full by multiple Arenas = 0
FREE/reuse before durable generation bump      = 0
reader-pinned slot reused                      = 0
old writer I/O corrupted a reused generation   = 0
same-coordinate conflicting payload overwritten = 0
valid recovery rewrite rejected solely for LAC/digest variation = 0
corrupt input accepted through logical dedup shortcut = 0
hole treated as existing entry from localLastEntryId alone = 0
buffer use after release or premature I/O-buffer reuse = 0
buffer reference leak or duplicate release in tested paths = 0
local success before readable location publication = 0
restart readable candidate reported durable before valid evidence or required recovery synchronization = 0
recovery rejected a fully proven move chain solely because retired intermediate DATA was reused = 0
recovery skipped missing current DATA or required control suffix, or fell back without authority = 0
successful data lost after process restart followed by power loss = 0
later sync success cleared unresolved I/O error or missing authority dependency = 0
read-after-success gap during hot-to-query-visible index handoff = 0
DB-first or stale-view fallback falsely reported absence during hot handoff = 0
unpersisted index contents counted in durable coverage = 0
stale async index update revived old selector or deleted entry = 0
append shard synchronous wait on derived-index write/flush/compaction = 0
index backlog exceeded total budget or starved fence/tombstone = 0
admitted DATA stranded by input consuming submission/locator working set after downstream resumes = 0
resource wait blocked Netty event loop, append shard or held ledger lock = 0
submitted DATA block mutated or old successful range rewritten = 0
ordinary write completion counted as durability = 0
barrier attributed to incomplete or unrelated batch writes = 0
local success outside recoverable physical durable prefix = 0
physical batch failed solely from one entry's logical failure = 0
entry succeeded solely because its batch was durable = 0
submitted coordinate became absent solely from timeout/cancel = 0
conflicting retry won before uncertain prior write was resolved = 0
I/O-unknown stream bypassed by sequence skip or unverified sync retry = 0
necessary point read rejected solely by stale local LAC = 0
DATA completion fabricated quorum LAC or per-Add LAC control fsync = 0
point read required whole batch as its only integrity check = 0
bad record length caused unbounded or out-of-allocation read = 0
unbounded dedicated buffers or slow-reader parent-buffer/pin retention = 0
delete-applied cursor depended on whole shared-block physical reclaim = 0
cursor advanced before durable tombstone/reconstructible obligation = 0
applied cursor falsely reported access-barrier/physical completion = 0
end-to-end request/bytes/inflight/waiter budget exceeded = 0
oldest batch deadline reset by newer arrival = 0
accepted oversize entry stalled permanently at queue head = 0
duplicate read blocked append loop or DATA exhaustion starved revocation = 0
required durable prefix misclassified as discardable tail = 0
unknown rebuild coverage reported definitive absence = 0
unfenced source or unresolved pre-cut write counted as definitive absence = 0
cross-Arena partial state expanded Bookie authority = 0
Arena independently recreated Bookie route/tombstone authority = 0
foreground allocation consumed maintenance reserve = 0
checkpoint replay authority divergence         = 0
authority-loss device resumed writable         = 0
derived-index rebuild changed payload facts    = 0
uncommitted move copy became authoritative     = 0
committed move lost after index deletion       = 0
multiple winning successors per predecessor    = 0
source freed before move commit/reader drain    = 0
single-ledger reason or partial move authorized whole shared-allocation FREE = 0
normal compaction selected mutable/unresolved source or duplicated a source/generation task = 0
no-gain victim set repeatedly copied without bounded backpressure = 0
net reusable gain reported without deducting actual destination allocation/padding = 0
move created new local-success fact             = 0
orphan GC removed current local-success payload = 0
index/task loss promoted an unselected relocation copy by scan order or physical generation = 0
relocation durable callback published ordinary Add locator or local success before MOVE apply = 0
MOVE publication rewrote DATA origin or commit bit = 0
checkpoint selector differed from full chain    = 0
late commit and orphan free both succeeded      = 0
conditional failure mutated authority state     = 0
conditional state applied before command durability or out of sequence = 0
checkpoint cut crossed a durable but unapplied command = 0
replay changed command outcome using current time/network/arbitrary metadata = 0
FREE replay invented pre-submission quiescence from zero current pins = 0
durable result before own sequence durability   = 0
duplicate operation created second winner/bump  = 0
recovery CLOSED paired prefix P with another coordinate's cumulative length = 0
unknown mandatory record skipped writable       = 0
selector cut allowed new old-location pin        = 0
queue/waiter/idempotency hard-cap violations     = 0
uncharged TLS/decoder/partial-frame or decoded-before-admission bytes = 0
large-body accumulation before required receive-credit reservation = 0
receive-credit leak or permanent duplicate charge after handoff/close/error = 0
DATA receive pause blocked all required bounded control paths = 0
control connection bypassed operation authorization or total resource cap = 0
unreplayable executed failure                  = 0
supported old binary crossed compatibility fence into storage-open/replay/write/registration = 0
supported old binary touched Profile authority/Arena before fail-stop = 0
supported old binary stamped over BKPF1 candidate      = 0
Cookie/new-file/registration-only false downgrade gate = 0
partial required-device migration became writable = 0
unknown/corrupt mandatory format became writable = 0
stale readiness/registration generation became writable = 0
persistent readiness missing/mismatch registered ephemeral writable = 0
unsafe old-binary rollback accepted             = 0
old service credential opened/wrote new-scope fallback = 0
missing startup/read-amplification raw metrics   = 0
format/readiness validation executed on normal Add = 0
```

外加B11完整资源账本、foreground p99 regression及B17锁定负载下的debt有界/排空Gate全部达到。不能用单ledger shallow size代替route、pool、selector、grant、pending、buffer、checkpoint和index/cache总成本。

## 12. 立即停止条件

任一 safety violation 立即：

1. 停止扩大 workload；
2. 冻结 device image、seed、logs 和 checker output；
3. 标记 Spike FAIL；
4. 回到 RFC-0003 修正 authority/ordering；
5. 使用新 immutable run 验证修正。

不得把 counterexample 标成 flaky 后删除，不得通过跳过 fault point 或扫描猜测 free list 继续。

stock old binary越过candidate fence触碰Profile storage、进入Journal replay/registration/write、stamp over mandatory sentinel，partial migration变成writable，或旧service credential能打开new-scope fallback，均属于同等级立即停止的safety violation。

## 13. 必交 artifacts

```text
manifest.json
results.json
gate-summary.json
resource-accounting.md
adapter-corpus-equivalence-results.json
buffer-ownership-and-failure-results.json
logical-identity-recovery-results.json
allocation-copy-cpu-and-batch-raw/
tail-classification-results.json
pool-writer-io-quiescence-results.json
bookie-control-multi-arena-crash-results.json
space-exhaustion-and-churn-raw/
normal-and-full-rebuild-raw/
performance-raw/
startup-performance-raw/
device-images-or-snapshots/
control-log-dumps/
checkpoint-dumps/
rebuild-dumps/
fault-injection-log/
failed-seed-reproducers/
stock-old-binary-boot-matrix/
cookie-autostamp-results/
pre-storage-open-file-touch-traces/
new-bookieid-new-scope-access-matrix/
migration-crash-matrix/
registration-readiness-history/
rollback-proof-results/
checksums.txt
README.md
```

敏感或过大的 device image 可用内容寻址存储，但 manifest 必须保留不可变 digest 和取证位置。

## 14. 结果解释

- PASS：未在锁定矩阵中否证allocator/资源模型，只解锁RFC-0003接受评审和isolated/discardable shadow prototype；若same-scope candidate失败但new-scope fallback通过，PASS必须明确记录该限定，不能继续宣称原地格式兼容。
- FAIL：存在 safety、resource 或 locked performance Gate 失败，Segment 保持 P0 Blocked。
- INCONCLUSIVE：证据不全、fault 未命中、环境漂移或结果不可重放。

PASS 不授权 Segment 成为 ACK authority，不授权生产 canary，也不证明 cluster delete。


2026-10-03：[隔离Arena checkpoint实现与回执](../implementation/local-storage/README.md)已覆盖applied cut、A/B image、commit后发布、完整后缀与corruption fallback，附独立固定向量、4个真实JVM halt边界及正常DATA重启测试。全部旧控制日志仍必需且不回收；Bookie checkpoint、rotation/retention、在线维护、MOVE/FREE、index和本Spike完整Gate仍未完成。checkpoint的有限状态投影及指定规则反例见[模型状态](../implementation/models/README.md#arena-checkpoint-增量验证)，不能替代真实掉电或完整allocator证明。

历史（`00edeb8f`版本）2026-10-03 Bookie applied checkpoint已接入，保留sealed credential、权限/条件结果、最新LAC及recovery close；共享codec各自拥有A/B发布页，先durable/apply commit再发布，验证完整旧日志后恢复image与后缀。两个control owner的DATA正常restart、7项新增真实文件回归及有限权限模型有回执：`unified-wal-local-rECuQDLZ`（192项/187通过/5平台跳过）和`unified-wal-model-pz77hf26`（4个正向、6个指定反例）。全部旧控制日志仍必需；rotation/回收、在线维护、派生索引、完整Model A/A+C及生产Gate未完成。

历史（`43bfd80b`版本）2026-10-03 Arena物理rotation已实现：停写后连续建立两个只差内部commit的applied checkpoint，绑定两条原commit identity，先force新日志代并同步目录，再force/验证两份恢复根，最后回收旧prefix。固定sidecar锁覆盖切换和另一owner身份检查；新代持续全局sequence，后续checkpoint保留日志代绑定。普通控制/故障预留保留416字节供rotation，两份image与全部首次条件结果仍受固定状态预算限制。真实重复填满/rotation、单页或image损坏回退、必需suffix损坏拒绝、短写/force失败、跨JVM排他、7个进程退出阶段及双owner DATA图两轮restart有回执`unified-wal-local-3Udoan9d`（195个源码输入、199项/194通过/5平台跳过）；有限模型`unified-wal-model-t1waf2bm`（29个实际输入、5个正向完整PASS、7个指定反例）新增双根/目录同步/旧prefix回收投影。Bookie旧日志仍全部必需；Bookie rotation、在线维护、长期幂等历史压缩、派生索引/MOVE/FREE、完整A/C及组合模型、真实掉电和生产Gate保持未完成。

2026-10-03 Bookie停写rotation已复用双恢复根协议，保留sealed credential、fence/tombstone、条件结果、最新LAC及grant/close；两个owner分别force新日志/目录与独立发布页后才回收旧prefix，当前必要suffix仍完整验证。最新[本地实现及验证](../implementation/local-storage/README.md#bookie停写rotation与共享物理协议)为`unified-wal-local-sKIZwMRg`（197输入、206项/201通过/5平台跳过）和`unified-wal-model-bu9e_n9l`（31输入、6个正向完整PASS/8个指定反例）。权限投影新增回收后terminal恢复性质，独立物理模型检查双根barrier，不构成A+C组合。在线维护、长期幂等历史压缩、派生索引/MOVE/FREE、真实掉电、完整模型及生产Gate仍未完成。

2026-10-03 [真实RocksDB冷重建/覆盖切片](../implementation/local-storage/README.md#rocksdb冷重建与持久覆盖切片)已接入隔离正常restart：单CF、WAL关闭、重建代先退休旧覆盖，实际flush后才发布identity/cut/stream/content绑定的A/B页；全库删除后由同步且route选择的DATA重建，不复活tombstone或未提交copy。Java回执`unified-wal-local-oPEjR8PE`为201输入、215项/210通过/5平台跳过；模型`unified-wal-model-p_83_s8r`为36输入、7个完整正向/9个指定反例。新覆盖投影否证未flush发布，不构成完整模型或掉电证据。在线异步handoff/hot淘汰、coverage跳过扫描、MOVE/delete/stall排序、长期资源与生产Gate仍未完成。


2026-10-03 [异步派生定位追加切片](../implementation/local-storage/README.md#异步派生定位追加切片)已复用owner/DATA票据和既有DATA I/O：每shard一个在途、每批最多256定位，热结果先成立，Put不推进持久覆盖；索引暂停或失败不占控制credit，热容量/后台故障在新DATA预留前背压。Java回执`unified-wal-local-RBhd0IZf`为201输入、218项/213通过/5平台跳过；模型`unified-wal-model-tn4d2l9o`为41输入、8个完整正向/10个指定反例，新增482状态的控制预留安全性投影，不证明死锁自由。hot记录仍保留，fresh fallback/淘汰、在线覆盖、MOVE/delete排序、长期预算、完整模型与生产Gate仍未完成。
