# Spike B：Allocator、Block 与 100k Ledger 资源模型否证规范

> 状态：**Planned / Not Executed**<br>
> 对应 RFC：[RFC-0003](../RFC-0003-segment-storage-allocator.md)<br>
> 性质：否证型；PASS最多解锁不会污染Classic cohort的isolated/discardable shadow prototype，不解锁live shadow

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
memory size
NVMe model, firmware and namespace
filesystem and mount options
I/O API and alignment
JDK and JVM flags
shard count
enabled paths and separately deferred feature/test scopes
transport adapter/refcount ownership and reference-corpus revision
logical identity byte ranges, LAC/digest rules and duplicate slow path
batch byte/count/wait limits and actual durability-barrier definition
immutable CRC32C capability/layout binding and outer/inner coordinate checks
DATA freeze point, stream/batch framing and contiguous-prefix replay/publication
full-write/short-write handling, file barrier coverage and cold directory durability
fixed shard/physical-stream/DATA-file mapping, file count and rotation bounds
physical batch result versus per-entry admission/cancellation/response result
submitted-coordinate retention and stream/file I/O-error isolation/recovery rules
record envelope integrity, bounded aligned point-read ranges and cache/pin lifetime
confirmed/unconfirmed/recovery read boundaries and LAC update/barrier accounting
shared-only first slice and separately deferred dedicated buffer-pool scope
end-to-end request/byte/batch/waiter credit ownership and release points
oldest-request monotonic deadline, maximum entry path and reserved control capacity
delete-applied cursor versus access barrier and physical reclaim predicates
allocation/copy/CPU per-entry instrumentation and thread/queue/lock counters
control-log/checkpoint region sizes
Bookie-level protected control-log record/batch/checkpoint and credential protection
control-tail fault model and required durable-prefix classification oracle
pool owner/shard generation and old writer I/O quiescence mechanism
per-Arena foreground/maintenance reserves and throttle/reject/resume thresholds
bounded live-set/churn rate, maintenance scheduling share and debt-drain deadline
normal/full-index-rebuild coverage, scan/I/O/memory/readiness limits
conditional API revision
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
| 初始写入切片 | 隔离新storage scope、一个Arena、Bookie/Arena必要控制记录、durable预分配、ByteBuf受控view、固定shard共享DATA block/文件；逐entry结果、独立record点读与基础restart；跑B18/B19适用项 | 无dedicated收益、生产listener/ACK、完整恢复、完整Spike或集群性能结论 |
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
- per-Arena conditional apply/result、operation identity、assigned control sequence 与 group `durableThrough`；
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
checkpoint current selector + anti-ABA state equals full-chain oracle
condition failure changed no authority state
durable apply result covered its own control sequence
duplicate operation produced no second winner/generation bump
uncommitted relocation copy never becomes authoritative
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

同一batch封入L1/100与L2/200，submission后fence L1并明确终止其Add；完整write/barrier后batch进入前缀、L2可成功，L1不成功且不制造物理gap。注入单条cancel/disconnect/callback失败、封包前全部取消和分配sequence后submission失败：不修改submitted record/buffer，空assembling不占序号，已分配的失败边界不得任意跳过。未回成功的有效DATA保留作恢复候选，tombstone另按删除合同处理。

### B3：ALLOC pool refill

反复refill、部分使用、shard crash、Bookie crash、未使用pool回收；延迟真实write submission/completion，覆盖旧shard退出、pool转交及generation bump后才到达的completion。

Oracle：pool/shard generation ownership不重叠；restart后unused/live/unknown分类可独立重放。旧写I/O未终结或可靠隔离前不能转交/reuse；仅取消future、忽略late callback或reader drain的方案必须被否证，旧I/O不能改写新owner数据。

### B4：Shared slab 多 ledger

多个 ledger 交错写同一 block，分别删除一个、部分、全部 ledger。

同一block混合L1/L2，按同一delete stream连续发送delete L1、delete L2。L1 tombstone durable但L2仍live导致block不可free时，L1 applied cursor先推进，L2仍可应用；加入checkpoint、清理派生队列丢失与restart/rejoin。不能以等待L1物理释放作为读取下一delete的前置。

Oracle：单 ledger logical delete 不影响其他 record；block 全死前不进入 free list；全死后按 durable generation bump 回收。

### B5：Hot promotion（dedicated启用时）

首个shared切片标NOT_APPLICABLE/DEFERRED，不纳入其性能结论。后续启用时在阈值边界晋升dedicated extent，crash于promotion决策、allocation、首条dedicated write各点；专属范围不混写其他ledger，共享barrier不要求共享block，不能产生每热ledger永久buffer/文件/线程。

Oracle：shared 旧数据和 dedicated 新数据可组成唯一 ledger history；不要求迁移，不重复/丢失 local-success entry。

### B6：Delete、reader drain 与 reuse

保持reader/pin和未完成writer I/O，触发delete；在admission close、durable terminal tombstone/可重建义务、applied cursor/result、access-barrier receipt、invalidate、drain、FREE_AND_BUMP、physical result及新owner allocation各点crash。覆盖effect-before-cursor、cursor-before-free和tombstone+cursor group commit。

Oracle：cursor只依赖durable delete-applied effect，不等待drain/free；缺effect不能推进，队列丢失可重建义务。reader/pin未drain或旧writer I/O未终结时slot不复用；tombstone后不产生新local success，屏障完成后不再服务；旧locator在新generation失败。applied、barrier、physical result分别验证，restart catch-up不等待compaction。

### B7：Checkpoint A/B rotation

在 control cut `S`、bounded checkpoint chunks、fsync、`CHECKPOINT_COMMIT(generation,S,identity)`、inactive superblock update/fsync、active generation switch、fallback-dependency verification和 old-log reclaim各点 crash。把 `MOVE_COMMIT` 放在 `S-1/S/S+1`，并覆盖 current selector已保存但old source尚未free、old free后historic chain被压缩、A/B fallback仍依赖旧suffix等状态。

Oracle：restart选择一个完整authority，`checkpoint through S + complete suffix >S`与full-chain replay产生相同allocation/current selector/retiring/anti-ABA状态；不得选择损坏的较新checkpoint或回收任一fallback仍依赖的唯一suffix。

### B8：Control authority corruption

分别损坏单个superblock、单个checkpoint、可证明未提交的末尾、必需committed prefix、unknown mandatory完整记录、边界无法分类的tail，以及A/B全部authority；同时覆盖durable但response丢失的完整batch。

Oracle：按RFC-0003 §5.4独立分类；只截断可证明未提交的末尾，完整durable transition即使未回ACK也重放；必需prefix损坏/unknown/无法分类保持non-writable。判定不能仅依赖文件末尾、checksum失败或客户端未记录ACK。

DATA部分另按§6.1验证物理前缀：从verified checkpoint/cut恢复，batch缺口后不得丢弃已允许成功的数据；required corruption保持fail closed，control已证明的free/retired区间不是未知gap。control durable-through、DATA physical durable-through与ledger LAC各自比较，不混用序号。

注入direct I/O错误且范围已部分修改、fdatasync错误后再次sync返回成功、11 unknown/12完整，以及同文件多个stream。Oracle：至少暂停受影响stream，无法缩小错误范围则暂停共享文件相关stream；必要时升级既有设备/Bookie non-writable。后续DATA不跨缺口成功、不因RPC超时丢弃、不任意跳号/新建stream或覆写绕过；重复sync成功不清除未解析失败。实际I/O终结后才释放无使用者buffer，未知坐标由pending或不可写范围接管，恢复前不接受冲突写；异常对象保持上限。

### B9：Derived index deletion

分别运行保留有效index/checkpoint的正常restart，以及完整删除RocksDB/locator index的full rebuild；首批数据包含active/sealed shared block与relocated record，dedicated启用后必须增加sealed dedicated extent，不能借首批延期漏扫已经启用的布局。

Oracle：full rebuild枚举全部需要重建的live allocation/有效DATA范围，结果与独立全量authority oracle一致；normal restart只能在有覆盖证明时缩小扫描范围。stale generation不进入index，未验证范围不返回确定absence；分别记录scan bytes/I/O、peak memory、read-only/可写时间及前台竞争。

包含乱序完成batch、已回收区间和搬迁目标；独立checker验证§6.1成功前缀/可发现性。physical durable-through丢失后从authority/framing重建，不假定内存table仍在；MOVE_COMMIT选择的新DATA不能落在恢复会截断的后缀。

补充大batch内小entry点读，分别命中/未命中cache；只读目标record所需对齐范围，独立验证envelope/坐标/generation及BK CRC，不强制整批读取。注入locator与header坏length/溢出/越界、外层身份损坏但BK CRC正确、pin取得时selector切换、慢reader/断连；范围与buffer有界，未恢复覆盖不返回确定absence。记录真实I/O及pin，不以模型计数冒充磁盘测量。

### B10：Compaction copy

对部分死亡shared block执行same-Arena compact，在新allocation、copy、DATA durability、conditional `MOVE_COMMIT` append/durability/response loss、原子selector发布并关闭old-pin admission、reader/writer quiescence、old free各点crash；不能把selector发布与new-old-pin阻断实现为两个独立cut。

必须覆盖：

- 同一 predecessor 的两个 concurrent moves；
- move chain `A -> B -> C`；
- batch/group-commit torn tail；
- new payload digest mismatch；
- 每个边界删除 derived index 后重建；
- 只迁移部分 records 时尝试 whole-block free；
- old reader/pin 跨越 cutover；
- checkpoint 覆盖/未覆盖 `MOVE_COMMIT` 时 rotation crash。
- checkpoint后stale old operation retry；
- logical entry已有local-success，但uncommitted new location从未成为success/lookup location；
- orphan GC与late commit在同/不同group竞争，分别覆盖free先赢与commit先赢；
- shared allocation一条orphan、一条live，以及writer-generation/pin quiescence race。
- 同一operation在enqueue前、append后force前、force后response loss重试；
- group内独立condition一成一败、`durableThrough < ownSequence`、middle gap/torn/unknown mandatory record；
- idempotency summary compact后的extremely-late retry，以及queue/waiter/future cap压力。

Oracle：无 commit 时 old authoritative、new copy 只是 orphan；durable commit 后 new authoritative 且 index 可重建；同一 predecessor 只有一个 winning successor；每个 live record 至少一个 authoritative lookup locator（允许cut前old reader pin）且不能有两个new lookup winners；old block只有在全部live records moved/dead、new pin被阻断、既有reader drain和durable free后回收。relocation不新增local-success fact；清理orphan new location不删除logical entry在current location承载的既存success。pending/admitted append不授予authority，condition failure无副作用；duplicate只返回same durable result或stale/conflict。

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

Oracle：Bookie级权限在所有Arena一致；部分成功不能扩张接受集合；required store不完整时不注册writable。normal Add无控制日志fsync或远程read，DATA不重复写入Bookie控制日志。报告真实fsync/bytes、cold/warm恢复时间及保护credential的非泄漏检查。

### B17：空间耗尽与长期回收进展

在manifest锁定的有界live set与admitted写入速率下持续create/write/delete，使大量shared block仅剩少量live records；另运行超过可持续能力的压力矩阵。分别耗尽前台whole-free blocks、compaction目标预算、Arena及Bookie控制日志/checkpoint预算；在maintenance与full-disk状态重启。

Oracle：前台在侵占维护保留量之前限流/拒绝，queue/memory保持有界；维护得到锁定最低调度份额，恢复空间后按hysteresis重新开放。受支持负载下debt/dead bytes不持续增长，停止新写后在锁定deadline内回到目标水位；不能靠暂停compaction通过p99。超额负载只要求有界拒绝及可验证恢复，不要求无限容量。全部数值先于正式run冻结。

### B18：ByteBuf、固定shard与批量DATA路径

按RFC-0001 §11.6及RFC-0005 §10.1实现transport adapter到append shard的路径。reference immutable byte-array codec保留作oracle，以相同valid/invalid/truncated/oversize corpus验证解析和拒绝等价。跟踪从完整输入到readable publication的每次payload复制和所有权转移；允许一次集中对齐复制，禁止未计量的逐层整包clone。异步retain/release覆盖normal、拒绝、取消、断连、retry和I/O失败；源view最后使用后释放，已提交I/O的buffer直到真实I/O终结才可复用。

多个ledger合批，固定shard，bytes/count/wait均有上限；注入低负载等待、队列满、pool refill、slow/failed force、fence/delete race及compaction并发。验证不持ledger锁等待I/O，普通已准入写不串行等待三层control/DATA队列或三个future。真实barrier前无local success；不能将取消future当成I/O终结。计量实际entries/barrier、force次数、thread hops、queue wait、lock hold和各层控制写；每entry force不能仅因API名为group commit就算批量化。

扩充现有B18负载：持续小请求不重置最老deadline；低负载到期padding；超过普通block但在声明entry上限内的请求走预算内大记录批次，否则明确拒绝；热ledger与低速ledger混合、重复核对慢读、locator/响应阻塞、断连/取消但I/O未终结、DATA满额时fence/tombstone到达。全阶段request/源与对齐bytes/inflight batches/waiters有界，出队不提前还credit，慢读不阻塞append主循环，控制保留容量可关闭准入。记录低负载、目标负载和过载结果，不只测queue.size()。

DATA freeze和barrier/prefix沿B2 oracle，量化padding bytes、write/durability/prefix/locator wait及阶段资源峰值。相同durability与TLS范围才比较；局部无网络run不计端到端增益。

同一切片消费B2逐entry/物理结果分离及B8错误隔离；记录每个实际DATA文件的barrier次数/等待/覆盖和受影响stream，不将per-shard table当独立flush域。点读消费B9路径，孤立请求不等待凑批，相邻请求有界合并；小entry受控复制释放大父buffer/pin，读盘、cache、response和回收并行都在总预算内。

### B19：同坐标identity与基础恢复重写

用现有DigestManager与LedgerRecoveryOp语义构造同instance/entryId/应用bytes/累计length、不同合法piggyback LAC及digest的normal/recovery重写，应幂等且不覆盖不同数据；另测不同应用payload、累计length冲突、坏digest、跨instance、并发pending、乱序/hole、`E>W`、restart及derived-index丢失。每份输入完整性与authority都验证，不以整包bytes或全BK digest判定逻辑冲突。

首批向量固定32-byte BK metadata + 4-byte CRC32C + opaque payload，验证CRC覆盖、长度/截断、outer/inner ledger/entry mismatch、未知或非CRC32C安装拒绝及不改变60-byte context。构造不同bytes但相同CRC32C的碰撞向量，证明checksum命中不能跳过真实bytes核对；相同业务bytes而LAC/digest不同仍幂等。不得从20-byte ledger credential推导HMAC能力、下发password/MAC key或为每entry新增SHA-256。布局向量属于后续新run，历史frame/operation corpus保持原证据身份。

K/X提交后timeout，再到K/Y：原坐标不变为空，Y不能成为新winner；分别令X迟到完成、部分写入、barrier unknown、restart重建，相同X重试只能复用已解析且权限有效的结果或暂不可用。未提交请求可原子撤销但不得与封包竞争后误释放；fenced原调用失败不抹除有效恢复候选。另按A16/RFC-0004 §7.5运行LAC=99/entry 100点读，confirmed/unconfirmed/恢复各守边界，DATA完成不伪造quorum LAC，explicit LAC合批不新增每Add控制fsync。

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
cross-Arena partial state expanded Bookie authority = 0
foreground allocation consumed maintenance reserve = 0
checkpoint replay authority divergence         = 0
authority-loss device resumed writable         = 0
derived-index rebuild changed payload facts    = 0
uncommitted move copy became authoritative     = 0
committed move lost after index deletion       = 0
multiple winning successors per predecessor    = 0
source freed before move commit/reader drain    = 0
move created new local-success fact             = 0
orphan GC removed current local-success payload = 0
checkpoint selector differed from full chain    = 0
late commit and orphan free both succeeded      = 0
conditional failure mutated authority state     = 0
durable result before own sequence durability   = 0
duplicate operation created second winner/bump  = 0
unknown mandatory record skipped writable       = 0
selector cut allowed new old-location pin        = 0
queue/waiter/idempotency hard-cap violations     = 0
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
