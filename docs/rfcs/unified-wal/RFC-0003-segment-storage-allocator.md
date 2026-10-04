# RFC-0003：Segment Storage、ArenaControlLog 与冷热混合 Allocator

> 状态：**Proposed / P0 Blocked**<br>
> 依赖：[RFC-0001](RFC-0001-profile-capability-install.md)；ACK authority 另依赖 [RFC-0005](RFC-0005-segment-bookie-state.md)<br>
> 验证：必须通过 [Spike B](spikes/SPIKE-B-allocator-block.md) 与 [Spike C](spikes/SPIKE-C-no-object-tla.md)<br>
> 解锁对象：Segment shadow writer；不直接解锁 Segment ACK authority

## 1. 摘要

本 RFC 为 Segment WAL 定义本地 authority 分层和 allocator crash-consistency 骨架。核心修正有两项：

1. 增加独立、不可随 data extent 回收的 `ArenaControlLog`，作为空间 ownership 与 generation 的权威；
2. 删除“每个ledger创建时至少拥有一个8 MiB dedicated extent”的不变量；首批切片统一shared block，dedicated hot extent保留为有条件启用的后续优化。

本文不冻结最终 on-disk bytes、extent 阈值或 direct-I/O 实现。任何正式编码都必须晚于 Spike B 的否证结果和本 RFC Accepted。

## 2. 范围

本 RFC 负责：

- NVMe/WalArena 的物理 authority 区域；
- `ArenaControlLog`、allocator checkpoint 与 superblock 切换；
- allocation、data ACK、delete、free、generation bump 与 reuse 顺序；
- 首批shared block，以及后续有条件启用的dedicated hot extent；
- block/record identity 的最小语义；
- restart、power-loss、corruption 与 device-failure 行为；
- 100k idle ledger 的空间和内存模型；
- derived index 与 payload authority 的边界；
- reclaim/compaction 的分级承诺。

本 RFC 不负责：

- Profile 创建和 ensemble install，见 [RFC-0001](RFC-0001-profile-capability-install.md)；
- 集群删除授权和离线 Bookie，见 [RFC-0004](RFC-0004-range-recovery-delete.md)；
- Segment Bookie 的 activation、fence、explicit LAC、recovery Add 与 ACK authority，见 [RFC-0005](RFC-0005-segment-bookie-state.md)；
- BookKeeper ensemble change 或 AutoRecovery 的总体协议。

## 3. Authority 分层

```text
Cluster MetadataStore
    ledger/ensemble/delete authority

BookKeeper quorum
    distributed ACK and recovery authority

Bookie-level protected control log
    route / activation / fence / grant / terminal tombstone authority (RFC-0005)

Per-device ArenaControlLog + AllocatorCheckpoint
    local allocation / generation / move / free authority

Data Arena
    local payload authority

Footer / RocksDB / cache
    rebuildable derived index
```

RocksDB、footer 或 data scan 不能自行宣布空间空闲。allocator authority 只能来自最新有效 checkpoint 与其后的 control log。

实现进展（2026-09-28）：[隔离本地存储](implementation/local-storage/README.md)已交付步骤1配置、步骤3实验格式/独立parser/固定容量文件与真实FileChannel同步、步骤5的Arena ALLOC/ALLOC_POOL持久后按序条件apply、stream首序号及DATA整段grant检查、步骤6的真实DATA write/force接原owner volatile连续物理前缀/有界物理热候选驱动、受控normal逐entry结果、在途同坐标重试等待及运行期已发布候选的owner/I/O重复点读慢路径、步骤7的对齐单record点读/停写后文件同步、重启物理清单、单stream序号候选检查及新owner有界物理热目录重建局部切片。[实验格式v1](implementation/local-storage/format-v1.md)记录DATA来源、控制framing、generation及校验边界；各切片的定向范围与结果见本地存储状态与回执。Bookie受控控制日志及normal批次逐entry准入复核已有局部实现；完整请求级timeout/取消及幂等、完整allocator恢复、生产逐entry成功及restart仍未实现，本RFC的Gate未提升。

## 4. 物理布局

候选布局：

```text
NVMe device or WalArena
├── Superblock A
├── Superblock B
├── ArenaControlLog region
├── AllocatorCheckpoint A
├── AllocatorCheckpoint B
└── Data Arena
    ├── Shared Slab regions
    └── Dedicated Extents (deferred for the first shared-block slice)
```

约束：

- `ArenaControlLog` 不属于任何 ledger；
- control log 和 allocator checkpoint 不在可由 whole-ledger delete 回收的 data extent 中；
- data allocator 不能覆盖 active control-log/checkpoint generation；
- control log 通过自己的 checkpoint/rotation 协议回收；
- superblock A/B 必须能检测 torn write、checksum failure 和 generation 回退。
- 每个 superblock 必须绑定Bookie storage incarnation、Arena/device identity、Arena format/mandatory feature set、control/checkpoint generation和device-manifest generation；任一required device missing/mismatch/partial migration使整个Segment Bookie non-writable；
- Arena superblock保护新实现之间的format/recovery，不能单独阻止stock old binary启动。RFC-0005的Bookie/storage compatibility fence必须在任何Journal replay/Arena writer/registration之前由旧binary mandatory path fail-stop；仅创建新文件或unknown control record不是downgrade fence。
- Round 7未证明同BookieId/同storage scope的stock-old-binary fence，因此same-scope format保持BLOCK；如果Spike B不能证明mandatory pre-open gate，当前合同必须采用RFC-0005锁定的new BookieId + new journal/ledger/index/Arena roots + new storage incarnation +独立OS/service credential scope，allocator不得继续假设原地升级。

区域大小、对齐、冗余、多设备布局、superblock bytes与mandatory feature encoding是 Spike 后冻结的参数，不在本骨架中写成生产默认。

## 5. ArenaControlLog

首批Arena权威record候选类型（物理编码仍未冻结）：

```text
ALLOC
ALLOC_POOL
MOVE_COMMIT
FREE_AND_BUMP
CHECKPOINT_BEGIN
CHECKPOINT_COMMIT
DEVICE_STATE
```

route/profile binding与terminal tombstone分别属于RFC-0005的Bookie级控制日志；`LEDGER_PROFILE_BIND`、`DELETE_TOMBSTONE`不属于本Arena的独立权威记录。Arena transition可以引用已验证的Bookie控制事实，但不产生另一份ledger生命周期，不要求Bookie tombstone durable后再逐Arena写tombstone/fsync。Bookie持有route/activation/fence/grant/tombstone，Arena持有allocation/generation/move/free。

记录的候选envelope与按类型使用的身份字段：

```text
controlSequence
recordType
slotOrExtentId
oldGeneration
newGeneration
allocation/shard owner identity where required by record type
ledger instance reference only where applicable
semantic predecessor / expected generation
operation identity and generation where externally retried
payloadDigest
checksum
```

具体字段按record type裁剪，shared allocation不以单个ledger作为完整owner。控制序号和checksum必须使replay能识别重复、缺口和torn tail；外部Bookie控制引用不成为本Arena的第二份生命周期authority。

### 5.1 Authority 规则

```text
allocator state = latest valid AllocatorCheckpoint
                + valid ArenaControlLog suffix after checkpoint
```

data extent header 可以用于交叉校验和诊断，但不能在 control authority 丢失时通过猜测空闲空间恢复 writable 服务。

如果 checkpoint A/B 和必要 control-log suffix 均无法验证，该设备进入 FAILED/QUARANTINED；不得扫描 data arena 后继续分配。

### 5.2 Relocation selection authority

当前 compaction relocation 只支持 old/new allocation 同属一个可线性化 `ArenaControlLog` authority domain。跨 Arena/device relocation 涉及两个独立 authority，当前明确 unsupported；如要支持，必须直接修改本RFC并重新评审，不能把单边 `MOVE_COMMIT` 扩展成隐式分布式事务。

下文“durable/committed ALLOC、MOVE_COMMIT、FREE”均指§5.3中命令已持久且条件应用成功的transition/result；仅把条件命令写入durable日志不算分配、搬迁或释放已生效。failed/no-op命令没有这些authority。

`MOVE_COMMIT` 是 derived locator 从 old location 切到 new location 的唯一 durable authority。它是条件化 transition，语义至少绑定：

```text
ledgerInstanceId
logicalEntryOrBoundedRangeIdentity
expectedOldLocationAndGeneration
newLocationAndGeneration
payloadDigest
moveOperationIdentityAndGeneration
controlSequence
durabilityBarrierOrDurableThroughCut
```

同一 expected predecessor 只能有一个 winning successor。无 durable `MOVE_COMMIT` 时 old location 仍 authoritative；new copy 即使 payload durable 或 derived locator 已更新，也只是可证明后才能清理的 orphan。durable commit 后新 lookup 必须走 new location；new payload digest/identity 无法验证时 fail closed，old copy 不得自行夺回 authority。

`MOVE_PREPARE` 可以作为 orphan discovery 或 QoS 优化，但不是 safety 必需。exact record bytes、checksum、per-entry/range packing 与 batch 大小保持开放。

group commit中，必须有明确覆盖该`controlSequence`的durability completion，并已按§5.3成功apply，才允许locator cutover；batch submission、内存append或其他record完成都不够。locator切换与阻断新的old-location pin形成一个本地同步cut：cut前取得old pin的reader可完成，cut后新reader只取得new locator。`durableThrough`不得跨control sequence gap或torn record。

### 5.3 Conditional apply 与 durability result

`ArenaControlLog`首批选择一种执行模式：**条件命令合批持久化，再由per-Arena sequencer按控制序号顺序应用**。不同时维护另一套可回滚的推测状态，也不逐操作fsync。现有Classic Journal group force仅作batching参考，其callback不直接代表semantic transition已成功。

实现状态：隔离文件原型已接通ALLOC/ALLOC_POOL的单batch持久化、按序条件apply、身份校验及有界全量重放；单在途Arena控制驱动与定向真实文件测试已由独立控制completion及I/O服务份额在DATA准入和I/O队列满时完成控制日志force、原owner顺序apply，显式`writeAuthorized`路径核对整段DATA batch grant。Arena分配命令现持久绑定每个shard/generation的首个物理sequence，冲突的后续分配按条件失败；物理前缀提交及重启候选扫描都核对该首序号。重放状态还可给出每文件成功授权block的最高结束位置，零填充block仍计入，条件失败不推进；只读扫描可在停写后验证完整framing、grant及ledger的固定shard，并把此水位随同步的物理候选报告。调用方选定的多个stream候选可在整组核对目录、持久文件配置、storage identity、容量和重复坐标后原子装入新owner的有界热目录；任一候选失败不留下部分装入；它不把零页宣布为unused，也不解除旧I/O错误或证明末端durable-through。Bookie控制闭环、实际服务生命周期接线、pool used/unused恢复、FREE/MOVE与生产逐entry成功均未完成，不能作为本节完整allocator authority。

隔离Arena控制日志的文件write/force已移出其grant状态锁；受控慢force期间既有grant查询不会等待该I/O，新的条件batch仍被单在途门槛拒绝。DATA文件的范围预留也与同文件串行write/force分锁，慢force不再占预留状态锁；但尚未接入完整普通Add请求生命周期，不据此声称完整append链路无阻塞。

```text
validate format / authority / bounded enqueue eligibility
    -> establish applicable protected DATA/reclaim checks and runtime quiescence gates
    -> assign control sequence and append the conditional command
    -> batch complete writes and the covering control-log durability barrier
    -> advance the complete durable command prefix
    -> in sequence order, evaluate predicate against current applied state
    -> apply this command's state change or deterministic no-op failure
    -> advance the contiguous applied prefix and return this command's result
```

入队/append阶段不授予allocation、selector或free authority；可过滤明显无效请求，但不能用入队前检查替代最终predicate。前一命令的applied结果对后一命令可见：同一group中的T1/T2竞争同一个free slot时，T1分配给shard A，随后T2看到已占用而`CONDITION_FAILED`，只需共享一次barrier。同批条件失败不回滚其他成功命令，也不形成跨operation事务；失败命令仍消耗sequence并推进已应用前缀，但不修改业务owner/selector/generation或返回成功。

概念结果至少等价于：

```text
appendConditional(transition)
    -> APPLIED_DURABLE | ALREADY_DURABLE | CONDITION_FAILED
     | STALE | INCOMPATIBLE/CORRUPT
    + operation identity
    + assigned control sequence/range
    + resulting generation/state identity
    + durableThrough
    + appliedThrough
```

predicate与状态更新在sequencer上对当前applied state原子执行，且命令自身sequence已位于完整durable prefix内。authority consumer只消费已durable且已apply的成功结果；`durableThrough`可领先于`appliedThrough`，仅fsync完成不授予cutover/free/reuse。一个bounded命令可由有限物理record承载，但完整命令边界必须可重放；apply中途crash从checkpoint的applied cut与完整后缀重演，不把fsync最大序号冒充已应用状态。

正常执行中的DATA checksum/身份、整单元回收条件及I/O/pin排空，在命令提交前通过受保护验证与既有gate建立，所需条件身份、验证范围和必要authority引用绑定到现有命令或checkpoint。FREE关闭相关新pin/I/O准入并完成quiescence，保护保持到transition结果解析，timeout/unknown不释放；MOVE同样保护已验证的目标generation与副本身份。入队前的这些验证不替代日志durable后对expected generation/predecessor及当前applied控制状态的最终原子判断。

可重放的predicate只依赖checkpoint、前序控制结果、expected generation/predecessor及命令绑定的必要事实，不依赖已经允许回收的历史DATA字节。不得读取当前磁盘内容、墙钟、网络或任意新metadata版本来改判历史命令；也不能以重启pin=0补认旧FREE。所需控制事实必须随checkpoint/完整后缀保留，缺失/冲突仍按authority恢复失败处理。§12先重演完整控制关系，随后验证最终必要DATA；完整条件命令仍可能失败，不能把跳过历史payload读取改成无条件应用所有MOVE。

externally retried transition绑定 Arena、operation identity/generation、expected predecessor/location/generation和payload identity。idempotency retention必须有界：current selector/free/checkpoint state能证明已提交时返回 `ALREADY_DURABLE`，已被后续 generation取代时返回 stale/conflict；不永久保存所有 request id或per-record future。runtime的unknown mandatory record、sequence gap或torn record阻断`durableThrough`并使Arena fail closed；restart按§5.4分类。

`MOVE_COMMIT`、conditional `FREE_AND_BUMP`、`ALLOC/ALLOC_POOL` 共用上述 per-Arena order、complete-prefix replay和group durability。Checkpoint data、inactive superblock publication与prefix reclaim仍按第11节分阶段执行；`CHECKPOINT_COMMIT`可以共用append/durability原语，但 `S` 必须是complete committed/applied cut，不能从任意fsync callback推断，也不需要三介质通用事务。

reader cutover在 durable move result 后，通过同一个 local selector/pin gate 原子完成“发布 new selector + 禁止新的 old-location pin + 建立 cutover epoch”。`acquireReadPin` 必须在 bounded retry中验证 selector/epoch，cut后不能从 stale cached locator取得old pin；pre-cut volatile readers可以完成并drain。具体使用stripe lock、seqlock/epoch或RCU保持开放，individual reader/future/buffer history不得持久化。

### 5.4 控制日志尾部分类

runtime不能越过torn/gap记录推进`durableThrough`。restart则必须先按以下规则分类，不能把所有checksum失败都截断，也不能把正常未提交尾部一概当作必需authority损坏：

| 分类 | 恢复动作 |
| --- | --- |
| 可证明位于任何必需durable prefix之后的不完整物理末尾 | 按已冻结framing丢弃未完成transition尾部，截断到最后完整可验证边界；同次group fsync不使独立transition自动变成可整体丢弃的事务 |
| 必需committed prefix内的缺口、损坏、缺失record，或完整记录携带unknown mandatory语义 | quarantine/non-writable，不跳过、不猜测 |
| 无法证明是哪一类 | 保持non-writable，保存原始bytes和诊断结果，进入显式修复流程 |

分类依据必须来自manifest锁定的故障模型、record/block framing、完整transition/batch边界及可独立验证的durable-prefix/checkpoint依赖。仅“位于文件末尾”“未收到ACK”“checksum失败”均不足以证明可截断；完整durable transition即使response丢失也要重放。只在排除必需authority损坏后恢复到可写，且不得跨过未知mandatory记录。

Spike B必须给出可执行replay oracle和原始故障镜像，证明截断不丢任何成功所依赖的authority，也不把可安全恢复的未提交尾部永久隔离。具体framing/持久边界方案未通过前，本节是待验证要求，不宣称已解决介质损坏判别。

## 6. 分配与 ACK 顺序

普通Add与合法recovery Add的最低顺序：

```text
1. append ALLOC or ALLOC_POOL with generation and owner
2. make the conditional allocation command durable and apply it successfully
3. expose space to WalAppendShard
4. assemble and freeze a DATA batch; write its authorized, previously unused ranges
5. complete all batch writes, then the durability barrier covering those exact writes
6. advance the stream's contiguous physical durable-through, without crossing a gap
7. for ENTRY_DATA, publish the correct readable locator under the selector/admission gate
8. allow local Bookie success to participate in quorum ACK
```

核心不变量：

```text
local durable Add success
    => durable allocation authority existed before DATA use
    && DATA durability barrier completed
    && batch belongs to the recoverable contiguous physical durable prefix
    && same-coordinate identity and readable location satisfy RFC-0005
```

为避免每个 Add 增加 control-log fsync，allocator 应提前批量分配：

- shard-owned free block pool；
- shared slab block pool；
- dedicated extent pool（仅在§8能力启用后）。

pool refill 通过 control-log group commit；pool 内空间的使用仍必须有可恢复的 block/record framing，但不要求每条 Add 写一条 allocator fsync。

第一批数据路径采用RFC-0005 §10.1—10.5的固定append shard与常驻owner；CPU placement和等待策略独立，内存池按本RFC §6.2初始化。普通写入：多个BK entry合并进有界、对齐buffer，使用预先durable分配的pool空间，以真实batch durability完成后逐entry发布可读定位和local success。上述1/2是预分配先决条件，不意味着每条Add同步经过Bookie控制、Arena控制和DATA三条执行队列。refill异步预备并在资源不足时背压；锁内只做短状态变化，不等待磁盘完成。集中对齐复制可以接受，但要计量各层payload复制、分配、线程hop以及每次force覆盖的entry数，不能把每entry force称为已实现group commit。

`ALLOC_POOL`的下一原型必须冻结：`Arena + pool range + owner shard + shard generation + allocation generation`，每个pool内record的used/unused识别方法，以及返还/转交的条件化状态机。未使用不能由“内存计数为0”推断；restart必须结合完整control authority与可恢复DATA framing确定live、unused或unknown，unknown不进入free pool。

该识别须覆盖复用残留：slot从generation 7经durable `FREE_AND_BUMP`重新分配为8，新DATA未写入或未完整写入即crash时，盘上可能仍是generation 7的合法header/CRC。旧字节不得进入当前index、恢复旧owner或充当generation 8的DATA；旧generation本身也不证明新generation已发生媒体损坏。必须结合完整分配顺序、当前stream/batch及selector/恢复依赖和已锁定故障模型，证明当前范围属于unused/可安全回收，或保持unknown/non-writable。不能仅凭旧CRC正确就认领数据，或仅凭generation不匹配就猜free。首批不在每次复用时主动清零整个allocation；新DATA须携带完整当前身份/校验，清零不能替代恢复判定。

pool转交及所有free/reuse先关闭旧writer admission，等待已提交写I/O完成或获得可靠的设备/进程隔离证明，再conditional free/bump并授予新owner。旧completion的generation检查只能防止错误发布，不能阻止已经提交的旧I/O覆盖新owner磁盘字节；timeout、取消future或reader drain都不能单独作为写I/O终结证明。buffer、submission及completion必须携带owner/generation，stale owner/generation的completion不得发布locator或success；仅RPC取消而storage generation仍有效的物理结果按§6.1保留。崩溃后如何终结旧提交者的I/O同样进入真实故障矩阵。

### 6.1 DATA批次的durability与恢复前缀

首个真实文件原型采用“本批完整write completion → 覆盖本批的文件级durability barrier”这一种模式。逐个检查写结果与短写；只有所有范围完整写入后才发barrier，未完成短写、I/O错误、底层I/O取消后结果不确定或durability unknown均不能证明该批durable。单条RPC取消与这个物理判断分开。补写剩余范围仍须符合声明的alignment和错误处理规则，不能重写含既有成功数据的范围。Linux原型可将该模式映射为完整写入后`fdatasync()`，映射及文件系统/设备假设在同一实验manifest冻结；其他等价同步写模式后续单独验证，不叠加重复barrier却漏算成本。

`write completed`、`durability completed`、`readable published`和`local success`是不同事实。`O_DIRECT`不单独提供`O_SYNC/O_DSYNC`的持久化保证；普通write完成（包括普通异步write CQE）不能直接授权成功。文件barrier也不自动保证新目录项durable，文件创建/预分配及所需parent-directory同步在冷路径完成后才公开可用空间。依据[Linux open(2)](https://man7.org/linux/man-pages/man2/open.2.html)和[fsync(2)](https://man7.org/linux/man-pages/man2/fsync.2.html)，实际硬件/I/O路径仍需故障实验。

每份write/barrier completion绑定storage/stream generation、batch sequence及其实际物理范围，通过已保留容量回原shard owner处理，不由I/O线程并发修改pending或前缀。阻塞write/barrier在有界I/O服务资源执行，不阻塞Netty/append owner。一个barrier可覆盖已完整write且明确列入覆盖集合的多个batch；不能因为更晚的fsync返回就把仍在飞行的异步write算作durable。批次跟踪有hard bound，不记录逐entry“ACK已完成”控制日志。

普通/恢复entry与搬迁副本共用有界buffer、write、barrier及物理前缀跟踪，但上层completion按§9的内部来源区分。`ENTRY_DATA`消费§6/§15的权限、定位及local-success路径；`RELOCATION_COPY`只返回目标副本已durable的内部结果，随后走§10的条件MOVE与selector切换，不能直接进入普通hot-locator发布callback。来源不改变物理batch是否完成，也不增加一套I/O引擎。

DATA batch只共享物理写入、barrier和buffer生命周期，不是跨ledger/entry事务。batch table分别记录完整写入、实际barrier覆盖与是否进入可恢复前缀；entry的admission、fence/tombstone、取消和callback状态由RFC-0005的pending处理，不能用一个`batch.success`表达二者。物理完成后，同批L1因fence失去成功资格，不能阻止仍合法的L2或把物理batch变回缺口；L1也不能因同批durable跳过权限检查。

物理batch sequence在非空容器封包、冻结并进入提交流程时分配；纯assembling且最终为空的容器不留下恢复必须解释的序号。序号分配后如果submission失败或结果不明，按真实失败边界解析，不能当空容器跳过。submitted后不因单条取消移除record/改写block或提前释放buffer，整个物理结果仍跟踪到终态。有效DATA未向原调用方返回成功也不能自动删除；fence候选与tombstoned数据分别按恢复/删除规则处理，保留同一locator/index的物理位置事实。

首批选择**每个物理append stream的连续前缀恢复和成功发布**。bounded batch table记录乱序completion；只有从已验证起点开始连续的batch均取得durability，才推进内存中的physical durable-through并允许对应entry发布locator/local success。batch 11未durable而12先完成时，12保持等待，不能先ACK再在restart的11缺口截断后缀。该序号属于物理stream，不是ledger entryId或LAC，也不是Arena控制日志sequence；不同ledger可同批，`E>W`不要求本地entryId连续。

实现状态：隔离物理驱动已将DATA坐标预留、真实write/force、owner连续前缀和有界热候选接成调用路径；热目录在DATA预留前占定位容量，只有连续结果才发布普通`ENTRY_DATA`候选，搬迁副本不进入普通候选。11/12乱序、首批force失败使两个未决回调报错且不发布候选、容量满额，以及同批两条entry共享一次force并各得可点读物理候选的局部真实文件测试已覆盖。DATA文件关闭已拒绝未终结的预留；I/O任务在执行前被拒绝时终结预留计数但不回退坐标，并保持物理前缀失败。首序号已由Arena控制重放核对，末端连续durable前缀仍无恢复证明；受控normal路径在候选发布后逐entry复核捕获的Bookie准入generation，在途同坐标normal重试可在原owner比较冻结内容并等待原物理结果，已发布运行期候选则经有界I/O服务点读比较重复payload。重启扫描候选仅是物理位置，不获得运行期durable标记；normal与重复请求提交后停止响应等待已有隔离切片；网络deadline/断连接线、完整请求生命周期、bytes预算、生产成功和restart前缀重建仍未接通，不能以该候选直接ACK。

允许有界I/O queue depth大于1；物理前缀排序在各stream内，不新增跨shard全局sequencer，但共享文件的barrier成本/错误范围不因此隔离。batch边界、stream lineage与范围发现由§9的可恢复framing和完整allocator authority给出；physical durable-through按§12先验证候选，再凭独立有效证据或必要恢复同步重建，不要求另写持久成功游标。restart从已验证checkpoint/覆盖cut按stream扫描活动后缀；已由control authority解释的FREE/retired范围不能被当作未知缺口。所谓“截断DATA后缀”仅指将已证明可丢弃的特定stream/generation未成功后缀排除出有效批次范围，不等于缩短共享文件；required数据损坏或边界无法判定时fail closed，不能仅凭CRC错误或内存watermark丢失猜测无ACK。

首个文件切片在一Arena内为固定append shard各设物理stream，共享预分配DATA文件；各自使用allocator授权的不重叠范围。实验manifest固定shard/stream/file映射、文件增长/rotation及最大文件数、活跃buffer上限，不为每ledger建文件。物理stream是恢复排序单位，不是独立设备或flush域：`fdatasync/fsync`作用于指定文件，不能同步一个应用定义的extent/shard而独立于同文件其他写入。先测共享文件的barrier数量、覆盖集合及等待；以后拆分文件须另给可比证据，本次不新增跨shard flush调度框架。[Linux fsync(2)](https://man7.org/linux/man-pages/man2/fsync.2.html)

**共享文件的逻辑尾部恢复：** 完整allocator authority先确定当前allocation/generation，再按stream identity与batch sequence识别有效范围；文件offset只表示位置，不定义跨stream恢复顺序。若同文件低offset的S0/batch 11尾部不完整，高offset的S1/batch 50已durable并成功，S0的后缀判定不得删除、覆盖或忽略S1有效DATA；S1的完整块也不能充当S0跨越缺口的证据。

首批单stream恢复不执行`ftruncate()`、移动文件内容、collapse中间范围或hole punch。`ftruncate()`缩短文件会丢弃指定长度之后的全部字节，它不识别应用stream，见[Linux truncate(2)](https://man7.org/linux/man-pages/man2/ftruncate.2.html)。证明可回收的allocation仍经I/O终结、条件化FREE/generation bump和allocator复用，不直接破坏文件范围。以后缩短/删除整个文件或归还物理区间须独立证明移除范围已无任何stream、current selector、必要恢复依赖及存活I/O，并验证发布/恢复协议；文件收缩不作为首批前置。控制日志§5.4按自身物理日志发布与尾部协议处理，不能把其操作直接套到共享DATA文件。

发生既定I/O合同内无法解决的写错误或durability unknown时，首版不在线补洞：至少关闭受影响物理stream的新DATA准入，阻止越过失败batch发布后续成功。共享文件错误无法归因到更小范围时，暂停该文件相关stream；影响设备或required authority时沿§16及RFC-0005 non-writable/quarantine处理。batch 11 unknown、12已完成时，不因调用超时丢弃12，不用跳号、新stream或覆盖旧范围绕过11，全部按现有恢复/generation规则解析，不自动跨设备迁移。

等待所有已提交I/O实际终结后，可释放确实没有使用者的buffer；未解析坐标仍由pending或已经接管该范围的不可写gate保护，关闭准入后不允许无限增长异常pending/waiter。重启默认在相关控制/allocator/DATA恢复解析前不开放这些坐标，不能仅因易失pending消失而再接收不同payload。请求timeout不等于I/O失败，更不等于未写入；direct I/O错误可能已部分修改范围，须按不一致数据处理。[Linux write(2)](https://man7.org/linux/man-pages/man2/write.2.html)

`fdatasync()`错误可能报告此前写回失败；再次sync返回成功不是对先前全部DATA安全的证明。按上述存储错误恢复流程解析受影响范围，不以最后一次返回值清除unknown/失败状态。该保守策略由本原型选择，实际错误范围仍需故障实验核验；不为此引入逐entry成功日志或正常路径额外fsync。[Linux fsync(2)](https://man7.org/linux/man-pages/man2/fsync.2.html)

2026-10-02 隔离实现已解释Arena `DEVICE_STATE`的单调DATA文件隔离子集。实际DATA write/force失败立即关闭该共享文件的新DATA、点读和重复成功资格。绑定实际Arena控制driver的合批路径在I/O前为同文件在途批次共享预留通知ticket与160-byte逻辑日志容量；至少保留一个普通控制名额，预留不足在DATA坐标占用前背压。控制忙时每文件只保留一条已预留通知，原批次终结后顺序执行，停收不撤销原ticket；正常成功只归还预留，不增加控制日志或fsync。记录不删除原allocation/stream所有权；其后的ALLOC条件失败，较早ALLOC不受易失故障时机影响。完整记录重放及恢复同步后仍拒绝DATA物理重开，不由后一次sync清除。该容量不是实际文件系统空间预分配；owner故障、实际控制I/O失败及落盘前退出仍未闭合跨重启判定。没有记录不证明没有旧错误，append-only重开仍无local-result authority，未启用clear/reset或旧错误解析，详见[实验格式](implementation/local-storage/format-v1.md)。

2026-10-02 写入会话切片复用Arena `DEVICE_STATE` action 2/3记录单调epoch的OPEN/CLOSE。隔离writer在首次ALLOC/DATA前显式`startSession`，OPEN force并apply前不开放该writer；先预留160-byte逻辑CLOSE容量，普通控制记录不能消耗。文件关闭先拒绝新DATA并检查实际I/O/预留已终结，关闭DATA descriptor后仍持有目录锁，再验证Arena控制/故障通知已排空、无运行时或持久文件错误并记录CLOSE。DATA失败、执行前取消或关闭失败不补写成功CLOSE；OPEN未匹配CLOSE的重放在DATA恢复同步前拒绝物理重开，也不会因后次sync成功解除。首个epoch拒绝接管已有legacy allocation。仅正常关闭的会话可继续物理重开并在写入前开新epoch，旧授权块仍全部跳过；append-only仍无local-result authority。该边界已验证真实进程在DATA force失败、隔离记录落盘前退出；未正常关闭会话的恢复解析/解除、自动服务生命周期及可写restart仍未完成，未提升Gate。

2026-10-02 正常关闭会话的隔离restart已新增`openForLocalResults`：要求已持久CLOSE及正epoch、每shard至多一个未退役stream，复用目录锁内的完整DATA扫描/同步和Bookie route选择，再开新writer会话。原owner bootstrap在稳定Bookie applied cut下先核对空前缀与持久stream起点/generation、恢复热定位容量，再原子装入该shard候选及连续through；每shard仅bootstrap一次。同内容旧entry可经原有点读比较与Bookie准入复核返回本地持久结果，冲突不增加DATA写；下一批接续同一stream序号，在fresh pool追加，旧授权范围仍全部跳过。实际新JVM正常写入/关闭、两轮重开/重复/新写的文件回归已通过；恢复sync失败或cut变化不发布owner，普通物理扫描仍不授予该资格。该路径限无MOVE/FREE、单stream/shard、正常关闭的管理会话；未正常关闭恢复、一般selector、尾部unused回收与完整服务restart/生产ACK仍未完成。

涉及既有成功数据的compaction同样须保证new DATA在恢复可发现边界内，才允许`MOVE_COMMIT`、selector切换或释放旧位置。以后若要跨缺口独立发布，须先接受独立块发现/恢复算法；本次不并行建设第二套模式。分别报告write wait、durability wait、physical-prefix wait和locator wait，不将同stream排队归为NVMe延迟。

### 6.2 对齐缓冲池的NUMA属地

2026-10-02 [隔离DATA提交](implementation/local-storage/README.md)已使用该固定owner池封包并执行真实write/force；完整对齐长度通过后先取得lease，再集中复制payload。driver接管后只有实际I/O和owner结果处理均终结才归还原池，取消响应等待或提前失败回调不释放仍在使用的buffer；池满或尺寸不足在复制/物理提交前返回资源不足。同日新增普通entry输入：整个执行组共享源额度，先计完整容量再分配固定大小的独立buffer，owner一次复制到对齐池后才归还源；源与目标共存时均计费，转换与物理驱动共用原完成ticket。池不足不占物理坐标，显式输入入口仍由调用方提供batch及授权地址，后续自动合批与大记录进展见下。堆buffer格式实验入口仍保留，读取/索引及网络bytes总账尚未闭合；不提升NUMA或资源Gate。


同日`NormalBatcher`逐entry入口复用该池与owner deadline，非空batch按bytes/count/最老deadline封包后才选当前已apply grant与下一物理地址，原预留入口仍重验完整范围；临时工作集拒绝不推进序号。固定源/池须能覆盖统一entry上限，超过普通batch阈值的合法大entry单独封包；1 MiB payload已有跨同allocation两个block的真实write/force和点读验证。没有新增临时大buffer；低水位预取、尾部回收与完整allocator/恢复总账仍未闭合。

同日运行期pool选择改用每个shard/generation的单调预留游标；文件高水位不再串行限制不同stream的独立授权pool。整批必须落在一个完整已apply allocation中，短尾或其他owner的grant可跳过到下一同stream pool，预留后不返回旧尾部，取消也不回退。游标只负责隔离运行期新范围，不证明persisted used/unused或回收资格；重开仍越过全部旧授权块，unknown范围不复用。此为空间接续进展，按需异步ALLOC_POOL请求见下，空间恢复总账尚未闭合。

同日按需refill已在实际Arena控制owner上合并有界pool请求，计划新范围只参考已apply high-watermark；force返回后顺序条件apply，再通知原DATA owner重验授权，不由计划/写完成推断grant。dispatch前每shard最多一个请求，dispatch后及满额背压，不追加另一重试层。无空间时不写空控制batch或消耗DATA序号；其后的小entry仍可使用当前pool实际剩余范围。组合多个合法最大entry时，先比较完整framing所需bytes并封包旧batch，再计算受格式上限约束的对齐长度，避免将需分批的合法输入当作owner错误。运行期按需refill、pool内无逐entry控制force已在真实文件验证，但预取策略、返还/FREE、pool used/unused恢复与完整allocator authority仍未完成。

2026-10-03，实际DATA游标已改按原成功allocation控制sequence/offset推进，后继grant可位于较低文件地址，进入后继后旧尾不可返回；restart已知grant全部跳过。恢复按每allocation连续区间及原控制顺序验证stream，并保持必要CRC/grant/DATA sync，checkpoint/rotation保留原operation序号。真实全量/覆盖辅助storage图与gap/duplicate回归通过，见[分配顺序证据](implementation/local-storage/README.md#分配控制顺序与低地址追加)。这是FREE/reuse必要前置，尚未授予generation bump、旧字节复用或整块回收authority。

2026-10-02 同一逐entry路径还可接纳已获Bookie exact grant的recovery输入：准入私有payload在driver claim后直接移交共享源预算并按完整array容量计费，直到封包复制或重复比较实际用完才归还。normal/recovery共用非空封包、stream序号、固定对齐池、ALLOC_POOL按需refill及物理write/force，不新增队列或提交票据；同坐标交叉重试共享待决结果，冲突不新增物理winner。取消response不结束DATA/grant义务，控制失败或DATA unknown按已有失败前缀处理。该入口仅覆盖快照移交后的存储持有阶段，完整wire/TLS/socket bytes credit交接仍未闭合。

2026-10-02 driver已发布候选的重复点读亦使用现有owner对齐池，不另建读池或大record临时buffer；源view已归driver拥有，不再重复制整payload。scratch按实际对齐record跨度申请，依次复用以校验文件/batch/record，返回record仅借用lease。池不足在I/O前背压，不产生第二DATA winner；源与scratch共同保持到实际比较及owner结果均终结。通用物理点读、只读重启检查及无池手动入口仍是堆buffer实验路径，完整读取/索引/网络总账未完成，不提升资源Gate。

状态：SYSTEM/OWNER_TOUCH隔离池已实现；PREFERRED和物理NUMA验证仍未完成。2026-09-26的[group启动与池](implementation/local-storage/README.md)已在owner绑定后分配/触碰对齐buffer，将同一全局数量和请求容量预算拆分给owner，预留原池归还容量并等待live lease后关闭。Netty/malloc页可能已被触碰，未测页分布，不声明硬属地或性能收益；未提升本节Gate。WalArena/DATA设备尽量与append owner、I/O服务资源及对齐DATA buffer pool同节点；NUMA只影响运行时placement，不新增Arena、控制日志、ProfileDescriptor字段或DATA身份。`ALLOC_POOL`的持久空间owner/generation与内存buffer pool的属地是不同事实；CPU/worker数量变化不重新解释ledger→shard或既有stream，重启调整worker映射遵守RFC-0005 §10.2。

首批仍一个Arena，优先在同时具备足够CPU、允许内存节点及本地NVMe的NUMA节点验证；未知设备归属必须标UNKNOWN。PCI sysfs `numa_node = -1`不能当node 0，复合设备归属无法证明时也不伪造本地性，见[Linux PCI sysfs ABI](https://cdn.kernel.org/doc/html/latest/admin-guide/abi-testing.html)。后续多Arena只为已有Arena分别规划worker/池属地，不在线跨Arena迁移。

启用属地优化时的池初始化顺序：

```text
create owner / locality initialization thread
    -> apply/verify planned affinity when enabled; OFF only records available placement observations
    -> create fresh aligned pool memory on that locality
    -> optionally set preferred-node policy on an owned mapping
    -> first-touch the new pages, within the same memory budget
    -> publish the initialized pool to its locality's execution path
```

不能由一个启动线程先分配并清零所有节点的buffer再宣称已就地分配。默认本地优先分配仍受内存策略、cpuset与余量影响；移动线程或后设策略不自动迁移已分配页。已有malloc/Netty池返回的页可能早已被触碰，因此owner-first-touch只是best-effort，须测实际分布，见[Linux NUMA memory policy](https://docs.kernel.org/admin-guide/mm/numa_memory_policy.html)。OFF或绑定降级后如实记录初始化线程位置，不能把触碰动作当成指定节点已达成。

| `walBufferNumaPolicy`拟值 | 初始化与失败边界 |
| --- | --- |
| `SYSTEM`（默认） | 不修改内存策略，仍保留对齐池、统一预算、原池归还及I/O生命周期 |
| `OWNER_TOUCH` | 在owner/本地初始化线程上创建并触碰新池页；已有页不因再触碰取得硬属地保证 |
| `PREFERRED` | 仅对Segment拥有的完整新匿名大块映射设置`mbind(MPOL_PREFERRED)`，再触碰并分割；只在池初始化/有界扩容调用，不在每Add调用 |

PREFERRED的目标须属于有效memory-node集合，配置非法仍报错；设备节点未知或native/策略调用失败时，按实验manifest预先声明的OWNER_TOUCH或SYSTEM回退并记录实际策略/原因，不伪称绑定。`MPOL_PREFERRED`允许内存不足时从其他节点分配，CPU REQUIRE不把它提升为页面硬绑定；策略不会默认搬迁已有页，见[Linux mbind(2)](https://man7.org/linux/man-pages/man2/mbind.2.html)。不能对任意Netty slice或可能共页的malloc范围执行mbind；只操作自己完整拥有的映射，不改进程全局内存策略或宿主机配置。

buffer携带原池/属地及既有owner/generation信息。I/O完成或网络线程最后释放时，走原池的有界归还路径；归还通知所需容量在准入/提交工作集内预留，不会因DATA输入满额而无法释放，也不进入另一个节点的全局空闲池。跨节点归还积压、池映射/页与在飞行bytes持续计入§13总账。拓扑变化不在线迁移、释放或重建在飞行I/O使用的内存；仅取消future仍不能复用buffer，重启/退出等既有终结条件不变。

网络与存储位于不同节点时允许一次必要交接：Netty在网络侧完成TLS/framing并移交受控view，owner集中复制进存储侧本地对齐buffer，后续batch/I/O/completion尽量留在存储侧。一次连接服务多个ledger不要求event loop与所有shard同节点；必要复制/远程访问计量，避免可变状态跨节点反复转交，不把形式上的零复制作为目标。NIC RSS/IRQ/irqbalance及NVMe完成中断属于部署记录，Bookie不自动配置，也不因Java线程已绑定就宣称I/O全程本地。

## 7. Shared Cold Slab

首批隔离切片统一采用固定shard和共享DATA block，低速及高吞吐ledger都在此路径验证batching、durability、点读、恢复和本地回收成本；dedicated extent不参与本切片性能结论。ledger不因活跃而拥有永久专属I/O buffer。

```text
WalAppendShard
├── shared active block 0
├── shared active block 1
└── shared active block 2

one block
├── ledger A / instance X / entry 0
├── ledger B / instance Y / entry 0
├── ledger C / instance Z / entry 18
└── ledger A / instance X / entry 1
```

特点：

- active block 数量是 `O(shards)`，不是 `O(ledgers)`；
- 多个ledger共享block write和durability barrier；
- ledger state 只保存 locator/tail/inflight 等小型元数据；
- block record header 必须携带 ledgerId、instance、entryId、length、checksum 和 generation identity。

代价：

- 删除一个冷 ledger 不能立即回收仍含其他 live record 的 block；
- 需要 dead-record accounting；
- 仅当 whole block 全死时直接回收；
- 否则可使用低优先级 compaction；
- allocator 应按 lifetime class 分组，降低冷热混合带来的碎片。

因此，Segment 不能宣称全局“零 compaction whole-ledger reclaim”。

共享block的物理等待不阻塞delete delivery：L1 tombstone durable且清理义务可重建后即可推进RFC-0004的delete-applied cursor，即使L2存活使该block不能free。后续delete L2仍可应用，或由compaction搬走live records；待回收bytes单独记debt。applied cursor不是物理释放或旧reader/writer/I/O drain完成证明。

## 8. Dedicated Hot Extent

本能力在首个shared-block切片中**DISABLED / DEFERRED**，不作为测清基础收益的前置，也不以模拟dedicated结果替代测量。后续只有对应资源/点读/恢复Gate接受且manifest启用后，ledger达到经Spike冻结的阈值才可对新allocation采用dedicated extent：

- 累计 bytes；
- 持续 throughput；
- active tail；
- shared-slab fragmentation 或 read amplification。

候选 extent sizes：

```text
1 MiB
8 MiB
32 MiB
128 MiB
512 MiB
```

这些只是 Spike 搜索空间，不是架构默认。晋升不要求迁移旧 shared-slab record；旧记录仍由 locator 和 ledger instance 关联，并在 delete/compaction 时失效。

dedicated空间只属于一个ledger，不能在同一专属block/range混入其他ledger DATA。跨ledger仍可共享已有的文件durability barrier，但共享barrier不等于共享物理block。不开每热ledger永久buffer、线程或文件；需要专属assembling state/buffer时从统一有界池按需取得，总数与峰值内存单列计量。达到上限时，后续新allocation可继续使用共享布局或明确背压，既有dedicated allocation不改为混写。布局选择不改变entry identity，也不要求搬迁旧记录。

dedicated extent 的优势是按 extent 列表快速 logical/free；但只有集群 delete authorization、本地 reader drain 和 durable generation bump 完成后才能物理复用。

2026-10-02 隔离`IsolatedSegmentStorage`已统一持有上述本地资源图：新目录或正常关闭restart须全部固定shard的原owner bootstrap和控制票据终结后才开放请求，总热定位容量按shard拆分而非复制。关闭先停止入口/执行组准入，等待实际I/O、owner及消费方读结果buffer lease，再在writer目录锁内关闭DATA并完成健康CLOSE，随后关闭双控制日志；超时保留原资源owner/锁，启动失败亦用同一路径清理，不删除数据。未正常关闭会话、完整Bookie/readiness/registration、一般MOVE/FREE selector和完整bytes预算仍未闭合，本地资源图不提升Gate。

## 9. Block 与 locator 最小合同

候选 block framing 必须能验证：

```text
formatVersion
blockId
slotOrExtentId
generation
writerShardId
blockSequence
payloadLength
recordCount
record origin: ENTRY_DATA or RELOCATION_COPY
relocation operation identity/generation and target binding when applicable
headerChecksum
per-record storage envelope integrity and BK entry CRC32C
optional whole-block payloadChecksum for scan/recovery/diagnostics
commit marker or torn-tail detector
```

每个 locator 至少绑定：

```text
deviceId
slotOrExtentId
generation
blockOffset
recordOffset
recordLength
ledgerId
ledgerInstanceId
entryId
```

**来源必须可从持久格式恢复。** 首批在已有存储envelope内区分普通/合法恢复写入的`ENTRY_DATA`与本地搬迁的`RELOCATION_COPY`；这是候选语义名，字节偏移与编码仍由同一实验manifest冻结。allocation/batch层的等价编码只有在不依赖RocksDB或易失任务对象、且每条record来源唯一可判定时才可采用。内部存储路径设置来源，普通客户端不能通过自带flag伪造；缺失或未知来源不猜成普通entry。

`ENTRY_DATA`仅表示来源，不证明原RPC曾获ACK或客户端AQ。relocation copy绑定逻辑坐标、目标allocation/generation及与MOVE匹配的operation identity/generation；完整控制历史的成功MOVE或有效checkpoint最终选择它，才授予当前定位权。没有该选择的副本保持未发布，不覆盖原位置；CRC正确、较新generation、较大offset、mtime或最后扫描到都不是winner规则。

MOVE成功后不原地改来源类型或写“提交位”；record保持不可变，是否生效从控制状态解析。这里的block完整性/torn-tail标识不代表relocation已提交。普通/恢复写入和搬迁共享物理完成，分别走§6.1的上层结果分支，不增加控制record、payload副本、fsync或密码学hash。

首批必须能独立校验一个record，不扫描整个大batch。固定envelope/header完整性覆盖来源、move身份（适用时）、instance、allocation/generation、record length及必要存储坐标；内层BK CRC32C覆盖其自身metadata与应用payload。使用现有候选的record级校验方案，不以正确BK CRC替代外层身份校验，也不增加第三套逐entry密码学hash。整块checksum可用于扫描/恢复/诊断，不能成为单entry点读的唯一验证方法。仅匹配物理offset不足以防止ABA。

实现状态：隔离`SegmentPointRead`已从完整解析的batch取得record位置，以当前Arena grant、DATA身份和batch header约束对齐单record读取，再独立验证record/BK CRC及locator身份；真实文件跨页header、错误定位、篡改和较低新写阈值重开的局部测试已覆盖。2026-10-02的`Reader.live/restarted`复用原owner队列/completion、有界I/O及固定对齐池，分别从live hot或已验证启动清单选择位置；I/O排队前的有界文件pin阻止提前关闭writer锁，回owner后复核Bookie本地读票据及候选/Arena cut。成功record持有池lease到消费者关闭，取消和读盘/owner/回调失败归还实际终结的资源；未知位置不报告absence。重开前要求池可覆盖旧record实际跨度，较低新写N不遗忘旧entry。完整Profile读授权、一般MOVE/FREE selector/代际pin、响应credit接线、旧I/O错误解析和服务级可写restart仍未接入，本段不能作为生产点读保证。

读取先按现有locator取得allocation/generation pin并复核selector与权限，再使用匹配且已验证的cache，或发起覆盖目标record的必要对齐I/O。locator的offset/length须检查非负、溢出、声明最大record及allocation边界后才分配大buffer；损坏length不能扩大成无界读取。外层固定header有界读取并验证后才信任其长度，record自身CRC/坐标仍需核对。对齐范围不能越过合法allocation，布局必须提供合法padding/读取路径，不能靠越界读取凑alignment。

同block相邻读可以有界合并，不等待未来请求或引入网络Range协议；孤立点读及时发起。返回目标entry后，buffer/pin随最后使用者和真实I/O终结释放；小slice长期占住大父buffer时可受控复制已验证小范围，及时释放不再依赖的父buffer/pin。缓存、父buffer、副本和pin全计入§13预算，不为形式零复制阻塞回收。示例1 MiB batch内读1 KiB若强制整批，读取bytes/返回bytes为1024；这是布局反例计算，非实测结果。

exact bytes、外层record校验算法、commit marker和direct-I/O alignment由Spike B数据决定；独立record验证这一要求不再是whole-block checksum可替代的开放选择。

**DATA提交后冻结：** block由assembling转为submitted前完成header、record count、checksum和padding；此后内存内容及物理写入范围不可追加修改，不更新header/footer，不以read-modify-write重写包含既有成功记录的对齐块。下一批取得另一个有界pool buffer和新的、已授权未使用范围；旧buffer等真实I/O终结才可复用，旧物理范围只有按§10 durable free/generation bump后才可另用。控制日志checkpoint/superblock仍按自身发布协议，不套用本DATA规则。

最大等待从最老请求计算，低负载到期即padding到声明alignment再提交。例如4 KiB对齐范围已保存并确认1 KiB数据，不能为了追加下一条数据重写该4 KiB；同generation不防后次torn write破坏旧成功数据。4 KiB仅为示例，alignment与padding预算由manifest冻结并计入磁盘写放大。

framing还须能恢复§6.1的stream identity/generation、batch边界、物理sequence及范围映射，不靠易失batch table、最大ledger entryId或文件offset顺序定位后缀。完整allocator/current-selector authority先于DATA归属判定，跨stream较高offset不能因另一stream尾部失败而被丢弃；复用后的旧generation header/CRC按§6识别残留，不能复活旧owner或猜测当前unused。多个block属于同一batch时，其完整性和barrier覆盖必须全部可判定；缺block/坏边界不能假装完整。不得把prototype framing写成已接受stable format。

## 10. Delete、Free 与 Reuse

集群级delete authorization由RFC-0004提供；本地先应用terminal effect，再异步回收：

```text
1. verify durable cluster/local delete authorization
2. reject new reads/writes for ledger instance
3. durable terminal tombstone with reconstructible cleanup obligations
4. allow RFC-0004 no-hole delete-applied cursor/result; do not wait for physical reclaim
asynchronously:
5. drain readers/pins and terminate or reliably isolate old writer I/O
6. clean derived state; compact live shared records if needed
7. hold quiescence/reclaim gates, durabilize the FREE command and apply it conditionally in sequence
8. expose new generation to allocator and record physical reclaim result
```

terminal tombstone由RFC-0005的Bookie级route/control authority持有，allocator消费该事实，不要求为推进cursor在每个Arena再造一份tombstone日志。删除准入关闭后旧locator也不能获得新pin；异步派生清理不代替这个gate。cleanup从tombstone、完整allocator/current-selector authority及有效DATA重建，cursor既不授权reuse也不证明强访问屏障完成。

`FREE_AND_BUMP`的主语是整个allocation/slot generation，不是某个ledger的删除。候选语义如下，不据此冻结完整二进制字段：

```text
FREE_AND_BUMP {
    allocationOrSlotId
    expectedOldGeneration
    newGeneration
    reclaimReason and required authority references, as applicable
}
```

接受条件是整个回收单元已无live occupant、待完成写入或旧pin，并满足current selector、tombstone及相应清理规则。delete、compaction、orphan清理可提供各自原因和必要引用，但原因不替代whole-allocation reclaimability。dedicated可附单ledger身份核验；shared不能用一个`oldLedgerInstanceId`/`deleteRequestId`证明所有occupants已终结，也不把完整occupant列表塞进每条FREE。正常提交前消费完整allocator/selector和有效DATA完成受保护检查；replay仅按§5.3绑定事实及前序控制状态判断，不重读已合法回收的旧occupants。幂等/冲突仍使用operation、当前generation及条件化顺序。

该命令durable且顺序apply成功前空间不得用于新owner。重复free返回既有结果或stale/conflict，不重复bump；expected generation或适用的authority引用不匹配时拒绝，不把任意一条ledger删除作为通用FREE前提。

对于 shared slab：

- logical tombstone 立即让目标 ledger record 不可见；
- dead accounting 在 authority 下可重建；
- block 未全死时不执行 slot free；
- compaction 复制 live record 时，必须按以下顺序执行：

```text
1. durable ALLOC/ALLOC_POOL for the new allocation
2. write immutable RELOCATION_COPY with full entry identity and matching move/target binding;
   make it durable and recoverably discoverable under §6.1, returning only internal copy completion
3. append conditional MOVE_COMMIT(expectedOld -> new) command
4. complete its group barrier, then apply in sequence; success selects the new authority
5. only after successful apply: atomic publishNewSelectorAndCloseOldPinAdmission()
6. drain pre-cut readers/pins and quiesce old-allocation writer I/O
7. when every occupant is moved/dead, hold the gates and submit a conditional FREE command
8. durabilize and apply FREE in sequence; a failed condition grants no reuse
9. expose the bumped generation only after successful apply
```

一个record move完成不等于整个shared block可free。`MOVE_COMMIT`不产生新的BookKeeper local success、AQ或ACK，只保持既有payload authority。有界source组的新DATA复制可合批，多个move record可跨source共享control-log group commit；每个cutover仍晚于覆盖自身sequence的durability completion，不要求每个victim或moved entry独立fsync，也不迫使foreground Add等额外relocation barrier。

copy完成不发布普通Add的hot locator；步骤4/5成功后才由MOVE分支切换selector并更新定位，DATA来源/提交位不再覆写。正常执行仍在提交MOVE前验证副本并保持所需保护；crash后则先重演完整控制链，只验证最终必要副本。A→B→C后A/B已FREE复用时不要求旧字节留存，也不强制每次FREE前生成新checkpoint；最终C缺失或控制链不完整仍fail closed，不回退到A/B或提升未发布copy。

首批compaction只选择已停止追加、原始DATA写入已解析、内容及占用集合可确定的物理回收单元，不扫描仍接收record的allocation后猜测其已无其他live数据。这个条件只约束block/allocation；OPEN ledger的旧不可变范围也可搬迁，不要求ledger close、集群fence或新ledger seal协议。同一source/generation最多一个正常compaction任务；异常重试、迟到任务仍按条件化`MOVE_COMMIT`防多winner，运行时去重不需新增持久任务系统。

候选优先取可整单元回收且live比例较低的冻结范围，dead/live accounting可以重建，但搬迁和FREE前仍复核当前selector、tombstone与generation。按有界的一组source估算`净可复用收益 = 可释放source容量 - 新占用destination容量`，后项计入实际allocation粒度、framing和对齐padding，不要求每个source单独盈利，也不把一换一搬迁记成净回收。存在旧pin或I/O时收益尚未实现；只有whole-allocation FREE后才计实际释放。选择阈值由既有Spike B负载测量冻结，不增加生命周期预测、classifier或通用成本框架。

### 10.1 Uncommitted relocation orphan GC

relocation 搬运的 logical entry 通常已有既存 local-success/AQ 事实。orphan 判定针对未 commit 的 **new allocation/location**，不能要求 logical entry 从未成功。

new copy 只有在完整 authority state 证明以下事实后才能回收：

- allocation owner、payload identity/location/generation 与该 move operation/generation 匹配；
- current selector 从未选择该 new location，或另一个 winning successor 已条件化排除它；
- checkpoint selector、完整 suffix 与 later authoritative reference 都不指向它；
- 该 new location 从未被发布为 local-success lookup location，既存 logical success 仍由 current authoritative location承接；
- move writer 已由 operation/control generation fence，迟到 commit 不可能成功；
- 没有 live writer、reader、locator 或 inflight pin；
- shared allocation 的其他 occupants 也都 dead/moved；
- 最后以同一 authority order 下的 conditional `FREE_AND_BUMP` durable 后才 reuse。

timeout 单独不是 orphan proof。`MOVE_COMMIT` 与 conditional free 在同一 `ArenaControlLog` apply order 中竞争：commit 先赢则 free 条件失败；free 先赢则 generation bump 使迟到 commit 的 expected generation/predecessor 失败，二者不能都成功。runtime GC 可以消费由完整 replay/checkpoint 构建的 current authority state，不要求每次全盘 replay；离线 checker 必须能从 checkpoint+suffix 独立复现结论。

## 11. Allocator checkpoint 与 rotation

2026-10-03 Arena rotation增量：现有applied checkpoint可在停写后建立两个相邻cut，绑定两条原commit identity，强制同步新物理代及目录，完成两个恢复根后才回收旧prefix。固定sidecar锁覆盖整个切换，后续checkpoint保留physical generation绑定；单根/image损坏可回退，当前必要日志及完整suffix仍强制验证。准确字节合同见[实验格式](implementation/local-storage/format-v1.md#arena日志代与双恢复根)。Bookie已复用同一双根物理协议，使用独立身份、magic、sidecar锁及第三页；权限恢复合同见[RFC-0005 §5.2](RFC-0005-segment-bookie-state.md#52-下一原型的真实持久化拓扑)。在线维护、长期历史压缩及完整步骤8未完成。

历史（`00edeb8f`版本）2026-10-03双owner实现增量：Arena codec已复用为`SegmentControlCheckpoint`，Arena仍拥有自己的第二页；Bookie拥有第三页，两者各写自己的完整页且不截断另一页。半写Bookie页不改变Arena有效发布判断，所有immutable身份页和旧控制字节仍必需。Bookie/真实DATA图新增回归及有限模型见[局部实现](implementation/local-storage/README.md)。prefix reclaim/rotation及完整步骤8仍未完成。

数据或 derived-index checkpoint 可以丢失并重建；allocator authority 不能全部丢失。

历史（`6402c1c6`版本）2026-10-03局部实现：隔离Arena已提供applied状态/字节cut快照、有界A/B image的force/验证、CHECKPOINT_COMMIT持久且顺序apply后的inactive发布页force，以及匹配commit/image的恢复后缀和旧image/全日志回退。真实文件、JVM halt边界、正常DATA重启及有限模型见[本地状态](implementation/local-storage/README.md)。当前完整旧日志仍保留并验证；没有prefix回收/rotation、Bookie checkpoint、MOVE/FREE selector或在线调度，不把以下完整协议称为已完成。

control-log 旧段的唯一合法回收顺序：

```text
1. choose a complete applied control cut S
2. freeze/COW allocator + current relocation selectors at S
3. write bounded checkpoint chunks
4. fsync and verify checkpoint content identity
5. append/durabilize CHECKPOINT_COMMIT(generation, S, identity), then apply in sequence
6. update and fsync the inactive superblock
7. atomically select the active superblock generation
8. verify fallback checkpoint/suffix dependencies
9. only then reclaim a prefix covered by surviving authority
```

如果步骤中途 crash，restart 必须选择最后一个完整、可验证且依赖 suffix 仍存在的 generation。不能只按最大 generation number 选择损坏 checkpoint。

inactive→active superblock publication必须同时校验相同storage incarnation、Arena identity、format/mandatory features、device-manifest generation与migration generation；response loss后重读两份superblock和committed checkpoint解析同一generation，不创建新generation猜测成功。superblock不能覆盖或auto-restamp RFC-0005的Bookie/storage compatibility fence，也不能被用作“旧binary会看见”的假设。

checkpoint through `S`只取§5.3连续applied prefix，不能取仅durable而未apply的最大sequence；条件失败的已应用no-op也属于此前缀。`S`之后已持久命令由正常apply或restart replay继续处理，rotation仍保留其必要后缀。checkpoint保存allocation ownership/generation、free/reusable/retiring、current selector、old-source retirement/new-pin gate、whole-allocation reclaimability及必要anti-ABA/权威引用；它是Arena控制状态的紧凑表示，不是derived locator。

完整历史 move chain 不是必需：已经由 current selector 与 durable free 完全取代、且不再存在可混淆副本的历史可以压缩；old source 尚未退休时必须保留 current selector/retiring gate，live process 中 cut 前已存在的 volatile readers 仍按 runtime drain。rotation 不得删除唯一 authority；旧 A/B checkpoint 仍作为 corruption fallback 时，其必要 suffix 不能先删。

checkpoint/fallback保留的是可重演到当前状态的控制依赖，不是让快照中曾引用的所有payload永久保留。旧checkpoint引用A、完整后缀证明A→B→C及A/B generation已退出时，fallback同样先重演到C，再验证当前必要DATA；不追读A/B旧字节。后缀缺失或无法证明退出时不能跳过依赖，也不通过每次FREE强制checkpoint来维持恢复正确性。

checkpoint不持久化individual reader、future、buffer reference或pin history。旧进程volatile pins不作为历史继承；已记录条件命令按§5.3确定性重放，不用当前pin=0改判旧FREE。重启先恢复selector/retiring gate并确认旧I/O终结/可靠隔离；需要提交新的FREE时再建立本次运行的quiescence与完整回收条件。

## 12. Restart 与 crash consistency

本节启动顺序只在RFC-0005的Bookie/storage compatibility fence已经验证、且尚未开放任何writer/registration之后执行：

1. 校验Bookie storage incarnation与完整required-device manifest；
2. 对每个required Arena读取并验证 superblock A/B、Arena identity、format/mandatory features与migration generation；
3. 选择最高的完整 committed checkpoint generation 及其 through-sequence `S`；
4. 验证sequence `> S`的完整连续条件命令后缀及绑定事实，按下述规则同步尚无独立持久证据的完整控制内容；仅排除§5.4证明可丢弃的末尾，缺口、generation冲突或其他未知分类保持non-writable；
5. 按§5.3顺序重演metadata predicate及成功/失败结果，构建最终allocation、generation、selector与retirement状态；不逐条读取历史MOVE目标，不开放服务或授予新的运行时空间使用权限；
6. 从完整控制关系枚举最终仍被引用及未完成操作仍依赖的DATA，按当前allocation/generation、stream/batch与有效coverage扫描验证；来源按§9区分，不把已合法退出的中间副本列为必需，也不按全文件offset截断共享后缀；
7. 根据最终selector及DATA来源重建候选directory/locator，未发布relocation copy不覆盖当前位置。S0尾部不完整不得损及同文件S1成功范围，未覆盖/不能判定的坐标不返回确定absence；
8. 对当前必要且结构/身份有效、但无独立持久证据的候选按文件完成恢复同步；确认所需控制状态完整且已持久/重演，再开放相应读取及local-durable幂等成功/恢复证据。同步或数据验证前的内部重建不构成服务权限；
9. ownership、必需记录、完整依赖或已知I/O错误仍未解析时fail closed，不能以本次sync成功清除异常；
10. 所有required Arena完成校验与必要恢复同步前Bookie保持RECOVERING/READ_ONLY；READ_ONLY也不豁免对应范围的数据验证与同步，之后仍须完成RFC-0005 route/delete/readiness和registration才可writable。

控制历史验证与当前必要DATA验证是两个阶段。例：checkpoint中K位于A，后缀101 MOVE A→B、102 FREE A、103 MOVE B→C、104 FREE B均按predicate成功，A/B随后复用，尚无新checkpoint便crash。完整重演应选择C，不能在101因B旧字节已退出而失败；条件命令的成功仍来自有序metadata predicate，不由日志完整性直接推断。若C损坏/缺失、必要后缀不全或无法证明某旧引用已退出，保持隔离且不擅自回退旧位置。恢复成本计当前必要读取，不增加FREE前checkpoint或永久历史payload保留。

**恢复同步是上述流程的durability步骤。** 完整bytes、framing/CRC/generation正确只证明候选可读，不证明此前barrier完成，更不证明客户端AQ。buffered write后旧进程在barrier前退出，新进程可能仍从page cache读到完整数据；不能据此对重试返回durable success。[write(2)](https://man7.org/linux/man-pages/man2/write.2.html)与[fsync(2)](https://man7.org/linux/man-pages/man2/fsync.2.html)区分可读写入和持久化完成，所选I/O/故障模型仍由实验核验。

对缺少独立有效持久证据的完整合法内容，以已验证范围确定需同步的Bookie/Arena权威控制文件及DATA文件，按既有依赖顺序批量执行文件级barrier；文件同步不等于可以将未验证范围一起认定为有效。新文件/目录项依旧遵守各自发布协议。已被有效证据覆盖的内容无需重写，不增加逐entry fsync或成功日志；index重建、readiness持久化或同步另一控制文件都不能替代目标DATA barrier。恢复后本地durable不推导原请求是否ACK、quorum LAC或最终ledger prefix，后者仍由RFC-0004取证/close决定。

实现状态：隔离`SegmentDataRecovery.validateAndSync`在目录锁下验证完整DATA文件的身份、framing与当前Arena grant，再执行文件`force(false)`；验证或同步失败不返回扫描结果。`recoverPhysicalInventory`在明确容量内，经全目录验证同步后返回普通DATA的物理位置，容量不足或同步失败不返回部分结果，搬迁副本不进入普通清单。`recoverPhysicalStreamCandidate`还可用调用方给定的shard/generation/起点，对匹配shard与generation的batch按物理stream sequence拒绝缺口、重复和容量超限；按Arena已apply的block ownership只解析目标stream范围；同文件其他stream不混入该候选，仍检查文件身份并执行共享文件同步，目标stream结构损坏或文件同步失败都拒绝返回候选；传入起点须匹配Arena已apply的持久首序号。隔离测试已在normal DATA真实force及durable fence后关闭、重开Bookie/Arena控制日志，并从同步候选点读。隔离回归证明旧stream结构损坏不阻断另一stream物理候选，但全量inventory仍拒绝该文件。`recoverLocallyReadableStreamCandidate`另以重放的Bookie exact route/tombstone状态，只对完整解析和校验后的目标batch普通DATA record筛选返回定位；被筛除record的损坏仍拒绝恢复。在当前只有ALLOC/ALLOC_POOL、无MOVE/FREE的单Arena范围，受控只读recovery重复检查还要求目标shard唯一stream、已验证连续前缀、DATA恢复同步、唯一物理坐标及重放的exact grant；点读比较并复核grant后只返回物理候选比对结果；旧进程DATA I/O错误没有持久分类，该结果仍不是durable duplicate成功。裸候选仍未依据完整Bookie/current-selector控制关系选出全部必要DATA，也不重建可写的末端空间状态；局部结果不能作为本节完整恢复或ACK证据。

受控只读normal重复检查复用同一物理候选扫描，先取得普通Bookie准入票据并在阻塞同步/点读期间保持占用，完成前复核normal资格，最终仅返回匹配、冲突、过期或未就绪的物理候选结果。真实normal DATA隔离本地结果后的关闭重开、同步失败释放及同步中tombstone撤权已有定向测试；候选比较不证明current selector或旧I/O错误结论，不形成最终Add冲突、durable duplicate或ACK权威。

普通Add另在本地结果后和DATA barrier前分别以子进程`Runtime.halt`，父进程重开双控制日志并完成恢复同步、受本地route约束的点读及物理候选比较。该回归验证进程终止窗口，第二次掉电、服务级可写restart和幂等成功仍未验证。

隔离单stream恢复读取现将DATA writer排他锁保持至受控点读或重复候选比较结束；在此范围内验证重放的Bookie本地route、连续物理stream、唯一坐标、恢复同步、record身份，并在末尾复核route与stream。缺失/歧义不报告NoSuchEntry，多stream不选择。此路径尚无一般current-selector与allocation pin，也未接Profile读授权或生产服务；未来MOVE/FREE不能沿用单stream唯一性作为选择依据。

隔离子进程回归现分别在recovery DATA本地持久结果后、以及完整写入但DATA force前执行`Runtime.halt`。父进程重新打开Bookie/Arena控制日志，扫描并同步目标DATA，再点读并比对物理候选。这验证进程终止后的控制重放、文件锁释放与恢复同步入口；`Runtime.halt`不代表掉电，物理比对也不解除旧I/O错误未知或形成durable duplicate/ACK权威。

追加式重开另有两次真实进程终止回归：首个进程得到隔离本地结果后退出，第二个进程恢复同步旧DATA、在新的已apply授权块写入另一物理stream后退出；父进程能分别恢复并点读两个stream，旧recovery重复请求面对同shard多stream仍返回`NOT_READY`。第二次`Runtime.halt`仍不是掉电，且新物理写入没有local result权限。

当前`IsolatedSegmentFiles`已有隔离的**物理append-only重开**入口：Arena控制状态须无未apply/I/O中的命令，取得独占writer锁后，在同一次持锁期间完整验证、同步旧DATA并重新核对文件身份；下一物理位置取每文件全部已apply授权块的最高结束位置，不复用块内全零尾页。新写必须另获已apply Arena grant；重开句柄不能交给返回local result的DATA driver，也拒绝无授权原始写入。Arena的持久首序号与授权水位仍不包含空间used/unused或末端durable-through；旧I/O错误、完整Bookie/current-selector及必要DATA依赖没有恢复判定，故此入口不等于服务级可写重启或durable duplicate权威。

可选的有界全目录清单在同一次writer锁内完成旧DATA验证和恢复同步，随重开句柄保留，并能在原owner经scope、容量、重复坐标及无在途提交检查后一次性装入物理热候选。扫描同时在显式stream容量内，按物理位置和Arena持久首序号保留各已观测stream的连续序号证据；缺口/重复stream不返回连续候选，空stream仅报告首序号之前的物理前缀。隔离点读现可在重开句柄的有界读取pin下消费该清单，跨stream要求目标坐标唯一、所在stream具有连续物理前缀、Bookie exact读票据及末尾复核；点读前后核对扫描绑定的Arena applied cut，后续新grant使旧清单失效，读取期间不释放writer锁。旧坐标只在经过该热目录准入的路径中被占住，低层物理写入尚不查询此目录；候选仍不带local-durable结果资格，任一容量不足或同步失败不返回部分句柄。DATA锁入口在本进程打开第二个锁文件channel前拒绝同scope重入，避免失败关闭channel释放原writer的POSIX锁；独立进程仍由文件锁排斥。当前物理序号证据没有完成旧I/O错误分类、一般current selector及完整Profile读授权，不能用它推导local-durable或服务级可写前缀。

2026-10-02 同锁全目录清单另可按重放Bookie本地route/tombstone筛选普通DATA定位，筛除record仍完成结构/CRC与所在物理stream校验。定位容量只计当前可读record，stream容量覆盖全部已扫描stream；扫描前后要求Bookie applied cut一致且无未决控制/entry义务，不持Bookie monitor等待DATA同步。任一已观测stream缺口/重复、当前坐标歧义、同步失败或控制cut变化均不返回重开句柄。无MOVE/FREE且已apply allocation不变的当前范围内，多个stream的唯一旧坐标可以从同一重开清单点读；这不证明一般selector、旧I/O错误已解析、后续新写定位或服务级可写restart，也不扩大单stream的只读重复候选入口。

此路径只补正常中断留下的持久化确认，不能修补缺失/损坏prefix、ownership冲突、未解析写回错误或依赖不完整；这些仍沿原隔离/恢复规则处理，不以“再sync一次成功”解除。B2/B9/B16须执行两次故障：write完成但barrier前终止进程，重启处理重复请求，再模拟掉电；已允许成功的内容仍须存在。一次kill后可读检查不足，恢复barrier按文件报告次数/等待且不计正常Add增量。

suffix 出现 sequence gap、必要 record 缺失或 checkpoint content identity 无法验证时 fail closed。不得用更大的物理 generation、mtime 或 data scan跨过 authority gap。

DATA后缀按§6.1逻辑排除及§10条件化回收处理，不能因单stream恢复缩短共享文件。B3/B6/B9同时注入FREE generation 7→8、重新分配后未写/部分写即crash、旧header/CRC仍合法：不恢复旧owner、不以整块清零作为识别前提；仅在当前分配/依赖已证明可回收时释放，否则保持不可写。是否存在未使用新范围与是否存在required媒体损坏分别判定。

upgrade/migration不要求跨device transaction：每个Arena按同一Bookie migration generation写入prepared/format-ready事实；只有全部required devices匹配时Bookie级readiness才可前进。任一边界crash、部分device完成或unknown mandatory feature都保持non-writable并重试同一generation。device移除必须由cluster-authorized新storage incarnation/device-manifest generation完成。存在Segment payload/control authority后能否rollback由RFC-0005 negative-proof gate决定，allocator不得因old binary可打开目录而宣称兼容。same-scope candidate未通过前，本节只允许isolated format prototype；不得把candidate superblock bytes发布为stable on-disk contract，也不得污染可继续运行的Classic scope。

必须注入的 crash 边界：

- ALLOC write 前后；
- control-log durability 前后；
- DATA write 中和 durability barrier 前后；
- local ACK 前后；
- Bookie级DELETE_TOMBSTONE持久化及Arena消费该外部事实前后，不追加Arena独立tombstone；
- reader drain 与 FREE_AND_BUMP 前后；
- checkpoint data、commit、superblock switch 和 old-log reclaim 各边界；
- compaction new allocation/data durability、`MOVE_COMMIT` append/durability/response loss、原子selector发布并关闭old-pin admission、reader/writer quiescence和old free各边界；
- 同一 predecessor 的并发 move、move chain、group-commit torn tail、new payload digest mismatch，以及每个边界删除 derived index 后的重建。
- checkpoint cut `S-1/S/S+1` 的 move、current-selector 压缩、A/B fallback suffix dependency、orphan GC 与迟到 commit/free 竞争。

## 13. 内存与空间模型

idle ledger 只保留小型状态：

```text
ledgerId and ledgerInstanceId
master-key hash/reference
fenced/sealed/deleted flags
tail pointer
inflight counters
index generation
```

Spike Gate：

- idle ledger resident state 目标不高于 512 bytes，硬 Gate 不高于 1 KiB；
- 100k idle active ledgers 的 ledger metadata resident memory 不高于 128 MiB/Bookie；
- 创建 100k ledger 不预留固定 extent；
- idle ledger 不拥有专属 block buffer；
- 首批shared assembling buffers为`O(shards)`，已提交/点读/cache等buffer另受统一hard cap；dedicated启用后的专属assembling池单列上限，不能把`O(active hot ledgers)`隐藏在该声明中。

所有测量必须区分 heap、direct memory、native allocator 和 RocksDB block cache，不能只报告 Java object shallow size。

固定worker数量与逻辑shard数分别计量：worker栈、队列、completion通知、拓扑快照和placement plan均有界；多个shard共享worker不消除各自的batch/恢复状态。§6.2的NUMA池只是既有buffer总预算的属地划分，不能给每节点/worker各复制一份完整全局额度，不能随hot ledger数增加常驻池。页分布、归还队列和fallback计入内存账，CPU时间/quota及服务余量按RFC-0005 §10.3另列，不以更多核、spin或NUMA池突破原bytes/inflight上限。

总资源账本还必须覆盖Bookie route/credential/fence state、allocation/pool/slot state、current selector/retirement/anti-ABA state、pending Add/grant/waiter、block buffers、checkpoint working set，以及derived index/cache。RFC-0001 §11.6的连接/TLS/header/read、半包及已解码未准入buffer也计入Bookie同一总量，向shard交接owner/credit而非整包后才开始计费；控制连接的有限份额不被DATA接收耗尽。分别报告随连接、ledger、entry、allocation、shard和pending operation增长的项；`1 KiB/idle ledger`不能替代总预算。

每shard的同一套预算覆盖准入、待合批、已提交I/O、等待physical prefix/locator及响应阶段；request count、源/对齐bytes、inflight batches和duplicate waiters分别有上限。出队或移交future不释放credit，真实资源释放或转移到另一已计费owner后才归还对应份额；复制期间源与目标同时持有的bytes都计入。断连/取消而I/O仍在进行时保留其buffer/inflight费用，不能留下队列之外无上限的任务。复用请求对象，避免每层重新包装大对象。

**准入必须保留完成工作集。** 在DATA准入前保障封包、提交、定位发布及结果处理所需的最低资源，不能先让网络payload耗尽总预算，再等同一预算释放一个输出buffer。首批每shard少量预分配对齐batch buffers计入总账并免受入口payload挤占；定位/完成状态优先由已有pending对象承接并在准入时取得容量，不等DATA durable后再争用最后一个热locator槽位，也不为每entry预留最大buffer。

入口payload、共享提交工作集、热定位/索引接管容量及控制/维护保留量是一份总账中的份额。ownership交接或有界申请必须立即成功或立即失败；容量等待使用已有有界调度，不阻塞Netty event loop、append shard主循环或持ledger锁等待其他阶段归还资源。大entry符合统一N但超过普通batch时，接受前取得对应有界工作集，否则返回背压，不能接受后永久堵队首。

§15的热定位、异步index batch/队列、查询已可见但尚未flush的memtable及相关native/cache内存也计入总预算。local success或热定位淘汰不等于资源消失；实际可查询且已计费的owner接管后才归还原credit。hot/index积压达到准入阈值时先背压新DATA，已准入/提交的数据仍有预算完成必要定位和结果处理，fence/tombstone仍有控制容量。不得通过丢有效DATA、无界队列或临时超限退出。B18暂停下游并把入口推到准入上限，恢复容量后须自行完成已接受请求并最终释放资源；不要求在磁盘/index永久不可用时继续接受新写。

同批物理buffer费用不随某条entry取消而归零；未解析写入的坐标保护按§6.1转交不可写范围后才收缩异常pending。账本同时覆盖点读缓存、对齐读buffer、大父buffer及小副本、pin数量/时长、DATA文件数与后续dedicated池。大量hot ledger或慢reader不能突破总量，也不能通过暂停回收改善短期p99。

最老请求的单调时钟deadline不被新到达重置。最大可接受entry统一按RFC-0001 §8取normal Add、single-entry Recovery Add、read response及Segment record/batch容量的最小值；普通block放不下但符合统一N的entry须有预算内大记录路径。否则在首次写入前拒绝，不能永久堵队首或等recovery失败。wire/BK entry/存储framing及alignment开销和checked arithmetic写入manifest，不因扩大临时buffer突破总预算，也不因收紧新写阈值降低已有entry读/恢复能力。

磁盘另按§13.1区分Arena内部可复用容量、文件系统可分配容量及索引/控制维护工作集。内存credits有界不证明SST/checkpoint有空间；Arena FREE也不自动增加文件系统available bytes。

重复坐标需要读盘核对时交给已有有界读取执行资源，不在append shard主循环同步等盘；正常新写不因此多一次索引I/O。连接/ledger只保留必要有界份额，控制持续热ledger的独占；fence/tombstone等撤权保留执行槽位、buffer及控制持久化预算，DATA满额不能永久阻止关闭DATA准入。超额返回明确可重试背压，不新增通用公平调度平台。

### 13.1 每Arena保留空间与进展

前台分配不得耗尽compaction目标、checkpoint/control-log rotation、tombstone/FREE及故障恢复所需空间。每Arena划分的是内部前台可用block与维护保留block；另按实际共享文件系统建立Bookie级磁盘总预算，为派生索引SST/flush/compaction输出、Bookie/Arena控制日志轮转、checkpoint发布及必要诊断文件保留真实可分配空间。跨Arena relocation未支持时，不能用另一Arena的内部空闲量掩盖当前Arena无法回收；多个Arena共用文件系统时也不能各自重复占用同一份文件系统余量。

`FREE_AND_BUMP`增加Arena reusable bytes，预分配DATA文件的物理占用不会因此自动返还文件系统。Linux预分配和释放文件区间空间是不同操作，见[fallocate(2)](https://man7.org/linux/man-pages/man2/fallocate.2.html)。首批回收以内部复用为主，不要求每次删除hole punch、缩短文件或返还磁盘；以后文件收缩按§6.1独立验证generation、I/O quiescence、selector及恢复依赖。

DATA预分配总量/增长受该Bookie级预算约束；复用现有[`DiskChecker`](../../../bookkeeper-server/src/main/java/org/apache/bookkeeper/util/DiskChecker.java)、[`LedgerDirsMonitor`](../../../bookkeeper-server/src/main/java/org/apache/bookkeeper/bookie/LedgerDirsMonitor.java)的磁盘检查和配置接入，以实际filesystem identity去重计量并限制并发维护承诺，不新增集群空间协调器。现有检查入口不是Segment预算已实现的证据。

运行前锁定各Arena可用whole blocks、DATA预分配总量、filesystem共享映射/available bytes、索引规模与SST/flush/compaction临时空间、control/checkpoint轮转发布余量、最大move/checkpoint工作集、并发维护上限、dead bytes/debt及低水位/hysteresis。真实保留量按允许同时执行的有界维护工作集推导，包含新旧文件发布期间并存的占用，不能只以DATA live bytes或未经验证的固定百分比代替。沿用现有状态顺序：

```text
NORMAL -> THROTTLED -> REJECT_NEW_DATA -> MAINTENANCE_ONLY
    -> RECOVERING/READ_ONLY if the next required durable step cannot be proven
```

Arena内部预算或filesystem余量达到各自低水位时，先限制DATA文件继续扩张和新写准入，不能因Arena仍有free slot而继续侵占文件系统维护余量。maintenance保留已预算的index flush/必要compaction、move、FREE、tombstone和checkpoint及最低调度份额。可恢复索引压力沿既有异步索引/背压解析；I/O error或authority不确定仍走§6.1/16错误恢复，不用“还有free slot”解除。低水位恢复与hysteresis、restart保留预算恢复、ENOSPC与必要诊断均须显式实现；只释放Arena slot未增加filesystem available时不能宣称索引空间压力已解除。

同一报告分别列`Arena reusable bytes`、`filesystem available bytes`、`derived-index disk bytes`和`maintenance temporary-space peak`，同时保留既有内存/负载/debt指标。B17/B18构造Arena有空闲块但filesystem不足、共享filesystem上的多Arena竞争、SST flush/compaction与控制checkpoint并行；限制新DATA后在声明保留量内验证维护可推进及恢复容量后的有界重新准入，不另建资源平台。

低水位可提高维护优先级，但仍按§10选择有净收益的有界victim组；没有足够候选时维持背压，等待实际删除或可用容量恢复，不循环搬迁近乎全live的块来声称进展。destination工作集和并发维护上限仍服从本节保留量；只累计copied bytes或source释放总量不足以证明净回收。

进展Gate限定在manifest声明的有界live set、受控admitted写入速率和可用维护I/O下：有可回收候选时，长期创建/写入/删除后的dead bytes与compaction debt不持续增长，停止新写后于锁定时间内达到可行目标水位。须明确候选分布、净收益阈值及保留量，不把无法获益的高live布局要求成无限搬迁任务。超过可持续负载或无获益候选时有界拒绝并等待容量变化；短期p99达标而维护长期饥饿不能PASS。

## 14. Reclaim 能力分级

| 布局 | logical delete | physical reclaim | compaction |
| --- | --- | --- | --- |
| Dedicated extent | tombstone 后立即不可见 | reader drain + durable free 后按 extent 回收 | 通常不需要 |
| Shared cold slab | tombstone 后立即不可见 | whole block 全死后回收 | 部分 block 需要低优先级 compact |

性能报告分别展示logical deletion latency、physical bytes reclaimed、pending dead bytes、compaction debt，另分列`compaction copied bytes`与`net reusable bytes gained`。后者按任务/有界victim组核算实际释放source减去新占destination的容量，不能把总FREE字节当净收益；未释放的pin/I/O等待另记debt，并结合前台p99判断维护成本。

理想化示例：大小B、live比例r的回收单元，复制约rB、净释放约(1-r)B，故复制/净释放约为r/(1-r)。r为20%、90%、99%时分别约0.25、9、99；这只是忽略allocation粒度、padding、索引和控制成本的算式，不是性能结果。实际按§10的一组victim及真实destination占用测量，不能由“有dead bytes”推断值得立即搬迁。

delete-applied/catch-up延迟另列，不以物理回收完成作为cursor条件；shared slab仍有live record、reader pin或旧I/O时，applied可完成而physical保持pending。

本布局的physical reclaim首先表示已满足安全条件的Arena内部复用；它不自动证明filesystem容量返还或介质字节擦除。报告按§13.1将内部reusable、文件系统available与实际文件收缩分开，普通删除不触发额外文件收缩协议。

## 15. Derived index

2026-10-03 局部实现：`SegmentDerivedIndex`使用既有rocksdbjni 9.9.3、单CF和有界WriteBatch，以当前完整同步/route选择的DATA inventory重建定位。重建先同批更新index UUID并删除旧namespace、flush，使旧coverage失效；定位flush完成后才force/验证独立A/B覆盖页，绑定identity、index UUID、两控制cut、连续stream范围及实际contents摘要。普通Add先由既有热表提供定位和local result，再经原owner的DATA票据及已有DATA I/O服务异步写入DB，每shard最多一个、每批最多256定位，不使用控制预留。完整删库后的正常restart可重建。后台失败保留热结果并在新DATA预留前背压；实际Put query-visible后由原owner淘汰匹配热定位。点读与重复写hot miss后fresh Get，再由同一DATA票据读取校验；在途捕获定位不依赖热表继续存在。有限坐标占用预算保留同坐标保护，热容量与完整恢复容量分开。正常关闭restart已验证实际持久coverage与当前控制/stream/grant，覆盖内只校验固定batch头及连续性，未覆盖授权tail完整解析并恢复同步；无效覆盖仍全量重建。有效覆盖复用原index代、仅补写tail并按当前Bookie后缀删除不可读定位，coverage不替代DATA sync或点读校验。运行期检查点现以每原worker一份DATA票据冻结已追平的shard cut，暂停Put后验证真实native内容、flush并force既有A/B覆盖页；同代publication按有效页最大序号递增，不因旧内容不匹配重置。暂停后新DATA只留hot，不扩大本次cut；失败/取消保持实际I/O及原owner终结边界。一般MOVE/delete排序及selector覆盖恢复、完整在线维护或全局native/磁盘预算未完成，不构成本节整体验收。字节/API与真实回执见[实验格式](implementation/local-storage/format-v1.md#rocksdb派生定位与覆盖页)及[本地状态](implementation/local-storage/README.md#rocksdb冷重建与持久覆盖切片)。

2026-10-03 真实运行期storage图新增active memtable未flush、覆盖页force前、PERSISTED owner回调内及正常CLOSE后4个JVM退出点。无CLOSE的物理扫描/派生重建不解除Arena隔离，local-result restart及点读仍拒绝；正常CLOSE对照使用同代coverage并恢复真实读/重试/追加。全类140项通过，详见[进程边界证据](implementation/local-storage/README.md#运行期索引的真实进程退出边界)。只证明所列进程故障边界，不证明掉电或未关闭会话恢复。

RocksDB可保存entry locator、ledger directory和tail summary，但必须：

- 不参与 payload ACK authority；
- 不参与 allocator ownership authority；
- key/value 带 format、instance 和 generation；
- 全库删除后可从 control log + data arena 重建；relocation winner 只能由 committed checkpoint current selector + complete conditional `MOVE_COMMIT` suffix 决定，checkpoint selector 必须可证明由此前完整 control history 产生，不能由 RocksDB、mtime、最大物理 generation 或 data scan 猜测；
- stale generation/selector locator在读取时被再次校验，并按当前authority重新解析，不能将落后的缓存项当作payload丢失；
- rebuild/compaction 有 foreground QoS 和 admission control。

**运行时可读发布与异步维护：** 首批写路径固定为：

```text
ENTRY_DATA enters its recoverable contiguous durable prefix
    -> publish locator in the existing bounded hot handle/index
    -> check each entry's authority and publish eligible local success
asynchronously:
    -> apply bounded batches to the derived index
    -> periodically persist the index contents
    -> durably publish a coverage checkpoint matching those persisted contents
```

local success之前目标entry必须可被当前点读正确定位；已有有界热定位结构可以承担该事实，不要求本次RocksDB Put/WriteBatch或flush完成。正常新写不增加同步索引读，也不增建一套独立存储系统。append shard不在主循环中同步调用/等待可能阻塞的RocksDB写入、flush或compaction；异步执行资源及积压复用§13统一预算，满额后背压后续准入。

上述是普通/恢复entry分支。relocation copy的durable completion仅交给§10的MOVE流程，成功应用并切换selector后才更新同一hot/derived index；不能使用一个通用callback无条件发布定位。全量重建先完成§12控制重演，再按§9受校验来源与最终selector选择位置；未被成功MOVE或有效checkpoint选择的copy保持未发布，即使其CRC正确且普通entry索引已丢失也不自动提升。正常DATA本身也不证明原调用获ACK，冲突仍按既有坐标规则处理。

`sync=false`不能证明调用不会阻塞：flush/compaction追不上时，RocksDB会延缓或停止写入调用。该成本须留在有界索引维护路径并计量，不能只用“没有index fsync”描述前台开销。依据[RocksDB Write Stalls](https://github.com/facebook/rocksdb/wiki/Write-Stalls)。

热定位只有在另一条已计费、实际query-visible的定位路径接管后才可淘汰；先完成查询可见的接管，再删除旧热记录，立即点读不得出现空窗。接管不强制等待flush：运行时查询可见与重启持久覆盖分开，尚未覆盖的DATA在crash后重放。不能把flush延迟变成每条热定位的强制驻留条件；异步队列、memtable及热结构的实际占用仍全部有界。

首批单entry查询固定顺序，与写侧接管共同保证没有虚假absence：

```text
lookup hot locator first
    -> hit: retain/protect the locator object
    -> miss: issue a fresh RocksDB Get against current state after the hot lookup
    -> validate either locator via the same selector/generation/read-pin rules
    -> on stale location, resolve current authority; unknown coverage stays not-ready
```

fallback不能复用早于hot查询的snapshot、iterator视图或negative-cache结果；即使从未同时缺失两份locator，先DB miss→后台DB接管/删除hot→再hot miss仍可能虚报不存在。RocksDB的[snapshot](https://github.com/facebook/rocksdb/wiki/Snapshot)固定创建时视图，[iterator](https://github.com/facebook/rocksdb/wiki/Iterator)也保持相应一致视图，后续query-visible写不会自动进入旧视图。其他需要快照的操作保留各自合同，当前点读不借旧视图判absence；不在DB I/O期间持ledger锁，也不建立跨两级索引的全局锁/事务。hot命中仍须保护对象生命周期，pin/selector校验不能省略。

**持久覆盖与恢复：** DATA physical durable-through和index persisted coverage cut独立维护。后者只覆盖相关index更新已完整持久化的连续物理范围，并绑定storage incarnation、Arena/stream、index format/generation及解释这些映射所需的control/selector cut；不能从最大entryId、最后一次Put成功、最大异步完成序号或查询可见水位推导。

首批纯派生RocksDB索引选择`disableWAL=true`，前置是实现并验证本节覆盖checkpoint。关闭WAL后的写入可能在进程crash后丢失，Put/WriteBatch返回不代表持久覆盖，见[RocksDB Basic Operations](https://github.com/facebook/rocksdb/wiki/Basic-Operations)。此选择不作用于Bookie控制日志、ArenaControlLog或其checkpoint，不能关闭权威状态的持久化。

关闭派生索引WAL不会消除SST、flush/compaction及其临时磁盘空间；这些输出和空间放大按§13.1计入真实filesystem维护预算，不能只计Java/direct/native内存。相关成本见[RocksDB Compaction](https://github.com/facebook/rocksdb/wiki/Compaction)。DATA过度预分配可能使index无法接管热定位，即使Arena内部仍有free slot；必须识别这一独立压力并有界背压，释放内部slot不算释放SST所需空间。

一次覆盖发布先选定有界update cut，确认到该cut的必要索引更新完成，再等待实际覆盖这些内容的flush持久化完成，最后沿既有checkpoint发布协议durable发布对应coverage marker。marker不能仅放入关闭WAL的未flush memtable后就用于跳过扫描；crash发生在flush后、marker发布前时，只能保守多重放。API、RocksDB版本、cut/marker编码与真实flush证据在同一实验manifest锁定，不凭一个异步回调猜覆盖。首批无需主动拆分更多column family；若使用多个CF，必须证明所有相关内容一致持久化后才推进cut，不能仅flush locator就将未flush的其他恢复信息算入。[RocksDB Atomic Flush](https://github.com/facebook/rocksdb/wiki/Atomic-flush)可用于此条件，但不是新增CF或全局索引事务的理由。

原始Add、MOVE后索引更新与删除清理按坐标/selector代际保持必要顺序，陈旧异步update须被丢弃或从当前authority重新生成，不能覆盖新selector或复活tombstone后的定位。点读发现派生locator陈旧时重新解析当前权威selector，按§9重新pin/复核；暂不能解析则有界重试或not-ready，不能把旧locator失败当成确定NoSuchEntry。覆盖声明也必须纳入相应已完成的更新顺序，不能用旧Add回调越过尚未解析的MOVE/delete。

coverage checkpoint仅缩短DATA重建扫描，不授权删除DATA、不替代allocator ownership、不决定relocation winner。完整allocator/current-selector/tombstone authority及其必要控制后缀仍按各自checkpoint恢复；index coverage不能省略后续MOVE/delete控制重放。

点读复用上述locator和§9独立record校验路径；confirmed边界由客户端、恢复候选范围由coordinator按RFC-0005 §8判定，Bookie不以陈旧local LAC截断合法物理点读。report实际读取bytes/返回bytes、每点读I/O数、cache命中、copy/父buffer持有及pin时长；批量写入大小不自动决定点读大小，未验证覆盖继续返回not-ready/unknown而非确定absence。

恢复必须实现并分别测量两条路径：

| 路径 | 必须扫描和验证的范围 |
| --- | --- |
| 正常restart | 仅跳过由有效持久index checkpoint证明已覆盖的DATA；验证实际索引、storage/stream/index generation与连续cut，对未覆盖且已授权DATA按§6.1物理stream前缀扫描必要tail，不用Put成功、最大completion或ledger entryId越过缺口 |
| 全部derived index丢失或覆盖证明无效 | 先从checkpoint+完整后缀重演最终authority，再枚举当前必要live allocation/DATA；区分ENTRY_DATA与relocation copy，只接纳最终已选择的位置，合法退出的历史副本无需重读。首批覆盖active/sealed shared及relocated数据，dedicated启用后同样覆盖；不能只扫描active tail |

两条路径均执行§12候选/持久证据区分及必要恢复同步；index coverage仅影响扫描量，不是DATA barrier证明。报告扫描bytes/I/O、恢复同步文件数/次数/等待、重建数量、资源峰值及到read-only/可写的时间。分批读能力须明确已验证覆盖，不能将未同步候选作为durable evidence/幂等成功；未验证范围不宣称absence。RocksDB全删不删allocator/current-selector authority，其空间、写放大和重启成本仍计入账本。

checkpoint缺失/损坏、索引代际不匹配或无法证明覆盖时回退必要tail扫描或完整重建。B2/B9/B12/B18至少验证Put成功未flush时crash、热定位淘汰与查询接管并发、index stall同时新写/fence、旧Add update晚于MOVE/delete，以及多CF启用时部分flush。报告index入库等待、query-visible积压、persisted coverage落后量/扫描代价和真实资源峰值，分别统计资源拒绝、重试及replacement；不新建监控平台。

## 16. 多设备与设备失败

每个 device/WalArena 有独立 control authority 和 generation namespace。跨设备 ledger 可以有多个 extent locator，但任何单个 allocation 只由一个 arena 管理。当前 compaction 只能在同一 Arena authority domain 内 relocation；device evacuation/cross-Arena move 保持 unsupported，不能由单边 `MOVE_COMMIT` 推断安全。

设备出现以下任一情况时进入 FAILED/QUARANTINED：

- 无法找到一致的 superblock/checkpoint/control suffix；
- control-log checksum gap 使 ownership 不可证明；
- generation regression 或 double ownership；
- payload 与已 ACK 事实无法调和。

设备重新加入前必须完成 allocator recovery、delete watermark 同步和上层 BookKeeper recovery 判定；不能仅因块扫描可读就恢复 writable。

同一Bookie任一manifest-required device处于FAILED/QUARANTINED、missing、incarnation mismatch或partial migration时，整个Bookie不能注册Segment writable；当前合同不以“剩余设备仍可用”自动缩容。合法移除或replacement必须先由cluster接受新的storage incarnation/device manifest generation，再按RFC-0005 startup/readiness重新建立authority。

## 17. 安全不变量

1. DATA使用前allocation authority已durable；local success还要求本批完整write、覆盖barrier、所在物理stream连续durable前缀及readable publication。
2. 同一 slot generation 不同时属于两个 owner。
3. `FREE_AND_BUMP`以allocation/generation为主语，整个单元无live occupant/待完成写入/旧pin且满足selector/清理条件后才可接受；durable前不可复用，单ledger删除原因不替代整单元证明。
4. locator的generation/instance不匹配时不返回错误owner数据；旧generation残留不进入当前index，也不单独证明unused或媒体损坏，当前分配/依赖无法证明时保持不可写。
5. checkpoint rotation 不得删除恢复当前 authority 所需的唯一 control suffix。
6. allocator authority 全损坏时设备 fail closed，不从 data scan 猜 free list。
7. derived index损坏或删除不改变payload恢复结果；local success依赖可查询定位，index coverage只由真实持久内容证明。热定位接管不等flush，append shard不同步等待索引维护，未覆盖DATA必须可重放。
8. 100k idle ledger 不产生 per-ledger extent 或 block-buffer reservation。
9. shared slab的delete-applied/cursor晚于durable tombstone及可重建清理义务，不依赖physical reclaim；待回收record不可接受新访问，cursor也不证明旧I/O/pin终结或允许reuse。
10. 受校验的ENTRY_DATA/RELOCATION_COPY来源及move/target身份可独立恢复，未成功提交的copy不成为authority；copy completion不发布普通Add定位，MOVE不修改DATA提交位，完整控制历史在index丢失后仍唯一决定当前位置。
11. compaction source停止追加且原DATA已解析；OPEN ledger的旧不可变范围不需ledger seal/fence。同source/generation最多一个正常任务，异常竞争仍条件化；old复用晚于cutover、new-pin阻断、drain、整单元可回收及durable FREE。
12. relocation 不创造新的 local success、AQ 或 ACK。
13. checkpoint through `S` +完整后缀先重演metadata predicate，得到与full history相同的最终selector，再验证当前及未完成操作仍需DATA；已合法退出的MOVE中间字节无需保留或重读，最终DATA坏/控制缺口仍隔离。历史chain可压缩，anti-ABA/retiring及必要绑定事实不丢失，不强制每FREE一次checkpoint。
14. orphan GC 只证明 new location 未承载 authority；logical entry 的既存 local success 不阻止清理 uncommitted copy。
15. conditional orphan free 与迟到 `MOVE_COMMIT` 不能同时成功；cutover 只晚于覆盖自身 sequence 的 durability completion。
16. per-Arena条件命令先合批durable，再按sequence对applied state原子判断/更新；失败是消费sequence的no-op，未apply命令不授予authority，checkpoint只取连续applied cut；FREE提交前quiescence gate不由重启pin=0补认。
17. duplicate externally retried transition只能得到同一durable result或stale/conflict，不产生第二winner或重复generation bump。
18. control log的unknown mandatory/gap/torn阻断其durable-through；restart只按§5.4证明后截断未提交末尾，必需prefix损坏或分类不明保持non-writable。DATA另外遵守§6.1物理批次前缀，不以控制日志sequence或最大完成batch推导DATA成功。
19. selector publish与block-new-old-pin形成同一同步cut；cut后read pin不能落回old location。
20. Arena superblock/format state不能替代old-binary-visible Bookie compatibility fence；任何partial required-device migration都不注册writable。
21. DATA block submitted后header/count/checksum/padding/payload不可修改或追加；后续写入不得覆盖已有成功记录，I/O buffer只在真实I/O终结后复用。
22. 连接/TLS/read及半包先计费，大body累积前按header预留额度，接收→存储交接owner/credit；出队/暂停/取消不提前归还仍持有资源。DATA准入保障提交/定位、回原owner的completion及原属地池归还工作集，满载不循环互等或阻塞event loop/shard/ledger锁。有限控制连接保持传输可达并受同一总账/授权约束；CPU/NUMA/等待策略不改持久身份、回收或I/O终结条件。
23. batch物理durability与逐entry结果分开；单条取消/fence不产生物理缺口，提交后坐标不因timeout视为空，未解析写入由bounded pending或不可写范围保护。
24. 点读独立验证record外层身份及BK CRC，长度/对齐范围有界且pin/selector正确；不因整批大而强制读整批，不以local LAC拒绝必要候选。
25. 首批shared布局及stream/file映射明确；单stream逻辑后缀不缩短共享文件或损害其他stream，offset不定义跨stream顺序。dedicated启用后专属空间、buffer池及文件总量仍有界，共享barrier不表示共享block或独立flush域。
26. Arena FREE增加内部可复用容量，不自动增加filesystem available；DATA预分配和共享filesystem上的SST/控制维护受同一Bookie磁盘总预算约束，低水位先限制新DATA，不能重复预留或侵占维护余量。

## 18. 接受 Gate

除下列既有Gate外，必须完成§5.4三类tail oracle、§6 pool ownership与旧writer I/O终结、§6.1/9 DATA冻结/完整写入/barrier覆盖/乱序完成前缀恢复、§13全生命周期预算、§13.1每Arena耗尽/恢复进展，以及§15两条index恢复路径；均保留独立原始证据。B4/B6及Model C+D还须证明共享L1/L2删除的applied cursor与rejoin不等待physical回收，checkpoint或派生队列丢失不遗漏清理义务，也不提前reuse。

本 RFC 进入 Accepted 前必须：

- [Spike B](spikes/SPIKE-B-allocator-block.md) 达到全部 safety、memory 和 latency Gate；
- [Spike C](spikes/SPIKE-C-no-object-tla.md) 的 Model C 无 counterexample；
- on-disk framing、checksum、alignment 和 compatibility version 冻结；
- checkpoint/control-log recovery 可由自动 crash matrix 重放；
- 已启用的lifetime class与同Arena `MOVE_COMMIT` relocation通过crash、并发move、reader pin及index rebuild测试；dedicated cold/hot promotion启用时另通过B5/B13，首个shared切片不声称覆盖该能力；
- B2/B8/B19证明物理batch与逐entry结果分离、timeout后坐标保护、写错误/unknown暂停及恢复，包含共享文件错误范围和重复sync不能抹除失败；B9/B12/B18验证小record独立点读、损坏length、慢reader/pin与大量hot ledger资源；
- B2/B3/B6/B9证明共享文件S0低offset不完整/S1高offset成功、复用generation 7→8旧合法CRC的恢复与回收；不依赖全文件截断或每次整块清零。B17/B18证明Arena内部空间与filesystem/SST/控制维护预算分层，preallocation低水位与真实临时空间峰值可核验；
- B18入口近满且下游暂停/恢复时，已准入请求有封包与定位资源、最终完成释放，无超额/执行线程阻塞/永久等待；B10/B17覆盖冻结source、OPEN ledger局部搬迁、每source正常任务去重、含padding的成组净收益和无获益时背压；
- B4/B10/B16证明Arena不重复持有Bookie binding/tombstone，shared多ledger及delete/compaction/orphan原因均走whole-allocation FREE条件；单ledger原因、部分move或残留pin不能提前free；
- B2/B9/B16验证正常中断后的完整候选经必要恢复同步才可幂等成功/提供durable证据，并执行重启后再次掉电；B3/B7/B10验证同批竞争ALLOC、durable未apply及apply中途crash、重复FREE和applied checkpoint cut；
- B9/B18验证hot-first后fresh Get与接管/淘汰交错，旧snapshot/iterator/negative-cache不参与当前点读的absence判断，DB I/O不持ledger锁；
- B9/B10及C-MOVE/C-REBUILD验证A→B→C后A/B合法复用、无新checkpoint的恢复与fallback；不追读退休字节，最终C坏或后缀缺失仍隔离。索引/task丢失不提升无成功MOVE的copy，持久来源/内部completion分支可验证且MOVE不覆写DATA；
- B18随固定owner验证安全binding、池初始化/归还及OFF→pin→NUMA→有界spin对照；启用路径的实际mask/页分布、quota/SMT/线程继承和降级有证据，开关优化不改ACK/fence/恢复或buffer生命周期。未验证/无收益的优化保留OFF/SYSTEM/PARK，OFF结果不替代其他组证据；
- B18/A26验证多连接TLS/半包、预留与实际held交接、暂停前pipeline残留和关闭/错误释放；DATA暂停时控制连接仍有界可达、身份/operation校验不放宽；
- current-selector checkpoint、orphan GC、late-commit/free competition 与 durable-through cutover 通过离线 oracle和 foreground p99 Gate；
- conditional apply/result、duplicate/response-loss、bounded waiter/idempotency retention、unknown record和selector/pin竞态通过crash/replay与资源Gate；
- 真实stock old binary compatibility fence由RFC-0005 Gate先行验证；Spike B同时覆盖Round 7 `BKPF1` Cookie sentinel candidate、data-integrity pre-storage-open instrumentation、Cookie auto-stamp、superblock A/B corruption、partial device migration、device-manifest change、migration response loss与rollback禁止条件；candidate失败时必须正式采用new BookieId/new scope fallback；
- 证明 shadow writer 可以与 Classic authority 隔离，失败不会影响 Classic ACK。

即使 Spike 通过，也只解锁 shadow implementation。Segment 成为 ACK authority 仍需要 [RFC-0005](RFC-0005-segment-bookie-state.md) Accepted、独立 canary Gate、回滚合同和 RFC-0001 安装/activation 证据。

## 19. 开放问题

- ArenaControlLog region sizing、segment rotation，以及§13.1前台/维护预算、限流/拒绝/恢复阈值与长期进展数据；
- conditional transition/result的exact Java API、physical record grouping、control-sequence encoding与operation summary packing；
- 线程角色及静态owner/placement边界按RFC-0005 §10.2—10.5；worker/shard比例、服务CPU余量、queue/waiter hard cap、batch/spin阈值与selector/pin具体同步原语由B18冻结；
- §6 `ALLOC_POOL` used/unused识别、返还/转交条件及真实I/O quiescence验证；
- exact block/record bytes、checksum 和 torn-write detector；
- direct I/O API、alignment、buffer ownership 与 kernel/filesystem 约束；
- shared slab lifetime classification、`MOVE_COMMIT` exact packing/batching、selector packing/dedup retention、可选 `MOVE_PREPARE` 与 orphan candidate index；
- checkpoint bytes/page layout、through-sequence encoding、A/B exact superblock protocol 与 prefix-retention policy；
- hot promotion/demotion 是否单向以及阈值；
- multi-device placement、device evacuation、cross-Arena relocation 和 rebuild；
- delete authorization receipt 的本地格式；
- metadata memory accounting 的实现与观测；
- same-scope Bookie/storage compatibility fence（当前BLOCK）、Arena/device exact superblock bytes、format/migration generation、minimum compatible reader/writer、device-manifest encoding，以及upgrade/reverse/wipe/rollback工具与策略；new BookieId/new scope fallback语义由RFC-0005锁定。

这些问题关闭、Spike B/C 通过且 RFC Accepted 前，Segment WAL 保持 P0 Blocked。


2026-10-04，隔离图已接通整allocation全死FREE。先冻结并排空Bookie entry admission，再关闭新文件reservation/read lease；有界真实DATA解析要求全部普通record匹配准确终态tombstone。实际控制force后原owner按前序grant/expected generation判断，成功撤销原grant并bump 1→2，保留退役序号区间及必要Bookie cut供metadata-only重放。旧I/O/原owner未终结和未知force不会释放保护或发布健康CLOSE，fresh generation 0不复占退役slot。真实共享活record拒绝、writer/read quiescence、durable未apply、unknown、rotation与全量/覆盖restart已由当前切片验证。后续[整块复用切片](implementation/local-storage/README.md#整-allocation-复用与健康关闭末端)已接通原allocator refill，准确FREE generation授权及健康CLOSED v2实际末端排除旧残留；同块两轮复用与共享活数据恢复已验证。OPEN仍拒绝；后续[运行期终态清理](implementation/local-storage/README.md#运行期终态索引与坐标回收)已接通准确终态native删除与原owner坐标释放、持久coverage及固定小容量循环。MOVE/compaction、控制历史/SST长期净回收仍待实现。 后续[relocation 内部完成](implementation/local-storage/README.md#relocation-副本内部持久化完成)已接通 copy-only 实际 DATA barrier、连续前缀与原 owner 内部结果；不发布普通定位，MOVE/selector 仍未启用。


2026-10-04 当前后续增量：[受保护的条件 MOVE 与当前位置](implementation/local-storage/README.md#受保护的条件-move-与当前位置)已复用现有全局entry/file排空与CONTROL service，接通实际copy字节验证、最多256行条件durable/apply、Arena selector、旧定位业务读/幂等与metadata-only控制重放。旧coverage不能跳过MOVE后的必要DATA，当前target缺失拒绝恢复；未退役历史allocation仍解析CRC。MOVE后源/orphan FREE、原owner native/coverage维护与部分存活compaction、A/B已复用后的C恢复、长期净容量及生产Gate仍未完成。当前六项真实/固定向量回归及16正向/36指定反例见该切片；此前receipt保持各自历史范围。


2026-10-04 最新后续：[搬迁源与orphan整块回收](implementation/local-storage/README.md#搬迁源与-orphan-的整块回收)已接通受保护当前后继字节验证、FREE v2 selector applied cut、源/orphan及准确T当前COPY退役与原整块generation复用；纯普通T v1字节不变。真实A→B→C、A/B均复用且无新增checkpoint、storage全量与rotation恢复保留C，坏/缺失C和陈旧proof拒绝。当前四项Java/固定向量及17正向/40指定反例保持局部范围；原owner在线native MOVE/coverage、合批victim compaction、长期净容量与生产Gate仍未完成，之前receipt不重绑。


2026-10-04 最新增量：[原owner MOVE索引维护与覆盖恢复](implementation/local-storage/README.md#native-move-索引维护与覆盖恢复)已复用现有checkpoint暂停全部原owner索引、Bookie entry/control及全文件I/O排空、原owner scratch池、256行native更新、flush与A/B coverage发布。当前MOVE目标实际字节校验失败不回退selector；旧native覆盖被拒绝，匹配覆盖恢复仍点读当前必要副本。取消保持保护到实际I/O和全owner终结，页发布失败可继续读并重发。四项新增真实回归及七类271通过/5平台跳过、18正向/44指定反例为当前局部证据；合批victim compaction请求、实际净回收循环、逐allocation在线cut、长期native/control/SST容量、多Arena、OPEN错误恢复和掉电/生产Gate仍未完成。


2026-10-04 当前增量：[同Arena有界合批compaction](implementation/local-storage/README.md#同-arena-有界合批-compaction)已组合显式1..16个同stream source的实际扫描、allocation粒度净收益判断、独立destination、一批COPY/条件MOVE、原owner native覆盖维护及一次CONTROL barrier的合批整块FREE。各v2 FREE绑定自身前序cut，共享保护到全部claimed成员终结；取消/发布失败不提前释放或伪造净回收，后续新operation可完成保留source的回收。真实共享块、零净收益拒绝、两块减一块的净容量、全T无destination、复用恢复及失败/取消重启由当前回归验证。仍为全图暂停/一个COPY/显式组，自动选组、逐allocation在线cut、长期control/SST/文件系统净预算、多Arena、OPEN错误恢复、掉电和生产ACK未完成，步骤9/10和完整Gate不提升。


2026-10-04 当前增量：[同Arena多批compaction](implementation/local-storage/README.md#同-arena-多批-compaction)在既有显式source组内按owner buffer与256行上限划分COPY/MOVE，所有批次完成后统一native覆盖及合批FREE。净成本包括各批framing/padding总和再按destination整allocation向上取整；后批失败保留已成功selector与所有source，不伪造整组回收。40/402坐标的跨批读取、覆盖/全量恢复及source复用、后批force unknown隔离、零净收益和源预算拒绝已由真实路径验证。源snapshot总预算和既有source batch扫描buffer仍有显式上限；自动选组、逐allocation在线cut、长期净容量、多Arena、OPEN错误恢复和生产Gate保持未完成。


2026-10-04 当前选择增量：[固定shard有界回收](implementation/local-storage/README.md#固定-shard-的有界回收选择)按成功ALLOC顺序查看同文件/唯一stream最多16个当前grant；真实DATA测量先于payload捕获，优先直接回收零current live组，最终仍独立whole-allocation后继证明与合批FREE。满容量且无关current live超过源输入预算的实际回收、source generation2复用及17个grant窗口保留年轻共享活数据已验证；既有共享T及多批COPY/MOVE回归改经shard入口。七类283通过/5平台跳过及20正向/51指定反例仅为局部证据。窗口外公平选择、partial任务自动收敛、逐allocation在线cut、长期完整净容量、多Arena、OPEN错误恢复与生产Gate仍未完成。


2026-10-04 当前轮转增量：[有界回收窗口与容量循环](implementation/local-storage/README.md#有界回收窗口轮转与有限容量循环)复用固定shard/file映射，按固定本轮ALLOC上限推进进程内游标；新grant留至下轮，首窗口拒绝后可轮到更晚dead并恢复destination，完成轮次后回访旧T。真实17个满容量grant、后续普通refill、1/4 worker及四shard四轮自动FREE/generation1→5、重启读取已验证。七类285通过/5平台跳过及21正向/53指定反例保持局部范围；跨窗口最优组合、部分任务收敛、维护调度、per-allocation cut、长期control/SST/文件系统完整净预算、多Arena、OPEN错误恢复与生产Gate未完成。


2026-10-04 当前池化增量：[池化分批捕获](implementation/local-storage/README.md#池化分批-compaction-捕获)复用原owner两个buffer逐批点读/复制，不持整组payload；有限计划以既有坐标上限约束，并在每批重新复核source hash、selector、Bookie cut与测量长度。所有COPY/MOVE、native覆盖及真实整块proof后仍才FREE；池不足或后批身份变化返回PARTIAL并保留实际占用。真实40条/三批超过源输入预算、单buffer拒绝及有效CRC源变化/实际控制变化、恢复验证通过。七类287通过/5平台跳过、22正向/55指定反例保持局部范围；原source batch扫描buffer、全graph暂停和单连续destination限制保留，长期完整净容量、多Arena、OPEN错误恢复、掉电/性能及生产Gate未完成。


2026-10-04 当前部分搬迁增量：[满容量保留tail续写](implementation/local-storage/README.md#满容量部分-move-的保留-tail-续写)在新grant无空间时，验证含current COPY的最新同stream grant并保留为目的地，剩余COPY总量须适配其实际空闲tail；只为受保护证明建立最新tail cursor，不回更旧grant，目的地排除FREE。真实PARTIAL后健康重启、1/4 worker、gen1/gen2旧残留tail、21条/两批续写、两源净FREE2 MiB与普通source复用/重启、tail不足及坏字节0COPY已验证。复用新增destination容量为0仍报告实际ID。七类289通过/5平台跳过及23正向/57指定反例保持局部范围；任意partial自动收敛、调度、长期完整容量、多Arena、OPEN错误恢复、掉电/性能及生产Gate仍未完成。

2026-10-04 当前空间准入增量：[前台refill维护容量预留](implementation/local-storage/README.md#前台-refill-的维护容量预留)把最大compaction目的地whole blocks与每DATA文件内部预留绑定，单compaction并发下同文件合批refill共同扣减，超过声明目的地上限在ALLOC前拒绝；预算从已apply grant/FREE重建。真实前台拒绝→COPY/MOVE/native/FREE→净恢复→健康重启/gen2再准入及两block工作集已验证。当前仅内部容量，extent碎片、filesystem/SST/control/checkpoint临时空间、低水位/hysteresis、长期负载与多Arena仍未闭合；不关闭B17、步骤9/10或生产Gate。历史回执保留原绑定。

2026-10-04 当前控制维护增量：[运行期控制日志轮转](implementation/local-storage/README.md#运行期控制日志轮转)已接入显式graph入口、原CONTROL票据/I/O及两个driver排他名额。在Bookie admission与全DATA文件保护下依次完成双owner rotation，保留真实OPEN session/epoch/pool/游标及CLOSE余量；取消只撤回复，保护留到原owner终结，已尝试失败报告UNKNOWN并停止准入。1/4 worker满额后两轮原pool续写、读/幂等/健康重启、真实pin与force暂停/失败验证通过。七类295通过/5平台跳过及25正向/65指定反例为局部证据；自动调度、filesystem/SST/checkpoint临时工作集、长期负载、多Arena与OPEN错误恢复仍未完成，不关闭步骤8/10、B17或生产Gate。

2026-10-04 当前自动恢复增量：[控制容量压力的自动轮转](implementation/local-storage/README.md#控制容量压力的自动轮转)把可fresh-fit的容量拒绝接入既有owner pump和受保护CONTROL rotation；pressure期间暂停新graph准入，已接受entry/I/O/pin排空后再尝试，同步quarantine拒绝也唤醒维护。新journal与原owner终结后恢复原pool续写；超大请求不触发无收益轮转，自动UNKNOWN保留cause并停止准入。真实红/绿、1/4 worker两轮满额/健康restart、单Arena拒绝唤醒及错误验证通过；七类298通过/5平台跳过、26正向/68指定反例为局部证据。提前低水位/一般调度、filesystem/SST/checkpoint临时工作集、长期负载、多Arena与OPEN错误恢复仍未完成，不关闭步骤8/10、B17或生产Gate。
