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

Per-device ArenaControlLog + AllocatorCheckpoint
    local allocation ownership and generation authority

Data Arena
    local payload authority

Footer / RocksDB / cache
    rebuildable derived index
```

RocksDB、footer 或 data scan 不能自行宣布空间空闲。allocator authority 只能来自最新有效 checkpoint 与其后的 control log。

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

候选 record 类型：

```text
ALLOC
ALLOC_POOL
LEDGER_PROFILE_BIND
MOVE_COMMIT
DELETE_TOMBSTONE
FREE_AND_BUMP
CHECKPOINT_BEGIN
CHECKPOINT_COMMIT
DEVICE_STATE
```

每条记录至少有：

```text
controlSequence
recordType
slotOrExtentId
oldGeneration
newGeneration
ledgerId or shardOwner
ledgerInstanceId
semantic predecessor / expected generation
operation identity and generation where externally retried
payloadDigest
checksum
```

具体字段按 record type 裁剪。控制序号和 checksum 必须使 replay 能识别重复、缺口和 torn tail。

### 5.1 Authority 规则

```text
allocator state = latest valid AllocatorCheckpoint
                + valid ArenaControlLog suffix after checkpoint
```

data extent header 可以用于交叉校验和诊断，但不能在 control authority 丢失时通过猜测空闲空间恢复 writable 服务。

如果 checkpoint A/B 和必要 control-log suffix 均无法验证，该设备进入 FAILED/QUARANTINED；不得扫描 data arena 后继续分配。

### 5.2 Relocation selection authority

当前 compaction relocation 只支持 old/new allocation 同属一个可线性化 `ArenaControlLog` authority domain。跨 Arena/device relocation 涉及两个独立 authority，当前明确 unsupported；如要支持，必须直接修改本RFC并重新评审，不能把单边 `MOVE_COMMIT` 扩展成隐式分布式事务。

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

group commit 中，只有明确覆盖该 `controlSequence` 的 durability completion 才允许 locator cutover；batch submission、内存 append 或其他 record 的完成都不够。locator 切换与阻断新的 old-location pin 必须形成一个本地同步 cut：cut 前已取得 old pin 的 reader 可以完成，cut 后的新 reader只能取得 new locator。`durableThrough` 不得跨越 control sequence gap 或 torn record。

### 5.3 Conditional apply 与 durability result

`ArenaControlLog` 对每个 Arena 提供一个 deterministic conditional state-machine order。现有 Classic Journal 的 group force可作为 batching参考，但其 append/fsync callback本身不具备 semantic predecessor、conditional apply、operation replay或state-conflict语义，不能直接冒充此接口。

概念结果至少等价于：

```text
appendConditional(transition)
    -> APPLIED_DURABLE | ALREADY_DURABLE | CONDITION_FAILED
     | STALE | INCOMPATIBLE/CORRUPT
    + operation identity
    + assigned control sequence/range
    + resulting generation/state identity
    + durableThrough
```

predicate 必须在 per-Arena sequencer 对当前 committed/applied state 原子求值；condition failure不改变状态。authority consumer 只能消费完整 log prefix durable through该 transition自身 sequence之后的 durable result，enqueue/admit/内存 append 不是 cutover、free或reuse许可。一个 bounded transition可由一条或有限多条物理 record承载，但必须有明确 all-or-nothing replay；共享 group append/fsync 的相邻 operation仍保留独立 condition/result，不形成通用事务。

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

最低顺序：

```text
1. append ALLOC or ALLOC_POOL with generation and owner
2. make control record durable
3. expose space to WalAppendShard
4. assemble and freeze a DATA batch; write its authorized, previously unused ranges
5. complete all batch writes, then the durability barrier covering those exact writes
6. advance the stream's contiguous physical durable-through, without crossing a gap
7. publish the correct readable locator under the selector/admission gate
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

第一批数据路径采用RFC-0005 §10.1的固定append shard：多个BK entry合并进有界、对齐buffer，使用预先durable分配的pool空间，以真实batch durability完成后逐entry发布可读定位和local success。上述1/2是预分配先决条件，不意味着每条Add同步经过Bookie控制、Arena控制和DATA三条执行队列。refill异步预备并在资源不足时背压；锁内只做短状态变化，不等待磁盘完成。集中对齐复制可以接受，但要计量各层payload复制、分配、线程hop以及每次force覆盖的entry数，不能把每entry force称为已实现group commit。

`ALLOC_POOL`的下一原型必须冻结：`Arena + pool range + owner shard + shard generation + allocation generation`，每个pool内record的used/unused识别方法，以及返还/转交的条件化状态机。未使用不能由“内存计数为0”推断；restart必须结合完整control authority与可恢复DATA framing确定live、unused或unknown，unknown不进入free pool。

pool转交及所有free/reuse先关闭旧writer admission，等待已提交写I/O完成或获得可靠的设备/进程隔离证明，再conditional free/bump并授予新owner。旧completion的generation检查只能防止错误发布，不能阻止已经提交的旧I/O覆盖新owner磁盘字节；timeout、取消future或reader drain都不能单独作为写I/O终结证明。buffer、submission及completion必须携带owner/generation，stale owner/generation的completion不得发布locator或success；仅RPC取消而storage generation仍有效的物理结果按§6.1保留。崩溃后如何终结旧提交者的I/O同样进入真实故障矩阵。

### 6.1 DATA批次的durability与恢复前缀

首个真实文件原型采用“本批完整write completion → 覆盖本批的文件级durability barrier”这一种模式。逐个检查写结果与短写；只有所有范围完整写入后才发barrier，未完成短写、I/O错误、底层I/O取消后结果不确定或durability unknown均不能证明该批durable。单条RPC取消与这个物理判断分开。补写剩余范围仍须符合声明的alignment和错误处理规则，不能重写含既有成功数据的范围。Linux原型可将该模式映射为完整写入后`fdatasync()`，映射及文件系统/设备假设在同一实验manifest冻结；其他等价同步写模式后续单独验证，不叠加重复barrier却漏算成本。

`write completed`、`durability completed`、`readable published`和`local success`是不同事实。`O_DIRECT`不单独提供`O_SYNC/O_DSYNC`的持久化保证；普通write完成（包括普通异步write CQE）不能直接授权成功。文件barrier也不自动保证新目录项durable，文件创建/预分配及所需parent-directory同步在冷路径完成后才公开可用空间。依据[Linux open(2)](https://man7.org/linux/man-pages/man2/open.2.html)和[fsync(2)](https://man7.org/linux/man-pages/man2/fsync.2.html)，实际硬件/I/O路径仍需故障实验。

每份write/barrier completion绑定storage/stream generation、batch sequence及其实际物理范围。一个barrier可覆盖已完整write且明确列入覆盖集合的多个batch；不能因为更晚的fsync返回就把仍在飞行的异步write算作durable。批次跟踪有hard bound，不记录逐entry“ACK已完成”控制日志。

DATA batch只共享物理写入、barrier和buffer生命周期，不是跨ledger/entry事务。batch table分别记录完整写入、实际barrier覆盖与是否进入可恢复前缀；entry的admission、fence/tombstone、取消和callback状态由RFC-0005的pending处理，不能用一个`batch.success`表达二者。物理完成后，同批L1因fence失去成功资格，不能阻止仍合法的L2或把物理batch变回缺口；L1也不能因同批durable跳过权限检查。

物理batch sequence在非空容器封包、冻结并进入提交流程时分配；纯assembling且最终为空的容器不留下恢复必须解释的序号。序号分配后如果submission失败或结果不明，按真实失败边界解析，不能当空容器跳过。submitted后不因单条取消移除record/改写block或提前释放buffer，整个物理结果仍跟踪到终态。有效DATA未向原调用方返回成功也不能自动删除；fence候选与tombstoned数据分别按恢复/删除规则处理，保留同一locator/index的物理位置事实。

首批选择**每个物理append stream的连续前缀恢复和成功发布**。bounded batch table记录乱序completion；只有从已验证起点开始连续的batch均取得durability，才推进内存中的physical durable-through并允许对应entry发布locator/local success。batch 11未durable而12先完成时，12保持等待，不能先ACK再在restart的11缺口截断后缀。该序号属于物理stream，不是ledger entryId或LAC，也不是Arena控制日志sequence；不同ledger可同批，`E>W`不要求本地entryId连续。

允许有界I/O queue depth大于1；物理前缀排序在各stream内，不新增跨shard全局sequencer，但共享文件的barrier成本/错误范围不因此隔离。batch边界、stream lineage与范围发现由§9的可恢复framing和allocator authority给出；physical durable-through可重建，不要求另写持久成功游标。restart从已验证checkpoint/覆盖cut开始扫描活动后缀；已由control authority解释的FREE/retired范围不能被当作未知缺口。仅能截断已证明可丢弃的未成功后缀，required数据损坏或边界无法判定时fail closed，不能仅凭CRC错误或内存watermark丢失猜测无ACK。

首个文件切片在一Arena内为固定append shard各设物理stream，共享预分配DATA文件；各自使用allocator授权的不重叠范围。实验manifest固定shard/stream/file映射、文件增长/rotation及最大文件数、活跃buffer上限，不为每ledger建文件。物理stream是恢复排序单位，不是独立设备或flush域：`fdatasync/fsync`作用于指定文件，不能同步一个应用定义的extent/shard而独立于同文件其他写入。先测共享文件的barrier数量、覆盖集合及等待；以后拆分文件须另给可比证据，本次不新增跨shard flush调度框架。[Linux fsync(2)](https://man7.org/linux/man-pages/man2/fsync.2.html)

发生既定I/O合同内无法解决的写错误或durability unknown时，首版不在线补洞：至少关闭受影响物理stream的新DATA准入，阻止越过失败batch发布后续成功。共享文件错误无法归因到更小范围时，暂停该文件相关stream；影响设备或required authority时沿§16及RFC-0005 non-writable/quarantine处理。batch 11 unknown、12已完成时，不因调用超时丢弃12，不用跳号、新stream或覆盖旧范围绕过11，全部按现有恢复/generation规则解析，不自动跨设备迁移。

等待所有已提交I/O实际终结后，可释放确实没有使用者的buffer；未解析坐标仍由pending或已经接管该范围的不可写gate保护，关闭准入后不允许无限增长异常pending/waiter。重启默认在相关控制/allocator/DATA恢复解析前不开放这些坐标，不能仅因易失pending消失而再接收不同payload。请求timeout不等于I/O失败，更不等于未写入；direct I/O错误可能已部分修改范围，须按不一致数据处理。[Linux write(2)](https://man7.org/linux/man-pages/man2/write.2.html)

`fdatasync()`错误可能报告此前写回失败；再次sync返回成功不是对先前全部DATA安全的证明。按上述存储错误恢复流程解析受影响范围，不以最后一次返回值清除unknown/失败状态。该保守策略由本原型选择，实际错误范围仍需故障实验核验；不为此引入逐entry成功日志或正常路径额外fsync。[Linux fsync(2)](https://man7.org/linux/man-pages/man2/fsync.2.html)

涉及既有成功数据的compaction同样须保证new DATA在恢复可发现边界内，才允许`MOVE_COMMIT`、selector切换或释放旧位置。以后若要跨缺口独立发布，须先接受独立块发现/恢复算法；本次不并行建设第二套模式。分别报告write wait、durability wait、physical-prefix wait和locator wait，不将同stream排队归为NVMe延迟。

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

首批必须能独立校验一个record，不扫描整个大batch。固定envelope/header完整性覆盖instance、allocation/generation、record length及必要存储坐标；内层BK CRC32C覆盖其自身metadata与应用payload。使用现有候选的record级校验方案，不以正确BK CRC替代外层身份校验，也不增加第三套逐entry密码学hash。整块checksum可用于扫描/恢复/诊断，不能成为单entry点读的唯一验证方法。仅匹配物理offset不足以防止ABA。

读取先按现有locator取得allocation/generation pin并复核selector与权限，再使用匹配且已验证的cache，或发起覆盖目标record的必要对齐I/O。locator的offset/length须检查非负、溢出、声明最大record及allocation边界后才分配大buffer；损坏length不能扩大成无界读取。外层固定header有界读取并验证后才信任其长度，record自身CRC/坐标仍需核对。对齐范围不能越过合法allocation，布局必须提供合法padding/读取路径，不能靠越界读取凑alignment。

同block相邻读可以有界合并，不等待未来请求或引入网络Range协议；孤立点读及时发起。返回目标entry后，buffer/pin随最后使用者和真实I/O终结释放；小slice长期占住大父buffer时可受控复制已验证小范围，及时释放不再依赖的父buffer/pin。缓存、父buffer、副本和pin全计入§13预算，不为形式零复制阻塞回收。示例1 MiB batch内读1 KiB若强制整批，读取bytes/返回bytes为1024；这是布局反例计算，非实测结果。

exact bytes、外层record校验算法、commit marker和direct-I/O alignment由Spike B数据决定；独立record验证这一要求不再是whole-block checksum可替代的开放选择。

**DATA提交后冻结：** block由assembling转为submitted前完成header、record count、checksum和padding；此后内存内容及物理写入范围不可追加修改，不更新header/footer，不以read-modify-write重写包含既有成功记录的对齐块。下一批取得另一个有界pool buffer和新的、已授权未使用范围；旧buffer等真实I/O终结才可复用，旧物理范围只有按§10 durable free/generation bump后才可另用。控制日志checkpoint/superblock仍按自身发布协议，不套用本DATA规则。

最大等待从最老请求计算，低负载到期即padding到声明alignment再提交。例如4 KiB对齐范围已保存并确认1 KiB数据，不能为了追加下一条数据重写该4 KiB；同generation不防后次torn write破坏旧成功数据。4 KiB仅为示例，alignment与padding预算由manifest冻结并计入磁盘写放大。

framing还须能恢复§6.1的stream identity/generation、batch边界、物理sequence及范围映射，不靠易失batch table或最大ledger entryId定位后缀。多个block属于同一batch时，其完整性和barrier覆盖必须全部可判定；缺block/坏边界不能假装完整。不得把prototype framing写成已接受stable format。

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
7. after whole-allocation reclaim conditions hold, durable FREE_AND_BUMP(oldGeneration, newGeneration)
8. expose new generation to allocator and record physical reclaim result
```

terminal tombstone由RFC-0005的Bookie级route/control authority持有，allocator消费该事实，不要求为推进cursor在每个Arena再造一份tombstone日志。删除准入关闭后旧locator也不能获得新pin；异步派生清理不代替这个gate。cleanup从tombstone、完整allocator/current-selector authority及有效DATA重建，cursor既不授权reuse也不证明强访问屏障完成。

候选原子控制记录：

```text
FREE_AND_BUMP {
    slotId
    oldGeneration
    newGeneration
    oldLedgerInstanceId
    deleteRequestId
}
```

该记录 durable 前，空间不得用于新 owner。重复 delete/free 必须幂等；instance 或 oldGeneration 不匹配必须拒绝。

对于 shared slab：

- logical tombstone 立即让目标 ledger record 不可见；
- dead accounting 在 authority 下可重建；
- block 未全死时不执行 slot free；
- compaction 复制 live record 时，必须按以下顺序执行：

```text
1. durable ALLOC/ALLOC_POOL for the new allocation
2. copy full payload identity; make new DATA durable and recoverably discoverable under §6.1
3. append conditional MOVE_COMMIT(expectedOld -> new)
4. make MOVE_COMMIT durable                 # authority cutover
5. atomic publishNewSelectorAndCloseOldPinAdmission()
6. drain pre-cut readers/pins and quiesce old-allocation writer I/O
7. only when every live record in the old allocation is moved/dead:
   append durable FREE_AND_BUMP
8. expose the bumped generation
```

一个 record move 完成不等于整个 shared block 可 free。`MOVE_COMMIT` 不产生新的 BookKeeper local success、AQ 或 ACK，只保持既有 payload authority。多个有界 move record 可以共享 control-log group-commit barrier；locator cutover 必须晚于覆盖自身 control sequence 的 durability completion，不要求每个 moved entry 独立 control fsync，也不能迫使 foreground Add 等待额外 relocation barrier。

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

数据或 derived-index checkpoint 可以丢失并重建；allocator authority 不能全部丢失。

control-log 旧段的唯一合法回收顺序：

```text
1. choose a complete applied control cut S
2. freeze/COW allocator + current relocation selectors at S
3. write bounded checkpoint chunks
4. fsync and verify checkpoint content identity
5. append/durable CHECKPOINT_COMMIT(generation, S, identity)
6. update and fsync the inactive superblock
7. atomically select the active superblock generation
8. verify fallback checkpoint/suffix dependencies
9. only then reclaim a prefix covered by surviving authority
```

如果步骤中途 crash，restart 必须选择最后一个完整、可验证且依赖 suffix 仍存在的 generation。不能只按最大 generation number 选择损坏 checkpoint。

inactive→active superblock publication必须同时校验相同storage incarnation、Arena identity、format/mandatory features、device-manifest generation与migration generation；response loss后重读两份superblock和committed checkpoint解析同一generation，不创建新generation猜测成功。superblock不能覆盖或auto-restamp RFC-0005的Bookie/storage compatibility fence，也不能被用作“旧binary会看见”的假设。

checkpoint through `S` 必须完整保存 allocation ownership/generation、free/reusable/retiring state、current authoritative selector、old-source retirement state、new-old-pin gate state、whole-allocation reclaimability，以及拒绝 stale predecessor/operation 所需的 winning generation 或等价 anti-ABA fence。它是 ArenaControl authority 的 compact representation，不是 derived locator。

完整历史 move chain 不是必需：已经由 current selector 与 durable free 完全取代、且不再存在可混淆副本的历史可以压缩；old source 尚未退休时必须保留 current selector/retiring gate，live process 中 cut 前已存在的 volatile readers 仍按 runtime drain。rotation 不得删除唯一 authority；旧 A/B checkpoint 仍作为 corruption fallback 时，其必要 suffix 不能先删。

checkpoint 不持久化 individual reader、future、buffer reference 或 pin history。runtime reader/pin tracking 可以是 bounded volatile state；process crash 后旧进程的 volatile pins 不作为 durable history 继承，但 restart 必须先恢复 selector/retiring gate，再重新判断 source 是否满足 free 条件。

## 12. Restart 与 crash consistency

本节启动顺序只在RFC-0005的Bookie/storage compatibility fence已经验证、且尚未开放任何writer/registration之后执行：

1. 校验Bookie storage incarnation与完整required-device manifest；
2. 对每个required Arena读取并验证 superblock A/B、Arena identity、format/mandatory features与migration generation；
3. 选择最高的完整 committed checkpoint generation 及其 through-sequence `S`；
4. replay sequence `> S` 的完整、连续、校验通过control-log suffix；仅截断§5.4可证明未提交的物理末尾，其余分类保持non-writable；
5. 重建 allocated/free/generation/device state；
6. 扫描已授权 active data tail，验证 block framing；
7. 重建 ledger directory 与 derived index；
8. 对无法证明 ownership 或 payload durability 的对象 fail closed；
9. 全部required Arena完成校验前Bookie保持RECOVERING/READ_ONLY，之后仍须完成RFC-0005 local route/delete/registration readiness才可writable。

suffix 出现 sequence gap、必要 record 缺失或 checkpoint content identity 无法验证时 fail closed。不得用更大的物理 generation、mtime 或 data scan跨过 authority gap。

upgrade/migration不要求跨device transaction：每个Arena按同一Bookie migration generation写入prepared/format-ready事实；只有全部required devices匹配时Bookie级readiness才可前进。任一边界crash、部分device完成或unknown mandatory feature都保持non-writable并重试同一generation。device移除必须由cluster-authorized新storage incarnation/device-manifest generation完成。存在Segment payload/control authority后能否rollback由RFC-0005 negative-proof gate决定，allocator不得因old binary可打开目录而宣称兼容。same-scope candidate未通过前，本节只允许isolated format prototype；不得把candidate superblock bytes发布为stable on-disk contract，也不得污染可继续运行的Classic scope。

必须注入的 crash 边界：

- ALLOC write 前后；
- control-log durability 前后；
- DATA write 中和 durability barrier 前后；
- local ACK 前后；
- DELETE_TOMBSTONE 前后；
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

总资源账本还必须覆盖Bookie route/credential/fence state、allocation/pool/slot state、current selector/retirement/anti-ABA state、pending Add/grant/waiter、block buffers、checkpoint working set，以及derived index/cache。分别报告随ledger、entry、allocation、shard和pending operation增长的项；`1 KiB/idle ledger`不能替代总预算。

每shard的同一套预算覆盖准入、待合批、已提交I/O、等待physical prefix/locator及响应阶段；request count、源/对齐bytes、inflight batches和duplicate waiters分别有上限。出队或移交future不释放credit，真实资源释放或转移到另一已计费owner后才归还对应份额；复制期间源与目标同时持有的bytes都计入。断连/取消而I/O仍在进行时保留其buffer/inflight费用，不能留下队列之外无上限的任务。复用请求对象，避免每层重新包装大对象。

§15的热定位、异步index batch/队列、查询已可见但尚未flush的memtable及相关native/cache内存也计入总资源预算。local success或热定位淘汰不等于这些资源消失；转交实际可查询且已计费的owner后才归还原credit。索引stall耗尽预算时背压后续新DATA，fence/tombstone仍保留控制处理容量；不能以无界索引积压隐藏吞吐瓶颈。

同批物理buffer费用不随某条entry取消而归零；未解析写入的坐标保护按§6.1转交不可写范围后才收缩异常pending。账本同时覆盖点读缓存、对齐读buffer、大父buffer及小副本、pin数量/时长、DATA文件数与后续dedicated池。大量hot ledger或慢reader不能突破总量，也不能通过暂停回收改善短期p99。

最老请求的单调时钟deadline不被新到达重置。最大可接受entry必须能走声明的batch/buffer路径：普通block放不下时使用同一总预算约束的有界大记录批次，或在明确size/capability检查处拒绝；不能永久堵在队首。wire frame、BK entry和存储record/alignment开销的上限关系写入manifest，不因扩大临时buffer突破总预算。

重复坐标需要读盘核对时交给已有有界读取执行资源，不在append shard主循环同步等盘；正常新写不因此多一次索引I/O。连接/ledger只保留必要有界份额，控制持续热ledger的独占；fence/tombstone等撤权保留执行槽位、buffer及控制持久化预算，DATA满额不能永久阻止关闭DATA准入。超额返回明确可重试背压，不新增通用公平调度平台。

### 13.1 每Arena保留空间与进展

前台分配不得耗尽compaction目标、checkpoint/control-log rotation、tombstone/FREE及故障恢复所需空间。下一原型为每Arena独立划分前台预算与维护保留预算；Bookie级控制日志也保留其checkpoint/terminal transition预算。跨Arena relocation未支持时，不能用另一Arena的空闲量掩盖当前Arena无法回收。

运行前锁定可用whole blocks、control/checkpoint余量、最大一次move/checkpoint工作集、并发维护上限、dead bytes/debt上限及各阈值。保留量由最坏一次有界维护步骤及其并发需求推导，不用未经测试的固定百分比代替。状态顺序为：

```text
NORMAL -> THROTTLED -> REJECT_NEW_DATA -> MAINTENANCE_ONLY
    -> RECOVERING/READ_ONLY if the next required durable step cannot be proven
```

阈值必须保证拒绝新数据发生在维护预算被侵占之前；maintenance仍可执行已预算的move、FREE、tombstone和checkpoint，并有最低调度份额。低水位恢复与hysteresis、restart后的保留预算恢复、ENOSPC结果和无法继续时的诊断均须显式实现；不在full-disk后无限排队。

进展Gate限定在manifest声明的有界live set、受控admitted写入速率和可用维护I/O下：长期创建/写入/删除之后，dead bytes与compaction debt不持续增长，并在停止新写后于锁定时间内排空到目标水位。超过可持续负载时要求有界拒绝和恢复路径，不承诺无限写入。短期p99达标而维护长期饥饿不能PASS。

## 14. Reclaim 能力分级

| 布局 | logical delete | physical reclaim | compaction |
| --- | --- | --- | --- |
| Dedicated extent | tombstone 后立即不可见 | reader drain + durable free 后按 extent 回收 | 通常不需要 |
| Shared cold slab | tombstone 后立即不可见 | whole block 全死后回收 | 部分 block 需要低优先级 compact |

性能报告必须分别展示 logical deletion latency、physical bytes reclaimed、pending dead bytes 和 compaction debt。

delete-applied/catch-up延迟另列，不以物理回收完成作为cursor条件；shared slab仍有live record、reader pin或旧I/O时，applied可完成而physical保持pending。

## 15. Derived index

RocksDB可保存entry locator、ledger directory和tail summary，但必须：

- 不参与 payload ACK authority；
- 不参与 allocator ownership authority；
- key/value 带 format、instance 和 generation；
- 全库删除后可从 control log + data arena 重建；relocation winner 只能由 committed checkpoint current selector + complete conditional `MOVE_COMMIT` suffix 决定，checkpoint selector 必须可证明由此前完整 control history 产生，不能由 RocksDB、mtime、最大物理 generation 或 data scan 猜测；
- stale generation/selector locator在读取时被再次校验，并按当前authority重新解析，不能将落后的缓存项当作payload丢失；
- rebuild/compaction 有 foreground QoS 和 admission control。

**运行时可读发布与异步维护：** 首批写路径固定为：

```text
DATA enters its recoverable contiguous durable prefix
    -> publish locator in the existing bounded hot handle/index
    -> check each entry's authority and publish eligible local success
asynchronously:
    -> apply bounded batches to the derived index
    -> periodically persist the index contents
    -> durably publish a coverage checkpoint matching those persisted contents
```

local success之前目标entry必须可被当前点读正确定位；已有有界热定位结构可以承担该事实，不要求本次RocksDB Put/WriteBatch或flush完成。正常新写不增加同步索引读，也不增建一套独立存储系统。append shard不在主循环中同步调用/等待可能阻塞的RocksDB写入、flush或compaction；异步执行资源及积压复用§13统一预算，满额后背压后续准入。

`sync=false`不能证明调用不会阻塞：flush/compaction追不上时，RocksDB会延缓或停止写入调用。该成本须留在有界索引维护路径并计量，不能只用“没有index fsync”描述前台开销。依据[RocksDB Write Stalls](https://github.com/facebook/rocksdb/wiki/Write-Stalls)。

热定位只有在另一条已计费、实际query-visible的定位路径接管后才可淘汰；先完成查询可见的接管，再删除旧热记录，立即点读不得出现空窗。接管不强制等待flush：运行时查询可见与重启持久覆盖分开，尚未覆盖的DATA在crash后重放。不能把flush延迟变成每条热定位的强制驻留条件；异步队列、memtable及热结构的实际占用仍全部有界。

**持久覆盖与恢复：** DATA physical durable-through和index persisted coverage cut独立维护。后者只覆盖相关index更新已完整持久化的连续物理范围，并绑定storage incarnation、Arena/stream、index format/generation及解释这些映射所需的control/selector cut；不能从最大entryId、最后一次Put成功、最大异步完成序号或查询可见水位推导。

首批纯派生RocksDB索引选择`disableWAL=true`，前置是实现并验证本节覆盖checkpoint。关闭WAL后的写入可能在进程crash后丢失，Put/WriteBatch返回不代表持久覆盖，见[RocksDB Basic Operations](https://github.com/facebook/rocksdb/wiki/Basic-Operations)。此选择不作用于Bookie控制日志、ArenaControlLog或其checkpoint，不能关闭权威状态的持久化。

一次覆盖发布先选定有界update cut，确认到该cut的必要索引更新完成，再等待实际覆盖这些内容的flush持久化完成，最后沿既有checkpoint发布协议durable发布对应coverage marker。marker不能仅放入关闭WAL的未flush memtable后就用于跳过扫描；crash发生在flush后、marker发布前时，只能保守多重放。API、RocksDB版本、cut/marker编码与真实flush证据在同一实验manifest锁定，不凭一个异步回调猜覆盖。首批无需主动拆分更多column family；若使用多个CF，必须证明所有相关内容一致持久化后才推进cut，不能仅flush locator就将未flush的其他恢复信息算入。[RocksDB Atomic Flush](https://github.com/facebook/rocksdb/wiki/Atomic-flush)可用于此条件，但不是新增CF或全局索引事务的理由。

原始Add、MOVE后索引更新与删除清理按坐标/selector代际保持必要顺序，陈旧异步update须被丢弃或从当前authority重新生成，不能覆盖新selector或复活tombstone后的定位。点读发现派生locator陈旧时重新解析当前权威selector，按§9重新pin/复核；暂不能解析则有界重试或not-ready，不能把旧locator失败当成确定NoSuchEntry。覆盖声明也必须纳入相应已完成的更新顺序，不能用旧Add回调越过尚未解析的MOVE/delete。

coverage checkpoint仅缩短DATA重建扫描，不授权删除DATA、不替代allocator ownership、不决定relocation winner。完整allocator/current-selector/tombstone authority及其必要控制后缀仍按各自checkpoint恢复；index coverage不能省略后续MOVE/delete控制重放。

点读复用上述locator和§9独立record校验路径；confirmed边界由客户端、恢复候选范围由coordinator按RFC-0005 §8判定，Bookie不以陈旧local LAC截断合法物理点读。report实际读取bytes/返回bytes、每点读I/O数、cache命中、copy/父buffer持有及pin时长；批量写入大小不自动决定点读大小，未验证覆盖继续返回not-ready/unknown而非确定absence。

恢复必须实现并分别测量两条路径：

| 路径 | 必须扫描和验证的范围 |
| --- | --- |
| 正常restart | 仅跳过由有效持久index checkpoint证明已覆盖的DATA；验证实际索引、storage/stream/index generation与连续cut，对未覆盖且已授权DATA按§6.1物理stream前缀扫描必要tail，不用Put成功、最大completion或ledger entryId越过缺口 |
| 全部derived index丢失或覆盖证明无效 | 从完整allocator/current-selector authority枚举所有需要重建的live allocation和有效DATA范围；首批覆盖active/sealed shared及relocated数据，dedicated启用后必须同样覆盖；不能只扫描active tail |

两条路径均报告扫描bytes/I/O、重建entry/locator数量、heap/direct/native峰值、到read-only/可写的时间及前台竞争。未验证范围不宣称确定absence；重建可以分批提供已验证读能力，但其覆盖与not-ready语义须冻结。RocksDB全删不会删除allocator/current-selector authority；这些持久映射的空间、写放大与重启成本必须计入账本。

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
3. `FREE_AND_BUMP` durable 前旧 generation 不可复用。
4. locator 的 generation/instance 不匹配时读取失败，不返回新 owner 数据。
5. checkpoint rotation 不得删除恢复当前 authority 所需的唯一 control suffix。
6. allocator authority 全损坏时设备 fail closed，不从 data scan 猜 free list。
7. derived index损坏或删除不改变payload恢复结果；local success依赖可查询定位，index coverage只由真实持久内容证明。热定位接管不等flush，append shard不同步等待索引维护，未覆盖DATA必须可重放。
8. 100k idle ledger 不产生 per-ledger extent 或 block-buffer reservation。
9. shared slab的delete-applied/cursor晚于durable tombstone及可重建清理义务，不依赖physical reclaim；待回收record不可接受新访问，cursor也不证明旧I/O/pin终结或允许reuse。
10. 未 commit 的 relocation copy 永远不能成为 authoritative；durable `MOVE_COMMIT` 在 derived index 丢失后仍唯一选择 new location。
11. old allocation 的复用晚于 move cutover、new-pin 阻断、reader drain、whole-allocation reclaimability 与 durable `FREE_AND_BUMP`。
12. relocation 不创造新的 local success、AQ 或 ACK。
13. checkpoint through `S` + complete suffix `>S` 与完整 control history 得到相同 current selector；历史 chain 可压缩，anti-ABA/retiring state 不得丢失。
14. orphan GC 只证明 new location 未承载 authority；logical entry 的既存 local success 不阻止清理 uncommitted copy。
15. conditional orphan free 与迟到 `MOVE_COMMIT` 不能同时成功；cutover 只晚于覆盖自身 sequence 的 durability completion。
16. per-Arena predicate 对 committed/applied state 原子求值；condition failure不改变authority，pending/admitted append不授予cutover/free/reuse。
17. duplicate externally retried transition只能得到同一durable result或stale/conflict，不产生第二winner或重复generation bump。
18. control log的unknown mandatory/gap/torn阻断其durable-through；restart只按§5.4证明后截断未提交末尾，必需prefix损坏或分类不明保持non-writable。DATA另外遵守§6.1物理批次前缀，不以控制日志sequence或最大完成batch推导DATA成功。
19. selector publish与block-new-old-pin形成同一同步cut；cut后read pin不能落回old location。
20. Arena superblock/format state不能替代old-binary-visible Bookie compatibility fence；任何partial required-device migration都不注册writable。
21. DATA block submitted后header/count/checksum/padding/payload不可修改或追加；后续写入不得覆盖已有成功记录，I/O buffer只在真实I/O终结后复用。
22. 请求的实际bytes、inflight和waiter覆盖其完整生命周期，出队/取消不提前归还仍持有的credit；最老合批deadline、大entry路径、异步重复读及fence/tombstone控制容量受§13同一预算约束。
23. batch物理durability与逐entry结果分开；单条取消/fence不产生物理缺口，提交后坐标不因timeout视为空，未解析写入由bounded pending或不可写范围保护。
24. 点读独立验证record外层身份及BK CRC，长度/对齐范围有界且pin/selector正确；不因整批大而强制读整批，不以local LAC拒绝必要候选。
25. 首批shared布局及stream/file映射明确；dedicated启用后专属空间、buffer池及文件总量仍有界，共享barrier不表示共享block或独立flush域。

## 18. 接受 Gate

除下列既有Gate外，必须完成§5.4三类tail oracle、§6 pool ownership与旧writer I/O终结、§6.1/9 DATA冻结/完整写入/barrier覆盖/乱序完成前缀恢复、§13全生命周期预算、§13.1每Arena耗尽/恢复进展，以及§15两条index恢复路径；均保留独立原始证据。B4/B6及Model C+D还须证明共享L1/L2删除的applied cursor与rejoin不等待physical回收，checkpoint或派生队列丢失不遗漏清理义务，也不提前reuse。

本 RFC 进入 Accepted 前必须：

- [Spike B](spikes/SPIKE-B-allocator-block.md) 达到全部 safety、memory 和 latency Gate；
- [Spike C](spikes/SPIKE-C-no-object-tla.md) 的 Model C 无 counterexample；
- on-disk framing、checksum、alignment 和 compatibility version 冻结；
- checkpoint/control-log recovery 可由自动 crash matrix 重放；
- 已启用的lifetime class与同Arena `MOVE_COMMIT` relocation通过crash、并发move、reader pin及index rebuild测试；dedicated cold/hot promotion启用时另通过B5/B13，首个shared切片不声称覆盖该能力；
- B2/B8/B19证明物理batch与逐entry结果分离、timeout后坐标保护、写错误/unknown暂停及恢复，包含共享文件错误范围和重复sync不能抹除失败；B9/B12/B18验证小record独立点读、损坏length、慢reader/pin与大量hot ledger资源；
- current-selector checkpoint、orphan GC、late-commit/free competition 与 durable-through cutover 通过离线 oracle和 foreground p99 Gate；
- conditional apply/result、duplicate/response-loss、bounded waiter/idempotency retention、unknown record和selector/pin竞态通过crash/replay与资源Gate；
- 真实stock old binary compatibility fence由RFC-0005 Gate先行验证；Spike B同时覆盖Round 7 `BKPF1` Cookie sentinel candidate、data-integrity pre-storage-open instrumentation、Cookie auto-stamp、superblock A/B corruption、partial device migration、device-manifest change、migration response loss与rollback禁止条件；candidate失败时必须正式采用new BookieId/new scope fallback；
- 证明 shadow writer 可以与 Classic authority 隔离，失败不会影响 Classic ACK。

即使 Spike 通过，也只解锁 shadow implementation。Segment 成为 ACK authority 仍需要 [RFC-0005](RFC-0005-segment-bookie-state.md) Accepted、独立 canary Gate、回滚合同和 RFC-0001 安装/activation 证据。

## 19. 开放问题

- ArenaControlLog region sizing、segment rotation，以及§13.1前台/维护预算、限流/拒绝/恢复阈值与长期进展数据；
- conditional transition/result的exact Java API、physical record grouping、control-sequence encoding与operation summary packing；
- sequencer/stripe线程布局、queue/waiter hard cap、batch size/wait阈值与selector/pin具体同步原语；
- §6 `ALLOC_POOL` owner/shard generation的exact packing、used/unused识别及真实I/O quiescence验证；
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
