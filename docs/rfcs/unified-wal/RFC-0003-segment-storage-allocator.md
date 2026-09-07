# RFC-0003：Segment Storage、ArenaControlLog 与冷热混合 Allocator

> 状态：**Proposed / P0 Blocked**<br>
> 依赖：[RFC-0001](RFC-0001-profile-capability-install.md)；ACK authority 另依赖 [RFC-0005](RFC-0005-segment-bookie-state.md)<br>
> 验证：必须通过 [Spike B](spikes/SPIKE-B-allocator-block.md) 与 [Spike C](spikes/SPIKE-C-no-object-tla.md)<br>
> 解锁对象：Segment shadow writer；不直接解锁 Segment ACK authority

## 1. 摘要

本 RFC 为 Segment WAL 定义本地 authority 分层和 allocator crash-consistency 骨架。核心修正有两项：

1. 增加独立、不可随 data extent 回收的 `ArenaControlLog`，作为空间 ownership 与 generation 的权威；
2. 删除“每个 ledger 创建时至少拥有一个 8 MiB dedicated extent”的不变量，改为冷 ledger 共享 slab、热 ledger 使用 dedicated extent。

本文不冻结最终 on-disk bytes、extent 阈值或 direct-I/O 实现。任何正式编码都必须晚于 Spike B 的否证结果和本 RFC Accepted。

## 2. 范围

本 RFC 负责：

- NVMe/WalArena 的物理 authority 区域；
- `ArenaControlLog`、allocator checkpoint 与 superblock 切换；
- allocation、data ACK、delete、free、generation bump 与 reuse 顺序；
- shared cold slab 和 dedicated hot extent；
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
    └── Dedicated Extents
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
4. write DATA into authorized space
5. complete DATA durability barrier
6. publish the correct readable locator under the selector/admission gate
7. allow local Bookie success to participate in quorum ACK
```

核心不变量：

```text
local durable Add success
    => durable allocation authority existed before DATA use
    && DATA durability barrier completed
    && same-coordinate identity and readable location satisfy RFC-0005
```

为避免每个 Add 增加 control-log fsync，allocator 应提前批量分配：

- shard-owned free block pool；
- shared slab block pool；
- dedicated extent pool。

pool refill 通过 control-log group commit；pool 内空间的使用仍必须有可恢复的 block/record framing，但不要求每条 Add 写一条 allocator fsync。

`ALLOC_POOL`的下一原型必须冻结：`Arena + pool range + owner shard + shard generation + allocation generation`，每个pool内record的used/unused识别方法，以及返还/转交的条件化状态机。未使用不能由“内存计数为0”推断；restart必须结合完整control authority与可恢复DATA framing确定live、unused或unknown，unknown不进入free pool。

pool转交及所有free/reuse先关闭旧writer admission，等待已提交写I/O完成或获得可靠的设备/进程隔离证明，再conditional free/bump并授予新owner。旧completion的generation检查只能防止错误发布，不能阻止已经提交的旧I/O覆盖新owner磁盘字节；timeout、取消future或reader drain都不能单独作为写I/O终结证明。buffer、submission及completion必须携带owner/generation，late completion不得发布locator或success。崩溃后如何终结旧提交者的I/O同样进入真实故障矩阵。

## 7. Shared Cold Slab

低流量 ledger 不拥有固定 extent，也不拥有专属 I/O block buffer。

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
- 多个冷 ledger 共享 block write 和 durability barrier；
- ledger state 只保存 locator/tail/inflight 等小型元数据；
- block record header 必须携带 ledgerId、instance、entryId、length、checksum 和 generation identity。

代价：

- 删除一个冷 ledger 不能立即回收仍含其他 live record 的 block；
- 需要 dead-record accounting；
- 仅当 whole block 全死时直接回收；
- 否则可使用低优先级 compaction；
- allocator 应按 lifetime class 分组，降低冷热混合带来的碎片。

因此，Segment 不能宣称全局“零 compaction whole-ledger reclaim”。

## 8. Dedicated Hot Extent

ledger 达到任一经 Spike 冻结的阈值后，后续写入可晋升到 dedicated extent：

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
payloadChecksum or per-record checksum
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

读取时必须先验证 generation、ledger instance 和 record checksum。仅匹配物理 offset 不足以防止 ABA。

exact bytes、checksum 算法、commit marker 和 direct-I/O alignment 由 Spike B 数据决定。

## 10. Delete、Free 与 Reuse

集群级 delete authorization 由 RFC-0004 提供。本地合法顺序：

```text
1. verify durable cluster/local delete authorization
2. reject new reads/writes for ledger instance
3. drain readers/pins and terminate or reliably isolate old writer I/O
4. append durable DELETE_TOMBSTONE
5. invalidate cache, locator and derived index
6. append durable FREE_AND_BUMP(oldGeneration, newGeneration)
7. expose new generation to allocator
```

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
2. copy full payload identity and make new DATA durable
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
- active block buffers 为 `O(shards)`。

所有测量必须区分 heap、direct memory、native allocator 和 RocksDB block cache，不能只报告 Java object shallow size。

总资源账本还必须覆盖Bookie route/credential/fence state、allocation/pool/slot state、current selector/retirement/anti-ABA state、pending Add/grant/waiter、block buffers、checkpoint working set，以及derived index/cache。分别报告随ledger、entry、allocation、shard和pending operation增长的项；`1 KiB/idle ledger`不能替代总预算。

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

## 15. Derived index

RocksDB可保存entry locator、ledger directory和tail summary，但必须：

- 不参与 payload ACK authority；
- 不参与 allocator ownership authority；
- key/value 带 format、instance 和 generation；
- 全库删除后可从 control log + data arena 重建；relocation winner 只能由 committed checkpoint current selector + complete conditional `MOVE_COMMIT` suffix 决定，checkpoint selector 必须可证明由此前完整 control history 产生，不能由 RocksDB、mtime、最大物理 generation 或 data scan 猜测；
- stale generation locator 在读取时被再次校验；
- rebuild/compaction 有 foreground QoS 和 admission control。

恢复必须实现并分别测量两条路径：

| 路径 | 必须扫描和验证的范围 |
| --- | --- |
| 正常restart | 验证仍有效的checkpoint/footer/derived-index覆盖证明，只对未覆盖且已授权的DATA范围扫描必要tail |
| 全部derived index丢失或覆盖证明无效 | 从完整allocator/current-selector authority枚举所有需要重建的live allocation和有效DATA范围，包含sealed/dedicated/shared数据；不能只扫描active tail |

两条路径均报告扫描bytes/I/O、重建entry/locator数量、heap/direct/native峰值、到read-only/可写的时间及前台竞争。未验证范围不宣称确定absence；重建可以分批提供已验证读能力，但其覆盖与not-ready语义须冻结。RocksDB全删不会删除allocator/current-selector authority；这些持久映射的空间、写放大与重启成本必须计入账本。

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

1. DATA 使用或 local success 前，allocation authority 已 durable。
2. 同一 slot generation 不同时属于两个 owner。
3. `FREE_AND_BUMP` durable 前旧 generation 不可复用。
4. locator 的 generation/instance 不匹配时读取失败，不返回新 owner 数据。
5. checkpoint rotation 不得删除恢复当前 authority 所需的唯一 control suffix。
6. allocator authority 全损坏时设备 fail closed，不从 data scan 猜 free list。
7. derived index 损坏或删除不改变 payload 恢复结果。
8. 100k idle ledger 不产生 per-ledger extent 或 block-buffer reservation。
9. shared slab 删除在 physical reclaim 前仍保持目标 record 不可见。
10. 未 commit 的 relocation copy 永远不能成为 authoritative；durable `MOVE_COMMIT` 在 derived index 丢失后仍唯一选择 new location。
11. old allocation 的复用晚于 move cutover、new-pin 阻断、reader drain、whole-allocation reclaimability 与 durable `FREE_AND_BUMP`。
12. relocation 不创造新的 local success、AQ 或 ACK。
13. checkpoint through `S` + complete suffix `>S` 与完整 control history 得到相同 current selector；历史 chain 可压缩，anti-ABA/retiring state 不得丢失。
14. orphan GC 只证明 new location 未承载 authority；logical entry 的既存 local success 不阻止清理 uncommitted copy。
15. conditional orphan free 与迟到 `MOVE_COMMIT` 不能同时成功；cutover 只晚于覆盖自身 sequence 的 durability completion。
16. per-Arena predicate 对 committed/applied state 原子求值；condition failure不改变authority，pending/admitted append不授予cutover/free/reuse。
17. duplicate externally retried transition只能得到同一durable result或stale/conflict，不产生第二winner或重复generation bump。
18. runtime的unknown mandatory/gap/torn阻断durable-through；restart只按§5.4证明后截断未提交末尾，必需prefix损坏或分类不明保持non-writable。
19. selector publish与block-new-old-pin形成同一同步cut；cut后read pin不能落回old location。
20. Arena superblock/format state不能替代old-binary-visible Bookie compatibility fence；任何partial required-device migration都不注册writable。

## 18. 接受 Gate

除下列既有Gate外，必须完成§5.4三类tail oracle、§6 pool ownership与旧writer I/O终结、§13.1每Arena耗尽/恢复进展，以及§15两条index恢复路径；均保留独立原始证据。

本 RFC 进入 Accepted 前必须：

- [Spike B](spikes/SPIKE-B-allocator-block.md) 达到全部 safety、memory 和 latency Gate；
- [Spike C](spikes/SPIKE-C-no-object-tla.md) 的 Model C 无 counterexample；
- on-disk framing、checksum、alignment 和 compatibility version 冻结；
- checkpoint/control-log recovery 可由自动 crash matrix 重放；
- cold/hot promotion、lifetime class 和同 Arena `MOVE_COMMIT` relocation 合同通过 crash、并发 move、reader pin 与 index rebuild 测试；
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
