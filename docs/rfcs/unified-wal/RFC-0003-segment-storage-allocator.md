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
- sequence takeover，见 [RFC-0002](RFC-0002-sequenced-wal.md)；
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

区域大小、对齐、冗余和多设备布局是 Spike 后冻结的参数，不在本骨架中写成生产默认。

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

首版 compaction relocation 只支持 old/new allocation 同属一个可线性化 `ArenaControlLog` authority domain。跨 Arena/device relocation 涉及两个独立 authority，首版明确 unsupported；若未来支持，必须另行评审，不能把单边 `MOVE_COMMIT` 扩展成隐式分布式事务。

`MOVE_COMMIT` 是 derived locator 从 old location 切到 new location 的唯一 durable authority。它是条件化 transition，语义至少绑定：

```text
ledgerInstanceId
logicalEntryOrBoundedRangeIdentity
expectedOldLocationAndGeneration
newLocationAndGeneration
payloadDigest
moveOperationIdentityAndGeneration
```

同一 expected predecessor 只能有一个 winning successor。无 durable `MOVE_COMMIT` 时 old location 仍 authoritative；new copy 即使 payload durable 或 derived locator 已更新，也只是可证明后才能清理的 orphan。durable commit 后新 lookup 必须走 new location；new payload digest/identity 无法验证时 fail closed，old copy 不得自行夺回 authority。

`MOVE_PREPARE` 可以作为 orphan discovery 或 QoS 优化，但不是 safety 必需。exact record bytes、checksum、per-entry/range packing 与 batch 大小保持开放。

## 6. 分配与 ACK 顺序

最低顺序：

```text
1. append ALLOC or ALLOC_POOL with generation and owner
2. make control record durable
3. expose space to WalAppendShard
4. write DATA into authorized space
5. complete DATA durability barrier
6. allow local Bookie success to participate in quorum ACK
```

核心不变量：

```text
local durable Add success
    => durable allocation authority existed before DATA use
    && DATA durability barrier completed
```

为避免每个 Add 增加 control-log fsync，allocator 应提前批量分配：

- shard-owned free block pool；
- shared slab block pool；
- dedicated extent pool。

pool refill 通过 control-log group commit；pool 内空间的使用仍必须有可恢复的 block/record framing，但不要求每条 Add 写一条 allocator fsync。

`ALLOC_POOL` 如何把 ownership 从 device allocator 下放到 shard、未使用 pool 在 crash 后如何回收，是 RFC 接受前必须冻结的合同。

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
3. drain readers and pin holders
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
5. publish/rebuild derived locator to new
6. block new old-location pins; drain existing readers/pins
7. only when every live record in the old allocation is moved/dead:
   append durable FREE_AND_BUMP
8. expose the bumped generation
```

一个 record move 完成不等于整个 shared block 可 free。`MOVE_COMMIT` 不产生新的 BookKeeper local success、AQ 或 ACK，只保持既有 payload authority。多个有界 move record 可以共享 control-log group-commit barrier；locator cutover 必须晚于覆盖自身的 durability completion，不要求每个 moved entry 独立 control fsync。

## 11. Allocator checkpoint 与 rotation

数据或 derived-index checkpoint 可以丢失并重建；allocator authority 不能全部丢失。

control-log 旧段的唯一合法回收顺序：

```text
1. build complete allocator checkpoint for generation N
2. fsync checkpoint and verify checksum
3. append/record CHECKPOINT_COMMIT
4. update inactive superblock to point at N
5. fsync inactive superblock
6. atomically select active superblock generation
7. only then reclaim covered control-log segments
```

如果步骤中途 crash，restart 必须选择最后一个完整、可验证且依赖 suffix 仍存在的 generation。不能只按最大 generation number 选择损坏 checkpoint。

checkpoint 必须包含所有仍有效 relocation cutover，或保留足以唯一重建其 move chain 的 control-log suffix。rotation 不得删除唯一 `MOVE_COMMIT` authority；move replay 只能沿条件化 predecessor 和 operation generation 前进，不能按最大物理 generation、mtime 或 derived locator 猜测 winner。

## 12. Restart 与 crash consistency

启动顺序：

1. 读取并验证 superblock A/B；
2. 选择最高的完整 committed checkpoint generation；
3. replay 其后的完整 control-log records，截断 torn tail；
4. 重建 allocated/free/generation/device state；
5. 扫描已授权 active data tail，验证 block framing；
6. 重建 ledger directory 与 derived index；
7. 对无法证明 ownership 或 payload durability 的对象 fail closed；
8. 完成校验前 Bookie 保持 RECOVERING/READ_ONLY。

必须注入的 crash 边界：

- ALLOC write 前后；
- control-log durability 前后；
- DATA write 中和 durability barrier 前后；
- local ACK 前后；
- DELETE_TOMBSTONE 前后；
- reader drain 与 FREE_AND_BUMP 前后；
- checkpoint data、commit、superblock switch 和 old-log reclaim 各边界；
- compaction new allocation/data durability、`MOVE_COMMIT` append/durability/response loss、locator publish、new-pin 阻断、reader drain 和 old free 各边界；
- 同一 predecessor 的并发 move、move chain、group-commit torn tail、new payload digest mismatch，以及每个边界删除 derived index 后的重建。

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

## 14. Reclaim 能力分级

| 布局 | logical delete | physical reclaim | compaction |
| --- | --- | --- | --- |
| Dedicated extent | tombstone 后立即不可见 | reader drain + durable free 后按 extent 回收 | 通常不需要 |
| Shared cold slab | tombstone 后立即不可见 | whole block 全死后回收 | 部分 block 需要低优先级 compact |

性能报告必须分别展示 logical deletion latency、physical bytes reclaimed、pending dead bytes 和 compaction debt。

## 15. Derived index

RocksDB 可保存 entry/sequence locator、ledger directory 和 tail summary，但必须：

- 不参与 payload ACK authority；
- 不参与 allocator ownership authority；
- key/value 带 format、instance 和 generation；
- 全库删除后可从 control log + data arena 重建；relocation winner 只能由 durable `MOVE_COMMIT` chain 决定；
- stale generation locator 在读取时被再次校验；
- rebuild/compaction 有 foreground QoS 和 admission control。

## 16. 多设备与设备失败

每个 device/WalArena 有独立 control authority 和 generation namespace。跨设备 ledger 可以有多个 extent locator，但任何单个 allocation 只由一个 arena 管理。首版 compaction 只能在同一 Arena authority domain 内 relocation；device evacuation/cross-Arena move 保持 unsupported，不能由单边 `MOVE_COMMIT` 推断安全。

设备出现以下任一情况时进入 FAILED/QUARANTINED：

- 无法找到一致的 superblock/checkpoint/control suffix；
- control-log checksum gap 使 ownership 不可证明；
- generation regression 或 double ownership；
- payload 与已 ACK 事实无法调和。

设备重新加入前必须完成 allocator recovery、delete watermark 同步和上层 BookKeeper recovery 判定；不能仅因块扫描可读就恢复 writable。

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

## 18. 接受 Gate

本 RFC 进入 Accepted 前必须：

- [Spike B](spikes/SPIKE-B-allocator-block.md) 达到全部 safety、memory 和 latency Gate；
- [Spike C](spikes/SPIKE-C-no-object-tla.md) 的 Model C 无 counterexample；
- on-disk framing、checksum、alignment 和 compatibility version 冻结；
- checkpoint/control-log recovery 可由自动 crash matrix 重放；
- cold/hot promotion、lifetime class 和同 Arena `MOVE_COMMIT` relocation 合同通过 crash、并发 move、reader pin 与 index rebuild 测试；
- 证明 shadow writer 可以与 Classic authority 隔离，失败不会影响 Classic ACK。

即使 Spike 通过，也只解锁 shadow implementation。Segment 成为 ACK authority 仍需要 [RFC-0005](RFC-0005-segment-bookie-state.md) Accepted、独立 canary Gate、回滚合同和 RFC-0001 安装/activation 证据。

## 19. 开放问题

- ArenaControlLog region sizing、segment rotation 和满盘行为；
- `ALLOC_POOL` 下放 ownership 的粒度与 crash 回收；
- exact block/record bytes、checksum 和 torn-write detector；
- direct I/O API、alignment、buffer ownership 与 kernel/filesystem 约束；
- shared slab lifetime classification、`MOVE_COMMIT` exact packing/batching、可选 `MOVE_PREPARE` 与 orphan GC；
- hot promotion/demotion 是否单向以及阈值；
- multi-device placement、device evacuation、cross-Arena relocation 和 rebuild；
- delete authorization receipt 的本地格式；
- metadata memory accounting 的实现与观测；
- on-disk format upgrade 和 downgrade 策略。

这些问题关闭、Spike B/C 通过且 RFC Accepted 前，Segment WAL 保持 P0 Blocked。
