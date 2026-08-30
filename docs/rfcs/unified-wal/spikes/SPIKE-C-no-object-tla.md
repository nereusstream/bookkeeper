# Spike C：无对象存储 TLA+ 安全模型否证规范

> 状态：**Planned / Not Executed**<br>
> 对应 RFC：[RFC-0001](../RFC-0001-profile-capability-install.md)、[RFC-0002](../RFC-0002-sequenced-wal.md)、[RFC-0003](../RFC-0003-segment-storage-allocator.md)、[RFC-0004](../RFC-0004-range-recovery-delete.md)、[RFC-0005](../RFC-0005-segment-bookie-state.md)<br>
> 性质：有界安全模型检查；PASS 不等于无限状态证明或生产就绪

## 1. 要回答的问题

本 Spike 建立不依赖对象存储/blob store 的独立 TLA+ 模型，尝试在以下并发和故障下寻找 safety counterexample：

- BookKeeper E/W/A、write-set rotation、fencing 和 ensemble change；
- Sequenced Classic 两 writer、response loss、recovery 和 successor ledger；
- allocator `ALLOC/DATA/DELETE/FREE/generation/checkpoint`；
- historical ensemble delete、offline Bookie 和 rejoin barrier。

BtrLog 可作为建模风格参考，但它的 durable blob-store entity、flush 和 recovery 结论不能导入本模型作为公理。

## 2. 非目标与证明边界

- 有界 TLC PASS 不证明任意集群规模、任意 entry 数或所有实现细节；
- 本 Spike 优先检查 safety，不以弱化 fairness 换取虚假的 liveness 结论；
- 不把 Java 测试、性能 benchmark 或人工 trace 当作模型检查替代；
- 不允许用“对象存储最终保存所有数据”作为恢复动作或不变量前提；
- Model E 的 general E/W/A range optimization 可在推进该功能时单独增加。

### 2.1 故障分类与保证边界

模型必须区分：

```text
ProcessCrash
    volatile state lost; durable media retained

PermanentFailureDomainLoss
    one declared placement failure domain's durable evidence
    is permanently unavailable

DetectedCorruption
    affected evidence is invalid and cannot count as surviving

ResponseLoss
    durable outcome may exist while the client does not know it
```

对 Profile 声明的永久损失预算 `F`：每个成功 ACK 必须包含来自至少 `F + 1` 个 distinct declared permanent-failure domains 的 durable acknowledgements。自上一次已证明完成的 repair/re-replication 后损失不超过 `F`，才承诺至少一个有效 payload/identity evidence 存活。

`A >= F + 1` 只是必要条件；ACK set 的 failure-domain 覆盖与 repair window 同样是合同的一部分。Profile 可以声明 `F = 0`，不能为了模型方便无条件提高所有部署的 A。metadata/auth authority 的生存预算独立于 payload evidence，本 Spike 不能把两者混为一个性质。

## 3. 运行前锁定 manifest

```text
sourceCommit
RFC revisions
TLA modules commit
TLA+ tools version
JDK
model checker and flags
worker count
heap/direct-memory limits
symmetry and constraints
config matrix
declared permanent-failure domain model
per-Profile permanent-loss budget F
repair/re-replication budget reset point
random seed where applicable
state/time budgets
artifact output directory
```

任何 config、constraint 或 invariant 在正式运行后改变，都产生一个新的 run identity，不能覆盖原结果。

## 4. 模块结构

建议拆成：

```text
UnifiedWalTypes.tla
BookKeeperCore.tla          Model A
SequencedClassic.tla        Model B
SegmentAllocator.tla        Model C
ClusterDelete.tla           Model D
GeneralEwaRecovery.tla      Model E, optional until feature advances
```

共享类型模块只定义集合、record shape 和纯函数，不把任一子模型的 safety 结论当作另一个模型的假设。

组合模型至少需要一次把 A+B、A+C 和 C+D 的关键边界共同展开，避免接口假设各自成立但组合后矛盾。

## 5. Model A：BookKeeper Core

### 5.1 最小状态

```text
ledger metadata version and state
ensemble history
E/W/A
entryId -> write set
Bookie up/down and fenced state
per-Bookie volatile and durable accepted entries
per-Bookie failure-domain identity and permanent-loss state
client pending operations
AQ evidence
LAC/close state
authoritative Classic/Profile route
profile install and activation state for new-profile variant
```

### 5.2 最小动作

```text
CreateLedger
InstallProfileOnBookie
PublishOpen
ActivateProfileOnBookie
SendAdd
SendLegacyAdd
BookieAcceptAdd
BookieAck
LoseResponse
CrashBookie
RestartBookie
PermanentlyLoseFailureDomain
DetectCorruption
BeginFence
FenceBookie
RecoverEntry
CloseLedger
BeginEnsembleChange
InstallReplacement
PublishEnsembleChange
```

### 5.3 检查目标

- AQ 定义与 recovery 保持一致；
- profiled Add 没有 matching durable activation 时不能被 Bookie 接受；
- legacy Add 不能绕过 Profile/Tombstoned route；
- fencing 后不能形成不合法的新 AQ；
- ensemble replacement 不绕过 Profile install；
- response loss 不产生两个 ledger instance/OPEN publication；
- `E > W` 轮转覆盖全 ensemble 安装需求。

## 6. Model B：Sequenced Classic

### 6.1 最小状态

```text
SequenceDomain and reservations
run/predecessor/successor metadata
writer epochs and local caches
appendId -> payload digest
inflight Adds, AQ candidates and completion order
predecessor fence/recovery/seal state
published contiguous frontier
```

### 6.2 最小动作

```text
ReserveSequenceRange
IssueAppend
ReachAQ
PublishWalCommitted
DeliverOrLoseCompletion
CompeteForRecoveryAuthority
FencePredecessor
RecoverMaximumPrefix
SealPredecessor
CreateSuccessor
PublishSuccessorActive
RetryAppendId
CrashWriter
```

### 6.3 检查目标

- sequence interval 不重叠；
- ordered frontier 不越过 hole；
- 相同 appendId 不绑定两个 payload；
- 已形成的 AQ evidence 不被伪造或抹除，但 sealed prefix 外 candidate 不成为 WAL COMMITTED；
- successor ACTIVE 后 predecessor sealed prefix 外的数据不再发布；
- successor 从 durable sealed prefix `P + 1` 开始；
- 两 writer 竞争不产生两个 ACTIVE successor；
- old Bookie 模式不假设 server 读取 epoch。

模型中禁止 `SameLedgerEpochTakeover` 动作。若未来加入 `EPOCH_AWARE_ADD_V1`，必须另建 capability variant。

## 7. Model C：Segment Allocator

### 7.1 最小状态

```text
slots/extents and generations
durable vs volatile ArenaControlLog records
allocator checkpoint generations
superblock A/B pointers
shard allocation pools
data records and durability
local-success publications
ledger tombstones
locators and reader pins
device state
```

### 7.2 最小动作

```text
AppendAlloc
DurabilizeControl
PublishSpaceToShard
WriteData
DurabilizeData
PublishLocalSuccess
AppendDeleteTombstone
DrainReader
AppendFreeAndBump
ReuseSlot
BuildCheckpoint
DurabilizeCheckpoint
SwitchSuperblock
ReclaimOldControlSegment
CrashDeviceProcess
RecoverAllocator
CorruptAuthority
```

### 7.3 检查目标

- local success 隐含 durable allocation 和 durable data；
- slot/generation 唯一 owner；
- free 未 durable 时不能 reuse；
- old locator 不能读新 generation；
- checkpoint rotation 不丢失唯一 authority suffix；
- authority 无法证明时 device 不变为 writable；
- delete/free 与 reader pin 顺序安全。

shared slab 可先用一个 block 包含两个 ledger 的最小域建模；dedicated extent 用单 owner block 建模。不能只建模 dedicated extent 后宣称覆盖 shared delete。

## 8. Model D：Cluster Delete

### 8.1 最小状态

```text
ledger metadata and version
ensemble history
delete manifest and epoch
frozen historical target set
AutoRecovery operations
per-Bookie local tombstone/apply watermark
Bookie online/offline/registered mode
decommission proofs
logical and physical completion
ledger instances
```

### 8.2 最小动作

```text
BeginDeleteIntent
PublishEnsembleChange
StartAutoRecoveryReplica
PublishAutoRecoveryReplica
FreezeEnsembles
PublishLogicalDelete
DispatchLocalDelete
ApplyLocalDelete
LoseDeleteReceipt
TakeBookieOffline
RejoinRecovering
CatchUpDeleteWatermark
RegisterWritable
DecommissionBookie
PublishPhysicalDelete
CompactTombstone
ReuseLedgerIdWithNewInstance
```

### 8.3 检查目标

- logical delete 后旧 instance 不再 open；
- frozen targets 覆盖所有可承载旧 instance 数据的历史节点；
- DELETE_INTENT 后 AutoRecovery 不产生漏删副本；
- offline Bookie 未 catch up 时不能 writable；
- physical completion 需要每个 target 的 terminal proof；
- 旧 instance delete 不影响新 instance；
- tombstone compact 不允许极晚 rejoin 复活旧数据。

## 9. 核心不变量

规范中使用可执行 predicate 表达，至少包括：

```text
NoFabricatedRecovery
AckedPayloadSurvivesWithinBudget
EvidenceExhaustedNeverReturnsSuccess
OpenImpliesAllEInstalled
ProfiledAckRequiresDurableActivation
LegacyAddCannotBypassProfileRoute
ReplacementInstalledBeforeActive
NoOverlappingPublishedSequence
AppendIdContentUnique
OrderedFrontierIsContiguous
NoPostSealPublicationOutsideRecoveredPrefix
AllocationDurableBeforeLocalSuccess
OneOwnerPerSlotGeneration
NoReuseBeforeDurableGenerationBump
OldLocatorNeverReadsNewGeneration
AllocatorAuthorityOrDeviceFailed
LogicalDeleteIsIrreversible
FrozenTargetsCoverReplicaHistory
NoWritableRejoinBeforeDeleteCatchup
PhysicalDeleteHasTerminalProofs
InstanceIsolation
```

关键自然语言对应：

- recovery 没有有效 evidence 时永远不能返回成功；
- ACK 且永久 failure-domain losses 在声明预算内时，至少一个有效 payload evidence 存活；
- evidence 全部耗尽时进入 `UNRECOVERABLE/DATA_LOSS`，不能伪造恢复成功；
- profiled Add ACK 之前有 matching durable activation，legacy Add 不能绕过 route；
- 同一 extent generation 不同时属于两个 ledger instance；
- ALLOC 未 durable 时 DATA 不可被允许 ACK；
- FREE 未 durable 时 slot 不可复用；
- takeover ACTIVE 后旧 writer 不能把 sealed prefix 外数据发布为成功；
- recovery 只发布最大连续可证明前缀；
- logical delete 后不能重新 open；
- 返回 Bookie 应用 tombstone 前不能成为 writable。

## 10. 最低配置矩阵

每个适用模型必须覆盖的最小结构：

```text
3/3/2
3/3/3
3/2/2
at least one E > W write-set rotation
ensemble change
two competing writers
Bookie crash/restart
permanent failure-domain loss within and beyond F
detected corruption
client/coordinator response loss
allocator generation reuse
offline Bookie delete and rejoin
```

并非每个子模型都展开所有变量；例如 Model C 不复制 quorum 全状态，但组合 A+C 必须覆盖 local success 到 distributed ACK 的接口。

建议 formal config 表：

| Config | Model | E/W/A | Failure/Concurrency |
| --- | --- | --- | --- |
| A-332 | A | 3/3/2 | response loss + Bookie crash |
| A-333 | A | 3/3/3 | fence + ensemble change |
| A-322 | A | 3/2/2 | write-set rotation |
| A-ACT | A | 3/3/2 | install complete, activation missing, legacy Add |
| A-FD-OK | A | profile-specific | `F` losses across declared domains within budget |
| A-FD-EXHAUST | A | profile-specific | all valid ACK evidence exhausted |
| A-REPAIR-LOSS | A | profile-specific | loss, proven repair, second loss |
| B-2W | A+B | 3/3/2 | two writers + takeover |
| B-RESP | B | bounded | completion reorder/loss |
| C-REUSE | C | local | crash at alloc/data/free/reuse |
| C-CKPT | C | local | checkpoint/superblock crash |
| D-OFF | D | historical ensembles | offline rejoin |
| D-RACE | A+D | E > W | delete vs ensemble/AutoRecovery |
| CD-REUSE | C+D | local + cluster | delete, free, ledgerId reuse |

正式运行前可增加 config，不得删除最低结构。

## 11. 状态空间控制规则

允许：

- 缩小 ledger、entry、sequence、generation 的数据域；
- symmetry sets；
- 等价状态归约；
- 将 payload 抽象为少量不同 digest；
- 分子模型后再做关键接口组合。

不允许：

- 删除 `E > W` write-set rotation；
- 把两 writer 归约成一个；
- 假设网络 response 可靠；
- 假设 Bookie 永不重返；
- 移除 generation reuse；
- 把 delete 与 AutoRecovery 串行化为不会竞争；
- 添加 blob store 作为永不丢失的恢复 oracle；
- 用 invariant 本身作为 ASSUME。

如果状态空间过大，应先缩小数据域但保留协议结构。仍无法 complete 的 config 标为 INCONCLUSIVE，并提供覆盖与资源证据；不能直接排除后宣称 PASS。

## 12. Fairness 与 liveness

首要 Gate 是 safety。可额外检查以下有界进展属性：

- 在足够 Bookie 可用且无持续故障时，install 最终 OPEN 或明确失败；
- recovery authority 唯一时，predecessor 最终 SEALED 或明确失败；
- delete targets 最终响应或被 durable decommission 时，physical delete 最终完成。

fairness assumptions 必须逐条记录。liveness 未完成不影响 safety counterexample 的有效性，但会使相应进展结论保持开放。

`AckedPayloadSurvivesWithinBudget` 只证明 payload evidence survival，不证明继续写可用性、read quorum 可用性、metadata/auth authority 生存或 general E/W/A recovery liveness。超过预算后的 `DATA_LOSS` 是明确 terminal state；没有证据却返回恢复成功仍是 safety violation。

## 13. Trace 对齐

每个模型 action 应映射到候选实现事件名和测试 fault point。至少产出：

```text
model action
RFC step
future code component
observable event/metric
fault injection hook
```

这样 counterexample 才能转成 Spike A/B 或集成测试，而不是停留在抽象 trace。

## 14. 硬 Gate

PASS 必须同时满足：

```text
safety invariant violations               = 0
deadlock violations not explicitly terminal = 0
minimum configs completed                 = 100%
configs excluded after formal-run lock    = 0
counterexamples discarded                 = 0
model/checker errors                      = 0
artifact checksum failures                = 0
```

每个 config 必须由 checker 报告 complete，或明确标为 INCONCLUSIVE。INCONCLUSIVE config 使整个 Spike 不能 PASS。

覆盖统计至少包含 generated states、distinct states、queue depth、diameter、runtime、worker count 和 peak memory。

## 15. 立即停止条件

任一 safety invariant 出现 counterexample：

1. 保存完整 trace、config、module hash 和 checker output；
2. 标记对应模型及整个 Spike FAIL；
3. 不继续以更弱 invariant 或更小结构替换正式结果；
4. 回到对应 RFC 修正合同；
5. 将 trace 转为 deterministic executable test；
6. 用新 run identity 重跑全部受影响 config。

checker internal error、OOM、timeout 或 incomplete exploration 是 INCONCLUSIVE，不是 PASS。

## 16. 必交 artifacts

```text
manifest.json
modules/
configs/
checker-output/
counterexamples/
coverage-summary.json
invariant-mapping.md
action-to-test-mapping.md
checksums.txt
README.md
```

`README.md` 必须明确：

- 模型抽象掉了什么；
- 哪些 safety/liveness property 被检查；
- 每个 config 是否 complete；
- 所有 assumptions 和 constraints；
- 是否有 counterexample；
- 与 BtrLog 模型的差异，特别是本模型没有 blob store。

## 17. 结果解释

- PASS：在锁定的有界配置中未找到 counterexample，可支持 RFC 继续接受评审。
- FAIL：至少一个真实 counterexample；相关 RFC/Segment 路径保持 P0 Blocked。
- INCONCLUSIVE：至少一个最低 config 未 complete、工具错误或证据缺失。

PASS 不能表述为“已证明生产系统安全”，也不能单独授权 Segment authority、Cluster Delete 或 general E/W/A fast recovery。
