# Spike C：无对象存储 TLA+ 安全模型否证规范

> 状态：**Planned / Not Executed**<br>
> 对应 RFC：[RFC-0001](../RFC-0001-profile-capability-install.md)、[RFC-0003](../RFC-0003-segment-storage-allocator.md)、[RFC-0004](../RFC-0004-range-recovery-delete.md)、[RFC-0005](../RFC-0005-segment-bookie-state.md)<br>
> 性质：有界安全模型检查；PASS 不等于无限状态证明或生产就绪

## 1. 要回答的问题

本 Spike 建立不依赖对象存储/blob store 的独立 TLA+ 模型，尝试在以下并发和故障下寻找 safety counterexample：

- BookKeeper E/W/A、write-set rotation、fencing 和 ensemble change；
- allocator `ALLOC/DATA/DELETE/FREE/generation/checkpoint`；
- historical ensemble delete、offline Bookie 和 rejoin barrier。

BtrLog 可作为建模风格参考，但它的 durable blob-store entity、flush 和 recovery 结论不能导入本模型作为公理。

## 2. 非目标与证明边界

- 有界 TLC PASS 不证明任意集群规模、任意 entry 数或所有实现细节；
- 本 Spike 优先检查 safety，不以弱化 fairness 换取虚假的 liveness 结论；
- 不把 Java 测试、性能 benchmark 或人工 trace 当作模型检查替代；
- 不允许用“对象存储最终保存所有数据”作为恢复动作或不变量前提；
- Model E 的 general E/W/A range optimization 可在推进该功能时单独增加。
- 受支持子集的基础point recovery、required frontier、normal-tail/outcome和durable close先由Model A检查，不能因Model E延期而省略。
- 不建模hash密码学、PKI、wire bytes、filesystem或old binary parser内部；只抽象其Spike A/B已验证的布尔结果与generation关系，不能把未执行的candidate测试预设为真。

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

### 2.2 首批启用scope与并行原型

Block G模型与Block H隔离、可丢弃的存储性能切片可并行；实际启用路径的安全Gate仍必须在authority/canary前闭合。先修CLIENT-1/2用现有Java定向回归，不以等待模型展开为前置；buffer字节/引用计数和CPU性能由Spike B证明，不为此扩大分布式状态机。

| Scope | 首批检查与后续条件 |
| --- | --- |
| 基础A/C及必要D组合 | 当前ACK/write set、normal/recovery identity、基础点恢复/durable close、分配/持久化/readable/复用；启用普通删除时加入admission/freeze/完整targets/tombstone及异步本地回收 |
| 普通删除 | logical completion不等全部target屏障，离线目标保持清理义务；允许旧reader在本地tombstone前继续读，新open/admission拒绝；本地free仍需全部必要drain/I/O条件 |
| strong completion/reset | 首批disabled，不新建pendingPublication，不由普通membership/close reset预算；后续仅整个ledger fenced+CLOSED才允许启用，旧未resolve token保留原解析规则 |
| 强访问撤销 | 首批disabled；将来独立验证全部target屏障或永久服务隔离proof后才完成 |
| Range与其他扩展 | Model E、在线删除、复杂故障域和扩展迁移按启用能力独立配置，不覆盖基础point recovery |

下文列出各能力的状态/动作全集，延期动作按上述scope禁用，不成为首批配置的隐含前提。必须测试禁用拒绝和边界：活跃ledger不生成strong token、基础close不reset、普通logical成功不宣称强撤权。未reset时以首次ACK或上次有效proof的loss window计预算，不把repair成功当作续期。future reset配置仍保留完整反例结构；run后不得把失败功能改成disabled来声称通过。

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
enabled/deferred feature scopes and required configurations per scope
ordinary-delete versus strong-access-revocation completion predicate
strong-reset enabled flag and whole-ledger-fenced-CLOSED precondition
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
SegmentAllocator.tla        Model C
ClusterDelete.tla           Model D
GeneralEwaRecovery.tla      Model E, optional until feature advances
```

共享类型模块只定义集合、record shape 和纯函数，不把任一子模型的 safety 结论当作另一个模型的假设。

组合模型至少需要一次把 A+C 和 C+D 的关键边界共同展开，避免接口假设各自成立但组合后矛盾。

## 5. Model A：BookKeeper Core

### 5.1 最小状态

```text
standard ledger metadata version/state, immutable instance backlink
ensemble history
Profile sidecar store version, semantic/control generation and READY/availability facts
bounded authority-domain heads, child/page publication refs and snapshot manifests
committed snapshot cut and complete bounded suffix per compacted authority domain
sidecar format version, mandatory feature set and compatibility state
sidecar operation identity -> semantic payload and durable result
immutable Profile descriptor identity/generation and permanent-loss budget F
DescriptorCanonical and descriptor matching identity state
non-anonymous control principal and ControlAuthorized exact operation/instance/target scope
AuthorityRead committed-authority verification state
protected local auth binding match
WireNegotiated, Profile logical opcode, capability match and no-downgrade state
old decoder accepted-as-Classic effect flag
client/coordinator Profile metadata-mutation authority
E/W/A
entryId -> write set
Bookie up/down and fenced state
per-Bookie volatile and durable accepted entries
per-Bookie failure-domain identity and permanent-loss state
per-range repair generation and immutable coverage cut
valid evidence domains per coordinate
failure-domain policy generation and loss generation
accepted loss event identity/domain/incarnation
overlapping-range loss/reset predecessor or head
durable range-scoped verifier assertion
optional coverage audit commitment
bounded child receipts and committed interval snapshot
client pending operations
logical Add identities and current delivery target/incarnation ACK sets
same-coordinate pending/durable payload identity and readable publication
supported point-recovery context, source coverage and rich terminal outcome
AQ evidence
LAC/close state
authoritative Classic/Profile route
profile inactive install, normal admission/fence generation,
bounded recovery grants and committed-readable range facts
Bookie storage incarnation, effective assignment readiness and writable registration generation
OldBinaryBlocked compatibility state and isolated-new-scope fallback state
FormatCompatible mandatory-format/device-manifest/incarnation state
migration/format/local-readiness generations and RegistrationGenerationMatches
```

### 5.2 最小动作

```text
AllocateLedgerInstance
CreateSidecarChild
PublishSidecarDomainHead
BuildSidecarSnapshot
PublishSidecarSnapshot
ReclaimCoveredSidecarChild
ReuseLedgerIdWithResetStoreVersion
IntroduceReferencedUnknownMandatorySidecarRecord
RetrySidecarOperationSamePayload
RetrySidecarOperationConflictingPayload
CreateProfileReservation
EstablishDescriptorCanonical
RejectInvalidOrUnknownDescriptor
AuthenticateControlPrincipal
EstablishControlAuthorized
EstablishAuthorityRead
InstallProfileOnBookie
CreateStandardLedgerMetadata
PublishReadyAuthorization
ActivateProfileOnBookie
PublishAvailabilityComplete
SendAdd
EstablishWireNegotiated
SendProfileNormalOpcode
SendProfileRecoveryOpcode
AttemptClassicDowngradeAfterProfileFailure
OldDecoderInterpretsProfileAsClassic
SendLegacyAdd
ClaimClassicRouteBeforeLazyCreate
ClaimProfileRouteBeforeLazyCreate
AttemptProfileMembershipMutation
BookieAcceptAdd
BookieAck
LoseResponse
CrashBookie
RestartBookie
PublishCompatibilityFence
AttemptOldBinaryStartup
PrepareFormatMigration
FormatRequiredDevice
PublishLocalFormatReadiness
PublishWritableRegistration
AttemptRollbackOldBinary
PermanentlyLoseFailureDomain
DetectCorruption
BeginFence
CloseAdmissionAndCaptureFenceCut
FenceBookie
RetryAddWithStaleAdmissionGeneration
RecoverEntry
CloseLedger
BeginEnsembleChange
InstallReplacement
PublishEnsembleChange
PublishReplacementActivationAuthority
ActivateReplacement
ResendPendingAdd
ReceiveLateOldTargetAck
CheckCurrentAckFailureDomains
RetrySameCoordinatePayload
RejectConflictingCoordinatePayload
PublishReadableLocation
ReadPointRecoveryEvidence
ClassifyRequiredHoleOrNormalTail
PublishDurableRecoveredClose
BeginBoundedRepair
DurabilizeReplacementEvidence
VerifyRangeCoverage
PublishRepairMembership
ActivateCurrentReplacement
PublishRepairCompletion
LoseRepairCompletionResponse
PermanentLossAfterRepairProof
AttemptReinterpretRepairAssertionUnderChangedProfile
AcceptLossDeclaration
ReplayDuplicateLossDeclaration
BuildRepairReceiptSnapshot
PublishRepairReceiptSnapshot
ReclaimCoveredRepairReceipts
```

### 5.3 检查目标

- AQ 定义与 recovery 保持一致；
- `DescriptorCanonical`为false、identity不匹配或unknown mandatory时不能安装/激活；identity不授权control operation；
- Profile control transition要求non-anonymous principal、`ControlAuthorized && AuthorityRead`与matching protected local binding；AuthN-only/master key单独不足，拒绝路径无route/credential/allocation/durable effect；
- Profile normal/recovery/Classic operation在抽象上distinct；`WireNegotiated=false`、capability mismatch或old decoder accepts-as-Classic时不能产生Profile/Classic effect，Profile失败不允许downgrade；
- normal profiled Add 没有 matching global READY 与 durable local normal activation 时不能被 Bookie 接受；
- normal Add 不依赖 sidecar read/watch/CAS；sidecar unavailable不能扩张或隐式收窄已经durable的local admission truth；
- legacy Add 不能绕过 Profile/Tombstoned route；
- route claim早于payload/handle lazy create；store version重置、ledgerId reuse或old retry不能跨instance/semantic generation形成ABA；
- unpublished child/page始终inert；published snapshot + complete suffix与未压缩authority history等价，publish/fallback proof前不能reclaim；
- referenced unknown mandatory sidecar record、missing chunk或suffix gap必须fail closed，不能解释为ABSENT/default；
- same sidecar operation identity只能绑定一个semantic payload；相同payload重试解析到同一durable result，冲突payload只能CONFLICT且无authority effect；
- profiled LedgerMetadata membership/backlink mutation 必须有 Profile mutation authority；master key 单独不足；
- fencing 后不能形成不合法的新 AQ；
- fence先关闭new admission并使所有pre-cut Add terminal；stale handle/admission generation不能在durable fence后形成local success；
- initial standard metadata 不得早于 all-E inactive Profile route claim，normal create/open success 晚于 all-E activation；
- active replacement 遵守 inactive install → `LAC+1` CAS → normal activation → resend，且不复制历史 fragment；
- 普通Add unknown可在正式换组后重发同一identity；控制unknown不盲换operation。旧target/incarnation ACK不进入当前quorum/domain集合，成功保持连续前缀；
- 同坐标相同payload幂等、不同payload冲突，DATA durable与readable publication未同时成立时不能local success；
- 模型区分不可变应用数据/累计length与可变piggyback LAC/封装digest；合法normal/recovery重写不因后者差异冲突，hole不由localLastEntryId推断存在。真实字节/完整性与慢路径成本由B19验证；
- 受支持子集的point oracle区分required hole、normal tail、temporary unavailability和authority loss；recovered success晚于durable close，重启后重新验证；
- 基础durable close不依赖strong-publication、不reset；首批active ledger不生成增强token，不因增强coordinator/sidecar故障新增replacement等待。既有metadata/installation不可用仍可defer，不能把此性质夸成无条件可写；
- 延期bounded repair reset启用后，只在整个ledger fenced+CLOSED、每个ACK-eligible coordinate有`F+1` distinct valid domains、exact membership已发布且conditional strong completion durable后成立；
- target durability、membership 或 activation 任一单独不能 reset；proof cut 后的 loss 进入新 window，迟到 completion 不得清零；
- verifier assertion必须存在；digest/root单独不reset，duplicate accepted loss不重复消费budget；
- conflicting/overlapping range loss与completion单序，proven-disjoint range可并发；snapshot durable前不reclaim child receipt；
- response loss 不产生两个 ledger instance/READY publication；
- `E > W` 轮转覆盖全 ensemble 安装需求。
- old binary只有在`OldBinaryBlocked=true`或新scope已证明隔离时才可接近Segment scope；`FormatCompatible=false`或`RegistrationGenerationMatches=false`时不能writable registration；unsafe rollback不能前进。

Model只抽象上述boolean/generation关系，不编码TLS、SHA-256、TLV bytes、Cookie/filesystem或decoder实现；这些由Spike A/B的真实bytes/binary Gate证明，Model中的true不能替代实际证据。

增强reset配置启用后，repair falsification至少覆盖：partial copy、仅缺一个coordinate、target durable但不足`F+1` domains、membership-only、activation-only、digest存在但verifier未完成、descriptor/`F`变化后重解释旧assertion、整个ledger CLOSED后无`NORMAL_ACTIVE`的合法reset、仅fragment sealed但ledger仍OPEN的拒绝、`E>W` per-entry write-set coverage、duplicate loss、loss/completion两种先后、unobserved failure after proof cut、disjoint并发、overlapping stale completion、small-range merge、snapshot publish/response loss/child reclaim、delete freeze和root/page cap超限。基础sidecar falsification仍覆盖child-before-head crash、head CAS conflict、snapshot期间head推进、fallback损坏、store-version reset/instance reuse、unknown mandatory referenced record与normal Add期间sidecar不可用。

## 6. Model C：Segment Allocator

除既有allocator状态外，加入pool/shard ownership generation、inflight writer I/O、三类control tail、每Arena维护保留预算及index rebuild coverage。组合A+C加入Bookie级control log与多Arena部分durability，不能假设它们一次原子提交。

### 6.1 最小状态

```text
slots/extents and generations
durable vs volatile ArenaControlLog records
allocator checkpoint generations
checkpoint through-sequence and current-selector snapshot
retiring source and anti-ABA/fencing state
superblock A/B pointers
shard allocation pools
data records and durability
local-success publications
ledger tombstones
locators and reader pins
conditional MOVE_COMMIT records and move generations
authoritative relocation chain and orphan copies
per-move control sequence and durable-through cut
per-Arena committed/applied conditional state
conditional operation identity, expected predecessor and assigned control sequence
pending vs durable conditional result and bounded idempotency summary
selector/pin epoch, old-pin admission gate and pre-cut readers
new-location lookup/local-success authority dependency
device state
```

### 6.2 最小动作

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
CopyForMove
DurabilizeMovedPayload
AppendMoveCommit
EnqueueConditionalTransition
EvaluateConditionalPredicate
ApplyConditionalTransition
DurabilizeConditionalResult
RetryConditionalOperation
IntroduceUnknownMandatoryControlRecord
DurabilizeMoveCommit
AcquireReadPinAtSelectorEpoch
PublishSelectorAndBlockNewOldPins
DrainOldReader
FreeMovedSource
BuildCurrentSelectorCheckpoint
ConditionalFreeOrphan
RetryLateMoveCommit
PublishDurableThrough
ClassifyControlTail
TruncateProvenUncommittedTail
ClosePoolWriterAdmission
CompleteOrIsolateOldWriterIO
TransferPoolOwnership
ConsumeForegroundSpaceBudget
RunReservedMaintenanceStep
RebuildAllAuthorizedLiveRanges
```

### 6.3 检查目标

- local success 隐含 durable allocation 和 durable data；
- slot/generation 唯一 owner；
- free 未 durable 时不能 reuse；
- old locator 不能读新 generation；
- checkpoint rotation 不丢失唯一 authority suffix；
- authority 无法证明时 device 不变为 writable；
- delete/free 与 reader pin 顺序安全；
- `MOVE_COMMIT` 为同一 Arena relocation 选择唯一 successor，未 commit copy 不成为 authoritative；
- committed move 在 index 丢失后仍可重建，source free 晚于 cutover、new-pin 阻断和 reader drain；
- move 不创造新的 local-success fact；logical entry已有success不阻止清理从未成为lookup authority的new-location orphan；
- conditional predicate在同一Arena committed/applied state求值；condition failure无状态变化，duplicate operation不产生第二winner或generation bump；
- `APPLIED_DURABLE/ALREADY_DURABLE`只在complete prefix durable through自身assigned sequence后成立，pending/enqueued result不授予cutover/free/reuse；
- selector publication与block-new-old-pin形成同一cut；cut后stale cached locator不能取得old pin，pre-cut reader可drain；
- checkpoint current selector + anti-ABA/retiring state与full-chain oracle等价；
- orphan free与late commit不能都成功，cutover晚于覆盖自身control sequence的durable-through。

shared slab 可先用一个 block 包含两个 ledger 的最小域建模；dedicated extent 用单 owner block 建模。不能只建模 dedicated extent 后宣称覆盖 shared delete。

## 7. Model D：Cluster Delete

首批delete前置为可验证fenced close；admission cut、standard metadata freeze、普通logical tombstone、target-local barrier和physical completion分别建模。普通logical成功可早于全部barrier，强撤权完成另设disabled predicate。扩展online-delete/rejoin动作可独立配置，但不能删掉实际启用路径中的offline pending与迟到请求。

### 7.1 最小状态

```text
ledger metadata and version
ensemble history
delete manifest and per-ledger-instance epoch
frozen historical target set
inert vs admitted durable RepairIntent children and lifecycle/retention
ledger lifecycle/delete-fence generation and RepairIntent admission cut
standard LedgerMetadata membership freeze marker/version and pendingPublication token
domain completion prepare, lifecycle publication reference and resolve state
pending accepted-loss declarations in a prepared conflicting domain
per-target access barrier admission cut, terminal state and durable receipt
AutoRecovery target payload and ensemble publication
per-target recovery-only/committed-readable role
delete streams and committed heads
finite stream assignment and assignment generation
assignment predecessor, PREPARED/effective status and handoff cut
Bookie stable identity and storage incarnation
per-stream durable applied cursor
snapshot generation, covered-through and digest
snapshot bounded chunk manifest/completeness and applicable effects
registration required-through cut
durable local registration readiness bound to storage incarnation,
effective assignment generation and cursor/snapshot cut
per-Bookie local tombstone/effects
Bookie online/offline/registered mode
decommission proofs
cluster-accepted irreversible wipe/decommission proof scope,
operation generation, acceptance version and permanent registration fence
ordinary logical, optional strong-access-revocation and physical completion
ledger instances
```

### 7.2 最小动作

```text
BeginDeleteIntent
PublishEnsembleChange
CreateInertRepairIntentChild
AdmitRepairIntentAgainstDeleteFence
GrantRecoveryOnlyAuthority
WriteRecoveryPayload
PublishRepairEnsemble
CommitRepairIntent
AbortDirtyRepairIntent
CompactRepairIntent
FreezeEnsembles
ReadAuthorityBeforeDelete
CASStandardMembershipFreeze
CASMembershipAndPendingPublication
ResolveMetadataPublicationToken
PrepareDomainCompletion
CASLifecycleCompletionPublication
ResolvePreparedCompletion
QueueLossDuringPreparedCompletion
CloseTargetAccessAdmission
DrainTargetServiceAndWriterIO
DurabilizeTargetAccessBarrier
PublishAccessBarrierComplete
PublishLogicalDelete
DispatchLocalDelete
ApplyLocalDelete
LoseDeleteReceipt
TakeBookieOffline
RejoinRecovering
AppendDeleteEvent
DurabilizeDeleteEffect
AdvanceDeleteCursor
LoseCursorResponse
BuildDeleteSnapshot
CompactJournalPrefix
ChangeStreamAssignment
PrepareStreamAssignment
DualRouteDeleteEvent
CatchUpPreparedAssignment
ActivateStreamAssignment
ReplaceStorageIncarnation
FetchRequiredThrough
RegisterWritable
PublishVersionedRegistrationReadiness
PublishSnapshotChunk
AcceptWipeProof
ReplayOldTerminalProofAcrossScope
DecommissionBookie
PublishPhysicalDelete
CompactTombstone
ReuseLedgerIdWithNewInstance
```

组合A+D时，`LoseResponse`覆盖admission、metadata freeze、logical tombstone及local barrier receipt；增强配置另覆盖completion prepare/publication/resolve。每个cold read与local durable grant分开执行，使模型枚举读到旧authority后delete先赢的时序。不能把跨两个CAS及本地持久化包装成原子action，也不能用“generation有效”的不变量本身约束掉竞争。

### 7.3 检查目标

- logical delete 后旧 instance 不再 open；
- unadmitted RepairIntent child不授予recovery grant或第一份payload authority；
- RepairIntent admission与DELETE_INTENT共享lifecycle/delete-fence cut：admission先赢则必被freeze枚举，delete先赢则后续admission失败；
- admission response loss后，matching committed admission只恢复同一intent/target；delete先赢或结果无法证明时不能grant/write，也不能创建第二intent/target；
- 基础copy/membership/close不运行strong publication或reset；延期增强配置中strong completion通过lifecycle CAS与delete竞争，未发布candidate不能reset，prepared期间loss保留且resolve不改变proof cut；
- metadata freeze CAS前的winner纳入history，freeze之后旧版本/retry不能发布ensemble；
- admission cut前已准入scope可能在本地barrier前留下payload，必须进入delete discovery；barrier后迟到grant、write success和新read均被拒绝；
- 普通logical completion晚于durable tombstone及完整targets/清理义务；旧reader在本地tombstone前可继续发起read，离线节点仅阻塞撤权/物理清理，新open/admission均拒绝；
- 只有启用的强访问撤销完成晚于全部target barrier/永久服务隔离proof；旧网络响应晚到不产生新authority；
- recovery target 第一份 durable payload 晚于可枚举的admitted RepairIntent；
- frozen targets 覆盖历史 ensembles 与 incomplete/completed/aborted-but-dirty RepairIntent 的 replaced member/target；
- DELETE_INTENT 后 AutoRecovery 不产生漏删副本；
- recovery-only/committed-readable role 不产生 normal writable authority；
- cursor 不跨 unexplained gap，且只能晚于对应 durable effect 或可验证 non-applicability；
- offline Bookie 的 current storage incarnation 未对 authoritative finite assignment 全部 catch up 时不能 writable；
- writable registration必须绑定current incarnation、effective assignment generation与durable readiness；旧registration generation不能在effective assignment前进后继续有效；
- snapshot + complete suffix 是唯一 compacted-prefix bootstrap；assignment removal 不能丢失 delete obligation；
- obligation-changing generation必须pre-catch并atomic activate，或旧writable先demote；PREPARED generation不必无条件demote；
- snapshot root必须有complete、可遍历、可应用effects/chunks，digest-only不足；handoff允许duplicate不允许gap；
- old storage incarnation 不能伪装新节点或借 ordinary ensemble replacement 绕过 catch-up；
- catch-up exemption只来自cluster-accepted irreversible wipe/permanent decommission fence；
- terminal proof绑定bookie、old incarnation、device/storage scope、operation generation与cluster acceptance；旧proof跨incarnation/device/scope重放不能免除catch-up；
- physical completion 需要每个 target 的 terminal proof；
- 旧 instance delete 不影响新 instance；
- tombstone compact 不允许极晚 rejoin 复活旧数据。

## 8. Model E：General E/W/A Recovery

Model E 只在推进 general E/W/A range optimization 时启用，但一旦启用必须把 fast path 与现有 point-read oracle 放入同一模型，不能只证明新路径自己的内部一致性。

### 8.1 最小状态

```text
immutable RecoveryContext and authority generation
entry -> write-set and ensemble history
required/committed frontier and CLOSED lastEntry boundary
fenced generation
TailSummary hints
per-coordinate range evidence/results
earliest unresolved coordinate
bounded volatile continuation/proof cache
point-read oracle evidence
definitive absence, transient, corrupt and conflicting evidence
later speculative vs required evidence
recovery target writes and per-entry results
attempt outcome class
operation scope and bounded rich outcome reason
legacy callback/future projection
AutoRecovery/marker scheduling state
published close/final prefix
authority-unrecoverable reason
```

### 8.2 最小动作

```text
ReadTailSummary
StartRangeFastPath
ReturnPartialRange
ExhaustFastPathBudget
SwitchToPointFallback
ReadPointEvidence
RecoveryAddOneEntry
LoseRangeOrAddResponse
CrashRecoveryCoordinator
ChangeRecoveryAuthorityGeneration
CancelOrExpireAttempt
InvalidateCorruptEvidence
DeclareEvidenceExhausted
ProveNormalTail
PublishRecoveredClose
ProjectRichOutcomeToLegacyResult
SkipUnrecoverableInLegacyAggregate
ClearRecoveryMarker
DeclareQuarantineConflict
DeclareAuthorityUnrecoverable
DeferUnavailableRecovery
```

### 8.3 检查目标

- unsupported、stale summary、partial response 与 fast budget exhaustion 从 earliest unresolved coordinate fallback；
- hole 阻止 frontier，即使 later entry 已验证；
- coordinator crash 丢失 volatile continuation 只导致重读，不产生 absence；
- single corrupt replica 失效但其他 valid evidence 仍可恢复；
- cancellation/deadline 只结束 attempt，不证明 DATA_LOSS；
- normal tail必须在fenced exact write set上有`W-A+1` definitive absence coverage，offline/timeout不计；
- required frontier来自accepted authority，later speculative payload不制造required hole；
- authority unrecoverable属于quarantine而非payload DATA_LOSS，required-coordinate finite evidence exhausted才是DATA_LOSS；
- recovered outcome晚于durable close/final-prefix publication；
- 只有matching durable `RECOVERED_AND_CLOSED`投影legacy `OK`；deferred、incomplete、quarantine和data loss都投影non-OK；
- fragment/partial progress与legacy skipped ledger不计为ledger recovered；aggregate skip不能clear intent/marker或发布repair completion；
- generic legacy rc不抹除coordinator/admin持有的rich outcome，AutoRecovery按rich class调度；
- fast path + fallback 的最终结果与全 point-read oracle 完全一致；
- proof cache、point-read 并发与内存保持 manifest-locked bounded。

## 9. 核心不变量

规范中使用可执行 predicate 表达，至少包括：

```text
NoFabricatedRecovery
AckedPayloadSurvivesWithinBudget
EvidenceExhaustedNeverReturnsSuccess
ProfileAvailabilityImpliesAllEActive
ProfiledNormalAckRequiresReadyAndLocalActive
DescriptorIdentityMatchesBeforeInstallOrActivate
DescriptorCanonicalBeforeInstallOrActivate
DescriptorHashDoesNotAuthorizeControl
ProfileControlRequiresNonAnonymousCommittedAuthority
ProfileControlRequiresExactOperationScopeAuthorization
ProfileControlEffectRequiresControlAuthorizedAndAuthorityRead
SecretCredentialDoesNotGrantControl
ProfileNormalRecoveryAndClassicOpcodesAreDistinct
ProfileEffectRequiresNegotiatedCapability
ProfileEffectRequiresWireNegotiated
OldDecoderCannotCreateClassicEffectFromProfile
ProfileFailureNeverDowngradesOrDoubleWrites
LegacyAddCannotBypassProfileRoute
ProfileMembershipMutationRequiresAuthority
SidecarStoreVersionDiffersFromSemanticGeneration
UnpublishedSidecarChildHasNoAuthority
SidecarSnapshotPlusSuffixComplete
SidecarOperationIdentityBindsSinglePayload
LedgerInstancePreventsStoreVersionABA
ReferencedUnknownMandatoryStateFailsClosed
NormalAddIndependentOfSidecarAvailability
InitialMetadataAfterAllEProfileClaim
LocalRouteClaimBeforeLazyCreate
StaleAdmissionGenerationCannotSucceed
RecoveryGrantAndReadableAreNotNormalWritable
ReplacementInstallCasActivateResendOrder
CurrentAckSetExcludesStaleDeliveries
SuccessfulAckCoversDeclaredFailureDomains
SameCoordinateNeverOverwritesConflictingPayload
LocalSuccessImpliesReadableLocation
RepairIntentBeforeTargetPayload
RecoveryRoleNeverGrantsNormalWrite
RepairResetImpliesCompleteRangeCoverage
RepairResetImpliesFPlusOneDistinctDomains
RepairResetRequiresVerifierAssertion
RepairAssertionBindsImmutableProfileF
AuditCommitmentAloneNeverResets
DuplicateLossDeclarationDoesNotDoubleCount
ConflictingRangeLossAndResetAreOrdered
RepairReceiptSnapshotBeforeChildReclaim
MembershipAloneNeverResetsLossBudget
ActivationAloneNeverResetsLossBudget
LocalTargetDurabilityAloneNeverResetsLossBudget
LossAfterProofCountsAgainstNewWindow
ClosedRepairResetDoesNotRequireNormalActive
CurrentNormalWritesRequirePostCasNormalActive
AllocationDurableBeforeLocalSuccess
OneOwnerPerSlotGeneration
NoReuseBeforeDurableGenerationBump
NoReuseBeforeOldWriterIOQuiescence
TruncationExcludesRequiredDurablePrefix
ForegroundPreservesMaintenanceBudget
FullIndexRebuildCoversAllRequiredLiveRanges
OldLocatorNeverReadsNewGeneration
AllocatorAuthorityOrDeviceFailed
MoveCommitSelectsUniqueAuthority
UncommittedCopyNeverBecomesAuthoritative
CommittedMoveSurvivesIndexLoss
NoFreeBeforeMoveCommitAndReaderDrain
MoveDoesNotCreateLocalSuccess
CheckpointSelectorEqualsFullChain
OrphanCleanupPreservesExistingLogicalSuccess
LateMoveCommitAndOrphanFreeAreExclusive
MoveCutoverRequiresOwnDurableSequence
ConditionalFailureHasNoEffect
DurableApplyCoversOwnSequence
DuplicateConditionalOpHasSingleResult
SelectorCutBlocksNewOldPins
OldBinaryBlockedBeforeReplayOrWrite
WritableImpliesKnownMandatoryFormat
WritableImpliesFormatCompatible
WritableImpliesCompleteDeviceManifestAndMigration
WritableRegistrationMatchesIncarnationAndReadiness
WritableImpliesRegistrationGenerationMatches
NewScopeFallbackRequiresOldCredentialIsolation
UnsafeRollbackCannotReachOldWritable
LogicalDeleteIsIrreversible
UnadmittedRepairIntentCannotGrantOrWrite
DeleteFenceOrdersRepairIntentAdmission
DeleteFreezeCoversAllAdmittedRepairIntents
MembershipPublicationTokenPreservesCompletionMapping
StrongCompletionRequiresLifecyclePublication
PreparedCompletionRetainsPendingLoss
LogicalDeleteRequiresTombstoneAndCompleteCleanupTargets
StrongAccessRevocationRequiresAllAccessBarriers
DisabledStrongResetCannotCreateActivePublicationToken
BasicCloseAndMembershipDoNotResetLossWindow
AdmissionResponseLossCannotDuplicateIntentOrTarget
RepairProgressDoesNotAdvanceUniversalHead
FrozenTargetsCoverReplicaAndRepairHistory
NoWritableRejoinBeforeDeleteCatchup
DeleteCursorImpliesDurableEffects
NoCursorAdvanceAcrossUnexplainedGap
WritableImpliesAllApplicableStreamsCaughtUp
AssignmentRemovalCannotLoseDeleteObligation
EffectiveAssignmentFencesStaleWritableGeneration
PreparedAssignmentNeedNotDemoteSafeWriter
DeleteHandoffHasNoRoutingGap
SnapshotContainsApplicableEffects
CatchupExemptionRequiresClusterTerminalProof
TerminalProofCannotReplayAcrossIncarnationOrScope
SnapshotPlusSuffixIsComplete
OldStorageIncarnationCannotBypassCatchup
PhysicalDeleteHasTerminalProofs
InstanceIsolation
FastPathFailureDoesNotChangeRecoveryTruth
FallbackStartsAtEarliestUnprovenCoordinate
RecoveryNeverSkipsHole
AttemptDeadlineDoesNotProveDataLoss
FastAndPointOracleEquivalent
NormalTailRequiresAckQuorumIntersectionAbsence
RequiredHoleNeverPublishesPrefixAcrossIt
TransientUnavailableIsNotEvidenceExhausted
SingleCorruptReplicaIsNotTerminal
AuthorityLossIsNotPayloadDataLoss
RecoveredOutcomeImpliesDurableClose
LegacyOkImpliesDurableRecoveredClose
LegacySkipDoesNotCreateRecoveryAuthority
RichOutcomeSurvivesGenericProjection
```

关键自然语言对应：

- recovery 没有有效 evidence 时永远不能返回成功；
- ACK 且永久 failure-domain losses 在声明预算内时，至少一个有效 payload evidence 存活；
- contract-required coordinate 的有限合法 evidence全部确定性耗尽时才进入 payload `DATA_LOSS`；authority无法判定进入quarantine；
- normal profiled Add ACK 之前有 matching global READY 与 durable local normal activation，legacy Add 不能绕过 route；
- `DescriptorCanonical`/match是identity前置但不授权control；control transition要求non-anonymous principal、`ControlAuthorized && AuthorityRead`和matching local binding，AuthN-only/master key单独不足；
- Profile normal/recovery/Classic opcode彼此distinct，`WireNegotiated=false`或capability mismatch无effect；old decoder接受Profile为Classic、Profile失败后downgrade/double-write都违反不变量；
- sidecar store version不替代semantic generation；unpublished child无authority，snapshot+suffix完整，referenced unknown mandatory state fail closed；
- sidecar operation identity只绑定一个semantic payload；same-payload retry不产生第二结果，conflicting-payload retry无authority effect；
- ledger instance隔离store-version reset/ID reuse；normal Add不等待sidecar read/watch/CAS；route claim先于lazy create，stale admission generation不能绕过fence；
- initial standard metadata 晚于 all-E inactive Profile claim；写期 replacement 按 install/CAS/activate/resend 排序；
- recovery payload 写入 target 前有 durable RepairIntent，recovery-only/committed-readable 不授予 normal write；
- bounded range 只有完整 `F + 1` distinct-domain coverage proof 与 conditional completion authority 才 reset loss window；
- verifier assertion是reset authority，并长期绑定immutable descriptor/`F`/policy；digest-only不足，duplicate loss不重复计数，receipt snapshot durable前不删child；
- 同一 extent generation 不同时属于两个 ledger instance；
- ALLOC 未 durable 时 DATA 不可被允许 ACK；
- FREE 未 durable 时 slot 不可复用；
- 未 commit move copy 不成为 authoritative；commit 后 index 丢失仍可重建，reader drain 前 source 不 free；
- checkpoint current selector等价full chain；orphan free不删除logical entry在current selector承载的既存success；
- conditional failure无effect；durable result覆盖自身sequence，duplicate op只有一个result；selector cut后不能取得new old-location pin；
- `OldBinaryBlocked`或isolated-new-scope fallback必须先于任何Segment effect；`FormatCompatible`或`RegistrationGenerationMatches`为false时不writable，缺少negative proof不能rollback；
- recovery 只发布最大连续可证明前缀；
- logical delete 后不能重新 open；
- unadmitted RepairIntent不授予grant/payload；delete fence与admission单序，cut前admitted intent全部freeze；admission response loss不复制intent/target，后续repair progress只推进owning domain head；
- effective obligation-changing assignment不能让stale generation继续writable；snapshot必须可应用，wipe/decommission proof由cluster接受且不能跨incarnation/device/scope重放；
- fast-path失败必须回退且不越hole；normal tail需要quorum-intersection absence；deadline/cancellation/authority loss不得伪造payload DATA_LOSS；
- recovered success蕴含同generation durable close/final-prefix publication。
- legacy `OK`蕴含matching durable recovered close；skip不产生recovery authority，generic projection不丢coordinator/admin rich outcome。

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
range repair verifier assertion, accepted-loss ordering and receipt compaction
sidecar child/head publication, snapshot+suffix, store-version/instance ABA and unknown mandatory state
local route claim, fence admission generation and registration readiness
DescriptorCanonical/WireNegotiated/ControlAuthorized/AuthorityRead and no-downgrade booleans
OldBinaryBlocked/FormatCompatible/RegistrationGenerationMatches, migration/device/incarnation generations and new-scope fallback
allocator generation reuse
per-Arena conditional apply/durable result, duplicate retry and selector/pin cut
same-Arena relocation, concurrent move and reader pin
checkpoint current-selector compression, orphan free and late move commit
offline Bookie delete and rejoin
RepairIntent inert-child/admission/delete-fence races, admission response loss and domain-local progress
delete stream gap, PREPARED/effective assignment handoff, applicable snapshot chunks and storage incarnation
range fast-path partial result, normal-tail proof, recovery outcome classification and point fallback where Model E applies
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
| A-REPAIR-LOSS (DEFERRED strong reset) | A | whole ledger fenced+CLOSED | descriptor/`F` binding, verifier assertion vs digest, loss ordering, proven repair and second loss |
| A-RECEIPT-COMPACT (DEFERRED strong reset) | A | bounded ranges in CLOSED ledger | child/page caps, snapshot publication, response loss and covered-child reclaim |
| A-SIDECAR | A | bounded authority domains | child/head ordering, snapshot+suffix, same/conflicting-payload retry, fallback/reclaim, store-version reset, instance reuse and referenced unknown mandatory record |
| A-LOCAL | A+D | local authority | Classic/Profile route claim before lazy create, independent normal/grant/readable facts, fence cut, stale handle and registration readiness |
| A-PROFILE-COMPAT | A | 3/3/2 | descriptor match, anonymous/authenticated-but-unauthorized/authorized control scope, normal/recovery/Profile opcode, negotiation, old-decoder Classic effect and no downgrade |
| A-FENCE-STALE | A | 3/3/2 | stale writer vs recovery fence, delayed responses |
| A-ACK-RESP | A | 3/3/2 | Add unknown/replacement, current ACK/domain set, ordered completion |
| A-POINT | A | 3/3/2 and 3/3/3 | point recovery, required hole/normal tail, durable close/restart without strong-publication/reset |
| C-REUSE | C | local | crash at alloc/data/free/reuse |
| C-CKPT | C | local | checkpoint current selector through `S`, fallback suffix and superblock/control-segment crash |
| C-MOVE | C | local | conditional move, orphan free vs late commit, own-sequence durable-through, index rebuild and reader drain |
| C-COND | C | local | predicate failure, group durability, response loss/duplicate retry, checkpoint cut, unknown record and selector/pin race |
| C-TAIL | C | local | proven uncommitted tail vs required prefix corruption vs unclassified boundary |
| C-WRITER | C | local | pool transfer/reuse with delayed old writer I/O and completion |
| C-SPACE | C | local | foreground/maintenance budgets, exhaustion, bounded debt and restart |
| AC-LOCAL-STORE | A+C | two Arenas | Bookie control-log partial durability, same-coordinate conflict and read-after-success |
| C-REBUILD | C | local | normal restart coverage vs full live-range index reconstruction |
| C-FORMAT | A+C | local | old-binary fence, partial required-device migration, unknown mandatory format, incarnation/readiness generations and unsafe rollback |
| D-OFF | D | historical ensembles | offline rejoin |
| D-RACE | A+D | E > W | delete vs ensemble/AutoRecovery |
| D-INTENT | A+D | closed fragment | inert child, admission/delete-fence winner, admission response loss/restart, domain progress, first payload, ensemble CAS and every crash boundary |
| D-LOGICAL | A+D | historical targets | tombstone/complete targets before logical success, offline cleanup pending and old reader before local barrier |
| D-PUBLISH (DEFERRED strong reset) | A+D | whole ledger fenced+CLOSED | token/prepare/publication/resolve, coordinator/sidecar outage, delete and pending loss |
| D-ACCESS | A+D | historical targets | local barrier vs old handle/grant/I/O, safe free and late response; no all-target wait for ordinary logical success |
| D-STRONG-ACCESS (DEFERRED) | A+D | all serving targets | independent strong-revocation completion requires every barrier/permanent service-isolation proof |
| D-STREAM | D | bounded streams | gaps, PREPARED/effective handoff, applicable snapshot chunks, terminal-proof scope/replay, incarnation and registration cut |
| CD-REUSE | C+D | local + cluster | delete, free, ledgerId reuse |
| E-FALLBACK | E | E > W | partial range, required/speculative hole, normal-tail proof, rich outcome/legacy projection, skip/marker handling, authority loss, durable close and point-oracle equivalence |

正式运行前锁定各启用scope的必需config；可增加配置，不得删除对应最低结构。表中DEFERRED增强项不是首批完整性分母，必须在输出中单列未验证；首批仍覆盖其disabled拒绝与发现旧token时保留解析的负向场景。不得在run后移除失败config。

当前模型为A/C/D及按能力启用的E，不存在Model B；上述A-FENCE-STALE/A-ACK-RESP保留stale writer、response loss和ordered completion场景，不恢复sequence/offset协议。

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

- 在足够 Bookie 可用且无持续故障时，install 最终 AVAILABLE 或明确失败；
- recovery authority 唯一时，predecessor 最终 SEALED 或明确失败；
- delete targets 最终响应或被 durable decommission 时，physical delete 最终完成。
- metadata可用且admission/freeze/targets可证明时，普通logical delete不因历史target离线等待全部屏障；强撤权/物理进展仍取决于目标或永久隔离证明；
- 增强scope启用且authority/store恢复可用、无持续竞争时，prepared completion最终按durable publication/delete winner resolve，不永久占用conflicting domain；
- 在锁定有界live set、admitted rate和维护调度份额下，maintenance可推进并使debt回落；超负载只要求有界拒绝及空间恢复后重新开放。

fairness assumptions 必须逐条记录。liveness 未完成不影响 safety counterexample 的有效性，但会使相应进展结论保持开放。

对本轮新增的prepared-operation恢复与空间回收进展，safety PASS不能替代相应进展Gate；缺少fairness/资源前提或未complete时该能力保持BLOCK/INCONCLUSIVE。TLC中的有界进展也不能代替Spike B真实I/O与长期churn数据。

`AckedPayloadSurvivesWithinBudget` 只证明 payload evidence survival，不证明继续写可用性、read quorum 可用性、metadata/auth authority 生存或 general E/W/A recovery liveness。超过 failure-domain 预算只终止 survival 保证，不自动证明 payload 丢失；只有 required-coordinate evidence 确定性耗尽后的 `DATA_LOSS` 才是明确 terminal state。没有证据却返回恢复成功仍是 safety violation。

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
sidecar operation identity payload aliases = 0
unadmitted RepairIntent grants/writes       = 0
admitted RepairIntents omitted from freeze = 0
admission response loss duplicated intent/target = 0
descriptor/auth/wire compatibility invariant violations = 0
old-binary/migration/readiness invariant violations = 0
```

每个锁定启用scope的必需config必须由checker报告complete，否则该scope为INCONCLUSIVE、不能PASS。延期scope单列DEFERRED，不计作PASS；局部结果必须命名scope，不声称整个Spike或全部增强能力通过。

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
