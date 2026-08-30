# RFC-0004：Batch/Range、Recovery 与 Cluster Delete

> 状态：**Proposed**<br>
> 依赖：[RFC-0001](RFC-0001-profile-capability-install.md)；Segment 本地回收依赖 [RFC-0003](RFC-0003-segment-storage-allocator.md)<br>
> 形式化验证：Model D；推进 general E/W/A 快速恢复时增加 Model E

## 1. 摘要

本 RFC 修正两个基线误差并补齐一个集群协议：

1. OSS 已存在 bounded Batch Read，不能把 Range Read 写成从零开始；新能力是 streaming continuation、per-entry result、missing bitmap、一般 E/W/A 合并、recovery semantics、QoS 和 cancellation。
2. `DEFERRED_SYNC_LEGACY` 不支持 failed Bookie 下的一般 ensemble change，不能作为首版 HA WAL durability。
3. Conditional delete 不能只是 Bookie 本地 extent reclaim；它必须冻结历史 ensembles，持久化 cluster tombstone，处理离线 Bookie、AutoRecovery race 和 rejoin barrier。

## 2. 范围

本 RFC 负责：

- 当前 BatchRead 能力边界；
- proposed streaming range request/response 语义；
- point/batch/range read 的错误与 cancellation；
- TailSummary 的证据边界；
- recovery-specific range merge 和 BatchRecoveryAdd 候选；
- general E/W/A 与 ensemble history；
- instance-specific RepairIntent、recovery-only authority 与 target history；
- LedgerDeleteCoordinator、manifest、逻辑/物理删除；
- 离线 Bookie、decommission、rejoin watermark；
- delete 与 open、ensemble change、AutoRecovery 的并发。

本 RFC 不负责：

- Sequence run takeover，见 [RFC-0002](RFC-0002-sequenced-wal.md)；
- Segment 本地 generation/free，见 [RFC-0003](RFC-0003-segment-storage-allocator.md)；
- Profile install wire details，见 [RFC-0001](RFC-0001-profile-capability-install.md)。

## 3. OSS 能力基线

当前能力应描述为：

```text
BATCH_READ_ENTRY exists
bounded by maxCount and maxSize
public async batch-read API exists
fast path has E = W and capability restrictions
RANGE_READ_ENTRY / RANGE_ADD_ENTRY are not general existing protocols
```

因此，后续 benchmark 必须同时包含：

- stock point read；
- stock bounded BatchRead 合法配置；
- proposed streaming range；
- fallback path。

不能把 stock BatchRead 的收益计入新协议增益。

## 4. Proposed Streaming Range Read

### 4.1 逻辑请求

```text
RangeReadRequest {
    ledgerId
    ledgerInstanceId
    startEntryId
    maxCount
    maxBytes
    continuationToken
    readMode
    priority
    deadline
    requestId
}
```

`readMode` 候选：

```text
NORMAL_BOUNDED
RECOVERY_EVIDENCE
SEQUENCE_LOOKUP
```

首版 wire schema、token 编码和 feature bit 由协议评审冻结。请求必须有硬 count/bytes/deadline 上限。

### 4.2 逻辑响应

```text
RangeReadResponse {
    requestId
    entries[]
    perEntryResult[]
    missingBitmap
    continuationToken
    observedLac
    evidenceDigest
    endReason
}
```

响应必须区分：

- entry present；
- entry known absent；
- replica unavailable/timeout；
- fenced/deleted/instance mismatch；
- response truncated by count、bytes、deadline 或 cancellation。

“本副本没读到”不能自动等于 quorum 意义上的 absent。

### 4.3 Continuation token

token 至少绑定 ledger instance、request mode、next coordinate 和 server/format generation，防止：

- ledgerId reuse 后 token 读到新 instance；
- delete/reuse 后 stale locator 返回新数据；
- 不同 read mode 之间复用证据；
- 客户端篡改 token 绕过 limits。

token 可以是 opaque，但 server 必须验证完整性。过期 token 返回明确错误并允许客户端按合同重新开始。

### 4.4 Flow control 与 cancellation

- server 每个 request/channel/tenant 有 bytes 和 entry 上限；
- cancellation 必须停止后续磁盘读取和网络发送；
- 已进入共享 I/O 的读取可以完成，但结果不得继续无界排队；
- recovery traffic 的 priority 不能饿死 foreground read/write；
- continuation 不隐含资源永久 pin。

## 5. 一般 E/W/A 读取与合并

在 `E > W` 下，单个连续 range 可能跨越不同 write set。ensemble change 又会引入历史 ensemble，因此 general merge 必须按每个 entry 的 write-set 和 metadata version 计算，不能只选一个 Bookie 的最长返回。

最低输入：

```text
sealed ledger metadata version
ensemble history
E/W/A
entryId -> write set mapping
per-replica results and checksums
fencing/recovery state
```

正常读取和 recovery evidence 的成功标准不同：

- normal read 可以在现有 BookKeeper read contract 下尽早返回；
- recovery 必须收集足以证明 entry 已达到可恢复条件或不可继续的证据；
- missing bitmap 是传输格式，不是 quorum proof；
- checksum/instance 冲突必须升级为 corruption/recovery error，不能任取一份。

具体 merge 算法必须在实现前单独给出伪代码、复杂度和 Model E；本文不以“取多数最长 tail”代替一般证明。

## 6. TailSummary

候选摘要：

```text
EntryTailSummary {
    ledgerId
    ledgerInstanceId
    localLastEntryId
    localDurableEntryId
    observedLac
    fencedOrSealed
    summaryGeneration
    checksum
}

SequenceTailSummary {
    sequenceDomainId
    runId
    ledgerInstanceId
    localLastSequence
    contiguousSequenceFrontier
    indexGeneration
    checksum
}
```

TailSummary 的定位：

- 用于快速选择读取范围和发现明显落后副本；
- 单副本 summary 不是 AQ 或最大连续恢复前缀的证明；
- summary 必须绑定 instance/generation；
- 丢失或损坏时 fallback 到权威 scan；
- general E/W/A 下仍需按 write set 合并证据。

如果 summary 本身进入 quorum proof，必须另行冻结更新 durability、单调性和 ensemble-change 合同。

## 7. Recovery-specific Range

候选流程：

1. 读取 sealed/fenced ledger metadata 与历史 ensemble；
2. 获取各候选 Bookie TailSummary，只用于确定安全上界和优先级；
3. 按 write-set 分段发出 `RECOVERY_EVIDENCE` range；
4. 合并 per-entry evidence；
5. 找到最大可证明连续前缀；
6. 对缺失副本执行现有 recovery add 或经接受的 BatchRecoveryAdd；
7. 更新/close ledger；
8. 生成 immutable recovery receipt。

所有步骤必须有 count、bytes、wall-clock 和 retry 上限。超过上限时返回明确的 non-promotable/需要人工处理状态，不能假装快速恢复完成。

### 7.1 BatchRecoveryAdd 候选

BatchRecoveryAdd 只优化传输与本地 I/O，不改变每个 entry 的 recovery 权限和幂等语义。

最低约束：

- 每个 entry 保留独立 entryId、checksum 和 result；
- batch 部分成功可以精确重试；
- 已存在相同 entry 幂等成功，不同 payload 冲突；
- fencing/master-key/recovery flag 与现有单 entry recovery 等价；
- request/response 有硬资源上限；
- 旧 Bookie 不支持时回退到现有单 entry path。

不在正式模型和兼容测试前宣称 batch 是原子操作。

## 8. Deferred Sync 限制

Profile 状态：

```text
SYNC_ON_ACK
    first-version production WAL default

DEFERRED_SYNC_LEGACY
    existing OSS behavior
    no normal ensemble change after failed Bookie
    not a general HA WAL contract
```

`DEFERRED_SYNC_V2` 如需推进，必须单独定义 durability barrier、failure detection、ensemble replacement、recovery 和 response-loss 语义。

## 9. AutoRecovery Repair Intent 与 LedgerDeleteCoordinator

### 9.1 Durable instance-specific RepairIntent

现有 underreplication marker 只拥有“ledger 需要检查/修复”和 missing replica scheduling 事实；worker lock 只提供当前执行者排他性。二者都不保存 target、fragment、instance 或 operation identity，也不能承担 crash 后的 recovery authorization 与 delete discovery。

任何 target 接收该 ledger instance 第一份 durable recovery payload 前，必须先有 cluster-authoritative、可由 delete freeze 枚举的 durable repair intent：

```text
RepairIntent {
    ledgerId
    ledgerInstanceId
    repairOperationId
    profileDescriptorHashOrGeneration
    baseLedgerMetadataVersion
    fragmentStart
    fragmentEndOrCanonicalRangeIdentity
    oldEnsembleDigest
    replacedMemberOrSlot
    targetBookie
    recoveryOnlyAuthorityGeneration
    lifecycleOrResultState
}
```

字段名和物理 schema 保持开放，但上述语义身份不可省略。`replacedMemberOrSlot` 必须持久化，因为 `replaceEnsembleEntry` 可能覆盖旧 membership，delete 仍需发现旧成员；本次实际从哪个 surviving replica 读取不属于 authority，可以动态重试且不得制造 metadata churn。不得向 MetadataStore 写每 entry progress、receipt、reader history 或无界 retry ID。

最低 AutoRecovery 顺序：

```text
1. CAS create instance-specific RepairIntent
2. target durable install, normal-inactive
3. target durable grant RECOVERY_ONLY(intent generation)
4. copy through the existing recovery Add data path
5. after copy, reread and validate standard LedgerMetadata
6. CAS standard ensemble replacement
7. transition target to COMMITTED_REPLICA/READABLE
   - close/revoke RECOVERY_ONLY
   - do not grant normal writable authority
8. durable mark intent COMMITTED
9. compact intent only after delete-history or cleanup authority safely supersedes it
```

CAS response loss 必须通过重读 exact fragment/replacement mapping 解析，不能盲选新 target。closed/historical fragment 的 committed target 不得 normal-active；只有 target 另行成为 current writable fragment member，并满足 RFC-0001 写期 replacement 的 post-CAS membership、fence 和 normal activation 合同，才能独立获得 normal writable authority。

Intent 生命周期语义至少区分 PREPARED/RECOVERY_AUTHORIZED、COMMITTED、ABORTED 或 ORPHAN_CLEANUP_PENDING。CAS 前已经接收 payload 的 intent 不得直接删除；COMMITTED 仍保留 old member/target history；只有这些身份已进入另一个不会丢失的 durable delete-history authority，或 target-local durable cleanup proof 已成立，才能 compact。“tombstone”不能把 target 从 delete enumeration 中移除。

物理上是否复用 underreplication namespace 保持开放，但语义 owner 固定为：

- underreplication marker：missing replica scheduling；
- worker lock：临时排他执行；
- standard LedgerMetadata：唯一最终 ensemble membership；
- RepairIntent：pre-publication target、recovery-only authorization 与 delete history；
- Bookie local authority：target 对该 intent 的 durable recovery-only acceptance。

新增成本严格限定为每 repair operation/fragment 的 intent create CAS、一次 target recovery-only control durability、现有 ensemble CAS 与 intent completion CAS。recovery payload 热路径不增加 per-entry metadata round trip、per-entry control fsync 或 per-entry intent update。

### 9.2 LedgerDeleteManifest

删除需要一个集群级协调器。逻辑 manifest：

当前 [`HandleFactoryImpl`](../../../bookkeeper-server/src/main/java/org/apache/bookkeeper/bookie/HandleFactoryImpl.java) 的 `recentlyFencedAndDeletedLedgers` 只是 Bookie 进程内、按访问 7 天过期的 cache，用于降低删除后冲突写风险；它既不持久化，也不枚举历史 ensembles，不能作为 cluster tombstone 或 rejoin 证明。

```text
LedgerDeleteManifest {
    ledgerId
    ledgerInstanceId
    deleteRequestId
    deleteEpoch

    sealedMetadataVersion
    ensembleHistoryDigest
    historicalBookies[]

    state
    acknowledgedBookies[]
    decommissionedBookies[]
    unrecoverableBookies[]

    createdAt
    retentionDeadlineOrPolicy
}
```

manifest 是权威 metadata 的一部分或由同等 CAS 语义的专用命名空间持久化。

## 10. Cluster delete 状态机

```text
ACTIVE / SEALED
    ↓
DELETE_INTENT
    ↓
ENSEMBLES_FROZEN
    ↓
LOGICALLY_DELETED
    ↓
PHYSICAL_DELETE_DISPATCHED
    ↓
PHYSICAL_DELETE_PENDING
    ↓
PHYSICALLY_DELETED
    ↓
TOMBSTONE_COMPACTABLE
```

### 10.1 DELETE_INTENT

通过 MetadataStore CAS：

- 绑定 ledgerId + instance + deleteRequestId；
- 阻止新的 open/append；
- 冻结或串行化 ensemble change；
- 阻止 AutoRecovery 为该 instance 创建新副本；
- 固定 ledger metadata version。

如果 ledger 尚未 seal，是否由 delete 协调器触发 fence/recovery，或要求调用方先 seal，是接受前开放项；不能跳过并发写入处理。

### 10.2 ENSEMBLES_FROZEN

从固定 metadata version 枚举所有历史 ensembles，生成 immutable target Bookie set 和 digest。

必须覆盖：

- initial ensemble；
- 每次 ensemble change 的旧/新成员；
- incomplete、completed、aborted-but-dirty RepairIntent 中的 replaced member 与 target；
- 已记录但尚未完成的 replacement。

目标集合生成后，新 replica creation 对该 instance 必须被拒绝。

### 10.3 LOGICALLY_DELETED

cluster tombstone durable 后，客户端视角 ledger 已删除：

- open/read/add 返回 deleted/instance-specific error；
- metadata 不得回到 OPEN；
- logical completion 不代表所有物理 bytes 已回收；
- tombstone 必须可被重启和 watcher 丢失后的组件重新发现。

### 10.4 PHYSICAL_DELETE_DISPATCHED/PENDING

协调器向每个 historical Bookie 发送 instance-specific delete。离线节点保持 pending；调用方可查询进度。

### 10.5 PHYSICALLY_DELETED

每个目标 Bookie 必须满足其一：

```text
durable local delete receipt received
permanently decommissioned with durable cluster proof
device/node declared unrecoverable by an accepted administrative process
```

超时或暂时离线不能自动等于 decommissioned。

### 10.6 TOMBSTONE_COMPACTABLE

只有全部目标被解释且满足最大允许重新加入窗口后，manifest 才能压缩。压缩后仍至少保留：

```text
ledgerId
ledgerInstanceId
deleteEpoch
terminal state/proof digest
```

不能固定使用“7 天内存 cache”作为集群 tombstone 合同。

## 11. Bookie 本地删除

候选请求：

```text
DELETE_LEDGER_INSTANCE {
    ledgerId
    ledgerInstanceId
    deleteRequestId
    deleteEpoch
    sealedMetadataVersion
    ensembleHistoryDigest
}
```

Bookie 必须：

1. 验证 instance、delete epoch 和授权；
2. durable 写本地 delete tombstone；
3. 阻止新 reader/writer；
4. drain active reader、zero-copy pin 和 cache reference；
5. 对 Classic/Direct 路径执行对应清理；
6. 对 Segment 路径按 RFC-0003 执行 invalidate 与 durable free/generation bump；
7. 持久化 `localDeleteAppliedThrough` 或等价 watermark；
8. 返回绑定 local generation 的 durable receipt。

重复请求幂等。相同 request 不同 instance/digest/epoch 必须冲突。

## 12. 离线 Bookie 与 rejoin barrier

Bookie 重新注册为 writable 前必须证明：

```text
localDeleteAppliedThrough >= clusterRequiredDeleteEpoch
```

候选启动流程：

```text
1. register as RECOVERING or READ_ONLY
2. fetch unapplied delete manifests/tombstones
3. apply local deletes and generation bumps
4. persist applied watermark
5. reconcile AutoRecovery/device state
6. only then register writable
```

长期离线节点带着旧 instance 数据直接返回 writable 是 safety violation。

集群必须定义 required watermark 的 authority、manifest pagination、丢失历史补偿和 bootstrap 行为。

## 13. API 完成语义

API 必须区分：

```text
deleteLedgerAsync()
    -> logical delete completion

awaitPhysicalDeletion()
    -> every historical target resolved and physical state terminal
```

名称仍可调整，但一个 boolean success 不能同时表示“客户端已不可见”和“所有已知副本空间已回收”。

进度查询至少返回：

- manifest state；
- target/ack/pending/decommissioned counts；
- unresolved Bookie IDs；
- logical completion time；
- physical completion time；
- tombstone retention state。

## 14. 并发规则

### 14.1 Delete vs ensemble change

标准 ensemble membership 仍以 LedgerMetadata CAS 串行化；delete/repair operation 还必须共享同一 ledger-instance control generation/CAS。DELETE_INTENT 获胜后，新的 ensemble change/repair intent 失败；ensemble change 或 repair intent 已提交时，delete 重新读取并将其纳入 frozen history。不能用“先 list child，再写 DELETE_INTENT”的非原子顺序冻结 targets。

### 14.2 Delete vs AutoRecovery

AutoRecovery 在创建 RepairIntent、授予 recovery-only authority、写 payload 和发布 ensemble 前都检查 instance control generation 与 delete state。DELETE_INTENT 先赢后不得创建新 intent、授予 recovery authority或产生该 instance 的新 replica。

RepairIntent 先赢时，delete frozen target 必须包含其 replaced member 与 target，无论 copy/CAS 是未开始、部分完成、COMMITTED 还是 aborted-but-dirty。该 intent 在 target 接收第一份 durable payload 前已存在，因此 metadata 尚未发布的 target 也可被集群完整发现。exact child enumeration、watermark/index 和 compaction encoding 保持开放。

### 14.3 Delete vs open/read

logical tombstone 是最终 authority。已打开 reader 在 local delete 前被 drain；新的 reader 被拒绝。是否允许 tombstone 前已开始的 read 完成必须形成统一合同，不能因存储引擎不同而变化。

### 14.4 Delete vs ledgerId reuse

所有操作绑定 `ledgerInstanceId`。新 instance 不能因旧 tombstone 被误删，旧 locator 不能因 ledgerId 相同读取新数据。

## 15. Tombstone retention

retention 至少覆盖：

```text
all historical targets acknowledged or durably decommissioned
+ maximum supported offline/rejoin window
+ metadata propagation and backup restore window
```

具体时间不是硬编码常量；它属于集群 policy，并进入 immutable delete receipt。永久或长周期 compact tombstone 用于防止极晚重返和 ledgerId reuse ABA。

## 16. 安全不变量

### 16.1 Range/Recovery

1. 单副本 missing 不被当作 quorum absence。
2. range optimization 不改变 point-read/recovery 的 entry 事实。
3. general E/W/A 合并按 entry write set 和 ensemble history 计算。
4. TailSummary 失效时回退，不伪造最大恢复前缀。
5. BatchRecoveryAdd 部分失败可精确重试，不引入不同 payload 覆盖。
6. recovery 只发布最大可证明连续前缀。

### 16.2 Delete

1. LOGICALLY_DELETED 后该 ledger instance 永远不能重新 OPEN。
2. frozen target set 覆盖固定 metadata version 的历史 ensembles，以及所有 incomplete/completed/aborted-but-dirty RepairIntent 的 replaced member 与 target。
3. unresolved offline Bookie 不能被超时自动解释为物理删除。
4. Bookie 缺失 required tombstone 时不能注册 writable。
5. local free/reuse 晚于 durable tombstone 和 reader drain。
6. 新 ledger instance 不受旧 instance delete 请求影响。
7. AutoRecovery 在 DELETE_INTENT 后不能创建或发布新副本。
8. PHYSICALLY_DELETED 只在每个 target 有 durable terminal proof 时成立。
9. target 的第一份 durable recovery payload 晚于可由 delete freeze 枚举的 RepairIntent。
10. recovery-only authority 永不隐式授予 normal writable authority。

## 17. Model D/E 最低场景

Model D：

- delete 与 ensemble change 同时 CAS；
- delete 与 RepairIntent create/recovery authority/first payload/ensemble CAS 逐边界竞争；
- repair target 收到部分或全部 payload、CAS 前 crash，restart 后仍可枚举并清理；
- ensemble CAS response loss 与 intent COMMITTED response loss；
- 部分 Bookie 收到请求、response loss、协调器 crash；
- Bookie 长期离线后 rejoin；
- local tombstone durable 前后 crash；
- reader drain、free、generation bump 各边界；
- ledgerId reuse/new instance；
- decommission 与恢复注册竞争。

Model E 在推进 general E/W/A fast recovery 时覆盖：

- 3/3/2、3/3/3、3/2/2；
- `E > W` write-set rotation；
- ensemble change；
- partial range response、missing bitmap 和 timeout；
- TailSummary stale/corrupt；
- batch recovery partial success；
- response loss 和 recovery coordinator crash。

模型不能依赖 durable blob store。

## 18. 接受 Gate

本 RFC 进入 Accepted 前必须：

- 当前 OSS BatchRead 行为和限制有 executable baseline tests；
- streaming continuation、per-entry result、limits 和 cancellation wire contract 冻结；
- normal read 与 recovery evidence 的成功条件分别定义；
- general E/W/A merge 有伪代码、复杂度、Model E 和故障测试；
- RepairIntent identity、retention、target discovery 与 delete/repair CAS 线性化点冻结；
- DeleteManifest schema、target freeze 和 CAS 线性化点冻结；
- offline Bookie rejoin watermark 有集群级端到端测试；
- logical/physical API completion 可分别观测；
- Model D 无 safety counterexample；
- Segment local reclaim 与 RFC-0003 generation tests 联动通过。

允许分阶段接受：Batch/Range 和 Cluster Delete 可以成为同一 RFC 的独立 feature gate；任何一部分通过不得自动提升另一部分。

## 19. 开放问题

- streaming range wire opcode 与现有 BatchRead 的兼容/复用方式；
- continuation token 的签名、过期和跨 Bookie 行为；
- general E/W/A recovery merge 的精确算法；
- TailSummary 是否仅为 hint，还是进入未来 quorum proof；
- recovery scan 的 hard bounds 和超限运维流程；
- Delete Coordinator 的部署、leader election 和 manifest namespace；
- RepairIntent exact path、child enumeration/watermark/index、状态名和 compaction encoding；
- recovery-only local record packing、BatchRecoveryAdd wire schema 与 batch limits；
- decommission/unrecoverable 的授权流程和 durable proof；
- maximum rejoin window 与 compact tombstone 生命周期；
- Classic/Direct/Segment reader drain 的统一 API；
- physical deletion receipt 的审计与 metrics。

这些问题关闭前，不得把 Range、general E/W/A fast recovery 或 Conditional Delete 标为 Implementation Ready。
