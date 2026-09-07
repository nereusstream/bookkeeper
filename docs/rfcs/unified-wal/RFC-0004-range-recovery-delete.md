# RFC-0004：Batch/Range、Recovery 与 Cluster Delete

> 状态：**Proposed**<br>
> 依赖：[RFC-0001](RFC-0001-profile-capability-install.md)；Segment 本地回收依赖 [RFC-0003](RFC-0003-segment-storage-allocator.md)<br>
> 形式化验证：Model D；推进 general E/W/A 快速恢复时增加 Model E

## 1. 摘要

本 RFC 修正两个基线误差并补齐一个集群协议：

1. OSS 已存在 bounded Batch Read，不能把 Range Read 写成从零开始；新能力是 streaming continuation、per-entry result、missing bitmap、一般 E/W/A 合并、recovery semantics、QoS 和 cancellation。
2. `DEFERRED_SYNC_LEGACY` 不支持 failed Bookie 下的一般 ensemble change，不能作为当前 HA WAL durability。
3. Conditional delete 不能只是 Bookie 本地 extent reclaim；它必须冻结历史 ensembles，持久化 cluster tombstone，处理离线 Bookie、AutoRecovery race 和 rejoin barrier。

2026-09-07基于`c79357d76f95dae4c27ffbddcc075b6385991daa`后的范围修订：首批实现基础点恢复和已fenced-close ledger的普通逻辑删除；普通删除允许旧reader在目标本地tombstone生效前继续读取。强访问撤销独立延期；strong completion/loss-window reset首批禁用，后续候选只允许整个ledger已fenced且durable CLOSED。以下增强协议保留为条件化合同，不成为普通Add、写期replacement、基础恢复或普通删除的隐含前置；全部仍为Proposed / Planned，未新增执行证据。

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
```

当前payload合同为opaque ledger，不提供独立sequence lookup；上层位置由client/protocol adapter解释。

Profile range/read必须位于RFC-0001独立Profile endpoint及mandatory subtype中；不能复用legacy Add/Read optional字段，也不能在unknown/partial/response loss后降级Classic。Round 7只为executable corpus预留`RANGE_READ=0x0301`，当前capability set必须absent、body不接受并返回UNSUPPORTED；正式wire schema、token与hard count/bytes/deadline在Model E/API闭合后直接修改当前capability合同并评审，不能把预留opcode写成已实现能力或另建并行代际路线。

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
- 单副本 checksum/identity corruption 只使该 evidence 失效，必须继续向其他合法 replica 取证；只有 contract-required coordinate 的有限合法 evidence 被确定性耗尽时才进入 payload `DATA_LOSS`，不可调和的 identity/quorum-proof conflict 进入 `QUARANTINED`，不能任取一份或混淆两类终态。

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

```

TailSummary 的定位：

- 用于快速选择读取范围和发现明显落后副本；
- 单副本 summary 不是 AQ 或最大连续恢复前缀的证明；
- summary 必须绑定 instance/generation；
- 丢失、损坏、stale 或 unsupported 时 deterministic fallback 到权威 point-read/evidence scan；
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

所有步骤必须有 count、bytes、wall-clock 和 retry 上限，但必须区分 fast-path 局部预算与整个 attempt 的 deadline/cancellation。fast-path budget 耗尽触发 deterministic fallback，不改变 recovery truth；authoritative fallback 临时不可用时返回 retryable/deferred 语义；caller deadline/cancellation 只使本次 attempt incomplete。不可调和 conflict 或必要 authority 无法恢复进入 quarantine；只有 contract-required coordinate 的合法 evidence 被确定性耗尽时才是 payload DATA_LOSS。

Profile recovery payload只能使用distinct `PROFILE_ADD_RECOVERY` logical operation，并消费target本地bounded grant；normal Profile Add、legacy `ADD_ENTRY` optional field或legacy recovery flag都不能替代。任何unsupported/response-loss路径只能重试/query同一Profile operation，不能Classic downgrade或双写。

### 7.1 Deterministic point-recovery fallback

range fast path 必须冻结一个 immutable `RecoveryContext`，至少绑定 ledger instance、sealed/fenced metadata version/digest、ensemble history、E/W/A 与 write-set mapping、fence/recovery generation，以及 RepairIntent/delete control generation。unsupported capability、TailSummary stale/invalid、partial range response 或 fast-path count/bytes/retry budget exhaustion 时：

```text
1. independently validate every returned coordinate/evidence
2. identify the earliest unresolved coordinate
3. resume with bounded-concurrency point-read/evidence merge
4. use idempotent single-entry recovery-add for unresolved entries
5. never publish a frontier across an earlier hole
```

同一 RecoveryContext 下的 later verified evidence 可以在有界内存内复用，也可以安全重读；本地未缓存 proof 不能被解释为 quorum absence。authority generation 变化时必须重读并证明 context 等价，或在新 generation 下重新开始。

continuation、bitmap 和 proof cache 默认可以是 bounded volatile state。coordinator crash 后从 durable RepairIntent 与 immutable RecoveryContext 重读；不要求 per-entry MetadataStore progress、per-entry durable checkpoint 或 per-entry control fsync。若 benchmark 证明 operation-scoped checkpoint 必需，它只能是绑定 RepairIntent generation/context digest 的可丢弃优化，exact encoding/durability 保持开放。

### 7.2 BatchRecoveryAdd 候选

当前manifest明确禁用`BATCH_RECOVERY_ADD=0x0302`：不advertise capability、accepted body/count为0并返回UNSUPPORTED。single-entry recovery只使用RFC-0001 executable manifest的`ADD_RECOVERY=0x0202`，至少携带60-byte ledger context、`repairIntentId[16]`、intent/grant generation、range start/end、entryId及20-byte credential；range必须包含entryId，target incarnation来自matching HELLO/local grant。该layout在raw old-decoder Gate通过前仍不是stable production wire。

未来BatchRecoveryAdd只优化传输与本地I/O，不改变每个entry的recovery权限和幂等语义。启用前最低约束：

- 每个 entry 保留独立 entryId、checksum 和 result；
- batch 部分成功可以精确重试；
- 已存在相同 entry 幂等成功，不同 payload 冲突；
- fencing/master-key/recovery flag 与现有单 entry recovery 等价；
- request/response 有硬资源上限；
- batch capability不支持时，Profile ledger只回退到distinct single-entry Profile recovery operation；Classic ledger才可继续现有legacy single-entry path。Profile ledger遇旧Bookie/mixed ensemble必须在payload前fail closed，不得降级到legacy recovery flag。
- partial success 或 response loss 按 entry 验证并精确重试；最早 unresolved coordinate 阻止 frontier，不能把 batch 当作原子或跳过 hole。

不在正式模型和兼容测试前启用batch，更不能宣称batch是原子操作。

### 7.3 Required frontier、normal tail 与 outcome taxonomy

RecoveryContext 必须先从 accepted durable authority 推导 required coordinate。至少包括：CLOSED metadata 的 `[0,lastEntryId]`、fenced context 下 quorum/coverage-proven LAC 及此前连续 prefix、冻结的 historical fragment，以及 accepted ACK/AQ/repair-completion authority。client pending Add、`lastAddPushed`、单副本 payload、TailSummary maximum、speculative range result 或 later payload 单独都不构成 required authority。

open-ledger recovery 的首个 missing coordinate `x` 只有同时满足以下条件，才能证明 normal tail `P=x-1`：

- ledger 已在同一 generation 下 fenced，且所有 `<x` required coordinate 已无洞恢复；
- `x` 位于 quorum-proven committed frontier之后；
- exact write set 上有与每个可能 Ack quorum 相交的 definitive absence coverage；Round-robin `E/W/A` 基线至少为 `W-A+1` 个 distinct write-set members；
- timeout、offline、connection failure 或 corrupt response 不算 definitive absence；
- 没有 accepted authority 证明 `x` 或更晚 coordinate 属于 required committed prefix。

较早 hole 后的 later payload 若没有 required authority，只是必须 suppress 的 speculative suffix，不得把 hole 命名为 DATA_LOSS；若 later accepted authority 使该 coordinate required，则根据证据进入 deferred、quarantine 或 DATA_LOSS，且绝不能发布跨 hole prefix。

顶层必须保留五类语义，exact Java enum/exception/admin API继续BLOCK；RFC-0001 transport status不替代本taxonomy：

```text
RECOVERED_AND_CLOSED(P)
    required prefix and normal tail proven; recovery-add and close publication durable

RETRYABLE / DEFERRED
    temporary authority/evidence unavailability, no quorum, or global operational bound

ATTEMPT_INCOMPLETE
    caller cancellation/deadline; no terminal ledger assertion

QUARANTINED
    irreconcilable payload/identity conflict, or AUTHORITY_UNRECOVERABLE

DATA_LOSS / EVIDENCE_EXHAUSTED
    a contract-required coordinate has a finite frozen source set and every legal source
    is definitively missing, permanently lost, or invalid
```

必要 authority 无法恢复意味着系统不能确定 required set，必须作为 quarantine/authority failure 与 payload DATA_LOSS 分开。暂时 offline/no-quorum/global bound 不是 evidence exhausted。success 只有在同一 generation 下 recovery-add 完成且 close/final prefix durable publication 后返回；normal tail 或 computed `P` 仍只在内存时不能成功。

API 语义至少携带 outcome class、retryable/terminal、RecoveryContext/authority generation、success prefix、相关 first unresolved/required coordinate、reason category 与 durable terminal-publication 标志。metrics 必须分别记录 normal-tail success、copy recovery、fallback reason、deferred、incomplete、conflict quarantine、authority-loss quarantine、required-coordinate exhaustion 和 suppressed suffix；timeout/cancel/single corruption/authority loss 不计入 `data_loss_total`，ledger ID 不作为高基数 label。

### 7.4 Rich outcome 与 legacy compatibility projection

现有 ledger recovery最终只向 public callback/future暴露整数 `rc`/handle；它不能承担五类 authority。实现必须先产生 additive internal rich outcome，再向legacy接口做有损但安全的投影。internal result至少语义绑定：

```text
semantic outcome class + operation scope
ledgerId + ledgerInstanceId
RecoveryContext identity/digest + attempt/operation generation
proven prefix/range and close metadata identity for success
bounded reason/cause + retryability/terminality
authority-conflict vs payload-evidence classification
completion/close generation where applicable
```

fragment/range repair不能命名为ledger `RECOVERED_AND_CLOSED`。ordinary read/add API无需新增这些字段；它们由recovery coordinator、AutoRecovery scheduler、additive admin/status API和bounded metrics消费。exact类型名、public稳定性和wire保持开放。

legacy projection锁定为：

| Semantic outcome | Legacy callback/future | Authority/scheduler |
| --- | --- | --- |
| durable `RECOVERED_AND_CLOSED(P)` | `OK` + handle | 唯一ledger recovery success |
| `RETRYABLE/DEFERRED` | non-OK；优先保留现有Timeout/Bookie/MetaStore/Read等transient cause，无精确码时generic recovery failure | backoff/retry，保留intent/marker |
| `ATTEMPT_INCOMPLETE` | cancellation/Interrupted/Timeout或generic non-OK | 不写terminal state；durable progress重验 |
| `QUARANTINED` | generic recovery non-OK | 隔离并保留conflict/authority reason |
| `DATA_LOSS/EVIDENCE_EXHAUSTED` | generic recovery non-OK | terminal/non-promotable，不伪造close |

只有matching durable close才能映射legacy `OK`；所有其他outcome必须non-OK，且generic rc不能擦除internal/admin rich outcome。`DataUnknownException`虽概念上接近quarantine，但当前legacy factory兼容不完整；是否修复factory、协商新code或继续generic mapping保持OPEN，不能提前锁为最终码或未经协商返回旧client。

现有Classic首个 `NoSuchEntry/NoSuchLedger` 即终止tail的行为不能直接复用于general E/W/A Profile recovery；新路径必须先满足第7.3节normal-tail oracle。`BookKeeperAdmin` 的 legacy `skipUnrecoverableLedgers` 可以保留aggregate completion，但skipped ledger必须出现在rich result中，不计recovered success，不清RepairIntent/underreplication/loss state，也不发布repair completion/reset；Profile automation不能消费aggregate `OK`作为authority。AutoRecovery/ReplicationWorker必须按rich outcome分别retry、结束attempt、quarantine或terminal，并只在durable metadata/receipt重验后clear marker。

close response loss按以下顺序解析：

```text
1. verifier determines P in exact RecoveryContext
2. required recovery Adds become durable
3. standard metadata CAS publishes CLOSED(P, length, exact context/membership)
4. on response loss, reread metadata
   matching CLOSED(P/context)       -> RECOVERED_AND_CLOSED
   metadata temporarily unavailable -> DEFERRED
   still IN_RECOVERY                -> retry same close operation
   incompatible CLOSED/prefix/instance -> QUARANTINED_CONFLICT
```

cancel/deadline只终止当前attempt，不能回滚已提交fence/payload/metadata；若与close CAS并发，status路径先重读authority。single corrupt replica只记endpoint corruption并继续其他evidence；valid-looking conflict进入quarantine，payload仍在但required authority丢失进入authority quarantine，只有required-coordinate finite evidence exhausted进入data loss。metrics reason/phase必须bounded，attempt与unique durable completion分开；metrics不是authority，不为exactly-once计数增加durable hot state。

### 7.5 首批基础点恢复必须独立闭合

基础恢复与streaming range分别接受。首批可保留数据的Segment canary必须先在RFC-0001 §9.4的受支持矩阵完成以下可执行算法；Range、TailSummary和BatchRecoveryAdd仍可disabled：

```text
CAS OPEN -> IN_RECOVERY under Profile metadata authority
    -> durable fence coverage for every possible current write set
    -> freeze instance, ensemble history, E/W/A and recovery context
    -> collect authoritative LAC/required-frontier evidence
    -> point-read each unresolved coordinate from its exact legal source set
    -> apply §7.3 normal-tail / deferred / quarantine / loss rules
    -> admitted, bounded single-entry Profile recovery Add
    -> standard metadata CAS publishes matching durable CLOSED(final prefix, length, context)
       while the same-record membership freeze and unresolved enhanced token are absent
    -> resolve the close result and retain admitted target/cleanup history
    -> reconcile response loss and revalidate after restart
```

若读取到IN_RECOVERY，重读并绑定同一恢复上下文后继续；若已CLOSED，保持原final prefix并验证其durable结果，不能重新OPEN。CLOSED但缺少删除所需fence proof时，对固定最后fragment补做fence coverage并重验metadata/context，不能直接把writer close当成fenced close。metadata/context改变时重新取证或defer，不覆盖已有close结果。

这里的point-read可读取LAC之后已正确持久化且可定位的候选，Bookie不能按`entryId > localLAC`拒绝或返回不存在。例：副本LAC=99，entry 100已达ACK quorum，writer在下一次piggyback前崩溃，必须读取100后再按本节取证/恢复写入/close判断；单份候选不等于quorum commit。客户端confirmed read由ReadHandle的已确认LAC/CLOSED边界限制，显式unconfirmed和恢复取证采用各自范围；已被当前恢复终态排除的数据不能因物理残留重新可见。locator/index覆盖未恢复或storage unknown返回not-ready/unknown，不计definitive absence。该分层消费RFC-0005 §8既有点读和本地权限检查，不新增每次读取的metadata查询。

基础`RECOVERED_AND_CLOSED`以matching durable close为完成事实，不创建`pendingPublication`，不等待domain prepare/lifecycle strong-publication，也不重置loss window。响应丢失按§7.4重读同一close/context；发现删除或不兼容上下文时返回deleted/authority-changed或对应non-OK，不回滚CLOSED、不重新OPEN。与随后删除重叠的已提交close可以有迟到response；它只证明历史恢复完成，不重新授予open或写权限。新open仍检查authoritative tombstone。强reset启用后的额外assertion与基础恢复结果分别查询，不能把两者合成一个成功位。

实现必须给出source selection、write-set覆盖、并发/内存/重试上限和终止判据的伪代码，并提供独立point oracle。`W-A+1`只计fenced context下的definitive absence，timeout/offline不计；单副本payload、TailSummary或后续speculative entry不能代替required frontier。新的repair target、grant、membership publication及history retention仍遵守§9.1及§14的delete竞争协议；normal/recovery相同应用数据的LAC/digest差异按RFC-0005 §6.1处理。

接受场景包括ACK response loss、旧writer失联、LAC=99/候选100的confirmed与恢复读分离、单副本missing/corrupt、预算内domain loss、required hole、正常未提交tail、换组历史、recovery Add/close每个crash cut和重启后再次读取；同批其他entry失败及原RPC超时不抹除有效恢复候选。没有基础恢复证据的Add/ACK实验只允许discardable数据，不作为可恢复WAL canary。Model A先承担受支持子集的point recovery、durable close和outcome检查；Model E负责扩展range与point oracle的等价性，不能借Model E延期来豁免基础恢复。

## 8. Deferred Sync 限制

Profile 状态：

```text
SYNC_ON_ACK
    current production WAL default

DEFERRED_SYNC_LEGACY
    existing OSS behavior
    no normal ensemble change after failed Bookie
    not a general HA WAL contract
```

如需推进可换ensemble的deferred durability，必须直接修改当前durability合同并同步定义barrier、failure detection、ensemble replacement、recovery、response-loss语义与Gate；不另立并行代际合同。

## 9. AutoRecovery Repair Intent 与 LedgerDeleteCoordinator

### 9.1 Durable instance-specific RepairIntent

现有 underreplication marker 只拥有“ledger 需要检查/修复”和 missing replica scheduling 事实；worker lock 只提供当前执行者排他性。二者都不保存 target、fragment、instance 或 operation identity，也不能承担 crash 后的 recovery authorization 与 delete discovery。

任何 target 接收该 ledger instance 第一份 durable recovery payload 前，必须先有 cluster-authoritative、可由 delete freeze 枚举的 durable repair intent：

```text
RepairIntent {
    ledgerId
    ledgerInstanceId
    repairOperationId
    repairOperationGeneration
    profileDescriptorHashOrGeneration
    baseLedgerMetadataVersion
    fragmentStart
    fragmentEndOrCanonicalRangeIdentity
    oldEnsembleDigest
    replacedMemberOrSlot
    targetBookie
    targetStorageIncarnation
    admissionLifecycleFenceGenerationOrRef
    recoveryOnlyAuthorityGeneration
    lifecycleOrResultState
}
```

字段名和物理 schema 保持开放，但上述语义身份不可省略。`replacedMemberOrSlot` 必须持久化，因为 `replaceEnsembleEntry` 可能覆盖旧 membership，delete 仍需发现旧成员；本次实际从哪个 surviving replica 读取不属于 authority，可以动态重试且不得制造 metadata churn。不得向 MetadataStore 写每 entry progress、receipt、reader history 或无界 retry ID。

首批基础copy/membership恢复顺序（不宣称strong completion/reset）：

```text
1. idempotently create immutable inert instance-specific RepairIntent child
2. start/complete target durable normal-inactive install; it may proceed
   independently of step 3, and neither inert child nor install authorizes payload
3. conditionally publish admission against the exact
   lifecycle/delete-fence predecessor/head
4. only after both inactive install and admission are durable,
   an authenticated non-anonymous control principal asks the target to direct-read
   exact committed authority, then grants RECOVERY_ONLY bound to admitted intent,
   target storage incarnation, range and fence/grant generation
5. copy through the existing recovery Add data path
6. after copy, reread exact LedgerMetadata, intent admission and delete fence
7. CAS standard ensemble replacement, conditioned on exact instance/version
   and absence of membershipFrozen or an unresolved enhanced-publication token;
   retain old/new target history through the already admitted intent
8. transition target to COMMITTED_REPLICA/READABLE
   - close/revoke RECOVERY_ONLY
   - do not grant normal writable authority
9. reconcile the matching durable copy/membership result and close the local grant;
   expose only that operation's basic result, without resetting a loss window
10. compact intent only after delete-history or cleanup authority safely supersedes it
```

inactive install与admission之间不锁定先后，可并行或交换；不可删合同是grant、第一份payload和ensemble publication都晚于有效admission。admission CAS response loss必须重读exact lifecycle/delete-fence head或其已提交snapshot/summary：matching operation identity+payload已admit时继续同一intent/target；delete先赢时child保持inert；conflicting payload、unknown mandatory state、head gap或无法判定时fail closed/deferred。timeout不得盲建第二个intent、选择第二个target或授予grant。

master key只继续作为data credential，不能授权grant/close；control只走RFC-0001独立TLS1.3/mTLS endpoint，`AuthDisabledPlugin`/anonymous、SASL-without-consumable-principal以及authenticated-but-unauthorized caller必须拒绝。caller必须被授权执行exact grant/close operation + ledger instance + target/range scope，且AuthN/AuthZ检查早于local grant、allocation或任何durable effect。grant/reference按recovery purpose domain-separate，并绑定ledger/instance/descriptor、RepairIntent identity/generation、target stable identity/storage incarnation、exact range、local grant generation、delete fence和operation payload identity。Bookie只在cold direct-read committed authority并durable写入本地bounded grant后接受Profile recovery Add；receipt/status不得泄漏secret、offline verifier或bearer/replay capability。principal allowlist/backend路径与local record packing保持OPEN/BLOCK，不再把当前transport泛化为待选机制。

ensemble CAS response loss 必须通过重读 exact fragment/replacement mapping 解析，不能盲选新 target。closed/historical fragment 的 committed target 不得 normal-active；只有 target 另行成为 current writable fragment member，并满足 RFC-0001 写期 replacement 的 post-CAS membership、fence 和 normal activation 合同，才能独立获得 normal writable authority。

Intent 生命周期语义至少区分 INERT/PREPARED、ADMITTED、RECOVERY_AUTHORIZED、replica/membership COMMITTED、ABORTED 或 ORPHAN_CLEANUP_PENDING。INERT/PREPARED不授予grant/payload；ADMITTED绑定lifecycle/delete-fence cut并进入delete discovery。CAS 前已经接收 payload 的 intent 不得直接删除；COMMITTED 仍保留 old member/target history；只有这些身份已进入另一个不会丢失的 durable delete-history authority，或 target-local durable cleanup proof 已成立，才能 compact。“tombstone”不能把 target 从 delete enumeration 中移除。

replica/membership `COMMITTED` 只证明 copy 与 exact ensemble mapping 已发布，默认不等于 permanent-loss budget 已重置。只有它同时满足下一节的完整 range coverage proof 时，才能承担 `LOSS_BUDGET_RESET_PROVEN` 语义；否则两个事实必须分离，不能因状态名相同而混用。

物理上是否复用 underreplication namespace 保持开放，但语义 owner 固定为：

- underreplication marker：missing replica scheduling；
- worker lock：临时排他执行；
- standard LedgerMetadata：唯一最终 ensemble membership；
- RepairIntent：pre-publication target、recovery-only authorization 与 delete history；
- RFC-0004 strong completion authority：range-scoped coverage assertion、accepted loss ordering 与 loss-window reset；
- Bookie local authority：target 对该 intent 的 durable recovery-only acceptance。

冷控制成本按每repair operation/fragment计量：immutable inert-child create、lifecycle admission CAS、normal-inactive install、recovery-grant durable transition、standard membership CAS和grant close/result reconciliation。首批不创建strong-publication token；目标在admission时已进入可枚举history，随后任何部分copy、response loss或delete都不能提前删除此history。增强reset将来启用时，另计§14.1的token/prepare/publication/resolve成本。matching install可复用已有结果，物理写可group commit；payload热路径仍无per-entry metadata/control fsync/intent update，normal Add不读取sidecar。

### 9.2 Range-scoped permanent-loss budget reset

**增强能力：首批DISABLED / DEFERRED。** 后续候选仅接受整个ledger已fenced且durable CLOSED，并锁定其final prefix；只封住历史fragment而ledger仍OPEN/IN_RECOVERY不够。启用检查必须早于创建`pendingPublication`，运行中不得重新OPEN。因此活跃ledger的normal replacement和基础恢复不会因为本增强功能新建的token等待sidecar completion。普通copy、membership COMMITTED与基础durable close均不重置预算；首次ACK建立窗口，未获得本节有效proof前不自动刷新，累计损失超出声明预算便不再承诺survival，但仍按实际evidence判定结果。

将来若需对活跃ledger提供strong reset，必须单独评审其写入可用性和证据，不能删除token保护后声称同等保证。已有未resolve token必须由§14.1原协议恢复解析，功能关闭、超时或协调器故障均不能直接清除；首批新scope不应生成此类token，发现时defer/quarantine并保留诊断。

Round 1 的 `F` 合同按 Profile 声明的 failure-domain policy 生效。repair/re-replication 只能对一个 bounded、immutable fragment/range 重置 loss window，不能把整个 ledger 无条件清零。proof context 至少绑定：

```text
ledgerInstanceId
sealed/fenced metadata version or digest
ensemble and write-set history
RecoveryContext identity or digest
Profile descriptor hash/generation
permanent-loss budget F
failure-domain policy identity/generation
RepairIntent identity/generation
coverage start/end cut
canonical required-coordinate definition
exact published membership mapping/version/digest
ledger-instance delete/control fence
overlapping range loss/reset predecessor or version
accepted loss ordering token at proof cut
completion operation identity/generation
```

non-Byzantine recovery verifier 必须以 bounded-memory streaming 检查该 range 中每个 required/ACK-eligible coordinate 都有匹配 payload/identity evidence，且在 evidence cut 上重新具备至少 `F + 1` 个 distinct declared permanent-failure domains 的 valid durable evidence；任何 hole、domain coverage 不足或不可验证 domain identity 都禁止 reset。membership CAS、target local durability、`NORMAL_ACTIVE` 或 generic `COMMITTED` 任一单独都不是 reset proof。

MetadataStore 保存的是“受信 verifier 已完成完整、无洞、per-coordinate `F+1` 检查”的条件化 durable assertion。该 assertion 必须长期可解析地绑定 immutable Profile descriptor、当时的 `F` 与 failure-domain policy；允许物理去重或引用 immutable summary，但 RepairIntent/receipt compaction 后不能丢失这些语义或让旧 assertion 在新 descriptor/`F` 下被重解释。coverage checksum/digest/root/count 只用于 identity、完整性或审计 commitment，不能单独冒充 proof，也不强制 Merkle tree、签名或 PKI。exact duplicate-field/schema 仍保持开放。

最低顺序：

```text
1. idempotently create immutable inert RepairIntent child whose payload
   freezes the bounded range/context
2. start/complete target normal-inactive install; it may proceed
   independently of step 3, and neither inert child nor install authorizes payload
3. conditionally publish admission against the exact
   lifecycle/delete-fence predecessor/head
4. after both inactive install and admission are durable,
   grant target RECOVERY_ONLY after authenticated direct-read verification,
   bound to admitted intent + target incarnation + range + fence generation
5. stream and validate every required entry with bounded memory
6. make replacement payload/identity durable
7. reread exact membership, admitted intent, Profile policy, delete/control fence,
   and overlapping range loss/reset head
8. prove complete per-coordinate F+1-domain coverage at the cut
9. CAS exact standard ensemble replacement and its pendingPublication token
10. close/revoke recovery grant and transition target role for its actual purpose
11. CAS domain-local COMPLETION_PREPARED with the exact proof cut,
    mapping/policy/intent and overlapping loss/reset predecessor
12. CAS final publication reference on the live lifecycle/delete-fence head
    -- completion becomes eligible here; the loss window remains bound to the proof cut --
13. resolve COMMITTED or ABORTED, clear the matching metadata publication token,
    and release the conflicting-domain prepare
```

整个ledger已CLOSED时，其closed/historical target不需要normal activation；strong completion receipt在证明完整range、exact mapping与`F+1` coverage后才可reset。活跃ledger上的写期`install → LAC+1 CAS → activate → resend`按RFC-0001继续，不运行本增强协议，也绝不能自行清零旧loss window。

completion authority 必须幂等、可重放，并绑定上述语义。accepted loss declaration 与影响相同或重叠 range 的 completion 必须有单一条件化顺序：loss 先赢则 verifier 排除该 domain 后重新证明；completion 先赢则该 loss 进入新 window。能证明不相交的 range 可以并发，无法证明时保守串行；本合同不要求所有不相交 repair 共享一个 ledger-global hot CAS。delete fence 仍全局阻止迟到 completion。

accepted-loss generation 是排序 token，不是物理 loss counter；相同 domain/incarnation 的 duplicate declaration不能重复消费预算。metadata只能排序 accepted/observed facts，不能宣称感知未观测的物理 failure；proof cut 后实际发生但尚未声明的 failure 仍在语义上进入新 window。迟到 response 或 overlapping stale completion不能抹掉它。

该assertion按operation/range持久化，采用§14.1的prepare/publication/resolve；允许bounded child pages/root、相同context相邻range合并和snapshot+suffix。只有最终publication成功的candidate才可承担reset；resolve不能把窗口起点移到更晚时刻，也不能抹掉proof cut之后已发生或待声明的loss。不要求stop-the-world、全ledger重复制、per-entry MetadataStore/control fsync或无界receipt history。

boundedness 至少要求单 page 的 bytes/interval count、root fan-out 与 retained suffix 有配置前上限；超限或 compaction失败时 defer/fail closed，不能继续无界 append。snapshot 必须先 durable publish，才能删除被覆盖 child receipt；不同 policy/context/control generation 的 interval 不能直接 merge。receipt compact 前，RepairIntent source/target/delete-discovery history 必须已由另一 durable snapshot/summary 接管。exact schema、page size、topology、merge threshold、audit artifact 与可选 commitment encoding 保持开放。

### 9.3 LedgerDeleteManifest

删除需要一个集群级协调器。逻辑 manifest：

当前 [`HandleFactoryImpl`](../../../bookkeeper-server/src/main/java/org/apache/bookkeeper/bookie/HandleFactoryImpl.java) 的 `recentlyFencedAndDeletedLedgers` 只是 Bookie 进程内、按访问 7 天过期的 cache，用于降低删除后冲突写风险；它既不持久化，也不枚举历史 ensembles，不能作为 cluster tombstone 或 rejoin 证明。

```text
LedgerDeleteManifest {
    ledgerId
    ledgerInstanceId
    deleteRequestId
    deleteEpoch

    sealedMetadataVersion
    fencedCloseProofIdentity
    admissionCutGeneration
    membershipFreezeVersionAndIdentity
    ensembleHistoryDigest
    historicalBookies[]
    admittedIntentSnapshotIdentity
    logicalTombstoneIdentity
    requestedCompletionScope
    accessBarrierReceiptsByTargetAndIncarnation[]  // optional strong-revocation progress

    state
    acknowledgedBookies[]
    decommissionedBookies[]
    unrecoverableBookies[]

    createdAt
    retentionDeadlineOrPolicy
}
```

manifest 是权威 metadata 的一部分或由同等 CAS 语义的专用命名空间持久化。

上述target/receipt集合是逻辑形状，物理实现必须用有界pages/snapshot与bounded root引用，不能把全部历史或access-barrier receipts堆入单个无界记录。

## 10. Cluster delete 状态机

```text
FENCED_AND_CLOSED (verified prerequisite)
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

强访问撤销是独立、首批disabled的完成条件：`LOGICALLY_DELETED → ACCESS_BARRIER_PENDING → ACCESS_BARRIER_COMPLETE`，与物理清理进度分别查询。本地清理仍按§11关闭准入、durable tombstone、确认delete-applied，再异步drain/回收；延期的是“普通API等待所有目标屏障”的保证，不是放宽本地free/reuse条件。

### 10.1 DELETE_INTENT

通过 MetadataStore CAS：

- 绑定 ledgerId + instance + deleteRequestId；
- 关闭新的open、lifecycle/repair admission与strong completion publication；
- 固定cut前admitted intent的可枚举history；
- 启动§14.1的标准metadata freeze，处理cut前已准入操作；
- 返回可查询的in-progress状态，尚不承诺所有已有handle失效。

首批删除只接受已完成fenced close的ledger：既有matching durable CLOSED metadata，也有覆盖所有可能write set的fence/recovery证明。仅writer写入CLOSED不足以证明本地写权限已撤销；未满足前置条件时返回明确precondition/non-OK，调用方先执行基础恢复。在线删除及delete协调器代为fence/recovery延期为独立feature gate。

### 10.2 ENSEMBLES_FROZEN

先按§14.1在标准LedgerMetadata同一记录CAS发布不可逆membership freeze，再从该版本及admission snapshot枚举历史，生成绑定Bookie/storage incarnation的immutable target set和digest。先list再CAS不能冻结完整集合。

必须覆盖：

- initial ensemble；
- 每次 ensemble change 的旧/新成员；
- incomplete、completed、aborted-but-dirty RepairIntent 中的 replaced member 与 target；
- 已记录但尚未完成的 replacement。

freeze之后membership CAS不得继续改变该instance的ensemble。cut前admitted操作可能留下局部payload，必须在其固定target scope被撤权/清理，不能新增未被枚举的target。

### 10.2.1 ACCESS_BARRIER_PENDING / COMPLETE

对全部冻结target执行instance-specific本地屏障：原子关闭normal/recovery/read admission，持久化terminal tombstone/generation，再终结pre-cut服务操作与相关写I/O，满足后才返回access-barrier receipt。§11的delete-applied及cursor可在runtime drain结束前成立，二者不能代替屏障完成。屏障证明不再服务该instance，尚不证明全部空间已回收。grant读取旧authority后迟到时，与本地tombstone在同一gate排序；tombstone先赢则grant失败，grant先赢则由该屏障撤销。

只有每个target都有matching durable barrier receipt，或有cluster-accepted且使旧incarnation无法再服务的永久decommission/wipe终结证明，才可进入COMPLETE。暂时offline、timeout、撤销writable registration或watch通知不足以证明旧read连接已失效；这些target使强访问撤销保持pending，但不阻止已满足§10.3的普通logical success。

首批不advertise强访问撤销能力；请求该完成级别时明确unsupported/non-OK，不能以普通logical success替代。后续启用需独立feature gate。屏障只约束后续服务，不能撤回客户端已取得的数据或网络中已生成的响应。

### 10.3 LOGICALLY_DELETED

`DELETE_INTENT`已阻止新open/admission，且`ENSEMBLES_FROZEN`的完整target/history及可恢复清理义务已durable保存后，条件化发布authoritative cluster tombstone并完成普通`deleteLedgerAsync()`。不等待离线目标ACK或所有本地屏障：

- 新open、新恢复admission及新的生命周期权限被拒绝；所有控制路径不得复活旧instance；
- 已打开reader在目标本地tombstone生效前仍可能读取；此前admitted的固定scope内grant/payload工作也可能收尾，必须可发现且最终撤销。普通成功不承诺所有旧handle立即失效；
- 本地屏障生效后，旧handle的新read/add/grant同样被拒绝。已经生成的响应可迟到，网络callback到达时刻不定义删除线性化；
- metadata 不得回到 OPEN；
- logical completion 不代表所有物理 bytes 已回收；
- tombstone 必须可被重启和 watcher 丢失后的组件重新发现。

### 10.4 PHYSICAL_DELETE_DISPATCHED/PENDING

协调器从durable manifest/投递义务向每个historical Bookie发送instance-specific delete。logical response之前必须已有可恢复的清理来源，不能只依赖易失发送队列；response loss或coordinator crash后按同一request/instance继续。离线节点仅使撤权/物理清理pending；调用方可分别查询进度。

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
    deleteDeliveryStreamIdentityAndGeneration
    deleteJournalSequence
    sealedMetadataVersion
    ensembleHistoryDigest
}
```

本地完成事实分为`delete-applied`与`physical-reclaimed`；强访问屏障另按§10.2.1判断。可推进cursor的delete-applied至少证明instance-specific terminal tombstone及拒绝新准入状态已durable，清理义务可在crash后重建，不证明runtime drain或物理释放完成。

Bookie执行顺序：

```text
verify instance / delete epoch / authorization / delivery identity
    -> close new normal/recovery/read admission under the existing gate
    -> durable terminal tombstone + reconstructible cleanup obligation
    -> advance the no-hole delete-applied cursor through durable effects only
    -> return the matching delete-applied result

asynchronously, without blocking subsequent delete delivery:
    -> terminate/wait for old readers, writers, I/O and pins
    -> report access-barrier completion only when its service-drain conditions hold
    -> rebuild/update dead accounting; compact remaining live shared-block records if needed
    -> after whole-allocation reclaim conditions hold, durable FREE_AND_BUMP
    -> record physical reclaim result bound to instance/generation/delivery identity
```

Classic/Direct仍执行其对应清理；Segment按RFC-0003判断whole-allocation free。每种result必须标明完成事实并绑定Bookie/storage incarnation、stream/sequence及local generation；不能用一个local delete receipt混淆applied、access-barrier和physical结果。

清理义务从已有terminal tombstone、完整allocator/current-selector authority及有效DATA重建；派生队列和dead accounting只作加速，不新增持久化任务系统或逐entry metadata。checkpoint、snapshot及tombstone压缩不得丢失尚未physical-reclaimed的义务或其重建输入；队列为空不证明已清理。

cursor可以与一组tombstone有序group commit，不要求每delete一次fsync。任何可恢复状态都不能出现cursor已durable而所覆盖tombstone/effect未durable。tombstone生效后迟到I/O不能发布新的local success；已经生成的响应仍可迟到。cursor不证明旧I/O已终结，buffer/slot复用仍等待真实I/O终结或可靠隔离。

共享block含L1/L2时，delete L1的tombstone和applied cursor可完成，即使L2存活使block不可free；随后delete L2继续消费，不等待L1物理回收。不能释放的空间保留为debt，`awaitPhysicalDeletion()`仍等待真正物理终态。长期reader pin、慢I/O或compaction延迟只阻塞相应屏障/回收，不阻塞已满足applied条件的cursor。

重复请求幂等。相同 request 不同 instance/digest/epoch 必须冲突。

## 12. Delete delivery stream 与 rejoin barrier

`deleteEpoch` 只表示单个 ledger instance 的 delete fencing/version；它不是跨 ledger 的 Bookie catch-up coordinate。delete delivery progress 使用独立、stream-scoped authority：

```text
DeleteDeliveryAssignment {
    assignmentGeneration
    predecessorGenerationOrCasVersion
    bookieStableIdentity
    storageOrDeviceIncarnation
    applicableStreams[]
    requiredThroughByStream
    handoffCut
    preparedOrEffectiveStatus
}

DeleteStreamCoordinate {
    streamIdentity
    streamGeneration
    deleteJournalSequence
}
```

物理 schema 保持开放，但以下语义锁定：

- 每个 committed stream 的 sequence 单调、无洞；cursor 不能越过 unexplained sequence；
- cursor 绑定 Bookie stable identity、storage/device incarnation、stream identity/generation 和 applied-through sequence；
- delete-applied cursor推进到`N`表示`<=N`每个event的terminal tombstone/拒绝新准入及可重建清理义务已durable，或有可验证routing/membership non-applicability证明；不要求FREE、compaction、reader/writer/I/O drain已完成；
- 新磁盘、重装或新 storage incarnation 不能继承旧 cursor，只能 verified bootstrap，或提供不可逆 wipe/decommission proof；
- cluster-authoritative assignment 给出有限的 applicable stream set 与 registration required-through；Bookie 不能只报告自己知道的 stream；
- 每 Bookie/storage incarnation 的 applicable stream 数有 manifest-locked finite maximum，超限或 assignment 无法证明时 fail closed；禁止每 ledger 一个长期 stream；
- stream committed head 的普通增长不是 assignment generation 变化，不要求 Bookie重新注册；
- obligation-changing generation 必须通过 predecessor/handoff cut 条件化生效；handoff 允许 duplicate delivery，绝不允许 old/new route 都不负责的 gap；
- ordinary ensemble replacement 既不删除旧节点数据，也不阻止旧 incarnation 返回，因此不能替代 decommission/wipe proof。

snapshot 必须绑定 stream identity/generation、snapshot generation、covered-through sequence、assignment generation/target incarnation、bounded manifest/chunk completeness 与 content/integrity digest，并提供可遍历、可应用的 still-required instance-specific delete effects。每个 retained effect 至少包含 ledger instance、delete epoch/request identity、effect/tombstone identity 与应用或验证 non-applicability 所需的 authority binding。digest-only root 不足；缺 chunk、suffix gap 或内容不完整时不能 bootstrap/writable。

snapshot 可以使用 bounded chunks/reference，root 不展开全部 effects；non-Byzantine 模型下不强制签名、Merkle proof 或 PKI。bootstrap 只能是 verified snapshot + complete no-hole suffix；journal prefix 只有在所有仍支持的 bootstrap 路径都有有效 snapshot 或 terminal decommission proof 后才能 compact。

snapshot消费的是delete-applied语义，必须保留尚需应用的tombstone及未回收义务的可重建性。restart/rejoin catch-up等待required-through范围的applied effects，不等待共享block compaction或全部物理回收；必要tombstone缺失、cursor gap或authority无法恢复仍禁止writable。新服务启动前还须满足既有存储/I/O安全恢复条件，cursor本身不免除这些条件。

assignment handoff 最低顺序：

```text
1. generation G remains active
2. create G+1 PREPARED with predecessor G and handoff cut
3. retain/dual-route old obligations while G is active
4. build/reference verified snapshot through cut N
5. Bookie applies snapshot + complete suffix
6. durably record G+1 and required-through cursors
7. registration/assignment CAS makes G+1 effective
8. only then retire G routes/obligations
```

Bookie 可以在 G active 且 G+1 PREPARED 时继续 writable；若 G+1 已预先 catch up，可 CAS 无可见 demotion地切换。若 obligation-changing G+1 已 effective 而 Bookie 尚未 catch up，则旧 writable registration 必须被撤销或 Bookie进入 RECOVERING。不是“任何 generation change 都立即 demote”，而是 stale generation 不能跨 effective cut继续 authoritative writable；assignment 不成为 Add-time lease。

候选启动流程：

```text
1. register as RECOVERING or READ_ONLY
2. fetch verified assignment generation and required-through vector
3. for each applicable stream, verify local cursor or snapshot
4. apply snapshot and complete suffix without sequence holes
5. durably apply each terminal tombstone and reconstructible cleanup obligation
   before advancing its delete-applied cursor; do not wait for physical reclaim
6. reconcile RepairIntent/device state and rebuild bounded derived cleanup work
7. registration CAS validates the same assignment generation/cursors
8. only then become writable
```

writable registration 必须与新 delete publication 有明确线性化 cut：delete 先赢则进入 required-through；registration 先赢则后续 delete 将该 Bookie 视为在线适用 target。不能依赖 watch callback 时序。local cursor/snapshot 丢失、suffix 缺口或 snapshot generation/digest 无法验证时，Bookie 保持 RECOVERING/READ_ONLY。

assignment 移除 stream 前，该 `(bookieStableIdentity, storageIncarnation)` 的 effects 必须由绑定同一 incarnation 的新 assignment/snapshot 接管，或 old incarnation 已有 cluster-accepted irreversible wipe proof/permanent decommission fence；不能把旧 incarnation 的物理删除 obligation 转嫁给另一个 Bookie。local self-report、timeout、offline、“新盘看起来为空”或普通 ensemble replacement均不够；exact authorization/attestation 保持开放。

terminal wipe/decommission proof 至少语义绑定 `bookieStableIdentity`、old storage/device incarnation、明确 device/storage scope、operation identity/generation、cluster acceptance authority/version，以及 irreversible result 或 permanent registration fence。wipe proof 只免除其明确覆盖 scope 的 catch-up；permanent decommission fence 必须保证该 old incarnation 永久不能重新注册 writable。旧 operation、另一个 device、部分目录或另一个 incarnation 的 proof 不能重放扩大作用域。exact authorization、attestation、人工审批与硬件证明保持开放。

exact topology 继续 OPEN：可以是 per-Bookie inbox、固定数量 global shards、hierarchical snapshot 或等价 bounded 方案。不要求全局单 sequence；共享 shard 中的 non-applicable event 必须有可验证 routing proof，不能无证明跳过。

## 13. API 完成语义

API 必须区分：

```text
deleteLedgerAsync()
    -> durable authoritative tombstone + complete retained cleanup targets/obligations

awaitAccessRevocation()  // proposed; first scope disabled
    -> all serving targets have durable barriers or permanent service-isolation proof

awaitPhysicalDeletion()
    -> every historical target resolved and physical state terminal
```

名称仍可调整，但一个boolean success不能同时表示“禁止新生命周期访问”“所有旧服务权限已撤销”和“物理清理终结”。普通删除明确允许旧reader在本地tombstone前继续读取；物理终态还须区分实际释放与接受的decommission/unrecoverable结果，不能声称已擦除所有字节。

进度查询至少返回：

- manifest state；
- target/ack/pending/decommissioned counts；
- unresolved Bookie IDs；
- logical completion time；
- admission cut、membership freeze与access-barrier完成时间；
- 各stream的delete-applied cursor/追赶状态，以及与之独立的physical-reclaimed数量/bytes和待回收debt；
- 请求的completion scope、强撤权能力是否启用，以及access-barrier pending target/incarnation及其原因；
- physical completion time；
- tombstone retention state。

Profile range/recovery wire、coordinator、admin status与scheduler必须贯通RFC-0001冻结的transport `statusClass + retryDisposition + durableResult`：unsupported、identity conflict、not-ready/stale、fenced/deleted、grant invalid、transient、durability unknown、quarantine、unauthorized与bad request不能坍缩成OK。该transport status只是operation结果，不替代本RFC五类recovery outcome；尤其`QUARANTINED_OR_UNKNOWN_MANDATORY`或authority loss不得投影成payload `DATA_LOSS`。exact BKException/detailCode与legacy projection继续BLOCK；unsupported batch只能回退distinct single-entry Profile recovery，控制操作的durability unknown只查询/重试同一Profile operation；normal Add按RFC-0001 §9.3允许正式换组重发，recovery更换target必须先resolve原intent并重新admit，不能盲建。任何路径均不降级legacy flag/Classic或绕过协议双写。

## 14. 并发规则

### 14.1 Delete vs ensemble change

下一原型采用以下竞争协议，状态仍为**Planned / Not Executed**；Model A+D必须展开每次远程读取、单记录CAS、本地durability与response loss，不能把整段抽象成一个原子动作。

**关闭admission与冻结membership：**

1. repair先持久化immutable inert child，再CAS lifecycle head发布admission；child绑定的target/range不能在admission后扩张。
2. delete CAS同一lifecycle head进入`DELETE_INTENT`，关闭新admission和strong completion publication，保留此前admitted目录/snapshot及未resolve publication引用。
3. delete读取标准LedgerMetadata，以其版本CAS写入绑定instance/delete operation的不可逆`membershipFrozen`语义marker，并保持CLOSED。所有Profile membership updater必须在同一记录的CAS predicate检查instance、合法state、freeze marker absent且无其他pending publication；不能只在CAS前读sidecar。marker/token的exact custom-metadata encoding及拒绝legacy mutation的ACL须在原型前冻结。delete freeze可保留未resolve publication token并关闭metadata变更，但不得改写它所绑定的mapping。
4. 旧membership CAS先赢时，freeze CAS冲突并重读，把赢家纳入历史后重试；freeze先赢时，旧CAS版本失败，重试读取marker后终止。marker durable后不再接受任何repair/replacement membership变更。
5. 以freeze版本、admitted intents及完成history形成固定targets和可恢复清理义务，持久化后发布普通logical tombstone；逐target屏障和物理清理异步推进。只有独立强撤权结果等待全部§10.2.1屏障。每个response loss重读同一identity/version/marker，不盲建新delete或repair。

标准LedgerMetadata仍是唯一membership truth；freeze marker只阻止该记录继续变更，不复制ensemble。无权绕过marker的Profile metadata mutation控制是此前§9协议的必要前提；其enforcement未验证时不得启用删除。

**仅用于延期的strong completion/reset：**

首批基础copy/membership/close不运行下述协议，也不声明reset；其admitted history和同记录freeze保护仍有效。下述协议启用后必须先满足§9.2的整个ledger fenced+CLOSED前置，不能只检查待修fragment已sealed。token可能因coordinator/sidecar故障长期保留，这是会阻塞该ledger后续membership更新的可用性成本，不能因属于冷路径而忽略。manifest和故障测试必须记录阻塞时长、恢复解析及前台影响；首批活跃路径应证明不创建这种token。

在进入下列步骤前，repair的membership CAS必须把新mapping与`pendingPublication(instance, operation, mapping identity)`一起写入同一条标准LedgerMetadata。需要发布completion但无需换组时，也须用该记录的conditional CAS保留同样的token。其他membership updater看到token就defer，直到该operation的durable结果已解析；copy工作仍可并行。这保证final publication消费的mapping不会在跨key读取后被另一个repair替换，标准metadata仍是唯一membership truth。

1. 在owning range/domain head CAS写`COMPLETION_PREPARED`，绑定immutable evidence、exact mapping/policy/intent、proof cut、accepted-loss predecessor及publication operation identity。该candidate本身不发布strong completion或loss reset；基础durable close是独立事实。
2. prepared期间仅对实际冲突/重叠domain阻止新的completion及accepted-loss head推进；新的loss观察/声明必须保留并在resolve后排序，不能丢弃、伪装未发生或重置其物理发生窗口。队列/等待有hard cap，超限defer。
3. CAS lifecycle head条件检查相同live instance/delete generation，发布该candidate的bounded reference。它与`DELETE_INTENT`只会有一个先赢：delete先赢则candidate只能abort；publication先赢则形成完成authority，随后delete必须保留其history。
4. coordinator按已提交publication或delete winner，条件化清除同operation的metadata token并resolve prepared domain为COMMITTED/ABORTED，再处理pending loss；清除时保留membershipFrozen及所有历史target。reset若成立仍绑定原proof cut，之后的loss计入新window；abort保留旧window。root仍live且无matching publication时重试同一publication CAS，不能猜abort。coordinator crash/response loss由reconciler重读metadata、domain与lifecycle记录完成同一结果；必要authority不可用时保留token并defer。
5. unresolved prepare依赖的publication引用不得先compact；bounded snapshot接管后才回收。missing/gap/unknown导致deferred/quarantine，不能按NOT_FOUND猜abort或重做reset。

这为最终完成增加operation级冷CAS和局部prepare等待，必须测量冲突率、时延与恢复进展；其正确性及资源Gate未通过前不能Accepted。普通copy progress、不相交range的数据工作和normal Add不经过这个publication CAS；不引入跨key通用事务、per-entry元数据更新或Add lease。

### 14.2 Delete vs AutoRecovery

AutoRecovery 可以先durable-create inert RepairIntent child，但在授予recovery-only authority、写第一份payload或发布ensemble前必须完成上述admission，并检查exact instance、intent generation和lifecycle/delete-fence cut。`DELETE_INTENT`先赢后，未admit child保持inert，不得授予recovery authority或产生该instance的新replica，只能在orphan proof后回收。

RepairIntent admission先赢时，delete frozen target必须包含其replaced member与target，无论copy/CAS是未开始、部分完成、COMMITTED还是aborted-but-dirty。`DELETE_INTENT`至目标本地屏障之间，已admitted且读取了旧authority的操作可能完成限定scope的grant/payload写入；这是可追踪的待撤权中间态，不代表新的admission或strong completion获准。基础membership与freeze按同记录CAS排序，freeze前赢家可有迟到结果但不复活生命周期。新cold read看到delete时拒绝grant；旧read与local tombstone的竞争由本地gate及drain解决，不能宣称远程CAS立即阻止所有磁盘写入。

访问屏障之后不得再授予该instance的grant、发布新local success或服务新read；此前partial payload由冻结target的物理删除处理。任何扩大target/range的行为都需要新admission，delete后必然失败。exact child enumeration、bounded directory与compaction encoding仍待实现。

### 14.3 Delete vs open/read

logical tombstone阻止新open；旧reader在目标本地tombstone前仍可能发起并完成read，不依赖每次read远程查询。目标关闭read admission后，已准入read完成或明确失败，屏障receipt前其服务工作必须terminal；屏障后的read即便复用旧handle也拒绝。已生成的网络响应可能晚于任一API结果。只有独立强撤权完成才承诺所有可能服务该instance的冻结target均已跨越屏障；fencing quorum和普通logical success都不作此承诺。

### 14.4 Delete vs ledgerId reuse

所有操作绑定 `ledgerInstanceId`。新 instance 不能因旧 tombstone 被误删，旧 locator 不能因 ledgerId 相同读取新数据。

### 14.5 Registration vs delete stream publication

registration CAS 必须校验相同 effective assignment generation、storage incarnation 与 required-through vector。delete event 先于 registration cut 提交时必须进入 required-through；registration 先赢时，后续 delete 通过 active online route投递并保留在 stream。assignment handoff时 event 可以双投但不能落入 routing gap。stale generation不能跨 effective obligation-changing cut writable；assignment watch、poll 或 response 到达顺序不构成线性化 authority。

### 14.6 Repair completion vs permanent loss

延期的range-scoped strong completion按§14.1的metadata token、domain prepare、lifecycle publication及resolve绑定failure-domain policy、accepted loss/reset predecessor、membership mapping、delete fence与coverage cut，不能把跨记录校验伪装成一个CAS。影响相同/重叠range的loss/strong completion单序。新permanent loss若在proof cut后发生，必须计入新window；旧response迟到不能将其清零。delete先赢时不得strong publication；strong publication先赢时delete保留receipt/target。首批基础repair只维护copy/membership事实，不运行reset、不丢失accepted loss或delete history。

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
7. fast-path 失败或局部预算耗尽从 earliest unresolved coordinate 回退，不改变全 point-read oracle 的结果。
8. deadline/cancellation 只终止 attempt；单 replica corruption 在其他 valid evidence 存在时不能伪造 DATA_LOSS。
9. loss-budget reset 只覆盖有完整 `F+1` distinct-domain evidence 的 bounded range；membership、activation 或 local durability 单独都不能 reset。
10. strong completion 是 verifier assertion；digest/root 单独不能 reset，且 conflicting range loss/completion有单一 conditional predecessor。
11. normal open-ledger tail 需要 fenced context、required prefix无洞，以及 exact write set 上至少 `W-A+1` definitive absences；temporary/no-quorum 不算 absence。
12. required coordinate只来自 accepted durable authority；speculative later payload不把前一个正常 tail变成 DATA_LOSS。
13. recovered success晚于 recovery-add 与 durable close/final-prefix publication；authority unrecoverable属于 quarantine，不是 payload DATA_LOSS。
14. legacy `OK`只投影matching durable ledger close；deferred、incomplete、quarantine和data loss均non-OK，generic rc不得成为repair completion authority。
15. rich outcome保留operation scope；fragment repair、legacy skipped ledger或partial progress不得计为ledger recovered。
16. Profile recovery Add使用distinct logical operation并匹配bounded local grant；legacy flag、Classic fallback或mixed old Bookie不能获得recovery authority。
17. recovery control grant/close要求non-anonymous且获授权执行exact operation/instance/target-range scope的principal，并由Bookie direct-read committed authority；AuthN-only/master key不授权control transition，receipt/status不泄漏secret或replay capability。
18. 当前`ADD_RECOVERY`与`ADD_NORMAL` subtype/body不可互换；range/batch subtype只预留且disabled，unsupported不得回退legacy recovery flag。
19. operation transport status不得把authority loss、quarantine、incomplete或durability unknown命名为payload DATA_LOSS或legacy OK。

### 16.2 Delete

1. LOGICALLY_DELETED 后该 ledger instance 永远不能重新 OPEN。
2. frozen target set 覆盖固定 metadata version 的历史 ensembles，以及所有 incomplete/completed/aborted-but-dirty RepairIntent 的 replaced member 与 target。
3. unresolved offline Bookie 不能被超时自动解释为物理删除。
4. Bookie 缺失 required tombstone 时不能注册 writable。
5. local free/reuse晚于durable tombstone、reader/pin drain和旧writer I/O终结或可靠隔离。
6. 新 ledger instance 不受旧 instance delete 请求影响。
7. DELETE_INTENT关闭新repair admission/strong completion；cut前固定scope内的partial写入必须可发现并在访问屏障撤销，membership freeze后不能发布新ensemble。
8. PHYSICALLY_DELETED 只在每个 target 有 durable terminal proof 时成立。
9. target 的第一份 durable recovery payload 晚于可由 delete freeze 枚举的 RepairIntent。
10. recovery-only authority 永不隐式授予 normal writable authority。
11. delete-applied cursor只在对应durable tombstone及可重建清理义务、或可验证non-applicability后推进，不能跨gap；不等待FREE且不证明强屏障/物理回收。
12. writable registration 意味着该 storage incarnation 对 cluster-authoritative finite assignment 的全部 required-through stream 已 catch up。
13. snapshot + suffix 必须完整；旧 storage incarnation 不能借新 identity 或 ordinary ensemble replacement 绕过 catch-up。
14. obligation-changing assignment generation只有在 handoff catch-up 后 effective；stale generation不能跨 effective cut继续 writable。
15. snapshot 必须可枚举并应用 effects，digest-only root不足；stream removal不能把同一 old incarnation obligation丢失或转嫁。
16. catch-up exemption只来自 cluster-accepted irreversible wipe/permanent decommission fence；proof绑定Bookie、old incarnation、storage scope、operation generation与cluster acceptance，不能跨scope重放。
17. 未完成lifecycle/delete-fence admission的RepairIntent child不授予grant或payload authority；cut前全部admitted intent必须进入frozen targets，admission后的progress不推进universal ledger head。
18. 普通logical success晚于authoritative tombstone和完整、可恢复的清理目标；不要求所有目标在线，也不等于强访问撤销。
19. 强访问撤销完成必须覆盖全部可服务target/incarnation；普通删除后尚未屏障的旧reader仍可读，不得将此计作强撤权PASS。

## 17. Model D/E 最低场景

Model D：

- delete 与 ensemble change 同时 CAS；
- delete 与 RepairIntent inert-child create/admission/recovery authority/first payload/ensemble CAS 逐边界竞争，覆盖child-before-admission crash、admission先赢、delete先赢及admission response loss/restart；
- repair target 收到部分或全部 payload、CAS 前 crash，restart 后仍可枚举并清理；
- ensemble CAS response loss 与 intent COMMITTED response loss；
- 部分 Bookie 收到请求、response loss、协调器 crash；
- Bookie 长期离线后 rejoin；
- 两个 delete stream 交错、middle sequence 缺失、duplicate/same-sequence conflict；
- cursor-before-effect 负向场景与 cursor response loss；
- shared block中L1/L2的连续delete、L1 applied但不可free时仍应用L2；长期pin/未完成I/O、effect-before-cursor/cursor-before-free crash与清理队列丢失重建；
- snapshot build/apply/compact 各 crash boundary、snapshot+suffix gap；
- snapshot root valid但chunk缺失、content digest conflict、prefix过早reclaim；
- assignment `G→G+1` prepare/catch-up/effective、stale watch/registration 与 delete handoff cut 前/同时/后；
- assignment扩张未catch up却writable、safe pre-catch无不必要demotion、removal obligation未接管；
- storage incarnation replacement、极晚 rejoin 与 decommission/wipe proof；
- local wipe self-report未被cluster接受、旧terminal proof跨incarnation/device/scope重放、decommission response loss、assignment generation ABA；
- applicable stream maximum 超限时 fail closed；
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
- unsupported/partial/fast-budget fallback 与 earliest unresolved hole；
- fallback 期间 coordinator crash、delete/control generation 变化；
- single corrupt replica、irreconcilable conflict 与 evidence exhausted；
- cancellation/deadline 不产生永久 DATA_LOSS；
- CLOSED required entry missing、open normal tail、`W-A+1` absence与`W-A + offline`对照；
- later speculative vs later required evidence、authority corruption但payload存在；
- close durable前后response loss、recovered outcome必须有durable close；
- 五类rich outcome到legacy/non-legacy API exhaustive projection、unknown/new code mixed-version兼容；
- `skipUnrecoverable`不clear marker/intent、不计success，AutoRecovery对各class采用不同调度；
- cancel在recovery Add/close各边界与close durable后、metrics cardinality和retry double-count；
- fast+fallback 与全 point-read oracle 等价；
- response loss 和 recovery coordinator crash。

模型不能依赖 durable blob store。

## 18. 接受 Gate

按实际启用能力分阶段接受；整个RFC Accepted仍需全部能力闭合，基础scope不等待disabled增强。当前优先级与独立Gate为：

| Scope | 必需证据 |
| --- | --- |
| 基础点恢复（首批） | 独立point oracle、normal-tail/required frontier、matching durable close、LAC/digest兼容、response loss/restart；无strong token、无预算reset |
| fenced-close普通删除（首批） | admission/freeze、完整target与可恢复清理义务、authoritative tombstone；离线目标不阻止logical success，旧reader直到本地tombstone可读；本地drain/I/O/free和rejoin保证不变 |
| 强访问撤销（DEFERRED） | 独立API/能力、所有可能服务target/incarnation的barrier或永久服务隔离proof，不将普通logical结果投影成强成功 |
| strong completion/reset（DEFERRED） | 整个ledger fenced+CLOSED前置、完整range/domain proof、token/prepare/publication/resolve和pending loss；coordinator/sidecar故障后的解析与membership停顿计量 |
| Batch/Range、在线删除、复杂故障域及迁移（DEFERRED） | 实际需求、各自伪代码/模型/真实原型及兼容/资源/进展证据 |

各scope从以下完整验收清单选择对应要求，在run前锁定，不得事后删场景：

- 当前 OSS BatchRead 行为和限制有 executable baseline tests；
- streaming continuation、per-entry result、limits 和 cancellation wire contract 冻结；
- normal read 与 recovery evidence 的成功条件分别定义；
- general E/W/A merge 有伪代码、复杂度、Model E 和故障测试；
- deterministic fallback 的 RecoveryContext、earliest-unresolved 与 outcome class 有 executable tests；
- required frontier、normal-tail quorum-intersection absence 与五类 recovery outcome有 executable point-read oracle tests；
- internal rich outcome、durable-close-only legacy `OK`、admin/AutoRecovery投影和compatibility matrix有executable tests；
- RFC-0001 Round 7 `ADD_NORMAL/ADD_RECOVERY` executable body、batch/range disabled behavior、legacy flag伪造、old/mixed Bookie no-downgrade、unknown subtype与response-loss exact retry有raw-wire/端到端测试；
- non-anonymous且exact operation/instance/target-range scope authorized repair control principal、authenticated-but-unauthorized负向路径、target incarnation/purpose/range/grant-generation binding、direct-read committed RepairIntent/delete fence、secret leak与grant status query通过测试；
- 基础RepairIntent identity、lifecycle/delete-fence admission、retention和target discovery冻结；strong assertion、`F+1` coverage、loss ordering及receipt snapshot由增强reset独立Gate拥有；
- DeleteManifest schema、target freeze 和 CAS 线性化点冻结；
- bounded stream assignment的 PREPARED/effective handoff、storage incarnation、可应用snapshot+suffix、per-stream cursor、registration cut 与 terminal wipe/decommission proof有集群级端到端测试；
- 普通logical/强撤权/physical API completion分别观测，禁用强能力返回unsupported/non-OK；
- admission、membership freeze、logical tombstone、本地access barrier及physical reclaim的cut分别观测；已有reader、离线target、迟到grant/I/O/response通过确定性测试；
- 增强reset启用时，domain prepare/lifecycle publication/resolve每个crash/response-loss边界及pending accepted-loss排序通过Model A+D和reconciler测试；首批只验证不创建token/不reset及旧token不可强删；
- Model D 无 safety counterexample；
- Segment local reclaim 与 RFC-0003 generation tests 联动通过。
- delete-applied、强屏障和physical结果分开，restart catch-up不等待compaction；shared L1/L2、cursor-before-free、checkpoint/队列重建及无提前reuse通过既有B4/B6和Model D/C+D场景。

首批可保留数据canary必须闭合基础恢复及实际启用的fenced-close普通删除/安全回收，不要求先接受disabled强撤权、strong reset或Range。任何局部通过不得自动提升其他scope；基础恢复不能借增强延期而省略。

## 19. 开放问题

- streaming range body/token后续改造与现有BatchRead的兼容/复用方式；`0x0301`在当前manifest只是disabled reservation；
- continuation token 的签名、过期和跨 Bookie 行为；
- general E/W/A recovery merge 的精确算法；
- TailSummary 是否仅为 hint，还是进入未来 quorum proof；
- recovery fast-path/global attempt 的 hard bounds、outcome exact error mapping 与超限运维流程；
- recovery outcome exact Java enum/exception、additive admin result/status、cancellation API与metrics names/thresholds；RFC-0001 transport status不能替代该API；
- `DataUnknownException` factory/mixed-version修复或generic quarantine mapping的最终选择；
- volatile proof cache 与可选 operation-scoped checkpoint 的 exact encoding/durability；
- Delete Coordinator 的部署、leader election 和 manifest namespace；
- §14.1基础membership freeze的exact encoding/ACL及target/history retention；增强pending-publication/prepare/resolve的schema与reconciliation单独延期，whole-ledger CLOSED限制及旧token恢复须可验证；
- §10普通tombstone/cleanup publication与三种API完成级别的exact schema/error mapping；§10.2.1强撤权全目标proof独立延期，本地终结/安全回收不能省略；在线删除继续延期；
- RepairIntent exact path、admission directory/head、child enumeration/watermark/index、状态名、sharding/batching和compaction encoding；
- repair strong-assertion receipt schema、audit commitment、failure-domain identity/policy、accepted-loss namespace、range-sharded/global topology 与 interval page/fan-out/compaction；
- recovery-only local record packing、BatchRecoveryAdd body/schema与batch limits后续改造；`0x0302`在当前manifest disabled；
- grant reference exact packing、principal allowlist/backend、target-incarnation binding、BKException/detailCode与legacy projection；single `ADD_RECOVERY` subtype/context不再OPEN但stable wire仍受RFC-0001 raw corpus Gate阻塞；
- delete stream topology/count、assignment store、handoff encoding、event batching、snapshot chunk/manifest encoding 与 journal compaction；
- decommission/unrecoverable 的授权流程和 durable proof；
- maximum rejoin window 与 compact tombstone 生命周期；
- Classic/Direct/Segment reader drain 的统一 API；
- physical deletion receipt 的审计与 metrics。

这些问题关闭前，不得把 Range、general E/W/A fast recovery 或 Conditional Delete 标为 Implementation Ready。
