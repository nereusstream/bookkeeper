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

Profile range/read必须位于RFC-0001无歧义Profile envelope及mandatory logical subtype中，并由per-connection capability negotiation启用；不能只复用legacy Add/Read的optional字段，也不能在unknown/partial/response loss后降级成语义不同的Classic请求。首版 exact wire schema/opcode、token 编码和 feature bit 由协议评审冻结。请求必须在分配前验证 hard count/bytes/deadline 上限。

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

BatchRecoveryAdd 只优化传输与本地 I/O，不改变每个 entry 的 recovery 权限和幂等语义。

最低约束：

- 每个 entry 保留独立 entryId、checksum 和 result；
- batch 部分成功可以精确重试；
- 已存在相同 entry 幂等成功，不同 payload 冲突；
- fencing/master-key/recovery flag 与现有单 entry recovery 等价；
- request/response 有硬资源上限；
- batch capability不支持时，Profile ledger只回退到distinct single-entry Profile recovery operation；Classic ledger才可继续现有legacy single-entry path。Profile ledger遇旧Bookie/mixed ensemble必须在payload前fail closed，不得降级到legacy recovery flag。
- partial success 或 response loss 按 entry 验证并精确重试；最早 unresolved coordinate 阻止 frontier，不能把 batch 当作原子或跳过 hole。

不在正式模型和兼容测试前宣称 batch 是原子操作。

### 7.3 Required frontier、normal tail 与 outcome taxonomy

RecoveryContext 必须先从 accepted durable authority 推导 required coordinate。至少包括：CLOSED metadata 的 `[0,lastEntryId]`、fenced context 下 quorum/coverage-proven LAC 及此前连续 prefix、冻结的 historical fragment、accepted ACK/AQ/repair-completion authority，以及未来被接受的 ordered committed frontier。client pending Add、`lastAddPushed`、单副本 payload、TailSummary maximum、speculative range result 或 later payload 单独都不构成 required authority。

open-ledger recovery 的首个 missing coordinate `x` 只有同时满足以下条件，才能证明 normal tail `P=x-1`：

- ledger 已在同一 generation 下 fenced，且所有 `<x` required coordinate 已无洞恢复；
- `x` 位于 quorum-proven committed frontier之后；
- exact write set 上有与每个可能 Ack quorum 相交的 definitive absence coverage；Round-robin `E/W/A` 基线至少为 `W-A+1` 个 distinct write-set members；
- timeout、offline、connection failure 或 corrupt response 不算 definitive absence；
- 没有 accepted authority 证明 `x` 或更晚 coordinate 属于 required committed prefix。

较早 hole 后的 later payload 若没有 required authority，只是必须 suppress 的 speculative suffix，不得把 hole 命名为 DATA_LOSS；若 later accepted authority 使该 coordinate required，则根据证据进入 deferred、quarantine 或 DATA_LOSS，且绝不能发布跨 hole prefix。

顶层必须保留五类语义，exact enum/exception/wire code 保持开放：

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

最低 AutoRecovery 顺序：

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
7. CAS standard ensemble replacement
8. transition target to COMMITTED_REPLICA/READABLE
   - close/revoke RECOVERY_ONLY
   - do not grant normal writable authority
9. publish domain-local replica/membership result COMMITTED,
   conditioned on the same intent/fence predecessor
10. compact intent only after delete-history or cleanup authority safely supersedes it
```

inactive install与admission之间不锁定先后，可并行或交换；不可删合同是grant、第一份payload和ensemble publication都晚于有效admission。admission CAS response loss必须重读exact lifecycle/delete-fence head或其已提交snapshot/summary：matching operation identity+payload已admit时继续同一intent/target；delete先赢时child保持inert；conflicting payload、unknown mandatory state、head gap或无法判定时fail closed/deferred。timeout不得盲建第二个intent、选择第二个target或授予grant。

master key只继续作为data credential，不能授权grant/close；`AuthDisabledPlugin`/anonymous以及authenticated-but-unauthorized caller必须拒绝。caller必须被授权执行exact grant/close operation + ledger instance + target/range scope，且AuthN/AuthZ检查早于local grant、allocation或任何durable effect。grant/reference按recovery purpose domain-separate，并绑定ledger/instance/descriptor、RepairIntent identity/generation、target stable identity/storage incarnation、exact range、local grant generation、delete fence和operation payload identity。Bookie只在cold direct-read committed authority并durable写入本地bounded grant后接受Profile recovery Add；receipt/status不得泄漏secret、offline verifier或bearer/replay capability。exact ACL/RBAC/plugin、auth channel与reference encoding保持OPEN。

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

新增冷控制成本严格限定为每repair operation/fragment的single-record immutable inert-child create、一次lifecycle/delete-fence admission-head CAS、target normal-inactive install durable transition、target recovery-grant durable transition、现有standard membership CAS与domain-local completion CAS。matching durable install已存在时，install transition可幂等解析为既存结果；这些是语义transition，不要求各自独占物理fsync。exact record packing、queueing、group commit和fsync次数保持开放，backend可group/batch，但portable safety不依赖multi-key transaction。benchmark必须分别覆盖cold install与already-installed/idempotent retry；recovery payload热路径不增加per-entry metadata round trip、per-entry control fsync、per-entry intent update或ledger-global repair-progress CAS，normal Add不读取sidecar。

### 9.2 Range-scoped permanent-loss budget reset

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
9. CAS exact standard ensemble replacement
10. close/revoke recovery grant and transition target role for its actual purpose
11. CAS domain-local durable strong completion conditioned on the same
   membership mapping, control/repair/policy generation,
   delete fence and overlapping loss/reset predecessor
   -- reset for this exact range linearizes here --
```

closed/historical target 不需要 normal activation；其 strong completion receipt 在证明完整 range、exact mapping 与 `F+1` coverage 后可以 reset。current writable replacement 若要承接未来 normal Adds，必须另行 post-CAS `NORMAL_ACTIVE`；这只授权未来写入，不能替代 CAS 前已 ACK prefix 的 coverage proof。写期 `install → LAC+1 CAS → activate → resend` 不复制历史 fragment，因此绝不能自行清零旧 loss window。

completion authority 必须幂等、可重放，并绑定上述语义。accepted loss declaration 与影响相同或重叠 range 的 completion 必须有单一条件化顺序：loss 先赢则 verifier 排除该 domain 后重新证明；completion 先赢则该 loss 进入新 window。能证明不相交的 range 可以并发，无法证明时保守串行；本合同不要求所有不相交 repair 共享一个 ledger-global hot CAS。delete fence 仍全局阻止迟到 completion。

accepted-loss generation 是排序 token，不是物理 loss counter；相同 domain/incarnation 的 duplicate declaration不能重复消费预算。metadata只能排序 accepted/observed facts，不能宣称感知未观测的物理 failure；proof cut 后实际发生但尚未声明的 failure 仍在语义上进入新 window。迟到 response 或 overlapping stale completion不能抹掉它。

该 assertion 按 operation/range 持久化；允许一个 post-membership completion CAS、bounded child pages + bounded root/head、同 RepairIntent/context/policy 下相邻 range 合并，以及 current interval snapshot + bounded suffix。不要求 stop-the-world、全 ledger 重复制、per-entry MetadataStore update、per-entry control fsync 或无界 receipt history。

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
    deleteDeliveryStreamIdentityAndGeneration
    deleteJournalSequence
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
7. 持久化绑定 Bookie/storage incarnation、stream 与 sequence 的 local effect/receipt；
8. effect durable 后才允许推进对应 per-stream cursor；
9. 返回绑定 local generation 与 delivery coordinate 的 durable receipt。

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
- cursor 推进到 `N` 意味着 `<= N` 的每个 event 都已 durable 应用 delete/tombstone/free 所需 effect，或由可验证 routing/membership proof 判定为不适用；
- 新磁盘、重装或新 storage incarnation 不能继承旧 cursor，只能 verified bootstrap，或提供不可逆 wipe/decommission proof；
- cluster-authoritative assignment 给出有限的 applicable stream set 与 registration required-through；Bookie 不能只报告自己知道的 stream；
- 每 Bookie/storage incarnation 的 applicable stream 数有 manifest-locked finite maximum，超限或 assignment 无法证明时 fail closed；禁止每 ledger 一个长期 stream；
- stream committed head 的普通增长不是 assignment generation 变化，不要求 Bookie重新注册；
- obligation-changing generation 必须通过 predecessor/handoff cut 条件化生效；handoff 允许 duplicate delivery，绝不允许 old/new route 都不负责的 gap；
- ordinary ensemble replacement 既不删除旧节点数据，也不阻止旧 incarnation 返回，因此不能替代 decommission/wipe proof。

snapshot 必须绑定 stream identity/generation、snapshot generation、covered-through sequence、assignment generation/target incarnation、bounded manifest/chunk completeness 与 content/integrity digest，并提供可遍历、可应用的 still-required instance-specific delete effects。每个 retained effect 至少包含 ledger instance、delete epoch/request identity、effect/tombstone identity 与应用或验证 non-applicability 所需的 authority binding。digest-only root 不足；缺 chunk、suffix gap 或内容不完整时不能 bootstrap/writable。

snapshot 可以使用 bounded chunks/reference，root 不展开全部 effects；non-Byzantine 模型下不强制签名、Merkle proof 或 PKI。bootstrap 只能是 verified snapshot + complete no-hole suffix；journal prefix 只有在所有仍支持的 bootstrap 路径都有有效 snapshot 或 terminal decommission proof 后才能 compact。

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
5. durably apply each effect before advancing its stream cursor
6. reconcile RepairIntent and device state
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

Profile range/recovery wire、coordinator、admin status与scheduler必须贯通RFC-0001 semantic error class：unsupported protocol/op/capability/Engine；instance/descriptor conflict；not-ready/stale generation；fenced/tombstoned/deleted；grant missing/stale/out-of-scope/closed；transient/read-only/overload；durability unknown/response loss；unknown mandatory/corrupt/quarantine。exact numeric code、BKException与legacy projection保持OPEN，但unsupported batch只能回退到single-entry Profile recovery，不能回退legacy flag；durability unknown不能触发Classic retry/double write；外部unauthorized可以coarse，内部rich outcome/authority reason不能被抹除。

## 14. 并发规则

### 14.1 Delete vs ensemble change

标准 ensemble membership 仍以 LedgerMetadata CAS 串行化。sidecar只让真正冲突的`DELETE_INTENT`与RepairIntent admission/publication共享同一个ledger-instance lifecycle/delete-fence逻辑顺序：先durable-create immutable inert intent child，再以conditional lifecycle/delete-fence head CAS发布admission reference。`DELETE_INTENT`获胜后，新的ensemble change和RepairIntent admission失败；ensemble change或admission先提交时，delete重读exact authority并将其纳入frozen history。不能用“先list child，再写DELETE_INTENT”的非原子顺序冻结targets。

RepairIntent admission后，copy progress、accepted loss、receipt和completion只推进owning authority-domain predecessor/head，同时绑定admitted intent generation并校验lifecycle/delete-fence generation；它们不为每次更新共享或推进ledger-global universal CAS。delete cut后的stale domain update不能产生grant、payload或completion authority。该逻辑cut不要求跨key transaction，exact admission directory/head、sharding、batching和encoding保持开放。

### 14.2 Delete vs AutoRecovery

AutoRecovery 可以先durable-create inert RepairIntent child，但在授予recovery-only authority、写第一份payload或发布ensemble前必须完成上述admission，并检查exact instance、intent generation和lifecycle/delete-fence cut。`DELETE_INTENT`先赢后，未admit child保持inert，不得授予recovery authority或产生该instance的新replica，只能在orphan proof后回收。

RepairIntent admission先赢时，delete frozen target 必须包含其 replaced member 与 target，无论 copy/CAS 是未开始、部分完成、COMMITTED 还是 aborted-but-dirty。admitted intent 在 target 接收第一份 durable payload 前已进入delete-discoverable authority，因此 metadata 尚未发布的 target 也可被集群完整发现。exact child enumeration、admission directory/head、watermark/index 和 compaction encoding 保持开放。

### 14.3 Delete vs open/read

logical tombstone 是最终 authority。已打开 reader 在 local delete 前被 drain；新的 reader 被拒绝。是否允许 tombstone 前已开始的 read 完成必须形成统一合同，不能因存储引擎不同而变化。

### 14.4 Delete vs ledgerId reuse

所有操作绑定 `ledgerInstanceId`。新 instance 不能因旧 tombstone 被误删，旧 locator 不能因 ledgerId 相同读取新数据。

### 14.5 Registration vs delete stream publication

registration CAS 必须校验相同 effective assignment generation、storage incarnation 与 required-through vector。delete event 先于 registration cut 提交时必须进入 required-through；registration 先赢时，后续 delete 通过 active online route投递并保留在 stream。assignment handoff时 event 可以双投但不能落入 routing gap。stale generation不能跨 effective obligation-changing cut writable；assignment watch、poll 或 response 到达顺序不构成线性化 authority。

### 14.6 Repair completion vs permanent loss

range-scoped completion CAS 必须条件化校验相同 failure-domain policy generation、accepted loss/reset predecessor、membership mapping、delete/control fence 与 coverage cut。影响相同/重叠 range 的 loss/completion单序，能证明不相交的 range 可以并发。新 permanent loss 若在 proof cut 后发生，必须计入新 window；旧 completion response 迟到不能把该 loss 清零。delete 先赢时 repair 不得 completion；repair completion 先赢时 delete history保留其 receipt/target。

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
11. delete cursor 只在对应 durable effect 或可验证 non-applicability 后推进，且不能跨越 stream gap。
12. writable registration 意味着该 storage incarnation 对 cluster-authoritative finite assignment 的全部 required-through stream 已 catch up。
13. snapshot + suffix 必须完整；旧 storage incarnation 不能借新 identity 或 ordinary ensemble replacement 绕过 catch-up。
14. obligation-changing assignment generation只有在 handoff catch-up 后 effective；stale generation不能跨 effective cut继续 writable。
15. snapshot 必须可枚举并应用 effects，digest-only root不足；stream removal不能把同一 old incarnation obligation丢失或转嫁。
16. catch-up exemption只来自 cluster-accepted irreversible wipe/permanent decommission fence；proof绑定Bookie、old incarnation、storage scope、operation generation与cluster acceptance，不能跨scope重放。
17. 未完成lifecycle/delete-fence admission的RepairIntent child不授予grant或payload authority；cut前全部admitted intent必须进入frozen targets，admission后的progress不推进universal ledger head。

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

本 RFC 进入 Accepted 前必须：

- 当前 OSS BatchRead 行为和限制有 executable baseline tests；
- streaming continuation、per-entry result、limits 和 cancellation wire contract 冻结；
- normal read 与 recovery evidence 的成功条件分别定义；
- general E/W/A merge 有伪代码、复杂度、Model E 和故障测试；
- deterministic fallback 的 RecoveryContext、earliest-unresolved 与 outcome class 有 executable tests；
- required frontier、normal-tail quorum-intersection absence 与五类 recovery outcome有 executable point-read oracle tests；
- internal rich outcome、durable-close-only legacy `OK`、admin/AutoRecovery投影和compatibility matrix有executable tests；
- Profile normal/recovery opcode separation、batch→single Profile fallback、legacy flag伪造、old/mixed Bookie no-downgrade、unknown subtype与response-loss exact retry有raw-wire/端到端测试；
- non-anonymous且exact operation/instance/target-range scope authorized repair control principal、authenticated-but-unauthorized负向路径、target incarnation/purpose/range/grant-generation binding、direct-read committed RepairIntent/delete fence、secret leak与grant status query通过测试；
- RepairIntent identity、lifecycle/delete-fence admission、retention、target discovery、strong assertion、range-scoped `F+1` coverage、conflicting-range loss ordering 与 bounded receipt snapshot冻结；
- DeleteManifest schema、target freeze 和 CAS 线性化点冻结；
- bounded stream assignment的 PREPARED/effective handoff、storage incarnation、可应用snapshot+suffix、per-stream cursor、registration cut 与 terminal wipe/decommission proof有集群级端到端测试；
- logical/physical API completion 可分别观测；
- Model D 无 safety counterexample；
- Segment local reclaim 与 RFC-0003 generation tests 联动通过。

允许分阶段接受：Batch/Range 和 Cluster Delete 可以成为同一 RFC 的独立 feature gate；任何一部分通过不得自动提升另一部分。

## 19. 开放问题

- streaming range wire opcode 与现有 BatchRead 的兼容/复用方式；
- continuation token 的签名、过期和跨 Bookie 行为；
- general E/W/A recovery merge 的精确算法；
- TailSummary 是否仅为 hint，还是进入未来 quorum proof；
- recovery fast-path/global attempt 的 hard bounds、outcome exact error mapping 与超限运维流程；
- recovery outcome exact enum/exception/wire mapping、additive admin result/status、cancellation API与metrics names/thresholds；
- `DataUnknownException` factory/mixed-version修复或generic quarantine mapping的最终选择；
- volatile proof cache 与可选 operation-scoped checkpoint 的 exact encoding/durability；
- Delete Coordinator 的部署、leader election 和 manifest namespace；
- RepairIntent exact path、admission directory/head、child enumeration/watermark/index、状态名、sharding/batching和compaction encoding；
- repair strong-assertion receipt schema、audit commitment、failure-domain identity/policy、accepted-loss namespace、range-sharded/global topology 与 interval page/fan-out/compaction；
- recovery-only local record packing、BatchRecoveryAdd wire schema 与 batch limits；
- Profile single/batch recovery logical subtype/opcode、grant reference/auth channel、target-incarnation binding、exact semantic error与legacy projection；
- delete stream topology/count、assignment store、handoff encoding、event batching、snapshot chunk/manifest encoding 与 journal compaction；
- decommission/unrecoverable 的授权流程和 durable proof；
- maximum rejoin window 与 compact tombstone 生命周期；
- Classic/Direct/Segment reader drain 的统一 API；
- physical deletion receipt 的审计与 metrics。

这些问题关闭前，不得把 Range、general E/W/A fast recovery 或 Conditional Delete 标为 Implementation Ready。
