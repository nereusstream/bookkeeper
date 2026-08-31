# Round 8：Sequenced WAL Client Authority 与 Frontier

> Reviewer：固定 `gpt-5.6-sol / max` agent `rfc2_design_reviewer`<br>
> 源码与文档基线：`9074497dde8746b3a80169e13178c05a830086a8`<br>
> 评审模式：client-side sequencing、frontier authority、suppressed outcome、E/W/A、性能和复杂度联合审查<br>
> 记录规则：以下 reviewer 回复未经删节，仅清理行尾空白。规范是否采纳以对应 RFC 的集成结果为准。

## 本轮 Questions

### Q1：Append admission linearization

冻结 sequence 分配、immutable envelope 与 existing Add admission 的同序线性化点；复用现有 ordered callback，不增加第二套 queue，也不预设修改 `PendingAddOp`。

### Q2：Commit、discoverability 与 sealed authority

区分 active writer commit、独立 reader discoverability 和 failover 后 sealed authority，判断 ordered success callback、LAC/explicit LAC、RunSeal/domain-head CAS 的职责，以及是否需要 per-append metadata 或 commit entry。

### Q3：Suppressed suffix outcome 与 horizon

判断 `ABORTED_SUPPRESSED` 的权威资格、bounded suppressed identity table 的作用、从未形成 durable identity 的 reservation，以及 retry/retention/horizon 中可锁定和必须保持 OPEN 的边界。

### Q4：E/W/A 与 capability scope

判断基础 client-side sequencing 是否必须 `E = W`，区分 fenced point recovery、installed Bookie local validation 与 range/TailSummary fast path，并复核 `CLIENT_ONLY`、installed epoch 和 production Add 热路径边界。

## Reviewer 完整回复

<!-- BEGIN VERBATIM REVIEW -->

总体裁定：**REVISE → 可锁定核心语义；物理 schema、retention、capability registry 与内部接缝继续 OPEN。**

当前设计方向正确：sequence 由受信任的 Client 分配，BookKeeper quorum 提供 durability，现有 ordered callback 推进活动写者 commit，fencing/recovery/RunSeal/domain-head CAS 决定切主后的最终前缀。需要修正的是四处权威边界，而不是增加 per-append metadata、第二套 completion queue 或限制所有场景为 `E=W`。

## Q1：sequence 分配、envelope 构造、Add 入队

**裁定：REVISE → 线性化语义 LOCK；内部实现接缝 OPEN。**

### 问题

当前 [RFC-0002](/Users/liusinan/apps/ideaproject/nereusstream/bookkeeper/docs/rfcs/unified-wal/RFC-0002-sequenced-wal.md:151) 只规定 sequence 在 Add 前分配，没有冻结多线程调用下的顺序关系。

现有 [LedgerHandle.doAsyncAddEntry](/Users/liusinan/apps/ideaproject/nereusstream/bookkeeper/bookkeeper-server/src/main/java/org/apache/bookkeeper/client/LedgerHandle.java:1690) 只在自己的同步区内完成：

```text
entryId 分配
ledger length 更新
PendingAddOp 入 pendingAddOps
```

如果 sequence 在这个同步区之外独立分配，会出现：

```text
线程 A 分配 sequence 100，构造 envelope 时暂停
线程 B 分配 sequence 101，先调用 Add，取得较小 entryId
线程 A 后调用 Add，取得较大 entryId
```

此时现有 ordered callback 会先提交 sequence 101，再遇到 sequence 100，无法得到连续 WalSequence frontier。仅使用原子 `nextSequence += recordCount` 不能解决这个问题。

### 可直接同步的 LOCK 合同

```text
Append admission linearization

1. 每个 active run 只有一个受信任的 SequencedWalHandle；
   底层 LedgerHandle 不得暴露给普通调用方，也不得允许 raw Add
   与 sequenced DATA Add 绕过同一 admission order。

2. inflight entry/bytes credit、recordCount/payload 校验和不依赖
   sequence 的 payload digest，应在线性化点之前完成。
   未获得 credit 的请求不得消耗 sequence。

3. 每个 DATA append 必须跨过唯一 admission linearization point。
   在同一串行化边界内必须完成：
   a. 从 nextSequence 选择 [sequenceStart, sequenceEnd]；
   b. 构造并冻结绑定 run identity、appendId、payload digest、
      recordCount 和该 interval 的 immutable envelope；
   c. 将该 envelope 交给同一个 LedgerHandle，并保证其 entryId
      已分配且 PendingAddOp 已进入现有 pendingAddOps；
   d. 然后才允许更晚 append 执行步骤 a。

4. 对同一 run 中按 admission 顺序相邻的 DATA append i、j：

   entryId(i) < entryId(j)
   sequenceEnd(i) < sequenceStart(j)
   sequenceStart(j) = sequenceEnd(i) + 1

5. admission linearization point 在现有 pendingAddOps 入队处结束，
   不等待 Bookie RPC、AQ 或 callback。入队后的 I/O 继续并发。

6. 在成功入队前失败的请求不形成 durable append identity，
   不得暴露 sequence receipt；入队后发生 timeout、response loss
   或 writer crash 的请求进入 OUTCOME_UNKNOWN/recovery 解析。

7. control entry 如果与 DATA 共用 ledger，必须经过同一 submission
   order 并有无歧义类型；它是否占用普通 entryId、是否消耗
   WalSequence 继续 OPEN。
```

### ordered callback 的复用

现有 [sendAddSuccessCallbacks](/Users/liusinan/apps/ideaproject/nereusstream/bookkeeper/bookkeeper-server/src/main/java/org/apache/bookkeeper/client/LedgerHandle.java:2198) 已经：

1. 只从 `pendingAddOps` 队头推进；
2. 队头未形成成功条件时停止；
3. 校验下一 entryId；
4. 更新 `pendingAddsSequenceHead/lastAddConfirmed`；
5. 再调用原 Add callback。

因此可锁定：

```text
- PendingAddOp 达到合法 AQ 只形成底层 AQ candidate。
- 只有现有队头 ordered success transition 才推进
  writerCommittedSequence。
- SequencedWalHandle 用 callback wrapper/ctx 持有
  run、sequence interval、appendId 和 digest。
- 原 callback 以 OK 到达时，直接把 writer frontier 更新到
  该 callback 对应的 sequenceEnd，并完成 WalAppendReceipt。
- 不增加第二个 pending queue、completion reorder buffer、
  commit state machine 或 sequence LAC。
```

callback wrapper 中的一个 `lastWriterCommittedSequence` 标量不是第二套 queue。

### 是否修改 `PendingAddOp`

**LOCK：当前不直接修改。**

理由：

- sequence tuple 可由 callback wrapper/ctx 完整携带；
- `PendingAddOp` 是生产 Classic Add 热路径上的回收对象，新增字段会扩大 reset、并发、recycle 和兼容审计面；
- 现有 ordered queue 已足够，不需要为了“复用”而把上层 WAL 类型灌入底层 op；
- Wave 0 明确禁止触碰生产 Add/ACK。

实现选择继续 OPEN：

```text
- SequencedWalHandle 自身的窄 admission lock；
- 复用 ledger-pinned ordered executor；
- 经 benchmark 证明必要后，为 LedgerHandle 增加只暴露
  “assign entryId + enqueue”的最小内部 seam。
```

如果使用 wrapper lock，健康路径只串行化固定 envelope header 的最终组装和同步入队；payload digest 等较重工作应提前完成。需要测量 `waitForWriteSet` 等现有同步逻辑是否扩大锁持有时间。未有数据前不要修改 `PendingAddOp`。

### 性能与复杂度影响

- 不增加 RPC、quorum、fsync 或 MetadataStore 访问；
- 只增加一个短 admission 临界区、固定 envelope 编码和每个 inflight callback 的少量状态；
- 网络写与 AQ 等待仍并发；
- 代价是 append admission 的本地串行化，但单写者日志本来就必须建立这个 total order；
- 避免第二套 reorder queue，显著降低状态机、回收和故障恢复复杂度。

---

## Q2：活动 commit、reader discoverability、sealed authority

**裁定：LOCK 三种 frontier；REVISE 当前含混的 `published frontier`；exact RunSeal schema OPEN。**

三者不能继续混称一个 frontier。

### 可直接同步的 LOCK 合同

```text
writerCommittedSequence

- 只属于当前 active writer/run。
- 由 SYNC_ON_ACK 下现有 LedgerHandle ordered success transition 推进。
- 当且仅当该 interval 有合法 AQ，且所有更早 DATA interval 已按
  admission order推进，才允许生成和发送 final receipt。
- response/receipt loss 不撤销已经成立的 commit。
- 不要求 per-append MetadataStore write 或额外 commit entry。
```

```text
readerDiscoverableSequence

- 是独立 reader 当前能够安全发现的 lower bound，不保证等于最新
  writerCommittedSequence。
- active ledger 上从 BookKeeper LAC/piggybacked LAC/explicit LAC
  得到 entry frontier。
- LAC 数字本身不是 WalSequence；reader 必须绑定正确的
  ledgerId + ledgerInstanceId + run authority，并读取、校验相应
  entry envelope 后才能得到 sequenceEnd。
- invalid/missing/mismatched envelope 必须 fail closed；不能从 LAC
  猜测 sequence。
- explicit LAC 只是降低尾部发现延迟的周期/批量机制，不成为
  correctness 依赖，也不要求每 append 写一次。
```

现有 [ExplicitLacFlushPolicy](/Users/liusinan/apps/ideaproject/nereusstream/bookkeeper/bookkeeper-server/src/main/java/org/apache/bookkeeper/client/ExplicitLacFlushPolicy.java:84) 正是周期性传播 writer `lastAddConfirmed`；[PendingReadLacOp](/Users/liusinan/apps/ideaproject/nereusstream/bookkeeper/bookkeeper-server/src/main/java/org/apache/bookkeeper/client/PendingReadLacOp.java:29) 也明确把 explicit/piggyback LAC 定位为 tailing reader 的 best effort。它可以安全滞后，不能承担 run 的最终 authority。

```text
sealedSequence

- predecessor 经唯一 recovery authority、BookKeeper fencing、
  point recovery 和 durable CLOSED metadata 后，由已验证 envelope
  推导 lastCommittedEntryId 与 lastCommittedSequence P。
- recovery 方先创建 immutable RunSeal child。
- RunSeal child 在被 SequenceDomain head 的 single-record versioned
  CAS 引用前只是 inert record。
- domain-head CAS 同时绑定 predecessor RunSeal 和 successor
  start=P+1；该 CAS 是 successor publication 的线性化点。
- CAS response loss 必须读取 exact head 解析，不能盲建第二个
  successor。
- head CAS 之后，任何 predecessor sequence>P 的 physical suffix
  永久不能进入 WAL_COMMITTED。
```

建议 `RunSeal` 最低语义绑定：

```text
sequenceDomainId
predecessorRunId / writerEpoch
ledgerId / ledgerInstanceId
recovered CLOSED metadata identity/version/digest
lastCommittedEntryId
lastCommittedSequence
seal operation identity/generation
optional suppressed-outcome child/reference
```

domain head 最低绑定：

```text
expected predecessor/head generation
RunSeal identity/content identity
successor run identity
successor ledgerId / ledgerInstanceId
successor startSequence = P + 1
```

不需要 MetadataStore 多 key transaction：immutable child 先落盘，single-record head CAS 再赋予 authority。

### 不需要的机制

**LOCK：健康 append 不增加以下任何一项：**

```text
per-append MetadataStore CAS
per-append committedSequence 更新
每条 payload 后的 commit control entry
每条 append 的额外 fsync
第二个 LAC
```

run footer、range-index control entry、TailSummary 都只能是可丢弃加速。RunSeal/head CAS 是每个 run 转换的冷路径，不是每 append 热路径。

### 需要修订的 `WAL_COMMITTED`

建议把活动写者定义精确为：

```text
WAL_COMMITTED(active run)
    = valid AQ evidence
    && every earlier DATA interval has crossed the existing ordered
       success transition
    && writerCommittedSequence covers this interval
```

final receipt 只在此后具备发送资格；独立 reader 是否已经从 LAC 发现该 entry 不参与 commit 判定。

failover 后则由 authoritative RunSeal 重建最终结论。恢复可以把 response 丢失但位于最大连续可证明前缀内的 AQ candidate 纳入 sealed prefix；这正是 `OUTCOME_UNKNOWN` 可能解析为 `COMMITTED` 的原因。

### 性能与复杂度影响

- 健康写路径没有新增远程操作；
- reader discoverability 可能落后于 writer commit，这是明确语义而不是 correctness bug；
- 若业务要求独立 reader 的即时 freshness，可以配置周期 explicit LAC、使用 receipt 中的 entryId 做定点读取，或显式定义一个付费 barrier；不能把该 SLA隐含进普通 append；
- RunSeal/head CAS 仅增加切主/关闭冷路径成本；
- immutable child + single head CAS 避免热点 head 无界增长和通用事务。

---

## Q3：suppressed suffix、appendId outcome 与 horizon

**裁定：REVISE；suppression safety 与权威结果的资格条件可 LOCK；完整 table、retry、retention/horizon 继续 OPEN。**

### 可直接同步的 LOCK 合同

```text
1. RunSeal(P) 本身已经永久 suppress predecessor 的全部 sequence>P；
   correctness 不依赖 suppressed appendId table。

2. ABORTED_SUPPRESSED 只能由 authoritative published RunSeal 给出，
   且该 seal 必须显式绑定对应 durable append identity，至少包括：

   predecessorRunId
   ledgerInstanceId
   appendId
   payloadDigest
   provisionalSequenceStart/End
   seal identity/generation

3. “durable append identity”至少要求 recovery 在冻结的
   RecoveryContext 下，从合法 predecessor replica 得到并校验一个
   完整 envelope，且该 identity 被纳入最终 RunSeal。
   它不等于已经达到 AQ；abort authority 来自 seal，不来自单副本。

4. 只存在于 writer 内存中的 sequence reservation、尚未形成任何
   可恢复 envelope 的请求，不能写入 suppressed table，不能获得
   cluster-authoritative ABORTED_SUPPRESSED。

5. suppressed table 中不存在某 appendId，不能解释为 REJECTED、
   未尝试或可安全复用；除非 recovery 另有完整枚举证明，否则只能
   返回 OUTCOME_UNKNOWN/NOT_FOUND_WITHIN_HORIZON 一类非终态结果。

6. 如果同一 appendId 已在 sealed committed prefix 中出现，
   query 必须返回 COMMITTED；suffix 中的同内容重复不能覆盖它。
   同一 appendId 出现不同 payloadDigest，或映射到两个不可调和的
   committed interval，必须 conflict/quarantine，不能任选一份。

7. sequence 被 successor 复用时，查询和 locator 必须绑定
   runId + ledgerInstanceId；仅凭 sequence 数字不得把旧 appendId
   映射到 successor payload。
```

### bounded suppressed table 的定位

建议把它冻结为**冷路径 appendId outcome accelerator**，而不是 commit authority：

```text
RunSeal 或其 immutable child 可以保存 recovery 实际观察并验证的
bounded suppressed identities。它由 maxInflightEntries、
maxInflightBytes 和 bounded recovery scan 约束，不进入普通 append
热路径，也不塞入不断增长的 SequenceDomain head。
```

如果 table 只覆盖 recovery 已观察到的 identities，它可以是 partial；absence 不能证明任何结果。

如果希望 absence 也具有权威含义，必须另外证明：

```text
- suffix 的所有可能合法 source set 已冻结；
- bounded scan 已完整枚举；
- offline/timeout/corrupt evidence 已按 recovery oracle 处理；
- table 未因 size/bytes limit 截断；
- RunSeal 显式标记 COMPLETE_SUPPRESSED_IDENTITY_COVERAGE。
```

否则 table 只能标记 partial。

### reservation 能否得到权威终态

**明确答案：不能。**

从未形成 durable identity 的 reservation：

- 可以在原进程仍存活且可证明没有入队时，向原调用方返回本地 `REJECTED`；
- writer crash 后，集群没有事实可证明该 appendId 与 reservation 的对应关系；
- 不得由 `nextSequence` 的缺口、inflight window 大小或调用方自报推导 `ABORTED_SUPPRESSED`；
- 为解决这一点而增加 per-append MetadataStore record 得不偿失，也不应采用。

### 结果 taxonomy

可以锁定边界，但不要现在锁死全部 API enum：

```text
COMMITTED
    authoritative committed prefix 中存在 exact appendId+digest。

ABORTED_SUPPRESSED
    authoritative RunSeal 明确绑定该 predecessor durable identity，
    且它不在 committed prefix。

REJECTED
    可证明在任何可能的数据面写入前失败；除非另有 durable record，
    通常只对当次调用响应成立，不是 crash 后的集群查询事实。

OUTCOME_UNKNOWN
    evidence 不足、table partial、authority 暂不可用或 identity
    从未 durable。

TRIMMED / EXPIRED
    只有 durable retention authority 明确证明结果曾在 horizon 内、
    现已按策略移除时才能返回；不能把记录缺失直接叫 TRIMMED。
```

### 必须保留的 OPEN

可直接写入开放问题：

```text
- suppressed table 是 mandatory 还是 optional accelerator；
- durable identity 的 exact evidence predicate；
- partial 与 complete coverage 的 proof/schema；
- 同一 appendId 在 ABORTED_SUPPRESSED 后是否可在 successor
  重新 admission，以及 outcome 如何演化；
- appendIdRetentionBoundary、查询 horizon、TRIMMED/EXPIRED 语义；
- table 的 hard entries/bytes bound、overflow/deferred 行为；
- immutable child/page/manifest 编码与 compaction；
- run 删除后由谁继续承载 appendId conflict horizon；
- duplicate same-content、different-content conflict 的 exact API；
- recovery scan 无法在当前 attempt 完整枚举时返回
  DEFERRED 还是 partial seal + OUTCOME_UNKNOWN。
```

### 性能与复杂度影响

- table 只在 failover/seal 冷路径生成，无普通 append 成本；
- 表大小受 inflight bounds 控制，但不能因此假定一定适合塞进 head；
- complete table 可能显著增加 takeover scan 和离线副本等待，因此不应成为 sealing safety 的必要条件；
- partial table 保持系统可用，但必须让 absence 非权威；
- 不增加 per-append durable operation record，避免 metadata 写放大和无界 idempotency history。

---

## Q4：E/W/A、point recovery、installed validation 与 fast path

**裁定：REVISE；禁止 blanket `E=W` 可 LOCK；exact production combination/capability matrix OPEN。**

### 当前合法性边界

[RFC-0001](/Users/liusinan/apps/ideaproject/nereusstream/bookkeeper/docs/rfcs/unified-wal/RFC-0001-profile-capability-install.md:163) 当前只冻结结构约束：

```text
1 <= A <= W <= E
```

production Profile 的 exact combination table 仍未接受，因此不能把任意组合宣称为当前 production-ready。

但 RFC-0002 的语义合同不应新增：

```text
E = W
```

这一 blanket 限制。

### 为什么不需要 blanket `E=W`

sequence 的 total order 来自单一 `SequencedWalHandle` 和现有 LedgerHandle entry admission，不来自单个 Bookie 看见所有 entry。

现有 BookKeeper point recovery 已经：

- 按 ledger metadata/ensemble history 计算每个 entry 的 write set；
- fencing 后从 LAC 向前逐 entry 读取；
- recovery read 使用 `W-A+1` 个 write-set member 的 definitive absence 作为停止条件；
- recovery-add 后发布 durable CLOSED metadata。

对应源码见 [LedgerRecoveryOp](/Users/liusinan/apps/ideaproject/nereusstream/bookkeeper/bookkeeper-server/src/main/java/org/apache/bookkeeper/client/LedgerRecoveryOp.java:92) 和 [PendingReadOp](/Users/liusinan/apps/ideaproject/nereusstream/bookkeeper/bookkeeper-server/src/main/java/org/apache/bookkeeper/client/PendingReadOp.java:271)。

因此 `CLIENT_ONLY` 的正确性基线可以复用现有 fenced point recovery，再校验 recovered entries 的 sequence envelope；不需要先建设 general range merge，也不要求每个 Bookie 持有完整 sequence history。

### 可直接同步的 LOCK 合同

```text
- 当前 Sequenced WAL 只允许 SYNC_ON_ACK。
- 基础 client-side sequencing 不要求 E=W。
- Model B 和 executable matrix 至少覆盖：
  3/3/2
  3/3/3
  3/2/2
  以及一般 E>W write-set rotation/ensemble change 结构。
- CLIENT_ONLY takeover 的 correctness oracle 是 fenced BookKeeper
  point recovery + durable close + envelope validation + RunSeal。
- range、TailSummary、derived index 都不能替代该 oracle。
```

### installed validation 的精确边界

`BK_SEQUENCED_CLASSIC_INSTALLED` 只说明：

```text
- RFC-0001 install/activation 成立；
- ledgerInstanceId 和 descriptor identity 可由 Bookie fail closed 校验；
- descriptor 中逐项声明的 mandatory capabilities 生效。
```

它不能整体隐含：

```text
writerEpoch rejection
same-ledger takeover
global appendId uniqueness
global sequence continuity
full local sequence index
authoritative TailSummary
```

在 `E>W` 下，一个 Bookie 只见 rotating write set 的子集，因此可以安全执行的本地 validation 包括：

```text
- request/ledger instance/profile identity；
- envelope 结构、bounds、checksum；
- request entryId 与本地 payload identity；
- 同一 entryId 上的 duplicate/different-payload conflict；
- 本节点实际观察到的 local index。
```

但单 Bookie不能宣称：

```text
- 整个 run 没有 sequence hole；
- 同一 appendId 没出现在另一个 write set；
- 它的 local last sequence 是全局 frontier。
```

若 capability 要宣称这些全局语义，必须明确其 scope、coverage 和恢复合并算法；不能由 `INSTALLED` 名称推断。

### Range/TailSummary fast path

可直接同步：

```text
- 当前 range/batch-recovery subtype 仍 reserved/disabled。
- TailSummary 默认只是绑定 instance/generation 的 hint。
- E=W 可以作为某个 fast path capability 的局部前置条件，
  但不是 Sequenced WAL Profile 的全局前置。
- general E>W range/TailSummary fast recovery 在 RFC-0004 exact
  merge algorithm、Model E 和 point-oracle equivalence 通过前禁用。
- unsupported、stale、partial 或 budget exhausted 必须回到
  earliest unresolved coordinate 的 point recovery。
```

这把复杂度限制在 fast path，不污染基础正确性。

### blanket `E=W` 的性能代价

以 `E=3,W=2` 为例，强制 `E=W=3` 会把每 entry 的 replica write 从 2 增加到 3，即约 50% 的副本网络和磁盘写入增量，同时减少 placement 灵活性。它确实让每个 Bookie 的本地 index 更完整，但不能为尚未启用的 fast path 让所有 client-only 写入永久承担该成本。

### exact combination 仍 OPEN

```text
- production descriptor 的 exact E/W/A/F/failure-domain matrix；
- 哪些 installed capability 允许 E>W；
- local partial index 的 coverage 表达；
- sequence lookup/range merge/TailSummary 的 capability bits；
- general E>W fast recovery 的算法、复杂度与 Model E；
- capability 不满足时的 exact error/API projection。
```

---

## `CLIENT_ONLY`、installed epoch 与 Profile 定位

### `CLIENT_ONLY` 不是演进阶段

**LOCK：**

```text
BK_SEQUENCED_CLASSIC_CLIENT_ONLY
BK_SEQUENCED_CLASSIC_INSTALLED
```

是能力与信任边界不同的并列 Profile，不是时间阶段、成熟度顺序或替代关系。

建议文档表述：

```text
CLIENT_ONLY 适用于受信任 SequencedWalHandle 独占写入入口的部署。
其正确性依赖 client admission contract、BookKeeper quorum、
fencing/recovery 和 run authority；旧 Bookie 只保存 opaque envelope。

INSTALLED 在显式安装和逐项 capability 声明下增加 Bookie 侧
fail-closed validation。是否采用哪一 Profile 是部署/信任边界选择，
不能从名称推断生产资格或演进顺序。
```

### installed 不隐含 epoch 校验

**LOCK：**

- `writerEpoch` 出现在 envelope/descriptor 中不等于 Bookie 拥有 epoch authority；
- `INSTALLED` 本身不允许 same-ledger epoch takeover；
- 只有单独接受并声明 `EPOCH_AWARE_ADD`，且闭合 durable epoch authority、Add identity、Bookie monotonic check、replacement/recovery/mixed-version model 后，才能改变该结论；
- 当前 `CLIENT_ONLY` 和不含该 capability 的 `INSTALLED` 都继续使用 predecessor fence/recovery/seal/successor 流程。

---

## 建议同步到 RFC-0002 的 OPEN 列表

```text
- admission linearization 的 exact Java/internal seam；
- control entry 与 DATA 的共用排序、entryId/sequence consumption；
- RunSeal immutable child 与 domain-head exact schema/path/hard bound；
- active reader freshness SLA 与 explicit LAC policy；
- suppressed identity table 的 completeness、evidence predicate、
  retry 和 retention horizon；
- exact production E/W/A/capability legal-combination table；
- E>W 下 local validation/index scope 与 general fast-path merge；
- EPOCH_AWARE_ADD 仍不属于当前 capability set；
- 只有 benchmark 证明 wrapper admission 成为瓶颈时，才评审
  LedgerHandle 最小 enqueue seam；不预设修改 PendingAddOp。
```

## 最终结论

本轮应同步的最小设计不是增加新的基础设施，而是冻结三条边界：

```text
admission order:
sequence allocation + immutable envelope + existing Add enqueue
必须同序线性化

commit/discovery/seal:
ordered success callback / LAC+envelope / RunSeal+head CAS
分别承担不同 authority

capability scope:
基础 sequencing 支持 E>W point recovery；
E=W 只可约束具体 fast path；
INSTALLED 和 epoch/global continuity 不得互相暗含
```

这样健康 append 仍保持：

```text
0 per-append MetadataStore operation
0 extra quorum round
0 extra commit entry
0 extra fsync
0 second completion queue
```

Wave 0 继续保持 `EXPERIMENTAL / NON-PROMOTABLE / NO_AUTHORITY / DISCARDABLE`。本轮不建议、也没有授权触碰生产 `LedgerHandle`、`PendingAddOp`、Add/ACK、Journal 或 wire/on-disk surface。

只读复核基线为 `turbo-bk@9074497dde8746b3a80169e13178c05a830086a8`，当时与 `origin/turbo-bk` 一致；原有未跟踪 `BtrLog Low-Latency Logging.pdf` 未触碰，本轮未修改、暂存、提交或推送任何文件。

<!-- END VERBATIM REVIEW -->

## Integration Audit 1

> Reviewer：同一固定 `gpt-5.6-sol / max` agent `rfc2_design_reviewer`<br>
> 结论：**REVISE / NOT APPROVED**<br>
> 归档规则：以下回复未经删节，仅清理行尾空白。

<!-- BEGIN VERBATIM INTEGRATION AUDIT 1 -->

结论：**REVISE，暂不 APPROVE。** Round 8 归档完整、绝大多数设计已正确集成，但存在 1 个 authority 线性化冲突和 1 组旧术语残留。当前不应把 README 状态改为 `Reviewed / Integrated`，也暂不授权提交/推送这 5 份 Markdown。

### P1 — 必须修复：suppression authority 被提前赋予 durable CLOSED

[RFC-0002 §10](/Users/liusinan/apps/ideaproject/nereusstream/bookkeeper/docs/rfcs/unified-wal/RFC-0002-sequenced-wal.md:306) 的恢复步骤 7 当前写成：

> durable close predecessor，并永久 suppress 所有 sequence > P

但步骤 9 才创建 inert `RunSeal`，步骤 10 才由 domain-head CAS 引用并发布它。这与同文档“RunSeal 在 head CAS 前无 authority”的 LOCK 冲突。

若进程在步骤 7 后、head CAS 前崩溃，文档会同时声称：

- sequence suffix 已获得永久终态；
- 又没有任何被 domain head 发布的 RunSeal 可供独立 reader/retry 权威证明。

这会把普通 BK `CLOSED` 错误提升成 SequenceDomain authority，并形成 CLOSED 与 domain head 两套 suppression authority。

建议精确改为：

1. 步骤 7：

   > 在 recovered lastEntryId durable close predecessor；该动作只冻结 BookKeeper entry boundary，不发布 SequenceDomain seal，也不赋予任何 appendId/sequence terminal suppression。

2. 步骤 9：

   > 创建 immutable、inert 的 `RunSeal(P)` child；在 domain-head CAS 成功前，它不是权威事实。

3. 步骤 10：

   > 以单记录 domain-head CAS 同时引用并发布 `RunSeal(P)` 与 successor。CAS 成功是唯一 publication linearization point；仅从此刻起，predecessor 中 sequence > P 才永久 suppressed，successor 从 P+1 获得 active authority。

同时将 [§7 当前“RunSeal(P) 本身已经永久 suppress”](/Users/liusinan/apps/ideaproject/nereusstream/bookkeeper/docs/rfcs/unified-wal/RFC-0002-sequenced-wal.md:217) 收紧为：

> 被当前 SequenceDomain head 权威引用并发布的 `RunSeal(P)` 永久 suppress predecessor 中所有 sequence > P。

### P2 — 必须修复：仍残留模糊的 `published frontier` 语言

[RFC-0002 §6.2](/Users/liusinan/apps/ideaproject/nereusstream/bookkeeper/docs/rfcs/unified-wal/RFC-0002-sequenced-wal.md:177) 和 [appendId 状态描述](/Users/liusinan/apps/ideaproject/nereusstream/bookkeeper/docs/rfcs/unified-wal/RFC-0002-sequenced-wal.md:204) 仍使用：

- `published WalSequence`
- `未发布 suffix`
- `尚未进入连续发布前缀`

Round 8 已明确禁止把 writer commit、reader discoverability、sealed authority 折叠成一个含混的 `published frontier`。这些表达应分别替换为：

- 正常 active writer 路径：`writerCommittedSequence` 的连续 committed prefix；
- `AQ_CANDIDATE`：底层 AQ 已成立，但尚未通过 ordered success callback 进入 `writerCommittedSequence`；
- failover suffix：位于 authoritative `sealedSequence=P` 之后，而不是笼统称为“未发布”。

### 其余审计结果

- Round 8 archive 完整保留了上一轮 Q1–Q4、CLIENT_ONLY、installed epoch、PendingAddOp、性能/复杂度及 Wave 0 边界反馈，无语义删节。
- Q1 已正确集成：sequence 分配、immutable envelope、Add 入队共享同一 serialized admission boundary；复用既有 ordered callback；不增加第二套 queue；内部 seam 保持 OPEN；不直接修改 `PendingAddOp`。
- Q2 的三条 frontier、LAC/explicit LAC discoverability、RunSeal/domain-head CAS 结构均已进入 RFC、总纲和 Model B；未引入 per-append MetadataStore、commit entry、额外 fsync/quorum/LAC。
- Q3 的 `ABORTED_SUPPRESSED` eligibility 与“从未形成 durable identity 的 reservation 不得获得权威终态”已保留；suppressed table 的 evidence、完整性、retry、retention/horizon 仍为 OPEN。
- Q4 正确拒绝 blanket `E=W`；client-only point recovery、installed validation、range/TailSummary fast path 的边界清楚；E/W/A 矩阵细节仍 OPEN。
- `CLIENT_ONLY` 与 `INSTALLED` 是并列 capability profile，不是演进阶段；installed 不隐含 epoch/global continuity。
- 未引入 V1/V2 或“第一版/后续版本”命名，未修改生产 Add/ACK，Wave 0 仍为 `EXPERIMENTAL / NON-PROMOTABLE / NO_AUTHORITY / DISCARDABLE`。
- 除上述语义问题外，未发现新的编号、Markdown 或跨文档结构错误。

<!-- END VERBATIM INTEGRATION AUDIT 1 -->

## Integration Audit 2

> Reviewer：同一固定 `gpt-5.6-sol / max` agent `rfc2_design_reviewer`<br>
> 结论：**PASS / APPROVE**<br>
> 归档规则：以下回复未经删节，仅清理行尾空白。

<!-- BEGIN VERBATIM INTEGRATION AUDIT 2 -->

结论：**PASS / APPROVE。无剩余阻塞项。**

本次只读复核严格限定于指定 5 份 Markdown，基线为 `9074497dde8746b3a80169e13178c05a830086a8`；PDF 未纳入审计，也未修改任何文件。

核对结果：

- **归档完整性：PASS。** Round 8 原始 review 与 Integration Audit 1 均完整保存，Q1–Q4、CLIENT_ONLY、installed epoch、PendingAddOp、性能/复杂度和 Wave 0 边界无语义删节，BEGIN/END 边界正确。历史归档中保留当轮 `published frontier` 等原文是逐字归档要求，不是当前规范残留，不应清理。
- **Audit 1 修复：PASS。** RFC §6 已改用 `writerCommittedSequence` / `sealedSequence`；`AQ_CANDIDATE` 明确落在 ordered success callback 之前。§7 只允许被当前 domain head 引用并发布的 RunSeal 产生 suppression authority。§10 明确 durable CLOSED 只冻结 BK entry boundary，inert RunSeal 不具 authority，单记录 head CAS 是唯一 publication linearization point，且只有 CAS 成功后 suppression 与 successor authority 才同时成立。footer 明确只是 accelerator。
- **Q1 LOCK：PASS。** sequence allocation、immutable envelope、entryId 分配及 existing Add enqueue 在同一短 admission boundary 内同序；复用既有 ordered success transition；没有第二套 queue/reorder/commit state machine；exact Java seam 与 control-entry consumption 仍 OPEN；未预设修改 `PendingAddOp`。
- **Q2 LOCK：PASS。** RFC、总纲和 Model B 均区分 writer commit、reader discoverability、sealed authority；LAC 必须结合 run/instance 和已验证 envelope；RunSeal child/head CAS 的 inert/publication 边界一致；无第二 authority。
- **Q3 LOCK/OPEN：PASS。** `ABORTED_SUPPRESSED` 只适用于被 authoritative RunSeal 显式绑定的 durable identity；从未形成可恢复 envelope 的 reservation 不获得权威终态；partial table absence 非权威。evidence predicate、complete coverage、retry、retention/horizon、overflow 等仍保持 OPEN。
- **Q4 LOCK/OPEN：PASS。** 未 blanket 限制 `E=W`；基础 CLIENT_ONLY 使用 fenced point recovery；installed local validation 不冒充全局 continuity；range/TailSummary 仍是受 RFC-0004/Model E 约束的 fast path；production E/W/A/capability matrix继续 OPEN。
- **Profile 边界：PASS。** CLIENT_ONLY 与 INSTALLED 是并列 capability/trust Profile，不是演进阶段；INSTALLED 不隐含 epoch rejection、same-ledger takeover 或全局 sequence/appendId authority；`EPOCH_AWARE_ADD` 仍是独立未纳入能力。
- **性能与范围：PASS。** 没有 per-append MetadataStore、commit entry、额外 quorum/fsync/LAC 或第二 completion queue；未引入过度设计；未触碰生产 Add/ACK、LedgerHandle、PendingAddOp、Journal 或 wire/on-disk surface。Wave 0 仍为 `EXPERIMENTAL / NON-PROMOTABLE / NO_AUTHORITY / DISCARDABLE`。
- **机械检查：PASS。** `git diff --check` 无错误，5 份文件代码围栏成对，README 表格与 Round 编号正确，未发现跨文档 authority 冲突或新的项目 V1/V2、“第一版/后续版本”演进命名。

现授权：

1. 将本回复完整追加到 `ROUND-08-sequenced-wal-client-authority.md`；
2. 将 README 的 Round 8 状态改为 `Reviewed / Integrated`；
3. 只提交并推送这 5 份 Markdown；
4. 继续排除 PDF，不暂存或提交其他文件。

<!-- END VERBATIM INTEGRATION AUDIT 2 -->
