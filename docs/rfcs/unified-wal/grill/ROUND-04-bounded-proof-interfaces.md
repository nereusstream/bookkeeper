# Round 4：Bounded Proof、Assignment Handoff 与 Recovery Outcome

> Reviewer：复用 Round 1–3 的固定 `gpt-5.6-sol / max` agent<br>
> 源码与文档基线：`dbc0f7fee412e68d1cecd40efbf6d80edfdc8af0`<br>
> 评审模式：正确性、热路径性能、复杂度和过度设计联合审查<br>
> 记录规则：以下 reviewer 回复未经删节；仅将 Markdown heading 统一下移一级并清理行尾空白，以保持文档单一 H1。规范是否采纳以对应 RFC 的集成结果为准。

## 本轮 Questions

### Q1：Repair receipt / loss-generation authority

冻结 strong completion assertion、conflicting-range loss ordering 与 bounded receipt/snapshot；明确 digest 只做绑定/审计，不替代 verifier proof，也不引入 per-entry MetadataStore state。

### Q2：Delete assignment / snapshot / storage incarnation

冻结 obligation-changing assignment generation 的 prepare/catch-up/effective handoff、可遍历可应用 snapshot、storage incarnation terminal proof 与 registration cut，同时保持 stream topology OPEN。

### Q3：Relocation checkpoint / orphan GC / group commit

决定 checkpoint 是否可把 move chain 压缩为 current selector，冻结 orphan new location 的 authority proof、late commit vs free ordering 和 durability-through cutover，继续禁止 cross-Arena transaction。

### Q4：Recovery outcome taxonomy / point-read oracle

区分 durable recovered close、deferred、attempt incomplete、quarantine 与 required-coordinate evidence exhausted；冻结 open-ledger normal tail 的 quorum-intersection absence proof，避免把 speculative suffix、temporary no-quorum 或 authority loss误报为 payload DATA_LOSS。

## Reviewer 完整回复

<!-- BEGIN VERBATIM REVIEW -->

## Round 4 独立审查结论

基线已核对：

- `turbo-bk@dbc0f7fee412e68d1cecd40efbf6d80edfdc8af0`
- `HEAD...origin/turbo-bk = 0/0`
- 工作区唯一变更为未跟踪的 `BtrLog Low-Latency Logging.pdf`
- 本轮未修改、暂存、提交或推送任何文件

总判定：

| 问题 | 结论 |
| --- | --- |
| Q1 Repair receipt / loss authority | **REVISE → 收窄 ordering domain、区分 assertion 与 audit commitment 后可 LOCK** |
| Q2 Delete assignment / snapshot / incarnation | **REVISE → 加入 effective-generation handoff 后可 LOCK** |
| Q3 Relocation checkpoint / orphan GC | **REVISE → current selector 足够，但必须修正 local-success 条件后可 LOCK** |
| Q4 Recovery outcome taxonomy | **REVISE → authority loss 与 payload DATA_LOSS 分离后可 LOCK** |

四题方向均成立，但原候选中各有一处容易造成过度设计或错误分类，不能原样锁定。

### Q1 — Repair coverage receipt 与 loss-generation authority

#### 结论：REVISE → 最小强 assertion 可 LOCK

Owner 划分正确：

- RFC-0001/Profile：拥有 immutable `F`、failure-domain policy identity/generation；
- RFC-0004：拥有 recovery verifier、accepted permanent-loss ordering、range completion/reset authority；
- 标准 LedgerMetadata：继续唯一拥有 ensemble membership；
- RFC-0005/Bookie：只提供或消费 local durable evidence，不拥有 distributed reset truth。

需要修正两点：

1. 不应强制所有不相交 range 的 loss/completion 都经过一个 ledger-global hot CAS。必须锁定的是：**会影响同一 coverage range 的 loss declaration 与 completion 有单一线性化顺序**。一个 ledger-instance root CAS 是合法实现，但不是唯一合同。
2. `coverage digest/root` 不是 proof。non-Byzantine verifier 的条件化 durable assertion 才是 authority；digest/root 只是 identity、完整性或审计 commitment。

#### 不可删不变量

一个 `LOSS_BUDGET_RESET_PROVEN` 语义至少必须绑定：

```text
ledger instance
bounded immutable range / coverage cut
canonical required-coordinate definition
RecoveryContext identity or digest
exact published membership CAS mapping/version/digest
RepairIntent identity and generation
Profile descriptor / F / failure-domain policy generation
ledger-instance delete/control fence
overlapping range loss/reset authority predecessor or version
complete/no-hole/F+1-domain verifier assertion
completion operation identity and generation
```

这里“字段必须 LOCK”是指上述语义绑定不可缺失，不要求现在冻结字段名、bytes 或单一 record shape。

还必须锁定：

- verifier 流式检查 range 中每个 required/ACK-eligible coordinate；
- 每个 coordinate 在 evidence cut 上有 matching payload/identity，并覆盖至少 `F+1` distinct valid domains；
- range 中任何 hole、unknown domain identity 或不足 coverage 都禁止 completion；
- receipt 是“受信 verifier 已完成上述检查”的 durable authority assertion；
- checksum、digest、Merkle root 或 count 不能单独证明检查发生；
- loss authority generation 是排序 token，不是“物理损失数量”；
- duplicate declaration 不得因 generation 多增一次就被解释为又损失一个 domain；
- metadata 只能排序 accepted/observed loss facts，不能宣称感知尚未观测的物理世界；
- proof cut 后发生但尚未声明的实际 failure，仍按语义计入新 window；
- overlapping stale completion 不能重复 reset 或清掉后来 loss；
- receipt compact 前，RepairIntent 的 source/target/delete-discovery history 必须已由另一 durable snapshot/summary 接管。

#### 推荐线性化与顺序

```text
1. freeze RepairIntent + bounded RecoveryContext
2. stream and verify every required coordinate
3. make all replacement payload/identity evidence durable
4. reread exact standard membership and Profile policy
5. read current ledger control fence and overlapping range loss/reset head
6. CAS standard ensemble mapping if not already published
7. conditional CAS durable strong completion:
   - same instance/context/range
   - same exact membership mapping
   - same RepairIntent/policy generation
   - same delete/control fence
   - same overlapping loss/reset predecessor
   - verifier asserts complete F+1 coverage
   -- reset linearizes here --
```

对于 accepted loss declaration 与 completion：

- loss 先赢：旧 verification 必须排除已损失 domain 后重新证明 coverage；
- completion 先赢：该 loss 属于新 window；
- 不相交 range 可以并发，但必须能证明不相交；
- 无法证明不相交时保守串行；
- delete fence 仍由 ledger-instance control generation 全局阻止迟到 completion。

不需要为此建立通用事务系统。标准 membership 先 CAS，sidecar completion 后 CAS；中间状态只是“membership 已更新但尚未 reset”，安全且可恢复。

#### Crash / response loss

- verification 中 crash：无 completion、无 reset，重新流式取证；
- membership CAS 前 crash：无 reset；
- membership CAS durable、response loss：重读 exact mapping，不能重新选 target；
- completion CAS 前 loss declaration 赢：旧 completion 条件失败；
- completion durable、response loss：按 operation/generation 重读，视为只 reset 一次；
- receipt snapshot build 后、publish 前 crash：旧 receipts 仍 authoritative；
- compact snapshot publish 后、旧 child 删除前 crash：重复记录允许，选择已提交 snapshot；
- 旧 child 先删而 snapshot 未 durable：safety violation，必须由 Gate 否证。

#### Boundedness、性能与复杂度

必须避免：

- per-entry MetadataStore record；
- per-entry completion CAS；
- mandatory Merkle tree、签名或 PKI；
- ledger root 内积累所有 historical receipt；
- generation delta 被当作 loss 数量；
- small fragment 每条 entry 一个 reset operation。

允许：

- verifier 使用 bounded-memory streaming；
- 同一 RepairIntent/context/policy 下合并相邻 repair ranges；
- 一个 repair operation 只有一个 post-membership completion CAS；
- bounded child pages + bounded root/head；
- current interval snapshot + bounded suffix；
- audit artifact 可选、可丢弃或单独保留，但不能成为安全所必需的远程热路径。

“bounded”至少要锁：

- 单 record/page 有最大 bytes/interval count；
- root fan-out 有上限；
- compact 失败或超过上限时 defer/fail closed，不能继续无界 append；
- snapshot 发布后才能删除其覆盖 receipts；
- 不同 policy/context/control generation 的 intervals 不能假装同一 coverage 直接 merge。

exact page size、fan-out、range coalescing threshold、audit encoding、是否使用 Merkle-like commitment 均保持 OPEN。

#### Spike / Model falsification

Spike C Model A 增加：

- accepted loss event identity/domain/incarnation；
- range loss/reset head；
- verifier assertion 与可选 audit commitment 分离；
- child receipt、snapshot、covered-through generation；
- overlapping/disjoint range；
- compact snapshot publication。

必须覆盖：

- digest 存在但 verifier 未完成，禁止 reset；
- duplicate loss declaration 不重复消费 budget；
- loss 先赢、completion 先赢；
- unobserved failure 在 proof cut 后发生；
- disjoint ranges 并发；
- overlapping stale completion；
- small fragments 合并；
- snapshot publish/response loss/child reclaim 各边界；
- delete 在 receipt compact 前后 freeze targets；
- root/page cap 超限 fail closed。

Spike B 只验证 Bookie local evidence、restart 和 storage incarnation，不能替代 Model A 的 distributed coverage assertion。

---

### Q2 — Delete assignment、snapshot trust 与 storage-incarnation proof

#### 结论：REVISE → effective-generation handoff 后可 LOCK

候选 assignment、snapshot 和 terminal proof 基本正确。关键修订是：

> 不需要在每次 assignment generation 变化时无条件立即 demote；但一个扩大或改变 obligations 的新 generation，不能在 Bookie 尚未 catch up 时成为其 authoritative writable generation。

否则会把安全依赖于 watch 到达时序，或者让 stale writable Bookie 永久漏掉新 stream 的历史 delete。

#### 不可删不变量

Assignment 必须语义绑定：

```text
bookie stable identity
storage/device incarnation
assignment generation
predecessor generation or equivalent CAS version
finite applicable stream set
registration required-through vector
handoff cut / effective status
```

必须保证：

- Bookie 不能自己声明适用 stream set；
- 每 Bookie/incarnation 的 applicable stream 数有 manifest-locked finite maximum；
- stream head 的普通增长不是 assignment generation 变更；
- 每个 committed delete event 在 assignment handoff 前后至少有一个 authoritative route；
- handoff 期间 duplicate delivery 可以接受，gap 不可接受；
- 移除 stream 不能把旧 incarnation 的物理删除义务“转给别的 Bookie”；
- 由新 stream/snapshot 接管时，接管对象仍必须是同一 `(bookieStableId, storageIncarnation)` 的 effects；
- cursor 只在 effect durable 或有可验证 non-applicability 后推进；
- registration CAS 必须校验同一 assignment generation、incarnation 和 cursor vector；
- ordinary ensemble replacement 不能替代 old incarnation 的 wipe/decommission proof。

#### Writable generation 变化

推荐锁定三种行为：

1. **Prepare 但未生效**

   Bookie 可以继续在旧 generation 下 writable；旧 route 继续承担全部 delete delivery。

2. **新 generation 已预先 catch up**

   Bookie durable 安装新 assignment、完成 handoff cut、达到 required-through，然后以 CAS 原子切换 registration；不需要经历可见 demotion。

3. **新 generation 先成为 effective，但 Bookie 未 catch up**

   Bookie 必须变为 non-writable/RECOVERING，或者 cluster authority 必须先撤销其 stale writable registration。不能继续把旧 generation 当有效 authority。

因此答案是：

```text
不是“任何 generation change 都立即 demote”，
而是“stale generation 不能跨越一个已经生效的 obligation-changing cut 继续 authoritative writable”。
```

这避免把 assignment generation 变成 Add-time lease，也避免每个新 delete event 都触发 re-registration。

#### 推荐 handoff 顺序

```text
1. G remains active
2. create G+1 PREPARED with predecessor G and handoff cut
3. retain/dual-route old obligations while G is active
4. build or reference verified snapshot through cut N
5. target Bookie applies snapshot + complete suffix
6. target durably records G+1 and required-through cursors
7. registration/assignment CAS activates G+1
8. only after activation may G obligations/routes be retired
9. reclaim old journal/snapshot only after every supported bootstrap path is covered
```

Delete publication 与 registration 仍需明确 cut：

- event 先于 registration cut：进入 required-through；
- registration 先赢：event 通过 active online route 投递，并保留在 stream 中供未来 rejoin；
- assignment handoff 同时发生时，event 可以双投，但不能落入 old/new 两侧都不负责的空隙。

不要求 MetadataStore 提供跨任意节点事务；可以通过同一 target/stream authority 的 conditional handoff protocol实现。

#### Snapshot 最小语义

Snapshot 不能只是 digest。它必须有可验证、可遍历、可应用的逻辑内容：

```text
stream identity/generation
snapshot generation
covered-through sequence
assignment generation / target incarnation
bounded manifest/chunk completeness
still-required instance-specific delete effects
content/integrity digest
```

每个 retained effect 至少能恢复：

- ledger instance；
- delete epoch/request identity；
- effect/tombstone identity；
- 应用或验证 non-applicability 所需的 authority binding。

允许 snapshot 是 bounded chunks/reference；不要求 root 内直接展开全部 effects。non-Byzantine 模型下：

- checksum/digest 用于完整性和 identity；
- 不强制签名、Merkle proof 或 PKI；
- digest 本身不能替代 chunk completeness 与 effect 可应用性。

如果 snapshot 缺 chunk、suffix 有洞或 root 无法验证，journal prefix 不能 reclaim，Bookie 不能 writable。

#### Storage-incarnation terminal proof

catch-up exemption 只能来自 cluster-accepted terminal fact：

- irreversible wipe proof，绑定 stable Bookie、old incarnation、device/storage scope、operation generation；
- 或 permanent decommission fence，保证 old incarnation 永远不能重新注册 writable。

仅有 local self-report、超时、offline、ensemble replacement 或“新磁盘看起来为空”均不够。

exact admin authorization、attestation 格式、是否使用人工审批或硬件证明保持 OPEN；本轮不要求 PKI。

#### Crash / response loss

- G+1 prepare 后 crash：G 继续 active；
- snapshot build 后、publish 前 crash：旧 journal authoritative；
- G+1 catch up 后、registration CAS 前 crash：重试，不得自行 writable；
- registration CAS durable、response loss：重读 exact generation/incarnation/cursors；
- dual-route 期间 duplicate event：local delete 幂等；
- stream removal durable、handoff snapshot 未 durable：禁止 retire old route；
- wipe/decommission response loss：重读 cluster terminal authority；
- old incarnation 重返：terminal fence 拒绝，不能以新 Bookie identity 绕过。

#### Boundedness、性能与复杂度

锁定：

- vector 大小由 finite stream maximum 限制，不随 ledger 数增长；
- 每 stream 独立 sequence，不要求全局单序列；
- snapshot root bounded，effect 内容分 chunk/page；
- snapshot/journal apply 有 bounded concurrency、bytes 和 wall-clock；
- 超限时保持 RECOVERING/READ_ONLY，不截断 journal；
- assignment/handoff/rejoin 都是冷路径；
- normal Add 不查 MetadataStore、不扫描 cursor vector。

不应锁定：

- global shard、per-Bookie inbox 或 hierarchical topology；
- stream 数值；
- chunk 大小；
- snapshot storage schema；
- exact online-delivery mechanism。

#### Model D falsification

增加：

- G→G+1 prepare/catch-up/activate 每个 crash point；
- delete event 在 handoff cut 前、同时、后；
- stale watch 与 stale registration；
- expanding assignment 未 catch up 却保持 writable 的负向场景；
- safe pre-catch transition不发生不必要 demotion；
- stream removal 但同一 incarnation obligation 未接管；
- dual delivery/duplicate；
- snapshot root valid 但 chunk 缺失；
- snapshot content digest mismatch；
- journal prefix 过早 reclaim；
- stream maximum 超限；
- old incarnation 使用新 stable identity；
- local wipe self-report 未被 cluster 接受；
- decommission response loss；
- assignment generation ABA。

---

### Q3 — Relocation checkpoint、orphan GC 与 group commit

#### 结论：REVISE → current selector 足够；修正 orphan local-success 条件后可 LOCK

Checkpoint 不需要保存完整历史 move chain。保存 **current authoritative selector + 拒绝 stale predecessor/operation 所需的 anti-ABA state** 足够。

候选 orphan 条件中的“无 local-success fact”必须修改。Relocation copy 的 logical payload 很可能本来就有既存 local-success/AQ 事实；否则没有必要搬它。正确条件是：

> 没有任何 local-success/reader/locator authority 依赖这个 uncommitted **new allocation/location**。

不能把“logical entry 曾成功”误当成“orphan new copy 不可清理”。

#### Checkpoint 不可删不变量

在 durable control cut `S` 上，checkpoint 必须完整表示：

- allocation ownership 和 generation；
- reusable/free/retiring 状态；
- 当前 authoritative relocation selector；
- 对仍可能看到 old/new 两份 payload 的 unresolved retirement 状态；
- winning move operation/generation 或等价 anti-ABA token；
- stale predecessor/late retry 无法重新成为 winner 所需的 fencing state；
- checkpoint generation、through-sequence `S`、content identity；
- restart 所需 suffix 起点。

不需要保存：

- 已被 current selector和 durable free完全取代的全部历史 chain；
- 每次 move 的永久审计历史；
- derived RocksDB locator；
- 每个 moved entry 一条固定大小历史记录。

已经完成 old `FREE_AND_BUMP`、不再存在可混淆副本的历史 move，可以从 authority snapshot 中压缩掉。仍有 old reader/source allocation 未退休的 move，必须保留 current selector/retiring state。

Checkpoint 是 control authority 的 committed compact representation，不是 derived locator，因为：

- 它由 control state 在明确 cut `S` 构造；
- 有 checksum/content identity；
- 经 `CHECKPOINT_COMMIT` 与 superblock durability 发布；
- 后续只按 `suffix > S` 演进；
- 不能通过扫描最大物理 generation 猜出来。

#### Checkpoint 顺序

```text
1. choose complete applied control cut S
2. freeze/COW snapshot of allocator + current selectors at S
3. write bounded checkpoint chunks
4. durably verify checkpoint content
5. append/durable CHECKPOINT_COMMIT(checkpoint generation, S, identity)
6. durable switch superblock
7. verify fallback checkpoint/suffix dependencies
8. reclaim only prefix covered by a surviving committed authority
```

A/B checkpoint 中，只要旧 checkpoint 仍是 corruption fallback，它依赖的 suffix 就不能先删。不能仅因“最新 generation number 更大”回收旧 authority。

Restart：

```text
latest valid committed checkpoint through S
+ complete valid control suffix with sequence > S
```

sequence gap、torn record 或必要 suffix 缺失时 fail closed。

#### Orphan GC 最小证明

一个 uncommitted relocation copy 只有在以下全部成立时才能回收：

- allocation owner 绑定该 move operation/generation；
- payload identity/location/generation 与 candidate 匹配；
- current selector 从未选择它，或已由另一个 winning successor明确排除；
- 完整 authority state 中不存在指向它的 winning `MOVE_COMMIT`、checkpoint selector 或 later authoritative reference；
- 该 location 未被发布为新的 local-success location；
- logical entry 的既存 local-success 仍由 old/current authoritative location承接；
- move writer 已被 operation/control generation fencing；
- 无可能迟到并成功的 commit；
- 无 live reader、writer、inflight locator/pin；
- shared allocation 中其他 occupants 也都 dead/moved；
- conditional `FREE_AND_BUMP` durable 后才 reuse。

Timeout 单独永远不够。

不要求每次 runtime GC 从磁盘完整 replay；它可以消费由完整 replay/checkpoint 构建的当前 authority state。但离线 checker 必须能从 checkpoint+suffix 独立复现结论。

#### GC 与 late commit 线性化

`MOVE_COMMIT` 和 conditional `FREE_AND_BUMP` 必须在同一 ArenaControlLog apply order 中竞争：

- commit 先赢：orphan-free 条件失败，转入正常 cutover/drain；
- free 先赢：allocation generation 已 bump，迟到 commit 的 expected generation/predecessor 失败；
- 两者不能都成功。

这比单纯“先检查、稍后 append free”更强。检查与条件化 free 必须在同一 control-generation authority 中闭合。

#### Group commit cutover

每个 `MOVE_COMMIT` 至少绑定：

```text
controlSequence
operation identity/generation
durability barrier or durable-through cut
expected predecessor
new selector
```

锁定：

- 只有 durability completion 明确覆盖该 `controlSequence` 后才能发布 locator cutover；
- batch submission、内存 append 或看到别的 record 完成均不足；
- locator 切换与阻断新 old pins 必须形成一个本地同步 cut；
- 已在 cut 前取得 old pin 的 reader 可以完成；
- old free 晚于所有这些 reader drain；
- fsync 成功但 response loss 时按 operation identity/replay解析；
- torn group 只接受完整、连续、校验通过的 prefix；
- `durableThrough` 不能跨 sequence gap。

#### 性能与复杂度

Checkpoint/selector state 应与 live allocations、未退休 moves 或 compacted ranges 成比例，而不是与历史 move 次数成比例。

必须测量并限制：

- checkpoint bytes/pages 和 build latency；
- control-log suffix retention；
- relocation control bytes；
- group barrier latency；
- concurrent checkpoint/compaction 对 Add p99 的干扰；
- background queue、bandwidth 和 CPU；
- shared-block partial relocation 写放大。

不应：

- 给每个 moved entry 单独 fsync；
- 让 checkpoint 在 Add shard 上长时间 stop-the-world；
- 因后台 move 强制 foreground Add 等待额外 barrier；
- 永久保存完整 move chain；
- 引入跨 Arena transaction。

cross-Arena/device relocation 继续明确 unsupported / OPEN。

#### Spike B / Model C falsification

增加：

- checkpoint current selector 与 full-chain oracle 等价；
- move commit 位于 `S-1/S/S+1`；
- chain compact 后删除全部 derived locator；
- current selector 已保存但 old source 尚未 free；
- old free 已完成后丢弃 historic chain；
- stale old operation 在 checkpoint 后重试；
- orphan copy 对应一个已有 local-success logical entry；
- orphan allocation本身从未成为 success location；
- GC 与 late commit 同 batch/不同 batch 竞争；
- free 先赢与 commit 先赢；
- torn group、durable-through response loss；
- checkpoint A/B fallback 仍依赖旧 suffix；
- shared allocation 一条 orphan、另一条 live；
- quiescence/pin/writer generation race；
- checkpoint/GC/compaction 压力下 foreground p99。

---

### Q4 — Recovery outcome taxonomy 与 point-read oracle

#### 结论：REVISE → 五类语义可 LOCK，但 authority loss 不能叫 DATA_LOSS

现行源码基线支持本轮区分：

- `LedgerRecoveryOp` 从 quorum-covered LAC 或 current fragment start 前一项开始向前读取；
- recovery read 只有在 exact write set 上获得 `W-A+1` 个确定缺失时才把 coordinate 判为不存在；
- first missing 后停止恢复，later payload 可以作为 speculative tail 出现；
- CLOSED LedgerMetadata 必须保存 `lastEntryId`，非 CLOSED metadata 没有该字段；
- `ReadLastConfirmedOp` 把 quorum coverage 与合法 LAC evidence结合，单个 TailSummary 不具备同等 authority。

因此 normal open-ledger tail 与 required-entry loss 必须分开。

#### 最小 taxonomy

建议锁语义，不锁 enum 名：

1. **RECOVERED_AND_CLOSED(P)**
   同一 RecoveryContext 下，所有 `<=P` required entries 已验证/恢复，正常 tail end 已证明，并且 close/metadata publication durable。`P` 只是中间计算结果时不能返回成功。

2. **RETRYABLE / DEFERRED**
   authority/evidence source 暂不可用、no quorum、global operational bound 耗尽或 capability fallback 暂时无法完成。没有 terminal data-loss assertion。

3. **ATTEMPT_INCOMPLETE**
   caller cancellation/deadline。只终止本次 attempt，不改变 ledger terminal state。

4. **QUARANTINED**
   valid-looking payload/identity/authority 有不可调和冲突，或者必要 authority 已损坏到无法判定 required set。后者应使用 `AUTHORITY_UNRECOVERABLE` reason，而不是伪称 payload DATA_LOSS。

5. **DATA_LOSS / EVIDENCE_EXHAUSTED**
   仅针对一个 contract-required/known-committed coordinate：全部合法、有限 evidence source 已被确定性检查，且都已永久丢失、确定缺失或失效。

`NORMAL_TAIL(P)`、`RECOVERED_AFTER_COPY(P)` 可以是 success reason/metric，不需要增加顶层 taxonomy。

#### “Known committed / required” 最小判据

一个 coordinate 只有由 accepted authority 明确要求时才是 required，例如：

- CLOSED metadata 的 `[0, lastEntryId]`；
- fenced RecoveryContext 下 quorum/coverage-proven LAC 及此前连续 prefix；
- ensemble change 前已冻结的 historical fragment；
- accepted durable AQ/ACK/recovery completion authority；
- RFC-0002 将来接受的 ordered committed frontier；
- 其他明确进入 Profile contract 的 durable publication。

以下事实单独不够：

- client 内存中的 pending Add；
- `lastAddPushed`；
- 单 replica payload；
- payload 恰好存在于若干节点但无 accepted commitment authority；
- TailSummary 最大值；
- speculative range result；
- later entry 存在；
- timeout/offline。

#### Normal tail proof

对于 open-ledger recovery，首个缺失 coordinate `x` 只有在以下条件成立时才能定义正常尾部 `P=x-1`：

- ledger 已在同一 generation 下 fenced；
- 所有 `<x` required coordinate 已无洞恢复；
- `x` 位于 quorum-proven committed frontier之后；
- 对 `x` 的 exact write set 得到足以与每个可能 Ack quorum 相交的 definitive absence coverage；
- Round-robin `E/W/A` 基线下即至少 `W-A+1` 个 distinct write-set members 返回合法、同 context 的确定缺失；
- timeout、connection failure、offline 和 corrupt response 不算 definitive absence；
- 没有 accepted authority 证明 `x` 或更晚 coordinate 属于 required committed prefix。

Later evidence 的处理：

- later payload 只是 speculative、无 required authority：可以作为 suppressed suffix 丢弃，不能把 `x` 叫 DATA_LOSS；
- later coordinate 有 accepted committed frontier，导致 `x` 必须存在：这是 required hole；根据证据状态进入 deferred、quarantine 或 DATA_LOSS；
- later payload identity 自相矛盾：quarantine；
- 较早 hole 未解析时，无论 later evidence 多完整都不能发布跨 hole prefix。

#### Evidence exhausted

`DATA_LOSS` 必须证明已枚举 frozen context 下的有限合法来源：

- exact historical ensemble/write set；
- replacement/RepairIntent targets；
- committed-readable replicas；
- accepted local durable authorities；
- 其他 Profile 明确声明的 evidence source。

结果必须是 definitive missing、permanently lost 或 invalid evidence。暂时 offline/no-quorum/global deadline 均不是 exhausted。

必要 metadata/control authority 无法恢复时，系统无法知道 required set，应进入 terminal quarantine/non-promotable authority failure；这与“已证明某 ACKed payload 丢失”不同，指标和运维动作不能混为一类。

#### 顺序与线性化

```text
1. freeze/fence immutable RecoveryContext
2. derive required committed frontier from accepted authority
3. try bounded range/TailSummary fast path
4. on unsupported/stale/partial/local-budget exhaustion:
   fallback at earliest unresolved coordinate
5. use point-read oracle and exact write-set coverage
6. classify first unresolved coordinate:
   valid payload / definitive normal absence / transient / conflict / exhausted required loss
7. recovery-add every salvaged coordinate through P
8. durable publish close/final prefix under same generation
9. only after publication return RECOVERED_AND_CLOSED(P)
```

fast-path local budget 永远只触发 fallback。global operational bound 只能产生 deferred；caller deadline/cancel 只能产生 incomplete。

#### Crash / response loss

- fast continuation crash：重读；volatile cache 不能被当作 absence；
- recovery-add response loss：按 entry identity幂等重读；
- normal tail 已证明但 close 未 durable：不能返回 recovered；
- close durable、response loss：重读 exact metadata/prefix，返回相同 success；
- quarantine/data-loss declaration response loss：若要形成 cluster terminal state，必须按 context/generation幂等重读；
- authority generation 变化：旧 outcome 失效，重读或重新开始；
- stale optional operation checkpoint：忽略并从 authoritative context重读。

#### API 与 metrics

API 必须至少保留以下语义信息，但 exact class/enum/wire code OPEN：

```text
outcome class
retryable vs terminal
RecoveryContext / authority generation
final prefix when success
first unresolved/required coordinate when relevant
reason category
whether durable terminal publication exists
```

Metrics 分开记录：

- recovered at normal tail；
- recovered after copying entries；
- fallback reason：unsupported/stale/partial/local budget；
- deferred：offline/no quorum/global bound；
- incomplete：cancel/deadline；
- quarantined conflict；
- quarantined authority loss；
- terminal required-coordinate evidence exhausted；
- suppressed speculative suffix；
- point-read bytes/count/latency。

不能把 timeout、cancel、single corruption 或 authority loss计入 `data_loss_total`。ledger ID 不应成为高基数 metric label。

#### Model E falsification

增加状态：

- required/committed frontier；
- closed lastEntry boundary；
- fenced generation；
- exact write set；
- definitive absence、transient、corrupt、conflicting evidence；
- later speculative evidence；
- published close/prefix；
- authority-unrecoverable reason。

场景：

- closed required entry missing；
- open LAC 后第一个正常 missing；
- `W-A+1` definitive absences；
- `W-A` absence + one offline，必须 deferred；
- single corrupt replica + valid survivor；
- later speculative entry after hole；
- later required entry after hole；
- TailSummary 超前/落后；
- fast budget exhaustion；
- global bound exhaustion；
- cancellation；
- authority corruption但 payload仍在；
- all sources definitive exhausted；
- close durable前后 response loss；
- stale operation checkpoint；
- generation change。

核心性质：

```text
NormalTailRequiresAckQuorumIntersectionAbsence
RequiredHoleNeverPublishesPrefixAcrossIt
TransientUnavailableIsNotEvidenceExhausted
SingleCorruptReplicaIsNotTerminal
AuthorityLossIsNotPayloadDataLoss
RecoveredOutcomeImpliesDurableClose
FastPlusFallbackEqualsFullPointOracle
```

---

## 1. 本轮可立即 LOCK 的精确清单

1. RFC-0001/Profile 只拥有 immutable `F` 与 failure-domain policy。
2. RFC-0004 verifier 流式验证 bounded range 的完整、无洞、per-coordinate `F+1` domain coverage。
3. MetadataStore 保存强 completion assertion，不保存 per-entry proof。
4. Digest/root 是 commitment 或完整性数据，不是 proof。
5. Receipt 绑定 instance、range/cut、RecoveryContext、membership mapping、RepairIntent、policy、control/loss predecessor、assertion 与 completion generation。
6. Conflicting/overlapping loss 与 completion 必须线性排序；不相交 range 不必强制全 ledger 串行。
7. Loss generation 是 ordering token，不是物理 failure counter。
8. Receipt root/page bounded；snapshot 发布后才能 reclaim children。
9. Delete assignment 由 cluster authority 定义 finite stream set 与 required-through。
10. Obligation-changing generation 必须 pre-catch+atomic activate，或先 demote；stale generation 不能跨 effective cut writable。
11. Stream head 普通增长不触发 assignment generation/re-registration。
12. Delete handoff允许 duplicate，不允许 gap。
13. Snapshot 必须包含可遍历、可应用的 effects；digest-only 不足。
14. Bootstrap 只能是 verified snapshot + complete suffix。
15. Wipe/decommission proof 必须 cluster accepted、绑定 old incarnation且不可逆。
16. Checkpoint through `S` 是 allocation/current-selector authority 的 compact representation。
17. Current selector + anti-ABA/fencing state足够，不要求完整历史 chain。
18. Prefix reclaim 晚于 checkpoint commit、superblock durability 与 fallback dependency解除。
19. Orphan GC证明针对 new allocation/location，不要求 logical entry 没有既存 local success。
20. GC 与 late `MOVE_COMMIT` 由同一 conditional ArenaControl ordering裁决。
21. `MOVE_COMMIT` cutover只在 durability completion覆盖其 control sequence 后发生。
22. Cross-Arena relocation仍 unsupported。
23. Recovery outcome 保留 success、deferred、incomplete、quarantine、payload data loss 五类语义。
24. 必要 authority unrecoverable属于 quarantine/authority failure，不是 payload DATA_LOSS。
25. Open-ledger normal tail需要 exact write set 上与所有 Ack quorum相交的 definitive absence coverage。
26. Required coordinate由 accepted durable authority定义；later speculative payload单独不构成 required。
27. Recovered success晚于 recovery-add完成和 durable close/publication。
28. fast local budget只触发 fallback；global bound只 deferred；caller cancel只 incomplete。

## 2. 必须保持 OPEN 的内容

- receipt exact schema、field names、canonical bytes；
- coverage audit artifact、checksum/Merkle-like encoding；
- interval page size、fan-out、merge threshold；
- ledger-global root或range-sharded child topology；
- loss-event physical namespace和索引；
- delete global shards、per-Bookie inbox或hierarchical topology；
- assignment store、stream count、chunk size；
- snapshot chunk/manifest encoding；
- wipe/decommission authorization和attestation机制；
- checkpoint bytes、page layout、A/B exact superblock protocol；
- selector packing、operation dedup retention horizon；
- `MOVE_PREPARE`、orphan candidate index；
- group-commit batch/bytes/time thresholds；
- recovery outcome exact enum、exception与wire mapping；
- fast/global budget数值；
- optional operation checkpoint encoding；
- general E/W/A merge的最终伪代码和复杂度；
- API backward compatibility策略；
- metrics阈值与production Gate。

## 3. 需同步的文档/章节

- `RFC-0001`
  - §5 ProfileDescriptor：保留 F/policy owner；
  - §13 invariants：loss generation不能重标/重复计数；
  - §15 OPEN：exact domain、policy evolution继续开放。

- `RFC-0003`
  - §5.2：group durability cut；
  - §10：conditional orphan free；
  - §11：checkpoint through-S、current selector、prefix reclaim；
  - §12：restart suffix；
  - §17–19：invariants、Gate、OPEN。

- `RFC-0004`
  - §7/§7.1：required frontier、normal tail、outcome taxonomy；
  - §9.1 owner 列表：显式加入 range completion/loss-reset authority；
  - §9.2：strong assertion、loss ordering、bounded receipt snapshot；
  - §12：assignment PREPARED/effective handoff与snapshot内容；
  - §14.5/§14.6：registration、delete publication、loss/completion ordering；
  - §16–19：invariants、Model scenarios、Gate、OPEN。

- `RFC-0005`
  - §2 owner：RFC-0004拥有 range reset和delete assignment；
  - §9 restart：消费 active assignment/current selector；
  - §10 performance：禁止 Add-time assignment lease或receipt lookup；
  - §11–13：stale generation、orphan location与outcome边界。

- `Spike B`
  - Authority Oracle；
  - B7 checkpoint；
  - B10 relocation/orphan；
  - failure matrix、write amplification、p99 Gate。

- `Spike C`
  - Model A：receipt/loss ordering/compaction；
  - Model C：checkpoint selector、orphan、durable-through；
  - Model D：assignment handoff、snapshot chunks、incarnation proof；
  - Model E：required frontier、normal tail和taxonomy；
  - core invariants、config matrix、hard Gate。

- `docs/turbo-bk.md`
  - authority owner；
  - cross-RFC invariants；
  - Model A/C/D/E scope；
  - OPEN 列表；
  - Stage 状态保持不提升。

- grill
  - 新增 Round 4 完整归档；
  - Integration Audit 完成前 README 保持 `Review Complete / Integration In Progress`。

## 4. 新 P0 矛盾与 owner

没有需要新增 RFC 的 P0 主题，但有三处必须修正的潜在矛盾：

1. “orphan 无 local-success fact”与 relocation 搬运既有成功 payload 冲突。必须改成“new location 不承载 local-success authority”。
2. “必要 authority 不可恢复 = DATA_LOSS”混淆了无法判定与已证明 payload 丢失。必须分为 quarantine authority loss。
3. “所有 loss/completion 共享一个 ledger-global CAS”虽然安全，但会把不相交小 fragment repair不必要地串行化。规范只锁 conflicting-range ordering。

Owner 补充：

- RFC-0004 明确拥有 strong completion assertion、accepted loss ordering、outcome taxonomy；
- RFC-0003 拥有 checkpoint selector、orphan GC、group durability；
- RFC-0004 拥有 delete assignment/snapshot/incarnation terminal proof；
- RFC-0005只消费本地状态；
- no new global transaction owner。

## 5. 下一轮 frontier 与 Implementation Ready 距离

下一轮最值得 grill 的 frontier 是“实现接口与有限状态落盘”，不是再讨论总体架构：

1. sidecar child/snapshot 的 canonical predecessor、versioning、unknown-record 与 retention接口；
2. ArenaControlLog checkpoint/selector/free 的 conditional record接口和 reader-cutover API；
3. Bookie local normal/recovery/fence/delete/assignment state 的最小原子落盘组合；
4. recovery semantic outcome 到现有 BookKeeper API/exception/admin状态的兼容映射；
5. wipe/decommission 的授权、撤销禁止与registration fence；
6. general E/W/A point-oracle merge伪代码及复杂度。

即使 Round 4 全部正确集成，整体仍不能标记 Implementation Ready，原因包括：

- RFC-0001–0005 仍为 Proposed；
- Spike A/B/C 均为 `Planned / Not Executed`；
- descriptor/hash/auth、wire opcode/error、local state packing仍未冻结；
- checkpoint/superblock/record framing尚未形成实现接口；
- general E/W/A merge算法尚未接受；
- delete topology、retention和decommission流程尚未实现；
- crash matrix、TLA+、资源/p99/write-amplification Gate均未执行；
- 尚无 canary-specific evidence。

当前可以继续做 Spike/prototype 和实现接口设计；还不能开始会产生 Segment ACK authority 或声称 production-ready 的实现。

<!-- END VERBATIM REVIEW -->

## Integration Audit 1（FAIL）

以下为同一 reviewer 对 Round 4 文档集成结果的第一次只读审计。审计正文完整保留；仅将 Markdown heading 统一下移一级并清理行尾空白。

<!-- BEGIN VERBATIM INTEGRATION AUDIT 1 -->

## Round 4 Integration Audit 1

**结论：FAIL，但范围很窄。**

四个设计主题的主体集成正确，没有重新引入全局 hot CAS、digest-as-proof、Add-time lease、完整 move-chain 保留、per-entry MetadataStore/fsync、跨 Arena transaction 或过早 wire/API 冻结。当前只需修复 4 个合同表述点和 1 个机械空白问题，不需要重开 Round 4 设计。

### 1. 基线与变更范围

已核对：

- HEAD：`dbc0f7fee412e68d1cecd40efbf6d80edfdc8af0`
- `HEAD...origin/turbo-bk`：`0 / 0`
- 变更范围：8 个 tracked 文档和新增 Round 4 归档，共 9 个本轮文件
- 无关 `BtrLog Low-Latency Logging.pdf` 仍未跟踪，未被修改或纳入范围
- 未修改、暂存、提交或推送任何文件

README 当前保持：

```text
Review Complete / Integration In Progress
```

在本次 FAIL 修复及 Audit 2 之前，这是正确状态。

## 2. 必须修复项

### 必修 1：strong completion 缺少对 Profile/F 的直接、可长期解析绑定

文件：

```text
docs/rfcs/unified-wal/RFC-0004-range-recovery-delete.md
§9.2 Range-scoped permanent-loss budget reset
```

当前 proof context 记录了：

```text
Profile failure-domain policy identity/generation
RepairIntent identity/generation
```

但 Round 4 锁定的是：

```text
Profile descriptor hash/generation
permanentLossBudgetF
failure-domain policy identity/generation
```

当前可以通过 `RepairIntent -> profileDescriptorHashOrGeneration` 间接推导部分内容，但 RFC 又允许 RepairIntent/receipt 分别 compact。若被引用记录压缩后不保证该绑定仍能解析，旧 strong assertion 可能被未来 reader 在错误 `F` 或 descriptor 下重解释。

最小修复二选一：

1. strong completion assertion 直接语义绑定 descriptor hash/generation、`F` 与 failure-domain policy generation；或
2. 明确 policy identity canonically 包含 `F`，且 receipt 引用的 immutable descriptor/RepairIntent summary 在任何 compaction 后仍保留、可解析并禁止重解释。

不要求重复存储物理字段；要求的是 durable semantic binding。exact bytes/schema 仍应 OPEN。

建议同步检查：

- RFC-0004 §9.2 字段清单；
- `docs/turbo-bk.md` invariant 20；
- Spike C Model A 的 verifier assertion state，确保模型不会在相同 policy token 下改变 `F`。

### 必修 2：wipe/decommission terminal proof 缺少防重放 scope

文件：

```text
docs/rfcs/unified-wal/RFC-0004-range-recovery-delete.md
§12 Delete delivery stream 与 rejoin barrier
```

当前已正确要求：

- cluster-accepted irreversible wipe proof；
- permanent decommission fence；
- local self-report、timeout、offline、ensemble replacement 均不足。

但没有完整锁定 terminal proof 的语义身份。Round 4 已要求至少绑定：

```text
bookieStableIdentity
old storage/device incarnation
device/storage scope
wipe/decommission operation identity/generation
cluster acceptance authority/version
irreversible result or permanent registration fence
```

缺少这些绑定时，一张针对旧目录、另一个 device、旧 operation 或部分 storage scope 的 proof 可能被错误复用于整个 Bookie incarnation。

最小修复：

- 在 §12 明确上述 semantic binding；
- permanent decommission fence 必须保证该 old incarnation 永久不能重新注册 writable；
- wipe proof 只能免除其明确覆盖 storage scope 的 catch-up；
- exact authorization、attestation、人工审批或硬件证明继续 OPEN。

Spike C Model D 应至少拥有 proof scope/operation generation，增加“旧 proof 跨 incarnation/device 重放”的负向检查。

### 必修 3：RFC-0003 Derived index 仍保留旧的完整-chain表述

文件：

```text
docs/rfcs/unified-wal/RFC-0003-segment-storage-allocator.md
§15 Derived index
```

当前仍写道：

```text
relocation winner 只能由 durable MOVE_COMMIT chain 决定
```

这与本轮已锁定的以下合同直接冲突：

```text
checkpoint current selector through S
+ complete suffix > S
= full-chain oracle
```

以及：

```text
历史 move chain 可以在 current selector + anti-ABA/retiring state
完整接管后压缩
```

如果历史 control prefix 已合法 reclaim，restart 不可能再依赖完整 `MOVE_COMMIT chain`。

最小修复建议：

```text
relocation winner 只能由 committed checkpoint current selector
+ complete conditional MOVE_COMMIT suffix 决定；
checkpoint selector 必须可证明由此前完整 control history产生，
不能由 RocksDB、mtime、最大物理 generation 或 data scan 猜测。
```

Spike B/C 继续使用 full-chain replay 作为独立 oracle是正确的；oracle 等价不等于生产 authority 必须永久保留 full chain。

### 必修 4：checkpoint 的 “reader state” 必须收窄为 bounded retirement state

文件：

```text
docs/rfcs/unified-wal/RFC-0003-segment-storage-allocator.md
§11 Allocator checkpoint 与 rotation
```

当前写：

```text
未退休 old source/reader state
```

这可能被实现者理解为 checkpoint 要持久化 individual reader/pin history，既不必要，也会制造 per-reader/per-record state 膨胀。

应明确锁定为：

```text
old-source retirement state
new-old-pin gate state
whole-allocation reclaimability
```

而不是 durable individual reader enumeration。

建议同时说明：

- process crash 后旧进程的 volatile reader pins 不作为 durable history继承；
- restart 必须恢复 selector/retiring gate，随后才能重新判断 free；
- runtime reader/pin tracking 可以是 bounded volatile state；
- checkpoint 不需要保存每个 reader、future 或 buffer引用。

这不会改变 safety，只是防止由模糊措辞引入过度设计和 checkpoint 膨胀。

### 必修 5：Round 4 新文件多一个 EOF 空白行

文件：

```text
docs/rfcs/unified-wal/grill/ROUND-04-bounded-proof-interfaces.md
```

只读校验结果：

```text
new blank line at EOF
```

文件以两个换行结束。删除 `END VERBATIM REVIEW` 后的额外空白行即可；不能改 reviewer 内容。

## 3. Q1 审计：除必修 1 外通过

已正确集成：

- strong verifier assertion 是 reset authority；
- digest/root/count 仅为 identity、完整性或 audit commitment；
- 不强制 Merkle tree、签名或 PKI；
- 每个 required coordinate 都流式验证 matching payload 与 `F+1` distinct domains；
- membership、target durability、activation、generic COMMITTED 单独不能 reset；
- accepted loss generation 只是 ordering token；
- duplicate domain/incarnation declaration 不重复消费 budget；
- conflicting/overlapping range loss 与 completion 单序；
- proven-disjoint ranges 可以并发；
- 没有要求所有 repair 使用 ledger-global hot CAS；
- delete fence 仍能全局阻止迟到 completion；
- proof cut 后未观测物理 failure 进入新 window；
- bounded child pages、bounded root/head、snapshot+suffix、publish-before-reclaim均已锁定；
- compaction 失败或超限时 defer/fail closed；
- source/target/delete history 必须先被另一 durable summary接管；
- 没有 per-entry MetadataStore、per-entry completion CAS 或 per-entry control fsync。

性能上仍是 repair operation/range 冷路径，一个 post-membership completion CAS，可合并相邻小范围，没有 steady-state Add 损耗。

## 4. Q2 审计：除必修 2 外通过

已正确集成：

- assignment 绑定 predecessor、generation、incarnation、finite stream set、required-through、handoff cut 和 PREPARED/effective；
- stream head 普通增长不被误写为 assignment generation变化；
- G active 时 G+1 PREPARED 不无条件 demote；
- G+1 pre-catch 后可以无可见 demotion原子切换；
- obligation-changing G+1 effective 后 stale G 不能继续 authoritative writable；
- assignment 没有成为 Add-time lease；
- handoff可以 duplicate，不能 gap；
- snapshot 不只是 digest；
- snapshot 必须可遍历、可应用，并携带 still-required instance-specific effects；
- bounded chunk/manifest completeness 已锁定；
- snapshot+complete suffix 是唯一合法 bootstrap；
- 缺 chunk、suffix gap、内容不完整时 non-writable；
- stream removal 不能把同一 old incarnation 的物理删除 obligation 转嫁给另一个 Bookie；
- local self-report、offline、timeout、新盘为空、ordinary ensemble replacement 均不能免除 catch-up；
- vector 受 finite maximum 限制；
- 每 stream 独立 sequence，没有全局序列瓶颈；
- topology、count、chunk size、schema、online delivery机制仍 OPEN。

性能路径正确：assignment/handoff/snapshot/rejoin均为冷路径，normal Add 不读取远程 assignment 或 cursor，也不扫描 vector。

## 5. Q3 审计：除必修 3/4 外通过

已正确集成：

- checkpoint 在完整 applied cut `S` 构建；
- bounded checkpoint chunks、content identity、`CHECKPOINT_COMMIT`、superblock顺序正确；
- prefix reclaim晚于 surviving checkpoint/fallback suffix依赖解除；
- restart 使用 `checkpoint through S + complete suffix >S`；
- sequence gap、torn tail、必要 suffix缺失均 fail closed；
- current selector + anti-ABA/retiring state可以压缩历史 chain；
- old source仍未退休时保留必要 selector/retirement authority；
- orphan判定针对 uncommitted new allocation/location；
- logical entry 可以保留既存 local success；
- new location 从未成为 lookup/local-success authority；
- complete authority state无 winning selector/reference；
- late writer被 generation fence；
- timeout单独不是 proof；
- conditional orphan free 与 late `MOVE_COMMIT` 在同一 Arena order竞争；
- commit先赢则free失败，free先赢则late commit predecessor/generation失败；
- own `controlSequence` 被 durability completion覆盖后才cutover；
- `durableThrough`不能跨gap/torn record；
- locator切换和阻断新old pin形成本地同步cut；
- existing old reader先drain，随后才free；
- relocation不产生新的 local success；
- 不要求每 moved entry独立fsync；
- 不允许 foreground Add等待额外 relocation barrier；
- cross-Arena仍 unsupported；
- Spike B有current-selector/full-chain独立oracle、late commit/free、shared orphan/live、p99和写放大计量。

除了 §15 旧表述与 “reader state” 模糊点，没有发现 full-chain retention、foreground fsync 或分布式事务回归。

## 6. Q4 审计：通过

Required frontier 已正确限定为 accepted durable authority：

- CLOSED `[0,lastEntryId]`；
- fenced context下 quorum/coverage-proven LAC及连续prefix；
- frozen historical fragment；
- accepted ACK/AQ/repair completion；
- 未来接受的 ordered committed frontier。

以下均没有被误提升为 required：

- pending Add；
- `lastAddPushed`；
- 单副本 payload；
- TailSummary maximum；
- speculative range result；
- later payload本身。

Normal tail 正确要求：

- 同一 generation fenced；
- earlier required prefix无洞；
- `x` 位于 committed frontier之后；
- exact write set 上与每个 Ack quorum相交的 definitive absence；
- Round-robin baseline 至少 `W-A+1`；
- timeout、offline、connection failure、corrupt response均不算 absence；
- later speculative suffix可以 suppress；
- later required authority出现时不得跨 hole发布。

五类语义完整：

- recovered and durably closed；
- retryable/deferred；
- attempt incomplete；
- quarantine，包括 authority unrecoverable；
- required-coordinate evidence exhausted后的 payload DATA_LOSS。

正确区分：

- authority loss不是payload data loss；
- single corrupt replica不是terminal；
- global bound只能deferred；
- caller deadline/cancel只能incomplete；
- success晚于recovery-add与durable close/final-prefix publication；
- close response loss通过重读解析。

没有过早冻结 exact enum、exception、wire code 或 backward-compatibility方案；这些明确保留在 OPEN。API只锁语义信息，不锁物理类型。

Model E 已覆盖 closed required missing、open normal tail、`W-A+1` 对照、offline、later speculative/required、authority corruption、durable close和point-oracle equivalence。

## 7. Owner、阶段与 OPEN 边界

Owner 基本一致：

- RFC-0001：immutable F/policy；
- RFC-0003：local allocation/relocation/checkpoint/orphan；
- RFC-0004：strong assertion、accepted loss ordering、delete assignment/snapshot、recovery outcome；
- RFC-0005：消费上述 authority并决定local success；
- Standard LedgerMetadata：OSS membership/state；
- no new global transaction owner。

状态没有误提升：

- RFC-0001/0003/0004/0005仍为 Proposed；
- Spike B/C仍为 `Planned / Not Executed`；
- `BK_SEGMENT_WAL`仍为 P0 Blocked；
- GENERAL E/W/A recovery仍为 Research/Spike；
- Round 4仍为 Integration In Progress；
- 没有从文档集成推导 Accepted、canary-ready或Implementation Ready。

OPEN 保留正确：

- receipt exact schema/bytes/topology/page/fan-out；
- audit commitment/Merkle-like encoding；
- assignment topology/count/store/handoff encoding；
- snapshot chunk/manifest schema；
- wipe/decommission authorization/attestation；
- checkpoint bytes/page/superblock protocol；
- selector packing/dedup horizon；
- orphan index/`MOVE_PREPARE`；
- group-commit thresholds；
- cross-Arena protocol；
- outcome exact enum/wire/API mapping；
- fast/global budgets；
- general E/W/A merge algorithm；
- metrics和production thresholds。

## 8. 归档与机械验证

Round 4 归档：

- 879 行、39,802 bytes；
- BEGIN/END marker各一个；
- Q1–Q4、LOCK、OPEN、同步位置、owner、frontier、Implementation Ready距离均存在；
- README链接与基线SHA正确。

我从当前 session JSONL提取原 reviewer `AgentMessage`，对原文执行归档声明中的唯一规范化：

- 所有 headings下移一级；
- 清理行尾空白。

比较结果：

```text
original_chars=29593
normalized_chars=29625
archived_chars=29625
PASS verbatim-content heading-shift/trailing-space-only
```

因此 reviewer 反馈内容没有遗漏、删节或语义改写。FAIL只包括文件末尾多出的空白行，不涉及归档正文。

其他验证：

- tracked `git diff --check`：PASS；
- 14 个相关 Markdown 单一 H1：PASS；
- fenced code blocks：PASS；
- repo-root/relative links：PASS；
- Round 4 no-index whitespace：仅 EOF 多一个空白行；
- branch ancestry：`0/0`。

## 9. 正确性、性能、复杂度与过度设计总评

正确性：主体通过；必须补齐 F/descriptor durable binding、terminal proof scope，并消除 checkpoint selector与旧 full-chain表述的冲突。

性能：通过。没有 steady-state Add远程I/O、Add-time lease、per-entry MetadataStore、per-entry control fsync或foreground relocation barrier。

复杂度：通过。没有新增通用事务、全局序列、mandatory Merkle/PKI、永久 full move history或每次 assignment变化无条件demotion。

过度设计：主体通过。“reader state”措辞必须收窄，避免未来实现成durable per-reader/pin history。

## 10. 下一步与 frontier

当前不能把 Round 4 标为 `Reviewed / Integrated`，也不能 commit/push。建议：

1. 只做上述5项最小修复；
2. 不改变其余已通过合同；
3. 保持 README `Integration In Progress`；
4. 重新执行 whitespace、链接、单H1、section和OPEN扫描；
5. 把修复后的 diff交回同一 reviewer做 Integration Audit 2。

Audit 2 通过后，下一轮最值得 grill 的 implementation-interface frontier仍是：

1. sidecar child/snapshot predecessor、version、unknown-record与retention接口；
2. ArenaControlLog conditional record、checkpoint selector/free和reader-cutover API；
3. Bookie local normal/recovery/fence/delete/assignment状态的最小原子落盘组合；
4. recovery outcomes到现有BookKeeper API/exception/admin状态的兼容映射；
5. wipe/decommission授权和registration fence；
6. general E/W/A point-oracle merge伪代码与复杂度。

这些接口关闭并通过 Spike/Gate之前，仍只能继续 Spike/prototype与接口设计，不能启动会产生 Segment ACK authority 的正式实现。

<!-- END VERBATIM INTEGRATION AUDIT 1 -->

## Integration Audit 2（PASS）

以下为同一 reviewer 对 Audit 1 修复结果的第二次只读审计。审计正文完整保留；仅将 Markdown heading 统一下移一级并清理行尾空白。

<!-- BEGIN VERBATIM INTEGRATION AUDIT 2 -->

## Round 4 Integration Audit 2

结论：**PASS**。

Audit 1 的 5 项必修均已按最小合同准确修复，未引入新的正确性冲突、热路径损耗、复杂度膨胀或过度设计。可以完整归档本回复，将 Round 4 标记为 `Reviewed / Integrated`，随后只暂存本轮 9 份文档并 commit/push。

### 1. 基线与范围

已核对：

- HEAD：`dbc0f7fee412e68d1cecd40efbf6d80edfdc8af0`
- `origin/turbo-bk`：同一 SHA
- `HEAD...origin/turbo-bk`：`0 / 0`
- staged files：空
- 范围仍为 8 个 tracked 文档和新增 Round 4 归档
- 无关 `BtrLog Low-Latency Logging.pdf` 仍未跟踪，未读取、修改或纳入范围
- 本次审计未修改、暂存、提交或推送任何文件

README 当前仍是：

```text
Review Complete / Integration In Progress
```

在 Audit 2 回复归档前这是正确状态。

### 2. Audit 1 五项修复核对

#### 修复 1：strong assertion 对 immutable Profile/F 的长期绑定

**PASS。**

RFC-0004 §9.2 的 proof context 现在直接语义绑定：

```text
Profile descriptor hash/generation
permanent-loss budget F
failure-domain policy identity/generation
```

并明确：

- strong assertion 必须在长期存储和 compaction 后仍可解析到 immutable descriptor、当时的 `F` 与 failure-domain policy；
- 可以物理去重或引用 immutable summary；
- RepairIntent/receipt compact 后不得丢失该语义；
- 旧 assertion 不得在新 descriptor 或新 `F` 下被重解释；
- digest/root/count 仍只是 identity、完整性或审计 commitment，不是 proof；
- exact duplicate fields、record shape 和 schema 继续 OPEN。

联动完整：

- `docs/turbo-bk.md` invariant 20 明确长期绑定 descriptor/F/policy，并禁止 compaction 后重解释；
- Spike C Model A 增加 descriptor hash/generation、`F` 状态；
- 增加 `AttemptReinterpretRepairAssertionUnderChangedProfile` 负向动作；
- 增加 `RepairAssertionBindsImmutableProfileF` 不变量；
- falsification 和 `A-REPAIR-LOSS` config 均覆盖 descriptor/F 重解释。

这没有改变 RFC-0001 的 owner：RFC-0001 仍拥有 immutable Profile/F/policy；RFC-0004 只保存并消费其不可重解释的 proof-cut binding。

性能上只增加 repair operation/range 冷路径中的常量级语义绑定，不要求重复物理字段，不增加 steady-state Add I/O、per-entry MetadataStore 状态或额外 per-entry fsync。

#### 修复 2：terminal wipe/decommission proof 防重放 scope

**PASS。**

RFC-0004 §12 现在完整要求 terminal proof 语义绑定：

```text
bookieStableIdentity
old storage/device incarnation
explicit device/storage scope
operation identity/generation
cluster acceptance authority/version
irreversible result or permanent registration fence
```

同时锁定：

- wipe proof 只免除其明确覆盖 scope 的 catch-up；
- permanent decommission fence 必须永久拒绝该 old incarnation 再注册 writable；
- 旧 operation、另一个 device、部分目录、另一个 incarnation 的 proof 不得扩大或重放；
- local self-report、timeout、offline、新盘为空和 ordinary ensemble replacement 仍不足；
- exact authorization、attestation、人工审批与硬件证明继续 OPEN。

Spike C Model D 已同步：

- proof scope、operation generation、acceptance version 和 permanent registration fence 状态；
- `ReplayOldTerminalProofAcrossScope` 负向动作；
- 跨 incarnation/device/scope 重放不得免除 catch-up 的不变量；
- `D-STREAM` config 包含 terminal-proof scope/replay。

这仍是 decommission、wipe、rejoin 和 registration 冷路径，不形成 Add-time lease，也不要求 normal Add 查询 terminal proof。

#### 修复 3：Derived index 不再要求永久完整 move chain

**PASS。**

RFC-0003 §15 现在明确 relocation winner 只能由：

```text
committed checkpoint current selector
+ complete conditional MOVE_COMMIT suffix
```

决定，并要求 checkpoint selector 可证明来自此前完整 control history。RocksDB、mtime、最大物理 generation 和 data scan 均不能猜测 winner。

因此已经与以下合同一致：

- checkpoint through `S` + complete suffix `>S` 等价 full-history oracle；
- 历史 chain 在 current selector、anti-ABA 和 retirement authority完整接管后可以 compact；
- Spike B/C 的 full-chain replay 仅作为独立验证 oracle，不是生产 authority 的永久保留要求。

没有重新引入 full-chain retention、per-record 永久历史或 derived-index authority。

#### 修复 4：reader state 收窄为 bounded retirement authority

**PASS。**

RFC-0003 §11 现在只要求 checkpoint 保存：

- old-source retirement state；
- new-old-pin gate state；
- whole-allocation reclaimability；
- current selector 与 anti-ABA/fencing state。

同时明确不持久化：

- individual reader；
- future；
- buffer reference；
- pin history。

Runtime reader/pin tracking 是 bounded volatile state；进程 crash 后旧进程 pins 不作为 durable history继承，但 restart 必须先恢复 selector/retiring gate，再重新判断 source 是否可 free。

这保留了正确的安全边界：

```text
cutover
→ block new old-location pins
→ drain cut 前 readers
→ whole-allocation reclaimable
→ durable FREE_AND_BUMP
→ reuse
```

同时避免了 durable per-reader state、checkpoint 无界膨胀和 foreground reader 元数据写入。

#### 修复 5：归档 EOF

**PASS。**

Round 4 文件现在仅有一个 EOF newline；最后两个 bytes 为：

```text
3e 0a
```

即 marker 的 `>` 后直接一个换行，没有额外空白行。

### 3. 联动与冲突审计

未发现新冲突。

Owner 仍清晰：

- RFC-0001：immutable Profile descriptor、`F`、failure-domain policy；
- RFC-0003：同 Arena allocation、relocation、checkpoint、retirement、free/reuse；
- RFC-0004：distributed verifier assertion、accepted-loss ordering、delete assignment/snapshot、terminal proof和 recovery outcome；
- RFC-0005：消费上述 durable authority并决定本地操作能否产生 local success；
- Standard LedgerMetadata：唯一 OSS state/ensemble membership authority；
- Spike B/C：否证模型和实验，不成为运行时 authority。

没有出现：

- ledger-global repair hot CAS；
- digest-as-proof；
- mandatory Merkle、签名或 PKI；
- per-entry MetadataStore receipt；
- per-entry repair/control fsync；
- Add-time assignment lease；
- normal Add 远程 metadata/cursor/proof 查询；
- 永久完整 move-chain；
- durable individual reader/pin enumeration；
- foreground Add 等待 relocation barrier；
- cross-Arena transaction；
- 全局 delete sequence；
- 无界 receipt root、cursor vector 或 snapshot root。

现行 OPEN 边界仍正确，包括 exact record/schema/bytes、page/fan-out、compaction threshold、proof attestation、assignment topology、snapshot chunks、checkpoint/superblock protocol、selector packing、group-commit thresholds、outcome enum/wire/API 和 general E/W/A merge algorithm。

### 4. 正确性、性能、复杂度与过度设计

正确性：**PASS。**

- Strong assertion 无法在 compaction 后被错误 descriptor/F 重解释；
- terminal proof 无法跨 Bookie/incarnation/device/scope 重放；
- relocation restart authority 已统一为 checkpoint selector + suffix；
- old source 只有在恢复 retirement gate、阻断新 pins、drain readers并满足 whole-allocation 条件后才可 free；
- 所有此前通过的 loss ordering、receipt compaction、assignment handoff、recovery outcome 合同均未被削弱。

性能：**PASS。**

- descriptor/F binding 只增加冷路径 receipt 的常量语义；
- wipe/decommission proof 只参与 admin/rejoin/registration；
- checkpoint 只保存 current live/retiring authority，不保存历史 chain或 reader history；
- relocation继续允许有界 group commit；
- normal Add 无新增网络 hop、MetadataStore读取、fsync、全局锁或 assignment检查；
- foreground p99、checkpoint bytes、suffix retention和compaction interference仍由现有 Spike/Gate约束。

复杂度：**PASS。**

修复只补全既有 authority identity和重启判据，没有新增通用事务系统、分布式 proof framework、PKI、全局序列器或第二份 membership authority。Exact encoding仍推迟到 implementation-interface round 和 Spike。

过度设计：**PASS。**

尤其是 reader wording 的收窄消除了实现 durable per-reader/pin history的风险；current-selector checkpoint也避免永久保存完整 move history。没有为了正确性引入热路径机制。

### 5. 归档与机械验证

Round 4 主 reviewer 回复：

```text
review_original_normalized_bytes=38014
review_archived_bytes=38014
review_compare=PASS
```

Audit 1 回复：

```text
audit1_original_normalized_bytes=16300
audit1_archived_bytes=16300
audit1_compare=PASS
```

两者均只应用声明过的规范化：

- Markdown headings 下移一级；
- 清理行尾空白。

BEGIN/END marker 各一个，未发现删节或语义改写。

当前 Round 4 文件：

```text
1314 lines
56401 bytes
single EOF newline
```

机械验证：

- tracked `git diff --check`：PASS；
- Round 4 `git diff --no-index --check`：无 whitespace 输出；退出码 `1` 仅表示新文件与 `/dev/null` 有内容差异；
- 14 个相关 Markdown 单一 H1：PASS；
- fenced code block balance：PASS；
- repo-root/relative local links：PASS；
- branch ancestry：`0 / 0`；
- staged files：空。

### 6. 阶段结论与发布授权

可以执行以下收尾：

1. 将本 Audit 2 回复完整归档到 Round 4 文件，仅允许 heading 下移和行尾空白清理；
2. 将 README 的 Round 4 状态改为：

```text
Reviewed / Integrated
```

3. 只暂存以下 9 份文档：

```text
docs/rfcs/unified-wal/RFC-0001-profile-capability-install.md
docs/rfcs/unified-wal/RFC-0003-segment-storage-allocator.md
docs/rfcs/unified-wal/RFC-0004-range-recovery-delete.md
docs/rfcs/unified-wal/RFC-0005-segment-bookie-state.md
docs/rfcs/unified-wal/grill/README.md
docs/rfcs/unified-wal/grill/ROUND-04-bounded-proof-interfaces.md
docs/rfcs/unified-wal/spikes/SPIKE-B-allocator-block.md
docs/rfcs/unified-wal/spikes/SPIKE-C-no-object-tla.md
docs/turbo-bk.md
```

4. 明确排除 `BtrLog Low-Latency Logging.pdf`；
5. 归档后重跑机械校验；
6. commit 并 push 到 `nereusstream/bookkeeper` 的 `turbo-bk`。

本次 PASS 只表示 **Round 4 文档评审已正确集成**。它不把 RFC 提升为 Accepted，不把 Spike 标为已执行，也不使 `BK_SEGMENT_WAL` 或整体设计成为 Implementation Ready。

### 7. 下一轮 implementation-interface frontier

下一轮最值得 grill：

1. Sidecar bounded root/child/snapshot interface：record identity、predecessor/CAS、unknown version、compaction接管和 ABA；
2. ArenaControlLog API：conditional append、durable-through receipt、checkpoint selector、conditional free和 reader-cutover接口；
3. Bookie local durable state：route/install/normal activation/fence/recovery-only/delete/assignment 的最小原子落盘组合；
4. Recovery outcome compatibility：五类语义到现有 BookKeeper API、exception、admin状态和 metrics 的映射；
5. Wipe/decommission registration fence：cluster acceptance owner、scope和 durable replay接口；
6. General E/W/A point-read oracle merge伪代码、复杂度与 bounded implementation contract。

在这些接口冻结、Spike A/B/C 实际执行且对应 RFC/Gate通过前，只能推进 Spike、prototype和接口设计，不能开始产生 Segment ACK authority 的正式实现。

<!-- END VERBATIM INTEGRATION AUDIT 2 -->
