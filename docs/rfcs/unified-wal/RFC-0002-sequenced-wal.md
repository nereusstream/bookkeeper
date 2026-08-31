# RFC-0002：Sequenced WAL、Stale Writer 与 Successor Ledger

> 状态：**Proposed / RFC Required**<br>
> 依赖：BookKeeper 原生 quorum fencing、recovery 与 ledger metadata CAS<br>
> 安装版依赖：[RFC-0001](RFC-0001-profile-capability-install.md)<br>
> 形式化验证：Model B，无对象存储

## 1. 摘要

本 RFC 定义 BookKeeper entry 坐标之上的 sequence 合同，并把原 `BK_SEQUENCED_CLASSIC` 拆成两个能力边界不同的 Profile：

```text
BK_SEQUENCED_CLASSIC_CLIENT_ONLY
BK_SEQUENCED_CLASSIC_INSTALLED
```

当前最关键的约束是：旧 Bookie 不理解 `writerEpoch`，所以 client-only Profile 禁止 same-ledger epoch takeover。新 writer 必须 fence 并恢复旧 ledger，seal predecessor，然后在 successor ledger 上继续写。

## 2. 范围

本 RFC 负责：

- `SequenceDomain`、sequence reservation 与 run；
- entryId、WalSequence 和协议原生位置的分离；
- `appendId` 和 outcome-unknown 解析；
- concurrent I/O 与 ordered durable frontier；
- old-ledger fencing、recovery、seal 与 successor publication；
- client-only 与 installed Profile 的能力边界；
- RocksDB/控制 entry 作为 derived sequence index 的边界。

本 RFC 不负责：

- Profile 显式安装 wire contract，见 [RFC-0001](RFC-0001-profile-capability-install.md)；
- Segment allocator 或磁盘格式，见 [RFC-0003](RFC-0003-segment-storage-allocator.md)；
- general E/W/A range recovery 和 cluster delete，见 [RFC-0004](RFC-0004-range-recovery-delete.md)；
- 上层协议的事务、消费进度或可见性。

## 3. Profile 边界

### 3.1 `BK_SEQUENCED_CLASSIC_CLIENT_ONLY`

部署组合：

```text
new client
old or new Bookie
Classic Journal + EntryLog
sequence envelope stored as opaque payload
SYNC_ON_ACK
```

允许：

- client-side SequenceDomain；
- bounded sequence range reservation；
- `appendId`；
- concurrent Add 与 ordered completion；
- run header/footer 或 ledger 内 range-index control entry；
- dual-coordinate receipt；
- client-side recovery scan。

不允许宣称：

- Bookie 按 `writerEpoch` 拒绝 stale writer；
- same-ledger epoch takeover；
- server-side sequence conflict detection；
- Bookie read-by-sequence pushdown；
- Bookie sequence TailSummary；
- Profile hash 校验。

这里的“兼容”只表示旧 Bookie 可以保存 opaque payload，不表示它执行新的 sequence 或 fencing 合同。

该 Profile 只适用于受信任的 `SequencedWalHandle` 独占写入入口的部署。底层 `LedgerHandle` 不得暴露给普通调用方，raw Add 也不得绕过同一 run 的 sequenced admission order。`CLIENT_ONLY` 与 `INSTALLED` 是能力和信任边界不同的并列 Profile，不是时间阶段、成熟度顺序或替代关系。

### 3.2 `BK_SEQUENCED_CLASSIC_INSTALLED`

要求新 Bookie 支持 RFC-0001 的 install、`ledgerInstanceId` 和 descriptor hash。它仍使用 Classic engine，但可按独立 capability 增加：

- sequence envelope validation；
- server-side duplicate/conflict detection；
- derived sequence index pushdown；
- sequence TailSummary。

这些能力必须逐项声明，不能由 Profile 名称整体推断。

`INSTALLED` 本身不隐含 `writerEpoch` rejection、same-ledger takeover、全局 appendId 唯一性、全局 sequence 连续性、完整本地 sequence index 或 authoritative TailSummary。选择哪一个 Profile 是部署和信任边界决策，不能从名称推断生产资格或演进顺序。

## 4. 坐标与身份

三个坐标必须分离：

```text
entryId            BookKeeper ledger 内物理/协议坐标
WalSequence        SequenceDomain 内逻辑顺序
protocolPosition   Kafka/Pulsar/DB 等上层坐标
```

必须区分底层 BookKeeper durability evidence、活动写者 commit、独立 reader 可发现水位与切主后的 sealed authority：

```text
AQ_CANDIDATE
    BookKeeper 已形成有效的 durable quorum evidence，
    但该 interval 尚未必跨过活动写者的有序成功边界。

writerCommittedSequence
    当前 active writer/run 已经通过现有 LedgerHandle ordered success
    transition 连续推进的最大 sequenceEnd。

readerDiscoverableSequence
    独立 reader 当前通过 LAC/explicit LAC 和已验证 envelope
    能够安全发现的 lower bound；允许落后于 writerCommittedSequence。

sealedSequence
    predecessor 经 fencing、point recovery、durable close 和
    authoritative RunSeal/domain-head publication 后冻结的最终 sequence。

WAL_COMMITTED(active run)
    有有效 AQ evidence
    && 所有更早 DATA interval 已跨过现有 ordered success transition
    && writerCommittedSequence 已覆盖该 interval
```

只有进入 `WAL_COMMITTED` 后，才有资格对外生成和发送 final 写入回执；独立 reader 是否已经从 LAC 发现该 entry 不参与 active-run commit 判定。response loss 或回执发送失败不会撤销已经成立的 commit；调用方可以观察到 `OUTCOME_UNKNOWN`，再通过活动 writer frontier 或 failover 后的 authoritative RunSeal 与 appendId 解析。回执至少表达：

```text
WalAppendReceipt {
    ledgerId
    ledgerInstanceId
    entryId
    sequenceStart
    sequenceEnd
    appendId
    commitState = WAL_COMMITTED
}
```

一个 entry 可以承载一条或多条逻辑记录，但 envelope 必须使 sequence interval、payload 边界和 checksum 可无歧义验证。AQ candidate 可以作为内部恢复证据，但在进入连续 frontier 前不能以 final receipt 暴露为上层成功。

活动 ledger 上的 LAC 数字本身不是 WalSequence。reader 必须把它绑定到正确的 `ledgerId + ledgerInstanceId + run authority`，并读取、校验对应 entry envelope 后才能得到 `sequenceEnd`；missing、invalid 或 identity mismatch 必须 fail closed。explicit LAC 只用于周期性或批量降低发现延迟，不成为 correctness 依赖，也不要求每 append 写一次。

## 5. SequenceDomain 与 run

`SequenceDomain` 是上层选择的连续顺序空间。一个 domain 可以跨多个 BookKeeper ledger，但每个 ledger 只承载一个明确的 run。

候选 run identity：

```text
SequenceRun {
    sequenceDomainId
    runId
    writerEpoch
    ledgerId
    ledgerInstanceId
    predecessorRunId
    reservedStart
    state
}
```

候选状态：

```text
PREPARING -> ACTIVE -> FENCING -> RECOVERING -> SEALED
                                         └-> FAILED
```

exact path、字段编码和 hard bound 保持开放，但 authority 形状固定为：immutable run/seal child 先持久化且保持 inert，再由 `SequenceDomain` single-record versioned head CAS 引用并授予 authority。ACTIVE successor 的发布必须晚于 predecessor fencing、recovery、durable close 和 seal。

## 6. Sequence reservation

sequence 必须在发送 Add 前分配，以便 envelope、appendId 和 ordered completion 使用稳定身份。当前合同固定为 single active writer 在本地 bounded in-flight window 内分配；普通 append 不执行 MetadataStore range allocation。

最低要求：

- writer-local reservation 是有界区间；
- range 不重叠；
- active `writerCommittedSequence` 与 authoritative `sealedSequence` 都只能跨过连续 DATA interval；
- crash 可以留下已有 AQ evidence、但尚未通过 ordered success callback 进入 `writerCommittedSequence` 的物理 suffix；只有当前 `SequenceDomain` head 权威发布 `RunSeal(P)` 后，其中 `sequence > P` 的部分才永久 suppressed；
- `writerEpoch` 变化不能使旧 reservation 在 successor 上重新合法；
- admission reservation、active writer commit 与 sealed authority 分离。

当前合同不要求 Bookie 分配全局 sequence，也不冻结 exact window。窗口由内存、head-of-line latency、吞吐和 takeover scan benchmark 决定。若加入durable range allocator，必须直接修改本RFC并闭合hole、takeover语义与对应Gate。

### 6.1 Append admission linearization

每个 active run 只有一个受信任的 `SequencedWalHandle`，它必须建立唯一 admission linearization point：

1. inflight entry/bytes credit、`recordCount`/payload 校验和不依赖 sequence 的 payload digest 在线性化点前完成；未获得 credit 的请求不得消耗 sequence；
2. 在同一串行化边界内选择 `[sequenceStart, sequenceEnd]`，冻结绑定 run identity、appendId、payload digest、record count 与该 interval 的 immutable envelope，并把 envelope 交给同一 `LedgerHandle`；
3. 该边界只有在底层 entryId 已分配且请求已进入现有 pending-add order 后结束；随后才允许更晚 append 选择 sequence；
4. 对 admission 顺序相邻的 DATA append `i`、`j`，必须满足 `entryId(i) < entryId(j)`、`sequenceEnd(i) < sequenceStart(j)` 和 `sequenceStart(j) = sequenceEnd(i) + 1`；
5. linearization point 不等待 Bookie RPC、AQ 或 callback，入队后的 I/O 继续并发；
6. 成功入队前失败的请求不形成 durable append identity，也不得暴露 sequence receipt；入队后的 timeout、response loss 或 writer crash 进入 `OUTCOME_UNKNOWN`/recovery 解析。

control entry 若与 DATA 共用 ledger，必须经过同一 submission order 且类型无歧义；它是否占用普通 entryId、是否消耗 WalSequence 保持开放。exact Java/internal seam 也保持开放，可以是窄 admission lock、ledger-pinned ordered executor，或 benchmark 证明必要后的最小 enqueue seam；当前合同不要求修改 `PendingAddOp`。

## 7. appendId 与不确定结果

每个逻辑 append 或原子 application batch 必须有稳定 `appendId`。重试复用相同 appendId 和相同 payload digest；相同 appendId、不同内容必须是冲突。

客户端把结果分为：

```text
AQ_CANDIDATE    底层 AQ 已成立，但尚未通过 ordered success callback
                进入 writerCommittedSequence
COMMITTED       exact interval 已被 active writerCommittedSequence，
                或 failover 后的 authoritative sealedSequence 覆盖；
                final success receipt 已具备发送资格
ABORTED_SUPPRESSED
                authoritative RunSeal 明确绑定该 durable append identity，
                且它位于 predecessor sealed prefix 之外
REJECTED        可证明未被接受
OUTCOME_UNKNOWN response loss、timeout 或 takeover 竞争
```

`OUTCOME_UNKNOWN` 不能直接重发为新的 sequence，也不能直接当作失败。恢复必须通过 predecessor 的最大连续可恢复前缀、appendId 索引或权威 scan 解析。

appendId index 可以是 control entry、footer 或 RocksDB derived index；任何派生索引损坏时都必须能从 payload 重建。

被当前 `SequenceDomain` head 权威引用并发布的 `RunSeal(P)` 永久 suppress predecessor 的全部 `sequence > P`，correctness 不依赖 suppressed appendId table。`ABORTED_SUPPRESSED` 只有在该 authoritative RunSeal 显式绑定 `predecessorRunId + ledgerInstanceId + appendId + payloadDigest + provisional interval + seal identity/generation` 时才成立。该 durable identity 至少来自冻结 RecoveryContext 下合法 predecessor replica 的完整、已验证 envelope，并被纳入最终 seal；它不要求已达到 AQ，abort authority 来自 head 发布的 seal。

只存在于 writer 内存、从未形成可恢复 envelope 的 reservation 不能获得 cluster-authoritative `ABORTED_SUPPRESSED`。suppressed table 缺少某 appendId 也不能推导 `REJECTED`、未尝试或可安全复用；除非另有完整枚举证明，只能返回非终态的 unknown/not-found-within-horizon 语义。exact API enum、retry、retention/horizon 与 table completeness 继续开放。

bounded suppressed identity table 只能是 failover/seal 冷路径的 outcome accelerator，不是 commit authority。它可以保存 recovery 实际观察并验证的 identities，并由 inflight entries/bytes 和 bounded recovery scan 约束；partial table 的 absence 不具权威。若要让 absence 有权威，必须另行证明完整 source-set 枚举、无截断和 complete coverage，并在 RunSeal 中显式标记。

## 8. Concurrent I/O 与 ordered writer frontier

客户端可以并发发送多个 entry，但推进 active `writerCommittedSequence` 时必须按 sequence 连续进行。

```text
reserved:  100 101 102 103
completed: yes yes no  yes
frontier:  101
```

entry 103 的网络 completion 或 AQ 不能让可见 frontier 跨过 102。实现必须对 inflight entry count 和 bytes 设置硬上限，避免一个早期慢 entry 导致无界缓存。被后续 seal suppress 的 AQ bytes、head-of-line latency 与 takeover scan 必须作为 benchmark 指标。

实现必须复用现有 LedgerHandle 队头 ordered success transition：`PendingAddOp` 达到合法 AQ 只形成 AQ candidate；只有现有队头成功 transition 才推进 `writerCommittedSequence`。`SequencedWalHandle` 可以用 callback wrapper/context 携带 run、sequence interval、appendId 和 digest，在原 callback 以 OK 到达时把一个 `lastWriterCommittedSequence` 标量推进到对应 `sequenceEnd` 并完成回执。不得为此增加第二个 pending queue、completion reorder buffer、独立 commit state machine 或第二个 sequence LAC。

当前不直接修改 `PendingAddOp`。sequence tuple 能由 callback wrapper/context 携带，而 `PendingAddOp` 位于生产 Classic Add 热路径并涉及 recycler/reset/concurrency。只有 benchmark 证明 wrapper admission 成为瓶颈时，才评审 LedgerHandle 的最小 enqueue seam；不得预设把上层 WAL 类型灌入底层 op。

当前 durability 仅允许 `SYNC_ON_ACK`。`DEFERRED_SYNC_LEGACY` 遇 failed Bookie 时不具备一般 ensemble-change 合同，不与当前 Sequenced WAL 自动 failover 组合。

## 9. Client-only stale-writer 合同

### 9.1 禁止 same-ledger epoch takeover

错误流程：

```text
writer epoch 31 writes ledger L1
owner changes to epoch 32
epoch 32 continues writing L1
```

旧 Bookie 无法按 envelope 中的 epoch 拒绝 Add，因此此流程没有 server-side fencing 依据。

合法流程：

```text
predecessor ledger L1
    -> acquire recovery authority
    -> BookKeeper quorum fencing
    -> recover maximum provable contiguous prefix
    -> close/seal L1
    -> create and prepare successor ledger L2
    -> publish successor run ACTIVE
    -> new writer appends only to L2
```

新 writer 在 successor ACTIVE 之前不得接受上层新 append。

### 9.2 takeover 线性化边界

旧操作按其与 BookKeeper fence 的关系分类：

| 类别 | 语义 |
| --- | --- |
| fence 生效前已经取得 AQ | AQ evidence 的物理事实保留；只有进入最终 sealed continuous prefix 才成为 WAL COMMITTED |
| 与 fence 并发且结果不确定 | 由 recovery 是否纳入最大连续前缀决定 |
| fence 后才尝试形成 AQ | 不能重新达到 AQ；失败或维持 outcome unknown |

owner metadata 的变化不能伪造或抹除已有 AQ evidence。反过来，response 在 successor ACTIVE 后到达也不能把 sealed prefix 外的 candidate 提升为 WAL COMMITTED；必须看 predecessor recovery 与 authoritative RunSeal/head publication 事实。

精确安全目标是：

```text
after successor ACTIVE,
no predecessor operation outside the sealed recovered prefix
can newly enter authoritative WAL_COMMITTED state
```

### 9.3 Metadata watch 的边界

writer 可以缓存/watch owner metadata 来提前停止发送，但它只是 early-rejection optimization。正确性来自 BookKeeper fencing、recovery、predecessor seal 和 successor publication。

因此不要求普通成功前远程读取 root/epoch，也不能以“不访问 MetadataStore”推导 stale-writer 安全。

## 10. Recovery 与 successor publication

恢复方必须：

1. 取得唯一 recovery authority；
2. fence predecessor ledger；
3. 读取足以证明 AQ candidate 与最大连续 committable prefix 的副本；
4. 校验 sequence interval、appendId 和 payload checksum；
5. 得到最大连续 committable prefix `P`，截止在第一个不可证明连续的位置；
6. 以 BookKeeper 合同恢复/关闭 predecessor；
7. 在 recovered `lastEntryId` durable close predecessor；该动作只冻结 BookKeeper entry boundary，不发布 SequenceDomain seal，也不赋予任何 appendId/sequence terminal suppression；
8. 创建并准备从 `P + 1` 开始的 successor；
9. 创建 immutable RunSeal child，至少绑定 predecessor/run/ledger instance、recovered CLOSED metadata identity、`lastCommittedEntryId`、`lastCommittedSequence=P` 与 seal operation generation；该 child 在被引用前保持 inert；
10. 以 `SequenceDomain` single-record versioned head CAS 同时引用并发布 `RunSeal(P)` 与 successor ACTIVE/start=`P+1`；该 CAS 是唯一 publication linearization point，只有从此刻起 predecessor 的 `sequence > P` 才永久 suppressed，successor 才获得从 `P+1` 开始的 active authority。

若第 10 步 response loss，恢复方必须读取 exact head 解析，不得盲目创建第二个 successor。head CAS 不要求 MetadataStore multi-key transaction；immutable child 先落盘，single-record head CAS 再授予 authority。

run footer 只能加速恢复，不是 seal authority；authoritative cut 固定为 domain-head CAS 对 immutable RunSeal 的引用与发布。footer 的 exact encoding/validation 保持开放，缺失或损坏时走 bounded scan/fallback；sequence 只有在该 head CAS 绑定 predecessor seal 与 successor authority 后才能复用。恢复上限和 fallback 成本需要 RFC-0004 闭合。

## 11. Epoch-aware capability boundary

如果要求 Bookie 解析 `writerEpoch`、拒绝 stale writer 并允许 same-ledger epoch change，必须直接在当前capability set与本RFC中加入：

```text
EPOCH_AWARE_ADD
```

它至少需要：

- durable epoch install/fence authority；
- Add 携带 epoch 与 instance/hash；
- Bookie 对 epoch 单调性和冲突的 fail-closed 校验；
- recovery、ensemble replacement 和 mixed-deployment compatibility 合同；
- 独立形式化模型。

该能力不属于 client-only Profile，也不能由 `INSTALLED` 名称、envelope 中存在 `writerEpoch` 或 descriptor identity 推断。当前不含该 capability 的 installed Profile 同样使用 predecessor fence/recovery/seal/successor 流程。加入时直接修改当前合同，不创建并行代际实现。

## 12. Derived sequence index

允许的加速结构：

- ledger 内 packed range-index control entry；
- run footer；
- Bookie installed Profile 下的 derived RocksDB index；
- client-side cache。

所有加速结构必须满足：

- 不改变 ACK 事实；
- 不把缺失索引解释为缺失 payload；
- 能从权威 entry envelope 重建；
- locator 包含 ledger instance，避免 ledgerId 重用 ABA；
- rebuild 与 foreground I/O 有明确 QoS。

基础 client-side sequencing 不要求 `E = W`。`CLIENT_ONLY` takeover 的 correctness oracle 是 fenced BookKeeper point recovery、durable close、envelope validation 与 authoritative RunSeal；sequence total order 来自单一 `SequencedWalHandle` admission，而不是单个 Bookie 看见全部 entry。

在 `E > W` 下，一个 Bookie 只拥有 rotating write set 的局部观察，因此 installed Bookie 可以校验 request/ledger instance/Profile identity、envelope bounds/checksum、entryId 与本地 payload identity，以及同一 entryId 的 duplicate/content conflict；不能仅凭本地事实宣称整个 run 无洞、全局 appendId 唯一或 local last sequence 等于全局 frontier。local derived index 必须明确是 partial/coverage-bounded。

`E = W` 可以是某个 range/TailSummary fast-path capability 的局部前置条件，但不是 Sequenced WAL Profile 的全局前置。general `E > W` range/TailSummary fast recovery 在 RFC-0004 exact merge algorithm、Model E 与 point-oracle equivalence 通过前禁用；unsupported、stale、partial 或 budget exhausted 必须从 earliest unresolved coordinate 回退到 point recovery。

## 13. 安全不变量

1. 同一 SequenceDomain 中两个已发布 payload interval 不重叠。
2. ordered durable frontier 只跨过连续且可证明 committed 的 sequence。
3. 相同 appendId 不得映射到不同 payload digest 或两个 committed interval。
4. successor ACTIVE 之前 predecessor 已 fence、恢复并 seal。
5. successor ACTIVE 后，predecessor sealed prefix 之外的数据不得新进入 authoritative WAL_COMMITTED state。
6. 已形成的 AQ evidence 不被伪造或抹除，但 sealed prefix 外的 AQ candidate 永远不能成为 WAL COMMITTED。
7. successor start 固定为 predecessor durable sealed prefix `P + 1`。
8. recovery 只发布最大可证明连续前缀。
9. 删除全部 derived index 不改变可恢复结果。
10. client-only Profile 不依赖旧 Bookie 解释 writerEpoch。
11. sequence allocation、immutable envelope 与 existing Add enqueue 按同一 admission order 线性化；不存在 raw Add bypass。
12. LAC 必须与 run/instance 和已验证 envelope 结合，不能单独推导 WalSequence。
13. sequence locator/query 绑定 runId 与 ledgerInstanceId；successor 复用 sequence 数字不能把旧 appendId 映射到新 payload。
14. 基础 Sequenced WAL correctness 不依赖 `E = W`、range/TailSummary fast path 或单 Bookie 全局 sequence index。

## 14. Model B 最低场景

形式化模型至少覆盖：

- 两个 writer 竞争 recovery authority；
- 并发 Add、乱序 completion 和 response loss；
- fence 前 AQ、fence 并发、fence 后到达；
- client crash before/after reservation；
- 并发 caller 在 sequence reservation 与 Add admission 之间重排；
- recovery crash and retry；
- successor publication CAS response loss；
- suffix entry 已取得 AQ 但未进入 writer committed frontier；
- authoritative RunSeal/head publication 后 late callback 到达；
- Bookie crash 和 ensemble change；
- appendId 相同内容重试与不同内容冲突；
- suppressed identity table partial/complete、从未 durable 的 reservation 与 sequence 数字复用；
- writer committed 已推进但 reader LAC 尚未发现，及 explicit LAC stale/missing；
- 3/3/2、3/3/3、3/2/2 以及一般 `E > W` 的协议结构。

模型不包含 blob store。

## 15. 接受 Gate

本 RFC 进入 Accepted 前必须：

- takeover 的 recovery authority、metadata schema 和 CAS 线性化点冻结；
- Append admission linearization 的语义、raw Add exclusion 与 entryId/sequence 同序不变量冻结；
- AQ candidate、WAL COMMITTED、authoritative RunSeal/head publication 与 successor `P + 1` 规则冻结；
- writer committed、reader discoverable 与 sealed authority 的边界冻结；
- outcome-unknown 解析有确定算法和有界 fallback；
- client-only mixed-version 集成测试证明不依赖 server epoch 解析；
- Model B 无 safety counterexample；
- 当前 API 明确只支持 `SYNC_ON_ACK`；
- derived index 删除/rebuild 不改变恢复结果。

任一测试出现两个 ACTIVE successor、sealed prefix 外旧数据被发布、sequence overlap 或 appendId 内容冲突被接受，本 RFC 不得提升状态。

## 16. 开放问题

- SequenceDomain/RunSeal immutable child 与 domain-head 的 exact schema/path/hard bound；
- recovery authority 的取得、租约失效和 CAS 细节；
- admission linearization 的 exact Java/internal seam、exact in-flight window 与 future durable range allocator；
- predecessor footer acceleration 的 exact encoding、validation 与 fallback；
- run header/footer 是否占用普通 entryId，以及 control entry 与 DATA 的区分；
- application batch 的原子边界；
- suppressed identity table 的 exact evidence predicate、partial/complete coverage、retry、retention/horizon、索引大小、overflow 与冲突窗口；
- recovery scan 的最大窗口与超限处理；
- active reader freshness SLA 与 explicit LAC policy；
- production exact E/W/A/failure-domain/capability legal-combination table，以及 `E > W` local validation/index coverage；
- installed Profile 的 server-side sequence capabilities 是否拆成多个 feature bit；`EPOCH_AWARE_ADD` 仍不属于当前 capability set；
- pooled lane 是否永远 deferred，或另立专门 RFC。

这些问题关闭且 Model B 通过前，Sequenced WAL 仍是 RFC Required。
