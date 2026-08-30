# RFC-0005：Segment Bookie State、Operation Semantics 与 ACK Authority

> 状态：**Proposed / P0 Prerequisite**<br>
> 依赖：[RFC-0001](RFC-0001-profile-capability-install.md)、[RFC-0003](RFC-0003-segment-storage-allocator.md)<br>
> 解锁对象：`BK_SEGMENT_WAL` ACK authority canary 的必要但不充分前置<br>
> 评审来源：[P0 Grill Round 1](grill/ROUND-01-root-contracts.md)、[Round 2](grill/ROUND-02-control-plane-authority.md)

## 1. 摘要

本 RFC 负责 Segment engine 对 BookKeeper 外部 operation 语义的等价实现、durability 线性化与本地 ACK authority。它关闭 RFC-0003 只定义 allocator/payload authority、却没有 RFC owner 负责 master key、activation、fence、explicit LAC、recovery Add 和 restart/replay 的缺口。

本轮只冻结职责、正确性边界和性能边界；exact control record、packing、on-disk bytes 与 group-commit 参数保持开放，等待后续 grill 与 Spike。

## 2. Authority 边界

RFC 所有权固定为：

| Authority | Owner |
| --- | --- |
| Profile lifecycle、install、activation authority | [RFC-0001](RFC-0001-profile-capability-install.md) |
| allocator、payload framing、generation、durable relocation authority/protocol 与 physical encoding | [RFC-0003](RFC-0003-segment-storage-allocator.md) |
| cluster delete、repair target 与 recovery placement authority | [RFC-0004](RFC-0004-range-recovery-delete.md) |
| 上述 authority 在 Segment Bookie 上的消费、durability、operation ordering 与 local success | 本 RFC |

本 RFC 不复制其他 RFC 的 metadata schema，也不把 derived locator 提升为 authority。
recovery/delete integration 依赖 RFC-0004，其具体接入边界仍是开放项；本 RFC Accepted 不能单独推导 ACK authority canary 已可执行。

## 3. 当前 Classic 行为基线

当前 Bookie 本地 durable 行为至少包括：

- handle 创建时安装并校验 master key；
- fence 只有在对应 Journal durability 完成后才完成 future；
- normal Add 拒绝 fenced ledger；
- recovery Add 在明确授权下绕过 normal fence 检查，但 payload 仍走正常 durable data path；
- explicit LAC 有 durable Journal 语义并可在 restart replay；
- master key 与 fence 可从 Journal/ledger storage 恢复。

当前 Bookie 没有一个需要 Segment 机械复制的本地 durable `CLOSED/SEALED` record。若未来 Segment 引入本地 seal，它必须由明确需求和独立合同证明，不能为了 record 对称性凭空增加。

## 4. 必须提供的 Segment operation 语义

Segment ACK authority 前必须定义并验证：

```text
ledger instance / Profile / Engine routing bind
protected authentication binding
durable activation gate
durable fence linearization and restart
normal Add
authorized recovery Add
explicit LAC
local delete/tombstone consumption
required read/LAC/list operations or explicit capability rejection
unknown/newer format fail closed
upgrade/downgrade fail closed
restart/replay
local success eligibility for BookKeeper AQ
```

“定义 operation 语义”不等于“一种语义必须独占一条 control record”。实现可以 group commit 或合并 framing，只要 crash/replay 后的状态、线性化点和拒绝集合可证明等价。

## 5. Routing、install 与 activation

Segment Bookie 必须消费 RFC-0001 的 authoritative local route：

- `CLASSIC` route 不能由 Segment data path 解释；
- `PROFILE` route 必须匹配 ledger instance、descriptor hash、Engine 和 protected auth binding；
- normal profiled Add 只有在匹配的 global READY authorization 和 durable local normal activation 存在后才能继续；
- `TOMBSTONED` route 永远不能重新进入 writable；
- restart 后的接受集合不能大于 durable route/install/activation 授权集合。

同一个 `PROFILE` route 至少需要表达以下互斥或受序约束的语义角色，但不要求现在冻结为独立 enum/control record：

```text
NORMAL_INACTIVE
NORMAL_ACTIVE
RECOVERY_ONLY(intentGeneration, authorizedFragmentOrRange)
COMMITTED_REPLICA_OR_READABLE
TOMBSTONED
```

`RECOVERY_ONLY` 和 `COMMITTED_REPLICA_OR_READABLE` 都不隐含 normal writable。activation proof、exact binding fields、角色 record packing 与 error mapping 仍保持开放。

## 6. Fence 与 Add

最低合同：

```text
normal Add local success
    => matching route/install/global READY/local NORMAL_ACTIVE
    && authentication accepted
    && ledger was not durably fenced before Add authorization
    && RFC-0003 allocation authority durable
    && payload durability barrier complete

fence completion
    => durable fence authority exists
    && restart cannot accept later normal Add as unfenced
```

normal Add 与 fence 的并发线性化点必须明确。fence response loss 由 durable state reread/replay 解析，不能通过内存 callback 顺序猜测。

## 7. Recovery Add

Recovery Add 是带有 recovery authority 的 payload 写入，不预设一条独立 `RECOVERY_ADD` control record。它必须：

- 只在匹配 ledger instance/Profile 且存在绑定 live RFC-0004 RepairIntent 的 durable `RECOVERY_ONLY` authority 时绕过 normal fence 拒绝；
- 匹配 target、authorized fragment/range 与 intent generation，不得写出授权范围；
- 使用与 normal Add 等价的 allocator/payload durability barrier；
- 在重复、response loss 和 restart 后保持 payload 幂等或明确冲突；
- 不要求 normal activation，也绝不授予 normal writable authority；
- 不能绕过 recovery-only authorization、authentication、instance、generation 或 delete/tombstone gate；
- 不能让普通客户端仅靠设置一个 flag 获得 recovery 权限。

Recovery-only authority 可以在 target 进入标准 ensemble 前存在。repair CAS 提交后，closed/historical target 通常转换为 `COMMITTED_REPLICA_OR_READABLE` 并关闭 recovery-only authority；只有 target 另行成为 current writable fragment member并满足 RFC-0001 post-CAS normal activation/fence 合同，才可独立进入 `NORMAL_ACTIVE`。

recovery authority 的集群来源、repair target、intent retention 与 delete discovery 由 RFC-0004 负责。Bookie local record 只消费该 authority，不复制 cluster schema。

## 8. Explicit LAC 与读操作

Segment engine 必须为 explicit LAC 定义：

- authorization；
- durable linearization；
- 单调/覆盖规则；
- restart replay；
- 与 fence、recovery 和 delete 的并发关系。

BookKeeper 对外需要的 read、LAC、list 或 storage introspection 操作必须逐项列入 capability matrix：支持的操作给出语义等价合同；不支持的操作必须在 placement/install 或调用点明确 capability-reject，不能静默返回不完整结果。

## 9. Delete 与 restart

Segment Bookie 只消费 RFC-0004 已授权的 instance-specific local tombstone/delete。local free/reuse 仍服从 RFC-0003 的 reader drain 与 durable generation bump。

restart 顺序至少满足：

1. 验证 Engine/superblock/format identity；
2. 恢复 allocator、payload 与同 Arena `MOVE_COMMIT` relocation authority；
3. 恢复 route、protected auth、normal activation、recovery-only/committed-readable role、fence、explicit LAC 和 tombstone 状态；
4. 按 durable move chain 重建 derived handle/locator/index，不能从物理 generation 或 stale locator 猜 relocation winner；
5. 对未知或无法证明的 state fail closed；
6. 按 RFC-0004 校验 storage incarnation、finite stream assignment、snapshot/suffix 与 per-stream durable cursors；
7. 完成 delete/recovery reconciliation 和 registration required-through 后才注册 writable。

unknown/newer format 或 Classic/Segment 错误 downgrade 不得通过扫描 payload 后继续 writable。Engine identity 的 cookie/superblock/registration exact encoding 是开放项，但 fail-closed 行为不是开放项。

## 10. 性能边界

必须保持：

- normal Add 不新增 per-entry control-log fsync；
- normal Add 不远程读取 MetadataStore；
- route/activation/fence 检查为有界本地 lookup，并尽量与 handle state 合并；
- allocator pool 与 DATA durability 继续允许 group commit；
- fence、normal activation、recovery-only grant/close、delete 等冷控制操作可以 durable/group commit；
- recovery Add 复用数据路径，不增加无意义的 per-entry control record。
- repair intent 与 local recovery authority 是 per-operation/per-fragment，不做 per-entry MetadataStore update 或 per-entry control fsync。
- compaction `MOVE_COMMIT` 可按有界 record/range group commit；它是 background relocation authority，不给 normal Add 增加 per-entry control fsync，也不创造新的 local success。
- delete effect 与 per-stream cursor 可 batch/group commit，但 cursor 永远晚于对应 effect durability。

所有 exact batching、record packing、cache layout 和阈值由 Spike 决定。不能用“正确性”作为无测量增加热路径 fsync、网络 hop 或全局锁的理由。

## 11. 安全不变量

1. `normal local success => matching durable route/install/global READY/local NORMAL_ACTIVE + valid authorization + durable allocation + durable payload`。
2. durable fence 之后，normal Add 不能形成新的 local success。
3. recovery Add 只能在 live RepairIntent 对应的 durable RECOVERY_ONLY authority 下绕过 normal fence，并仍满足 payload durability。
4. recovery-only 与 committed-readable role 永不隐式授予 normal writable authority。
5. restart 后接受集合不大于 crash 前的 durable authority 集合。
6. explicit LAC、tombstone 与 generation 不因 derived index 丢失而回退。
7. unknown/newer format 或错误 Engine 不能注册 writable。
8. local success 只有满足本 RFC 与 RFC-0003 时才能参与 BookKeeper AQ。
9. 同 Arena relocation cutover 不改变既有 local-success/AQ 事实；未 commit copy 不能扩大 payload authority。
10. durable `MOVE_COMMIT` 后新 lookup 走 new location，old allocation 的复用晚于 new-pin 阻断、reader drain 与 durable free/generation bump。
11. writable registration 意味着当前 storage incarnation 对 RFC-0004 authoritative assignment 的全部 required-through stream 已无洞 catch up。

## 12. 接受 Gate

进入 Accepted 前必须至少覆盖：

- normal Add 与 fence 的每个 durability/response-loss 边界；
- fence 后 normal Add 拒绝与 authorized recovery Add 成功；
- recovery-only grant、scope、response loss、close/committed-readable transition 与 restart；
- recovery Add 重试、payload conflict、普通 flag 伪造与 delete race；
- explicit LAC durability、单调性和 replay；
- install/activation/fence/delete state 的 restart 恢复；
- unknown control record、newer format 与 downgrade fail closed；
- derived index 全删重建不扩大接受集合；
- `MOVE_COMMIT` response loss、index rebuild、reader pin、old free 与 restart 不改变唯一 payload authority；
- delete stream gap、snapshot/assignment/incarnation mismatch 时保持 non-writable；
- Classic baseline 对比下的 throughput、p99、CPU、fsync 与 lock contention；
- Model A/C 中 Segment Bookie state 与 allocator state 的组合无 safety counterexample。

RFC-0005 未 Accepted 前，RFC-0003 只能解锁 Segment shadow writer，不能解锁 Segment ACK authority。RFC-0005 Accepted 也只是 canary 的必要前置；仍需 canary-specific evidence 与所有实际启用路径的依赖闭合，才能执行对应 ACK authority canary。

## 13. 开放问题

- durable state 的 exact record set、packing、checksum 与 group-commit 边界；
- protected auth binding 的本地表示；
- activation proof 的消费方式与 initial/replacement 差异；
- normal-inactive/active、recovery-only、committed-readable 的 exact local state mapping 与 error mapping；
- fence 与 inflight Add 的精确线性化实现；
- explicit LAC 与 payload block 的写序；
- read/LAC/list capability matrix；
- Engine identity 在 cookie、directory layout、superblock 和 registration 中的编码；
- local seal 是否确有需求；若有，其 authority 与 metadata CLOSED 的关系；
- RFC-0003 `MOVE_COMMIT` exact local record packing、batch completion、reader cutover接口、orphan GC 与 cross-Arena unsupported 后续协议；
- RFC-0004 delete stream topology、assignment/snapshot schema 与 exact local cursor packing；
- recovery/delete integration 对 RFC-0004 的 exact dependency boundary；
- performance Gate 的 exact thresholds。

这些问题关闭、相关 Spike/Model 通过前，本 RFC 不得标为 Implementation Ready。
