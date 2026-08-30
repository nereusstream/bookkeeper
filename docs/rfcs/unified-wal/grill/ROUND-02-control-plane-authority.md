# Round 2：Control-plane Authority、Activation 与 Repair

> Reviewer：复用 Round 1 的固定 `gpt-5.6-sol / max` agent<br>
> 源码与文档基线：`cc7e03c81571b01b7f9da97a4c9bf1154d743c1c`<br>
> 评审模式：正确性、热路径性能、复杂度和过度设计联合审查<br>
> 记录规则：以下 reviewer 回复未经删节；规范是否采纳以对应 RFC 的集成结果为准。

## 本轮 Questions

### Q1：Metadata namespace / lifecycle authority

在独立 CAS sidecar、完整 LedgerMetadata custom metadata lifecycle、纯 Bookie-local receipts 三种方案中选择 Profile control authority；要求解决 standard LedgerMetadata 与 sidecar 的 ABA、发布顺序和 response-loss，又不能引入通用跨节点事务或 Add-time metadata lease。

### Q2：Initial activation publication

冻结 initial ensemble 的 route claim、标准 metadata 创建、READY authorization、Bookie durable activation 与 create/open success 顺序；要求持有合法 master key 的 stale/legacy client 在 READY 前仍不能取得 profiled ACK，普通 Add 只检查本地状态。

### Q3：Active write-time ensemble replacement

按现有 `LAC+1` CAS 与 pending Add resend 路径，决定 replacement install、membership publication、normal activation 的顺序；禁止为了 Profile 复制整个历史 fragment或加入 per-entry metadata 操作。

### Q4：AutoRecovery repair / delete discovery

决定 recovery target 在接收第一份 durable payload 前是否必须有 cluster-durable repair intent，以及 recovery-only authority、ensemble CAS、committed/readable state、delete freeze target enumeration 的边界。

## Reviewer 完整回复

<!-- BEGIN VERBATIM REVIEW -->

## Round 2 独立审查结论

基线已核对为 `turbo-bk@cc7e03c81571b01b7f9da97a4c9bf1154d743c1c`。本轮总判定：

| 问题 | 结论 |
|---|---|
| Q1 Metadata namespace / lifecycle authority | **REVISE → 采用最小化 A′ 后可 LOCK** |
| Q2 Initial activation publication | **REVISE → 调整初始发布顺序后可 LOCK** |
| Q3 Active write-time ensemble replacement | **LOCK，附带必须写入的并发/response-loss/fence 条款** |
| Q4 AutoRecovery repair / delete discovery | **REVISE → durable repair intent 可 LOCK；“CAS 后 normal activation”必须收窄** |

### 已核对的 BookKeeper 事实

1. `LedgerMetadata.State` 和 proto 都只有 `OPEN / IN_RECOVERY / CLOSED`：
   - `bookkeeper-server/.../client/api/LedgerMetadata.java:153-167`
   - `bookkeeper-proto/.../DataFormats.proto:32-36`

2. `customMetadata` 会被当前标准 mutation path 保留：
   - `LedgerMetadataBuilder.from(...)` 复制整个 map；
   - ensemble change、recovery、close/fence 等主要路径都从现有 metadata builder 派生；
   - `LedgerMetadataSerDe` 会完整序列化它。

   因此，不能声称当前 OSS writer 必然丢失未知 custom metadata；但它仍然不是 Bookie Add gate，旧客户端也不会理解其中的 Profile lifecycle。

3. 当前写期 ensemble change：
   - 单个 `LedgerHandle` 用 `changingEnsemble` 串行化换组；
   - 只在 metadata 仍为 `OPEN` 时推进；
   - 新 fragment 起点是 `LAC + 1`；
   - LedgerMetadata CAS 成功后才对 pending Adds 执行 resend；
   - 不复制整段历史数据。

   对应 `LedgerHandle.java:2266-2435`。

4. 当前 AutoRecovery：
   - replacement target 只存在于 `BookKeeperAdmin`/`LedgerFragmentReplicator` 的内存 map；
   - 使用 `FLAG_RECOVERY_ADD` 逐 entry 写目标节点；
   - copy 完成后才 CAS 替换 ensemble；
   - `LedgerFragmentReplicator` 明确拒绝未关闭 fragment；
   - `ReplicationWorker` 也跳过 open fragment。

   对应：
   - `BookKeeperAdmin.java:992-1042, 1121-1161`
   - `LedgerFragmentReplicator.java:138-149, 341-565, 586-666`
   - `ReplicationWorker.java:450-487`

5. 现有 underreplication marker 只有 `ledgerId + missing replica list + optional ctime`，worker lock 是临时锁；没有 target、fragment、instance 或 operation identity。完成后 marker 会被删除。因此它不能承担未发布 target 的长期 delete discovery。

6. 当前 `RECOVERY_ADD` 本质上只是 wire flag 加 master key，并直接绕过普通 fence 检查；它不是可审计的 durable recovery authority：
   - `WriteEntryProcessor.java:70-79`
   - `BookieImpl.java:1018-1034`

---

## Q1 — Metadata namespace / lifecycle authority

### 结论：REVISE → A′ 可 LOCK

独立 sidecar 是正确方向，但不能把 A 写成“一个 sidecar 大记录拥有所有事实”，也不能把 raw LedgerMetadata version 当作永久有效的 Add-time identity。

### 可锁定的最小 A′

1. **标准 LedgerMetadata 继续唯一拥有 OSS state 与 ensemble membership。**

   Sidecar 不得覆盖或重新解释 `OPEN / IN_RECOVERY / CLOSED`，也不能成为第二份 ensemble truth。

2. **Sidecar namespace 拥有 Profile 实例身份和 Profile 控制事实。**

   至少包括：

   - `ledgerId + ledgerInstanceId`
   - immutable descriptor/hash
   - creation request identity
   - READY/activation authorization generation
   - RFC-0004 拥有语义的 repair/delete operation records

   “同一 namespace”不等于“同一个不断膨胀的 root blob”。Repair intent 应允许是有界 child record；RFC-0001、RFC-0004 的语义 owner 仍需保持分离。

3. **必须解决 LedgerMetadata 与 sidecar 的 ABA 绑定。**

   仅用 `ledgerId + metadata version + ensemble digest` 不足以区分 ledgerId 删除后重建，因为 metadata version 可从头开始。最小选择二选一：

   - 标准 LedgerMetadata 的 reserved custom metadata 中写一个小型、immutable 的 `ledgerInstanceId/sidecar reference`；或
   - 明确禁止 ledgerId 在 sidecar/tombstone 生命周期内重用。

   推荐前者。这里只在 custom metadata 中放 immutable backlink，不把完整 lifecycle 塞进去。

4. **LedgerMetadata version 是发布时的 CAS/binding evidence，不是永久 Add gate。**

   每次 READY、replacement activation 或 repair intent 的 authority 可以绑定：

   - 当时观察到的标准 metadata version；
   - canonical relevant ensemble/fragment digest；
   - instance marker；
   - sidecar operation generation。

   但普通 Add 不得读取当前 metadata version，也不应因不相关的 metadata 写入而要求全 E 重激活。Bookie 热路径只比较已经 durable 到本地的紧凑 activation identity。

5. **两个 metadata node 不需要通用事务。**

   最小 ordering 合同是：

   ```text
   sidecar intent/reservation
       alone does not authorize normal Add

   standard LedgerMetadata membership CAS
       alone does not activate a new Bookie

   post-CAS sidecar activation authorization
       binds the exact committed membership/version

   local durable ACTIVE
       occurs only after that authorization
   ```

   任一步 response loss 都通过读取两个 authority 并比较 operation identity/version/digest 解析。任何中间态都只能是可重试或 inert，不得扩大接受集合。

6. **只需要一个 ledger-instance 冷控制 CAS generation/fencing token。**

   它用于 coordinator crash/leader change、replacement 与 delete/repair 的冲突解析。不要把它扩展成通用两阶段事务、全局锁或 Add-time lease。

### A 的必要限制

Sidecar 无法自动阻止一个有 LedgerMetadata 写权限、却完全不理解 Profile 的旧客户端直接 CAS ensemble。因此必须明确以下二选一边界：

- profiled ledger 的标准 metadata mutation 只授予 Profile-aware coordinator；master key 本身不授予 metadata mutation；
- 或提供等价的 ACL/fencing enforcement。

如果旧客户端同时拥有 master key 和不受限的 LedgerMetadata 写权限，A/B 都无法仅靠 Bookie 本地 Add gate保证 membership protocol。具体 ACL/credential 机制保持 OPEN，但该 authority boundary 不能不写。

### B：REJECT 作为“完整 lifecycle”方案

当前源码确实会保留 custom metadata，所以 B 不是因为“必然丢字段”而失败。拒绝它的理由是：

- 旧客户端忽略其中 lifecycle；
- full descriptor、receipts、repair history 会随每次 ensemble/fence/close CAS 被整块读取和重写；
- 容易造成 metadata node 膨胀、CAS contention 和无界 history；
- LedgerMetadata 删除后，instance tombstone/delete discovery 也随之消失；
- 把 RFC-0001、RFC-0004 的 operation semantics 耦合进所有 OSS metadata writers。

允许在 custom metadata 中保留一个小型 immutable Profile backlink；不要放完整状态机和 receipts/history。

### C：REJECT

只有 Bookie-local receipt 和 coordinator 内存聚合不能解决：

- coordinator crash 后创建/activation response loss；
- 两个 live instance；
- orphan install；
- repair target delete discovery；
- ledgerId reuse/tombstone；
- leader change 后的幂等恢复。

### 性能与复杂度

A′ 增加的是创建、换组、repair、delete 冷路径 CAS/read，不增加普通 Add 的 MetadataStore I/O。真正要避免的是：

- 一个不断增长的 sidecar root；
- 每 entry 更新 sidecar；
- raw metadata version 的每 Add 校验；
- 为两个 node 实现通用事务系统。

---

## Q2 — Initial activation publication

### 结论：REVISE → 以下序列可 LOCK

推荐序列的 `READY → local ACTIVE` 方向正确，但“先创建标准 LedgerMetadata，再开始 local route install”在当前威胁模型下留下 Classic lazy-create 窗口。

标准 LedgerMetadata 创建时已经是 OSS `OPEN`。如果持有 ledgerId/master key 的 stale/legacy client 在 route 仍为 `ABSENT` 时发送 legacy Add，它可以先抢到 `CLASSIC` route。之后 Profile install 会冲突，虽然能 fail closed，却留下“sidecar PREPARING + 同 ledgerId Classic ACK/data”的失败实例和复杂清理。

### 最小安全序列

```text
1. allocate ledgerId + ledgerInstanceId，选择 initial ensemble
2. sidecar create-if-absent PREPARING/reservation
3. 对全部 E 个 Bookie durable install PROFILE，但保持 normal-inactive
   - 此步同时完成 ABSENT -> PROFILE 的原子 route claim
4. create-if-absent 标准 LedgerMetadata
   - initial ensemble 必须与 install 集合一致
   - 携带 immutable instance/sidecar backlink
5. sidecar CAS 发布 READY_AUTHORIZED
   - 绑定实际 LedgerMetadata version + canonical ensemble digest
   - 仅在 all-E durable install 已被验证后允许
6. 以 READY authority 做幂等 ACTIVATE RPC
7. 全部 E 个 Bookie durable normal ACTIVE 后，create/open 才返回成功
```

如果实现能证明 ledgerId、master key 和标准 metadata 在 route claim 前对任何 stale/legacy client 都不可见且不可修改，原始 standard-first 顺序可以等价；在当前明示威胁模型下不能依赖这个假设。

### 可以锁定的安全合同

```text
ACK(normal profiled Add)
    => matching global READY authorization existed first
    && matching local durable normal ACTIVE existed first
```

同时：

- READY 可以早于部分 Bookie local ACTIVE；
- 未激活节点必须返回明确的 fail-closed/transient-unavailable；
- watch 只能提前触发 activation 或优化失败响应，不能作为正确性依据；
- ACTIVATE 必须幂等，response loss 后查询或重试同一 operation；
- 普通 Add 只查本地 durable/replayed state；
- local durable fence/tombstone 不能被迟到 ACTIVATE 重新打开。

### 是否需要两个 sidecar 状态名

正确性上需要区分两个**事实**：

1. `READY_AUTHORIZED`：全局允许 Bookie执行 activation；
2. `ALL_E_ACTIVATED/AVAILABLE`：正常 create/open 可以对外成功。

不要求现在锁死两个 enum 名。第二个事实可以实现为：

- 一个 availability-complete bit/generation；或
- READY 加上对 E 个 Bookie local state 的有界重查，再补写 completion。

不要为了它在 root 中保存无界完整 receipt blob。状态名、receipt packing 和 exact schema 保持 OPEN。

### Partial activation 的边界

分布式 E 节点无法原子同时 ACTIVE。必须接受一个短暂的 partial-active interval。安全要求是该窗口不会产生错误 Profile/durability，只可能：

- 部分 write set 成功；
- 部分节点返回 transient unavailable；
- create/open 尚不返回正常成功。

如果产品要求“返回前任何客户端都不能写入”，还必须依赖未向调用方发放 Profile write credential，不能假设全 E local activation 原子完成。该 credential 机制保持 OPEN。

### 性能

创建多出 bounded sidecar CAS、E 个 install 和 E 个 activation，均为冷路径。Bookie control durability 可以 group commit；不要求每 ledger 单独 fsync。正常 Add 无新增网络 hop、MetadataStore read 或重型 proof verify。

---

## Q3 — Active write-time ensemble replacement

### 结论：LOCK

以下核心序列可以直接写为规范合同：

```text
select compatible replacement
-> durable install inactive
-> CAS standard LedgerMetadata at existing LAC+1 fragment authority
-> publish/verify post-CAS membership activation authority
-> replacement durable normal ACTIVE
-> resend pending Adds
```

并明确：

- 不为写期换组复制整个历史 fragment；
- replacement 在标准 membership CAS 前不得 normal-active；
- CAS 成功但 activation 未完成时不得向 replacement resend；
- CAS→activation 的中间态只允许写暂停/明确 transient failure，不允许降级 Classic；
- 普通 Add 不做 per-entry metadata 操作。

### 与现有源码的对齐

当前实现已经：

- 用 `changingEnsemble` 串行化单 handle 换组；
- 在 metadata 仍为 `OPEN` 时才 CAS；
- 以 `LAC+1` 建新 fragment；
- CAS 成功后重发 pending Adds；
- 不做历史 copy。

实现时不能把 install RPC/fsync 塞进 `MetadataUpdateLoop.transform`；transform 可能因 CAS conflict 被多次调用。需要显式的异步冷路径 phase machine。

### 并发 writer

- 继续继承 BookKeeper 的 single-writer/fence 语义；本轮不应发明新的多 writer consensus。
- 同一 handle 的 replacement 继续串行。
- 两个候选并发时，以标准 LedgerMetadata CAS winner 为唯一 membership winner。
- loser 重读后：
  - 若自己的 exact replacement mapping 已提交，则进入幂等 activation；
  - 若当前 mapping 不含自己的 target，则 target 保持 inactive/orphan，不得自行激活；
  - 若 ledger 已 `IN_RECOVERY/CLOSED`，立即停止 normal activation/resend。

另一个必须保持 OPEN 的边界是：谁有权修改 profiled ledger 的标准 LedgerMetadata。Sidecar 只约束 Profile-aware writer，无法约束拥有 metadata write ACL 的完全旧客户端。

### Fence/recovery race

必须锁定本地状态序：

- durable fence/tombstone 先发生时，迟到 activation 必须失败；
- activation 先发生时，后续 durable fence 必须关闭 normal Add；
- `IN_RECOVERY/CLOSED` metadata 不能产生新的 normal activation；
- recovery-only authority 不能被提升为 normal ACTIVE。

现有 `MetadataUpdateLoop` 在 CAS conflict 后重读，并在 `IN_RECOVERY/CLOSED` 下停止换组，可作为标准 membership 侧基础，但 Bookie local activation/fence 仍需 RFC-0005 证明。

### CAS response loss

不得盲目再选 replacement。必须重读标准 LedgerMetadata：

- exact fragment start、old ensemble identity、replacement mapping、instance marker 与 operation generation 匹配：视作 CAS 已提交，继续 post-CAS activation；
- 不匹配：不得激活该 target，将其视为 inactive orphan 或重新规划。

### Activation response loss

- 只重试/查询相同 activation operation；
- Bookie durable ACTIVE 返回相同 generation/receipt；
- 不得仅因 response timeout 选择第二个 target；
- 如果最终另一次 CAS 把该 target 移出当前 fragment，必须使旧 activation generation 不再成为当前 writer 的 authority，并保留足够记录供 orphan/delete 处理。

### Orphan install

写期 replacement 在 CAS 前不复制 payload，因此 inactive orphan 不要求加入 data repair transaction。最低要求：

- 从未 normal-active；
- GC 不能只靠超时；
- 必须用 sidecar/standard metadata、instance/hash、稳定 grace 和 durable tombstone 证明不会被迟到 response/retry 激活。

是否在 install 前增加 durable replacement-attempt record，可由 orphan/delete schema 决定；不要把它强行变成每次换组额外的通用事务。如果未记录，必须证明未发布 target 从未收到 payload且能安全本地回收。

### Sidecar binding 最小化

Post-CAS authority 只需绑定：

- ledger instance/profile identity；
- committed standard metadata version；
- relevant fragment start；
- canonical new ensemble digest；
- activation operation generation。

不要把完整 LedgerMetadata、pending Add 列表或 per-entry 状态复制进 sidecar。

### 性能

相较 OSS 换组，会增加：

- replacement install durable barrier；
- 一个 post-CAS activation authority step；
- replacement local activation durable barrier。

这些会增加故障换组尾延迟，但不影响 steady-state Add throughput。必须测 failover stall/p99；可通过 control group commit、缓存 immutable descriptor、避免额外 sidecar history rewrite 降低成本。禁止用整段 copy、逐 entry metadata write 或逐 Add远程校验来换取正确性。

---

## Q4 — AutoRecovery repair / delete discovery

### 结论：REVISE

**Durable instance-specific repair intent 是必要合同，可以 LOCK。**

但推荐序列中的“CAS ensemble 后 normal activation”不能无条件锁定。当前 AutoRecovery 只复制 closed fragment；对 closed ledger 或历史 fragment 的 target 授予 normal writable authority，会扩大接受集合，尤其新 target 未必已有 durable fence。正确的 post-CAS 状态通常应是 `COMMITTED_REPLICA/READABLE`，不是 normal ACTIVE。

只有 target 同时成为当前 writable fragment 成员，并满足 Q3 的 post-CAS authority/fence 条件时，才允许单独执行 normal activation；当前 OSS AutoRecovery 路径并不属于这种情况。

### 为什么 durable intent 必须存在

现有 target 只在 worker 内存中，而 underreplication marker只保存 missing Bookie。Crash 可能发生在：

```text
target 已选
-> recovery Add 已写入部分/全部数据
-> ensemble CAS 尚未提交
```

此时：

- restart 后 worker 会重新选 target；
- LedgerMetadata 看不到旧 target；
- underreplication marker 看不到旧 target；
- delete freeze 也无法枚举旧 target。

仅靠 local install receipt 要求 delete 扫描全部 Bookie，无法处理离线 target，也没有完整集群枚举 authority。因此，在 target 能接收本 instance 的任何 durable recovery payload 前，必须先存在 durable cluster intent。

### 修订后的最小序列

```text
1. CAS create instance-specific repair intent
2. target durable install inactive
3. target durable grant RECOVERY_ONLY(intent generation)
4. 复用现有 recovery Add data path进行 copy
5. copy 完成后重读并验证 standard metadata
6. CAS standard ensemble replacement
7. target transition to COMMITTED_REPLICA/READABLE
   - revoke/close RECOVERY_ONLY authority
   - 不自动授予 normal writable
8. durable mark intent COMMITTED
9. 仅在满足保留/删除发现条件后 compact intent
```

CAS response loss必须通过重读 standard metadata 的 exact fragment/replacement mapping 解析。

### 最小字段

应锁语义，不锁 exact wire/schema。最小需要：

- `ledgerId`
- `ledgerInstanceId`
- `repairOperationId/request identity`
- `descriptorHash/profile generation`
- base LedgerMetadata version，作为 CAS/rebase evidence
- canonical fragment identity：
  - fragment start；
  - fragment end/range，或可唯一推导 range 的 identity；
  - old ensemble digest；
- 被替换的 member/slot；
- target Bookie；
- recovery-only authority generation；
- lifecycle/result state。

“source”必须区分：

- **需要持久化**：被替换的旧 ensemble member，因为 completed rereplication 会覆盖同一 fragment key，之后 LedgerMetadata 不再保留旧 Bookie；delete 仍需找到它。
- **不需要持久化为 authority**：本次实际从哪个 surviving replica 读取数据。读取源可以动态重试，写入 intent 会造成不必要 churn。

不要记录：

- 每 entry copy progress 到 MetadataStore；
- 每 entry receipt；
- 实际 reader 选择历史；
- 无界重试 ID 列表。

### 生命周期与 retention

至少需要区分：

- PREPARED/RECOVERY_AUTHORIZED；
- COMMITTED；
- ABORTED 或 ORPHAN_CLEANUP_PENDING。

不要求每个名称都是独立持久状态，但以下不能省：

1. CAS 前 target 已收到数据时，intent 不得直接删除；
2. COMMITTED intent 仍需保留 source/target history；
3. 只有 target 已进入另一个不会丢失的 durable delete-history authority，或已有 target-local durable cleanup proof，intent 才能 compact；
4. “tombstone”不能等同于把 target 从 delete enumeration 中移除。

这点尤其重要，因为 `replaceEnsembleEntry` 会覆盖同一 fragment 的旧 ensemble，标准 LedgerMetadata 并不保存完整 rereplication target/source 历史。

### 与 underreplication marker 的 owner 关系

- Underreplication marker：拥有“该 ledger 需要检查/修复”和 missing replica scheduling事实；
- worker lock：只负责当前 worker 排他执行，不是 crash 后 target authority；
- standard LedgerMetadata：唯一拥有最终 ensemble；
- RFC-0004 repair intent：拥有 pre-publication target、recovery authorization 和 delete discovery history；
- Bookie local record：拥有该 target 对此 intent 的 durable install/recovery-only acceptance。

不要把 existing marker 直接膨胀成唯一 repair transaction record；它完成后会删除，而且没有 instance identity。物理上是否复用 underreplication namespace仍可 OPEN，但语义和 retention 必须满足上述边界。

### Delete freeze

必须锁定：

```text
任何 target 在接收第一份 durable instance data 前
    => 已存在可由 delete freeze 枚举的 durable intent
```

以及：

- delete freeze 与新 repair intent 通过同一 instance operation generation/CAS 串行；
- delete 先赢：之后不能新建 repair intent或授予 recovery authority；
- repair intent 先赢：delete target set 必须包含其 source/target，无论 copy/CAS 是否完成；
- exact child enumeration、watermark/index 和 compaction schema 保持 OPEN。

简单地“先 list children，再写 DELETE_INTENT”会漏掉并发 intent，不能作为最终方案。

### RFC-0005 必须同步收窄

当前 RFC-0005 写的是 recovery Add 不能绕过 activation。应改成角色化合同：

```text
normal Add
    => matching durable normal activation

recovery Add
    => matching durable recovery-only activation/authority
       bound to a live repair intent
       and never grants normal writable authority
```

Recovery-only authority可以在标准 ensemble CAS 前存在，但只能：

- 绕过 normal fence 来执行授权范围内的恢复写；
- 匹配 instance、fragment/range、target 和 intent generation；
- 受 delete/tombstone gate约束。

普通客户端设置 `RECOVERY_ADD` flag 不能获得该 authority。

### 性能

新增成本应严格限定为每 repair operation/fragment：

- intent create CAS；
- 一次 target recovery-only local durable control operation；
- 已有 standard ensemble CAS；
- intent completion CAS。

不得给 recovery payload hot path增加：

- per-entry MetadataStore round trip；
- per-entry control fsync；
- per-entry intent progress update。

BatchRecoveryAdd/wire schema、copy checkpoint、control group-commit packing保持 OPEN。大量小 fragment 下的 intent CAS 放大需要 benchmark，但不是 normal Add 性能回退。

---

## 本轮可立即同步的合同

1. 标准 LedgerMetadata 只保留 OSS 三态，并继续作为唯一 membership authority。
2. Profile 使用独立 CAS sidecar namespace；允许标准 custom metadata 只带 immutable instance/sidecar backlink。
3. Sidecar 与 LedgerMetadata 不做通用跨 node transaction；所有中间态按 fail-closed ordering恢复。
4. Metadata version/digest 是控制面 publication binding，不是普通 Add 的远程或永久检查条件。
5. 初始创建先 durable claim all-E Profile inactive route，再创建标准 LedgerMetadata并发布 READY。
6. READY authorization 先于任何 local normal ACTIVE。
7. create/open 正常成功晚于 all-E durable activation。
8. 写期 replacement 顺序锁为 install inactive → `LAC+1` CAS → normal activate → resend。
9. 写期 replacement 不复制历史 fragment。
10. CAS/activation response loss必须通过 exact authority reread解析，不得盲目换 target。
11. Fence/tombstone 不能被迟到 activation重新打开。
12. AutoRecovery 在写 target payload 前必须有 durable instance-specific repair intent。
13. Recovery Add 使用 recovery-only authority，不要求或授予 normal writable activation。
14. AutoRecovery CAS 后通常进入 committed/readable replica，而不是 normal ACTIVE。
15. Delete freeze 必须覆盖 incomplete、completed、aborted-but-dirty repair intents 的 source/target。
16. 所有新增 metadata/control durability均是 operation级，不是 entry级。

## 只能保留为 OPEN 的内容

- exact sidecar path、record schema、field numbers和状态名；
- ledgerId reuse最终策略，以及 backlink exact encoding；
- activation proof/certificate、protected credential和 Add request exact fields；
- profiled LedgerMetadata mutation 的 ACL/credential enforcement；
- receipt保存、压缩、completion bit与审计布局；
- sidecar child record enumeration/watermark和 compaction；
- activation/recovery-only local record packing与 group-commit方式；
- CAS→activation gap的 exact error code、retry/backoff；
- orphan install/possibly-activated target 的 GC state machine；
- BatchRecoveryAdd/wire schema和 batch limits；
- failover latency、metadata contention、Classic throughput/p99的具体预算；
- partial activation期间是否通过 credential distribution进一步禁止 pre-return write。

## 下一轮被解锁的 frontier

只列问题，不在本轮展开：

1. activation/recovery-only proof 与 profiled metadata mutation authority；
2. sidecar root/child schema、instance ABA 与 ledgerId reuse；
3. repair history retention、delete freeze watermark与 orphan GC；
4. normal-active、recovery-only、committed-readable 的本地角色状态机及 error mapping；
5. 对应 Spike A/Model D 的 fault matrix与性能预算。

## 过度设计与性能判定

应明确拒绝：

- 把全部 lifecycle/receipts/repair history塞进 LedgerMetadata custom metadata；
- 一个无界增长的 sidecar root blob；
- 两个 metadata node 的通用事务系统；
- 普通 Add 远程读 MetadataStore；
- per-entry proof heavy verification、control fsync或 metadata update；
- Q3 写期换组复制历史 fragment；
- Q4 对 closed/historical repair target无条件授予 normal ACTIVE；
- 在 MetadataStore 持久化每 entry recovery progress。

经上述修订后，推荐设计不会实质损耗 steady-state Add 性能。主要新增成本集中于 create、failure replacement、repair 和 delete 的冷路径；其中 post-CAS activation会增加换组恢复尾延迟，repair intent会增加每 fragment 的 metadata 操作数，必须在 Spike 中单独测量，但它们不是正常写入热路径回退。

<!-- END VERBATIM REVIEW -->

## Integration Audit 1 完整回复

<!-- BEGIN VERBATIM INTEGRATION AUDIT 1 -->

## Round 2 Integration Audit：FAIL

主体集成正确，但有 4 项必须修复后才能标记 `Reviewed / Integrated`、commit/push。

### 必须修复

1. **`customMetadata` 约束误伤 OSS 用户字段**

   [RFC-0001](docs/rfcs/unified-wal/RFC-0001-profile-capability-install.md:160) 当前写成：

   > `LedgerMetadata.customMetadata` 只允许保存一个小型 immutable backlink

   字面上会禁止现有用户自定义 metadata，属于未评审的兼容性破坏。Round 2 锁定的是：

   - Profile 自己占用的 reserved entry 只保存 immutable instance/sidecar backlink；
   - 不把完整 lifecycle、receipts、repair/delete history 塞进去；
   - 无关的现有 OSS/user custom metadata 不应被禁止。

   应把作用域收窄到“Profile-reserved customMetadata entry”。这也更准确地区分“允许小 backlink”与已拒绝的方案 B。

2. **Partial activation 窗口被写成了过强且内部不一致的合同**

   [RFC-0001](docs/rfcs/unified-wal/RFC-0001-profile-capability-install.md:181) 写成：

   > READY 可以早于部分 Bookie active；该窗口只能造成明确的 transient unavailability

   但同文件紧接着的 ACK 合同只要求 `READY + target local ACTIVE`。若部分 Bookie 已 active，某些 write set 可能达到合法 local success，甚至达到 AQ；all-E 只负责 gate create/open success。Round 2 完整结论也明确保留了这种可能性。

   在 exact credential distribution 仍 OPEN 时，不能把“所有 pre-return write 都失败”写成最终合同。应改为：

   - 已 active target 可能产生合法 local/write-set success；
   - 未 active target 明确 transient unavailable；
   - create/open normal success 仍晚于 all-E activation；
   - 是否通过 credential distribution 禁止所有 pre-return write 保持 OPEN。

   同时应把 [Spike A](docs/rfcs/unified-wal/spikes/SPIKE-A-profile-install.md:275) 的模糊 Gate：

   ```text
   pre-activation profiled Add accepted = 0
   ```

   收窄成：

   ```text
   profiled Add accepted without matching READY/local NORMAL_ACTIVE = 0
   ```

   否则它可被误读为禁止部分 activation 后的合法 success，与 A16 oracle 和 RFC ACK 合同冲突。

3. **Spike A / Spike C 缺少 profiled metadata mutation authority 的否证面**

   RFC-0001 已正确锁定：

   - master key 不授予 LedgerMetadata mutation；
   - profiled membership mutation 必须由 Profile-aware coordinator 或等价 ACL/fencing authority执行；
   - exact ACL/credential mechanism 保持 OPEN。

   但当前 Spike A/C 没有对应 manifest prerequisite、action、scenario 或 invariant。现有 old-client 场景只发 Add，不尝试修改标准 LedgerMetadata。这样 Spike/Model 仍可能在“legacy client 可直接 CAS ensemble”的配置下错误 PASS。

   不需要现在冻结 ACL schema，但至少应同步抽象测试面：

   - Spike A manifest 增加 `profiledMetadataMutationAuthority` 或等价项；
   - 增加 stale/legacy client 持 master key、但无 Profile metadata authority 时尝试 membership mutation 的场景；
   - oracle 要求 mutation 被拒绝，或该环境明确判定为不支持/FAIL，不能由 sidecar假装约束；
   - Spike C Model A 增加 mutation-authority state/action/invariant，例如 `ProfileMembershipMutationRequiresAuthority`。

4. **Round 2 新文档存在 trailing whitespace**

   `ROUND-02-control-plane-authority.md` 的完整内容确实未经删节保存，但新文件的若干空行带尾随空格。只读检查：

   ```text
   git diff --no-index --check /dev/null docs/rfcs/unified-wal/grill/ROUND-02-control-plane-authority.md
   ```

   报告第 `51, 60, 69, 92, 96, 108, 117, 128, 148` 行。提交前应只规范化空行 whitespace，不删改 reviewer 内容。

### 已通过的部分

- Round 2 reviewer 回复完整归档，未发现内容删节。
- README 正确保持 `Review Complete / Integration In Progress`。
- RFC-0001 已正确集成：
  - 标准 LedgerMetadata 唯一 OSS state/membership authority；
  - 独立、有界 sidecar；
  - ABA backlink/control generation；
  - 无通用跨节点事务；
  - route-first initial sequence；
  - READY-before-local-active、all-E-before-success；
  - 写期 `install → LAC+1 CAS → activate → resend`；
  - 无历史 copy；
  - CAS/activation response loss、fence 与 orphan边界。
- RFC-0004 的 RepairIntent identity、owner、retention、first-payload invariant、delete freeze 串行和 operation级成本均准确。
- RFC-0005 已正确分离 `NORMAL_ACTIVE`、`RECOVERY_ONLY`、`COMMITTED_REPLICA_OR_READABLE`；closed/historical repair target没有被 normal-active。
- Spike A/C 和 turbo 文档已移除旧的写期 copy、OPEN publication、未发布 target discovery歧义。
- exact path/schema/proof/ACL mechanism/error/packing/retention topology/thresholds仍保持 OPEN。
- 未新增 steady-state Add 远程 metadata I/O、逐请求重验证、per-entry metadata/control fsync、全局锁或无界 root blob。

### 可后置项

- exact ACL/credential、sidecar child topology、activation proof、error mapping、record packing、retention watermark和性能阈值继续留待后续 frontier。
- Post-CAS activation 的 failover stall/p99，以及大量小 fragment 下 RepairIntent CAS 放大，留给 Spike benchmark。

修复上述 4 项后，应由同一 reviewer 再做一次短 integration audit；当前还不能把 Round 2 标为 `Reviewed / Integrated`，也不应 commit/push。

<!-- END VERBATIM INTEGRATION AUDIT 1 -->

## Integration Audit 2 完整回复

<!-- BEGIN VERBATIM INTEGRATION AUDIT 2 -->

## Round 2 Integration Audit 2：PASS

基线与发布边界已核对：

- 当前 `HEAD` 为 `cc7e03c81571b01b7f9da97a4c9bf1154d743c1c`。
- 与 `origin/turbo-bk` 为 `0 / 0`，当前没有分叉。
- 本轮仅涉及 7 个已跟踪文档和新增的 Round 2 grill 文档。
- 未跟踪的 `BtrLog Low-Latency Logging.pdf` 与本轮无关，commit 时必须排除。

### 四项必修修复均通过

1. **Profile-reserved customMetadata 作用域：PASS**

   RFC-0001 已明确：

   - 只有 Profile 占用的 reserved entry 被限制为 immutable instance/sidecar backlink；
   - 完整 descriptor、receipt、repair/delete history 不进入该 entry；
   - 其他 OSS/user custom metadata 明确不受禁止；
   - sidecar root 不能无界增长，repair/delete 可使用有界 child record。

   这准确修复了兼容性误伤，也没有退回把 lifecycle 塞进 LedgerMetadata 的方案 B。

2. **Partial activation 语义：PASS**

   RFC-0001 现已一致表达：

   - READY 必须早于任何 local normal activation；
   - 已 durable active 的 target/write set 可以形成合法 local success 或 AQ；
   - 未 active target 返回明确 transient unavailable；
   - all-E activation 只 gate create/open 的正常成功；
   - 是否通过 pre-return credential distribution 禁止所有提前写入继续保持 OPEN。

   Spike A 的 A16、硬 Gate 和立即停止条件也已统一成：

   ```text
   profiled Add accepted without matching READY/local NORMAL_ACTIVE = 0
   ```

   因此不再错误禁止 partial activation 后满足局部 authority 的合法成功。

3. **Profile metadata mutation authority：PASS**

   Spike A 已补齐：

   - manifest prerequisite：`profiledMetadataMutationAuthority`；
   - prototype abstraction；
   - A20 stale/legacy metadata mutation 场景；
   - fuzz action；
   - 硬 Gate 与停止条件。

   A20 正确要求：持有 master key 但没有 Profile metadata authority 的客户端，不能在 CAS 生效前修改 profiled membership 或 immutable backlink；若 metadata driver/ACL 无法提供等价 enforcement，该组合必须判为 unsupported/FAIL，不能让 sidecar 事后“修复”。

   Spike C Model A 也已加入对应 state、action、check 和 `ProfileMembershipMutationRequiresAuthority` invariant。exact ACL/credential encoding 仍保持 OPEN，没有被提前设计成某个固定实现。

4. **Round 2 归档与 whitespace：PASS**

   - 原始 Round 2 reviewer 回复仍完整保留在 verbatim markers 内。
   - Integration Audit 1 全文已完整追加，四项失败原因、已通过项和可后置项均未丢失。
   - `git diff --check` 无诊断。
   - 对新增 Round 2 文件执行 `git diff --no-index --check` 无 whitespace 诊断。
   - 12 个相关 Markdown 文件的单一 H1、fence 配对、链接解析和 verbatim marker 检查通过。
   - README 当前仍保持 `Review Complete / Integration In Progress`，符合本次 Audit 2 完成前的状态要求。

### 新冲突、过度设计与性能审查

未发现新的规范冲突或复杂度膨胀：

- 标准 LedgerMetadata 仍是 OSS state/membership 的唯一 authority。
- Profile sidecar 只承担 instance、READY/control generation 和有界 operation authority，不复制 membership。
- 没有引入两个 metadata node 的通用事务、全局锁或 Add-time lease。
- initial publication 仍为 all-E inactive route claim → standard metadata → READY → local activation → all-E availability。
- write-time replacement 仍为 inactive install → `LAC+1` CAS → normal activation → resend，不复制历史 fragment。
- AutoRecovery 仍要求第一份 target payload 前存在 durable RepairIntent。
- `NORMAL_ACTIVE`、`RECOVERY_ONLY`、`COMMITTED_REPLICA_OR_READABLE` 角色没有重新混合；closed/historical target 不会被自动 normal-active。
- Delete freeze 仍覆盖 incomplete、completed、aborted-but-dirty RepairIntent。
- 没有新增 steady-state Add 远程 MetadataStore I/O、逐请求重型 proof、per-entry metadata update、per-entry control fsync 或无界 receipt/history blob。

新增 authority 检查只存在于创建、metadata mutation、replacement、repair 和 delete 等冷控制路径。主要性能风险仍是：

- post-CAS activation 增加 failure replacement stall/p99；
- 大量小 fragment 时 RepairIntent CAS 数量放大；
- mutation-authority enforcement 对不同 metadata driver 的可用性和控制面延迟。

这些风险均已正确留给 Spike/benchmark，没有通过增加 Add 热路径成本来解决。

### 仍只能保持 OPEN

以下内容没有被误写成最终合同：

- sidecar exact path、schema、field numbers、状态名和 backlink encoding；
- activation proof/certificate、protected credential 和 Add exact binding fields；
- profiled metadata mutation 的 exact ACL/credential enforcement；
- receipt packing、保存与审计布局；
- sidecar child topology、enumeration、watermark 和 compaction；
- activation/recovery-only local record packing 与 group commit；
- CAS→activation exact error、retry 和 backoff；
- partial-activation credential distribution；
- orphan/possibly-activated target GC；
- BatchRecoveryAdd wire schema 和 batch limits；
- retention topology、failover latency、metadata contention及 throughput/p99 数值预算。

## 最终判定

**PASS。**

可以按以下顺序收口：

1. 将本次 Integration Audit 2 回复完整、未经删节追加到 `ROUND-02-control-plane-authority.md`。
2. 将 README 中 Round 2 状态改为 `Reviewed / Integrated`。
3. 再执行一次 scoped diff、Markdown 和 whitespace 校验。
4. 只暂存本轮 8 个文档，明确排除 `BtrLog Low-Latency Logging.pdf`。
5. commit 并 fast-forward push `turbo-bk`。

完成上述归档后，可以进入下一轮 grill。

<!-- END VERBATIM INTEGRATION AUDIT 2 -->
