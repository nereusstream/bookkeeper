# Round 1：P0 根合同

> Reviewer：固定 `gpt-5.6-sol / max` agent<br>
> 源码与文档基线：`3a476baa6005881e34de3ac876a4f4294487d645`<br>
> 评审模式：正确性、热路径性能、复杂度和过度设计联合审查<br>
> 记录规则：以下 reviewer 回复未经删节；规范是否采纳以对应 RFC 的集成结果为准。

## 本轮 Questions

### Q1：READY 的安全边界

选择信任 coordinator/client 的 READY 时序，还是要求 Bookie 面对持有合法 master key 的 stale/legacy client 也能在 READY 前 fail closed。推荐后者，同时保持普通 Add 无逐请求 MetadataStore I/O。

### Q2：Legacy Add 的路由规则

决定 Profile routing 已存在时，旧 `ADDENTRY/ADD_ENTRY` 是否必须在 Classic lazy-create 前拒绝，以及 Classic/Profile 首次创建如何避免并发双重认领。

### Q3：Descriptor 与操作身份

决定 `installRequestId`、credential material、runtime limits 是否从 immutable descriptor/hash 中拆出，以及哪些字段可安全进入 semantic contract。

### Q4：WalSequence hole 合同

选择 published sequence 是否无洞，并冻结 predecessor fence/recovery/seal 后 successor 的起点以及 BookKeeper AQ 与上层 commit 的关系。

### Q5：Segment Bookie durable state 的 RFC owner

选择继续扩展 allocator RFC，还是新增独立 RFC 负责 Segment Bookie operation、durability 与 ACK authority。

### Q6：永久介质失败预算

为 `AckedEntryRecoverable` 定义 process crash、permanent loss、failure-domain 和 repair-window 前提，避免无对象存储条件下的无条件恢复承诺。

## Reviewer 完整回复

<!-- BEGIN VERBATIM REVIEW -->

本轮结论：

```text
Q1 = LOCK：接受 B 的安全边界；activation 的具体状态/证明机制保持 OPEN
Q2 = REVISE → 可 LOCK：接受 legacy fail-closed，但必须补“原子 routing claim”；具体 opcode/error code 保持 OPEN
Q3 = REVISE → 可 LOCK：接受 descriptor / operation / runtime 三分；拒绝现在就锁 masterKeyDigest、keyVersion 与泛化 safety ceilings
Q4 = REVISE → 可 LOCK：接受 published WalSequence 无洞与 successor=P+1；必须先拆开 BookKeeper AQ 和 WAL COMMITTED
Q5 = LOCK：新增 RFC-0005，但只负责 Segment Bookie 行为与 durability，不重复 RFC-0001/0003/0004
Q6 = REVISE → 可 LOCK：接受参数化永久损失预算；A>F 只是必要条件，还必须绑定 distinct failure domains 和 repair window
```

以下完整反馈都应进入本轮 review 记录；只有明确标为可 LOCK 的内容进入规范合同。

## Q1 — READY 安全边界

**结论：LOCK B 的安全边界；具体实现 OPEN。**

仓库事实确认：

- RFC-0001 要求 OPEN CAS 成功后才能 Add，同时普通 Add 不读 MetadataStore。
- Bookie 设想中的 durable 状态只有 install；all-E install 完成但 OPEN CAS 失败，与 OPEN 成功对 Bookie 本地不可区分。
- RFC 又要求 orphan install 不得接受 Add，因此当前合同自相矛盾。
- `activationEpoch` 字段本身不是 proof；持有 master key、instance/hash 的客户端也能照抄该字段。

可立即锁定：

```text
Threat model:
- MetadataStore / activation authority 非 Byzantine；
- coordinator 可能 crash、retry、response loss；
- 任何客户端，包括持有合法 master key 的 stale/legacy client，
  都不被信任为会遵守 READY 时序；
- Bookie 在缺少独立 activation authority 时必须 fail closed。

ACK(profiled Add)
    => matching durable install
    && matching durable activation existed before Add processing
```

性能边界也应锁定：

- 普通 Add 不做远程 metadata 读取。
- 普通 Add 不做每请求证书签名验证。
- 热路径只允许本地 routing/state lookup、instance/hash/activation identity 比较。
- activation 是 ledger 创建/replacement 的冷路径，可 group commit；不应规定每个 ledger 单独 fsync。

不能现在锁定：

- `ACTIVE(activationEpoch, readyMetadataVersion)` 的精确字段。
- `OPEN / ACTIVATING / READY` 的具体状态数。
- Bookie 是冷路径读取一次 MetadataStore、验证可缓存 certificate，还是消费其他 authority receipt。
- replacement 激活使用 initial OPEN proof 还是 membership-specific proof。

必须新增 Spike A 场景：

- all-E installed、OPEN CAS 尚未提交时，由合法 master-key client 发 Add；
- OPEN CAS 确定失败后发 Add；
- activation durable 前后 crash/response loss；
- old instance、wrong hash、stale activation proof；
- restart 后不能放大 activation 接受集合；
- 验证普通 Add 没有 metadata I/O/逐请求密码学成本。

## Q2 — Legacy Add 路由

**结论：REVISE；补足原子 claim 后语义可 LOCK。**

仓库事实确认：

- v2 `ADDENTRY` 和 v3 `ADD_ENTRY` 都可走 normal/recovery Add。
- 两条路径最终进入 `BookieImpl.addEntry/recoveryAddEntry`。
- `HandleFactoryImpl.getHandle()` 只按 `ledgerId + masterKey` 懒创建 Classic descriptor。
- 如果只在新 opcode handler 校验 Profile，legacy normal Add、legacy recovery Add 均可绕过。
- 当前 processor 还会把多数新增 `BookieException` 压成 `EUA`，新错误码需要完整贯通。

原提案缺少一个 P0 条件：**routing 查询与 Classic/Profile 创建必须是同一个原子 claim**。否则：

```text
legacy Add:  observe ABSENT
profile install: observe ABSENT
legacy Add creates CLASSIC
profile install persists PROFILE
```

仍可能产生混合身份。

可锁定的修正版：

```text
每个 ledgerId 在 Bookie 上只有一个 authoritative route slot：

ABSENT
CLASSIC
PROFILE(instanceId, descriptorHash, lifecycleState)
TOMBSTONED(instanceId)

legacy Add:
    CLASSIC -> existing Classic path
    ABSENT  -> atomically claim CLASSIC, then lazy-create
    PROFILE/TOMBSTONED -> reject before Classic handle/storage creation

profile install:
    ABSENT -> atomically and durably claim PROFILE/RESERVED
    CLASSIC -> conflict
    matching PROFILE -> idempotent continuation
    conflicting PROFILE/TOMBSTONED -> reject
```

其他必须条件：

- normal 和 `RECOVERY_ADD` legacy 变体都受相同 routing gate。
- restart 必须在注册 writable 前恢复 authoritative route。
- legacy 抢先成功时，后续 Profile install 只能冲突失败，不能升级或混用。
- tombstone 不一定返回 `EPROFILE_REQUIRED`；更准确的 deleted/fenced error 可后定，但必须 fail closed。
- Classic 热路径至多增加一次可与 handle lookup 合并的内存查询，不能增加磁盘或 metadata I/O。

具体 wire encoding 保持 OPEN。独立 `PROFILED_ADD_ENTRY` 是当前最简单的候选，因为旧 Bookie 会拒绝未知 operation；也可使用保证旧服务器 fail closed 的 incompatible protocol version。不能只增加会被旧 protobuf 实现忽略的 optional fields。

Spike A 应增加：

- legacy v2/v3 normal Add targeting RESERVED/installed/active/tombstoned ledger；
- legacy recovery Add；
- install 与首次 Classic Add 的并发、各 durability 边界 crash；
- 断言未创建 Classic handle、未写 master key、未写 payload；
- Classic-only throughput/p99 回归与 Profile Add CPU 成本。

## Q3 — Descriptor、操作与 runtime policy

**结论：REVISE；三分原则可 LOCK，具体字段集不能照原提案锁定。**

当前矛盾成立：

- `installRequestId` 在 descriptor 内且被 hash 覆盖；
- 同时又允许相同 hash、不同 request；
- 两者不可能同时成立。

可锁定：

```text
ProfileDescriptor
    只包含 immutable semantic contract
    canonical serialization
    descriptorHash

InstallOperation
    installRequestId
    descriptorHash
    authorization/request correlation
    protected credential or proof

RuntimePolicy
    可在线调整的 admission/rate/resource limits
    不改变已持久化 payload 的解释、durability 或 recovery
```

应进一步简化幂等实现：

- durable install 的语义身份由 `ledgerId + instanceId + descriptorHash + protected auth binding` 决定。
- retry request ID 可在响应中 echo，不应为了支持任意新 request ID 建立无限 durable idempotency 表。
- 相同语义的新 request 可返回原 install generation；相同 request ID 携带冲突内容必须失败。

原提案中这些内容不应现在锁定：

1. **公开 `masterKeyDigest` 有安全风险。**

   当前 BookKeeper master key 本身是从 password 生成的 20-byte verifier，并直接随 Add 发送。把它或可离线验证的派生值放进 metadata descriptor，可能新增密码猜测/凭据验证面。应先定义受保护的 `authBinding`，可使用本地受保护 verifier、cluster-keyed HMAC 或 opaque binding；具体方案需安全评审。

2. **不要为了尚未存在的 key rotation 引入泛化 `keyVersionSemantics`。**

   首版若不支持 key rotation，应明确 immutable credential binding、变更即拒绝。KMS reference 与 key version 留作开放项。

3. **不是所有 limit 都是 descriptor safety ceiling。**

   - payload 编码、解析或恢复正确性依赖的 `maxEntrySize` 可以进入 contract。
   - `maxInflightEntries/maxInflightBytes` 通常是客户端/Bookie runtime admission policy，不应因调参改变 descriptor hash。
   - 只有确实影响跨实现安全解释的上限才进入 hash；不要创建泛化“所有 runtime limit 都必须小于 descriptor ceiling”的复杂层。

需验证：

- canonical hash golden vectors、unknown fields、default values；
- credential/reference 改变不意外改变 semantic descriptor；
- 同语义不同 request 的 restart 幂等；
- runtime policy 调整不触发 reinstall；
- protected auth binding 不出现在普通日志、receipt dump 或公开 metadata。

## Q4 — WalSequence hole

**结论：REVISE；无洞发布合同可 LOCK，但必须重写 AQ/COMMITTED 语义。**

RFC 已把 `SequenceDomain` 定义为连续空间，且现有 BookKeeper client 本身按 entryId 顺序发送成功 callback。因此首版锁定 published WalSequence 无洞是合理的，不会凭空引入一种此前完全不存在的顺序等待。

必须先修正当前冲突：

```text
BookKeeper AQ/durable evidence != Sequenced WAL COMMITTED
```

否则“fence 前 AQ 永不撤销”和“suffix 未进入连续 frontier 可被 suppress/reuse”无法同时成立。

可锁定：

```text
AQ_CANDIDATE:
    底层 BookKeeper 已形成 durable quorum evidence，
    但尚未必成为上层可见 WAL commit。

WAL COMMITTED:
    entry/batch 有有效 AQ evidence
    && 所有更早 sequence 已 committed
    && published frontier 已覆盖该 interval
    && 成功 receipt 已按该顺序发布。
```

takeover：

```text
fence predecessor
→ recover maximum contiguous committable prefix P
→ durably seal predecessor at P
→ permanently suppress every predecessor suffix interval > P
→ publish successor start = P + 1
```

附加约束：

- suffix 即使曾取得 AQ，只要未进入 published frontier，就不能被称为 WAL COMMITTED。
- late BookKeeper callback 只能更新底层证据，不能越过已 durable seal 重新发布。
- sequence 只有在 predecessor seal 已 durable、successor authority 已绑定后才能复用。
- physical old suffix 可以暂时保留，但 read/recovery authority 必须忽略它。
- 对外 sequence receipt 在 COMMITTED 前不能暴露为最终成功身份。

性能取向：

- 首版保持 single active writer。
- sequence 可在 writer 内按 bounded in-flight window 本地分配；普通 append 不做 MetadataStore range allocation。
- “small”不是规范常量，窗口大小由内存、HOL latency 和吞吐 benchmark 决定。
- 必须测量早期慢 entry 导致的可见性阻塞、被 suppress 的 AQ bytes、takeover scan 上限。

仍只能 OPEN：

- appendId 在 suppressed suffix 中是 retry、ABORTED 还是终态查询；
- appendId retention/horizon；
- footer/等价 seal authority 的位置；
- run header/footer 是否占普通 entryId；
- control entry 与 DATA 的区分；
- exact in-flight window；
- future 是否允许 durable range allocator。

当前“fence 前 AQ 的合法结果不被撤销”应改为：AQ evidence 的物理事实不被伪造或删除，但只有 sealed continuous prefix 内的结果能成为 WAL COMMITTED。

## Q5 — Segment Bookie state RFC owner

**结论：LOCK B，新增 RFC-0005。**

理由：

- RFC-0003 明确不负责 BookKeeper quorum ACK、ensemble change、AutoRecovery 总体协议，并明确只解锁 shadow writer。
- 当前 Classic 路径具有 master key、fence、explicit LAC 的 durable/replay 行为。
- normal Add 检查 fence；`recoveryAdd` 是绕过 fence 检查的正常数据写路径，不是一种独立 control record。
- 当前 Bookie 没有本地 durable `CLOSED` record，不能为了表面对称发明一个。

RFC-0005 建议定位为：

```text
Segment Bookie State, Operation Semantics and ACK Authority
```

它负责 Segment engine 对 Bookie 外部行为的等价实现与 durability/linearization：

- ledger instance/profile/engine routing bind；
- protected authentication binding；
- activation gate；
- durable fence 的线性化与 restart；
- normal Add 与 authorized recovery Add；
- explicit LAC；
- local delete/tombstone 消费；
- read/LAC/list 等必须支持或明确 capability-reject 的操作；
- restart/replay；
- unknown/newer format、upgrade/downgrade fail closed；
- local success 何时可参与 BookKeeper AQ。

边界：

- RFC-0001 继续拥有 Profile lifecycle、安装和 activation authority。
- RFC-0003 拥有 allocator、payload framing、physical relocation encoding。
- RFC-0004 拥有 cluster delete/recovery target authority。
- RFC-0005 只规定这些 authority 在 Segment Bookie 上如何被消费和线性化，不复制其 schema。

性能约束：

- 不要求“一种语义一条 record”。
- normal Add 不新增 per-entry control-log fsync。
- fence、activation、delete 等冷控制操作可以 durable/group commit。
- recovery Add 使用数据路径与明确授权，不创建无意义的 `RECOVERY_ADD` control record。
- exact record packing/layout 必须由 Spike 结果决定。

RFC-0005 应增加 fence/Add 并发、fence response loss、restart、recovery Add、explicit LAC、unknown control record 和 downgrade 场景。

## Q6 — 永久介质失败预算

**结论：REVISE；收紧后可 LOCK。**

核心方向正确，但 `A >= F + 1` 单独不足以表达保证。必须说明 ACK 来自不同的永久失败域，并限定损失发生在两次已证明 repair 之间。

可锁定：

```text
ProcessCrash:
    volatile state lost;
    durable media retained.

PermanentFailureDomainLoss:
    one declared placement failure domain's durable evidence
    is permanently unavailable.

For profile-declared budget F:
    every successful ACK must contain durable acknowledgements from
    at least F + 1 distinct declared permanent-failure domains.

losses since the last proven repair/re-replication <= F
    => at least one valid durable payload/identity evidence survives.
```

因此：

- `A >= F + 1` 是必要条件；
- ACK set 覆盖 `F+1` 个独立 failure domains 才是足够条件；
- correlated device/rack loss 必须按实际声明的 failure unit 计数；
- detected corruption 不能算 surviving evidence，应按 unavailable/lost 处理；
- repair 只有在新副本 durable 且 authority 已发布后才能重置预算。

性质拆分：

```text
NoFabricatedRecovery:
    unconditional safety;
    recovery without valid evidence never returns success.

AckedPayloadSurvivesWithinBudget:
    ACK && lossesWithinBudget
    => exists valid durable evidence.

EvidenceExhausted:
    explicit UNRECOVERABLE / DATA_LOSS
    != successful recovery.
```

边界：

- 该性质只证明 payload evidence survival。
- 不证明继续写可用性、read quorum 可用性、metadata/auth authority 生存或 general E/W/A recovery liveness。
- metadata/auth authority 必须有独立 failure budget。
- Profile 可以声明 `F=0`；不要无条件提高所有部署的 A。
- 提高 A 或强制 fault-domain 覆盖会增加网络、尾延迟和降级时等待，这是宣称永久损失预算的真实成本，不能隐藏。当前 client 已有按 placement fault domain 延迟 ACK 的机制，可作为实现参考，但仍需 profile-specific benchmark。

Spike C / fault tests 应覆盖：

- `F < A` positive；
- `F = A` evidence exhausted；
- 多个 ACK 在同一 correlated domain 的负向场景；
- crash 与永久 loss 分离；
- corruption；
- loss、repair、再次 loss；
- evidence 全失时只能明确 DATA_LOSS。

## 可立即同步的合同

1. 不信任所有持有 master key 的客户端遵守 READY；profiled Add 必须有 Bookie 本地 durable activation gate。
2. 普通 Add 热路径不得访问 MetadataStore或逐请求验证重型 proof。
3. Classic/Profile route 必须是单一、原子、可恢复的本地 claim；legacy Add 不能绕过。
4. Descriptor 只 hash immutable semantics；request identity、credential material、runtime admission policy 分离。
5. Published WalSequence 无洞；AQ candidate 与 WAL COMMITTED 分离；seal P 后 successor 从 P+1 开始。
6. 新增 RFC-0005，负责 Segment Bookie operation/durability/ACK authority。
7. 永久损失保证按 distinct failure domains 和 repair window 参数化；无证据时永不伪造恢复成功。

## 只能保留为开放项

- metadata namespace、OPEN/ACTIVATING/READY 状态与 activation proof 机制；
- initial activation 与 replacement activation 的 publication 顺序；
- profiled wire opcode/version 和精确错误码；
- auth binding、KMS/key rotation；
- runtime limit 分类的最终字段表；
- appendId suppression/horizon、footer 和 control-entry encoding；
- Segment exact durable record schema、packing、relocation authority；
- production failure-domain 定义、默认 F、general E/W/A 可用性模型；
- 所有性能阈值，必须以 Classic baseline 和 Spike 数据冻结。

## 下一轮被解锁的问题

- RFC-0001 metadata namespace 与 activation publication；
- initial creation、active write-time ensemble change、AutoRecovery repair 三种激活/替换顺序；
- replacement 在 metadata publication 前后如何获得 active authority；
- Segment relocation 的 durable source-of-truth 与 crash recovery；
- repair/placement intent 如何进入 delete target discovery；
- Q4 下游的 appendId horizon、seal/footer authority；
- Q6 下游的 general E/W/A recovery 与 permanent-loss/repair 状态机。

只读复核结束：HEAD 仍为 `3a476baa6005881e34de3ac876a4f4294487d645`，工作区没有新增修改；原有未跟踪 `BtrLog Low-Latency Logging.pdf` 未触碰。

<!-- END VERBATIM REVIEW -->

## Integration Audit 1 完整回复

<!-- BEGIN VERBATIM INTEGRATION AUDIT 1 -->

FAIL：Round 1 integration 还需修复 4 个合同问题和 1 个记录状态问题。

## 必须修复

1. **仍属 OPEN 的 `activationBinding` 被锁成每次 Add 的固定字段。**

   [RFC-0001](/Users/liusinan/apps/ideaproject/nereusstream/bookkeeper/docs/rfcs/unified-wal/RFC-0001-profile-capability-install.md:19)、[AddRequest schema](/Users/liusinan/apps/ideaproject/nereusstream/bookkeeper/docs/rfcs/unified-wal/RFC-0001-profile-capability-install.md:291) 和 [Spike A](/Users/liusinan/apps/ideaproject/nereusstream/bookkeeper/docs/rfcs/unified-wal/spikes/SPIKE-A-profile-install.md:71) 都要求每次 Add 携带并比较 `activationBinding`。

   Round 1 锁定的是：

   ```text
   ACK(profiled Add)
       => matching durable activation existed
   ```

   并未锁定独立 activation 字段必须随每次 Add 发送。activation 可能已由 instance/hash、本地 ACTIVE route、session 或未来 certificate 机制充分绑定。应改成抽象的“请求身份必须足以匹配 durable activation”，把 exact binding fields 留在 OPEN。

   同时，`protectedCredentialOrProof` 与“credential/proof 均不得进入公开 metadata”混淆了秘密 credential 和可能公开验证的 certificate。只应禁止 secret/offline-verifiable credential material 泄漏；proof 的可见性由后续机制决定。

2. **`WAL_COMMITTED` 与 receipt 形成循环，并破坏 response-loss 语义。**

   [RFC-0002](/Users/liusinan/apps/ideaproject/nereusstream/bookkeeper/docs/rfcs/unified-wal/RFC-0002-sequenced-wal.md:101) 定义：

   ```text
   WAL_COMMITTED => final receipt 已发布
   ```

   紧接着又规定：

   ```text
   final receipt 只在 WAL_COMMITTED 后生成
   ```

   这是循环。commit 也不能依赖调用方是否收到 response，否则 durable commit 丢 response 后无法表达“已提交但客户端未知”。

   必须改成：

   ```text
   WAL_COMMITTED
       = valid AQ evidence
       + all prior sequence committed
       + authoritative published frontier includes interval

   only then:
       final receipt becomes eligible to emit
   ```

   receipt 丢失不撤销 commit。Spike C 已把 `PublishWalCommitted` 与 `DeliverOrLoseCompletion` 分成两个动作，RFC 应与之对齐。

3. **RFC-0005 的硬依赖、解锁声明与实施阶段互相矛盾。**

   [RFC-0005](/Users/liusinan/apps/ideaproject/nereusstream/bookkeeper/docs/rfcs/unified-wal/RFC-0005-segment-bookie-state.md:4) 把 RFC-0004 列为依赖，并声称解锁 Segment ACK canary；但 [总体状态表](/Users/liusinan/apps/ideaproject/nereusstream/bookkeeper/docs/turbo-bk.md:171) 的 Segment Gate 不含 RFC-0004，[Stage 6/7](/Users/liusinan/apps/ideaproject/nereusstream/bookkeeper/docs/turbo-bk.md:258) 又先做 Segment authority canary、后接受 RFC-0004。

   本轮无需提前解决 replacement/recovery 设计，但必须消除当前承诺冲突。最小处理是明确：

   - RFC-0005 是 ACK canary 的必要但不充分前置；
   - recovery/delete integration 对 RFC-0004 的依赖仍开放；
   - 在依赖闭合前不能从“RFC-0005 Accepted”直接推导 canary 可执行。

4. **RFC-0005 的 relocation owner 表述有重叠。**

   owner 表把 physical relocation encoding 归 RFC-0003，但 [开放问题](/Users/liusinan/apps/ideaproject/nereusstream/bookkeeper/docs/rfcs/unified-wal/RFC-0005-segment-bookie-state.md:181) 又写成 RFC-0005 的“compaction relocation authority”。

   应明确单一 owner：

   - RFC-0003：durable relocation authority/protocol、physical encoding；
   - RFC-0005：消费 relocation authority后的 operation ordering、reader cutover 和 local-success 影响。

## 记录状态必须修复

[grill/ROUND-01](/Users/liusinan/apps/ideaproject/nereusstream/bookkeeper/docs/rfcs/unified-wal/grill/ROUND-01-root-contracts.md:34) 已完整、未经删节保存首轮 reviewer 回复，这项 PASS。

但本次 integration audit 也是该固定 reviewer 的反馈，按“不丢失任何反馈”规则也应完整保存。修复并重新审计通过前，[grill README](/Users/liusinan/apps/ideaproject/nereusstream/bookkeeper/docs/rfcs/unified-wal/grill/README.md:16) 不应提前标记 `Reviewed / Integrated`。

## 已通过部分

- Q2 的单一原子 route claim、legacy normal/recovery Add fail-closed 已准确进入 RFC 和 Spike。
- Q3 的 descriptor/operation/runtime 三分、避免无界 request-id 表、master-key digest 风险均已正确集成。
- Q4 的 published sequence 无洞、AQ candidate、seal `P`、successor `P+1` 与 suffix suppression 除 receipt 循环外均正确。
- Q6 的 distinct failure domains、repair window、`NoFabricatedRecovery` 和超预算 DATA_LOSS 已正确进入 Spike C。
- 未新增普通 Add 的远程 metadata I/O、逐请求重型 proof、per-entry control fsync 或全局锁合同。
- RFC-0005 没有发明 `RECOVERY_ADD`/`CLOSED` 对称记录，exact record/packing/group commit 仍正确保持 OPEN。

## 可后置项

- metadata namespace、activation proof/state schema；
- profiled opcode/error mapping；
- auth binding、KMS/key rotation；
- exact runtime-limit 字段；
- appendId horizon、footer/seal 编码、exact window；
- Segment record packing、relocation协议细节；
- failure-domain 默认值、性能阈值；
- Spike C 的明确 repair-completion action，可在 replacement/repair 合同冻结后补齐。

本次只读 audit 未修改文件；原未跟踪 PDF 未触碰。

<!-- END VERBATIM INTEGRATION AUDIT 1 -->

## Integration Audit 2 完整回复

<!-- BEGIN VERBATIM INTEGRATION AUDIT 2 -->

PASS：Round 1 Integration Audit 2 通过。

## 四个必修项

1. **Activation 字段边界：PASS**

   - RFC-0001 不再要求每次 Add 携带固定 `activationBinding`。
   - 锁定的是“请求身份必须足以匹配 durable activation”。
   - 是否需要额外 proof/certificate、exact fields 和 wire encoding 均明确保持 OPEN。
   - Spike A 已同步采用相同抽象。
   - secret/offline-verifiable credential material 与可能公开的 proof/certificate 已在规范中区分。

   `protectedCredentialOrProof` 仍出现在候选 install schema 中，但最终字段表明确未冻结，且公开性由后续机制决定；这是命名上的可后置清理，不构成合同冲突。

2. **WAL commit 与 receipt：PASS**

   - `WAL_COMMITTED` 现在只依赖有效 AQ evidence、此前 sequence committed 和 authoritative published frontier。
   - receipt 仅在 commit 后获得发送资格。
   - response/receipt loss 不撤销 commit，调用方可观察 `OUTCOME_UNKNOWN` 后通过 frontier/appendId 解析。
   - 与 Spike C 的 `PublishWalCommitted`、`DeliverOrLoseCompletion` 两个独立动作一致。
   - 不再存在循环定义。

3. **RFC-0005 依赖和 canary Gate：PASS**

   - RFC-0005 硬依赖只保留 RFC-0001/0003。
   - 明确是 Segment ACK canary 的必要但不充分前置。
   - recovery/delete 对 RFC-0004 的 exact dependency 保持 OPEN。
   - Accepted 不会自动推导 canary 可执行。
   - turbo Stage 6 明确只完成 normal Add/fence local authority 必要合同；Stage 7 再接受 RFC-0004。
   - 实际启用路径仍需各自依赖和 canary evidence 闭合，没有提前提升 production readiness。

4. **Relocation owner：PASS**

   - RFC-0003 单独拥有 durable relocation authority、protocol 和 physical encoding。
   - RFC-0005 只负责消费该 authority 后的 operation ordering、reader cutover 和 local-success 影响。
   - 没有复制 schema，也没有把 derived locator 提升为 authority。

## 记录与状态

- Round 1 初始 reviewer 回复完整保留。
- Integration Audit 1 已全文、未经删节保存在 `ROUND-01-root-contracts.md`。
- 历史 FAIL 文本中的旧字段和旧 receipt 定义只存在于明确标记的 verbatim audit 记录中，不是现行规范合同。
- README 当前仍为 `Integration Review In Progress`，状态准确。
- 本次 Audit 2 回复也应按同一规则完整追加后，再把状态改为 `Reviewed / Integrated`。

## 性能与复杂度复核

没有发现新增热路径损耗或过度设计：

- 普通 Add 无远程 MetadataStore I/O。
- 无逐请求重型 proof/certificate 验证。
- 无 per-entry control-record fsync。
- route/activation/fence 是有界本地 lookup，可与 handle state 合并。
- 冷控制路径允许 group commit，但未锁具体 packing/fsync 参数。
- 没有要求一种语义对应一条 record。
- 没有新增全局锁合同。
- exact window、record layout、proof、opcode、错误码和性能阈值仍由后续 RFC/Spike 决定。

可以在完整归档本次 Audit 2 后，将 Round 1 标为 `Reviewed / Integrated`，进入本轮文档的 commit/push。无需第三次设计审查；归档 Audit 2 和状态更新属于机械性收尾。`git diff --check` 当前通过，未跟踪 PDF 仍应排除。

<!-- END VERBATIM INTEGRATION AUDIT 2 -->
