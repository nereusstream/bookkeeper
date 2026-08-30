# Round 5：Implementation Interface

> Reviewer：复用 Round 1–4 的固定 `gpt-5.6-sol / max` agent<br>
> 源码与文档基线：`ba820b28babd7683e8dfe6a724d1b8aeecf7c2e1`<br>
> 评审模式：实现接口正确性、热路径性能、复杂度和过度设计联合审查<br>
> 记录规则：以下 reviewer 回复未经删节；仅将 Markdown heading 统一下移一级并清理行尾空白，以保持文档单一 H1。规范是否采纳以对应 RFC 的集成结果为准。

## 本轮 Questions

### Q1：Sidecar bounded root/child/snapshot interface

冻结 domain-specific single-record CAS、store version 与 semantic generation、family head、verified snapshot + suffix、unknown mandatory version、ABA 与 publish-before-reclaim；禁止多键通用事务、universal ledger head、无界 root 和 Add-time sidecar dependency。

### Q2：ArenaControlLog conditional/durability/checkpoint/free/reader-cutover API

冻结 per-Arena conditional apply、operation identity、control sequence、durable-through、MOVE/free ordering、checkpoint publication 与 selector/pin synchronous cut；保留 group durability，禁止 per-record fsync/future、foreground barrier、full-chain retention 和 cross-Arena transaction。

### Q3：Segment Bookie local durable authority state

冻结 route/install/auth/activation/fence/recovery grant/readable/tombstone/assignment 的最小原子与有序关系、stale-handle generation 和 unknown/downgrade fail-closed；不提前选择统一物理日志或 flat role enum，normal Add 只增加 bounded local checks。

### Q4：Recovery outcome compatibility mapping

冻结内部 rich outcome、durable close-only legacy OK、其他语义 non-OK、admin/AutoRecovery 细分消费与 metrics 边界；不把现有首个 NoSuch、timeout、single corruption、authority loss 或 legacy aggregate OK 误当成功/数据丢失，exact enum/code/API 保持 OPEN。

## Reviewer 完整回复

<!-- BEGIN VERBATIM REVIEW -->

## Unified WAL P0 Grill Round 5：Implementation Interface 独立审查

### 总结论

基线核对为 `turbo-bk@ba820b28babd7683e8dfe6a724d1b8aeecf7c2e1`。工作区没有 tracked/staged 变更；唯一无关项为未跟踪的 `BtrLog Low-Latency Logging.pdf`，本轮未读取或触碰。

四题 verdict：

| 题目 | Verdict |
| --- | --- |
| Q1 Sidecar bounded root/child/snapshot | **REVISE 后可 LOCK** |
| Q2 ArenaControlLog API | **REVISE 后可 LOCK** |
| Q3 Segment Bookie local durable state | **REVISE 后可 LOCK** |
| Q4 Recovery outcome compatibility | **REVISE 后可 LOCK** |

没有发现需要新 RFC 或重开总体架构的 `BLOCK`。但四题都不能按“一个通用 root、一种通用事务、一个 flat local enum、直接复用现有整数 rc”的朴素实现进入编码。下面给出的收紧合同可以作为本轮 LOCK 内容；明确列为 OPEN 的部分不得写成最终格式合同。

### 真实源码边界

本轮实现建议依赖以下已核实事实：

1. `LedgerManager` 只提供单个标准 ledger metadata 的 create/read/versioned-write/remove/listener/range scan，不提供通用 child namespace、多键 CAS 或事务。
2. 旧 `MetastoreTable` 虽提供单 key versioned put/remove，但 `Version.ANY` 被源码明确警告可能破坏一致性；相关 metastore ledger manager 也是 deprecated/non-production 路径，不能直接提升为 Profile sidecar 的生产方案。
3. ZooKeeper/etcd 底层可能各自有更强能力，但公开 metadata driver 抽象没有可移植的多键事务合同。因此 sidecar 不得要求跨 backend 的通用事务。
4. 当前 `Journal` 已有成熟 group force：一个 fsync 可以覆盖多个 waiter；但它没有：
   - semantic predecessor；
   - conditional apply；
   - operation identity replay；
   - logical control sequence receipt；
   - state conflict result。
5. 当前 Bookie route 实际只有 `ledgerId + masterKey` lazy handle：
   - `HandleFactoryImpl.getHandle()` 可以在首次 Add 时创建；
   - fenced-and-deleted 防护只是一个进程内、7 天过期 cache；
   - 没有 durable instance/Profile/TOMBSTONED route。
6. normal Add 在 handle 锁内检查 fence；`recoveryAddEntry` 直接绕过 fence，并没有 durable RepairIntent/recovery-only scope gate。
7. Journal replay 会跳过未知的负 entry id，源码注释把它当作 rollback compatibility。这对于 Segment mandatory authority 不安全：旧二进制必须在 registration/superblock/format gate 处被阻止 writable，不能指望旧 Journal replay fail closed。
8. `BookieStateManager` 当前 writable registration 只提交 `bookieId/readOnly/serviceInfo`；没有 storage incarnation、assignment generation、required-through cursor 的 CAS 验证。
9. 当前 `LedgerRecoveryOp`：
   - 在首个 `NoSuchEntry` 或 `NoSuchLedger` 时把 tail 视为结束；
   - recovery writes 完成后返回内部 OK；
   - `ReadOnlyLedgerHandle.recover()` 随后以 metadata CAS durable close；
   - public callback/future最终只暴露整数 rc/handle。
10. 现有 `BKException.Code.DataUnknownException` 语义接近“无法判定/limbo”，但 legacy `BKException.create(int)` 没有对应 switch case，会退化成 `UnexpectedConditionException`。不能未经 compatibility 修复就把它指定为新的 quarantine 映射。
11. `BookKeeperAdmin` 和 `ReplicationWorker` 大量依赖整数 rc；`skipUnrecoverableLedgers` 甚至可以把被跳过 ledger 的错误转成整体 OK。新 Profile recovery 不能把这个整体 OK 当作“已恢复”authority。

---

## Q1 — Sidecar bounded root/child/snapshot interface

### Verdict：REVISE 后可 LOCK

需要锁定一个有界、domain-specific 的 `ProfileControlStore` 语义接口；不应锁定一个 ledger-global universal head，也不应把它扩展成通用 metadata transaction/database。

现有 `LedgerManager` 不足以承载该接口。实现可以由各 metadata backend 提供 adapter，但 portable contract 只能依赖单 record create/read/versioned CAS、bounded enumeration 和明确的 authority publication ordering。

### 最小不可删不变量

1. 每个 authority record 都必须绑定：
   - `ledgerId`；
   - `ledgerInstanceId`；
   - record family/kind；
   - format version 与 mandatory feature compatibility；
   - semantic generation/predecessor；
   - payload/content identity。
2. 可外部重试的 mutation 还必须绑定 stable operation identity/generation。不可要求每个 immutable snapshot chunk 都维护独立 operation id；chunk 可用 snapshot identity、ordinal、digest 做身份。
3. MetadataStore 的 opaque store version 与业务 control generation 是两种不同概念：
   - store version负责 CAS；
   - control/operation generation负责 ABA、lineage 和 response-loss 解析。
   两者都不能省略，也不能用 `Version.ANY` 代替。
4. standard `LedgerMetadata` 继续是唯一 OSS state/membership authority。sidecar root 只能绑定/消费它的 version、instance backlink、relevant ensemble digest，不能复制完整 membership。
5. root 必须有 manifest-locked hard bound。它只能保存：
   - ledger-instance identity；
   - descriptor/lifecycle summary；
   - global lifecycle fence/control generation；
   - 有限的 authority-family directory/head；
   - current snapshot identity/cut；
   - bounded suffix/page references和计数。
   receipt、RepairIntent、delete history或逐 entry 状态不得不断追加进 root。
6. 不同 authority domain 只在确有冲突时共享 predecessor order：
   - lifecycle/READY/delete terminal 等 ledger-instance global fact可共享 lifecycle generation；
   - conflicting/overlapping repair/loss domain必须排序；
   - 已证明不相交的 repair range不得为了统一 root head被 ledger-global CAS串行化。
7. 每个可 compact authority domain 必须可表示为：

```text
current committed snapshot through cut S
+ complete, bounded suffix after S
```

8. child/page 在 parent/head 引用前不是已发布 authority。创建 child/page 后 root/head CAS失败，只留下 inert orphan，不得授权 activation、recovery、delete或loss reset。
9. ledgerId reuse不能覆盖旧 instance root。旧/new instance可以物理共存，但任何 read/CAS/child reference都必须验证 exact `ledgerInstanceId`；标准 metadata backlink选择当前 instance。
10. normal Add 不读取 sidecar、不跟踪 root generation、不依赖 watch，也不执行 per-entry node/CAS。

### 推荐最小接口

接口名称和 Java signature 保持 OPEN，但语义应等价于：

```text
read(recordKey)
    -> FOUND(value, storeVersion)
     | ABSENT
     | INCOMPATIBLE_VERSION
     | CORRUPT

createIfAbsent(recordKey, value, semanticIdentity)
    -> CREATED(storeVersion)
     | ALREADY_EXISTS(existingIdentity, storeVersion)
     | CONFLICT

compareAndSet(
    recordKey,
    expectedStoreVersion,
    expectedLedgerInstance,
    expectedSemanticPredecessor,
    newValue
)
    -> APPLIED(newStoreVersion)
     | ALREADY_APPLIED(currentValue, currentStoreVersion)
     | CONDITION_FAILED(currentSummary)
     | INCOMPATIBLE_VERSION

readBoundedPage(manifest/page reference, continuation, hardLimit)
```

关键收紧：

- 这是 domain-specific wrapper，不是允许调用者任意拼接跨 key transaction 的公开数据库接口。
- 不要求多键原子提交。
- 不建立所有 record 共用的 ledger-global `rootHeadSequence`。
- 每个 child family/domain必须声明其 owner、semantic predecessor和有界 discovery方法。
- watches只能做 cache invalidation/提前唤醒，所有 correctness决策必须可通过显式 read+CAS重建。

### Root 与 child 的最低公共 envelope

可 LOCK 的是语义，不是字段名/field number：

```text
record family / semantic kind
format version
mandatory feature set
ledgerId
ledgerInstanceId
authority-domain identity
semantic generation / expected predecessor
operation identity + generation      # 仅对可重试 mutation
payload/body identity or digest
lifecycle/terminal state where applicable
snapshot/superseded-by identity where applicable
```

不应强制：

- 所有 family 使用同一个业务 payload；
- 所有 child 进入一个 universal state machine；
- 所有操作都更新 root；
- 每个 chunk 保存重复的完整 descriptor；
- root保存任意 operation历史。

### Publication 与 compaction 顺序

最低顺序：

```text
1. choose authority-domain cut S
2. build immutable snapshot/page chunks
3. persist and verify all referenced chunks
4. create snapshot manifest binding identity, chunks, cut S and content identity
5. CAS the owning domain head/root snapshot pointer
   - expected predecessor/head must still match
   - suffix anchor after S must be explicit
6. retain old snapshot/root and required suffix as corruption/response-loss fallback
7. only after new authority is independently readable and no supported root/fallback references old data:
   reclaim superseded children/pages
```

如果 snapshot build期间 child head推进：

- snapshot publication必须 CAS失败并重试；或
- manifest必须明确从 cut S 继续消费完整 suffix。

不得发布一个既没有稳定 cut，也没有 complete suffix anchor 的“最佳努力 snapshot”。

### Crash、response-loss 与 ABA

- Child durable、parent未发布时 crash：child是 orphan，不授权任何 operation。
- Parent CAS成功但 response loss：重读 parent/head；exact operation/snapshot identity匹配则 `ALREADY_APPLIED`，否则按当前 lineage决定 stale/conflict，不盲写第二个 root。
- Parent已继续推进：若原 operation已被当前 snapshot/terminal state吸收，可以返回等价已完成结果；否则返回 stale/conflict并让调用方重读，不要求保存无界 operation-id历史。
- LedgerMetadata create response loss：继续按现有 RFC 读取 standard metadata backlink、instance、ensemble和sidecar root，不建第二 instance。
- ledgerId复用：旧 store version即使从另一个 path/backend重新计数，也不能跨 `ledgerInstanceId + control generation` 通过 CAS。
- unknown mandatory version/feature：
  - 被当前 root/head引用时，affected authority domain fail closed；
  - 旧 writer不得用自己能解析的旧字段覆盖它；
  - 未被任何 authority引用的未知 orphan可在独立 GC证明后处理。
- referenced child缺失、page gap或snapshot chunk缺失：不是 ABSENT/default；该 domain进入 unavailable/quarantine。

### Orphan 与 retention

Orphan child/page 只有在以下事实成立后才能回收：

- 没有 current root/domain head引用；
- 没有当前或 fallback snapshot引用；
- 没有 suffix/operation lineage仍可能发布它；
- 对应 operation generation已被 terminal/superseding generation fencing；
- retention/grace和并发 writer fence满足；
- delete discovery所需 source/target历史已经由另一 durable snapshot/summary接管。

Timeout 单独不够。Exact retention duration和GC topology保持 OPEN。

### 性能与复杂度

该接口只服务 create/install/activation/repair/delete/compaction等冷控制路径。正确实现不会改变 normal Add：

- 无远程 read；
- 无 watch lease；
- 无 root version检查；
- 无 per-entry CAS；
- 无 per-entry sidecar child；
- 无无界 cursor/vector。

有界 page与family head会增加冷路径 CAS，但避免了一个 ledger-global repair hot spot。Small-fragment repair必须测量 CAS retries和snapshot频率；不得为了减少开发工作把所有不相交 range强制串行化。

### 禁止机制

- `Version.ANY` authority update/delete；
- 跨 standard metadata和sidecar的通用事务；
- unbounded root blob；
- 每 entry一个 metadata node/receipt；
- 一个 ledger-global universal operation log/head；
- normal Add远程读或watch correctness；
- digest-only snapshot；
- 先 reclaim旧 child，再发布新 snapshot；
- 把 referenced unknown record当作不存在。

### Spike/Model/Test Gate

Spike A / Model A至少增加：

- two instance same ledgerId、store version重新计数；
- child durable、root CAS前 crash；
- root CAS response loss；
- same operation same/different payload重试；
- unknown root/child mandatory version；
- missing snapshot chunk、suffix gap；
- snapshot cut与并发 child CAS；
- compaction publish前/后 crash；
- old snapshot过早 reclaim；
- root/page hard bound溢出；
- 不相交 repair domain不经过 ledger-global hot CAS；
- normal Add在 metadata故障/sidecar watch stale时保持同样本地路径。

### 必须保持 OPEN

- exact path、namespace、backend adapter；
- root/child/page field numbers与serialization；
- family/domain sharding拓扑；
- page/chunk大小、fan-out、root byte cap；
- compaction threshold、retention数值；
- checksum/hash；
- watcher/cache策略；
- snapshot manifest bytes；
- whether a backend uses ZK multi-op/etcd transaction internally。它们不能成为 portable safety依赖。

---

## Q2 — ArenaControlLog conditional/durability/checkpoint/free/reader-cutover API

### Verdict：REVISE 后可 LOCK

现有 Journal 的 group fsync机制可作为性能实现参考，但不能直接作为 ArenaControlLog semantic API。需要增加 per-Arena deterministic conditional state-machine接口；不能扩展成跨 Arena transaction，也不能为每个 record建立永久 future/idempotency表。

### 最小不可删不变量

1. 每个 Arena有单一 applied order和连续 `controlSequence` authority。
2. Conditional predicate必须在该 Arena sequencer内对当前 committed/applied state原子求值。
3. externally retried transition至少绑定：
   - Arena identity；
   - operation identity/generation；
   - expected predecessor/location/generation；
   - transition-specific payload identity。
4. `APPLIED` 的 authority结果只有在完整 log prefix durable through该 transition自身 sequence后才成立。
5. `durableThrough` 不能跨 sequence gap、torn record或unknown mandatory record。
6. condition failure不能改变 allocator/selector/free state。
7. 同一 predecessor只有一个 winner；重复请求不能产生第二 successor。
8. Operation idempotency retention必须有界：
   - 当前 selector/free/checkpoint state能证明同一 operation已提交时返回 `ALREADY_APPLIED`；
   - 已被后续 generation取代时可返回 `STALE/CONFLICT`；
   - 不要求永久保存所有历史 request id。
9. batching只共享 append/fsync，不创造跨 operation通用原子事务。

### 推荐 API 语义

概念接口：

```text
appendConditional(transition)
    -> DurableApplyResult {
         outcome:
             APPLIED_DURABLE
             ALREADY_DURABLE
             CONDITION_FAILED
             STALE
             INCOMPATIBLE/CORRUPT
         operationIdentity
         assignedControlSequence or sequenceRange
         resultingGeneration/stateIdentity
         durableThrough
       }
```

内部实现可以拆成：

```text
enqueue/admit -> pending sequence token
group append/force
durability completion -> durable result
```

但 allocator、locator、GC等 authority消费者只能拿到 durable completion；pending/admitted结果不得作为 cutover或reuse许可。

一个 bounded operation可编码成一条或有限多条物理 record，但其 semantic transition必须有明确 all-or-nothing apply规则。Group batch中的相邻 operations彼此保持独立 condition/result，不因共享 fsync形成通用事务。

### 共用与必须分离的原语

#### 可以共用

`MOVE_COMMIT`、conditional `FREE_AND_BUMP`、ALLOC/ALLOC_POOL等可共用：

- per-Arena sequencer；
- expected predecessor/generation检查；
- operation identity；
- assigned control sequence；
- complete-prefix replay；
- group append/fsync；
- durable-through receipt；
- response-loss reread/replay。

`MOVE_COMMIT` 与 orphan free必须进入同一个 apply order：

```text
MOVE_COMMIT first
    => selector generation advances
    => stale FREE condition fails

FREE_AND_BUMP first
    => allocation generation advances
    => late MOVE_COMMIT expected-old condition fails
```

#### 必须分离

Checkpoint不能被伪装成普通 single append：

1. choose committed applied cut `S`；
2. freeze/COW state；
3. write/verify checkpoint chunks；
4. `CHECKPOINT_COMMIT` 可以共用 control append/durability primitive；
5. inactive superblock write/fsync与active-generation选择是独立 authority publication protocol；
6. prefix reclaim又是后续 conditional operation。

`S` 必须是完整 committed/applied state cut，不是“最近收到某个 fsync callback”的位置。

Snapshot chunks、superblock和control-log record不需要一个通用三介质事务。现行 crash ordering与A/B fallback足以提供安全性。

### Reader cutover API

最低语义应等价于：

```text
durable MOVE_COMMIT receipt
    -> under a local selector/pin gate:
         validate current selector/predecessor
         publish new selector
         close acquisition of new old-location pins
         establish cutover epoch
    -> readers with pre-cut old pin may finish
    -> wait/drain pre-cut pins
    -> verify whole-allocation reclaimability
    -> conditional FREE_AND_BUMP
    -> wait durable free
    -> expose new generation for reuse
```

`publish new selector` 与 `block new old pin` 必须是同一个同步 cut。实现可用 stripe lock、seqlock/epoch或RCU，但不能：

- 先发布 locator、稍后才阻止 old pin；
- 先阻止 old pin、却让新 reader继续读 stale cached locator；
- 持久化 individual reader/future/buffer历史。

`acquireReadPin` 必须在 bounded loop中验证 selector/epoch，确保 cut后无法取得 old-location pin。

### Crash 与 response loss

- MOVE_COMMIT append但未 durable：restart仍以 old selector为 authority。
- MOVE_COMMIT durable、进程在 locator cutover前 crash：replay从 checkpoint+suffix恢复 new selector和retiring gate；不能继续用 stale derived locator。
- cutover后、reader drain前 crash：旧进程 volatile pins消失，但restart仍先恢复 retirement/new-pin gate，再判断 whole-allocation free。
- FREE append但未 durable：old generation不能reuse。
- FREE durable、response loss：retry从 current generation返回 already/stale，不重复 bump。
- group torn tail：只应用最后完整、连续、checksum有效且在durability cut内的 prefix。
- checkpoint `S-1/S/S+1` crash：restart选择完整 checkpoint + complete suffix，不按最大 generation猜。
- unknown mandatory control record：Arena quarantine；不能像现有 Classic Journal那样跳过。

### 性能与复杂度预算

- 一个 per-Arena sequencer是必要的局部顺序，不是 cluster/global锁。
- Journal现有 group force证明可以一轮 fsync完成多个 waiter；ArenaControlLog应保留同类batching。
- 不应为每个 physical record分配长期 future；建议一个 bounded operation/barrier completion，batch内部复用 waiter数组或轻量 token。
- MOVE_COMMIT是后台路径，不影响 normal Add。
- ALLOC_POOL refill可以批量，normal Add消费 durable pool，不因每 entry等待 control fsync。
- checkpoint/GC必须限流并纳入 foreground p99 Gate。
- checkpoint state只保存 live/current/retiring authority，不保存完整 move chain和reader history。

### 禁止机制

- per-moved-entry fsync；
- per-entry永久 future/idempotency map；
- cutover早于自身 sequence durability；
- derived locator作为 authority；
- timeout作为 orphan free proof；
- 永久完整 move chain；
- checkpoint保存reader/pin列表；
- foreground Add等待 relocation barrier；
- Bookie-global或cross-Arena transaction；
- 用数据扫描/最大物理 generation跨过 control gap。

### Spike/Model/Test Gate

Spike B / Model C至少覆盖：

- same predecessor双 MOVE winner；
- duplicate request before/after response loss；
- MOVE与FREE两种顺序；
- pending append、append complete、force complete边界；
- `durableThrough < ownSequence`负向测试；
- group中间 torn/gap/unknown record；
- checkpoint `S-1/S/S+1`；
- committed checkpoint + suffix与full-history oracle等价；
- selector publish与old-pin acquire竞态；
- cut前reader、cut后reader；
- crash后retirement gate恢复；
- shared allocation部分 live时禁止free；
- operation-id retention compaction后的old retry；
- queue/future/lock数量hard bound；
- relocation/checkpoint/GC对foreground p99、CPU、fsync和write amplification影响。

### 必须保持 OPEN

- record bytes、checksum、alignment；
- one-record vs bounded-range packing；
- exact control sequence encoding；
- sequencer线程/stripe布局；
- batch大小、wait阈值、queue cap；
- selector/pin gate具体同步原语；
- checkpoint page layout、superblock bytes；
- idempotency summary packing；
- `MOVE_PREPARE`；
- cross-Arena relocation，首版继续 unsupported。

---

## Q3 — Segment Bookie local durable state 的最小原子落盘组合

### Verdict：REVISE 后可 LOCK

必须锁定一个“逻辑上的 Bookie local authority state machine”，但不能现在锁定成一个物理统一日志或一个 flat role enum。

关键修正有两个：

1. `NORMAL_ACTIVE`、recovery grants、committed-readable range不是天然互斥的单一 ledger enum；一个 Bookie上的同一 ledger instance可能同时拥有不同 fragment/range的读取或repair能力。
2. Per-Arena `ArenaControlLog` 不是当然的 route owner。ledger route、auth、fence和assignment可能跨多个 Arena/device；直接挑一个 Arena保存会引入隐含单点或cross-Arena问题。

### 推荐逻辑状态形状

概念上至少分成：

```text
LedgerRouteAuthority {
    routeClass: ABSENT | CLASSIC | PROFILE | TOMBSTONED
    ledgerInstanceId
    descriptor/profile/engine identity
    protected auth binding
    install generation
    normal admission state + activation generation
    fence/admission generation
    bounded recovery grants by intent/range
    committed-readable range facts
    explicit LAC authority
    tombstone/delete generation
}

BookieRegistrationAuthority {
    bookie stable identity
    storage/device incarnation and scope
    effective assignment generation
    durable applied cursor/snapshot readiness
    writable-registration generation
}
```

字段和物理编码保持 OPEN。不能把 unbounded range/grant历史塞进单个 ledger state blob；active grants必须有 manifest cap和compaction。

### 必须是同一原子 transition 的组合

| Transition | 必须原子绑定的语义 |
| --- | --- |
| `ABSENT -> CLASSIC` | route claim + Classic/master-key binding，且发生在 payload/lazy handle创建前 |
| `ABSENT -> PROFILE_INSTALLED` | route claim + ledger instance + descriptor/Profile/Engine identity + protected auth binding + install generation + initial `NORMAL_INACTIVE` |
| normal activation | exact route/instance + READY/membership activation generation + `INACTIVE -> ACTIVE`；不得与initial install合并，因为READY稍后才存在 |
| recovery grant | exact route/instance + RepairIntent identity/generation + target/range scope + capability generation |
| recovery close/commit | 对同一 scope原子 revoke/close recovery-write capability并发布 committed-readable fact，或采用等价的“先不可逆关闭 admission、后发布 readable”fail-closed序列；不得留下 committed range仍接受任意late recovery写 |
| tombstone | exact instance terminal route + revoke normal admission + revoke/close全部该instance recovery grants + reject reads/writes |
| Bookie registration readiness | storage incarnation + effective assignment generation +其required-through满足证据必须在同一 Bookie-scope durable readiness fact中绑定 |

Snapshot chunk、payload、physical free不需要和这些 transition做跨存储事务。

### 只需条件化有序、不应强行同一原子 record

1. Sidecar READY/standard membership 与 local activation：
   - cluster authority先提交；
   - local activation再条件化消费。
2. Route/install 与 Arena allocation：
   - durable route/install先存在；
   - allocator再分配；
   - 不需要跨两个日志事务。
3. Fence与install/activation：
   - fence是独立、单调的 conditional transition；
   - late activation必须因fence/tombstone generation失败。
4. Tombstone与physical delete/free：
   - terminal admission gate先；
   - reader drain；
   - local tombstone durable；
   - allocator conditional free/generation bump；
   - 不做一个巨型原子 delete。
5. Delete effect与stream cursor：
   - effect durable必须先于cursor；
   - 可以同一个durable batch，但不要求跨多个 ledger tombstone的全局原子事务。
6. Assignment catch-up与cluster effective publication：
   - PREPARED；
   - snapshot+suffix catch-up；
   - durable local readiness；
   - cluster effective/registration CAS；
   - 不要求 normal Add查assignment lease。

### Fence 与 inflight Add 的实现合同

现行文档中“Add授权时未durably fenced”和“durable fence后无新local success”必须用 authority order统一解释：

```text
1. under bounded per-ledger admission gate:
   close new normal admissions and capture fence cut/epoch
2. every Add admitted before the cut:
   either reaches its terminal durable local-success/failure before fence authority
   or is explicitly failed
3. append/durable fence transition
4. complete fence response
5. any Add using post-cut/stale admission epoch is rejected
```

如果 data和fence共享一个sequencer，也可以证明 pre-cut Add sequence严格早于fence sequence。若物理日志不同，等待/drain pre-cut admissions是更小、更易证明的方案。

Network callback晚到不改变顺序：只要 Add的local-success linearization已在fence cut前发生，它不是“fence后新成功”；不能用callback wall-clock顺序定义安全性。

### Normal Add 热路径

推荐最小路径：

```text
1. route gate before HandleFactory/lazy storage creation
2. bounded local handle-state lookup
3. match ledgerInstance/Profile/auth
4. capture current normal-admission generation
5. require NORMAL_ACTIVE and not fenced/tombstoned
6. existing entry validation
7. RFC-0003 allocation + payload durability path
8. revalidate/complete against captured admission ordering
9. local success
```

可以把route/activation/fence generation缓存进handle，避免重复map lookup；但只在handle创建时检查一次不够。Fence/delete/activation transition必须能推进generation并使stale handle fail closed。

normal Add不得：

- 读MetadataStore；
- 读sidecar；
- 查remote assignment；
- 写control record；
- 等每Add control fsync；
- 只靠master key决定route；
- 在Profile/TOMBSTONED上进入Classic lazy create。

### Recovery Add

现有 `recoveryAddEntry` 仅靠内部opcode绕过fence，必须替换为：

```text
route/instance/Profile match
+ bounded local RECOVERY_ONLY grant lookup
+ exact intent generation
+ target/range/entry scope check
+ protected recovery authorization
+ delete/tombstone/fence-generation policy
+ idempotent payload identity
+ normal data durability barrier
```

普通客户端设置flag不能获得recovery权限。Recovery grant不授予normal writable；committed-readable不授予normal writable。

### Assignment/incarnation 与 registration

最低安全序列：

```text
1. recover storage/engine/incarnation identity
2. load authoritative PREPARED assignment
3. apply verified snapshot + complete suffix
4. persist effects before no-hole cursors
5. persist local readiness for exact assignment generation/incarnation
6. cluster effective/registration CAS verifies same generation/incarnation/readiness
7. only then register/serve writable
```

不需要在writable期间每 Add查assignment generation。为了避免 correctness依赖watch：

- obligation-increasing effective assignment只能在Bookie已经durable catch-up并出具readiness后发布；
- PREPARED本身不demote安全旧writer；
- effective generation前进后，旧 registration generation必须不可被placement视为有效；
- Bookie发现local/cluster registration mismatch时转non-writable并重新协调；
- ordinary ensemble replacement不改变storage incarnation或免除catch-up。

当前 `RegistrationManager` 不提供该 CAS语义，因此需要新的 registration/assignment adapter或协调协议；不应把它偷偷塞进普通 `registerBookie()` 而无version/response-loss合同。

### 物理存储选择

可 LOCK：

- 对调用方暴露一个逻辑 ordered conditional durable transition接口；
- restart必须从它恢复不大于crash前的接受集合；
- 安全相关状态不能散落在多个互不排序、任一缺失就默认允许的store；
- 若语义拆分到多个store，中间态必须fail closed，并由上述顺序证明安全。

必须 OPEN：

- dedicated Segment Bookie control log；
- 扩展现有 Journal；
- 独立小型 durable state store；
- 某个明确拥有全ledger route的reserved control arena；
- 分片方式和group commit。

非规范性原型建议：优先验证一个Bookie-level、只承载冷控制transition的Segment control store。直接复用per-Arena log会遇到跨Arena route owner问题；直接复用现有Journal又必须先解决unknown special record被旧二进制跳过的问题。最终选择应由Spike的restart、write amplification和p99数据决定。

### Unknown/newer 与 downgrade

- unknown mandatory local record：按其scope使ledger/device/Bookie non-writable，不能skip后继续。
- unknown optional diagnostic hint可以忽略，但不得改变接受集合。
- engine/superblock/registration必须声明mandatory control format compatibility。
- 旧二进制看到Segment storage时必须在Journal replay/handle creation前停止writable。
- derived locator、payload scan、现有master key都不能替代missing route/activation/fence authority。
- tombstoned instance不能通过清cache、restart或ledgerId reuse重新进入CLASSIC/PROFILE。

### 性能与复杂度

- normal Add新增成本应是已缓存handle上的常量次 identity/generation/role检查。
- Route claim、activation、fence、grant、close、tombstone和assignment都是冷控制操作，可group commit。
- Recovery Add复用payload barrier，不加per-entry control fsync。
- 一个逻辑state machine不会要求一个巨型物理日志；可以按ledger/Arena/Bookie scope分层。
- 不应把normal/recovery/readable做成一个flat enum；正交的normal admission + bounded range capability更准确，也避免为了组合状态枚举爆炸。
- Active grants、range facts和idempotency summary必须有硬上限、snapshot/compaction和超限fail-closed策略。

### 禁止机制

- flat `NORMAL_ACTIVE | RECOVERY_ONLY | READABLE`互斥enum；
- Profile route只在handle创建时检查一次；
- 继续使用进程内7天cache充当tombstone；
- 让旧Journal跳过mandatory Segment control record；
- recovery flag绕过intent授权；
- scattered state缺失时默认allow；
- Add-time assignment lease；
- per-Add remote lookup/control fsync；
- 为了原子性引入跨Arena/MetadataStore transaction；
- tombstone前物理free；
- assignment PREPARED无条件demote安全writer。

### Spike/Model/Test Gate

Spike A / Model A：

- Classic Add vs Profile install原子claim；
- route+auth部分写crash；
- activation/fence/tombstone所有顺序；
- stale handle generation；
- legacy normal/recovery Add绕过尝试；
- restart和old binary downgrade；
- unknown mandatory record。

Spike B / Model C：

- route authority与allocator authority组合；
- allocation存在但route missing；
- tombstone/free ordering；
- multi-Arena ledger route owner；
- control-store loss与Arena存活；
- normal Add hot-path lookup/lock/CPU/p99。

Spike C Model D：

- assignment PREPARED/catch-up/effective；
- registration response loss；
- stale registration generation；
- incarnation/scope mismatch；
- cursor-before-effect负向；
- old incarnation极晚rejoin。

共同硬 Gate：

- restart接受集合不扩大；
- durable fence后无post-cut local success；
- tombstone后normal/recovery/read均fail；
- unknown/newer不可writable；
- Classic baseline相较无remote I/O、无新增per-entry fsync；
- 100k ledger下local control内存有硬上限。

### 必须保持 OPEN

- exact physical owner/log/store；
- record set、bytes、checksum、packing；
- batching和group-commit阈值；
- handle cache/RCU实现；
- fence inflight drain的具体同步原语；
- protected auth本地表示；
- recovery grant/range index packing；
- explicit LAC record组合；
- assignment readiness/registration bytes；
- exact local errors；
- local state snapshot/compaction阈值。

---

## Q4 — Recovery outcome 到现有 API/exception/admin/metrics 的兼容映射

### Verdict：REVISE 后可 LOCK

必须先建立内部/additive rich outcome，再向旧整数 rc接口做有损但安全的适配。不能把五类语义直接压进当前 `LedgerRecoveryOp` 的 `CompletableFuture<LedgerHandle>` 或 `RecoverCallback(int rc)`，更不能把 `NoSuchEntry`、timeout或skip flag转换成伪成功。

### 最小内部结果合同

具体类型名/enum保持 OPEN，但内部结果至少语义携带：

```text
outcome semantic class
operation scope: ledger-close recovery or fragment/range repair
ledgerId + ledgerInstanceId
RecoveryContext identity/digest
attempt/operation generation
proven prefix/range and close metadata identity for success
bounded reason/cause classification
retryability / terminality
authority-conflict vs payload-evidence classification
completion/close generation where applicable
```

普通 read/add API不需要暴露这些字段。它们应出现在：

- internal recovery coordinator；
- AutoRecovery scheduling decision；
- additive BookKeeperAdmin detailed result/status；
- audit/status endpoint；
- bounded metrics labels。

Fragment repair完成不能被命名为 ledger `RECOVERED_AND_CLOSED`；result必须保留 operation scope。

### 兼容 decision tree

| Semantic outcome | 旧 callback/future/BKException 安全映射 | 新 internal/admin 语义 | Scheduler/authority 行为 |
| --- | --- | --- | --- |
| `RECOVERED_AND_CLOSED(P)` | `OK`，返回handle/完成callback | exact prefix、length、context、close metadata version/generation | 唯一可计 ledger recovery success |
| `RETRYABLE/DEFERRED` | 非OK；优先保留已有 transient cause：Timeout、Bookie unavailable、NotEnoughBookies、ZK/MetaStore/Read；无安全精确码则 `LedgerRecoveryException` | DEFERRED + bounded reason | backoff/retry；保留intent/underreplication |
| `ATTEMPT_INCOMPLETE` | 新future可cancel；legacy按触发源映射 Interrupted/Timeout或generic recovery failure | ATTEMPT_INCOMPLETE，明确只是本attempt结束 | 不写terminal状态；durable progress可重验/续做 |
| `QUARANTINED_CONFLICT/AUTHORITY_UNRECOVERABLE` | 基线安全降级为 `LedgerRecoveryException`；绝不OK | QUARANTINED + conflict/authority reason | 隔离/人工；不声称payload data loss |
| `DATA_LOSS/EVIDENCE_EXHAUSTED` | 基线安全降级为 `LedgerRecoveryException`；绝不用单个NoSuch作为ledger-wide error | DATA_LOSS + required coordinate/evidence summary | terminal/non-promotable；不伪造close |

Exact新exception code保持 OPEN，但旧接口的最小安全降级可以立即 LOCK：

```text
only durable recovered close => OK
all other semantic classes  => non-OK
legacy generic failure must not erase rich internal/admin outcome
```

### 为什么暂不 LOCK `DataUnknownException`

`DataUnknownException` 在概念上适合 quarantine，但当前代码存在兼容缺口：

- public Code已有 `-108`；
- Bookie protocol也能映射unknown ledger state；
- legacy `BKException.create(int)` 却没有 `DataUnknownException` case，会返回 `UnexpectedConditionException`。

因此可以在后续 compatibility Spike中选择：

- 修复 legacy factory并采用 `DataUnknown`；
- 新增明确协商的新code；
- 继续让legacy使用 `LedgerRecoveryException`。

在该测试完成前，不应把 `DataUnknown`写成最终映射。

### Existing API 行为

#### `openLedger` / recovery open

现有API保持：

- 只有 durable close成功才返回handle/OK；
- deferred、cancel、quarantine、data loss均不返回可误用的成功handle；
- 不强制改变普通 read/add callback签名。

建议增加 additive admin/internal detailed API；exact方法名和公开稳定性保持 OPEN。

#### `BookKeeperAdmin.recoverBookieData`

现有 `skipUnrecoverableLedgers=true` 可以为了legacy batch compatibility继续让整体调用完成，但必须满足：

- rich result列出被跳过ledger及其真实semantic outcome；
- skipped不计recovered success；
- 不清除Profile RepairIntent、underreplication marker、loss状态；
- 不发布repair completion/reset；
- 不把overall `OK`当作所有ledger均恢复；
- 新Profile automation不能消费这个legacy aggregate rc做authority决定。

#### `ReplicationWorker`

AutoRecovery内部必须消费 rich outcome，不能只依赖generic exception：

- deferred：release/defer lock并重试；
- attempt incomplete：根据cancel/deadline结束本attempt；
- quarantine：保留隔离状态和可观测reason；
- data loss：terminal/non-promotable；
- recovered/fragment completion：只有durable metadata/receipt验证后才能clear marker。

### Normal tail 与现有首个 NoSuch 行为

当前 `LedgerRecoveryOp` 首个 `NoSuchEntry/NoSuchLedger` 即停止并返回内部OK的行为，只能留给其已证明安全的Classic baseline语境；不能直接用于新general E/W/A Profile recovery。

新路径必须先经过 Round 4 oracle：

- speculative suffix第一个无法形成AQ不是DATA_LOSS；
- normal tail必须由exact fenced write set上的quorum-intersection absence证明；
- earlier hole后later required evidence不能跳过；
- known committed/required coordinate evidence exhausted才是DATA_LOSS；
- offline/no quorum是DEFERRED；
- single corrupt replica继续读取其他合法evidence；
- irreconcilable valid-looking identity/payload才进入QUARANTINED。

Fast local budget用尽仍进入point-read fallback，不映射terminal rc。

### Close response loss

最低顺序：

```text
1. recovery verifier determines prefix P in exact RecoveryContext
2. required recovery Adds become durable
3. standard metadata close CAS publishes CLOSED(P, length, exact context/membership)
4. response may be lost
5. reread metadata:
   matching CLOSED(P/context)     -> RECOVERED_AND_CLOSED
   metadata temporarily unavailable -> DEFERRED
   still IN_RECOVERY             -> retry same close operation
   incompatible CLOSED/prefix/instance -> QUARANTINED_CONFLICT
```

不能因为close CAS timeout：

- 回滚已经durable payload；
-发布第二个不同prefix；
- 返回DATA_LOSS；
- 返回OK而不重读。

### Cancel/deadline

- Caller cancellation/global deadline只终止当前attempt。
- 已提交的fence、recovery payload、RepairIntent或metadata CAS不回滚。
- Retry必须复用/重验相同 RecoveryContext；context失效时重新分类，不把旧结果直接当authority。
- 如果cancel与close CAS并发，必须先重读close authority：matching durable close仍可在admin status中显示成功，即使原future已cancel。
- Exact Java cancellation API保持 OPEN。

### Single corrupt replica 与 authority loss

- Single corrupt replica：
  - endpoint read metric记录corrupt；
  - recovery继续其他replica/fallback；
  - 不向legacy caller直接冒泡成ledger-wide `DigestMatchException`或DATA_LOSS。
- Multiple valid-looking conflicting payload/identity：
  - QUARANTINED；
  - legacy generic recovery failure。
- Payload仍在但required metadata/sidecar/control authority不可恢复：
  - QUARANTINED_AUTHORITY；
  - 不是payload DATA_LOSS。
- Required coordinate所有合法evidence已被证明永久丢失：
  - DATA_LOSS。
- Metadata `NoSuchLedger`：
  - 有authoritative delete proof时按delete语义处理；
  - 无delete proof但expected live instance metadata消失时是authority quarantine；
  - 不能自动解释为payload data loss或recover success。

### Metrics 合同

至少分离：

- attempt started/completed；
- recovered-and-closed；
- deferred by bounded reason；
- attempt incomplete/cancel/deadline；
- quarantined conflict；
- quarantined authority loss；
- evidence-exhausted data loss；
- fallback invoked；
- fast-budget exhausted；
- single-replica corruption；
- close response-loss resolved；
- recovered entries/bytes；
- attempt latency与end-to-end recovery latency。

约束：

- 不使用ledgerId、operationId、raw exception message作为metric label；
- reason和phase必须是bounded enum；
- retry attempt计数与unique durable completion计数分离；
- partial progress不能计为ledger recovered；
- metrics不是authority，不要求为了exactly-once metrics增加durable hot state。

### 禁止机制

- 非durable close返回OK；
- 首个NoSuch直接映射DATA_LOSS；
- timeout/offline/global budget映射terminal；
- single corrupt replica映射ledger-wide corruption；
- authority loss冒充payload loss；
- legacy `skipUnrecoverable`清除repair/underreplication状态；
- 改动全部read/add API只为携带diagnostic；
- 未协商就向旧client返回新code；
- 用ledgerId做metrics label；
- 把generic rc当作repair completion authority。

### Spike/Model/Test Gate

Model E / API compatibility至少覆盖：

- five outcome到legacy/new API的exhaustive table；
- 任何非success outcome都不能产生legacy OK；
- close CAS前/后/response loss；
- cancel before recovery Add、during Add、during close、close durable后；
- timeout、Bookie offline、MetadataStore unavailable；
- single corrupt replica与multiple conflicting valid payload；
- required coordinate evidence exhausted；
- missing authority但payload存在；
- first NoSuch + later speculative evidence；
- first hole + later required evidence；
- `skipUnrecoverable`不clear marker、不计success；
- AutoRecovery对deferred/quarantine/data loss采用不同调度；
- unknown/newexception code的old client/server compatibility；
- `DataUnknown` factory映射测试；
- metrics cardinality和retry double-count检查；
- fast+fallback结果与全point-read oracle一致。

### 必须保持 OPEN

- exact public/internal enum名；
- exact BKException code/class；
- new admin method/result type；
- JSON/CLI/wire schema；
- cancellation API；
- metric names和threshold；
- status retention；
- Classic与Profile engine切换点；
- general E/W/A merge algorithm本身。

---

## 本轮可立即同步的精确合同

1. 新增 domain-specific `ProfileControlStore` 语义接口；只依赖单record create/read/versioned CAS、bounded page discovery，不依赖多键事务。
2. Root有硬上限；authority family/domain拥有各自predecessor/head，禁止所有repair走ledger-global universal head。
3. Child/page先durable、再由root/domain head CAS发布；snapshot必须是verified chunks + manifest + cut + complete suffix；publish-before-reclaim。
4. Store version和semantic generation分离；authority禁止`Version.ANY`。
5. Referenced unknown/newer mandatory record fail closed，旧writer不得覆盖。
6. ArenaControlLog提供per-Arena conditional deterministic transition、operation identity、assigned sequence、durable-through结果。
7. `MOVE_COMMIT`和conditional free共享Arena apply order；checkpoint chunks/superblock保持独立publication protocol。
8. Reader selector publish与block-new-old-pin形成一个同步cut；volatile pins不持久化。
9. Segment Bookie使用逻辑local authority state machine；route claim原子绑定instance/Profile/auth/install/initial inactive。
10. Normal activation、fence、recovery grant、recovery close/committed-readable、tombstone是独立conditional transitions，不合并成flat enum。
11. Fence先关闭admission并处理pre-cut Add，再durable完成；callback wall-clock不是线性化点。
12. Tombstone原子终止该instance normal/recovery/read接受集合；physical free后置。
13. Assignment/incarnation是Bookie-scope registration authority，不是per-ledger Add lease。
14. Normal Add只做bounded local route/identity/role/generation lookup，并沿现有data durability barrier。
15. Recovery内部保留rich semantic outcome；旧接口只有durable recovered close映射OK，其余全部non-OK。
16. Quarantine authority loss与payload data loss保持分离；single corrupt replica、timeout、offline和cancel均不terminal。
17. Legacy `skipUnrecoverable`不得成为Profile recovered authority。
18. Exact enum/schema/bytes/threshold继续OPEN。

## 需同步的文档位置

- `RFC-0001`
  - §6 Profile control namespace；
  - §6.1 sidecar authority；
  - §6.3 response loss；
  - §10 orphan；
  - §14 Gate；
  - §15 OPEN。
- `RFC-0003`
  - §5/§5.1 ArenaControlLog API；
  - §5.2 MOVE_COMMIT；
  - §10/§10.1 free/orphan；
  - §11 checkpoint；
  - §12 restart；
  - §15 derived selector；
  - §18 Gate；
  - §19 OPEN。
- `RFC-0004`
  - §7.1 fallback；
  - §7.3 outcome taxonomy；
  - §9 RepairIntent/receipt snapshot；
  - §13 API completion；
  - §17 Model E；
  - §18 Gate；
  - §19 OPEN。
- `RFC-0005`
  - §5 route/install/activation；
  - §6 fence/Add；
  - §7 recovery Add；
  - §9 delete/restart；
  - §10 performance；
  - §11 invariants；
  - §12 Gate；
  - §13 OPEN。
- `Spike A`
  - manifest、minimal prototype、response-loss/ABA/unknown-version、route-state crash与performance Gate。
- `Spike B`
  - Arena conditional API oracle、durable-through、selector/pin API、checkpoint/free、bounded waiter/perf。
- `Spike C`
  - Model A local state与sidecar domains；
  - Model C transition/result/cutover；
  - Model D assignment registration；
  - Model E API outcome adapter。
- `docs/turbo-bk.md`
  - owner、cross-RFC invariants、OPEN、Stage状态。
- grill
  - 新增Round 5完整归档；
  - Integration Audit前README保持`Review Complete / Integration In Progress`。

## 是否足以开始实现

### Docs

**可以。**

本轮收紧合同同步并通过同一 reviewer Integration Audit 后，implementation-interface 文档层可以完成。它不等于 RFC Accepted。

### Spike

**可以开始实现和执行 Spike harness/model，但正式运行前必须冻结各自 manifest。**

- Spike A：sidecar adapter、route/local state原型；
- Spike B：ArenaControlLog conditional/group durability/checkpoint/pin原型；
- Spike C：Models A/C/D/E状态与动作。

Spike结果可能否证物理实现选择；不能反向削弱已锁安全不变量。

### Prototype

**可以开始非权威、可丢弃的 prototype。**

允许：

- in-memory/fault-injected `ProfileControlStore`；
- ZK/etcd单record CAS adapter prototype；
- ArenaControlLog writer/replay/checkpoint prototype；
- local authority state-machine prototype；
- additive internal `RecoveryOutcome`和legacy adapter；
- benchmarks和crash harness。

不应冻结production bytes或迁移策略。

### Shadow

**只能开始隔离的shadow prototype，不能进入正式/live Stage 5 promotion。**

实际 Bookie shadow writer仍需：

- RFC-0003接口集成并Accepted；
- Spike B/C safety通过；
- authority与Classic ACK严格隔离；
- on-disk format/upgrade gate；
- failure不影响Classic路径的证据。

### Segment ACK authority

**不能开始。**

当前仍缺：

- RFC-0001/0003/0005 Accepted；
- Spike A/B/C实际PASS；
- descriptor/hash/protected auth；
- profiled wire/capability/error compatibility；
- local control physical format与downgrade gate；
- canary-specific evidence和rollback合同。

## 到整体 Implementation Ready 还差的最小 frontier

Round 5完成后，剩余最小P0 frontier是：

1. **Profile security/wire compatibility**
   - canonical descriptor/hash；
   - protected auth/activation proof；
   - profiled opcode；
   - exact mixed-version/error/upgrade/downgrade gate。
2. **Local physical authority choice**
   - Bookie-level route/fence/grant/tombstone state由哪个物理store拥有；
   - record framing、format version、checkpoint/rotation；
   - 与Arena/payload durability如何组合；
   - old binary如何被强制non-writable。
3. **General E/W/A point-read oracle**
   - merge伪代码；
   - required frontier/normal-tail判据；
   - bounded memory/complexity；
   - fast+fallback equivalence。
4. **Wipe/decommission registration fence**
   - cluster acceptance owner；
   - irreversible scope；
   - registration CAS/replay/revocation禁止；
   - old incarnation处理。
5. **执行证据**
   - Spike A/B/C；
   - TLA+/crash matrix；
   - resource/write-amplification/p99 Gate；
   - compatibility matrix。
6. **RFC acceptance与canary**
   - owner文档一致；
   - exact enabling dependency；
   - rollback与non-promotable terminal结果。

本轮没有新增P0 owner冲突，也没有发现必须引入通用事务、PKI、Merkle、per-entry MetadataStore、Add-time lease、per-entry control fsync或cross-Arena transaction的理由。四个接口经上述收紧后足以进入 Spike/prototype 编码，但距离整体 `Implementation Ready` 仍至少需要关闭以上 frontier并取得实际否证证据。

<!-- END VERBATIM REVIEW -->

## Integration Audit 1

> Verdict：**FAIL**。以下回复由同一 reviewer 在 Round 5 集成后给出；按相同规则仅下移 Markdown heading 并清理行尾空白。所有反馈完整保留。

<!-- BEGIN VERBATIM INTEGRATION AUDIT 1 -->

## Round 5 Integration Audit 1

### 总 verdict：FAIL

整体设计方向、正确性边界和性能约束基本集成正确，但提交前仍有两项范围很小、必须修复的合同缺口。当前不能标记 `Reviewed / Integrated`，也不能 commit/push。

没有发现新的 P0 架构冲突、热路径远程 I/O、per-entry control fsync、跨 Arena 事务或明显过度设计。

### 必须修复

#### P1 — RFC-0001 仍残留“单一全局 generation”歧义

`RFC-0001-profile-capability-install.md:165` 仍写道：

> 每个 ledger instance 只需要一个冷控制 CAS generation/fencing token

这与同文件 §6.1 已锁定的以下合同存在直接歧义：

- bounded authority-domain heads；
- 不同 domain 只在实际冲突时共享 order；
- proven-disjoint repair range 不进入 ledger-global universal head。

实现者可能据此把 lifecycle、repair、loss、receipt、delete 等全部更新串到一个 ledger-global CAS generation 上，造成：

- small-fragment repair CAS 放大；
- 不相交 repair 无谓串行；
- 单 root hot spot；
- 与 Round 5 明确禁止的 universal head 相冲突。

最小修复应明确：

- 单一 token 只指 global lifecycle/READY/delete-terminal 等真正 ledger-global 的 lifecycle fence；
- repair/loss/receipt 等 operation 使用 owning authority-domain predecessor/head；
- child/domain 更新不因 root 存在而必须更新一个 ledger-global head；
- 只有实际冲突的 domain 才共享顺序。

`docs/turbo-bk.md:204`、不变量 20/24、Spike A A21 当前表达是正确的，不需要扩大修改范围。

#### P1 — Sidecar operation identity 的冲突 payload 缺少显式否证

当前 RFC 已包含 operation identity、payload/content identity、`ALREADY_APPLIED/CONFLICT` 等构件，但没有把最关键的幂等判定直接写成不可误解的合同：

```text
same operation identity + same semantic payload
    => equivalent ALREADY_APPLIED/APPLIED result

same operation identity + conflicting semantic payload
    => CONFLICT
    => never APPLIED or ALREADY_APPLIED
```

Round 5 原审查明确要求 Spike 覆盖“same operation same/different payload retry”。目前：

- Spike A A21 只泛化描述 response-loss 后 `already/stale/conflict`；
- fuzz 只有通用 `retry`；
- Model A 有 `LoseResponse`，但没有明确的同 operation 异 payload retry action/invariant；
- hard Gate 也没有直接禁止一个 operation identity 绑定两个 payload。

最小修复建议：

- 在 RFC-0001 §6.1/§6.3 明确上述同/异 payload 结果；
- 在 Spike A A21 增加 response loss 前后，同 operation identity 分别携带相同和冲突 payload 的测试；
- 在 Spike C Model A 增加等价 action/check，或明确参数化现有 retry action；
- 增加类似 `SidecarOperationIdentityBindsSinglePayload` 的 invariant/hard Gate。

这不会增加热路径成本；它只要求冷控制 CAS/replay 使用已经锁定的 content identity 做比较。

### 逐文件审计

#### RFC-0001 — 除上述一项外通过

已正确集成：

- domain-specific `ProfileControlStore`；
- single-record versioned CAS；
- store version 与 semantic generation 分离；
- authority update/delete 禁止 `Version.ANY`；
- bounded root、family/domain head、child/page；
- child 先 durable、head 后 publication；
- verified snapshot + complete suffix；
- publish-before-reclaim；
- ledger-instance/store-version ABA 防护；
- referenced unknown mandatory state fail closed；
- normal Add 不读 sidecar、不依赖 watch；
- exact path/schema/sharding/bounds/hash/retention 继续 OPEN。

#### RFC-0003 — PASS

已忠实集成 Q2：

- per-Arena deterministic conditional apply；
- expected predecessor/generation；
- operation identity；
- assigned control sequence；
- own-sequence `durableThrough`；
- condition failure 无 effect；
- bounded idempotency；
- `MOVE_COMMIT` 与 conditional free 共用 Arena order；
- checkpoint publication 与普通 transition 分离；
- selector publication 与 block-new-old-pin 为同一 cut；
- volatile reader/pin 不持久化；
- cross-Arena relocation 继续 unsupported。

没有新增 per-record fsync、永久 future、foreground Add relocation barrier、full move-chain retention或跨 Arena 事务。

#### RFC-0004 — PASS

Rich recovery outcome 集成正确：

- 只有 matching durable ledger close 可投影 legacy `OK`；
- deferred、incomplete、quarantine、data loss 全部 non-OK；
- authority loss 与 payload data loss 分离；
- timeout/offline/cancel/single corruption 不被误报 terminal；
- fragment repair不命名为 ledger recovered；
- legacy skip 不清 intent/marker，不产生 repair completion；
- AutoRecovery 消费 rich outcome；
- `DataUnknownException` 最终映射仍保持 OPEN；
- metrics reason/phase bounded，且 metrics 不成为 authority。

#### RFC-0005 — PASS

Logical local authority 已正确收紧：

- route claim 早于 lazy handle/payload create；
- Classic/Profile/Tombstoned 为 durable route；
- normal admission、recovery grants、committed-readable facts不是互斥 flat enum；
- route/install 原子绑定 instance/Profile/auth/initial inactive；
- activation、fence、grant、close、tombstone、registration 按最小原子或条件化顺序执行；
- fence admission cut 与 callback wall-clock 分离；
- stale handle generation fail closed；
- unknown/newer/old-binary downgrade fail closed；
- physical owner 保持 OPEN；
- normal Add 只增加 bounded local lookup，不读取 remote assignment/sidecar，也不等待 per-Add control fsync。

#### Spike A — 一项待补，其余 PASS

A21、A22 已覆盖：

- child/head/snapshot crash；
- store-version reset 与 ledgerId reuse；
- root/page cap；
- unknown mandatory；
- disjoint domain 不走 global head；
- route/install/fence/grant/tombstone/registration；
- stale handle、multi-Arena、control-store loss和old binary。

必须补上同 operation 的 same/conflicting payload retry。

#### Spike B — PASS

Conditional apply、group durability、own-sequence durability、duplicate response loss、queue/waiter cap、checkpoint、pin cut、orphan free/late commit和 foreground p99 accounting 均已进入可执行否证面。

#### Spike C — 一项待补，其余 PASS

Model A/C/D/E 的 state、action、invariant和最低 config 已同步。A+C、A+D、C+D 的组合要求仍在。

必须补上 sidecar operation identity 不能绑定两个 semantic payload 的显式 action/invariant。实际写 TLA module 时还应确保 A/D 中的 local route/grant/readable state 是同一共享 authority，而不是两份彼此独立的抽象；这属于实现检查，不要求本轮提前选择物理存储。

#### `docs/turbo-bk.md` — PASS

总纲仍保持：

- `Proposed / Not Implementation Ready`；
- bounded domain heads；
- 无 ledger-global universal hot head；
- local physical owner OPEN；
- rich recovery projection；
- Stage 5 仍只允许 shadow writer；
- Segment ACK authority 和 live promotion仍禁止。

没有把本轮 OPEN 项伪装成最终实现合同。

#### Grill README 与 Round 5 归档 — PASS

- README 正确保持 `Review Complete / Integration In Progress`。
- Round 5 reviewer 原回复经允许的 heading 下移和行尾清理后，规范化内容逐字符一致：
  - original/archived：`49,865` chars；
  - SHA-256：`31a29c8f16f592683a1e58c62e68cec5808362cb1e673a9304d4d0827eaf8e1c`。
- BEGIN/END marker 各一个。
- 单一 H1、fence、EOF newline、相对链接和 whitespace 检查通过。

### 性能、复杂度与过度设计结论

性能边界总体通过：

- normal Add 无 MetadataStore/sidecar/assignment remote read；
- 无 per-entry metadata node/CAS；
- 无 per-Add control fsync；
- allocator 和 cold transition 保留 group commit；
- relocation/checkpoint/GC 进入 p99、CPU、write-amplification Gate；
- rich outcome 不强迫普通 read/add API 扩展。

复杂度控制总体通过：

- sidecar 是 domain-specific wrapper，不是通用事务数据库；
- ArenaControlLog 是 per-Arena state machine，不是 Bookie-global transaction；
- local authority 锁逻辑接口、物理 owner 继续 OPEN；
- normal/recovery/readable 使用正交 capability，避免 flat enum 状态爆炸；
- exact bytes、schema、API、packing、threshold和拓扑均未提前冻结。

唯一可能造成实质性能回退的文本风险，就是 RFC-0001 的“一个 generation”歧义；必须在提交前消除。

### 必须保持 OPEN

本轮正确保留为 OPEN 的内容包括：

- sidecar exact namespace/backend/schema/field number/sharding；
- root/page/fan-out 数值、snapshot bytes、retention和hash；
- backend 是否内部使用 ZK multi-op/etcd transaction；
- ArenaControlLog exact Java API、record bytes、sequence encoding、batch/queue阈值；
- selector/pin 同步原语；
- local authority physical owner、record framing和checkpoint；
- protected auth、activation proof和profiled wire/opcode；
- registration CAS adapter bytes；
- recovery rich enum、BKException code、admin/wire API和metric names；
- `DataUnknownException` compatibility选择；
- general E/W/A merge算法和exact point-read oracle；
-所有 production Gate 数值。

### 提交条件

当前不能 commit/push。

最小流程应为：

1. 修复上述两项；
2. 将本 Audit 1 完整归档，README继续保持 Integration In Progress；
3. 交回同一 reviewer 做 Integration Audit 2；
4. Audit 2 PASS 后，归档该回复并将 Round 5 改为 `Reviewed / Integrated`；
5. 只暂存本轮十份 Markdown，再 commit/push。

### 下一轮最小 frontier

Round 5 通过后，下一轮最值得 grill 的单一 frontier 是 Profile security/wire compatibility：

- canonical descriptor serialization/hash；
- protected auth 与 activation proof；
- profiled opcode/capability negotiation；
- mixed-version error mapping；
- engine/superblock/registration downgrade gate。

在此之前可以开始 Spike harness、TLA module和不接管 authority 的局部/隔离 shadow prototype；仍不能开始 Segment ACK authority、live Stage 5 promotion或生产 canary。

<!-- END VERBATIM INTEGRATION AUDIT 1 -->

## Integration Audit 2

> Verdict：**FAIL**。以下回复由同一 reviewer 在 Audit 1 修复后给出；按相同规则仅下移 Markdown heading 并清理行尾空白。所有反馈完整保留。

<!-- BEGIN VERBATIM INTEGRATION AUDIT 2 -->

## Round 5 Integration Audit 2

### 总 verdict：FAIL

Audit 1 的 payload/idempotency P1 已完整闭合，单一 generation 的 RFC-0001 局部文字也已正确修改。但完整跨 RFC 检查发现一个残余合同冲突：RFC-0004 §14.1 和总纲不变量 13 仍可把所有 repair operation 拉回同一个 ledger-global CAS，与 Round 5 新锁定的 domain-head 合同不一致。

这是一个范围明确的集成修复，不需要新 RFC 或重开架构。修复并通过下一次复核前，Round 5 不能标记 `Reviewed / Integrated`，也不能 commit/push。

### Audit 1 两个 P1 的复核

#### P1-1 lifecycle generation 与 domain heads：局部通过，跨文档未完全闭合

RFC-0001 当前修改方向正确：

- lifecycle/READY/delete-terminal 被限定为真正 ledger-global fact；
- repair/loss/receipt 使用 owning authority-domain predecessor/head；
- 普通 child/domain 更新不因 root 存在推进 universal head；
- proven-disjoint repair range 不共享 ledger-global hot CAS。

但现行 `RFC-0004-range-recovery-delete.md:696` 仍写道：

> delete/repair operation 还必须共享同一 ledger-instance control generation/CAS

`docs/turbo-bk.md:225` 也仍泛化为 delete、RepairIntent、ensemble change 的权威状态转换全部由同一个 sidecar control generation 协同串行。

两种字面实现都会违反一侧合同：

- 若完全按 RFC-0001 实现独立 domain head，却没有 RepairIntent 与 delete fence 的共同入场 cut，可能发生：
  1. delete 冻结 target；
  2. 另一个 domain 随后发布 RepairIntent；
  3. stale coordinator 授予 recovery grant或写入第一份 payload；
  4. target 未进入 delete frozen history，形成漏删副本。
- 若完全按 RFC-0004 的广义表述，让 repair intent、progress、loss、receipt、completion 全部更新一个 ledger-global CAS，则会重新产生：
  - universal repair head；
  - 不相交 repair 串行；
  - small-fragment CAS 放大；
  - 与 Round 5/Q1 和总纲不变量 20/24 冲突。

因此 Audit 1 的第一个 P1 只能判为“局部修复正确、系统级仍 FAIL”。

#### P1-2 sidecar operation identity 与 conflicting payload：PASS

这一项已完整闭合。

RFC-0001 已明确：

- operation identity 不可变地绑定一个 semantic payload/content identity；
- same identity + same payload 解析到同一 durable result；
- same identity + conflicting payload 只能失败且不能改变 authority；
- snapshot/terminal summary 可以有界证明已经吸收的 operation；
- 超出可证明 retention 后只能 stale/conflict，不能作为新 operation成功；
- 不要求保存无界 operation history。

Spike A 已加入：

- A21 response-loss 前后 same/conflicting payload retry；
- snapshot/terminal summary 吸收；
- bounded retention 退出；
- fuzz action；
- hard Gate；
- stop condition。

Spike C Model A 已加入：

- operation identity → semantic payload/result state；
- same/conflicting retry actions；
-检查目标；
- `SidecarOperationIdentityBindsSinglePayload`；
- A-SIDECAR config；
- hard Gate。

这组修改足够形成可执行否证面，并且只影响冷控制 CAS/replay，不进入 normal Add 路径。

### bounded retention 判断：PASS

当前合同没有要求无界 idempotency：

- snapshot/terminal summary可以接管旧 operation；
- retention退出后不再承诺永久返回 `ALREADY_APPLIED`；
- 极晚 retry只能 stale/conflict；
- conflicting payload在任何情况下都不能变成新成功；
- exact retention数值、summary packing与compaction threshold仍保持 OPEN。

非阻塞措辞建议：A21 中“same payload只返回同一 APPLIED/ALREADY结果”应理解为仍处于 current/snapshot-provable window；退出该窗口后允许 stale/conflict。RFC-0001 已经表达了这个边界，因此不单独作为提交阻塞项。实际 TLA module和Spike实现仍必须把 retained operation set做成 manifest-bounded state，而不是无限 map。

### 唯一必须修复项：RepairIntent admission 与 domain progress 分层

需要同时修改 RFC-0001、RFC-0004 和总纲，冻结下面的最小逻辑合同。

#### 推荐最小顺序

```text
1. durable-create immutable RepairIntent child, initially inert
   - exact instance, target, source/range, operation generation

2. publish/admit that RepairIntent against the ledger-global
   lifecycle/delete-fence cut
   - condition: DELETE_INTENT has not won
   - publication makes the intent discoverable to delete freeze
   - only this admitted state may authorize recovery grant/first payload

3. after admission, repair progress/loss/receipt/completion use
   their owning authority-domain predecessor/head
   - bind admitted RepairIntent generation
   - bind/check lifecycle/delete-fence generation
   - do not advance a universal ledger head for every progress update

4. DELETE_INTENT wins the global lifecycle/delete-fence cut
   - no later RepairIntent admission may succeed
   - freeze every intent admitted before the cut
   - include each admitted source/target regardless of copy/CAS state

5. a child durable but not admitted before DELETE_INTENT remains inert
   - it cannot grant recovery authority or accept first payload
   - it may later be reclaimed under normal orphan proof
```

该合同保留 Round 2 的 delete-discovery安全性，同时保留 Round 5 的 domain-head性能边界。

它不要求跨 key transaction。一个可移植实现可以沿用已经锁定的 child-before-parent publication：

```text
durable inert child
    -> conditional lifecycle/delete-fence head CAS publishes admission reference
```

exact root/directory/head layout、sharding、batching和encoding继续 OPEN。若采用其他物理方案，必须证明同样的 admission/delete cut，不能依赖“先 list child，再写 DELETE_INTENT”。

#### 需同步的位置

- `RFC-0001` §6/§6.1：
  - global fact 应包含 `DELETE_INTENT/delete fence`，不能只写最终 delete-terminal；
  - 明确 RepairIntent admission 是“实际冲突”例外；
  - admitted 后的 progress/receipt才完全留在 domain head。

- `RFC-0004` §14.1/§14.2：
  - 将“所有 delete/repair operation共享同一 CAS”收窄为：
    - `DELETE_INTENT` 与 RepairIntent admission/publication共享一个逻辑顺序；
    - 后续 progress/loss/receipt不共享 universal CAS；
    - grant/first payload必须晚于 admission；
    - delete cut后的 stale domain update不产生 authority。

- `docs/turbo-bk.md` 不变量 13：
  - 从所有 repair 状态转换协同串行，收窄为 delete-terminal/admission/membership等真正冲突的 transition；
  - 与不变量 20/24 的 disjoint-domain并发保持一致。

- Spike C Model D/A+D：
  - 至少区分或明确抽象：
    - inert RepairIntent child；
    - admitted RepairIntent；
    - delete fence cut；
  - 覆盖 child-before-admission crash、admission先赢、delete先赢；
  - 证明未 admission 的 child不能 grant/write，已 admission 的 intent必被 frozen target枚举。
  - 可以把 `CreateRepairIntent`直接定义为逻辑 admission线性化点，不要求提前冻结物理 record数量。

### 性能与复杂度结论

除上述歧义外，当前性能保护仍通过：

- normal Add 无 sidecar/MetadataStore/assignment remote read；
- 无 per-entry metadata node/CAS；
- 无 per-Add control fsync；
- RepairIntent admission最多是每 repair operation/fragment 的冷控制 cut；
- recovery payload、loss receipt和completion不走 ledger-global hot head；
- ArenaControlLog继续支持 group durability；
- local route检查仍是 bounded local lookup；
- exact topology、batch size、retention和同步原语没有被提前冻结。

推荐修复不会引入通用事务。它只区分一次冷路径 admission CAS与后续 domain-local progress，复杂度低于建立跨 backend transaction或让所有 repair永久共用一个 universal state machine。

### 其余文件与合同

以下仍维持 Audit 1 的 PASS：

- RFC-0003 conditional apply/durable-through/checkpoint/selector-pin cut；
- RFC-0004 rich recovery outcome及legacy projection；
- RFC-0005 logical local authority与physical owner OPEN；
- Spike B conditional/group durability与资源 Gate；
- Spike A A22 local authority crash/downgrade面；
- Spike C Model C/D/E 其余状态与不变量；
- `docs/turbo-bk.md` 的 `Proposed / Not Implementation Ready`、Stage 5 shadow-only与ACK authority禁止；
-所有 exact schema/bytes/API/threshold/physical owner 继续 OPEN。

没有新增 steady-state Add成本，也没有引入 Merkle、PKI、per-entry MetadataStore、Add-time lease、cross-Arena transaction或无界 root。

### 归档与机械检查

归档完整性通过：

- Round 5 原 reviewer回复规范化比较 PASS：
  - SHA-256：`31a29c8f16f592683a1e58c62e68cec5808362cb1e673a9304d4d0827eaf8e1c`
- Integration Audit 1 规范化比较 PASS：
  - `7,729` Unicode chars；
  - SHA-256：`08a7c439eb88df60f151d4a4c380362ae5729915904fb2a54bec1d9936d49e8f`
- Audit 1 BEGIN/END marker各一个。
- README 正确保持 `Review Complete / Integration In Progress`。
- 十份 Markdown：
  - 单一 H1 PASS；
  - code fences平衡；
  - EOF newline PASS；
  -相对链接 PASS；
  - `git diff --check` PASS；
  -新增 Round 5 文件 whitespace check PASS。
- HEAD 与 `origin/turbo-bk` 均为 `ba820b28babd7683e8dfe6a724d1b8aeecf7c2e1`，无 staged文件。
- 无关未跟踪文件未读取、未修改、未纳入。

### 残余 OPEN

保持 OPEN 的内容没有变化：

- sidecar exact backend/path/schema/sharding和数值 bounds；
- RepairIntent admission directory/head 的exact encoding与batching；
- local authority physical owner；
- ArenaControlLog physical records和同步原语；
- protected auth、activation proof、profiled wire/opcode；
- registration CAS adapter；
- rich outcome exact exception/admin/wire mapping；
- `DataUnknownException`兼容选择；
- general E/W/A merge/point-read oracle；
-正式性能阈值与全部执行证据。

### commit/push 许可

当前不许可 commit/push，也不能将 Round 5 标为 `Reviewed / Integrated`。

完成上述单一跨文档修复后，应：

1. 完整归档本 Audit 2；
2. README继续保持 Integration In Progress；
3. 交回同一 reviewer做一次聚焦 lifecycle/delete-admission 的最终 Integration Audit；
4. PASS 后再归档、标记 `Reviewed / Integrated`，只暂存本轮十份 Markdown并 commit/push。

### 下一轮 frontier

Round 5 最终通过后，下一轮仍应聚焦 Profile security/wire compatibility：

- canonical descriptor serialization/hash；
- protected auth与activation proof；
- profiled opcode/capability negotiation；
- mixed-version error mapping；
- engine/superblock/registration downgrade gate。

在此之前仍只允许 Spike harness、TLA module以及不接管 authority 的局部/隔离 shadow prototype；不能开始 Segment ACK authority或live Stage 5 promotion。

<!-- END VERBATIM INTEGRATION AUDIT 2 -->

## Integration Audit 3

> Verdict：**FAIL**。以下回复由同一 reviewer 在 Audit 2 修复后给出；按相同规则仅下移 Markdown heading 并清理行尾空白。所有反馈完整保留。

<!-- BEGIN VERBATIM INTEGRATION AUDIT 3 -->

## Round 5 Integration Audit 3

### 总 verdict：FAIL

Audit 2 要求的 lifecycle/delete-admission 分层本身已经正确闭合：

- 未 admitted 的 RepairIntent child 不可授予 grant、不可接收 payload；
- admission 与 `DELETE_INTENT` 共享必要的 lifecycle/delete-fence cut；
- cut 前 admitted intents 均进入 delete frozen history；
- admission 后的 progress/loss/receipt/completion 使用 owning domain head，不推进 universal ledger head；
- 不需要跨 key 事务，不增加 normal Add 成本。

但完整 diff 中仍有一个范围明确的 P1 集成遗漏：RFC-0004 的权威 AutoRecovery 最小顺序和成本记账仍保留旧的“一次 RepairIntent create 后即可 grant”表述，没有纳入刚锁定的独立 admission transition。它会让实现者绕过新合同，因此当前不能标记 `Reviewed / Integrated`，也不能 commit/push。

这不是新的架构问题，只需同步一个既有合同。

### Audit 2 修复逐项判断

#### 1. 无漏删窗口：新合同 PASS

以下内容一致且正确：

- RFC-0001 §6.1 先创建 immutable inert child，再通过 conditional lifecycle/delete-fence head CAS 发布 admission reference；
- 只有 admitted intent 才能获得 recovery grant 或接收第一份 durable payload；
- delete 先赢后新 admission 失败；
- admission 先赢时，source、target 与 operation generation 必须进入 frozen history；
- 未 admitted child 始终 inert，只能按 orphan proof 清理；
- RFC-0004 §14.1/§14.2、总纲不变量 13、Spike C Model D/A+D 均已同步这一 cut。

`UnadmittedRepairIntentCannotGrantOrWrite`、`DeleteFenceOrdersRepairIntentAdmission`、`DeleteFreezeCoversAllAdmittedRepairIntents` 三个 invariant 直接覆盖关键安全面。

#### 2. 无 universal repair head：PASS

RFC-0001、RFC-0004、Spike C 和总纲都已明确：

- ledger-global lifecycle head 只承担 READY、`DELETE_INTENT`、delete fence/terminal 和 RepairIntent admission 等真正冲突的 transition；
- admitted 后的 copy progress、accepted loss、receipt、strong completion 使用 owning authority-domain predecessor/head；
- disjoint repair 不因 root 存在而推进一个 ledger-global CAS；
- stale post-delete domain update不能产生 grant、payload 或 completion authority。

没有重新引入 universal repair head、全局 repair lock 或 small-fragment ledger-global CAS 放大。

#### 3. Standard membership、orphan 与 strong completion：核心合同 PASS

现行新合同正确保持：

- Standard LedgerMetadata CAS 是 ensemble membership 唯一 authority；
- sidecar admission 不提前发布 membership；
- copy 后仍需重读 exact metadata/fence，再做标准 ensemble CAS；
- unadmitted child 或 inactive install 只能成为无 authority 的可清理 orphan；
- admitted intent 即使 copy 尚未开始、部分完成或 aborted-but-dirty，也必须被 delete freeze 枚举；
- delete cut 后 stale progress/completion 无 authority；
- membership、target durability、normal activation 或 generic `COMMITTED` 任何单项都不能冒充 range-scoped strong completion/loss reset。

没有发现与 repair completion、loss-generation ordering 或 delete freeze 的新冲突。

### 唯一必须修复项

#### P1 — RFC-0004 §9 的执行顺序仍允许跳过 admission

RFC-0004 §9.1 当前仍写：

```text
1. CAS create instance-specific RepairIntent
2. target durable install, normal-inactive
3. target durable grant RECOVERY_ONLY(intent generation)
4. copy ...
```

这与同文件 §14.1/§14.2 以及 RFC-0001 §6.1 的新合同不一致。`CAS create RepairIntent` 现在只能创建 immutable inert child；它不能同时代表 lifecycle/delete-fence admission。若实现者按 §9.1 编码，step 3 仍可能在 admission 前授予 grant，重新打开漏删窗口。

RFC-0004 §9.2 的 loss-reset 顺序也仍写：

```text
1. create durable RepairIntent and freeze bounded range/context
2. grant target RECOVERY_ONLY
```

这里同样没有明确 admission cut。“freeze range/context”不能替代与 `DELETE_INTENT` 条件化排序的 admission authority。

#### 最小修复

RFC-0004 §9.1 和 §9.2 的顺序应共同表达：

```text
1. idempotently create immutable inert RepairIntent child
2. target normal-inactive install and RepairIntent admission may be ordered
   independently, but neither authorizes payload
3. conditionally publish admission against the exact
   lifecycle/delete-fence predecessor/head
4. only after both inactive install and admission are proven durable,
   grant RECOVERY_ONLY bound to the admitted intent and fence generation
5. copy through recovery Add
6. reread exact membership, intent admission and delete fence
7. CAS standard ensemble membership
8. close/revoke recovery grant and publish the appropriate readable/result fact
9. publish domain-local completion conditioned on the same fence/predecessor
```

不必锁定 inactive install 与 admission 两者之间的 exact 先后；它们可以并行或交换。不可删的不变量只有：grant、first payload 和 ensemble publication 必须晚于有效 admission。

#### Response-loss 行为也需在该顺序中明确

RepairIntent admission CAS response loss 后必须：

- 重读 exact lifecycle/delete-fence head或其已提交 snapshot/summary；
- matching operation identity + matching payload 已 admitted：继续同一 intent/target；
- delete 已先赢：该 child 保持 inert，不能 grant/write；
- conflicting payload、unknown mandatory state、head gap 或结果无法判定：fail closed/deferred；
- 不得因 timeout 盲建第二个 intent、选择第二个 target 或授予本地 grant。

现有 RFC-0001 的通用 sidecar response-loss合同可以复用，不需要新事务或新协议族。但 RFC-0004 的执行序列和 Spike C `D-INTENT` 必须明确覆盖 admission response loss；如果通用 `LoseResponse` 已参数化覆盖所有 conditional publication，只需把这一映射写清，无需增加另一套状态机。

#### 成本记账必须同步

RFC-0004 当前仍写：

> 每 repair operation/fragment 的 intent create CAS……

新合同语义上包含两个有序的冷控制 mutation：

1. immutable inert child create；
2. lifecycle/delete-fence admission-head CAS。

成本表必须如实列出这两项。物理 backend 可以 group/batch，或在支持时优化 network/fsync，但 multi-key transaction 不能成为 portable safety 前提。

这仍然是每 repair operation/fragment 的冷路径固定成本，不是：

- per-entry MetadataStore write；
- per-entry control fsync；
- recovery payload hot-path round trip；
- normal Add sidecar read/CAS；
- ledger-global repair progress CAS。

因此修复不会引入实质性热路径性能损耗，也不会要求通用事务系统。

### Spike/Model 判断

Spike C 当前新增的状态、动作和 invariant方向正确：

- inert/admitted children；
- `AdmitRepairIntentAgainstDeleteFence`；
- admission/delete winner；
- admitted-only first payload；
- domain-local progress；
- freeze coverage；
- `D-INTENT` 的每个 crash boundary。

唯一需要补明的是 admission publication 的 response-loss投影。最小做法是在 `D-INTENT` 场景或动作说明中明确：

- admission durable但响应丢失；
- admission未提交且 delete 先赢；
- coordinator restart 后重读相同 operation；
- 两种情况均不能产生未 admitted grant/payload；
- matching committed admission不能生成第二 intent/target。

不需要增加新的通用模型、全局状态或独立事务 abstraction。

### 归档与机械检查

归档完整性全部 PASS：

- Round 5 原 reviewer回复规范化逐字符比较 PASS：
  - chars：`49865`
  - SHA-256：`31a29c8f16f592683a1e58c62e68cec5808362cb1e673a9304d4d0827eaf8e1c`
- Integration Audit 1 比较 PASS：
  - chars：`10250`
  - SHA-256：`08a7c439eb88df60f151d4a4c380362ae5729915904fb2a54bec1d9936d49e8f`
- Integration Audit 2 比较 PASS：
  - chars：`11062`
  - SHA-256：`71b01eaed817e61f48786d59829fddd180b2f09bda0963df4fc1135dc1e075a5`
- 三组 BEGIN/END marker 均各一个；
- README 正确保持 `Review Complete / Integration In Progress`；
- 十份 Markdown 均为单 H1、成对 fence、单一 EOF newline，relative links 校验 PASS；
- `git diff --check` PASS；
- 新增 Round 5 文件独立 whitespace check PASS；
- staged files 为 0；
- `HEAD` 与 `origin/turbo-bk` 均为 `ba820b28babd7683e8dfe6a724d1b8aeecf7c2e1`，ahead/behind 为 `0/0`；
- 工作区范围仍是九份 tracked Markdown 加一份 Round 5 新 Markdown；
- 未跟踪 PDF 未读取、未修改、未纳入任何检查或发布范围。

### 残余 OPEN

原有 OPEN 边界保持正确，没有被伪装成最终合同：

- admission directory/head 的 exact path、schema、page/index、sharding与batching；
- child/operation/snapshot retention和orphan cleanup阈值；
- backend是否利用 multi-op/grouping 优化；
- exact wire bytes、proof/credential、ACL和error mapping；
- local durable state 的物理 owner、record packing与downgrade gate；
- recovery rich outcome exact enum/API；
-数值 budget、queue depth和benchmark阈值。

本次修复不得顺带冻结这些内容。

### 性能、复杂度与过度设计结论

正确性方面，新 admission cut 是必要且最小的：没有它，delete无法原子地区分“可以忽略的 inert child”和“可能已接收 payload、必须冻结的 repair”。

性能方面，其代价仅是每 repair operation的一次额外冷路径 admission CAS。后续 progress/loss/receipt/completion不进入 global head，normal Add完全不受影响。该成本应被 benchmark 和资源上限衡量，但不能为了少一次冷路径 CAS牺牲 delete correctness。

复杂度方面，没有必要引入：

- 跨 key transaction；
- universal repair head；
- per-entry metadata；
- Add-time lease；
- Merkle/PKI；
- 新的通用 workflow engine。

现有 inert child + conditional admission reference + domain-local progress 已是足够小的状态机。

### commit/push 许可

当前不许可：

- 将 Round 5 标记为 `Reviewed / Integrated`；
- commit；
- push。

最小闭环是：

1. 同步 RFC-0004 §9.1、§9.2 和成本记账；
2. 在 Spike C `D-INTENT` 明确 admission response-loss边界；
3. 完整归档本 Audit 3；
4. README继续保持 Integration In Progress；
5. 交回同一 reviewer 做一次只聚焦上述顺序与成本的最终复核。

其余已通过内容不应重写。

### 下一轮 frontier

Round 5 真正 PASS 后，下一轮仍应聚焦 Profile security/wire compatibility：

- canonical descriptor serialization/hash；
- protected auth与activation proof；
- profiled opcode/capability negotiation；
- mixed-version error mapping；
- engine/superblock/registration downgrade gate。

在本次最小同步完成前，仍只允许 Spike harness、模型和不接管 authority 的隔离 prototype/shadow；不能开始 Segment ACK authority或 live Stage 5 promotion。

<!-- END VERBATIM INTEGRATION AUDIT 3 -->

## Integration Audit 4

> Verdict：**FAIL**。以下回复由同一 reviewer 在 Audit 3 修复后给出；按相同规则仅下移 Markdown heading 并清理行尾空白。所有反馈完整保留。

<!-- BEGIN VERBATIM INTEGRATION AUDIT 4 -->

## Round 5 Integration Audit 4

### 总 verdict：FAIL

Audit 3 的全部正确性缺口已经闭合。§9.1、§9.2、response-loss、delete-first、同 operation retry、membership CAS、role transition、strong completion 和模型 Gate 均通过。

当前仅剩一个真实且很小的性能记账缺口：RFC-0004 §9.1 明确要求 target 的 durable inactive install 与之后的 durable recovery grant 是两个有序语义 transition，但 §9.1 成本段只列了“一次 target recovery-only control durability”，漏掉了 inactive-install transition。

这不要求改设计，只需修正成本措辞。由于本轮明确要求成本诚实，修复前仍不能标记 `Reviewed / Integrated` 或 commit/push。

### 逐项判断

#### 1. §9.1 / §9.2 不再允许 create 后直接 grant：PASS

两套顺序现均明确：

1. 幂等创建 immutable inert RepairIntent child；
2. target durable normal-inactive install；
3. conditional lifecycle/delete-fence admission；
4. 只有 install 与 admission 均 durable 后才授予 `RECOVERY_ONLY`；
5. first payload、copy 和 ensemble publication 均晚于 admission。

`INERT/PREPARED`、`ADMITTED`、`RECOVERY_AUTHORIZED`、`COMMITTED` 等语义已正确分开，且 exact 状态名仍保持 OPEN。

没有残留“child create 等于 admission”或“install 等于 write authority”的路径。

#### 2. Response loss、第二 intent/target 与 delete-first：PASS

RFC-0004 已锁定：

- admission durable但响应丢失时，重读 exact head/snapshot并恢复同一 intent/target；
- same operation + same payload只解析同一 durable result；
- delete 先赢时 child 保持 inert；
- conflict、unknown mandatory state、head gap或结果无法证明时 fail closed/deferred；
- timeout不能盲建第二 intent、选择第二 target或授予 grant。

Spike C 已正确复用 Model A 的参数化 `LoseResponse`，没有另建第二套事务状态机，并增加：

- admission response-loss/restart 场景；
- matching admission恢复同一 target；
- delete-first和unknown结果不得 grant/write；
- `AdmissionResponseLossCannotDuplicateIntentOrTarget` invariant；
- duplicated intent/target 必须为零的硬 Gate。

该部分足够可执行，没有过度设计。

#### 3. 成本记账：唯一 FAIL

RFC-0004 当前最低顺序分别要求：

- step 2：`target durable normal-inactive install`；
- step 4：在 admission 和 install 均 durable 后，执行 durable recovery grant。

RFC-0005 §5.1 同样把它们定义成不同语义 transition：

- `ABSENT -> PROFILE_INSTALLED` 绑定 route/instance/Profile/auth/install generation/normal-inactive；
- recovery grant另行绑定 RepairIntent、target/range scope和capability generation。

但 RFC-0004 当前成本段只写：

> 一次 target recovery-only control durability

这没有计入 target inactive-install 的 local control mutation。即使最终实现能利用 group commit、已有 install 返回 `ALREADY_APPLIED`，或选择更紧凑的物理 framing，规范也不能在基准成本中假装 install 没有发生。

#### 唯一最小修复

只需把成本措辞改成类似：

```text
每 repair operation/fragment 的冷控制成本包括：
- single-record immutable inert-child create；
- lifecycle/delete-fence admission-head CAS；
- target normal-inactive install durable transition；
- target recovery-grant durable transition；
- existing standard membership CAS；
- domain-local completion CAS。
```

同时保留：

- target 已存在 matching durable install 时，install transition可以幂等解析为既存结果；
- exact record packing、queueing、group commit与物理 fsync次数保持 OPEN；
- 不要求每个语义 transition独占一次物理 fsync；
- benchmark必须覆盖 cold install 与 already-installed/idempotent retry 两种成本；
- portable safety不依赖 multi-key transaction。

不要把修复扩大为：

- 强制两个独占 fsync；
- 新的 target transaction protocol；
- per-entry control record；
- universal repair head；
- normal Add控制路径。

这是成本口径修正，不是新安全状态。

#### 4. Membership、strong completion与role transition：PASS

完整顺序仍正确：

- sidecar admission不复制或提前替代 standard membership；
- copy 后重读 exact LedgerMetadata、admission和delete fence；
- standard ensemble CAS仍是 membership唯一 authority；
- closed/historical target只进入 committed-readable，不自动 normal-active；
- current writable target必须另行满足 post-CAS normal activation；
- recovery grant先关闭/revoke，再发布 readable/result；
- range loss reset只在线性化到 domain-local strong completion后成立；
- membership、local durability、activation或generic `COMMITTED` 任一单独都不能 reset；
- delete cut后的 stale progress/completion没有 authority。

没有发现新的 role expansion、response-loss回归或 orphan-GC冲突。

### 性能、复杂度与过度设计结论

除成本漏记外，当前设计保护了正确的性能边界：

- normal Add没有 sidecar read、remote lookup、control write或新增 fsync；
- recovery payload没有 per-entry MetadataStore round trip或per-entry control fsync；
- repair progress/loss/receipt/completion不推进 ledger-global head；
- admission只对真正与 delete冲突的每 repair operation发生一次；
- local cold transitions可以使用现有 group-commit思路；
- exact fsync、packing、batch和queue参数仍由 Spike/benchmark决定。

修正成本表不会增加实现复杂度；它只是承认当前已锁顺序中本来就存在的 install transition，避免低估 write amplification和repair latency。

### 归档与机械检查

全部 PASS：

- Round 5 原 reviewer回复：
  - chars `49865`
  - SHA-256 `31a29c8f16f592683a1e58c62e68cec5808362cb1e673a9304d4d0827eaf8e1c`
- Audit 1：
  - chars `10250`
  - SHA-256 `08a7c439eb88df60f151d4a4c380362ae5729915904fb2a54bec1d9936d49e8f`
- Audit 2：
  - chars `11062`
  - SHA-256 `71b01eaed817e61f48786d59829fddd180b2f09bda0963df4fc1135dc1e075a5`
- Audit 3：
  - chars `11098`
  - SHA-256 `2c8212df8eff036fc59e5812b5edac39bb9775b161c0ef5ddd7a1fa719a888c2`
- 四组归档 BEGIN/END marker 均各一个；
- README仍为 `Review Complete / Integration In Progress`；
- 十份 Markdown 的单 H1、fence、relative link、EOF newline和trailing-whitespace检查均 PASS；
- `git diff --check` 与新增文件独立检查 PASS；
- staged files为零；
- `HEAD == origin/turbo-bk == ba820b28babd7683e8dfe6a724d1b8aeecf7c2e1`，ahead/behind为 `0/0`；
- 未跟踪 PDF 未读取、未修改、未纳入范围。

### 残余 OPEN

原有 OPEN 均保持正确：

- target local state的exact record packing、physical owner和group-commit策略；
- inactive install与grant是否能在特定已证明等价的实现中共享物理 durability batch；
- sidecar exact schema/path/sharding/batching；
- wire/proof/credential/error mapping；
-数值 queue、batch、p99和write-amplification阈值。

修复成本措辞不得提前冻结这些内容。

### commit/push 许可

当前不许可标记 `Reviewed / Integrated`、commit或push。

只需：

1. 修正 RFC-0004 §9.1 成本段，显式计入 target inactive-install 与 recovery-grant 两个语义 transition；
2. 完整归档本 Audit 4；
3. README继续保持 Integration In Progress；
4. 交回同一 reviewer 做一次只核对该成本句和文件范围的最终检查。

其余已通过内容不应再改动。

### 下一轮 frontier

该单点修复通过后，下一轮仍是 Profile security/wire compatibility：

- canonical descriptor serialization/hash；
- protected auth与activation proof；
- profiled opcode/capability negotiation；
- mixed-version error mapping；
- engine/superblock/registration downgrade gate。

在此之前仍不能开始 Segment ACK authority或 live Stage 5 promotion。

<!-- END VERBATIM INTEGRATION AUDIT 4 -->

## Integration Audit 5

> Verdict：**PASS**。以下回复由同一 reviewer 在 Audit 4 修复后给出；按相同规则仅下移 Markdown heading 并清理行尾空白。所有反馈完整保留。

<!-- BEGIN VERBATIM INTEGRATION AUDIT 5 -->

## Round 5 Integration Audit 5

### 总 verdict：PASS

Audit 4 的唯一成本缺口已经完整闭合。当前文本没有新的正确性、性能、复杂度或过度设计阻塞。

许可在完整归档本 Audit 5 后：

1. 将 Round 5 状态改为 `Reviewed / Integrated`；
2. 只暂存本轮十份 Markdown；
3. commit；
4. push 到 `nereusstream/bookkeeper` 的 `turbo-bk`；
5. 继续排除未跟踪 PDF。

### 成本修复判断：PASS

RFC-0004 §9.1 现在如实列出每 repair operation/fragment 的全部冷控制语义：

- immutable inert-child create；
- lifecycle/delete-fence admission-head CAS；
- target normal-inactive install durable transition；
- target recovery-grant durable transition；
- standard membership CAS；
- domain-local completion CAS。

这与权威执行顺序和 RFC-0005 local authority合同一致，不再漏记 target install。

#### Idempotency 边界正确

文本明确 matching durable install 已存在时可以解析为既存结果，因此：

- response-loss retry不会重复安装；
- restart不会为同一 operation制造第二份 local route/install authority；
- already-installed路径不会被错误计为必须重做完整 cold install；
- cold-install与idempotent-retry成本被要求分别 benchmark。

#### 没有冻结物理 fsync 或事务：PASS

当前措辞清楚区分：

- 语义 transition；
- physical record packing；
- queueing；
- group commit；
- fsync次数；
- backend batching。

“install transition”和“grant transition”不等于强制各自独占一个 fsync。实现可以在满足 durable ordering和crash equivalence的前提下利用 group commit；但 portable safety不依赖 backend multi-key transaction。

没有引入：

- 固定两次独占 fsync；
- 跨 MetadataStore/local store事务；
- 新的 repair transaction framework；
- universal repair head；
- per-entry control record。

### 性能与复杂度结论

性能边界正确：

- 新成本仅位于 repair operation/fragment冷路径；
- normal Add不读取sidecar；
- recovery payload路径不增加per-entry MetadataStore round trip；
- 不增加per-entry control fsync或intent update；
- repair progress不推进ledger-global CAS；
- matching install retry可以直接复用durable结果；
- exact batching、queue和fsync策略仍由Spike及benchmark决定。

复杂度没有膨胀。修复只是把既有两个本地语义 transition计入成本，没有新增状态、owner、RPC或持久化机制。

### 完整 diff 回归判断

此前通过的合同均无回归：

- inert child不授权grant/payload；
- admission与delete fence正确单序；
- response loss恢复同一intent/target；
- delete-first、conflict、unknown或gap均fail closed；
- Standard LedgerMetadata仍是membership唯一authority；
- recovery grant与committed-readable不授予normal write；
- strong completion仍绑定exact membership、policy、coverage和overlapping predecessor；
- post-admission progress继续使用owning domain head；
- 未重新引入global repair-progress CAS；
- OPEN项没有被转写为exact实现合同。

本次增量范围与声明一致：RFC-0004成本句修正，加上Audit 4完整归档；其余已通过内容未发生语义回归。

### 归档与机械检查

全部 PASS：

- Audit 4原文归档规范化逐字符一致：
  - chars：`7967`
  - SHA-256：`3817b152c3b04552df824542847a6ae0faf6b13ca198ebfca2bd8df428925609`
- Round 5原回复及Audit 1–4的BEGIN/END marker均各一个；
- README当前正确保持 `Review Complete / Integration In Progress`；
- 十份Markdown的单H1、fence、relative link、EOF newline和trailing-whitespace校验PASS；
- `git diff --check` PASS；
- Round 5新增文件独立diff check PASS；
- staged files为零；
- `HEAD == origin/turbo-bk == ba820b28babd7683e8dfe6a724d1b8aeecf7c2e1`；
- ahead/behind为`0/0`；
- tracked diff仍为九份预期Markdown，另有Round 5新增归档；
- 未跟踪PDF未读取、未修改、未纳入发布范围。

归档Audit 5并修改README状态后，应再次执行相同的Markdown、whitespace、staging范围和Git ancestry检查。

### 残余 OPEN

以下内容继续保持OPEN，不影响本轮集成通过：

- target local state的exact physical owner与record encoding；
- install/grant transition的exact packing和group-commit策略；
- exact fsync、queue、batch与p99阈值；
- sidecar path/schema/sharding/snapshot retention；
- protected proof、credential、ACL和wire encoding；
- recovery exact enum/API/error mapping；
- mixed-version和downgrade实现；
-正式Spike/benchmark结果。

这些OPEN意味着Round 5完成的是 implementation-interface合同，不代表整体系统已经Implementation Ready。

### commit/push 许可

明确许可：

1. 将本 Audit 5完整归档到Round 5文档；
2. 将README中的Round 5状态改为`Reviewed / Integrated`；
3. 重新运行机械校验；
4. 只暂存以下十份Markdown：
   - RFC-0001；
   - RFC-0003；
   - RFC-0004；
   - RFC-0005；
   - Spike A；
   - Spike B；
   - Spike C；
   - `docs/turbo-bk.md`；
   - grill README；
   - Round 5归档；
5. 确认未跟踪PDF未进入index；
6. commit并push `turbo-bk`；
7. push后核对remote ref等于本次commit。

Round 5可以正式标记为`Reviewed / Integrated`。

### 下一轮 frontier

下一轮应聚焦 Profile security/wire compatibility：

- canonical descriptor serialization/hash；
- protected auth与activation proof；
- profiled opcode/capability negotiation；
- mixed-version error mapping；
- engine/superblock/registration downgrade gate。

Round 5通过后，可以继续Spike harness、TLA/crash模型、局部prototype和不接管authority的隔离shadow；Segment ACK authority与live Stage 5 promotion仍需等待上述frontier及实际Spike证据闭合。

<!-- END VERBATIM INTEGRATION AUDIT 5 -->
