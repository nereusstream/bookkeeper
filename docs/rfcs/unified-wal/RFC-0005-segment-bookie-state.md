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
| cluster delete、repair target/placement、range reset assertion/loss ordering、delete assignment/snapshot 与 recovery outcome authority | [RFC-0004](RFC-0004-range-recovery-delete.md) |
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

当前`DigestManager.generateMasterKey()`生成20-byte `SHA-1("ledger" || password)` verifier并随Add发送；它是data credential/verifier，不是 Profile control authority。源码基线还包含两个必须在 Profile 路径关闭的问题：`LedgerDescriptorImpl.checkAccess()` mismatch 会记录请求与缓存的完整 master-key byte array；`AuthDisabledPlugin` 可让 anonymous connection 认证成功。Profile INSTALL/ACTIVATE/repair/delete必须拒绝anonymous或authenticated-but-unauthorized control principal，并对日志、metric、receipt、exception做secret-leak hard regression；不因此重设计整个Classic password/KDF协议，也不声称继承的SHA-1 verifier获得更强安全性。

当前`BookieProtoEncoding.RequestDecoder`在v3 protobuf解析抛`RuntimeException`时会将连接切换到pre-v3并用同一bytes重解，legacy ADD parser又不严格拒绝所有unknown protocol version；因此只加optional field/new enum不构成Profile隔离。当前启动顺序是environment check后Journal replay再到registration，unknown negative Journal meta-entry会被skip；Cookie future layout/optional field也没有既成的旧binary拒绝语义，data-integrity路径还可能auto-stamp。现有registration只发布Bookie identity/read-only/service info的ephemeral事实，没有storage-incarnation/readiness generation CAS。这些都是后续wire、pre-replay fence与registration adapter必须由真实binary否证的源码基线。

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

首版logical operation/capability matrix至少为：

| Operation family | Segment Profile 要求 | 路径 |
| --- | --- | --- |
| INSTALL/STATUS、ACTIVATE/STATUS | mandatory control subtype；non-anonymous + exact operation/instance/target-scope AuthZ + direct authority read | cold control |
| `PROFILE_ADD_NORMAL` | distinct mandatory data subtype；local normal admission | hot data |
| `PROFILE_ADD_RECOVERY`、grant/close/status | distinct data/control subtype；bounded local grant | recovery data/cold control |
| POINT_READ、FENCE、READ/WRITE LAC | instance-aware等价语义；否则install/call-time capability reject | data/control |
| FORCE/LIST/storage introspection | 显式支持或明确 capability reject | control |
| BATCH/RANGE/BatchRecoveryAdd | optional negotiated capability；unsupported按RFC-0004安全fallback | data |
| TOMBSTONE/DELETE consumption | instance/incarnation/generation bound control subtype | cold control |

exact opcode、framing、request fields与error number保持OPEN；mandatory区别是Profile normal/recovery/Classic不能共享会被旧decoder忽略的optional语义。

## 5. Routing、install 与 activation

Segment Bookie 必须消费 RFC-0001 的 authoritative local route：

- `CLASSIC` route 不能由 Segment data path 解释；
- `PROFILE` route 必须匹配 ledger instance、descriptor hash、Engine 和 protected auth binding；
- normal profiled Add 只有在匹配的 global READY authorization 和 durable local normal activation 存在后才能继续；
- `TOMBSTONED` route 永远不能重新进入 writable；
- restart 后的接受集合不能大于 durable route/install/activation 授权集合。
- INSTALL/ACTIVATE等冷控制必须来自non-anonymous、且对exact operation/ledger instance/target scope有授权的authenticated principal，并由Bookie direct-read exact committed cluster authority后才可写本地状态；AuthN-only caller、自述generation、registration hint或master key都不能替代该验证，AuthN/AuthZ早于route/credential/allocation/durable effect；
- local protected auth binding与semantic descriptor在同一instance内immutable，receipt只暴露secret-free identity/result。

同一个 ledger instance 的 local authority 不能压成互斥 flat role enum：normal admission、bounded recovery grants与committed-readable range可以正交存在。逻辑形状至少表达：

```text
LedgerRouteAuthority {
    routeClass: ABSENT | CLASSIC | PROFILE | TOMBSTONED
    ledgerInstance/Profile/Engine/protected-auth/install identity
    normal admission state + activation generation
    fence/admission generation
    bounded recovery grants by intent/range
    bounded committed-readable range facts
    explicit LAC authority
    tombstone/delete generation
}

BookieRegistrationAuthority {
    bookie stable identity + storage/device incarnation/scope
    effective assignment generation
    durable cursor/snapshot readiness
    writable-registration generation
}
```

recovery grant和committed-readable都不隐含normal writable；active grants/range facts有manifest hard cap、snapshot/compaction和超限fail-closed。activation proof、exact binding fields、role index/record packing与error mapping保持开放。

### 5.1 最小原子 transition 与物理 owner 边界

以下语义必须在本地同一 conditional durable transition中绑定：

- `ABSENT -> CLASSIC`：route claim + Classic/master-key binding，早于payload/lazy handle创建；
- `ABSENT -> PROFILE_INSTALLED`：route + instance/Profile/Engine/auth + install generation + initial normal-inactive；
- normal activation：exact route/instance + target stable identity/storage incarnation + READY/membership activation generation + purpose + inactive-to-active；不能与initial install合并，initial/replacement purpose不能重放；
- recovery grant：exact route/instance + RepairIntent/generation + target/range scope + capability generation；
- recovery close/commit：对同一scope不可逆关闭recovery-write admission并发布committed-readable fact，或等价fail-closed有序transition；
- tombstone：exact instance terminal route，同时撤销normal admission和全部该instance recovery grants并拒绝read/write；
- Bookie registration readiness：storage incarnation + effective assignment generation + required-through满足证据。

cluster READY/standard membership先提交、local activation后消费；route/install先于Arena allocation；fence是独立单调transition；tombstone admission gate早于reader drain/local delete/free；delete effect durable早于stream cursor；assignment按PREPARED/catch-up/local readiness/effective registration排序。这些只需条件化有序，不需要跨MetadataStore/Arena/payload的通用事务或巨型原子delete。

本 RFC 锁定一个对调用方可见的logical ordered conditional durable transition interface，但不选择dedicated Bookie-level control log、扩展Classic Journal、独立state store或reserved control arena。Per-Arena `ArenaControlLog`不当然拥有跨Arena的ledger route；若语义拆到多个store，中间态必须fail closed，任一authority缺失不能default allow。exact physical owner由Spike restart、write amplification和p99数据决定。

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

normal Add 与 fence 使用同一个bounded per-ledger admission order：

```text
1. close new normal admissions and capture a fence cut/epoch
2. every pre-cut admitted Add reaches terminal durable local-success/failure,
   or is explicitly failed
3. append/durable fence transition
4. complete fence response
5. reject post-cut or stale-admission generation
```

若data/fence共享sequencer，可用sequence证明pre-cut Add严格早于fence；物理日志分离时，drain/fail pre-cut admission是最小合同。network callback wall-clock不定义线性化：pre-cut local success的callback可以晚到，但durable fence后不能形成新的post-cut local success。response loss由durable state reread/replay解析。

normal Add必须使用RFC-0001 distinct Profile normal logical operation。最低本地路径为：route gate早于HandleFactory/lazy storage create，bounded handle-state lookup对已验证缓存的固定长度instance/Profile/protected-auth identity或verifier做comparison，capture current route/admission generation，要求normal-active且非fenced/tombstoned，沿RFC-0003 allocation+payload durability，并在完成时服从captured admission order。route/activation/fence generation可以缓存进handle，但不能只在handle创建时检查；transition必须推进generation使stale handle fail closed。普通Add不解析/重算完整descriptor/hash或auth-binding hash/HMAC，不携带完整Engine/capability vector或READY proof，不读MetadataStore/sidecar/remote assignment，不做KMS/signature/certificate验证，不写control record或等待per-Add control fsync。携带credential的Profile transport满足manifest选择的confidentiality/integrity；exact TLS/SASL机制由RFC-0001保持OPEN。

## 7. Recovery Add

Recovery Add 是带有 recovery authority 的 distinct Profile logical operation，不预设一条独立持久 `RECOVERY_ADD` control record。它必须：

- 只在匹配 ledger instance/Profile 且存在绑定 live RFC-0004 RepairIntent 的 durable `RECOVERY_ONLY` authority 时绕过 normal fence 拒绝；
- 匹配 target、authorized fragment/range 与 intent generation，不得写出授权范围；
- 使用与 normal Add 等价的 allocator/payload durability barrier；
- 在重复、response loss 和 restart 后保持 payload 幂等或明确冲突；
- 不要求 normal activation，也绝不授予 normal writable authority；
- 不能绕过 recovery-only authorization、authentication、instance、generation 或 delete/tombstone gate；
- 不能让普通客户端仅靠设置legacy flag、复用normal opcode或重放normal activation获得 recovery 权限。

Recovery-only authority 可以在 target 进入标准 ensemble 前存在。repair CAS 提交后，closed/historical target 通常转换为 `COMMITTED_REPLICA_OR_READABLE` 并关闭 recovery-only authority；只有 target 另行成为 current writable fragment member并满足 RFC-0001 post-CAS normal activation/fence 合同，才可独立进入 `NORMAL_ACTIVE`。

对同一 intent/range 的 recovery completion 必须先关闭late recovery-write admission，再发布committed-readable fact；不得留下“已提交可读但同一grant仍能任意写”的窗口。Profile recovery Add必须验证exact instance/descriptor/auth、RepairIntent admission、bounded local grant、intent/grant generation、target stable identity/storage incarnation、range/entry scope、delete/tombstone gate和payload identity；legacy recovery flag不足以取得grant。

recovery authority 的集群来源、repair target、intent retention 与 delete discovery 由 RFC-0004 负责。Bookie local record 只消费该 authority，不复制 cluster schema。

## 8. Explicit LAC 与读操作

Segment engine 必须为 explicit LAC 定义：

- authorization；
- durable linearization；
- 单调/覆盖规则；
- restart replay；
- 与 fence、recovery 和 delete 的并发关系。

BookKeeper 对外需要的 read、LAC、list 或 storage introspection 操作必须逐项列入 capability matrix：支持的操作给出语义等价合同；不支持的操作必须在 placement/install 或调用点明确 capability-reject，不能静默返回不完整结果。

Profile read/LAC/list请求必须通过mandatory Profile discriminator并至少匹配ledger instance/descriptor route identity以及该operation所需的fence/tombstone/readable generation；若实现不能进行该instance-aware校验，就必须在install/handshake或调用点明确reject，不能复用Classic opcode后忽略Profile状态。

## 9. Delete 与 restart

Segment Bookie 只消费 RFC-0004 已授权的 instance-specific local tombstone/delete。local free/reuse 仍服从 RFC-0003 的 reader drain 与 durable generation bump。

兼容与readiness分成三个scope，不能互相替代：

```text
Bookie/storage compatibility fence
    Engine, stable identity/incarnation, mandatory local control format/features,
    device manifest digest, migration generation and minimum compatible reader/writer

device/Arena superblock
    storage incarnation, Arena format/features, control/checkpoint generation

cluster registration readiness
    stable identity/incarnation, Engine, protocol/capability generation,
    verified format, effective assignment and writable-registration generation
```

第一层必须落在受支持stock old binary启动时**必然读取且在任何Journal replay、Arena writer、handle/lazy storage、registration之前确定拒绝**的mandatory path；这是阻断downgrade的authority。只增加Cookie optional property/version、registration property、Arena/superblock文件、unknown negative Journal meta-entry、文档/client placement或启动后demote都不足，因为旧binary可能忽略、auto-restamp或skip后继续writable。exact physical encoding保持OPEN，但若无法证明现有BookieId/storage scope上存在这种old-visible fence，最低安全迁移是使用新的BookieId、全新storage scope与access credentials，并保留旧scope不可写。

restart/startup顺序锁定为：

1. 在触碰Journal replay、Segment writer、handle或registration前读取并验证Bookie/storage compatibility fence；
2. 校验Bookie stable identity/incarnation、Engine、migration generation、mandatory features和完整device manifest；
3. 校验每个required device/Arena superblock、format/features/control/checkpoint generation；missing/corrupt/unknown mandatory/partial migration使整个Bookie non-writable；
4. 恢复allocator、payload与checkpoint current selector + complete suffix的同Arena relocation authority；
5. 恢复route、protected auth、normal activation、recovery-only/committed-readable role、fence、explicit LAC和tombstone；
6. 按current selector + suffix重建derived handle/locator/index，不能从物理generation、mtime、data scan或stale locator猜winner；
7. 校验current effective assignment/incarnation，apply verified delete snapshot+complete suffix，effect先于no-hole cursor，并完成delete/recovery reconciliation；
8. durable记录local readiness，再由cluster registration CAS校验同一stable identity/incarnation/Engine/protocol/capability/format/assignment/readiness generation；
9. 只有全部步骤成功才启动writable Profile RPC。

Bookie registration readiness遵循PREPARED assignment → catch up → durable local readiness → effective/registration CAS。obligation-increasing generation只有Bookie durable catch-up后才effective；PREPARED不无条件demote当前safe writer，effective前进后stale registration generation不再可placement。现有registration接口不足，需独立versioned registration/assignment adapter或明确协议；registration只是cluster placement/readiness authority，不是local old-binary fence，也不成为per-Add lease。

upgrade推荐使用独立Segment cohort/新storage incarnation。最低状态为`CLASSIC_COMPATIBLE -> PREPARED_NON_WRITABLE -> FORMAT_READY -> REGISTRATION_READY`：先drain/demote，publish并验证old-visible fail-stop fence，再初始化全部device/superblock，恢复并建立local readiness，最后registration与traffic。无需跨device transaction；任一步crash或部分device完成都保持non-writable，重试同一migration generation。

rollback到old binary只在下列negative proof全部成立时允许：从未出现Segment/Profile local success或durability-unknown authority；没有不兼容control/payload，或已由验证过的reverse/wipe流程清除；cluster已接受decommission/new incarnation；stale registration已fence；rollback marker按generation CAS提交。否则必须保持non-writable并选择roll-forward、export或wipe/new incarnation，不能让old binary解释现存Segment authority。required device移除也必须由cluster-authorized新incarnation/device-manifest generation完成，不能本地删文件后继续。

unknown/corrupt mandatory state按其Bookie/device/ledger scope quarantine；只有明确safety-neutral optional hint可忽略。response loss重读同一generation解析，compatibility fence不得被Cookie/data-integrity auto-restamp覆盖。minimum compatible reader/writer范围、marker bytes、Cookie/superblock布局、registration adapter与migration/rollback tooling保持OPEN。

## 10. 性能边界

必须保持：

- normal Add 不新增 per-entry control-log fsync；
- normal Add 不远程读取 MetadataStore；
- route/activation/fence 检查为有界本地 lookup，并尽量与 handle state 合并；
- descriptor canonicalize/hash、control authority direct-read、compatibility/registration validation与capability negotiation只在create/control/connect/startup执行，不进入normal Add；
- normal Add auth-binding hash/HMAC/KMS/signature/certificate invocation为0，只做缓存固定长度identity/verifier comparison；
- allocator pool 与 DATA durability 继续允许 group commit；
- fence、normal activation、recovery-only grant/close、delete 等冷控制操作可以 durable/group commit；
- recovery Add 复用数据路径，不增加无意义的 per-entry control record。
- repair intent 与 local recovery authority 是 per-operation/per-fragment，不做 per-entry MetadataStore update 或 per-entry control fsync。
- compaction `MOVE_COMMIT` 可按有界 record/range group commit；它是 background relocation authority，不给 normal Add 增加 per-entry control fsync，也不创造新的 local success。
- delete effect 与 per-stream cursor 可 batch/group commit，但 cursor 永远晚于对应 effect durability。
- normal Add 不读取 repair receipt、loss ordering、delete assignment 或 cursor 的远程 authority；这些事实只在冷控制/restart/registration路径消费，不形成 Add-time lease。
- route/install/activation/fence/grant/tombstone/registration等冷transition可group commit；active grant/range与idempotency summary有hard cap，不形成unbounded per-ledger state。
- compatibility fence、device manifest/superblock、recovery与readiness只在startup/migration/registration读取；必须按cold/warm、device count记录phase latency、read bytes与I/O count，并与Classic-only startup匹配比较，exact threshold保持OPEN。

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
12. obligation-changing effective assignment 已前进时，stale generation不能继续 authoritative writable；PREPARED generation 不无条件demote当前 safe writer。
13. orphan GC只清理从未成为 authoritative lookup 的 new location；logical entry的既存 local-success事实继续由 current selector承接。
14. required authority无法恢复时本地保持 quarantine/non-writable，不把“无法判定”上报为 payload DATA_LOSS；recovered success只消费RFC-0004 durable close outcome。
15. route claim原子绑定instance/Profile/auth/install/initial inactive；Profile/Tombstoned请求不能在route gate前进入Classic lazy create。
16. normal admission、bounded recovery grants与committed-readable facts不是互斥flat enum；grant/close/tombstone按exact scope条件化更新。
17. fence先关闭new admission并处理pre-cut Add，再durable完成；callback到达时间不改变local-success authority order。
18. tombstone原子收窄该instance normal/recovery/read接受集合，physical free必须后置。
19. unknown mandatory local state或old-binary incompatibility不能skip后writable；missing/scattered authority不能default allow。
20. assignment/incarnation readiness属于Bookie-scope registration authority，不是per-ledger Add lease。
21. master key只授权data access；Profile control要求non-anonymous且对exact operation/instance/target scope有授权的principal，AuthN-only/anonymous不能产生transition，secret/offline verifier不能出现在公开或诊断surface。
22. Profile normal/recovery operation彼此且与Classic wire distinct；legacy flag、unknown Profile subtype或decoder fallback不能产生Profile/Classic local effect。
23. old binary在mandatory pre-replay compatibility fence上fail-stop；Cookie optional field、registration hint、new superblock或unknown Journal record不能单独充当该fence。
24. 任一required device partial migration、unknown/corrupt mandatory state或incarnation/manifest mismatch时整个Bookie non-writable。
25. 现存Segment/Profile success或durability-unknown authority没有negative proof时不得rollback到old binary。

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
- prepared/effective assignment handoff、cluster terminal wipe/decommission fence 与 stale registration；
- required authority loss、payload evidence exhaustion 与 normal-tail success的本地/API语义不混淆；
- Classic baseline 对比下的 throughput、p99、CPU、fsync 与 lock contention；
- Classic/Profile route atomic claim、stale handle generation、flat-role负向组合、unknown mandatory record和old-binary downgrade；
- distinct Profile normal/recovery wire operation、legacy flag伪造、unknown subtype、v3→legacy fallback与semantic error propagation；
- fence admission cut/pre-cut Add、multi-store fail-closed、multi-Arena route owner与Bookie registration response-loss/stale generation；
- non-anonymous且exact operation/instance/target scope authorized control principal、authenticated-but-unauthorized负向路径、direct-read exact READY/membership/RepairIntent、target/incarnation/purpose binding与secret-leak regression；
- credential-bearing Profile transport confidentiality/integrity，以及normal Add auth-binding hash/HMAC/KMS/signature/certificate invocation为0；
- 真实stock old binary在mandatory compatibility fence上于Journal replay/registration/write前退出，且Cookie version/optional property、auto-restamp、unknown Journal record与registration-only负向候选均被否证；
- 全startup order、crash-at-each-migration-boundary、partial device、superblock corruption、device-manifest change、registration CAS、rollback positive/negative proof与new-incarnation路径；
- cold/warm startup latency、compatibility/device/readiness read bytes与I/O count、required-device scaling及Classic-only baseline原始证据；normal Add中的format/readiness read或remote I/O为0；
- local control resident memory/active grant/waiter hard bounds，以及normal Add local lookup/lock/CPU/p99；
- Model A/C 中 Segment Bookie state 与 allocator state 的组合无 safety counterexample。

RFC-0005 未 Accepted 前，RFC-0003 只能解锁 Segment shadow writer，不能解锁 Segment ACK authority。RFC-0005 Accepted 也只是 canary 的必要前置；仍需 canary-specific evidence 与所有实际启用路径的依赖闭合，才能执行对应 ACK authority canary。

## 13. 开放问题

- logical local authority的exact physical owner：dedicated control log、existing Journal extension、small state store或reserved control arena；
- durable state 的 exact record set、format/version、packing、checksum、snapshot/rotation 与 group-commit 边界；
- protected auth binding 的本地表示；
- activation proof 的消费方式与 initial/replacement 差异；
- normal admission、bounded recovery grant/range index、committed-readable fact、idempotency summary的exact packing/caps/compaction与error mapping；
- fence 与 inflight Add 的精确线性化实现；
- explicit LAC 与 payload block 的写序；
- read/LAC/list capability matrix；
- Engine identity 在 cookie、directory layout、superblock 和 registration 中的编码；
- old-binary-visible compatibility fence的exact physical marker/path/bytes、minimum reader/writer manifest，以及Cookie auto-stamp隔离方式；
- device/Arena superblock与Bookie/device manifest的exact关系、partial migration状态和required-device removal tooling；
- migration/rollback/reverse/wipe工具、stock old binary版本矩阵与rollback negative-proof receipt；
- local seal 是否确有需求；若有，其 authority 与 metadata CLOSED 的关系；
- RFC-0003 `MOVE_COMMIT` exact local record packing、batch completion、reader cutover接口、orphan GC 与 cross-Arena unsupported 后续协议；
- RFC-0004 delete stream topology、assignment/snapshot schema 与 exact local cursor packing；
- assignment readiness/registration CAS adapter、storage incarnation bytes与response-loss/reconciliation接口；
- recovery strong assertion/local evidence binding、accepted loss ordering与五类 outcome到现有Bookie API/error的exact dependency mapping；
- performance Gate 的 exact thresholds。

这些问题关闭、相关 Spike/Model 通过前，本 RFC 不得标为 Implementation Ready。
