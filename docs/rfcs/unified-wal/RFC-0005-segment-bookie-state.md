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

当前logical operation/capability matrix至少为：

| Operation family | Segment Profile 要求 | 路径 |
| --- | --- | --- |
| INSTALL/STATUS、ACTIVATE/STATUS | RFC-0001 `0x0101..0x0107` control subtype；mTLS principal + exact operation/instance/target-scope AuthZ + direct authority read | cold control |
| `ADD_NORMAL=0x0201` | distinct mandatory data subtype；60-byte ledger context + local normal admission | hot data |
| `ADD_RECOVERY=0x0202`、grant/close/status | distinct data/control subtype；RepairIntent/grant/range + bounded local grant | recovery data/cold control |
| READ/FENCE/LAC `0x0203..0x0206` | instance-aware等价语义；否则install/call-time capability reject | data/control |
| FORCE/LIST/storage introspection | 显式支持或明确 capability reject | control |
| `RANGE_READ=0x0301`、`BATCH_RECOVERY_ADD=0x0302` | 当前manifest reserved/disabled，不advertise capability、不接受body并返回UNSUPPORTED | data |
| TOMBSTONE/DELETE consumption | instance/incarnation/generation bound control subtype | cold control |

上述subtype、fixed context与status class是Round 7 executable test manifest，不是stable production wire；在真实old decoder/raw corpus PASS与control tail闭合前保持BLOCK。Profile normal/recovery/Classic不能共享会被旧decoder忽略的optional语义。

## 5. Routing、install 与 activation

Segment Bookie 必须消费 RFC-0001 的 authoritative local route：

- `CLASSIC` route 不能由 Segment data path 解释；
- `PROFILE` route 必须匹配 ledger instance、36-byte descriptor identity、Engine 和 protected 20-byte data credential；
- normal profiled Add 只有在匹配的 global READY authorization 和 durable local normal activation 存在后才能继续；
- `TOMBSTONED` route 永远不能重新进入 writable；
- restart 后的接受集合不能大于 durable route/install/activation 授权集合。
- INSTALL/ACTIVATE等冷控制只走独立`bookie-profile` immediate-TLS1.3/mTLS listener，来自non-anonymous X509 principal且通过exact operation/ledger instance/target scope authorizer，并由Bookie direct-read exact committed cluster authority后才可写本地状态；AuthN-only/coarse OU role、自述generation、registration hint或master key都不能替代该验证，AuthN/AuthZ早于route/credential/allocation/durable effect；
- local protected auth binding与semantic descriptor在同一instance内immutable，receipt只暴露secret-free identity/result。

同一个 ledger instance 的 local authority 不能压成互斥 flat role enum：normal admission、bounded recovery grants与committed-readable range可以正交存在。逻辑形状至少表达：

```text
LedgerRouteAuthority {
    routeClass: ABSENT | CLASSIC | PROFILE | TOMBSTONED
    ledgerInstance/descriptorIdentity[36]/Engine
    credentialKind=BK_MASTER_KEY_SHA1 + protected credential[20]
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

recovery grant和committed-readable都不隐含normal writable；active grants/range facts有manifest hard cap、snapshot/compaction和超限fail-closed。logical descriptor/credential/control tuple已由RFC-0001冻结；local physical owner、role index/record packing、at-rest protection与general recovery error API仍BLOCK。

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

本 RFC 锁定`ProtectedProfileStateStore`形状的logical ordered conditional durable transition interface，但不选择dedicated Bookie-level control log、扩展Classic Journal、独立state store或reserved control arena。Per-Arena `ArenaControlLog`不当然拥有跨Arena的ledger route；若语义拆到多个store，中间态必须fail closed，任一authority缺失不能default allow。exact physical owner、crash record framing与at-rest protection是Round 7后仍阻塞stable on-disk和Segment ACK的frontier，必须由Spike restart、write amplification与p99数据闭合。

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

normal Add使用RFC-0001 `ADD_NORMAL=0x0201`与60-byte ledger context；mTLS/HELLO只在Profile连接建立时完成。route gate早于HandleFactory/lazy storage create，bounded handle-state lookup constant-time比较缓存的36-byte descriptor identity与20-byte verifier，capture current route/admission generation，要求normal-active且非fenced/tombstoned，沿RFC-0003 allocation+payload durability，并在完成时服从captured admission order。route/activation/fence generation可以缓存进handle，但不能只在handle创建时检查；transition必须推进generation使stale handle fail closed。普通Add不解析/重算descriptor/hash或auth-binding hash/HMAC，不携带Engine/capability vector/READY/target/certificate，不读MetadataStore/sidecar/remote assignment，不做KMS/signature/certificate验证，不写control record或等待per-Add control fsync。

## 7. Recovery Add

Recovery Add使用RFC-0001 `ADD_RECOVERY=0x0202` executable body，携带ledger context、RepairIntent ID/generation、grant generation、range、entry与20-byte credential；它不预设一条独立持久 `RECOVERY_ADD` control record。它必须：

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

第一层必须落在受支持stock old binary启动时**必然读取且在任何Profile storage open、Journal replay、Arena writer、handle/lazy storage、registration之前确定拒绝**的mandatory path。只增加Cookie optional property/version、registration property、Arena/superblock文件、unknown negative Journal meta-entry、文档/client placement或启动后demote都不足。

Round 7对同BookieId/同storage scope给出明确`BLOCK`。源码只提供一个Spike B candidate：把metadata Cookie与每个required current/VERSION的第一行改为nonnumeric sentinel `"BKPF1\n"`，随后是`recordLength:u32 + BookieFormatRecord + CRC32C:u32`；metadata使用versioned CAS，本地通过temp write → fsync temp → atomic rename → fsync parent发布。CRC32C只做corruption detection。该candidate不能进入最终合同，因为尚未证明全部stock old binary，data-integrity模式会在Cookie validation前创建`LedgerStorage`，现有local VERSION不是atomic publication，physical Segment owner未选，也未证明所有启动/tool/storage-expansion入口或已运行旧进程都被挡住。

如果same-scope Gate不能证明，当前安全fallback立即锁定为：

```text
new BookieId
+ new journal/ledger/index/Arena storage roots
+ new storageIncarnation
+ old service无法访问的独立OS/service credential或ACL scope
+ separate bookie-profile endpoint/readiness
```

旧BookieId/storage必须drained、readonly或decommissioned；Profile placement只选择matching readiness的新BookieId。不能只改目录名而让旧binary/service account继续有写权限，也不能把ordinary ensemble replacement冒充wipe/decommission proof。新scope unknown/corrupt format仍non-writable，不允许in-place rollback到旧scope。

Bookie compatibility/readiness record逻辑上绑定stable ID、storage incarnation、active Engine、Profile wire discriminator、descriptor hash suite、mandatory local features、device-manifest identity/generation、local format generation、effective delete-assignment generation、minimum reader/writer及migration state/generation。device/Arena superblock另行绑定incarnation、device identity、Arena/control/checkpoint format与generation；它不能替代Bookie fence。

cluster persistent readiness record绑定Bookie ID/incarnation、Engine、protocol/capability、local format、device manifest、effective assignment、local readiness、minimum reader/writer和READY state。新增domain-specific interface：

```text
ProfileRegistrationStore.read(bookieId)
ProfileRegistrationStore.compareAndSet(
    bookieId,
    expectedStoreVersion,
    expectedReadinessGeneration,
    newRecord)
```

BookieServiceInfo ephemeral registration只发布`bookie-profile` endpoint、readiness generation/reference与capability hint。顺序固定为persistent readiness CAS → matching service info → existing ephemeral writable registration；response loss重读两者，generation/incarnation mismatch立即demote/non-writable。backend exact path/schema/escaping与adapter保持BLOCK，readiness不成为per-Add lease。

restart/startup hook必须放在`EmbeddedServer`创建任何可能触碰Profile/Segment storage的组件之前，顺序锁定为：

1. 读取并验证compatibility fence；
2. 校验BookieId/storage incarnation/Engine/migration state；
3. 校验device manifest；
4. 校验每个required device/Arena superblock；
5. missing/corrupt/unknown/partial mismatch进入non-writable quarantine；
6. 恢复ArenaControlLog/checkpoint/allocator与relocation selector；
7. 恢复route/credential/activation/fence/grant/readable/LAC/tombstone；
8. apply delete assignment snapshot+complete suffix并完成reconciliation；
9. durable local readiness；
10. persistent readiness CAS；
11. ephemeral registration；
12. 最后开启Profile write acceptance。

same-scope candidate migration只能按drain旧writer/connection → exclusive storage lock → cluster CAS PREPARED/old-binary sentinel → 逐required directory原子local sentinel → per-device superblock → local authority recovery → FORMAT_READY → persistent readiness → Profile endpoint registration执行；任一crash都必须让old binary被第一道fence挡住、新binary non-writable，不能把missing marker猜成Classic/new disk。Spike B PASS前这是candidate，不是production contract，也不要求跨device transaction。

新Profile scope出现任一local success、route/install/activation、fence/grant/tombstone、Arena allocation/control或durability-unknown outcome后，old binary都不能再解释同一scope。恢复只允许roll-forward、verified export/rebuild、irreversible wipe/decommission或new-incarnation rejoin；exact reverse/wipe CLI继续BLOCK。required device移除也必须由cluster-authorized新incarnation/device-manifest generation完成。

## 10. 性能边界

必须保持：

- normal Add 不新增 per-entry control-log fsync；
- normal Add 不远程读取 MetadataStore；
- route/activation/fence检查为有界本地lookup并尽量与handle state合并；normal Add只解析fixed frame/context并constant-time比较36-byte descriptor identity与20-byte verifier；
- descriptor canonicalize/hash、control authority direct-read、compatibility/registration validation与capability negotiation只在create/control/connect/startup执行，不进入normal Add；
- normal Add auth-binding hash/HMAC/KMS/signature/certificate invocation为0，只做缓存固定长度identity/verifier comparison；
- TLS1.3 record AEAD、Profile固定约60–100 bytes request identity/header与local state lookup是新增data-path成本，必须与Classic endpoint分开benchmark；Classic decoder/pool不承担Profile handshake或framing成本；
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
26. same BookieId/storage scope在真实stock binary与pre-storage-open Gate通过前保持BLOCK；candidate `BKPF1`不能冒充accepted format。
27. same-scope无法证明时必须使用new BookieId/new storage roots/new incarnation/new credential scope，旧service不得打开或写新scope。
28. persistent readiness CAS先于ephemeral writable registration；BookieServiceInfo endpoint/hint不能代替generation/incarnation匹配。
29. startup compatibility hook早于任何可能触碰Profile storage的component construction，而不只是早于Journal replay或registration。

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
- RFC-0001 Round 7 exact executable frame/HELLO/normal/recovery subtype、legacy flag伪造、unknown subtype、v3→legacy fallback、status/retry/durable-result propagation与Profile/Classic pool isolation；
- fence admission cut/pre-cut Add、multi-store fail-closed、multi-Arena route owner与Bookie registration response-loss/stale generation；
- non-anonymous且exact operation/instance/target scope authorized control principal、authenticated-but-unauthorized负向路径、direct-read exact READY/membership/RepairIntent、target/incarnation/purpose binding与secret-leak regression；
- 独立immediate-TLS1.3/mTLS Profile endpoint、exact-scope authorizer、credential redaction，以及normal Add auth-binding hash/HMAC/KMS/signature/certificate invocation为0；
- 真实stock old binary在mandatory compatibility fence上于Profile storage open/Journal replay/registration/write前退出；Round 7 `BKPF1` metadata/local Cookie candidate、data-integrity pre-storage-open file/byte instrumentation、auto-stamp、unknown Journal record与registration-only负向候选均被否证；candidate失败时new BookieId/new scope fallback成为当前唯一路径；
- 全startup order、crash-at-each-migration-boundary、partial device、superblock corruption、device-manifest change、registration CAS、rollback positive/negative proof与new-incarnation路径；
- cold/warm startup latency、compatibility/device/readiness read bytes与I/O count、required-device scaling及Classic-only baseline原始证据；normal Add中的format/readiness read或remote I/O为0；
- local control resident memory/active grant/waiter hard bounds，以及normal Add local lookup/lock/CPU/p99；
- Model A/C 中 Segment Bookie state 与 allocator state 的组合无 safety counterexample。

RFC-0005 未 Accepted 前，RFC-0003 只能解锁 Segment shadow writer，不能解锁 Segment ACK authority。RFC-0005 Accepted 也只是 canary 的必要前置；仍需 canary-specific evidence 与所有实际启用路径的依赖闭合，才能执行对应 ACK authority canary。

## 13. 开放问题

- logical local authority的exact physical owner：dedicated control log、existing Journal extension、small state store或reserved control arena；该项是stable on-disk与ACK authority的BLOCK；
- durable state 的 exact record set、format/version、packing、checksum、snapshot/rotation 与 group-commit 边界；
- protected `credentialKind=1 + 20-byte verifier`、route/activation逻辑绑定已冻结；physical packing、at-rest protection与secure deletion仍BLOCK；
- initial/replacement exact purpose与direct-read语义已冻结；physical record/authority-reference packing仍BLOCK；
- normal admission、bounded recovery grant/range index、committed-readable fact、idempotency summary的exact packing/caps/compaction与error mapping；
- fence 与 inflight Add 的精确线性化实现；
- explicit LAC 与 payload block 的写序；
- read/LAC/list capability matrix；
- Engine identity 在 cookie、directory layout、superblock 和 registration 中的编码；
- same-scope old-binary-visible compatibility fence当前BLOCK；`BKPF1` exact candidate只属于Spike，仍需minimum reader/writer matrix、pre-storage-open证明与Cookie auto-stamp隔离；
- device/Arena superblock与Bookie/device manifest的exact关系、partial migration状态和required-device removal tooling；
- migration/reverse/wipe CLI、stock old binary版本矩阵与rollback negative-proof receipt；new BookieId/new roots/new credential scope fallback语义不再OPEN；
- local seal 是否确有需求；若有，其 authority 与 metadata CLOSED 的关系；
- RFC-0003 `MOVE_COMMIT` exact local record packing、batch completion、reader cutover接口、orphan GC 与 cross-Arena unsupported 后续协议；
- RFC-0004 delete stream topology、assignment/snapshot schema 与 exact local cursor packing；
- `ProfileRegistrationStore` backend exact path/schema/escaping/adapter与response-loss reconciliation实现；persistent-before-ephemeral顺序已冻结；
- recovery strong assertion/local evidence binding、accepted loss ordering与五类 outcome到现有Bookie API/error的exact dependency mapping；
- performance Gate 的 exact thresholds。

这些问题关闭、相关 Spike/Model 通过前，本 RFC 不得标为 Implementation Ready。
