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

recovery grant和committed-readable都不隐含normal writable；active grants/range facts有manifest hard cap、snapshot/compaction和超限fail-closed。logical descriptor/credential/control tuple已由RFC-0001冻结；所选Bookie级control-log原型的物理验证、role index/record packing、at-rest protection与general recovery error API仍BLOCK。

### 5.1 最小原子 transition 与物理 owner 边界

以下语义必须在本地同一 conditional durable transition中绑定：

- `ABSENT -> CLASSIC`：route claim + Classic/master-key binding，早于payload/lazy handle创建；
- `ABSENT -> PROFILE_INSTALLED`：route + instance/Profile/Engine/auth + install generation + initial normal-inactive；
- normal activation：exact route/instance + target stable identity/storage incarnation + READY/membership activation generation + purpose + inactive-to-active；不能与initial install合并，initial/replacement purpose不能重放；
- recovery grant：exact route/instance + RepairIntent/generation + target/range scope + capability generation；
- recovery close/commit：对同一scope不可逆关闭recovery-write admission并发布committed-readable fact，或等价fail-closed有序transition；
- tombstone：exact instance terminal route，同时撤销normal admission和全部该instance recovery grants并拒绝read/write；
- Bookie registration readiness：storage incarnation + effective assignment generation + required-through满足证据。

cluster READY/standard membership先提交、local activation后消费；route/install先于Arena allocation；fence是独立单调transition。tombstone先关闭新admission，terminal effect durable且清理义务可重建后即可推进delete-applied cursor；reader/writer/I/O drain和free异步进行，不能成为该cursor前置。assignment按PREPARED/catch-up/local readiness/effective registration排序。这些只需条件化有序，不需要跨MetadataStore/Arena/payload的通用事务或巨型原子delete。

`ProtectedProfileStateStore`保留logical ordered conditional durable transition接口。下一隔离原型固定采用§5.2的Bookie级独立控制日志；这收敛此前四选一的原型路线，但不构成已接受的stable on-disk合同。record framing、at-rest protection和真实crash证据仍阻塞Segment ACK。

### 5.2 下一原型的真实持久化拓扑

状态：**Planned / Not Executed**。在new BookieId/new storage scope内实现：

```text
Bookie-level protected control log + A/B checkpoint
    route / credential / install / activation / fence / recovery grant /
    committed-readable / explicit LAC / tombstone / local delete cursor/readiness
per-Arena ArenaControlLog + allocator checkpoint
    allocation ownership / generation / relocation selector / retirement
Data Arena
    payload and recoverable record framing
```

Bookie级日志是跨Arena ledger权限的唯一持久化owner；不把相同route/fence/grant分别交给多个Arena决定。其conditional transition按bounded per-ledger顺序执行，物理append/fsync可跨ledger group commit；只记录控制事实，不重复记录DATA payload，不给normal Add增加控制日志fsync。实现前在Spike B manifest锁定record/batch边界、完整prefix、A/B checkpoint/rotation、credential protection及queue/waiter上限。

跨三层的执行顺序固定为：

1. durable route/install后才授予该instance的Arena allocation；normal activation是后续独立权限；
2. Add先取得Bookie级admission，再使用durable allocation，DATA durable且可读定位发布后才local success；
3. fence/grant close仍按各自合同关闭admission并终结所需操作；tombstone先关闭normal/recovery/read admission并durable，使清理义务可重建后可确认delete-applied/推进cursor，runtime drain完成后才确认access barrier，不能混为同一完成结果；
4. reader/writer/I/O均满足RFC-0003 quiescence条件后，才允许Arena free/reuse；
5. restart先恢复Bookie控制日志的terminal gates，再恢复各Arena authority/data/index，最后重验delete/readiness。任一required store缺失、损坏或无法分类，整个Bookie保持non-writable，不能由另一个store的部分成功推断allow。

Spike必须覆盖Bookie控制日志已durable但某Arena尚未完成、DATA durable但响应丢失、tombstone与迟到grant、多个Arena的部分失败，以及控制日志checkpoint/rotation各cut。内存adapter测试不能替代这些真实文件/fsync/restart结果；process kill也不能单独代表power-loss/torn-write覆盖。

## 6. Fence 与 Add

最低合同：

```text
normal Add local success
    => matching route/install/global READY/local NORMAL_ACTIVE
    && authentication accepted
    && ledger was not durably fenced before Add authorization
    && RFC-0003 allocation authority durable
    && payload durability barrier complete
    && batch within the recoverable contiguous physical durable prefix
    && same-coordinate payload identity accepted
    && authoritative point-read location published

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

这里explicitly failed只终止该Add的normal-success资格，不证明底层I/O未写入或已终结；已提交坐标及共享batch按§6.1继续跟踪。合法recovery重试重新验证自身grant与当前物理结果，不能把原normal RPC的fenced/cancelled结果当作DATA不存在或永久不可恢复。

normal Add使用RFC-0001 `ADD_NORMAL=0x0201`与60-byte ledger context；mTLS/HELLO只在Profile连接建立时完成。route gate早于HandleFactory/lazy storage create，bounded handle-state lookup constant-time比较缓存的36-byte descriptor identity与20-byte verifier，capture current route/admission generation，要求normal-active且非fenced/tombstoned，沿RFC-0003 allocation+payload durability，并在完成时服从captured admission order。route/activation/fence generation可以缓存进handle，但不能只在handle创建时检查；transition必须推进generation使stale handle fail closed。普通Add不解析/重算descriptor/hash或auth-binding hash/HMAC，不携带Engine/capability vector/READY/target/certificate，不读MetadataStore/sidecar/remote assignment，不做KMS/signature/certificate验证，不写control record或等待per-Add control fsync。

### 6.1 同坐标写入、幂等与读可见性

normal与recovery写入共享同一个`(ledgerInstanceId, entryId)`的冲突判定；recovery grant不授予覆盖不同payload的权限。logical payload identity绑定不可变应用数据及必要entry语义字段，传输完整性校验与逻辑重复判断分开。不能直接比较整个BK entry或将其digest当作不可变payload identity。

源码依据：[`LedgerRecoveryOp`](../../../bookkeeper-server/src/main/java/org/apache/bookkeeper/client/LedgerRecoveryOp.java)读取应用数据再调用`asyncRecoveryAddEntry()`；[`PendingAddOp`](../../../bookkeeper-server/src/main/java/org/apache/bookkeeper/client/PendingAddOp.java)用当时的`lh.lastAddConfirmed`重新打包；[`DigestManager`](../../../bookkeeper-server/src/main/java/org/apache/bookkeeper/proto/checksum/DigestManager.java)的完整性校验覆盖`ledgerId / entryId / lastAddConfirmed / length / application payload`。合法重写可具有不同piggyback LAC与digest，不能因此误报不同业务数据。

首批按RFC-0001 §5.4只接收安装时绑定的CRC32C布局。普通/恢复DATA operation内的BK entry表示为以下big-endian字段，不含Classic RPC header，instance由外层60-byte context绑定：

```text
offset  0.. 7  ledgerId
offset  8..15  entryId
offset 16..23  piggyback lastAddConfirmed
offset 24..31  cumulative ledger length
offset 32..35  CRC32C, 4 bytes
offset 36..   opaque application payload
```

CRC32C按现有[`CRC32CDigestManager`](../../../bookkeeper-server/src/main/java/org/apache/bookkeeper/proto/checksum/CRC32CDigestManager.java)/DigestManager规则覆盖32-byte metadata及应用payload，跳过digest本身。先检查完整frame/entry长度、outer/inner ledger与entry坐标、route/auth/grant及输入CRC，再进入逻辑重复判定；有效entry大小包含36-byte BK overhead，还受存储record/block与总预算约束。安装时缓存布局参数，普通Add不重读descriptor或逐请求协商digest。

[`MacDigestManager`](../../../bookkeeper-server/src/main/java/org/apache/bookkeeper/proto/checksum/MacDigestManager.java)用`genDigest("mac", password)`初始化HMAC，master key则由`genDigest("ledger", password)`产生；20-byte credential不能提供HMAC校验能力。首批不支持HMAC/CRC32/DUMMY等未声明布局，创建/安装明确拒绝，不静默改用CRC32C，也不下发password/MAC key。已有任意byte-array wire corpus仍只证明frame/operation解析，不证明新entry语义校验；B19在实际实施时增加独立布局/CRC向量，不重绑历史receipt。

原型先冻结以下比较边界，再实现B19回归：

| 字段 | 逻辑重复/冲突规则 |
| --- | --- |
| ledgerId、ledgerInstanceId、entryId | 验证route/context与坐标，跨instance绝不幂等命中 |
| 应用payload长度及bytes | 同坐标必须相等；不同数据不能覆盖 |
| BK `length`累计ledger长度等不可变entry语义 | 显式纳入一致性验证；`length`不等于本entry payload长度，不能作为LAC一样忽略。恢复重写必须重建相同值 |
| piggyback `lastAddConfirmed` | 每份输入按其完整性合同校验，但可合法变化，不参与业务payload冲突；沿既有LAC authority/单调性规则处理，不能由重复Add擅自推进LAC |
| BK digest、target、request/attempt、delivery generation及normal/recovery标志 | digest用于校验各自输入，其他字段用于权限/投递检查；不把封装差异当成payload差异 |

每份输入先完成所需route/auth/grant及固定CRC32C布局的完整性验证，再做逻辑比较；接受合法LAC差异不能放宽corrupt frame/digest的拒绝。同数据重复沿用已保存的自洽entry表示，不为刷新LAC重写DATA。CRC32C相等不证明业务数据相同，fingerprint只筛选，重复/冲突慢路径在需要时核对实际bytes。比较实现及数值上限写入prototype manifest，历史wire corpus不变。

职责分开：BK CRC32C验证当前entry封装，Segment record/block checksum检测存储framing/媒体损坏，TLS与credential/grant负责连接/访问授权；三者不能互代，Bookie不解析Kafka/Pulsar消息内容。正常新写复用现有CRC32C与必要Segment校验，不新增第三套逐entry SHA-256、凭据派生或远程核验。

```text
local route/auth/admission check and capture generation
    -> reserve bounded pending slot for (instance, entryId, payload identity)
    -> use durable authorized allocation
    -> complete batch writes and the exact covering DATA durability barrier
    -> enter the recoverable contiguous physical durable prefix (RFC-0003 §6.1)
    -> preserve physical location under matching storage generation/selector
    -> check each entry's current operation admission and publish eligible readable/local success
    -> client current ACK set and ordered-prefix completion (RFC-0001)
```

- 同坐标相同payload的并发重试复用pending result，waiter/bytes有hard cap；已有durable且可读结果时幂等返回，不另写覆盖副本。权限校验仍先执行，已tombstoned的重试不能返回旧成功。
- 不同payload返回明确conflict，不能覆盖已有pending winner或durable payload；crash后从有效DATA/selector重建去重事实，不能以易失pending表丢失推断坐标为空。
- 普通新entry利用已有热尾状态与索引定位，不强制每entry读RocksDB或额外计算SHA-256；并发同坐标请求复用有界pending，仅可能命中已有坐标时进入数据核对慢路径，复用现有派生索引，不建全量去重数据库。缓存fingerprint不能单独替代必要数据相等验证；`entryId <= localLastEntryId`也不证明坐标存在，必须保留乱序、hole和`E>W`的分布语义。
- DATA durable而locator未发布时不能local success；符合instance/权限的点读在成功后必须能找到数据。尚未覆盖的rebuild范围返回not-ready/transient，不能返回确定`NoSuchEntry`。
- DATA batch虽已durable但前一物理batch仍有缺口时，同样不能local success；physical sequence不是entryId/LAC，不改变逻辑quorum与连续entry前缀的定义。
- locator可在内存/可重建索引发布，不要求RocksDB独立fsync参与ACK。它必须与DATA、generation和current selector一致；普通client read仍服从BookKeeper LAC/可见性合同，local success不提升客户端LAC。
- fence/tombstone竞争沿captured admission order终结；已durable但未成功的数据不能仅凭callback丢失当作free。冲突隔离、orphan回收和极晚重试使用有界authority proof，不保存无界request history。

物理batch table与逐entry pending各自维护完成事实。batch只共享写入、barrier和buffer生命周期，不提供跨entry/ledger事务原子性；完整物理结果不由RPC取消、断连、fence拒绝或回调失败决定。进入可恢复durable前缀后，每条entry分别按captured admission-generation、fence/tombstone、去重结果和定位条件发布成功。例如同批L1的Add被fence顺序明确终止而L2仍合法，batch的完整durable事实保留，L2可以成功，后续物理前缀不被L1的逻辑失败卡住；batch durable也不能替L1跳过权限检查。原Add不能成功不妨碍保留storage generation/selector有效的候选locator；它不授予read权限，读取仍走§8的独立检查。

已提交batch不能因取消某条请求而修改block、移除record或提前释放共享buffer。未提交且最终为空的合批容器不分配永久物理序号；在封包并进入提交流程时才按RFC-0003 §6.1分配。fence后未回成功的有效DATA仍可能是恢复候选，tombstone后的DATA按删除合同回收；两者不能仅因原callback失败便删除。沿用现有locator/index保存物理位置并消费权限/恢复上下文，不建立两套全量索引。

请求响应等待终止与写入结果终止分开：

| 所处阶段 | 坐标占位与退出规则 |
| --- | --- |
| 已证明没有提交任何DATA | 在同一准入/封包执行顺序内撤销pending并释放实际不再持有的资源，不能与submission竞争后仍按未提交处理 |
| 已提交、部分写入或durability unknown | timeout/cancel/disconnect只终止该调用的响应等待；不删除坐标已有或可能写入的事实。相同数据重试绑定已有结果或返回明确暂不可用，不同数据不能成为第二winner；无法比对时fail closed，不猜absence |
| 已确定完整durable、在可恢复前缀且可读 | 继续参与同坐标冲突检查；当前操作权限有效时幂等返回已有结果，是否向原调用者成功发过响应无关 |
| 无法按既定I/O合同解析的写错误/unknown | 按RFC-0003 §6.1暂停受影响stream/file范围的新DATA准入及跨缺口成功，等真实I/O终结，再释放无使用者的buffer；不可写gate承接未解析义务后才可收缩pending，不无限保留每次重试对象 |

不可写范围必须覆盖该坐标原有和可能迟到的写入，不能通过换shard/stream重新接受冲突写；重启或恢复完成前不重新开放。隔离状态及解析依据消费已有storage generation、allocator/控制恢复事实，不新增逐entry成功回执日志。仅原调用超时而I/O仍可正常完成时，继续跟踪该物理结果，不将其误报为物理失败。

接受场景必须包含相同/不同payload并发、normal/recovery交叉重试、合法LAC/digest变化、相同payload但累计length冲突、corrupt digest、乱序/holes/`E>W`、DATA与locator间crash、ACK后立即合法点读、index全删恢复、stale handle与delete race；补充同批L1 fence/L2成功、提交后timeout再到不同payload、batch 11 unknown/12已完成及隔离恢复。分别记录正常新写与重复/冲突慢路径的索引读、hash、分配和复制成本。

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

只读打开按RFC-0001 §6.4消费已安装的合法读取能力，不执行ACTIVATE或等待全E在线/normal-active。客户端确认可见范围、Bookie物理数据存在性和恢复取证范围是三种事实，不能合成一个readable水位：

| 层次/操作 | 边界与检查 |
| --- | --- |
| 客户端confirmed read | 由ReadHandle按已确认LAC或CLOSED metadata的final boundary限定请求；Bookie副本的陈旧local LAC不能替代该客户端边界 |
| Bookie合法点读 | 检查instance/descriptor、读取权限、tombstone、locator、allocation/generation和完整性；不得仅因`entryId > localLAC`返回不存在或拒绝必要物理点读 |
| 显式unconfirmed read与恢复点读取证 | 在各自合法上下文内允许读取LAC之后的可定位候选；读到DATA不表示已提交，恢复前缀由RFC-0004 §7.5取证、recovery Add和durable close决定 |

索引未恢复、定位覆盖不确定或storage状态unknown时返回not-ready/unknown，不能返回确定`NoSuchEntry`。已有恢复上下文/终态已证明不属于合法读取范围的数据，不因磁盘上仍有bytes而重新对外可见；也不能用副本local LAC猜出这种排除证明。normal fence关闭写准入，不关闭合法读取；terminal tombstone按删除合同关闭该instance的新读准入。恢复写仍需显式grant，不使normal权限复活。本节复用点读接口、已安装的本地权限及恢复上下文，不新增读协议或逐次MetadataStore访问。

源码依据：[`ReadHandle.readUnconfirmedAsync()`](../../../bookkeeper-server/src/main/java/org/apache/bookkeeper/client/api/ReadHandle.java)允许越过客户端LAC，[`LedgerHandle.readAsync()`](../../../bookkeeper-server/src/main/java/org/apache/bookkeeper/client/LedgerHandle.java)在客户端检查confirmed范围；[`BookieImpl.readEntry()`](../../../bookkeeper-server/src/main/java/org/apache/bookkeeper/bookie/BookieImpl.java)取得readonly handle后点读，入口不按local LAC截断。验证示例为local LAC=99、entry 100已达ACK quorum、writer在下一次piggyback前崩溃：恢复必须能读取100，再按证据判断结果，不能将其视为确定absence。

普通DATA durable completion只证明本地存储完成，不自行推导quorum LAC。LAC继续来自合法writer的piggyback/explicit LAC及相应恢复证据；explicit LAC沿既有控制持久化路径合批，同instance的合法单调更新可合并，但需满足其durable completion/覆盖合同。不得为了每条Add立刻对confirmed reader可见而新增逐entry控制日志fsync。

点读按RFC-0003 §9/15执行以下有界路径：

```text
check instance / descriptor / read authority / tombstone
    -> resolve existing locator
    -> acquire allocation/generation read pin and revalidate locator/selector
    -> use a matching validated cache entry, or read the target record's required aligned ranges
    -> verify storage envelope identity/length, coordinates/generation and BK entry CRC32C
    -> return only the target entry within the request's read contract
    -> release each buffer/pin after its last user or I/O has finished
```

首批支持独立record校验，不能为读一条entry强制读取整个大batch。长度/范围检查先于大buffer分配，冷读不越过合法allocation；相邻读可有界合并但不等未来请求凑批。慢客户端持有小slice而阻留大父buffer时，允许复制已验证的必要小范围，在没有I/O/零复制引用依赖后及时释放父buffer和pin；原分配的pin不必跟随独立副本的网络等待。复用有界cache/read资源，测量实际读取bytes/返回bytes、I/O次数、cache hit、copy bytes、父buffer驻留与pin持有时间，不以形式零复制牺牲内存或回收。

## 9. Delete 与 restart

Segment Bookie只消费RFC-0004已授权的instance-specific local tombstone/delete。普通cluster logical completion不等待全部Bookie屏障，旧reader在本地tombstone前仍可读；强访问撤销独立延期。本地先关闭normal/recovery/read admission并durable tombstone/可重建清理义务，再推进delete-applied cursor；reader/writer/I/O/pin终结及physical free随后异步执行。cursor/readiness不等待compaction，也不证明强屏障或物理释放；local free/reuse的drain、I/O隔离与durable generation bump不能省略。

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

Round 7对同BookieId/同storage scope给出明确`BLOCK`。源码只提供一个Spike B candidate：把metadata Cookie与每个required current/VERSION的第一行改为nonnumeric sentinel `"BKPF1\n"`，随后是`recordLength:u32 + BookieFormatRecord + CRC32C:u32`；metadata使用versioned CAS，本地通过temp write → fsync temp → atomic rename → fsync parent发布。CRC32C只做corruption detection。该candidate不能进入最终合同，因为尚未证明全部stock old binary，data-integrity模式会在Cookie validation前创建`LedgerStorage`，现有local VERSION不是atomic publication，所选physical Segment control-store原型尚未验证，也未证明所有启动/tool/storage-expansion入口或已运行旧进程都被挡住。

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
6. 先恢复Bookie级control log/checkpoint中的route/credential/activation/fence/grant/readable/LAC/tombstone及local cursor/readiness，恢复terminal gates但不开放服务；
7. 恢复各ArenaControlLog/checkpoint/allocator、relocation selector与DATA/index，再交叉验证Bookie级权限；
8. apply delete assignment snapshot+complete suffix中的terminal tombstone/可重建清理义务，按delete-applied cursor完成reconciliation并重建有界派生清理工作；不等待全部compaction/free；
9. durable local readiness；
10. persistent readiness CAS；
11. ephemeral registration；
12. 最后开启Profile write acceptance。

### 9.1 Wave 0 reference harness状态

既有reference harness把recovery消费为typed semantic facts；历史测试不证明本轮Bookie控制日志、delete-applied/cursor与physical reclaim分离或物理replay子顺序。新验证归Spike B B4/B6/B16及Model D，历史receipt/source lock不重绑。

截至2026-09-02，Wave 0在`bookkeeper-common`的独立`profile.startup` package已完成当时逻辑顺序的typed reference implementation与immutable receipt：compatibility、required-device/superblock、allocator/route/delete recovery、durable local readiness、persistent versioned CAS、matching service-info和ephemeral writable registration。reference adapters的17项普通测试覆盖response loss重读、CAS conflict、generation/incarnation mismatch demotion、stale registration、九个边界crash/restart、partial/missing/corrupt/unknown mandatory device、rollback拒绝及new-scope旧身份/credential拒绝；normal Add没有调用点，相关cold-path增量计数为0。

这只是`EXPERIMENTAL / NON-PROMOTABLE / NO AUTHORITY / DISCARDABLE`的semantic reference implementation。它不接入`EmbeddedServer`或生产registration，不定义physical Cookie/superblock bytes、backend path、OS credential/ACL修改、migration/wipe工具，不运行same-scope `BKPF1`或真实old-binary Gate，也不改变本RFC的Proposed状态、Spike B状态或Segment ACK authority BLOCK。

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
- terminal tombstone/可重建清理义务与delete-applied cursor可有序group commit；cursor不早于对应effect durability，不等待physical free或runtime drain。
- normal Add 不读取 repair receipt、loss ordering、delete assignment 或 cursor 的远程 authority；这些事实只在冷控制/restart/registration路径消费，不形成 Add-time lease。
- route/install/activation/fence/grant/tombstone/registration等冷transition可group commit；active grant/range与idempotency summary有hard cap，不形成unbounded per-ledger state。
- compatibility fence、device manifest/superblock、recovery与readiness只在startup/migration/registration读取；必须按cold/warm、device count记录phase latency、read bytes与I/O count，并与Classic-only startup匹配比较，exact threshold保持OPEN。

所有 exact batching、record packing、cache layout 和阈值由 Spike 决定。不能用“正确性”作为无测量增加热路径 fsync、网络 hop 或全局锁的理由。

### 10.1 第一批可测的最小存储路径

状态：**PLANNED / NOT EXECUTED**。保留§5.2三层持久化owner，首批采用固定shard与shared DATA block；dedicated hot extent按RFC-0003 §8有条件延期。普通Add执行路径：

```text
ByteBuf fixed-header/context parsing and bounded admission/local checks
    -> route ledger to a fixed append shard
    -> check/reserve coordinate using hot state and bounded pending
    -> seal a nonempty batch, assign physical sequence and freeze all bytes for submission
    -> complete full writes into unused preallocated DATA ranges, then the covering barrier
    -> advance only the recoverable contiguous physical durable prefix
    -> preserve physical locations and check each entry's selector/admission/pending result
    -> publish eligible entries' local success independently
    -> release each buffer when its last user/I/O has finished
```

Bookie控制日志提前建立install/activation等条件，Arena控制日志提前建立pool allocation；普通已准入新写不逐条穿越三套队列、线程切换、锁或持久化future。pool不足时走有界refill/背压，不能把先决authority省掉；控制变化仍与captured admission/fence/tombstone order一致。

- shard合批按bytes/count及最老请求的单调时钟deadline触发，新请求不重置等待；到期padding提交。DATA submitted后不可改写，下一批用新范围/有界buffer。按真实覆盖barrier计量entry数，不将write CQE当durable或每entry force称为group commit；物理前缀发布与恢复按RFC-0003 §6.1/9。
- ledger锁只保护短状态转换，不持锁等待磁盘future。固定shard是第一批执行模型，测量线程hop/queue wait/lock hold后再决定必要拆分，不新增调度框架。
- adapter保留reference codec作oracle；使用受控payload视图，允许一次集中对齐复制。每层不可变包装不得反复clone整份payload。所有权覆盖拒绝、取消、断连、retry和I/O错误；若已复制且无引用可及源数据可释放源view，I/O仍引用的对齐buffer必须等真实I/O终结才复用，取消future不等于I/O已终结。
- 热尾、pending、定位与权限状态尽量合并进现有handle/index；维持hard caps和hole语义，慢路径只处理实际需要核对的已有坐标。
- batch table管理物理结果，pending管理逐entry结果；单条fence/取消不污染同批其他entry或物理前缀。提交后的超时不释放坐标占位，无法解析的I/O走§6.1隔离/恢复，不以无界pending、任意跳号或重复sync掩盖异常。
- shard、物理stream和DATA文件映射按RFC-0003 §6.1在manifest冻结；首个文件切片在一Arena内共享预分配DATA文件，barrier成本与错误影响按实际文件计量，不宣称shard之间独立flush。
- 同时运行写入与回收，保留RFC-0003 §13.1的维护份额和空间保留；不能通过停止compaction改善p99。

准入预算贯穿整个请求生命周期，不只限制queue.size()：

| 持有阶段/场景 | 计费、释放与进展规则 |
| --- | --- |
| 待合批、已出队、已提交I/O、等待physical prefix/locator/响应 | 同一预算持续计入request、源/对齐bytes、inflight batch；只在真实资源释放或转移到另一已计费owner时归还对应credit，不按出队释放 |
| 复制、断连、取消、慢响应 | 源/目标同时存在则都计bytes；I/O仍在飞行则保留buffer/inflight费用，传输层持有的响应也必须有界，不能藏在future或executor队列 |
| I/O unknown与慢点读 | stream不可写gate承接未解析义务前不丢坐标保护；实际I/O终结后才释放不再使用的buffer。点读父buffer/独立副本/pin与cache均计费，小slice可受控复制以释放大父buffer |
| 普通block放不下的entry | 在统一总预算下走有界大记录批次，或按size/capability检查明确拒绝；声明可接受的最大entry必须存在可行路径，不能永远卡队首 |
| 重复/冲突核对旧坐标 | 复用有界读取执行资源，append shard主循环不同步等盘；pending/waiter和读取buffer计费，正常新写不因此多一次索引I/O |
| 持续热ledger与撤权请求 | 连接/ledger只设必要有界份额；为fence/tombstone保留控制执行容量及持久化预算，DATA满额仍能关闭DATA准入 |

实现复用现有shard/pending/buffer预算和请求对象，不新增通用调度平台。所有被接受的entry满足wire frame、36-byte BK overhead、Segment record/framing/alignment及runtime总预算的兼容上限；资源不足明确可重试背压，不先接收再无界积压。

首个isolated/discardable性能切片可与实际启用路径的Model A/C及必要D子集验证并行，不必等待所有延期功能模型。切片先用真实control/DATA I/O、一个Arena及固定shards完成normal Add、点读、基础restart和本地回收；多Arena、全量重建和完整故障/资源Gate逐步补齐，未覆盖范围不得进入canary。无真实网络的shard测试不能证明transport/TLS成本，无真实恢复的性能run不能证明WAL可恢复性。

Spike B B18/B19固定硬件、I/O模式、stream/file映射、durability、TLS范围、E/W/A、payload分布及负载，分别测低负载延迟、目标负载吞吐/尾延迟、过载有界拒绝、写入与回收并行、restart/fault recovery。报告allocation/copy bytes/entry、CPU/entry、吞吐/p99、按文件计的entries/barrier、padding/实际磁盘写放大、debt，以及write/durability/physical-prefix/locator wait和全阶段资源峰值；补充点读bytes放大/I/O/cache hit/pin时长、大父buffer与大量hot ledger内存。首批shared-block结果不包含dedicated收益；replacement或真实网络未执行时单列NOT_EXECUTED，不把局部无网络结果当端到端增益。

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
- §5.2真实Bookie控制日志与多个Arena的partial durability/restart、checkpoint/rotation及tail分类；
- §6.1同坐标去重/冲突及LAC/digest兼容边界、DATA durable到locator publication间的crash、ACK后点读与全部index丢失；
- §8以LAC=99/entry 100验证confirmed、unconfirmed及恢复点读分层，Bookie不按local LAC截断，DATA completion不推导quorum LAC或增加逐Add LAC fsync；
- 同批L1 fence/L2合法、已提交timeout后同坐标不同payload、batch 11 unknown/12已完成、共享文件错误与恢复；逐entry结果不污染物理前缀，异常pending及buffer有界；
- 大batch中单entry独立envelope/CRC校验及必要对齐I/O、坏length、cache/pin竞态与慢客户端小slice持有；首批shared-only及后续dedicated池的资源/性能结论分别计量；
- §10.1 ByteBuf生命周期、reference corpus等价、固定shard合批、分配/复制/CPU与真实durability指标通过B18/B19；
- 初始writer/只读/恢复打开分离，CRC32C布局/outer-inner坐标检查及unsupported digest在创建/安装拒绝；DATA不可改写、短写/barrier覆盖与physical prefix重建通过B2/B8/B9/B18/B19；
- delete-applied/cursor先于异步drain/free仍无遗漏或提前reuse，shared L1/L2顺序和清理队列丢失可重建；全阶段预算、最老deadline、大entry、慢重复核对和DATA满额时fence/tombstone进展有确定性测试；
- RFC-0004 admission cut、membership freeze、普通logical tombstone和本地grant/tombstone组合；延期strong publication/强撤权只在各自能力启用时验证，不作为首批前置；
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

- §5.2选定Bookie级独立控制日志原型的真实durability、跨Arena顺序和资源证据；physical owner路线已收敛，stable on-disk与ACK authority仍BLOCK；
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
