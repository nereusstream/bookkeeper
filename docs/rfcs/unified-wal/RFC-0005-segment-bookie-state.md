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

当前`DigestManager.generateMasterKey()`生成20-byte `SHA-1("ledger" || password)` verifier并随Add发送；它是data credential/verifier，不是 Profile control authority。原源码中`LedgerDescriptorImpl.checkAccess()` mismatch记录请求与缓存master-key完整字节数组的问题已在[定向红绿回归](implementation/profile-control/classic-master-key-log-receipt.json)中修复；`AuthDisabledPlugin`仍可让anonymous connection认证成功，Profile control必须另行拒绝。Profile INSTALL/ACTIVATE/repair/delete必须拒绝anonymous或authenticated-but-unauthorized control principal，并对日志、metric、receipt、exception完成secret-leak hard regression；不因此重设计整个Classic password/KDF协议，也不声称继承的SHA-1 verifier获得更强安全性。

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
- normal activation：exact route/instance + target stable identity/storage incarnation + 已按RFC-0001发布并持久留存target的READY/membership activation authority/generation + purpose + inactive-to-active；不能与initial install合并，initial/replacement purpose不能重放；
- recovery grant：exact route/instance + RepairIntent/generation + target/range scope + capability generation；
- recovery close/commit：对同一scope不可逆关闭recovery-write admission并发布committed-readable fact，或等价fail-closed有序transition；
- tombstone：exact instance terminal route，同时撤销normal admission和全部该instance recovery grants并拒绝read/write；
- Bookie registration readiness：storage incarnation + effective assignment generation + required-through满足证据。

cluster READY/standard membership先提交、local activation后消费；route/install先于Arena allocation；fence是独立单调transition。tombstone先关闭新admission，terminal effect durable且清理义务可重建后即可推进delete-applied cursor；reader/writer/I/O drain和free异步进行，不能成为该cursor前置。assignment按PREPARED/catch-up/local readiness/effective registration排序。这些只需条件化有序，不需要跨MetadataStore/Arena/payload的通用事务或巨型原子delete。

`ProtectedProfileStateStore`保留logical ordered conditional durable transition接口。下一隔离原型固定采用§5.2的Bookie级独立控制日志；这收敛此前四选一的原型路线，但不构成已接受的stable on-disk合同。record framing、at-rest protection和真实crash证据仍阻塞Segment ACK。

### 5.2 下一原型的真实持久化拓扑

状态：**局部隔离实现 / 整体未完成**。`INSTALL`与`ACTIVATE`的Bookie控制文件切片已有真实FileChannel写入、force、顺序apply及完整重放；受控`FENCE`先切断新准入，等待已准入票据标记物理终结后再force，重放可恢复fenced状态；隔离测试还覆盖真实normal DATA force与durable fence后关闭、重开Bookie/Arena控制日志并点读同步候选，尚非崩溃恢复或完整read gate。受控`TOMBSTONE`也已接入该控制文件：匹配实例在force前关闭normal新准入和在途结果资格，durable后建立terminal route，重放仍拒绝迟到INSTALL/ACTIVATE；错误实例不撤销当前route，同批其他ledger的DATA结果独立判定。隔离`ReadAdmission`现允许匹配安装实例在inactive或fenced状态下点读，并在pending/durable tombstone时拒绝新读，并使尚未判定结果的旧票据失败；受控点读入口在物理读取后复核本地route并释放票据。它只接受调用方提供的typed committed authority或受控fence凭据，尚未接真实MetadataStore、mTLS授权、Bookie服务或生产DATA成功路径。票据已接入隔离真实DATA FileChannel驱动，待物理I/O和owner回调都终结后才释放；候选发布后、票据释放前按entry复核捕获的准入generation，可返回隔离本地结果。Bookie控制owner驱动已将INSTALL/ACTIVATE与FENCE写入和force放到预留控制I/O服务，durable后回原owner顺序apply。隔离`EXPLICIT_LAC`现通过exact instance/credential及normal-active检查，作为独立有界控制记录write/force后apply；同值同body重试在fence后仍可解析为已持久，低值或同值不同body不覆盖现值，重开同步并重放，pending fence等待前序LAC barrier。当前LAC记录仍逐条force；现可由Bookie控制driver在原owner预留、控制I/O服务force、completion回owner后apply，未启动的I/O拒绝会撤销预留而不跳sequence。尚未接控制group commit、网络handler或confirmed-read可见性。在途同坐标normal重试已有受控合并；normal与重复请求提交后停止响应等待已有隔离切片；网络deadline/断连接线与完整请求生命周期、完整read权限/selector/pin、tombstone清理义务/delete-applied cursor、完整checkpoint/rotation及完整启动仍未实施。在new BookieId/new storage scope内的目标拓扑为：

隔离本地 `RECOVERY_GRANT` 现接受调用方提供的 typed committed authority 与 RepairIntent ID/generation、grant generation；实验性 domain-separated SHA-256 public payload identity 绑定 exact context、target/storage incarnation、domain 和 entry range。Bookie 控制文件先 force 再 apply，重开后重放有界 grant；局部 `ADD_RECOVERY` 准入逐项匹配 intent、generation、range/entry、credential 和未删除 route，不要求 normal activation，也不开放 normal Add。同操作冲突确定性拒绝，tombstone 撤销 grant 及未完成票据的结果资格。隔离 `RECOVERY_GRANT_CLOSE` 通过独立的close-purpose payload identity绑定原grant operation、intent/generation、目标与范围；先关闭该grant新准入，已取得的受控票据终结前返回WAITING，随后force并apply关闭记录，重放恢复局部committed-readable控制事实。grant/close 的物理写入与 force 现由预留控制 I/O 服务执行，结果回原 owner 顺序 apply；close 在提交 I/O 前切断新准入，未开始 I/O 的拒绝仅撤销日志预留，force 不确定则在本进程 fail-closed。重开按完整记录解析；tombstone再次撤销该事实。重叠grant仍计入有界名额；同一entry只要落在任一活跃grant内，旧close事实便不对该entry给出局部可读资格。隔离物理驱动现要求每张recovery票据绑定wire请求的context、entryId和完整BK entry bytes，复用DATA force、连续前缀与热定位；close在物理/owner终结前等待，cut后的结果返回stale，重开扫描仍能验证DATA候选。同坐标相同逻辑payload可经点读比对解析运行期重试，不同payload返回冲突；裸的重启扫描候选不返回 durable duplicate success；当前无MOVE/FREE的单Arena只读恢复比对要求唯一stream和坐标、连续前缀、DATA恢复同步及重放的exact grant，真实点读后复核grant，只可返回物理候选比对结果；旧DATA I/O错误尚无持久分类，不能据此报告durable duplicate成功。真实live RepairIntent冷读、mTLS AuthZ、完整网络请求生命周期及selector/实际读可见性仍未接线，不能据此授予生产recovery写入或读权限。

隔离子进程在recovery DATA本地持久结果后或DATA force前直接终止，重开后的Bookie/Arena控制重放和DATA恢复同步均能保留物理候选；这只验证进程边界下的本地恢复入口，不证明掉电、真实RepairIntent取证或生产ACK。

```text
Bookie-level protected control log + A/B checkpoint
    route / credential / install / activation / fence / recovery grant /
    committed-readable / explicit LAC / tombstone / local delete cursor/readiness / prepared assignment adoption
per-Arena ArenaControlLog + allocator checkpoint
    allocation ownership / generation / relocation selector / retirement
Data Arena
    payload and recoverable record framing
```

Bookie级日志是跨Arena ledger权限的唯一持久化owner；不把相同route/fence/grant分别交给多个Arena决定。其conditional transition按bounded per-ledger顺序执行，物理append/fsync可跨ledger group commit；只记录控制事实，不重复记录DATA payload，不给normal Add增加控制日志fsync。实现前在Spike B manifest锁定record/batch边界、完整prefix、A/B checkpoint/rotation、credential protection及queue/waiter上限。

历史（`00edeb8f`版本）2026-10-03 Bookie checkpoint局部实现：已冻结applied控制状态及真实字节cut，保留sealed INSTALL credential、operation条件结果、FENCE、最新LAC和recovery grant/close，以共享有界image codec及Bookie独立A/B第三页发布；CHECKPOINT_COMMIT须先durable/apply。完整验证/同步旧日志后，恢复匹配的image、检查条件结果并重放后缀；缺mandatory字节/密钥/identity仍拒绝。真实JVM边界、两个owner页面隔离、DATA图正常restart及有限权限模型已有回执，详见[局部实现](implementation/local-storage/README.md)。旧日志仍完整保留，rotation/长期幂等压缩/在线调度和完整Model A/Block G未完成。

历史（`43bfd80b`版本）2026-10-03 Arena增量已在健康CLOSE后显式rotation并回收自己的旧控制prefix，两个owner的DATA图经过两轮维护/restart仍可读、幂等及新追加；Bookie checkpoint页和完整旧Bookie日志保持原合同。Arena字节及恢复根由[RFC-0003 §11](RFC-0003-segment-storage-allocator.md#11-allocator-checkpoint-与-rotation)与[实验格式](implementation/local-storage/format-v1.md#arena日志代与双恢复根)拥有。此切片不完成Bookie rotation或在线维护。

2026-10-03 Bookie rotation增量：在无在途normal/read/recovery admission、控制I/O或未apply batch时，连续建立两个applied checkpoint，保留sealed credential、首次条件结果、fence/tombstone、最新LAC与grant/close；新物理文件及目录同步、两个恢复根force/验证后才回收旧prefix。固定Bookie sidecar锁及第三页独立于Arena；当前物理代及完整suffix仍必需，单image可回退，两image不可用则拒绝。真实短写/force失败、七个JVM退出阶段、三轮容量耗尽、慢I/O期间拒绝新票据、跨JVM锁和双owner DATA图两轮正常restart已有验证，见[本地实现](implementation/local-storage/README.md#bookie停写rotation与共享物理协议)及[字节合同](implementation/local-storage/format-v1.md#bookie日志代与双恢复根)。有限权限模型新增prefix回收投影，独立C模型检查双根/屏障；两者未组合为完整A+C。在线调度、长期幂等历史压缩、在线派生索引、真实掉电及完整Model A/Block G仍未完成。

2026-10-03 隔离restart另在新DATA OPEN及owner暴露前，由已同步且Bookie本地route选择的inventory重建真实RocksDB派生定位；fenced仍可读、tombstone不入库，未提交relocation copy不提升。覆盖以实际flush和独立页force发布，保留两控制cut及index generation；它不替代权限日志或DATA sync。正常关闭restart已按当前完整Bookie/Arena后缀验证真实覆盖：覆盖内仍验证批头/连续性，未覆盖授权tail完整解析及同步；无效覆盖全扫描。当前tombstone过滤覆盖行和新tail，不因旧index恢复权限。普通/恢复ENTRY_DATA现在在热定位与local result后，复用DATA票据和I/O服务异步追加派生定位；后台Put不推进持久覆盖，失败不撤销已成立的热结果，控制I/O仍独立预留。query-visible后热淘汰及hot-first/fresh Get的点读、普通/恢复重复比较已接线；相同票据跨Get/DATA阶段保留输入、Bookie准入及文件pin，终结时复核当前单stream前缀、grant和权限。有限坐标占用预算避免派生missing授权覆盖旧DATA，完整恢复容量独立于hot容量。运行期派生检查点已从所有原worker冻结当前连续DATA前缀/坐标cut并暂停Put，在真实flush后force覆盖页；此时Bookie applied cut只记录派生元数据，不要求所有新DATA停写，也不能代替严格recovery/read cut或授予权限。一般MOVE/FREE selector覆盖恢复、完整在线维护与生产readiness未完成。详见[RFC-0003 §15](RFC-0003-segment-storage-allocator.md#15-derived-index)。

2026-10-03，实际DATA游标已改按原成功allocation控制sequence/offset推进，后继grant可位于较低文件地址，进入后继后旧尾不可返回；restart已知grant全部跳过。恢复按每allocation连续区间及原控制顺序验证stream，并保持必要CRC/grant/DATA sync，checkpoint/rotation保留原operation序号。真实全量/覆盖辅助storage图与gap/duplicate回归通过，见[分配顺序证据](implementation/local-storage/README.md#分配控制顺序与低地址追加)。这是FREE/reuse必要前置，尚未授予generation bump、旧字节复用或整块回收authority。

2026-10-03 真实运行期storage图新增active memtable未flush、覆盖页force前、PERSISTED owner回调内及正常CLOSE后4个JVM退出点。无CLOSE的物理扫描/派生重建不解除Arena隔离，local-result restart及点读仍拒绝；正常CLOSE对照使用同代coverage并恢复真实读/重试/追加。全类140项通过，详见[进程边界证据](implementation/local-storage/README.md#运行期索引的真实进程退出边界)。只证明所列进程故障边界，不证明掉电或未关闭会话恢复。

跨三层的执行顺序固定为：

1. durable route/install后才授予该instance的Arena allocation；normal activation是后续独立权限；
2. Add先取得Bookie级admission，再使用durable allocation，DATA durable且可读定位发布后才local success；
3. fence/grant close仍按各自合同关闭admission并终结所需操作；tombstone先关闭normal/recovery/read admission并durable，使清理义务可重建后可确认delete-applied/推进cursor，runtime drain完成后才确认access barrier，不能混为同一完成结果；
4. reader/writer/I/O均满足RFC-0003 quiescence条件后，才允许Arena free/reuse；
5. restart先验证Bookie控制候选、完成必要恢复同步后恢复terminal gates，再按RFC-0003 §5.3/12处理各Arena条件命令、DATA候选及其同步/定位，最后重验delete/readiness。可读bytes或另一store的sync不证明目标DATA durable；任一required store缺失、损坏或无法分类仍non-writable。

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

durable fence response也不自动证明任意坐标为空。恢复点读的否定结果按RFC-0004 §7.3逐来源检查：本instance的normal fence已durable、target identity/incarnation及恢复上下文匹配，并且该坐标的pre-cut写入和定位覆盖已解析；否则返回not-ready/unknown。fence复用现有Bookie级控制事实，不新增逐entry控制记录或每Arena副本；restart重验该事实后才复用。

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

这里的cumulative length属于该entryId的应用数据前缀，不属于同记录的piggyback LAC，不包含BK/Profile/Segment framing或padding。恢复沿RFC-0004 §7.4保留提示来源并为最终P取得匹配L；已经读到相邻entry时检查累计关系和溢出，不能改length掩盖冲突，也不要求普通新写逐次读前驱。合法同payload重试仍按本节比较累计length，LAC变化不改变这个身份字段。

CRC32C按现有[`CRC32CDigestManager`](../../../bookkeeper-server/src/main/java/org/apache/bookkeeper/proto/checksum/CRC32CDigestManager.java)/DigestManager规则覆盖32-byte metadata及应用payload，跳过digest本身。先检查完整frame/entry长度、outer/inner ledger与entry坐标、route/auth/grant及输入CRC，再进入逻辑重复判定；有效entry大小包含36-byte BK overhead，还受存储record/block与总预算约束。安装时缓存布局参数，普通Add不重读descriptor或逐请求协商digest。

本地长度检查消费RFC-0001 §8统一`maxPayload=N`，取normal Add、single-entry Recovery Add、read response、Segment record/batch各自可容纳应用数据的最小值。`entryPayload`是完整BK entry，首批其长度为`36 + applicationPayloadLength`；frame内外长度及对齐范围另按自身开销验证，不能混用应用bytes、BK entry bytes和wire frame bytes。所有计算先检查溢出，N+1或任一路径无法表达的entry在首次写入/大分配前拒绝；read/record容量尚未冻结时不把normal单项余量当完整支持上限。

创建/安装验证ledger所需大小能力并缓存N，不新增逐Add协商或分片。资源暂满返回背压；运行时降低新写阈值不能降低既有合法entry的读取/恢复能力。当前实验normal/recovery frame单项差44 bytes的推导与N-1/N/N+1验收由RFC-0001 §8统一拥有，原wire corpus不重绑。

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
validate cached cross-operation payload limit and checked entry/frame lengths
    -> local route/auth/admission check and capture generation
    -> reserve bounded pending slot for (instance, entryId, payload identity)
    -> use durable authorized allocation and internally frame this Add as ENTRY_DATA
    -> complete batch writes and the exact covering DATA durability barrier
    -> enter the recoverable contiguous physical durable prefix (RFC-0003 §6.1)
    -> publish matching storage generation/selector locator in the bounded hot handle/index
    -> check each entry's current operation admission and publish eligible local success
    -> client current ACK set and ordered-prefix completion (RFC-0001)
asynchronously:
    -> bounded derived-index batch updates, periodic persistence and coverage checkpoint
```

上面仅是普通/合法recovery Add的完成分支。底层batch write/barrier/物理前缀由两种来源共用，但内部completion必须携带RFC-0003 §9的来源与操作身份：`ENTRY_DATA`才进入本节定位/local-success检查；`RELOCATION_COPY`只通知搬迁路径目标副本已durable，后续conditional MOVE成功apply并切换selector后才可查询，不直接调用普通hot-locator发布回调。来源写入受校验envelope，普通客户端不可伪造；MOVE后不修改DATA来源或提交位，不增加I/O引擎、payload副本或fsync。

- 同坐标相同payload的并发重试复用pending result，waiter/bytes有hard cap；已有durable且可读结果时幂等返回，不另写覆盖副本。权限校验仍先执行，已tombstoned的重试不能返回旧成功。
- 不同payload返回明确conflict，不能覆盖已有pending winner或durable payload；crash后从有效DATA/selector重建去重事实，不能以易失pending表丢失推断坐标为空。
- 普通新entry利用已有热尾状态与索引定位，不强制每entry读RocksDB或额外计算SHA-256；并发同坐标请求复用有界pending，仅可能命中已有坐标时进入数据核对慢路径，复用现有派生索引，不建全量去重数据库。缓存fingerprint不能单独替代必要数据相等验证；`entryId <= localLastEntryId`也不证明坐标存在，必须保留乱序、hole和`E>W`的分布语义。
- DATA durable而locator未发布时不能local success；符合instance/权限的点读在成功后必须能找到数据。尚未覆盖的rebuild范围返回not-ready/transient，不能返回确定`NoSuchEntry`。
- DATA batch虽已durable但前一物理batch仍有缺口时，同样不能local success；physical sequence不是entryId/LAC，不改变逻辑quorum与连续entry前缀的定义。
- local success前的readable locator由已有有界热handle/index发布，与DATA、generation和current selector一致；append shard不同步等待RocksDB Put/WriteBatch、flush或compaction。本次索引入库/持久化都不是成功前置，async backlog持续计入总预算。热定位在另一已计费且实际可查询路径接管后淘汰，不等flush，也不允许查询空窗。普通client read仍服从BookKeeper LAC/可见性合同，local success不提升客户端LAC。
- DATA durable-through与index persisted coverage cut按RFC-0003 §15分开；首批纯派生索引关闭WAL，以实际flush内容和匹配generation的durable checkpoint证明连续覆盖，未覆盖DATA重放。Put成功、查询可见或最后一个回调不能授权restart跳过扫描，也不能替代allocator/selector/tombstone authority。
- fence/tombstone竞争沿captured admission order终结；已durable但未成功的数据不能仅凭callback丢失当作free。冲突隔离、orphan回收和极晚重试使用有界authority proof，不保存无界request history。

物理batch table与逐entry pending各自维护完成事实。batch只共享写入、barrier和buffer生命周期，不提供跨entry/ledger事务原子性；完整物理结果不由RPC取消、断连、fence拒绝或回调失败决定。进入可恢复durable前缀后，每条entry分别按captured admission-generation、fence/tombstone、去重结果和定位条件发布成功。当前选择drain pre-cut：同批L1/L2都已合法准入时，L1随后开始fence仍等待L1完成，不能仅凭pending fence撤销L1的成功资格；L2独立完成。若另行启用§5的显式终止pre-cut Add路径，同批L1被fence顺序明确终止而L2仍合法时，batch的完整durable事实仍保留，L2可以成功，后续物理前缀不被L1的逻辑失败卡住；batch durable也不能替L1跳过权限检查。原Add不能成功不妨碍保留storage generation/selector有效的候选locator；它不授予read权限，读取仍走§8的独立检查。

已提交batch不能因取消某条请求而修改block、移除record或提前释放共享buffer。未提交且最终为空的合批容器不分配永久物理序号；在封包并进入提交流程时才按RFC-0003 §6.1分配。fence后未回成功的有效DATA仍可能是恢复候选，tombstone后的DATA按删除合同回收；两者不能仅因原callback失败便删除。沿用现有locator/index保存物理位置并消费权限/恢复上下文，不建立两套全量索引。

请求响应等待终止与写入结果终止分开：

| 所处阶段 | 坐标占位与退出规则 |
| --- | --- |
| 已证明没有提交任何DATA | 在同一准入/封包执行顺序内撤销pending并释放实际不再持有的资源，不能与submission竞争后仍按未提交处理 |
| 已提交、部分写入或durability unknown | timeout/cancel/disconnect只终止该调用的响应等待；不删除坐标已有或可能写入的事实。相同数据重试绑定已有结果或返回明确暂不可用，不同数据不能成为第二winner；无法比对时fail closed，不猜absence |
| restart扫描到完整且身份/CRC有效的候选，barrier证据尚未成立 | 保留坐标并按RFC-0003 §12完成必要文件恢复同步；不能仅凭locator/CRC对重试返回durable success，等待有界恢复或返回not-ready/unknown，不让冲突数据另成winner |
| 已确定完整durable、在可恢复前缀且可读 | 须有有效持久证据，或必要恢复同步已完成；继续参与冲突检查。当前权限有效才幂等返回，是否向原调用者发过响应无关；不能由重建index或readiness推断DATA durable |
| 无法按既定I/O合同解析的写错误/unknown | 按RFC-0003 §6.1暂停受影响stream/file范围的新DATA准入及跨缺口成功，等真实I/O终结，再释放无使用者的buffer；不可写gate承接未解析义务后才可收缩pending，不无限保留每次重试对象 |

不可写范围必须覆盖该坐标原有和可能迟到的写入，不能通过换shard/stream重新接受冲突写；重启或恢复完成前不重新开放。隔离状态及解析依据消费已有storage generation、allocator/控制恢复事实，不新增逐entry成功回执日志。仅原调用超时而I/O仍可正常完成时，继续跟踪该物理结果，不将其误报为物理失败。

接受场景必须包含相同/不同payload并发、normal/recovery交叉重试、合法LAC/digest变化、相同payload但累计length冲突、corrupt digest、乱序/holes/`E>W`、DATA与locator间crash、ACK后立即合法点读、index全删恢复、stale handle与delete race；补充同批L1/L2 pre-cut Add完成后L1 fence才durable、提交后timeout再到不同payload、batch 11 unknown/12已完成及隔离恢复。若启用显式终止pre-cut路径，另验同批L1失败/L2成功。分别记录正常新写与重复/冲突慢路径的索引读、hash、分配和复制成本。

2026-09-29局部实现边界：[隔离重复比较](implementation/local-storage/README.md)已复用独立CRC32C解析及真实DATA候选点读，验证同payload/累计length但不同LAC相同、payload或累计length变化冲突、坏CRC拒绝。受控normal批次现于物理候选发布后、票据释放前按entry复核准入generation；真实force期间只有L1 activation generation改变时，L1为`STALE_ADMISSION`、L2为`LOCAL_DURABLE`，两条物理候选均保留。在途同坐标normal重试由原owner比较冻结逻辑内容：相同内容等待原批次连续持久化及候选发布，冲突内容不发第二次物理写，失败等待者得到`UNKNOWN`；已发布运行期候选仍由有界I/O服务点读并复核Bookie准入与候选。仅由重启扫描恢复的物理候选返回`NOT_READY`，不能据此宣布absence或durable成功。normal与重复请求提交后停止响应等待已有隔离切片；网络deadline/断连接线、跨入口bytes预算、route/auth/selector及恢复可发布前缀尚未接入；当前drain pre-cut的同批fence顺序已有隔离验证，显式终止pre-cut分支未启用，步骤6整体仍未完成。

2026-10-02 启动全目录清单可在DATA writer持续排他下，以已重放Bookie本地route/tombstone筛除不可读实例的定位，并检查前后控制cut稳定、无未决控制/entry义务、各已观测stream连续及当前坐标唯一。重开读取不再要求shard只有一个stream，持读取pin和当前Bookie读票据点读唯一旧位置，完成前重验route及Arena applied cut；后续grant会使旧清单失效。同步中tombstone变化、坐标歧义、stream缺口及同步失败不返回交接结果。定位仍为非local-durable候选，未解析旧I/O错误不能据此返回durable duplicate，尚无动态写入定位、一般MOVE/FREE selector或完整Profile读取/服务级restart。

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

recovery authority 的集群来源、repair target、intent retention 与 delete discovery 由 RFC-0004 负责。Bookie local record 只消费该 authority，不复制 cluster schema。基础恢复复用Classic的write-set、fencing read、`W-A+1`聚合、顺序交付及write-back，按RFC-0004 §7.4适配Profile身份/grant/错误分类；recovery Add的写授权不替代读取来源的本地fence或absence证据。

2026-10-02 隔离逐entry recovery入口已复用普通写的合批、固定源/对齐池、deadline、按需ALLOC_POOL refill及assembling/pending/published重复坐标处理。先以现有Bookie准入验证exact context/credential/RepairIntent/grant/range，driver接管其私有wire payload快照后，在DATA预留前验证CRC、BK entry ID和固定shard；源budget按完整array容量计费且不再复制快照。合法recovery输入可无normal activation，普通与恢复来源也可同批，但normal captured generation与recovery grant/close/tombstone资格分别决定逐entry隔离结果；交叉重试不会绕过各自权限或创建第二winner。grant close等待期间恢复结果仍沿既有本地gate判为过期，normal pre-cut fence仍等待原票据终结。取消响应等待不释放storage/grant义务。此路径尚未接真实集群RepairIntent、完整wire status、接收credit/整体bytes预算或服务级restart，不形成生产ACK。

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

恢复所用missing还须满足RFC-0004 §7.3：当前合法source/write set、matching instance/descriptor和Bookie identity/storage incarnation、该source自身durable normal fence，以及该坐标无未解析pre-cut写入/定位覆盖。尚未fenced的source先完成现有Profile fence再取证；不能因其他副本已构成fencing coverage而计入其missing。热索引未命中、旧RPC结束、normal Add被failed或仅收到`NoSuchLedger`都不能跳过这些检查。重复响应只计一次，旧上下文响应不计；已验证且仍有效的本地fence可跨entry复用，无逐entry metadata或fsync，也不要求全E在线。普通合法读取沿上表执行，不因本节的恢复否定证据要求而额外fence正常reader。

源码依据：[`ReadHandle.readUnconfirmedAsync()`](../../../bookkeeper-server/src/main/java/org/apache/bookkeeper/client/api/ReadHandle.java)允许越过客户端LAC，[`LedgerHandle.readAsync()`](../../../bookkeeper-server/src/main/java/org/apache/bookkeeper/client/LedgerHandle.java)在客户端检查confirmed范围；[`BookieImpl.readEntry()`](../../../bookkeeper-server/src/main/java/org/apache/bookkeeper/bookie/BookieImpl.java)取得readonly handle后点读，入口不按local LAC截断。验证示例为local LAC=99、entry 100已达ACK quorum、writer在下一次piggyback前崩溃：恢复必须能读取100，再按证据判断结果，不能将其视为确定absence。

隔离实现当前只保存单调的explicit LAC数值及不透明body，返回前完成Bookie控制文件barrier；实验记录body为固定Profile context、8字节LAC、4字节长度及有界不透明内容，header的operation generation绑定本地准入generation，credential不落盘；force不确定时不向原调用报成功，完整记录可在重开同步后重放。局部查询只验证Bookie route，尚不定义客户端confirmed边界，也未合并DATA piggyback LAC。

普通DATA durable completion只证明本地存储完成，不自行推导quorum LAC。LAC继续来自合法writer的piggyback/explicit LAC及相应恢复证据；explicit LAC沿既有控制持久化路径合批，同instance的合法单调更新可合并，但需满足其durable completion/覆盖合同。不得为了每条Add立刻对confirmed reader可见而新增逐entry控制日志fsync。

点读按RFC-0003 §9/15执行以下有界路径：

```text
check instance / descriptor / read authority / tombstone
    -> lookup hot locator first and protect the object on a hit
    -> on hot miss, issue a fresh current-state derived-index Get after that lookup
    -> acquire allocation/generation read pin and revalidate locator/selector
    -> use a matching validated cache entry, or read the target record's required aligned ranges
    -> verify storage envelope identity/length, coordinates/generation and BK entry CRC32C
    -> return only the target entry within the request's read contract
    -> release each buffer/pin after its last user or I/O has finished
```

热定位与异步索引接管遵守RFC-0003 §15：写侧先query-visible再淘汰hot，读侧先hot、miss后才Get当前DB状态。fallback不复用早于hot查询的snapshot、iterator或negative-cache；不能先DB miss再hot miss后误判交接中的entry不存在。hot命中保护对象生命周期，两路locator都执行相同selector/generation/pin校验；陈旧则解析当前authority，未知覆盖返回not-ready/unknown。RocksDB I/O期间不持ledger锁，不增跨索引全局锁/事务。旧Add/MOVE/delete不打乱定位顺序，query-visible不等于persisted coverage；restart按§9恢复，索引重建不替代DATA同步。

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

BookieServiceInfo ephemeral registration只发布`bookie-profile` endpoint、readiness generation/reference与capability hint。顺序固定为persistent readiness CAS → matching service info → existing ephemeral writable registration；response loss重读两者，generation/incarnation mismatch立即demote/non-writable。实验 ZooKeeper persistent CAS 的 path/schema/escaping 与故障验证已有[步骤 12 增量](implementation/startup-readiness/README.md#步骤-12真实-zookeeper-readiness-cas-增量)；另已有[实验 session-owned info/ephemeral 发布](implementation/startup-readiness/README.md#步骤-12真实-ephemeral-发布增量)及真实过期/交错验证；稳定 backend 合同、生产 BookieServiceInfo/registry、真实 startup 与持续失连降级仍 BLOCK，readiness 不成为 per-Add lease。

restart/startup hook必须放在`EmbeddedServer`创建任何可能触碰Profile/Segment storage的组件之前，顺序锁定为：

1. 读取并验证compatibility fence；
2. 校验BookieId/storage incarnation/Engine/migration state；
3. 校验device manifest；
4. 校验每个required device/Arena superblock；
5. missing/corrupt/unknown/partial mismatch进入non-writable quarantine；
6. 验证Bookie级control checkpoint/后缀，按依赖对正常中断留下且缺少独立持久证据的完整控制内容完成恢复同步，再恢复route/fence/grant/tombstone等terminal gates但不开放服务；
7. 按RFC-0003 §5.3/12先重演完整控制后缀的metadata predicate，得到最终allocation/selector/retirement；不追读已合法退出的历史MOVE副本，也不将完整命令都视为成功。再校验当前仍引用及未完成操作所需DATA，按来源重建定位、完成必要恢复同步并交叉核验权限；此前不开放相应读取/幂等成功/恢复证据。index或另一个文件sync不替代目标barrier，最终数据损坏、控制缺口及已知I/O错误仍隔离，不擅自回退旧位置；
8. apply delete assignment snapshot+complete suffix中的terminal tombstone/可重建清理义务，按delete-applied cursor完成reconciliation并重建有界派生清理工作；不等待全部compaction/free；
9. 所有required控制/DATA持久依赖与恢复同步已验证后，durable local readiness；
10. persistent readiness CAS；
11. ephemeral registration；
12. 最后开启Profile write acceptance。

2026-10-02 隔离DATA路径新增Arena `DEVICE_STATE`文件隔离记录：已知write/force失败先切断受影响共享文件的新DATA、点读及重复成功。实际绑定的合批路径在I/O前共享预留每文件故障通知的控制ticket和逻辑日志容量，至少保留一个普通控制槽；容量不足先背压，配置无法同时容纳通知与普通控制时拒绝绑定。控制忙时保留原通知，原批次终结后执行；停收等待这些已预留义务，Arena关闭也拒绝未终结通知。成功DATA等实际I/O与owner结果终结才归还通知；失败记录经原Arena owner持久化并apply，原grant仍是所有权事实，不产生FREE。重放及后续sync不能清除隔离。实际控制I/O或owner故障、记录落盘前退出仍未闭合；未知force的完整记录可重放确认，但缺少记录不构成旧错误已解析的证据。未启用解除隔离、服务级可写restart、readiness或生产ACK；详细范围见[本地存储状态](implementation/local-storage/README.md)。

2026-10-02 Arena写入会话已由既有`DEVICE_STATE` action 2/3与单调epoch记录OPEN/CLOSE：隔离writer在首次分配/写入前显式启动，正常关闭在停止DATA、排空实际I/O并关闭descriptor之后、释放目录锁之前记录CLOSE；控制未终结、DATA错误或消耗坐标后的执行前取消均保留OPEN。未匹配CLOSE的重放在DATA恢复同步前保持不可写，不把缺少隔离记录解释为旧错误不存在。CLOSE容量在OPEN前逻辑预留，无逐entry日志或额外DATA force；CLOSE不清除文件隔离。legacy已分配目录不能补认首个会话，未启用的旧手动原型仍保留原证据边界。Bookie服务应先关闭准入并排空执行组，再关闭DATA writer，最后关闭控制日志；完整服务启动、未正常关闭恢复、Bookie/readiness/registration及可写restart仍未接线。

2026-10-02 正常关闭的管理会话新增隔离local-result restart入口，完整DATA验证/同步及replayed Bookie route选择后，在原owner上按稳定Bookie cut恢复连续前缀和本地持久候选；后续normal/recovery请求仍分别捕获及复核Bookie准入，不由恢复候选单独授予ACK。bootstrap拒绝cut变化、错误stream generation、定位容量不足及同shard再次装入；校验失败不留下可用driver。真实正常关闭进程的旧entry重试/内容冲突、同stream下一序号及第二轮重开已验证。plain append-only扫描、legacy和未正常关闭会话不获得该资格；无MOVE/FREE、每shard唯一未退役stream是当前边界。Bookie readiness/registration、MetadataStore激活、未正常关闭恢复及一般服务restart仍未完成。

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

2026-10-02 局部提交工作集已接入现有owner对齐池：先校验大小、取得固定buffer后集中复制并冻结，DATA driver在真实I/O与owner结果终结后归还原池。真实文件回归覆盖停止响应等待、验证/准入/I/O拒绝及前批失败时后批仍在途；同日的普通entry输入由执行组共享源额度预留后再分配，owner复制到对齐池后才归还；输入转换与物理驱动共用原owner/completion ticket，池不足返回隔离`TRANSIENT_UNAVAILABLE`且不占物理坐标。显式入口仍由调用者提供batch集合/授权地址；下述自动入口另接入普通重试的源生命周期，recovery输入、点读、TLS/socket及索引预算与接收credit仍待统一接线，详见[本地存储状态](implementation/local-storage/README.md)。


同日`NormalBatcher`沿用固定owner/deadline/完成ticket进行逐entry合批，按framing bytes/count/最老deadline触发，非空封包才从已apply grant与原文件/prefix入口取得物理地址；资源拒绝不消耗序号，I/O unknown继续关闭该stream新写。启用前校验固定源/池覆盖统一entry上限；超过普通batch阈值的合法大entry独立封包。assembling相同坐标重试、已冻结等待者及已发布点读均复用已有比较/定位原语，保持逐票据generation和真实I/O终结边界。shutdown提前封包已接纳输入，owner故障排空未提交源，回调异常仍完成其余entry/ticket；这些是隔离存储进展，不证明真实接收接线、完整bytes预算或生产ACK。

同日该逐entry路径已从当前stream的单调预留游标选取足够大的同owner已apply pool；其他stream使用高地址grant不会阻止本stream消费其低地址grant，短尾可跳到下一同stream pool且不留下额外物理序号。不同allocation不能拼包，预留后尾部/取消范围不复用，文件故障仍切断整个共享文件的写入。此不恢复旧I/O unknown，不改变Bookie票据或local result判定；按需异步refill进展见下，空间恢复总账与生产接线仍未完成。

同日普通入口缺pool时可保留既有有界输入/Bookie票据，异步请求实际Arena控制driver；durable并顺序apply后，以同一DATA ticket的保留完成名额回原owner继续，池内已有足够授权空间的普通Add仍无control force；缺空间的批次才增加refill阶段。等待中的新坐标临时拒绝，同坐标重试沿用assembling比较/共享结果；control unknown或owner故障排空未提交源和票据，shutdown期间已准入的refill可继续至实际终结。恢复仍不把已分配零页、内存游标或refill callback当作durable duplicate/unused证明；生产权限/ACK及完整空间与网络总账尚未闭合。

状态：**整体PLANNED / 未闭合**。隔离固定owner/物理DATA切片及Profile固定头前置校验、明文接收预算组件已有定向测试；尚无Bookie权限化普通Add、真实TLS listener或完整接收总账。保留§5.2三层持久化owner，首批采用固定shard、常驻平台线程owner及shared DATA block；§10.2—10.5的执行映射随该切片落地，默认affinity OFF、PARK；dedicated hot extent按RFC-0003 §8有条件延期。普通Add执行路径：

```text
client obtains bounded credits before assigning the logical entry where possible
    -> Bookie receives TLS/fixed header within bounded connection/read working set
    -> validate frame length/phase/subtype, reserve receive credits before accumulating large body
    -> decode framing with charged buffers; reject invalid declared lengths/context encoding
    -> hand off buffer ownership and receive credits to storage admission
    -> route ledger to its fixed logical WalShard and hand off to its owner worker
    -> owner validates complete context, unified size and local authority
    -> secure the bounded submission and locator/completion working set before DATA admission
    -> check/reserve coordinate using hot state and bounded pending
    -> seal a nonempty ENTRY_DATA batch, assign physical sequence and freeze all bytes for submission
    -> I/O backend completes full writes into unused preallocated DATA ranges, then the covering barrier
    -> return batch completion to the original owner through reserved completion capacity
    -> owner advances only the recoverable contiguous physical durable prefix
    -> publish physical locators in the bounded hot handle/index
    -> check each entry's selector/admission/pending result
    -> owner publishes eligible entries' local success independently and dispatches responses to the original connection
    -> release each buffer when its last user/I/O has finished
asynchronously:
    -> bounded derived-index updates, periodic persistence and durable coverage checkpoint
```

Bookie控制日志提前建立install/activation等条件，Arena控制日志提前建立pool allocation；普通已准入新写不逐条穿越三套队列、线程切换、锁或持久化future。pool不足时走有界refill/背压，不能把先决authority省掉；控制变化仍与captured admission/fence/tombstone order一致。

接收阶段按RFC-0001 §11.6先取连接级及Bookie总量内的额度，再继续累积大body；TLS、半包、已解码未准入请求及暂停前pipeline字节均入总账。逻辑预留不要求整包提前分配，接收credit不授予存储admission；交接后仍须取得既定提交/定位完成工作集，不能将接收预算挤占这份保留量。无额度时非阻塞暂停后续read或按明确策略关闭，close/timeout/解析错误释放不再持有的资源，不以暂停等同释放。

- shard合批按bytes/count及最老请求的单调时钟deadline触发，新请求不重置等待；到期padding提交。DATA submitted后不可改写，下一批用新范围/有界buffer。按真实覆盖barrier计量entry数，不将write CQE当durable或每entry force称为group commit；物理前缀发布与恢复按RFC-0003 §6.1/9。
- ledger锁只保护短状态转换，不持锁等待磁盘future。固定shard由§10.2的单一owner执行；阻塞write/fdatasync、索引与慢读在有界服务资源上执行，I/O完成回原owner。affinity OFF仍使用同一owner/队列模型；测量thread hops/queue wait/lock hold，不新增调度框架。
- adapter保留reference codec作oracle；使用受控payload视图，允许一次集中对齐复制。每层不可变包装不得反复clone整份payload。所有权覆盖拒绝、取消、断连、retry和I/O错误；若已复制且无引用可及源数据可释放源view，I/O仍引用的对齐buffer必须等真实I/O终结才复用，取消future不等于I/O已终结。
- 热尾、pending、定位与权限状态尽量合并进现有handle/index；维持hard caps和hole语义，慢路径只处理实际需要核对的已有坐标。
- 派生索引维护按RFC-0003 §15有界异步批处理，不能让append shard同步卡在RocksDB WriteBatch/flush/compaction；无sync/fsync不保证WriteBatch不stall。热定位接管以实际query-visible为条件，coverage推进以真实持久化为条件，两者不互相等待；索引瓶颈通过统一预算对后续准入背压。
- batch table管理物理结果，pending管理逐entry结果；单条fence/取消不污染同批其他entry或物理前缀。提交后的超时不释放坐标占位，无法解析的I/O走§6.1隔离/恢复，不以无界pending、任意跳号或重复sync掩盖异常。
- 搬迁复用同一batch I/O与预算，但completion只交给内部copy结果/MOVE流程，不进入上面ENTRY_DATA的立即定位发布分支；受校验来源及最终selector决定重建结果，任务对象或派生索引丢失不能提升未发布副本。
- shard、物理stream和DATA文件映射按RFC-0003 §6.1在manifest冻结；首个文件切片在一Arena内共享预分配DATA文件，barrier成本与错误影响按实际文件计量，不宣称shard之间独立flush。
- 同时运行写入与回收，保留RFC-0003 §13.1的Arena内部维护块及实际filesystem余量；共享filesystem的DATA预分配受Bookie总预算约束，保留SST/compaction/控制checkpoint进展。free slot不等于文件系统available，不能通过停止compaction改善p99。

准入预算贯穿整个请求生命周期，并保证已接受请求能完成下一步，不只限制queue.size()。首批在总账内保留每shard少量预分配对齐batch buffers；普通入口payload不得占用这份共享提交工作集。定位/完成状态优先由已有pending对象承接，其容量在DATA准入前计费并保障，不等DATA durable后再竞争最后一个locator槽位；不为每entry预留最大尺寸buffer。

预算由连接/TLS/header/read工作集、半包/已解码入口payload、共享提交工作集、热定位/索引接管容量及控制/维护保留量组成，仍是一份总账。hot/index积压到准入阈值时先背压新DATA，已准入及已提交entry仍有完成定位/结果处理的预算。大entry即使符合统一N，也须在接受前取得对应有界工作集，否则明确背压。ownership转移或容量申请要么立即成功，要么立即失败后进入既有有界等待/重试；不阻塞Netty event loop、append shard主循环，不持ledger锁等其他阶段归还资源。索引恢复容量后须自动继续推进，不靠丢有效DATA、无界扩容或临时突破上限解围。

各阶段计费规则：

2026-10-02 `SegmentPointRead.Reader`的隔离本地读请求使用现有owner/completion及I/O服务，Bookie读票据与有界DATA文件pin持续到实际I/O/owner结果终结；成功结果将固定池lease交给最后消费者，未关闭结果使shutdown等待，并对读/DATA形成有界背压。取消仅停止响应，撤权、读盘错误、owner故障及回调异常均释放其实际终结的资源；重开读取维持唯一连续stream/Arena cut校验，池不足以覆盖旧entry时不启用reader。原同步堆buffer检查路径保留；真实读授权、网络响应credit及完整读取/索引/Bookie总账尚未闭合。

2026-10-02 隔离driver的已发布坐标重复比较已使用既有owner固定池的对齐scratch，normal/recovery源view不再重复复制；池不足在真实读盘前返回临时不可用，原坐标与DATA force不变。scratch和源保持至实际I/O与owner结果均终结，取消等待和读取错误均按原完成票据排空。通用物理点读、只读重启检查及无池手动入口仍保留堆buffer实验路径，网络/完整读取/索引bytes总账未完成，详见[本地存储状态](implementation/local-storage/README.md)。

| 持有阶段/场景 | 计费、释放与进展规则 |
| --- | --- |
| socket/TLS/header、未完成frame、已解码未准入 | 固定工作集和实际buffer均计入连接/Bookie总量；先验证header并预留额度再继续大body，暂停前pipeline字节不漏记。向存储移交owner/credit，close/timeout/解析失败释放实际不再使用的资源及未消费预留 |
| 待合批、已出队、已提交I/O、等待physical prefix/locator/响应 | 同一预算持续计入request、源/对齐bytes、inflight batch；只在真实资源释放或转移到另一已计费owner时归还对应credit，不按出队释放 |
| 复制、断连、取消、慢响应 | 源/目标同时存在则都计bytes；I/O仍在飞行则保留buffer/inflight费用，传输层持有的响应也必须有界，不能藏在future或executor队列 |
| I/O unknown与慢点读 | stream不可写gate承接未解析义务前不丢坐标保护；实际I/O终结后才释放不再使用的buffer。点读父buffer/独立副本/pin与cache均计费，小slice可受控复制以释放大父buffer |
| 普通block放不下的entry | 首先满足RFC-0001 §8跨normal/recovery/read/storage统一N，接受前取得同一总预算内的大记录工作集；无法表达首次拒绝，暂时取不到工作集则背压。已接收entry持续可读/恢复，不能永远卡队首 |
| 重复/冲突核对旧坐标 | 复用有界读取执行资源，append shard主循环不同步等盘；pending/waiter和读取buffer计费，正常新写不因此多一次索引I/O |
| 热定位、异步索引积压、query-visible未flush内容 | 队列/WriteBatch、hot记录、memtable/native/cache分别计实际bytes/对象；准入为完成定位留容量，stall达阈值先背压新DATA，已接受entry不再争用被入口耗尽的额度。接管后转移credit，local success不提前释放后台费用，控制容量仍可推进 |
| 资源拒绝后的客户端退避/retry | 同一逻辑Add继续占用原inflight/bytes预算，deadline和ACK独立；拒绝不抹除旧unknown/坐标事实，不复制无界请求，不因短暂拥塞直接换组 |
| 持续热ledger与撤权请求 | 连接/ledger只设必要有界份额；沿RFC-0001 §11.6在同一Profile listener保留有界独立控制连接及接收/执行/持久化份额。DATA暂停不阻断全部控制可达性，fence/tombstone不排在其半包之后；控制也计总账并验证真实operation/身份/授权 |

实现复用现有shard/pending/buffer预算和请求对象，不新增通用调度平台。所有被接受entry遵守§6.1/RFC-0001 §8统一应用payload上限、完整BK/frame/framing/alignment检查和runtime总预算。协议/实现不能表达时首次写入前size/capability拒绝；暂时资源不足才在明确DATA准入前背压，不先接收再无界积压。后者使用RFC-0001 §11.5现有`TRANSIENT_UNAVAILABLE / SAME_PROFILE_OPERATION / NONE`，NONE仅针对本次尝试；已有同坐标unknown不能被覆盖。进入可能产生DATA的阶段后按§6.1如实保留unknown，不能伪装准入拒绝。

Profile客户端在ensemble failure handling前消费完整三元组，按RFC-0001 §9.3有界退避原逻辑Add、等待已有activation协调或执行满足故障条件的正式replacement。FENCED/TOMBSTONED/冲突/无权限不得换目标绕过；客户端预算尽量先于entryId及累计length分配，已分配的Add不因背压/deadline变成可忽略的hole。以上不增加每entry远程状态查询、控制日志或独立持久retry任务。

replacement完成remote phase后按RFC-0001 §9.3继续阻塞成功发布，直到全部受影响pending Add完成所有被替换slot的ACK撤销及资格重算，再开放连续前缀回调；身份未变的有效ACK保留，不全量重发W份DATA。Bookie共享文件的单stream恢复按RFC-0003 §6.1/12逻辑判定范围，不缩短文件或丢弃其他stream有效DATA；旧generation残留不进入新index，回收按完整authority及I/O终结判定。

首个isolated/discardable性能切片可与实际启用路径的Model A/C及必要D子集验证并行，不必等待所有延期功能模型。切片先用真实control/DATA I/O、一个Arena及固定shards完成normal Add、点读、基础restart和本地回收；多Arena、全量重建和完整故障/资源Gate逐步补齐，未覆盖范围不得进入canary。无真实网络的shard测试不能证明transport/TLS成本，无真实恢复的性能run不能证明WAL可恢复性。

Spike B B18/B19固定硬件、I/O模式、stream/file映射、durability、TLS范围、E/W/A、payload分布及负载，分别测低负载延迟、目标负载吞吐/尾延迟、过载有界拒绝、写入与回收并行、restart/fault recovery。报告allocation/copy bytes/entry、CPU/entry、吞吐/p99、按文件计的entries/barrier、padding/实际磁盘写放大、debt，以及write/durability/physical-prefix/locator wait和全阶段资源峰值；补充点读bytes放大/I/O/cache hit/pin时长、大父buffer与大量hot ledger内存。首批shared-block结果不包含dedicated收益；replacement或真实网络未执行时单列NOT_EXECUTED，不把局部无网络结果当端到端增益。

同一报告增加index入库等待、persisted coverage落后量、资源拒绝、retry和真正replacement数量；分别观察前台定位发布与后台索引stall。拥塞不统一计为磁盘故障，不能用无限积压、丢弃新写或停止回收改善p99。B2/B9/B18联动A26/A27验证索引stall下新写有界背压、fence推进、容量恢复后原逻辑Add重试及提交后unknown保留。

B18在已有资源测量中分列TLS/decoder retained bytes、未完成frame数、接收reserved/实际held峰值、暂停/恢复与释放，以及DATA暂停时控制接收/完成等待。逐连接合法frame不证明合计有界；只注入已解码请求或只测控制执行队列不能覆盖本项。无真实网络/TLS时该部分保持NOT_EXECUTED，不另建流控协议或监控平台。

既有指标继续区分Arena reusable bytes、filesystem available bytes、derived-index disk bytes、maintenance temporary-space peak及换组必要resend/复用写放大。B18另验证入口满载后下游恢复能完成已接受请求；B10/B17按RFC-0003分列compaction copied bytes和净可复用收益，冻结source、实际destination/padding及whole-allocation FREE不能被单ledger原因替代。A16/A26/B19验证逐来源fence否定证据，保留原A29、共享S0/S1、旧generation和统一N场景。数值由同一manifest冻结，不新增控制平台、逐entry控制fsync或持久任务系统。

### 10.2 固定owner与执行线程分工

2026-09-28：[隔离实现](implementation/local-storage/README.md)已交付固定owner/PARK循环、DATA/控制独立预留的completion、从既有I/O线程总额划分的控制专用有界服务、显式绑定、拓扑/placement、group启动和SYSTEM/OWNER_TOUCH池；Arena控制在DATA工作线程及队列满额时的真实force后原owner apply及绑定模式的低频资源复查/PREFER/REQUIRE本地处置已有定向验证。仍未接入append状态机、Bookie生命周期自动重启或PREFERRED页策略，无性能结论。目标映射明确分开：

```text
ledger instance -> logical WalShard -> fixed owner worker -> CPU / NUMA placement
```

隔离文件原型固定`ledgerId % logicalShards`路由，normal准入、物理热候选、点读和重启扫描均拒绝错shard记录；这仍不构成Bookie权限化服务。WalShard保留现有路由/stream身份；owner是运行其状态机的常驻平台线程，CPU mask只是该线程的可运行位置。一个worker可承载一个或多个属地一致或明确兼容的shard，每个shard同一时刻只能由一个owner执行。首个切片可取1:1，比例不进入磁盘格式。例如8个shard分给4个worker，worker 0持0/4、1持1/5、2持2/6、3持3/7，属于启动计划，不改变8个逻辑shard。

部署从16核变成8核时，不得按新CPU/worker数重新取模解释旧ledger/DATA。关闭旧执行者、按既有规则终结其I/O并完成恢复后，可在重启时调整shard→worker映射；既有stream、generation、allocator authority不因此改变。首批不做在线work stealing、热shard迁移或跨NUMA自动均衡，不为每ledger建线程，也不为每shard建立独占I/O/控制线程组。

| 执行单元 | 职责 | CPU/并发边界 |
| --- | --- | --- |
| Netty event loop | TLS、framing、接收预算、分发及响应 | 网络/服务CPU集合；连接可承载多个ledger，不强行与append owner同核 |
| Append owner worker | 本shard的pending、合批、物理前缀与逐entry完成 | 优先每worker使用不同物理核的一个逻辑CPU；只处理有界任务，不同步等盘 |
| I/O执行/完成 | 实际write、barrier或异步I/O完成 | 尽量靠近DATA设备的服务CPU集合；线程数有界，不要求每shard独占 |
| Bookie/Arena控制执行 | 各自既定控制命令顺序、apply、checkpoint协调 | 少量有界服务资源；保留控制容量，不等DATA队列腾空 |
| 点读、派生索引、compaction | 读盘、RocksDB批处理、复制与维护 | 服务CPU集合和既有并发预算，不进入append owner同步主循环 |

正常交接为Netty→目标owner→I/O backend→原owner→原连接。阻塞式write/fdatasync不得在Netty或append owner执行；真正非阻塞的backend可减少一次提交交接，但其提交调用本身也不得隐藏同步磁盘等待。completion线程不直接并发修改shard的pending、合批或前缀状态；可按batch合并通知，复用batch table/pending，不为每entry另建future/任务链。

DATA准入前，沿§10.1既有完成工作集预留completion通知和buffer归还容量。DATA队列满时，已提交I/O的完成仍可进入原owner并释放资源；通知的owner/storage generation验证和物理终态跟踪仍按RFC-0003执行。owner必须处理输入、completion、控制通知和释放事件，多shard轮转有界，不能被一个热shard或持续DATA饿死其他工作。single owner和绑核均不替代点读、selector/pin、fence/tombstone的同步、内存可见性或既有linearization合同。

### 10.3 启动拓扑计划与安全显式绑定

静态源码核对锁定`6e5df388ab1882caaa4de7e03d078f6be7196a95`：[CpuAffinityImpl](../../../cpu-affinity/src/main/java/org/apache/bookkeeper/common/util/affinity/impl/CpuAffinityImpl.java)的`acquireCore()`检查root、选isolated CPU并取得同core的文件锁；[IsolatedProcessors](../../../cpu-affinity/src/main/java/org/apache/bookkeeper/common/util/affinity/impl/IsolatedProcessors.java)会写`cpu*/online`上下线sibling。[ProcessorsInfo](../../../cpu-affinity/src/main/java/org/apache/bookkeeper/common/util/affinity/impl/ProcessorsInfo.java)只按`core id`分组，未纳入socket，不能直接作为双插槽拓扑依据。[OrderedExecutor](../../../bookkeeper-common/src/main/java/org/apache/bookkeeper/common/util/OrderedExecutor.java)及[Journal/ForceWrite](../../../bookkeeper-server/src/main/java/org/apache/bookkeeper/bookie/Journal.java)在busy-wait分支调用该接口。这些是现有兼容路径，不是Segment的默认策略；本轮只记录独立接入合同，不修改Classic行为。

2026-09-26实现补充：`cpu-affinity` 已新增 `CpuBinding` 与独立JNI mask接口，设置/读回/同线程恢复、非法参数和虚拟线程拒绝已有测试。非root Linux容器的真实绑定与子线程继承通过，输入/源码及具体覆盖见[绑定回执](implementation/local-storage/cpu-binding-receipt.json)。拓扑/placement现已加入只读快照和静态计划，macOS 16通过/2平台跳过，Linux容器17通过/1平台跳过，见[拓扑回执](implementation/local-storage/topology-placement-receipt.json)。[group启动与池](implementation/local-storage/README.md)已接入绑定、全部ready屏障及原线程清理；macOS 26通过/1平台跳过，Linux owner/group 12通过/1平台跳过。运行期低频资源检查与PREFER/REQUIRE本地处置见[运行期回执](implementation/local-storage/runtime-placement-receipt.json)；隔离owner低频placement check抛错后的停收、已提交I/O与排队ticket排空已用确定性交错回归覆盖；Bookie生命周期自动重启、空闲I/O线程即时释放与物理NUMA仍待实现/验证，不能将隔离group测试当作整个§10.3或§10.5完成。

Segment仅显式设置自身应用线程的affinity：不自动开关CPU、不改宿主机isolcpus/cpuset/IRQ配置、不以文件锁声称整机独占、不要求root。复用`cpu-affinity`模块及[JNI工程](../../../cpu-affinity/src/main/affinity/cpp/affinity_jni.c)，增加独立安全接口，不能只移除root检查后继续走自动抢核策略。Linux自身线程绑定受权限、online/cpuset和容器限制；必须读取实际mask，不能从调用成功推断完整请求已生效。依据[Linux sched_setaffinity(2)](https://man7.org/linux/man-pages/man2/sched_setaffinity.2.html)。

`CpuTopologySnapshot`现已实现只读采集（已接入隔离owner group启动）：应用缩窄任何线程mask前保存的初始部署允许集合、online CPUs、cgroup有效CPU/memory-node集合与quota、socket/core/SMT sibling/NUMA拓扑、DATA设备NUMA归属。物理核按package及必要层级/经验证sibling集合识别，不跨socket合并同号core；拓扑未知如实标记，不能伪造独占物理核。sysfs拓扑依据[Linux CPU topology](https://docs.kernel.org/admin-guide/cputopology.html)。候选集合为：

```text
eligible CPUs = initial deployment mask ∩ online CPUs ∩ effective cgroup CPUs
                ∩ configured candidate CPUs (when provided)
```

不得把某个已绑单核worker的当前mask当成全进程初始部署额度。cgroup v2使用`cpuset.cpus.effective`、`cpuset.mems.effective`及`cpu.max`，同时识别适用的上层限制；其他环境记录实际探测能力和未知项，不把探测缺失当无限资源。CPU quota是时间预算，不是CPU可运行集合：可见16个逻辑CPU但quota约2个CPU时间份额时，不能规划16个持续spin的owner。未知/不足的专用CPU余量不启用spin。依据[Linux cgroup v2](https://docs.kernel.org/admin-guide/cgroup-v2.html)。

`ExecutionPlacementPlan`现已实现静态规划（已接入隔离owner group）：保存shard→worker、worker→CPU mask、buffer pool→NUMA preference的静态映射，只校验placement，不定序DATA。先为Netty、I/O、控制、索引、compaction和JVM留CPU余量，再定worker数；同物理核优先只选一个SMT sibling给append，并避免把重负载服务线程放另一个sibling。应用只选CPU，不关闭sibling。单NUMA节点8个可用物理核时“4 append＋4服务共享”仅为实验起点，TLS/维护重时可减少append worker，不能随ledger数扩线程。

安全接口候选为`CpuBinding bindCurrentThread(CpuSet requested)`，返回请求/实际mask及可释放作用域。Java/JNI须验证非空集合、编号/范围与native mask容量，读取验证实际mask，传播errno/不支持原因；不能把稀疏CPU编号当数组长度或让固定`cpu_set_t`越界。仅在绑定验证成功后登记进程内部placement占用；部分启动失败须清理登记，已绑定线程在自身作用域内恢复当前允许范围中的原mask并报告恢复结果。不能留下“登记已占用但未绑定”，也不能把恢复失败伪报为已降级成功；无法完成必要清理时在服务开放前退出，不修改宿主机状态。

worker自身启动顺序为“按模式绑定并验证（OFF不设置mask）→按RFC-0003 §6.2初始化/触碰本地池→发布就绪→服务循环”。Linux新线程继承创建者的affinity，因此自建服务线程在入口应用自己的placement，不能让绑单核owner懒建一组服务线程而全部继承单核；OFF的线程从未缩窄部署mask的创建路径启动。首批不遍历改绑JVM内部GC/JIT线程，测量其干扰。依据[Linux pthread_setaffinity_np(3)](https://man7.org/linux/man-pages/man3/pthread_setaffinity_np.3.html)。

### 10.4 等待策略独立于affinity

首批默认`PARK`，`spin budget = 0`。`BOUNDED_SPIN`只在明确CPU余量及B18对照支持时显式启用：检查输入/completion/控制/归还事件→处理有界任务→空闲最多spin指定预算→仍无工作则park，新工作唤醒。spin与多shard轮转不能重置最老batch deadline；park时间服从最近合批deadline，到期仍须提交低负载批次，不能只等下一个请求唤醒。

复用经验证的有界队列和等待原语，入睡前重查队列并处理enqueue/notification竞争，避免丢失唤醒。completion与fence/tombstone事件不能被DATA或spin饿死。affinity只回答线程在哪里运行，绑核成功后仍可park；首批没有无限busy-wait、实时调度或“所有I/O/索引/后台线程都spin”模式，也不消费Classic的`enableBusyWait`作为Segment绑核开关。

### 10.5 配置、降级与实施顺序

以下为拟配置名及设计默认值，**不是当前仓库已存在参数**；具体数量、CPU列表与阈值由同一实验manifest锁定，不进入ProfileDescriptor、DATA identity、quorum或持久shard数。

| 拟配置 | 语义与默认 |
| --- | --- |
| `walAppendWorkerCount` | 常驻owner数量，与逻辑shard数分开；按服务余量/quota规划，不以机器标称核数或ledger数扩张 |
| `walAffinityMode` | `OFF / PREFER / REQUIRE`，默认`OFF` |
| `walAppendCpuList` | 显式候选CPU；未指定时由启动拓扑计划选择 |
| `walServiceCpuList` | 应用服务线程CPU集合，可共享；校验与append物理核/sibling的冲突及服务余量 |
| `walBufferNumaPolicy` | `SYSTEM / OWNER_TOUCH / PREFERRED`，默认`SYSTEM`；池行为与降级见RFC-0003 §6.2 |
| `walWaitStrategy` | `PARK / BOUNDED_SPIN`，默认`PARK` |
| `walSpinBudgetMicros` | 有界空闲spin时间，默认`0`；不改变batch deadline |

CPU列表语法/编号非法、重复占用关键核、映射矛盾等是配置错误，任何模式都不静默忽略。环境不支持native/affinity、cpuset或quota不足按模式处理，不通过线程超额扩张强凑“每shard一核”。`OFF`不主动绑核，保留完全相同的owner、队列及存储逻辑；`PREFER`尝试合法计划，环境限制时清理部分绑定/登记、报告实际mask与降级原因，使用普通调度及PARK；`REQUIRE`在开放服务前验证所要求绑定全部成立，否则失败。REQUIRE只保证本进程所要求的线程mask，不承诺宿主机独占、NUMA页面硬绑定、无GC/IRQ或实时调度；NUMA策略的best-effort回退单独报告。

低频后台检查运行时CPU/cpuset/memory-node/quota变化，不在每Add读sysfs。首批不在线迁移owner：PREFER保持原owner/shard，在当前合法CPU集合继续并关闭spin，记录属地降级；REQUIRE在所要求绑定失效时停止新准入、沿既有drain/I/O终结和退出流程重启。没有可运行资源或无法安全继续时按既有退出处理，不假设drain一定立刻成功。拓扑变化本身不是磁盘authority损坏，不能重分配ledger、创建新stream或改变ACK/fence/去重/恢复结果；已有buffer不因此迁移或提前释放。

绑核不阻止其他线程、进程或中断使用该CPU。真正隔离由部署层提供，NIC RSS/IRQ/irqbalance、NVMe队列及完成中断配置单独记录，Bookie不自动修改。Kubernetes CPU Manager `static`可为符合条件的容器提供独占CPU；完整核、NUMA/内存对齐需相应策略及实际版本核验，普通CPU limit不证明这些条件。首批不开发Operator，见[Kubernetes CPU管理](https://kubernetes.io/docs/tasks/administer-cluster/cpu-management-policies/)。

实现归既有UW-3/UW-7和B18：先交付固定owner＋PARK＋安全binding/拓扑校验，再接入owner初始化及可选preferred-node池，最后做有界spin与部署干扰对照。三项小职责仅为`CpuTopologySnapshot`、`ExecutionPlacementPlan`及`cpu-affinity`显式绑定作用域，不新建调度框架、NUMA classifier或持久化控制层。固定owner及安全接入点随第一批线程/池构建完成；可选优化没有测出收益就保持OFF/SYSTEM/PARK，不阻止隔离原型开工，也不以开启affinity作为存储正确性前提。

2026-10-02 本地隔离存储图已由`IsolatedSegmentStorage`统一接线：全shard原owner bootstrap及控制票据终结后才开放normal/recovery/点读与控制入口；使用既有Bookie准入、合批器、固定源/对齐池和有界热目录。停止入口与执行组准入后，等实际I/O、owner及已交付读结果buffer归还，再依次关闭DATA、Bookie与Arena控制日志。超时仍持原对象/锁，启动失败走同一清理路径，owner回调阻塞关闭在取得关闭锁前拒绝。交接异常只清理未被driver接管的票据，已接管票据继续由driver终结。该本地owner尚非EmbeddedServer启动钩子，不发布readiness/ephemeral registration，不完成真实网络/MetadataStore或未正常关闭恢复。

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
- B9/B10先重演完整控制再验当前必要DATA：A→B→C且A/B FREE复用、无新checkpoint仍恢复C，C坏/后缀缺失隔离。copy durable无成功MOVE时清空索引/task不提升，来源受校验且completion不误走普通定位分支；
- B2/B9/B16两次故障：write完成但barrier前进程退出，重启处理重复Add后再模拟掉电，成功数据仍在；仅CRC/locator/readiness不授予durability，后一次sync不掩盖已知I/O错误；
- B3/B7/B10/B16控制命令同批durable后顺序apply、同slot ALLOC竞争、apply中途crash、重复FREE、durable/applied cut分离及提交前quiescence gate；
- §6.1同坐标去重/冲突及LAC/digest兼容边界、DATA durable到locator publication间的crash、ACK后点读与全部index丢失；
- 跨normal/recovery/read/storage统一N的N-1/N/N+1及normal允许但recovery超限44-byte区间；首次拒绝不可表达entry，降低新写阈值后旧entry仍可读/恢复，溢出早于大分配；
- A29/CLIENT-1整体gate在多slot ACK更新完成后恢复回调，未变有效ACK保留；B2/B3/B6/B9证明单stream逻辑后缀不损及同文件其他成功DATA，复用残留不复活且不依赖每次整块清零；
- B17/B18分别核算Arena与filesystem空间，DATA预分配不侵占SST/控制维护余量；shared-filesystem多Arena不重复预留，实际I/O/authority异常仍沿错误恢复流程；
- DATA durable且index Put成功但未flush时crash、热定位淘汰与query-visible接管并发、陈旧Add索引更新晚于MOVE/delete；持久覆盖不能虚报，已成功entry不出现临时NoSuchEntry；
- B9/B18精确交错hot-first查询与DB接管/淘汰，禁止旧snapshot/iterator/negative-cache fallback；hot对象生命周期、selector/pin安全，DB I/O不持ledger锁；
- index持续stall与新写/fence并发，预算有界且控制推进；短暂资源拒绝不立即触发replacement，重试NONE不覆盖旧UNKNOWN，deadline不制造可跳过的entry hole；
- B18把入口payload推至准入上限并暂停提交或索引接管，再恢复下游；共享buffer/定位工作集保证已准入entry完成且资源最终释放，不超额、不阻塞event loop/shard/ledger锁、不永久互等；大entry无工作集时在接受前背压；
- B18/A26从socket/TLS到半包/完整未准入请求均计费，固定header后先取额度再收大body，交接/关闭/错误释放无漏记；DATA读取暂停时有限独立控制连接仍可达且认证/operation/总量约束不被绕过；
- RFC-0004 §7.3的未fenced来源先missing再收旧写、fence durable但pre-cut I/O未解析、重复/旧incarnation响应；复用Classic quorum算法并适配Profile，不能误close或新增逐entryfence fsync；
- §8以LAC=99/entry 100验证confirmed、unconfirmed及恢复点读分层，Bookie不按local LAC截断，DATA completion不推导quorum LAC或增加逐Add LAC fsync；
- A16/A26/B19核验P与累计应用length同前缀，LAC落后、fragment抬升、无新尾部/空ledger、边界读不可用及同P不同L的close重读均不得错误成功；
- 同批L1/L2 pre-cut Add与L1 fence的drain顺序、已提交timeout后同坐标不同payload、batch 11 unknown/12已完成、共享文件错误与恢复；逐entry结果不污染物理前缀，异常pending及buffer有界；显式终止pre-cut路径仅在启用时验同批L1失败/L2成功；
- 大batch中单entry独立envelope/CRC校验及必要对齐I/O、坏length、cache/pin竞态与慢客户端小slice持有；首批shared-only及后续dedicated池的资源/性能结论分别计量；
- §10.1—10.5的ByteBuf、固定owner/shard、受保留容量保护的completion及真实durability通过B18/B19；B18用相同实现分离OFF/PARK、CPU pin、NUMA池和有界spin，对启用/可用路径验证实际mask/页分布、quota与降级、继承/唤醒竞争和语义不变。仅静态计划或无硬件run不能宣称收益；
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
- `ProfileRegistrationStore` 与 `EphemeralProfileRegistrationStore` 已有隔离 ZooKeeper persistent CAS、session-owned publication/demotion/expiration 与 response-loss fresh-read 验证；稳定 path/schema、生产 registry、真实 startup/local receipt/持续失连降级和完整 readiness 接线仍待实现；persistent-before-ephemeral 顺序已冻结；
- recovery strong assertion/local evidence binding、accepted loss ordering与五类 outcome到现有Bookie API/error的exact dependency mapping；
- performance Gate 的 exact thresholds。

这些问题关闭、相关 Spike/Model 通过前，本 RFC 不得标为 Implementation Ready。


2026-10-04，[整allocation全死FREE](implementation/local-storage/README.md#整-allocation-全死-free-与退役前缀)已复用Bookie准确终态tombstone作为唯一删除权威；临时reclamation lease只关闭并排空entry admission，不持久复制另一份ledger生命周期。Arena保存已验证范围的generation/序号及必要Bookie applied cut，重放不重新访问合法退役字节。真实storage图与rotation后全量/覆盖恢复已验证；inactive、unknown或索引缺失不授权FREE。后续[健康整块复用](implementation/local-storage/README.md#整-allocation-复用与健康关闭末端)通过当前grant generation与CLOSED末端区分旧残留，重放仍使用同一Bookie终态权威。后续[运行期终态清理](implementation/local-storage/README.md#运行期终态索引与坐标回收)复用同一已applied终态的有限snapshot、entry drain与原owner，实际native删除后释放对应坐标并更新coverage；晚到tombstone留到下次。MOVE/compaction、长期净空间及集群删除仍未完成。 后续[relocation 内部完成](implementation/local-storage/README.md#relocation-副本内部持久化完成)仅返回实际副本持久化事实，不取得普通 Bookie 成功或替代 MOVE/selector 权限。


2026-10-04 当前后续增量：[受保护的条件 MOVE 与当前位置](implementation/local-storage/README.md#受保护的条件-move-与当前位置)已复用现有全局entry/file排空与CONTROL service，接通实际copy字节验证、最多256行条件durable/apply、Arena selector、旧定位业务读/幂等与metadata-only控制重放。旧coverage不能跳过MOVE后的必要DATA，当前target缺失拒绝恢复；未退役历史allocation仍解析CRC。MOVE后源/orphan FREE、原owner native/coverage维护与部分存活compaction、A/B已复用后的C恢复、长期净容量及生产Gate仍未完成。当前六项真实/固定向量回归及16正向/36指定反例见该切片；此前receipt保持各自历史范围。


2026-10-04 最新后续：[搬迁源与orphan整块回收](implementation/local-storage/README.md#搬迁源与-orphan-的整块回收)已接通受保护当前后继字节验证、FREE v2 selector applied cut、源/orphan及准确T当前COPY退役与原整块generation复用；纯普通T v1字节不变。真实A→B→C、A/B均复用且无新增checkpoint、storage全量与rotation恢复保留C，坏/缺失C和陈旧proof拒绝。当前四项Java/固定向量及17正向/40指定反例保持局部范围；原owner在线native MOVE/coverage、合批victim compaction、长期净容量与生产Gate仍未完成，之前receipt不重绑。


2026-10-04 最新增量：[Bookie保护下的MOVE索引维护](implementation/local-storage/README.md#native-move-索引维护与覆盖恢复)已复用现有checkpoint暂停全部原owner索引、Bookie entry/control及全文件I/O排空、原owner scratch池、256行native更新、flush与A/B coverage发布。当前MOVE目标实际字节校验失败不回退selector；旧native覆盖被拒绝，匹配覆盖恢复仍点读当前必要副本。取消保持保护到实际I/O和全owner终结，页发布失败可继续读并重发。四项新增真实回归及七类271通过/5平台跳过、18正向/44指定反例为当前局部证据；合批victim compaction请求、实际净回收循环、逐allocation在线cut、长期native/control/SST容量、多Arena、OPEN错误恢复和掉电/生产Gate仍未完成。


2026-10-04 当前增量：[同Arena有界合批compaction](implementation/local-storage/README.md#同-arena-有界合批-compaction)已组合显式1..16个同stream source的实际扫描、allocation粒度净收益判断、独立destination、一批COPY/条件MOVE、原owner native覆盖维护及一次CONTROL barrier的合批整块FREE。各v2 FREE绑定自身前序cut，共享保护到全部claimed成员终结；取消/发布失败不提前释放或伪造净回收，后续新operation可完成保留source的回收。真实共享块、零净收益拒绝、两块减一块的净容量、全T无destination、复用恢复及失败/取消重启由当前回归验证。仍为全图暂停/一个COPY/显式组，自动选组、逐allocation在线cut、长期control/SST/文件系统净预算、多Arena、OPEN错误恢复、掉电和生产ACK未完成，步骤9/10和完整Gate不提升。


2026-10-04 当前增量：[同Arena多批compaction](implementation/local-storage/README.md#同-arena-多批-compaction)在既有显式source组内按owner buffer与256行上限划分COPY/MOVE，所有批次完成后统一native覆盖及合批FREE。净成本包括各批framing/padding总和再按destination整allocation向上取整；后批失败保留已成功selector与所有source，不伪造整组回收。40/402坐标的跨批读取、覆盖/全量恢复及source复用、后批force unknown隔离、零净收益和源预算拒绝已由真实路径验证。源snapshot总预算和既有source batch扫描buffer仍有显式上限；自动选组、逐allocation在线cut、长期净容量、多Arena、OPEN错误恢复和生产Gate保持未完成。


2026-10-04 当前选择增量：[固定shard有界回收](implementation/local-storage/README.md#固定-shard-的有界回收选择)按成功ALLOC顺序查看同文件/唯一stream最多16个当前grant；真实DATA测量先于payload捕获，优先直接回收零current live组，最终仍独立whole-allocation后继证明与合批FREE。满容量且无关current live超过源输入预算的实际回收、source generation2复用及17个grant窗口保留年轻共享活数据已验证；既有共享T及多批COPY/MOVE回归改经shard入口。七类283通过/5平台跳过及20正向/51指定反例仅为局部证据。窗口外公平选择、partial任务自动收敛、逐allocation在线cut、长期完整净容量、多Arena、OPEN错误恢复与生产Gate仍未完成。


2026-10-04 当前轮转增量：[有界回收窗口与容量循环](implementation/local-storage/README.md#有界回收窗口轮转与有限容量循环)复用固定shard/file映射，按固定本轮ALLOC上限推进进程内游标；新grant留至下轮，首窗口拒绝后可轮到更晚dead并恢复destination，完成轮次后回访旧T。真实17个满容量grant、后续普通refill、1/4 worker及四shard四轮自动FREE/generation1→5、重启读取已验证。七类285通过/5平台跳过及21正向/53指定反例保持局部范围；跨窗口最优组合、部分任务收敛、维护调度、per-allocation cut、长期control/SST/文件系统完整净预算、多Arena、OPEN错误恢复与生产Gate未完成。


2026-10-04 当前池化增量：[池化分批捕获](implementation/local-storage/README.md#池化分批-compaction-捕获)复用原owner两个buffer逐批点读/复制，不持整组payload；有限计划以既有坐标上限约束，并在每批重新复核source hash、selector、Bookie cut与测量长度。所有COPY/MOVE、native覆盖及真实整块proof后仍才FREE；池不足或后批身份变化返回PARTIAL并保留实际占用。真实40条/三批超过源输入预算、单buffer拒绝及有效CRC源变化/实际控制变化、恢复验证通过。七类287通过/5平台跳过、22正向/55指定反例保持局部范围；原source batch扫描buffer、全graph暂停和单连续destination限制保留，长期完整净容量、多Arena、OPEN错误恢复、掉电/性能及生产Gate未完成。


2026-10-04 当前部分搬迁增量：[满容量保留tail续写](implementation/local-storage/README.md#满容量部分-move-的保留-tail-续写)在新grant无空间时，验证含current COPY的最新同stream grant并保留为目的地，剩余COPY总量须适配其实际空闲tail；只为受保护证明建立最新tail cursor，不回更旧grant，目的地排除FREE。真实PARTIAL后健康重启、1/4 worker、gen1/gen2旧残留tail、21条/两批续写、两源净FREE2 MiB与普通source复用/重启、tail不足及坏字节0COPY已验证。复用新增destination容量为0仍报告实际ID。七类289通过/5平台跳过及23正向/57指定反例保持局部范围；任意partial自动收敛、调度、长期完整容量、多Arena、OPEN错误恢复、掉电/性能及生产Gate仍未完成。

2026-10-04 当前空间准入增量：[前台refill维护容量预留](implementation/local-storage/README.md#前台-refill-的维护容量预留)把最大compaction目的地whole blocks与每DATA文件内部预留绑定，单compaction并发下同文件合批refill共同扣减，超过声明目的地上限在ALLOC前拒绝；预算从已apply grant/FREE重建。真实前台拒绝→COPY/MOVE/native/FREE→净恢复→健康重启/gen2再准入及两block工作集已验证。当前仅内部容量，extent碎片、filesystem/SST/control/checkpoint临时空间、低水位/hysteresis、长期负载与多Arena仍未闭合；不关闭B17、步骤9/10或生产Gate。历史回执保留原绑定。

2026-10-04 当前控制维护增量：[运行期控制日志轮转](implementation/local-storage/README.md#运行期控制日志轮转)已接入显式graph入口、原CONTROL票据/I/O及两个driver排他名额。在Bookie admission与全DATA文件保护下依次完成双owner rotation，保留真实OPEN session/epoch/pool/游标及CLOSE余量；取消只撤回复，保护留到原owner终结，已尝试失败报告UNKNOWN并停止准入。1/4 worker满额后两轮原pool续写、读/幂等/健康重启、真实pin与force暂停/失败验证通过。七类295通过/5平台跳过及25正向/65指定反例为局部证据；自动调度、filesystem/SST/checkpoint临时工作集、长期负载、多Arena与OPEN错误恢复仍未完成，不关闭步骤8/10、B17或生产Gate。

2026-10-04 当前自动恢复增量：[控制容量压力的自动轮转](implementation/local-storage/README.md#控制容量压力的自动轮转)把可fresh-fit的容量拒绝接入既有owner pump和受保护CONTROL rotation；pressure期间暂停新graph准入，已接受entry/I/O/pin排空后再尝试，同步quarantine拒绝也唤醒维护。新journal与原owner终结后恢复原pool续写；超大请求不触发无收益轮转，自动UNKNOWN保留cause并停止准入。真实红/绿、1/4 worker两轮满额/健康restart、单Arena拒绝唤醒及错误验证通过；七类298通过/5平台跳过、26正向/68指定反例为局部证据。提前低水位/一般调度、filesystem/SST/checkpoint临时工作集、长期负载、多Arena与OPEN错误恢复仍未完成，不关闭步骤8/10、B17或生产Gate。

2026-10-04 冷启动容量增量：[满控制日志的健康冷启动](implementation/local-storage/README.md#满控制日志的健康冷启动)在真实恢复扫描/必要同步与同一DATA锁之后、下一OPEN之前，仅对确为CLOSED且新journal能容纳OPEN及实际CLOSE/双root预留的Arena执行既有轮转，保留Bookie恢复cut。健康重启、写者排他和失败边界的证据见该节；一般OPEN错误恢复、filesystem临时预算、长期负载、多Arena及步骤8/10/B17/生产Gate仍未完成。


2026-10-04 空间压力维护增量：[数据空间压力的自动回收](implementation/local-storage/README.md#数据空间压力的自动回收)把前台pool/refill的内部容量拒绝接到既有owner pump；控制rotation先行，同文件固定shard按冻结cut和16-grant窗口检查，实际pin/Bookie/buffer条件满足后复用native cleanup/COPY/MOVE/FREE。无净收益轮停下等待新控制事实，已知预写入控制容量拒绝先rotation再重新保护，真实UNKNOWN保留cause并停止图。旧生产红、新生产1/4worker自动恢复/当前COPY tail/健康restart、跨shard资源等待、全live与T、控制满额及COPY force失败的局部证据为七类306通过/5平台跳过、28正向/75指定反例。单buffer或长期资源占用没有自动进展保证；低水位/维护份额、filesystem/SST/control-checkpoint临时预算、长期负载、多Arena、全部partial收敛、一般OPEN错误恢复及性能/掉电仍待验证，步骤9/10、B17和生产Gate未完成。

2026-10-04 内部 checkpoint 历史增量：[有界内部保留](implementation/local-storage/README.md#内部-checkpoint-历史的有界保留)仅压缩已被后续根取代的成功 CHECKPOINT_COMMIT；当前双 root 身份与最新两个内部 commit 合计至多四项，原业务首次结果、grant/credential/T 与失败条件仍保留，实际物理回收完成后才清理进程内表。[image v2](implementation/local-storage/format-v1.md#内部-checkpoint-历史压缩与-image-v2)明确允许该内部 generation 间隔，v1 仍要求原完整历史，准确 predecessor 与必要 suffix 检查保留。真实旧生产预算耗尽红、新生产同 16384-byte 预算固定状态 96 次双 owner 轮转、健康 restart、旧 v1、独立新 v2 向量与单 image 损坏回退已验证；七类 309 通过/5 平台跳过、29 正向/79 指定反例保持局部范围。业务/会话历史压缩、持续创建删除、filesystem/SST/checkpoint 临时总账、长期负载、多 Arena、一般 OPEN 错误恢复、掉电/性能和完整模型仍待完成，步骤 8/10、B17 与生产 Gate 不关闭。

2026-10-05 原 journal checkpoint 恢复增量：[统一恢复入口](implementation/local-storage/README.md#原-journal-的压缩-checkpoint-恢复)使 Bookie/Arena 在未物理轮转时也按合法 v2 内部代际间隔恢复，复用轮转路径已有的准确 predecessor、原条件结果和完整必要 suffix 验证；无格式或预算变化。真实旧生产两项红，新生产八次 checkpoint 后选择最高 image、最新 image 损坏后选择上一代并重放 suffix、幂等/失败/冲突/T 拒绝已验证，七类 311 通过/5 平台跳过及 style 通过。本轮扩展有限模型 29 正向完整通过、80 指定反例符合预期，保持局部输入假设；业务/会话历史压缩、长期负载、多 Arena、完整模型及步骤 8/10、B17 和生产 Gate 仍未完成。


2026-10-05 退役 DATA 区间增量：[合并恢复投影](implementation/local-storage/README.md#退役-data-序号区间的合并)仅合并准确 stream、连续物理序号且不跨 current grant 原控制顺序的成功退役事实，包括未写 grant 的边界；全部 FREE 的最大必要 Bookie cut 含空退役，缺少时拒绝恢复。真实 24 次同 block FREE/reuse、第 25 次当前 DATA、双 owner rotation/健康 restart、未写中间 grant 与空 FREE 缺少 cut 的反例已验证。七类 314 通过/5 平台跳过、style 通过，有限模型 30 正向完整通过/84 指定反例；历史表与排序工作集仍增长，未实现请求代、完整业务状态裁剪、长期负载或多 Arena，步骤 8/10、B17 与生产 Gate 仍未完成。


2026-10-05 Arena 当前索引增量：[当前授权与历史身份分离](implementation/local-storage/README.md#arena-当前授权与历史身份的分离)由 current grant 保存原成功顺序，已用 ID 闭区间与成功高水位保留空洞及 anti-ABA；成功 FREE 增量折叠退役范围，空 FREE 移除当前边界后也重新合并并保留必要 Bookie cut。恢复读者不再排序历史 FREE，但 image 仍保存原业务命令/首次结果并重放，原 UUID、session、MOVE/Bookie T 历史未裁剪，实验格式不变。真实 ID 空洞/失败高 ID/退役身份拒绝、最高 image 与损坏后 fallback、原 grant 顺序、空边界合并和 gen2 DATA 双 owner rotation/健康 restart 已验证；七类 315 通过/5 平台跳过、style 通过，有限模型 31 正向完整通过/89 指定反例。未实现请求代、完整业务 image、总工作集有界、长期负载、多 Arena、掉电/性能或完整模型，步骤 8/10、B17 与生产 Gate 仍未完成。


2026-10-05 当前 COPY 状态增量：[COPY 证明与 selector](implementation/local-storage/README.md#当前-copy-证明与-selector-状态)把当前 allocation 内逐条成功 COPY 的 location/UUID/generation/digest 与当前 selector 的 target/身份/原序号/必要 Bookie cut 从历史 MOVE 查询中分离。失去 selector 的成功 COPY 在 allocation 当前时仍保留证明，失败 MOVE 不发布，FREE 删除该 allocation 的全部证明；selector 不持有原整批 MOVE。原历史结果仍保留，image v1/v2 与实验字节不变。真实成功/失败 COPY、最高 image/损坏后 fallback、物理 rotation、FREE/复用/restart 及 force UNKNOWN 回归通过；七类 315 通过/5 平台跳过、style 通过，有限模型 32 正向完整通过/93 指定反例。请求代、独立紧凑业务 image、UUID/session/MOVE/T 历史裁剪、总工作集、长期负载、多 Arena、掉电/性能及完整模型尚未完成，步骤 8/10、B17 和生产 Gate 保持未完成。


2026-10-05 步骤 12 增量：[真实 ZooKeeper readiness CAS](implementation/startup-readiness/README.md#步骤-12真实-zookeeper-readiness-cas-增量)复用原接口，验证完整持久记录、store/readiness 双版本、birth 原子检查和提交后 UNKNOWN/fresh-read 解析；真实双 client/server 的 8 项测试加原 17 项 reference 测试全部通过，Spotless/Checkstyle 通过。有限 registration 投影为 Model A 局部验证，当前矩阵 33 正向完整通过/96 指定反例，不代表 A+C 组合或真实 quorum。原 Wave 0 回执冻结；其旧 scope guard 对累计后续路径返回 BLOCK，未扩充 allowlist。真实 startup/ephemeral、ProfileControlStore、安装激活及生产 readiness 仍未完成，步骤 12/Gate 不提升。


2026-10-05 registration 增量：[session-owned ephemeral 发布](implementation/startup-readiness/README.md#步骤-12真实-ephemeral-发布增量)复用既有接口及 persistent scope，info/writable multi 检查 authority、nonce 与版本；demote 永久退休 nonce，实际过期清除 ephemeral，旧删除不命中新同版本注册。12 新测试加原 25 项全部通过，Spotless/Checkstyle 通过；有限 Model A publication 投影及全部先前切片为 34 正向完整通过/100 指定反例，不是 A+C/D 完整组合。typed local facts/内存 local receipt 不代表真实恢复，生产 BookieServiceInfo/registry、startup/delete catch-up 与持续连接事件降级仍未接线；步骤 12 与生产 Gate 保持 PARTIAL，历史 receipt/scope guard 不改绑。


2026-10-05 本地 readiness 增量：[真实文件 receipt](implementation/startup-readiness/README.md#步骤-12真实本地-readiness-receipt)实现原 LocalReadinessStore，复用实际 writer 锁与 FileChannel；完整 temp write/force→原子替换→目录 force 后才 APPLIED，UNKNOWN 实例拒绝读成功，新 attempt 严格解析并同步已发布 receipt，未发布 temp 不恢复。100 代替换保持固定文件集合，短写、两个 barrier 故障、scope/格式损坏、跨进程互斥及两轮退出/重开已验证；原 harness 已组合真实文件与两个 ZK stores。48 项相关测试及 style 通过，有限本地 publication 与既有模型为 35 正向完整通过/105 指定反例。输入的 recovery/device/delete facts 仍非真实恢复，pre-replay startup、生产 registry、持续失连准入、control store/安装激活及完整模型/掉电/性能未完成，步骤 12 与生产 Gate 保持 PARTIAL。 原 reference receipt 和既有真实 ZK 历史结果不改绑。


2026-10-05 控制读取前置：[真实 ZK cold reader](implementation/profile-control/README.md#步骤-12真实-zk-控制权威读取)已实现独立 view 的固定键读取、严格隔离与 snapshot 版本绑定；73 项定向测试及 style 通过。它没有将 IsolatedSegmentStorage 的实际恢复转成 readiness facts；required-device、兼容检查及 delete assignment/cursor 尚未与 startup/reference 闭合，不能以 authority 可读或 local receipt 可读开放服务。生产 registration/持续失连准入、完整控制发布与安装激活及步骤 12 保持 PARTIAL。

2026-10-05 准入增量：[session 绑定的本地 DATA gate](implementation/startup-readiness/README.md#步骤-12session-绑定的本地-data-准入)在首次 cold 操作前绑定实际 ZK watcher manager；匹配 owned registration 后开放，观察到失连、过期、close/demote 即永久退休原 attempt，同 session 重连及迟到读取不重开。隔离 graph 的普通/恢复 DATA 按本地 Bookie/incarnation 检查，热路径不查 metadata；已提交 I/O 与原池资源正常排空。八类 296 项及 style 通过，有限矩阵 37 正向完整 PASS/113 指定反例。真实 startup/fresh recovery/delete facts、生产 listener/registry、持续远端 authority、完整安装激活、quorum、完整模型与步骤 12/Gate 仍未完成。

2026-10-05 启动排他增量：[restart DATA 排他前置](implementation/local-storage/README.md#restart-的-data-排他与控制恢复顺序)先取得真实 DATA 目录 writer lock，再恢复控制及 index，把同一 native lock 交给 files，保留原全部 header 校验/恢复同步与 CLOSED 限制。实际子进程外部锁红绿、控制打开/DATA 恢复失败清理及成功移交/读回验证完成；340 项中 335 通过、5 项 macOS 上的 Linux 亲和性跳过，style 通过。有限矩阵 38 正向完整 PASS/117 指定反例。只补本地 ownership 前置，compatibility/device/delete 权威、真实 startup/readiness、生产接线及完整模型仍待完成。

2026-10-06 控制网络增量：[真实隔离FENCE与控制额度](implementation/profile-wire/README.md#真实隔离-fence-与控制额度)复用原context/credential校验、Bookie控制driver与实际write/force/owner apply，首次授权后把连接/frame/解析响应workspace原子转入CONTROL；后续逐次鉴权。pre-cut等待返回原操作transient/NONE、force/apply不确定性保留UNKNOWN，断连不提前释放已复制控制请求或实际I/O资源。同一port的真实DATA半包/第二DATA额度拒绝、UNCLASSIFIED满额下已有CONTROL推进及零effect拒绝已验证；14类391项为386通过/5平台跳过，style通过；有限矩阵40正向完整PASS/128指定反例。生产listener、完整control-plane、TLS/socket/native/storage总账、fresh startup/readiness及步骤11/Gate仍未完成。


2026-10-06 恢复写入增量：[真实隔离ADD_RECOVERY](implementation/profile-wire/README.md#真实隔离-add_recovery-接线)沿用144字节固定prefix、精确本地OPEN grant与原DATA驱动，按实际源额度先预留再复制完整BK entry；reference网络整包克隆不进入server路径。恢复成功/重复校验不激活normal writer，grant close晚于DATA写入的过期结果与force失败保持UNKNOWN；断连仅停等待，实际I/O/owner排空及native锁保留。七个真实socket用例已通过；最终相关14类398项为393通过/5平台跳过，style及41正向/134指定反例通过，详细源码绑定见上述实现记录。READ_ENTRY响应/确定missing、真实RepairIntent及grant控制面、客户端quorum/完整恢复、生产listener/readiness与TLS/socket/storage总预算尚未闭合，步骤11/14和生产Gate保持OPEN。

2026-10-06 [Netty backing buffer额度接线](implementation/profile-wire/README.md#netty-backing-buffer-额度接线)已把隔离channel的heap/direct capacity、扩缩容时旧加新backing、derived view最终释放、TLS/socket通过channel allocator分配的ByteBuf及响应workspace拆分加入原连接/Bookie账本。已有read backing跨用途保留原lane；断连归还名额但不提前归还实际仍持有的backing。真实socket漏记红绿、六项allocator回归、TLS分配拒绝及原22项socket场景已验证。JDK/native TLS内部对象、kernel buffers及存储统一总账仍未闭合，生产listener/readiness、READ_ENTRY、完整模型/长期/掉电/性能与步骤11/Gate仍OPEN。

2026-10-06 [接收至存储源副本交接](implementation/profile-wire/README.md#接收至存储源副本交接)使隔离normal/recovery同步entry复制在原source子上限之外，复用借用frame连接的exact heap backing额度；frame/workspace/source共存均计费，断连后源保留至owner实际用完，复制到预分配对齐池后才归还源，实际I/O仍持有原pool lease。明确credit拒绝回滚source并沿用原未接纳结果；其他分配错误传播，手动/pooled调用者保留独立unpooled副本合同。两条真实socket路径及额度拒绝/恢复单测通过，未改wire或存储格式。完整TLS/kernel/object/index/storage总账、生产listener/readiness、READ_ENTRY及步骤6/11/Gate仍未闭合。
2026-10-09 [独立CLOSED readonly-open](implementation/profile-wire/README.md#独立-closed-readonly-open-与原-sdk-读取)已复用原SDK只读handle/digest/point read，固定matching P/L且confirmed/unconfirmed均受其上界约束；bounded范围/读额度与cancel custody、最后cold delete/birth复核、本地close不增加metadata/control写已有真实验证。现有reader可在普通删除的本地tombstone前继续读取，新的open必须通过live gate；这不提供强撤权、全读取预算或生产readiness，步骤14及生产Gate仍OPEN。

2026-10-09 [整体fenced point recovery](implementation/profile-wire/README.md#整体-fenced-point-recovery-入口与持久身份续接)已把现有metadata/fence/SDK point oracle/selection/SELF write-back/terminal quorum/matching CLOSED串成单attempt入口，采用已发布context/selection/intent/close ID续接，cancel/deadline保留actual gate；A2/A3空/非空及历史末段、A3不足quorum后resume、required hole/limit已有真实验证。RECOVERED_AND_CLOSED只消费matching close，不清marker或做strong reset；AutoRecovery、普通writer close adoption、离线catch-up等仍OPEN，步骤14及生产Gate不提升。

2026-10-09 [CLOSED fragment repair](implementation/profile-wire/README.md#closed-fragment-repair-与固定-close-history-cut)新增同一次standard CLOSED CAS的UWCF80 close-history reference，关闭后copy/membership publication沿用原context/P/L及birth/inverse验证，目标inactive/grant/Add/close复用。真实A2/A3连续新目标换组、回复丢失/fresh snapshot零recopy及target restart通过；原readRecovery拒绝CLOSED、不清marker，ReplicationWorker route/最终健康清理及生产Gate仍OPEN，步骤14保持PARTIAL。

2026-10-09 [CLOSED 全副本健康复核](implementation/profile-wire/README.md#closed-全副本健康复核)新增独立 matching CLOSED 全副本复核：逐坐标要求所有 exact write-set DATA digest/payload/累计 length 合法，再重验完整 metadata birth/version/context/head/sources 与 DELETE。空 ledger/末段要求所有当前目标匹配 incarnation 和 durable FENCE；closed fence 已包含已发布 inactive repair 目标。真实 historical hole、CLOSED repair 后完整、取消/deadline实际 gate 和 DELETE/recreation 验证见该记录。私有 proof 只绑定该 snapshot，未消费 worker lock/marker 同事务清理条件，ReplicationWorker 接线与 marker 清理、步骤14及生产 Gate仍PARTIAL/OPEN。

2026-10-09 [Profile marker 条件化清理](implementation/profile-wire/README.md#profile-marker-条件化清理)新增显式 completeReplication：领取在health read前绑定原task的coordinator/deadline/单次健康proof，再fresh读取CLOSED/DELETE，以同一个ZK multi消费metadata/lifecycle和原worker session witness、marker physical birth/version，并删除原task/lock。默认OFF，UNKNOWN保留义务，受控release/session终止不删除replacement lock；强管理reset仍disabled。原ReplicationWorker自动路由、候选/intent续接与hierarchy/page GC尚未接通，步骤14及生产Gate仍PARTIAL/OPEN。

2026-10-09 [原ReplicationWorker的Profile自动循环](implementation/profile-wire/README.md#原-replicationworker-的-profile-自动循环)新增显式默认OFF cohort constructor，复用原thread/factory/backoff/stats与恢复、真实COPY、完整W健康proof及原task guarded removal；A3在fixed selection下先修last再close，published close丢回复用原ID零recopy，stop/shutdown等actual child排空才release。实际A2/A3 OPEN/CLOSED/空健康与历史fragment、重启/后续损失及Classic两项回归见该记录。空缺副本grantless repair、service bootstrap/动态pool/rejoin/delete/GC及综合Gate仍OPEN，步骤14保持PARTIAL。

2026-10-09 [空副本grantless membership repair](implementation/profile-wire/README.md#空副本的-grantless-membership-repair)复用原固定32槽/INSTALL/admission及UIC6，实际terminal force/apply后同事务发布membership与hash绑定UWEB124 receipt；空UWRP/UWRM显式schema2/kind1，DATA schema1向量不变。A2/A3空ledger与历史空末段、目标文件restart、reply丢失同ID续接、stop/DELETE及force失败/receipt损坏均有真实验证；零DATA/grant/normal，后续完整W健康与原task guarded removal沿用。步骤14仍PARTIAL，service bootstrap/动态pool/rejoin/delete/GC及综合生产Gate仍OPEN。


2026-10-12 [实际恢复 graph 与原 startup 发布桥接](implementation/startup-readiness/README.md#实际恢复-graph-与原-startup-发布桥接)已消费原全部声明DATA/双控制/派生定位/owner恢复和实际delete-applied追赶，产生本地device/allocator/route/delete facts及observed rollback flags，再沿原DLR→persistent→service-info→writable→本地准入顺序显式接consumer。required scope/client不匹配与scheduler拒绝不开放。兼容性fence/migration与独立credential/new-scope仍是policy输入，真实issuer、pre-replay权威和默认EmbeddedServer接线继续OPEN；reference feature1不改磁盘/wire格式。77项阶段回归、最终1项组合、20配置有限模型与style按原source保留，RAT443历史问题无新增，步骤12/14/15与生产Gate不提升。


2026-10-12 [持久startup requirements与冻结发布条件](implementation/startup-readiness/README.md#持久-startup-requirements-与冻结发布条件)已建立初始单Arena完整compatibility/expected-device的独立ZK immutable source与实验UWSQ/schema1，显式read/check早于replay graph构造，publisher的writable multi消费冻结birth/version。实际runtime version1与物理恢复事实独立于caller声明；expected READY不是observed READY，metadata声明不是stock old-binary mandatory fence或OS/service credential隔离证明。阶段85/最终26项Java、相关18/最终4配置模型与style按原source保留，RAT443历史问题无新增。same-scope迁移/old binary、独立credential/new-scope物理issuer、默认EmbeddedServer与SDK routing、完整startup及步骤12/14/15/生产Gate保持OPEN/PARTIAL。


2026-10-12 [实际Unix credential new-scope access快照](implementation/startup-readiness/README.md#实际-unix-credential-的-new-scope-access-快照)已用native UID501/4294967294、完整bounded现有文件和已知可读marker验证真实读取/写打开/创建权限，directory-only假成功在0711目录/0644已知文件上真实RED后修复。原SDK/mTLS/实际restart/source/readiness组合导入真实access与canonical roots，31项最终验证通过、style通过、RAT443历史问题无新增。old-service disposition仍NOT_VERIFIED，Classic journal/ledger为guard目录，stock binary/提升权限/持续隔离和默认启动hook未证明；整体fallback/B14/B15及生产Gate仍OPEN。


2026-10-12 [实际旧Classic episode关闭与new-scope组合](implementation/startup-readiness/README.md#实际旧-classic-episode-的有序关闭与-new-scope-组合)已在old UID的独立真实JVM上复用原EmbeddedServer/Classic SDK Add/read、stack/Journal/storage/metadata driver关闭与native process终态，原注册session/birth匹配后fresh absence，closed但live或halt37后的absence均不产生DRAINED。受控observation与actual Unix access导入原replay/harness组合，34项最终验证/style通过，RAT443历史问题无新增。Classic native incarnation未新增，episode UUID为admin实验标识；外部服务/永久退役/stock binary/default生产hook和整体fallback/B14/B15/Gate仍OPEN。


2026-10-12 [显式Profile listener及网络实际排空](implementation/profile-wire/README.md#显式-profile-listener-与实际网络-credit-排空)复用actual opened graph、TLS/HELLO/control/DATA pipeline，parent close后worker barriers消除queued accepted socket未入budget的空窗；原receive ledger退休admission并等actual cold/frame/response/backing/slots归还，future副本不授权终态。borrowed graph/cold/loops不归listener关闭，存储native I/O仍依原owner排空。46项Java/13配置有限模型及style验证，RAT443历史问题无新增；未接default pre-storage hook/full startup authority或闭合full-bytes，步骤11/12/14/15及生产Gate保持PARTIAL/OPEN。

2026-10-12 [显式 Profile 服务 owner](implementation/startup-readiness/README.md#显式-profile-服务启动与-owner-排空)已把 pre-open requirements/实际 Arena-Index 根绑定、原 graph/DLR、实际 listener 地址、readiness 发布/admission/consumer 和原 SDK 注册发现接为一次隔离 episode；关闭依次退休、demote、native network drain、原 storage drain 与 DLR lock。真实 force 未结束和 publication 晚到均不能提前释放锁或恢复 writable；58项相关 Java/style通过，RAT443父 HEAD 路径无新增。default EmbeddedServer、外部永久 physical/service authority、released stock矩阵、full-bytes 与全16步综合 Gate 仍 OPEN。

2026-10-12 [EmbeddedServer 显式 Profile 分支](implementation/startup-readiness/README.md#embeddedserver-显式-profile-分支与-lifecycle)已将 builder opt-in 置于 Classic metadata/directory/cookie/storage 构造前，原 stack 只在实际启动成功后标 STARTED、实际 owner drain 后标 STOPPED/CLOSED；错误不吞掉，stack 不越过未排空 component。真实 SDK/注册发现/restart、publication stop/held force 和原 Classic 回归有28项验证。无 opt-in 仍走 Classic 默认；配置 factory、完整外部 authority/stock矩阵与整体 Gate仍 OPEN。

2026-10-12 [配置/显式 bootstrap](implementation/startup-readiness/README.md#配置转换与显式-bootstrap)已让配置转换冻结原 graph 参数、复用公开 typed authority/transport 和原 owner；mode默认OFF，配置不生成 disposition/credential/access proof，ISOLATED缺 owner 不回退 Classic。实际 properties→原服务/SDK/restart与 pre-open 配置拒绝通过30项验证；Main/owned环境/完整外部 authority、stock matrix、full-bytes及综合 Gate仍 OPEN。

2026-10-12 [owned资源初始化/终止](implementation/startup-readiness/README.md#同一服务-episode-的-owned-资源初始化与终止)现已由同一handle deferred创建原ZK/metadata/cold/scheduler/loops，实际network/storage drain后才关闭它们，metadata scheduler和native ZK线程/loops终态未确认时不完成。真实SDK/restart、held任务及partial-init失败有33项验证；TLS/physical authority和完整Main/stock/full-bytes/Gate仍OPEN。

2026-10-12 [本地删除空间终结](implementation/profile-wire/README.md#本地删除空间终结)已将实际terminal TOMBSTONE接到原native index/coordinate/coverage及COPY/MOVE/FREE，按实际allocated DATA/旧COPY复核本地REUSABLE；混合块可零净容量增益退休，普通维护仍要求正净增益。旧IO/owner credit与deadline/cancel独立排空，其他ledger保留、corrupt/limit不伪造成功。3类282项Java与20有限模型配置（4完整正向/16指定反例）通过；网络physical receipt/全目标/retention/GC/退役及其他16步验收仍OPEN，生产Gate不提升。


2026-10-12 [网络删除空间查询与全目标观察](implementation/profile-wire/README.md#网络删除空间查询与全目标观察)已接独立UIC8/OPERATION_STATUS、同一持久UIC7事件和原mTLS/direct/exact权威检查，实际FREE后fresh查询才报告REUSABLE；完整历史manifest逐target/incarnation观察，缺peer保留PENDING，全部REUSABLE仍是ephemeral结果。durable terminal receipts/retention/GC/退役及其他16步验收继续OPEN；默认OFF/SYSTEM/PARK和disabled能力不变，生产Gate不提升。


2026-10-12 [删除物理终结记录与持久接续](implementation/profile-wire/README.md#删除物理终结记录与持久接续)已把实际UIC8 REUSABLE绑定完整事件/request/target/incarnation，条件化存入原ZK不可变row与birth guard；新await入口按完整历史targets重读全部记录，支持coordinator重建及已有记录的实际离线目标。原观察入口保持只读，PENDING/UNKNOWN无记录，不删除历史义务或授予GC权限。最终6类105项Java/31有限模型配置（4正向/27指定反例）及style通过；retention、accepted decommission/unrecoverable、snapshot/handoff/history GC和其他16步验收仍OPEN，生产Gate不提升。


2026-10-12 [删除流完整 snapshot 与真实启动追赶](implementation/profile-wire/README.md#删除流完整-snapshot-与真实启动追赶)已将原generation1 shared stream的完整prefix保存为有界immutable pages/root＋conditional publication witness，绑定scope/原event birth并接入实际TOMBSTONE/cursor启动追赶。真实缺prefix＋suffix、65-ledger两page、missing/recreate、取消/回复丢失及并发有111项Java/23有限模型配置（3正向/20指定反例）证据；无旧journal删除、assignment handoff或GC权限。retention/terminal退役/history GC及其他16步验收继续OPEN，生产Gate不提升。


2026-10-12 [Assignment handoff 的本地持久 adoption](implementation/profile-wire/README.md#assignment-handoff-的本地持久-adoption)已将实际PREPARED Plan接入原Bookie type14 force/owner apply/checkpoint/replay；必须先完成原G1 cut，空cut不生成effect，重复相同Plan不再force，普通G1 DATA保持有效。等待超时不释放实际I/O，fresh native admission ABA拒绝重新确认。最终4类27项Java、20有限模型配置（3正向/17指定反例）及style通过；native catch-up ACK/EFFECTIVE CAS/old-route retirement、retention/GC及其他16步验收仍OPEN，生产Gate不提升。


2026-10-12 [Assignment handoff 的 native ACK](implementation/profile-wire/README.md#assignment-handoff-的-native-ack)已将原storage graph实际force/apply及owner终态产生的不可公开构造凭据，接到原ZK conditional immutable ACK row/birth guard。完整Plan、local control sequence与native PREPARED/cut绑定，丢回复/取消后重建读取首条原记录，已关闭owner不能新确认；ACK保持G1 cursor/assignment与原义务。最终6类32项Java、26有限模型配置（4正向/22指定反例）、1919源码输入及style通过；conditional EFFECTIVE/G2 journal与registration/old-route retirement、retention/GC及其他16步验收仍OPEN，生产Gate不提升。


2026-10-12 [Assignment handoff 的 EFFECTIVE 与 G2/G3 接续](implementation/profile-wire/README.md#assignment-handoff-的-effective-与-g2g3-接续)已把原graph gate退役、owned registration删除、NON_WRITABLE投影与native assignment/head更新放进原publisher同一ZK multi；原type15 force/apply/checkpoint/replay接续effective progress，原事件身份保留。三目标真实G2/G3 READY、SDK Add/close、新旧事件与混合snapshot、健康文件旋转重启、native丢回复/held callback与type15 UNKNOWN已有验证。79项原阶段Java（server 71/common codec 8）/最终21项补验及35有限模型配置通过，1929个最终源码输入核验一致，style通过、RAT443父HEAD无新增；完整Gate不提升。retention/offline/backup-restore、accepted terminal decommission/unrecoverable/history GC、其他required authority/default routing/full-bytes/兼容/正式性能/运维/全16步验收继续OPEN。
