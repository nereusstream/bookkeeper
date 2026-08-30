# Spike A：Profile 显式安装协议否证规范

> 状态：**Planned / Not Executed**<br>
> 对应 RFC：[RFC-0001](../RFC-0001-profile-capability-install.md)<br>
> 性质：否证型；PASS 只允许 RFC 继续评审，不代表实现或生产就绪

## 1. 要回答的问题

本 Spike 只回答：两阶段 `INSTALL_LEDGER_PROFILE` 是否能在创建、重试、崩溃、mixed-version 和 ensemble replacement 下保持 fail closed。

主要风险：

- 未安装 ledger 被新 Profile Add 接受；
- 相同 instance、不同 descriptor 被静默合并；
- 安装信息重启后丢失；
- 一般 `E > W` 的轮转 write set 触达未安装 Bookie；
- 标准 LedgerMetadata 在 all-E Profile route claim 前暴露 Classic lazy-create 窗口；
- 写期 replacement 先进入 metadata，随后才尝试 install/activation；
- response loss 产生第二个 instance 或不一致 READY；
- 创建失败留下可被误用的 orphan install。

## 2. 非目标

- 不测 Segment payload 性能；
- 不决定最终 proto field number 或 on-disk layout；
- 不实现 Sequenced takeover；
- 不证明 general E/W/A recovery；
- 不把 happy-path demo 当作 RFC acceptance。

## 3. 运行前必须锁定

以下 manifest 任一字段为 `TBD` 时不得开始正式 Spike：

```text
sourceCommit
RFC0001Revision
prototypeCommit
metadataDriverAndVersion
profileControlStoreAdapterRevision
root/page/fan-out/retention hard bounds
BookieCount
clientCount
E/W/A configurations
Bookie engine cohorts
protocol version matrix
supported old Bookie/client commits and built artifacts
canonical descriptor codec/field manifest
fixed-length collision-resistant hash suite/length/domain separator and candidate eligibility check
descriptor/parser/capability hard bounds
control endpoint/auth plugin and non-anonymous principal policy
exact operation/instance/target-scope authorization policy
credential-bearing Profile transport confidentiality/integrity mechanism
protected local auth binding revision
cold authority read/reference and activation mechanism
Profile framing/discriminator/opcode/subtype manifest
connection handshake and semantic error mapping
profiledMetadataMutationAuthority
localAuthorityStoreAndFormatRevision
Classic route throughput/p99 regression budget
JDK and JVM flags
fault injector version
test seed policy
artifact output directory
```

最低 quorum 配置：

```text
3/3/2
3/3/3
4/3/2 or another E > W rotation case
```

正式结果必须使用全新 immutable output 目录；不得覆盖先前运行。

## 4. 最小原型范围

原型必须包含：

- 经 RFC-0001 冻结的 metadata/activation 状态；
- 独立 Profile sidecar、标准 LedgerMetadata 唯一 membership authority 与 immutable instance backlink；
- domain-specific `ProfileControlStore`：single-record create/read/versioned CAS、store-version/semantic-generation分离、bounded family head/page/snapshot+suffix；
- canonical descriptor 与 hash；
- frozen golden descriptor corpus、bounded parser与独立reference recomputation；
- `INSTALL_LEDGER_PROFILE` request/receipt；
- authenticated non-anonymous cold control endpoint、Bookie direct-read committed authority、protected local binding与secret-free status/receipt；
- Bookie durable install 与 activation authority；
- 单一、原子、可恢复的 Classic/Profile route slot；
- logical local authority state machine：orthogonal normal admission、bounded recovery grant/readable facts、fence/tombstone generation与Bookie registration readiness；
- Bookie restart replay；
- Add 请求身份能够匹配 Bookie 本地 durable activation；exact binding fields 由已接受的 activation/auth 机制决定；
- distinct Profile normal/recovery operation、mandatory non-ambiguous framing、connection-amortized handshake与semantic error propagation；
- capability/engine placement filter；
- initial all-E inactive route claim before standard LedgerMetadata create；
- READY authorization before local normal ACTIVE，all-E activation before create/open success；
- profiled membership mutation authority abstraction；
- active replacement 的 inactive install → `LAC+1` CAS → normal activation → pending resend；
- 可观测的 orphan install 状态；
- unknown mandatory sidecar/local record与old-binary downgrade gate；
- 精确错误码和 audit events。

允许使用简化 payload engine，但不得用 in-memory install 代替 durable record。

## 5. 观测与 Oracle

每个事件必须记录：

```text
testRunId
scenarioId
logicalStep
metadataVersion
sidecarStoreVersion
sidecarControlGeneration
sidecarAuthorityDomain/head/snapshotCut
sidecarLifecycleFact
ledgerId
ledgerInstanceId
descriptorHash
installRequestId
bookieId
localInstallGeneration
bookieStorageIncarnation
protocolGeneration
connectionHandshakeGeneration
localProfileRole
localAdmissionGeneration
requestType
responseCode
faultId
monotonicTimestamp
```

Oracle 从三类证据独立计算：

1. MetadataStore versioned history；
2. 每个 Bookie durable control-record dump；
3. client request/response 与 fault timeline。

只看最终内存状态或最终 API success 不足以判定。

## 6. 场景矩阵

### A1：Install 前 Add

步骤：创建 sidecar reservation，在标准 LedgerMetadata 尚未创建时，于 0、部分和全部 Bookie inactive install 前分别发送新 Profile Add。

Oracle：所有未 durable install 的目标 Bookie 返回 `EPROFILE_NOT_INSTALLED`；不能创建 Classic handle，不能写 payload。

### A2：部分安装成功

步骤：E 个节点中只允许子集完成 install，阻断其余节点，尝试创建标准 LedgerMetadata、发布 READY 和 write-set rotation。

Oracle：标准 LedgerMetadata 不得创建、READY 不得发布；任何轮转 entry 都不能绕过未安装节点形成被客户端视为合法的创建完成。

### A3：Install response loss

步骤：Bookie durable install 后丢失 response，使用相同 request 重试，并分别让协调器 crash/restart。

Oracle：返回同一 instance/hash 的幂等 receipt；不产生第二个 local generation owner，不产生冲突 metadata。

### A4：Standard metadata create / READY CAS response loss

步骤：全部 install 后，分别在标准 LedgerMetadata create-if-absent 与 sidecar READY CAS 的提交前、提交后 response loss、CAS conflict 注入；另注入 metadata create 成功但 READY 永久失败。

Oracle：重读标准 metadata 与 sidecar 后只能得到一个 backlink 一致的 instance；非 READY instance 不得 normal-active或返回 create/open success；不得盲建第二个 ledger instance。

### A5：相同 instance、不同 Profile

步骤：对已安装 instance 修改任一 mandatory descriptor 字段并重算 hash，发送 install/Add。

Oracle：确定返回 `EPROFILE_MISMATCH`；不得更新既有 routing，不得降级。

### A6：相同 ledgerId、不同 live instance

步骤：并发创建两个 instance，或在旧 instance 未终结时安装新 instance。

Oracle：至多一个 instance 可进入 READY/AVAILABLE；Bookie routing 与 immutable backlink 不得把两者合并。

### A7：Engine/capability mismatch

步骤：将 Segment descriptor 发送到 Classic cohort；分别缺失每个 mandatory capability。

Oracle：placement 排除或 install 明确失败；无 payload 写入，无 lazy Classic handle。

### A8：`E > W` write-set rotation

步骤：全部 E install 后逐 entry 轮转；另做只安装 W 个节点的负向控制。

Oracle：全 E 安装并激活时所有 write set 可用；只安装 W 的负向控制必须无法创建标准 metadata或发布 READY。

### A9：Active write-time ensemble replacement

步骤：在 active write failure 后选择满足 capability 的 replacement，durable inactive install，以现有 `LAC+1` fragment authority CAS 标准 metadata，发布 post-CAS activation authority，等待 replacement durable normal-active，再 resend pending Add。

Oracle：durable replacement install 早于 membership CAS；normal activation 晚于 exact CAS；pending resend 晚于 activation。该路径不复制历史 fragment，不做 per-entry metadata update。

### A10：Replacement install 失败

步骤：在 install validation/control durability/receipt、membership CAS response、activation authority/local durability/receipt 各处失败或丢包，并与 `IN_RECOVERY/CLOSED`、durable fence 竞争。

Oracle：install 失败节点不进入 ensemble；CAS winner 以 exact mapping 重读恢复；CAS 后 activation 未完成时不 resend；fence/tombstone 不被迟到 activation重开；允许选择新节点但不降低 Profile或盲目制造多个 target。

### A11：Bookie restart

步骤：在 install durable 前后、receipt 前后、首次 Add 前后重启。

Oracle：接受集合只由 durable record 决定；未 durable install 不复活，已 durable install 不丢失。

### A12：Old client / new Bookie

步骤：向 `ABSENT/CLASSIC` route 发送现有 Classic 请求。

Oracle：保持 Classic 兼容；不能误要求新 Profile identity，也不能把 Classic handle 标记为已安装新 Profile。命中 Profile route 的负向场景由 A17 覆盖。

### A13：New client / old Bookie

步骤：分别使用 client-only opaque Profile 和需要 install 的 Profile。

Oracle：client-only 按 Classic 兼容合同工作；installed/Segment Profile 在 placement 或 install 阶段明确失败。

### A14：Orphan install

步骤：分别在 sidecar reservation、部分/全部 install、标准 metadata create 和 READY CAS 各阶段永久放弃创建；触发候选 GC，并重放极晚 install/activation response与 retry。

Oracle：orphan 从未接受 normal Add；GC 同时读取 sidecar与标准 membership authority，并以 stable grace + durable tombstone 证明；GC 后旧请求不能重新激活。

### A15：Watch/cache stale

步骤：暂停 metadata watch、返回旧 cache，直接发送 instance/hash 不匹配 Add。

Oracle：Bookie 本地 durable install 校验仍 fail closed；watch 不是正确性依赖。

### A16：Install 完成但 activation/READY 未成立

步骤：全部 E 个 Bookie durable install 后，分别在标准 metadata create 前后、READY authority 提交前/确定失败后、local normal activation durable 前后，由持有合法 master key 的客户端发送 profiled Add；在 activation request/receipt response loss 时重试并重启 Bookie。

Oracle：缺少 matching global READY 或 local durable normal ACTIVE 的 normal Add 接受数为 0；客户端可复制的 epoch/field 不能单独激活；READY 可早于部分 local active，但 create/open success 必须晚于 all-E activation；restart 后接受集合不扩大。

### A17：Legacy Add targeting Profile route

步骤：使用 v2/v3 legacy normal Add 与 recovery Add 命中 `PROFILE/RESERVED`、installed、active 与 tombstoned route。

Oracle：请求在 Classic handle、master key 和 payload 创建前 fail closed；normal/recovery 变体均不能绕过 route。

### A18：首次 Classic Add 与 Profile install 并发

步骤：让 legacy Add 与 Profile install 同时观察初始 `ABSENT`，在 route claim、durable write、handle publish 和 response 各边界 crash/retry。

Oracle：最终只能存在一个 authoritative `CLASSIC` 或 `PROFILE` owner；不得同时持久化两种身份。legacy 先赢时 install 冲突；Profile 先赢时 legacy fail closed。

### A19：Hot-path performance boundary

步骤：对未启用 Profile 的 Classic-only baseline 与加入统一 route gate 后的 Classic path 做 matched offered-load 对比；对 Profile Add 记录 route lookup、CPU、allocation、metadata I/O 和 proof verification。

Oracle：普通 Add 的远程 MetadataStore I/O、descriptor canonical/hash、auth-binding hash/HMAC、KMS/signature/certificate与逐请求重型 proof verification均为0，只做缓存固定长度identity/verifier comparison；Classic throughput/p99与legacy connection建立wire/latency回归不超过运行前manifest锁定预算，且legacy Classic connection不执行Profile handshake。超限为FAIL或经预定义规则判为INCONCLUSIVE，不能事后放宽。

### A20：Legacy metadata mutation authority

步骤：让持有合法 master key、但没有 Profile metadata mutation authority 的 stale/legacy client，分别尝试 CAS 更换 profiled ledger ensemble、删除或替换 immutable Profile backlink，并与合法 Profile-aware replacement并发。

Oracle：未授权 mutation 必须在标准 LedgerMetadata CAS 生效前被拒绝；master key 不能隐含 metadata write authority。若目标 metadata driver/ACL 配置无法提供等价 enforcement，则该 Profile 组合明确为 unsupported/FAIL，不能依赖 sidecar事后修复或仍让 Spike PASS。exact ACL/credential encoding 保持开放。

### A21：Sidecar child/snapshot/ABA 与 unknown version

步骤：分别在child/page durable、manifest complete、domain-head CAS、response send、old child reclaim前后crash；在response loss前后用同一operation identity分别重试相同semantic payload与冲突payload，并覆盖operation被snapshot/terminal summary吸收及退出bounded retention；并发推进head与snapshot cut；对同一ledgerId创建新instance且让backend store version重新计数；注入referenced unknown mandatory root/child、missing chunk、suffix gap和root/page cap溢出。

Oracle：未被head发布的child只是inert orphan；same operation + same payload只返回同一`APPLIED/ALREADY_APPLIED`结果，same operation + conflicting payload只能`CONFLICT`且不改变authority；退出可证明retention后不得把任一payload当作新幂等成功。response loss按operation/snapshot identity返回already/stale/conflict；old/new instance绝不因store version重计数合并；snapshot只有verified chunks+manifest+stable cut+complete suffix才发布，publish早于reclaim；referenced unknown/missing/gap和超限使domain fail closed。已证明不相交repair domain不经过ledger-global universal head，normal Add路径不受sidecar fault/watch stale影响。

### A22：Local authority composition、stale handle 与 downgrade

步骤：在route+auth/install原子claim、normal activation、fence admission cut、recovery grant/close、tombstone、assignment readiness各边界crash/restart；保持stale handle并推进generation；让old binary读取unknown mandatory control format；覆盖multi-Arena ledger与control-store loss。

Oracle：restart接受集合不扩大；normal/recovery/readable不是互斥flat enum；stale handle不能跨activation/fence/tombstone generation成功；old binary在writable registration/handle create前fail closed；任一scattered authority缺失不default allow；normal Add无remote lookup/per-entry control fsync。

### A23：Canonical descriptor golden corpus

步骤：用至少两个独立codec/reference implementation覆盖同语义不同输入顺序、set/list顺序、absent/default、duplicate singular/map/set、unknown schema/field/enum/mandatory capability、optional hint变化、oversize/deep/numeric-bound输入、known-field old-reader rewrite/strip、declared hash mismatch与跨schema输入；持久化canonical bytes/hash后重启并重新计算。

Oracle：同schema同semantic contract产生完全相同bytes/hash；任何safety semantics变化改变identity；duplicate/unknown/oversize在allocation/state mutation前拒绝；optional neutral hint不改变semantic identity；cross-schema不自行等价；consumer不信任declared hash且旧writer不能strip/rewrite后发布。hash不被任何路径当作authorization；candidate必须固定长度、对对抗输入collision-resistant，CRC/checksum/不满足属性的过短截断candidate在正式运行前拒绝。

### A24：Control principal、protected binding 与 secret leak

步骤：分别用authenticated authorized、authenticated unauthorized、`AuthDisabledPlugin`/anonymous、只有合法master key、stale initial/replacement purpose、wrong target/incarnation/generation调用INSTALL/ACTIVATE/recovery grant/status；在direct authority read、local durable transition和response各边界丢包/restart。扫描全部log、metric、receipt、exception、event artifact，并触发`LedgerDescriptorImpl.checkAccess` mismatch。

Oracle：只有non-anonymous且对exact operation/ledger instance/target scope有授权的principal、并且exact committed authority匹配时control transition成功；AuthN-only/master key不授权control，AuthN/AuthZ早于route/credential/allocation/durable effect；initial/replacement/recovery purpose不能重放；response loss只解析同一operation/generation；credential-bearing Profile control/data transport满足manifest confidentiality/integrity；公开/诊断面不出现master key/password/verifier/offline digest/bearer capability，secret leak count为0。

### A25：Raw Profile decoder corpus 与 no downgrade

步骤：将每个合法Profile operation以及损坏、截断、length变异、unknown version/op/subtype、protobuf parse exception、随机prefix/suffix语料直接投喂manifest中的真实stock old v2/v3 decoder与new decoder；特别覆盖当前v3 decode `RuntimeException`后用同bytes切pre-v3的路径。捕获route claim、handle/master-key persistence、payload、ACK与connection mode。

Oracle：old decoder对任何Profile corpus均不产生legacy `ADDENTRY/RECOVERY_ADD` effect；new decoder识别Profile framing后parse error明确拒绝/关闭连接且不legacy fallback；client在unsupported、response loss或durability unknown后不Classic downgrade、不双写。

### A26：Handshake、mixed matrix 与 semantic error propagation

步骤：覆盖old/old Classic、old/new `ABSENT/CLASSIC/PROFILE/TOMBSTONED`、new Profile/old、mixed ensemble、Bookie restart/incarnation/protocol generation change、unknown capability与每类semantic failure；检查registration hint→connection handshake→durable receipt三层证据，贯通processor/client future/admin/metric。

Oracle：Profile create/open在old/mixed target上payload前失败；准备发送Profile operation的Profile-capable connection只在connect/reconnect协商，restart/generation变化重新handshake；old Classic client无需新handshake仍保持现行`ABSENT/CLASSIC`路径，共享pool中的Profile operation只能使用已协商context；registration/handshake不替代install receipt；unsupported、identity conflict、not-active/stale、fenced/deleted、grant mismatch、transient、durability unknown和unknown mandatory/corrupt不被压成会误导retry的generic success/unauthorized；协商不发生在每个Add。

## 7. 故障注入点

至少覆盖：

- sidecar reservation create/CAS 前后；
- child/page/manifest/domain-head CAS、snapshot cut/suffix anchor、covered-child reclaim前后；
- Bookie control record append、fsync、routing publish 前后；
- install receipt serialization/send 前后；
- standard LedgerMetadata create 与 immutable backlink publish 前后；
- READY authorization CAS、availability completion 前后；
- activation authority publish、Bookie activation durable、receipt send 前后；
- Classic/Profile atomic route claim 与 durable publish 各边界；
- active replacement install、`LAC+1` ensemble CAS、post-CAS activation、pending resend 各边界；
- Bookie restart/replay 中；
- local authority transition、stale-handle generation与old-binary format gate；
- canonical parser/hash、cold authority direct-read、control principal与protected binding各边界；
- Profile raw decoder corpus、connection handshake/reconnect与semantic error projection；
- coordinator restart 和 leader change；
- duplicate、delay、reorder、drop response。

fault injector 必须输出实际命中计数。计划注入但未命中的 case 不算执行。

## 8. 并发与随机测试

除确定性场景外，执行有 seed 的状态机 fuzz：

```text
create
install
activate
retry
retry sidecar operation with same/conflicting payload
cancel
profiled add
legacy normal/recovery add
restart Bookie
restart coordinator
replace Bookie
stale metadata read
snapshot/compact sidecar domain
inject unknown mandatory record
legacy metadata mutation
mutate canonical descriptor input
anonymous/stale-purpose control operation
raw malformed Profile frame
restart connection/renegotiate
```

每个 seed 的操作序列、fault sequence 和最终 oracle dump 必须保留。失败 seed 必须可单独重放。

## 9. 硬 Gate

PASS 必须同时满足：

```text
uninstalled new-profile Add accepted                = 0
profiled Add accepted without matching READY/local NORMAL_ACTIVE = 0
legacy Add bypassed Profile/Tombstoned route        = 0
dual Classic/Profile authoritative route owners     = 0
profile mismatch silently downgraded                = 0
instance mismatch silently merged                   = 0
engine/capability mismatch payload writes           = 0
ensemble metadata active before replacement install = 0
standard metadata before all-E Profile route claim = 0
normal ACTIVE before matching READY                 = 0
create/open success before all-E activation         = 0
pending resend before replacement normal ACTIVE    = 0
restart lost durable install                        = 0
restart resurrected non-durable install             = 0
restart expanded durable activation acceptance      = 0
duplicate READY/AVAILABLE instances                 = 0
normal Add remote metadata reads                    = 0
normal Add per-request heavy proof verifications    = 0
unreplayable executed fault scenarios               = 0
unauthorized profiled membership mutations          = 0
sidecar store-version/instance ABA                  = 0
child/page authorized before domain-head publication = 0
snapshot published without complete chunks/suffix  = 0
covered child reclaimed before snapshot publication = 0
referenced unknown mandatory record defaulted/overwritten = 0
same sidecar operation identity accepted conflicting payload = 0
old binary became writable on Segment control format = 0
stale handle crossed admission generation          = 0
normal Add per-entry control fsync                  = 0
descriptor golden-vector divergence                = 0
duplicate/unknown/oversize descriptor accepted     = 0
declared descriptor hash trusted without recompute = 0
descriptor hash used as authorization              = 0
anonymous/master-key-only control accepted         = 0
authenticated-but-unauthorized control accepted    = 0
control purpose/target/incarnation replay accepted = 0
secret/offline-verifier disclosure                 = 0
Profile bytes decoded as legacy ADD/RECOVERY_ADD   = 0
Profile parse failure triggered legacy fallback    = 0
Profile failure caused Classic fallback/double write = 0
mixed/old Profile target received payload          = 0
normal Add per-request capability negotiation      = 0
legacy Classic connection forced Profile handshake = 0
normal Add auth-binding hash/HMAC/KMS/signature/certificate invocations = 0
credential observed on unprotected Profile transport = 0
semantic safety error collapsed to success         = 0
```

所有指定 deterministic scenarios 必须 100% 执行并命中 fault；所有断言为硬失败，不接受“低概率”。

## 10. 立即停止条件

发现以下任一项立即停止扩展原型，保留现场并回到 RFC：

- 未安装 ledger 的新 Profile Add 被接受；
- 缺少 matching READY/local NORMAL_ACTIVE 的 profiled Add 被接受；
- legacy normal/recovery Add 绕过 Profile/Tombstoned route；
- 并发 claim 产生 Classic/Profile 双重 owner；
- mismatch 进入 Classic 或其他静默降级；
- replacement metadata 先于 durable install 生效；
- standard metadata 在 all-E Profile route claim 前创建；
- local normal ACTIVE 早于 matching READY，或 pending resend 早于 replacement activation；
- crash/retry 产生两个 READY/AVAILABLE instance；
- 同一sidecar operation identity接受两个冲突semantic payload，或把冲突payload返回为`ALREADY_APPLIED`；
- 持有 master key 但无 Profile metadata authority 的 legacy client 成功修改 profiled membership/backlink；
- Bookie restart 后接受未由 durable record 授权的请求；
- orphan GC 能让旧请求重新激活。
- canonical descriptor出现跨实现identity分叉、unknown/duplicate被接受或hash被当作authorization；
- anonymous/master-key-only caller完成Profile control transition，或任何secret/offline verifier进入公开/诊断surface；
- 任一Profile byte corpus被old decoder解释成legacy Add/RECOVERY_ADD，或new decoder错误触发legacy fallback；
- Profile失败/response loss触发Classic downgrade、双写或mixed target payload。

停止后不得通过修改 oracle、忽略 seed 或缩小协议结构继续宣称 PASS。

## 11. 必交 artifacts

```text
manifest.json
results.json
scenario-matrix.csv
event-log/
metadata-history/
bookie-control-dumps/
fault-injection-log/
failed-seed-reproducers/
descriptor-golden-corpus/
raw-wire-corpus/
old-new-decoder-results/
secret-leak-scan/
checksums.txt
README.md
```

`README.md` 必须写明 source/prototype/RFC revision、环境、开始结束时间、执行者、Gate 结果和已知未覆盖项。

## 12. 结果解释

- PASS：只证明该原型和锁定矩阵未否证 RFC-0001，可以进入 RFC 接受评审。
- FAIL：RFC-0001 保持 P0 Blocked，先处理 counterexample。
- INCONCLUSIVE：场景未执行、fault 未命中、证据缺失或 oracle 不可复现；不得按 PASS 处理。

即使 PASS，也不授权 Segment 正式实现、authority 切换或生产部署。
