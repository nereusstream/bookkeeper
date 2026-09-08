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
- 不把Round 7 executable frame/control-tail candidate提前声明为stable production wire，也不决定on-disk local authority layout；
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
activation-target identity/incarnation retention, lifecycle publication and snapshot/reclaim encoding
pre-admission resource status mapping, bounded retry/backoff/deadline and client credit limits
BookieCount
clientCount
E/W/A configurations
Bookie engine cohorts
protocol version matrix
supported old Bookie/client commits and built artifacts
descriptorCodec=strict-flat-TLV/BKPD/10-fields/big-endian
descriptorIdentitySuite=SHA-256/suite-1/36-byte-identity/exact-domain-separator
descriptorBounds=legal-124-plus-6N/124..508-bytes/absolute-input-cap-1024/10-fields/0..64-capabilities/depth-1
accepted failure-domain policy and capability registries
controlEndpoint=bookie-profile/immediate-TLS1.3/mTLS
principalMappingMode=SUBJECT_UNIQUE_WITHIN_TRUST_SET|TRUST_DOMAIN_ISSUER_PLUS_SUBJECT
principal/X509 allowlist, trust-set uniqueness evidence and non-anonymous policy
exact operation/instance/target-scope authorization policy
protectedCredential=kind-1/20-byte-verifier/redaction-revision
cold authority read/reference and activation mechanism
ProfileFrame=0x0FF04250/32-byte-header/exact-subtype-table; original 0x0FFE4250 counterexample frozen
HELLO/connection-context/status-retry-durable-result mapping
Profile/Classic pool isolation revision
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
- RFC-0001 `ProfileDescriptor/ProfileDescriptorCodec/ProfileDescriptorIdentity` exact TLV/SHA-256 codec；
- authoritative `.bin` golden corpus、typed fixture、expected digest/36-byte identity/field dump、strict bounded parser与不共享production parser helper的独立recomputation；
- `INSTALL_LEDGER_PROFILE` request/receipt；
- 独立immediate-TLS1.3/mTLS `bookie-profile` listener、X509 principal、exact authorizer、Bookie direct-read committed authority、protected kind-1 20-byte credential与secret-free status/receipt；
- Bookie durable install 与 activation authority；
- 单一、原子、可恢复的 Classic/Profile route slot；
- logical local authority state machine：orthogonal normal admission、bounded recovery grant/readable facts、fence/tombstone generation与Bookie registration readiness；
- Bookie restart replay；
- `LedgerContext`、`ADD_NORMAL/ADD_RECOVERY` executable bodies与Bookie本地durable activation/grant匹配；
- exact outer length/32-byte header/magic/subtype、HELLO/connection context、status/retry/durable result、connection-amortized handshake与no-fallback parser；
- 当前`RANGE_READ/BATCH_RECOVERY_ADD` reserved/disabled behavior；
- capability/engine placement filter；
- initial all-E inactive route claim before standard LedgerMetadata create；
- READY authorization before local normal ACTIVE，all-E initial activation before returning a newly created normal writer；
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
descriptorIdentity[36]
operationId[16]
publicSemanticPayloadIdentity[32]
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

Oracle：重读标准 metadata 与 sidecar 后只能得到一个 backlink 一致的 instance；非 READY instance 不得 normal-active或返回初始创建normal writer的success；不得盲建第二个 ledger instance。

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

步骤：在active write failure后选择满足capability的replacement，durable inactive install，以现有`LAC+1` fragment authority CAS标准metadata，再由conditional lifecycle publication同时发布activation authority和delete可枚举target/incarnation，等待replacement durable normal-active后resend。

Oracle：durable replacement install早于membership CAS；normal activation晚于exact CAS及持久target retention，pending resend晚于activation。LAC固定99，在同一fragment 100先换D并接收部分/完整DATA，再换E，最终map可无D但清理历史必须保留D/incarnation；旧D迟到ACK不计当前write set。该路径不复制历史fragment、不做per-entry metadata update。

### A10：Replacement install 失败

步骤：在 install validation/control durability/receipt、membership CAS response、activation authority/local durability/receipt 各处失败或丢包，并与 `IN_RECOVERY/CLOSED`、durable fence 竞争。

Oracle：install 失败节点不进入 ensemble；CAS winner 以 exact mapping 重读恢复；CAS 后 activation 未完成时不 resend；fence/tombstone 不被迟到 activation重开；允许选择新节点但不降低 Profile或盲目制造多个 target。

增加membership CAS已成功但delete先于activation-authority publication，以及authority/retention CAS已提交但响应丢失的顺序：前者无新授权、target仍inactive，后者即使离线也被delete枚举。inert child不授权；从未获授权的install沿原orphan GC，不用最终map缺席证明已授权目标无DATA。

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

初始E的READY与target/incarnation retention在同一lifecycle publication生效；注入child durable、READY CAS、local activation间crash/delete。任一初始目标激活或接收normal DATA前必须已可恢复枚举，history压缩后重试READY仍解析同一授权。

步骤：全部 E 个 Bookie durable install 后，分别在标准 metadata create 前后、READY authority 提交前/确定失败后、local normal activation durable 前后，由持有合法 master key 的客户端发送 profiled Add；在 activation request/receipt response loss 时重试并重启 Bookie。

在同一场景补充open purpose：已有CLOSED/fenced ledger一个Bookie离线，其他有效副本可读时，只读open成功且ACTIVATE/新增控制持久化次数为0；未验证read范围not-ready，不伪造absence。恢复open使用显式grant/durable close，不重新normal-active。初始writer仍等待全部E initial install/activation；同phase请求有界并发，不能用串行E次网络往返作为默认实现，跨phase依赖不变。

补充LAC分层：副本local LAC=99、entry 100已达ACK quorum且writer在下一次piggyback前崩溃，合法unconfirmed/恢复点读可读100，confirmed read仍由客户端确认边界限制。Bookie不能以local LAC拒绝候选或伪造absence，读取候选不直接发布recovered close；DATA completion不生成quorum LAC，显式LAC合法单调更新可合批，不增加每Add控制fsync。与B19及Model A-POINT联合验证。

Oracle：缺少 matching global READY 或 local durable normal ACTIVE 的 normal Add 接受数为 0；客户端可复制的 epoch/field 不能单独激活；READY 可早于部分 local active，但 初始创建返回normal writer必须晚于all-E initial activation；restart 后接受集合不扩大。

### A17：Legacy Add targeting Profile route

步骤：使用 v2/v3 legacy normal Add 与 recovery Add 命中 `PROFILE/RESERVED`、installed、active 与 tombstoned route。

Oracle：请求在 Classic handle、master key 和 payload 创建前 fail closed；normal/recovery 变体均不能绕过 route。

### A18：首次 Classic Add 与 Profile install 并发

步骤：让 legacy Add 与 Profile install 同时观察初始 `ABSENT`，在 route claim、durable write、handle publish 和 response 各边界 crash/retry。

Oracle：最终只能存在一个 authoritative `CLASSIC` 或 `PROFILE` owner；不得同时持久化两种身份。legacy 先赢时 install 冲突；Profile 先赢时 legacy fail closed。

### A19：Hot-path performance boundary

步骤：对未启用Profile的Classic-only endpoint/decoder/pool与加入统一route gate后的Classic path做matched offered-load对比；对Profile Add记录TLS record CPU、fixed frame/context bytes、route lookup、constant-time 20-byte credential compare、allocation、metadata I/O和proof verification。

Oracle：normal Add的远程MetadataStore I/O、descriptor parse/hash、auth-binding hash/HMAC、KMS/signature/certificate与逐请求重型proof均为0，只解析fixed header/context并比较36-byte identity/20-byte verifier；Classic throughput/p99和connection wire/latency回归不超过运行前预算，legacy Classic连接不执行TLS/HELLO/Profile framing。Profile TLS AEAD、约60–100 bytes identity/header和local lookup单独报告；超限为FAIL或按预定义规则INCONCLUSIVE，不能事后放宽。

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

步骤：production codec严格执行`BKPD` 16-byte header、十个递增TLV、合法长度`124 + 6 * capabilityCount`（124..508 bytes）、allocation前1024-byte绝对input cap和SHA-256 suite 1/domain separator；用不共享production parser helper的独立verifier覆盖capability count 0/1/64、508-byte合法向量、509/1024-byte非法向量、1025-byte allocation前oversize拒绝、E/W/A/F边界、policy generation，以及duplicate/out-of-order/missing/unknown schema/type/field/enum/capability、nonzero flags、wrong scalar/set/total length、truncation/oversize/trailing bytes/default alias、known-field old-reader rewrite/strip、declared identity mismatch与跨schema输入。

Oracle：authoritative `.bin`、typed fixture、expected SHA-256、36-byte identity和field dump逐byte一致；input必须原生canonical而不是parse→normalize后接受；任何safety语义变化改变identity；全部非法输入在allocation/state mutation前拒绝；cross-schema不自行等价；consumer重算identity，hash不授权。optional hint不存在于当前descriptor bytes；policy/capability registry未接受的production descriptor不得mint。

### A24：Control principal、protected binding 与 secret leak

步骤：用独立immediate-TLS1.3 listener分别覆盖valid/invalid/missing client cert、authorized/unauthorized X509 principal、coarse-OU-only role、`AuthDisabledPlugin`/anonymous、SASL-without-consumable-principal、只有合法master key、stale initial/replacement purpose及wrong operation/ledger/instance/target/range/incarnation/generation；根据manifest选择并验证受信CA集合内subject唯一性，或把trust-domain/issuer纳入principal mapping，并覆盖不同issuer/trust-domain签发相同subject。按 TLS/mTLS → bounded parse → non-anonymous/static operation precheck → fixed authority key → direct-read committed authority → exact post-read authorizer → credential validation → conditional local transition 的顺序，在static precheck、direct read、post-read AuthZ、local durable transition和response各边界丢包/restart。扫描全部log/metric/receipt/status/exception/event/admin dump并触发`LedgerDescriptorImpl.checkAccess` mismatch与secret wrapper `toString()`。

Oracle：只有non-anonymous mTLS principal、static operation precheck、exact post-read tuple authorizer与exact committed authority全部匹配时control transition成功；两处AuthZ fault rejection的route/credential/allocation/durable effect均为0，且整个cold operation只有一次authority read。AuthN-only/coarse role/master key不授权；manifest选择`SUBJECT_UNIQUE_WITHIN_TRUST_SET`时发现重复subject必须fail Gate，选择`TRUST_DOMAIN_ISSUER_PLUS_SUBJECT`时不同issuer/trust-domain不得映射为同一principal；initial/replacement/recovery purpose不能重放；same operation/public payload/secret幂等，conflicting payload或secret只返回coarse conflict；公开/诊断面master key/password/verifier/credential digest/bearer capability为0，secret wrapper固定`<redacted>`，normal Add certificate/signature验证为0。

Wave 0 partial evidence：typed reference endpoint的12项普通功能测试已覆盖immediate TLS1.3/mTLS transport facts、static/exact denial zero effect、single cold read、tuple/purpose/state、strict descriptor/Engine、durable-only success、redacted credential/status与dependency fail-closed。它不包含real listener/TLS provider/X509 extraction、concrete MetadataStore/protected local store、crash/restart、全surface secret scan或`LedgerDescriptorImpl.checkAccess()` regression，因此A24仍未PASS，Spike A状态不变。

### A25：Raw Profile decoder corpus 与 no downgrade

状态：**DEFERRED_NOT_RUN**。当前 Wave 0 不执行 released decoder 或 localhost stock binary 回放，已存在的 corpus、harness 与 `0x0FFE4250` counterexample 只冻结保留。此状态既不是PASS也不是waiver；A25未闭合，Spike A与G1继续BLOCK，只有未来单独显式授权的fresh run才能产生结果。

步骤：保留已否证`0x0FFE4250`的frozen反例，将TLS ClientHello、完整合法replacement `0x0FF04250` frame、每个subtype、magic逐byte翻转、1..31-byte header truncation、outer/body length/oversize、major/minor/headerLength/flags/reserved/subtype、malformed HELLO、合法v3 prefix后接Profile magic、触发current v3各种`RuntimeException`、pre-v3 version=0/nonzero与接近legacy ADD opcode的prefix、normal/recovery互换及seeded fuzz，投喂每个受支持真实stock old v2/v3 decoder/binary与new decoder。

Oracle：每个vector的Classic route claim、handle create、master-key persistence、ledger allocation、payload/journal write、ACK/OK与permanent legacy decoder downgrade followed by effect全为0；new decoder错误关闭连接且不legacy fallback；legacy resolver仍只选`bookie-rpc`。任一失败只调整magic/framing并重跑，不Classic downgrade或双写。

### A26：Handshake、mixed matrix 与 semantic error propagation

补充首批CRC32C布局绑定：创建/安装核对immutable capability、实验manifest及客户端/metadata声明；未知、缺失、不匹配和HMAC/CRC32/DUMMY等未支持布局在入口拒绝。replacement、只读与recovery消费同一已安装布局，不新增60-byte context字段、逐Add协商或生产capability ID；拒绝不能触发静默CRC32C转换或password/MAC-key下发。CRC32C与具体entry字节验证由B19配合，不将frame corpus当作布局语义PASS。

步骤：覆盖old/old Classic、old/new route matrix、new Profile/old、mixed ensemble、Bookie restart/incarnation/protocol generation change、HELLO第一帧/4KiB bound/strict capability order、server HELLO全部字段的byte-exact golden vector及`reserved:u16=0`/nonzero拒绝、BookieId/incarnation/readiness mismatch、Profile/Classic physical pool key、unknown capability，以及全部12类status（1 OK + 11 non-OK）、5类retry与4类durable result的固定数值golden vector；贯通processor/client future/admin/metric。

Oracle：Profile初始创建/install在old/mixed target上payload前失败；只读/恢复校验实际所需target操作，不支持的target不执行Profile语义也不降级，按既有读/恢复规则选有效副本或失败，不把all-E探测/activation作为通用open前置。Profile只在独立mTLS connection首次HELLO，restart/generation变化重连；Classic client/endpoint/pool没有Profile TLS/HELLO；physical channel key包含protocol/BookieId/incarnation/generation/TLS identity；registration hint、HELLO与durable receipt分层；unsupported/identity/stale/fenced/deleted/grant/transient/unknown/quarantine/unauthorized/bad-request/durability-unknown不坍缩成OK，external unauthorized可coarse但internal class保留，协商不发生在每Add。

在既有三元组矩阵验证RFC-0001 §11.5的资源映射：准入前资源拒绝进入有界same-operation backoff，已知activation未完成复用协调等待，提交后unknown不能投影成NONE；terminal fence/delete/conflict/unauthorized不靠换组绕过。检查分类发生在`handleBookieFailure`之前，不先压成通用WriteException；不为该行为修订旧wire枚举/corpus或历史receipt。

### A27：Add unknown、换组与ACK故障域

原target DATA durable后丢ACK并永久离线，按RFC-0001 §9.3执行inactive install → membership CAS → authority/target retention publication → activation → resend同一entry/payload；再注入旧target/旧incarnation迟到ACK、重复ACK、换组slot撤销以及连续前缀未完成。另覆盖最快ACK来自同一声明域、unknown域身份与policy检查被关闭的负向路径。

增加暂时资源拒绝后容量恢复、持续不可用达到既有故障阈值、已提交unknown后的重试收到NONE、activation协调中多个Add，以及退避期间deadline到达。首次预算尽量早于entryId/累计length分配；分配后保持同位置/payload重试或按ledger失败/恢复结束，不能跳过hole。retry沿原inflight/bytes计费，timer/waiter有界，不延长调用deadline。

Oracle：有合法替代资源和明确控制结果时，原DATA outcome unknown不阻止正式换组；控制INSTALL/ACTIVATE的unknown仍重试同operation。成功只使用当前write set和声明故障域内的有效ACK，不拼接旧投递集合，不改变逻辑payload，不重复callback或Classic fallback。故障域模型/`F`在run前锁定。

资源Oracle：短暂拥塞不直接触发replacement；容量恢复后原逻辑Add继续。当前拒绝NONE不抹除旧UNKNOWN/占位，已提交不能返回肯定未写入；持续故障达到锁定条件后仍可正式换组。与B18索引stall/满额场景联动，分别记录拒绝、retry与replacement数，未执行真实网络时不宣称集群效果。

### A28：Delete cut、membership freeze与完成发布

基础scope按RFC-0004 §14.1逐步展开admission、cold authority read、local grant、membership CAS与history retention；在每两步间插入DELETE_INTENT、标准metadata freeze、普通logical tombstone、本地access barrier、response loss与coordinator crash。至少一个历史target离线，logical成功后仍用旧reader访问尚未tombstoned的目标，再恢复节点并执行本地屏障与回收。

Oracle：delete后新open/admission失败；membership与freeze按同记录version分出赢家；普通logical completion只在authoritative tombstone及完整可恢复清理目标已durable时成立，离线target使撤权/物理清理pending，旧reader在本地tombstone前可读。旧grant仅能作用于已枚举scope，本地屏障后read/grant/local success均拒绝，free晚于reader/writer/I/O终结。基础恢复matching durable close不依赖strong-publication token，也不reset loss window。

复用A9同起点D→E：对已发布activation-target历史去重/分页，snapshot提交前后crash，回收旧页后重启再delete，D离线后rejoin。完整snapshot+suffix必须仍含D及其incarnation/授权义务，或有效终结证明；缺页/gap不能发布完整freeze。标准map不是全历史oracle，target retention与membership职责分开；记录冷路径CAS/留存bytes，不引入每Add控制更新。

强访问撤销及strong reset为独立DEFERRED配置，首批只验证能力拒绝及不创建token。后续启用时：强撤权必须等全部target屏障/永久服务隔离proof；strong reset必须整个ledger fenced+CLOSED，逐步执行token、prepare、lifecycle publication、resolve，注入sidecar故障、coordinator crash及pending loss，未最终发布candidate不能reset。已有token即使功能关闭也不得超时强删。记录新增冷CAS、membership阻塞时长、恢复解析和前台影响；不能因不在per-entry路径就忽略停顿。

### A29：CLIENT-1当前ACK数量与故障域一致

在现有`PendingAddOpTest`和真实rack-aware policy层构造`E/W/A=3/3/2`：旧ensemble为A/X、B/Y、C/Y；entry e先收到C ACK，另一pending entry超时触发C→D/X replacement，C仍在knownBookies。e随后仅收到A和D ACK，B未成功。必须仍不满足两rack约束；B的有效ACK到达后才可成功，迟到C不能补覆盖。

另构造e的`completed=true`但callback被先前pending/换组阻挡：撤销唯一异域ACK后数量仍>=A而覆盖不足，必须撤销completed并等待有效新覆盖。覆盖多slot replacement、`E>W`未涉及本entry write set的变更、重复/旧地址响应、策略启用/关闭、timeout统计和对象复用。oracle从当前mapping与实际有效响应独立计算，不复用被测成功集合；关闭policy时不引入冗余集合或通用rack检查，数量及连续前缀不变。

### A30：CLIENT-2最后响应与对象池归还

控制请求计数、initiate完成和逻辑callback时序，分别令最后响应来自被替换旧Bookie、当前Bookie，以及所有响应先于逻辑callback。观察`PendingAddOp` recycler归还和`toSend`引用计数：满足既有回收条件时恰好归还一次，callback未完成时不提前归还，buffer不重复/提前释放，旧地址响应仍不计ACK。覆盖与timeout监控交错及下一次对象复用，保留既有同步保护。不能仅凭GC或heap曲线推断通过。

A29/A30是独立Classic客户端修复的确定性回归，不运行Profile listener或Segment ACK。源码事实已检查，修复/测试均PLANNED / NOT EXECUTED；它们可先于Wave 0模型和原型实施，结果不提升Spike整体或production authority。

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
normal activation without durable delete-enumerable target retention = 0
authorized target lost by same-start replacement or history compaction = 0
new activation authority published after delete admission cut = 0
immediate replacement caused solely by transient resource rejection = 0
prior logical Add UNKNOWN erased by current-attempt resource NONE = 0
backpressure retry exceeded budget/deadline or skipped assigned entry = 0
initial normal writer returned before all-E activation         = 0
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
noncanonical descriptor alias accepted              = 0
descriptor bytes/identity mismatch                 = 0
declared descriptor digest trusted without recompute = 0
descriptor digest used as authorization              = 0
anonymous/master-key-only control accepted         = 0
authenticated-but-unauthorized control accepted    = 0
ambiguous X509 subject principal accepted          = 0
control purpose/target/incarnation replay accepted = 0
secret/offline-verifier disclosure                 = 0
raw credential rendered outside redacted wrapper   = 0
Profile bytes decoded as legacy ADD/RECOVERY_ADD   = 0
Profile parse failure triggered legacy fallback    = 0
Profile corpus caused Classic route/handle/allocation/journal/payload/ACK effect = 0
Profile failure caused Classic fallback/double write = 0
mixed/old Profile target received payload          = 0
normal Add per-request capability negotiation      = 0
legacy Classic connection forced Profile handshake = 0
Profile operation used Classic physical pool/channel = 0
HELLO identity/incarnation/readiness mismatch accepted = 0
normal Add auth-binding hash/HMAC/KMS/signature/certificate invocations = 0
credential observed on unprotected Profile transport = 0
semantic safety error collapsed to success         = 0
stale delivery ACK counted in current quorum/domain set = 0
logical Add identity changed during replacement    = 0
Profile ACK succeeded without declared domain coverage = 0
membership mutation bypassed durable freeze marker = 0
completion mapping changed while publication token held = 0
unpublished completion candidate reset loss window = 0
logical delete completed without durable tombstone and complete cleanup targets = 0
strong access revocation completed before all access barriers = 0
first-scope active ledger created strong-publication token = 0
basic recovery or membership result reset loss window = 0
replacement completed with stale ACK/domain coverage = 0
final response missed eligible recycler return = 0
premature/double recycler or buffer release = 0
read-only open invoked or waited for normal activation = 0
recovery open implicitly re-enabled normal writes = 0
unsupported/mismatched entry layout installed or silently converted = 0
password or MAC key distributed for first-scope digest validation = 0
```

所选启用scope的指定deterministic scenarios必须100%执行并命中fault；所有断言为硬失败，不接受“低概率”。manifest在run前锁定scope，延期增强矩阵保持DEFERRED，基础scope的拒绝能力测试不算增强PASS；不得运行后排除失败场景。单项客户端回归或局部原型结果不宣称整个Spike PASS。

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
tls-mtls-authz-matrix/
hello-status-projection-matrix/
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
