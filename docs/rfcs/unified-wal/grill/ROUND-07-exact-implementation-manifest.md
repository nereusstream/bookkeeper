# Round 7：Exact Implementation Manifest 与实施解锁边界

> Reviewer：复用 Round 1–6 的固定 `gpt-5.6-sol / max` agent<br>
> 源码与文档基线：`9885dbda26f55dbe3c20c530a36b22109d76b7e2`<br>
> 评审模式：exact implementation manifest、旧 decoder/旧 binary 兼容、热路径性能、复杂度和过度设计联合审查<br>
> 记录规则：以下 reviewer 回复未经删节；仅将 Markdown heading 统一下移一级并清理行尾空白，以保持文档单一 H1。规范是否采纳以对应 RFC 的集成结果为准。

## 本轮 Questions

### Q1：Descriptor exact manifest

选择可直接编码、严格 canonical 且不进入 Add 热路径的 codec/hash/field/bounds/golden-corpus manifest；无法由 owner RFC 决定的 registry 明确保持 OPEN/BLOCK。

### Q2：Control/auth exact manifest

基于现有 Bookie AuthN/TLS/AuthZ/MetadataStore 能力冻结 endpoint、transport、exact operation/instance/target-scope AuthZ、cold committed-authority read、protected local binding、redaction与 response-loss；不引入通用 RBAC 或 per-Add crypto。

### Q3：Wire exact manifest

冻结不会被旧 decoder 解释为 Classic Add 的独立 framing、HELLO、subtype、bounds、error/retry/close 与共享 pool 边界，并以真实 v3 RuntimeException→pre-v3 fallback corpus作为稳定 wire Gate。

### Q4：Format/readiness exact manifest

核对 Cookie/VERSION/startup/replay/registration，选择 stock old binary 的 mandatory pre-replay fence；若同 BookieId/storage scope无法证明安全，则锁定 new BookieId/new storage scope fallback并保持 same-scope candidate BLOCK。

### Q5：Implementation unlock

逐层裁定 Docs、harness/Model、reference codec、isolated prototype、stable wire/on-disk、live shadow与ACK authority能否开始，并给出下一轮最小 frontier。

## Reviewer 完整回复

<!-- BEGIN VERBATIM REVIEW -->

## Unified WAL P0 Grill Round 7：Exact Implementation Manifest 独立审查结论

### 总结论

| 题目 | Verdict |
| --- | --- |
| Q1 Descriptor exact manifest | **REVISE 后可 LOCK**：严格扁平 TLV、SHA-256 与 hard bounds 可立即冻结；failure-domain policy/capability 的业务编号表仍受各 owner RFC 阻塞 |
| Q2 Control/auth exact manifest | **REVISE 后可 LOCK**：独立、immediate-TLS Profile endpoint 与 exact-scope AuthZ/interface 可冻结；local protected state 的物理 owner/packing 仍 BLOCK |
| Q3 Wire compatibility exact manifest | **REVISE 后可 LOCK 为测试 manifest**：独立 endpoint、固定 frame/header/subtype 候选可冻结用于 corpus；在真实旧 decoder Gate 通过前不能成为稳定 wire 合同 |
| Q4 Format/readiness exact manifest | **BLOCK（同 BookieId / 同 storage scope）**：源码给出了可测试的 Cookie sentinel 候选，但不能证明全部 stock old binary 在触碰 storage 前 fail-stop；安全回退可立即 LOCK 为新 BookieId + 新 storage scope + 独立凭据 |
| Q5 Implementation unlock | **部分 YES**：Docs、harness/Model、reference codec 和隔离 prototype 可开始；稳定 wire/on-disk、live shadow、Segment ACK authority 均 NO |

本轮确实把实现面大幅收窄，但没有证据支持把整个项目标为 `Implementation Ready`。尤其不能因为已经选出 wire/format 候选，就把尚未执行的旧二进制兼容性 Gate 当成已通过。

---

### 1. 基线与源码事实

核对结果：

```text
repository: /Users/liusinan/apps/ideaproject/nereusstream/bookkeeper
branch:     turbo-bk
HEAD:       9885dbda26f55dbe3c20c530a36b22109d76b7e2
origin:     nereusstream/bookkeeper
ahead/behind HEAD...origin/turbo-bk: 0/0
```

工作区仅有未跟踪的 `BtrLog Low-Latency Logging.pdf`。本轮未读取、未触碰该文件，也没有修改、暂存、commit 或 push。

#### 1.1 Java/protobuf 依赖事实

- 项目使用 Java 17。
- 根 POM 管理 protobuf `4.34.0`，但 `bookkeeper-proto` 当前协议实际由 `io.streamnative.lightproto:lightproto-maven-plugin:0.7.0` 生成。
- `bookkeeper-proto` 模块自身只直接依赖 Netty buffer，不依赖 protobuf-java runtime。
- 当前 `BookkeeperProtocol.proto` 是 proto2。
- 仓库没有现成的 deterministic protobuf serialization 使用证据，也没有证明 LightProto 对 duplicate singular field、unknown field、default/absent 和非最短编码提供了 strict-canonical input contract。

因此，单纯启用“deterministic protobuf output”不能解决：

- 重复 singular field；
- unknown field 保留/丢弃；
- absent/default 同义；
- old writer strip/rewrite；
- 同一语义存在多个可接受 input bytes。

要用 protobuf 做 canonical descriptor，至少还需要一层 raw-wire strict validator；这比一个约 100 行的扁平 bounded codec 更复杂。

#### 1.2 旧 wire decoder 的真实风险

当前 Bookie legacy endpoint：

1. 使用 4-byte big-endian length prefix；
2. 默认最大 frame 为 5 MiB；
3. `BookieProtoEncoding.RequestDecoder` 先尝试 LightProto v3；
4. 任一 `RuntimeException` 会把整个连接永久切到 pre-v3；
5. 同一 byte buffer reset 后按 legacy header 重解码。

pre-v3 header 的第二个 byte 是 opcode；只要被解释为 `ADDENTRY=1`，decoder 就会读 20-byte master key 并把剩余 bytes 当 entry。

所以：

- 给现有 `ADD_ENTRY` 增加 optional Profile 字段不安全；
- 只在 proto enum 中新增 operation 也不足；
- malformed Profile frame 不能复用当前 v3→pre-v3 fallback；
- stable Profile bytes 必须用真实支持范围内的 old decoder corpus 证明不会产生 Classic route、handle、payload 或 ACK effect。

#### 1.3 当前 auth 实现边界

`BookieAuthProvider` 是连接级握手接口：

- 成功回调只把连接标为 authenticated；
- 没有 operation/ledger/instance/target-scope AuthZ 方法；
- 默认 `AuthDisabledPlugin` 会成功完成认证并把 principal 设为 `ANONYMOUS`；
- `BookieAuthZFactory` 只从 TLS leaf certificate 的第一个 OU 取 role，并与全局 `authorizedRoles` allowlist 比较；
- 它不是 exact operation/ledger/target authorizer；
- `SASLBookieAuthProvider` 验证 allowed ID 后没有调用 `ConnectionPeer.setAuthorizedId()`，不能假设其结果天然提供可消费的 non-anonymous principal。

当前 TLS server context 使用 `startTls(true)`，是 legacy `START_TLS` 形态；不能直接当成“连接第一字节即 TLS”的独立 Profile listener。

可复用的是证书、trust store、Netty/JDK TLS provider 和连接 principal 基础设施，不是现行 coarse AuthZ 语义。

#### 1.4 endpoint discovery 对独立 Profile listener 友好

`BookieServiceInfo` 已能发布多个带 `protocol` 的 endpoint。现行 `DefaultBookieAddressResolver` 明确只选择：

```text
protocol == "bookie-rpc"
```

因此新增例如 `bookie-profile-v1` endpoint，不要求 legacy client 认识它，也不需要改变原有 `bookie-rpc` 解析规则。这是选择独立 endpoint 的重要源码依据。

#### 1.5 Cookie、startup 与 registration 事实

- `Cookie.CURRENT_COOKIE_LAYOUT_VERSION=5`。
- `Cookie.verifyInternal()` 对 `layoutVersion >= 3` 不要求与当前版本相等，单纯 bump 版本不能阻止旧 binary。
- Cookie 第一行不是十进制整数时，当前 parser 会抛 `IOException`。
- Data-integrity cookie validator 对无法解析的 Cookie 会直接失败，不会进入 mismatch 后的 auto-stamp 分支。
- 但 data-integrity 模式下，`EmbeddedServer` 会先创建 `LedgerStorage`，随后才运行 Cookie validation。
- legacy 模式会先 Cookie validation，再创建 `LedgerStorage`。
- Cookie 本地 `VERSION` 当前通过直接 `FileOutputStream` 覆写，没有 temp-file、fsync、atomic rename、parent fsync 合同。
- `BookieImpl.start()` 才执行 Journal replay；之后才 writable registration。
- Journal replay 对未知 negative entry id 明确跳过，不能作为 mandatory downgrade fence。
- `RegistrationManager.registerBookie()` 只有 `bookieId/readOnly/BookieServiceInfo`；ZooKeeper 实现只是 ephemeral create，没有 expected readiness generation、storage incarnation CAS 或 stale writer fence。
- `writeCookie()` 在 ZK/etcd 上已有 versioned write 能力，但 Cookie schema本身没有 Profile mandatory format 语义。

这些事实给出了一个可测试的 nonnumeric Cookie sentinel 候选，但还不能证明它在全部 stock old binary、所有启动模式和 physical owner 选择下都早于“触碰 Segment local authority”。

---

## Q1 — Descriptor exact manifest

### Verdict：REVISE 后可 LOCK

推荐首版选择：

> JDK/Netty-only、扁平、严格、有序、固定宽度整数 TLV。

拒绝两个替代：

- **deterministic protobuf + 普通 parser**：不能拒绝同义异 bytes；不成立。
- **deterministic protobuf + raw strict prevalidator**：能做，但引入两层解析规则和 LightProto/protobuf 行为依赖，复杂度高于首版需要。
- **完全固定 struct**：当前 capability/failure policy 注册表尚未最终接受，过早固定全部 tail 会迫使不兼容扩展。

### 2.1 Exact canonical envelope

所有整数 unsigned、big-endian。canonical bytes 包含 header 与全部 fields：

```text
DescriptorHeaderV1                 16 bytes
  magic                            4 bytes = 42 4b 50 44 ("BKPD")
  codecVersion                    u16 = 1
  semanticSchemaVersion           u16 = 1
  totalLength                     u32, inclusive
  fieldCount                      u16 = 10
  flags                           u16 = 0

FieldHeaderV1                       8 bytes
  fieldId                         u16
  type                            u16
  valueLength                     u32
```

字段必须按 `fieldId` 严格递增，首版恰好十个字段，每个正好出现一次：

| fieldId | 字段 | type | value |
| --- | --- | --- | --- |
| 1 | requiredEngineProfile | `U16=1` | u16 enum |
| 2 | payloadFormat | `U16=1` | u16 enum |
| 3 | durabilityMode | `U16=1` | u16 enum |
| 4 | ensembleSize | `U16=1` | u16 |
| 5 | writeQuorumSize | `U16=1` | u16 |
| 6 | ackQuorumSize | `U16=1` | u16 |
| 7 | permanentLossBudgetF | `U16=1` | u16 |
| 8 | failureDomainPolicyId | `U32=2` | u32 |
| 9 | failureDomainPolicyGeneration | `U64=3` | u64 |
| 10 | mandatoryCapabilities | `CAPABILITY_SET=4` | bounded vector |

`CAPABILITY_SET`：

```text
count                            u16
repeated count times:
  capabilityId                  u32
  semanticVersion              u16
```

规则：

- capability 按 `capabilityId` 严格递增；
- 一个 capabilityId 只能出现一次；
- id 必须非零；
- semanticVersion 必须非零；
- 相同 ID 不能靠重复多个版本表达兼容范围；
- writer 必须选择一个 exact semantic version；
- unknown ID/version 一律拒绝。

首版不要同时保留 `indexProfile/sequenceProfile/recoveryProfile/deleteProfile` 四个重复真相。它们应由 mandatory capability registry 表达；否则会出现 `deleteProfile=CONDITIONAL` 但 capability vector 缺失或冲突的双源状态。

Engine/payload/durability 的首版 numeric registry 可以冻结为：

```text
Engine:
  1 CLASSIC_ENGINE
  2 DIRECT_JOURNAL_ENGINE
  3 SEGMENT_WAL_ENGINE

Payload:
  1 OPAQUE_LEDGER

Durability:
  1 SYNC_ON_ACK
  2 DEFERRED_SYNC_LEGACY
```

failure-domain policy 的具体 ID/默认 domain 和 capability ID registry 仍归其 owner RFC，不能在本轮凭空分配。这是剩余 BLOCK，不影响 codec 本身实现。

### 2.2 Exact bounds 与 validator

首版 hard bounds：

```text
canonical descriptor bytes       124..1024
fieldCount                       exactly 10
capability count                 0..64
nesting depth                    1
semantic strings                 0
free-form semantic bytes         0
unknown fields                   0 accepted
unknown type                     0 accepted
optional semantic fields         0
```

值验证：

```text
1 <= A <= W <= E <= 65535
0 <= F < A
failureDomainPolicyId != 0
failureDomainPolicyGeneration != 0
all enums known
all mandatory capabilities known
cross-field Profile compatibility table passes
totalLength == actual bytes
all fixed scalar lengths exact
CAPABILITY_SET length == 2 + count * 6
no trailing bytes
```

不要“parse 后重新编码并接受”；input 本身必须已经 canonical。任何：

- duplicate；
- out-of-order；
- unknown；
- reserved/flags 非零；
- wrong length；
- trailing bytes；
- omitted field；
- default alias；

都直接拒绝。

首版 `OptionalHintEnvelope` 不进入 semantic descriptor，也不随 descriptor V1 编码。需要 hint 时使用独立、明确标为 safety-neutral 的外层记录；V1 descriptor 内 hint 的 count/bytes 都是零。

### 2.3 Exact hash/identity

推荐冻结：

```text
hashSuiteId = 1
algorithm   = JDK MessageDigest "SHA-256"
digestSize  = 32 bytes
```

preimage：

```text
US-ASCII "org.apache.bookkeeper/ProfileDescriptor/v1\0"
|| uint16BE(hashSuiteId = 1)
|| uint16BE(semanticSchemaVersion = 1)
|| uint32BE(canonicalBytes.length)
|| canonicalBytes
```

descriptor identity bytes：

```text
uint16BE(hashSuiteId)
|| uint16BE(semanticSchemaVersion)
|| digest[32]
```

总长 36 bytes。

该 hash 只做 identity/integrity：

- 不授权；
- 不证明 publisher；
- 不提供 secrecy；
- 不替代 MetadataStore CAS；
- 不是 capability；
- 不需要签名、Merkle、cluster HMAC 或 PKI。

### 2.4 Version evolution

- codec version、semantic schema、hash suite 分开。
- V1 reader 对任一 unknown codec/schema/type/field/capability 拒绝。
- V1 writer 不得读取新 schema 后按 V1 重写。
- 新 optional semantic field 不能偷偷附加到 schema 1；必须发布新 schema。
- 不跨 schema 宣称 semantic equivalence。
- 同一 ledger instance descriptor immutable；任何 descriptor 变化使用新 instance。
- 首版同 instance credential rotation继续禁止。

### 2.5 Golden corpus 与 reference implementation

应创建一个 JDK-only production codec：

```text
ProfileDescriptorV1
ProfileDescriptorCodecV1
ProfileDescriptorIdentity
```

生产 encoder 是唯一 canonical writer；decoder strict-validate input。

测试资产至少包括：

- authoritative `.bin` canonical bytes；
- descriptor typed fixture；
- expected 36-byte identity；
- expected SHA-256；
- human-readable field dump；
- 一个不共享 production parser helper 的独立 test verifier。

正向 corpus：

- 每个合法 Engine/payload/durability 组合；
- capability count 0/1/64；
- quorum/F 边界；
- multiple policy generations；
- max 1024-byte padding不可存在，确保实际 canonical 长度稳定。

负向 corpus：

- duplicate/out-of-order field；
- duplicate capability；
- missing required field；
- unknown enum/type/field/schema；
- nonzero flags；
- truncated/oversized/length mismatch；
- trailing bytes；
- E/W/A/F 不合法；
- policy generation 0；
- capability count 65；
- same semantics different bytes；
- different semantics same declared identity。

### 2.6 性能与复杂度

- descriptor encode/hash 只在 create/install/open/control cold path。
- normal Add 只携带/比较 36-byte descriptor identity。
- 不在 Add 解析 descriptor、遍历 capability、执行 SHA-256。
- 最大 descriptor 1 KiB，不产生大对象或递归解析。
- 手写 codec 比 protobuf+strict raw validator 更小、更容易 fuzz。
- 不引入新的 runtime dependency。

### 2.7 Q1 可 LOCK 与剩余 BLOCK

可立即 LOCK：

- 上述 envelope、field IDs、type encoding、ordering、duplicate/default/unknown rules；
- 1024 bytes、64 capabilities、depth/string/free-bytes bounds；
- SHA-256 suite、domain separator、36-byte identity；
- strict parser/golden corpus策略；
- index/sequence/recovery/delete 不重复成为第二套字段。

仍 BLOCK/OPEN：

- failure-domain policy ID/default/domain semantics；
- capability ID 与 semanticVersion registry；
- 每个高层 Profile 具体要求哪些 capability；
- cross-field legal-combination table中尚未被 owner RFC 接受的条目。

因此可以编码 codec，但不能据此 mint 一个包含未接受 capability 的生产 descriptor。

---

## Q2 — Control/auth exact manifest

### Verdict：REVISE 后可 LOCK；local physical owner 仍 BLOCK

### 3.1 endpoint 选择

推荐冻结：

> 一个独立 `bookie-profile-v1` TCP listener，连接第一字节即 TLS；不复用 legacy `bookie-rpc` port，也不使用 legacy START_TLS。

首版 transport：

```text
TLS protocol: TLSv1.3
server authentication: required
client certificate: required
client principal: leaf X509 subject X500Principal.CANONICAL
trust/key material: reuse existing BookKeeper TLS configuration/provider
application framing: Q3 Profile frame
```

这不是新建 descriptor-signing PKI，也不是让证书签每条 Add；只是复用现有 operator TLS trust model保护 credential-bearing transport。

选择所有 Profile connection 都 mTLS，而不是同一 listener 上做 optional-client-cert/control-only升级，原因是：

- 状态更少；
- 不需要第二个 control listener；
- control operation天然有 non-anonymous principal；
- certificate validation只发生在连接握手；
- normal Add不做 certificate/signature verification；
- Profile 是新协议，不影响 Classic client。

现有 `TLSContextFactory.startTls(true)` 不能原样使用；需要一个共享相同 key/trust 配置、但 `startTls(false)` 且 `ClientAuth.REQUIRE` 的 Profile server context。

拒绝：

- legacy port + optional Profile field；
- legacy port + START_TLS 后再猜 Profile；
- AuthDisabled；
- SASL-only 当前实现；
- master key 作为 control capability；
- caller 自带 READY blob；
-每 Add读取 MetadataStore；
- bearer install certificate；
-通用 RBAC 平台。

### 3.2 最小 control authorization 接口

冻结一个小接口，而不是通用 RBAC：

```text
ProfileControlAuthorizer.authorize(
    ProfilePrincipal principal,
    ProfileControlScope scope,
    CommittedAuthority authority
) -> ALLOW | DENY
```

`ProfileControlScope` 至少含：

```text
logical operation/purpose
ledgerId
ledgerInstanceId[16]
descriptorIdentity[36]
target Bookie stable identity
target storageIncarnation[16]
authority domain
authority generation
operationId[16]
public semantic payload identity[32]
range/fragment scope, where applicable
```

首版 operation classes：

```text
INSTALL
ACTIVATE_INITIAL
ACTIVATE_REPLACEMENT
RECOVERY_GRANT
RECOVERY_GRANT_CLOSE
TOMBSTONE_OR_DELETE_APPLY
STATUS_QUERY
```

READY publication不是 Bookie RPC；它由 coordinator 通过 MetadataStore ACL/CAS 发布。Bookie只 direct-read并验证已经 committed 的 READY。

AuthZ 的准确含义是：

1. mTLS principal 被配置允许执行该 operation class；
2. request 的 exact ledger/instance/target/scope 与 committed authority 匹配；
3. committed authority 的 purpose、generation、operation identity和target允许该 effect。

不要求静态 ACL 为每个 ledger保存一行，也不能只做“controller role可以任意 INSTALL”。exact tuple 必须进入决策，动态 ledger scope由 committed authority收窄。

具体 ACL 文件、role命名和后端仍 OPEN；接口与检查顺序可 LOCK。

### 3.3 cold direct committed-authority read

新增 domain-specific 接口：

```text
ProfileControlStore.read(AuthorityKey)
    -> VersionedAuthority(bytes, opaqueStoreVersion)

ProfileControlStore.compareAndSet(...)
```

Bookie：

- 从 request 的 ledger/instance/purpose 派生 key；
- 不信任 caller 传入任意 MetadataStore path；
- direct-read committed record；
- strict-parse schema/mandatory fields；
-验证 store version、semantic generation、instance、descriptor、target/incarnation、operation/purpose；
- watch/cache只做加速；
- read unavailable返回 transient/reconciling；
- unknown/gap/corrupt返回 quarantined/fail-closed。

INSTALL/ACTIVATE 每个操作至多一次 cold MetadataStore read；normal Add 为零。

### 3.4 exact verification 顺序

#### INSTALL

1. TLS/mTLS已完成；
2. principal non-anonymous；
3. 解析并在分配前验证 bounds/schema；
4. AuthZ exact INSTALL tuple；
5. direct-read committed PREPARING/descriptor authority；
6. 验证 ledger/instance/descriptor/Engine/capability/target/incarnation/op id/public payload identity；
7. 验证 credential kind/length；
8. conditional local transition原子安装：
   - route=PROFILE；
   - instance；
   - descriptor identity；
   - Engine；
   - protected credential；
   - install generation；
   - normal-inactive；
9. local durability barrier完成后返回 secret-free result。

#### ACTIVATE_INITIAL

1. exact ACTIVATE_INITIAL AuthZ；
2. direct-read READY；
3. 验证 READY 与 initial metadata version/ensemble digest；
4. 验证target、incarnation、local install generation；
5. fence/tombstone仍未先赢；
6. durable normal-active；
7. response。

#### ACTIVATE_REPLACEMENT

与 initial 分 purpose，必须读取 post-membership authority；不能重放 initial activation。

#### Normal Add

只做：

```text
Profile frame/context valid
ledgerId/instance/descriptor identity match
local route == PROFILE
local role == NORMAL_ACTIVE
fence/tombstone/admission cut permits
20-byte data credential constant-time match
entry/payload checks
existing data durability barrier
```

不做 MetadataStore、descriptor hash、HMAC、KMS、certificate、signature或control fsync。

#### Recovery Add

额外要求：

```text
distinct ADD_RECOVERY subtype
exact admitted RepairIntent
target/incarnation match
bounded local recovery grant match
range/grant generation match
grant not closed
delete/tombstone/fence check
```

不能用 legacy recovery flag或NORMAL_ACTIVE替代 recovery grant。

### 3.5 protected local binding exact logical representation

首版继续使用现有 20-byte BookKeeper master-key verifier作为 data credential：

```text
credentialKind      u16 = 1  // BK_MASTER_KEY_SHA1_V1
credentialLength    u16 = 20
credentialBytes     20 bytes, secret
```

local durable logical record绑定：

```text
ledgerId
ledgerInstanceId
descriptorIdentity
credentialKind
secret credential bytes
install generation
route/activation/fence state
```

关键收窄：

- 不再为它计算一个公开 unkeyed `authBindingHash`；
- 不把 credential bytes或其可离线验证 digest写入 receipt/status/log/metric/exception；
- normal Add 用 `MessageDigest.isEqual()` 或等价 constant-time fixed-length compare；
- same-instance credential immutable；
- credential改变使用新 instance；
- TLS certificate rotation只重建连接，不修改 ledger data credential；
- fence/tombstone是撤销接受集合的机制。

物理 owner、是否加密 at rest、record packing、group commit和secure deletion仍由 Round 5 local state frontier决定，不能在 Q2 伪锁为某个 Journal/Arena store。

建议接口：

```text
ProtectedProfileStateStore
  conditionalInstall(...)
  conditionalActivate(...)
  queryOperationResult(...)
  compareDataCredential(...)
```

它是语义接口，不要求一个新的通用 local transaction engine。

### 3.6 idempotency、response loss与 secret-free status

每个 control `operationId[16]`绑定单一 public semantic payload identity。

- same op + same public payload + same protected credential：原结果或等价 `ALREADY_APPLIED`；
- same op + conflicting public payload：`CONFLICT`；
- same op + conflicting secret credential：`CONFLICT`，但不回显哪一部分不匹配；
- operation status可返回：
  - NOT_FOUND；
  - APPLIED；
  - ALREADY_APPLIED；
  - CONFLICT；
  - DURABILITY_UNKNOWN；
  - STALE/COMPACTED；
- status不返回 master key、verifier、credential digest或bearer token；
- response loss后只重试/查询相同 endpoint、subtype、operationId和payload；
- 不盲建新 install/activation generation；
- 不回退 Classic。

INSTALL 的 public payload identity必须排除 credential bytes；secret是否相同由受保护 local state比较。否则 unkeyed payload digest会成为离线 verifier。

### 3.7 redaction Gate

Profile secret wrapper必须：

- `toString()`固定输出 `<redacted>`；
- exception不携带 secret；
- structured logger不接受 raw credential字段；
- receipt/status/admin dump不含 secret/digest；
- metric label不含 ledger/op/credential高基数内容；
-安装完成后尽力清零临时 heap/ByteBuf；
-对现存 `LedgerDescriptorImpl.checkAccess()` 输出完整 master key 的路径建立 hard regression test。

### 3.8 性能/复杂度

成本：

- 每连接一次 TLS/mTLS + HELLO；
- INSTALL/ACTIVATE各一次 direct authority read；
- 每个 local semantic transition走现有可选择的 group commit/durability机制；
- Profile data每 request解析固定 header和固定长度 identity；
- TLS record AEAD是transport成本，必须 benchmark。

明确为零：

- Classic connection Profile handshake；
- normal Add MetadataStore/KMS；
- normal Add descriptor/hash/HMAC/certificate/signature；
- per-Add control fsync；
- bearer proof验证；
- general RBAC lookup；
- control watch correctness dependency。

### 3.9 Q2 OPEN/BLOCK

仍 OPEN：

-具体 principal allowlist配置格式；
- X509 subject之外未来是否支持修复后的SASL principal；
- MetadataStore backend path；
- local at-rest encryption/tooling；
- certificate/trust rotation runbook；
- connection pool大小/timeout。

仍 BLOCK：

- `ProtectedProfileStateStore` 的物理 owner与 crash-record framing；
- production secret-at-rest承诺；
-未通过 secret-leak/TLS/auth negative matrix前的生产控制面。

---

## Q3 — Wire compatibility exact manifest

### Verdict：REVISE 后可 LOCK 为 executable test manifest；stable wire 仍 BLOCK

### 4.1 transport/framing 选择

冻结候选：

- 独立 `bookie-profile-v1` endpoint；
- TLS ClientHello 从连接第一字节开始；
- TLS解密后使用 4-byte unsigned big-endian frame length；
- legacy `bookie-rpc`不接收Profile应用frame；
- Profile client pool与Classic pool完全分开。

Profile application header：

```text
outerLength                         u32 BE
  // excludes this 4-byte prefix;
  // equals 32 + bodyLength

ProfileFrameHeaderV1                32 bytes
  magic                             u32 = 0x0FFE4250
  protocolMajor                     u16 = 1
  protocolMinor                     u16 = 0
  headerLength                      u16 = 32
  subtype                           u16
  flags                             u32
  requestId                         u64
  bodyLength                        u32
  reserved                          u32 = 0
```

选择 `0x0FFE4250` 的理由：

- 第一 byte `0x0f` 对 protobuf 是 field 1 + invalid wire type 7，应触发 v3 parse failure；
- legacy header 的 version byte为 `0x0f`、opcode为 `0xfe`，不是 ADD/READ/AUTH；
- 因此当前源码路径会在 pre-v3 unknown opcode 上停止，而不会读 master key；
- 这个结论仍必须由真实旧 LightProto/v2/v3 decoder corpus验证，不能只靠位级推导。

flags：

```text
0x00000001 RESPONSE
0x00000002 ERROR
0x00000004 END_OF_STREAM
all other bits: reject
```

request flags通常为零；未知 major/minor/headerLength/flags/reserved一律拒绝并关闭连接。

不增加 header CRC。TLS已提供传输完整性；CRC不会提供安全价值，只增加热路径成本。

### 4.2 subtype allocation

```text
0x0001 HELLO

0x0101 INSTALL
0x0102 ACTIVATE_INITIAL
0x0103 ACTIVATE_REPLACEMENT
0x0104 RECOVERY_GRANT
0x0105 RECOVERY_GRANT_CLOSE
0x0106 TOMBSTONE_OR_DELETE_APPLY
0x0107 OPERATION_STATUS

0x0201 ADD_NORMAL
0x0202 ADD_RECOVERY
0x0203 READ_ENTRY
0x0204 FENCE_LEDGER
0x0205 READ_LAC
0x0206 WRITE_LAC
0x0207 FORCE_LEDGER
0x0208 LIST_ENTRIES

0x0301 RANGE_READ              reserved, disabled in v1
0x0302 BATCH_RECOVERY_ADD      reserved, disabled in v1
```

response使用相同 subtype + `RESPONSE` flag，不为每个response再分配opcode。

READY不是Bookie subtype；它是MetadataStore authority publication。

### 4.3 HELLO exact body

HELLO必须是连接上的第一条application frame，且最大4 KiB。

Client：

```text
minMajor                         u16 = 1
maxMajor                         u16 = 1
minMinor                         u16 = 0
maxMinor                         u16 = 0
capabilityCount                  u16 <= 64
reserved                         u16 = 0
capabilities[capabilityCount]:
  capabilityId                  u32
  semanticVersion              u16
  flags                         u16 = 0
```

capabilities严格按ID递增、无重复、unknown mandatory拒绝。

Server HELLO response：

```text
selectedMajor                    u16 = 1
selectedMinor                    u16 = 0
activeEngine                     u16
reserved                         u16 = 0
bookieIdLength                   u16 <= 255
capabilityCount                  u16 <= 64
storageIncarnation               16 opaque nonzero bytes
readinessGeneration              u64
bookieId                         UTF-8 bytes, no NUL
capabilities                     same 8-byte entries
```

client必须将 BookieId/storageIncarnation/readiness 与 placement/registration上下文匹配。HELLO完成后这些值进入connection context，不需要在每 Add重复 target身份。

Classic connection完全不执行该HELLO。Profile operation不能借用未协商的legacy channel；共享上层pool时，physical channel key必须至少包含：

```text
endpoint protocol
BookieId
storage incarnation
Profile protocol generation
TLS/auth identity
```

### 4.4 common ledger context与 data bodies

```text
LedgerContextV1                    60 bytes
  ledgerId                        i64
  ledgerInstanceId                16 opaque nonzero bytes
  descriptorIdentity              36 bytes
```

`ADD_NORMAL`：

```text
LedgerContextV1
entryId                           i64
writeFlags                        u32
credentialKind                    u16 = 1
credentialLength                  u16 = 20
credential                        20 secret bytes
entryLength                       u32
entryPayload                      entryLength bytes
```

服务端还必须验证 existing BookKeeper entry payload内部的 ledger/entry coordinate与 envelope一致，避免双重坐标分歧。

`ADD_RECOVERY`：

```text
LedgerContextV1
repairIntentId                    16 bytes
repairIntentGeneration            u64
grantGeneration                   u64
rangeStart                        i64
rangeEnd                          i64
entryId                           i64
credentialKind                    u16 = 1
credentialLength                  u16 = 20
credential                        20 secret bytes
entryLength                       u32
entryPayload
```

target incarnation由HELLO context和local grant共同绑定。range必须包含entryId。normal与recovery不能互换。

其他 data operation：

```text
READ_ENTRY:
  LedgerContextV1
  entryId i64
  readFlags u32

FENCE_LEDGER:
  LedgerContextV1
  operationId[16]
  credentialKind/length/credential

READ_LAC:
  LedgerContextV1

WRITE_LAC:
  LedgerContextV1
  lac i64
  credentialKind/length/credential
  bodyLength u32
  body

FORCE_LEDGER:
  LedgerContextV1
  operationId[16]

LIST_ENTRIES:
  LedgerContextV1
```

不要在每个请求重复 Engine、capability vector、full descriptor、READY proof、target BookieId或certificate。

### 4.5 control common context

```text
ControlContextV1                  136 bytes
  operationId                    16
  LedgerContextV1                60
  targetStorageIncarnation       16
  authorityPurpose               u16
  reserved                       u16 = 0
  authorityGeneration            u64
  publicPayloadIdentity          32
```

operation tail：

- INSTALL：canonical descriptor（max 1024）、credential block、requested install generation；
- ACTIVATE：matching local install generation与activation generation；
- RECOVERY_GRANT：intent/range/grant generation；
- CLOSE：same intent/grant；
- TOMBSTONE：delete authority generation；
- STATUS：original operation subtype。

public payload identity 使用 SHA-256 的 control-domain-separated cold-path digest，但明确排除 secret credential bytes。它不授权 operation。

上述 exact tail field表仍需 RFC owner 在 stable wire前逐项确认；在此之前可以实现 codec prototype，不能发布兼容承诺。

### 4.6 frame/batch bounds

```text
pre-HELLO frame hard max        4096 bytes
control frame hard max          65536 bytes
all Profile frame hard max      5242880 bytes
descriptor in any frame         1024 bytes
HELLO capabilities              64
string count                    1 only in HELLO response
BookieId bytes                  255
nesting depth                   1
```

frame length在分配 body前验证。

`RANGE_READ` 与 `BATCH_RECOVERY_ADD` 在 v1 manifest中：

```text
advertised capability = absent
max batch count = 0
accepted body = none
result = UNSUPPORTED
```

这比现在凭空选择1024条或4 MiB batch更小。待Model E与recovery wire owner闭合后再通过新 capability/version启用。

### 4.7 semantic status与 projection

response body先含：

```text
statusClass                      u16
retryDisposition                 u8
durableResult                    u8
detailCode                       u16
reserved                         u16 = 0
```

建议冻结 status class：

```text
0  OK
1  UNSUPPORTED_PROTOCOL_OPCODE_CAPABILITY_ENGINE
2  PROFILE_IDENTITY_CONFLICT
3  PROFILE_NOT_READY_OR_STALE
4  FENCED
5  TOMBSTONED_OR_DELETED
6  RECOVERY_GRANT_INVALID
7  TRANSIENT_UNAVAILABLE
8  DURABILITY_RESULT_UNKNOWN
9  QUARANTINED_OR_UNKNOWN_MANDATORY
10 UNAUTHORIZED
11 BAD_REQUEST
```

retry disposition：

```text
0 NEVER
1 SAME_PROFILE_OPERATION
2 AFTER_CONTROL_RECONCILIATION
3 REPLACE_TARGET
4 ATTEMPT_BOUNDED_OR_CANCELLED
```

durable result：

```text
0 NONE
1 APPLIED
2 ALREADY_APPLIED
3 UNKNOWN
```

外部 unauthorized response可以coarse；detailCode不得泄漏credential匹配信息。

Java兼容投影：

- Profile client/admin保留完整status/retry/durableResult；
- legacy callback只能返回非OK安全降级，绝不把 partial/unknown映射为OK；
- `FENCED`可继续映射现有 LedgerFenced；
- `UNAUTHORIZED`映射现有 Unauthorized；
- unsupported/not-ready/transient可在legacy表面降级为相应非OK Bookie unavailable/illegal operation；
- conflict/grant/quarantine/durability unknown不能丢失于coordinator/admin rich result；
- exact新BKException class与一般E/W/A recovery outcome API仍 BLOCK。

本表不替代Round 4的五类 recovery outcome；它只是operation transport status。

### 4.8 retry与close规则

- TLS/HELLO response loss：重连并重新协商。
- control response loss：相同 endpoint/subtype/opId/public payload查询或重试。
- Add response loss：相同 Profile subtype、instance、entry和payload重试。
- protocol major、magic、header、length、unknown subtype错误：关闭连接，无fallback。
- well-formed但不支持的capability：返回UNSUPPORTED，可关闭或仅继续支持已协商operation。
- unauthorized control：coarse error后关闭。
- conflict/fenced/deleted：不重试同target。
- durability unknown：只允许same Profile operation/status query。
- 任一close/EBADREQ/unsupported都不触发Classic encoder。
- 不同时向Profile和Classic双写。
-同entry不同payload不能被response-loss逻辑覆盖。

### 4.9 raw old-decoder corpus Gate

production LOCK前必须把至少下列 bytes 投喂每个受支持的真实 old binary/decoder：

- TLS ClientHello prefix；
-完整合法Profile frame；
-每个subtype；
- magic每个byte翻转；
-截断1..31-byte header；
-非法outer/body length；
- oversized；
- unknown major/minor/subtype/flags；
- malformed HELLO；
-合法v3前缀后接Profile magic；
-使current v3 parser抛各种RuntimeException的变体；
-使pre-v3 version=0及非0分支变化的prefix；
- first bytes接近 legacy ADD opcode；
- recovery subtype互换；
-随机/fuzz corpus。

每个vector断言：

```text
no Classic route claim
no Classic handle create
no master-key persistence
no ledger allocation
no payload write
no journal write
no ACK/OK
no permanent decoder downgrade followed by effect
```

同时验证 legacy `bookie-rpc`仍只选protocol=`bookie-rpc`，新增endpoint不改变old client解析。

### 4.10 Q3 LOCK/BLOCK结论

可冻结为 executable manifest：

- 独立 endpoint；
- immediate TLS；
- frame header、magic、subtype allocation；
- HELLO与connection context；
- fixed data common fields；
- frame bounds；
- response/status/retry classes；
- no downgrade/dual-write。

仍 BLOCK：

- 在真实旧 decoder corpus PASS前，把这些 bytes声明为stable production wire；
- control operation tail最终field表；
- batch/range encoding；
-一般E/W/A recovery result Java API；
- production pool/backpressure数值。

---

## Q4 — Format/readiness exact manifest

### Verdict：BLOCK（同 BookieId / 同 storage scope）；安全回退可 LOCK

### 5.1 可测试但尚不能锁定的 same-scope candidate

当前源码提示最小候选是复用旧 binary 必读的 Cookie surface，并把第一行改成非数字 sentinel，例如：

```text
42 4b 50 46 31 0a
"B K P F 1 \\n"
```

候选记录：

```text
"BKPF1\n"
recordLength u32
BookieFormatRecordV1 bytes
CRC32C u32
```

同一 exact bytes 通过 versioned CAS存入 metadata Cookie，并通过：

```text
write temp
fsync temp
atomic rename
fsync parent
```

发布到所有 required current/VERSION。

为什么它值得进入Spike：

- 当前 old parser第一行 `Integer.parseInt()` 会失败；
- invalid parse不会进入 data-integrity auto-stamp；
- metadata Cookie本身已有versioned write；
-先CAS metadata sentinel可在逐device publication前把当前源码old binary挡住；
- CRC32C只做损坏检测，不承担semantic identity。

为什么现在仍不能LOCK：

1. 尚未用全部受支持stock old binary证明同样行为；
2. data-integrity模式在Cookie validation前创建`LedgerStorage`；
3. local VERSION当前写入不是原子的；
4. physical Segment authority目录/owner尚未选定，无法证明old `LedgerStorage`不会先触碰；
5. 未证明所有旧启动入口、工具和storage expansion路径都必读同一surface；
6. 未证明正在运行的旧进程已经被exclusive drain/fence；
7. 没有完整partial multi-device crash corpus。

所以 `BKPF1` 只能归档为Spike B candidate/open question，不能写成最终format合同。

### 5.2 可立即 LOCK 的安全 fallback

如果same-scope Gate不能证明，首版必须使用：

```text
new BookieId
+ new journal/ledger/index/Arena storage roots
+ new storage incarnation
+ separate service/storage access credentials or ACL
+ separate Profile endpoint/readiness
```

具体约束：

- old binary配置和OS/service credential不能打开新scope；
- old BookieId/storage保持drained、readonly或decommissioned；
-不能只换目录名字但继续让old service account有写权限；
- Profile placement只选择matching readiness的新BookieId；
- ordinary ensemble replacement不能冒充decommission/wipe proof；
-新scope任何unknown/corrupt format仍non-writable；
-不做in-place rollback到旧scope。

这是本轮唯一可证明的old-binary安全形状。

### 5.3 Bookie/device/Arena logical manifest

即使使用新scope，仍需三个层次：

#### Bookie compatibility/readiness record

绑定：

```text
Bookie stable ID
storageIncarnation[16]
active Engine
Profile wire major/minor
descriptor hash suite
mandatory local feature set
device manifest identity/generation
local format generation
effective delete-assignment generation
minimum reader/writer
migration state/generation
```

#### Device/Arena superblock

绑定：

```text
storage incarnation
device identity
Arena format/version
ArenaControlLog/checkpoint format
mandatory features
active checkpoint/superblock generation
```

具体bytes、A/B slot、checksum与physical owner仍归RFC-0003/Spike B。

#### Cluster readiness

persistent versioned record绑定：

```text
Bookie ID
storage incarnation
Engine
protocol/capability generation
local format generation
device manifest identity
effective assignment generation
local readiness generation
minimum reader/writer
READY state
```

现有 `BookieServiceInfo` ephemeral registration只发布：

- `bookie-profile-v1` endpoint；
- readiness generation/reference；
- capability hint。

它不替代persistent CAS。

### 5.4 registration interface

需要新增：

```text
ProfileRegistrationStore.read(bookieId)
ProfileRegistrationStore.compareAndSet(
    bookieId,
    expectedStoreVersion,
    expectedReadinessGeneration,
    newRecord
)
```

随后才调用现有 ephemeral `registerBookie()`。

顺序：

1. persistent readiness CAS成功；
2.发布matching BookieServiceInfo；
3. ephemeral writable registration；
4. response loss后重读persistent record与ephemeral registration；
5. generation/incarnation不匹配则demote/non-writable。

backend exact path/schema现在不能LOCK，因为：

-现有RegistrationManager没有通用CAS readiness API；
- ZK与etcd driver布局不同；
- physical store owner仍开放。

可以锁接口、record semantics与顺序；exact path/escaping/adapter实现保持BLOCK。

### 5.5 startup顺序

冻结：

1. 在创建/打开任何Profile local authority、Journal replay或writable registration前读取compatibility fence；
2. 验证BookieId/storage incarnation/Engine/migration state；
3. 验证device manifest；
4.验证每个required device/Arena superblock；
5. unknown/corrupt/missing/partial mismatch进入non-writable quarantine；
6.恢复ArenaControlLog/checkpoint；
7.恢复route/auth/activation/fence/grant/tombstone；
8.完成delete assignment catch-up；
9. durable local readiness；
10. persistent readiness CAS；
11. ephemeral registration；
12.最后开启Profile write acceptance。

必须把hook移到 `EmbeddedServer` 创建任何可能接触Segment storage的组件之前，不能只放在现有 `BookieImpl.checkEnvironment()` 或 registration前。

### 5.6 migration/crash/response loss

same-scope候选顺序只能是：

1. drain旧writable与所有连接；
2.取得storage exclusive lock；
3. cluster CAS发布PREPARED/old-binary sentinel；
4. response loss重读CAS；
5.逐required directory原子写local sentinel；
6.逐device初始化superblock；
7.恢复/验证local authority；
8.发布FORMAT_READY；
9.发布persistent readiness；
10.注册Profile endpoint。

任一步 crash：

- old binary必须被第一道fence挡住；
- new binary看到partial state non-writable；
-不能把missing marker猜成Classic/new disk；
-不能让auto-stamp覆盖unknown mandatory；
-不需要跨device transaction。

但在Spike B证明前，上述仍是candidate，不是production contract。

### 5.7 rollback

允许旧Classic cohort继续运行的前提是它与新scope完全隔离。

新Profile scope一旦存在任一：

- local success；
- route/install/activation；
- fence/grant/tombstone；
- Arena allocation/control；
- durability-unknown outcome；

就不能启动old binary解释同一scope。

恢复方式只能：

- roll forward；
- verified export/rebuild；
- irreversible wipe/decommission；
-以新storage incarnation重新加入。

exact reverse/wipe CLI仍BLOCK。

### 5.8 Spike B硬 Gate

必须运行真实binary matrix：

-每个受支持old release/commit；
- data-integrity enabled/disabled；
- legacy/storage expansion入口；
- metadata Cookie sentinel；
- local VERSION sentinel；
- sentinel response loss；
-每个multi-device crash cut；
- missing/partial/corrupt marker；
- unknown mandatory/optional；
-旧进程已运行、未完全退出、stale storage lock；
- Cookie auto-stamp；
- BookieId reuse；
- stale ephemeral registration；
- partial device migration；
- rollback before/after first local success；
- new BookieId/new scope fallback；
- cold/warm startup bytes、I/O、latency和device-count scaling。

断言old binary：

```text
exits before Journal replay
does not register writable
does not open/write Profile authority/Arena
does not stamp over mandatory marker
```

因为当前 data-integrity路径先create storage，Gate还必须instrument“触碰了哪些文件/bytes”，不能只看最终未注册。

### 5.9 性能与过度设计

全部format/readiness成本位于启动/迁移/registration：

-一次Bookie marker/readiness读取；
-每required device一次superblock读取；
-一次persistent CAS；
-一次ephemeral registration。

明确禁止：

- per-Add format read；
- per-Add registration lease；
-每ledger一份Engine superblock；
-跨device原子事务；
-全局集群锁；
-用SHA/Merkle证明本地format；
-让Classic cohort执行Profile启动检查。

---

## Q5 — Implementation unlock

### 6.1 分层 verdict

| 层级 | 是否可开始 | 边界 |
| --- | --- | --- |
| Docs integration | **YES** | 集成本轮LOCK、candidate、BLOCK，完整归档本回复 |
| Harness / Model | **YES** | Descriptor corpus、auth negative matrix、wire raw corpus、format/old-binary harness；Model只抽象状态 |
| Descriptor reference implementation | **YES** | 可编码V1 strict codec/hash/golden vectors；不能分配未接受的policy/capability业务ID |
| Control/auth interface prototype | **YES** | 独立immediate-TLS listener、mTLS principal、exact-scope authorizer、cold-read adapter、redaction |
| Wire codec prototype | **YES，experimental only** | 按本轮frame/magic/subtype编码并投喂old decoder；不得声明stable |
| Format fence prototype | **YES，experimental only** | `BKPF1` Cookie sentinel和new-scope fallback harness |
| Stable public wire | **NO** | raw old-v2/v3/stock-binary corpus未PASS，control tail未最终闭合 |
| Stable on-disk format | **NO** | same-scope fence BLOCK；local physical authority owner/record framing未选 |
| Live shadow | **NO** | 只允许隔离、可丢弃、无ACK authority、不会污染Classic rollback cohort的shadow prototype |
| Segment ACK authority | **NO** | descriptor registry、wire Gate、format fence、local owner、RFC acceptance、Spike A/B/C均未闭合 |
| Overall Implementation Ready | **NO** | 仍是 Proposed / P0 Blocked |

因此，“Round 7 后可以开始实施”的准确说法是：

> 可以开始 reference codec、manifest harness、独立endpoint/control interface和隔离prototype的编码；不能开始稳定wire/on-disk兼容面、live authority promotion或Segment ACK接管。

---

## 7. 本轮完整 LOCK 清单

1. Descriptor V1使用扁平strict TLV，不用普通protobuf canonicalization。
2. Descriptor header、field header、十个field IDs与big-endian编码按Q1冻结。
3. index/sequence/recovery/delete语义统一进入mandatory capability set，不维护第二份互相冲突的profile字段。
4. Descriptor V1 hard max 1024 bytes、fieldCount=10、capability count≤64、depth=1、semantic string/bytes=0。
5. duplicate、out-of-order、unknown、missing、trailing、wrong length全部拒绝。
6. E/W/A/F与policy generation验证规则冻结。
7. SHA-256、suite ID 1、domain separator、36-byte identity冻结。
8. canonical input必须本身canonical，不接受parse→normalize alias。
9. 同instance descriptor/auth immutable。
10. descriptor不需要签名、Merkle、PKI或per-Add hash。
11. Profile使用独立`bookie-profile-v1` endpoint；不复用legacy port。
12. Profile listener第一字节即TLS，不使用START_TLS。
13.首版Profile connection统一TLS1.3+mTLS；证书验证只在连接建立。
14. control caller必须non-anonymous并经过exact tuple AuthZ。
15. current `BookieAuthZFactory` coarse OU allowlist不足以单独满足Profile AuthZ。
16. Bookie对INSTALL/ACTIVATE direct-read committed authority。
17. READY不是Bookie wire operation。
18. normal Add没有MetadataStore/hash/HMAC/KMS/certificate/signature/control-fsync。
19. data credential继续是protected 20-byte verifier；不生成公开offline-verifier digest。
20. receipt/status/log/metric/exception不携带secret或可离线验证派生物。
21. same operation+same payload幂等；conflicting payload/secret conflict且不泄漏。
22. frame外层length和32-byte header候选冻结为Round7 executable manifest。
23. Profile magic候选为`0x0FFE4250`。
24. HELLO/control/data subtype allocation按Q3冻结为测试manifest。
25. normal/recovery/read/fence/LAC使用distinct mandatory subtype。
26. Profile connection parsing失败永不进入legacy fallback。
27. Profile pool与Classic pool分离。
28. HELLO只在Profile connection一次执行；Classic无handshake。
29. frame/control/HELLO hard bounds按Q3冻结。
30. range/batch V1禁用，count=0，不凭空冻结优化参数。
31. semantic status/retry/durable-result分类端到端保留。
32.任一Profile失败/close/response loss不Classic downgrade或双写。
33. stable wire必须通过真实old decoder raw corpus。
34. same-scope old-binary format fence在证据通过前保持BLOCK。
35.无法证明same-scope时，首版必须使用new BookieId/new storage scope/new credentials。
36. compatibility fence早于任何Profile storage open/replay/register/write。
37. readiness使用persistent versioned CAS；BookieServiceInfo只做endpoint/hint。
38. device/Arena superblock不替代Bookie old-binary fence。
39. partial migration/unknown/corrupt/missing mandatory state均non-writable。
40. Segment authority存在后禁止old binary直接rollback。
41. format/readiness检查不进入normal Add。
42.不引入global lease、cross-device transaction或per-entry format/metadata state。

---

## 8. 必须保持 OPEN / BLOCK 的清单

### OPEN

- exact TLS cipher/provider tuning；
- principal allowlist配置文件和future SASL adapter；
- Profile client connection pool/backoff数值；
- MetadataStore sidecar/readiness backend path；
- operation detailCode与诊断文本；
- runtime lower frame limit；
- startup/performance Gate最终阈值和硬件；
-未来hint envelope；
-未来batch/range版本。

### BLOCK

- failure-domain policy ID/default/domain registry；
- mandatory capability ID/semanticVersion registry；
-每个production Profile的exact capability combination；
- local protected route/auth/activation state的physical owner；
- local crash record framing与at-rest protection；
- control tail最终字段表；
- stable production wire，直到raw old decoder Gate PASS；
- same BookieId/storage scope old-visible fence；
- Bookie/device/Arena exact superblock bytes；
- registration CAS exact backend path/schema；
- migration/reverse/wipe CLI；
- general E/W/A recovery outcome exact Java API；
- live shadow promotion；
- Segment ACK authority。

---

## 9. 需同步的位置

### RFC-0001

- §5：替换为Descriptor V1 exact TLV/hash manifest，并把index/sequence/recovery/delete收敛为capability registry。
- §6.6/§7：独立Profile endpoint、mTLS、exact authorizer、cold direct-read、protected 20-byte credential。
- §8：Q3 `LedgerContextV1`与normal/recovery distinction。
- §11：独立endpoint/framing/magic/HELLO/no fallback。
- §14：descriptor golden corpus、auth/redaction、raw decoder Gates。
- §15：policy/capability registry、control tail、physical owner继续BLOCK。

### RFC-0003

-只同步Q4的owner边界：
  - Arena/device superblock bytes仍OPEN/BLOCK；
  - compatibility fence不由Arena superblock单独承担；
  -不拥有registration/auth。
- Spike B通过前不能把任何superblock布局写成accepted format。

### RFC-0004

- recovery wire使用`ADD_RECOVERY`、intent/grant/range字段；
- range/batch subtype V1 reserved/disabled；
-一般E/W/A outcome API继续BLOCK；
- status class不能把authority loss误报data loss。

### RFC-0005

- 独立Profile listener与startup hook；
- protected local binding logical representation；
- normal Add fixed local comparisons；
- readiness persistent CAS vs ephemeral registration；
- same-scope fence BLOCK与new-scope fallback；
- physical owner仍OPEN。

### Spike A

新增：

- descriptor exact corpus；
- independent hash recomputation；
- TLS1.3/mTLS/authz negative matrix；
- anonymous/AuthDisabled/SASL-without-principal拒绝；
-secret leak scan；
- exact raw frame corpus；
- current与stock old decoder；
- endpoint discovery/Classic resolver；
- per-Add instrumentation。

### Spike B

新增：

- `BKPF1` metadata/local Cookie candidate；
- data-integrity pre-storage-open instrumentation；
- stock old binary matrix；
- Cookie auto-stamp；
- atomic local publication crash cuts；
- new BookieId/new scope fallback；
-persistent readiness adapter；
- cold/warm/device-scale成本。

### Spike C

只增加抽象：

```text
DescriptorCanonical
WireNegotiated
ControlAuthorized
AuthorityRead
OldBinaryBlocked
FormatCompatible
RegistrationGenerationMatches
```

不要在Model中放TLS、SHA、TLV bytes或filesystem细节。

### `docs/turbo-bk.md`

- Stage 2记录 exact manifest candidate与evidence Gate的区别；
- “可开始实施”限定为reference/harness/isolated prototype；
- stable wire/on-disk与Segment ACK仍BLOCK；
-本轮没有解除Round 5 physical owner和Model E API frontier。

### Grill archive

新增Round 7 archive时应完整保存本回复。Integration Audit通过前，README保持：

```text
Review Complete / Integration In Progress
```

---

## 10. 过度设计与性能检查

本轮推荐没有引入：

- descriptor签名；
- Merkle tree；
- per-Add proof；
- per-Add MetadataStore；
- per-Add descriptor/auth hash；
- KMS；
- global lease；
- universal lifecycle head；
- per-entry metadata node；
- per-entry control fsync；
- cross-device transaction；
-通用RBAC平台。

新增成本仅是：

- 1 KiB以内descriptor cold encode/hash；
- Profile连接一次TLS/mTLS+HELLO；
- INSTALL/ACTIVATE cold authority read；
-固定header/identity解析；
-启动marker/device/readiness检查；
-persistent readiness CAS。

唯一值得重点benchmark的新增数据路径成本是：

- Profile TLS record processing；
-固定约60–100 bytes request identity/header；
- local route/auth/active lookup；
- constant-time 20-byte verifier comparison。

Classic endpoint、decoder和pool不应承担这些成本。

---

## 11. 距整体 Implementation Ready 的最小 frontier

下一轮不应再讨论“是否需要PKI/Merkle/per-Add proof”等总体方向。最小剩余frontier是证据驱动的两项闭合：

1. **执行而不是继续设计 Q3**
   - 实现Round 7 frame candidate；
   -跑真实old v2/v3/raw byte corpus；
   -若任何vector产生Classic effect，只修改magic/framing，不重开架构；
   -通过后才能把test manifest晋升stable wire。

2. **Q4 + Round 5 physical owner 联合闭合**
   - 决定local route/auth/activation/format authority的物理owner与record framing；
   -用该owner评估`BKPF1`是否真的pre-open；
   -跑stock old binary、auto-stamp、partial-device、rollback Gate；
   -如果失败，正式采用new BookieId/new scope fallback，不再保留同scope幻想。

之后仍需：

- capability/failure-domain registry由owner RFC接受；
- general E/W/A recovery outcome API；
- RFC-0001/0003/0005接受；
- Spike A/B/C实际PASS；
-隔离shadow与canary-specific ACK/fence/rollback证据。

最终结论：

> Round 7 足以开始精确接口、reference codec、compatibility harness和隔离prototype的编码；不足以开始稳定wire/on-disk实现、live shadow promotion或Segment ACK authority。

<!-- END VERBATIM REVIEW -->


## Integration Audit 1

> Reviewer：同一固定 `gpt-5.6-sol / max` agent
> 结论：**REVISE**
> 归档规则：以下回复未经删节；仅将 Markdown heading 统一下移一级并清理行尾空白。

<!-- BEGIN VERBATIM INTEGRATION AUDIT 1 -->

## Round 7 Integration Audit 1

### 总 verdict：REVISE

没有 P0。发现 4 组 P1 exact-contract 问题，修复前不能将 Round 7 标为 `Reviewed / Integrated`，不能 commit/push。其余 owner、BLOCK 边界、性能约束及归档完整性基本正确。

### P0–P3 Findings

#### P1-1 Descriptor 合法长度算术错误

[RFC-0001 §5.2](/Users/liusinan/apps/ideaproject/nereusstream/bookkeeper/docs/rfcs/unified-wal/RFC-0001-profile-capability-install.md:163)、[Spike A](/Users/liusinan/apps/ideaproject/nereusstream/bookkeeper/docs/rfcs/unified-wal/spikes/SPIKE-A-profile-install.md:49) 和 [turbo-bk](/Users/liusinan/apps/ideaproject/nereusstream/bookkeeper/docs/turbo-bk.md:153) 都把 canonical descriptor 写成 `124..1024 bytes`。

按已冻结的十字段结构：

```text
16-byte header
+ 10 * 8-byte field headers
+ 7 * 2-byte U16
+ 4-byte U32
+ 8-byte U64
+ 2-byte capability count
+ N * 6-byte capability entries
= 124 + 6N bytes
```

`N <= 64`，所以 V1 合法编码只能是 `124 + 6N`，最大 `508` bytes。由于 padding、optional field、free bytes 和 trailing bytes 都禁止，`509..1024` 不能成为合法 V1 编码。

最小修复：

- 锁定合法长度为 `124 + 6 * capabilityCount`，即 `124..508`；
- `1024` 保留为解析器在分配前执行的绝对输入/分配 hard cap；
- 明确 `509..1024` 在 V1 中仍非法；
- Spike A golden corpus增加 `508` 合法、`509/1024` 非法、`1025` oversize-before-allocation；
- 同步 RFC-0001、Spike A、turbo-bk；原 Round 7 reviewer 归档保持原样，由本 Audit 记录纠正。

这是 exact codec 的正确性问题，不是数值调优 OPEN。

#### P1-2 HELLO/status wire manifest 不够 exact

[RFC-0001 §11.5](/Users/liusinan/apps/ideaproject/nereusstream/bookkeeper/docs/rfcs/unified-wal/RFC-0001-profile-capability-install.md:609) 已给 status class 数值，但没有同步 reviewer 已给出的 `retryDisposition` 和 `durableResult` 数值：

```text
retryDisposition:
0 NEVER
1 SAME_PROFILE_OPERATION
2 AFTER_CONTROL_RECONCILIATION
3 REPLACE_TARGET
4 ATTEMPT_BOUNDED_OR_CANCELLED

durableResult:
0 NONE
1 APPLIED
2 ALREADY_APPLIED
3 UNKNOWN
```

[RFC-0001 §12](/Users/liusinan/apps/ideaproject/nereusstream/bookkeeper/docs/rfcs/unified-wal/RFC-0001-profile-capability-install.md:619) 的 server HELLO 只用 prose 描述，遗漏了主审查 exact body 中的 `reserved:u16=0`，也没有逐项锁定宽度和顺序。不同 prototype 会产生不同字节。

另外，[Spike A A26](/Users/liusinan/apps/ideaproject/nereusstream/bookkeeper/docs/rfcs/unified-wal/spikes/SPIKE-A-profile-install.md:297) 和 [turbo-bk](/Users/liusinan/apps/ideaproject/nereusstream/bookkeeper/docs/turbo-bk.md:157) 写成“11 类 status”，但 `0..11` 实际是 12 类，即 `OK + 11 non-OK`。

最小修复：

- 将 server HELLO exact struct完整写入 RFC，包括全部宽度、顺序和 `reserved:u16=0`；
- 为 retry/durable 枚举补齐上述固定数值；
- 将数量统一为“12 status classes（1 OK + 11 non-OK）”；
- Spike A A26增加 server HELLO reserved/nonzero、byte-exact enum golden vectors及全部 12 类贯通测试；
- turbo-bk同步正确数量。

这些仍是 executable test manifest；修复不会把 stable production wire 从 BLOCK 提前解锁。

#### P1-3 旧 INSTALL schema 与新安全合同冲突

[RFC-0001 §7](/Users/liusinan/apps/ideaproject/nereusstream/bookkeeper/docs/rfcs/unified-wal/RFC-0001-profile-capability-install.md:374) 仍保留：

```text
descriptorHash
installRequestId
authorizationOrRequestCorrelation
protectedCredentialOrProof
```

这与本轮已经锁定的合同冲突：

- authority identity 应是 36-byte `descriptorIdentity`，不能退回不含 suite/schema 的模糊 `descriptorHash`；
- externally retried identity 已冻结为 `operationId[16]`；
- AuthZ 来自 mTLS principal、exact scope 和 committed authority，不是 generic request correlation；
- 首版 credential 是 protected `kind=1 + length=20 + secret[20]`，明确拒绝 bearer install proof。

同类旧术语还存在于 PREPARING、receipt、route 和 Spike A observation 字段中，例如 [RFC-0001 §6.1](/Users/liusinan/apps/ideaproject/nereusstream/bookkeeper/docs/rfcs/unified-wal/RFC-0001-profile-capability-install.md:276)、[§6.2](/Users/liusinan/apps/ideaproject/nereusstream/bookkeeper/docs/rfcs/unified-wal/RFC-0001-profile-capability-install.md:284)、[§7.1](/Users/liusinan/apps/ideaproject/nereusstream/bookkeeper/docs/rfcs/unified-wal/RFC-0001-profile-capability-install.md:404) 和 [Spike A observation](/Users/liusinan/apps/ideaproject/nereusstream/bookkeeper/docs/rfcs/unified-wal/spikes/SPIKE-A-profile-install.md:109)。

最小修复是只同步逻辑名称和已锁字段：

```text
descriptorIdentity[36]
operationId[16]
publicSemanticPayloadIdentity[32]
credentialKind/credentialLength/protected credential
```

删除 `authorizationOrRequestCorrelation` 和 `protectedCredentialOrProof`。继续明确这只是 logical request contract；control tail最终 physical wire packing仍 BLOCK。

#### P1-4 AuthZ 与 committed-authority read 的顺序自相矛盾

[RFC-0001 §6.6](/Users/liusinan/apps/ideaproject/nereusstream/bookkeeper/docs/rfcs/unified-wal/RFC-0001-profile-capability-install.md:344) 定义：

```text
authorize(principal, scope, CommittedAuthority)
```

但同节 INSTALL 顺序在 [line 366](/Users/liusinan/apps/ideaproject/nereusstream/bookkeeper/docs/rfcs/unified-wal/RFC-0001-profile-capability-install.md:366) 要求 exact tuple AuthZ 先于 direct-read committed authority。实现无法在尚未读取 authority 时调用这个 exact authorizer。

最小且不增加 I/O 的顺序应为：

```text
TLS/mTLS
→ bounded parse
→ non-anonymous + static operation precheck
→ derive fixed authority key
→ direct-read committed authority
→ exact authorize(principal, scope, committedAuthority)
→ credential validation
→ conditional local transition
```

最终 exact AuthZ 仍必须早于 route claim、credential persistence、allocation 和任何 durable effect。只保留现有每个冷 operation 至多一次 authority read；不引入通用 RBAC或额外 metadata round trip。Spike A A24应按这个两阶段检查顺序注入 fault并断言拒绝路径无 effect。

#### P2：X509 subject 身份的部署唯一性需要进入 Gate

当前首版 principal 固定为 leaf certificate 的 `X500Principal.CANONICAL`，可以作为最小方案。但若同一 trust 配置允许不同 issuer/trust domain签发相同 subject，它们会被 authorizer视为同一 principal。

这不是本轮集成阻塞项；principal allowlist/trust配置本来保持 OPEN。正式 Spike A manifest应二选一：

- 声明并验证受信 CA 范围内 subject 唯一性；或
- 在配置层把 trust-domain/issuer identity纳入 principal mapping。

不需要 SPKI pinning、每请求证书检查或新的 PKI。

#### P3：重复小标题

[RFC-0004 §7.2](/Users/liusinan/apps/ideaproject/nereusstream/bookkeeper/docs/rfcs/unified-wal/RFC-0004-range-recovery-delete.md:224) 连续出现“启用前最低约束”和“最低约束”。纯编辑问题，可随 P1 修复机械清理，不影响合同。

### 42 项 LOCK 逐项复核

| # | 结果 | 复核结论 |
|---:|---|---|
| 1 | PASS | Descriptor V1已锁 strict flat TLV，拒绝 ordinary protobuf canonicalization。 |
| 2 | PASS | 16-byte header、8-byte field header、十字段ID、BE编码已进入 RFC-0001。 |
| 3 | PASS | index/sequence/recovery/delete只由 mandatory capability set表达。 |
| 4 | REVISE | 1024 hard cap可保留，但合法最大长度必须从1024纠正为508。 |
| 5 | PASS | duplicate/order/unknown/missing/trailing/wrong-length全部拒绝。 |
| 6 | PASS | E/W/A/F与policy ID/generation验证已锁。 |
| 7 | PASS | SHA-256、suite 1、domain separator与36-byte identity完整。 |
| 8 | PASS | input自身必须canonical，禁止parse-normalize alias。 |
| 9 | PASS | 同instance descriptor/auth immutable，变化使用新instance。 |
| 10 | PASS | 无descriptor签名、Merkle、PKI或per-Add hash。 |
| 11 | PASS | 独立`bookie-profile-v1` endpoint已锁。 |
| 12 | PASS | immediate TLS，明确不复用START_TLS/legacy port。 |
| 13 | PASS | TLS1.3+mTLS在连接建立执行，normal Add不验certificate。 |
| 14 | REVISE | exact AuthZ语义正确，但与authority read的书面顺序冲突。 |
| 15 | PASS | coarse OU/AuthDisabled/SASL-without-principal均不足。 |
| 16 | REVISE | direct-read committed authority已锁；需修正与exact AuthZ的调用顺序。 |
| 17 | PASS | READY明确不是Bookie subtype。 |
| 18 | PASS | normal Add远程I/O/hash/HMAC/KMS/cert/control-fsync均为0。 |
| 19 | REVISE | 新合同正确，但旧`protectedCredentialOrProof` schema仍造成冲突。 |
| 20 | PASS | receipt/status/log/metric/exception的secret non-disclosure完整。 |
| 21 | REVISE | 幂等/冲突语义已锁，但旧requestId/correlation schema必须统一为operation/public-payload identity。 |
| 22 | PASS | 4-byte outer length与32-byte frame header已标为test manifest。 |
| 23 | PASS | magic `0x0FFE4250`正确保留为需raw corpus验证的候选。 |
| 24 | REVISE | subtype完整；server HELLO exact struct缺reserved/宽度。 |
| 25 | PASS | normal/recovery/read/fence/LAC使用distinct mandatory subtype。 |
| 26 | PASS | Profile parse failure禁止legacy fallback。 |
| 27 | PASS | Profile与Classic physical pool分离。 |
| 28 | PASS | HELLO只在Profile connection首次执行，Classic无handshake。 |
| 29 | PASS | pre-HELLO/control/all-frame及capability bounds已进入RFC。 |
| 30 | PASS | range/batch V1 reserved/disabled、count/body为0。 |
| 31 | REVISE | semantic classes存在，但retry/durable数值及12类计数需修复。 |
| 32 | PASS | Profile失败、close、response loss均禁止Classic downgrade/double write。 |
| 33 | PASS | stable wire仍受真实old-decoder raw corpus Gate阻塞。 |
| 34 | PASS | same-scope old-binary fence保持BLOCK。 |
| 35 | PASS | 无法证明same-scope时，new BookieId/new roots/new incarnation/new credential scope已锁。 |
| 36 | PASS | compatibility hook必须早于任何Profile storage component/open/replay/register/write。 |
| 37 | PASS | persistent readiness versioned CAS先于ephemeral registration。 |
| 38 | PASS | device/Arena superblock不能替代Bookie old-visible fence。 |
| 39 | PASS | partial/missing/corrupt/unknown mandatory migration状态均non-writable。 |
| 40 | PASS | 现存authority后禁止old binary直接rollback。 |
| 41 | PASS | format/readiness验证明确不进入normal Add。 |
| 42 | PASS | 无global lease、cross-device transaction或per-entry format/metadata state。 |

结论：42 项中 35 项直接 PASS，7 项由上述 4 组 P1 文案冲突覆盖；没有架构方向被推翻。

### 逐文件结论

- RFC-0001：REVISE。四组 P1都集中在这里。
- RFC-0003：PASS。只消费format边界，不拥有registration/auth；same-scope仍BLOCK。
- RFC-0004：PASS，只有一个P3重复标题。Profile recovery不legacy fallback，general E/W/A API仍BLOCK。
- RFC-0005：PASS。logical local authority与physical owner正确分离；readiness、fallback与startup gate完整。
- Spike A：REVISE。需同步descriptor长度、identity术语、HELLO/status exact vectors与AuthZ/read顺序。
- Spike B：PASS。stock binary、pre-storage-open、auto-stamp、partial migration、fallback与startup性能证据完整。
- Spike C：PASS。只抽象boolean/generation，没有把TLS、SHA、TLV、Cookie/filesystem细节塞入Model。
- turbo-bk：REVISE。需修正descriptor合法长度和status数量；实施边界本身正确。
- grill README：PASS，仍为 `Review Complete / Integration In Progress`。
- Round 7 archive：PASS。

### OPEN / BLOCK 边界复核

仍正确保持 OPEN：

- TLS cipher/provider tuning；
- principal allowlist、trust rotation及future SASL adapter；
- Profile pool/backoff数值；
- sidecar/readiness backend path与adapter；
- detailCode和诊断文本；
- lower runtime limits、性能阈值、硬件；
- future hint envelope；
- future batch/range版本。

仍正确保持 BLOCK：

- failure-domain policy registry；
- mandatory capability registry及production组合；
- local route/auth/activation的physical owner、record framing、at-rest protection；
- control tail最终字段/packing；
- stable wire直至raw decoder corpus PASS；
- same BookieId/storage scope fence；
- Bookie/device/Arena exact superblock bytes；
- readiness CAS exact backend schema；
- migration/reverse/wipe CLI；
- general E/W/A exact outcome API；
- live shadow promotion；
- Segment ACK authority。

没有发现 OPEN 被伪装为生产合同，也没有把 `BKPF1` candidate误写为accepted format。

### 性能、复杂度与过度设计

性能边界整体 PASS：

- Classic endpoint、decoder和pool不承担Profile TLS/HELLO/framing；
- normal Add无MetadataStore、descriptor/auth hash、KMS、signature/certificate或control fsync；
- 新数据面成本仅为TLS AEAD、固定header/context、bounded local lookup和20-byte constant-time compare；
- startup/readiness、control authority read均在冷路径；
- 没有全局lease、universal hot head、per-entry metadata node或跨device transaction。

四组必修项不会增加热路径成本：

- 修正508-byte合法上界反而减少parser歧义和攻击面；
- HELLO/status数值只是codec确定性；
- INSTALL术语统一不增加持久状态；
- AuthZ顺序仍只有一次cold authority read。

复杂度与过度设计整体 PASS：独立endpoint虽增加listener/pool，但由真实legacy decoder downgrade风险直接要求；设计没有引入通用RBAC、PKI扩张、Merkle、bearer proof或新分布式事务系统。

### 归档与机械验证

- HEAD与`origin/turbo-bk`均为 `9885dbda26f55dbe3c20c530a36b22109d76b7e2`，ahead/behind `0/0`。
- 变更范围为9份tracked Markdown加Round 7归档。
- PDF仍是唯一无关untracked文件，未读取、未触碰。
- `git diff --check`：PASS。
- 10份相关Markdown单一H1：PASS。
- fenced code blocks：PASS。
- relative Markdown links：PASS。
- README状态：PASS。
- 我从当前session JSONL提取原reviewer final，对归档反向恢复heading层级并清理行尾后逐行比较；正文唯一差异是允许的文件末尾换行，未发现删节或语义改写。

### 发布许可

当前明确禁止：

- 将Round 7标为`Reviewed / Integrated`；
- commit；
- push；
- 进入下一轮；
- 开始stable wire/on-disk、live shadow或Segment ACK authority工作。

允许的下一步仅是：

1. 完整归档本 Audit；
2. 做上述4组最小文档修复；
3. README继续保持`Integration In Progress`；
4. 将修复后的完整diff交回同一 reviewer做 Integration Audit 2。

Audit 2通过后，才可归档PASS回复、更新README，并只暂存本轮10份Markdown提交/推送，继续排除PDF。

后续 implementation frontier不变：先执行raw old-decoder corpus；再联合闭合local physical authority owner/record framing与same-scope `BKPF1`或new-scope fallback证据。不要重开PKI、Merkle、per-Add proof等已拒绝方向。

<!-- END VERBATIM INTEGRATION AUDIT 1 -->


## Integration Audit 2

> Reviewer：同一固定 `gpt-5.6-sol / max` agent
> 结论：**REVISE**
> 归档规则：以下回复未经删节；仅将 Markdown heading 统一下移一级并清理行尾空白。

<!-- BEGIN VERBATIM INTEGRATION AUDIT 2 -->

## Round 7 Integration Audit 2

### 总 verdict：REVISE

没有 P0、没有 P1。Audit 1 的四组实质合同问题均已闭合，但总纲还残留一处 P2 exact-manifest 歧义。修复前不应将 Round 7 标记为 `Reviewed / Integrated`，也不应 commit/push。

这不是新设计问题，不需要 Round 8；只需一处文字修正后由同一 reviewer 做一次极短的最终复核。

### P0–P3 findings

#### P0

无。

#### P1

无。Audit 1 的安全、wire、AuthZ 和幂等性问题均已修复。

#### P2：总纲仍可能把 1024 bytes 误读为 V1 合法上限

[docs/turbo-bk.md:330](/Users/liusinan/apps/ideaproject/nereusstream/bookkeeper/docs/turbo-bk.md:330) 当前仍写：

```text
ProfileDescriptorV1 BKPD strict TLV + ten fields + 1024-byte/64-capability bounds ...
```

同文件第 153 行、RFC-0001 和 Spike A 已正确锁定：

- 合法长度只能是 `124 + 6 * capabilityCount`；
- 合法范围为 124..508 bytes；
- 509..1024 bytes 非法；
- 1024 只是 allocation 前 absolute input cap；
- 1025 必须在 allocation 前判定 oversize。

但第 330 行位于“稳定 wire/format 前必须冻结并归档”的 exact manifest 列表中，`1024-byte bounds` 仍可能被实现者理解为合法 descriptor 最大长度，与本轮刚修正的 exact codec 合同产生歧义。

最小修复仅需将该行对应部分改成等价的明确表述，例如：

```text
ProfileDescriptorV1 BKPD strict TLV + ten fields + legal-124-plus-6N/124..508-byte + absolute-input-cap-1024-before-allocation + 0..64-capability bounds + SHA-256 suite 1/36-byte identity
```

不需要修改 RFC、Spike、字段布局或测试设计。

#### P3

无新的 P3。Audit 1 指出的 RFC-0004 重复标题已清理。

### Audit 1 六项逐项复核

1. Descriptor 长度：主体已闭合，但总纲第 330 行仍有上述一处摘要歧义。RFC-0001、Spike A 和总纲第 153 行均正确；508 legal、509/1024 invalid、1025 pre-allocation oversize 的 vectors 完整。
2. HELLO/status：PASS。server HELLO 的字段宽度、顺序、`reserved:u16=0`、非零 storage incarnation、BookieId/capability bounds 均已冻结；`retryDisposition=0..4`、`durableResult=0..3`、12 个 status class 均有固定数值，Spike A A26 覆盖 exact golden vectors。
3. INSTALL identity/schema：PASS。PREPARING、request、receipt、route 与 Spike observation 已统一使用 `descriptorIdentity[36]`、`operationId[16]`、`publicSemanticPayloadIdentity[32]`；request 的 protected credential 使用 kind/length/bytes，receipt 不携带秘密。旧 correlation/proof/bearer schema 已删除。physical control-tail packing仍明确 BLOCK。
4. INSTALL AuthZ 顺序：PASS。顺序已统一为 TLS/mTLS → bounded parse → non-anonymous/static precheck → fixed authority key → 一次 direct committed-authority read → exact post-read authorize → credential validation → conditional local transition。两处 AuthZ 拒绝均要求 route、credential、allocation、durable effect 为零。
5. X509 subject collision：PASS。manifest必须二选一验证 trusted-set subject uniqueness，或把 trust-domain/issuer 纳入 principal mapping；Spike A 有同 subject/不同 issuer 的可否证场景。未引入 SPKI pinning、每请求证书重验或新 PKI。
6. RFC-0004 duplicate heading：PASS。

### 42 项 LOCK 复核

| # | 结果 | 结论 |
|---:|---|---|
| 1 | PASS | V1 为 strict flat TLV，不使用 ordinary protobuf canonicalization。 |
| 2 | PASS | 16-byte header、8-byte field header、十字段 ID 与 big-endian 编码已锁。 |
| 3 | PASS | index/sequence/recovery/delete 仅由 mandatory capability set 表达。 |
| 4 | REVISE | 权威合同已正确锁定 124..508/1024 absolute cap，但总纲第 330 行仍需消除歧义。 |
| 5 | PASS | duplicate、ordering、unknown、missing、trailing、wrong length 全部拒绝。 |
| 6 | PASS | E/W/A/F 与 policy identity/generation 校验完整。 |
| 7 | PASS | SHA-256、suite 1、domain separator 和 36-byte identity 完整。 |
| 8 | PASS | input 本身必须 canonical，不接受 parse-normalize alias。 |
| 9 | PASS | 同 instance descriptor/auth immutable，变更必须创建新 instance。 |
| 10 | PASS | 不要求 descriptor 签名、Merkle、PKI 或 per-Add hash。 |
| 11 | PASS | 独立 `bookie-profile-v1` endpoint 已锁。 |
| 12 | PASS | immediate TLS，禁止 START_TLS 和 legacy-port 协议复用。 |
| 13 | PASS | TLS1.3/mTLS 只在连接建立执行，normal Add 不验证 certificate。 |
| 14 | PASS | exact operation/instance/target-scope AuthZ 顺序已修正。 |
| 15 | PASS | coarse OU、AuthDisabled 和 SASL-without-principal 均不足以授权。 |
| 16 | PASS | 每个 cold control operation 只有一次 committed-authority direct read。 |
| 17 | PASS | READY 不是 Bookie subtype。 |
| 18 | PASS | normal Add 的 remote I/O、descriptor/auth hash、KMS、certificate 和 control fsync 均为零。 |
| 19 | PASS | protected credential schema 已统一，旧 proof/bearer 表达已删除。 |
| 20 | PASS | receipt/status/log/metric/exception 的 secret non-disclosure 完整。 |
| 21 | PASS | operation identity、public payload identity、secret conflict 和 bounded idempotency 已锁。 |
| 22 | PASS | 4-byte outer length 与 32-byte frame header 仍只属于 executable test manifest。 |
| 23 | PASS | magic `0x0FFE4250` 保持 raw-corpus 待验证候选。 |
| 24 | PASS | subtype 与完整 server HELLO exact struct 已同步。 |
| 25 | PASS | normal/recovery/read/fence/LAC 使用 distinct mandatory subtype。 |
| 26 | PASS | Profile parse failure 绝不触发 legacy fallback。 |
| 27 | PASS | Profile 与 Classic physical pool 分离。 |
| 28 | PASS | HELLO 只在 Profile connection 首次执行，Classic 无 handshake。 |
| 29 | PASS | pre-HELLO/control/all-frame/capability bounds 完整。 |
| 30 | PASS | range/batch V1 reserved/disabled，accepted body/count 为零。 |
| 31 | PASS | 12 status、5 retry、4 durable-result 的数值映射已锁。 |
| 32 | PASS | Profile failure、close 和 response loss 禁止 Classic downgrade/double write。 |
| 33 | PASS | stable wire 仍受真实 old-decoder raw corpus Gate 阻塞。 |
| 34 | PASS | same-scope old-binary fence 仍 BLOCK。 |
| 35 | PASS | 无法证明 same-scope 时，new BookieId/new roots/new incarnation/new credential scope 已锁。 |
| 36 | PASS | compatibility hook 必须早于任何 Profile storage open/replay/register/write。 |
| 37 | PASS | persistent readiness versioned CAS 先于 ephemeral registration。 |
| 38 | PASS | device/Arena superblock 不能替代 Bookie old-visible fence。 |
| 39 | PASS | partial/missing/corrupt/unknown mandatory migration 均 non-writable。 |
| 40 | PASS | 已产生 authority 后禁止 old-binary 原地 rollback。 |
| 41 | PASS | format/readiness 检查不进入 normal Add。 |
| 42 | PASS | 未引入 global lease、cross-device transaction 或 per-entry format/metadata state。 |

结论：41 项完全 PASS；第 4 项权威正文已正确，只剩总纲一处摘要措辞需要同步。

### 逐文件结论

- RFC-0001：PASS。descriptor、control/auth、wire 与 cold-authority owner 正确。
- RFC-0003：PASS。只消费 compatibility/format 边界，不拥有 registration/auth；cross-Arena 和 same-scope 均未被误解锁。
- RFC-0004：PASS。Profile recovery 不允许 legacy fallback；general E/W/A exact API 仍 BLOCK；重复标题已清理。
- RFC-0005：PASS。logical local authority 与 physical owner 正确分离；startup/readiness/new-scope fallback 完整。
- Spike A：PASS。descriptor、AuthZ、secret leak、HELLO/status、raw old-decoder vectors 均可执行。
- Spike B：PASS。stock-old pre-storage-open、Cookie auto-stamp、partial migration、fallback、startup bytes/I/O/device scaling完整。
- Spike C：PASS。只建模 boolean/generation/authority ordering，没有把 TLS、SHA、TLV、X509 或 filesystem bytes 塞进状态空间。
- `docs/turbo-bk.md`：REVISE，仅第 330 行的 1024-byte 摘要歧义。
- grill README：PASS，当前保持 `Review Complete / Integration In Progress`。
- Round 7 archive：PASS。

### OPEN / BLOCK 边界

仍正确保持 OPEN：

- TLS cipher/provider tuning；
- principal allowlist、trust rotation、future SASL adapter；
- Profile pool/backoff 数值；
- sidecar/readiness backend path与adapter；
- detailCode和诊断文本；
- lower runtime limits、性能阈值和硬件；
- future hint envelope；
- future batch/range版本。

仍正确保持 BLOCK：

- failure-domain policy registry；
- mandatory capability registry及production合法组合；
- local route/auth/activation physical owner、record framing、at-rest protection；
- control tail最终字段与packing；
- stable production wire，直至raw old-decoder corpus实际PASS；
- same BookieId/storage scope format fence；
- Bookie/device/Arena exact superblock bytes；
- readiness CAS exact backend schema；
- migration/reverse/wipe CLI；
- general E/W/A exact outcome API；
- live shadow promotion；
- Segment ACK authority。

没有发现 OPEN 被伪装成最终生产合同，也没有把 `BKPF1` candidate 写成 accepted format。

### 性能、复杂度与过度设计

性能边界 PASS：

- Classic endpoint、decoder 和 pool 不承担 Profile TLS/HELLO；
- normal Add 不读取 MetadataStore、sidecar 或 readiness；
- normal Add 不做 descriptor/auth hash、HMAC/KMS、signature/certificate 或 control fsync；
- Profile 数据面只增加 TLS AEAD、fixed header/context、bounded local lookup 和固定长度比较；
- authority read、install、readiness 和 startup 检查均为冷路径；
- 没有 universal hot head、per-entry MetadataStore node、全局 lease 或跨 device transaction。

本次唯一修复只是消除文档歧义，运行时成本为零。独立 endpoint/listener/pool 的复杂度由真实 legacy decoder downgrade 风险直接要求；没有理由重新引入通用 RBAC、PKI 扩张、Merkle、bearer proof、per-Add nonce 或通用事务系统。

### 归档与机械验证

- HEAD 与 `origin/turbo-bk` 都是 `9885dbda26f55dbe3c20c530a36b22109d76b7e2`，ahead/behind 为 `0/0`。
- 范围为 9 份 tracked Markdown 加新 Round 7 archive。
- PDF 是唯一无关 untracked 文件；未读取、未触碰。
- `git diff --check`：PASS。
- 10 份 Markdown 单一 H1：PASS。
- fenced code blocks：PASS。
- trailing whitespace 与单一 EOF newline：PASS。
- Markdown links按repo-root及本地`:line`链接解析：PASS。
- 从当前 session JSONL 提取主 review 与 Audit 1 后，反向恢复 heading 层级并规范化允许的行尾，均与归档逐字一致；未发现删节或语义改写。
- README 在本次未通过前保持 Integration In Progress，状态正确。

### 发布许可

当前不许可：

- 把 Round 7 标为 `Reviewed / Integrated`；
- commit；
- push；
- 开始 stable production wire/on-disk；
- 开始 same-scope format migration；
- 开始 live shadow 或 Segment ACK authority。

允许的下一步仅是：

1. 完整归档本 Audit 2；
2. 修正 `docs/turbo-bk.md` 第 330 行；
3. README继续保持 Integration In Progress；
4. 交回同一 reviewer做一个仅核对该行和范围状态的最终 Audit。

最终 Audit PASS 后，可以将 Round 7 标为 `Reviewed / Integrated`，且只提交/推送这 10 份 Markdown 到 `nereusstream/bookkeeper` 的 `origin/turbo-bk`；PDF必须继续排除。

### 停止条件与实施解锁

由于本次 verdict 是 REVISE，当前尚未形式化触发“可以开始实施”的停止条件。但剩余问题只是单行文档歧义，不是设计 frontier。

该行修复并通过最终复核后：

- 可以开始 exact interfaces；
- 可以实现 `ProfileDescriptorV1` reference codec、strict validator、independent verifier和golden corpus；
- 可以实现独立 Profile endpoint/listener、exact authorizer接口及受保护binding的logical接口；
- 可以实现 experimental frame/HELLO/status codec与raw old-decoder corpus harness；
- 可以实现 compatibility/startup/readiness harness；
- 可以做 `BKPF1` candidate和new-scope fallback的isolated prototype；
- 可以做不接管authority、不污染Classic rollback cohort的discardable prototype。

届时继续纯设计 Round 8 会是缺少运行证据的过度设计。下一步应转向真实 raw decoder、stock binary、crash/restart、startup I/O 和 physical-owner prototype 证据，再由证据决定是否需要补充合同。

仍不得开始：

- stable production wire；
- stable on-disk/local authority format；
- same-scope format promotion；
- live shadow；
- Segment ACK authority。

<!-- END VERBATIM INTEGRATION AUDIT 2 -->


## Integration Audit 3

> Reviewer：同一固定 `gpt-5.6-sol / max` agent
> 结论：**PASS / APPROVE**
> 归档规则：以下回复未经删节；仅将 Markdown heading 统一下移一级并清理行尾空白。

<!-- BEGIN VERBATIM INTEGRATION AUDIT 3 -->

## Round 7 Integration Audit 3

### 总 verdict：PASS / APPROVE

没有 P0、P1、P2 或 P3 阻塞项。Audit 2 的唯一修复已准确闭合，没有引入新的正确性、兼容性、性能、复杂度或 owner 边界问题。

### 1. Descriptor exact-manifest 修复

PASS。

[docs/turbo-bk.md:330](/Users/liusinan/apps/ideaproject/nereusstream/bookkeeper/docs/turbo-bk.md:330) 现已明确写为：

```text
legal-124-plus-6N/124..508-byte
+ absolute-input-cap-1024-before-allocation
+ 0..64-capability bounds
```

现在与 RFC-0001、Spike A 和总纲正文完全一致：

- V1 合法长度仅为 `124 + 6 * capabilityCount`；
- 合法范围为 124..508 bytes；
- 509..1024 bytes 非法；
- 1024 仅为 allocation 前 absolute input cap；
- 1025 必须在 allocation 前判定 oversize。

不再存在把 1024 bytes 误解为合法 descriptor 上限的路径。

### 2. Reviewer 原文归档

PASS。

从当前 session JSONL 提取原 reviewer 输出，并对归档执行允许的 heading 层级反向恢复和行尾规范化后：

- Round 7 主 review：逐字一致；
- Integration Audit 1：逐字一致；
- Integration Audit 2：逐字一致。

三段 BEGIN/END marker 完整、顺序正确，没有删节、重写或反馈丢失。归档末尾是合法 Markdown 空白，不包含 trailing whitespace，也不影响逐字规范化、渲染或机械检查。

### 3. 范围、README 与机械检查

PASS。

- HEAD：`9885dbda26f55dbe3c20c530a36b22109d76b7e2`。
- `origin/turbo-bk`：同一 SHA，ahead/behind `0/0`。
- origin：`https://github.com/nereusstream/bookkeeper.git`。
- 工作区范围仍精确为 9 份 tracked Markdown 加新 Round 7 archive。
- `BtrLog Low-Latency Logging.pdf` 仍是唯一无关 untracked 文件；未读取、未触碰。
- README 中 Round 7 仍为 `Review Complete / Integration In Progress`，符合终审完成前状态。
- `git diff --check`：PASS。
- 10 份相关 Markdown 单一 H1：PASS。
- fenced code blocks：PASS。
- trailing whitespace：PASS。
- Markdown links，包括归档中的本地 `:line` 链接：PASS。
- 文件均有合法 EOF newline。

### 4. 既有合同回归复核

PASS。

Audit 1 的四组 P1 均保持闭合：

1. Descriptor 长度算术、bounds 与 golden vectors正确；
2. server HELLO、12 status、5 retry、4 durable-result exact manifest正确；
3. PREPARING/request/receipt/route/observation identity与secret-free边界统一；
4. INSTALL 的 TLS/AuthZ/direct-read/credential/local-transition顺序正确。

42 项 LOCK 现在全部 PASS，包括此前唯一残留的第 4 项 descriptor bounds。没有旧 correlation/proof/bearer schema重新出现。

Owner 边界保持正确：

- RFC-0001拥有 descriptor、Profile control/auth和executable wire manifest；
- RFC-0003只消费format边界，不拥有registration/auth；
- RFC-0004拥有Profile recovery语义，禁止legacy fallback；
- RFC-0005拥有logical local authority、readiness和downgrade gate；
- physical local owner、record framing与at-rest protection仍未被提前冻结。

OPEN/BLOCK 没有被伪装成最终合同：

- failure-domain/capability registry与production组合仍 BLOCK；
- physical local authority owner/control-tail packing仍 BLOCK；
- stable production wire仍等待raw old-decoder corpus；
- same-scope format仍 BLOCK；
- general E/W/A exact outcome API仍 BLOCK；
- live shadow与Segment ACK authority仍 BLOCK；
- `BKPF1`仍只是Spike candidate。

### 性能、复杂度与过度设计

PASS。

本次单行修复没有运行时影响。整体合同继续保证：

- Classic endpoint、decoder和pool不承担Profile TLS/HELLO；
- normal Add无MetadataStore/sidecar/readiness远程读取；
- normal Add无descriptor/auth hash、HMAC、KMS、signature/certificate或control fsync；
- normal Add只执行fixed header/context解析、bounded local lookup和固定长度比较；
- startup、readiness与authority direct-read均留在冷路径；
- 没有global lease、universal hot head、per-entry metadata state、cross-device transaction、PKI扩张或Merkle设计。

### 发布许可

明确授权：

1. 将本回复完整归档到 Round 7 archive；
2. 将 grill README 中 Round 7 状态改为 `Reviewed / Integrated`；
3. 只暂存本轮 10 份 Markdown；
4. commit；
5. push 到组织仓库 `nereusstream/bookkeeper` 的 `origin/turbo-bk`。

必须继续排除 `BtrLog Low-Latency Logging.pdf`，不得暂存或提交。

本次 PASS 只完成 Round 7 文档与implementation-interface合同集成；总体 RFC 状态仍可保持 `Proposed / Not Implementation Ready`，不能由此宣称生产系统已就绪。

### Grill 停止条件

停止条件已经满足。

现在可以开始边界受控的实施：

- exact logical interfaces；
- `ProfileDescriptorV1` reference codec、strict validator、independent verifier与golden corpus；
- Profile endpoint/listener及exact authorizer接口；
- experimental frame/HELLO/status codec；
- raw old-decoder compatibility harness/corpus；
- startup/readiness/new-scope fallback harness；
- `BKPF1` candidate与local-authority owner的isolated、discardable prototype。

不应继续进行纯设计 Round 8；在没有真实 decoder、stock binary、crash/restart、startup I/O 和prototype数据前继续扩展合同会成为无证据的过度设计。下一阶段应转向实现和否证证据，再由证据决定是否需要补充设计。

仍然 BLOCK：

- stable production wire；
- stable production on-disk/local-authority format；
- same-scope format promotion；
- live shadow promotion；
- Segment ACK authority。

<!-- END VERBATIM INTEGRATION AUDIT 3 -->
