# Round 6：Profile Security、Wire Compatibility 与 Downgrade Gate

> Reviewer：复用 Round 1–5 的固定 `gpt-5.6-sol / max` agent<br>
> 源码与文档基线：`1c2fc1b30aa69b55cbfac4116609c4f4e78fb2b5`<br>
> 评审模式：安全、wire/format compatibility、热路径性能、复杂度和过度设计联合审查<br>
> 记录规则：以下 reviewer 回复未经删节；仅将 Markdown heading 统一下移一级并清理行尾空白，以保持文档单一 H1。规范是否采纳以对应 RFC 的集成结果为准。

## 本轮 Questions

### Q1：Canonical ProfileDescriptor serialization/hash/version

冻结 canonical semantic/runtime 边界、schema/mandatory feature 兼容、derived hash identity、unknown/default/duplicate 规则和首版 immutable descriptor/auth binding；避免签名、Merkle、PKI 与 per-Add hash 过度设计。

### Q2：Protected auth binding 与 install/activation proof

冻结 data credential/control authority 分离、non-anonymous cold control、Bookie direct-read committed authority、本地 protected binding、secret-free receipt 与 bounded Add check；禁止公开 verifier、per-Add KMS/signature/nonce/fsync。

### Q3：Profiled opcode/capability/error compatibility

冻结 distinct Profile logical operations、old decoder 不得解释成 Classic Add 的 mandatory discriminator、connection-amortized negotiation、mixed-version fail-closed matrix和semantic error classes；exact opcode/bytes/codes保持 OPEN。

### Q4：Engine/superblock/registration downgrade 与 rollback gate

冻结 old-binary-visible pre-replay compatibility fence、device superblock、registration readiness、partial migration/rollback fail-closed 和真实 stock binary Gate；exact marker/bytes/tooling保持 OPEN，Segment ACK authority继续 BLOCK。

## Reviewer 完整回复

<!-- BEGIN VERBATIM REVIEW -->

## Unified WAL P0 Grill Round 6 独立主审查结论

### 总结论

本轮四题都能收束到最小安全合同，但都需要对当前候选方案做明确收窄：

| 题目 | Verdict |
| --- | --- |
| Q1 Canonical `ProfileDescriptor` | **REVISE 后可 LOCK** |
| Q2 Protected auth / install / activation proof | **REVISE 后可 LOCK** |
| Q3 Profiled wire / capability / error compatibility | **REVISE 后可 LOCK** |
| Q4 Engine / superblock / registration downgrade gate | **REVISE 后可 LOCK；当前 Segment ACK authority 仍然 BLOCK** |

这里没有要求新增签名体系、PKI、Merkle tree、per-Add MetadataStore I/O、per-Add proof 验证、全局 capability lease 或跨存储事务。推荐方案的新增成本集中在 create/install/activation、连接建立、启动恢复和 registration 等冷路径。

但是，Round 6 结束后仍不能宣称整体 `Implementation Ready`。至少还要把以下四个 implementation manifest 选型并以真实旧二进制/字节向量验证：

1. canonical descriptor codec 与 hash suite；
2. control-plane auth 与本地 protected binding 的具体机制；
3. 无歧义的 Profile wire discriminator/opcode/error mapping；
4. stock old binary 在 Journal replay 前必然失败的 format fence。

---

### 1. 基线与源码事实

我核对的工作区状态为：

```text
branch: turbo-bk
HEAD:   1c2fc1b30aa69b55cbfac4116609c4f4e78fb2b5
origin/turbo-bk: same
ahead/behind: 0/0
```

唯一工作区项是未跟踪的 `BtrLog Low-Latency Logging.pdf`；本轮未读取、未触碰。没有修改、暂存、commit 或 push。

#### 1.1 当前 wire 与错误面

`bookkeeper-proto/src/main/proto/BookkeeperProtocol.proto` 当前是 proto2/v3：

- `BKPacketHeader` 要求 `version / operation / txnId`；
- `OperationType` 只有现行 read/add/auth/LAC/info/TLS/force/list/batch read；
- `RANGE_READ_ENTRY/RANGE_ADD_ENTRY` 虽预留但注释为未支持；
- `AddRequest` 只有 `ledgerId / entryId / masterKey / body / writeFlags`，recovery 只是 optional flag；
- 当前错误码不足以表达 Profile 未安装、未激活、instance/hash mismatch、recovery grant mismatch、unknown mandatory format 等语义。

`WriteEntryProcessorV3` 的异步回调还会把许多非 IO 的 Bookie 错误统一压成 `EUA`。因此新增异常类而不贯通 processor、client callback/future、admin 和 metric，不能形成可靠 compatibility contract。

#### 1.2 v3 → legacy decoder fallback 是真实 downgrade 风险

`BookieProtoEncoding.RequestDecoder` 的当前行为是：

1. 先按 v3 protobuf 解码；
2. 任何 `RuntimeException` 都把整条连接永久切到 pre-v3；
3. reset reader index 后，用相同 bytes 重新解释为 legacy packet。

pre-v3 ADD decoder并不严格拒绝未知 protocol version；只要重新解释出的 opcode 是 `ADDENTRY=1`，就会读取 20-byte master key 并构造 Add。

因此，“给现有 `ADD_ENTRY` 加 optional Profile 字段”以及“只在 v3 增加一个新 enum”都没有得到 fail-closed 保证。必须用原始 wire byte corpus 证明：

```text
任何合法、损坏、截断或unknown-version的Profile request
都不能被任一受支持旧Bookie解码为legacy ADDENTRY或RECOVERY_ADD。
```

这是 Q3 的硬 Gate，不是理论上的兼容性备注。

#### 1.3 当前 master key 与认证不能直接充当 control authority

当前 `DigestManager.generateMasterKey()` 生成：

```text
SHA-1("ledger" || password)
```

即一个 20-byte password verifier，并随每个 Add 发送。它不是 READY/activation proof，也不应被放进公开 descriptor、sidecar、receipt 或诊断摘要。

还有两个必须正视的源码事实：

- `LedgerDescriptorImpl.checkAccess()` 在 mismatch 时会把请求和缓存中的完整 master key byte array 都写入日志；
- Bookie authentication plugin 默认可以是 `AuthDisabledPlugin`，此时连接被标记为匿名但认证成功。

因此：

- 当前 master key 可以继续作为首版 data-plane ledger credential；
- 它不能授权 INSTALL、READY publication、ACTIVATE、repair grant 或 delete；
- `AuthDisabledPlugin` 不能满足 Profile control-plane authorization；
- Profile 路径必须消除 master key/proof/capability 的日志、metric、receipt 与 exception 泄漏。

本轮不应扩大成 BookKeeper password/KDF 全面重设计；否则会把 P0 变成另一套认证系统。但 Profile 也不能声称继承的 SHA-1 verifier 模型获得了更强安全性。

#### 1.4 当前启动/rollback 行为不能保护 Segment mandatory authority

`BookieImpl` 构造时会调用 `checkEnvironment()`，随后在 `start()` 中：

1. `readJournal()`；
2. flush；
3. 启动 storage/thread；
4. `registerBookie()`。

`BookieServer` 只有在 Bookie 成功启动后才启动接受请求的 Netty server。这给新 preboot gate 提供了合理插入位置，但必须确认它早于任何 Segment local authority/Arena 初始化，而不只是早于注册。

现有 Journal replay 对未知负 entry id 的行为是明确跳过，源码注释把它当成 rollback compatibility。因此新 mandatory authority 不能只编码成未知 Journal meta-entry。

现有 Cookie 也不能直接承担未来版本拒绝：

- `CURRENT_COOKIE_LAYOUT_VERSION=5`；
- `verifyInternal()` 对 `layoutVersion >= 3` 不要求版本相等；
- 仅把 version 从 5 增到更大或增加字段，不能证明旧 binary 会停止；
- data-integrity cookie validation 在配置允许时会运行 preboot check 后自动重新 stamp cookie。

所以必须证明新的 format fence：

- 是旧 binary 在 Journal replay/handle create 前已经读取的 mandatory surface；
- 旧 binary 对它确定失败，而不是忽略、降级或 auto-heal；
- current/new binary 对部分写、unknown mandatory、corruption 也 fail closed。

#### 1.5 当前 registration 只是提示，不是 readiness CAS

`RegistrationManager.registerBookie()` 只有：

```text
bookieId
readOnly
BookieServiceInfo
```

ZooKeeper 实现创建普通 ephemeral znode。`BookieServiceInfo.properties` 可以携带 capability hint，但当前没有：

- expected registration generation；
- storage incarnation CAS；
- verified format readiness；
- assignment required-through；
- stale registration fencing。

因此 capability advertisement 可用于 placement hint，但不能单独证明 format/assignment/activation 已准备，也不能阻止 old binary 用同一 BookieId 重新注册。

---

## Q1 — Canonical ProfileDescriptor serialization/hash/version

### Verdict：REVISE 后可 LOCK

当前 descriptor/runtime/operation 三分是正确的，但还需修正四点：

1. `descriptorHash` 必须是 canonical semantic bytes 的派生输出，不能成为自身 hash preimage 中的普通字段；
2. `profileVersion` 必须拆清“canonical schema version”与“mandatory semantic feature set”，不能让旧 writer 按旧模型重写新 descriptor；
3. safety-neutral optional hint 不应混进 semantic descriptor/hash；
4. 首版 descriptor 与 auth binding 都必须 immutable；不应提前引入泛化 mutation/key-rotation state machine。

### 1.1 最小 canonical semantic fields

首版必须 hash 的是会改变跨实现安全解释的 immutable semantic contract，至少包括：

```text
descriptor schema/canonical version
required Engine Profile
required semantic capabilities + their semantic versions
payload interpretation/format
durability mode
quorum contract or binding to immutable E/W/A semantics
permanent-loss budget F
failure-domain policy identity/generation
required index/sequence/recovery/delete semantics
other fields whose difference changes:
  - legal payload interpretation
  - ACK eligibility
  - fence/recovery behavior
  - delete/reuse safety
```

不应进入 descriptor/hash 的包括：

```text
ledgerId / ledgerInstanceId
MetadataStore node version
current ensemble or membership CAS version
install/activation/repair/delete operation identity
request/correlation id
master key, password, verifier, authBinding secret
receipt/proof/capability bearer material
rate/admission limits
max inflight / queue / cache budgets
batch/group-commit thresholds
placement preferences
runtime circuit breakers
benchmark thresholds
```

`ledgerId + ledgerInstanceId` 在 descriptor 外层绑定它；这样一个 canonical descriptor 可以被多个 ledger 使用，而不会制造同语义异 hash。

只有真正改变可接受 payload 或恢复正确性的 hard safety limit 才能进入 descriptor。例如，一个跨实现必须一致的 payload encoding bound 可以进入；`maxInflightBytes`、queue depth、compaction threshold 不应进入。

### 1.2 Canonicalization 的不可删规则

在不提前锁 exact bytes 的前提下，可以立即锁定以下语义：

1. 每个 schema version 是一个闭合、确定的 canonical schema。
2. parser 必须在有界大小检查后才分配/解析；exact byte/count limit OPEN。
3. singular 字段重复、map key 重复、set 元素重复一律拒绝，不使用 last-one-wins。
4. set-valued capability 按 canonical numeric identity 排序；有语义顺序的 list 才保留顺序。
5. absent 与 explicit default 只有在该 schema version 明确定义二者语义相同时，才归一成同一 canonical representation。
6. 数字、布尔、byte string、字符串不得依赖 locale、运行时 map order 或实现默认值。
7. 如使用字符串标识，必须规定 byte-level normalization；更小方案是安全关键 identity 使用稳定 numeric ID/opaque bytes。
8. semantic descriptor 内出现 unknown field、unknown enum 或 unknown mandatory capability时拒绝。
9. safety-neutral optional hint 放在 canonical semantic descriptor 外；它不得影响 hash、durability、acceptance 或 recovery。
10. 旧 reader/writer 遇到新 schema version不得 parse-known-fields 后重写，也不得清除 unknown data；它只能 fail closed。
11. canonical bytes 与 hash 必须一起持久化/传输。消费方重算 hash，不信任 caller 直接给出的 hash。
12. descriptor 大小、capability 数和嵌套深度必须有 hard bound；exact 数值 OPEN。

同一 schema version 内必须满足：

```text
semantic equality => identical canonical bytes/hash
different safety semantics => different canonical bytes
```

不同 schema version 不应自动宣称“语义相同”。除非未来有专门、双向验证的 translation contract，否则版本变化形成不同 descriptor identity。

### 1.3 Hash 最小合同

推荐锁定：

```text
descriptorIdentity =
    hashSuiteId
    + canonicalSchemaVersion
    + Hash(domainSeparator || hashSuiteId || schemaVersion || canonicalBytes)
```

其中：

- hash 算法必须是固定长度、collision-resistant 的现代 hash；
- exact algorithm/byte length 由 implementation manifest 冻结，当前保持 OPEN；
- hash suite 在同一 ledger instance 内不可变化；
- `descriptorHash` 不包含自身；
- 解析/安装时重算并比对；
- normal Add 只比较已经验证并缓存的固定长度 identity，不重新 canonicalize/hash descriptor。

Hash 仅用于：

- semantic identity；
- content-addressing；
- corruption/transport mismatch 检测；
- install/activation/route 绑定。

Hash 不提供：

- authorization；
- MetadataStore writer 身份；
- non-repudiation；
- credential secrecy；
- bearer capability；
- Byzantine proof。

在当前 MetadataStore/activation authority 非 Byzantine 的威胁模型下，不需要：

- descriptor 签名；
- PKI；
- Merkle tree；
- cluster-keyed descriptor HMAC。

Descriptor 是一个有界小对象，Merkle tree只会增加格式、key lifecycle和审计复杂度。ACL/control authority负责授权；hash不能替代 ACL。

### 1.4 Ledger instance、generation 与 mutation

首版最小合同应是：

```text
immutable identity tuple:
ledgerId
ledgerInstanceId
canonicalSchemaVersion
descriptorHash
immutable authBinding identity/generation
```

- reserved backlink、sidecar root、INSTALL、本地 route、READY 和 ACTIVATE 都绑定这个 tuple；
- descriptor creation generation 与 MetadataStore store version 分离；
- 首版同一 ledger instance 内 descriptor generation 不递增；
- descriptor 变化意味着拒绝，或创建新的 ledger instance；
- auth binding 变化同样不允许原地 rotation；首版需要新 ledger instance；
- 不为尚未设计的 future key rotation引入通用 epoch/KMS state machine。

控制平面 TLS/SASL principal 的基础设施轮换可独立发生，但不得改变已有 ledger data credential 的语义。

### 1.5 Crash、response loss 与 ABA

- canonical bytes/root 未 durable publication：没有 descriptor authority；
- child durable 但未被 instance/domain head 引用：inert/orphan；
- publication response loss：按 exact instance + semantic generation + bytes/hash 重读；
- 同 operation identity、同 bytes：解析为同一 durable result；
- 同 identity、不同 bytes/hash：`CONFLICT`，无 effect；
- ledgerId reuse：必须使用新 `ledgerInstanceId`，即使 store version/hash碰巧相同；
- hash mismatch、missing descriptor bytes、unknown version、unknown mandatory feature：对应 instance fail closed，不能 fallback 为 Classic；
- compaction/snapshot 只能发布 verified replacement 后回收旧 descriptor/reference。

### 1.6 性能与复杂度

允许的成本：

- create/install/open 冷路径 canonicalization 和一次 hash；
- Bookie install 时重算/验证；
- handle 建立时缓存 compact identity；
- Add 时 bounded identity comparison。

禁止：

- per-Add descriptor parsing/hash；
- per-Add remote lookup；
- per-entry descriptor node/CAS；
-全局 descriptor registry；
- descriptor Merkle proof；
- 因 runtime policy 调整重新 install；
- 因无关 LedgerMetadata version变化重新激活。

### 1.7 Spike/Test Gate

Spike A 必须新增：

- 至少两个独立 encoder/decoder 或 reference encoder 的 golden vectors；
- reordered sets/maps；
- duplicate fields/keys；
- absent vs explicit defaults；
- unknown semantic field/capability/version；
- optional hint变化不改变 semantic hash；
- hash self-inclusion负例；
- truncated/oversized/deep nesting；
- schema-version cross-write；
- old writer不得覆盖新 bytes；
- store version reset与ledgerId reuse ABA；
- hash mismatch/corruption；
- normal Add instrumentation确认零 canonicalization/hash/remote read。

Model A 只需抽象 `descriptorSupported/hashMatches/instanceMatches`，不需要把 hash bytes或密码学放进状态空间。

### 1.8 保持 OPEN

- deterministic protobuf、自定义 TLV、CBOR 等 exact codec；
- field numbers/byte order/hash algorithm/hash length；
- descriptor 最大 bytes/capability count；
-最终 safety-limit 字段表；
-未来 descriptor translation；
-未来 key rotation/KMS；
-optional hint 的exact外层容器。

---

## Q2 — Protected auth binding + install/activation proof

### Verdict：REVISE 后可 LOCK

应拒绝两种极端：

- 把现有 master key 或其公开 digest 当 activation proof；
- 为此引入每次 Add 的签名、证书链、KMS 或 nonce state。

首版最小可实现方案是：

```text
authenticated cold control channel
+ Bookie direct verification of committed MetadataStore authority
+ Bookie local durable protected binding
+ normal Add bounded local comparison
```

这利用现有“MetadataStore 非 Byzantine”假设，不需要新的 PKI。

### 2.1 两类授权必须分开

#### Data-plane ledger credential

当前 master key verifier可继续用于 normal Add/fence等现有 ledger access control，但：

- 只在受保护 transport/内存/本地 protected storage 中出现；
- 不进入 descriptor、sidecar、receipt、日志、metric、exception；
- 不授权 INSTALL、READY、ACTIVATE、repair/delete；
- 不能证明 READY 曾存在。

首版本地实现可以缓存已验证 verifier并做固定长度、最好 constant-time 的比较；不要求每 Add重新 SHA-1/HMAC/KMS。

#### Control-plane authority

INSTALL、profile metadata mutation、ACTIVATE、recovery grant、tombstone等必须由 distinct control authority触发：

- authenticated non-anonymous principal，或等价的受保护内部 endpoint；
- `AuthDisabledPlugin/ANONYMOUS` 不能调用 Profile control operations；
- master-key holder不自动成为 control-plane principal；
- profile sidecar namespace mutation必须由现有 `profiledMetadataMutationAuthority` 约束。

### 2.2 推荐的首版 proof 机制

不建议首版把 activation proof做成 bearer MAC/certificate。推荐：

1. ACTIVATE 请求只携带 bounded authority reference；
2. Bookie 在冷路径直接读取并验证 committed READY或post-membership authority；
3. Bookie确认 exact route/install/instance/hash/target/incarnation/membership；
4. Bookie持久化本地 ACTIVE transition；
5. 后续 normal Add只检查本地 durable state。

这意味着：

```text
caller-supplied activationEpoch != proof

committed sidecar/membership authority
+ Bookie independent cold-path verification
+ durable local ACTIVE
= activation authority
```

优点：

- 不需要签名服务、PKI、cluster MAC key、token revocation list；
- 不把 correctness绑定 watch；
- 一次激活一次远程读取，不进入 Add 热路径；
- response loss 可重读 MetadataStore和本地 durable result；
- READY compaction由既有 sidecar snapshot+suffix合同承接。

如果未来使用离线 certificate/MAC，必须满足下面相同的 scope/replay合同，但首版没有必要。

### 2.3 INSTALL 最小验证与顺序

INSTALL 最低顺序：

1. 验证 control connection不是 anonymous，principal有 Profile install权限；
2. 冷读/验证 exact PREPARING instance root 与 canonical descriptor；
3. 校验：
   - ledgerId；
   - ledgerInstanceId；
   - descriptor schema/hash；
   - required Engine/capability；
   - target Bookie stable ID；
   - target storage incarnation；
   - install operation identity/generation；
   - semantic payload identity；
4. 建立 protected local credential binding；
5. 原子持久化：
   - `PROFILE` route claim；
   - instance/Profile/Engine；
   - protected auth binding identity；
   - install generation；
   - initial normal-inactive；
6. durability完成后返回 secret-free receipt。

相同 operation identity + 相同 semantic payload + matching credential，可返回既存结果。相同 identity + conflict descriptor/instance/credential/target必须 `CONFLICT`。

Receipt只可暴露：

- ledger/instance/descriptor identity；
- target/incarnation；
- install result/generation；
- operation identity；
- durable status。

Receipt不能包含：

- password/master key/verifier；
- unkeyed digest of password/verifier；
- bearer token；
-可离线试猜的 auth commitment；
- secret KMS material；
-可直接重放的 control capability。

公开的 opaque `authBindingId` 只有在它本身不能授权、不能离线验证猜测时才可进入 receipt。非 Byzantine coordinator不需要通过公开 credential commitment证明所有 Bookie用了同一秘密。

### 2.4 READY 与 ACTIVATE

READY publication继续由 RFC-0001 sidecar lifecycle authority拥有。它绑定：

- ledger/instance/descriptor；
- exact standard metadata membership/version/digest；
-目标 install generations；
- lifecycle generation；
- initial或replacement purpose。

ACTIVATE：

1. 校验 control caller；
2. direct-read exact READY/membership authority；
3. 检查 target仍是该 authority 指定的 Bookie/incarnation；
4. 检查本地 matching inactive install/auth binding；
5. 检查 fence/tombstone/delete generation没有使其失效；
6. durable切换到 matching `NORMAL_ACTIVE`；
7. durability完成后返回。

Initial activation消费 READY；write-time replacement消费 post-membership-CAS authority。两者不得复用一个未绑定 membership/purpose 的通用 token。

READY 已存在后，重复 ACTIVATE最多触发相同幂等 transition；它不能让调用方选择新的 descriptor、target或generation。

### 2.5 normal Add

normal profiled Add 必须验证：

```text
profile wire operation
+ exact ledger instance/descriptor route identity
+ matching local protected credential
+ matching durable NORMAL_ACTIVE
+ current fence/admission generation
+ entry/payload identity
```

它不需要：

-读取 READY sidecar；
-重新验证 activation certificate；
-重新 canonicalize/hash descriptor；
-调用 KMS；
-做公钥签名验证；
-创建 nonce/control record；
-等待 activation/control fsync。

请求可以携带 compact route/activation handle，也可以由 server handle捕获 generation；exact fields OPEN。不可删语义是 stale request/handle不能匹配新的 instance或activation generation。无需在每次 Add重复完整 Engine、capability vector和descriptor bytes。

### 2.6 recovery Add

Recovery不能继续只依靠 legacy `RECOVERY_ADD` flag。最低验证：

```text
profile recovery operation
+ instance/descriptor/auth match
+ exact RepairIntent/admission/grant identity
+ target/incarnation
+ range/entry scope
+ grant generation
+ delete-fence/tombstone check
+ payload identity
```

grant由 admitted RepairIntent与delete-fence authority导出，冷路径验证并持久化到本地。Recovery Add只查 bounded local grant；grant close后旧请求不能继续写。

### 2.7 防重放绑定

每个 control proof/reference或未来 certificate必须 domain-separate，并至少绑定：

```text
protocol purpose / logical opcode
ledgerId + ledgerInstanceId
descriptor hash/schema
target Bookie stable ID + storage incarnation
local route/install generation
relevant READY/membership/RepairIntent/delete-fence generation
operation identity + semantic payload identity
normal vs recovery purpose
```

不得跨：

- ledger；
- reused instance；
- target；
- storage incarnation；
- INSTALL/ACTIVATE；
- normal/recovery Add；
- initial/replacement activation；
-旧/new membership；
- closed grant/tombstone。

不需要 per-Add nonce。Cold operation identity、CAS predecessor、本地 durable generation和entry/payload idempotency已经提供所需 replay fencing。

### 2.8 Crash 与 response loss

- INSTALL 在 durable transition前 crash：无 install authority；
- transition后 response loss：同 op重试/查询得到既存 result；
- READY存在但本地 ACTIVE未 durable：Add transient unavailable/fail closed；
- ACTIVATE durable后 response loss：重试同 authority得到 ACTIVE；
- stale proof/old membership：拒绝，不回退；
- fence/delete先赢：activation/grant失败；
- local auth state无法恢复：quarantine/non-writable，不从 master key猜 route；
- credential conflict：永久 conflict，不能创建第二 local binding；
- normal Add response loss：只以相同 Profile operation/entry/payload重试，绝不尝试 Classic Add。

### 2.9 性能与复杂度

冷路径新增：

- INSTALL一次 authenticated control RPC和MetadataStore read；
- ACTIVATE一次 authority read和本地 durable transition；
- recovery grant同类操作。

热路径新增上限：

-一次 bounded route/handle lookup；
- instance/hash或compact handle比较；
-固定长度 credential比较；
- existing fence/admission lookup；
- existing payload durability。

禁止：

- per-Add MetadataStore/KMS；
- per-Add certificate chain/signature；
- per-Add auth-binding HMAC重算；
- per-entry nonce表；
- public credential digest；
- global key-rotation service；
-将 control principal 与 ledger master key合并。

### 2.10 Spike/Test Gate

Spike A必须覆盖：

- `AuthDisabledPlugin` 调用 INSTALL/ACTIVATE必拒绝；
-合法 master-key client在 READY/ACTIVE前尝试 normal/recovery Add；
- secret出现在 mismatch日志、MDC、metric label、receipt、exception、admin dump 的负向扫描；
- INSTALL/ACTIVATE authority direct-read response loss；
- wrong instance/hash/target/incarnation/purpose/opcode；
- initial proof重放到replacement；
- replacement proof重放到initial；
- normal/recovery proof互换；
- grant close/delete/fence并发；
- restart后 protected binding与接受集合不扩大；
- normal Add CPU、allocation、remote call、crypto/fsync instrumentation。

`LedgerDescriptorImpl.checkAccess()` 当前输出完整 master key 的行为必须成为明确的 hard-fail regression test。

Model A/C只抽象：

```text
controlAuthorized
authorityExists
localBindingMatches
localActivationDurable
grantMatches
```

不应在 TLA+ 中建证书、MAC或KMS模型。

### 2.11 保持 OPEN

- protected credential在本地的exact storage/packing；
-是否复用现有 master-key storage或独立 protected slot；
- control RPC走现有 auth plugin、mTLS endpoint或内部通道；
- authority reference字段名/path；
-未来是否使用MAC/certificate；
- transport deployment policy；
-未来 credential rotation/KMS；
- constant-time compare具体实现；
- exact public receipt schema。

---

## Q3 — Profiled opcode/capability negotiation/error compatibility

### Verdict：REVISE 后可 LOCK

必须锁“逻辑操作和 fail-closed行为”，不必现在锁具体 opcode number、protobuf field number或 exception class。

核心修订是：

> Profile安全语义不能附着在旧 `ADD_ENTRY` 的 optional fields或 legacy recovery flag上。必须有一个旧 decoder 无法解释成 Classic Add 的 mandatory Profile wire discriminator。

### 3.1 最小逻辑 operation surface

P0至少需要以下逻辑操作：

#### Cold control

```text
PROFILE_INSTALL
PROFILE_INSTALL_STATUS / idempotent retry result
PROFILE_ACTIVATE
PROFILE_ACTIVATION_STATUS / idempotent retry result
PROFILE_RECOVERY_GRANT / CLOSE
PROFILE_TOMBSTONE or local delete consumption
```

它们可以共用一个 versioned Profile control envelope和 mandatory subtype；不需要每个语义占一个物理 opcode。

#### Data path

```text
PROFILE_ADD_NORMAL
PROFILE_ADD_RECOVERY
PROFILE_POINT_READ
PROFILE_FENCE
PROFILE_READ_LAC
PROFILE_WRITE_LAC / EXPLICIT_LAC
PROFILE_FORCE or explicit capability rejection
PROFILE_LIST_ENTRIES or explicit capability rejection
```

normal与recovery必须是不同 logical operation。可以共享一个 Profile write envelope，但 operation kind必须是 mandatory、unknown-reject，不能继续依靠 legacy optional recovery flag。

#### Optional fast paths

```text
PROFILE_BATCH_READ
PROFILE_RANGE_READ
PROFILE_BATCH_RECOVERY_ADD
```

这些由独立 capability控制：

- unsupported必须明确返回 capability error；
- recovery按 RFC-0004 fallback到point-read/single-entry add；
-不能盲目复用当前注释为unsupported的 `RANGE_*` number；
-不能让 fast-path capability成为普通 Profile ACK前置。

所有 Profile route上的 mutating operation都必须有 instance-aware Profile语义。旧 read/add/LAC/fence请求命中 Profile/Tombstone route必须在 Classic handle/lazy create前拒绝。否则 ledgerId reuse仍可能让旧请求作用到新 instance。

### 3.2 Physical wire 可以合并，但 discriminator 必须独立

可接受的最小物理方案是：

-一个新的 Profile protocol envelope；
- mandatory operation subtype；
- connection-level一次协商；
- compact per-request route identity。

不要求：

-每项一个 opcode；
-每 Add发送完整 descriptor/Engine/capability vector；
-每 Add重新协商。

但必须证明：

```text
Profile bytes在所有旧decoder上：
- 要么明确unsupported/EBADREQ；
- 要么connection close；
- 永远不能产生ParsedAddRequest；
- 永远不能写master key、route、payload。
```

鉴于当前 v3异常会 fallback到legacy decoder，这必须是 byte-level property。单元测试里 mock“old server不支持新 enum”不够。

可行实现候选包括新 framing/preamble、明确不会映射为 legacy ADD 的 reserved discriminator、独立 advertised endpoint等；exact选择 OPEN。无论选择哪种，都必须用真实旧 binary与 raw byte corpus验证。

### 3.3 Capability negotiation

最小顺序：

1. placement读取 registration capability hint；
2.连接建立时做一次 version/capability握手，绑定：
   - server stable ID；
   - storage incarnation；
   - active Engine；
   - protocol generation；
   - supported mandatory capabilities；
   - readiness/registration generation；
3.结果缓存在该 channel/pool；
4.只有握手与 descriptor mandatory requirements匹配后才发送 Profile op；
5. durable install receipt仍是最终 install evidence。

Registration不是 install proof，握手也不是 READY/ACTIVE proof。

Reconnect/Bookie restart后必须重新协商。无需在每 Add做 lease或远程查询。

### 3.4 每个 Profile request 的最小绑定

请求必须携带或通过 compact negotiated handle不可歧义地绑定：

```text
Profile protocol version
logical operation kind
ledgerId
ledgerInstanceId
descriptor identity
auth identity/credential
route or activation generation identity
entry/range identity
operation/payload identity where idempotency requires
```

Recovery额外绑定 intent/grant/range generation。

Engine和完整 capability vector可以从 descriptor + negotiated channel + local route推导，不应在每 Add重复发送。不可删要求是不能把请求路由到不同 Engine/instance/activation；不是要求字段机械重复。

### 3.5 Mixed-version/fail-closed matrix

| Client | Bookie | Route/状态 | 必须行为 |
| --- | --- | --- | --- |
| old | old | Classic | 保持现行行为 |
| old | new | `ABSENT/CLASSIC` | 保持现行 Classic |
| old | new | `PROFILE` | normal/recovery/read/LAC等在lazy create前拒绝 |
| old | new | `TOMBSTONED` | terminal reject |
| new Profile | old | 任意 | placement/handshake unsupported；绝不降级到Classic |
| new Profile | new但无matching Engine/cap | 任意 | unsupported/capability mismatch |
| new Profile | new matching | 未install/未active | fail closed；可区分control reconciliation |
| new Profile | new matching | exact active | Profile operation |
| mixed ensemble | 部分旧或missing capability | create/open | Profile create/open失败；不得部分降级 |
| rolling restart | Bookie generation变化 | existing connection | reconnect/re-negotiate；stale binding拒绝 |
| unknown Profile version/op | new Bookie | 任意 | unknown mandatory/unsupported，无route/payload effect |
| Profile bytes | old Bookie | 任意 |不得被解释为legacy ADD/RECOVERY_ADD |

如果现有 Profile ledger中的一个 Bookie以old/no-capability registration重新出现，客户端只能：

-将其视为 unavailable；
-按 install→membership CAS→activate替换；
-或等待正确版本恢复。

不能在该 replica上改发 Classic Add。

### 3.6 Unknown protobuf field/opcode

锁定：

- safety语义不得放在 optional field；
- Profile envelope中的 unknown mandatory field/version/subtype拒绝；
- safety-neutral optional diagnostic可忽略；
- old Bookie unknown operation的响应或connection close不能触发 client downgrade；
- malformed/unknown v3不能经当前 fallback变成legacy write；
- current LightProto对unknown enum/required field的exact行为必须实测，不能靠普通 protobuf假设。

推荐增加一个 decoder invariant：

```text
一旦连接被明确识别为Profile framing，
任何解析失败都关闭连接；
不得切到legacy mode。
```

Profile client也不得在同一 logical Add失败后自动尝试 legacy encoder。

### 3.7 最小 semantic error classes

不锁 exact code，但至少需要以下稳定分类：

1. `UNSUPPORTED_PROFILE_PROTOCOL / OPCODE / CAPABILITY / ENGINE`
   - 对该 target非重试；
   - 允许placement替换；
   - 禁止Classic downgrade。

2. `PROFILE_IDENTITY_CONFLICT`
   - instance/descriptor/auth/operation payload冲突；
   - 非重试；
   - 不泄漏具体 credential。

3. `PROFILE_NOT_READY / NOT_ACTIVE / STALE_GENERATION`
   - only Profile control reconciliation后可重试；
   -不能改成Classic。

4. `FENCED / TOMBSTONED / DELETED`
   - 对该 instance/op terminal。

5. `RECOVERY_GRANT_MISSING / STALE / OUT_OF_SCOPE / CLOSED`
   - 当前 recovery op无authority；
   -不能用normal或legacy recovery flag替代。

6. `TRANSIENT_UNAVAILABLE / READ_ONLY / OVERLOADED / CONTROL_RECONCILING`
   - same Profile、same payload可重试。

7. `DURABILITY_RESULT_UNKNOWN / RESPONSE_LOSS`
   -重读status或相同 operation identity重试；
   -不能双写两个 protocol。

8. `UNKNOWN_MANDATORY / CORRUPT_AUTHORITY / QUARANTINED`
   - fail closed；
   -需要管理/升级处理。

外部 unauthorized错误可保持粗粒度，避免暴露“哪一段 credential匹配”；内部 metric可以记录 bounded reason，但不能带 secret、高基数 operation ID或ledger vector。

当前 async processor把大量错误压成 `EUA` 的路径必须改造并有端到端测试。

### 3.8 Response loss 与 retry

- connection negotiation response loss：重连并重新协商；
- INSTALL/ACTIVATE：same op + same payload查询/重试；
- Add response loss：same Profile operation + same entry/payload重试；
- ambiguous close/EBADREQ：不降级，只标为 unsupported或unknown；
- server返回 conflict：不重试；
- capability改变：重新placement/replace，不改协议；
-任何 retry不得同时向旧/new opcode双写；
-同 entry不同 payload必须 conflict，不以“第一次response丢了”为理由覆盖。

### 3.9 性能与复杂度

允许：

- registration/placement读取 capability；
-每连接一次 handshake；
-每 request解析 compact Profile identity；
-本地 bounded route/state lookup。

禁止：

- per-Add negotiation；
- per-Add registration/MetadataStore lookup；
-每 Add完整 descriptor/capability vector；
- Classic请求做Profile remote check；
- generic “try Profile then Classic”；
-为每 feature建立独立连接/状态机；
-为optional fast path阻塞核心 Add。

Classic新Bookie唯一不可删成本是本地 route gate；它应与 handle lookup合并并被 benchmark。

### 3.10 Spike/Test Gate

Spike A必须增加真实 compatibility corpus：

- stock old client/new Bookie；
- new client/stock old Bookie；
- mixed ensemble；
- rolling restart；
- stale registration/service info；
- v2/v3 decoder；
- valid/invalid/truncated/oversized Profile frame；
- unknown enum/op/version/required field；
-当前 v3→pre-v3 fallback；
-每个 byte vector断言：
  - no Classic handle；
  - no master-key persistence；
  - no route claim；
  - no payload；
  - no ACK；
- response loss与connection close不得触发 downgrade；
- normal/recovery subtype互换；
- error从Bookie processor到client future/admin/metric完整贯通；
- capability negotiation只发生于connection，不发生于每 Add；
- Classic throughput/p99/allocations回归。

Compatibility matrix必须锁定真实 binary commit/版本，不用“old/new”抽象标签代替。

Model A加入：

```text
WireNegotiated
ProfileOpcode
OldDecoder
CapabilityMatches
NoDowngrade
```

但 raw decoder bytes留在 executable Spike，不塞入 TLA+。

### 3.11 保持 OPEN

- exact opcode numbers；
- protobuf或其他 envelope bytes；
-是否使用独立 endpoint/preamble；
- exact handshake fields；
- exact error numbers/BKException classes；
- capability bit/ID allocation；
- batch/range wire schema；
- timeout/backoff数值；
-外部错误信息细度；
-client pool/cache实现。

---

## Q4 — Engine/superblock/registration downgrade and rollback gate

### Verdict：REVISE 后可 LOCK；当前 Segment ACK authority 仍 BLOCK

语义合同可以锁定，但当前源码没有一个已经证明有效的 old-binary blocker。必须通过真实 stock old binary测试后，才能解除 Segment ACK authority 的 BLOCK。

### 4.1 最小分层，不创建重复状态机

至少需要三个不同职责的事实：

#### A. Bookie/storage-scope compatibility fence

负责：

- active Engine family；
- Bookie stable identity；
- storage incarnation；
- mandatory local control-format generation/features；
-受管 device/directory manifest或其digest；
- migration state/generation；
-最低可读/可写兼容边界。

它必须处于旧 binary已经 mandatory读取、且unknown会在 replay前失败的路径。仅新增一个旧 binary忽略的文件不够。

#### B. Device/Arena superblock

负责：

- device/storage incarnation；
- Arena format/version；
- mandatory feature set；
- control/checkpoint format与generation；
- active checkpoint/superblock选择。

它保护新 binary之间的格式兼容和partial device migration，但它本身不能阻止完全不认识Arena的old binary。

#### C. Cluster registration readiness

绑定：

```text
Bookie stable ID
storage incarnation
Engine Profile
protocol/capability generation
verified local format generation
effective assignment/readiness generation
writable registration generation
```

它是 placement/readiness authority，但不能替代本地 old-binary blocker。

Cookie可以承载或引用 A，但不能同时积累Arena checkpoint、per-ledger route、repair/delete history。Registration也不能成为 Add-time lease。

### 4.2 当前明确禁止的伪方案

以下方案不能通过：

-只 bump现有 Cookie layout version；
-只增加 Cookie optional field；
-只增加 BookieServiceInfo property；
-只在Arena目录放一个新 superblock；
-只写unknown negative Journal meta-entry；
-只依赖部署文档说“不要rollback”；
-只依赖new client placement排除old Bookie；
-让data-integrity auto-stamp覆盖未知 mandatory marker；
-old binary启动后再由registration/watch demote。

原因分别是现行 Cookie不比较future version、registration无CAS、old binary不读Arena、Journal明确跳过unknown special entry，而且 direct legacy client仍可能绕过placement。

### 4.3 最小启动顺序

推荐冻结：

1. 在创建 Journal/LedgerStorage/Arena writer、replay和registration前读取 Bookie compatibility fence；
2. 验证 stable ID、storage incarnation、Engine、migration state、mandatory feature set和配置device manifest；
3. 验证每个required device/Arena superblock；
4. unknown/corrupt/missing/partial mismatch时进入non-writable/quarantine；
5.恢复 ArenaControlLog/checkpoint；
6.恢复 route/auth/activation/fence/grant/tombstone；
7.完成delete stream/assignment catch-up；
8.持久化exact local readiness generation；
9. cluster conditional registration验证相同 incarnation/format/assignment；
10.才允许 writable registration和RPC write acceptance。

现有 `checkEnvironment()` 早于 `readJournal()` 是可利用的hook，但实现必须确认更早创建的storage对象没有先打开或改写Segment authority。Gate应位于真正的preboot边界，而不只是形式上位于register之前。

### 4.4 Upgrade/migration 顺序

首版推荐独立 Segment cohort/新 storage incarnation，避免在一个仍服务Classic的进程中按ledger切换Engine。

最小迁移状态可等价为：

```text
CLASSIC_COMPATIBLE
MIGRATION_PREPARED_NON_WRITABLE
SEGMENT_FORMAT_READY
REGISTRATION_READY
```

exact名称 OPEN。顺序：

1. drain并撤销旧 writable registration；
2. cluster/local publication一个old-binary-visible的 fail-stop migration fence；
3. response loss后重读 exact operation/generation；
4.在所有required device初始化/验证Segment superblock与local authority store；
5.只在全部一致后发布 `SEGMENT_FORMAT_READY`；
6.恢复并验证route/control/delete assignment；
7.发布local readiness；
8. conditional cluster registration；
9.开始 serving Profile traffic。

第2步必须先让rollback最危险的old binary停止；设备写可随后逐个完成。这样不需要跨所有文件/device的原子事务：partial state保持non-writable即可。

如果无法实现一个 stock old binary必然读取并拒绝的兼容性 fence，则首版只能使用一个旧 binary无法取得的全新 BookieId、storage scope和访问凭据，并仍需本地 format marker。仅“运维通常不会启动旧binary”不构成安全合同。

### 4.5 Scope 与 partial device

- Engine/compatibility fence：Bookie/storage-incarnation scope；
- format superblock：device/Arena scope；
- route/profile：ledger-instance scope；
- registration：Bookie/incarnation/effective-assignment scope。

不要给每个ledger复制一份Engine superblock。

一个required device为：

- missing marker；
- old format；
- unknown mandatory；
- corruption；
-不同 storage incarnation；

则整个包含它的 writable Bookie readiness失败。可以保留受限read-only/recovery模式，但不能静默从device manifest移除后注册writable。

若确需移除device，必须作为cluster-authorized新 storage incarnation/device manifest generation处理，而不是本地忽略。

### 4.6 Mixed version 与 rolling upgrade

- Classic-only Bookie、目录和ledger未出现Segment marker时维持现行行为；
- Segment Profile placement只选择matching Engine/protocol/readiness的Bookie；
- PREPARED migration不影响其他Classic cohort；
- Bookie降级成missing capability registration时，从Profile placement剔除；
-旧 registration generation不能继续作为matching ready Bookie；
-现有 Profile ledger遇到downgraded replica时只能 unavailable/replace，不能Classic fallback；
- capability/assignment generation变更不要求每 Add远程查lease；
-本地进程得知自身effective readiness失效时demote/non-writable，客户端通过连接断开/retry/placement处理。

### 4.7 Rollback 合同

允许直接rollback到旧 binary的条件必须非常窄：

1. 从未发布任何 Segment/Profile local success；
2. 不存在不可由旧 binary解释的route、activation、fence、grant、tombstone、Arena control或payload authority；
3. 所有migration/device写要么未发生，要么经过验证的reverse migration/wipe；
4. cluster已接受旧incarnation decommission/新incarnation publication；
5. stale registration已fence；
6. rollback marker通过CAS/generation持久发布。

一旦存在任何旧 binary无法安全解释的 mandatory authority，直接rollback禁止，状态是：

```text
NON_WRITABLE / MIGRATION_REQUIRED
```

可接受的恢复方式是：

- roll forward到兼容binary；
- verified export/rebuild；
- irreversible wipe/decommission后以新storage incarnation加入。

不能依赖old Journal“跳过unknown entry”继续运行。

同一format/capability family内的binary rollback可以允许，但只有目标binary声明支持所有mandatory features和min-reader/min-writer gate。Exact compatibility range由manifest决定。

### 4.8 Corruption、unknown 与 response loss

- unknown mandatory marker/superblock/control record：scope quarantine；
- unknown optional仅在不影响acceptance/replay时可忽略；
- corrupt marker不能当成ABSENT/Classic；
-一个device marker缺失不能让系统猜它是新盘；
- cluster marker存在但本地marker缺失：non-writable；
- migration publication response loss：重读exact generation，不重复创建新incarnation；
- registration response loss：重读cluster readiness/ephemeral generation；
- stale old process尝试注册：local format fence先阻止；cluster registration再防第二层；
- data-integrity cookie repair不得删除、降级或重写unknown mandatory Segment fence；
- wipe/decommission必须绑定exact Bookie/device/storage incarnation，不能跨scope重放。

### 4.9 性能与复杂度

成本都在启动/迁移/registration路径：

-一次Bookie compatibility marker读取；
-每required device一次superblock验证；
-恢复时检查mandatory format；
-一次conditional readiness/registration。

不增加：

- normal Add远程I/O；
- per-Add registration lease；
- per-entry format marker；
- per-entry fsync；
- Classic cohort的Profile handshake；
-全局cluster stop-the-world。

复杂度控制：

-只有一个Bookie compatibility fence，不创建通用migration transaction manager；
- Arena superblock继续由RFC-0003拥有；
- local route由RFC-0005拥有；
- registration/assignment由RFC-0004/0005拥有；
- publish blocker → 初始化device → publish ready的fail-safe ordering代替跨device事务；
- exact bytes/tooling由Spike决定。

### 4.10 Spike/Test Gate

Spike A：

- new/old client与new/old Bookie compatibility matrix；
- registration missing/stale generation；
- Profile control endpoint在anonymous auth下拒绝；
-旧Bookie direct legacy Add不能绕过；
-rolling restart/reconnect；
- downgrade不触发双写。

Spike B：

-真实stock old binary在compatibility fence存在时：
  - 早于Journal replay退出；
  - 不注册writable；
  - 不打开/写入Segment route/Arena；
-仅bump Cookie version的负例；
- unknown Cookie field/version的真实parser行为；
- data-integrity auto-stamp不能“修复”Segment fence；
- migration每一步crash；
- compatibility fence response loss；
- partial device、missing superblock、different incarnation；
- unknown mandatory/optional format；
- checkpoint/superblock corruption；
- rollback before/after first Segment authority；
- stale registration/BookieId reuse；
- startup时间、read amplification和Classic无热路径回归。

Spike C：

抽象状态至少增加：

```text
LocalFormatCompatible
OldBinaryBlocked
StorageIncarnation
MandatoryFeaturesKnown
MigrationPrepared
FormatReady
RegistrationReady
WireNegotiated
```

检查：

```text
Writable => format compatible
Writable => registration matches exact incarnation/readiness
Old binary + Segment marker => not writable
Unknown mandatory => not writable
Partial migration => not writable
Rollback after Segment authority => not writable
```

不要把Cookie bytes、filesystem或crypto细节放进TLA+。

### 4.11 保持 OPEN

- compatibility fence是在Cookie、existing mandatory VERSION surface还是等价机制；
- exact marker/superblock bytes；
- checksum/hash与A/B slot布局；
- device manifest编码；
- migration工具/CLI；
-min reader/writer具体版本；
- registration CAS adapter/path；
-新BookieId还是same BookieId + storage incarnation；
- read-only quarantine能力；
- exact rollback tooling；
-partial migration批次与超时。

但以下不是 OPEN，而是硬合同：

- old binary必须在 replay/handle create/write/register前fail closed；
-新文件被old binary忽略不够；
- Cookie version bump本身不够；
- unknown mandatory不能skip；
- partial migration不能writable；
- direct rollback在不可解释authority存在后禁止。

---

## 5. 跨 RFC 一致性与同步位置

本轮没有发现与 Round 1–5 已锁合同不可调和的新矛盾。推荐方案保持：

- READY先于local ACTIVE；
- normal Add只查本地 durable authority；
- standard LedgerMetadata唯一拥有membership；
- sidecar使用bounded CAS domain，不建通用事务；
- route-before-lazy-create；
- repair grant与normal activation分离；
- local physical owner继续OPEN；
- no per-entry MetadataStore/control fsync。

需要同步以下位置。

### RFC-0001

- §3.1：补 control-plane principal与anonymous-auth禁止；hash不等于authorization。
- §5：canonical schema/version、derived hash、duplicate/default/unknown规则、optional hint隔离、immutable descriptor/auth binding。
- §7：INSTALL protected binding与secret-free receipt。
- §8：compact Profile request identity；不要求每 Add完整Engine/descriptor/proof。
- §11：完整mixed-version matrix与decoder fallback风险。
- §12：registration hint + connection handshake + durable receipt三层。
- §13：新增无downgrade、secret non-disclosure、unknown schema、immutable key binding不变量。
- §14：golden vectors、raw wire corpus、old binary与secret leak Gates。
- §15：只保留exact codec/hash/opcode/auth transport等实现项。

### RFC-0003

- §4/§5：Arena superblock format family、mandatory features、storage incarnation。
- §11/§12：unknown newer superblock/checkpoint fail closed。
- §16：partial device migration/readiness。
- §18：真实old/new binary、superblock corruption与startup performance Gate。
- §19：exact bytes/tooling继续OPEN。

RFC-0003不应拥有Bookie registration或ledger auth。

### RFC-0004

- §4/§7：range/batch operation作为optional Profile capability；unsupported进入point fallback。
- §7.2：recovery add不得使用legacy flag，必须消费Profile recovery grant。
- §9：RepairIntent grant proof的cold verification/target/incarnation/opcode绑定。
- §13/§17/§18：Profile error semantic class与mixed-version recovery fallback。
- 不需要引入新PKI或per-entry proof。

### RFC-0005

- §3：补充现有master key derivation、secret logging、auth-disabled、decoder fallback、cookie与registration事实。
- §4：完整Profile logical operation/capability matrix。
- §5：protected local binding、control/data authority分离。
- §6/§7：distinct Profile normal/recovery operation与stale generation。
- §8：Profile read/LAC/list必须instance-aware或明确reject。
- §9：Bookie compatibility fence、startup order、migration/rollback。
- §10：connection-amortized negotiation与bounded local Add check。
- §11：no wire downgrade、old binary fail-stop、secret non-disclosure不变量。
- §12：raw byte/stock binary/partial migration/performance Gates。
- §13：exact codec/opcode/marker/physical store保持OPEN。

### Spike A

新增 descriptor、auth、wire、mixed-version、secret leak、control principal、raw decoder corpus与normal Add instrumentation场景。

### Spike B

新增 compatibility fence、old binary、Cookie auto-stamp、partial device、superblock/version/migration/rollback场景。

### Spike C

只抽象 format/wire/auth compatibility状态和fail-closed不变量，不建密码学或filesystem模型。

### `docs/turbo-bk.md`

同步：

- §5.4/§5.5 canonical descriptor与首版禁止rotation/downgrade；
- §8 新增 secret/hash/wire/old-binary不变量；
- §10 在Stage 2/5/6前加入compatibility manifest与stock binary Gate；
- §11 增加descriptor/wire/auth/format证据；
- §12 保留exact bytes/算法/physical owner为OPEN；
- §13 明确isolated prototype与live shadow/ACK authority边界。

新增 `grill/ROUND-06-profile-security-wire-downgrade.md` 时，应完整归档本回复；Integration Audit期间 README保持 `Review Complete / Integration In Progress`。

---

## 6. 本轮可立即 LOCK 的精确合同

1. Descriptor只包含immutable safety semantics；operation、credential、runtime policy分离。
2. `descriptorHash`是canonical bytes派生值，不进入自身preimage。
3. schema version、hash suite与canonical bytes共同定义identity。
4. duplicate字段/key/set item拒绝；默认值和集合顺序canonical。
5. unknown semantic version/field/mandatory capability拒绝。
6. safety-neutral optional hint在semantic descriptor/hash外。
7.同一instance内descriptor与auth binding immutable；变化使用新instance。
8. hash只做identity/integrity，不做authorization。
9.首版不需要descriptor签名、Merkle、PKI或cluster HMAC。
10. master key/password/verifier及可离线验证派生物不得进入公开metadata、receipt、日志、metric、exception。
11. master key只作为data credential，不能授权Profile control operations。
12. Profile control operations要求distinct non-anonymous authority。
13.首版activation推荐使用Bookie cold-read committed authority + local durable ACTIVE，不在每 Add传/验proof。
14. INSTALL原子绑定route、instance、Profile、Engine、protected auth、generation、inactive。
15. ACTIVATE绑定exact READY或post-membership authority、target/incarnation/purpose/generation。
16. normal Add只做bounded local route/auth/active/fence检查。
17. recovery Add使用distinct Profile logical operation和bounded local grant；legacy flag不足。
18. Profile safety语义不得加在legacy ADD optional字段上。
19. Profile framing必须证明旧decoder永远不能解释为Classic Add。
20. client在任何Profile失败/response loss后不得自动Classic downgrade或双写。
21. capability negotiation按registration hint → connection handshake → durable install receipt分层；不做per-Add negotiation。
22. old/new/mixed/rolling/unknown matrix按fail closed执行。
23. exact error code可OPEN，但unsupported、identity conflict、not-active、fenced/deleted、grant mismatch、transient、durability unknown、unknown mandatory语义必须区分。
24. Bookie compatibility fence必须在old binary的mandatory pre-replay path上确定失败。
25.仅Cookie version bump、registration property、新superblock或unknown Journal record均不足。
26. startup按compatibility fence → device superblock → recovery → assignment → readiness → registration排序。
27. partial device migration、unknown mandatory、corruption均non-writable。
28.不可解释的Segment authority存在后直接rollback到old binary禁止。
29. compatibility/registration检查不进入normal Add热路径。
30.所有上述边界必须由真实old/new binary、raw bytes和performance instrumentation验证。

---

## 7. 必须保持 OPEN 的清单

- exact canonical codec与field numbers；
- hash algorithm/length/domain-separator bytes；
- descriptor最大大小/capability count；
- optional hint envelope；
-最终 safety-limit字段表；
- protected auth本地物理表示；
- control endpoint/auth plugin/mTLS选择；
- authority reference/certificate exact机制；
-未来key rotation/KMS；
- Profile opcode number与wire framing bytes；
- connection handshake exact fields；
- exact error/BKException/admin codes；
- batch/range/recovery wire schema；
- Bookie compatibility fence exact physical encoding；
- Cookie/superblock/marker布局；
-local authority physical owner；
- registration CAS adapter/path；
- migration/rollback工具；
-min reader/writer版本和数值阈值；
- performance Gate的最终硬件与数字。

这些 OPEN 项不能被写成最终合同。特别是“canonical规则已锁”不等于“codec已选”，“old binary必须失败已锁”不等于“当前Cookie bump已经安全”。

---

## 8. 本轮后各层能否开始

### Docs

**可以。**

可以按本轮合同同步 RFC、Spike、总纲并做Integration Audit。

### Spike harness / Model

**可以开始实现。**

- Spike A可实现descriptor golden vectors、auth/wire/mixed-version harness；
- Spike B可实现format/cookie/old-binary migration harness；
- Spike C可加入抽象状态。

正式运行前必须在immutable manifest中锁 exact codec/hash、wire candidate、auth mechanism、old binary commits和format fence candidate。

### Prototype

**可以开始隔离、无authority的原型。**

允许：

- canonical encoder/reference vectors；
- cold authority verifier；
- protected local binding interface；
- Profile decoder/handshake；
- preboot compatibility fence；
- registration CAS adapter；
- secret leak tests。

不得把原型结果直接等同于安全合同或生产ready。

### Shadow

**只允许隔离或不接管ACK authority的shadow prototype。**

Live Stage 5 promotion仍需：

- RFC-0003接受；
- local format/old-binary gate实际通过；
- Spike B crash/performance evidence；
-不会污染可rollback的Classic cohort。

### Segment ACK authority

**不可以，仍 BLOCK。**

至少还缺：

- exact descriptor/hash manifest；
- protected control/auth机制；
-无歧义wire与真实旧binary matrix；
- provable pre-replay downgrade fence；
- local physical authority格式；
- RFC-0001/0003/0005接受；
- Spike A/B/C实际PASS；
- canary-specific ACK/fence/rollback evidence。

---

## 9. 距整体 Implementation Ready 的最小剩余 frontier

Round 6后，设计树不再需要继续讨论“要不要签名/Merkle”“要不要per-Add proof”“要不要optional field兼容”这类总体方向。下一轮应只 grill 一个实现选择 manifest：

1. **Descriptor manifest**
   - exact canonical codec；
   - exact hash suite；
   - golden corpus；
   - bounded parser limits。

2. **Control/auth manifest**
   - cold direct authority read的接口；
   - non-anonymous control principal；
   - protected local binding；
   - secret redaction；
   - response-loss query。

3. **Wire compatibility manifest**
   - exact non-ambiguous framing/discriminator；
   - logical subtype分配；
   - old v2/v3 decoder byte corpus；
   - exact retry/error projection。

4. **Format/readiness manifest**
   - old-binary-visible compatibility fence；
   - Bookie/device/Arena marker关系；
   - registration CAS；
   - migration/rollback工具与stock binary测试。

完成这个 manifest round并通过Integration Audit后，才适合编码具有稳定持久/wire兼容面的实现。之后仍需：

- Round 5留下的local physical authority owner/record framing最终选择；
- General E/W/A point-read oracle及rich outcome的exact API实现面；
- Spike A/B/C执行与RFC接受；
- shadow、ACK canary和rollback证据。

没有新的通用事务、全局锁或热路径性能依赖需要引入。当前真正的P0风险不是算法复杂度，而是四个边界如果仍停留在“某种hash、某种proof、某个opcode、某个marker”，实现很容易在旧decoder、匿名control caller、secret logging或old-binary replay处静默降级。

<!-- END VERBATIM REVIEW -->

## Integration Audit 1

> Reviewer：同一固定 `gpt-5.6-sol / max` agent
> 结论：**FAIL / REQUEST CHANGES**
> 归档规则：以下回复未经删节；仅将 Markdown heading 统一下移一级并清理行尾空白。

<!-- BEGIN VERBATIM INTEGRATION AUDIT 1 -->

## Round 6 Integration Audit 1

### 总 verdict：FAIL / REQUEST CHANGES

审计范围正确：9 个已跟踪 Markdown、1 个新 Round 6 归档；未读取或触碰 PDF。已跟踪 diff 为 378 行新增、60 行删除，`git diff --check` 通过。

集成的大方向正确，30 项 LOCK 中大多数已忠实进入对应 owner；但有 6 处合同或验证面尚未闭合，其中 authentication authorization、hash安全属性、key-rotation矛盾和Classic连接协商范围会直接影响安全或兼容性。本轮不能标记 `Reviewed / Integrated`，也不能 commit/push。

按 `code-review-expert` 的严重级别审查：

- P0：0
- P1：6
- P2：2
- P3：0

---

### 1. 30 项 LOCK 逐项对照

| # | 审计 | 结论 |
| --- | --- | --- |
| 1 | Descriptor只含immutable safety semantics | PASS：RFC-0001 §5、turbo §5.4已分离operation/credential/runtime |
| 2 | hash为canonical bytes派生且不自包含 | PASS |
| 3 | schema/hash suite/canonical bytes定义identity | **PARTIAL**：组合已写入，但缺少“fixed-length collision-resistant”最低算法属性 |
| 4 | duplicate/default/order canonical | PASS |
| 5 | unknown semantic/mandatory拒绝 | PASS |
| 6 | optional neutral hint在hash外 | PASS |
| 7 | descriptor/auth binding同instance immutable | **PARTIAL**：正文已锁，但RFC-0001 §15仍把“首版是否禁止key rotation”列为OPEN |
| 8 | hash仅identity/integrity | PASS |
| 9 | 无descriptor签名/Merkle/PKI/cluster HMAC | PASS |
| 10 | secret/offline verifier不进入公开与诊断面 | PASS；但credential transport保护另有缺口 |
| 11 | master key只作data credential | PASS |
| 12 | control使用distinct non-anonymous authority | **PARTIAL**：当前规范只要求authenticated/non-anonymous，没有规范性要求“authorized for exact operation/scope” |
| 13 | 首版cold committed-authority verification + local ACTIVE | PASS；direct-read作为首版选择已一致同步，exact endpoint/reference仍OPEN |
| 14 | INSTALL原子绑定route/instance/Profile/Engine/auth/generation/inactive | PASS |
| 15 | ACTIVATE绑定READY/post-membership、target/incarnation/purpose/gen | PASS |
| 16 | normal Add只有bounded local checks | **PARTIAL**：remote/hash/KMS/signature/fsync已禁止，但未明确禁止per-Add auth-binding hash/HMAC重算 |
| 17 | recovery使用distinct Profile op与bounded grant | PASS |
| 18 | 不用legacy ADD optional field表达Profile safety | PASS |
| 19 | old decoder绝不能将Profile framing解释成Classic Add | PASS |
| 20 | Profile失败/response loss不得Classic fallback或双写 | PASS |
| 21 | registration hint→connection handshake→receipt；无per-Add negotiation | **PARTIAL**：无per-Add已锁，但RFC-0001目前写成“每条连接”必须协商，未限定Profile-capable连接 |
| 22 | old/new/mixed/rolling/unknown矩阵fail closed | PASS |
| 23 | semantic errors区分，exact code OPEN | PASS；RFC与A26已要求不能压成误导retry的generic EUA |
| 24 | old-visible mandatory pre-replay fence | PASS |
| 25 | Cookie bump/registration/new file/unknown Journal均不足 | PASS |
| 26 | startup完整排序 | PASS |
| 27 | partial/unknown/corrupt format不writable | PASS |
| 28 | unsafe Segment authority后禁止直接old-binary rollback | PASS |
| 29 | compatibility/registration不进入Add热路径 | PASS |
| 30 | real binaries/raw bytes/performance instrumentation | **PARTIAL**：raw decoder与stock binary完整；Spike B缺startup/read-amplification性能记账 |

---

## 2. 必须修复项

### P1-1：Authenticated/non-anonymous 被误当成 authorized control authority

涉及：

- `RFC-0001-profile-capability-install.md` §3.1、§6.6、§7、§14
- `RFC-0004-range-recovery-delete.md` §9.1/§9.2、invariant/Gate
- `RFC-0005-segment-bookie-state.md` §4/§5、Gate
- `SPIKE-C-no-object-tla.md` Model A
- `docs/turbo-bk.md` §5.4、invariant 29、manifest

当前规范只要求：

```text
non-anonymous authenticated principal
```

这只是 AuthN，不是 AuthZ。一个有普通BookKeeper身份但没有Profile lifecycle权限的caller，按当前文字仍可能满足INSTALL/ACTIVATE/grant前置。即使direct-read阻止伪造READY，它仍可能通过INSTALL抢占route、绑定错误credential或制造DoS。

Spike A A24已经正确测试：

```text
authenticated authorized
authenticated unauthorized
anonymous
```

但RFC、总纲和Model C没有把第二类明确建模为拒绝。

最小修复：

```text
authenticated and authorized for the exact
control operation + ledger instance + target/scope
```

- AuthZ检查必须早于route claim、credential persistence、local allocation和任何durable effect；
- `non-anonymous`只是必要条件；
- exact ACL/RBAC/plugin保持OPEN；
- Model A增加`controlAuthorizedForOperationScope`，不能只保留`nonAnonymous`；
- 不需要引入通用RBAC框架或新PKI。

### P1-2：Descriptor hash 缺少 collision-resistance 最低属性

涉及：

- `RFC-0001-profile-capability-install.md` §5、invariant/Gate
- `docs/turbo-bk.md` §5.4/§11
- `SPIKE-A-profile-install.md` manifest/A23

当前只说“hash suite”，理论上CRC、截断过短hash或旧弱算法仍满足文字。由于客户端可提供descriptor，且hash参与instance/install/route identity，“异义同hash”必须依赖collision resistance防止。

最小修复：

- 锁定所选suite必须是固定长度、面向对抗输入的collision-resistant hash；
- checksum/CRC不能作为descriptor semantic identity；
- exact算法、长度和domain-separator bytes继续OPEN；
- Spike manifest拒绝不满足该属性的候选；
- 不新增签名、Merkle、PKI或HMAC。

### P1-3：首版key rotation同时被LOCK和OPEN

涉及：

- `RFC-0001-profile-capability-install.md:176`
- `RFC-0001-profile-capability-install.md:589`

正文已明确：

```text
同一instance auth binding immutable
首版不引入key-rotation state machine
变化使用新instance
```

但§15仍写：

```text
首版是否禁止 key rotation
```

这是直接合同矛盾。

最小修复：把OPEN改成只保留：

```text
未来跨instance credential/KMS rotation的协议、版本、迁移和兼容边界
```

首版禁止原地rotation不能继续标OPEN。

### P1-4：Capability handshake 当前可能被解释为所有Classic连接都必须执行

涉及：

- `RFC-0001-profile-capability-install.md` §12
- `docs/turbo-bk.md` §5.4/§11
- `SPIKE-A-profile-install.md` A26与性能Gate

RFC-0001目前写“每条连接建立时必须协商”。这会与以下合同冲突：

- old client + new Bookie在`ABSENT/CLASSIC`保持现行行为；
- Classic路径不承担Profile negotiation成本；
- 旧客户端根本不会执行新Profile handshake。

最小修复：

- 限定为“准备发送Profile operation的Profile-capable connection”在连接上协商一次；
- legacy Classic连接不要求新handshake；
- shared pool若同时承载两类流量，Profile operation只能使用已协商的channel/context；
- restart/incarnation变化后Profile channel重新协商；
- Spike A加入：
  - old Classic client无需新handshake仍保持现行路径；
  - Classic-only connection建立成本、wire与Add throughput没有Profile negotiation回归。

这不改变registration hint→Profile handshake→durable receipt三层合同。

### P1-5：Protected transport与per-Add crypto禁区没有完整同步

涉及：

- `RFC-0001` §3.1、§6.6、§8
- `RFC-0005` §6/§10
- `docs/turbo-bk.md` §5.4
- `SPIKE-A` A19/A24/hard Gate

主审查明确指出：

- credential只应出现在受保护transport/内存/本地storage；
- normal Add不应重新计算auth-binding HMAC/hash、调用KMS或验证证书。

当前集成只写了authenticated channel以及“不做KMS/signature”，没有锁：

- credential-bearing Profile transport的confidentiality/integrity属性；
- per-Add auth-binding hash/HMAC重算为零。

最小修复：

- 要求携带master key/verifier/bearer secret的Profile control/data transport满足manifest选定的confidentiality/integrity；exact TLS/SASL/mTLS机制仍OPEN；
- 如果明确不把网络攻击者纳入威胁模型，则必须直说且不得声称transport-protected；不能只写“authenticated”留下歧义；
- normal Add只允许缓存后的固定长度identity/verifier比较；
- 增加Spike计数：
  - `normal Add auth-binding hash/HMAC/KMS/signature invocations = 0`
  - `credential observed on unprotected Profile transport = 0`
- 不要求改变Classic password/KDF，不要求每Add新crypto。

### P1-6：Spike B遗漏startup/read-amplification性能记账

涉及：

- `SPIKE-B-allocator-block.md` §8
- `RFC-0005` §10/§12
- `docs/turbo-bk.md` §11

B14/B15的正确性矩阵很好，但主审查要求的以下性能反馈未进入Spike B：

- cold/warm startup latency；
- compatibility fence/superblock/device manifest/readiness产生的read bytes/IO count；
- device数量增长下的startup scaling；
- Classic-only startup与hot Add无回归。

最小修复：

- 在Spike B §8加入上述测量；
- exact阈值继续OPEN，当前只要求报告原始数据、分布和matched baseline；
- 明确format验证只发生于startup/migration/registration；
- 不把它做成per-Add lease或remote read。

---

### P2-1：`routeOrAdmissionGeneration` 容易被误读为强制per-Add wire field

`RFC-0001` §8 的逻辑request block直接列出：

```text
routeOrAdmissionGeneration
```

Round 1和本轮主审查锁的是：

```text
请求或negotiated/local handle context必须足以匹配durable activation；
exact binding fields保持OPEN。
```

当前文字可能重新冻结一个显式per-request field，并带来不必要wire字节与prior-contract冲突。

最小修复：

- 将标题改成“logical binding，由request或negotiated/local handle context提供”；
- 明确不锁一个叫`routeOrAdmissionGeneration`的wire field；
- 只锁stale context/handle不能跨generation成功。

### P2-2：RFC-0005遗漏当前master key derivation源码事实

主审查要求在RFC-0005 §3同步：

```text
DigestManager.generateMasterKey()
= 20-byte SHA-1("ledger" || password) verifier
```

当前只同步了“随Add发送的verifier”、secret logging和AuthDisabledPlugin，未记录SHA-1 derivation。这不会单独改变不变量，但会弱化“不能公开offline verifier、且本轮不宣称重设计KDF”的证据链。

最小修复：在Classic源码基线中补一句精确事实；不要借此扩大成KDF重构。

---

## 3. 已通过的核心面

### Descriptor

已正确同步：

- semantic/operation/runtime分离；
- hash不自包含；
- duplicate/default/order规则；
- unknown mandatory拒绝；
- optional hint在hash外；
- old writer不能strip/rewrite；
- descriptor/auth binding同instance immutable；
- no signature/Merkle/PKI；
- canonical work不进入Add。

除collision-resistance和key-rotation OPEN矛盾外，Q1集成方向正确。

### Auth/activation

已正确同步：

- master key与control authority分离；
- AuthDisabled/anonymous拒绝；
- install/activate绑定target/incarnation/purpose/generation；
- secret-free receipt；
- response loss重读同operation；
- normal Add只消费local durable ACTIVE；
- initial/replacement/recovery不能重放；
- no per-Add proof/KMS/signature/remote read/control fsync。

需要补的是精确AuthZ，而不是增加更复杂认证系统。

### Wire/recovery/error

已通过：

- normal/recovery logical operation分离；
- legacy recovery flag不足；
- Profile safety不依附legacy optional field；
- v3→pre-v3 fallback被明确列为真实风险；
- raw corpus使用真实old binaries；
- no Classic fallback/double write；
- mixed/rolling/unknown矩阵；
- batch unsupported只回退single Profile recovery；
- semantic error从processor/future/admin/metric端到端保留；
- exact opcode/code/schema仍OPEN。

### Format/downgrade

已通过：

- RFC-0005拥有Bookie compatibility fence/registration消费；
- RFC-0003只拥有Arena superblock/allocator，不拥有auth/registration；
- Cookie bump/property、新文件、registration、unknown Journal record均明确不足；
- stock old binary pre-replay Gate；
- Cookie auto-stamp负例；
- partial device、incarnation、manifest、unknown/corrupt mandatory；
- migration无cross-device transaction；
- unsafe rollback禁止；
- new BookieId/storage scope fallback；
- readiness不成为Add lease。

没有引入全局锁、跨device事务或通用migration transaction manager。

---

## 4. OPEN 边界审计

以下仍准确保持OPEN：

- exact descriptor codec/field numbers；
- hash algorithm/length/domain-separator；
- parser/capability bounds；
- optional hint envelope；
- protected local auth physical representation；
- control endpoint/auth plugin/mTLS；
- authority reference/certificate encoding；
- Profile opcode/framing/handshake fields；
- exact errors/BKException/admin codes；
- batch/range/recovery wire schema；
- compatibility marker bytes/path；
- Cookie/superblock/device-manifest layout；
- local authority physical owner；
- registration CAS adapter；
- migration/rollback tooling；
- minimum reader/writer versions；
- numeric performance thresholds。

Direct-read committed authority目前被一致集成为首版冷路径语义；我没有把它判为误锁。OPEN的是endpoint、transport、authority record/reference/certificate的exact表示，而不是允许caller scalar/watch/registration替代独立验证。

唯一真实OPEN冲突是RFC-0001残留的“首版是否禁止key rotation”。

---

## 5. 性能与复杂度结论

除上述两个性能缺口外，集成没有新增过度设计：

- 无descriptor签名、PKI、Merkle；
- 无per-Add MetadataStore；
- 无per-Add descriptor hash；
- 无per-Add negotiation；
- 无per-entry control fsync；
- 无global capability lease；
- 无cross-device transaction；
- physical owner仍OPEN；
- Profile control、negotiation、format/readiness均在冷路径；
- normal Add保持bounded local route/identity/auth/active/fence检查；
- Classic/Profile route gate仍可与handle lookup合并。

当前性能风险只有：

1. “每条连接必须协商”可能误伤Classic/old client；
2. 未显式禁止per-Add auth HMAC/hash；
3. Spike B没有记录startup/read-amplification。

这些可用小幅合同修正闭合，不需要新架构。

---

## 6. 归档、README 与文件质量

PASS：

- `ROUND-06-profile-security-wire-downgrade.md` 为单一H1；
- 1,518行、59,873 bytes；
- 含完整基线、四题、30项LOCK、完整OPEN、层级readiness与remaining frontier；
- `BEGIN VERBATIM REVIEW` / `END VERBATIM REVIEW`完整；
- heading下移和行尾清理未见反馈丢失；
- README保持 `Review Complete / Integration In Progress`；
- 10份Markdown均单一H1；
- code fence数量均为偶数；
-本地相对链接检查通过；
- `git diff --check`通过；
- Round 6归档无trailing whitespace且单一EOF newline；
- 文件范围只有约定的9个tracked docs和Round 6 archive；
- PDF仍是唯一无关untracked项，未读取或纳入。

---

## 7. 状态与许可

当前必须继续保持：

```text
Round 6: Review Complete / Integration In Progress
overall: Proposed / Not Implementation Ready
live Stage 5 promotion: BLOCKED
Segment ACK authority: BLOCKED
stable wire/on-disk compatibility surface: BLOCKED
```

允许继续：

- docs修复；
- Spike harness；
- Model；
- isolated/no-authority prototype。

在上述6个P1和2个P2最小修复完成、Audit 1全文归档后，再交回同一reviewer做Integration Audit 2。当前不许可标记 `Reviewed / Integrated`，不许可commit/push。

<!-- END VERBATIM INTEGRATION AUDIT 1 -->

## Integration Audit 2

> Reviewer：同一固定 `gpt-5.6-sol / max` agent
> 结论：**PASS / APPROVE**
> 归档规则：以下回复未经删节；仅将 Markdown heading 统一下移一级并清理行尾空白。

<!-- BEGIN VERBATIM INTEGRATION AUDIT 2 -->

## Round 6 Integration Audit 2

### 总 verdict：PASS / APPROVE

Audit 1 的 6 个 P1 和 2 个 P2 已全部以最小方式闭合。未发现修复引入的新安全冲突、性能回归合同、owner 越界或过度设计。

按 `code-review-expert` 的 P0–P3 标准：

- P0：0
- P1：0
- P2：0
- P3：0

允许完整归档本回复，将 Round 6 标记为 `Reviewed / Integrated`，仅暂存本轮 10 份 Markdown 后 commit，并 push 到 `nereusstream/bookkeeper` 的 `origin/turbo-bk`。不得纳入 PDF。

### 1. Audit 1 八项修复复核

#### 1.1 Exact-scope AuthZ：PASS

合同已在以下位置一致闭合：

- RFC-0001 §3.1、§6.6、§7、§13/§14；
- RFC-0004 repair grant/close、invariant 与 Gate；
- RFC-0005 §4–§7、invariant 与 Gate；
- Spike A A24、hard Gate 与停止条件；
- Spike C Model A state/action/check/invariant；
- `docs/turbo-bk.md` §5.4、不变量 29 与 manifest Gate。

现在要求同时满足：

```text
authenticated
&& non-anonymous
&& authorized for exact operation
&& authorized for exact ledger instance
&& authorized for exact target/scope
```

而且 AuthN/AuthZ 明确早于 route claim、credential persistence、ledger/storage allocation及任何 durable effect。`AuthDisabledPlugin`、anonymous、master-key-only 和 authenticated-but-unauthorized caller 均不能产生 transition。

Spike C 虽未强制一个具体 TLA+ 变量名，但其：

- `exact operation/instance/target-scope authorization` state；
- `AuthorizeControlOperationScope` action；
- `ProfileControlRequiresExactOperationScopeAuthorization` invariant；
- `A-PROFILE-COMPAT` config；

已经构成与 `controlAuthorizedForOperationScope` 等价、可执行的抽象面。

Exact ACL、RBAC、plugin 和 policy mapping 继续 OPEN；文档没有要求通用 RBAC framework、PKI 或新身份系统。

#### 1.2 Descriptor hash 最低属性：PASS

RFC-0001 已明确：

- suite 固定长度；
- 面向对抗输入 collision-resistant；
- CRC、普通 checksum 和不满足该属性的过短截断值不能承担 semantic identity；
- exact algorithm、length 和 domain-separator bytes 保持 OPEN。

Spike A manifest、A23、hard Gate 与 `docs/turbo-bk.md` 已同步 candidate eligibility 和负向验证。该修复只冻结安全下界，没有提前选择 SHA 系列、BLAKE 系列或具体长度，也没有引入签名、Merkle、PKI 或 cluster HMAC。

RFC-0003 中用于 block/control-log 完整性的 checksum 与此不冲突；被禁止的是用 checksum 充当 ProfileDescriptor semantic identity。

#### 1.3 Key rotation：PASS

现在不存在 LOCK/OPEN 矛盾：

- 同一 ledger instance 的 semantic descriptor 与 protected auth binding immutable；
- 首版禁止同 instance 原地 key rotation；
- 变化必须使用新 ledger instance；
- OPEN 仅保留未来跨 instance credential/KMS rotation 的协议、版本、迁移与兼容边界。

RFC-0001 §5、§6.6、§15及总纲已经一致。

#### 1.4 Profile-only capability handshake：PASS

RFC-0001 §12 已准确收窄为：

- 只有准备发送 Profile operation 的 Profile-capable connection 执行连接级协商；
- restart、storage incarnation 或 protocol generation 变化后重新协商；
- legacy Classic connection 不要求新 handshake，且不能被 Profile handshake 阻断；
- shared pool 中 Profile operation 只能使用已协商 channel/context；
- negotiation 按连接摊销，不在每个 Add 执行。

Spike A A19/A26、hard Gate和总纲不变量 33均已同步 Classic connection 建立成本、wire、throughput 和 no-handshake Gate。三层证据仍保持：

```text
registration hint
→ Profile connection handshake
→ durable install/activation receipt
```

前一层不能替代后一层。

#### 1.5 Protected transport与normal Add crypto边界：PASS

规范现已锁定：

- 任何携带 master key、verifier 或 bearer secret 的 Profile control/data transport，必须满足 implementation manifest 选择的 confidentiality 和 integrity 属性；
- exact TLS/SASL/mTLS 机制保持 OPEN；
- 该合同不追溯重写 Classic password/KDF；
- normal Add 只比较已验证、已缓存的固定长度 descriptor/auth identity 或 verifier；
- per-request auth-binding hash/HMAC、KMS、certificate/signature validation 均为 0；
- canonical encode/hash、完整 descriptor parse、authority read和capability negotiation也不进入 normal Add。

Spike A A19/A24及hard Gate已经包含：

```text
normal Add auth-binding hash/HMAC/KMS/signature/certificate invocations = 0
credential observed on unprotected Profile transport = 0
legacy Classic connection forced Profile handshake = 0
```

这些计数针对新增的 Profile auth-binding 工作，不取消 BookKeeper 现有 payload integrity 语义或所选安全 transport 的正常 record protection。

#### 1.6 Spike B startup/read-amplification：PASS

Spike B §8 已增加独立的 cold-path 矩阵：

- cold/warm Classic-only startup；
- cold/warm Segment startup；
- required-device count scaling；
- compatibility-fence read bytes/I/O；
- device manifest和per-Arena superblock read bytes/I/O；
- allocator/route/delete recovery、readiness和registration phase latency；
- time-to-read-only 与 time-to-writable。

要求保留原始样本、分布、matched baseline、device scaling和可归因计数；exact阈值保持 OPEN，并必须在正式运行前冻结，不能看到结果后追溯补 Gate。

Format/readiness验证被限制在 startup/migration/registration，normal Add相关read/remote-I/O计数必须为0。这没有把startup验证变成Add-time lease或远程查询。

#### 1.7 Add logical binding未冻结wire field：PASS

RFC-0001 §8 现在明确：

- 锁定的是 logical route/admission context binding；
- context可来自request、已协商channel或server local handle；
- 不锁定名为 `routeOrAdmissionGeneration` 的显式per-request wire field；
- exact fields保持 OPEN；
- stale request/channel/handle不能跨instance、activation、fence或admission generation成功。

这保留了正确性，同时避免多余wire字节和对既有接口合同的过早冻结。

#### 1.8 20-byte SHA-1源码事实：PASS

RFC-0005 §3准确记录：

```text
DigestManager.generateMasterKey()
= 20-byte SHA-1("ledger" || password) verifier
```

源码复核一致：

- `DigestManager.generateMasterKey()`调用`MacDigestManager.genDigest("ledger", password)`；
- `genDigest`依次输入UTF-8 `"ledger"`与password；
- `MacDigestManager.DIGEST_ALGORITHM`为`SHA-1`；
- `BookieProtocol.MASTER_KEY_LENGTH`为20。

文档只把它作为现行data credential/verifier事实，并明确：

- 不把它提升为Profile control authority；
- 不声称SHA-1 verifier具有更强安全性；
- 不借Round 6扩大成Classic KDF重构。

### 2. 30项 LOCK 逐项复核

| # | LOCK | Audit 2 |
| --- | --- | --- |
| 1 | Descriptor只包含immutable safety semantics | PASS；operation、credential、runtime policy和hint已分离 |
| 2 | hash从canonical bytes派生且不进入自身preimage | PASS |
| 3 | schema version、hash suite和canonical bytes共同定义identity | PASS；并补足fixed-length collision resistance |
| 4 | duplicate/default/order canonical | PASS；singular/map/set duplicate拒绝，list/set语义分开 |
| 5 | unknown semantic version/field/mandatory capability拒绝 | PASS；unknown mandatory不能被解释为default |
| 6 | safety-neutral optional hint在semantic hash外 | PASS |
| 7 | 同instance descriptor/auth binding immutable | PASS；首版原地rotation明确禁止 |
| 8 | hash只做identity/integrity | PASS；明确不授权control或data operation |
| 9 | 不要求signature/Merkle/PKI/cluster HMAC | PASS |
| 10 | secret/offline verifier不进入公开或诊断面 | PASS；metadata、receipt、log、metric、exception均覆盖 |
| 11 | master key只作data credential | PASS |
| 12 | Profile control使用distinct non-anonymous authority | PASS并收紧；还必须exact-scope AuthZ，AuthN-only拒绝 |
| 13 | cold committed-authority verification + local durable ACTIVE | PASS；direct-read是首版冷路径合同 |
| 14 | INSTALL原子绑定route/instance/Profile/Engine/auth/generation/inactive | PASS |
| 15 | ACTIVATE绑定exact READY/post-membership、target/incarnation/purpose/generation | PASS |
| 16 | normal Add只有bounded local checks | PASS；remote read、canonical/auth hash、proof、KMS、control fsync均为0 |
| 17 | recovery使用distinct Profile operation和bounded local grant | PASS |
| 18 | Profile safety不能依赖legacy ADD optional field | PASS |
| 19 | old decoder不能将Profile framing解释成Classic effect | PASS；raw corpus Gate覆盖route/handle/payload/ACK |
| 20 | Profile失败/response loss不得Classic fallback或双写 | PASS |
| 21 | registration hint→connection handshake→receipt，无per-Add negotiation | PASS；handshake只约束Profile-capable connection |
| 22 | old/new/mixed/rolling/unknown矩阵fail closed | PASS |
| 23 | semantic error class保留，exact code OPEN | PASS；processor/future/admin/metric端到端保留 |
| 24 | old-visible mandatory pre-replay compatibility fence | PASS；RFC-0005 owner清楚 |
| 25 | Cookie bump/registration/new file/unknown Journal record均不足 | PASS；Spike B B14真实旧binary否证 |
| 26 | startup完整排序 | PASS；fence→device→recovery→assignment→readiness→registration |
| 27 | partial migration/unknown mandatory/corruption不writable | PASS |
| 28 | unsafe Segment authority存在后禁止直接old-binary rollback | PASS；negative proof和new-incarnation路径保留 |
| 29 | compatibility/registration不进入Add热路径 | PASS |
| 30 | 真实binary、raw bytes和performance instrumentation | PASS；Spike A/B/C与总纲均有可执行Gate |

全部30项均已进入正确owner，没有将原本的exact实现选择伪装成最终合同。

### 3. OPEN边界

以下Round 6项目继续正确保持OPEN：

- exact canonical codec、field numbers和descriptor version-evolution编码；
- exact hash algorithm、length和domain-separator bytes；
- descriptor/parser/capability hard bounds；
- optional hint envelope；
- 最终safety-limit字段表；
- protected auth本地物理表示；
- control endpoint、auth plugin和exact ACL/RBAC mapping；
- credential-bearing transport所采用的exact TLS/SASL/mTLS机制；
- authority reference/certificate exact encoding；
- 未来跨instance credential/KMS rotation协议；
- Profile opcode number、wire framing bytes和subtype encoding；
- connection handshake exact fields；
- exact BKException、admin和numeric error codes；
- batch/range/recovery wire schema；
- old-binary-visible compatibility fence的exact marker/path/bytes；
- Cookie、Bookie marker、device manifest和Arena superblock布局；
- local durable authority physical owner；
- registration CAS adapter/path；
- migration、rollback、reverse、wipe工具；
- minimum reader/writer版本；
- benchmark硬件和全部numeric performance thresholds。

需要特别说明：

- `fixed-length collision-resistant`是已锁的安全属性，不等于已经选择算法或长度；
- old binary必须在pre-replay路径fail-stop是已锁语义，不等于当前Cookie bump或某个candidate marker已经安全；
- direct-read committed authority是首版已锁冷路径语义，OPEN的是endpoint、transport和reference representation；
- 首版同instance原地rotation已禁止，不再属于OPEN；
- broader Round 1–5的schema、topology、physical owner和数值OPEN项没有被本次修改关闭。

### 4. Owner与跨文档一致性

Owner边界保持正确：

- RFC-0001拥有descriptor canonical semantics、Profile lifecycle/install/activation、control/data auth separation、wire downgrade和capability negotiation；
- RFC-0003只拥有Arena allocator、payload/control-log、superblock与device-format recovery；不拥有Profile AuthZ或cluster registration；
- RFC-0004拥有repair/delete authority、recovery grant来源、range outcome与no-legacy-fallback语义；
- RFC-0005拥有Segment Bookie对上述authority的本地消费、old-binary compatibility fence、startup/readiness和ACK eligibility；
- Spike A验证descriptor/auth/wire/route；
- Spike B验证format/old binary/migration/performance；
- Spike C只抽象Spike A/B必须实际验证的布尔结果和generation关系，不伪造密码学、filesystem或decoder证明。

未发现：

- RFC-0003重复拥有registration/auth；
- Profile recovery回退legacy flag；
- semantic error被generic result抹除；
- descriptor hash被误当authorization；
- registration hint替代durable receipt；
- superblock替代old-visible compatibility fence；
- readiness演化成per-Add lease。

### 5. 性能、复杂度与过度设计

#### 热路径

PASS：

- normal Add不读MetadataStore、sidecar、assignment或format state；
- 不做per-Add negotiation；
- 不重新canonicalize/hash descriptor；
- 不重新计算Profile auth-binding hash/HMAC；
- 不调用KMS、signature或certificate verifier；
- 不新增per-entry control record/fsync；
- 仅执行可与handle lookup合并的bounded local route、fixed-length identity/verifier、active/fence/admission检查。

#### 冷路径

合理且有成本记账：

- AuthN/AuthZ、authority direct-read、descriptor parse/hash、INSTALL/ACTIVATE发生在control/create路径；
- capability negotiation按Profile connection摊销；
- format/device/readiness验证只在startup/migration/registration；
- Spike B单独记录cold/warm、read bytes/I/O和device scaling；
- 冷transition仍可queue/group commit，未要求一种语义一次独占fsync。

#### 复杂度

没有引入：

- descriptor signature、PKI或Merkle；
- 通用RBAC系统；
- per-Add proof/nonce；
- global capability lease；
- universal metadata hot head；
- cross-device transaction；
- 通用migration transaction manager；
- local physical owner的提前锁定。

本轮增加的是必要的domain separation、fail-closed gate和可否证instrumentation，不是新分布式事务层。

### 6. 归档、README与文件范围

验证结果：

- HEAD：`1c2fc1b30aa69b55cbfac4116609c4f4e78fb2b5`；
- `origin/turbo-bk`：同一SHA；
- `HEAD...origin/turbo-bk = 0/0`；
- push remote为`git@github.com:nereusstream/bookkeeper.git`；
- 9份tracked Markdown修改；
- 1份新Round 6归档；
- PDF仍是唯一无关untracked文件，未读取、未修改、未纳入。

归档完整性：

- 主审查原文仍精确结束于第1518行；
- 主审查prefix仍为1518行、59,873 bytes，与Audit 1记录一致；
- Audit 1完整位于其后，包含全部6个P1、2个P2、已通过面、OPEN、性能、文件和许可结论；
- `BEGIN/END VERBATIM REVIEW`与`BEGIN/END VERBATIM INTEGRATION AUDIT 1`四个边界齐全；
- 当前归档1953行、75,989 bytes；
- README仍为`Review Complete / Integration In Progress`，符合审计期间要求。

文件校验：

- `git diff --check`：PASS；
- 10份Markdown均恰好一个H1；
- 所有code fence计数为偶数；
- relative links：PASS；
- 无trailing whitespace；
- 全部文件以单一LF结尾。

### 7. 状态与发布许可

Round 6集成现在可以执行以下收尾：

1. 将本回复完整追加到Round 6归档；
2. 将README中的Round 6状态改为`Reviewed / Integrated`；
3. 仅暂存本轮10份Markdown；
4. 明确排除`BtrLog Low-Latency Logging.pdf`；
5. commit；
6. push到`nereusstream/bookkeeper`的`origin/turbo-bk`；
7. 核对远端SHA与提交内容后进入下一轮grill。

本次PASS不改变整体产品状态：

```text
Round 6 integration: PASS
overall: Proposed / Not Implementation Ready
stable wire/on-disk compatibility surface: BLOCKED pending exact manifest + real gates
live Stage 5 promotion: BLOCKED
Segment ACK authority: BLOCKED
production canary: BLOCKED
```

### 8. 下一轮最小frontier

下一轮应聚焦唯一的implementation manifest选择，不再重开总体架构：

1. canonical codec、field manifest、hash suite/length/domain separator和parser bounds；
2. exact control endpoint/AuthZ mapping、protected local binding、cold authority-read/reference与credential-bearing transport；
3. Profile framing/opcode/subtype、handshake字段、semantic error projection和raw old-decoder corpus；
4. old-visible compatibility-fence candidate、Cookie/superblock/device-manifest关系、registration CAS、minimum reader/writer及migration/rollback tooling；
5. 对上述manifest执行Spike A/B/C真实binary、raw-byte、crash和性能Gate。

这些选择与实际证据闭合后，才可判断稳定wire/on-disk surface和更高层authority实现是否解锁；Round 6文档集成本身已经通过。

<!-- END VERBATIM INTEGRATION AUDIT 2 -->
