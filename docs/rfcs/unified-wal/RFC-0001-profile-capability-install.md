# RFC-0001：Profile、Capability 与显式安装协议

> 状态：**Proposed / P0 Prerequisite**<br>
> 依赖：BookKeeper 现有 metadata CAS、ensemble 与 fencing 语义<br>
> 解锁对象：`BK_SEGMENT_WAL` 及其组合<br>
> 验证：必须通过 [Spike A](spikes/SPIKE-A-profile-install.md)

## 1. 摘要

本 RFC 将原先混合在一个 Profile 名称中的三类概念拆开，并为需要 Bookie 执行的新 ledger 合同定义两阶段显式安装协议。

核心候选决策：

- Engine Profile 由 Bookie 进程或部署 cohort 决定；
- Ledger Contract Profile 由 immutable ProfileDescriptor 描述；
- ProfileDescriptor使用strict flat TLV与SHA-256的36-byte identity；policy/capability registry仍由owner RFC接受；
- Client/Protocol Profile 不冒充 Bookie capability；
- Profile control/data使用独立`bookie-profile` immediate-TLS1.3/mTLS endpoint；legacy `bookie-rpc`不解析Profile frame；
- 新合同在 ledger `OPEN` 前安装到当前 ensemble 的全部 E 个 Bookie；
- 标准 LedgerMetadata 只拥有 OSS state/membership；独立 sidecar 拥有 Profile instance 与控制事实；
- normal Profile Add 只有在 global READY 与目标 Bookie durable normal activation 都匹配时才能 ACK；
- 每个新 Profile Add 都携带60-byte ledger context并匹配Bookie本地durable activation；wire bytes在真实old decoder Gate通过前仅是executable test manifest；
- 未安装、instance/hash/engine 不匹配全部 fail closed；
- Classic/Profile routing 是单一、原子、可恢复的本地 claim；
- 写期 replacement 按 inactive install → `LAC+1` membership CAS → normal activation → resend 排序。

本文是待接受合同，不是已存在的 wire protocol 或实现说明。

## 2. 动机

当前 Bookie 首次收到 Add 时可根据 `ledgerId + masterKey` 懒创建 handle。Add 请求不携带完整 ProfileDescriptor，Bookie 也不会为该请求读取 Ledger Metadata。因此，只在 metadata 写入 `storage_format` 无法建立 fail-closed 路由。

如果没有显式安装协议，至少会出现以下歧义：

- 部分 Bookie 不知道 ledger 需要 Segment engine；
- 一般 `E > W` 下，后续轮转 write set 可能首次触达未安装节点；
- ensemble replacement 可能先进入 metadata，再在第一次 Add 时失败；
- 相同 ledgerId 的旧 instance 或不同 descriptor 被静默当作 Classic ledger；
- Bookie 重启后失去内存路由信息。

## 3. 范围

本 RFC 负责：

- Profile 分层与命名；
- ProfileDescriptor 的最小语义字段；
- metadata 创建状态机；
- install 请求、durable receipt、幂等和冲突；
- Add 身份校验；
- capability negotiation；
- ensemble replacement 顺序；
- restart、orphan install 和 mixed-version 行为。

本 RFC 不负责：

- Segment 磁盘格式和 allocator，见 [RFC-0003](RFC-0003-segment-storage-allocator.md)；
- range、recovery 和 delete，见 [RFC-0004](RFC-0004-range-recovery-delete.md)；
- Segment Bookie 对 install、activation、fence、recovery Add 和 ACK authority 的消费，见 [RFC-0005](RFC-0005-segment-bookie-state.md)；
- master key 的现有安全模型重设计。

现有 master key 可以继续作为当前 data-plane ledger credential，但不因此获得 Profile control-plane authority。本 RFC 只冻结两类 authority 的分离、secret non-disclosure 与 fail-closed 消费合同；它不把 P0 扩成 password/KDF、PKI 或 KMS 的全面重设计。

### 3.1 威胁模型与热路径边界

本 RFC 锁定以下安全边界：

- MetadataStore 与 activation authority 非 Byzantine；
- coordinator 可以 crash、retry、丢 response 或发生 leader change；
- 任何客户端，包括持有合法 master key 的 stale/legacy client，都不被信任为遵守 `OPEN/READY` 时序；
- Bookie 缺少匹配的 durable activation authority 时必须 fail closed；
- `activationEpoch` 或其他客户端可复制字段本身不是 activation proof。
- descriptor hash 只提供 identity/integrity，不提供 authorization、non-repudiation、secrecy 或 capability proof；
- INSTALL、ACTIVATE、repair grant 与 delete 等 Profile control operation 必须来自 non-anonymous、且对exact operation + ledger instance + target/scope有明确授权的authenticated principal；`AuthDisabledPlugin`/`ANONYMOUS`或仅通过认证但无该scope权限都不满足合同；
- master key、password、verifier、可离线验证派生物以及 bearer/replay capability 不得进入公开 metadata、sidecar、receipt、日志、metric 或 exception。
- AuthN/AuthZ检查必须早于route claim、credential persistence、local allocation和任何durable effect；当前合同使用mTLS principal与小型exact-scope authorizer，具体allowlist/backend配置保持开放，不引入通用RBAC框架。
- 任何携带master key或verifier的Profile control/data transport只走独立TLS1.3+mTLS endpoint；该要求不追溯重设计Classic wire/KDF，也不要求每Add做certificate验证。

同时锁定以下性能边界：

- 普通 Add 不远程读取 MetadataStore；
- 普通 Add 不逐请求执行重型签名/证书验证；
- 热路径只允许与 handle lookup 合并的本地 routing/state lookup，以及 instance/hash/activation identity 比较；
- canonical encode/hash、完整 descriptor parse、control authority read 和 capability vector negotiation 都不进入普通 Add；
- normal Add只允许对已验证、已缓存的固定长度descriptor/auth identity或verifier做bounded comparison；每请求auth-binding hash/HMAC、KMS调用、certificate/signature验证均为0；
- install、activation、fence 和 delete 属于冷控制路径，可以 group commit；本 RFC 不要求每 ledger 单独 fsync。

## 4. Profile 分层

### 4.1 Bookie Engine Profile

```text
CLASSIC_ENGINE
DIRECT_JOURNAL_ENGINE
SEGMENT_WAL_ENGINE
```

Engine Profile 是进程/cohort 级能力。Bookie 启动后公布唯一 active engine 及 capability set。当前合同不支持同一进程按 ledger 动态切换三种 engine。

### 4.2 Ledger Contract Profile

候选合同维度：

```text
payloadFormat       = OPAQUE_LEDGER
durabilityMode      = SYNC_ON_ACK | DEFERRED_SYNC_LEGACY
quorumProfile       = existing E/W/A plus declared restrictions
requiredCapabilities = exact capabilityId + semanticVersion set
```

Ledger Contract Profile 是 immutable descriptor 的一部分。需要 Bookie 解析或执行的 index、sequence、recovery、delete 语义只由 `requiredCapabilities` 表达，不再保存一组重复的 `indexProfile/sequenceProfile/recoveryProfile/deleteProfile` 字段；否则 descriptor 会出现两份可能冲突的真相。

### 4.3 Client/Protocol Profile

协议原生位置、事务和可见性属于客户端/适配层，不进入 Bookie Profile 合同。

## 5. ProfileDescriptor

### 5.1 Canonical codec

当前codec冻结为 JDK/Netty-only、扁平、严格、有序的固定宽度整数 TLV；不使用 ordinary protobuf canonicalization，也不接受 parse 后 normalize 的同义输入。所有整数均为 unsigned big-endian：

```text
DescriptorHeader                   16 bytes
  magic                            4 bytes = 42 4b 50 44 ("BKPD")
  codecVersion                    u16 = 1
  semanticSchemaVersion           u16 = 1
  totalLength                     u32, inclusive
  fieldCount                      u16 = 10
  flags                           u16 = 0

FieldHeader                         8 bytes
  fieldId                         u16
  type                            u16
  valueLength                     u32
```

十个字段必须各出现一次并按 `fieldId` 严格递增：

| fieldId | 字段 | type | value |
| --- | --- | --- | --- |
| 1 | `requiredEngineProfile` | `U16=1` | u16 enum |
| 2 | `payloadFormat` | `U16=1` | u16 enum |
| 3 | `durabilityMode` | `U16=1` | u16 enum |
| 4 | `ensembleSize` | `U16=1` | u16 |
| 5 | `writeQuorumSize` | `U16=1` | u16 |
| 6 | `ackQuorumSize` | `U16=1` | u16 |
| 7 | `permanentLossBudgetF` | `U16=1` | u16 |
| 8 | `failureDomainPolicyId` | `U32=2` | u32 |
| 9 | `failureDomainPolicyGeneration` | `U64=3` | u64 |
| 10 | `mandatoryCapabilities` | `CAPABILITY_SET=4` | `count:u16`，随后 `count` 个严格递增的 `capabilityId:u32 + semanticVersion:u16` |

当前基础枚举冻结为：

```text
Engine:     1 CLASSIC_ENGINE; 2 DIRECT_JOURNAL_ENGINE; 3 SEGMENT_WAL_ENGINE
Payload:    1 OPAQUE_LEDGER
Durability: 1 SYNC_ON_ACK; 2 DEFERRED_SYNC_LEGACY
```

failure-domain policy ID/default/domain semantics、capability ID/version registry、各 production Profile 的 exact capability 组合与完整 cross-field compatibility table 仍由对应 owner RFC 接受，未接受前不得 mint production descriptor。

### 5.2 Strict validation 与 bounds

当前hard bounds：canonical descriptor 的合法长度精确为 `124 + 6 * capabilityCount`，即 124..508 bytes；`fieldCount=10`，capability count 为 0..64，nesting depth 为 1，semantic string/free-form bytes/optional semantic field 均为 0。1024 bytes 只是 parser 在任何 body allocation 前执行的绝对 input/allocation hard cap，不是合法 descriptor 最大长度；509..1024 bytes 同样非法。`CAPABILITY_SET.valueLength == 2 + count * 6`，全部 fixed scalar length 必须精确，`totalLength` 必须等于实际 bytes，且禁止 padding 与 trailing bytes。

validator 必须满足：

- duplicate、out-of-order、missing、unknown field/type/schema/capability、nonzero flags/reserved、wrong length、truncated/oversized、default alias 与 trailing bytes 全部在分配或状态变更前拒绝；input 本身必须 canonical，不能用重新编码后的相等掩盖 alias；
- capability ID 严格递增、非零且不重复，semantic version 非零并表示一个 exact version；
- `1 <= A <= W <= E <= 65535`、`0 <= F < A`，failure-domain policy ID/generation 均非零，且已接受的 cross-field Profile compatibility table 必须通过；
- unknown/new schema 不得由旧 reader/writer 读取已知字段后 strip/rewrite；跨 schema 不推断 semantic equivalence；
- safety-neutral hint 如需加入，只存在于独立外层记录，不进入当前 descriptor 或 identity。

### 5.3 Hash 与 identity

当前identity codec冻结：

```text
hashSuiteId = 1
algorithm   = JDK MessageDigest "SHA-256"
digestSize  = 32 bytes

preimage = US-ASCII "org.apache.bookkeeper/ProfileDescriptor/v1\0"
           || uint16BE(hashSuiteId)
           || uint16BE(semanticSchemaVersion)
           || uint32BE(canonicalBytes.length)
           || canonicalBytes

identity = uint16BE(hashSuiteId)
           || uint16BE(semanticSchemaVersion)
           || digest[32]                    // total 36 bytes
```

canonical bytes 与 declared 36-byte identity 必须一起持久化/传输，consumer 独立重算并 constant-time 比较 identity。hash 只做 identity/integrity，不授权、不保密、不证明 publisher、不替代 MetadataStore CAS；CRC/checksum不能承担 semantic identity，也不引入 descriptor signature、Merkle、PKI、cluster HMAC 或 per-Add hash/proof。

### 5.4 Semantic boundary 与 reference corpus

约束：

- `ledgerInstanceId` 在 ledgerId 重用或重建时必须变化；
- semantic descriptor 只包含 immutable safety semantics：Engine、mandatory semantic capability及其版本、payload format、durability、E/W/A binding、`F`/failure-domain policy、recovery/delete safety semantics，以及真正影响跨实现安全的 limits；它不包含 ledger/instance/store version、当前 membership、operation/request identity、credential/receipt、runtime admission policy、implementation preference、benchmark threshold 或 safety-neutral hint；
- durable install 的语义身份至少绑定 `ledgerId + ledgerInstanceId + descriptorIdentity + protected auth binding`；
- 相同语义的新 `operationId` 可以返回原 install generation，不为任意 operation identity 建立无界 durable idempotency 表；
- 相同 `operationId` 携带冲突的public semantic payload identity或secret必须失败且不改变authority；
- secret 或可离线验证的 credential material 不得进入普通日志、公开 metadata、receipt dump、metric、exception 或诊断输出；receipt 也不得成为 bearer/replay capability；
- 只有确实影响跨实现 payload 解释、durability 或 recovery 正确性的 limit 才能进入 descriptor；`maxInflightEntries/maxInflightBytes` 等通常属于 runtime policy。
- `permanentLossBudgetF` 与 declared failure-domain policy 是 immutable recovery safety contract：normal ACK 必须覆盖至少 `F + 1` 个 distinct declared domains；RFC-0004 只有在 bounded range 完整恢复到同等 coverage 并发布强 completion proof 后才能重置该 range 的 loss window。

同一 ledger instance 的 semantic descriptor 与 protected auth binding 均 immutable；任何变更使用新 ledger instance，当前合同不引入 key-rotation state machine。受保护 local state 的物理 owner/record framing、policy/capability registry 和后续跨instance key/KMS改造仍未关闭；codec/hash本身已不再OPEN。

reference implementation 由 `ProfileDescriptor`、`ProfileDescriptorCodec` 与 `ProfileDescriptorIdentity` 组成，生产 encoder 是唯一 canonical writer，decoder strict-validate input。golden corpus必须保存 authoritative `.bin`、typed fixture、expected SHA-256/36-byte identity与field dump，并由不共享 production parser helper 的 test verifier独立复算；覆盖合法组合和 duplicate/order/missing/unknown/length/trailing/E-W-A-F/policy generation/capability count等全部负向向量。该实现只允许 create/install/open/control 冷路径调用，normal Add只比较缓存的36-byte identity。

项目内部只使用上述无代际后缀名称。`codecVersion=1`、`semanticSchemaVersion=1`与hash domain separator中的`/v1`是当前字节合同的technical discriminator，不是项目代际；后续改造直接修改本RFC和实现，并在不兼容时同步更新discriminator、migration/rollback边界与compatibility corpus，不建立并行代际类型或合同。

### 5.5 Wave 0 reference implementation 状态

截至 2026-08-30，`bookkeeper-common/src/main/java/org/apache/bookkeeper/common/profile/` 已实现上述 immutable model、canonical codec、strict validator 与 identity；test source 中的独立 verifier、fixture-only registries、6 个 valid/40 个 invalid byte corpus、expected identities/field dumps/checksums 和 fixed-seed fuzz receipt 已提交。可复核范围、命令和机器回执见 [`implementation/profile-descriptor/`](implementation/profile-descriptor/README.md)。

该状态只表示 Wave 0 reference evidence 已落地，不改变本 RFC 的 Proposed 状态，不接受 production policy/capability ID 或 legal-combination table，不解锁 endpoint/stable wire/live shadow/Segment ACK authority，也不把本地测试结果冒充 Spike A/B/C 或 production Gate。

## 6. Profile control namespace 与 initial publication

标准 `LedgerMetadata` 继续唯一拥有 OSS `OPEN / IN_RECOVERY / CLOSED` 状态和 ensemble membership。Profile 使用独立、带 CAS 语义的 sidecar namespace 保存 ledger instance 与 Profile 控制事实；sidecar 不复制或重新解释标准 membership。

Profile 占用的 reserved `LedgerMetadata.customMetadata` entry 只保存一个小型 immutable backlink，至少能把 `ledgerId` 绑定到 `ledgerInstanceId/sidecar reference`，防止 ledgerId 删除重建后 metadata version 从头开始产生 ABA。完整 descriptor、receipt、repair/delete history 不得塞入该 reserved entry，也不得累积在一个无界增长的 sidecar root；其他现有 OSS/user custom metadata 不受本合同禁止。RFC-0004 拥有语义的 repair/delete operation 可以使用有界 child record。

LedgerMetadata version、relevant ensemble/fragment digest、instance marker 与 sidecar operation generation 只作为冷控制路径的 publication/CAS evidence。普通 Add 不读取当前 metadata version，也不因不相关 metadata mutation 要求全 E 重新激活。真正 ledger-global 的 lifecycle/READY/`DELETE_INTENT`/delete-fence/delete-terminal fact只共享一个 lifecycle CAS generation/fencing token；repair/loss/receipt等操作使用owning authority-domain predecessor/head，child/domain更新不因root存在而推进ledger-global head，只有实际冲突的domain共享顺序。本 RFC 不引入跨两个 metadata node 的通用事务、全局锁或 Add-time lease。

初始创建的最低顺序固定为：

```text
1. allocate ledgerId + ledgerInstanceId; select planned initial ensemble
2. create-if-absent sidecar PREPARING/reservation
3. durable install PROFILE on all E Bookies, normal-inactive
   - each install atomically claims ABSENT -> PROFILE
4. create-if-absent standard LedgerMetadata
   - exact initial ensemble equals the installed set
   - immutable instance/sidecar backlink is present
5. sidecar CAS publishes READY authorization
   - binds actual LedgerMetadata version + canonical ensemble digest
   - requires verified all-E durable install
6. idempotently ACTIVATE each Bookie from that READY authority
7. only after all E are durably normal-active may create/open return normal success
```

这些名称表达必须区分的事实，不冻结 exact enum/schema：全局 `READY_AUTHORIZED` 先于任何 local normal activation；`ALL_E_ACTIVATED/AVAILABLE` 晚于全部 E 的 durable activation。READY 可以早于部分 Bookie active；已 active target/write set 可能形成合法 local success 或 AQ，未 active target 必须明确 transient unavailable，且 create/open 正常成功仍晚于 all-E activation。是否通过 credential distribution 禁止所有 pre-return write 保持开放；任何情况都不能回退 Classic 或扩大错误 Profile/durability 接受集合。

安全合同固定为：

```text
ACK(normal profiled Add)
    => matching global READY authorization existed before Add processing
    && matching local durable normal ACTIVE existed before Add processing
```

### 6.1 Sidecar reservation 与 authority boundary

sidecar 需要一个 domain-specific `ProfileControlStore` 语义 adapter。portable contract 只依赖单 record create/read/versioned CAS、bounded page enumeration 和显式 publication ordering；现有 `LedgerManager` 不提供通用 child namespace/multi-key transaction，底层 ZK multi-op 或 etcd transaction 可以作为 backend 优化，但不能成为跨 driver 的 safety 前提。

每个 authority record 至少语义绑定：

```text
record family / semantic kind
format version + mandatory feature set
ledgerId + ledgerInstanceId
authority-domain identity
semantic generation / expected predecessor
operation identity + generation       # externally retried mutation only
payload/content identity
snapshot/superseded-by identity where applicable
```

MetadataStore opaque store version 只负责单 key CAS；semantic/control generation 负责 lineage、ABA 与 response-loss 解析。两者不能互相替代，authority mutation/delete 禁止使用无条件 `Version.ANY`。immutable snapshot chunk 不要求各自维护 operation id，可以由 snapshot identity、ordinal 和 content digest 唯一标识。

语义接口至少区分 `FOUND/ABSENT/INCOMPATIBLE_VERSION/CORRUPT` read，以及 `APPLIED/ALREADY_APPLIED/CONDITION_FAILED/CONFLICT/INCOMPATIBLE_VERSION` create/CAS 结果。exact Java API、类型名和 backend adapter 保持开放；该 wrapper 不向调用方暴露任意跨 key transaction。

每个可外部重试的 operation identity不可变地绑定一个semantic payload/content identity。同identity重试相同payload时，若原transition已提交，必须返回等价`APPLIED/ALREADY_APPLIED`；同identity携带冲突payload必须返回`CONFLICT`，永远不能返回`APPLIED/ALREADY_APPLIED`或改变authority。current snapshot/terminal summary可以作为已吸收operation的有界证明；identity退出可证明retention后只能返回stale/conflict，不能为满足极晚retry保存无界history。

root 必须有 manifest-locked hard bound，只保存 instance/descriptor/lifecycle summary、global lifecycle fence/control generation、有限 authority-family directory/head、current snapshot identity/cut 和 bounded suffix/page references。每个 child family/domain 声明 owner、semantic predecessor 与 bounded discovery；不同 domain 只在实际冲突时共享 order，已证明不相交的 repair range 不进入 ledger-global universal head。

RepairIntent admission是与delete fence实际冲突的例外：先durable-create绑定exact instance/target/source-range/operation generation的immutable inert child，再以single-record conditional lifecycle/delete-fence head CAS发布admission reference。只有admitted intent可授予recovery grant或接收第一份durable payload；`DELETE_INTENT`先赢后任何新admission失败，并冻结cut前全部admitted intent的source/target。admission后的progress/loss/receipt/completion绑定该intent generation并校验lifecycle/delete-fence generation，但只推进owning authority-domain head，不为每次更新推进ledger-global head。未admit child始终inert并按orphan proof回收；该顺序不要求跨key transaction，exact directory/head layout与batching保持开放。

PREPARING/reservation 至少绑定 `ledgerId + ledgerInstanceId + descriptorIdentity[36] + operationId[16] + publicSemanticPayloadIdentity[32] + planned initial ensemble`。它本身不授权 normal Add，也不是第二份 ensemble truth。同一 operation 重试必须解析到相同 public semantic payload、instance 与 descriptor identity；冲突请求失败。

Profiled ledger 的标准 metadata mutation 只能由 Profile-aware coordinator 或等价 ACL/fencing authority 执行；持有 master key 本身不授予绕过 Profile lifecycle 修改 LedgerMetadata 的权限。exact credential/ACL 机制保持开放，但不能假设 sidecar 能约束一个拥有不受限 metadata 写权限的 legacy client。

### 6.2 All-E inactive install

标准 LedgerMetadata 创建前，协调方必须对 planned initial ensemble 的全部 E 个 Bookie 执行 `INSTALL_LEDGER_PROFILE`，同时 durable claim `PROFILE` route，但保持 normal-inactive。不能只安装当前 write set 的 W 个节点，也不能先暴露标准 OPEN metadata 后再补 route claim。

部分成功或 response loss 时，只重试相同 install operation；未形成全部 E durable receipts 前不能创建标准 LedgerMetadata。receipt 至少绑定：

```text
bookieId
bookieStorageIncarnation
ledgerId
ledgerInstanceId
descriptorIdentity[36]
operationId[16]
publicSemanticPayloadIdentity[32]
bookieEngineProfile
bookieCapabilityDigest
localInstallGeneration
```

receipt 的保存、压缩和审计布局保持开放；root record 不得保存无界 receipt history。receipt 只证明目标本地 durable result，不替代 READY/membership authority，也不得携带任何 secret、offline verifier 或 replay capability。

### 6.3 Standard metadata 与 READY publication

标准 LedgerMetadata create-if-absent 必须携带 immutable instance backlink，且 initial ensemble 精确匹配已安装集合。之后 sidecar 以 CAS 发布 READY authorization，并绑定实际 metadata version、canonical ensemble digest、instance 与 control generation。任一 CAS response loss 都必须重读两份 authority，按 operation identity/version/digest 判断已提交、可重试或冲突；不得盲建第二个 instance。

sidecar CAS response loss 时，重读 exact domain head：operation/snapshot identity匹配返回等价 `ALREADY_APPLIED`；若已被 current snapshot/terminal generation吸收，可返回等价完成；否则返回 stale/conflict并重建 lineage。不得为了永久回答所有 old request 保存无界 idempotency history。

上述重读同时比较semantic payload/content identity：same operation + same payload只解析为同一既存结果；same operation + conflicting payload只能`CONFLICT`，不能借response loss或snapshot吸收伪装成幂等成功。

Sidecar reservation 单独存在不授权 Add；标准 membership 单独存在不激活 Bookie；只有 post-publication authority 才允许 Bookie durable normal ACTIVE。所有中间态必须 inert 或可恢复，不能扩大接受集合。

### 6.4 Local activation 与 availability completion

ACTIVATE 是幂等冷路径操作。Bookie 只有在 READY authority 匹配本地 instance/hash、且 durable fence/tombstone 未先发生时才能 durable normal-active；迟到 activation 不能重新打开 fenced/deleted route。watch/cache 只能提前触发 activation或优化失败响应，不是正确性依赖。

create/open 正常成功必须晚于全部 E durable activation。availability completion 可以是有界 completion fact，或由 E 个 local state 的有界重查证明；exact state name、receipt packing、proof/certificate 与 partial-activation credential distribution 保持开放。普通 Add 仍只执行有界本地 lookup，不增加 MetadataStore I/O 或逐请求重型验证。

### 6.5 Child publication、snapshot 与 unknown version

child/page 在 owning family head/root CAS 引用前只是 inert orphan，不授权 activation、recovery、delete 或 loss reset。可 compact domain 的 authority 固定为：

```text
current committed snapshot through cut S
+ complete bounded suffix after S
```

最低 publication/compaction 顺序：

```text
1. choose stable authority-domain cut S
2. persist and verify immutable chunks/pages
3. create bounded manifest binding identity, chunks, cut S and content identity
4. CAS owning domain head/root with the same expected predecessor and suffix anchor
5. retain prior snapshot and required suffix as response-loss/corruption fallback
6. only after no supported root/fallback references old data, reclaim covered children/pages
```

head 在 build 期间推进时，publication 必须 CAS 失败重试，或 manifest 明确从 cut `S` 接完整 suffix；不能发布 best-effort snapshot。referenced child缺失、page/suffix gap、corrupt content 或 unknown mandatory version使该 authority domain fail closed，不能解释为 `ABSENT/default`，旧 writer也不得覆盖。未被任何 authority引用的 unknown orphan只有在 generation fence、fallback/retention和GC proof完整后才可处理。

### 6.6 Control authority 与 protected local binding

Profile control plane 与现有 data credential 是两套权限：master key/password verifier 只参与 ledger data access，不能授权 INSTALL、READY publication、ACTIVATE、repair grant、tombstone 或 delete。

当前合同冻结一个独立 `bookie-profile` TCP listener：连接第一字节即 TLS 1.3，不复用 legacy `bookie-rpc` port 或 legacy START_TLS；server authentication 与 client certificate 均 required，principal取 leaf X509 `X500Principal.CANONICAL`。实现复用现有 BookKeeper key/trust/provider配置，但需要 `startTls(false)` 与 `ClientAuth.REQUIRE` 的Profile context。证书验证只在连接建立；Classic endpoint、resolver、pool与handshake不变。正式部署 manifest 必须验证受信 CA 集合内 subject 唯一，或在配置层把 trust-domain/issuer identity 纳入 principal mapping；具体 allowlist/trust rotation 仍 OPEN，不引入 SPKI pinning或每请求证书检查。现有 `AuthDisabledPlugin`、SASL-without-consumable-principal或 `BookieAuthZFactory` 的coarse OU allowlist都不能单独满足Profile control AuthZ。

当前合同只增加domain-specific authorizer，不建设通用RBAC：

```text
ProfileControlAuthorizer.authorize(
    ProfilePrincipal principal,
    ProfileControlScope scope,
    CommittedAuthority authority
) -> ALLOW | DENY

ProfileControlScope = operation/purpose
                    + ledgerId + ledgerInstanceId[16]
                    + descriptorIdentity[36]
                    + targetBookie + targetStorageIncarnation[16]
                    + authorityDomain + authorityGeneration
                    + operationId[16] + publicSemanticPayloadIdentity[32]
                    + optional exact range/fragment scope
```

operation class固定为 `INSTALL`、`ACTIVATE_INITIAL`、`ACTIVATE_REPLACEMENT`、`RECOVERY_GRANT`、`RECOVERY_GRANT_CLOSE`、`TOMBSTONE_OR_DELETE_APPLY` 与 `STATUS_QUERY`。mTLS principal的静态operation权限与committed authority的exact ledger/instance/target/purpose/generation/scope约束必须同时通过；“controller role可以任意INSTALL”不成立。具体allowlist配置、role命名和backend路径保持OPEN。

Bookie以request的ledger/instance/purpose派生domain key，通过`ProfileControlStore.read(AuthorityKey)` direct-read committed record；caller不得指定任意MetadataStore path。strict parser验证store version、semantic generation、instance、descriptor、target/incarnation、operation/purpose；unavailable返回transient/reconciling，unknown/gap/corrupt返回quarantine/fail-closed。watch/cache只做加速。INSTALL/ACTIVATE每个operation至多一次cold read，normal Add为零。READY由coordinator通过MetadataStore ACL/CAS发布，不是Bookie RPC subtype。

冷路径 authority/reference 按 purpose domain-separate。INSTALL 的顺序固定为 TLS/mTLS → bounded parse → non-anonymous与static operation precheck → 派生fixed authority key → direct-read PREPARING/descriptor committed authority → `authorize(principal, scope, committedAuthority)` exact AuthZ → 校验 ledger/instance/schema/descriptor identity/Engine/capability/target/incarnation/op identity/generation/public payload → 校验credential kind/length → 一个conditional local transition写入route/instance/descriptor/Engine/protected credential/install generation/normal-inactive → local durability barrier → secret-free response。最终 exact AuthZ 仍早于route claim、credential persistence、allocation和任何durable effect；每个INSTALL/ACTIVATE operation至多一次cold authority read，不增加通用RBAC或额外MetadataStore round trip。ACTIVATE_INITIAL读取exact READY和initial metadata/ensemble；ACTIVATE_REPLACEMENT读取post-membership authority；两者不能互相重放，且fence/tombstone先赢时迟到activation失败。

当前data credential逻辑表示固定为 `credentialKind:u16=1 (BK_MASTER_KEY_SHA1) + credentialLength:u16=20 + credentialBytes[20]`。它与ledger/instance/descriptor identity/install generation/route/activation/fence state一起进入`ProtectedProfileStateStore`语义接口；normal Add使用`MessageDigest.isEqual()`或等价constant-time comparison。不得生成或公开unkeyed `authBindingHash`，同instance credential immutable，物理owner、record packing、at-rest protection、group commit与secure deletion继续BLOCK。

每个 `operationId[16]` 只绑定一个不含credential bytes的public semantic payload identity：same operation/same public payload/same secret返回原结果或`ALREADY_APPLIED`；任一public payload或secret冲突返回`CONFLICT`且不泄漏差异。status只返回`NOT_FOUND/APPLIED/ALREADY_APPLIED/CONFLICT/DURABILITY_UNKNOWN/STALE_OR_COMPACTED`。response loss只能在相同endpoint/subtype/operationId/payload下查询或重试，不能回退Classic、双写或盲建generation。

receipt/status/log/metric/exception只能公开ledger/instance/descriptor、target/incarnation、Engine/capability、local generation、operation identity与durable result；不得包含master key/password/verifier、credential digest、bearer/replay capability。secret wrapper的`toString()`固定为`<redacted>`，structured logger禁止raw credential，临时heap/ByteBuf尽力清零，并对`LedgerDescriptorImpl.checkAccess()`的完整master-key日志路径建立hard regression test。

### 6.7 Wave 0 typed reference endpoint状态

Wave 0 已在 `bookkeeper-common` internal unstable package实现typed principal/transport/scope、static与exact authorizer、fixed derived authority key、single cold-read store、committed authority、protected credential/state-store semantic interfaces，以及只消费transport facts的隔离`bookie-profile` reference endpoint。12项普通功能测试覆盖顺序、zero-effect denial、exact tuple/purpose/state、strict descriptor/Engine、durable-only success、secret-free status/redaction与dependency fail-closed。

该实现没有socket listener、TLS termination、concrete MetadataStore/local-store adapter、production route/registration/Add/ACK接入或physical control bytes，不能证明real TLS/auth/secret-leak matrix、A24或stable wire，也不解锁G1和任何production authority。normal Add调用点仍为0，因而没有remote MetadataStore、per-request descriptor/auth hash、KMS、certificate/signature或control fsync增量。

## 7. Bookie 安装语义

候选请求：

```text
INSTALL_LEDGER_PROFILE {
    ledgerId
    ledgerInstanceId[16]
    descriptor
    descriptorIdentity[36]
    operationId[16]
    publicSemanticPayloadIdentity[32]
    credentialKind:u16 = 1
    credentialLength:u16 = 20
    protectedCredentialBytes[20]
    expectedBookieEngineProfile
}
```

这是 logical request contract；control tail 的最终 physical wire packing 仍 BLOCK。当前合同不接受 bearer install proof。

Bookie 必须按顺序完成：

1. 在分配前验证请求 hard bounds、canonical schema/descriptor identity、unknown mandatory semantics，并执行caller non-anonymous与static INSTALL operation precheck；
2. 由ledger/instance/purpose派生fixed authority key，cold-read并验证exact committed PREPARING/descriptor authority，绑定target Bookie stable identity/storage incarnation与operation identity/generation；
3. 调用exact `authorize(principal, scope, committedAuthority)`，拒绝路径不得产生route、credential、allocation或durable effect；
4. 验证public semantic payload、credential kind/length、active Engine Profile和全部mandatory capability；
5. 对 ledgerId 的 authoritative route slot 执行单一原子 claim，并检查instance、descriptorIdentity、purpose/generation与protected credential binding冲突；
6. durable 写入 route、Profile reservation/install、受保护认证状态、install generation与normal-inactive；
7. 建立可从 durable authority 恢复的 routing entry；
8. 返回 secret-free durable receipt。

仅在第 5-7 步的 durability barrier 完成后返回成功。内存对象创建不能替代 durable install。exact record packing 和是否合并为一次 group commit 由实现与 Spike 决定，不要求“一种语义一条记录”。

### 7.1 Authoritative route claim

每个 ledgerId 在一个 Bookie 上只有一个 authoritative route slot：

```text
ABSENT
CLASSIC
PROFILE(ledgerInstanceId, descriptorIdentity[36], lifecycleState)
TOMBSTONED(ledgerInstanceId)
```

Classic lazy-create 与 Profile install 必须通过同一个原子 claim 竞争：

```text
legacy Add:
    CLASSIC -> existing Classic path
    ABSENT -> atomically claim CLASSIC, then lazy-create
    PROFILE/TOMBSTONED -> reject before Classic handle/storage creation

profile install:
    ABSENT -> atomically and durably claim PROFILE/RESERVED
    CLASSIC -> conflict
    matching PROFILE -> idempotent continuation
    conflicting PROFILE/TOMBSTONED -> reject
```

normal Add 与 legacy `RECOVERY_ADD` 变体都必须经过相同 routing gate。restart 必须在注册 writable 前恢复 authoritative route。legacy 抢先 claim 成功时，后续 Profile install 只能冲突失败，不能升级或混用。

### 7.2 幂等与冲突

| 已有状态 | 请求 | 结果 |
| --- | --- | --- |
| `ABSENT` | 合法 Profile operation | durable claim + install |
| `CLASSIC` | Profile install | 冲突；不得升级 |
| 相同 instance/descriptorIdentity/operationId/public payload/secret | 重试 | 返回相同语义 receipt 或 `ALREADY_APPLIED` |
| 相同 instance/descriptorIdentity，新 operationId + 相同 semantic payload/secret | 等价重试 | 允许，receipt 绑定原 install generation |
| 相同 operationId，冲突 public payload identity 或 secret | 冲突 | `CONFLICT`且不改变authority |
| 相同 instance，不同 descriptorIdentity | 冲突 | `EPROFILE_MISMATCH` |
| 相同 ledgerId，不同 live instance | 冲突 | `ELEDGERINSTANCE_MISMATCH` |
| engine 不匹配 | 冲突 | `EUNSUPPORTED_STORAGE_ENGINE` |
| mandatory capability 缺失 | 冲突 | `EUNSUPPORTED_LEDGER_CAPABILITY` |

冲突不能通过删除本地状态或退回 Classic 自动修复。

## 8. AddRequest 身份

所有需要安装的新 Profile normal Add 必须使用 distinct Profile logical operation，而不是现有 legacy `ADD_ENTRY` 的 optional 字段。Round 7 executable wire manifest固定common context：

```text
LedgerContext                      60 bytes
  ledgerId                        i64
  ledgerInstanceId                16 opaque nonzero bytes
  descriptorIdentity              36 bytes

ADD_NORMAL:
  LedgerContext
  entryId                         i64
  writeFlags                      u32
  credentialKind                  u16 = 1
  credentialLength                u16 = 20
  credential                      20 secret bytes
  entryLength                     u32
  entryPayload                    entryLength bytes

ADD_RECOVERY:
  LedgerContext
  repairIntentId                  16 bytes
  repairIntentGeneration          u64
  grantGeneration                 u64
  rangeStart/rangeEnd/entryId     i64 each
  credentialKind/length/credential
  entryLength + entryPayload
```

target Bookie/storage incarnation来自已验证的HELLO connection context并由local authority复核，不在每Add重复；服务端还必须验证现有BookKeeper entry payload内部ledger/entry coordinate与envelope一致。range必须包含entryId，normal/recovery不得互换。完整control tail、batch/range encoding与general E/W/A recovery outcome API仍BLOCK，因此本节字节布局只能用于reference codec/corpus，在raw old-decoder Gate通过前不是stable wire承诺。

Bookie 校验顺序的语义要求：

```text
authoritative route is CLASSIC/TOMBSTONED/conflicting -> fail closed
install missing                                        -> reject
ledger instance mismatch                               -> reject
descriptor hash mismatch                               -> reject
normal request cannot match durable normal-active role -> reject
engine mismatch                                        -> reject
then existing fencing/auth/entry validation
```

新 Profile 请求缺失上述identity字段时必须拒绝。normal Add 必须匹配durable normal-active role；recovery Add必须匹配exact instance/descriptor/credential、已admit RepairIntent、bounded local grant、target/incarnation/range/grant generation、delete/tombstone与payload identity；legacy recovery flag不足以取得该权限，也不能借recovery grant获得normal writable。

普通 Add 不携带完整descriptor、Engine/capability vector、READY proof、target BookieId或certificate，不做canonical hash、auth-binding hash/HMAC、MetadataStore read、KMS/signature/certificate verification或control fsync；只解析固定header/context，constant-time比较缓存的36-byte descriptor identity与20-byte verifier，再执行local route/active/fence/admission-generation与payload检查。Classic请求仅在route为`ABSENT/CLASSIC`时走现有路径，命中`PROFILE/TOMBSTONED`时必须在创建Classic handle、persist master key或payload前拒绝。

## 9. Ensemble change

Active write-time replacement 与 AutoRecovery 是两条不同流程，不能用同一个“copy then CAS”序列概括。

### 9.1 Active write-time replacement

写期换组的最低顺序固定为：

```text
1. select Bookie satisfying engine and capabilities
2. durable install replacement, normal-inactive
3. CAS standard LedgerMetadata at the existing LAC+1 fragment authority
4. publish/verify post-CAS membership activation authority
5. replacement becomes durable normal-active
6. only then resend affected pending Adds
```

该路径不复制整个历史 fragment。CAS 到 activation 之间只能暂停写入或返回明确 transient failure；不能向 replacement resend、降级 Classic 或执行 per-entry metadata operation。install/activation 属于 `MetadataUpdateLoop.transform` 外的显式异步冷路径 phase，避免 CAS conflict 重放 transform 时重复外部副作用。

并发和 response-loss 合同：

- 继续继承 BookKeeper single-writer/fence；同一 handle replacement 串行，不新增多 writer consensus；
- 标准 LedgerMetadata CAS winner 是唯一 membership winner；
- CAS response loss 后重读 exact fragment start、old ensemble identity、replacement mapping、instance marker 与 operation generation；匹配才继续 activation，不匹配则 target 保持 inactive/orphan；
- activation response loss 只重试/查询同一 operation，不因 timeout 盲选第二个 target；
- ledger 已 `IN_RECOVERY/CLOSED` 时停止 normal activation/resend；
- durable fence/tombstone 先发生时迟到 activation 失败；activation 先发生时后续 durable fence 关闭 normal Add；
- sidecar post-CAS authority 只绑定 instance/profile、committed metadata version、relevant fragment start、new ensemble digest 和 activation generation，不复制 pending Add 或完整 metadata。

如果 install 失败：

- replacement 不进入 ensemble；
- 选择其他满足条件的节点或使操作失败；
- 不降低 capability；
- 不把首次 Add 当作安装触发器。

CAS 前的 inactive replacement 不接收 payload，因此不强制为每次写期换组建立通用 repair transaction。若不记录 replacement-attempt，GC 必须从 sidecar/standard metadata、instance/hash、stable grace 与 durable tombstone 证明它从未 active、不会被迟到 response 激活，才能回收。

### 9.2 AutoRecovery repair

AutoRecovery 的 target 在接收第一份 durable payload 前，必须已有 RFC-0004 的durable、已完成lifecycle/delete-fence admission的instance-specific RepairIntent，并获得仅绑定该admitted intent的recovery-only authority。copy 完成并 CAS standard ensemble 后，closed/historical fragment target 通常转换为 `COMMITTED_REPLICA/READABLE`，关闭 recovery-only authority，但不自动成为 normal-active。

只有 target 同时成为当前 writable fragment member，并重新满足本节 9.1 的 post-CAS membership、fence 和 normal activation合同时，才可独立进入 normal-active。实际 surviving reader source 可以动态选择，不进入持久 authority；repair target、被替换 member、fragment identity 和 operation generation 必须可被 delete freeze 枚举。

## 10. Restart 与 orphan install

Bookie restart 必须从 durable control record 恢复 install、instance、hash、master key 状态和 routing，再注册为可接收相应 Profile 的节点。

sidecar reservation、标准 metadata create、READY publication 或 replacement CAS 失败都可能留下 orphan install。处理最低要求：

- orphan install 不允许接收 normal Add，因为不存在匹配 READY/membership activation；
- install 状态不得仅凭超时删除；
- GC 必须读取 sidecar 与标准 membership authority，并使用 instance/hash、operation generation 与稳定 grace/window 证明该 install 永远不会发布或被迟到 response 激活；
- GC 还必须证明 current/fallback root、snapshot、suffix或operation lineage均不再引用候选 child/page，且 delete discovery history 已由另一 durable summary 接管；timeout 单独不够；
- GC 本身需要 durable tombstone，防止 response loss 后旧请求重新激活。

orphan GC 的完整状态机是本 RFC 接受前的开放项，也是 Spike A 的必测场景。

## 11. Mixed-version 兼容

### 11.1 新 client + 旧 Bookie

任何需要 Bookie 执行 Profile safety semantics 的 create/open/install在发现旧 Bookie、缺少 mandatory handshake/capability或mixed ensemble时必须在payload前失败；不得把Profile请求改写成Classic、重试legacy opcode或双写。

### 11.2 新 Profile + 不支持 install 的 Bookie

placement 阶段排除；若仍被选中则创建失败。不得静默降级。

### 11.3 旧 client + 新 Bookie

旧 Classic 请求在 `ABSENT/CLASSIC` route 上维持现有行为。命中 `PROFILE/TOMBSTONED` route 时，normal 与 recovery Add 都必须在 Classic lazy-create 前 fail closed。具体 deleted/profile-required/fenced error code 由 wire 评审冻结。

### 11.4 Profile wire discriminator 与 downgrade boundary

Round 7 的原始 candidate `0x0FFE4250` 被真实 Apache BookKeeper 4.14.8 decoder 的 frozen `magic-flip-1` 反例否证：第二个magic byte XOR `0xff` 后成为pre-v3 `ADDENTRY` opcode `0x01`。owner RFC 因此只替换 experimental discriminator 为 `0x0FF04250`；原始反例永久保留，其他header、subtype、HELLO、status、bound与zero-effect Oracle不变。replacement仍是**仅供reference implementation与raw corpus的executable manifest**：独立`bookie-profile` endpoint在TLS解密后使用4-byte unsigned big-endian outer length，随后为固定32-byte header：

```text
outerLength                         u32 BE, excludes prefix, equals 32 + bodyLength
ProfileFrameHeader                  32 bytes
  magic                             u32 = 0x0FF04250
  protocolMajor                     u16 = 1
  protocolMinor                     u16 = 0
  headerLength                      u16 = 32
  subtype                           u16
  flags                             u32
  requestId                         u64
  bodyLength                        u32
  reserved                          u32 = 0
```

flags只允许`RESPONSE=0x1`、`ERROR=0x2`、`END_OF_STREAM=0x4`；unknown major/minor/header length/flag/reserved拒绝并关闭连接。TLS已经提供传输完整性，header不增加CRC。

subtype分配：

```text
0x0001 HELLO
0x0101 INSTALL                 0x0102 ACTIVATE_INITIAL
0x0103 ACTIVATE_REPLACEMENT    0x0104 RECOVERY_GRANT
0x0105 RECOVERY_GRANT_CLOSE    0x0106 TOMBSTONE_OR_DELETE_APPLY
0x0107 OPERATION_STATUS
0x0201 ADD_NORMAL              0x0202 ADD_RECOVERY
0x0203 READ_ENTRY              0x0204 FENCE_LEDGER
0x0205 READ_LAC                0x0206 WRITE_LAC
0x0207 FORCE_LEDGER            0x0208 LIST_ENTRIES
0x0301 RANGE_READ              reserved/disabled in current manifest
0x0302 BATCH_RECOVERY_ADD      reserved/disabled in current manifest
```

response复用相同subtype并置`RESPONSE`；READY不是Bookie subtype。pre-HELLO frame最大4096 bytes，control frame最大65536 bytes，全部Profile frame最大5242880 bytes，descriptor input/allocation绝对hard cap为1024 bytes，而当前合法长度仍只能是`124 + 6 * capabilityCount`（最大508）；length必须在body allocation前验证。range/batch在当前manifest中不advertise capability、不接受body并返回UNSUPPORTED，不能凭空冻结batch参数。

magic的位级候选意图是让current v3 protobuf先遇invalid wire type，pre-v3再看到unknown opcode `0xfe`；这不是证据。Spike A必须把TLS ClientHello、全部合法subtype、bit flip、1..31-byte truncation、length/version/flag/subtype变体、current v3 `RuntimeException`变体、version=0/nonzero legacy prefix及fuzz corpus投喂每个受支持真实old decoder/binary，证明route claim、handle/master-key persistence、allocation、payload/journal write与ACK全部为0。任一失败只允许调整magic/framing后重跑，不允许fallback或双写；Gate PASS前本manifest不得晋升stable production wire。当前 Wave 0 将该真实binary矩阵标记为`DEFERRED_NOT_RUN`并排除出常规实现/CI；这不删除Gate或降低Oracle，G1保持`BLOCKED_UNVERIFIED`，未来只有在单独显式授权并产生fresh run identity与完整证据后才能重新判定。

### 11.5 Mixed/rolling matrix 与 semantic errors

最低matrix固定为：old client+old Bookie的Classic不变；old client+new Bookie仅在`ABSENT/CLASSIC`接受；new Profile client+old Bookie拒绝且不降级；Profile create/open遇mixed ensemble失败；Bookie restart/incarnation或protocol generation变化后重新handshake；unknown Profile version/subtype拒绝且无effect；任何Profile bytes在旧Bookie上都不形成legacy Add。registration/capability hint、connection handshake和durable install receipt是三层证据，任一层不能替代下一层。

response body前缀冻结为`statusClass:u16 + retryDisposition:u8 + durableResult:u8 + detailCode:u16 + reserved:u16=0`。status class共有12类（1个OK + 11个non-OK）：`0 OK`、`1 UNSUPPORTED_PROTOCOL_OPCODE_CAPABILITY_ENGINE`、`2 PROFILE_IDENTITY_CONFLICT`、`3 PROFILE_NOT_READY_OR_STALE`、`4 FENCED`、`5 TOMBSTONED_OR_DELETED`、`6 RECOVERY_GRANT_INVALID`、`7 TRANSIENT_UNAVAILABLE`、`8 DURABILITY_RESULT_UNKNOWN`、`9 QUARANTINED_OR_UNKNOWN_MANDATORY`、`10 UNAUTHORIZED`、`11 BAD_REQUEST`。其余枚举值固定为：

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

Profile client/admin必须保留完整三元组；legacy callback只能安全投影为non-OK，不能把partial/unknown变成OK。conflict/grant/quarantine/durability-unknown不得从coordinator/admin rich result丢失；exact新BKException、general E/W/A recovery outcome API与detail code继续BLOCK。protocol/header/length/unknown subtype错误关闭连接；control response loss只重试/查询same endpoint/subtype/opId/public payload；Add response loss只重试same Profile subtype/instance/entry/payload；durability unknown不得换target或Classic。

## 12. Capability negotiation

HELLO是每个Profile connection的第一条application frame且最大4KiB。client body固定为：

```text
minMajor/maxMajor/minMinor/maxMinor u16 = 1/1/0/0
capabilityCount                     u16 <= 64
reserved                            u16 = 0
capabilities                        count * (capabilityId:u32 + semanticVersion:u16 + flags:u16=0)
```

server HELLO response body的字段宽度与顺序固定为：

```text
selectedMajor                       u16 = 1
selectedMinor                       u16 = 0
activeEngine                        u16
reserved                            u16 = 0
bookieIdLength                      u16 <= 255
capabilityCount                     u16 <= 64
storageIncarnation                  16 opaque bytes, nonzero
readinessGeneration                 u64
bookieId                            bookieIdLength bytes, UTF-8 without NUL
capabilities                        count * (capabilityId:u32 + semanticVersion:u16 + flags:u16=0)
```

capability entries严格递增、非零且不重复。client必须将BookieId/incarnation/readiness与placement/registration上下文匹配；任何nonzero reserved/flags、错误长度、无效UTF-8/NUL或乱序/重复capability均拒绝。

registration只做hint；connection context也不替代durable install/activation receipt。Profile physical channel key至少含endpoint protocol、BookieId、storage incarnation、Profile protocol generation与TLS/auth identity。Profile pool与Classic pool物理分离；restart/incarnation/generation变化后重新HELLO；Classic连接完全不执行HELLO，normal Add不重复BookieId、target、Engine、capability vector或certificate。

## 13. 安全不变量

1. Profile create/open 正常成功意味着 initial ensemble 的全部 E 个 Bookie 已 durable install 且 normal-active。
2. `ACK(normal profiled Add) => matching global READY authorization && matching local durable normal ACTIVE existed before Add processing`。
3. Classic/Profile/Tombstoned route 对同一 ledgerId 是单一、原子、可恢复的 claim。
4. legacy normal/recovery Add 不能绕过 `PROFILE/TOMBSTONED` route 进入 Classic lazy-create。
5. 相同 ledgerId 的两个 live instance 不得共享同一 Bookie routing identity。
6. descriptor 或 protected auth binding 冲突不能被重试、restart 或 lazy-create 消解为成功。
7. 标准 LedgerMetadata 是唯一 membership authority；sidecar intent或 membership CAS 单独存在都不能激活新 Bookie。
8. 写期 replacement 必须按 inactive install → `LAC+1` membership CAS → normal activation → resend 排序，且不复制历史 fragment。
9. AutoRecovery recovery-only authority不授予 normal writable；任何 target durable recovery payload 都晚于可枚举 repair intent。
10. Bookie restart 后的接受集合不能大于 crash 前由 durable install/activation 授权的集合。
11. metadata watch/cache 失效不能破坏上述不变量。
12. 普通 Add 热路径不依赖远程 MetadataStore I/O 或逐请求重型 proof 验证。
13. failure-domain policy generation 或 domain identity 不能在 repair proof 中被重新标注以伪造 `F + 1` coverage。
14. RFC-0004 的 accepted-loss generation 只排序 loss/reset authority，不能按 generation delta 计算物理损失数量；相同 domain/incarnation 的 duplicate declaration 不得重复消费预算。
15. store version 与 semantic generation 分离；无条件 `Version.ANY` 不能更新或删除 authority。
16. child/page 只有被 exact instance/domain head 条件化发布后才拥有 authority；snapshot publish先于covered-child reclaim。
17. ledgerId reuse、store-version重新计数或旧 operation retry 都不能跨 `ledgerInstanceId + semantic generation` 形成 ABA。
18. referenced unknown/newer mandatory record、missing chunk或suffix gap必须使对应 domain fail closed；watch/cache只做优化。
19. sidecar operation identity只绑定一个semantic payload；冲突payload重试不能成功、不能返回`ALREADY_APPLIED`，也不能改变authority。
20. RepairIntent只有在lifecycle/delete-fence cut完成admission后才可授予grant/接收payload；delete cut后的admission失败，admission后的domain progress不推进universal ledger head。
21. canonical descriptor identity只由schema/hash suite/canonical semantic bytes派生；hash不授权任何control/data operation，optional hint不改变identity或接受集合。
22. descriptor semantic identity使用固定长度、面向对抗输入的collision-resistant hash；CRC/checksum不能替代。
23. master key/verifier与Profile control authority分离；Profile control caller必须non-anonymous且获授权执行exact operation/instance/target scope，AuthN-only、anonymous或任何secret/offline verifier泄漏均不满足合同。
24. Profile normal/recovery使用distinct logical operation；Profile framing在任何受支持旧decoder上都不能形成legacy Add/route/payload/ACK。
25. Profile错误、response loss或unknown version后client不得Classic downgrade或双写；Profile capability negotiation只约束准备发送Profile operation的Profile-capable connection，不成为per-Add lease，也不强制legacy Classic connection执行新handshake。
26. 当前descriptor input必须是严格canonical TLV，identity固定为suite/schema加32-byte SHA-256；normal Add不解析或hash descriptor。
27. Profile只通过独立immediate-TLS/mTLS endpoint承载；legacy endpoint永远不解析Profile application frame，stable wire受raw old-decoder Gate阻塞。
28. same operation identity只绑定一个public payload与一个protected secret；冲突不得回显secret差异，公开面不得出现credential或offline verifier。
29. Profile connection HELLO context、persistent readiness、ephemeral registration与local durable receipt分层；任一层不能替代下一层。

## 14. Spike 与接受 Gate

RFC 进入 Accepted 前必须：

- [Spike A](spikes/SPIKE-A-profile-install.md) 全场景通过；
- Model A 包含创建、安装、response loss 与 ensemble replacement 的抽象；
- strict TLV descriptor reference codec与独立golden verifier按§5的exact bytes/bounds/SHA-256 manifest通过；
- golden corpus覆盖same-semantics稳定bytes/hash、different-semantics identity变化、duplicate/default/order、unknown schema/field/enum/mandatory capability、oversize-before-allocation、old writer rewrite/strip、declared hash mismatch，以及capability 0/1/64与E/W/A/F边界；
- protected auth binding 经过安全评审且不会泄漏可离线验证的 credential material；
- non-anonymous且exact operation/instance/target scope authorized control principal、Bookie direct-read committed authority、initial/replacement purpose separation、target/incarnation binding、secret-free receipt、response-loss status query，以及authenticated-but-unauthorized/`AuthDisabledPlugin`负向路径通过测试；
- master key/proof/capability在日志、metric、receipt、exception中的hard leak regression通过，包括access mismatch路径；
- 独立immediate-TLS1.3/mTLS endpoint、X509 principal、exact authorizer与redaction负向矩阵通过，且normal Add auth-binding hash/HMAC/KMS/signature/certificate invocation计数为0；
- sidecar/backlink ABA、initial route-first publication、READY/availability 与 profiled metadata mutation authority 经过安全评审；
- domain-specific single-record CAS adapter、store-version/semantic-generation 分离、bounded root/page、unknown mandatory version 与 snapshot publish-before-reclaim通过 fault/compatibility测试；
- same operation identity的same/conflicting payload在response loss、snapshot吸收与bounded retention边界通过幂等/冲突测试；
- orphan install 回收合同冻结；
- §11 exact header/magic/subtype/status executable manifest与mixed-version matrix有确定测试；
- raw Profile wire corpus在真实受支持old v2/v3 decoder/stock binary上证明TLS ClientHello及合法/损坏/截断/unknown Profile request均不产生legacy route/handle/master-key/allocation/journal/payload/ACK effect，且Profile parse error不触发legacy fallback；
- per-connection handshake、restart/incarnation重新协商、unknown subtype、mixed ensemble、response loss no-downgrade与semantic error端到端传播通过测试；
- route claim、legacy normal/recovery Add 与 activation gate 经过并发、restart 和源码评审；
- Classic-only throughput/p99 与 Profile Add CPU 成本证明 routing gate 未引入远程 I/O或不可接受回退。

任一场景出现未安装 Add 被接受、mismatch 静默降级或 metadata 先于 replacement install 生效，RFC 保持 P0 Blocked。

## 15. 开放问题

- failure-domain policy ID/default/domain registry、mandatory capability ID/semanticVersion registry、各production Profile的exact capability组合与尚未接受的cross-field legal-combination条目；
- descriptor schema/hash-suite后续改造与独立optional hint envelope；任何改造直接更新当前合同并同步technical discriminator、migration边界与corpus，当前codec/field/hash/bounds不再OPEN；
- sidecar exact path/backend adapter、root/child/page schema、field numbers、状态名与 immutable backlink encoding；
- authority family/domain sharding、root/page/fan-out hard bounds、snapshot manifest bytes、compaction threshold与retention数值；
- checksum/hash、watch/cache策略，以及backend内部是否使用ZK multi-op/etcd transaction；
- principal allowlist配置、future corrected SASL adapter、MetadataStore backend path、certificate/trust rotation runbook与profiled metadata mutation ACL/credential enforcement；
- `ProtectedProfileStateStore`物理owner、local crash-record framing、at-rest protection、group commit与secure deletion；
- receipt 的持久化位置、压缩和审计方式；
- protected 20-byte credential、install control record与现有master-key persistence的物理整合；逻辑表示与non-disclosure已冻结；
- 后续跨instance credential/KMS rotation只能直接修改当前协议并同步technical discriminator、迁移与兼容边界；当前同instance原地rotation不是OPEN；
- create cancellation、orphan install tombstone 与 GC；
- persistent readiness CAS adapter/path/schema、ephemeral BookieServiceInfo刷新与降级行为；
- CAS→activation gap 的 exact error/retry/backoff、availability completion 与 partial-activation credential distribution；
- write-time inactive orphan/possibly-activated target 的 GC state machine；
- ledgerId reuse 最终策略，以及 instance 分配 authority；
- control subtype最终tail字段表、exact detailCode/BKException/admin mapping、general E/W/A recovery outcome API与batch/range body/schema后续改造；
- stable production wire在raw old-decoder/stock-binary corpus PASS前保持BLOCK；当前矩阵为`DEFERRED_NOT_RUN`、G1为`BLOCKED_UNVERIFIED`，Round 7 bytes只允许reference/corpus prototype；
- 哪些 limit 影响跨实现安全语义、哪些只属于 runtime admission policy。

这些问题关闭并通过 Gate 前，本 RFC 不得标为 Implementation Ready。
