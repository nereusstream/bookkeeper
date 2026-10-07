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

**首批entry布局范围（2026-09-08，Planned / Not Executed）：** 新Profile存储原型只支持CRC32C BK entry布局，应用payload保持opaque。通过现有immutable required-capability及实验Profile manifest绑定布局语义和ledger instance，安装时校验客户端/metadata声明并缓存布局参数；不增加descriptor字段、60-byte ledger context字段或逐Add协商，不在registry Accepted前分配production capability ID。未知、不匹配及尚未支持的digest类型在创建/安装时明确拒绝，不能静默降级CRC32C；Classic不受此限制。replacement和recovery继续使用同一布局，不能由retry改变。

20-byte master-key credential只证明data credential匹配，不表示拥有任意entry digest验证能力。当前`DigestManager.generateMasterKey()`使用`SHA-1("ledger" || password)`，`MacDigestManager`的HMAC key使用`SHA-1("mac" || password)`，两者不能互推。首批不支持HMAC entry，也不下发原始password或MAC key；布局、CRC覆盖及与Segment/TLS的分工由RFC-0005 §6.1固定。reference descriptor/wire corpus和历史receipt仍只证明原有字节合同，不是本次entry布局的验证证据。

同一 ledger instance 的 semantic descriptor 与 protected auth binding 均 immutable；任何变更使用新 ledger instance，当前合同不引入 key-rotation state machine。受保护 local state 的物理 owner/record framing、policy/capability registry 和后续跨instance key/KMS改造仍未关闭；codec/hash本身已不再OPEN。

reference implementation 由 `ProfileDescriptor`、`ProfileDescriptorCodec` 与 `ProfileDescriptorIdentity` 组成，生产 encoder 是唯一 canonical writer，decoder strict-validate input。golden corpus必须保存 authoritative `.bin`、typed fixture、expected SHA-256/36-byte identity与field dump，并由不共享 production parser helper 的 test verifier独立复算；覆盖合法组合和 duplicate/order/missing/unknown/length/trailing/E-W-A-F/policy generation/capability count等全部负向向量。该实现只允许 create/install/open/control 冷路径调用，normal Add只比较缓存的36-byte identity。

项目内部只使用上述无代际后缀名称。`codecVersion=1`、`semanticSchemaVersion=1`与hash domain separator中的`/v1`是当前字节合同的technical discriminator，不是项目代际；后续改造直接修改本RFC和实现，并在不兼容时同步更新discriminator、migration/rollback边界与compatibility corpus，不建立并行代际类型或合同。

### 5.5 Wave 0 reference implementation 状态

截至 2026-09-02，`bookkeeper-common/src/main/java/org/apache/bookkeeper/common/profile/` 已实现上述 immutable model、canonical codec、strict validator 与 identity；test source 中的独立 verifier、fixture-only registries、6 个 valid/40 个 invalid byte corpus、expected identities/field dumps/checksums 和 fixed-seed fuzz receipt 已提交。可复核范围、命令和机器回执见 [`implementation/profile-descriptor/`](implementation/profile-descriptor/README.md)。

该状态只表示 Wave 0 reference evidence 已落地，不改变本 RFC 的 Proposed 状态，不接受 production policy/capability ID 或 legal-combination table，不解锁 endpoint/stable wire/live shadow/Segment ACK authority，也不把本地测试结果冒充 Spike A/B/C 或 production Gate。

## 6. Profile control namespace 与 initial publication

标准 `LedgerMetadata` 继续唯一拥有 OSS `OPEN / IN_RECOVERY / CLOSED` 状态和 ensemble membership。Profile 使用独立、带 CAS 语义的 sidecar namespace 保存 ledger instance 与 Profile 控制事实；sidecar 不复制或重新解释标准 membership。

Profile的reserved `LedgerMetadata.customMetadata`保留小型immutable backlink，将`ledgerId`绑定到`ledgerInstanceId/sidecar reference`以防ABA。RFC-0004 §14.1另定义固定大小`membershipFrozen`，与membership在同一记录CAS。单个`pendingPublication`仅属于延期的strong completion/reset，首批禁用，后续仅在整个ledger fenced且durable CLOSED后创建；基础恢复和写期replacement不创建它。不得修改immutable backlink或累积operation列表。完整descriptor、receipt、repair/delete history不得塞入该reserved entry或无界sidecar root；其他OSS/user custom metadata不受本合同禁止。repair/delete operation可以使用有界child record。

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
   - atomically publishes delete-enumerable activation targets for all E
6. idempotently ACTIVATE each Bookie from that READY authority
7. only after all E are durably normal-active may initial creation return a normal writer
```

这些名称表达必须区分的事实，不冻结exact enum/schema：全局`READY_AUTHORIZED`先于任何local normal activation；`ALL_E_ACTIVATED/AVAILABLE`晚于全部E的durable activation。READY可以早于部分Bookie active；已active target/write set可能形成合法local success或AQ，未active target必须明确transient unavailable，且初始创建返回normal writer仍晚于all-E activation。该条件不用于只读打开或恢复打开，见§6.4。是否通过credential distribution禁止所有pre-return write保持开放；任何情况都不能回退Classic或扩大错误Profile/durability接受集合。

安全合同固定为：

```text
ACK(normal profiled Add)
    => matching global READY authorization existed before Add processing
    && matching local durable normal ACTIVE existed before Add processing
```

2026-10-06 [标准metadata backlink创建](implementation/profile-control/README.md#步骤-12标准-metadata-backlink-创建)已将固定52-byte instance/opaque sidecar reference写入原标准metadata custom entry，并经真实ZK LongHierarchicalLedgerManager create-if-absent及每次创建结果后的实际read解析同内容重试与回复丢失；instance/reference/初始内容冲突不覆盖，实际创建后删除并读到NONODE仍保持UNKNOWN。新9项真实server回归及相关验证见该记录。它只提供全E安装前置已验证之后的创建原语；真正sidecar/全E receipts/READY/activation、后续birth-bound mutation CAS与完整步骤12仍OPEN，未返回normal writer或提升生产Gate。

### 6.1 Sidecar reservation 与 authority boundary

初始READY/写期activation authority及其target retention、RepairIntent admission，以及后续启用的RFC-0004 §14.1 strong-publication reference更新推进lifecycle记录的store version；只有真正的生命周期撤权cut推进delete/fencing generation。无关operation完成不能让既有grant或normal activation失效。domain progress不发布ledger-global head；上述与delete实际冲突的授权发布是operation级冷路径，root引用及retention必须有hard bound。增强strong completion首批仍不运行。

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

root必须有manifest-locked hard bound，只保存instance/descriptor/lifecycle summary、global lifecycle fence/control generation、有限authority-family directory/head、current snapshot identity/cut和bounded suffix/page references。每个child family/domain声明owner、semantic predecessor与bounded discovery；不同domain只在实际冲突时共享order，已证明不相交range的progress/loss不进入ledger-global universal head；activation authority/target retention、RepairIntent admission和启用后的最终completion publication按各自冷路径合同处理。

RepairIntent admission实际冲突于delete fence。先durable-create绑定exact instance/target/source-range/operation generation的immutable inert child，再以single-record conditional lifecycle/delete-fence head CAS发布admission reference。只有admitted intent可授予recovery grant或接收第一份durable payload；`DELETE_INTENT`先赢后任何新admission失败，cut前全部admitted intent的source/target必须保留在delete discovery中。首批copy/membership结果与matching durable close按RFC-0004 §9.1/7.5解析，不创建strong-publication token或重置loss window。延期的strong completion才使用§14.1的domain prepare → lifecycle publication CAS → resolve，并服从整个ledger fenced+CLOSED前置；未最终发布的candidate不授予strong assertion/reset authority。各路径均不增加per-entry metadata/control progress。

`DELETE_INTENT`是关闭新准入的cut；已有授权的撤销、排空和访问屏障由RFC-0004分别定义。一次cold authority read加一次local conditional write不能被解释为跨MetadataStore/Bookie的原子撤权。所有中间态、response loss与crash必须按该RFC验证；不引入跨key通用事务或Add-time metadata read。

PREPARING/reservation 至少绑定 `ledgerId + ledgerInstanceId + descriptorIdentity[36] + operationId[16] + publicSemanticPayloadIdentity[32] + planned initial ensemble`。它本身不授权 normal Add，也不是第二份 ensemble truth。同一 operation 重试必须解析到相同 public semantic payload、instance 与 descriptor identity；冲突请求失败。

Profiled ledger 的标准 metadata mutation 只能由 Profile-aware coordinator 或等价 ACL/fencing authority 执行；持有 master key 本身不授予绕过 Profile lifecycle 修改 LedgerMetadata 的权限。exact credential/ACL 机制保持开放，但不能假设 sidecar 能约束一个拥有不受限 metadata 写权限的 legacy client。

2026-10-06 [真实PREPARING reservation与INSTALL冷读](implementation/profile-control/README.md#步骤-12真实-preparing-reservation-与-install-冷读)已将最多32个计划目标及完整instance/backlink/descriptor/operation/payload写入真实ZK不可覆盖root，并为指定本地Bookie投影原ProfileControlStore的INSTALL authority；真实双client竞争、同identity冲突不覆盖、响应丢失/提交后timeout及scope/ACL/坏root拒绝已有10项回归。该root不授予READY/normal activation，计划集合不替代标准membership；all-E local receipts、READY/delete cut与birth-bound mutation CAS、activation/coordinator及完整步骤12仍OPEN。

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

2026-10-06 [PREPARING到真实local inactive install](implementation/profile-control/README.md#步骤-12preparing-接线真实-local-inactive-install)已让原typed endpoint及实际ZK reservation冷读复用原Bookie控制driver/write/force/owner apply，只有durable结果成功，提交后timeout不取消实际任务或提前释放writer isolation。6项新验证含真实3个目标同phase并发force、全部durable install后仍normal-inactive；transport仍为typed facts，INSTALL真实TLS/wire、全局receipt/coordinator与metadata前置门槛、READY/CAS/activation及完整步骤12仍OPEN。

### 6.3 Standard metadata 与 READY publication

标准 LedgerMetadata create-if-absent 必须携带 immutable instance backlink，且 initial ensemble 精确匹配已安装集合。之后 sidecar 以 CAS 发布 READY authorization，并绑定实际 metadata version、canonical ensemble digest、instance 与 control generation。任一 CAS response loss 都必须重读两份 authority，按 operation identity/version/digest 判断已提交、可重试或冲突；不得盲建第二个 instance。

READY的同一次conditional lifecycle publication还须使全部初始E的Bookie identity、storage incarnation及对应激活授权进入delete可枚举的持久控制状态；有界child/page先durable，随后由该CAS发布有效引用。发布前child保持inert，不能授予normal activation。它与`DELETE_INTENT`按同一lifecycle cut排序，留存及压缩规则统一见§9.1；不要求标准metadata与sidecar跨key原子提交。

sidecar CAS response loss 时，重读 exact domain head：operation/snapshot identity匹配返回等价 `ALREADY_APPLIED`；若已被 current snapshot/terminal generation吸收，可返回等价完成；否则返回 stale/conflict并重建 lineage。不得为了永久回答所有 old request 保存无界 idempotency history。

上述重读同时比较semantic payload/content identity：same operation + same payload只解析为同一既存结果；same operation + conflicting payload只能`CONFLICT`，不能借response loss或snapshot吸收伪装成幂等成功。

Sidecar reservation 单独存在不授权 Add；标准 membership 单独存在不激活 Bookie；只有 post-publication authority 才允许 Bookie durable normal ACTIVE。所有中间态必须 inert 或可恢复，不能扩大接受集合。

2026-10-06 [all-E后的初始metadata协调门槛](implementation/profile-control/README.md#步骤-12all-e-后的初始-metadata-协调门槛)已固定descriptor/E/W/A/有序计划/credential与peer binding，先真实PREPARING，再并发收齐全部E的matching durable INSTALL及稳定birth-generation receipts，fresh-read原root snapshot后才调用标准metadata create/actual read；A个成功、UNKNOWN、错binding或root变化均不推进。冻结caller custom bytes及receipt capability digest，无secret字符串展开。返回metadata/receipts，尚无persistent retention、READY/delete cut CAS、actual INSTALL TLS/wire、全E activation或normal writer，步骤12仍PARTIAL。

2026-10-06 [initial READY与DELETE_INTENT CAS](implementation/profile-control/README.md#步骤-12initial-ready目标留存与-delete_intent-cas)已真实持久化有界initial activation page，再由同一birth/semantic/version-bound lifecycle head CAS发布READY并与DELETE排序；旧root/page dataVersion0不能接受旧CAS，READY后的DELETE保留全部E及activation授权。actual READY/metadata冷读接入原typed endpoint和reserved physical driver，3个目标durable activation后本地normal admission开放；transport仍typed facts，没有实际control wire、normal writer返回或物理删除，完整步骤12/Gate仍PARTIAL。

### 6.4 Local activation 与 availability completion

ACTIVATE 是幂等冷路径操作。Bookie 只有在 READY authority 匹配本地 instance/hash、且 durable fence/tombstone 未先发生时才能 durable normal-active；迟到 activation 不能重新打开 fenced/deleted route。watch/cache 只能提前触发 activation或优化失败响应，不是正确性依赖。

全部E durable install和initial activation仅是**初始创建并返回normal writer**的发布条件；availability completion可以是有界completion fact，或由E个local state有界重查证明。exact state name、receipt packing及partial-activation credential distribution仍开放；普通Add保持有界本地检查。

| 操作 | 接受条件与结果 | 不执行的动作 |
| --- | --- | --- |
| 初始创建normal writer | 全E inactive install → 标准metadata → READY → 全E initial activation，满足后才返回writer | 不把install降为A或W，不向未激活target发送normal Add |
| 只读打开已有ledger | 校验instance、descriptor/布局和生命周期；confirmed范围由客户端LAC/CLOSED边界限定，显式unconfirmed/恢复点读按自身合同选有效副本；未验证定位覆盖返回not-ready | 不ACTIVATE，不等待全E在线或normal-active，不因打开reader新增控制日志持久化，也不以副本local LAC截断合法点读 |
| 打开以执行恢复 | 按RFC-0004执行fencing、取证、显式recovery grant、恢复与matching durable close，返回对应outcome | 不以normal activation替代recovery权限，不重新开放normal写入 |

已安装的CLOSED/fenced replica可服务合法读取；normal admission关闭不等于读取权限关闭。RFC-0005 §8分别定义客户端confirmed边界、Bookie物理存在性和恢复候选范围，不能以单个readable/local LAC水位替代：LAC=99而entry 100已正确落盘时，合法unconfirmed/恢复点读可以读取100，是否进入恢复前缀由RFC-0004决定，物理存在本身不授予confirmed可见性。新open仍受authoritative delete检查，既有reader服从普通删除合同；不增加same-ledger writer takeover或逐次read MetadataStore查询。读取所需副本不可用时按读合同失败/defer，但不能仅因无关副本离线而等待all-E activation。

同一install或initial activation phase内对E个target的请求可有界并发，不逐Bookie串行往返；跨phase仍等待相应全部durable结果，response loss重试同operation。验证须覆盖CLOSED ledger部分副本离线仍可只读打开、recovery/readable不授予normal写，以及全E尚未安装/激活时初始writer不返回成功。

2026-10-06 [可恢复初始协调与all-E availability](implementation/profile-control/README.md#步骤-12可恢复初始协调与-all-e-availability-snapshot)已从fresh/PREPARING/candidate/READY恢复同一初始操作；持久page证明metadata曾存在，恢复只读校验并隔离缺失，不重建未知record。全部E当前matching durable activation及最后metadata/READY取证后返回AVAILABLE snapshot，历史fenced activation返回STALE不计数。实际新coordinator/ZK session及Bookie文件replay、partial/UNKNOWN/DELETE门槛已验证；无actual control TLS/wire、LedgerHandle/writer或production authority，步骤12仍PARTIAL。

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

2026-10-05 后续隔离实现：[真实 ZK 控制权威读取](implementation/profile-control/README.md#步骤-12真实-zk-控制权威读取)实现原 cold-read 接口的 provisioned view reader，严格 key/state/codec 与单次 bytes/Stat.mzxid 绑定，73 项相关测试及 style 通过。该 view 不能替代本 RFC 的完整 sidecar、target 集、immutable backlink 或 coordinator 发布/CAS；完整 scope/AuthZ/credential 和 local durable effect 仍由 endpoint/physical owner 拥有。只读 adapter 不完成安装激活、真实 startup、stable wire 或生产 authority，历史 Wave 0 回执不改绑。

2026-10-05 准入增量：[session 绑定的本地 DATA gate](implementation/startup-readiness/README.md#步骤-12session-绑定的本地-data-准入)在首次 cold 操作前绑定实际 ZK watcher manager；匹配 owned registration 后开放，观察到失连、过期、close/demote 即永久退休原 attempt，同 session 重连及迟到读取不重开。隔离 graph 的普通/恢复 DATA 按本地 Bookie/incarnation 检查，热路径不查 metadata；已提交 I/O 与原池资源正常排空。八类 296 项及 style 通过，有限矩阵 37 正向完整 PASS/113 指定反例。真实 startup/fresh recovery/delete facts、生产 listener/registry、持续远端 authority、完整安装激活、quorum、完整模型与步骤 12/Gate 仍未完成。

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

**跨操作一致的entry大小：** ledger instance的最大可接受应用payload必须同时满足normal Add、single-entry Recovery Add、读取响应和Segment record/batch的表达能力。创建/安装按已有capability/布局及同一实验manifest验证并缓存统一上限，普通Add不得只用自身frame余量放行：

```text
maxPayload = min(normalAddPayloadCapacity,
                 recoveryAddPayloadCapacity,
                 readResponsePayloadCapacity,
                 segmentRecordBatchPayloadCapacity)
```

每项分别扣除自身frame/header、context、credential、entry metadata/checksum及存储framing开销；对齐项还须证明相应buffer和授权物理范围可分配。长度相加、减去固定开销、对齐向上取整和offset/range计算先做溢出与范围检查，再执行大分配或准入。安装缓存后的上限供正常路径常数时间检查，不增加逐请求协商、entry分片、descriptor/context字段或生产capability ID。

本轮在`635c383e80ef353753712a7117aef32fd6b8f313`核对的[`ProfileFrameCodec`](../../../bookkeeper-common/src/main/java/org/apache/bookkeeper/common/profile/wire/ProfileFrameCodec.java)以`ABSOLUTE_FRAME_MAX=5,242,880`限制不含最外层4-byte length prefix的frame，内部header为32 bytes；[`ProfileOperationCodec`](../../../bookkeeper-common/src/main/java/org/apache/bookkeeper/common/profile/wire/ProfileOperationCodec.java)的normal/recovery固定body分别为100/144 bytes，均不含BK entry本身。首批CRC32C entry另含36-byte metadata/digest，因此静态计算为：

| 当前实验布局的单项约束 | 最大应用payload bytes |
| --- | --- |
| ADD_NORMAL自身frame | `5,242,880 - 32 - 100 - 36 = 5,242,712` |
| ADD_RECOVERY自身frame | `5,242,880 - 32 - 144 - 36 = 5,242,668` |

区间`5,242,669..5,242,712`共44个长度值满足normal frame却超出recovery frame，首次普通写必须拒绝。`5,242,668`也只是上述两项的候选上界，读取响应和Segment布局可能进一步收紧；它不是生产默认值或完整支持承诺。原型必须先冻结并验证全部四项及统一N，不能将尚未冻结的read/record容量视为无限。历史reference codec/corpus仍只证明原wire边界，不因本轮文档成为统一entry限制的实现证据。

运行时可在已声明能力内收紧新写准入阈值，但必须继续读取、恢复此前合法接收的entry。暂时缺少buffer/I/O/空间预算按§9.3背压；协议或实现根本无法表达的entry在首次写入前按size/capability明确拒绝，不能等到recovery再失败。验收覆盖统一N的N-1/N/N+1、上述44-byte区间，以及降低新写阈值后旧entry仍可读/恢复。

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
4. publish/verify post-CAS activation authority together with retained cleanup target identity
5. replacement becomes durable normal-active
6. only then resend affected pending Adds
```

该路径不复制整个历史 fragment。CAS 到 activation 之间只能暂停写入或返回明确 transient failure；不能向 replacement resend、降级 Classic 或执行 per-entry metadata operation。install/activation 属于 `MetadataUpdateLoop.transform` 外的显式异步冷路径 phase，避免 CAS conflict 重放 transform 时重复外部副作用。

标准LedgerMetadata只保存当前有效fragment/ensemble映射，不保证保存同一起点被连续覆盖前的所有成员。当前[`LedgerHandle.ensembleChangeLoop()`](../../../bookkeeper-server/src/main/java/org/apache/bookkeeper/client/LedgerHandle.java)在新起点等于最后起点时调用[`LedgerMetadataBuilder.replaceEnsembleEntry()`](../../../bookkeeper-server/src/main/java/org/apache/bookkeeper/client/LedgerMetadataBuilder.java)，覆盖该key。例如LAC始终为99，fragment 100先换到D并向D写入，再换到E，最终map可能不再含D。这个静态反例要求补全清理发现，不能由此推断已确认数据丢失，也不要求标准metadata保存全部版本。

每个instance的初始激活和写期replacement都遵守同一目标留存合同：

- 任何Bookie取得durable normal activation、接收第一份normal DATA前，其Bookie identity、storage incarnation及对应激活授权已进入可恢复、delete可枚举的控制状态。post-CAS authority与该目标引用由同一次conditional lifecycle CAS发布，复用§6.1/6.5的bounded child/page及snapshot；先写child不等于授权生效。
- 发布检查相同live instance与delete/fencing generation，并与`DELETE_INTENT`竞争同一lifecycle cut。delete先赢则禁止新授权；membership CAS已经成功也不能绕过该条件，未获授权的target保持inactive。授权先赢则后续membership覆盖、activation响应丢失或target离线都不能抹掉其清理义务。
- 留存可按instance内的target identity/storage incarnation去重，压缩为有界页和snapshot，并保留解析激活授权所需的摘要/lineage。旧记录的义务由已提交snapshot完整接管，或相应目标已有可验证终结证明后，还须满足§6.5的current/fallback/operation引用退出条件才可回收。达到资源上限时暂停新的冷路径发布，不能为满足上限丢弃可能持有DATA的target。
- 历史仅承担清理发现和控制追踪，不替代membership authority，不让旧target/旧incarnation的迟到ACK重新计入当前write set。ACTIVATE消费本次已发布的exact authority，不为激活遍历全部留存历史；delete/reconcile才按snapshot/suffix枚举。普通Add不读取该历史、不更新metadata；成本计入换组冷路径的控制写入、CAS等待和留存空间。

授权先发布并不表示远程delete CAS能瞬时阻止所有旧本地操作；迟到activation/DATA仍按已有本地gate、fence/tombstone与drain排序，由留存目标承担可发现的清理义务。这里不创建延期strong completion的`pendingPublication` token，也不建立通用repair transaction。

并发和 response-loss 合同：

- 继续继承 BookKeeper single-writer/fence；同一 handle replacement 串行，不新增多 writer consensus；
- 标准 LedgerMetadata CAS winner 是唯一 membership winner；
- CAS response loss 后重读 exact fragment start、old ensemble identity、replacement mapping、instance marker 与 operation generation；匹配才继续 activation，不匹配则 target 保持 inactive/orphan；
- activation response loss 只重试/查询同一 operation，不因 timeout 盲选第二个 target；
- ledger 已 `IN_RECOVERY/CLOSED` 时停止 normal activation/resend；
- standard membership freeze marker存在时拒绝metadata变更；若发现增强协议遗留的pendingPublication则保留并按RFC-0004解析，不得强删。首批活跃路径不创建这种token，不能让strong completion成为normal replacement的依赖；本地durable fence/tombstone先发生时迟到activation失败，activation先发生时由后续本地屏障撤销；
- sidecar post-CAS authority绑定instance/profile、committed metadata version、relevant fragment start、new ensemble digest、activation generation及上述target/incarnation留存引用，不复制pending Add或完整metadata。

如果 install 失败：

- replacement 不进入 ensemble；
- 选择其他满足条件的节点或使操作失败；
- 不降低 capability；
- 不把首次 Add 当作安装触发器。

install成功但从未获激活授权的inactive replacement不接收payload，继续使用既有orphan清理，不为它建立复制事务。GC须从sidecar/standard metadata、instance/hash、完整activation retention、stable grace与durable tombstone证明它未获授权且不会被迟到response激活；仅最终ensemble map没有该target不能构成证明。已获授权的目标按留存历史解析，不能重新当作无历史inactive orphan。

### 9.2 AutoRecovery repair

AutoRecovery 的 target 在接收第一份 durable payload 前，必须已有 RFC-0004 的durable、已完成lifecycle/delete-fence admission的instance-specific RepairIntent，并获得仅绑定该admitted intent的recovery-only authority。copy 完成并 CAS standard ensemble 后，closed/historical fragment target 通常转换为 `COMMITTED_REPLICA/READABLE`，关闭 recovery-only authority，但不自动成为 normal-active。

只有 target 同时成为当前 writable fragment member，并重新满足本节 9.1 的 post-CAS membership、fence 和 normal activation合同时，才可独立进入 normal-active。实际 surviving reader source 可以动态选择，不进入持久 authority；repair target、被替换 member、fragment identity 和 operation generation 必须可被 delete freeze 枚举。

### 9.3 普通 Add 的逻辑身份、unknown 与换组

普通 Add 分开维护两种身份：

| 身份 | 必须保持的语义 |
| --- | --- |
| 逻辑写入 | `ledgerInstanceId + entryId + payload identity`不可变；同坐标冲突由RFC-0005拒绝 |
| 副本投递 | 绑定当前committed fragment/ensemble、target及storage incarnation；合法换组后可建立新的投递尝试 |

原target已durable但ACK丢失或永久离线时，不要求先从该target消除unknown才能启动§9.1正式换组。客户端可以把同一逻辑写入重发到已完成inactive install、membership CAS和normal activation的新target；不得改entry/payload、重分配逻辑entry或降级Classic。INSTALL/ACTIVATE/GRANT等控制操作的unknown仍须查询或重试原operation，不能用数据投递规则盲建第二个control operation。

客户端实现必须逐项保留以下记账规则，并以当前`PendingAddOp`的换组重发和旧地址响应过滤为源码起点：

1. membership CAS与必要activation完成后仍保持现有completion gate，直到全部受影响pending Add的映射及ACK状态完成切换；不能仅因远程phase完成就恢复成功回调；
2. 为所有受影响pending Add更新当前mapping，撤销全部被替换slot的旧ACK及相应故障域状态，再按当前有效数量/已启用policy重算各entry完成资格；新响应按现有同步规则记账，但切换中途不得发布成功。迟到旧target/旧incarnation ACK不计入当前集合；
3. ACK集合只包含该entry当前有效write set中的distinct replicas；被替换slot的旧投递响应不能拼回当前quorum/故障域覆盖。未变slot的有效ACK按下述身份规则保留，不能因ledger-global ensemble版本变化一律失效；
4. local success、当前ACK quorum与客户端连续前缀completion分别记录，逻辑Add至多完成一次；
5. recovery/delete/membership freeze先赢时停止normal activation/resend，返回保留unknown事实的non-OK结果。

一次多slot replacement的本地完成顺序为：membership/activation完成 → 保持`changingEnsemble`或等价gate → 全部受影响pending Add完成映射更新、所有被替换slot撤销及完成资格重算 → 解除gate → 显式尝试原有连续entry前缀回调。某slot不在该entry write set中，只表示该slot无需撤销/重发，不能在逐slot处理中调用可越过gate的成功发布；全部更新后仍须触发回调，避免全是未受影响entry时无人推进。

未被替换、仍属于当前write set且Bookie identity/storage incarnation未变的有效ACK可以保留；只重发实际需要替换的副本，不因ensemble整体变化清空全部ACK或无条件重发W份DATA。实现复用ledger执行顺序、completion gate及pending同步保护，具体回调/新响应/重发交错由CLIENT-1验证；不新增持久化epoch、metadata CAS或分布式协议，正常Add不增加新的持久化/协调工作。

**资源背压与故障重试：** 当前[`PendingAddOp.writeComplete()`](../../../bookkeeper-server/src/main/java/org/apache/bookkeeper/client/PendingAddOp.java)的未单独分类non-OK会进入Bookie failure处理；delayed ensemble change只改变触发条件，不能把暂时拥塞自动变成正确的Profile语义。因此Profile adapter必须在进入ensemble failure handling前消费§11.5完整`statusClass + retryDisposition + durableResult`，不能先压成通用`WriteException`。

- 明确在本次DATA准入前因资源暂满拒绝时，有界退避并优先等待原target容量恢复，不立即换组；持续不可用仍可在满足既有故障判定条件后执行正式replacement。已知activation/control协调阶段等待已有操作解析，不由每条Add重新协调。
- 本次拒绝的`durableResult=NONE`只描述本次尝试，不能抹掉此前同一逻辑Add的UNKNOWN。提交后、部分写入或结果不明沿原unknown合同处理，不能称为肯定未写入的资源拒绝；服务端不能因此清除旧坐标占位。
- retry仍占用原逻辑Add的inflight/bytes预算，保持instance、entryId、累计length与payload；复用现有executor/timer/连接状态，waiter及请求副本有界。可按连接合并唤醒，但各Add的deadline、结果和ACK记账独立。
- 初始客户端准入应尽量在分配entryId及递增累计ledger length前取得必要预算。已分配逻辑位置后，必须完成、在同一位置重试或进入既有ledger失败/恢复流程；退避不无限延长调用deadline，deadline到达按该流程结束等待，不能跳过此entry继续发布更高entry成功。
- FENCED、TOMBSTONED、身份/payload冲突或无权限按对应终止语义处理，不能靠换target绕过。admission拒绝、重试与真正replacement分别计量，不把拥塞拒绝统计成磁盘故障，也不以无限丢弃新写取得低延迟。

这些是待实现的Profile客户端要求，未改变当前production `PendingAddOp`或reference wire corpus的证据范围；不增加逐retry持久记录或通用流量调度平台。

### 9.4 首批故障域与ACK成功判定

最小集群原型先覆盖共同矩阵`3/3/2`与`3/3/3`，以stable Bookie identity作为声明故障域，测试`F=1`；storage incarnation用于区分证据身份，同一Bookie的新旧incarnation不得计为两个独立故障域。这是实验矩阵，不分配production policy/capability ID；更多组合、rack/AZ策略须独立接受。Model A仍保留`E>W`轮转场景。

每次客户端成功必须同时满足当前write set的`A`个有效ACK、至少`F+1`个声明故障域和连续前缀要求。当前客户端已有可选的`enforceMinNumFaultDomainsForWrite`及`areAckedBookiesAdheringToPlacementPolicy()`入口；Profile必须绑定immutable policy/域身份，并证明所选policy确实实施检查，不能继承默认返回true或依赖运维可关闭的开关。首批distinct-Bookie policy在已证明replica去重、当前write set和`F+1 <= A`时，直接由同一有效ACK状态推导域覆盖，无需每entry额外域证明、集合或通用rack lookup；此优化不能外推到rack/AZ。placement只有在证明每个合法ACK子集均满足该policy时，才能替代逐ACK集合检查。

接受测试包括：最快ACK来自同一域、重复/迟到ACK、换组后的旧域计数清除、unknown/missing域身份、incarnation替换与预算内永久丢失。未满足覆盖时不发布成功，不把超时当成payload loss。

### 9.5 现有客户端的两个维护项

在`c79357d76f95dae4c27ffbddcc075b6385991daa`上记录的两个源码维护项继续保留；本轮在`635c383e80ef353753712a7117aef32fd6b8f313`补充核对CLIENT-1的多slot本地切换顺序。CLIENT-2已完成一行回收修复和4种响应/callback排列的确定性红绿回归（现有测试类共9项通过）；[实现回执](implementation/client-maintenance/README.md)限定其证据范围。CLIENT-1现已完成整体gate、ACK/故障域同步撤销与保留未变ACK的修复；先获得5个反例及补充的入队交错反例，最终85项定向及既有测试通过。证据仅为Classic客户端切片，不提升Profile/Spike/Gate。它们是独立的Classic客户端维护项，不属于Wave 0的Segment ACK接入，也不授予新协议/存储authority。

| 工作项 | 源码事实与实施要求 | 确定性验收 |
| --- | --- | --- |
| CLIENT-1 ACK状态一致性 | [`PendingAddOp`](../../../bookkeeper-server/src/main/java/org/apache/bookkeeper/client/PendingAddOp.java)原先撤销`ackSet`却未同步清理`addEntrySuccessBookies`，`completed`只按数量撤销；原先[`LedgerHandle`](../../../bookkeeper-server/src/main/java/org/apache/bookkeeper/client/LedgerHandle.java)先解除`changingEnsemble`再逐slot unset，不在write set的分支会尝试成功回调。现已按§9.3保持整体completion gate，全部受影响pending/slot更新后共同重算当前有效数量和policy覆盖，再开放连续回调；保留身份未变的有效ACK | Spike A A29：保留旧ACK/故障域残留和数量够但域不足；增加4/3/2双slot替换，先处理entry write set外slot时仍不回调，后续slot旧ACK全部撤销后再判定；未变副本不被清空或无条件重发 |
| CLIENT-2 迟到响应回收 | `writeComplete()`旧地址分支原先漏掉`maybeRecycle()`；现已补安全回收检查并保持既有条件；回收后立即return，不能再访问已清空字段 | Spike A A30：最后一个旧地址响应、最后一个当前地址响应、逻辑callback未完成三种顺序；请求归零后恰好归还一次，buffer不提前或重复释放 |

CLIENT-1同时检查timeout统计及recycle重置对成功集合的依赖；未启用故障域检查时避免维护冗余Bookie集合，但不能因此改变诊断含义或丢失必要ACK状态。具体复用/派生方式由小范围补丁决定，不要求新增协议或大规模重构。`RackawareEnsemblePlacementPolicyImpl.areAckedBookiesAdheringToPlacementPolicy()`会计入传入集合里仍属knownBookies的旧节点机架，因此只修policy内部而不修调用方状态不足。

多slot确定性反例：`E/W/A=4/3/2`，旧slots为`[D,A,B,C]`，entry 1写入A/B/C并已获A、B ACK，作为队首因换组尚未callback；本轮替换0、1为D'、A'。源码先解除gate，若先unset slot 0，`hasEntry(1,0)=false`分支调用`sendAddSuccessCallbacks()`，可能在A旧ACK尚未撤销时返回成功；新write set A'/B/C此时仅B有效。测试须固定0→1处理顺序并交错新/旧响应，验证整体gate而非仅最终集合内容。首批存储矩阵E=W不覆盖此分支，不能替代一般E>W的CLIENT-1兼容回归。

CLIENT-2的确定性测试已证明最后旧响应漏归还对象池，修复后按原条件恰好归还一次；`toSend`通常已在callback后的`maybeRecycle()`释放，不将其表述为已证实的直接内存永久泄漏。回归沿用[`PendingAddOpTest`](../../../bookkeeper-server/src/test/java/org/apache/bookkeeper/client/PendingAddOpTest.java)及相关placement测试，保留现有timeout/callback同步保护。

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

初始创建/install选中的target含旧Bookie、缺少mandatory handshake/capability或形成mixed ensemble时，必须在payload前失败。已有ledger按§6.4区分只读/恢复打开：实际选择执行Profile读、fencing或recovery操作的target也必须支持对应语义，否则拒绝该target操作，按已有读/恢复合同选择有效副本或返回失败；这不要求仅为打开reader探测全部E在线或执行activation。任何Profile请求均不得改写成Classic、重试legacy opcode或双写。

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

magic的位级候选意图是让current v3 protobuf先遇invalid wire type，pre-v3再看到unknown opcode `0xf0`；这不是证据。Spike A必须把TLS ClientHello、全部合法subtype、bit flip、1..31-byte truncation、length/version/flag/subtype变体、current v3 `RuntimeException`变体、version=0/nonzero legacy prefix及fuzz corpus投喂每个受支持真实old decoder/binary，证明route claim、handle/master-key persistence、allocation、payload/journal write与ACK全部为0。任一失败只允许调整magic/framing后重跑，不允许fallback或双写；Gate PASS前本manifest不得晋升stable production wire。当前 Wave 0 将该真实binary矩阵标记为`DEFERRED_NOT_RUN`并排除出常规实现/CI；这不删除Gate或降低Oracle，G1保持`BLOCKED_UNVERIFIED`，未来只有在单独显式授权并产生fresh run identity与完整证据后才能重新判定。

### 11.5 Mixed/rolling matrix 与 semantic errors

最低matrix固定为：old client+old Bookie的Classic不变；old client+new Bookie仅在`ABSENT/CLASSIC`接受；new Profile client+old Bookie操作拒绝且不降级；Profile初始创建/install遇mixed ensemble失败，只读/恢复打开按§6.4及§11.1校验所需target操作；Bookie restart/incarnation或protocol generation变化后重新handshake；unknown Profile version/subtype拒绝且无effect；任何Profile bytes在旧Bookie上都不形成legacy Add。registration/capability hint、connection handshake和durable install receipt是三层证据，任一层不能替代下一层。

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

Profile client/admin必须保留完整三元组；legacy callback只能安全投影为non-OK，不能把partial/unknown变成OK。conflict/grant/quarantine/durability-unknown不得从coordinator/admin rich result丢失；exact新BKException、general E/W/A recovery outcome API与detail code继续BLOCK。protocol/header/length/unknown subtype错误关闭连接；control response loss只重试/查询same endpoint/subtype/opId/public payload；Add response loss保持same Profile subtype/instance/entry/payload；原DATA target的durability unknown可按§9.3正式换组后重发，不能盲改control operation/target或降级Classic。

以下映射消费已有枚举，不分配新wire值或改写历史corpus；有界资源原因分类及exact detailCode仍由原型manifest冻结：

| 事实 | 三元组与客户端处理 |
| --- | --- |
| 本次DATA准入前明确因资源暂满拒绝 | `TRANSIENT_UNAVAILABLE + SAME_PROFILE_OPERATION + NONE`；按§9.3有界退避原逻辑Add，优先原target，不直接进入Bookie failure handling。NONE仅描述本次尝试，保留此前UNKNOWN |
| 已知activation/短暂control协调尚未完成 | `PROFILE_NOT_READY_OR_STALE + AFTER_CONTROL_RECONCILIATION`；durableResult如实描述该操作，等待已有协调，不为每条Add启动新install/activation |
| DATA已提交且结果unknown | `DURABILITY_RESULT_UNKNOWN + SAME_PROFILE_OPERATION + UNKNOWN`；保持原逻辑身份重试，只有满足既有故障策略时才选择`REPLACE_TARGET`并完成§9.1后resend；不能回报为准入前NONE |
| FENCED、TOMBSTONED、身份/payload冲突、无权限 | 对应终止status及`NEVER`，保留实际durableResult；不能用更换target绕过。payload冲突的exact detail/API映射仍BLOCK |
| 确认target不可用或达到既有故障判定条件 | 保留实际status和durableResult，按正式replacement处理当前ACK/投递身份及unknown；资源暂满不被定义为永远禁止replacement |

先执行该分类，再进行legacy callback的最终投影。提交阶段决定能否声明本次无DATA effect；caller timeout、后续重试的资源拒绝或回调失败都不能重写已提交事实。重试预算、deadline与entryId/累计length的分配顺序由§9.3约束，不能因背压制造可跳过的逻辑空洞。

### 11.6 Reference codec与数据路径adapter

`ProfileFrameCodec.decode()`、`ProfileFrame`构造/`body()`及`ProfileOperationCodec.AddNormal`解码/构造/`entryPayload()`会分配并复制body或payload。静态调用链说明多次payload量级复制，尚无压测结论。不可变`byte[]` reference codec继续作为字节合同、corpus和oracle；当前隔离明文接收组件先复用固定header校验并交付有界`ByteBuf` body视图，operation/context原位解析与同一valid/invalid corpus等价拒绝仍待接入，不原样串联整包clone接口进入热路径。

派生`slice()`共享父buffer内存且不自动增加引用计数；异步跨组件持有必须明确retain/所有权转移及唯一release责任，不能提前释放父buffer。依据[Netty引用计数文档](https://netty.io/wiki/reference-counted-objects.html)。header、长度、subtype和上限检查仍早于大分配/排队。集中复制到shard对齐批量buffer可以接受；复制、取消、拒绝、断连、I/O失败及重试的生命周期按RFC-0005 §10.1和Spike B B18验证，不要求不切实际的全程零复制。

adapter另消费§8统一maxPayload：wire absolute limit只证明frame可表达，不能单独决定普通Add准入。创建/安装缓存的N同时受normal/recovery/read response/Segment布局约束；检查应用payload、完整BK entry和对齐存储范围各自长度，N+1及溢出在大分配/准入前拒绝。reference frame仍按原枚举/绝对上限解析，统一entry限制由新adapter及独立边界场景验证，不修改旧wire corpus或其receipt。

**接收预算早于完整请求。** 同一资源账本覆盖socket/TLS、decoder累积的未完成frame及已解码未准入请求，不能等pending Add创建后才计payload。首批接收顺序为：

```text
reserve bounded connection/TLS/header/read working set within the Bookie total
    -> receive enough fixed header bytes to check length, phase, subtype and limits
    -> reserve per-connection and Bookie receive credits before accumulating the large body
    -> continue bounded reads/decryption/framing while charging all retained buffers
    -> validate the complete operation, context, unified entry limit and local authority
    -> transfer buffer ownership/credits to the bounded storage admission path
    -> acquire its submission/locator working set before DATA admission (RFC-0005 §10.1)
```

body额度可逻辑预留，不按声明长度立即分配整块内存。连接数量、初始TLS/固定header及单次read工作集也有总量边界；header与body同次到达或暂停读取前已进入pipeline的字节仍计费。密文/明文/复制buffer同时持有时按实际资源计费，reservation与同一buffer的ownership交接不得漏记或永久重复扣款；接收额度不授予DATA写入权限。

[ByteToMessageDecoder](https://netty.io/4.1/api/io/netty/handler/codec/ByteToMessageDecoder.html)会保留未解码完的累积buffer，而[LengthFieldBasedFrameDecoder](https://netty.io/4.1/api/io/netty/handler/codec/LengthFieldBasedFrameDecoder.html)的maxFrameLength约束单frame，不能代替连接合计预算。额度不足时用现有非阻塞read控制或明确关闭策略，不阻塞event loop，不继续收完整payload才拒绝。[ChannelConfig.setAutoRead](https://netty.io/4.1/api/io/netty/channel/ChannelConfig.html)可控制后续自动read，但不释放已进入pipeline的buffer；暂停/恢复、close、timeout、坏长度和解析失败均要解析reservation/ownership并释放实际不再持有的资源。计费可按buffer/read批次或credit，不逐byte增加全局原子计数；阈值与read粒度由B18锁定。

**控制通路也须可达。** 首批在现有Profile listener/pool内保留少量有界的独立控制连接，客户端按连接用途隔离DATA发送，必要fence/tombstone不排在已暂停DATA连接的未完成body后。复用相同TLS/HELLO、认证授权及控制执行资源，不新增listener、wire字段/协商、每ledger连接或普通Add往返。每条连接仍绑定§12身份；连接用途只负责调度，不授予控制权限。

控制连接数量及TLS/接收额度计入Bookie同一总量，保留的控制份额不能被DATA耗尽，也不能作为绕过预算的无限通道。实际header/subtype、operation与授权仍校验；DATA不能冒充控制请求占用保留通路，不能只凭自称控制类型跳过检查。`FENCE_LEDGER`虽属实验wire的DATA枚举，在连接用途隔离上按控制操作处理，已分类DATA的连接在固定头阶段拒绝该操作。B18覆盖多连接半包、暂停后pipeline残留、额度交接/失败释放，以及DATA暂停时合法控制请求仍能接收并推进；A26消费身份/operation拒绝边界。

同一listener不增加HELLO用途字段。连接先占有独立、有界的未分类准入、TLS及接收份额；TLS身份与HELLO通过后，首个非HELLO固定头选择候选DATA或CONTROL用途。DATA在收大body前转入DATA额度；控制请求须完整校验operation/context/credential及授权，才取得保留控制连接名额，之后连接用途固定。入站buffer中跨分类边界的剩余字节保持原计费归属直到释放，长期工作集转移须原子检查并换账。未分类名额耗尽时，新控制连接也无法被预先识别并保证进入；此时只保证资源有界和已有控制连接推进，不宣称新控制连接始终可达。TLS、HELLO及首请求等待各有独立有限期限，避免慢连接无限占位。

实现进度（2026-09-28）：`ProfileFrameCodec.inspectHeader`复用reference固定头/长度/phase/subtype校验，使明文接收适配器在body到达前取得合法声明长度；`ProfileReceiveDecoder`在Netty入口按同次入站ByteBuf、固定连接份额及已交付但未释放的完整frame向同一Bookie的未分类/DATA/保留CONTROL账本计费，容量不足直接关闭并释放。完整首帧在交付前用`ProfileHello.decodeClient`校验body和零flags，解析副本亦计入瞬时额度。定向测试覆盖半包、pipeline已到字节、非法HELLO、断连和交付后释放；reference corpus仍通过。这只是未注册的明文组件，TLS/socket实际buffer、mTLS身份、HELLO授权、存储交接与真实listener均未接通，不作为B18或步骤11完整接收预算证据。

同日新增`TLSContextFactory.forProfileServer()`独立模式：复用现有key/trust加载与刷新，只在该模式固定immediate TLS1.3及必需客户端证书；成功握手后提取`TRUST_DOMAIN_ISSUER_PLUS_SUBJECT`叶子证书身份，握手前及Classic工厂拒绝该提取。双端握手、Classic START_TLS对照及解密后的HELLO/DATA帧经接收器的定向测试通过。独立Bookie端口、listener首字节TLS顺序证明、`ProfileTransportContext`与HELLO能力/operation接线及TLS/socket buffer总账仍未完成，因此不视为真实Profile网络接入或A24/B18通过。

后续定向修复使下游同步关闭或移除接收器后，同次read的残留帧不再被继续解析；固定100字节的`FENCE_LEDGER`请求在申请body额度前拒绝其他声明长度。当前decoder在HELLO阶段占用有限未分类明文份额，首个非HELLO固定头把DATA连接的基础额度原子转入DATA；首个控制请求的frame先占未分类额度，只有下游针对该frame显式报告已授权，才把基础及仍持有的frame额度原子转入CONTROL。同次read已到的ByteBuf仍按原未分类额度计费至释放，转移失败不先归还旧账；后续连接用途固定。`ProfileInboundPipeline`在构造`SslHandler`前先取得未分类连接名额，名额与明文额度在同一账本中跨用途原子交接；握手、HELLO及首请求分别以10/10/30秒绝对期限关闭慢连接，零碎字节不续期。首请求后，后续帧从收到首字节起也有不因零碎字节续期的30秒接收期限，完整交付后取消；空闲连接及已交付下游frame的执行时间不受此期限约束，frame仍由最后使用者释放额度。首个控制请求授权前收到下一帧的任意字节即关闭。localhost NIO socket测试已验证带SAN证书的真实mTLS主机名校验、服务端提取已验证的客户端证书身份、HELLO/DATA解帧，以及`channelInactive`后的账本归零；应用handler只验证framing并关闭连接，不返回业务成功。测试中的下游授权标记仅验证计费和状态转换，不证明真实operation/credential授权。管线尚未接入Bookie listener；TLS/socket实际buffer计费、真实控制授权及存储接管仍未实现。

2026-10-06 后续步骤11已有[真实隔离普通 Add 接线](implementation/profile-wire/README.md#真实隔离普通-add-接线)：使用实际 mTLS/HELLO 与存储 identity，在固定头阶段检查本地 N 上限，再校验 credential/context/CRC/entry 坐标并复用原有界输入复制；仅实际 local-durable 结果产生 OK/APPLIED，force/提交不确定性保留 UNKNOWN。已取消但仍排队的 completion、下游持有的响应，均等实际终结才归还额度。八个真实 socket 场景及相关383项（378通过、5项macOS/Linux亲和性跳过）、style与有限39正向/122指定反例通过。readiness仍为显式输入，生产listener、真实控制授权、TLS/socket/存储总账、完整startup/readiness、quorum、性能及完整模型未闭合；稳定wire与Gate不提升。

2026-10-06 控制网络增量：[真实隔离FENCE与控制额度](implementation/profile-wire/README.md#真实隔离-fence-与控制额度)复用原context/credential校验、Bookie控制driver与实际write/force/owner apply，首次授权后把连接/frame/解析响应workspace原子转入CONTROL；后续逐次鉴权。pre-cut等待返回原操作transient/NONE、force/apply不确定性保留UNKNOWN，断连不提前释放已复制控制请求或实际I/O资源。同一port的真实DATA半包/第二DATA额度拒绝、UNCLASSIFIED满额下已有CONTROL推进及零effect拒绝已验证；14类391项为386通过/5平台跳过，style通过；有限矩阵40正向完整PASS/128指定反例。生产listener、完整control-plane、TLS/socket/native/storage总账、fresh startup/readiness及步骤11/Gate仍未完成。

2026-10-06 恢复写入增量：[真实隔离ADD_RECOVERY](implementation/profile-wire/README.md#真实隔离-add_recovery-接线)沿用144字节固定prefix、精确本地OPEN grant与原DATA驱动，按实际源额度先预留再复制完整BK entry；reference网络整包克隆不进入server路径。恢复成功/重复校验不激活normal writer，grant close晚于DATA写入的过期结果与force失败保持UNKNOWN；断连仅停等待，实际I/O/owner排空及native锁保留。七个真实socket用例已通过；最终相关14类398项为393通过/5平台跳过，style及41正向/134指定反例通过，详细源码绑定见上述实现记录。READ_ENTRY响应/确定missing、真实RepairIntent及grant控制面、客户端quorum/完整恢复、生产listener/readiness与TLS/socket/storage总预算尚未闭合，步骤11/14和生产Gate保持OPEN。

2026-10-06 [Netty backing buffer额度接线](implementation/profile-wire/README.md#netty-backing-buffer-额度接线)已把隔离channel的heap/direct capacity、扩缩容时旧加新backing、derived view最终释放、TLS/socket通过channel allocator分配的ByteBuf及响应workspace拆分加入原连接/Bookie账本。已有read backing跨用途保留原lane；断连归还名额但不提前归还实际仍持有的backing。真实socket漏记红绿、六项allocator回归、TLS分配拒绝及原22项socket场景已验证。JDK/native TLS内部对象、kernel buffers及存储统一总账仍未闭合，生产listener/readiness、READ_ENTRY、完整模型/长期/掉电/性能与步骤11/Gate仍OPEN。

2026-10-06 [接收至存储源副本交接](implementation/profile-wire/README.md#接收至存储源副本交接)使隔离normal/recovery同步entry复制在原source子上限之外，复用借用frame连接的exact heap backing额度；frame/workspace/source共存均计费，断连后源保留至owner实际用完，复制到预分配对齐池后才归还源，实际I/O仍持有原pool lease。明确credit拒绝回滚source并沿用原未接纳结果；其他分配错误传播，手动/pooled调用者保留独立unpooled副本合同。两条真实socket路径及额度拒绝/恢复单测通过，未改wire或存储格式。完整TLS/kernel/object/index/storage总账、生产listener/readiness、READ_ENTRY及步骤6/11/Gate仍未闭合。

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

1. Profile初始创建返回normal writer意味着initial ensemble全部E已durable install且initial normal-active；只读/恢复打开分别按§6.4检查，不调用或等待normal activation。
2. `ACK(normal profiled Add) => matching global READY authorization && matching local durable normal ACTIVE existed before Add processing`。
3. Classic/Profile/Tombstoned route 对同一 ledgerId 是单一、原子、可恢复的 claim。
4. legacy normal/recovery Add 不能绕过 `PROFILE/TOMBSTONED` route 进入 Classic lazy-create。
5. 相同 ledgerId 的两个 live instance 不得共享同一 Bookie routing identity。
6. descriptor 或 protected auth binding 冲突不能被重试、restart 或 lazy-create 消解为成功。
7. 标准 LedgerMetadata 是唯一 membership authority；sidecar intent或 membership CAS 单独存在都不能激活新 Bookie。
8. 写期replacement按inactive install → `LAC+1` membership CAS → activation authority及目标留存发布 → normal activation → resend排序；初始E同样先留存后激活，同起点membership覆盖不丢删除目标，不复制历史fragment。
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

- [Spike A](spikes/SPIKE-A-profile-install.md) 当前启用scope的全部必需场景通过；延期增强项保持独立BLOCK，不以disabled测试冒充其PASS；
- Model A 包含创建、安装、response loss 与 ensemble replacement 的抽象；
- A9/A10/A16/A28及Model A+D证明初始/写期activation前已持久留存target/incarnation；LAC不变、同fragment起点D→E覆盖后删除/rejoin仍发现D，history snapshot/压缩和重启不丢清理义务；
- A26/A27证明完整status三元组在failure handling前分类；资源拒绝/恢复容量不立即换组，当前NONE不抹除旧UNKNOWN，重试预算/deadline有界且不跳过逻辑位置；
- A26/B18验证§11.6接收预算早于大body累积、TLS/半包/已解码请求交接与失败释放，以及DATA暂停时同listener有界控制连接的可达性；实际operation/身份/授权和Bookie总量均不豁免，不以单frame limit证明所有连接有界；
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
- §6.4初始writer/只读open/恢复open条件分离，同phase all-E并发不越过durable依赖；§5.4的immutable CRC32C布局及unsupported/mismatch拒绝由A16/A26/B19验证；
- §11 exact header/magic/subtype/status executable manifest与mixed-version matrix有确定测试；
- raw Profile wire corpus在真实受支持old v2/v3 decoder/stock binary上证明TLS ClientHello及合法/损坏/截断/unknown Profile request均不产生legacy route/handle/master-key/allocation/journal/payload/ACK effect，且Profile parse error不触发legacy fallback；
- per-connection handshake、restart/incarnation重新协商、unknown subtype、mixed ensemble、response loss no-downgrade与semantic error端到端传播通过测试；
- route claim、legacy normal/recovery Add 与 activation gate 经过并发、restart 和源码评审；
- §9.3普通Add unknown/正式换组/旧ACK清除、§9.4实际ACK故障域及RFC-0004普通delete/admission/freeze竞争通过A27/A28；strong-publication/强撤权仅在启用时接受其独立矩阵；
- §9.5的CLIENT-1/2定向回归通过A29/A30，§11.6数据adapter与reference corpus等价并满足Spike B B18复制/分配/生命周期证据；
- CLIENT-1/A29的4/3/2多slot撤销在整体gate内完成，不被write-set外slot或交错响应提前回调；未变有效ACK保留。A26/B18/B19证明统一entry上限N-1/N/N+1、44-byte差异区间和旧entry读/恢复能力不因新写阈值降低而丢失；
- Classic-only throughput/p99 与 Profile Add CPU 成本证明 routing gate 未引入远程 I/O或不可接受回退。

任一场景出现未安装 Add 被接受、mismatch 静默降级或 metadata 先于 replacement install 生效，RFC 保持 P0 Blocked。

## 15. 开放问题

- failure-domain policy ID/default/domain registry、mandatory capability ID/semanticVersion registry、各production Profile的exact capability组合与尚未接受的cross-field legal-combination条目；
- descriptor schema/hash-suite后续改造与独立optional hint envelope；任何改造直接更新当前合同并同步technical discriminator、migration边界与corpus，当前codec/field/hash/bounds不再OPEN；
- sidecar exact path/backend adapter、root/child/page schema、field numbers、状态名与 immutable backlink encoding；
- authority family/domain sharding、root/page/fan-out hard bounds、snapshot manifest bytes、compaction threshold与retention数值；
- checksum/hash、watch/cache策略，以及backend内部是否使用ZK multi-op/etcd transaction；
- principal allowlist配置、future corrected SASL adapter、MetadataStore backend path、certificate/trust rotation runbook与profiled metadata mutation ACL/credential enforcement；
- RFC-0005 §5.2选定Bookie级独立控制日志原型的local crash-record framing、at-rest protection、group commit、secure deletion及真实验证；
- receipt 的持久化位置、压缩和审计方式；
- protected 20-byte credential、install control record与现有master-key persistence的物理整合；逻辑表示与non-disclosure已冻结；
- 后续跨instance credential/KMS rotation只能直接修改当前协议并同步technical discriminator、迁移与兼容边界；当前同instance原地rotation不是OPEN；
- create cancellation、orphan install tombstone 与 GC；
- persistent readiness 已有隔离 [ZooKeeper CAS/path/schema 实验增量](implementation/startup-readiness/README.md#步骤-12真实-zookeeper-readiness-cas-增量)；另有 [session-owned info/ephemeral 发布实验](implementation/startup-readiness/README.md#步骤-12真实-ephemeral-发布增量)；另有 [有界真实文件 local readiness receipt](implementation/startup-readiness/README.md#步骤-12真实本地-readiness-receipt)，只持久化 supplied facts、未接真实 recovery/delete catch-up；稳定 metadata 合同、真实启动、生产 BookieServiceInfo/registry 与持续失连降级仍开放；
- CAS→activation gap 的 exact error/retry/backoff、availability completion 与 partial-activation credential distribution；
- write-time inactive orphan/possibly-activated target 的 GC state machine；
- ledgerId reuse 最终策略，以及 instance 分配 authority；
- control subtype最终tail字段表、exact detailCode/BKException/admin mapping、general E/W/A recovery outcome API与batch/range body/schema后续改造；
- stable production wire在raw old-decoder/stock-binary corpus PASS前保持BLOCK；当前矩阵为`DEFERRED_NOT_RUN`、G1为`BLOCKED_UNVERIFIED`，Round 7 bytes只允许reference/corpus prototype；
- 哪些 limit 影响跨实现安全语义、哪些只属于 runtime admission policy。

这些问题关闭并通过 Gate 前，本 RFC 不得标为 Implementation Ready。


2026-10-06 [初始control的真实隔离socket](implementation/profile-wire/README.md#真实隔离初始-control-接线)增量：显式 opt-in 实验 capability/schema1 已把 INSTALL/ACTIVATE_INITIAL 接到真实隔离 mTLS、原 endpoint 与physical driver；默认能力及通用control-body冻结状态不变。真实principal、cold authority与exact AuthZ通过后，event loop原子转账；cold timeout/UNKNOWN/断连和实际I/O终结分开，副本持续计费至最后使用。七项socket回归含实际ZK PREPARING/DELETE cut和迟到EL授权零写入。非阻塞实际client、完整READY/metadata reader与all-E coordinator经socket组合、生产listener/readiness和统一总预算仍待完成，步骤11/12与stable/G1/生产Gate保持PARTIAL/OPEN。 相关17类412通过/5平台跳过、style通过；有限矩阵49正向完整PASS/184指定反例，完整源码及receipt绑定见实现记录。


2026-10-06 [初始all-E真实socket组合](implementation/profile-control/README.md#可恢复初始协调器的真实-all-e-socket-组合)增量：实际非阻塞initial client借用已有event loops/TLS context，验证actual HELLO/target/incarnation及完整request/reply关联，pending quota在EL排队前取得并等wire/write两种终结，取消/断连不释放仍使用的副本；无自动重试或重连。可恢复初始coordinator现直接经3个mTLS socket与实际ZK PREPARING/READY/标准metadata/原Bookie文件driver完成全E两phase，再返回AVAILABLE snapshot；actual activation reply丢失后显式新client/新coordinator解析同一READY/metadata/op。缺必需metadata仍不重建。8项组合及输出额度RED/GREEN、18类420通过/5平台跳过、style、50正向完整PASS/191指定反例已验证。尚无LedgerHandle/生产writer、普通DATA quorum SDK、replacement/recovery/full delete、完整server-process启动/readiness、统一总预算或production authority，步骤11/12与stable/G1/生产Gate仍PARTIAL/OPEN。 源码与回执绑定见实现记录。


2026-10-06 [普通DATA client](implementation/profile-wire/README.md#普通-data-client-与-sdk-接线边界)增量：actual ordinary DATA client复用shared TLS/HELLO/correlation及write/wire quota，借用原DigestManager/v3 ByteBuf/ByteBufList并保留完整status，initial-control公共API不变。真实三目标force后的status进入原AckSet，A=2不计重复target，ordinary路径无authority cold-read。取消、quota/N/context/credential、timeout同坐标解析、force UNKNOWN和reply破坏通过；这是transport/AckSet组合，尚无完整LedgerHandle/PendingAddOp SDK、有界退避/UNKNOWN累积、Profile换组或fenced-close。步骤11/13及stable/G1/生产Gate不关闭。 18类426通过/5平台跳过、style及51正向完整PASS/196指定反例，绑定见实现记录。

2026-10-06 [固定初始 ensemble 的原 SDK 写入接线](implementation/profile-wire/README.md#固定初始-ensemble-的原-sdk-写入接线)增量：AVAILABLE snapshot 经 metadata/page、全 E matching activation 与实际 DATA HELLO incarnation 校验后构造实验 ProfileLedgerHandle，复用原 LedgerHandle/PendingAddOp 的坐标、digest、quorum 和连续回调。每目标在原 worker/scheduler 上对同一坐标及保留字节做有限退避；当前 NONE 不清除先前 UNKNOWN，in-flight 超时/本地停止保留不确定性。前台 entry 和目标投递分别有界。真实 3/3/2 的后 entry ACK 先到、slot 背压/前台拒绝、deadline、force UNKNOWN→NONE 和 scheduler 拒绝已验证。Profile 换组、fenced-close、点读/recovery 尚未接通，当前停止只排空本地资源，metadata 保持 OPEN；关闭接口返回 IllegalOp，不能误入 Classic metadata 变更。步骤 13 仍 PARTIAL；步骤 11/12、stable/G1 与生产 Gate 不关闭。新回执与有限模型范围见实现记录，历史快照不重绑定。

2026-10-06 [标准 membership 的出生绑定 CAS](implementation/profile-control/README.md#标准-membership-的出生绑定-cas)增量：原 ProfileLedgerMetadataStore 的显式借用 ZK context 现在提供 cold readMembership/replaceEnsemble。一次操作冻结实际父记录 czxid、creator token、immutable backlink、完整 metadata/version 与 LAC+1/mapping；只读 birth child 与 parent version 在同一 ZK multi 中检查，防止 precheck 后删除重建的 version0 ABA。保留 user/custom/credential/state/E/W/A/ctime/creator token，拒绝 recovery/CLOSED、membershipFrozen、pendingPublication 和未知 reserved key；UNKNOWN 不重建缺失 record，不重新捕获新出生身份，不发布 activation/writer authority。九项真实 ZK 验证与 marker guard RED 已覆盖；这仍是步骤13冷路径前置，post-membership authority/历史目标留存、实际 replacement INSTALL/ACTIVATE 与原 SDK gated resend 尚未闭合。步骤11/12/13和生产Gate保持PARTIAL/OPEN，回执见实现记录。

2026-10-06 [replacement 授权与目标留存](implementation/profile-control/README.md#replacement-授权与目标留存)增量：现有 lifecycle head增加experimental REPLACEMENT kind4，最多32个immutable predecessor-linked pages；post-membership权限与本次目标引用由同一次head CAS发布，并在同一ZK multi校验标准metadata birth marker/version。DELETE先赢不授权；授权先赢后的DELETE保留initial与全部replacement目标，历史结果不等于当前可写。最新页及fresh matching metadata可投影原POST_MEMBERSHIP_REPLACEMENT冷读；actual reserved file driver现在支持typed ACTIVATE_REPLACEMENT。独立page向量、真实ZK竞争/保留/上限及typed endpoint+真实文件force/apply已验证。新目标的完整inactive INSTALL准备、replacement wire/TLS client、原SDK whole-pending gate与resend、fenced-close/recovery/delete/GC/legacy ACL仍待闭合，步骤11/12/13及production Gate仍PARTIAL/OPEN；回执范围见实现记录。

2026-10-06 [new-peer inactive INSTALL与replacement wire](implementation/profile-control/README.md#new-peer-inactive-install-与-replacement-wire)增量：same sidecar持久化bounded next-slot准备页，绑定frozen metadata physical birth/version、旧/新fragment digest、完整INSTALL scope；原metadata birth guard用于准备事务，DELETE/freeze竞争不扩大授权。verified初始或匹配prepare的receipt才能发布replacement。UIC1/schema1原bytes保留，ACTIVATE_REPLACEMENT新增独立experimental UIC2/schema2协商，复用CONTROL连接/TLS/HELLO/correlation/实际write与wire终结。四节点真实ZK、mTLS、文件driver已完成new peer inactive INSTALL→membership CAS→目标留存/授权→replacement activation→durable DATA；lost ACTV response用显式新client解析同operation。普通Add没有逐entrymetadata。SDK自动replacement、whole-pending gate/resend、recovery/fenced-close/delete/GC/legacy ACL与生产Gate仍OPEN，步骤13仍PARTIAL，实际回执见实现记录。

2026-10-07 [原 SDK 写期换组与整体回调门](implementation/profile-wire/README.md#原-sdk-写期换组与整体回调门)接通预连接 bounded pool 的真实 inactive INSTALL、membership CAS、retained authority、全新目标 activation 和原 PendingAddOp 多 slot ACK 撤销/必要重发。五节点、两个 pending entry、响应丢失及停止窗口已有真实定向证据；跳过 activation 不能报告 AVAILABLE，UNKNOWN 只能解析原操作，不复活失败 writer。同 ID/incarnation rejoin、registration discovery、controller restart、恢复/删除/GC、生产和完整验收仍待完成，步骤13保持 PARTIAL；本段为当前增量，前述日期回执保留当时范围。

2026-10-07 [3/3/3及同起点连续换组](implementation/profile-wire/README.md#333-与同起点连续换组验证)新增实际SDK证据：普通写必须等第三个durable副本，两个pending entry在起点0经历socket1→socket3→socket4两次实际失败/换组后连续回调；未变副本ACK保留、不重发，两个replacement页及全部历史目标留存。仍是隔离有限范围，更多故障交错、同ID incarnation/rejoin、恢复/删除和生产验收保持OPEN。

2026-10-07 [READ_ENTRY接线与验收范围](implementation/profile-wire/README.md#read_entry-接线与验收范围)已接显式读scope权限、URD1/schema1、实际Get/read/pin/loan/response ownership、fenced完整覆盖的MISSING和原SDK flags0普通读。77项相关Java和新增有限读取分发模型通过；没有normal activation或逐entry控制持久化增量。READ_LAC/fencing read/独立readonly open、完整恢复/CLOSED/AutoRecovery/删除及生产验收仍OPEN，步骤11/14保持PARTIAL，历史回执不重绑。

2026-10-07 [CONTROL客户端FENCE前置](implementation/profile-wire/README.md#真实control客户端fence前置)复用冻结100字节body/8字节status、verified peer与原quota/write/wire custody，实际force/同ID ALREADY/credential拒绝及UNKNOWN后同ID重连解析通过。相关59项Java和既有296配置模型矩阵通过；LAC/source/length、fencing read及matching CLOSED仍未接通，步骤14保持PARTIAL/OPEN。

2026-10-07 [READ_LAC源取证](implementation/profile-wire/README.md#read_lac-源取证)已接独立URL1/schema1、完整source entryId/piggyback LAC/sourceLength与真实Get/read/loan路径；EMPTY要求整ledger完整稳定fenced proof且无explicit LAC。源length不授权CLOSED前缀；原SDK LAST_ADD_CONFIRMED/quorum、explicit LAC、length锚点、fencing read与matching CLOSED仍OPEN，步骤14保持PARTIAL。

2026-10-07 [原SDK的LAC覆盖与length锚定](implementation/profile-wire/README.md#原sdk的lac覆盖与length锚定)新增显式readLacEvidence：原coverage/write-set与digest校验收集source样本，再取得同一handle metadata快照下的prefix边界length；源length不冒充prefix length，缺锚点或矛盾返回non-OK，不改writer状态。它不是fresh远端RecoveryContext、fencing coverage或CLOSED事实；完整恢复/AutoRecovery及步骤14保持OPEN。

2026-10-07 [恢复上下文与fencing前置](implementation/profile-wire/README.md#恢复上下文与fencing前置)已实现真实ZK同记录OPEN→IN_RECOVERY、固定UWRC184恢复token、metadata/lifecycle物理birth guard和同操作重试/重开；显式coordinator复用已验证CONTROL peers及原coverage，只计durable FENCE并重新核对同一上下文。FENCED_CONTEXT不是RECOVERED_AND_CLOSED，完整frontier、恢复Add/close及步骤14仍OPEN。

2026-10-07 [fresh-context只读取证](implementation/profile-wire/README.md#fresh-context只读取证)将同一IN_RECOVERY token下的fence/LAC/length与原SDK recovery点读串联；每个missing来源先完成本instance的durable fence，预算终止不等于tail。readonly session与coordinator不写Add/CLOSED，observed boundary不替代完整required authority或恢复完成；步骤14仍OPEN。

2026-10-07 [RepairIntent admission与实际恢复写回](implementation/profile-wire/README.md#repairintent-admission与实际恢复写回)新增固定32槽的immutable inert计划、actual inactive INSTALL后的同birth/context admission、UIC3 recovery grant及原DATA client单entry恢复Add；admitted目标在DELETE后仍留存。真实ZK/mTLS/files、9类151项Java、Spotless/Checkstyle及有限341配置（67正向PASS/274指定反例）通过；恢复权限不激活normal writer。grant close、membership/replica-readable publication、required frontier与匹配CLOSED、完整AutoRecovery及步骤14/生产Gate仍OPEN。

2026-10-08 [UIC4 recovery grant close接线](implementation/profile-wire/README.md#uic4-recovery-grant-close接线)已将独立exact close authority接到实际CONTROL write/force/owner apply，pre-cut DATA未终结不发布readable；timeout保留UNKNOWN并可显式重连解析同操作，重复close无额外force，不激活normal writer。现有ZK repair reader仍拒绝仅凭admission/grant授权close；membership/copy结果与真实close-authority发布器、matching CLOSED和完整AutoRecovery仍OPEN。

2026-10-08 [历史fragment repair membership与close授权](implementation/profile-wire/README.md#历史fragment-repair-membership与close授权)已用同一ZK事务发布标准membership和immutable repair结果，实际close authority随后接入UIC4；bounded head/digest/inverse校验继续采用同一恢复token，physical admission guard防止同UUID/version0重建通过旧cut。真实copy/socket/files与ZK连续commit/reply-loss/delete竞争已有定向验证。最后开放fragment仍需terminal/frontier proof；完整copy coordinator、AutoRecovery和matching CLOSED保持OPEN，不授予normal或strong loss reset。
