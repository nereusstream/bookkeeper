# RFC-0001：Profile、Capability 与显式安装协议

> 状态：**Proposed / P0 Prerequisite**<br>
> 依赖：BookKeeper 现有 metadata CAS、ensemble 与 fencing 语义<br>
> 解锁对象：`BK_SEQUENCED_CLASSIC_INSTALLED`、`BK_SEGMENT_WAL` 及其组合<br>
> 验证：必须通过 [Spike A](spikes/SPIKE-A-profile-install.md)

## 1. 摘要

本 RFC 将原先混合在一个 Profile 名称中的三类概念拆开，并为需要 Bookie 执行的新 ledger 合同定义两阶段显式安装协议。

核心候选决策：

- Engine Profile 由 Bookie 进程或部署 cohort 决定；
- Ledger Contract Profile 由 immutable ProfileDescriptor 描述；
- Client/Protocol Profile 不冒充 Bookie capability；
- 新合同在 ledger `OPEN` 前安装到当前 ensemble 的全部 E 个 Bookie；
- 标准 LedgerMetadata 只拥有 OSS state/membership；独立 sidecar 拥有 Profile instance 与控制事实；
- normal Profile Add 只有在 global READY 与目标 Bookie durable normal activation 都匹配时才能 ACK；
- 每个新 Profile Add 的请求身份都必须足以匹配 Bookie 本地 durable activation；exact binding fields 保持开放；
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

- sequence run 与 stale-writer takeover，见 [RFC-0002](RFC-0002-sequenced-wal.md)；
- Segment 磁盘格式和 allocator，见 [RFC-0003](RFC-0003-segment-storage-allocator.md)；
- range、recovery 和 delete，见 [RFC-0004](RFC-0004-range-recovery-delete.md)；
- Segment Bookie 对 install、activation、fence、recovery Add 和 ACK authority 的消费，见 [RFC-0005](RFC-0005-segment-bookie-state.md)；
- master key 的现有安全模型重设计。

现有 master key 可以继续作为首版 data-plane ledger credential，但不因此获得 Profile control-plane authority。本 RFC 只冻结两类 authority 的分离、secret non-disclosure 与 fail-closed 消费合同；它不把 P0 扩成 password/KDF、PKI 或 KMS 的全面重设计。

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
- AuthN/AuthZ检查必须早于route claim、credential persistence、local allocation和任何durable effect；exact ACL/RBAC/plugin机制保持开放，不要求引入通用RBAC框架。
- 任何携带master key、verifier或bearer secret的Profile control/data transport必须满足implementation manifest选定的confidentiality与integrity属性；exact TLS/SASL/mTLS机制保持开放，该要求不追溯重设计Classic wire/KDF。

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

Engine Profile 是进程/cohort 级能力。Bookie 启动后公布唯一 active engine 及 capability set。首版不支持同一进程按 ledger 动态切换三种 engine。

### 4.2 Ledger Contract Profile

候选合同维度：

```text
payloadFormat       = OPAQUE_LEDGER_V1 | SEQUENCED_LEDGER_V1
durabilityMode      = SYNC_ON_ACK | DEFERRED_SYNC_LEGACY
indexProfile        = NONE | TAIL_SUMMARY_V1 | DERIVED_SEQUENCE_INDEX_V1
deleteProfile       = CLASSIC_DELETE | CONDITIONAL_DELETE_V1
quorumProfile       = existing E/W/A plus declared restrictions
```

Ledger Contract Profile 是 immutable descriptor 的一部分。需要 Bookie 解析或执行的维度必须出现在 `requiredCapabilities`。

### 4.3 Client/Protocol Profile

`SequenceDomain`、`appendId`、ordered frontier、协议原生位置、事务和可见性属于客户端/适配层。它们只有在 Bookie capability 明确声明时，才能要求服务器解析或索引。

## 5. ProfileDescriptor

本 RFC 先冻结三类信息的边界，不在本轮冻结最终 wire 字段表：

```text
SemanticProfileDescriptor {
    canonicalSchemaVersion
    requiredEngineProfile
    mandatorySemanticCapabilities[]
    payloadFormat
    durabilityMode
    quorumProfile
    permanentLossBudgetF
    failureDomainPolicyIdentityOrGeneration
    indexProfile
    sequenceProfile
    deleteProfile
    formatOrRecoverySafetyFields
}

ProfileDescriptorIdentity {
    hashSuiteId
    canonicalSchemaVersion
    descriptorHash
}

CanonicalProfileDescriptorEncoding {
    canonicalSchemaVersion
    canonicalSemanticBytes
}

InstallOperation {
    ledgerId
    ledgerInstanceId
    installRequestId
    descriptorHash
    authorizationOrRequestCorrelation
    protectedCredentialOrProof
}

RuntimePolicy {
    admissionLimits
    rateLimits
    resourceBudgets
}

OptionalHintEnvelope {
    safetyNeutralHints
}
```

约束：

- `ledgerInstanceId` 在 ledgerId 重用或重建时必须变化；
- semantic descriptor 只包含 immutable safety semantics：Engine、mandatory semantic capability及其版本、payload format、durability、E/W/A binding、`F`/failure-domain policy、sequence/recovery/delete safety semantics，以及真正影响跨实现安全的 limits；它不包含 ledger/instance/store version、当前 membership、operation/request identity、credential/receipt、runtime admission policy、implementation preference、benchmark threshold 或 safety-neutral hint；
- `descriptorHash` 从 canonical semantic bytes 派生，不进入自己的 preimage。descriptor identity 由 `hashSuiteId + canonicalSchemaVersion + Hash(domainSeparator || hashSuiteId || canonicalSchemaVersion || canonicalSemanticBytes)` 共同定义；exact codec、suite、domain-separator bytes、field numbers 与大小上限保持开放；
- implementation manifest选择的descriptor hash suite必须是固定长度、面向对抗输入的collision-resistant hash；CRC、普通checksum或无法满足该属性的过短截断值不能承担semantic identity。exact algorithm与length仍保持开放；
- parser 必须在分配前执行 manifest-locked bounds。duplicate singular field、duplicate map key、duplicate set item全部拒绝；set按稳定数值顺序编码，semantic list保留声明顺序；只有 schema 明确等价时 absent/default 才可归一；canonical bytes不得依赖locale、map iteration order或实现默认值；
- unknown semantic field/enum、unknown schema 或 mandatory capability 一律拒绝；safety-neutral optional hint在 semantic descriptor/hash之外，可忽略但不得改变接受集合；已知字段不能被旧 reader/writer strip、rewrite或降为default后重新发布；
- consumer 必须同时持久化/取得 canonical semantic bytes 与 declared identity，并重新计算比较；同 schema 语义相同必须产生相同 bytes/hash，安全语义不同必须产生不同 identity，跨 schema 不假定语义等价；
- mandatory capability 未识别时拒绝；optional hint 可以忽略，但不得改变 durability；
- durable install 的语义身份至少绑定 `ledgerId + ledgerInstanceId + descriptorHash + protected auth binding`；
- 相同语义的新 request 可以返回原 install generation，不为任意 request ID 建立无界 durable idempotency 表；
- 相同 request ID 携带冲突内容必须失败；
- secret 或可离线验证的 credential material 不得进入普通日志、公开 metadata、receipt dump、metric、exception 或诊断输出；receipt 也不得成为 bearer/replay capability；
- 只有确实影响跨实现 payload 解释、durability 或 recovery 正确性的 limit 才能进入 descriptor；`maxInflightEntries/maxInflightBytes` 等通常属于 runtime policy。
- `permanentLossBudgetF` 与 declared failure-domain policy 是 immutable recovery safety contract：normal ACK 必须覆盖至少 `F + 1` 个 distinct declared domains；RFC-0004 只有在 bounded range 完整恢复到同等 coverage 并发布强 completion proof 后才能重置该 range 的 loss window。

同一 ledger instance 的 semantic descriptor 与 protected auth binding 均 immutable；任何变更使用新 ledger instance，首版不引入 key-rotation state machine。canonical 规则已经冻结，但规范编码、hash suite、受保护 `authBinding` 的物理机制、最终 safety-limit 字段表和未来 key/KMS evolution 仍是 RFC 接受前必须关闭的开放项。首版不要求 descriptor signature、Merkle tree、PKI、cluster HMAC 或 per-Add nonce/proof，也不能把可离线验证的 `masterKeyDigest` 暴露为公开 descriptor 字段。

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

PREPARING/reservation 至少绑定 `ledgerId + ledgerInstanceId + descriptorHash + creation request identity + planned initial ensemble`。它本身不授权 normal Add，也不是第二份 ensemble truth。同一 creation request 重试必须解析到相同 instance/descriptor；冲突请求失败。

Profiled ledger 的标准 metadata mutation 只能由 Profile-aware coordinator 或等价 ACL/fencing authority 执行；持有 master key 本身不授予绕过 Profile lifecycle 修改 LedgerMetadata 的权限。exact credential/ACL 机制保持开放，但不能假设 sidecar 能约束一个拥有不受限 metadata 写权限的 legacy client。

### 6.2 All-E inactive install

标准 LedgerMetadata 创建前，协调方必须对 planned initial ensemble 的全部 E 个 Bookie 执行 `INSTALL_LEDGER_PROFILE`，同时 durable claim `PROFILE` route，但保持 normal-inactive。不能只安装当前 write set 的 W 个节点，也不能先暴露标准 OPEN metadata 后再补 route claim。

部分成功或 response loss 时，只重试相同 install operation；未形成全部 E durable receipts 前不能创建标准 LedgerMetadata。receipt 至少绑定：

```text
bookieId
bookieStorageIncarnation
ledgerId
ledgerInstanceId
descriptorHash
installRequestId
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

Profile control plane 与现有 data credential 是两套权限：master key/password verifier 只参与 ledger data access，不能授权 INSTALL、READY publication、ACTIVATE、repair grant、tombstone 或 delete。首版推荐的最小实现形状是：满足manifest confidentiality/integrity要求的authenticated cold control channel上，non-anonymous principal还必须被授权执行exact operation/ledger instance/target scope；目标 Bookie 先完成该AuthZ，再直接读取并验证已提交的 MetadataStore authority；验证通过后才将受保护的 auth binding 与 route/instance/generation 一起 durable 落本地；普通 Add 只比较这个本地有界状态。watch、caller 携带的 generation、registration property 或客户端自述都不能替代该 direct-read verification。

冷路径 authority/reference 必须按 purpose domain-separate，并至少绑定：logical opcode/purpose、ledger/instance、descriptor identity、target Bookie stable identity与storage incarnation、本地 install/activation generation、相关 READY/membership/RepairIntent authority generation，以及 operation payload identity。initial activation 与 replacement activation 使用不同 purpose；recovery grant 与 normal activation不能互相重放。exact control endpoint、auth plugin/mTLS、authority reference/certificate、本地受保护存储，以及未来跨instance credential/KMS rotation协议保持开放；首版同instance原地rotation已明确禁止。

INSTALL 的最小验证与 durable effect 为：验证 caller non-anonymous且被授权执行exact INSTALL/ledger instance/target scope；cold-read exact PREPARING/descriptor authority；校验 ledger/instance/schema/hash/Engine/capability/target/incarnation/operation identity/generation/payload；建立 protected auth binding；再以一个本地 conditional durable transition原子写入 route、instance、descriptor、Engine、auth、install generation和normal-inactive。ACTIVATE 必须重新验证 caller对exact ACTIVATE/ledger instance/target scope的授权，并 direct-read exact READY或post-membership authority，校验target/incarnation、local inactive state、purpose、generation、fence与tombstone，再 durable active。任一 response loss只查询/重试相同 operation并重读authority；不能回退Classic、双写或盲建新 generation。

receipt 只能公开 secret-free identity/result：ledger/instance/descriptor identity、target stable identity/incarnation、Engine/capability result、local generation、operation identity和durable result。它不得包含master key、password、verifier、可离线验证的unkeyed digest、bearer token或可重放控制能力。

## 7. Bookie 安装语义

候选请求：

```text
INSTALL_LEDGER_PROFILE {
    ledgerId
    ledgerInstanceId
    descriptor
    descriptorHash
    installRequestId
    authorizationOrRequestCorrelation
    protectedCredentialOrProof
    expectedBookieEngineProfile
}
```

Bookie 必须按顺序完成：

1. 在分配前验证请求 hard bounds、canonical schema/hash identity、unknown mandatory semantics，以及caller non-anonymous并被授权执行exact INSTALL/ledger instance/target scope；
2. cold-read并验证exact committed PREPARING/descriptor authority，绑定target Bookie stable identity/storage incarnation与operation identity/generation；
3. 验证 active Engine Profile 和全部 mandatory capability；
4. 对 ledgerId 的 authoritative route slot 执行单一原子 claim；
5. 检查 instance、descriptorHash、purpose/generation 和 protected auth binding 冲突；
6. durable 写入 route、Profile reservation/install、受保护认证状态、install generation与normal-inactive；
7. 建立可从 durable authority 恢复的 routing entry；
8. 返回 secret-free durable receipt。

仅在第 4-7 步的 durability barrier 完成后返回成功。内存对象创建不能替代 durable install。exact record packing 和是否合并为一次 group commit 由实现与 Spike 决定，不要求“一种语义一条记录”。

### 7.1 Authoritative route claim

每个 ledgerId 在一个 Bookie 上只有一个 authoritative route slot：

```text
ABSENT
CLASSIC
PROFILE(ledgerInstanceId, descriptorHash, lifecycleState)
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
| 相同 instance/hash/request | 重试 | 返回相同语义 receipt |
| 相同 instance/hash，不同 request | 等价重试 | 允许，receipt 绑定原 install generation |
| 相同 instance，不同 hash | 冲突 | `EPROFILE_MISMATCH` |
| 相同 ledgerId，不同 live instance | 冲突 | `ELEDGERINSTANCE_MISMATCH` |
| engine 不匹配 | 冲突 | `EUNSUPPORTED_STORAGE_ENGINE` |
| mandatory capability 缺失 | 冲突 | `EUNSUPPORTED_LEDGER_CAPABILITY` |

冲突不能通过删除本地状态或退回 Classic 自动修复。

## 8. AddRequest 身份

所有需要安装的新 Profile normal Add 必须使用 distinct Profile logical operation，而不是现有 legacy `ADD_ENTRY` 的 optional 字段。其logical binding至少覆盖以下语义；activation/admission context可以由request携带，也可以由已协商channel或server local handle捕获，不锁定一个显式per-request generation wire field：

```text
ledgerId
ledgerInstanceId
profileDescriptorHash
current route/admission context identity or generation
entryId
masterKey/authentication data
body
writeFlags
```

exact wire fields保持开放；不可删要求是stale request/channel/handle context不能跨instance、activation、fence或admission generation成功。

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

新 Profile 请求缺失已冻结 activation/auth 机制要求的 identity 字段时必须拒绝。normal Add 必须匹配 durable normal-active role；RFC-0004/0005 定义的 recovery Add 使用 distinct Profile recovery logical operation，匹配 exact instance/descriptor/auth、RepairIntent admission与bounded local grant、target/incarnation/range/grant generation、delete/tombstone和payload identity；legacy recovery flag不足以取得该权限，也不能借 recovery grant 获得 normal writable。

普通 Add 不携带完整 descriptor、Engine/capability vector、READY certificate或per-request nonce，不做canonical hash、auth-binding hash/HMAC、MetadataStore read、KMS/signature/certificate verification或control fsync；只对已验证缓存的固定长度descriptor/auth identity或verifier做bounded comparison，再执行local route/active/fence/admission-generation与payload检查。携带data credential的Profile transport仍必须满足manifest confidentiality/integrity要求。具体 Profile opcode number、framing bytes、proof/reference表示与error code保持开放。Classic 请求仅在 route 为 `ABSENT/CLASSIC` 时走现有路径，命中 `PROFILE/TOMBSTONED` 时必须在创建 Classic handle、persist master key或payload前拒绝。

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

仅 `BK_SEQUENCED_CLASSIC_CLIENT_ONLY` 可以使用旧 Bookie，并且 sequence envelope 只作为 opaque payload。它不发送 install 请求，也不能要求 server-side hash、epoch、index 或 tail-summary 语义。

任何需要 Bookie 执行 Profile safety semantics 的 create/open/install在发现旧 Bookie、缺少 mandatory handshake/capability或mixed ensemble时必须在payload前失败；不得把Profile请求改写成Classic、重试legacy opcode或双写。

### 11.2 新 Profile + 不支持 install 的 Bookie

placement 阶段排除；若仍被选中则创建失败。不得静默降级。

### 11.3 旧 client + 新 Bookie

旧 Classic 请求在 `ABSENT/CLASSIC` route 上维持现有行为。命中 `PROFILE/TOMBSTONED` route 时，normal 与 recovery Add 都必须在 Classic lazy-create 前 fail closed。具体 deleted/profile-required/fenced error code 由 wire 评审冻结。

### 11.4 Profile wire discriminator 与 downgrade boundary

Profile safety semantics不得通过现有`ADD_ENTRY`的optional protobuf字段或legacy recovery flag表达。wire必须存在mandatory Profile discriminator，使任何受支持旧decoder都不能把合法、损坏、截断、unknown-version或unknown-subtype Profile bytes解释为legacy `ADDENTRY/RECOVERY_ADD`。Profile连接一旦识别该framing，parse/version/subtype错误必须关闭或明确拒绝，不能触发现有v3→pre-v3 decoder fallback并用相同bytes重解释。client遇到unsupported、response loss或durability unknown也不得自动Classic fallback或双写。

logical operation最少区分：

```text
cold control: INSTALL/STATUS, ACTIVATE/STATUS, RECOVERY_GRANT/CLOSE,
              TOMBSTONE/DELETE_CONSUMPTION
data:         PROFILE_ADD_NORMAL, PROFILE_ADD_RECOVERY, POINT_READ,
              FENCE, READ_LAC, WRITE_LAC, FORCE/LIST or explicit reject
optional:     BATCH/RANGE/RECOVERY_ADD only behind mandatory capability
```

物理协议可以使用一个versioned Profile control envelope、一个data envelope及mandatory subtype，也可以选择等价无歧义framing；exact opcode/framing保持开放。Spike A必须把raw byte corpus同时投喂真实受支持old v2/v3 decoder，证明无Profile byte sequence产生Classic route claim、handle/master-key persistence、payload write或ACK。

### 11.5 Mixed/rolling matrix 与 semantic errors

最低matrix固定为：old client+old Bookie的Classic不变；old client+new Bookie仅在`ABSENT/CLASSIC`接受；new Profile client+old Bookie拒绝且不降级；Profile create/open遇mixed ensemble失败；Bookie restart/incarnation或protocol generation变化后重新handshake；unknown Profile version/subtype拒绝且无effect；任何Profile bytes在旧Bookie上都不形成legacy Add。registration/capability hint、connection handshake和durable install receipt是三层证据，任一层不能替代下一层。

wire/client/admin至少保留以下semantic error class，exact numeric/code/legacy projection保持开放：unsupported protocol/op/capability/Engine；identity conflict；not-ready/not-active/stale generation；fenced/tombstoned/deleted；recovery grant missing/stale/out-of-scope/closed；transient/read-only/overload/reconciling；durability unknown/response loss；unknown mandatory/corrupt/quarantine。外部unauthorized响应可以coarse，但内部不能把上述安全分类统一压成generic unauthorized；processor、callback/future、admin status和bounded metrics必须端到端保留足以做fail-closed retry的class。

## 12. Capability negotiation

Bookie registration 至少发布：

```text
bookieStableIdentity
storageIncarnation
engineProfile
protocolGeneration
protocolVersions[]
mandatoryCapabilities[]
readinessGeneration
limitsDigest or bounded limits
```

placement 只把满足 descriptor mandatory requirements 的 Bookie 作为候选。注册信息只是选择hint；**准备发送Profile operation的Profile-capable connection**必须在连接上协商stable identity/incarnation/Engine/protocol generation/mandatory capabilities/readiness generation，restart或generation变化后重新协商；最终仍以目标 Bookie exact durable install/activation receipt为准。legacy Classic connection不要求、也不能被新Profile handshake阻断；共享pool同时承载两类流量时，Profile operation只能使用已经协商的channel/context。协商按Profile connection摊销，不在每个Add发送完整capability vector或查询registration。

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

## 14. Spike 与接受 Gate

RFC 进入 Accepted 前必须：

- [Spike A](spikes/SPIKE-A-profile-install.md) 全场景通过；
- Model A 包含创建、安装、response loss 与 ensemble replacement 的抽象；
- canonical descriptor/hash 与未知字段规则冻结；
- golden corpus覆盖same-semantics稳定bytes/hash、different-semantics identity变化、duplicate/default/order、unknown schema/field/enum/mandatory capability、optional hint hash不变、oversize-before-allocation、old writer rewrite/strip和declared hash mismatch；
- hash manifest拒绝CRC/checksum、非固定长度或不满足对抗输入collision-resistance最低属性的candidate；
- protected auth binding 经过安全评审且不会泄漏可离线验证的 credential material；
- non-anonymous且exact operation/instance/target scope authorized control principal、Bookie direct-read committed authority、initial/replacement purpose separation、target/incarnation binding、secret-free receipt、response-loss status query，以及authenticated-but-unauthorized/`AuthDisabledPlugin`负向路径通过测试；
- master key/proof/capability在日志、metric、receipt、exception中的hard leak regression通过，包括access mismatch路径；
- credential-bearing Profile control/data transport的confidentiality/integrity验证通过，且normal Add auth-binding hash/HMAC/KMS/signature/certificate invocation计数为0；
- sidecar/backlink ABA、initial route-first publication、READY/availability 与 profiled metadata mutation authority 经过安全评审；
- domain-specific single-record CAS adapter、store-version/semantic-generation 分离、bounded root/page、unknown mandatory version 与 snapshot publish-before-reclaim通过 fault/compatibility测试；
- same operation identity的same/conflicting payload在response loss、snapshot吸收与bounded retention边界通过幂等/冲突测试；
- orphan install 回收合同冻结；
- protocol error mapping 和 mixed-version matrix 有确定测试；
- raw Profile wire corpus在真实受支持old v2/v3 decoder上证明合法/损坏/截断/unknown request均不产生legacy Add/RECOVERY_ADD effect，且Profile parse error不触发legacy fallback；
- per-connection handshake、restart/incarnation重新协商、unknown subtype、mixed ensemble、response loss no-downgrade与semantic error端到端传播通过测试；
- route claim、legacy normal/recovery Add 与 activation gate 经过并发、restart 和源码评审；
- Classic-only throughput/p99 与 Profile Add CPU 成本证明 routing gate 未引入远程 I/O或不可接受回退。

任一场景出现未安装 Add 被接受、mismatch 静默降级或 metadata 先于 replacement install 生效，RFC 保持 P0 Blocked。

## 15. 开放问题

- canonical serialization、hash 算法和 descriptor version negotiation；
- canonical codec/field numbers、hash suite/length/domain-separator bytes、descriptor/parser hard bounds、optional hint envelope与最终safety-limit字段表；
- sidecar exact path/backend adapter、root/child/page schema、field numbers、状态名与 immutable backlink encoding；
- authority family/domain sharding、root/page/fan-out hard bounds、snapshot manifest bytes、compaction threshold与retention数值；
- checksum/hash、watch/cache策略，以及backend内部是否使用ZK multi-op/etcd transaction；
- control endpoint/auth plugin、credential-bearing Profile control/data transport的exact TLS/SASL/mTLS机制、cold authority reference/certificate、protected local binding物理表示、exact binding fields 与 profiled metadata mutation ACL/credential enforcement；
- receipt 的持久化位置、压缩和审计方式；
- protected auth binding、install control record 与现有 master-key persistence 的整合；
- 未来跨instance credential/KMS rotation的协议、版本、迁移与兼容边界；首版同instance原地rotation不是OPEN；
- create cancellation、orphan install tombstone 与 GC；
- Bookie registration capability 的刷新与降级行为；
- CAS→activation gap 的 exact error/retry/backoff、availability completion 与 partial-activation credential distribution；
- write-time inactive orphan/possibly-activated target 的 GC state machine；
- ledgerId reuse 最终策略，以及 instance 分配 authority；
- Profile opcode number/framing bytes、connection handshake exact fields、unknown operation行为、exact BKException/admin/error mapping，以及batch/range/recovery wire schema；
- 哪些 limit 影响跨实现安全语义、哪些只属于 runtime admission policy。
- failure-domain exact定义、domain identity来源、默认 `F`、policy evolution 与 compatibility rules。

这些问题关闭并通过 Gate 前，本 RFC 不得标为 Implementation Ready。
