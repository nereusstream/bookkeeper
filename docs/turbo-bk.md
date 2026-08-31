# BookKeeper Unified WAL 总体架构 RFC

> 状态：**Proposed / Not Implementation Ready**<br>
> 文档类型：总体架构、边界与依赖导航<br>
> 更新时间：2026-08-30<br>
> 结论：保留 Unified WAL 方向；除 `BK_CLASSIC_TUNED` 外，不授权按本文直接进入正式实现。

## 1. 决策摘要

本文取代此前的“Profile 化最终详细设计方案”定位。此前草稿形成了较完整的设计地图，但 Profile 安装、allocator authority、规模化资源模型、旧 Bookie stale-writer 合同和集群级删除仍未闭合，不能作为实现合同。

当前决策如下：

1. `BK_CLASSIC` 是现有 OSS 基线；`BK_CLASSIC_TUNED` 可以立即进入基准和低风险优化。
2. `DirectJournal` 是 Bookie 进程或部署 cohort 级 Engine Profile，不是任意 ledger 可切换的属性。
3. `BK_SEQUENCED_CLASSIC_CLIENT_ONLY` 必须通过“fence 旧 ledger、recovery、seal、发布 successor ledger”完成 takeover；旧 Bookie 不理解 `writerEpoch`。
4. Segment WAL 在 Profile install/activation、`ArenaControlLog`、冷热混合 allocator、Segment Bookie state/ACK authority 和无对象存储形式化模型闭合前保持 **P0 Blocked**。
5. Conditional delete 是集群协议，必须覆盖历史 ensemble、离线 Bookie、持久 tombstone、AutoRecovery 竞争和 Bookie rejoin barrier。
6. 当前 OSS 已有 bounded Batch Read；新增范围是 streaming continuation、一般 E/W/A 合并、恢复语义、QoS 和 cancellation。
7. BtrLog 只提供设计启发。它的对象存储恢复假设不能证明本方案安全。

本文已冻结Round 7 reference/test manifest，但不把未经真实old decoder/stock binary验证的candidate宣称为stable wire/on-disk compatibility contract，也不冻结仍由Spike选择的physical owner、阈值或实现类。

### 1.1 单一基线持续演进

Unified WAL只维护一套当前设计与实现基线，不建立项目内部的代际产品线、并行合同或版本化类型后缀。后续改造直接修改本文、owner RFC、Spike与当前实现；新增能力直接加入当前capability set，并在同一变更中同步更新Gate、兼容矩阵和证据要求。

`codecVersion`、`semanticSchemaVersion`、`protocolMajor/minor`及hash domain separator中的数值仅用于wire/format字节判别和fail-closed兼容，不构成项目代际。确需不兼容字节改造时，必须在当前合同中同步修改technical discriminator、migration/rollback边界与raw corpus，不能另建并行代际实现线。历史grill归档为保证reviewer反馈逐字完整，可能保留现已废弃的内部代际措辞；这些措辞不再是实现命名或规范来源。外部固定标识如旧BookKeeper v2/v3协议、TLS 1.3和SHA-1/SHA-256算法名不受此规则影响。

## 2. 文档边界

本文只负责：

- 目标与非目标；
- 三层 Profile 模型；
- 合法组合与状态；
- 模块关系与跨模块不变量；
- 子 RFC、Spike 和实施阶段的依赖；
- Gate 的证据结构。

实现合同拆分到以下子 RFC：

| 文档 | 负责范围 | 当前状态 |
| --- | --- | --- |
| [RFC-0001：Profile、Capability 与安装协议](rfcs/unified-wal/RFC-0001-profile-capability-install.md) | Engine/Ledger/Client Profile，显式安装，capability，ensemble replacement | Proposed / P0 prerequisite |
| [RFC-0002：Sequenced WAL](rfcs/unified-wal/RFC-0002-sequenced-wal.md) | sequence、appendId、ordered frontier、stale writer、successor ledger | Proposed |
| [RFC-0003：Segment Storage 与 Allocator](rfcs/unified-wal/RFC-0003-segment-storage-allocator.md) | `ArenaControlLog`、slab/extent、generation、崩溃恢复、资源模型 | Proposed / P0 prerequisite |
| [RFC-0004：Batch/Range、Recovery 与 Delete](rfcs/unified-wal/RFC-0004-range-recovery-delete.md) | OSS BatchRead 基线、streaming range、TailSummary、一般 E/W/A、集群删除 | Proposed |
| [RFC-0005：Segment Bookie State 与 ACK Authority](rfcs/unified-wal/RFC-0005-segment-bookie-state.md) | Segment operation、activation/fence、recovery Add、explicit LAC、restart 与 local success | Proposed / P0 prerequisite |

逐轮 P0 合同审查及固定 reviewer 的完整反馈见 [grill 记录](rfcs/unified-wal/grill/README.md)。

否证型 Spike：

| 文档 | 要否证的风险 | 当前状态 |
| --- | --- | --- |
| [Spike A：Profile 安装协议](rfcs/unified-wal/spikes/SPIKE-A-profile-install.md) | 未安装或错误 Profile 被静默接受，replacement 顺序错误 | Planned |
| [Spike B：Allocator 与 Block 原型](rfcs/unified-wal/spikes/SPIKE-B-allocator-block.md) | double allocation、stale-generation read、100k ledger 资源失控 | Planned |
| [Spike C：无对象存储 TLA+](rfcs/unified-wal/spikes/SPIKE-C-no-object-tla.md) | crash、response loss、takeover、reuse、offline delete 下的安全性 | Planned |

`Proposed` 只表示问题和候选合同已记录，不表示设计已接受、实现已就绪或生产可用。

## 3. 目标与非目标

### 3.1 目标

- 在不破坏 BookKeeper quorum、fencing、recovery 和兼容边界的前提下，为多类 WAL 工作负载提供可选择的优化路径。
- 将存储引擎、ledger durability 合同和上层 sequence 语义分离。
- 使所有新格式和新协议 fail closed；不得静默回退到 Classic lazy-create。
- 将 ACK authority、allocator authority、derived index 和集群 metadata authority 明确分层。
- 用数值 Gate、故障注入和独立形式化模型决定是否进入下一阶段。

### 3.2 非目标

- 不用一个新引擎替换所有 BookKeeper 使用场景。
- 不把 Kafka、Pulsar 或数据库的事务与可见性语义下沉到 BookKeeper Core。
- 不承诺所有 ledger 都能零 compaction 回收。
- 不把 RocksDB 作为已 ACK payload 或 allocator ownership 的唯一权威。
- 不把 BtrLog 的 blob-store 模型或验证结论直接迁移到无对象存储设计。
- 不在本文中承诺一般 E/W/A 快速恢复、same-ledger epoch takeover 或生产默认切换。

## 4. 当前 OSS 基线

后续评审必须以实际 OSS 能力为基线，不能把已有功能描述为不存在，也不能把受限功能描述为一般能力。

### 4.1 Bookie handle 与 Journal

- 当前 [`HandleFactoryImpl`](../bookkeeper-server/src/main/java/org/apache/bookkeeper/bookie/HandleFactoryImpl.java) 根据 `ledgerId + masterKey` 懒创建 handle；它不读取完整 Ledger Metadata，也没有 ProfileDescriptor 输入。
- 当前 [`BookieImpl`](../bookkeeper-server/src/main/java/org/apache/bookkeeper/bookie/BookieImpl.java) 将多个 ledger 路由到进程内 Journal。当前 `DirectJournal` 合同因而只能作为 Bookie/cohort 级引擎部署。
- 仅在 Ledger Metadata 增加 `storage_format` 不能让 Bookie 获得 fail-closed 的 per-ledger 路由合同。

### 4.2 Batch Read

- 当前 [`BookkeeperProtocol.proto`](../bookkeeper-proto/src/main/proto/BookkeeperProtocol.proto) 已定义 `BATCH_READ_ENTRY`；Range Add/Read 仍标为未支持。
- [`ReadHandle`](../bookkeeper-server/src/main/java/org/apache/bookkeeper/client/api/ReadHandle.java) 已公开 bounded batch read；[`LedgerHandle`](../bookkeeper-server/src/main/java/org/apache/bookkeeper/client/LedgerHandle.java) 当前只在 `E = W` 且能力启用时走 batch fast path，否则回退。
- 新设计不得再把 Batch Read 描述为“完全不存在”。

### 4.3 Deferred Sync

- [`LedgerHandle`](../bookkeeper-server/src/main/java/org/apache/bookkeeper/client/LedgerHandle.java) 中的 `DEFERRED_SYNC_LEGACY` 是现有能力，但 failed Bookie 场景下不能按一般 HA WAL 假设执行正常 ensemble change。
- 当前 Sequenced WAL 只允许 `SYNC_ON_ACK`。
- 如果可换 ensemble 的 deferred durability 得到完整证据支持，直接修改当前 durability 合同并同步更新 failure detection、replacement、recovery、response-loss 与 Gate；不另立并行代际合同。

## 5. 三层 Profile 模型

### 5.1 Bookie Engine Profile

由 Bookie 进程配置和部署 cohort 决定：

```text
CLASSIC_ENGINE
DIRECT_JOURNAL_ENGINE
SEGMENT_WAL_ENGINE
```

当前合同使用独立 cohort，不支持一个 Bookie 进程内按 ledger 动态混用 Classic Journal、Direct Journal 和 Segment WAL。

### 5.2 Ledger Contract Profile

定义 ledger 在已选 Engine 上要求的持久化合同：

```text
OPAQUE_LEDGER
SEQUENCED_LEDGER
SYNC_ON_ACK
DEFERRED_SYNC_LEGACY
TAIL_SUMMARY
CONDITIONAL_DELETE
```

需要 Bookie 执行的新合同必须显式安装；仅由客户端解释的合同不冒充 Bookie 能力。

### 5.3 Client/Protocol Profile

由客户端或上层协议负责：

```text
SequenceDomain
appendId
ordered durable frontier
protocol-native position
transaction and visibility state
```

旧 Bookie 可以把 sequence envelope 当作 opaque payload，但不能据此宣称支持 server-side epoch rejection、sequence conflict detection、read pushdown 或 tail summary。

### 5.4 Canonical 组合

| Canonical Profile | Engine | Bookie 可见的 ledger contract | Client/Protocol contract | Durability |
| --- | --- | --- | --- | --- |
| `BK_CLASSIC` | `CLASSIC_ENGINE` | 现有 Classic | opaque | 现有模式 |
| `BK_CLASSIC_TUNED` | `CLASSIC_ENGINE` | 与 Classic 相同 | opaque | 与基线匹配 |
| `BK_DIRECT_JOURNAL` | `DIRECT_JOURNAL_ENGINE` | 当前仍为 opaque ledger | opaque | `SYNC_ON_ACK` |
| `BK_SEQUENCED_CLASSIC_CLIENT_ONLY` | `CLASSIC_ENGINE` | 仍为 Classic opaque payload | sequence、appendId、ordered frontier | `SYNC_ON_ACK` |
| `BK_SEQUENCED_CLASSIC_INSTALLED` | `CLASSIC_ENGINE` | 显式安装的 sequence capabilities | sequence、appendId、ordered frontier | `SYNC_ON_ACK` |
| `BK_SEGMENT_WAL` | `SEGMENT_WAL_ENGINE` | 显式安装的 Segment contract | opaque | `SYNC_ON_ACK` |
| `BK_SEQUENCED_SEGMENT_WAL` | `SEGMENT_WAL_ENGINE` | Segment + installed sequence capabilities | sequence、appendId、ordered frontier | `SYNC_ON_ACK` |

每个需要Bookie执行的新组合由immutable semantic `ProfileDescriptor`定义。当前codec使用`BKPD` strict flat big-endian TLV、十个mandatory fields，合法长度精确为`124 + 6 * capabilityCount`（124..508 bytes、0..64 capabilities），1024 bytes仅是allocation前绝对input hard cap；JDK SHA-256 suite 1 identity为suite/schema加32-byte digest共36 bytes。duplicate/out-of-order/missing/unknown/default alias/509..1024-byte非法编码/trailing bytes全部拒绝。index/sequence/recovery/delete只进入mandatory capability registry，不维护第二份profile字段。failure-domain/capability registry与production legal-combination table仍BLOCK；codec/hash本身可开始reference implementation，normal Add不解析或hash descriptor。

Wave 0 的 `ProfileDescriptor` reference implementation 已落在 `bookkeeper-common` 内部 unstable 包，并提交 byte-exact valid/invalid corpus、独立 verifier、expected identity/field dump、checksums 与 fixed-seed fuzz receipt。该实现没有网络、磁盘、MetadataStore、registration 或 ACK 接入，registry 只有 test fixture；详见 [`implementation/profile-descriptor/`](rfcs/unified-wal/implementation/profile-descriptor/README.md)。这只关闭 reference implementation 交付，不接受 production registry/legal combinations，不提升本 RFC 或任何 Spike/Gate 状态。

Profile control plane与20-byte master-key data credential分离。所有Profile control/data只走独立`bookie-profile` immediate-TLS1.3/mTLS endpoint，Classic endpoint/pool无Profile handshake；mTLS X509 principal还必须通过exact operation/ledger instance/target scope authorizer，目标Bookie冷路径direct-read committed authority后才durable local binding。普通Add只解析fixed header/60-byte ledger context，constant-time比较缓存的36-byte identity与20-byte verifier并检查active/fence；MetadataStore、descriptor/auth hash、KMS、certificate/signature、control fsync均为0。principal config、physical local owner/packing/at-rest protection仍BLOCK；不引入descriptor签名、Merkle、PKI扩张、per-Add proof/nonce或key rotation state machine。

Wave 0 已在 `bookkeeper-common` internal unstable package完成Profile control/auth typed interfaces与隔离endpoint reference implementation。它把immediate TLS1.3/mTLS transport facts、static precheck、fixed-key single cold authority read、exact post-read AuthZ、strict tuple/descriptor/Engine/credential检查、conditional semantic transition与durable-only secret-free result串成12项普通功能测试；不监听socket、不终止TLS、不实现physical protected store，也没有production startup、route、normal Add或ACK调用点。该结果只验证typed reference semantics；real TLS/listener/store、secret-leak完整矩阵、A24、stable wire、G1与production authority仍OPEN/BLOCK，详见 [`implementation/profile-control/`](rfcs/unified-wal/implementation/profile-control/README.md)。

Round 7 原始 wire candidate `0x0FFE4250` 已被真实 Apache BookKeeper 4.14.8 decoder 的 `magic-flip-1` 反例否证并冻结；当前 replacement candidate 为 `0x0FF04250`。它继续使用TLS后4-byte length + 32-byte header、byte-exact HELLO、distinct control/normal/recovery/read/LAC subtypes和12类status（1 OK + 11 non-OK）；range/batch在当前manifest中reserved/disabled。新 candidate 仍只授权reference codec与raw corpus，不是stable wire：每个受支持old decoder/binary对TLS ClientHello及合法/损坏/截断/fuzz bytes的Classic route/handle/master-key/allocation/journal/payload/ACK effect都必须为0，任何失败不得Classic fallback或双写。当前 Wave 0 不执行这层 released-decoder/stock-binary 矩阵，状态为`DEFERRED_NOT_RUN`而不是PASS/waiver，G1保持`BLOCKED_UNVERIFIED`。

same BookieId/same storage scope的old-binary fence仍BLOCK；`BKPF1` nonnumeric Cookie sentinel只是Spike B candidate。若无法证明hook早于任何Profile storage open，当前合同必须使用new BookieId + new journal/ledger/index/Arena roots + new storage incarnation +旧service不可访问的credential/ACL scope。persistent readiness CAS先于BookieServiceInfo/ephemeral registration；Arena superblock不能替代Bookie fence，format/readiness检查不进入Add。

### 5.5 非法或当前禁止组合

- 在 Classic cohort 中按单 ledger 切换 DirectJournal；
- Segment ledger 未完成 RFC-0001 install 即接受 Add；
- client-only Profile 声称具有 Bookie epoch/hash/index 能力；
- `DEFERRED_SYNC_LEGACY` 与当前 Sequenced WAL 自动 failover；
- 旧 Bookie 上的 same-ledger epoch takeover；
- 在独立 fencing 合同前启用 pooled ledger lane。
- 把Profile safety fields作为legacy `ADD_ENTRY` optional字段，或让legacy recovery flag取得Profile recovery grant；
- Profile失败、response loss、unknown version/capability后自动Classic fallback或双写；
- 使用anonymous/AuthDisabled control caller、master key、descriptor hash或公开receipt授权Profile control operation；
- 在同一ledger instance内修改descriptor/auth binding，或在unsafe Segment authority存在时直接rollback到old binary。

## 6. Profile 状态

| Profile | 状态 | 进入下一状态的前置条件 |
| --- | --- | --- |
| `BK_CLASSIC` | OSS Existing | 维护兼容基线 |
| `BK_CLASSIC_TUNED` | Implementation Ready | 锁定可复现 benchmark manifest |
| `BK_DIRECT_JOURNAL` | Spike Ready，Engine/cohort 级 | 独立 cohort 原型与崩溃测试 |
| `BK_SEQUENCED_CLASSIC_CLIENT_ONLY` | RFC Required | 接受 RFC-0002 与 Model B |
| `BK_SEQUENCED_CLASSIC_INSTALLED` | Blocked | 接受 RFC-0001/0002，Spike A 通过 |
| `BK_SEGMENT_WAL` | P0 Blocked | RFC-0001/0003/0005 接受，Spike A/B/C 通过 |
| `BK_SEQUENCED_SEGMENT_WAL` | P0 Blocked | Segment authority 与 Sequenced WAL 分别通过 Gate |
| `DEFERRED_SYNC_LEGACY` | Existing with HA Restriction | 不作为当前 WAL 默认 |
| `GENERAL_EWA_FAST_RECOVERY` | Research/Spike | RFC-0004 和 Model E |
| `POOLED_LEDGER_LANE` | Deferred | 独立 fencing 与资源合同 |

状态不能从“代码存在”“进程跑完”或单次性能改善推断。提升状态必须同时具备接受的合同、可追溯证据和相应 Gate 结果。

## 7. 总体模块关系

```text
Client / Protocol Adapter
    │ sequence, appendId, ordered frontier
    ▼
Standard LedgerMetadata + Profile Sidecar Coordinator
    │ OSS state/membership CAS + instance/READY/operation authority
    ▼
Bookie Cohort selected by Engine Profile
    ├── CLASSIC_ENGINE
    ├── DIRECT_JOURNAL_ENGINE
    └── SEGMENT_WAL_ENGINE
            ├── ArenaControlLog      allocation/generation/relocation-selection authority
            ├── Data Arena          payload authority
            └── Derived Index       rebuildable acceleration

Cluster Lifecycle
    ├── AutoRecovery + durable RepairIntent
    └── LedgerDeleteCoordinator
```

权威层次必须保持：

- Standard LedgerMetadata：OSS state 与 ensemble membership 的唯一权威。
- Profile sidecar namespace：通过 domain-specific `ProfileControlStore` 的单-record versioned CAS、bounded domain heads与snapshot+complete suffix，承载 ledger instance、immutable descriptor、READY/activation generation、有界 repair/delete operation、accepted loss ordering 与 distributed verifier strong completion；store version不替代semantic generation，不复制 membership，也不形成ledger-global universal hot head或无界 root blob。
- BookKeeper quorum：entry 是否达到 ACK quorum 的权威。
- `ArenaControlLog`：以per-Arena deterministic conditional state machine承载空间 ownership、generation、reuse 与同 Arena relocation selection；只有覆盖transition自身sequence的durable result才授予cutover/free/reuse。checkpoint 中的 current selector、anti-ABA 和未退休状态必须等价于完整 move-chain oracle，derived locator 不能决定 move winner。
- Data Arena：Segment payload 的权威。
- RFC-0005 Segment Bookie state：逻辑 `LedgerRouteAuthority` 与 `BookieRegistrationAuthority` 决定install/normal admission/fence、bounded recovery grant/readable facts及assignment/incarnation readiness；normal、recovery与readable不是互斥flat role，物理owner仍由Spike选择。
- RocksDB、footer、cache：可丢弃并重建的派生加速结构。

## 8. 跨 RFC 安全不变量

1. 新 Profile ledger 必须先在 planned initial ensemble 的全部 E 个 Bookie durable claim inactive Profile route，再创建带 immutable instance backlink 的标准 LedgerMetadata。
2. `ACK(normal profiled Add)` 之前，matching global READY 与目标 Bookie durable local normal activation 均已成立；普通 Add 不远程读取 MetadataStore。
3. Classic/Profile/Tombstoned route 是单一、原子、可恢复的本地 claim；legacy normal/recovery Add 不能绕过 Profile route。
4. 写期 replacement 按 inactive install → `LAC+1` membership CAS → normal activation → pending resend 排序，不复制历史 fragment。
5. ACKed data 的物理空间必须已有 durable allocation authority。
6. 同一 slot/extent generation 不得同时属于两个 ledger instance。
7. `FREE` 或 generation bump 未 durable 前，空间不得复用。
8. 旧 generation locator 永远不能读取新 generation payload。
9. BookKeeper AQ evidence 与上层 WAL COMMITTED 分离；published WalSequence 只推进连续前缀。
10. successor ledger 从 predecessor durable sealed prefix `P + 1` 开始；sealed prefix 外 AQ candidate 永远不能进入 WAL COMMITTED。
11. logical delete 后 ledger 不能重新 open；Bookie 应用缺失 tombstone 前不能重新成为 writable。
12. AutoRecovery target 接收第一份 durable payload 前必须有可由 delete freeze 枚举的 RepairIntent；recovery-only/committed-readable 不授予 normal writable。
13. 标准 ensemble membership由LedgerMetadata CAS串行；sidecar只让`DELETE_INTENT`/delete fence与RepairIntent admission等真正冲突的transition共享ledger-instance lifecycle cut。未admit intent不授予grant/payload；cut前全部admitted intent进入frozen history。admission后的repair progress/loss/receipt/completion使用owning domain head，不推进universal ledger CAS，任一中间态fail closed。
14. permanent-loss 保证只在声明的 distinct failure-domain 预算与 repair window 内成立；无有效 evidence 时 recovery 永不返回成功。
15. derived index 全部删除后，系统仍可从权威 payload 和控制元数据恢复。
16. 同一 Arena compaction 只有 durable conditional `MOVE_COMMIT` 能切换 locator authority；old free 晚于 cutover、new-pin 阻断、reader drain 与 durable generation bump，cross-Arena move 当前 unsupported。
17. range/TailSummary/BatchRecovery fast path 的 unsupported、stale、partial 或局部预算耗尽必须从 earliest unresolved coordinate 回退；不能越过 hole，deadline/cancellation 不伪造 DATA_LOSS。
18. offline rejoin 使用 cluster-authoritative finite stream assignment、storage incarnation、no-hole per-stream cursor 与 snapshot+complete suffix；per-ledger `deleteEpoch` 不能兼任 catch-up watermark。
19. permanent-loss budget 只能对有完整 `F+1` distinct-domain evidence 的 bounded range reset；membership、local durability、activation 或 generic repair COMMITTED 单独都不够。
20. verifier strong completion 必须长期绑定 exact range/context/membership、immutable Profile descriptor、当时的 `F`、failure-domain policy、control fence 和 overlapping-range predecessor；audit digest 单独不能 reset，compaction 后也不能在新 descriptor/`F` 下重解释。只要求冲突或重叠 range 的 accepted loss/completion 单序，已证明不相交的 range 不进入 ledger-global hot CAS。
21. obligation-changing delete assignment 先 PREPARED、catch up snapshot+complete suffix，再原子 effective；安全的旧 generation 不因 PREPARED 无条件 demote，但新 generation 生效后 stale writable authority 必须失效。snapshot 必须可遍历并应用 delete effects，不能只有 digest；catch-up exemption proof 必须绑定 Bookie、old incarnation、device/storage scope、operation generation 与 cluster acceptance，不能跨 scope 重放。
22. relocation orphan 只在 new allocation/location 未被任何 authority 依赖时回收；既存 logical success 继续由 old/current selector 承载。late `MOVE_COMMIT` 与 orphan `FREE_AND_BUMP` 由同一 Arena control order 条件互斥，cutover 不能早于覆盖该 move 自身 sequence 的 durable-through。
23. recovery 必须区分 durable recovered close、temporary deferred、attempt incomplete、quarantine/authority-unrecoverable 与 required-coordinate evidence exhausted；authority loss、timeout、offline 或 speculative suffix 不得伪造 payload `DATA_LOSS`，open-ledger normal tail 需要 fenced exact write-set 的 quorum-intersection absence proof。
24. sidecar store version与semantic generation分离；child/page只有被exact instance/domain head条件化发布后才有authority。compaction authority是committed snapshot+complete bounded suffix，publish/fallback proof先于reclaim；referenced unknown mandatory state、missing chunk或gap必须fail closed，普通Add不依赖sidecar读写。
25. per-Arena conditional predicate对committed/applied state原子求值，condition failure无effect，duplicate operation不能产生第二winner；durable result覆盖自身sequence。selector publish与block-new-old-pin形成同一cut，cut后stale locator不能取得old pin。
26. local route claim早于payload/handle lazy create；fence先关闭new admission并终结pre-cut Add，stale admission generation不能成功。tombstone原子收窄该instance的normal/recovery/read接受集合；registration readiness绑定incarnation、effective assignment和cursor/snapshot cut，不成为per-Add lease。
27. 只有matching durable ledger close可以投影legacy recovery `OK`；fragment/partial/skip不计ledger recovered，不清intent/marker。generic legacy rc不能抹除coordinator/admin rich outcome，AutoRecovery按rich class调度。
28. descriptor identity由canonical immutable safety semantics与固定长度、对对抗输入collision-resistant hash派生；CRC/checksum不能替代，hash只做identity/integrity；unknown/duplicate mandatory语义拒绝，optional neutral hint不改变接受集合。
29. master key只作为data credential；Profile control要求non-anonymous且获授权执行exact operation/instance/target scope的authority，AuthN-only不够；secret/offline verifier不得进入公开metadata、receipt、日志、metric或exception，credential-bearing Profile transport满足manifest confidentiality/integrity。
30. Profile normal/recovery使用distinct logical operation和mandatory wire discriminator；任何Profile bytes在受支持旧decoder上都不能形成legacy Add effect，任何失败都不Classic downgrade或双写。
31. Bookie/storage compatibility fence必须在stock old binary mandatory pre-replay path fail-stop；Cookie optional/version、registration、new superblock/file或unknown Journal record均不能单独替代。
32. startup按compatibility fence → required device/superblock → allocator/route/delete recovery → local readiness → registration排序；partial migration/unknown mandatory/incarnation mismatch保持non-writable。
33. 现存Segment/Profile local success或durability-unknown authority没有完整negative proof时不得rollback old binary；format/readiness检查不进入normal Add；只有准备发送Profile operation的Profile-capable connection执行一次Profile negotiation，legacy Classic connection不承担该handshake。
34. 当前descriptor input必须是严格canonical TLV并产生固定36-byte identity；未接受policy/capability registry时不得mint production descriptor。
35. Profile只走独立immediate-TLS/mTLS endpoint与pool；Round 7 frame bytes在raw old-decoder Gate PASS前只属于executable test manifest。
36. same BookieId/storage scope在stock binary pre-storage-open证据通过前保持BLOCK；失败时new BookieId/new roots/new incarnation/new credential scope fallback是当前唯一安全路径。
37. persistent readiness CAS先于ephemeral writable registration；generation/incarnation mismatch non-writable，registration hint不替代local receipt或old-binary fence。

任何子 RFC 或 Spike 发现这些不变量不可同时满足，都必须停止相应路径，而不是降低不变量。

## 9. BtrLog 证据边界

可借鉴：single-writer、client-driven quorum、bounded window、thread affinity、direct NVMe 和 tail recovery 的思路。

不可继承：依赖 durable blob store 的 flush、repair、recovery 路径及其已经验证的安全结论。

本方案至少需要五个相互可组合但独立检查的模型：

| 模型 | 范围 |
| --- | --- |
| Model A | BookKeeper E/W/A、write-set rotation、sidecar child/head/snapshot+suffix、store-version/instance ABA、descriptor/auth/wire compatibility abstraction、local route/admission/fence、install/activation、legacy routing、AQ、LAC、ensemble change、failure-domain loss、strong verifier completion 与 bounded receipt compaction |
| Model B | sequence、appendId、ordered completion、old-ledger fencing、successor ledger |
| Model C | `ALLOC/DATA/DELETE/FREE/MOVE_COMMIT`、conditional apply/durable result、generation、current-selector checkpoint、orphan cleanup、selector/pin cut、old-binary/format/migration/readiness abstraction 与 power loss |
| Model D | historical ensembles、partial delete、assignment handoff、applicable snapshot/incarnation、versioned registration readiness、offline rejoin、AutoRecovery race |
| Model E | TailSummary、required frontier、normal-tail proof、rich outcome/legacy projection、skip/marker handling、range fallback 与一般 E/W/A point-oracle equivalence；仅在推进该能力时必需 |

Model A-D 未通过前，Segment authority 不得进入 production candidate。

## 10. 实施顺序

```text
Stage 0  总体文档降级；建立五份子 RFC、三个 Spike 与逐轮 grill 记录
Stage 1  BK_CLASSIC / BK_CLASSIC_TUNED 可复现基线
Stage 2  Round 7 exact manifest：descriptor与control interface已冻结；wire为test candidate；
         same-scope format BLOCK，new BookieId/new scope fallback已冻结
         开始reference codec、harness与isolated prototype；不得发布stable wire/disk
         Spike A：Profile install、mTLS/exact AuthZ；raw old-decoder wire compatibility合同保留、当前执行延期
         Spike B：Allocator/block
                  + stock old binary pre-replay fence、partial migration/rollback
         Spike C：No-object TLA+
Stage 3  接受 RFC-0001/0002；实现 Sequenced Classic client-only
Stage 4  DirectJournal 独立 cohort prototype；不声称一次本地 payload 写
Stage 5  接受 RFC-0003；仅在Spike证据后进入isolated/discardable Segment shadow；
         live shadow仍需证明不污染Classic rollback cohort，Classic仍为ACK authority
Stage 6  接受 RFC-0005；完成 normal Add/fence local authority 的必要合同；具体 Segment authority canary 仍受 canary-specific evidence 与已启用路径依赖约束，不含 recovery/delete 集成
Stage 7  接受 RFC-0004；Streaming Range / Recovery / Delete
Stage 8  Sequenced Segment WAL、derived index、production canary
```

禁止跨越依赖：Round 7 test manifest不等于stable contract；raw decoder/stock old binary Gate未通过、physical owner未闭合时，不得写入稳定wire/disk compatibility surface。isolated shadow prototype完成不等于live shadow或Segment authority，Segment authority不等于Sequenced Segment WAL或production readiness。Stage 6 ACK authority仍要求RFC-0001/0003/0005接受、Spike A/B/C实际PASS和canary-specific ACK/fence/rollback evidence。

## 11. Gate 与证据合同

每个正式性能 Gate 必须在运行前冻结：

```text
reference commit
reference hardware
filesystem and mount options
NVMe model and firmware
JDK
Bookie configuration
E/W/A
entry size
concurrent ledger count
offered load
duration and warmup
fault injection points
numeric thresholds
stop conditions
```

在任何Profile/Segment稳定wire或format编码前，还必须冻结并归档：

```text
ProfileDescriptor BKPD strict TLV + ten fields + legal-124-plus-6N/124..508-byte + absolute-input-cap-1024-before-allocation + 0..64-capability bounds + SHA-256 suite 1/36-byte identity
bookie-profile immediate-TLS1.3/mTLS + exact operation/instance/target-scope authorizer + cold authority-read + protected kind-1/20-byte credential interface
0x0FF04250/32-byte-header/subtype/HELLO/status replacement executable wire manifest, explicitly not stable before complete released-decoder and localhost stock-binary corpus PASS
supported old client/Bookie exact commits and built artifacts + raw decoder corpus
same-scope BKPF1 candidate + pre-storage-open instrumentation + Cookie/superblock/device-manifest relationship
new BookieId/new roots/new incarnation/new credential-scope fallback
persistent readiness CAS-before-ephemeral registration + migration/reverse/wipe tooling + minimum reader/writer range
```

硬兼容证据包括：exact descriptor `.bin` golden vectors与独立SHA-256/identity复算；TLS/client-cert/exact AuthZ/anonymous/SASL-without-principal负向矩阵和secret-leak scan；TLS ClientHello及所有合法/损坏/截断/fuzz Profile bytes在真实old decoder/binary上不产生route/handle/master-key/allocation/journal/payload/ACK effect；Classic endpoint/pool无Profile TLS/HELLO；stock old binary在mandatory fence上于任何Profile storage open/replay/registration/write前退出且不stamp over sentinel；new-scope fallback拒绝old service credential；partial device/unknown format/stale persistent-vs-ephemeral readiness/unsafe rollback负向矩阵；startup bytes/I/O/device scaling；以及normal Add证明无remote read、format/readiness read、per-Add negotiation/descriptor/auth hash/KMS/certificate/control fsync。

首批共同矩阵：

- entry size：1 KiB、4 KiB、32 KiB；
- quorum：3/3/2、3/3/3；
- offered load：基线饱和点的 30%、60%、80%；
- ledger scale：1、1k、10k、100k idle/low-rate。

`BK_CLASSIC_TUNED` 的候选 promotion gate：

- matched throughput 下，p99 不高于 stock baseline 的 0.80，p99.9 不高于 0.85；
- 最大满足 SLO 的吞吐不低于 stock baseline 的 0.95；
- correctness、recovery 和兼容测试不得出现新增回归。

Segment production candidate 的最低 Gate：

- host payload write amplification 不高于 1.25x；
- 删除全部 RocksDB derived index 后可重建；
- allocator crash tests 为 0 safety violation；
- 100k idle ledger 无固定 extent reservation、无 per-ledger block buffer；
- background reclaim/compaction 使 foreground p99 回退不超过 5%。

候选阈值可以在正式测试前经一次评审调整；看到结果后不得追溯修改 Gate。

## 12. 当前开放问题

以下问题必须在对应 RFC 接受前闭合：

- failure-domain policy与mandatory capability ID/semantic-version registry、各production Profile exact combination、optional hint envelope；descriptor schema/hash后续改造必须直接修改当前合同并同步technical discriminator、migration边界与compatibility corpus，当前codec/hash/bounds不再OPEN；
- sidecar backend adapter、exact root/child/page schema、domain sharding与hard bounds、snapshot retention/hash、immutable backlink encoding、ledgerId reuse 和 profiled metadata mutation authority；
- principal allowlist/backend配置、Profile metadata mutation ACL、cold authority reference packing、protected local physical owner/record framing/at-rest protection、bounded grant/readable packing、future corrected SASL与跨instance key rotation；endpoint/mTLS/exact authorizer逻辑不再OPEN；
- control tail最终字段、batch/range future body、general E/W/A exact Java/admin outcome与BKException/detailCode；Round 7 frame/HELLO/subtype只有未来显式恢复并通过raw corpus后才能晋升stable，当前执行延期且不再作为任意设计空间；
- successor ledger 的 publication record、owner 状态和跨 run continuity；
- appendId suppressed-suffix/horizon、durable seal/footer authority；
- `ArenaControlLog` conditional transition/result exact API、sequencer/queue/batch bounds、segment、checkpoint A/B、selector/pin同步、current-selector/retiring-state packing、superblock 切换、`MOVE_COMMIT` encoding/orphan GC 与设备失败判定；
- RFC-0005 logical state的physical owner/exact durable packing；same-scope old-binary-visible fence当前BLOCK且`BKPF1`仅candidate；Cookie/superblock/device-manifest exact bytes、minimum reader/writer、migration/reverse/wipe tooling与persistent readiness backend adapter；new BookieId/new scope fallback语义不再OPEN；
- shared slab 的 lifetime class、compaction policy 和 hot promotion threshold；
- RepairIntent child enumeration/retention/orphan cleanup、strong completion record/page/snapshot schema、overlapping-range predecessor encoding、coverage audit commitment 与 compaction policy；
- Delete Coordinator 的 stream topology、PREPARED/effective assignment encoding、snapshot chunk/schema、cluster-accepted wipe/decommission proof 和 tombstone retention；
- streaming range 在一般 E/W/A 下的结果合并与point-oracle伪代码、required-coordinate/normal-tail proof、rich outcome exact enum/wire/admin API/legacy mapping、cancellation、可选 checkpoint 与 recovery-specific semantics；
- 每个正式 Gate 的基准 commit、硬件和完整 manifest。

开放问题存在时，对应章节只能标为 Proposed、Spike Ready 或 Blocked，不能标为 Implementation Ready。

## 13. 当前可执行工作

现在可以直接开始：

- 固定 stock `BK_CLASSIC` benchmark；
- 对 `BK_CLASSIC_TUNED` 做不改变 on-disk/wire semantics 的调优；
- 继续逐轮评审五份子 RFC；
- 实现Spike A/B/C的manifest-locked harness/Model，并在锁定环境和停止条件后执行；
- 实现`ProfileDescriptor` reference codec、golden corpus与独立verifier；不能分配未接受的policy/capability业务ID；
- 实现独立immediate-TLS/mTLS endpoint、exact-scope authorizer、cold authority adapter、protected-state语义接口与redaction的隔离prototype；
- 保留已实现的Round 7 frame/HELLO/status experimental codec、610-vector corpus与冻结反例；raw old-decoder/stock-binary执行延期，不发布stable compatibility；
- 实现`BKPF1` same-scope candidate与new BookieId/new-scope fallback harness、persistent readiness adapter prototype；same-scope结果在Spike PASS前保持BLOCK；
- Wave 0已完成隔离的startup/readiness/new-scope reference implementation及immutable receipt：typed compatibility/device/recovery facts、九阶段fail-closed顺序、durable local readiness、versioned persistent CAS、matching service-info、stale demotion、ephemeral writable registration、九个crash/restart边界和17项普通测试；它不接入`EmbeddedServer`、生产registration、真实OS权限或normal Add，不提升Spike/Gate/production状态；
- 实现不接管authority、可丢弃且不污染Classic rollback cohort的局部/isolated shadow prototype，用于否证crash recovery、资源上限和p99假设。

现在不能直接开始：

- Segment WAL ACK authority实现、live Stage 5 promotion或authority切换；
- 在raw decoder/stock old binary Gates、policy/capability registry、local physical owner与control tail闭合前提交stable wire/on-disk兼容面，或让shadow污染可rollback Classic cohort；
- 把 per-ledger minimum 8 MiB extent 当作不变量；
- 在旧 Bookie 上实现 same-ledger epoch takeover；
- 将本地 delete success 当作全物理删除完成；
- 把 BtrLog 模型结果当作本方案的形式化证明；
- 将 `DEFERRED_SYNC_LEGACY` 与当前 Sequenced WAL 自动 failover 组合。

本文的下一次状态提升，必须引用被接受的子 RFC、Spike immutable artifacts 和完整 Gate receipts。
