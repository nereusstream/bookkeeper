# BookKeeper Unified WAL 总体架构 RFC

> 状态：**Proposed / Not Implementation Ready**<br>
> 文档类型：总体架构、边界与依赖导航<br>
> 更新时间：2026-09-22<br>
> 结论：保留 Unified WAL 方向；除 `BK_CLASSIC_TUNED` 外，不授权按本文直接进入正式实现。

## 1. 决策摘要

本文取代此前的“Profile 化最终详细设计方案”定位。Profile 安装、allocator authority、规模化资源模型和集群级删除仍未闭合，不能作为实现合同。

当前决策如下：

1. `BK_CLASSIC` 是现有 OSS 基线；`BK_CLASSIC_TUNED` 可以立即进入基准和低风险优化。
2. `DirectJournal` 是 Bookie 进程或部署 cohort 级 Engine Profile，不是任意 ledger 可切换的属性。
3. Segment WAL在Profile install/activation、`ArenaControlLog`、已启用布局的allocator、Segment Bookie state/ACK authority和无对象存储形式化模型闭合前保持**P0 Blocked**；首批shared block，dedicated hot extent有条件延期。
4. Conditional delete 是集群协议，必须覆盖历史 ensemble、离线 Bookie、持久 tombstone、AutoRecovery 竞争和 Bookie rejoin barrier。
5. 当前 OSS 已有 bounded Batch Read；新增范围是 streaming continuation、一般 E/W/A 合并、恢复语义、QoS 和 cancellation。
6. BtrLog 只提供设计启发。它的对象存储恢复假设不能证明本方案安全。
7. 下一步先固定基线，并行推进CLIENT-1/2独立Classic维护、实验格式/I/O及固定owner/安全binding/NUMA池，再完成Bookie控制日志、per-Arena控制日志、DATA写入、点读与restart闭环。客户端维护不阻塞本地文件原型，但须在客户端接入前完成；隔离存储与实际启用路径的模型验证并行。
8. 首批集群闭合普通Add/点读/restart、基础点恢复和已fenced-close ledger的普通逻辑删除。强访问撤销、strong completion/loss reset及高级Range延期；strong reset后续也先限于整个ledger fenced+CLOSED，不让增强token阻塞活跃ledger的正常replacement。

当前实施规划基于`e8b162076fd8cfa84a11351a82caac8c5ac1f939`（2026-09-22 08:29:33 UTC，北京时间16:29:33），按“基础与执行模型→单机可恢复存储→索引与回收→协议与集群→完整验收与灰度”推进，详见§10及[16步实施清单](rfcs/unified-wal/implementation/README.md#实施顺序)。固定owner、安全binding、NUMA池随线程与缓冲池落地，默认OFF/SYSTEM/PARK，B18分离pin、属地池和有界spin收益。测试、故障注入和测量贯穿各模块；现有持久化、恢复、接收预算和控制可达合同保持。2026-09-22的规划更新仅整理实施顺序；后续实际源码、验证与未闭合范围以§10及实施清单的当前状态为准。

本文已冻结Round 7 reference/test manifest，但不把未经真实old decoder/stock binary验证的candidate宣称为stable wire/on-disk compatibility contract，下一原型按RFC-0005选择Bookie级独立控制日志；其stable format、阈值和真实证据仍由Spike接受。

### 1.1 单一基线持续演进

Unified WAL只维护一套当前设计与实现基线，不建立项目内部的代际产品线、并行合同或版本化类型后缀。后续改造直接修改本文、owner RFC、Spike与当前实现；新增能力直接加入当前capability set，并在同一变更中同步更新Gate、兼容矩阵和证据要求。

`codecVersion`、`semanticSchemaVersion`、`protocolMajor/minor`及hash domain separator中的数值仅用于wire/format字节判别和fail-closed兼容，不构成项目代际。确需不兼容字节改造时，必须在当前合同中同步修改technical discriminator、migration/rollback边界与raw corpus，不能另建并行代际实现线。外部固定标识如旧BookKeeper v2/v3协议、TLS 1.3和SHA-1/SHA-256算法名不受此规则影响。

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
- 将存储引擎、ledger durability 合同和上层协议语义分离。
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
- `c79357d76f95dae4c27ffbddcc075b6385991daa`的`BookieImpl.addEntryInternal()`仍先`handle.addEntry(entry)`再提交Journal；descriptor/wire/control/startup reference不代表新的Segment写入路径已接入。

现有`PendingAddOp`另有两项优先修复：换组只撤销`ackSet`但未同步旧Bookie的故障域成功集合，且`completed`未重新检查覆盖；旧地址最后响应减计数后提前return缺少回收检查。详见[RFC-0001 §9.5](rfcs/unified-wal/RFC-0001-profile-capability-install.md)。源码缺口已只读确认，修复/测试尚未执行；这是Classic维护范围，不是Segment authority解锁。

### 4.2 Batch Read

- 当前 [`BookkeeperProtocol.proto`](../bookkeeper-proto/src/main/proto/BookkeeperProtocol.proto) 已定义 `BATCH_READ_ENTRY`；Range Add/Read 仍标为未支持。
- [`ReadHandle`](../bookkeeper-server/src/main/java/org/apache/bookkeeper/client/api/ReadHandle.java) 已公开 bounded batch read；[`LedgerHandle`](../bookkeeper-server/src/main/java/org/apache/bookkeeper/client/LedgerHandle.java) 当前只在 `E = W` 且能力启用时走 batch fast path，否则回退。
- 新设计不得再把 Batch Read 描述为“完全不存在”。

### 4.3 Deferred Sync

- [`LedgerHandle`](../bookkeeper-server/src/main/java/org/apache/bookkeeper/client/LedgerHandle.java) 中的 `DEFERRED_SYNC_LEGACY` 是现有能力，但 failed Bookie 场景下不能按一般 HA WAL 假设执行正常 ensemble change。
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
SYNC_ON_ACK
DEFERRED_SYNC_LEGACY
TAIL_SUMMARY
CONDITIONAL_DELETE
```

需要 Bookie 执行的新合同必须显式安装；仅由客户端解释的合同不冒充 Bookie 能力。

### 5.3 Client/Protocol Profile

由客户端或上层协议负责：

```text
protocol-native position
transaction and visibility state
```

### 5.4 Canonical 组合

| Canonical Profile | Engine | Bookie 可见的 ledger contract | Client/Protocol contract | Durability |
| --- | --- | --- | --- | --- |
| `BK_CLASSIC` | `CLASSIC_ENGINE` | 现有 Classic | opaque | 现有模式 |
| `BK_CLASSIC_TUNED` | `CLASSIC_ENGINE` | 与 Classic 相同 | opaque | 与基线匹配 |
| `BK_DIRECT_JOURNAL` | `DIRECT_JOURNAL_ENGINE` | 当前仍为 opaque ledger | opaque | `SYNC_ON_ACK` |
| `BK_SEGMENT_WAL` | `SEGMENT_WAL_ENGINE` | 显式安装的 Segment contract | opaque | `SYNC_ON_ACK` |

每个需要Bookie执行的新组合由immutable semantic `ProfileDescriptor`定义。当前codec使用`BKPD` strict flat big-endian TLV、十个mandatory fields，合法长度精确为`124 + 6 * capabilityCount`（124..508 bytes、0..64 capabilities），1024 bytes仅是allocation前绝对input hard cap；JDK SHA-256 suite 1 identity为suite/schema加32-byte digest共36 bytes。duplicate/out-of-order/missing/unknown/default alias/509..1024-byte非法编码/trailing bytes全部拒绝。index/recovery/delete只进入mandatory capability registry，不维护第二份profile字段。failure-domain/capability registry与production legal-combination table仍BLOCK；codec/hash本身可开始reference implementation，normal Add不解析或hash descriptor。

Wave 0 的 `ProfileDescriptor` reference implementation 已落在 `bookkeeper-common` 内部 unstable 包，并提交 byte-exact valid/invalid corpus、独立 verifier、expected identity/field dump、checksums 与 fixed-seed fuzz receipt。该实现没有网络、磁盘、MetadataStore、registration 或 ACK 接入，registry 只有 test fixture；详见 [`implementation/profile-descriptor/`](rfcs/unified-wal/implementation/profile-descriptor/README.md)。这只关闭 reference implementation 交付，不接受 production registry/legal combinations，不提升本 RFC 或任何 Spike/Gate 状态。

Profile control plane与20-byte master-key data credential分离。所有Profile control/data只走独立`bookie-profile` immediate-TLS1.3/mTLS endpoint，Classic endpoint/pool无Profile handshake；mTLS X509 principal还必须通过exact operation/ledger instance/target scope authorizer，目标Bookie冷路径direct-read committed authority后才durable local binding。普通Add只解析fixed header/60-byte ledger context，constant-time比较缓存的36-byte identity与20-byte verifier并检查active/fence；MetadataStore、descriptor/auth hash、KMS、certificate/signature、control fsync均为0。principal config、所选Bookie control-log原型的物理验证/packing/at-rest protection仍BLOCK；不引入descriptor签名、Merkle、PKI扩张、per-Add proof/nonce或key rotation state machine。

Wave 0 已在 `bookkeeper-common` internal unstable package完成Profile control/auth typed interfaces与隔离endpoint reference implementation。它把immediate TLS1.3/mTLS transport facts、static precheck、fixed-key single cold authority read、exact post-read AuthZ、strict tuple/descriptor/Engine/credential检查、conditional semantic transition与durable-only secret-free result串成12项普通功能测试；不监听socket、不终止TLS、不实现physical protected store，也没有production startup、route、normal Add或ACK调用点。该结果只验证typed reference semantics；real TLS/listener/store、secret-leak完整矩阵、A24、stable wire、G1与production authority仍OPEN/BLOCK，详见 [`implementation/profile-control/`](rfcs/unified-wal/implementation/profile-control/README.md)。

Classic `LedgerDescriptorImpl.checkAccess()`错误master key时的双侧原始字节日志泄漏已由[独立红绿回归](rfcs/unified-wal/implementation/profile-control/classic-master-key-log-receipt.json)修复；这只关闭一个前置泄漏，完整Profile control认证、授权与secret-leak验证仍未完成。

Round 7 原始 wire candidate `0x0FFE4250` 已被真实 Apache BookKeeper 4.14.8 decoder 的 `magic-flip-1` 反例否证并冻结；当前 replacement candidate 为 `0x0FF04250`。它继续使用TLS后4-byte length + 32-byte header、byte-exact HELLO、distinct control/normal/recovery/read/LAC subtypes和12类status（1 OK + 11 non-OK）；range/batch在当前manifest中reserved/disabled。新 candidate 仍只授权reference codec与raw corpus，不是stable wire：每个受支持old decoder/binary对TLS ClientHello及合法/损坏/截断/fuzz bytes的Classic route/handle/master-key/allocation/journal/payload/ACK effect都必须为0，任何失败不得Classic fallback或双写。2026-10-13 [fresh released compatibility run](rfcs/unified-wal/implementation/profile-wire/README.md#恢复执行的-released-compatibility-矩阵)已在当前四版本665-vector（含完整实验control请求）decoder/私有loopback stock网络及JDI内部effect层通过，665条channel实际终结、storage/namespace不变、前后Classic sentinel正常，状态`PASS_SCOPED`；实际control codec请求样本及四版本resolver协议选择已验证，mixed rolling versions/auth/semantic/startup及general stable control-body Gate未闭合，G1保持`BLOCKED_UNVERIFIED`。

same BookieId/same storage scope的old-binary fence仍BLOCK；`BKPF1` nonnumeric Cookie sentinel只是Spike B candidate。若无法证明hook早于任何Profile storage open，当前合同必须使用new BookieId + new journal/ledger/index/Arena roots + new storage incarnation +旧service不可访问的credential/ACL scope。persistent readiness CAS先于BookieServiceInfo/ephemeral registration；Arena superblock不能替代Bookie fence，format/readiness检查不进入Add。

### 5.5 非法或当前禁止组合

- 在 Classic cohort 中按单 ledger 切换 DirectJournal；
- Segment ledger 未完成 RFC-0001 install 即接受 Add；
- client-only Profile 声称具有 Bookie epoch/hash/index 能力；
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
| `BK_SEGMENT_WAL` | P0 Blocked | RFC-0001/0003/0005及RFC-0004基础恢复/已启用删除Gate接受，Spike A/B/C通过 |
| `DEFERRED_SYNC_LEGACY` | Existing with HA Restriction | 不作为当前 WAL 默认 |
| `GENERAL_EWA_FAST_RECOVERY` | Research/Spike | RFC-0004 和 Model E |
| `POOLED_LEDGER_LANE` | Deferred | 独立 fencing 与资源合同 |

状态不能从“代码存在”“进程跑完”或单次性能改善推断。提升状态必须同时具备接受的合同、可追溯证据和相应 Gate 结果。

## 7. 总体模块关系

```text
Client / Protocol Adapter
    │ protocol request
    ▼
Standard LedgerMetadata + Profile Sidecar Coordinator
    │ OSS state/membership CAS + instance/READY/operation authority
    ▼
Bookie Cohort selected by Engine Profile
    ├── CLASSIC_ENGINE
    ├── DIRECT_JOURNAL_ENGINE
    └── SEGMENT_WAL_ENGINE
            ├── ArenaControlLog      allocation/generation/relocation-selection authority
            ├── Bookie control log  route/fence/grant/tombstone authority across Arenas
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
- RFC-0005 Segment Bookie state：逻辑 `LedgerRouteAuthority` 与 `BookieRegistrationAuthority` 决定install/normal admission/fence、bounded recovery grant/readable facts及assignment/incarnation readiness；normal、recovery与readable不是互斥flat role，隔离物理owner已采用RFC-0005 §5.2的Bookie级独立控制日志；grant/close已有经预留控制I/O持久化、回owner apply、准入cut与重放切片，隔离recovery DATA已接真实文件force/运行期重试比对；裸的重启候选不直接报告durable duplicate；当前单stream无MOVE/FREE范围已有同步、权限复核与真实点读的只读物理候选比对；旧DATA I/O错误尚无持久分类，仍不能报告重启后durable duplicate，隔离子进程在DATA force前/后直接终止的只读重放已验证，隔离文件层已可在全部旧授权块之后物理追加且拒绝本地成功权限；整体仍待服务级可写重启、掉电、真实集群与完整恢复验证。
- RocksDB、footer、cache：可丢弃并重建的派生加速结构。

## 8. 跨 RFC 安全不变量

1. 初始创建normal writer必须先在planned initial ensemble全部E个Bookie durable claim inactive Profile route，再发布带immutable instance backlink的标准LedgerMetadata、READY和全E initial activation后返回writer；同phase可并发，跨phase保持持久化依赖。只读打开不ACTIVATE、不等待全E在线或normal-active；恢复打开使用fencing/grant/durable close。
2. `ACK(normal profiled Add)` 之前，matching global READY 与目标 Bookie durable local normal activation 均已成立；普通 Add 不远程读取 MetadataStore。
3. Classic/Profile/Tombstoned route 是单一、原子、可恢复的本地 claim；legacy normal/recovery Add 不能绕过 Profile route。
4. 写期replacement按inactive install → `LAC+1` membership CAS → authority/target retention → normal activation → pending本地切换排序。全部受影响pending的映射/被替换slot ACK及故障域资格更新后才开completion gate并恢复连续回调；未变identity/incarnation的有效ACK保留，只重发必要副本。初始E同样先留存后激活，同起点map覆盖不丢删除目标，不复制历史fragment。
5. ACKed DATA必须已有durable allocation authority；submitted块不可修改，完整write/覆盖barrier、可恢复物理前缀及locator就绪后逐entry检查成功资格。单条取消/fence不把已durable batch变成物理gap，batch durable也不代替该entry权限；物理序号不等于entryId/LAC，也不以共享文件offset定义跨stream顺序。单stream逻辑尾部排除不得截短共享文件或破坏其他stream的有效DATA。timeout不抹除已提交坐标，未解析I/O暂停相应stream/file并恢复，不以跳号/新stream/重复sync成功绕过。
6. 同一slot/extent generation只有一个allocator owner；shared owner可承载多个ledger instance的record，不把allocator ownership与record所属ledger混为一项身份。
7. FREE以allocation/generation为主语，整单元无live occupant/待完成写入/旧pin且满足selector及清理条件后才接受；单ledger删除、部分move或orphan原因不替代该证明。durable bump及旧I/O终结前不复用，旧generation残留不复活，unused不靠每次整块清零猜测；内部FREE不自动增加filesystem available。
8. 旧 generation locator 永远不能读取新 generation payload。
9. logical delete 后 ledger 不能重新 open；Bookie 应用缺失 tombstone 前不能重新成为 writable。
10. AutoRecovery target 接收第一份 durable payload 前必须有可由 delete freeze 枚举的 RepairIntent；recovery-only/committed-readable 不授予 normal writable。
11. membership由标准LedgerMetadata CAS串行，同记录freeze阻止删除后的新membership；初始/写期activation authority与目标留存、RepairIntent admission都与DELETE_INTENT共享lifecycle publication cut，cut前已授权target进入完整history。基础copy/membership/close不创建strong-publication token、不重置loss window；延期strong completion仍须整个ledger fenced+CLOSED并按token/prepare/publication/resolve执行，旧token不得超时强删。
12. permanent-loss 保证只在声明的 distinct failure-domain 预算与 repair window 内成立；无有效 evidence 时 recovery 永不返回成功。
13. DATA成功前通过有界热handle/index可定位，append shard不等RocksDB写/flush/compaction；热定位在已计费query-visible路径接管后淘汰，不等flush。点读先hot并保护命中引用，miss后fresh current DB Get；早于hot查询的snapshot/iterator/negative cache不用于fallback的absence判断，两路径同样核对selector/generation/pin。首批纯派生索引关闭WAL，真实持久内容及matching generation才推进连续coverage checkpoint；未覆盖DATA重放，索引全删可重建，旧update不复活陈旧selector/tombstone。coverage不替代allocator/控制authority或授权free。
14. 同Arena compaction只选冻结且原写已解析source，同source/generation一个正常任务，OPEN ledger无需close/fence。framing持久区分ENTRY_DATA与绑定move/target身份的RELOCATION_COPY，来源由内部设置并受校验；copy durable只给内部结果，MOVE成功apply才切换selector，不覆写DATA来源/提交位。FREE晚于cutover/drain并保持gate；成组净收益扣除destination/padding，无获益背压，cross-Arena仍unsupported。
15. range/TailSummary/BatchRecovery fast path 的 unsupported、stale、partial 或局部预算耗尽必须从 earliest unresolved coordinate 回退；不能越过 hole，deadline/cancellation 不伪造 DATA_LOSS。
16. offline rejoin使用cluster-authoritative finite stream assignment、storage incarnation、no-hole delete-applied cursor与snapshot+complete suffix；cursor只覆盖durable tombstone及可重建清理义务或有效non-applicability proof，不等drain/compaction/free，也不证明访问屏障或physical完成；per-ledger `deleteEpoch`不能兼任catch-up watermark。
17. permanent-loss budget只能对有完整`F+1` distinct-domain evidence的bounded range reset；首批reset disabled，未获得有效proof前不刷新既有loss window。membership、local durability、activation或generic repair COMMITTED单独都不够；增强候选仅限整个ledger fenced+CLOSED。
18. verifier strong completion 必须长期绑定 exact range/context/membership、immutable Profile descriptor、当时的 `F`、failure-domain policy、control fence 和 overlapping-range predecessor；audit digest 单独不能 reset，compaction 后也不能在新 descriptor/`F` 下重解释。只要求冲突或重叠 range 的 accepted loss/completion 单序，已证明不相交的 range 不进入 ledger-global hot CAS。
19. obligation-changing delete assignment 先 PREPARED、catch up snapshot+complete suffix，再原子 effective；安全的旧 generation 不因 PREPARED 无条件 demote，但新 generation 生效后 stale writable authority 必须失效。snapshot 必须可遍历并应用 delete effects，不能只有 digest；catch-up exemption proof 必须绑定 Bookie、old incarnation、device/storage scope、operation generation 与 cluster acceptance，不能跨 scope 重放。
20. relocation orphan 只在 new allocation/location 未被任何 authority 依赖时回收；既存 logical success 继续由 old/current selector 承载。late `MOVE_COMMIT` 与 orphan `FREE_AND_BUMP` 由同一 Arena control order 条件互斥，cutover 不能早于覆盖该 move 自身 sequence 的 durable-through。
21. recovery保留durable close、temporary deferred、attempt incomplete、quarantine/authority loss及required evidence exhausted分类，复用Classic quorum算法作Profile适配。normal tail的每个否定来源须自身durably fenced、身份/incarnation/context匹配且pre-cut写/定位已解析，distinct W-A+1覆盖不能由global fence或重复响应代替；timeout/offline不伪造absence或DATA_LOSS。
22. sidecar store version与semantic generation分离；child/page只有被exact instance/domain head条件化发布后才有authority。compaction authority是committed snapshot+complete bounded suffix，publish/fallback proof先于reclaim；referenced unknown mandatory state、missing chunk或gap必须fail closed，普通Add不依赖sidecar读写。
23. per-Arena命令合批durable后按序原子判断/修改applied state，失败no-op消费sequence但不改业务状态或回滚其他命令；成功及duplicate仍受原条件约束，checkpoint只取appliedThrough。正常提交前受保护的数据/回收/gate检查与可重放metadata predicate分开；后者依赖控制状态及绑定事实，不读已合法回收的历史payload，也不因命令完整就无条件成功。selector publish与block-new-old-pin仍为同一cut。
24. Bookie控制日志独占route/activation/fence/grant/tombstone，Arena只持allocation/generation/move/free并消费必要外部引用，不重复创建ledger生命周期。route claim先于lazy create，fence关闭new admission并终结pre-cut成功资格；RPC失败不证明物理不存在。tombstone收窄normal/recovery/read，readiness仍绑定incarnation/assignment/cursor，不成为per-Add lease。
25. 恢复先为最终前缀P取得匹配的累计应用length L，再发布并核对durable CLOSED(P,L,context)；源entryId/piggyback LAC/源length不能混用或各取最大。空ledger为(-1,0)，缺锚点defer、矛盾conflict，必要时只补边界点读；重试P相同但L不同不算legacy `OK`。fragment/partial/skip不计ledger recovered，不清intent/marker，generic rc不抹除rich outcome。
26. descriptor identity由canonical immutable safety semantics与固定长度、对对抗输入collision-resistant hash派生；CRC/checksum不能替代，hash只做identity/integrity；unknown/duplicate mandatory语义拒绝，optional neutral hint不改变接受集合。
27. master key只作为data credential，不是MAC key或entry布局说明。首批新Profile只接受immutable capability/manifest绑定的CRC32C entry布局，创建/安装拒绝不支持类型且不降级、不下发密码/MAC key；同数据不同LAC可幂等，CRC相等不能替代实际bytes比较。缓存的统一maxPayload取normal Add、recovery Add、read response和Segment record/batch容量的最小值，首次写入前执行；收紧新写阈值不降低已接受entry的读取/恢复能力。Profile control要求non-anonymous且获授权执行exact operation/instance/target scope的authority，AuthN-only不够；secret/offline verifier不得进入公开metadata、receipt、日志、metric或exception，credential-bearing Profile transport满足manifest confidentiality/integrity。
28. Profile normal/recovery使用distinct logical operation和mandatory wire discriminator；任何Profile bytes在受支持旧decoder上都不能形成legacy Add effect，任何失败都不Classic downgrade或双写。
29. Bookie/storage compatibility fence必须在stock old binary mandatory pre-replay path fail-stop；Cookie optional/version、registration、new superblock/file或unknown Journal record均不能单独替代。
30. startup按compatibility fence → required device/superblock → 完整控制候选验证/必要同步及metadata predicate重演 → 最终selector与未完成依赖的DATA验证/同步 → route/delete/readiness → registration排序。A→B→C且A/B已合法FREE复用时不追读旧payload或强制FREE前checkpoint；最终C坏/控制后缀缺失仍隔离，不回退旧位置。可读/CRC、索引或readiness不替代DATA barrier，验证同步前不开放对应服务；同步不推导AQ/LAC或清除已知I/O错误，migration/format/incarnation gate保持。
31. 现存Segment/Profile local success或durability-unknown authority没有完整negative proof时不得rollback old binary；format/readiness检查不进入normal Add；只有准备发送Profile operation的Profile-capable connection执行一次Profile negotiation，legacy Classic connection不承担该handshake。
32. 当前descriptor input必须是严格canonical TLV并产生固定36-byte identity；未接受policy/capability registry时不得mint production descriptor。
33. Profile只走独立immediate-TLS/mTLS endpoint与pool；Round 7 frame bytes在raw old-decoder Gate PASS前只属于executable test manifest。
34. same BookieId/storage scope在stock binary pre-storage-open证据通过前保持BLOCK；失败时new BookieId/new roots/new incarnation/new credential scope fallback是当前唯一安全路径。
35. persistent readiness CAS先于ephemeral writable registration；generation/incarnation mismatch non-writable，registration hint不替代local receipt或old-binary fence。
36. 同一预算从连接/TLS/header/read、半包及已解码未准入请求延伸到存储/索引；固定header检查并预留额度后才积累大body，ownership交接与暂停前pipeline字节不漏计。DATA准入仍保障提交/定位完成工作集，大entry满足统一N，不阻塞event loop/shard/ledger锁。同Profile listener保留少量有界独立控制连接及接收/执行份额，DATA暂停仍可达且不豁免认证或总预算。最老deadline/重试身份、Arena/filesystem维护余量及满载推进合同保持。 固定owner/CPU/NUMA按RFC-0005 §10.2—10.5，完成与原池归还容量受保护；放置或等待策略失效不改变ACK、fence、恢复与持久身份。
37. confirmed可见范围由客户端LAC/CLOSED边界限定，Bookie不以local LAC截断合法物理点读，恢复候选不等于commit；DATA完成不推导quorum LAC或增加逐Add LAC fsync。点读须独立校验record外层身份与BK CRC，按有界必要对齐范围读取，未知覆盖不能伪造absence。

任何子 RFC 或 Spike 发现这些不变量不可同时满足，都必须停止相应路径，而不是降低不变量。

## 9. BtrLog 证据边界

可借鉴：single-writer、client-driven quorum、bounded window、thread affinity、direct NVMe 和 tail recovery 的思路。

不可继承：依赖 durable blob store 的 flush、repair、recovery 路径及其已经验证的安全结论。

当前有四个相互可组合但独立检查的模型，其中A/C/D是基础必需，E按扩展能力启用：

| 模型 | 范围 |
| --- | --- |
| Model A | BookKeeper E/W/A、write-set rotation、sidecar child/head/snapshot+suffix、store-version/instance ABA、descriptor/auth/wire compatibility abstraction、local route/admission/fence、install/activation、legacy routing、AQ、LAC、ensemble change、failure-domain loss、strong verifier completion 与 bounded receipt compaction |
| Model C | `ALLOC/DATA/DELETE/FREE/MOVE_COMMIT`、conditional apply/durable result、generation、current-selector checkpoint、orphan cleanup、selector/pin cut、old-binary/format/migration/readiness abstraction 与 power loss |
| Model D | historical ensembles、partial delete、assignment handoff、applicable snapshot/incarnation、versioned registration readiness、offline rejoin、AutoRecovery race |
| Model E | TailSummary、required frontier、normal-tail proof、rich outcome/legacy projection、skip/marker handling、range fallback 与一般 E/W/A point-oracle equivalence；仅在推进该能力时必需 |

实际启用路径的Model A、C、D及必要组合未通过前，Segment authority不得进入production candidate；强reset/强撤权等disabled动作按独立配置延期，基础scope保留拒绝和边界测试。Model A仍须验证基础point recovery与durable close，Model E延期不豁免该要求。isolated/discardable性能原型可与相关模型并行，不等待所有增强模型。当前没有Model B。

## 10. 实施顺序

以下施工顺序已开始实施，从当前reference implementation走到首个可保留数据的生产候选。步骤1隔离配置与构建入口已交付，2项参数边界测试通过；步骤2的[CLIENT-1/2](rfcs/unified-wal/implementation/client-maintenance/README.md)已完成红绿回归（当前85项通过），步骤3实验格式与真实FileChannel I/O已通过14项定向测试，步骤4已交付固定owner/PARK、DATA/控制独立completion与有界I/O份额、显式绑定、拓扑/placement及group启动与SYSTEM/OWNER_TOUCH池；低频运行期CPU/cgroup检查及PREFER/REQUIRE本地处置已接入隔离group；owner检查抛错后的停收及在途I/O/排队ticket排空已有定向回归。步骤5已有隔离Arena ALLOC/ALLOC_POOL控制日志、持久stream首序号与单在途控制驱动、DATA准入及I/O服务满额时控制force后原owner apply的定向路径、显式DATA整段grant检查、停写后的只读framing扫描，以及Bookie INSTALL/ACTIVATE/FENCE/TOMBSTONE局部持久化、重放和预留控制I/O后原owner apply；explicit LAC已在隔离Bookie控制日志中作单调独立barrier与重放，并接入预留控制I/O及原owner apply，尚无网络/confirmed-read接线或控制group commit；步骤6已有固定shard准入校验、DATA坐标预留、真实write/force、原owner volatile连续物理前缀和有界物理热候选接通的隔离驱动，以及受控normal逐entry结果、在途同坐标重试等待和已发布候选的owner/I/O重复点读慢路径；步骤6的DATA文件关闭也拒绝未终结预留，执行前的I/O拒绝只终结预留计数而不回退坐标；步骤7已有核对grant和身份的对齐单record点读、停写后文件同步、有界重启物理清单、按Arena持久首序号及授权块筛选的单stream候选检查（旧stream结构损坏不阻断另一物理候选，共享文件同步失败仍拒绝）及选定多stream整组核对存储scope、容量和重复坐标后原子装入的新owner物理热目录重建局部实现；隔离本地点读增加Bookie安装实例读票据，fence保留读资格，tombstone切断新读并使尚未判定结果的旧票据失败；只读stream恢复在完整校验目标batch后可按重放的Bookie本地route筛掉已tombstone实例的定位，损坏record仍拒绝恢复；通过normal DATA真实force和durable fence后的双控制日志重开、同步候选点读局部回归。步骤11已有固定头/完整HELLO body前置校验、三份额明文接收预算、首个非HELLO操作分类与阶段期限、独立immediate-TLS1.3/mTLS工厂模式、握手后叶子证书身份提取及隔离TLS→接收器联测，并通过隔离localhost NIO socket联测，但未接入真实Bookie listener。Bookie控制票据已与隔离实际DATA I/O交错验证；受控normal批次在连续物理候选发布后按entry判定捕获的准入generation，同批过期与有效entry可得到不同隔离本地结果；在途同坐标normal重试可在原owner比较内容并等待原物理结果，已发布运行期候选仍通过有界I/O服务真实点读并复核权限，重启物理候选不授予幂等成功；normal与重复请求提交后停止响应等待已有隔离切片，网络deadline/断连接线、跨入口bytes预算、restart末端前缀、pool used/unused恢复、权限化热定位、selector/pin和生产逐entry结果仍未接通；Bookie生命周期重启/PREFERRED及步骤5～16完整合同尚未完成。当前结果见[本地存储状态](rfcs/unified-wal/implementation/local-storage/README.md)和[实施清单](rfcs/unified-wal/implementation/README.md)，不提升Gate。完整依赖、实施内容与完成条件由[现有实施清单](rfcs/unified-wal/implementation/README.md#实施顺序)维护；步骤号只用于排序，不新增RFC、Maven模块或工作项体系。UW-1～UW-7继续表示职责分类，例如UW-1集群删除依赖UW-6基础fenced-close，不能按UW编号施工。

2026-10-02 步骤6新增[固定对齐池的DATA提交切片](rfcs/unified-wal/implementation/local-storage/README.md)：先取得足够的owner池buffer再复制冻结，driver持有到真实I/O与owner结果处理均终结；取消响应等待和提前`UNKNOWN`回调不提前复用。相关测试100项执行通过、5项Linux亲和性用例在macOS跳过；输入/TLS/socket/读取/索引总预算和完整步骤5～7仍未闭合，不提升Gate。

2026-10-02 步骤6继续接入[普通entry源buffer](rfcs/unified-wal/implementation/local-storage/README.md)：执行组共享输入bytes额度，分配前预留完整容量；原owner复制到固定对齐池后归还源，转换和物理提交共用一个completion ticket。取消等待不提前释放；源/池不足有界拒绝，池不足不占物理序号/offset，资源恢复后可原坐标重试。相关测试105项执行通过、5项Linux亲和性用例在macOS跳过。此显式入口仍由调用者提供batch/授权地址；后续自动合批进展见下，Profile接收credit交接和完整总账尚未完成。

2026-10-02 步骤6新增[逐entry自动合批](rfcs/unified-wal/implementation/local-storage/README.md)：固定owner按bytes/count/最老deadline封包，非空时从已apply grant选地址并重验预留；复用每条entry的完成ticket，shutdown封包已接纳输入。同坐标重试在assembling/冻结/已发布阶段分别比较、等待或点读；合法1 MiB payload已以固定池跨同allocation两个block独立封包。相关测试113项执行通过、5项Linux亲和性用例在macOS跳过，Spotless/Checkstyle通过。临时拒绝不占序号，unknown不绕过原stream；预取/尾部回收、网络交接与总预算仍未闭合，不提升Gate。

2026-10-02 步骤6继续接通[独立stream的pool空间接续](rfcs/unified-wal/implementation/local-storage/README.md)：共享文件以各shard/generation的单调游标预留其已apply grant，独立owner的高/低地址pool不依赖全局尾指针先后；整批放不下时跳过短尾及其他owner范围，到下一同stream pool继续序号。相关集117项执行通过、5项Linux亲和性用例在macOS跳过；补强allocation边界后的1项定向复验及Spotless/Checkstyle通过。预留后不回退或复用跳过范围，重开仍跳过全部旧授权块。按需refill进展见下，尾部回收及used/unused恢复仍待完成，不提升Gate。

2026-10-02 步骤6接通[异步按需pool refill](rfcs/unified-wal/implementation/local-storage/README.md)：有限shard请求由Arena控制owner合并control force、顺序apply，再以原DATA ticket继续封包并重验grant。池内普通Add无额外控制force，等待资源及回通知都有既有上限；空间耗尽不force空日志、不占DATA序号，control/owner故障及shutdown排空已有实际文件验证。相关集131项执行通过、5项Linux亲和性用例在macOS跳过；修正2 MiB批次中组合两个合法最大entry的封包边界，最后1项定向复验及Spotless/Checkstyle通过。低水位预取、空间恢复总账、尾部回收与完整生产/网络接线仍未完成，不提升Gate。

2026-10-02 步骤7接通[Bookie本地route筛选的多stream重开读取](rfcs/unified-wal/implementation/local-storage/README.md)：同锁扫描与恢复同步后，稳定控制cut、连续stream及唯一坐标共同约束启动定位；tombstone记录仍校验但不占可读定位容量。保持读取pin、Bookie读票据和Arena cut复核，未知/歧义不报不存在，后续grant使旧清单失效。两次进程终止后的多stream旧entry可由同一重开清单点读；旧I/O错误判定、一般selector、动态写入定位及服务级可写restart仍未完成，不提升Gate。

2026-10-02 步骤6补齐[合法recovery Add的逐entry合批入口](rfcs/unified-wal/implementation/local-storage/README.md)：私有exact payload快照移交共享源额度，普通/recovery共用封包、pool refill与重复坐标状态，并分别复核准入generation及exact grant/close/tombstone。相同内容交叉重试共用DATA结果，冲突不新增force，recovery-only不授予normal activation；取消response、shutdown及I/O失败仍排空实际资源。wire到存储credit、全局bytes总账、旧I/O恢复与生产接线仍未完成，不提升Gate。

2026-10-02 步骤6/7的[driver重复点读](rfcs/unified-wal/implementation/local-storage/README.md)复用既有owner固定对齐池和已拥有的源view，不再分配大record副本或重复制输入；池不足在I/O前背压，取消与读取错误不提前归还源/scratch。通用物理点读、只读重启检查及无池手动入口仍为堆buffer实验路径，全读取/网络/索引预算和服务级可写restart尚未闭合，不提升Gate。

2026-10-06 步骤11增量：[真实隔离普通 Add 接线](rfcs/unified-wal/implementation/profile-wire/README.md#真实隔离普通-add-接线)把 mTLS/HELLO、固定布局与本地 N 上限、credential/context/CRC/entry 坐标、既有有界同步输入复制、真实 DATA 结果和完整 status 三元组接通。声明的 N+1 在 body 分配前关闭；force 前不回 OK，实际 UNKNOWN 不回 NONE；断连只停止等待。排队 completion 和下游持有的响应在真实消费/write promise 终结前继续计费。八个真实 socket 场景（含取消额度红绿）与相关14类383项中378通过、5项macOS上的Linux亲和性跳过；Spotless/Checkstyle通过。有限矩阵39正向完整PASS/122指定反例。HELLO identity来自实际存储，readiness generation仍为显式调用输入（测试为0）；生产listener、控制操作授权、TLS/socket及存储总账、完整startup/readiness、quorum和性能对照仍未完成，步骤11/B18与生产Gate保持OPEN。

2026-10-06 恢复写入增量：[真实隔离ADD_RECOVERY](rfcs/unified-wal/implementation/profile-wire/README.md#真实隔离-add_recovery-接线)沿用144字节固定prefix、精确本地OPEN grant与原DATA驱动，按实际源额度先预留再复制完整BK entry；reference网络整包克隆不进入server路径。恢复成功/重复校验不激活normal writer，grant close晚于DATA写入的过期结果与force失败保持UNKNOWN；断连仅停等待，实际I/O/owner排空及native锁保留。七个真实socket用例已通过；最终相关14类398项为393通过/5平台跳过，style及41正向/134指定反例通过，详细源码绑定见上述实现记录。READ_ENTRY响应/确定missing、真实RepairIntent及grant控制面、客户端quorum/完整恢复、生产listener/readiness与TLS/socket/storage总预算尚未闭合，步骤11/14和生产Gate保持OPEN。

2026-10-06 [Netty backing buffer额度接线](rfcs/unified-wal/implementation/profile-wire/README.md#netty-backing-buffer-额度接线)已把隔离channel的heap/direct capacity、扩缩容时旧加新backing、derived view最终释放、TLS/socket通过channel allocator分配的ByteBuf及响应workspace拆分加入原连接/Bookie账本。已有read backing跨用途保留原lane；断连归还名额但不提前归还实际仍持有的backing。真实socket漏记红绿、六项allocator回归、TLS分配拒绝及原22项socket场景已验证。JDK/native TLS内部对象、kernel buffers及存储统一总账仍未闭合，生产listener/readiness、READ_ENTRY、完整模型/长期/掉电/性能与步骤11/Gate仍OPEN。

2026-10-06 [接收至存储源副本交接](rfcs/unified-wal/implementation/profile-wire/README.md#接收至存储源副本交接)使隔离normal/recovery同步entry复制在原source子上限之外，复用借用frame连接的exact heap backing额度；frame/workspace/source共存均计费，断连后源保留至owner实际用完，复制到预分配对齐池后才归还源，实际I/O仍持有原pool lease。明确credit拒绝回滚source并沿用原未接纳结果；其他分配错误传播，手动/pooled调用者保留独立unpooled副本合同。两条真实socket路径及额度拒绝/恢复单测通过，未改wire或存储格式。完整TLS/kernel/object/index/storage总账、生产listener/readiness、READ_ENTRY及步骤6/11/Gate仍未闭合。

2026-10-02 步骤7接入[owner/I/O本地点读](rfcs/unified-wal/implementation/local-storage/README.md)：live hot与多stream重开清单共用固定池读取，排队前保持文件pin，回owner复核Bookie资格及候选/Arena cut。结果由最后消费者关闭归还lease，慢消费背压读/DATA，取消/撤权/读盘/owner/回调失败按真实终结排空；重开池须覆盖保留旧entry，不能因降低新写N忽略旧读。真实网络读授权、响应credit、一般MOVE/FREE selector和可写服务restart仍未闭合，不提升Gate。

2026-10-02 步骤5～7新增[DATA文件持久隔离子集](rfcs/unified-wal/implementation/local-storage/README.md)：实际绑定的DATA在I/O前共享预留每文件通知ticket与逻辑日志容量，普通控制仍保留槽位；不足先背压，不占DATA坐标。失败先切断文件准入，控制忙时通知等待原批次，停收后用原ticket排空，再持久化/apply已有DEVICE_STATE；旧grant保留，重放与后续sync不解除隔离。实际控制I/O或owner故障、落盘前退出仍未闭合跨重启判定；未见记录不授权旧错误清除，clear/reset及服务级可写restart未启用，不提升Gate。

2026-10-02 步骤5～7继续新增[写入会话启动/关闭边界](rfcs/unified-wal/implementation/local-storage/README.md)：隔离writer显式在首次分配/写入前持久OPEN，预留CLOSE容量；停止DATA、排空I/O并关闭descriptor后仍持目录锁，检查控制与故障通知排空才记录CLOSE。DATA错误、消耗坐标后的执行前取消或无CLOSE退出保持不可写；真实进程在DATA force失败、隔离记录尚未生成就退出的回归，验证重放在恢复sync前拒绝重开，后次sync不解除。正常关闭后可开下一物理epoch；不补认legacy历史，也不直接获得local-result authority。自动服务生命周期、未正常关闭会话恢复解析及可写restart仍未完成，步骤5～7及总体Gate不标完成。

2026-10-02 步骤5～7已接通[正常关闭会话的隔离local-result restart](rfcs/unified-wal/implementation/local-storage/README.md)：持久CLOSE、完整DATA验证/sync及Bookie route选择后，由原owner在稳定cut下恢复连续前缀与持久候选。旧entry重试/冲突和新entry同stream接续已在正常关闭的新JVM及两轮重开真实验证，旧授权块仍跳过，未用新stream绕过旧前缀。legacy、未关闭会话、多未退役stream、恢复sync失败或cut变化拒绝开放相同结果资格；plain物理扫描保持原边界。完整服务readiness/MetadataStore、未正常关闭恢复、一般selector与可持续空间回收仍未完成，当前Gate不提升。

| 阶段 | 步骤与主线 | 完成边界 |
| --- | --- | --- |
| 基础与执行模型 | 1 基线/测试入口；2 CLIENT-1/2；3 实验格式与文件I/O；4 固定owner、安全binding与NUMA池 | 可复用的隔离入口；OFF/SYSTEM/PARK基线；基础ownership和预算 |
| 单机可恢复存储 | 5 本地控制日志/启动检查/allocator → 6 普通Add/合批持久化/幂等 → 7 点读/restart | 第一里程碑：真实写入—点读—重启恢复；仅报告实际覆盖的局部性能 |
| 索引与回收 | 8 checkpoint/rotation/异步索引/全量重建 → 9 本地tombstone/compaction/FREE/reuse → 10 空间耗尽/长期负载/多Arena | 第二里程碑：有界、可长期运行的本地存储；尚未证明分布式quorum和兼容 |
| 协议与集群 | 11 Profile transport/接收背压 → 12 MetadataStore/install/activation/registration → 13 ACK/retry/replacement → 14 recovery/AutoRecovery → 15 集群删除/离线追赶/rejoin | 第三里程碑：创建、写、读、换组、恢复、关闭、删除、重返完整闭环；仍待综合验收 |
| 完整验收与灰度 | 16 全链路故障、模型、性能、兼容、运维与独立小cohort canary | 实际启用能力证据闭合后，才提升对应RFC/feature状态和部署范围 |

步骤2不阻塞本地文件原型，须在步骤13前完成；步骤3、4可并行，步骤11、12在接口稳定后可提前并行开发，真实集群成功路径联调等待本地存储通过对应验证。Block H与Block G实际启用A/C、必要D及组合并行，模型不能证明真实fsync/TLS/资源，性能run也不能替代quorum安全证据。测试、故障注入和测量随每个模块交付，步骤16负责组合验收。

DirectJournal是独立对照/优化支线，可并行或等主线取得基础数据后开展，不是Segment开工前置。它保留Journal＋LedgerStorage结构，不宣称一份payload持久化，不替代Segment安全证据。首个完整WAL之后再按需求或测量推进dedicated extent、其他I/O backend、高级Range、rack/AZ、强撤权、strong reset、在线删除及跨Arena迁移；无证据时保持disabled，详见[延期能力及前置条件](rfcs/unified-wal/implementation/README.md#directjournal与延期能力)。

可保留数据canary前必须完成启用范围的基础恢复、普通删除、全量重建、维护进展及资源Gate，并接受对应RFC与Spike证据。Round 7 test manifest和isolated shadow不授予stable wire/disk、live shadow、Segment authority或production readiness；live shadow仍须证明不污染Classic rollback cohort。raw old-decoder/stock-binary矩阵已有2026-10-13的PASS_SCOPED证据，内部effect已在当前665-vector run直接观测为零；已验证实际control codec请求样本，mixed rolling versions/auth/semantic/startup及general stable control-body G1范围仍未闭合，发布对应stable面前必须补齐并通过；所选control-store物理协议同样须验证，不能以永久延期宣称兼容。首个new-scope canary不额外要求原地same-scope迁移；退出时停止新写并保留新引擎读取/恢复能力，不能让旧binary打开Segment文件。

### 10.1 下一阶段实施顺序与交付物

当前继续完成步骤5～7，形成真实写入、点读与重启恢复闭环。具体合同仍由既有owner RFC拥有，后续索引/回收和网络/集群按上述依赖集成。

| 步骤 | 实施内容 | Owner与必须提交的证据 |
| --- | --- | --- |
| 1 | 固定source/config、单Arena/shared block、OFF/SYSTEM/PARK及最小测试入口 | 本文§11、Spike B manifest；明确构建/运行命令、独立结果目录，暂未实现操作拒绝，不覆盖旧receipt |
| 2 | CLIENT-1 ACK/故障域/多slot整体gate，保留未变ACK；CLIENT-2最后旧响应回收 | RFC-0001 §9.3/9.5、A29/A30；4/3/2先处理write-set外slot不提前callback；确定性修复前失败、修复后通过 |
| 3 | 实验framing、独立parser/固定向量、ENTRY_DATA/RELOCATION_COPY区分及一种真实I/O后端 | RFC-0003 §5/6/9、RFC-0005 §5.2；短写、sync失败、截断、坏长度/generation；固定容量满额停止 |
| 4 | 固定owner＋PARK、安全binding，再owner本地池及可选PREFERRED，spin最后单测 | RFC-0003 §6.2/13、RFC-0005 §10.2—10.5、B18；实际mask、继承/降级、socket/SMT、原池归还及完成保留容量 |
| 5～7 | 控制日志/ALLOC→DATA合批与幂等→点读/restart，先重演完整控制再验证当前必要DATA | UW-2/3/4/7、RFC-0003 §5.3/6.1/9/12、RFC-0005 §5.2/6.1/8/10.1；真实I/O、逐entry结果、连续物理前缀、统一N、unknown与双故障恢复 |

每个模块提交实际实现、对应测试、可重复测量及未覆盖范围说明；未执行范围保持NOT_EXECUTED。亲和性按相同worker/batch/I/O/TLS/E/W/A与维护负载分离A=OFF/SYSTEM/PARK、B=pin、C=NUMA池、D=有界spin，报告实际mask/页分布、CPU/entry、空闲成本、p99.9及quota干扰；无收益保持默认。CPU/NUMA不进入持久身份，不新增通用调度/测试平台，不重绑旧receipt。

2026-10-02 步骤5～7继续接通[隔离存储资源图生命周期](rfcs/unified-wal/implementation/local-storage/README.md)：统一持有双控制日志、DATA、执行组及全shard合批器/点读，全部原owner bootstrap和控制槽归还后才开放本地请求。关闭按停收→排空I/O、owner和读结果buffer→锁内DATA健康CLOSE→双控制日志释放；超时保留原资源owner/目录锁，启动失败也按此顺序清理。总热定位与源/对齐buffer预算不随worker复制，普通与恢复准入仍独立。此为正常关闭的隔离存储图，不替代Bookie/readiness/registration、未正常关闭恢复、一般selector/回收或完整接收bytes预算；步骤5～7及总Gate仍未完成。

2026-10-03 [Spike C有限模型切片](rfcs/unified-wal/implementation/models/README.md)已实际验证：管理会话、控制/DATA barrier、连续前缀和旧错误隔离的两个正向配置完整探索PASS，三个负向候选规则均得到指定性质反例。回执`unified-wal-model-jqvpwfsw`保留source/config/tool身份、完整trace和统计；这是局部模型进展，不提升完整Model C、组合模型或Block G。未正常关闭会话的故障解析仍未完成，Java现有隔离行为保持。

2026-10-03 Arena checkpoint已补applied状态/字节cut、有界A/B image、durable CHECKPOINT_COMMIT后发布、完整后缀与旧image/全日志回退。新增真实JVM四个发布边界及DATA重启验证；checkpoint有限模型179,279 distinct states正向PASS，两种错误规则取得指定反例。新回执为`unified-wal-local-rO6YbO7C`和`unified-wal-model-thb7_nqa`，历史回执保持原绑定。全部旧控制日志仍必需，Bookie checkpoint、在线维护、prefix回收/rotation、index/MOVE/FREE及完整Model C/Block G尚未完成。

2026-10-06 控制网络增量：[真实隔离FENCE与控制额度](rfcs/unified-wal/implementation/profile-wire/README.md#真实隔离-fence-与控制额度)复用原context/credential校验、Bookie控制driver与实际write/force/owner apply，首次授权后把连接/frame/解析响应workspace原子转入CONTROL；后续逐次鉴权。pre-cut等待返回原操作transient/NONE、force/apply不确定性保留UNKNOWN，断连不提前释放已复制控制请求或实际I/O资源。同一port的真实DATA半包/第二DATA额度拒绝、UNCLASSIFIED满额下已有CONTROL推进及零effect拒绝已验证；14类391项为386通过/5平台跳过，style通过；有限矩阵40正向完整PASS/128指定反例。生产listener、完整control-plane、TLS/socket/native/storage总账、fresh startup/readiness及步骤11/Gate仍未完成。

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
- 在锁定有界live set和admitted速率下debt/dead bytes有界、停止新写后按deadline排空，前台不能侵占维护保留量；
- 同坐标不同payload不覆盖，local success后授权点读可定位，normal/full-index-loss两条恢复路径分别达标；
- 基础point recovery、当前ACK故障域、普通delete竞争与安全本地回收Gate通过；普通logical success不等待全部访问屏障，旧reader在本地tombstone前可读，强撤权另行接受；
- ByteBuf生命周期、normal/recovery LAC/digest兼容与实际shard合批通过；allocation/copy bytes/entry、CPU/entry、entries/durability barrier及全部control/compaction写成本均有原始计量；strong reset disabled不构成恢复保证续期。
- 初始writer/只读/恢复接受条件与CRC32C布局单义；submitted DATA不覆写、barrier覆盖明确且全部允许成功的数据重启可发现；delete-applied不依赖physical回收也不冒充强屏障；全阶段预算、最老deadline、大entry和控制保留容量均有确定性反例验证。
- LAC 99/候选100、同批L1 fence/L2合法、提交X超时后重试Y、11 unknown/12完成、独立小record点读及大量hot ledger/慢reader六组场景通过；未解析I/O不转为absence，点读完整性与buffer/pin有界，shared和dedicated证据分开。

候选阈值可以在正式测试前经一次评审调整；看到结果后不得追溯修改 Gate。

## 12. 当前开放问题

以下问题必须在对应 RFC 接受前闭合：

- failure-domain policy与mandatory capability ID/semantic-version registry、各production Profile exact combination、optional hint envelope；descriptor schema/hash后续改造必须直接修改当前合同并同步technical discriminator、migration边界与compatibility corpus，当前codec/hash/bounds不再OPEN；
- sidecar backend adapter、exact root/child/page schema、domain sharding与hard bounds、snapshot retention/hash、immutable backlink encoding、ledgerId reuse 和 profiled metadata mutation authority；
- principal allowlist/backend配置、Profile metadata mutation ACL、cold authority reference packing、所选protected Bookie control-log的physical framing/at-rest protection与验证、bounded grant/readable packing、future corrected SASL与跨instance key rotation；endpoint/mTLS/exact authorizer逻辑不再OPEN；
- control tail最终字段、batch/range future body、general E/W/A exact Java/admin outcome与BKException/detailCode；Round 7 frame/HELLO/subtype只有未来显式恢复并通过raw corpus后才能晋升stable，当前执行延期且不再作为任意设计空间；
- production exact E/W/A/failure-domain/capability组合，以及`E>W`下local validation/index coverage；general range/TailSummary fast path仍由RFC-0004和Model E阻塞；
- `ArenaControlLog` conditional transition/result exact API、sequencer/queue/batch bounds、segment、checkpoint A/B、selector/pin同步、current-selector/retiring-state packing、superblock 切换、`MOVE_COMMIT` encoding/orphan GC 与设备失败判定；
- RFC-0005选定Bookie级control-log原型的exact durable packing与真实故障证据；same-scope old-binary-visible fence当前BLOCK且`BKPF1`仅candidate；Cookie/superblock/device-manifest exact bytes、minimum reader/writer、migration/reverse/wipe tooling与persistent readiness backend adapter；new BookieId/new scope fallback语义不再OPEN；
- shared slab的lifetime class、compaction policy/hot promotion、每Arena维护保留预算、准入阈值及长期回收进展证据；
- RepairIntent child enumeration/retention/orphan cleanup、strong completion record/page/snapshot schema、overlapping-range predecessor encoding、coverage audit commitment 与 compaction policy；
- Delete Coordinator 的 stream topology、PREPARED/effective assignment encoding、snapshot chunk/schema、cluster-accepted wipe/decommission proof 和 tombstone retention；
- streaming range 在一般 E/W/A 下的结果合并与point-oracle伪代码、required-coordinate/normal-tail proof、rich outcome exact enum/wire/admin API/legacy mapping、cancellation、可选 checkpoint 与 recovery-specific semantics；
- 每个正式 Gate 的基准 commit、硬件和完整 manifest。

开放问题存在时，对应章节只能标为 Proposed、Spike Ready 或 Blocked，不能标为 Implementation Ready。

## 13. 当前可执行工作

当前工作按§10的施工顺序推进：

- 固定 stock `BK_CLASSIC` benchmark；
- 按CLIENT-1/2修复现有客户端并运行A29/A30定向回归；独立于Wave 0，不改变Segment生产authority；
- 对 `BK_CLASSIC_TUNED` 做不改变 on-disk/wire semantics 的调优；
- 随原型关闭实际遇到的四份子RFC问题，优先交付最小可测路径；不把新一轮全量架构扩展作为前置；
- 实现Spike A/B/C的manifest-locked harness/Model，并在锁定环境和停止条件后执行；
- 复用已完成的`ProfileDescriptor` reference codec、golden corpus与独立verifier；不能分配未接受的policy/capability业务ID；
- 复用已完成的Profile control/auth typed reference endpoint、exact-scope authorizer、cold authority与protected-state语义接口；真实TLS listener和MetadataStore adapter按步骤11、12接入隔离实验；
- 保留已实现的Round 7 frame/HELLO/status experimental codec、610-vector corpus与冻结反例；raw old-decoder/stock-binary执行延期，不发布stable compatibility；
- 首批使用new BookieId/new-scope隔离；persistent readiness真实adapter按步骤12推进，`BKPF1` same-scope candidate保持独立兼容Gate，原地迁移不作为首版额外功能；
- Wave 0已完成隔离的startup/readiness/new-scope reference implementation及immutable receipt：typed compatibility/device/recovery facts、九阶段fail-closed顺序、durable local readiness、versioned persistent CAS、matching service-info、stale demotion、ephemeral writable registration、九个crash/restart边界和17项普通测试；它不接入`EmbeddedServer`、生产registration、真实OS权限或normal Add，不提升Spike/Gate/production状态；
- 实现不接管authority、可丢弃且不污染Classic rollback cohort的局部/isolated shadow prototype，用于否证crash recovery、资源上限和p99假设。

现在不能直接开始：

- 让Segment接管生产ACK authority、进入live shadow或生产promotion；
- 在raw decoder/stock old binary Gates、policy/capability registry、所选local physical owner验证与control tail闭合前提交stable wire/on-disk兼容面，或让shadow污染可rollback Classic cohort；
- 把 per-ledger minimum 8 MiB extent 当作不变量；
- 将本地 delete success 当作全物理删除完成；
- 把 BtrLog 模型结果当作本方案的形式化证明；

本文的下一次状态提升，必须引用被接受的子 RFC、Spike immutable artifacts 和完整 Gate receipts。

历史（`00edeb8f`版本）2026-10-03 Bookie applied checkpoint已接入，保留sealed credential、权限/条件结果、最新LAC及recovery close；共享codec各自拥有A/B发布页，先durable/apply commit再发布，验证完整旧日志后恢复image与后缀。两个control owner的DATA正常restart、7项新增真实文件回归及有限权限模型有回执：`unified-wal-local-rECuQDLZ`（192项/187通过/5平台跳过）和`unified-wal-model-pz77hf26`（4个正向、6个指定反例）。全部旧控制日志仍必需；rotation/回收、在线维护、派生索引、完整Model A/A+C及生产Gate未完成。

历史（`43bfd80b`版本）2026-10-03 Arena物理rotation已实现：停写后连续建立两个只差内部commit的applied checkpoint，绑定两条原commit identity，先force新日志代并同步目录，再force/验证两份恢复根，最后回收旧prefix。固定sidecar锁覆盖切换和另一owner身份检查；新代持续全局sequence，后续checkpoint保留日志代绑定。普通控制/故障预留保留416字节供rotation，两份image与全部首次条件结果仍受固定状态预算限制。真实重复填满/rotation、单页或image损坏回退、必需suffix损坏拒绝、短写/force失败、跨JVM排他、7个进程退出阶段及双owner DATA图两轮restart有回执`unified-wal-local-3Udoan9d`（195个源码输入、199项/194通过/5平台跳过）；有限模型`unified-wal-model-t1waf2bm`（29个实际输入、5个正向完整PASS、7个指定反例）新增双根/目录同步/旧prefix回收投影。Bookie旧日志仍全部必需；Bookie rotation、在线维护、长期幂等历史压缩、派生索引/MOVE/FREE、完整A/C及组合模型、真实掉电和生产Gate保持未完成。

2026-10-03 Bookie停写rotation已复用双恢复根协议，保留sealed credential、fence/tombstone、条件结果、最新LAC及grant/close；两个owner分别force新日志/目录与独立发布页后才回收旧prefix，当前必要suffix仍完整验证。最新[本地实现及验证](rfcs/unified-wal/implementation/local-storage/README.md#bookie停写rotation与共享物理协议)为`unified-wal-local-sKIZwMRg`（197输入、206项/201通过/5平台跳过）和`unified-wal-model-bu9e_n9l`（31输入、6个正向完整PASS/8个指定反例）。权限投影新增回收后terminal恢复性质，独立物理模型检查双根barrier，不构成A+C组合。在线维护、长期幂等历史压缩、派生索引/MOVE/FREE、真实掉电、完整模型及生产Gate仍未完成。

2026-10-03 [真实RocksDB冷重建/覆盖切片](rfcs/unified-wal/implementation/local-storage/README.md#rocksdb冷重建与持久覆盖切片)已接入隔离正常restart：单CF、WAL关闭、重建代先退休旧覆盖，实际flush后才发布identity/cut/stream/content绑定的A/B页；全库删除后由同步且route选择的DATA重建，不复活tombstone或未提交copy。Java回执`unified-wal-local-oPEjR8PE`为201输入、215项/210通过/5平台跳过；模型`unified-wal-model-p_83_s8r`为36输入、7个完整正向/9个指定反例。新覆盖投影否证未flush发布，不构成完整模型或掉电证据。在线异步handoff/hot淘汰、coverage跳过扫描、MOVE/delete/stall排序、长期资源与生产Gate仍未完成。


2026-10-03 [异步派生定位追加切片](rfcs/unified-wal/implementation/local-storage/README.md#异步派生定位追加切片)已复用owner/DATA票据和既有DATA I/O：每shard一个在途、每批最多256定位，热结果先成立，Put不推进持久覆盖；索引暂停或失败不占控制credit，热容量/后台故障在新DATA预留前背压。Java回执`unified-wal-local-RBhd0IZf`为201输入、218项/213通过/5平台跳过；模型`unified-wal-model-tn4d2l9o`为41输入、8个完整正向/10个指定反例，新增482状态的控制预留安全性投影，不证明死锁自由。hot记录仍保留，fresh fallback/淘汰、在线覆盖、MOVE/delete排序、长期预算、完整模型与生产Gate仍未完成。

2026-10-03 [Query-visible热定位接管切片](rfcs/unified-wal/implementation/local-storage/README.md#query-visible-热定位接管切片)已在实际Put后由原owner释放hot，点读及普通/恢复重复写hot miss后fresh Get、DATA校验复用同一票据，捕获定位与文件pin保护在途使用。有限坐标占用与完整恢复容量独立于hot容量，支持超过hot容量后的正常restart/删库重建及乱序填洞，不把derived missing授权为同坐标重写。Java回执`unified-wal-local-eIFmOEQ7`为201输入、222项/217通过/5平台跳过；模型`unified-wal-model-2qh8pivn`为43输入、8个完整正向/11个指定反例，接管投影858状态。在线coverage/扫描跳过、MOVE/delete、完整native/disk总账与长期负载、未正常关闭恢复、完整模型及生产Gate仍未完成。


2026-10-03 [运行期索引覆盖检查点切片](rfcs/unified-wal/implementation/local-storage/README.md#运行期索引覆盖检查点切片)已复用每原worker一份DATA票据冻结已追平cut，暂停索引pump后校验实际native内容、真实flush及A/B页force；同代publication按有效页递增，新hot DATA不扩大该cut。部分DATA额度拒绝先解冻，取消/关闭等待实际I/O及原owner终结，页force失败不报告成功或破坏既有query-visible行。Java回执`unified-wal-local-EG93uRLJ`为201输入、226项/221通过/5平台跳过；模型`unified-wal-model-47t1o26s`为48输入、9个完整正向/14个指定反例，新覆盖投影3,102状态。当前仍全量扫描；coverage缩短扫描、MOVE/delete、在线控制维护、完整native/disk预算、长期负载、未正常关闭恢复、当前在线页真实进程退出/掉电及完整模型和生产Gate未完成。


2026-10-03 [持久覆盖辅助的正常恢复切片](rfcs/unified-wal/implementation/local-storage/README.md#持久覆盖辅助的正常恢复切片)已验证实际index内容与当前scope/控制/stream/grant；覆盖内校验固定批头和连续性，未覆盖授权tail完整解析及DATA恢复sync，无效覆盖全扫描。有效覆盖保持index代，只补写tail并按Bookie后缀删除不可读行；头缺失/物理缺口/必要CRC或sync失败不开放，旧定位不豁免点读校验。Java回执`unified-wal-local-hqPDoMPC`为201输入、231项/226通过/5平台跳过；模型`unified-wal-model-h1roqnsp`为53输入、10个完整正向/17个指定反例，新恢复投影280状态。一般MOVE/FREE、完整全局native/disk预算、在线控制维护、长期负载/多Arena、未正常关闭恢复、真实掉电及完整模型和生产Gate未完成。

2026-10-03 [运行期索引的真实进程退出边界](rfcs/unified-wal/implementation/local-storage/README.md#运行期索引的真实进程退出边界)新增实际memtable、覆盖页force前、PERSISTED owner回调内和正常CLOSE后4个JVM退出点；未关闭会话即使物理重建后仍拒绝，正常CLOSE后可用同代coverage恢复并读/重试/追加。Java回执`unified-wal-local-Gs6OWJ9U`绑定父提交`3b1b6cfd`与201输入，Bookie全类140项通过、0失败/错误/跳过。现有53个模型输入哈希未变，本轮未重复运行TLC；未关闭会话恢复、真实掉电、一般MOVE/FREE、长期负载/多Arena及生产Gate仍未完成。


2026-10-03 [分配控制顺序与低地址追加](rfcs/unified-wal/implementation/local-storage/README.md#分配控制顺序与低地址追加)消除FREE/reuse必要的offset排序前置：游标按原成功allocation控制sequence推进，旧grant尾部不返还；恢复按每allocation连续序号区间拼接，跨grant gap/duplicate仍拒绝。Arena rotation及真实全量/同代覆盖storage重启已验证。Java`unified-wal-local-N16YauZh`为201输入、237项/232通过/5平台跳过；模型`unified-wal-model-p7nk5a81`为57输入、11个完整正向/19个指定反例，新投影302状态。实际FREE/bump、整块存活与旧I/O/pin排空、退休sequence/旧字节恢复、compaction、长期负载/多Arena和生产Gate仍未完成。


2026-10-04，[整allocation全死FREE与退役前缀](rfcs/unified-wal/implementation/local-storage/README.md#整-allocation-全死-free-与退役前缀)已接通真实DATA/终态tombstone proof、准入/I/O/read pin排空、CONTROL durable及原owner条件apply、generation 1→2与metadata-only退休范围恢复。rotation后覆盖退役字节的全量/同代覆盖storage重启仍保留同文件活数据。后续[健康整块复用](rfcs/unified-wal/implementation/local-storage/README.md#整-allocation-复用与健康关闭末端)已由原refill选择匹配FREE generation并以实际CLOSED末端恢复当前prefix；同块两轮复用及共享活数据恢复通过。后续[运行期终态清理](rfcs/unified-wal/implementation/local-storage/README.md#运行期终态索引与坐标回收)已接通native删除、原owner预算释放及持久覆盖，1/4 worker固定容量四轮FREE/reuse验证。OPEN恢复、MOVE/compaction、长期净收益、多Arena及生产Gate仍未完成，步骤9保持PARTIAL。 后续[relocation 内部完成](rfcs/unified-wal/implementation/local-storage/README.md#relocation-副本内部持久化完成)已接通 copy-only DATA 实际持久化结果与原 owner 资源归还，尚不启用 MOVE/selector 或部分存活 compaction。


2026-10-04 当前增量见[受保护的条件MOVE](rfcs/unified-wal/implementation/local-storage/README.md#受保护的条件-move-与当前位置)：实际copy验证、双坐标全批条件、durable后Arena selector、业务读取/幂等及全量/rotation恢复已接通；复用既有全局排空与CONTROL service。MOVE与源/orphan FREE、native维护/coverage、部分存活compaction和长期净容量仍未组合完成，步骤9/整体Gate保持未完成。当前模型矩阵16正向/36指定反例，局部证据不授予生产authority。


2026-10-04 当前增量为[搬迁源与orphan整块回收](rfcs/unified-wal/implementation/local-storage/README.md#搬迁源与-orphan-的整块回收)：FREE v2持有selector cut与原保护，迁走源实际验证当前目标，orphan/终态COPY退役且旧grant不得复活。真实A/B均复用、无新checkpoint和rotation后的C恢复通过；在线native MOVE/coverage、部分存活合批compaction、长期净容量仍待完成，步骤9和总目标保持未完成。模型矩阵17正向/40指定反例不授予完整Gate。


2026-10-04 当前后续：[Native MOVE索引维护与覆盖恢复](rfcs/unified-wal/implementation/local-storage/README.md#native-move-索引维护与覆盖恢复)已接通现有checkpoint的原owner旧Put排空/暂停、保护下实际target验证与native更新、flush及A/B coverage；覆盖恢复仍验证当前必要副本，陈旧hint及坏/缺失target不能回退旧权限。取消/短关闭、维护校验失败及发布失败重发已由真实路径验证；七类271通过、5平台跳过，有限矩阵18正向/44指定反例。storage图的合批victim compaction/净回收循环、逐allocation在线cutover、长期控制/SST空间、多Arena、OPEN错误恢复及掉电/生产ACK仍待完成；完整16步与Gate不提升。


2026-10-04 当前增量：[同Arena有界合批compaction](rfcs/unified-wal/implementation/local-storage/README.md#同-arena-有界合批-compaction)已组合显式1..16个同stream source的实际扫描、allocation粒度净收益判断、独立destination、一批COPY/条件MOVE、原owner native覆盖维护及一次CONTROL barrier的合批整块FREE。各v2 FREE绑定自身前序cut，共享保护到全部claimed成员终结；取消/发布失败不提前释放或伪造净回收，后续新operation可完成保留source的回收。真实共享块、零净收益拒绝、两块减一块的净容量、全T无destination、复用恢复及失败/取消重启由当前回归验证。仍为全图暂停/一个COPY/显式组，自动选组、逐allocation在线cut、长期control/SST/文件系统净预算、多Arena、OPEN错误恢复、掉电和生产ACK未完成，步骤9/10和完整Gate不提升。


2026-10-04 当前增量：[同Arena多批compaction](rfcs/unified-wal/implementation/local-storage/README.md#同-arena-多批-compaction)在既有显式source组内按owner buffer与256行上限划分COPY/MOVE，所有批次完成后统一native覆盖及合批FREE。净成本包括各批framing/padding总和再按destination整allocation向上取整；后批失败保留已成功selector与所有source，不伪造整组回收。40/402坐标的跨批读取、覆盖/全量恢复及source复用、后批force unknown隔离、零净收益和源预算拒绝已由真实路径验证。源snapshot总预算和既有source batch扫描buffer仍有显式上限；自动选组、逐allocation在线cut、长期净容量、多Arena、OPEN错误恢复和生产Gate保持未完成。


2026-10-04 当前选择增量：[固定shard有界回收](rfcs/unified-wal/implementation/local-storage/README.md#固定-shard-的有界回收选择)按成功ALLOC顺序查看同文件/唯一stream最多16个当前grant；真实DATA测量先于payload捕获，优先直接回收零current live组，最终仍独立whole-allocation后继证明与合批FREE。满容量且无关current live超过源输入预算的实际回收、source generation2复用及17个grant窗口保留年轻共享活数据已验证；既有共享T及多批COPY/MOVE回归改经shard入口。七类283通过/5平台跳过及20正向/51指定反例仅为局部证据。窗口外公平选择、partial任务自动收敛、逐allocation在线cut、长期完整净容量、多Arena、OPEN错误恢复与生产Gate仍未完成。


2026-10-04 当前轮转增量：[有界回收窗口与容量循环](rfcs/unified-wal/implementation/local-storage/README.md#有界回收窗口轮转与有限容量循环)复用固定shard/file映射，按固定本轮ALLOC上限推进进程内游标；新grant留至下轮，首窗口拒绝后可轮到更晚dead并恢复destination，完成轮次后回访旧T。真实17个满容量grant、后续普通refill、1/4 worker及四shard四轮自动FREE/generation1→5、重启读取已验证。七类285通过/5平台跳过及21正向/53指定反例保持局部范围；跨窗口最优组合、部分任务收敛、维护调度、per-allocation cut、长期control/SST/文件系统完整净预算、多Arena、OPEN错误恢复与生产Gate未完成。


2026-10-04 当前池化增量：[池化分批捕获](rfcs/unified-wal/implementation/local-storage/README.md#池化分批-compaction-捕获)复用原owner两个buffer逐批点读/复制，不持整组payload；有限计划以既有坐标上限约束，并在每批重新复核source hash、selector、Bookie cut与测量长度。所有COPY/MOVE、native覆盖及真实整块proof后仍才FREE；池不足或后批身份变化返回PARTIAL并保留实际占用。真实40条/三批超过源输入预算、单buffer拒绝及有效CRC源变化/实际控制变化、恢复验证通过。七类287通过/5平台跳过、22正向/55指定反例保持局部范围；原source batch扫描buffer、全graph暂停和单连续destination限制保留，长期完整净容量、多Arena、OPEN错误恢复、掉电/性能及生产Gate未完成。


2026-10-04 当前部分搬迁增量：[满容量保留tail续写](rfcs/unified-wal/implementation/local-storage/README.md#满容量部分-move-的保留-tail-续写)在新grant无空间时，验证含current COPY的最新同stream grant并保留为目的地，剩余COPY总量须适配其实际空闲tail；只为受保护证明建立最新tail cursor，不回更旧grant，目的地排除FREE。真实PARTIAL后健康重启、1/4 worker、gen1/gen2旧残留tail、21条/两批续写、两源净FREE2 MiB与普通source复用/重启、tail不足及坏字节0COPY已验证。复用新增destination容量为0仍报告实际ID。七类289通过/5平台跳过及23正向/57指定反例保持局部范围；任意partial自动收敛、调度、长期完整容量、多Arena、OPEN错误恢复、掉电/性能及生产Gate仍未完成。

2026-10-04 当前空间准入增量：[前台refill维护容量预留](rfcs/unified-wal/implementation/local-storage/README.md#前台-refill-的维护容量预留)把最大compaction目的地whole blocks与每DATA文件内部预留绑定，单compaction并发下同文件合批refill共同扣减，超过声明目的地上限在ALLOC前拒绝；预算从已apply grant/FREE重建。真实前台拒绝→COPY/MOVE/native/FREE→净恢复→健康重启/gen2再准入及两block工作集已验证。当前仅内部容量，extent碎片、filesystem/SST/control/checkpoint临时空间、低水位/hysteresis、长期负载与多Arena仍未闭合；不关闭B17、步骤9/10或生产Gate。历史回执保留原绑定。

2026-10-04 当前控制维护增量：[运行期控制日志轮转](rfcs/unified-wal/implementation/local-storage/README.md#运行期控制日志轮转)已接入显式graph入口、原CONTROL票据/I/O及两个driver排他名额。在Bookie admission与全DATA文件保护下依次完成双owner rotation，保留真实OPEN session/epoch/pool/游标及CLOSE余量；取消只撤回复，保护留到原owner终结，已尝试失败报告UNKNOWN并停止准入。1/4 worker满额后两轮原pool续写、读/幂等/健康重启、真实pin与force暂停/失败验证通过。七类295通过/5平台跳过及25正向/65指定反例为局部证据；自动调度、filesystem/SST/checkpoint临时工作集、长期负载、多Arena与OPEN错误恢复仍未完成，不关闭步骤8/10、B17或生产Gate。

2026-10-04 当前自动恢复增量：[控制容量压力的自动轮转](rfcs/unified-wal/implementation/local-storage/README.md#控制容量压力的自动轮转)把可fresh-fit的容量拒绝接入既有owner pump和受保护CONTROL rotation；pressure期间暂停新graph准入，已接受entry/I/O/pin排空后再尝试，同步quarantine拒绝也唤醒维护。新journal与原owner终结后恢复原pool续写；超大请求不触发无收益轮转，自动UNKNOWN保留cause并停止准入。真实红/绿、1/4 worker两轮满额/健康restart、单Arena拒绝唤醒及错误验证通过；七类298通过/5平台跳过、26正向/68指定反例为局部证据。提前低水位/一般调度、filesystem/SST/checkpoint临时工作集、长期负载、多Arena与OPEN错误恢复仍未完成，不关闭步骤8/10、B17或生产Gate。

2026-10-04 冷启动容量增量：[满控制日志的健康冷启动](rfcs/unified-wal/implementation/local-storage/README.md#满控制日志的健康冷启动)在真实恢复扫描/必要同步与同一DATA锁之后、下一OPEN之前，仅对确为CLOSED且新journal能容纳OPEN及实际CLOSE/双root预留的Arena执行既有轮转，保留Bookie恢复cut。健康重启、写者排他和失败边界的证据见该节；一般OPEN错误恢复、filesystem临时预算、长期负载、多Arena及步骤8/10/B17/生产Gate仍未完成。


2026-10-04 空间压力维护增量：[数据空间压力的自动回收](rfcs/unified-wal/implementation/local-storage/README.md#数据空间压力的自动回收)把前台pool/refill的内部容量拒绝接到既有owner pump；控制rotation先行，同文件固定shard按冻结cut和16-grant窗口检查，实际pin/Bookie/buffer条件满足后复用native cleanup/COPY/MOVE/FREE。无净收益轮停下等待新控制事实，已知预写入控制容量拒绝先rotation再重新保护，真实UNKNOWN保留cause并停止图。旧生产红、新生产1/4worker自动恢复/当前COPY tail/健康restart、跨shard资源等待、全live与T、控制满额及COPY force失败的局部证据为七类306通过/5平台跳过、28正向/75指定反例。单buffer或长期资源占用没有自动进展保证；低水位/维护份额、filesystem/SST/control-checkpoint临时预算、长期负载、多Arena、全部partial收敛、一般OPEN错误恢复及性能/掉电仍待验证，步骤9/10、B17和生产Gate未完成。

2026-10-04 内部 checkpoint 历史增量：[有界内部保留](rfcs/unified-wal/implementation/local-storage/README.md#内部-checkpoint-历史的有界保留)仅压缩已被后续根取代的成功 CHECKPOINT_COMMIT；当前双 root 身份与最新两个内部 commit 合计至多四项，原业务首次结果、grant/credential/T 与失败条件仍保留，实际物理回收完成后才清理进程内表。[image v2](rfcs/unified-wal/implementation/local-storage/format-v1.md#内部-checkpoint-历史压缩与-image-v2)明确允许该内部 generation 间隔，v1 仍要求原完整历史，准确 predecessor 与必要 suffix 检查保留。真实旧生产预算耗尽红、新生产同 16384-byte 预算固定状态 96 次双 owner 轮转、健康 restart、旧 v1、独立新 v2 向量与单 image 损坏回退已验证；七类 309 通过/5 平台跳过、29 正向/79 指定反例保持局部范围。业务/会话历史压缩、持续创建删除、filesystem/SST/checkpoint 临时总账、长期负载、多 Arena、一般 OPEN 错误恢复、掉电/性能和完整模型仍待完成，步骤 8/10、B17 与生产 Gate 不关闭。

2026-10-05 原 journal checkpoint 恢复增量：[统一恢复入口](rfcs/unified-wal/implementation/local-storage/README.md#原-journal-的压缩-checkpoint-恢复)使 Bookie/Arena 在未物理轮转时也按合法 v2 内部代际间隔恢复，复用轮转路径已有的准确 predecessor、原条件结果和完整必要 suffix 验证；无格式或预算变化。真实旧生产两项红，新生产八次 checkpoint 后选择最高 image、最新 image 损坏后选择上一代并重放 suffix、幂等/失败/冲突/T 拒绝已验证，七类 311 通过/5 平台跳过及 style 通过。本轮扩展有限模型 29 正向完整通过、80 指定反例符合预期，保持局部输入假设；业务/会话历史压缩、长期负载、多 Arena、完整模型及步骤 8/10、B17 和生产 Gate 仍未完成。


2026-10-05 退役 DATA 区间增量：[合并恢复投影](rfcs/unified-wal/implementation/local-storage/README.md#退役-data-序号区间的合并)仅合并准确 stream、连续物理序号且不跨 current grant 原控制顺序的成功退役事实，包括未写 grant 的边界；全部 FREE 的最大必要 Bookie cut 含空退役，缺少时拒绝恢复。真实 24 次同 block FREE/reuse、第 25 次当前 DATA、双 owner rotation/健康 restart、未写中间 grant 与空 FREE 缺少 cut 的反例已验证。七类 314 通过/5 平台跳过、style 通过，有限模型 30 正向完整通过/84 指定反例；历史表与排序工作集仍增长，未实现请求代、完整业务状态裁剪、长期负载或多 Arena，步骤 8/10、B17 与生产 Gate 仍未完成。


2026-10-05 Arena 当前索引增量：[当前授权与历史身份分离](rfcs/unified-wal/implementation/local-storage/README.md#arena-当前授权与历史身份的分离)由 current grant 保存原成功顺序，已用 ID 闭区间与成功高水位保留空洞及 anti-ABA；成功 FREE 增量折叠退役范围，空 FREE 移除当前边界后也重新合并并保留必要 Bookie cut。恢复读者不再排序历史 FREE，但 image 仍保存原业务命令/首次结果并重放，原 UUID、session、MOVE/Bookie T 历史未裁剪，实验格式不变。真实 ID 空洞/失败高 ID/退役身份拒绝、最高 image 与损坏后 fallback、原 grant 顺序、空边界合并和 gen2 DATA 双 owner rotation/健康 restart 已验证；七类 315 通过/5 平台跳过、style 通过，有限模型 31 正向完整通过/89 指定反例。未实现请求代、完整业务 image、总工作集有界、长期负载、多 Arena、掉电/性能或完整模型，步骤 8/10、B17 与生产 Gate 仍未完成。


2026-10-05 当前 COPY 状态增量：[COPY 证明与 selector](rfcs/unified-wal/implementation/local-storage/README.md#当前-copy-证明与-selector-状态)把当前 allocation 内逐条成功 COPY 的 location/UUID/generation/digest 与当前 selector 的 target/身份/原序号/必要 Bookie cut 从历史 MOVE 查询中分离。失去 selector 的成功 COPY 在 allocation 当前时仍保留证明，失败 MOVE 不发布，FREE 删除该 allocation 的全部证明；selector 不持有原整批 MOVE。原历史结果仍保留，image v1/v2 与实验字节不变。真实成功/失败 COPY、最高 image/损坏后 fallback、物理 rotation、FREE/复用/restart 及 force UNKNOWN 回归通过；七类 315 通过/5 平台跳过、style 通过，有限模型 32 正向完整通过/93 指定反例。请求代、独立紧凑业务 image、UUID/session/MOVE/T 历史裁剪、总工作集、长期负载、多 Arena、掉电/性能及完整模型尚未完成，步骤 8/10、B17 和生产 Gate 保持未完成。


2026-10-05 步骤 12 增量：[真实 ZooKeeper readiness CAS](rfcs/unified-wal/implementation/startup-readiness/README.md#步骤-12真实-zookeeper-readiness-cas-增量)复用原接口，验证完整持久记录、store/readiness 双版本、birth 原子检查和提交后 UNKNOWN/fresh-read 解析；真实双 client/server 的 8 项测试加原 17 项 reference 测试全部通过，Spotless/Checkstyle 通过。有限 registration 投影为 Model A 局部验证，当前矩阵 33 正向完整通过/96 指定反例，不代表 A+C 组合或真实 quorum。原 Wave 0 回执冻结；其旧 scope guard 对累计后续路径返回 BLOCK，未扩充 allowlist。真实 startup/ephemeral、ProfileControlStore、安装激活及生产 readiness 仍未完成，步骤 12/Gate 不提升。


2026-10-05 registration 增量：[session-owned ephemeral 发布](rfcs/unified-wal/implementation/startup-readiness/README.md#步骤-12真实-ephemeral-发布增量)复用既有接口及 persistent scope，info/writable multi 检查 authority、nonce 与版本；demote 永久退休 nonce，实际过期清除 ephemeral，旧删除不命中新同版本注册。12 新测试加原 25 项全部通过，Spotless/Checkstyle 通过；有限 Model A publication 投影及全部先前切片为 34 正向完整通过/100 指定反例，不是 A+C/D 完整组合。typed local facts/内存 local receipt 不代表真实恢复，生产 BookieServiceInfo/registry、startup/delete catch-up 与持续连接事件降级仍未接线；步骤 12 与生产 Gate 保持 PARTIAL，历史 receipt/scope guard 不改绑。


2026-10-05 本地 readiness 增量：[真实文件 receipt](rfcs/unified-wal/implementation/startup-readiness/README.md#步骤-12真实本地-readiness-receipt)实现原 LocalReadinessStore，复用实际 writer 锁与 FileChannel；完整 temp write/force→原子替换→目录 force 后才 APPLIED，UNKNOWN 实例拒绝读成功，新 attempt 严格解析并同步已发布 receipt，未发布 temp 不恢复。100 代替换保持固定文件集合，短写、两个 barrier 故障、scope/格式损坏、跨进程互斥及两轮退出/重开已验证；原 harness 已组合真实文件与两个 ZK stores。48 项相关测试及 style 通过，有限本地 publication 与既有模型为 35 正向完整通过/105 指定反例。输入的 recovery/device/delete facts 仍非真实恢复，pre-replay startup、生产 registry、持续失连准入、control store/安装激活及完整模型/掉电/性能未完成，步骤 12 与生产 Gate 保持 PARTIAL。 原 reference receipt 和既有真实 ZK 历史结果不改绑。


2026-10-05 控制读取增量：[真实 ZK 控制权威读取](rfcs/unified-wal/implementation/profile-control/README.md#步骤-12真实-zk-控制权威读取)直接实现原 ProfileControlStore，在独立 provisioned namespace 内按 derived key 执行一次 sync/get；只有真实 NONODE 才是不存在，无效/不完整/ephemeral view 隔离，错误/超时/过期为 unavailable，payload 与实际 Stat.mzxid 来自同一 snapshot。73 项相关测试及 style 通过，有限读取和全部既有模型为 36 正向完整通过/109 指定反例。它只读取单份 experimental authority view，未实现完整 sidecar/backlink/fanout/发布 CAS/安装激活；真实 startup/delete catch-up、生产 registry/持续准入及步骤 12/Gate 仍未完成。

2026-10-05 准入增量：[session 绑定的本地 DATA gate](rfcs/unified-wal/implementation/startup-readiness/README.md#步骤-12session-绑定的本地-data-准入)在首次 cold 操作前绑定实际 ZK watcher manager；匹配 owned registration 后开放，观察到失连、过期、close/demote 即永久退休原 attempt，同 session 重连及迟到读取不重开。隔离 graph 的普通/恢复 DATA 按本地 Bookie/incarnation 检查，热路径不查 metadata；已提交 I/O 与原池资源正常排空。八类 296 项及 style 通过，有限矩阵 37 正向完整 PASS/113 指定反例。真实 startup/fresh recovery/delete facts、生产 listener/registry、持续远端 authority、完整安装激活、quorum、完整模型与步骤 12/Gate 仍未完成。

2026-10-05 启动排他增量：[restart DATA 排他前置](rfcs/unified-wal/implementation/local-storage/README.md#restart-的-data-排他与控制恢复顺序)先取得真实 DATA 目录 writer lock，再恢复控制及 index，把同一 native lock 交给 files，保留原全部 header 校验/恢复同步与 CLOSED 限制。实际子进程外部锁红绿、控制打开/DATA 恢复失败清理及成功移交/读回验证完成；340 项中 335 通过、5 项 macOS 上的 Linux 亲和性跳过，style 通过。有限矩阵 38 正向完整 PASS/117 指定反例。只补本地 ownership 前置，compatibility/device/delete 权威、真实 startup/readiness、生产接线及完整模型仍待完成。

2026-10-06 [标准metadata backlink创建](rfcs/unified-wal/implementation/profile-control/README.md#步骤-12标准-metadata-backlink-创建)已将固定52-byte instance/opaque sidecar reference写入原标准metadata custom entry，并经真实ZK LongHierarchicalLedgerManager create-if-absent及每次创建结果后的实际read解析同内容重试与回复丢失；instance/reference/初始内容冲突不覆盖，实际创建后删除并读到NONODE仍保持UNKNOWN。新9项真实server回归及相关验证见该记录。它只提供全E安装前置已验证之后的创建原语；真正sidecar/全E receipts/READY/activation、后续birth-bound mutation CAS与完整步骤12仍OPEN，未返回normal writer或提升生产Gate。


2026-10-06 [真实PREPARING reservation与INSTALL冷读](rfcs/unified-wal/implementation/profile-control/README.md#步骤-12真实-preparing-reservation-与-install-冷读)已将最多32个计划目标及完整instance/backlink/descriptor/operation/payload写入真实ZK不可覆盖root，并为指定本地Bookie投影原ProfileControlStore的INSTALL authority；真实双client竞争、同identity冲突不覆盖、响应丢失/提交后timeout及scope/ACL/坏root拒绝已有10项回归。该root不授予READY/normal activation，计划集合不替代标准membership；all-E local receipts、READY/delete cut与birth-bound mutation CAS、activation/coordinator及完整步骤12仍OPEN。


2026-10-06 [PREPARING到真实local inactive install](rfcs/unified-wal/implementation/profile-control/README.md#步骤-12preparing-接线真实-local-inactive-install)已让原typed endpoint及实际ZK reservation冷读复用原Bookie控制driver/write/force/owner apply，只有durable结果成功，提交后timeout不取消实际任务或提前释放writer isolation。6项新验证含真实3个目标同phase并发force、全部durable install后仍normal-inactive；transport仍为typed facts，INSTALL真实TLS/wire、全局receipt/coordinator与metadata前置门槛、READY/CAS/activation及完整步骤12仍OPEN。


2026-10-06 [all-E后的初始metadata协调门槛](rfcs/unified-wal/implementation/profile-control/README.md#步骤-12all-e-后的初始-metadata-协调门槛)已固定descriptor/E/W/A/有序计划/credential与peer binding，先真实PREPARING，再并发收齐全部E的matching durable INSTALL及稳定birth-generation receipts，fresh-read原root snapshot后才调用标准metadata create/actual read；A个成功、UNKNOWN、错binding或root变化均不推进。冻结caller custom bytes及receipt capability digest，无secret字符串展开。返回metadata/receipts，尚无persistent retention、READY/delete cut CAS、actual INSTALL TLS/wire、全E activation或normal writer，步骤12仍PARTIAL。


2026-10-06 [initial READY与DELETE_INTENT CAS](rfcs/unified-wal/implementation/profile-control/README.md#步骤-12initial-ready目标留存与-delete_intent-cas)已真实持久化有界initial activation page，再由同一birth/semantic/version-bound lifecycle head CAS发布READY并与DELETE排序；旧root/page dataVersion0不能接受旧CAS，READY后的DELETE保留全部E及activation授权。actual READY/metadata冷读接入原typed endpoint和reserved physical driver，3个目标durable activation后本地normal admission开放；transport仍typed facts，没有实际control wire、normal writer返回或物理删除，完整步骤12/Gate仍PARTIAL。


2026-10-06 [可恢复初始协调与all-E availability](rfcs/unified-wal/implementation/profile-control/README.md#步骤-12可恢复初始协调与-all-e-availability-snapshot)已从fresh/PREPARING/candidate/READY恢复同一初始操作；持久page证明metadata曾存在，恢复只读校验并隔离缺失，不重建未知record。全部E当前matching durable activation及最后metadata/READY取证后返回AVAILABLE snapshot，历史fenced activation返回STALE不计数。实际新coordinator/ZK session及Bookie文件replay、partial/UNKNOWN/DELETE门槛已验证；无actual control TLS/wire、LedgerHandle/writer或production authority，步骤12仍PARTIAL。


2026-10-06 [初始control的真实隔离socket](rfcs/unified-wal/implementation/profile-wire/README.md#真实隔离初始-control-接线)增量：显式 opt-in 实验 capability/schema1 已把 INSTALL/ACTIVATE_INITIAL 接到真实隔离 mTLS、原 endpoint 与physical driver；默认能力及通用control-body冻结状态不变。真实principal、cold authority与exact AuthZ通过后，event loop原子转账；cold timeout/UNKNOWN/断连和实际I/O终结分开，副本持续计费至最后使用。七项socket回归含实际ZK PREPARING/DELETE cut和迟到EL授权零写入。非阻塞实际client、完整READY/metadata reader与all-E coordinator经socket组合、生产listener/readiness和统一总预算仍待完成，步骤11/12与stable/G1/生产Gate保持PARTIAL/OPEN。 相关17类412通过/5平台跳过、style通过；有限矩阵49正向完整PASS/184指定反例，完整源码及receipt绑定见实现记录。


2026-10-06 [初始all-E真实socket组合](rfcs/unified-wal/implementation/profile-control/README.md#可恢复初始协调器的真实-all-e-socket-组合)增量：实际非阻塞initial client借用已有event loops/TLS context，验证actual HELLO/target/incarnation及完整request/reply关联，pending quota在EL排队前取得并等wire/write两种终结，取消/断连不释放仍使用的副本；无自动重试或重连。可恢复初始coordinator现直接经3个mTLS socket与实际ZK PREPARING/READY/标准metadata/原Bookie文件driver完成全E两phase，再返回AVAILABLE snapshot；actual activation reply丢失后显式新client/新coordinator解析同一READY/metadata/op。缺必需metadata仍不重建。8项组合及输出额度RED/GREEN、18类420通过/5平台跳过、style、50正向完整PASS/191指定反例已验证。尚无LedgerHandle/生产writer、普通DATA quorum SDK、replacement/recovery/full delete、完整server-process启动/readiness、统一总预算或production authority，步骤11/12与stable/G1/生产Gate仍PARTIAL/OPEN。 源码与回执绑定见实现记录。


2026-10-06 [普通DATA client](rfcs/unified-wal/implementation/profile-wire/README.md#普通-data-client-与-sdk-接线边界)增量：actual ordinary DATA client复用shared TLS/HELLO/correlation及write/wire quota，借用原DigestManager/v3 ByteBuf/ByteBufList并保留完整status，initial-control公共API不变。真实三目标force后的status进入原AckSet，A=2不计重复target，ordinary路径无authority cold-read。取消、quota/N/context/credential、timeout同坐标解析、force UNKNOWN和reply破坏通过；这是transport/AckSet组合，尚无完整LedgerHandle/PendingAddOp SDK、有界退避/UNKNOWN累积、Profile换组或fenced-close。步骤11/13及stable/G1/生产Gate不关闭。 18类426通过/5平台跳过、style及51正向完整PASS/196指定反例，绑定见实现记录。

2026-10-06 [固定初始 ensemble 的原 SDK 写入接线](rfcs/unified-wal/implementation/profile-wire/README.md#固定初始-ensemble-的原-sdk-写入接线)增量：AVAILABLE snapshot 经 metadata/page、全 E matching activation 与实际 DATA HELLO incarnation 校验后构造实验 ProfileLedgerHandle，复用原 LedgerHandle/PendingAddOp 的坐标、digest、quorum 和连续回调。每目标在原 worker/scheduler 上对同一坐标及保留字节做有限退避；当前 NONE 不清除先前 UNKNOWN，in-flight 超时/本地停止保留不确定性。前台 entry 和目标投递分别有界。真实 3/3/2 的后 entry ACK 先到、slot 背压/前台拒绝、deadline、force UNKNOWN→NONE 和 scheduler 拒绝已验证。Profile 换组、fenced-close、点读/recovery 尚未接通，当前停止只排空本地资源，metadata 保持 OPEN；关闭接口返回 IllegalOp，不能误入 Classic metadata 变更。步骤 13 仍 PARTIAL；步骤 11/12、stable/G1 与生产 Gate 不关闭。新回执与有限模型范围见实现记录，历史快照不重绑定。

2026-10-06 [标准 membership 的出生绑定 CAS](rfcs/unified-wal/implementation/profile-control/README.md#标准-membership-的出生绑定-cas)增量：原 ProfileLedgerMetadataStore 的显式借用 ZK context 现在提供 cold readMembership/replaceEnsemble。一次操作冻结实际父记录 czxid、creator token、immutable backlink、完整 metadata/version 与 LAC+1/mapping；只读 birth child 与 parent version 在同一 ZK multi 中检查，防止 precheck 后删除重建的 version0 ABA。保留 user/custom/credential/state/E/W/A/ctime/creator token，拒绝 recovery/CLOSED、membershipFrozen、pendingPublication 和未知 reserved key；UNKNOWN 不重建缺失 record，不重新捕获新出生身份，不发布 activation/writer authority。九项真实 ZK 验证与 marker guard RED 已覆盖；这仍是步骤13冷路径前置，post-membership authority/历史目标留存、实际 replacement INSTALL/ACTIVATE 与原 SDK gated resend 尚未闭合。步骤11/12/13和生产Gate保持PARTIAL/OPEN，回执见实现记录。

2026-10-06 [replacement 授权与目标留存](rfcs/unified-wal/implementation/profile-control/README.md#replacement-授权与目标留存)增量：现有 lifecycle head增加experimental REPLACEMENT kind4，最多32个immutable predecessor-linked pages；post-membership权限与本次目标引用由同一次head CAS发布，并在同一ZK multi校验标准metadata birth marker/version。DELETE先赢不授权；授权先赢后的DELETE保留initial与全部replacement目标，历史结果不等于当前可写。最新页及fresh matching metadata可投影原POST_MEMBERSHIP_REPLACEMENT冷读；actual reserved file driver现在支持typed ACTIVATE_REPLACEMENT。独立page向量、真实ZK竞争/保留/上限及typed endpoint+真实文件force/apply已验证。新目标的完整inactive INSTALL准备、replacement wire/TLS client、原SDK whole-pending gate与resend、fenced-close/recovery/delete/GC/legacy ACL仍待闭合，步骤11/12/13及production Gate仍PARTIAL/OPEN；回执范围见实现记录。

2026-10-06 [new-peer inactive INSTALL与replacement wire](rfcs/unified-wal/implementation/profile-control/README.md#new-peer-inactive-install-与-replacement-wire)增量：same sidecar持久化bounded next-slot准备页，绑定frozen metadata physical birth/version、旧/新fragment digest、完整INSTALL scope；原metadata birth guard用于准备事务，DELETE/freeze竞争不扩大授权。verified初始或匹配prepare的receipt才能发布replacement。UIC1/schema1原bytes保留，ACTIVATE_REPLACEMENT新增独立experimental UIC2/schema2协商，复用CONTROL连接/TLS/HELLO/correlation/实际write与wire终结。四节点真实ZK、mTLS、文件driver已完成new peer inactive INSTALL→membership CAS→目标留存/授权→replacement activation→durable DATA；lost ACTV response用显式新client解析同operation。普通Add没有逐entrymetadata。SDK自动replacement、whole-pending gate/resend、recovery/fenced-close/delete/GC/legacy ACL与生产Gate仍OPEN，步骤13仍PARTIAL，实际回执见实现记录。

2026-10-07 [原 SDK 写期换组与整体回调门](rfcs/unified-wal/implementation/profile-wire/README.md#原-sdk-写期换组与整体回调门)接通预连接 bounded pool 的真实 inactive INSTALL、membership CAS、retained authority、全新目标 activation 和原 PendingAddOp 多 slot ACK 撤销/必要重发。五节点、两个 pending entry、响应丢失及停止窗口已有真实定向证据；跳过 activation 不能报告 AVAILABLE，UNKNOWN 只能解析原操作，不复活失败 writer。同 ID/incarnation rejoin、registration discovery、controller restart、恢复/删除/GC、生产和完整验收仍待完成，步骤13保持 PARTIAL；本段为当前增量，前述日期回执保留当时范围。

2026-10-07 [3/3/3及同起点连续换组](rfcs/unified-wal/implementation/profile-wire/README.md#333-与同起点连续换组验证)新增实际SDK证据：普通写必须等第三个durable副本，两个pending entry在起点0经历socket1→socket3→socket4两次实际失败/换组后连续回调；未变副本ACK保留、不重发，两个replacement页及全部历史目标留存。仍是隔离有限范围，更多故障交错、同ID incarnation/rejoin、恢复/删除和生产验收保持OPEN。

2026-10-07 [READ_ENTRY接线与验收范围](rfcs/unified-wal/implementation/profile-wire/README.md#read_entry-接线与验收范围)已接显式读scope权限、URD1/schema1、实际Get/read/pin/loan/response ownership、fenced完整覆盖的MISSING和原SDK flags0普通读。77项相关Java和新增有限读取分发模型通过；没有normal activation或逐entry控制持久化增量。READ_LAC/fencing read/独立readonly open、完整恢复/CLOSED/AutoRecovery/删除及生产验收仍OPEN，步骤11/14保持PARTIAL，历史回执不重绑。

2026-10-07 [CONTROL客户端FENCE前置](rfcs/unified-wal/implementation/profile-wire/README.md#真实control客户端fence前置)复用冻结100字节body/8字节status、verified peer与原quota/write/wire custody，实际force/同ID ALREADY/credential拒绝及UNKNOWN后同ID重连解析通过。相关59项Java和既有296配置模型矩阵通过；LAC/source/length、fencing read及matching CLOSED仍未接通，步骤14保持PARTIAL/OPEN。

2026-10-07 [READ_LAC源取证](rfcs/unified-wal/implementation/profile-wire/README.md#read_lac-源取证)已接独立URL1/schema1、完整source entryId/piggyback LAC/sourceLength与真实Get/read/loan路径；EMPTY要求整ledger完整稳定fenced proof且无explicit LAC。源length不授权CLOSED前缀；原SDK LAST_ADD_CONFIRMED/quorum、explicit LAC、length锚点、fencing read与matching CLOSED仍OPEN，步骤14保持PARTIAL。

2026-10-07 [原SDK的LAC覆盖与length锚定](rfcs/unified-wal/implementation/profile-wire/README.md#原sdk的lac覆盖与length锚定)新增显式readLacEvidence：原coverage/write-set与digest校验收集source样本，再取得同一handle metadata快照下的prefix边界length；源length不冒充prefix length，缺锚点或矛盾返回non-OK，不改writer状态。它不是fresh远端RecoveryContext、fencing coverage或CLOSED事实；完整恢复/AutoRecovery及步骤14保持OPEN。

2026-10-07 [恢复上下文与fencing前置](rfcs/unified-wal/implementation/profile-wire/README.md#恢复上下文与fencing前置)已实现真实ZK同记录OPEN→IN_RECOVERY、固定UWRC184恢复token、metadata/lifecycle物理birth guard和同操作重试/重开；显式coordinator复用已验证CONTROL peers及原coverage，只计durable FENCE并重新核对同一上下文。FENCED_CONTEXT不是RECOVERED_AND_CLOSED，完整frontier、恢复Add/close及步骤14仍OPEN。

2026-10-07 [fresh-context只读取证](rfcs/unified-wal/implementation/profile-wire/README.md#fresh-context只读取证)将同一IN_RECOVERY token下的fence/LAC/length与原SDK recovery点读串联；每个missing来源先完成本instance的durable fence，预算终止不等于tail。readonly session与coordinator不写Add/CLOSED，observed boundary不替代完整required authority或恢复完成；步骤14仍OPEN。

2026-10-07 [RepairIntent admission与实际恢复写回](rfcs/unified-wal/implementation/profile-wire/README.md#repairintent-admission与实际恢复写回)新增固定32槽的immutable inert计划、actual inactive INSTALL后的同birth/context admission、UIC3 recovery grant及原DATA client单entry恢复Add；admitted目标在DELETE后仍留存。真实ZK/mTLS/files、9类151项Java、Spotless/Checkstyle及有限341配置（67正向PASS/274指定反例）通过；恢复权限不激活normal writer。grant close、membership/replica-readable publication、required frontier与匹配CLOSED、完整AutoRecovery及步骤14/生产Gate仍OPEN。

2026-10-08 [UIC4 recovery grant close接线](rfcs/unified-wal/implementation/profile-wire/README.md#uic4-recovery-grant-close接线)已将独立exact close authority接到实际CONTROL write/force/owner apply，pre-cut DATA未终结不发布readable；timeout保留UNKNOWN并可显式重连解析同操作，重复close无额外force，不激活normal writer。现有ZK repair reader仍拒绝仅凭admission/grant授权close；membership/copy结果与真实close-authority发布器、matching CLOSED和完整AutoRecovery仍OPEN。

2026-10-08 [历史fragment repair membership与close授权](rfcs/unified-wal/implementation/profile-wire/README.md#历史fragment-repair-membership与close授权)已用同一ZK事务发布标准membership和immutable repair结果，实际close authority随后接入UIC4；bounded head/digest/inverse校验继续采用同一恢复token，physical admission guard防止同UUID/version0重建通过旧cut。真实copy/socket/files与ZK连续commit/reply-loss/delete竞争已有定向验证。最后开放fragment仍需terminal/frontier proof；完整copy coordinator、AutoRecovery和matching CLOSED保持OPEN，不授予normal或strong loss reset。

2026-10-08 [历史fragment完整copy协调与同操作恢复](rfcs/unified-wal/implementation/profile-wire/README.md#历史fragment完整copy协调与同操作恢复)已将原SDK recovery read/digest、单entry actual recovery Add、ZK publication及UIC4 close串成bounded协调入口；取消保留在途permit，copy失败不发布membership，published intent按原close ID解析且不recopy。换组fencing来源补齐未变成员，已验证repair-head metadata版本可继续只读。underreplication worker/scheduling、最后开放fragment/frontier和matching CLOSED仍OPEN。


2026-10-08 [URL2 explicit LAC 双证据](rfcs/unified-wal/implementation/profile-wire/README.md#url2-explicit-lac-双证据与前缀锚点)已在 opt-in schema2 中分别返回原 DATA source 与 durable explicit LAC digest，SDK 校验两份证据后沿用原 coverage，并重新锚定 exact P 的应用 length；损坏 endpoint 排除，timeout/unknown不计 absence。真实 mTLS/ZK/files/SDK、9类176项Java、Spotless/Checkstyle及相关34配置有限模型通过；URL1字节与历史回执保持原绑定。网络 WRITE_LAC、完整 required-frontier/normal-tail oracle、最后开放fragment恢复/CLOSED与AutoRecovery仍OPEN，步骤14及生产Gate不提升。

2026-10-08 [required prefix 与 normal-tail 只读取证](rfcs/unified-wal/implementation/profile-wire/README.md#required-prefix-与-normal-tail-只读取证)已复用原SDK write-set/AckSet/fencing transport，对 required 坐标收齐来源、检查连续性/有效冲突并区分有限exhaustion与unknown；empty、未提交suffix、count/deadline/cancel与context变化已有真实验证。历史copy同步消费完整来源且保留rich失败分类。广度180项与最终4方法、style及相关57配置有限模型通过；root RAT仍有443个既有问题。VERIFIED_NORMAL_TAIL尚不授予CLOSED，最后开放fragment write-back/visibility/匹配关闭、AutoRecovery与生产Gate保持OPEN。

2026-10-08 [固定prefix与真实last-fragment写回](rfcs/unified-wal/implementation/profile-wire/README.md#固定最后-prefix-与真实-last-fragment-写回)已将原读predicate固定为单份UWRF节点及标准metadata的出生绑定UWRS引用；原RepairIntent/UIC3/UIC4支持selected last range的新target复制和current成员SELF recovery Add/close，history穿插/重建/回复丢失/取消已有真实验证。189项广度与最终10项、style及相关79模型配置通过，旧run不重绑；root RAT仍443个既有问题。整体quorum、正常DATA终态visibility、标准matching CLOSED与AutoRecovery仍OPEN，步骤14及生产Gate不提升。
2026-10-08 [非空恢复terminal cut与matching CLOSED](rfcs/unified-wal/implementation/profile-wire/README.md#非空恢复-terminal-cut-与-matching-closed)已接原UIC4语义能力UIC5、接收端native type11持久P上界、全部pre-cut normal/recovery I/O排空、distinct原quorum ACK及标准CLOSED(P,length)同出生/版本/lifecycle/selection CAS。旧type9同操作升级在checkpoint/rotation后仍保留cut；真实回复丢失/new coordinator/restart、3/3/2和3/3/3已有验证。当前完成状态只代表matching CLOSED元数据事实与已接收节点的本地cut；empty-last close、独立readonly-open、离线追赶/rejoin、AutoRecovery和完整步骤14/生产Gate仍OPEN，历史回执不重绑。

2026-10-08 [空最后fragment的独立终态关闭](rfcs/unified-wal/implementation/profile-wire/README.md#空最后-fragment-的独立终态关闭)新增同一实验CONTROL内的UIC6/schema5/op0x0108、P/L/selection digest绑定与Bookie type12；仅既有activated当前成员、P=lastStart-1，复用cold reader/原close driver/distinct ACK及matching CLOSED CAS，不制造grant/copy。A2/A3的(-1,0)和历史CLOSED(2,48)、回复丢失/cancel/restart已有真实验证；非空路径保持UIC4/type11。readonly-open、AutoRecovery/离线追赶等仍OPEN，步骤14与生产Gate不提升。

2026-10-09 [独立CLOSED readonly-open](rfcs/unified-wal/implementation/profile-wire/README.md#独立-closed-readonly-open-与原-sdk-读取)已从实际matching recovery-CLOSED独立核验context/selection/birth/历史与live delete gate，复用原ReadOnlyLedgerHandle/digest/point read。构造后最后cold复核、固定P/L、confirmed/unconfirmed上界、bounded read quota与cancel custody、本地close无metadata/control写已有真实SDK/TLS/ZK/files验证；默认BookKeeper.open routing、普通writer closed adoption、AutoRecovery/离线追赶等仍OPEN，步骤14及生产Gate不提升。

2026-10-09 [整体fenced point recovery](rfcs/unified-wal/implementation/profile-wire/README.md#整体-fenced-point-recovery-入口与持久身份续接)已把现有metadata/fence/SDK point oracle/selection/SELF write-back/terminal quorum/matching CLOSED串成单attempt入口，采用已发布context/selection/intent/close ID续接，cancel/deadline保留actual gate；A2/A3空/非空及历史末段、A3不足quorum后resume、required hole/limit已有真实验证。RECOVERED_AND_CLOSED只消费matching close，不清marker或做strong reset；AutoRecovery、普通writer close adoption、离线catch-up等仍OPEN，步骤14及生产Gate不提升。

2026-10-09 [CLOSED fragment repair](rfcs/unified-wal/implementation/profile-wire/README.md#closed-fragment-repair-与固定-close-history-cut)新增同一次standard CLOSED CAS的UWCF80 close-history reference，关闭后copy/membership publication沿用原context/P/L及birth/inverse验证，目标inactive/grant/Add/close复用。真实A2/A3连续新目标换组、回复丢失/fresh snapshot零recopy及target restart通过；原readRecovery拒绝CLOSED、不清marker，ReplicationWorker route/最终健康清理及生产Gate仍OPEN，步骤14保持PARTIAL。

2026-10-09 [CLOSED 全副本健康复核](rfcs/unified-wal/implementation/profile-wire/README.md#closed-全副本健康复核)新增独立 matching CLOSED 全副本复核：逐坐标要求所有 exact write-set DATA digest/payload/累计 length 合法，再重验完整 metadata birth/version/context/head/sources 与 DELETE。空 ledger/末段要求所有当前目标匹配 incarnation 和 durable FENCE；closed fence 已包含已发布 inactive repair 目标。真实 historical hole、CLOSED repair 后完整、取消/deadline实际 gate 和 DELETE/recreation 验证见该记录。私有 proof 只绑定该 snapshot，未消费 worker lock/marker 同事务清理条件，ReplicationWorker 接线与 marker 清理、步骤14及生产 Gate仍PARTIAL/OPEN。

2026-10-09 [Profile marker 条件化清理](rfcs/unified-wal/implementation/profile-wire/README.md#profile-marker-条件化清理)新增显式 completeReplication：领取在health read前绑定原task的coordinator/deadline/单次健康proof，再fresh读取CLOSED/DELETE，以同一个ZK multi消费metadata/lifecycle和原worker session witness、marker physical birth/version，并删除原task/lock。默认OFF，UNKNOWN保留义务，受控release/session终止不删除replacement lock；强管理reset仍disabled。原ReplicationWorker自动路由、候选/intent续接与hierarchy/page GC尚未接通，步骤14及生产Gate仍PARTIAL/OPEN。

2026-10-09 [原ReplicationWorker的Profile自动循环](rfcs/unified-wal/implementation/profile-wire/README.md#原-replicationworker-的-profile-自动循环)新增显式默认OFF cohort constructor，复用原thread/factory/backoff/stats与恢复、真实COPY、完整W健康proof及原task guarded removal；A3在fixed selection下先修last再close，published close丢回复用原ID零recopy，stop/shutdown等actual child排空才release。实际A2/A3 OPEN/CLOSED/空健康与历史fragment、重启/后续损失及Classic两项回归见该记录。空缺副本grantless repair、service bootstrap/动态pool/rejoin/delete/GC及综合Gate仍OPEN，步骤14保持PARTIAL。

2026-10-09 [空副本grantless membership repair](rfcs/unified-wal/implementation/profile-wire/README.md#空副本的-grantless-membership-repair)复用原固定32槽/INSTALL/admission及UIC6，实际terminal force/apply后同事务发布membership与hash绑定UWEB124 receipt；空UWRP/UWRM显式schema2/kind1，DATA schema1向量不变。A2/A3空ledger与历史空末段、目标文件restart、reply丢失同ID续接、stop/DELETE及force失败/receipt损坏均有真实验证；零DATA/grant/normal，后续完整W健康与原task guarded removal沿用。步骤14仍PARTIAL，service bootstrap/动态pool/rejoin/delete/GC及综合生产Gate仍OPEN。

2026-10-09 [registration消费者发现与再次复核](rfcs/unified-wal/implementation/startup-readiness/README.md#消费者发现与再次复核)新增既有ZK store的独立reader读取，核验持久READY/birth、两份ephemeral projection、实际publisher owner/nonce marker和末次物理身份；同字节重建/更新/expiry等真实ZK验证见该记录。只读观察不授予transport或ledger authority，后续仍须mTLS/HELLO及再次冷复核。service bootstrap/动态pool尚未接通，步骤12/14及生产Gate仍PARTIAL/OPEN。

2026-10-11 [注册观察驱动的连接与实际本地排空](rfcs/unified-wal/implementation/profile-wire/README.md#注册观察驱动的连接与实际本地排空)接既有consumer观察、真实mTLS/HELLO和最后cold复核；CONTROL/DATA client保持原RPC超时，closeAsync等待cold/channel/HELLO输出及实际RPC借用与promise terminal。发现/关闭不授予ledger authority或remote abort/NONE/FREE；动态pool、service bootstrap和总预算尚未接线，步骤12/14及生产Gate仍PARTIAL/OPEN。

2026-10-11 [worker/service关闭边界](rfcs/unified-wal/implementation/profile-wire/README.md#worker-与服务关闭的中断及并发边界)修复join中断和重复shutdown提前关manager/SDK、self-join及启动前关闭；caller signal在procedure结束后恢复，AutoRecoveryMain不把继承signal送进SDK等待。真实READ/task与真实metadata client RED/GREEN见记录；动态pool/Profile bootstrap尚未接通，步骤14与生产Gate保持PARTIAL/OPEN。

2026-10-11 [registered cohort pool与worker lease](rfcs/unified-wal/implementation/profile-wire/README.md#注册-cohort-peer-pool-与实际-worker-lease)消费真实READY/mTLS/HELLO创建固定snapshot，建立/借用/排空共同保守计费connection和RPC-slot预算；原worker在实际lease drain后才清原task/marker。实际缺副本修复、held promise/cold及episode/incarnation验证见记录；service bootstrap、全registry/字节预算与生产Gate仍OPEN，步骤12/14保持PARTIAL。

2026-10-11 [显式AutoRecovery service bootstrap](rfcs/unified-wal/implementation/profile-wire/README.md#显式-autorecovery-service-bootstrap-与-owned-资源)由服务自身SDK/ZK创建原coordinator/pool并消费真实任务；owned manager/SDK/cold/group在实际worker/lease排空后关闭，失败也收尾，默认入口OFF。Classic periodic payload/replica/placement audit非0明确拒绝；完整Profile availability/audit、字节预算与综合生产Gate仍OPEN，步骤12/14保持PARTIAL。

2026-10-11 [SDK关闭期间的新中断](rfcs/unified-wal/implementation/profile-wire/README.md#sdk-关闭过程中的新中断)已真实RED/GREEN验证并修复：原Main使用独立一次性SDK closer，caller中断只记账并等实际procedure结束，随后回收cold/group并恢复signal。48项Java与相关54模型配置得到预期结果；Profile availability/audit、全字节预算及步骤12/14/生产Gate仍PARTIAL/OPEN。

2026-10-11 [Auditor实际排空与中断](rfcs/unified-wal/implementation/profile-wire/README.md#auditor-排空与-owned-sdk-关闭)已真实RED/GREEN验证原audit executor等待中的interrupt不再提前进入SDK关闭；Auditor/elector ownBkc复用one-shot closer。51项Java（含Classic auditor关闭/连续换选举）与相关55模型配置得到预期结果，runner绑定实际owner源码；Profile availability/audit及步骤12/14/生产Gate仍PARTIAL/OPEN。

2026-10-11 [Profile成员故障发现](rfcs/unified-wal/implementation/profile-wire/README.md#profile-成员故障发现与原-auditor-接线)已接原auditor启动/cohort watch/periodic扫描与原任务生成，按descriptor/instance/retained incarnation和fresh cut核验；Classic同名registry不掩盖Profile缺失，UNKNOWN/CORRUPT不计缺失。真实自动任务→COPY/成员替换→全W复核/排空/marker清理通过；注册观察不授予repair/normal权限。周期DATA审计、总预算、rejoin/delete/综合Gate仍OPEN，步骤12/14保持PARTIAL。

2026-10-11 [共享索引与legacy退役拒绝](rfcs/unified-wal/implementation/profile-wire/README.md#共享引用索引与-legacy-退役拒绝)修复Profile过滤影响共享BookKeeperAdmin的问题：公开索引恢复全引用，仅Classic auditor过滤Profile；legacy退役对已知ensemble/marker中的Profile引用明确拒绝，避免无marker时提前成功。真实退役RED/GREEN与公开index/Classic实际任务范围验证通过；完整Profile退役/retained-history清理仍未实现，生产Gate不提升。

2026-10-11 [CLOSED周期DATA审计](rfcs/unified-wal/implementation/profile-wire/README.md#profile-closed-data-周期审计)已复用原replica-check CLOSED范围、streamed metadata与真实全W验证，共用单peer pool/coordinator预算，actual health/lease drain与最后fresh cut后只生成检查任务。真实历史DATA hole→原worker修复/全W/marker闭环、健康/权限拒绝不clear、deadline/关闭保留actual资源通过。OPEN/正常writer close、payload/placement、总字节预算、离线/rejoin/delete与生产Gate仍OPEN，步骤12/14保持PARTIAL。


2026-10-11 [SDK writer stop 与实际 DATA 排空](rfcs/unified-wal/implementation/profile-wire/README.md#sdk-writer-stop-与实际-data-排空)已真实 RED/GREEN 修复逻辑 delivery 完成早于排队源/output promise 终结的边界；stop 等自身全部普通 DATA attempts 的实际终态，caller cancellation 不污染共享 completion，借用 socket 保留，UNKNOWN/OPEN 不变。35 个相关 SDK/socket/recovery/worker 用例零失败/skip，17 个有限模型配置得到预期结果，style 通过；RAT 保留 443 父 HEAD 历史问题、无新增。此为普通 close 的必要排空依赖；冻结 P/length 与 durable terminal/metadata close、adoption 尚未接通，步骤 12/14/15 和生产 Gate 仍 PARTIAL/OPEN。


2026-10-11 [普通 writer close 与固定 cut 续接](rfcs/unified-wal/implementation/profile-wire/README.md#普通-writer-close-与固定-cut-续接)在显式 cold 依赖下已接原 SDK close：冻结确认P/L、actual DATA排空、同CAS writer cut/UWRC schema2、固定prefix验证与既有SELF/terminal/标准CLOSED。A2/A3空/非空物理tail、健康文件restart、caller cancel/output、enter回复窗口新session续接、cut损坏拒绝、历史replacement与丢CLOSED回复均有真实证据；旧schema1向量不变，旧严格reader拒schema2为源码检查，不计二进制兼容PASS。阶段广度102/73/74与最终8项、27模型配置各按原source绑定保留，style通过，RAT443历史问题无新增。该有界close/adoption切片不闭合默认SDK路由、其他required authority、OPEN/payload/placement、full-bytes、删除/离线/rejoin/GC与综合Gate，步骤12/14/15仍PARTIAL/OPEN。


2026-10-11 [Fenced-CLOSED 集群逻辑删除与目标义务](rfcs/unified-wal/implementation/profile-wire/README.md#fenced-closed-集群逻辑删除与目标义务)已显式接matching CLOSED前置、原root delete cut、标准metadata不可逆seal、完整initial/replacement/admitted-repair targets及出生绑定cluster tombstone。同起点覆盖目标/离线incarnation保留，三段回复丢失续接，cancel等actual cold completion，manifest缺失/重建拒绝；新open拒绝、旧reader未本地tombstone前可读，未冒充physical/access barrier。76与14项Java、27有限模型配置各按当前source绑定通过，style通过，RAT443历史问题无新增。实际delivery/wire/delete-applied/cursor/catch-up/rejoin/回收与GC/默认SDK routing及综合Gate仍未闭合，步骤12/14/15保持PARTIAL/OPEN。


2026-10-12 [初始共享 delete delivery journal](rfcs/unified-wal/implementation/profile-wire/README.md#初始共享-delete-delivery-journal)已复用logical tombstone/完整target，实际ZK同事务提交event/index/连续head；一个Bookie/incarnation跨两真实ledger共用initial stream，旧绑定不可盲建新stream，丢回复/并发seq赢家/缺失与同字节重建拒绝已有证据。阶段79与最终7项Java、15配置有限模型各保留原source，style通过，RAT443历史问题无新增。4096-event有限prototype无snapshot/GC扩容，不授予本地cursor/registration authority；实际TOMBSTONE/apply、catch-up/rejoin/physical与综合Gate仍OPEN，步骤12/14/15保持PARTIAL。


2026-10-12 [UIC7 本地 delete-applied 与连续 cursor](rfcs/unified-wal/implementation/profile-wire/README.md#uic7-本地-delete-applied-与连续-cursor)已将committed event/实际cold authority接到mTLS CONTROL与原Bookie type13 write/force/owner apply，TOMBSTONE和连续cursor同record；gap写盘前可重试，旧seq不回退，UNKNOWN不发布cursor，absent ledger仍有durable effect坐标。实际非空/空两ledger、旧读取/目标拒绝、held pin、丢目标回复/fresh重试、force失败、checkpoint rotation/restart有定向证据。158项相关Java、23个有限模型配置（3正向/20指定反例）及style通过，RAT443历史问题无新增；原history receipt保留；registration required-through cut、startup/catch-up/rejoin、snapshot/handoff/physical/GC及综合Gate仍OPEN，步骤12/14/15保持PARTIAL。


2026-10-12 [初始 required-through cut 与真实 rejoin 追赶](rfcs/unified-wal/implementation/profile-wire/README.md#初始-required-through-cut-与真实-rejoin-追赶)已绑定原恢复control cursor与实际shared assignment/head，把head版本/birth/assignment检查加入原writable multi；delete先赢需继续追赶，普通head增长不demote既有registration。原graph实际restart、bounded LIMIT/UNKNOWN/独立completion、真实seq2注册竞争、DLR force、held pin和丢注册回复同session解析已有证据。101项相关Java、20个有限模型配置（3正向/17指定反例）及style通过，RAT443历史问题无新增。仅explicit initial gen1；在线delivery/default bootstrap、其他required facts、snapshot/handoff/GC/decommission与综合Gate未闭合，步骤12/14/15保持PARTIAL/OPEN。


2026-10-12 [全目标 publication 与在线节点消费](rfcs/unified-wal/implementation/profile-wire/README.md#全目标-publication-与在线节点消费)已把每target event/index/head与publication前缀同事务记录，全部target就绪才发布logical tombstone，消除logical success后journal尚未可见的窗口；PREPARED不授予local delete。schema2 reference绑定prefix birth，原schema1 bytes保留；原注册graph借用bounded cold/scheduler持续消费，UNKNOWN/stop等actual local completion，close未排空保留锁。真实自动投递/注册race/cancel续接/required prefix loss/held force已有证据；阶段176与当前10项Java、31个有限模型配置分别保留原source，style通过，RAT443历史问题无新增。default bootstrap、snapshot/handoff/GC/退役、其余facts/full-bytes/综合Gate仍OPEN，步骤12/14/15保持PARTIAL。


2026-10-12 [实际恢复 graph 与原 startup 发布桥接](rfcs/unified-wal/implementation/startup-readiness/README.md#实际恢复-graph-与原-startup-发布桥接)已将actual device/allocator/route/delete facts接回原Harness、DLR/ZK发布/session admission及既有consumer，manifest/client不匹配、consumer scheduler拒绝会拒绝或退休/demote。广度77项与最终1项真实组合、20配置有限模型按阶段source保留，style通过，RAT443父HEAD历史问题无新增。兼容性fence与credential/new-scope真实issuer、默认bootstrap/SDK routing、snapshot/handoff/GC、full-bytes及综合验收仍OPEN；步骤12/14/15保持PARTIAL，生产Gate不提升。


2026-10-12 [持久startup requirements与冻结发布条件](rfcs/unified-wal/implementation/startup-readiness/README.md#持久-startup-requirements-与冻结发布条件)已接独立immutable ZK兼容格式/单Arena expected set、显式构造前检查及writable同事务frozen source guard；caller自报未来runtime不能改变本实现version1。阶段85/最终26项Java、相关18/最终4配置模型分别保留source，style通过、RAT443历史问题无新增。required声明仍不能证明stock fence或物理credential隔离，default bootstrap/SDK routing、snapshot/handoff/GC/full-bytes/综合验收未闭合，步骤12/14/15与生产Gate保持PARTIAL/OPEN。


2026-10-12 [实际Unix credential new-scope快照](rfcs/unified-wal/implementation/startup-readiness/README.md#实际-unix-credential-的-new-scope-access-快照)已验证native双UID访问完整bounded现有文件及可读marker，真实0711/0644反例修复，access/canonical roots导入原actual restart/source/readiness组合。最终31项及style通过，RAT443历史问题无新增；这只是ACCESS_SNAPSHOT_ONLY，old-service disposition/privilege/stock binary/default bootstrap与完整fallback和综合验收仍OPEN，步骤12/14/15及生产Gate保持PARTIAL/OPEN。


2026-10-12 [实际old Classic episode与new-scope replay组合](rfcs/unified-wal/implementation/startup-readiness/README.md#实际旧-classic-episode-的有序关闭与-new-scope-组合)已真实SDK写读/原stack与driver关闭/actual JVM退出/原session registration终态，owned observation接入actual Unix access与原startup组合；closed-live和abrupt-expired absence不等于DRAINED。最终34项/style通过，RAT443历史问题无新增。当前构建与受控episode证明不覆盖外部service authority、released stock矩阵、永久退役或default hook，步骤12/14/15/生产Gate与综合验收仍PARTIAL/OPEN。


2026-10-12 [显式Profile listener与actual网络credit drain](rfcs/unified-wal/implementation/profile-wire/README.md#显式-profile-listener-与实际网络-credit-排空)已管理native parent/child、worker交接及receive ledger；retire新连接不提前归还frame/cold/backing，caller copy completion不污染actual终态，borrowed graph/loops/cold保持原owner。46项相关Java/13配置模型得到预期结果，style通过、RAT443历史问题无新增。完整startup owner/default hook/SDK routing、full-bytes、stock binary及综合验收仍OPEN，步骤11/12/14/15和生产Gate保持PARTIAL/OPEN。

2026-10-12 [显式 Profile 服务 owner](rfcs/unified-wal/implementation/startup-readiness/README.md#显式-profile-服务启动与-owner-排空)已连接 requirements/pre-open Arena/Index 根绑定、实际 native bind endpoint、原恢复/Harness/registration/consumer、注册发现 SDK 和 listener→storage→DLR 实际排空。held DATA force 与 publication stop 保留 owner/locks，actual old Classic drain/Unix access 接入 existing restart；最终58项 Java、style通过，RAT443父 HEAD 路径无新增。default EmbeddedServer/完整外部 authority、stock矩阵、snapshot/handoff/GC/decommission/full-bytes/性能/运维和综合验收继续 OPEN，步骤11/12/14/15及生产 Gate保持 PARTIAL/OPEN。

2026-10-12 [EmbeddedServer 显式 Profile hook](rfcs/unified-wal/implementation/startup-readiness/README.md#embeddedserver-显式-profile-分支与-lifecycle)已在 Classic 构造前选择隔离 owner 分支，原 stack 驱动实际 startup/SDK/restart；关闭失败保留非终态并阻止 stack 释放更早资源，handler 返回不伪造 STARTED。最终28项/1100源码输入及 style通过，独立RAT443父 HEAD 路径无新增；Classic 默认保留。配置/bootstrap factory、完整外部 authority、stock矩阵及其余综合验收仍 OPEN，步骤11/12/14/15和生产 Gate保持 PARTIAL/OPEN。

2026-10-12 [配置转换与显式 bootstrap](rfcs/unified-wal/implementation/startup-readiness/README.md#配置转换与显式-bootstrap)已冻结原有 bounded layout/execution/batch 参数并公开原 typed 环境装配；actual properties load 后复用原 EmbeddedServer/SDK/restart。unknown/multi-value/non-integer/越界及 ISOLATED 无 owner 在 Classic/storage open 前拒绝，真实 prefix enumeration red/green保留；最终30项/1100源码输入与style通过，RAT443父 HEAD 问题无新增。Main/external authority/owned environment 和其余16步验收仍 OPEN，生产 Gate 不提升。

2026-10-12 [同一 episode owned资源](rfcs/unified-wal/implementation/startup-readiness/README.md#同一服务-episode-的-owned-资源初始化与终止)已接 deferred native ZK/原metadata driver-LM、bounded cold/scheduler/loops，原服务实际drain后才确认各终态；metadata scheduler held任务、初始化失败部分资源、三节点SDK/restart得到最终33项/1103输入与style证据，RAT443父HEAD无新增。TLS/physical issuer仍借用，完整Main/外部authority/stock矩阵/full-bytes及其他综合验收继续OPEN，生产Gate不提升。

2026-10-12 [OPEN 固定 LAC 前缀审计](rfcs/unified-wal/implementation/profile-wire/README.md#open-固定-lac-前缀审计)已接原周期任务/SDK/coordinator/共享池，固定下界P/L逐坐标全W观察且不fence writer；健康结果无CLOSED proof、不清marker，未fenced缺失仍UNKNOWN，actual health/lease排空后才复核并标记实际损坏/稳定缺失。最终28项Java与20有限模型配置（2完整正向/18指定反例）、style通过，RAT443父HEAD路径无新增；完整OPEN审计、其他required authority/总预算/Main/外部authority/stock矩阵/GC/退役及综合验收仍OPEN，生产Gate不提升。

2026-10-12 [网络 WRITE_LAC 与确认前缀生产](rfcs/unified-wal/implementation/profile-wire/README.md#网络-write_lac-与确认前缀生产)已接原网络WRITE_LAC、durable control force/apply与SDK确认prefix的distinct AQ，fence后只解析同值，UNKNOWN/cancel不提前归还actual output/wire custody；既有URL2/required-prefix实际消费新producer。7类337项Java和29有限模型配置（5完整正向/24指定反例）通过；其他required authority/自动策略/默认routing/总预算/Main/stock/GC/退役/性能/运维及全16步综合验收仍OPEN，生产Gate不提升。

2026-10-12 [本地删除空间终结](rfcs/unified-wal/implementation/profile-wire/README.md#本地删除空间终结)已将实际terminal TOMBSTONE接到原native index/coordinate/coverage及COPY/MOVE/FREE，按实际allocated DATA/旧COPY复核本地REUSABLE；混合块可零净容量增益退休，普通维护仍要求正净增益。旧IO/owner credit与deadline/cancel独立排空，其他ledger保留、corrupt/limit不伪造成功。3类282项Java与20有限模型配置（4完整正向/16指定反例）通过；网络physical receipt/全目标/retention/GC/退役及其他16步验收仍OPEN，生产Gate不提升。


2026-10-12 [网络删除空间查询与全目标观察](rfcs/unified-wal/implementation/profile-wire/README.md#网络删除空间查询与全目标观察)已接独立UIC8/OPERATION_STATUS、同一持久UIC7事件和原mTLS/direct/exact权威检查，实际FREE后fresh查询才报告REUSABLE；完整历史manifest逐target/incarnation观察，缺peer保留PENDING，全部REUSABLE仍是ephemeral结果。durable terminal receipts/retention/GC/退役及其他16步验收继续OPEN；默认OFF/SYSTEM/PARK和disabled能力不变，生产Gate不提升。
本里程碑349项原阶段Java/70项最终源码复核与24有限模型配置通过，style通过；独立RAT443父HEAD无新增。证据与最终1874源码输入由上述profile-wire章节拥有，阶段结果不移绑。


2026-10-12 [删除物理终结记录与持久接续](rfcs/unified-wal/implementation/profile-wire/README.md#删除物理终结记录与持久接续)已把实际UIC8 REUSABLE绑定完整事件/request/target/incarnation，条件化存入原ZK不可变row与birth guard；新await入口按完整历史targets重读全部记录，支持coordinator重建及已有记录的实际离线目标。原观察入口保持只读，PENDING/UNKNOWN无记录，不删除历史义务或授予GC权限。最终6类105项Java/31有限模型配置（4正向/27指定反例）及style通过；retention、accepted decommission/unrecoverable、snapshot/handoff/history GC和其他16步验收仍OPEN，生产Gate不提升。
原105项阶段保留source绑定；最终还原同bytes/version/guards的metadata birth ABA补验2项通过，31模型与1884当前输入一致，style通过，RAT443父HEAD无新增。


2026-10-12 [删除流完整 snapshot 与真实启动追赶](rfcs/unified-wal/implementation/profile-wire/README.md#删除流完整-snapshot-与真实启动追赶)已将原generation1 shared stream的完整prefix保存为有界immutable pages/root＋conditional publication witness，绑定scope/原event birth并接入实际TOMBSTONE/cursor启动追赶。真实缺prefix＋suffix、65-ledger两page、missing/recreate、取消/回复丢失及并发有111项Java/23有限模型配置（3正向/20指定反例）证据；无旧journal删除、assignment handoff或GC权限。retention/terminal退役/history GC及其他16步验收继续OPEN，生产Gate不提升。


2026-10-12 [Assignment handoff 的 PREPARED 与投递 cut](rfcs/unified-wal/implementation/profile-wire/README.md#assignment-handoff-的-prepared-与投递-cut)已把完整native snapshot/COMMITTED effects接到原ZK admission/head同multi排序，两个实际delete publisher路径都不能跨冻结cut；旧G1 DATA与已记录effect仍有效，计划不生成local G2或EFFECTIVE事实。114项原阶段/最终6项Java和23有限模型配置（3正向/20指定反例）通过；local adoption/ACK/EFFECTIVE/route retirement、retention/GC及其他16步验收仍OPEN，生产Gate不提升。


2026-10-12 [Assignment handoff 的本地持久 adoption](rfcs/unified-wal/implementation/profile-wire/README.md#assignment-handoff-的本地持久-adoption)已将实际PREPARED Plan接入原Bookie type14 force/owner apply/checkpoint/replay；必须先完成原G1 cut，空cut不生成effect，重复相同Plan不再force，普通G1 DATA保持有效。等待超时不释放实际I/O，fresh native admission ABA拒绝重新确认。最终4类27项Java、20有限模型配置（3正向/17指定反例）及style通过；native catch-up ACK/EFFECTIVE CAS/old-route retirement、retention/GC及其他16步验收仍OPEN，生产Gate不提升。


2026-10-12 [Assignment handoff 的 native ACK](rfcs/unified-wal/implementation/profile-wire/README.md#assignment-handoff-的-native-ack)已将原storage graph实际force/apply及owner终态产生的不可公开构造凭据，接到原ZK conditional immutable ACK row/birth guard。完整Plan、local control sequence与native PREPARED/cut绑定，丢回复/取消后重建读取首条原记录，已关闭owner不能新确认；ACK保持G1 cursor/assignment与原义务。最终6类32项Java、26有限模型配置（4正向/22指定反例）、1919源码输入及style通过；conditional EFFECTIVE/G2 journal与registration/old-route retirement、retention/GC及其他16步验收仍OPEN，生产Gate不提升。


2026-10-12 [Assignment handoff 的 EFFECTIVE 与 G2/G3 接续](rfcs/unified-wal/implementation/profile-wire/README.md#assignment-handoff-的-effective-与-g2g3-接续)已把原graph gate退役、owned registration删除、NON_WRITABLE投影与native assignment/head更新放进原publisher同一ZK multi；原type15 force/apply/checkpoint/replay接续effective progress，原事件身份保留。三目标真实G2/G3 READY、SDK Add/close、新旧事件与混合snapshot、健康文件旋转重启、native丢回复/held callback与type15 UNKNOWN已有验证。79项原阶段Java（server 71/common codec 8）/最终21项补验及35有限模型配置通过，1929个最终源码输入核验一致，style通过、RAT443父HEAD无新增；完整Gate不提升。retention/offline/backup-restore、accepted terminal decommission/unrecoverable/history GC、其他required authority/default routing/full-bytes/兼容/正式性能/运维/全16步验收继续OPEN。


2026-10-12 [删除 retention policy 与原子 receipt](rfcs/unified-wal/implementation/profile-wire/README.md#删除-retention-policy-与原子-receipt)已将显式offline/rejoin、metadata propagation、backup/restore窗口绑定到原逻辑删除的native同事务metadata/immutable receipt，保留策略历史与全部target义务。策略竞争整体拒绝，旧窗口不随新策略缩短，无策略历史继续受保护。6类38项真实回归、29有限模型配置及1938冻结source通过，style exit0；RAT443父HEAD路径无新增。BOUND不授权GC，实际backup/restore/rejoin接受、历史GC与全16步其余验收继续OPEN；默认OFF/SYSTEM/PARK和生产Gate不提升。


2026-10-12 [删除 journal prefix 的实际 compaction](rfcs/unified-wal/implementation/profile-wire/README.md#删除-journal-prefix-的实际-compaction)已接完整snapshot与同incarnation EFFECTIVE cut后的bounded native duplicate-row删除，并接通删后physical receipt持久化。真实G2/G3、snapshot publication竞争、回复丢失/held cancel、rotation/restart有6类34项原阶段及最终1项补验、31有限模型配置和1946冻结source证据。全部effect/index/Manifest/tombstone/retention/physical及handoff义务保留；snapshot/page/lineage/target GC、backup restore接受和16步其余验收仍OPEN，默认OFF/SYSTEM/PARK及生产Gate不提升。


2026-10-13 [客户端 transport backing 与实际关闭](rfcs/unified-wal/implementation/profile-wire/README.md#客户端-transport-backing-与实际关闭)已把原SDK/cohort客户端TLS/帧及普通/恢复Add、WRITE_LAC私有snapshot接入现有backing ledger，实际close等待最后view归还；cohort原slot上限之外可观察其transport backing与固定quota。最终4类69项Java、24有限模型配置及1946当前source/style通过。仅transport ByteBuf计量，opaque TLS/codec/caller数组及存储/index/control等统一总账和正式性能仍OPEN；默认OFF/SYSTEM/PARK、disabled与完整生产Gate不提升。


2026-10-13 [客户端 codec workspace 与有界 reply 解析](rfcs/unified-wal/implementation/profile-wire/README.md#客户端-codec-workspace-与有界-reply-解析)已在原backing账本中预留临时codec byte数组工作集，并删除fallback full-frame多层副本；真实CONTROL发送前拒绝、已协商read reply拒绝与最后owner drain得到验证。8类93项Java（common21/server72）、24有限模型配置、1946当前source及style通过。只完成客户端暂存工作集，caller/长期对象、server/control/cold/storage/index及opaque TLS等总账和全16步Gate仍OPEN，默认与disabled能力不提升。

2026-10-13 [接收端 workspace 分类与响应 backing 转交](rfcs/unified-wal/implementation/profile-wire/README.md#接收端-workspace-分类与响应-backing-转交)已复用原connection/lane/Credit区分接收端解析、冷任务及响应workspace，按原quota在响应分配前原子转成backing；CONTROL转交失败不改账，caller/transport终结不提前结束actual cold/force/last view。4类47项原阶段Java及1项定向补验、7个原backing有限模型配置有分阶段输入证据。这里完成预算分类与owner转交，未补齐MetadataStore内部、长期对象、caller/opaque TLS/kernel或storage/index统一总账；默认OFF/SYSTEM/PARK、disabled和全16步生产Gate保持OPEN。

2026-10-13 [固定存储池与网络共享 DATA 预算](rfcs/unified-wal/implementation/startup-readiness/README.md#固定存储池与网络共享-data-预算)已在原ProfileReceiveBudget中于storage open前保留固定对齐池全部root容量，网络背压不能侵占这份完成工作集；listener只等待实际connection/cold/output排空，service在实际storage owner释放池后归还reservation。最终5类67项中62项实际通过、5项Linux条件跳过，17原有限模型配置结果符合预期；未闭合索引/控制/长期对象/opaque依赖/cohort总账、正式性能与16步综合Gate，默认OFF/SYSTEM/PARK及disabled不变。

2026-10-13 [派生 index block cache 与共享 DATA 预算](rfcs/unified-wal/implementation/local-storage/README.md#派生-index-block-cache-与共享-data-预算)已将原1 MiB index block cache启用strict capacity，并在ProfileService storage open前与固定池合并预留于原DATA额度；原缓存大小与协议/格式不变。旧service只预留池的真实quota红、当前拒绝绿及最终2类34项真实Java通过；memtable/WBM并非hard native上限，所有native/对象总账与正式性能仍OPEN。本轮不重跑不覆盖该native政策的TLC，旧模型保持原绑定，默认OFF/SYSTEM/PARK及16步生产Gate不提升。

2026-10-13 [全目标终结屏障与单调 retention 窗口](rfcs/unified-wal/implementation/profile-wire/README.md#全目标终结屏障与单调-retention-窗口)已在全部历史target的原native REUSABLE receipts后原子建立retention barrier，保留原policy/receipt出生关系；coordinator使用opaque本地单调观察，重启重新等待完整窗口，每次检查重验native事实。6类30项Java与9有限模型配置有分阶段证据；elapsed不授权GC，backup/restore接受、accepted decommission/unrecoverable、剩余history GC及全16步Gate继续OPEN，默认OFF/SYSTEM/PARK及disabled不变。

2026-10-13 [默认 SDK open 的 Profile 拒绝边界](rfcs/unified-wal/implementation/profile-wire/README.md#默认-sdk-open-的-profile-拒绝边界)已在原LedgerOpenOp密码/digest验证后、Classic handle/listener/recovery创建前识别mandatory Profile backlink，返回原IllegalOpException，避免当前SDK误用Classic路径。真实CLOSED旧行为红、新OPEN/CLOSED/valid-CRC future marker拒绝绿及三Bookie Classic兼容，最终2类17项通过；Main/外部issuer、默认Profile routing、released stock矩阵及全16步Gate仍OPEN，默认OFF/SYSTEM/PARK与disabled不变。

2026-10-13 [恢复执行的 released compatibility 矩阵](rfcs/unified-wal/implementation/profile-wire/README.md#恢复执行的-released-compatibility-矩阵)：父源码ea98f8b20bebcf639c3ddf07effdeae2aebfd7e3上四个released decoder和真实私有loopback stock Main各跑610向量，原probe Oracle均通过；Classic sentinel成功、stock文件/native namespace快照不变、原旧magic控制在四个decoder均预期FAIL。636个输入/实际JAR/JDK/class-load与312 raw文件归档核验，所有自有进程终结/端口释放。当前PASS_SCOPED，内部handle/master-key/allocation无直接观测使完整G1仍BLOCKED_UNVERIFIED；步骤11/12/14/15、生产Gate与16步验收仍OPEN，默认OFF/SYSTEM/PARK及disabled不变。继续其余兼容effect、Main/default routing/required authority、full-bytes、backup/restore/accepted退役/GC、正式性能与运维；不重绑历史回执。

2026-10-13 [released stock内部effect与corpus补齐](rfcs/unified-wal/implementation/profile-wire/README.md#released-stock-内部-effect-与-corpus-补齐)：父源码55388d6da0上新增外部JDI方法/channel观测，四版本最终611向量的Classic effect为零、611连接实际终结，前后Classic控制触发实际路径。当前生成器要求RECOVERY_TERMINAL_CLOSE而committed corpus缺失的三项RED已用现有生成器补资源，原610 raw bytes和14补充fixtures不变；当前Profile五类37项通过，observer定向Checkstyle通过。639 stock/corpus及1116 Java输入/三阶段raw/checker归档见profile-wire记录，旧610结果不移绑。PASS_SCOPED完成固定reference corpus的内部effect观察；完整current control-body/mixed-version/auth/semantic/startup与16步其余验收仍OPEN，G1 BLOCKED_UNVERIFIED、生产Gate OPEN，默认OFF/SYSTEM/PARK及disabled不变。继续其余兼容、Main/default routing/required authority、full-bytes、backup/restore/accepted退役/GC、正式性能与运维；目标active。

2026-10-13 [当前control请求与released resolver](rfcs/unified-wal/implementation/profile-wire/README.md#当前-control-请求与-released-resolver)：父365dcb802e上将actual client两个control codec的9完整请求及45 magic/truncation变体接入原corpus，665-vector原611 bytes不变；七类47项Profile测试PASS，四版decoder/真实stock各665连接与actual channel drain/内部zero effect/native快照均通过，old CE仍各expected FAIL。四个released resolver的四种endpoint选择/拒绝输入也通过pure logic，registration I/O/publication不因此获证。815 stock与1170 Java输入、独立resolver source、1185 unique SHA/raw/RED/GREEN/checker证据由profile-wire记录拥有；common及resolver style通过，独立RAT仍父443无新增。支持的实验control schema样本已覆盖，mixed rolling versions/auth/semantic/startup及general stable control-body、Main/default route/其他required authority/full-bytes/backup-restore/accepted退役/GC/性能/运维与16步综合验收继续OPEN，G1 BLOCKED_UNVERIFIED、生产Gate OPEN、目标active。默认OFF/SYSTEM/PARK及disabled不变，原610/611结果不移绑。

2026-10-13 [owned lifecycle实际self-drain拒绝](rfcs/unified-wal/implementation/startup-readiness/README.md#owned-lifecycle-的实际-self-drain-拒绝)：父54800228d8上将同一owned服务cold/delete/metadata/ZK event contexts及原Netty loops按实际Thread identity拦在退休/demotion/shutdown前；原metadata明确await API也先拒绝self，legacy非阻塞close及合法外部timeout/actual drain不变。实际旧cold自等待RED、四种managed contexts/直接metadata self-await及同名外部caller正常关闭GREEN，最终3类44 registered/42 PASS/2既有hierarchical条件skip，1170 inputs/style通过、独立RAT443父已有无新增。旧source/receipt保留原绑定，本次native caller identity未移绑TLC；没有新线程/队列/格式或authority issuer。继续Main/default SDK/其他required producer/full-bytes/backup-restore/accepted退役/GC/mixed兼容/性能/运维及16步验收；目标active，步骤11/12/14/15和生产Gate OPEN，默认OFF/SYSTEM/PARK及disabled不变。

2026-10-13 [CLOSED readonly open独立actual-drain](rfcs/unified-wal/implementation/profile-wire/README.md#closed-readonly-open-的独立-actual-drain-信号)：父9cbeb5e2e7上原coordinator新增beginOpenReadOnly/ReadOnlyOperation分开caller completion/cancel与实际cold/未交付handle终结；原openReadOnly兼容委托，peers继续borrowed，合法交接先claim再释放cold quota，callback可继续open。真实held最后metadata、cancel仍占quota、delete/birth拒绝、success callback/nested open及其他调用方最终8项通过，相关11有限模型1正向/10指定反例、730仅catalog；2005 unique source输入冻结、style通过、独立RAT父443无新增。默认SDK公开Profile入口选择仍待明确，本增量不等同已完成默认路由。继续完整Main/required producer/full-bytes/backup-restore/accepted退役/GC/mixed/性能/运维和16步验收；目标active、生产Gate OPEN、默认OFF/SYSTEM/PARK及disabled不变。


2026-10-13 [SDK 读响应的 backing 所有权交接](rfcs/unified-wal/implementation/profile-wire/README.md#sdk-读响应的-backing-所有权交接)：父 eda1d7d760 上把原 READ_ENTRY response backing 通过 AutoCloseable/retained slice 交给标准 SDK ReadOp，取消丢弃和 ordered executor 拒绝均释放未交付 custody，最后 caller view 归还前 closeAsync 继续等待。父 SDK adapter 的真实 retained-entry RED 与最终四类45项 GREEN、style通过；原 backing 模型7配置按自己的阶段源码保留，2005 unique输入分别核验。独立RAT父443无新增；full-bytes总预算、默认SDK公开入口及全16步验收仍OPEN，目标active、生产Gate不提升，默认OFF/SYSTEM/PARK与disabled不变。


2026-10-13 [SDK LAC evidence 的 backing 所有权](rfcs/unified-wal/implementation/profile-wire/README.md#sdk-lac-evidence-的-backing-所有权)：父73b67bc6e9上将 URL1/URL2 的 source/explicit payload 和 matching READ_ENTRY anchor 交给原已计费 frame，fan-in/ordered 等待期间保留，最终 evidence 前或失败/拒绝时释放。真实父consumer RED、最终三类24项与style通过；原17配置模型3正向/14指定反例按独立stage保留，2005 unique输入核验。独立RAT父443无新增；该切片不闭合 full-bytes 总账、默认SDK公开入口或全16步Gate，目标active、生产Gate OPEN，默认OFF/SYSTEM/PARK与disabled不变。


2026-10-13 [Recovery candidate 的 backing 所有权](rfcs/unified-wal/implementation/profile-wire/README.md#recovery-candidate-的-backing-所有权)：父2719623ab6后新增显式owned point-evidence入口，原source ByteBuf延续至candidate/标准packed payload最后引用释放；原worker/prefix/health/repair内部调用已交接并关闭，旧heap接口兼容。真实legacy分支RED、最终21项Java/style与40有限模型5正向/35指定反例通过，2005 unique输入SHA核验一致；独立RAT父443无新增。legacy/observation/header/caller及统一full-bytes、默认SDK/Main/其余16步验收仍OPEN，目标active、生产Gate不提升，默认OFF/SYSTEM/PARK及disabled不变。


2026-10-13 [Repair packing header 的 DATA 预算](rfcs/unified-wal/implementation/profile-wire/README.md#repair-packing-header-的-data-预算)：父58b942ee53上标准V3新增explicit header allocator，原内部repair使用目标DATA预算，source payload credit保持原归属；quota不足DEFERRED、不写DATA/发布membership，最后header view释放后target close才drain。真实原Unpooled分支RED、最终两类17项（含Classic digest6）/style与17有限模型2正向/15指定反例通过，2016 unique输入SHA核验；Java/model/style终态后的独立RAT exit1，443 unapproved路径全部存在父HEAD，无新增；不称CI全绿。默认/legacy/observation/caller及full-bytes总账、默认SDK/Main/其余16步仍OPEN，目标active、生产Gate不提升，默认OFF/SYSTEM/PARK及disabled不变。


2026-10-13 [Normal quorum probe 的 owned candidate](rfcs/unified-wal/implementation/profile-wire/README.md#normal-quorum-probe-的-owned-candidate)：父af791ffb69上新增原Listener/fencing/quorum算法的retained candidate入口，caller关闭或取消未交付时释放，legacy接口保持。真实heap分支RED、最终10项Java/style与16有限模型3正向/13指定反例通过，2005 unique输入核验；Java/model/style终态后的独立RAT exit1，443 unapproved全部存在父HEAD，无新增；不称CI全绿。此为observation接线的基础，observe()列表本身尚未切换，释放约定按AGENTS公共接口要求待答复；统一full-bytes、默认SDK/Main和全16步仍OPEN，目标active、生产Gate不提升，默认OFF/SYSTEM/PARK及disabled不变。


2026-10-13 [派生 index 的有界 Java 读取](rfcs/unified-wal/implementation/local-storage/README.md#派生-index-的有界-java-读取)：父69699cd886上将原RocksDB完整数组Get/iterator改为fixed-output并检查完整长度，binding/key/location格式64/77/160不变，坏value保持IOException。真实父源码8MiB→8,390,664B分配RED、新Get2144B与最终18项storage/index/style通过，1170 stage输入及format-only source分开归档；Java/style终态后的独立RAT exit1，443 unapproved均存在父HEAD、无新增；不称CI全绿。未改/重跑不覆盖JNI分配的TLC，native/object/统一full-bytes、默认SDK/observe公共约定及其余16步仍OPEN，目标active、生产Gate不提升，默认OFF/SYSTEM/PARK及disabled不变。


[声明模型目录与现有 Java 测试整轮验证](rfcs/unified-wal/implementation/profile-wire/README.md#声明模型目录与现有-java-测试整轮验证)：2026-10-13父fd3b1be724上的声明目录完整验证：730配置全部终结，118正向完整搜索PASS/612指定反例；现有Unified WAL Java43类整轮866项原exit1（2个遗留契约断言失败、0错误、5个Linux条件跳过），已修正两处测试并独立补验失败方法2项通过。四类补验首次被会话中断、回执为INCONCLUSIVE；新阶段253项全部通过，style通过；终态后独立RAT exit1/443父HEAD路径无新增，不称CI全绿，原失败/模型回执和修正stage分别保留，不改称整轮全绿。完整A+C+D组合、全字节、默认SDK/observe公共约定、Main/required producer、backup-restore/accepted退役/GC/mixed/正式性能/运维和16步验收继续OPEN，目标active、生产Gate不提升，默认OFF/SYSTEM/PARK及disabled不变。


[C+D local FREE 组合与实际边界验证](rfcs/unified-wal/implementation/profile-wire/README.md#cd-local-free-组合与实际边界验证)：2026-10-13父36163d9908后在原WholeAllocationFree新增CDSpec，直接组合两个原ProfileLogicalDelete实例与共享allocation的tombstone force/apply、旧I/O/reader drain、FREE force/apply；新四个指定反例覆盖早投递、无force、丢邻居和部分effect伪FREE。最终10配置2完整正向/8指定反例与实际两类6项Java通过，2010 unique source匹配；原Spec五配置保持原计数，catalog735尚未全跑、原730回执不移绑。style通过，实际终态后的独立RAT exit1/443父HEAD路径无新增，不称CI全绿。该单target/两ledger组合不冒充完整A+C+D，继续全16步及其余OPEN范围，目标active、生产Gate不提升，默认OFF/SYSTEM/PARK及disabled不变。


[A+C local publication 到 SDK ACK 的实际验证](rfcs/unified-wal/implementation/profile-wire/README.md#ac-local-publication-到-sdk-ack-的实际验证)：2026-10-22父30377492f4后在原ProfileSdkBridge新增ACSpec，直接由两个独立SegmentAllocator实例的local publication供给原A2 SDK ACK，slot2保留验证过的remote响应输入；已发请求可在caller超时后进入owner，取消不取消accepted I/O，恢复同步不mint原live ACK。最终17配置4完整正向/13指定反例、旧11配置计数不变，两阶段8+2原Java与最终新增1项真实队列/独立force测试通过，2016 unique路径按stage冻结，1项测试强化差异分别保留；catalog741未全跑，历史receipt不移绑。Spotless/Checkstyle通过；Java/model/style实际终结后独立RAT exit1，443父HEAD路径无新增，不称CI全绿。该单entry/每episode一个batch/pair内一次crash组合仍未闭合同批fence/冲突重试/跨batch UNKNOWN/多Arena/连续两次故障与完整A+C+D；继续全16步，目标active、生产Gate不提升，默认OFF/SYSTEM/PARK及disabled不变。


[Physical prefix 的实际 I/O 排空验证](rfcs/unified-wal/implementation/profile-wire/README.md#physical-prefix-的实际-io-排空验证)：2026-10-22父098838a574后在原C中分开file durability与逐batch actual I/O return，只有完整actual-return prefix才可local publication，未返回任务继续保留inflight；原A+C实例直接消费该事实。新增suffix提前发布反例精确命中“两批字节durable、仅第二批返回、第一批仍在途”。最终18配置4完整正向/14指定反例及现有4项真实排空/fence/mixed测试通过，2017 unique路径与重叠SHA核验一致；C/A+C状态计数细化，旧SDK6配置计数未变，catalog742未全跑、历史结果不移绑。Spotless/Checkstyle通过；Java/model/style实际终结后的独立RAT exit1，443父HEAD路径无新增，不称CI全绿。Java/wire/持久格式未改，分布式双batch/multi-Arena/冲突/完整A+C+D及其他16步验收仍OPEN，目标active、生产Gate不提升，默认OFF/SYSTEM/PARK及disabled不变。


[跨 batch suffix 的真实 quorum](rfcs/unified-wal/implementation/profile-wire/README.md#跨-batch-suffix-的真实-quorum)：2026-10-22父85716ef7f3后在原A+C新增suffix pending操作的分布式ACK窗口，完整DATA prefix durable仍必须等待predecessor实际返回；错误suffix DURABLE一直传播到SDK success的反例已命中。最终20配置5完整正向/15指定反例、旧18配置计数不变；两项新增真实A2/A3双batch与两项默认夹具回归通过，2019 unique路径/重叠SHA一致。两轮fixture未进入目标窗口的失败保留，当前hold在原Obligation终态交接，生产代码/wire/持久格式与旧fixture默认不变。catalog744未全跑，历史receipt不移绑。Spotless/Checkstyle通过；Java/model/style实际终结后独立RAT exit1，443父HEAD路径无新增，不称CI全绿。完整多peer/多SDK操作/multi-Arena/fence/冲突/恢复/两次故障/A+C+D及其余16步仍OPEN，目标active、生产Gate不提升，默认OFF/SYSTEM/PARK及disabled不变。


[超时后同坐标重试的身份验证](rfcs/unified-wal/implementation/profile-wire/README.md#超时后同坐标重试的身份验证)：2026-10-22父c60271c0f7上加强已有真实socket测试：原force在途且caller已超时，payload-only/length-only改变都冲突，LAC-only变化续接原操作、只发生一次force；该项及assembling/pending/index三项现有回归全部通过。原A+C增加同坐标cut与四个指定反例，最终25配置6完整正向/19指定反例、原20计数不变，2024 unique source/重叠SHA一致；owner为坐标保护oracle，不宣称第二个Y物理写、hot/index或restart冲突恢复已建模。Spotless/Checkstyle通过；终态后独立RAT exit1/443父HEAD路径无新增。catalog749未全跑，历史receipt保持绑定；生产/wire/格式未改，完整冲突恢复/multi-Arena/fence/两次故障/A+C+D和其余16步仍OPEN，目标active、生产Gate不提升，默认OFF/SYSTEM/PARK及disabled不变。


[Checkpoint record 的流式摘要与写入](rfcs/unified-wal/implementation/local-storage/README.md#checkpoint-record-的流式摘要与写入)：2026-10-22父830918cbb2上去掉原双owner image摘要/写入阶段的完整record临时复制，改为一个16-byte prefix和原record只读view，字节/SHA/force与publication合同不变。相同约8MiB workload旧digest/write分配8,407,640/8,421,296 bytes触发真实RED，新为12,992/29,704 bytes；最终两类20项全部通过，含向量、partial body失败、force/损坏回退/子进程退出及DATA图restart。1170输入按stage冻结，style通过；终态后独立RAT exit1/443父HEAD路径无新增。额外分段write的I/O成本未测，不宣称性能/full-bytes闭合；模型无变化不重跑。其他required范围与全16步仍OPEN，目标active、生产Gate不提升，默认OFF/SYSTEM/PARK及disabled不变。


[Recovery reader 的本地 RPC 实际排空](rfcs/unified-wal/implementation/profile-wire/README.md#recovery-reader-的本地-rpc-实际排空)：2026-10-22父f115cd7d22上修复reader已回逻辑结果却未等自身READ/FENCE/LAC/anchor output终态即关闭的真实缺口；复用原request drain锁/集合，parent coordinator六处等待close完成才归还名额，borrowed peers保持可用。真实旧源码RED、最终五窗口及相关23项零failure/error/skip，15原有限模型配置符合预期；模型没有新reader组合，2024路径按stage冻结、import-only排序单列。style通过；终态后独立RAT exit1/443父HEAD路径无新增。observe结果公共约定/默认SDK/Main/required producer/full-bytes/backup-restore/accepted退役/GC/mixed/完整模型/正式性能/运维与全16步仍OPEN，目标active、生产Gate不提升，默认OFF/SYSTEM/PARK及disabled不变。


[LAC collector 排队期间的关闭排空](rfcs/unified-wal/implementation/profile-wire/README.md#lac-collector-排队期间的关闭排空)：2026-10-22父36eb57665d后补原RPC终态之外的collector排队窗口；LAC内部result在原request drain登记，caller取消不提前释放，executor关闭兜底也等待owned reply清理。真实旧源码RED、新窗口通过；24项相关回归原23通过/1旧hold清理失败，已按actual-drain合同修正该方法并定向补验两项通过，原失败保留、不改称整轮全绿。1170输入按四stage冻结，style通过，终态后独立RAT443父HEAD路径无新增。模型未改不重跑，其他16步验收/生产Gate仍OPEN，目标active，默认OFF/SYSTEM/PARK及disabled不变。


[Checkpoint 流式写入的本地 JMH 对照](rfcs/unified-wal/implementation/local-storage/README.md#checkpoint-流式写入的本地-jmh-对照)：2026-10-22父bc2f189186上新增原microbenchmarks入口，以相同有效1/64条ALLOC image、digest/open/truncate/write/file force/目录同步对照830918旧codec与当前流式codec。旧→新→新→旧的8个fork均核验所加载class及完整image字节一致。64条Java分配约50–52KB/op→25.8–25.9KB/op，write66→130/op，worker CPU上升；短运行延迟波动不支持稳定延迟/吞吐提升结论。141相关输入、冻结jar/旧class、原始JMH/环境与复现脚本归档，package/style通过，独立终态RAT443父HEAD路径无新增。无生产改动，不提升B18/正式性能或其他Gate；全16步目标active，生产Gate OPEN，Linux NUMA/掉电/正式性能/canary仍NOT_EXECUTED，默认OFF/SYSTEM/PARK及disabled不变。


[Backup/restore 首版接受语义](rfcs/unified-wal/implementation/profile-wire/README.md#backuprestore-的首版接受语义)已按用户指定规划会话的2026-10-22裁决冻结：只支持新 BookieId/incarnation/目录的 verified export/rebuild，由当前未回滚集群授予 repair/membership 与新目标 bootstrap；旧控制/cursor/grant 不恢复为权限，旧 incarnation 删除及物理义务原样保留。原地 restore/同 BookieId 切换/MetadataStore 回滚继续 disabled；恢复接受下界未实现前不据窗口到期 GC。owner RFC 已同步，导出/导入及恢复专属验收仍 OPEN；后续据此实施，非语义完成即实现完成，完整目标 active、生产 Gate OPEN。


[SDK ACK→CLOSED删除→共享FREE组合](rfcs/unified-wal/implementation/models/README.md#sdk-ack-到-closed-删除及共享-free-的生命周期组合)：2026-10-22父b0710fd09c上直接组合原SDK/两个storage episode与两logical-delete/原allocation。最终五配置为1完整正向（529798/99442、queue0）、3指定mutation反例及1完整成功路径witness；只合并closed且drained后冻结SDK历史的显式TLC VIEW，原动作/性质保留，无state constraint或symmetry。1131输入/全部早期求值失败、未完成搜索及最终源码各自冻结；Python/TLC通过，独立RAT443父HEAD既有无新增。无生产Java改动，不重复Java测试，不将原receipt升级本组合native验收；catalog754未全跑，完整A+C+D及16步/生产Gate仍OPEN、目标active、默认OFF/SYSTEM/PARK与disabled不变。


[离线 backup image 与不可服务输入](rfcs/unified-wal/implementation/local-storage/README.md#离线-backup-image-与不可服务输入)：2026-10-22父4b1ba0889d上复用原DATA/双control锁和完整恢复扫描，建立有界manifest/外部SHA reference、全native文件复制/force/重读及借用scratch的instance点读；marked input在服务replay前拒绝，旧权限不作新授权。新启动拒绝RED，最终两类六项0 failure/error/skip、style通过；1171输入各stage冻结，独立RAT443父HEAD既有无新增。仅完成离线输入层，repair来源/新目标导入/membership/bootstrap及恢复专属组合仍OPEN，未称restore完成；旧目标义务保留，全16步目标active、生产Gate OPEN，默认OFF/SYSTEM/PARK和disabled不变。


[离线镜像接入当前 CLOSED repair-copy](rfcs/unified-wal/implementation/profile-wire/README.md#离线镜像接入当前-closed-repair-copy)：2026-10-23父b7c9e32bc6上，镜像仅作附加source，经current CLOSED/context/birth/retained source校验，复用inactive install/admit/grant/copy/membership/close；缺项取当前副本，冲突隔离，不降frontier或授normal。原copy同时等待reply与actual output终结，caller cancel保持实际资源；新增七场景及既有路径共12方法全部通过，reply-only敏感性RED保留。repair模型12配置2正向/10指定反例，catalog756未全跑；1171 Java/1134模型输入去重2033，各stage保持原绑定。style通过；RAT补两新cfg许可证后回到443父HEAD既有路径，无新增。新目标assignment/snapshot/no-hole suffix/required-through/readiness/registration、restore reuse/crash/lost reply及完整接受仍OPEN，旧目标义务不解除。完整16步目标active、生产Gate OPEN、默认OFF/SYSTEM/PARK与disabled不变，Linux NUMA/真实掉电/正式性能/canary未执行。


[恢复目标的首次 service bootstrap 组合验证](rfcs/unified-wal/implementation/profile-wire/README.md#恢复目标的首次-service-bootstrap-组合验证)：2026-10-23父389b49fada上，仅补原真实restore fixture的组合覆盖。新目标copy完成后可经当前verified-zero/head重启注册并mTLS读出原数据；首次注册前删除则用该目标自己的完整snapshot追赶，publication实际提交前已有cursor1/tombstone且尚无注册，旧Manifest/目标义务不变。最终三方法0 failure/error/skip（扩展restore方法九场景＋既有service drain/restart及snapshot/suffix），1171输入和早期零cursor测试假设失败各自冻结，无生产Java/TLA改动、不重跑TLC。完整restore的reuse/crash/lost reply/接受下界及其余16步验收仍OPEN，目标active、生产Gate OPEN、默认OFF/SYSTEM/PARK和disabled不变。


[旧恢复输入与当前 metadata 身份隔离](rfcs/unified-wal/implementation/profile-wire/README.md#旧恢复输入与当前-metadata-身份隔离)：2026-10-23父7021519bb3上，仅补真实restore当前metadata身份覆盖：相同body/version重建但native birth不同、同ledgerId非法替换instance/backlink，两者均QUARANTINED且零DATA/零copy/metadata不变/不授normal；既有镜像context隔离和wrong-instance tombstone原native/replay验证复用。最终两类三方法通过，restore方法11场景；两轮fixture/API与分类假设失败分别保留，1171输入冻结，无生产Java/TLA变更、不重跑TLC。该结果不授予合法reuse/原地restore；恢复crash/lost reply/接受下界/运维及完整16步继续OPEN，目标active、生产Gate OPEN、默认OFF/SYSTEM/PARK和disabled不变。


[镜像导入丢回复后的同 intent 续接](rfcs/unified-wal/implementation/profile-wire/README.md#镜像导入丢回复后的同-intent-续接)：2026-10-23父796582579f上，真实首条recovery DATA force后丢回复/断DATA，目标经正常停止和service重启、新coordinator同intent续接仅为缺项新增一次force；membership后grant-close丢回复/断CONTROL，新coordinator续接copy0/零新增DATA force。13场景restore方法与原historical lost-close两方法通过，原CLOSED及权限保持；1171输入/两轮fixture假设失败分别冻结，无生产Java/TLA改动、不重跑TLC。仅验证正常restart及上述回复丢失，非进程kill/掉电；恢复crash/cursor/readiness/registration丢回复/接受下界/运维与完整16步仍OPEN，目标active、生产Gate OPEN、默认OFF/SYSTEM/PARK和disabled不变。


[Owned service 的 repair INSTALL 与崩溃目标隔离](rfcs/unified-wal/implementation/profile-wire/README.md#owned-service-的-repair-install-与崩溃目标隔离)：2026-10-23父3e233f324b上，修复owned PREPARING_INSTALL漏查repair authority：仅initial NOT_FOUND时查原repair reader。真实owned/ZK/mTLS INSTALL NOT_FOUND RED已复现；修复后子JVM首条recovery DATA OK/APPLIED前halt23、无Java cleanup，原target因unclosed DATA session拒绝重开/注册；另新身份可完成copy/member/close，随后删除仍保留崩溃及新target义务。最终两方法通过（restore14场景及原owned SDK/drain），1171输入/五阶段保持原绑定，TLA未改不移绑旧receipt。**同target/同intent的unclean-session crash resume及新target4注册闭环尚未验收，不能以新身份重建替代该完整目标**；cursor/readiness/registration恢复、接受下界/运维及全16步仍OPEN、目标active、生产Gate OPEN，默认OFF/SYSTEM/PARK与disabled不变，真实掉电/Linux NUMA/正式性能/canary未执行。


[崩溃后新身份目标的注册与在线删除闭环](rfcs/unified-wal/implementation/profile-wire/README.md#崩溃后新身份目标的注册与在线删除闭环)：2026-10-23父807f78579a上，只扩展原restore测试。真实child halt后原target3保持隔离，新身份target4完成当前owned startup/registration/fresh peer+mTLS点读，再由原follower投递实际删除到cursor1/tombstone并拒绝后续读；Manifest完整字节与旧、新target义务保持。原14场景方法通过，1171 native输入及最终format-only换行分开保存；生产Java/TLA未改，不重跑TLC。补齐上一阶段新target4注册缺口，**不替代原target/原intent的unclean-session恢复**；相关新增数据语义已提请用户选择，未答复不放行。完整16步仍OPEN、目标active、生产Gate OPEN、默认OFF/SYSTEM/PARK与disabled不变，Linux NUMA/真实掉电/正式性能/canary未执行。


[派生 index 的 write-buffer 配置容量预留](rfcs/unified-wal/implementation/local-storage/README.md#派生-index-的-write-buffer-配置容量预留)：2026-10-23父a08d52884a上，原shared DataWorkingSet启动前预留扩展为固定pool＋1 MiB cache＋两份1 MiB write-buffer配置容量，实际RocksDB选项值不变。真实quota缺口RED已复现；最终三方法通过，含原owned SDK/drain及恢复14场景，1171输入/red-green/同版依赖源码分别冻结。测试fixture显式4 MiB total，生产quota不自动增大；reservation保持至实际storage/index关闭。只是配置容量预留，不宣称memtable/native/RSS硬上限或full-bytes闭合。TLA转移未改；统一预算、同身份unclean-session恢复待决语义及其余16步继续OPEN，目标active、生产Gate OPEN、默认OFF/SYSTEM/PARK和disabled不变，Linux NUMA/真实掉电/正式性能/canary未执行。

[只读磁盘状态诊断](rfcs/unified-wal/implementation/local-storage/README.md#只读磁盘状态诊断)：2026-10-23父4907120bff上，实验 main 复用原 Arena replay，以既有三类排他锁和READ-only内容channel报告观察到的身份/布局、session/quarantine与分配状态，不执行恢复sync或授予服务/restore权限；原正常恢复与unclosed gate保持。真实文件/独立JVM/锁、fallback与坏suffix验证由该节拥有。这里只交付离线诊断，完整运维/同身份crash接受语义/16步验收仍OPEN，目标active、生产Gate OPEN，默认OFF/SYSTEM/PARK及disabled不变。

[恢复 bootstrap 的 snapshot suffix 与发布丢回复](rfcs/unified-wal/implementation/profile-wire/README.md#恢复-bootstrap-的-snapshot-suffix-与发布丢回复)：2026-10-23父d4a3e38114上，补实际恢复目标snapshot cut1＋真实第二ledger删除suffix/head2；完整时cursor2/两tombstone后注册，缺event2时保持cursor1且无writable。persistent readiness与writable两次实际ZK multi后分别回CONNECTIONLOSS，同session精确确认成功，Manifest/旧目标义务保持。仅测试fixture与既有文档变化，原14场景保持；不替代同incarnation unclosed crash恢复或完整接受，16步目标active、生产Gate OPEN、默认OFF/SYSTEM/PARK及disabled不变。

[显式 SDK writer 的 generated header 字节额度](rfcs/unified-wal/implementation/profile-wire/README.md#显式-sdk-writer-的-generated-header-字节额度)：2026-10-23父9a9aa9cd79上，普通V3 header在entryId前取得原DATA额度，WRITE_LAC header同样计费；额度不足不分配entry、不发送DATA，取消/回收与source→socket实际交接保持。仅绕过计费的敏感性RED已复现，最终三类37项通过，Classic三digest实际读写及原SDK/close/替换/恢复排空保持。这里是显式writer生成buffer的单handle额度，不关闭全字节/性能/默认SDK及其他16步范围，目标active、生产Gate OPEN，默认OFF/SYSTEM/PARK及disabled不变。

[三副本 SDK 到本地删除 FREE 的组合](rfcs/unified-wal/implementation/models/README.md#三副本-sdk-到本地删除-free-的组合)：2026-10-23父f5fc64ace4上，原SDK/两个storage与delete/FREE保持，第三slot新增自己的原allocator publication/actual I/O return；close输入还须SDK停止及三请求/source/I/O排空。六配置为1完整正向（41770502/6170077、queue0）、4指定反例和1三副本成功路径witness，1142输入相同、显式3600秒预算；762目录未全跑。仍只有一个本地allocation的D/FREE，metadata/fenced-CLOSED为输入，旧slot迟到回复的全部close路径与完整A+C+D/16步验收继续OPEN；无Java变更，目标active、生产Gate OPEN、默认OFF/SYSTEM/PARK及disabled不变。

[SDK 停止后丢弃回复的实际请求结算](rfcs/unified-wal/implementation/models/README.md#sdk-停止后丢弃回复的实际请求结算)：2026-10-23父1fef8057fc上，组合模型补原slot0/1在SDK停止且实际I/O返回后的sent结算，不生成ACK。九配置为1完整正向（44131126/6740125、queue0）、6指定反例、2个49步A=2成功后停止/丢弃回复/local FREE witness；1146模型输入相同，native1172未变、不重跑Java。具体witness的I/O在stop前返回；仍非全目标物理完成、全部timeout/故障或完整A+C+D。766目录未全跑，16步目标active、生产Gate OPEN，默认OFF/SYSTEM/PARK及disabled不变。

[SDK 迟到 DATA 到删除复用及健康重启的原生组合](rfcs/unified-wal/implementation/profile-wire/README.md#sdk-迟到-data-到删除复用及健康重启的原生组合)：2026-10-23父bf9c813280上，新增一方法串联实际3/3/2 SDK ACK、第三副本force跨越stop、固定P/L普通close、三目标delete/本地FREE、ledger45复用generation2及逐节点健康restart/mTLS点读。最终1 PASS、1172输入匹配、生产Java/TLA未改、style通过；旧Manifest/三target义务保留。仅单Arena顺序复用的有限组合，非crash、多Arena、持久physical receipt/GC或完整16步验收；目标active、生产Gate OPEN，默认OFF/SYSTEM/PARK及disabled不变。

[Profile 启动恢复扫描的 DATA 字节额度](rfcs/unified-wal/implementation/local-storage/README.md#profile-启动恢复扫描的-data-字节额度)：2026-10-23父66e860ea32上，将原 service DATA quota 传给 restart scan，在分配 header/page/batch/covered MOVE scratch 前计费，实际 scope 结束归还；额度不足不启动新 DATA session。真实大 batch 拒绝、caller cancel/实际 force、CRC 失败回收及原 covered MOVE/owned SDK restart 共4方法通过，1172输入匹配、最终style通过；绕过 budget 转发的敏感性RED独立保留。只闭合启动DATA buffer计费，全字节、同身份crash/多Arena待决语义和完整16步仍OPEN，目标active、生产Gate OPEN，默认OFF/SYSTEM/PARK及disabled不变。

[共享 allocation 存活邻居的搬迁与复用](rfcs/unified-wal/implementation/profile-wire/README.md#共享-allocation-存活邻居的搬迁与复用)：2026-10-23父c8483d3549上，原三副本SDK生命周期加入同块未删除ledger45；第三COPY force未返回时无MOVE/FREE，实际搬迁后邻居可读，ledger46的256条普通Add经合法尾部耗尽后复用旧块generation2。三节点健康restart后邻居/新首末entry仍可读、44拒绝、历史义务保持。三方法通过，1172输入/三轮fixture假设失败/最终纯换行分别保留，style通过；生产Java/TLA未改，不重跑模型。仅闭合该有限组合，完整16步目标active、生产Gate OPEN，默认OFF/SYSTEM/PARK及disabled不变。

[控制 checkpoint 的流式读回校验](rfcs/unified-wal/implementation/local-storage/README.md#控制-checkpoint-的流式读回校验)：2026-10-23父b03297b919上，Bookie/Arena 写后验证复用原 parser，用最多64 KiB record scratch逐条比较冻结状态；恢复仍返回独立record，格式/force/commit/publication不变。8 MiB有限image的旧比较额外Java分配8,535,328 bytes，新版本最终185,200 bytes；最终两类48方法通过、1172输入匹配、style通过，旧算法RED独立保留。不是RSS/吞吐或统一CONTROL总账证明；完整16步目标active、生产Gate OPEN，默认OFF/SYSTEM/PARK及disabled不变。

[Profile checkpoint 校验的 CONTROL 容量预留](rfcs/unified-wal/implementation/local-storage/README.md#profile-checkpoint-校验的-control-容量预留)：2026-10-23父a533d249d2上，Service在storage open前从原CONTROL额度保留69776 bytes，保持至实际storage关闭；未接线启动缺口及同步drain回调重入重复归还两次RED分别保留，最终先移走token引用再归还。最终两类19方法通过、1172输入匹配、style通过，覆盖真实held checkpoint force、停止准入后verification、原SDK/Embedded/owned restart与bind cleanup。测试只增加固定容量，原DATA/网络空间保持；统一CONTROL/full-bytes和其余16步仍OPEN，目标active、生产Gate OPEN，默认OFF/SYSTEM/PARK及disabled不变。

[Arena checkpoint 的借用记录恢复](rfcs/unified-wal/implementation/local-storage/README.md#arena-checkpoint-的借用记录恢复)：2026-10-23父8579d1e211上，原parser以一份record scratch同步回调Arena解码，完整digest成功前只形成私有候选，失败清空后fallback；Bookie retained read与写后verify保持各自ownership。4 MiB有限image额外Java分配4,269,832→最终135,536 bytes，晚digest错误不遗留ghost allocation/id。最终三类56方法通过、1172输入匹配、style通过；启动streaming复用既有CONTROL预留，未改格式/配置/TLA。typed/frozen/Bookie状态及完整full-bytes/16步仍OPEN，目标active、生产Gate OPEN，默认OFF/SYSTEM/PARK及disabled不变。


[Arena checkpoint 的按需编码与持有态](rfcs/unified-wal/implementation/local-storage/README.md#arena-checkpoint-的按需编码与持有态)：2026-10-23父e1fb23a0fa上，Arena只冻结原不可变命令/状态/序号，摘要、写出及精确读回比较时按需编码单条record；Bookie retained原始字节保持。实际两个JVM在同16,000控制结果/首次image force前的full-GC histogram中，只读buffer增量16,000→0、byte[]增量3,393,904→66,064，完整image字节相同且各自重开；非RSS/峰值/性能结论，重复编码成本未测。Service原CONTROL预留扩为200848 bytes并保持至实际storage关闭，生产quota不自动增加；不足启动的敏感性RED和原始失败分层保留；最终四类72项通过、1172输入一致、style通过，TLA未改不重跑TLC。typed/Bookie/其他full-bytes及完整16步仍OPEN，目标active、生产Gate OPEN，默认OFF/SYSTEM/PARK及disabled不变。


[Bookie control 原始记录的容量预留与关闭释放](rfcs/unified-wal/implementation/local-storage/README.md#bookie-control-原始记录的容量预留与关闭释放)：2026-10-23父8b245c0ce5上，原Service CONTROL working-set在storage open前增加3倍journal的Bookie canonical backing容量，合计200848＋3×controlLogBytes，实际storage终结后归还；原raw map在close路径清空，durable字节/cut/closed拒绝保持。父版2RED、同选择修复2PASS；同4096实际INSTALL文件的两JVM在保留closed对象时，父版仍4096个只读buffer，新版降至0，磁盘字节相同且重开成功；最终三类37项通过、1172输入一致、style通过，TLA未改不重跑TLC。仅raw backing预留/生命周期与有限full-GC持有态，typed/JDK/native等完整full-bytes未闭合；全部16步目标active、生产Gate OPEN，默认OFF/SYSTEM/PARK及disabled不变。


[控制批次的逐条编码与完整预检](rfcs/unified-wal/implementation/local-storage/README.md#控制批次的逐条编码与完整预检)：2026-10-23父ab44b475a5上，双owner在任何I/O前逐条完整预检/累计整批长度，预留原坐标后逐条编码writeFully、整批一次force及原逐序apply；去掉整批serialized列表，写者排他/UNKNOWN/partial前缀恢复保持。新两项后段拒绝/第二write失败与既有定向共6项通过；相同observer的四JVM在首次write前只读buffer增量均4096→1，Arena journal字节相同，Bookie逐项重开核对context/credential。codec预留331920＋既有3倍journal的raw backing，实际inline值核对；最终四类82项通过、1172输入一致、style通过，TLA未改不重跑TLC。CPU/累计分配未测，typed/JDK/native等完整full-bytes未闭合。全部16步目标active、生产Gate OPEN，默认OFF/SYSTEM/PARK及disabled不变。


[控制批次流式编码的本地 JMH 对照](rfcs/unified-wal/implementation/local-storage/README.md#控制批次流式编码的本地-jmh-对照)：2026-10-23父ed036afd83上，原microbench新增固定256批append/force/apply入口；每批1/64条，Arena同块竞争/Bookie新INSTALL。两版相同release17编译、16有效fork按父→新→新→父完成真实force及每轮重开/class校验；初轮major65/61混用的16 fork保留且不作性能对照。累计分配上升已实测（64条Arena59.5→78.2–96.6KB，Bookie约700→803KB），CPU/wall波动不支持稳定收益。生产Java未改，后续复用原codec校验/尺寸以消除预检frame额外构造；完整full-bytes/正式性能与16步目标仍active、生产Gate OPEN，默认OFF/SYSTEM/PARK及disabled不变。


[控制批次预检的校验与尺寸复用](rfcs/unified-wal/implementation/local-storage/README.md#控制批次预检的校验与尺寸复用)：2026-10-24父3c99a1f92c上，双owner整批预检改为原codec共享字段校验/尺寸，移除预检frame/CRC构造；实际完整编码、整批坐标预留、write/一次force/顺序apply及UNKNOWN不变。两项codec边界与既有五类最终87项、style通过，独立RAT443父路径无新增。同一benchmark/依赖、同release17编译67 class并核对实际加载/Maven指令流；16有效fork实测64条分配Arena96.6→63.3 KB、Bookie803.7–808.3→747.4–750.0 KB，CPU/wall波动仍不支持稳定收益。未改格式/模型或缩减CONTROL预留，完整full-bytes/正式性能与16步继续OPEN，目标active、生产Gate OPEN，默认OFF/SYSTEM/PARK及disabled不变。


[当前 Java 整轮与注册池排空观察](rfcs/unified-wal/implementation/profile-wire/README.md#当前-java-整轮与注册池排空观察)：2026-10-24父5a16759da1上的43类整轮实际897项，891通过/1 error/5个Linux条件skip；唯一旧pool测试在hold输出promise时提前等channel close，独立单方法亦复现。按原reader actual-drain先于lease关闭合同，仅修正该方法的观察顺序并加强task拒绝提前释放；最终四项回归通过，其他方法字节及生产Java/TLA不变。原整轮exit1和全部输入/回执保留，不把补验改称整轮全绿；style通过、独立RAT443父路径无新增。其余16步/完整验收继续OPEN，目标active、生产Gate OPEN，默认OFF/SYSTEM/PARK及disabled不变。


[跨 batch quorum 的 durable 回复观察](rfcs/unified-wal/implementation/profile-wire/README.md#跨-batch-quorum-的-durable-回复观察)：2026-10-25父85d158c432上，原43类整轮暴露回复总数观察错误；真实input额度拒绝产生NONE后，旧总数断言的两个受控反例失败。最终仅统计durable结果，先完成第三副本entry0再对suffix拒绝，保留前两副本physicalTerminal与A2/A3屏障，三个节点使用原子force调用计数并结合实际终态。原尝试数3/退避5ms/deadline10s保持。最终同一43类整轮897项为892通过/0 failure/error/5个Linux条件skip，用例身份及1172输入一致；style通过、独立RAT443父路径无新增。所有先前失败分阶段保留；仅macOS Java PASS_SCOPED，生产/TLA未改，完整16步目标active、生产Gate OPEN，默认OFF/SYSTEM/PARK及disabled不变。


[控制 owner 关闭后的历史状态释放](rfcs/unified-wal/implementation/local-storage/README.md#控制-owner-关闭后的历史状态释放)：2026-10-25父8d2b448a0b上，Bookie/Arena在原close guard之后替换9/10个历史容器并释放pending，保留rollback/delete/session/quarantine/cut诊断；Arena关闭后的历史查询明确拒绝，原真实排空与protected settlement不变。扩展6个既有方法，9项定向及最终4类114项全部通过、输入/用例身份匹配、style通过；独立RAT仍443父路径无新增。相同冻结native输入与同JDK的两JVM持续持有closed owner：Bookie LedgerState4096→0、Arena AppliedOperation16001→0、被观察HashMap table57392→0槽位，重开及磁盘SHA不变；不是峰值/RSS或完整full-bytes。未改格式/模型或减少CONTROL预留，默认SDK/observe、同身份unclean-session恢复、多Arena及其余16步仍OPEN，目标active、生产Gate OPEN，默认OFF/SYSTEM/PARK及disabled不变。


[按 allocation 索引当前与退休区间](rfcs/unified-wal/implementation/local-storage/README.md#按-allocation-索引当前与退休区间)：2026-10-25父f51c412490上，Arena current/retired位置索引从逐block展开改为每allocation单项区间，保留exact FREE/reuse、跨grant拒绝、generation和实际保护终结；有ID的grant查询继续O(1)，容量/Inspection仍按block计数。新增1方法及扩展真实2-block FREE/reuse方法；fixture仅补header占用的4096B后7项定向通过，原容量失败独立保留。最终5类590项零failure/error/skip，用例身份和1172输入匹配，style通过；独立RAT仍443父路径无新增。四JVM真实control对照中65536-block索引/Slot从65536降为1，control字节及replay一致；仅control metadata稀疏DATA夹具，不是完整预分配或服务证明。全部生成DATA已按终态/句柄检查清理，证据保留。完整full-bytes/B11/其余16步仍OPEN，目标active、生产Gate OPEN，默认OFF/SYSTEM/PARK及disabled不变。


[Bookie checkpoint 的 typed 按需编码与 100k 驻留](rfcs/unified-wal/implementation/local-storage/README.md#bookie-checkpoint-的-typed-按需编码与-100k-驻留)：2026-10-25父e227679ff2上，Bookie common checkpoint记录改为冻结Command/sequence/status的typed按需编码，保留原sealed、首次结果及terminal成功覆盖，special raw和CONTROL预留不变；checkpointId仅解析commit。新journal/image精确bytes及最终五类591项全部通过、source/用例身份匹配，style通过、独立RAT443父路径无新增。真实100k native-control驻留中raw backing降至0；metadata保守下界append146.1→69.3MB、replay152.5→75.7MB，整进程live增量约下降87MiB。下界不代替上界，完整B11/full-bytes仍OPEN；fixture不含MetadataStore/SDK/service/RocksDB。原始测量、失败准备和cwd误复制清理独立保留，未修改原PDF。完整16步目标active、生产Gate OPEN，默认OFF/SYSTEM/PARK及disabled不变。


[Bookie common 结果与 checkpoint 的单一索引](rfcs/unified-wal/implementation/local-storage/README.md#bookie-common-结果与-checkpoint-的单一索引)：2026-10-25父bd3beb346f上，common AppliedOperation同时拥有原结果与checkpoint tuple，保留失败terminal upgrade的少量历史override及原bytes/status/sequence。首次592项出现TLS close操作promise清理error，原失败保留；只修正测试helper的actual closeFuture/最终drain观察，额度与生产allocator不变。最终同一五类592项全部通过、输入/用例身份匹配、style通过，独立RAT443父路径无新增。相同100k seed重放中Map entries50万→30万、UUID40万→20万，整进程live增量减少19,845,032 B；当前九metadata根去重对象图171,946,208 B包含数组头/包装/String，但不是独占retained size或B11 PASS。完整full-bytes/16步目标active、生产Gate OPEN，默认OFF/SYSTEM/PARK及disabled保持。


[Bookie 私有 scope 的紧凑驻留](rfcs/unified-wal/implementation/local-storage/README.md#bookie-私有-scope-的紧凑驻留)：2026-10-25父a0f6d8217a上，私有StoredScope共享准确context/固定target，保留原domain String等值、外部target/incarnation检查、原sealed与canonical bytes。新增2项真实文件回归及最终五类594项全部通过、source/用例身份匹配，style通过；独立RAT443父路径无新增。两独立JVM同100k native seed的九根去重对象图171,946,208→122,346,384 B（163.98→116.68 MiB），减少约140万对象；这是reachable shallow，未证明独占retained/完整Bookie B11。CONTROL预留和格式不变，完整full-bytes/16步目标active、生产Gate OPEN，默认OFF/SYSTEM/PARK及disabled保持。


[Managed storage 的 100k 账本与 Arena 短记录预留缺口](rfcs/unified-wal/implementation/local-storage/README.md#managed-storage-的-100k-账本与-arena-短记录预留缺口)：2026-10-25源码8a81b08fb4上，新增正常startNew→原CONTROL driver填充100k→真实healthy close→fresh restart的完整isolated storage计量；200000 write/1563 force、全部route与drain通过，重放included图122,370,392 B（116.70 MiB）、整进程live134,196,304 B，FD/线程实际终结。另以原requestPool生成10000短记录、rotation和force冻结，Arena十根可达图3,812,536 B，高于整个storage CONTROL公式3,477,648 B，栈上snapshot另列；不是独占retained超额或portable bound证明。当前预留未单列Arena/快照成本，下一项按record/rows/high-water和真实并发重算，不以任意倍数替代。未改Java/TLA，原594/style PASS保持原绑定；B11/full-bytes及16步目标active、生产Gate OPEN，OFF/SYSTEM/PARK与disabled保持。


[CONTROL typed 状态与工作集的启动预留](rfcs/unified-wal/implementation/local-storage/README.md#control-typed-状态与工作集的启动预留)：2026-10-25父0e63840b99上，以支持布局的typed/history/index、快照/recovery及预检前pending模型替代旧3L金额，仍在原service open前单次预留、实际close后释放，生产quota不自动扩大。支持64位HotSpot17/21与8..256对齐，其他布局拒绝启用实验服务；默认OFF/Classic不变。旧额度可启动的真实RED保留；8项定向和六类597项全部通过，source/用例匹配、style通过。四JVM实测所选类型shallow≤模型；1MiB日志/P16/S4保守预留96,518,568B，128MiB对应11,534,644,136B，不等于实际驻留。模型不闭合任意Throwable/JCA/native或完整状态计数证明；B11/full-bytes及16步目标active、生产Gate OPEN，OFF/SYSTEM/PARK与disabled保持。


[显式离线 backup/restore 运维入口](rfcs/unified-wal/implementation/local-storage/README.md#显式离线-backuprestore-运维入口)：2026-10-25父86c9259dd7上，公开 offlineBackup factory 复用原 ISOLATED 配置冻结历史身份/bounds，capture 交付独立 CREATE_NEW/force 的56 B外部reference；import 复用当前 CLOSED/RepairIntent 授权与原复制/bootstrap，私有 image 只在实际 completion 后关闭。17个既有恢复场景改走公开入口，覆盖源目录丢失、坏reference、同步失败清理、取消及外部完成future不能提前释放native锁；定向5方法及扩大三类215项全部通过，输入/用例身份匹配。最终三处换行有非空白相同证明，style通过、独立RAT仍443父路径无新增；原失败与源码绑定保留，详细证据由上述 canonical 节拥有。仍为显式Java运维API，默认SDK/Main、完整恢复接受下界、同incarnation unclosed-session接受、B11/full-bytes及其余16步继续OPEN，目标active、生产Gate OPEN，默认OFF/SYSTEM/PARK和disabled保持。


[显式 SDK CLOSED 只读入口与依赖关闭顺序](rfcs/unified-wal/implementation/profile-wire/README.md#显式-sdk-closed-只读入口与依赖关闭顺序)：2026-10-25父cc7b8cdd82上，BookKeeper显式enable/open复用原registered pool与CLOSED coordinator，单个open/handle持有lease到实际read/output终结；SDK close先排空Profile再关闭原metadata/executors，中断可继续，self-drain提前拒绝。三项真实服务定向与相关两类23项全部通过、输入匹配，style通过；新增有限模型1正向/3指定反例符合预期，旧Spec/11cfg保持原绑定；独立RAT仍443父路径无新增，原证据由上述canonical节拥有。默认builder拒绝保持，writer create/默认routing/Main可信issuer/full-bytes及其余16步继续OPEN，目标active、生产Gate OPEN，OFF/SYSTEM/PARK与disabled保持。


[Writer close 的实际终态与排队关闭](rfcs/unified-wal/implementation/profile-wire/README.md#writer-close-的实际终态与排队关闭)：2026-10-25父4910d16933上，beginCloseWriter分开bounded result与actualCompleted；包内writer owner将已接受但尚未freeze的close纳入实际排空，local-stop先赢不发布CLOSED，成功callback等待lease、失败/deadline保持原回复。三项真实定向与相关14项全部通过、style通过，新增有限模型1正向/3指定反例符合预期，旧Spec/8cfg保持原绑定；独立RAT仍443既有路径无新增，原证据由上述canonical节拥有。用户已选择显式create/open；public create下一项需要原子fresh reservation归属，不能以matching READY重发writer。该入口仍待接入，完整目标active、生产Gate OPEN，OFF/SYSTEM/PARK与disabled保持。


[显式 SDK fresh create 与 writer 归属](rfcs/unified-wal/implementation/profile-wire/README.md#显式-sdk-fresh-create-与-writer-归属)：2026-10-25父9ef30669e4上，公开enableProfile/createProfileLedger复用单owner、原all-E initializer和writer close；只有本次原生CREATE2 birth回执可取得fresh claim，matching READY/metadata不重发writer。同SDK只续接未交付的原完整请求，handoff消费origin；cancel/global stop等待实际init/config/DATA/lease，SDK stop保留OPEN。额外standby不安装/激活/DATA，CLOSED只读仅过滤非retained成员。定向11项及扩大四类68项全部通过、输入/用例身份匹配，style通过；新增有限模型1正向/4指定反例符合预期，旧Spec/8cfg保留，独立RAT仍443既有路径无新增。实际源码/回执由canonical节拥有，历史证据不移绑。默认builder/可信Main输入/full-bytes及其余16步边界保持，完整目标active、生产Gate OPEN、OFF/SYSTEM/PARK与disabled保持。


[共享 cohort 的多 ledger SDK 与子视图排空](rfcs/unified-wal/implementation/profile-wire/README.md#共享-cohort-的多-ledger-sdk-与子视图排空)：2026-10-26父c5ec73163f上，显式SDK增加有界maxOpenLedgers、共享单Epoch/原6连接与独立CONTROL/DATA子视图；READ/LAC derived views按回复分离，最后子操作才关闭父lease/pool。首CONTROL复用已有pending等待实际授权，取消ready观察前不归还名额；原NONE背压与fresh归属保持。六项定向与五类275项全部通过、用例/输入匹配；最后一行纯换行有6个owner class指令一致证明且六项补验通过，style通过。新增有限模型1正向/5指定反例符合预期、原Spec/15cfg保持；独立RAT仍443既有路径无新增。各阶段源码/回执由canonical节拥有。首版默认builder继续Classic；完整目标active、生产Gate OPEN、OFF/SYSTEM/PARK与disabled保持。


[显式 SDK 写期换组与实际排空](rfcs/unified-wal/implementation/profile-wire/README.md#显式-sdk-写期换组与实际排空)：2026-10-26父d232f2df64上，显式enableProfile可提供原replacement coordinator，请求冻结0..32的maxEnsembleChanges、默认0；同cohort standby在fresh claim前验证，初始DATA仍只含E，close保留未来目标CONTROL。已接受换组从ordered排队到实际cold/完成callback及连续变更均持有episode，SDK stop等待实际终结及子视图输出，停止后不采纳/重发且不制造CLOSED。三项定向及五类278项全部通过、输入/用例匹配、style通过；新增有限模型1正向/3指定反例符合预期、旧Spec/10cfg保持。首轮两项权限/存在oracle错误及原源码回执保留，修正仅查实际hot/index空坐标。独立RAT仍443既有路径无新增，证据由canonical节拥有。完整目标active、生产Gate OPEN、OFF/SYSTEM/PARK与disabled保持；Main可信输入/full-bytes/B11/恢复/GC/兼容/正式性能及其余16步继续OPEN。


[已发布 repair intent 的跨进程续接](rfcs/unified-wal/implementation/profile-wire/README.md#已发布-repair-intent-的跨进程续接)：2026-10-26父4ba371d277上，prepareRepair在当前完整recovery context校验后优先取回全字节相同的持久intent，只有新intent才要求predecessor ensemble匹配；后续admit/publication/close权限校验保持。原生产在第二新JVM重载时CONFLICT的真实RED已复现；修复后两个协调器JVM从保存输入独立重读，首进程在durable close丢回复后actual completion并halt23，第二进程同birth/version/closeId续接、copy0且无新增DATA force，目标持续运行。定向1项及五类279项全部通过、源码/用例/child绑定匹配，style通过；独立RAT仍443父路径且字节全同、无新增。TLA未改，不移绑旧PASS；源码回执由canonical节拥有。原unclosed-session隔离、恢复接受下界/GC/Main/full-bytes/B11/兼容/正式性能及其余16步仍OPEN，目标active、生产Gate OPEN、OFF/SYSTEM/PARK与disabled保持。


[永久坐标根的 DATA 预留与关闭退休](rfcs/unified-wal/implementation/local-storage/README.md#永久坐标根的-data-预留与关闭退休)：2026-10-27父277f4d6514上，原DATA启动单次预留加入按logicalShards/indexedEntries计算的永久occupied HashSet/Key/context容量，复用既有HotSpot布局模型，quota不足在storage/index/目录open前拒绝。实际owner/I/O/pin及资源close成功后退休hot五容器与共享startup inventory，再退额度；false/异常close保持原状态。两个目标RED已复现，修正后测试源码相同；两项定向及六类618项全部通过、输入/用例匹配，四JVM碰撞Key图不超模型、style通过，独立RAT443父路径且字节全同、无新增。原夹具第二写拒绝单独保留，不称产品RED；证据由canonical节拥有。金额只覆盖该永久根，full-bytes/B11及其余16步仍OPEN；Main首版按用户选择限已有旧服务受控部署，issuer/装载/进程验收未完成。目标active、生产Gate OPEN、OFF/SYSTEM/PARK与disabled保持。


[无客户端证书的真实 TLS 拒绝与预算归还](rfcs/unified-wal/implementation/profile-wire/README.md#无客户端证书的真实-tls-拒绝与预算归还)：2026-10-27父47ba97ef85上，新增真实TLS1.3无客户端证书拒绝测试；以exact accepted server握手失败及实际channel终止为准，验证零明文/响应、AuthZ/authority read与存储effect，预算归零后同listener带证书UIC1完成durable INSTALL/ACTIVATE及normal admission。生产源码未改；定向1项及三类285项通过、冻结输入与用例匹配。实际格式/RAT回执由canonical节拥有。仅补认证边界证据，完整G1、Main issuer/装载、TLS总内存/full-bytes/B11及其余16步仍OPEN；目标active、生产Gate OPEN、OFF/SYSTEM/PARK与disabled保持。


[Owned Profile TLS 的 native 引用与服务终态](rfcs/unified-wal/implementation/startup-readiness/README.md#owned-profile-tls-的-native-引用与服务终态)：2026-10-27父3414108da6上，明确无factory重载在原episode内创建TLS，实际listener/storage/credit/metadata/loops终结后释放；旧带factory重载仍borrowed。Profile handler选择/构造配置、refresh与close共用monitor，成功refresh释放旧owner、失败保留旧context，未交付engine异常归还；成功reinit拒绝，关闭幂等且拒绝新入口。相同probe字节在旧native context残留上RED、修复后2→1→0 GREEN；17项定向通过，五类540项中532通过/8为原Classic格式前置条件跳过、0失败错误。初次编译接线错误及原输入独立保留，源码/验证由canonical节拥有。仅补TLS实际ownership，不闭合Main issuer/装载、TLS总字节/full-bytes/B11、完整G1或其余16步；目标active、生产Gate OPEN、OFF/SYSTEM/PARK与disabled保持。


[Hot 与 restart inventory 的驻留元数据预算](rfcs/unified-wal/implementation/local-storage/README.md#hot-与-restart-inventory-的驻留元数据预算)：2026-10-27父d4837e0ea7上，原DATA单次启动预留加入hot/pending元数据与restart inventory，复用Layout的identity/immutable容器及direct view上界；两map高水位、各shard失败admission、旧index返回窗口计入，正常Key/context沿occupied。Inventory寿命跨terminal退休/新ledger复用，另计旧C个context，不重复Key；quota不足在storage/目录前拒绝，原actual关闭/五容器与inventory退休后退费。预算RED已复现，阶段6/600项保留原绑定；最终费用修正仅inventory函数/注释，三类260项零失败/错误/跳过，style/RAT以canonical实际回执为准。四最终JVM独立2C context图不超模型，但不证明状态数/RSS/native/完整B11。raw-unpooled backing、解析/执行器/pool其余元数据、Main/完整G1与其余16步仍OPEN；目标active、生产Gate OPEN、OFF/SYSTEM/PARK与disabled保持。


[Pooled 解析工作区与派生 pending context](rfcs/unified-wal/implementation/local-storage/README.md#pooled-解析工作区与派生-pending-context)：2026-10-27父5d4cd9ccda上，原DATA一次预留加入按pool几何K、owner/维护并发及P票据返回窗口计算的pooled解析工作区和PendingNormal独立context A；4KiB K20，hot重解析B不共享A。PoolSubmission在invoke前消费callback，切断旧CONTROL task到future DATA leader的回指；异常汇总/后续投递/actual drain保持。冷扫描复用page/content credit计本批元数据，按已验证n限制重读记录数，超额整批拒绝。预算与两callback实际RED已复现，6项定向通过，四JVM真实解析/Hot/Pending/A+B图不超模型；最终六类627项零失败/错误/跳过，style/RAT以canonical实际回执为准。完整read/driver/跨批扫描/compaction/pool/raw图及B11仍OPEN，不计性能/native/RSS或状态数证明；目标active、生产Gate OPEN、OFF/SYSTEM/PARK与disabled保持。


[恢复扫描的跨批工作区](rfcs/unified-wal/implementation/local-storage/README.md#恢复扫描的跨批工作区)：2026-10-27父9a195f5b7f上，冷路径新增一个跨批credit，按C/S与Arena实际M/R/A计entries、foundMoves、stream ranges、headers/postcoordinates及独立旧seed；indexed入口先付费再建seed，保持到实际扫描/postcheck终结，不给normal Add新增credit或扩大生产quota。目标RED复现且同方法字节GREEN，四项定向与四JVM有限图通过；初次627项整集5失败/2错误及fork超时保留；同码7项及完整SDK235项复验通过，另五类392项保持原通过，全部627用例身份匹配；style通过，RAT443原路径/原字节无新增，具体初次根因未证明，实际回执由canonical拥有。该证据不闭合真实峰值/状态数/native/RSS或B11；其余full-bytes/Main issuer/完整G1与16步仍OPEN，目标active、生产Gate OPEN、OFF/SYSTEM/PARK与disabled保持。


[点读元数据与 loan 资源退休](rfcs/unified-wal/implementation/local-storage/README.md#点读元数据与-loan-资源退休)：2026-10-28父c51f3ba9b5上，将 pooled reader/command/result/fresh Get 的已知元数据加入原service预留，无buffer LAC loan由原wire ResponseCredit另付；loan关闭清空三项资源，command在物理/owner双终态后退休payload。真实RED、定向/整类回执、初次cold-restart欠额与仅成功夹具同额补费、四JVM有限图及最终源码/class等价边界由canonical拥有。没有normal Add新增credit/atomic或生产quota扩容；此切片不闭合全部point-read调用者图、driver/执行器/pool、native/RSS或B11。Main具体issuer/受保护装载、完整G1及其余16步继续OPEN，目标active、生产Gate OPEN、OFF/SYSTEM/PARK和disabled能力保持。


[失败 DATA 前缀的 payload 引用退休](rfcs/unified-wal/implementation/local-storage/README.md#失败-data-前缀的-payload-引用退休)：2026-10-28父a08d1043d1上，actual completion 后同时摘除prefix与Hot的已终结batch/reservation根，保留未完成I/O身份、UNKNOWN坐标、原through与真实双门归还；owner失败但force成功仍UNKNOWN且无虚构quarantine。三条真实RED与同文件GREEN、相关Bookie/SDK/重启回执由canonical拥有。execution/pool已知图与终止退根、Input的U/36边界、opaque/native、Main issuer、完整G1和B11仍OPEN；完整目标active、生产Gate OPEN、OFF/SYSTEM/PARK及disabled保持。


[执行与池的固定 Java 容量和关闭退根](rfcs/unified-wal/implementation/local-storage/README.md#执行与池的固定-java-容量和关闭退根)：2026-10-28父c474431233上，第八项service DATA预留支付已知execution/owner/I/O/pool固定Java图及PREFER旧空容器重叠；成功native close/actual termination/input0后清多条内部执行别名，timeout/本次失败保持。旧路径两条RED、同文件定向/整类、四JVM边界与RefCnt补列的实际回执由canonical拥有。Input动态N=U/36、Thread/registry/Worker/AQS waiter、CPU/cleaner/native及完整B11仍OPEN；Main具体issuer/受保护装载、完整G1及其余16步继续推进，目标active、生产Gate OPEN、OFF/SYSTEM/PARK与disabled保持。


[Input 核心元数据与源引用退休](rfcs/unified-wal/implementation/local-storage/README.md#input-核心元数据与源引用退休)：2026-10-28父cab51e1c8e上，第九项service DATA预留按N=floor(U/36)支付已知Input/context/heap-root/RefCnt/NIO与claim工作区；成功release后清根并退款，Entry/bulk同步退休旧源引用。四条真实RED及同字节测试的13项GREEN由canonical记录；Driver描述符/解析图、native、Main issuer、完整G1和B11仍OPEN，持续目标active、生产Gate OPEN，OFF/SYSTEM/PARK与disabled保持。


[Driver 批次回调与重复请求引用退休](rfcs/unified-wal/implementation/local-storage/README.md#driver-批次回调与重复请求引用退休)：2026-10-28父2aadc71bb1上，首次deliver消费completion，duplicates/waiters在成功交接后清根；UNKNOWN与actual I/O双门、错误汇总保持。真实RED/GREEN和当前验证由canonical拥有；Driver完整容量、Main issuer、G1与B11仍OPEN，完整目标active、生产Gate OPEN，OFF/SYSTEM/PARK与disabled保持。


[Driver normal 与 recovery 的已知 Java 容量](rfcs/unified-wal/implementation/local-storage/README.md#driver-normal-与-recovery-的已知-java-容量)：2026-10-28父16933ee747上，第十项DATA预留支付源/preparation、独立Bookie/owner集合及整串返回窗口的已知Java图；waiter成功post后换空容器，旧九项金额保持。公式、实际验证及有限测量边界由canonical拥有；raw/compaction/JDK/native、Main issuer、G1和B11仍OPEN，目标active、生产Gate OPEN，OFF/SYSTEM/PARK与disabled保持。


[Released SDK 与当前 Classic Bookie 的互操作](rfcs/unified-wal/implementation/profile-wire/README.md#released-sdk-与当前-classic-bookie-的互操作)：四版released SDK对固定28e4c48138当前Classic完成真实create/write/close、独立重读及健康同roots重启后重读，12个SDK进程/两次server/JDI effect与实际排空由canonical记录。原665-vector不重跑、不移绑；此分支PASS_SCOPED，完整G1仍BLOCKED_UNVERIFIED，Main可信输入与B11仍OPEN，完整目标active、生产Gate OPEN，OFF/SYSTEM/PARK与disabled保持。


[Compaction 计划元数据与终态退休](rfcs/unified-wal/implementation/local-storage/README.md#compaction-计划元数据与终态退休)：2026-10-28父3b78f0084d上，运行时计划在checkpoint前动态预留DATA额度，两代plan/capture及当前COPY/MOVE/FREE按已知几何计费；实际owner终态退根后退款，CONTROL旧wrapper先清根再交付，旧常驻金额与quota不变。真实RED/GREEN及有限对象图边界由canonical记录；full-bytes/B11、Main issuer、完整G1继续OPEN，目标active、生产Gate OPEN，OFF/SYSTEM/PARK与disabled保持。


[Handler Pending 核心元数据](rfcs/unified-wal/implementation/local-storage/README.md#handler-pending-核心元数据)：2026-10-28父26ec583289上，原ResponseCredit在Pending/timeout创建前加计已知核心与回调壳，原CONTROL转移、终态门和quota不变。真实socket RED/GREEN与有限header witness由canonical拥有；decoder/codec/完整transport及raw入口、B11/Main issuer/完整G1仍OPEN，目标active、生产Gate OPEN，OFF/SYSTEM/PARK与disabled保持。


[接收 frame 元数据与 backing 终态](rfcs/unified-wal/implementation/local-storage/README.md#接收-frame-元数据与-backing-终态)：2026-10-28父5331415a09上，service在首个header字节前用原credit预留已知metadata，完整校验后grow backing；CONTROL整帧转移、weighted resize及最后view归还均沿用原总账。原通用pipeline重载及quota不变，真实RED/GREEN与有限对象图由canonical拥有；full-bytes/B11、Main issuer、完整G1与生产Gate仍OPEN，完整目标active，OFF/SYSTEM/PARK与disabled保持。


[worker 注册前的 socket 准入](rfcs/unified-wal/implementation/local-storage/README.md#worker-注册前的-socket-准入)：2026-10-28父f4df185c7c上，显式listener在child registration排队前取得原slot/base，超额连接在acceptor实际关闭；同一credit交给decoder，失败和close沿原owner实际排空。真实RED/GREEN及证据边界由canonical拥有；完整连接图/full-bytes/B11、Main issuer、完整G1与生产Gate仍OPEN，目标active，OFF/SYSTEM/PARK与disabled保持。


[实际 Profile client 对 released Classic 的拒绝](rfcs/unified-wal/implementation/profile-wire/README.md#实际-profile-client-对-released-classic-的拒绝)：2026-10-28父f66d2639fe上，两类公开client对四版真实stock共8次拒绝，零明文Profile/Classic回退、JDI零effect、storage/namespace不变且实际连接/credit排空；前后旧SDK控制及独立mTLS/HELLO-only控制有效。源码、失败诊断、45子进程终态及有限范围由canonical拥有；其他mixed/auth/route/startup/stable接受、完整G1/B11/Main issuer仍OPEN，目标active、生产Gate OPEN，OFF/SYSTEM/PARK与disabled保持。


[真实 mixed E 的公开 SDK 创建拒绝](rfcs/unified-wal/implementation/profile-wire/README.md#真实-mixed-e-的公开-sdk-创建拒绝)：2026-10-28父f134939c1b上，两个实际Profile peer与四版released Classic第三目标组成3/3/2选E，公开create均在fresh claim前返回NotEnoughBookies；旧连接/effect和双方持久状态变化均为零，pool实际排空。随后同fixture全Profile公开create/Add/close/只读成功，证据与失败诊断由canonical拥有。完整mixed/rolling/auth/stable G1、Main issuer和B11仍OPEN，目标active、生产Gate OPEN，OFF/SYSTEM/PARK与disabled保持。


[误指旧 endpoint 的真实 Profile hint 拒绝](rfcs/unified-wal/implementation/profile-wire/README.md#误指旧-endpoint-的真实-profile-hint-拒绝)：2026-10-28父d74303e8d5上，完整原生Profile发现记录为第三个当前身份指向四版旧Classic endpoint，公开create均在fresh claim前拒绝；旧端口实际CTRL/DATA连接退出且零Classic effect，双方持久状态不变、pool实际排空。原publisher公开demote并以新nonce修正endpoint后，五个同fixture公开create/Add/close/只读控制成功，含一版默认missing回归。原生产Java/Fixture/Oracle/observer/lock未改；源码/run及验证和清理由canonical拥有。完整G1、Main issuer/受保护装载、B11与其余16步仍OPEN，目标active、生产Gate OPEN，OFF/SYSTEM/PARK与disabled保持。


[公开 create 的第三目标 HELLO 拒绝](rfcs/unified-wal/implementation/profile-wire/README.md#公开-create-的第三目标-hello-拒绝)：2026-10-28父7f0ffdc6f3上，原真实SDK fixture的第三目标分别返回错误BookieId或缺UIC1控制capability，即使另两个目标满足AQ，公开create仍在fresh claim前拒绝，持久状态不变且pool实际排空；解除故障后同一请求完成三目标create/Add/close/只读。五项定向测试通过，生产Java未改，范围与原诊断由canonical拥有。完整G1、Main issuer/受保护装载、B11及其余16步仍OPEN，目标active、生产Gate OPEN，OFF/SYSTEM/PARK与disabled保持。


[Classic 与 Profile 的共同 route claim 接线缺口](rfcs/unified-wal/implementation/profile-wire/README.md#classic-与-profile-的共同-route-claim-接线缺口)：2026-10-28父628fae199b的源码复核确认，显式Profile模式与Classic栈分离，原Classic handle入口没有接Profile本地控制owner；独立TLS连接拒绝不证明RFC-0001 §7.1共同持久claim。首版边界已由指定规划会话冻结，实际Classic入口接线仍待完成；未增加legacy listener或双引擎模式；这项完整合同继续OPEN。本轮仅文档同步、未执行新运行时测试，完整G1/B11/Main issuer与生产Gate仍OPEN，目标active、OFF/SYSTEM/PARK及disabled保持。


[接收连接 base 与名额的实际终态](rfcs/unified-wal/implementation/local-storage/README.md#接收连接-base-与名额的实际终态)：2026-10-28父8a5c302b57上，原receive预算的base与slot延后到最后实际frame/backing/workspace归还；真实retained-view RED/GREEN、三类43项及未修改SDK整类241项通过，原backing模型1正向/8指定反例符合预期。初次失败、源码/receipt与完整边界由canonical拥有。完整连接价格、full-bytes/B11、Main issuer、共同route claim/完整G1及其余16步仍OPEN，目标active、生产Gate OPEN，OFF/SYSTEM/PARK与disabled保持。


[注册前预付已知连接元数据](rfcs/unified-wal/implementation/local-storage/README.md#注册前预付已知连接元数据)：2026-10-28父ebce998bf1上，显式listener与直接storage入口在注册/应用构造前用原base支付已知连接对象及pending table高水位；共用一份credit并等实际drain。真实RED、六项定向、四整类285项及后补直接入口1项分别留存原源码绑定，四JVM有限浅大小/高水位样本低于列明费用。完整TLS/Netty/JDK/native图与峰值、full-bytes/B11、Main issuer、共同route claim/完整G1和其余16步仍OPEN，目标active、生产Gate OPEN，OFF/SYSTEM/PARK与disabled保持；范围及证据由canonical拥有。


[公开只读的逐台健康重启与旧发现拒绝](rfcs/unified-wal/implementation/profile-wire/README.md#公开只读的逐台健康重启与旧发现拒绝)：2026-10-28父982a18dad9上，三台真实Profile服务在原endpoint/identity逐台健康关闭重开，旧observation拒绝、新HELLO generation2及原entry读取有效，同一公开SDK/pool重新open/read并等双方实际drain，metadata版本不变。首轮四项与最终强化单项分别通过，生产Java未改，源码/run绑定及有限范围由canonical拥有。进程crash/跨版本rolling、Main issuer、共同route claim/完整G1、B11与其余16步仍OPEN，目标active、生产Gate OPEN，OFF/SYSTEM/PARK及disabled保持。


[启动前沿用既有磁盘阈值](rfcs/unified-wal/implementation/startup-readiness/README.md#启动前沿用既有磁盘阈值)：2026-10-28父9cfe97c910上，显式Profile工厂冻结原DiskChecker hard/warn配置，可信root核对后、storage打开前检查已存在目录；hard拒绝、warn仅告警。只读入口避免恢复时创建缺失目录，Classic checkDir顺序保持；两条真实RED及最终9项定向通过，阶段源码/输入由canonical拥有。持续filesystem/维护预算、B17/B11、多Arena、Main issuer与完整16步仍OPEN，目标active、生产Gate OPEN，OFF/SYSTEM/PARK及disabled保持。


[固定 Arena manifest 与显式 v2 身份基础](rfcs/unified-wal/implementation/local-storage/README.md#固定-arena-manifest-与显式-v2-身份基础)：2026-10-30按指定规划裁决接完整固定拓扑、独立Bookie控制根及v2角色/共同identity，v1字节保持；实际多Arena运行/恢复/维护尚未接线，单Arena服务显式拒绝v2部分图。源码、原失败/父反例、定向补验和独立parser证据由canonical拥有，style通过、RAT433既有路径无新增，不称CI全绿。Main受控issuer/loader和共同Classic/Profile route claim已裁定待实现，完整16步目标active，生产Gate OPEN，OFF/SYSTEM/PARK及disabled保持。


[独立 Bookie owner 与严格 Arena 成员接线](rfcs/unified-wal/implementation/local-storage/README.md#独立-bookie-owner-与严格-arena-成员接线)：2026-10-30父f59731767a上解耦Bookie control根并绑定完整manifest，per-Arena原生文件/allocator/index验证固定shard；实际两Arena写读、健康重开、index互换/丢失重建与独立FREE由同一Bookie owner驱动，七类409项同源码通过。实际style/RAT及阶段边界由canonical拥有；service v2发布、共同Classic route、Main及全局预算仍OPEN，完整目标active、生产Gate OPEN，OFF/SYSTEM/PARK与disabled保持。


[共同 ROUTE 的持久命令与恢复](rfcs/unified-wal/implementation/local-storage/README.md#共同-route-的持久命令与恢复)：2026-10-30父8269cd0328上新增native真实ROUTE/64-byte body、schema2完整Classic根角色及同owner混合batch、operation index/checkpoint/rotation恢复；最终八类420项同源码通过，style exit0、RAT432父路径无新增，有限浅大小与所有早期诊断由canonical拥有。BookieImpl异步入口、markers/native commissioning binding与独占导入尚未接线；完整G1/Main/full-bytes与16步总验收仍OPEN，目标active，默认OFF/SYSTEM/PARK和disabled保持。


[共同启用的 native 绑定、根标记与启动拒绝](rfcs/unified-wal/implementation/local-storage/README.md#共同启用的-native-绑定根标记与启动拒绝)：2026-10-30父4c02a7378b上，真实ZK固定namespace的PREPARED/ENABLED、原Cookie/native出生条件及实际根marker子组件已实现，Classic/Profile启动前冷读拒绝不允许OFF/unknown/null绕过；普通未绑定ZK Classic保留原路径。Profile manager-only冷读不获得mutation能力，旧driver二进制可装载与unsupported运行时拒绝分开说明。实际验证与source/receipt由canonical拥有。完整共同owner装配、Classic导入/replay/flush/异步claim、fresh-disposable Null上下文、Main/G1/full-bytes和16步总验收继续OPEN，目标active，默认OFF/SYSTEM/PARK及disabled保持。


[fresh Classic 根租约与重用拒绝](rfcs/unified-wal/implementation/local-storage/README.md#fresh-classic-根租约与重用拒绝)：2026-10-31父a9a06410d5上，完整空根的真实独占租约、持久一次使用标记与物理身份复核已接线到根资源子组件，Classic/Profile冷启动不绕过标记；五类29项及style通过，原失败/默认目录恢复及有限证据由canonical拥有。稳定fresh外层owner/permit/逐资源初始化与实际drain仍未实现，不增加EmbeddedServer fresh。共同owner/Main/G1/full-bytes及完整16步保持OPEN，目标active，OFF/SYSTEM/PARK和disabled保持。


[fresh 执行器的实际终结与严格 flush](rfcs/unified-wal/implementation/local-storage/README.md#fresh-执行器的实际终结与严格-flush)：2026-10-31父03835ed598上，Sync严格flush/终结后重试、monitor及state manager的真实executor termination与自等待拒绝已实现；四类26项通过，阶段源码/回执与有限边界由canonical拥有。fresh外层owner/窄facade、逐资源登记及Journal/GC/预分配/文件关闭仍未实现，不发行许可或提前解锁；共同owner/Main/G1/full-bytes及完整目标保持OPEN，目标active，OFF/SYSTEM/PARK与disabled保持。

[fresh GC 的删除权威与实际终结](rfcs/unified-wal/implementation/local-storage/README.md#fresh-gc-的删除权威与实际终结)：2026-10-31父6b47e4de8e上，fresh GC固定无集群删除authority，保留本地metadata/compaction/预分配；原executor及真实线程退出后才清理map，中断重试和自等待拒绝不提前退役资源。五类30项通过，独立旧GC轮次5通过/1因macOS JNI缺失报错；原失败与配置归一化边界由canonical拥有。完整fresh owner/facade、Journal/allocator/文件cache关闭和共同owner/Main/G1/full-bytes仍OPEN，不发行许可或提前解锁；16步目标active，OFF/SYSTEM/PARK与disabled保持。


[fresh 预分配资源与固定坐标 flush](rfcs/unified-wal/implementation/local-storage/README.md#fresh-预分配资源与固定坐标-flush)：2026-10-31父41aba209ca上，fresh buffer保留native close的sticky结果与单次自有引用释放，部分flush使用冻结范围/固定坐标重试；allocator登记open/构造/header/lastId及未交付候选，停止新提交后等真实任务/线程退出再清理，结果分别保留allocation failure与close failure。七类81项与style通过，原磁盘满夹具错误及17项阶段回执由canonical拥有；夹具改为真实本地Interleaved，未放宽Null启动。DefaultEntryLogger完整图、Journal/cache、根解锁严格结果和稳定owner/facade仍待接线，不授予许可或全Bookie排空证明；完整目标active，相关Gate OPEN。


[fresh entry logger 的实际资源图关闭](rfcs/unified-wal/implementation/local-storage/README.md#fresh-entry-logger-的实际资源图关闭)：2026-10-31父babf7cc4e2上，包内DefaultEntryLogger先登记owner再初始化，实际active/candidate/rotation/compaction及读fd的winner/loser/partial均保留关闭结果；弱heap alias不强留，活引用各释放一次，命中CLOSING拒绝，未知首次close仍sticky。结果区分资源drained、operation/flush/close失败，framing buffer按owner回收，独立caller ByteBuf保持。八类95项通过，原阶段/格式失败及交付输入由canonical拥有；不是全Bookie、根解锁或ACK验收。下一项继续Interleaved/index cache、Journal、根释放进度及完整owner/facade，目标active，相关Gate OPEN。
