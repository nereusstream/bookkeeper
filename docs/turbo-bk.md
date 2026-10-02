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

Round 7 原始 wire candidate `0x0FFE4250` 已被真实 Apache BookKeeper 4.14.8 decoder 的 `magic-flip-1` 反例否证并冻结；当前 replacement candidate 为 `0x0FF04250`。它继续使用TLS后4-byte length + 32-byte header、byte-exact HELLO、distinct control/normal/recovery/read/LAC subtypes和12类status（1 OK + 11 non-OK）；range/batch在当前manifest中reserved/disabled。新 candidate 仍只授权reference codec与raw corpus，不是stable wire：每个受支持old decoder/binary对TLS ClientHello及合法/损坏/截断/fuzz bytes的Classic route/handle/master-key/allocation/journal/payload/ACK effect都必须为0，任何失败不得Classic fallback或双写。当前 Wave 0 不执行这层 released-decoder/stock-binary 矩阵，状态为`DEFERRED_NOT_RUN`而不是PASS/waiver，G1保持`BLOCKED_UNVERIFIED`。

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

| 阶段 | 步骤与主线 | 完成边界 |
| --- | --- | --- |
| 基础与执行模型 | 1 基线/测试入口；2 CLIENT-1/2；3 实验格式与文件I/O；4 固定owner、安全binding与NUMA池 | 可复用的隔离入口；OFF/SYSTEM/PARK基线；基础ownership和预算 |
| 单机可恢复存储 | 5 本地控制日志/启动检查/allocator → 6 普通Add/合批持久化/幂等 → 7 点读/restart | 第一里程碑：真实写入—点读—重启恢复；仅报告实际覆盖的局部性能 |
| 索引与回收 | 8 checkpoint/rotation/异步索引/全量重建 → 9 本地tombstone/compaction/FREE/reuse → 10 空间耗尽/长期负载/多Arena | 第二里程碑：有界、可长期运行的本地存储；尚未证明分布式quorum和兼容 |
| 协议与集群 | 11 Profile transport/接收背压 → 12 MetadataStore/install/activation/registration → 13 ACK/retry/replacement → 14 recovery/AutoRecovery → 15 集群删除/离线追赶/rejoin | 第三里程碑：创建、写、读、换组、恢复、关闭、删除、重返完整闭环；仍待综合验收 |
| 完整验收与灰度 | 16 全链路故障、模型、性能、兼容、运维与独立小cohort canary | 实际启用能力证据闭合后，才提升对应RFC/feature状态和部署范围 |

步骤2不阻塞本地文件原型，须在步骤13前完成；步骤3、4可并行，步骤11、12在接口稳定后可提前并行开发，真实集群成功路径联调等待本地存储通过对应验证。Block H与Block G实际启用A/C、必要D及组合并行，模型不能证明真实fsync/TLS/资源，性能run也不能替代quorum安全证据。测试、故障注入和测量随每个模块交付，步骤16负责组合验收。

DirectJournal是独立对照/优化支线，可并行或等主线取得基础数据后开展，不是Segment开工前置。它保留Journal＋LedgerStorage结构，不宣称一份payload持久化，不替代Segment安全证据。首个完整WAL之后再按需求或测量推进dedicated extent、其他I/O backend、高级Range、rack/AZ、强撤权、strong reset、在线删除及跨Arena迁移；无证据时保持disabled，详见[延期能力及前置条件](rfcs/unified-wal/implementation/README.md#directjournal与延期能力)。

可保留数据canary前必须完成启用范围的基础恢复、普通删除、全量重建、维护进展及资源Gate，并接受对应RFC与Spike证据。Round 7 test manifest和isolated shadow不授予stable wire/disk、live shadow、Segment authority或production readiness；live shadow仍须证明不污染Classic rollback cohort。raw old-decoder/stock-binary矩阵当前保持DEFERRED_NOT_RUN，但发布对应stable面前必须恢复执行并通过；所选control-store物理协议同样须验证，不能以永久延期宣称兼容。首个new-scope canary不额外要求原地same-scope迁移；退出时停止新写并保留新引擎读取/恢复能力，不能让旧binary打开Segment文件。

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
