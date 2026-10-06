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
