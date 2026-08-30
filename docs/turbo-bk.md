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

本文不冻结尚未由 RFC 接受或 Spike 验证的 wire format、磁盘格式、阈值和实现类。

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
- 当前 [`BookieImpl`](../bookkeeper-server/src/main/java/org/apache/bookkeeper/bookie/BookieImpl.java) 将多个 ledger 路由到进程内 Journal。初版 `DirectJournal` 因而只能作为 Bookie/cohort 级引擎部署。
- 仅在 Ledger Metadata 增加 `storage_format` 不能让 Bookie 获得 fail-closed 的 per-ledger 路由合同。

### 4.2 Batch Read

- 当前 [`BookkeeperProtocol.proto`](../bookkeeper-proto/src/main/proto/BookkeeperProtocol.proto) 已定义 `BATCH_READ_ENTRY`；Range Add/Read 仍标为未支持。
- [`ReadHandle`](../bookkeeper-server/src/main/java/org/apache/bookkeeper/client/api/ReadHandle.java) 已公开 bounded batch read；[`LedgerHandle`](../bookkeeper-server/src/main/java/org/apache/bookkeeper/client/LedgerHandle.java) 当前只在 `E = W` 且能力启用时走 batch fast path，否则回退。
- 新设计不得再把 Batch Read 描述为“完全不存在”。

### 4.3 Deferred Sync

- [`LedgerHandle`](../bookkeeper-server/src/main/java/org/apache/bookkeeper/client/LedgerHandle.java) 中的 `DEFERRED_SYNC_LEGACY` 是现有能力，但 failed Bookie 场景下不能按一般 HA WAL 假设执行正常 ensemble change。
- 第一版 Sequenced WAL 只允许 `SYNC_ON_ACK`。
- 如果将来需要可换 ensemble 的 deferred durability，必须另立 `DEFERRED_SYNC_V2` 合同。

## 5. 三层 Profile 模型

### 5.1 Bookie Engine Profile

由 Bookie 进程配置和部署 cohort 决定：

```text
CLASSIC_ENGINE
DIRECT_JOURNAL_ENGINE
SEGMENT_WAL_ENGINE
```

初版使用独立 cohort，不支持一个 Bookie 进程内按 ledger 动态混用 Classic Journal、Direct Journal 和 Segment WAL。

### 5.2 Ledger Contract Profile

定义 ledger 在已选 Engine 上要求的持久化合同：

```text
OPAQUE_LEDGER_V1
SEQUENCED_LEDGER_V1
SYNC_ON_ACK
DEFERRED_SYNC_LEGACY
TAIL_SUMMARY_V1
CONDITIONAL_DELETE_V1
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
| `BK_DIRECT_JOURNAL` | `DIRECT_JOURNAL_ENGINE` | 首版仍为 opaque ledger | opaque | `SYNC_ON_ACK` |
| `BK_SEQUENCED_CLASSIC_CLIENT_ONLY` | `CLASSIC_ENGINE` | 仍为 Classic opaque payload | sequence、appendId、ordered frontier | `SYNC_ON_ACK` |
| `BK_SEQUENCED_CLASSIC_INSTALLED` | `CLASSIC_ENGINE` | 显式安装的 sequence capabilities | sequence、appendId、ordered frontier | `SYNC_ON_ACK` |
| `BK_SEGMENT_WAL` | `SEGMENT_WAL_ENGINE` | 显式安装的 Segment contract | opaque | `SYNC_ON_ACK` |
| `BK_SEQUENCED_SEGMENT_WAL` | `SEGMENT_WAL_ENGINE` | Segment + installed sequence capabilities | sequence、appendId、ordered frontier | `SYNC_ON_ACK` |

### 5.5 非法或首版禁止组合

- 在 Classic cohort 中按单 ledger 切换 DirectJournal；
- Segment ledger 未完成 RFC-0001 install 即接受 Add；
- client-only Profile 声称具有 Bookie epoch/hash/index 能力；
- `DEFERRED_SYNC_LEGACY` 与首版 Sequenced WAL 自动 failover；
- 旧 Bookie 上的 same-ledger epoch takeover；
- 在独立 fencing 合同前启用 pooled ledger lane。

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
| `DEFERRED_SYNC_LEGACY` | Existing with HA Restriction | 不作为首版 WAL 默认 |
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
            ├── ArenaControlLog      allocator authority
            ├── Data Arena          payload authority
            └── Derived Index       rebuildable acceleration

Cluster Lifecycle
    ├── AutoRecovery + durable RepairIntent
    └── LedgerDeleteCoordinator
```

权威层次必须保持：

- Standard LedgerMetadata：OSS state 与 ensemble membership 的唯一权威。
- Profile sidecar namespace：ledger instance、immutable descriptor、READY/activation generation 和有界 repair/delete operation 的权威；不复制 membership，不形成无界 root blob。
- BookKeeper quorum：entry 是否达到 ACK quorum 的权威。
- `ArenaControlLog`：Segment 本地空间 ownership、generation 和 reuse 的权威。
- Data Arena：Segment payload 的权威。
- RFC-0005 Segment Bookie state：install/activation/fence/recovery authorization 被消费后 local success 是否有资格参与 AQ 的权威。
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
13. 删除、RepairIntent 和 ensemble change 对同一 ledger instance 的权威状态转换必须由标准 membership CAS 与 sidecar control generation 协同串行，任一中间态 fail closed。
14. permanent-loss 保证只在声明的 distinct failure-domain 预算与 repair window 内成立；无有效 evidence 时 recovery 永不返回成功。
15. derived index 全部删除后，系统仍可从权威 payload 和控制元数据恢复。

任何子 RFC 或 Spike 发现这些不变量不可同时满足，都必须停止相应路径，而不是降低不变量。

## 9. BtrLog 证据边界

可借鉴：single-writer、client-driven quorum、bounded window、thread affinity、direct NVMe 和 tail recovery 的思路。

不可继承：依赖 durable blob store 的 flush、repair、recovery 路径及其已经验证的安全结论。

本方案至少需要五个相互可组合但独立检查的模型：

| 模型 | 范围 |
| --- | --- |
| Model A | BookKeeper E/W/A、write-set rotation、install/activation、legacy routing、AQ、LAC、fencing、ensemble change、failure-domain loss |
| Model B | sequence、appendId、ordered completion、old-ledger fencing、successor ledger |
| Model C | `ALLOC/DATA/DELETE/FREE`、generation、checkpoint rotation、power loss |
| Model D | historical ensembles、partial delete、offline Bookie、rejoin、AutoRecovery race |
| Model E | TailSummary 与一般 E/W/A recovery；仅在推进该能力时必需 |

Model A-D 未通过前，Segment authority 不得进入 production candidate。

## 10. 实施顺序

```text
Stage 0  总体文档降级；建立五份子 RFC、三个 Spike 与逐轮 grill 记录
Stage 1  BK_CLASSIC / BK_CLASSIC_TUNED 可复现基线
Stage 2  Spike A：Profile install
         Spike B：Allocator/block
         Spike C：No-object TLA+
Stage 3  接受 RFC-0001/0002；实现 Sequenced Classic client-only
Stage 4  DirectJournal 独立 cohort prototype；不声称一次本地 payload 写
Stage 5  接受 RFC-0003；Segment shadow writer，Classic 仍为 ACK authority
Stage 6  接受 RFC-0005；完成 normal Add/fence local authority 的必要合同；具体 Segment authority canary 仍受 canary-specific evidence 与已启用路径依赖约束，不含 recovery/delete 集成
Stage 7  接受 RFC-0004；Streaming Range / Recovery / Delete
Stage 8  Sequenced Segment WAL、derived index、production canary
```

禁止跨越依赖：shadow write 的完成不等于 Segment authority，Segment authority 的完成不等于 Sequenced Segment WAL 或 production readiness。

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

- ProfileDescriptor 的规范序列化、hash 算法、版本升级和 master key 处理；
- sidecar exact root/child schema、immutable backlink encoding、ledgerId reuse 和 profiled metadata mutation authority；
- activation/recovery-only proof、local role mapping、receipt packing 与失败清理；
- profiled wire opcode、atomic Classic/Profile route claim 的实现与 exact errors；
- successor ledger 的 publication record、owner 状态和跨 run continuity；
- appendId suppressed-suffix/horizon、durable seal/footer authority；
- `ArenaControlLog` segment、checkpoint A/B、superblock 切换与设备失败判定；
- RFC-0005 exact durable state packing、Engine identity 与 operation capability matrix；
- shared slab 的 lifetime class、compaction policy 和 hot promotion threshold；
- RepairIntent child enumeration/retention/orphan cleanup，以及 Delete Coordinator 的存储布局、decommission 证明、tombstone retention 和 rejoin watermark；
- streaming range 在一般 E/W/A 下的结果合并与 recovery-specific semantics；
- 每个正式 Gate 的基准 commit、硬件和完整 manifest。

开放问题存在时，对应章节只能标为 Proposed、Spike Ready 或 Blocked，不能标为 Implementation Ready。

## 13. 当前可执行工作

现在可以直接开始：

- 固定 stock `BK_CLASSIC` benchmark；
- 对 `BK_CLASSIC_TUNED` 做不改变 on-disk/wire semantics 的调优；
- 继续逐轮评审五份子 RFC；
- 在锁定环境和停止条件后执行三个 Spike。

现在不能直接开始：

- Segment WAL 正式实现或 authority 切换；
- 把 per-ledger minimum 8 MiB extent 当作不变量；
- 在旧 Bookie 上实现 same-ledger epoch takeover；
- 将本地 delete success 当作全物理删除完成；
- 把 BtrLog 模型结果当作本方案的形式化证明；
- 将 `DEFERRED_SYNC_LEGACY` 与首版 Sequenced WAL 自动 failover 组合。

本文的下一次状态提升，必须引用被接受的子 RFC、Spike immutable artifacts 和完整 Gate receipts。
