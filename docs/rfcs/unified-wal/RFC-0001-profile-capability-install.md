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
- 每个新 Profile Add 都携带 `ledgerInstanceId + profileDescriptorHash`；
- 未安装、instance/hash/engine 不匹配全部 fail closed；
- replacement Bookie 的 durable install 先于 ensemble metadata CAS。

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
- master key 的现有安全模型重设计。

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

逻辑 schema 至少包含：

```text
ProfileDescriptor {
    ledgerId
    ledgerInstanceId
    profileVersion

    requiredEngineProfile
    requiredCapabilities[]

    payloadFormat
    durabilityMode
    quorumProfile
    indexProfile
    sequenceProfile
    deleteProfile

    maxEntrySize
    maxInflightEntries
    maxInflightBytes

    installRequestId
    descriptorHash
    masterKeyMaterialOrReference
}
```

约束：

- `ledgerInstanceId` 在 ledgerId 重用或重建时必须变化；
- `descriptorHash` 覆盖除自身外的全部规范字段；
- 字段序列化、默认值和未知字段处理必须 canonical，禁止不同实现得到不同 hash；
- mandatory capability 未识别时拒绝；optional hint 可以忽略，但不得改变 durability；
- `masterKeyMaterialOrReference` 的具体编码必须服从现有认证模型，不能把明文凭据写入普通日志或诊断输出。

规范编码、hash 算法和敏感字段保护是 RFC 接受前必须关闭的开放项。

## 6. 创建状态机

```text
PREPARING
    │ durable metadata contains full descriptor and initial ensemble
    ▼
INSTALLING
    │ install to every Bookie in current ensemble
    ▼
INSTALLED
    │ all E durable receipts recorded or provably referenced
    ▼
OPEN
```

### 6.1 PREPARING

创建方通过 MetadataStore 写入完整 descriptor、initial ensemble 和 creation request identity。该状态：

- 不允许正常 open；
- 不允许 Add；
- 不对外声明 ledger 可用；
- 同一个 creation request 重试必须解析到同一 instance/descriptor，冲突请求失败。

### 6.2 INSTALLING

协调方对 initial ensemble 的全部 E 个 Bookie发送 `INSTALL_LEDGER_PROFILE`。不能只安装本次 write set 中的 W 个节点，因为一般 `E > W` 会轮转 write set。

部分成功或 response loss 时维持 INSTALLING，并按相同 `installRequestId` 幂等重试。不能回退为 Classic lazy-create。

### 6.3 INSTALLED

只有全部 E 个节点返回可验证的 durable receipt 后才能进入 INSTALLED。receipt 至少绑定：

```text
bookieId
ledgerId
ledgerInstanceId
descriptorHash
installRequestId
bookieEngineProfile
bookieCapabilityDigest
localInstallGeneration
```

receipt 的存放位置和是否内嵌 metadata 是开放项，但必须能审计 `OPEN` CAS 的前置证据。

### 6.4 OPEN

协调方以 metadata version CAS 从 INSTALLED 发布 OPEN。CAS 失败时不得开始 Add；必须重新读取 authority 并判断是幂等完成、并发修改还是创建失败。

普通 Add 不要求每次远程读取 MetadataStore。正确性来自已发布的 OPEN 状态、Add 身份字段与 Bookie durable install；watch/cache 只用于性能和提前失败。

## 7. Bookie 安装语义

候选请求：

```text
INSTALL_LEDGER_PROFILE {
    descriptor
    installRequestId
    expectedBookieEngineProfile
}
```

Bookie 必须按顺序完成：

1. 验证请求大小、版本和认证；
2. 验证 active Engine Profile；
3. 验证全部 mandatory capability；
4. 检查 ledgerId、instance 和 descriptorHash 冲突；
5. durable 写入 `LEDGER_PROFILE_INSTALL` 控制记录；
6. durable 安装 master key 或其受保护形式；
7. 建立可从控制记录恢复的 routing entry；
8. 返回 durable receipt。

仅在第 5-7 步的 durability barrier 完成后返回成功。内存对象创建不能替代 durable install。

### 7.1 幂等与冲突

| 已有状态 | 请求 | 结果 |
| --- | --- | --- |
| 无 install | 合法 descriptor | durable install |
| 相同 instance/hash/request | 重试 | 返回相同语义 receipt |
| 相同 instance/hash，不同 request | 等价重试 | 允许，receipt 绑定原 install generation |
| 相同 instance，不同 hash | 冲突 | `EPROFILE_MISMATCH` |
| 相同 ledgerId，不同 live instance | 冲突 | `ELEDGERINSTANCE_MISMATCH` |
| engine 不匹配 | 冲突 | `EUNSUPPORTED_STORAGE_ENGINE` |
| mandatory capability 缺失 | 冲突 | `EUNSUPPORTED_LEDGER_CAPABILITY` |

冲突不能通过删除本地状态或退回 Classic 自动修复。

## 8. AddRequest 身份

所有需要安装的新 Profile Add 至少携带：

```text
ledgerId
ledgerInstanceId
profileDescriptorHash
entryId
masterKey/authentication data
body
writeFlags
```

Bookie 校验顺序：

```text
install missing             -> EPROFILE_NOT_INSTALLED
ledger instance mismatch    -> ELEDGERINSTANCE_MISMATCH
descriptor hash mismatch    -> EPROFILE_MISMATCH
engine mismatch             -> EUNSUPPORTED_STORAGE_ENGINE
then existing fencing/auth/entry validation
```

新 Profile 请求缺失 identity 字段时必须拒绝。Classic 请求仍走现有兼容路径，但两类请求必须在协议层可无歧义区分。

## 9. Ensemble change

replacement 的唯一合法顺序：

```text
1. select Bookie satisfying engine and capabilities
2. INSTALL_LEDGER_PROFILE on replacement
3. wait for durable install receipt
4. copy/recover data according to existing recovery contract
5. CAS ensemble metadata
6. allow write sets to address replacement
```

第 4、5 步与现有 recovery 的精确相对次序需结合 BookKeeper ensemble-change 流程评审，但以下关系不可改变：replacement install 必须先于任何 metadata 或 write set 使其成为 active member。

如果 install 失败：

- replacement 不进入 ensemble；
- 选择其他满足条件的节点或使操作失败；
- 不降低 capability；
- 不把首次 Add 当作安装触发器。

## 10. Restart 与 orphan install

Bookie restart 必须从 durable control record 恢复 install、instance、hash、master key 状态和 routing，再注册为可接收相应 Profile 的节点。

metadata OPEN CAS 失败可能留下 orphan install。候选处理：

- orphan install 不允许接收 Add，因为 metadata 从未发布 OPEN；
- install 状态不得仅凭超时删除；
- GC 必须读取权威 metadata，并使用 instance/hash 与稳定 grace/window 证明该 install 永远不会发布；
- GC 本身需要 durable tombstone，防止 response loss 后旧请求重新激活。

orphan GC 的完整状态机是本 RFC 接受前的开放项，也是 Spike A 的必测场景。

## 11. Mixed-version 兼容

### 11.1 新 client + 旧 Bookie

仅 `BK_SEQUENCED_CLASSIC_CLIENT_ONLY` 可以使用旧 Bookie，并且 sequence envelope 只作为 opaque payload。它不发送 install 请求，也不能要求 server-side hash、epoch、index 或 tail-summary 语义。

### 11.2 新 Profile + 不支持 install 的 Bookie

placement 阶段排除；若仍被选中则创建失败。不得静默降级。

### 11.3 旧 client + 新 Bookie

旧 Classic 请求维持现有行为；新 Bookie 不能把旧请求误判为已安装的新 Profile。

## 12. Capability negotiation

Bookie registration 至少发布：

```text
engineProfile
protocolVersions[]
mandatoryCapabilities[]
limitsDigest or bounded limits
```

placement 只把满足 descriptor mandatory requirements 的 Bookie 作为候选。注册信息是选择输入，不是 install 完成证据；最终仍以目标 Bookie durable receipt 为准。

## 13. 安全不变量

1. `OPEN => all current ensemble Bookies durably installed the same instance/hash`。
2. `ACK(new-profile Add) => matching durable install existed before Add processing`。
3. 相同 ledgerId 的两个 live instance 不得共享同一 Bookie routing identity。
4. descriptor 冲突不能被重试、restart 或 lazy-create 消解为成功。
5. ensemble metadata 不能先于 replacement durable install 引用 replacement。
6. Bookie restart 后的接受集合不能大于 crash 前由 durable install 授权的集合。
7. metadata watch/cache 失效不能破坏上述不变量。

## 14. Spike 与接受 Gate

RFC 进入 Accepted 前必须：

- [Spike A](spikes/SPIKE-A-profile-install.md) 全场景通过；
- Model A 包含创建、安装、response loss 与 ensemble replacement 的抽象；
- canonical descriptor/hash 与未知字段规则冻结；
- orphan install 回收合同冻结；
- protocol error mapping 和 mixed-version matrix 有确定测试；
- 代码改造位置经过独立源码评审，确认不存在绕过 install 的新 Profile Add 路径。

任一场景出现未安装 Add 被接受、mismatch 静默降级或 metadata 先于 replacement install 生效，RFC 保持 P0 Blocked。

## 15. 开放问题

- canonical serialization、hash 算法和 descriptor version negotiation；
- metadata 中 PREPARING/INSTALLING/INSTALLED/OPEN 的具体 schema；
- receipt 的持久化位置、压缩和审计方式；
- install control record 与现有 master-key persistence 的整合；
- create cancellation、orphan install tombstone 与 GC；
- Bookie registration capability 的刷新与降级行为；
- ensemble change 中 install、data recovery 与 CAS 的完整线性化点；
- ledgerId reuse 是否允许，以及 instance 分配 authority；
- limits 变化是新 descriptor version 还是 runtime admission policy。

这些问题关闭并通过 Gate 前，本 RFC 不得标为 Implementation Ready。
