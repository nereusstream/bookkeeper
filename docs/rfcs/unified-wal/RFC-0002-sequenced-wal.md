# RFC-0002：Sequenced WAL、Stale Writer 与 Successor Ledger

> 状态：**Proposed / RFC Required**<br>
> 依赖：BookKeeper 原生 quorum fencing、recovery 与 ledger metadata CAS<br>
> 安装版依赖：[RFC-0001](RFC-0001-profile-capability-install.md)<br>
> 形式化验证：Model B，无对象存储

## 1. 摘要

本 RFC 定义 BookKeeper entry 坐标之上的 sequence 合同，并把原 `BK_SEQUENCED_CLASSIC` 拆成两个能力边界不同的 Profile：

```text
BK_SEQUENCED_CLASSIC_CLIENT_ONLY
BK_SEQUENCED_CLASSIC_INSTALLED
```

首版最关键的约束是：旧 Bookie 不理解 `writerEpoch`，所以 client-only Profile 禁止 same-ledger epoch takeover。新 writer 必须 fence 并恢复旧 ledger，seal predecessor，然后在 successor ledger 上继续写。

## 2. 范围

本 RFC 负责：

- `SequenceDomain`、sequence reservation 与 run；
- entryId、WalSequence 和协议原生位置的分离；
- `appendId` 和 outcome-unknown 解析；
- concurrent I/O 与 ordered durable frontier；
- old-ledger fencing、recovery、seal 与 successor publication；
- client-only 与 installed Profile 的能力边界；
- RocksDB/控制 entry 作为 derived sequence index 的边界。

本 RFC 不负责：

- Profile 显式安装 wire contract，见 [RFC-0001](RFC-0001-profile-capability-install.md)；
- Segment allocator 或磁盘格式，见 [RFC-0003](RFC-0003-segment-storage-allocator.md)；
- general E/W/A range recovery 和 cluster delete，见 [RFC-0004](RFC-0004-range-recovery-delete.md)；
- 上层协议的事务、消费进度或可见性。

## 3. Profile 边界

### 3.1 `BK_SEQUENCED_CLASSIC_CLIENT_ONLY`

部署组合：

```text
new client
old or new Bookie
Classic Journal + EntryLog
sequence envelope stored as opaque payload
SYNC_ON_ACK
```

允许：

- client-side SequenceDomain；
- bounded sequence range reservation；
- `appendId`；
- concurrent Add 与 ordered completion；
- run header/footer 或 ledger 内 range-index control entry；
- dual-coordinate receipt；
- client-side recovery scan。

不允许宣称：

- Bookie 按 `writerEpoch` 拒绝 stale writer；
- same-ledger epoch takeover；
- server-side sequence conflict detection；
- Bookie read-by-sequence pushdown；
- Bookie sequence TailSummary；
- Profile hash 校验。

这里的“兼容”只表示旧 Bookie 可以保存 opaque payload，不表示它执行新的 sequence 或 fencing 合同。

### 3.2 `BK_SEQUENCED_CLASSIC_INSTALLED`

要求新 Bookie 支持 RFC-0001 的 install、`ledgerInstanceId` 和 descriptor hash。它仍使用 Classic engine，但可按独立 capability 增加：

- sequence envelope validation；
- server-side duplicate/conflict detection；
- derived sequence index pushdown；
- sequence TailSummary。

这些能力必须逐项声明，不能由 Profile 名称整体推断。

## 4. 坐标与身份

三个坐标必须分离：

```text
entryId            BookKeeper ledger 内物理/协议坐标
WalSequence        SequenceDomain 内逻辑顺序
protocolPosition   Kafka/Pulsar/DB 等上层坐标
```

写入回执至少表达：

```text
WalAppendReceipt {
    ledgerId
    ledgerInstanceId
    entryId
    sequenceStart
    sequenceEnd
    appendId
    durability
}
```

一个 entry 可以承载一条或多条逻辑记录，但 envelope 必须使 sequence interval、payload 边界和 checksum 可无歧义验证。

## 5. SequenceDomain 与 run

`SequenceDomain` 是上层选择的连续顺序空间。一个 domain 可以跨多个 BookKeeper ledger，但每个 ledger 只承载一个明确的 run。

候选 run identity：

```text
SequenceRun {
    sequenceDomainId
    runId
    writerEpoch
    ledgerId
    ledgerInstanceId
    predecessorRunId
    reservedStart
    state
}
```

候选状态：

```text
PREPARING -> ACTIVE -> FENCING -> RECOVERING -> SEALED
                                         └-> FAILED
```

状态 schema 和 authority 存放位置是开放项；但 ACTIVE successor 的发布必须晚于 predecessor fencing、recovery 和 seal。

## 6. Sequence reservation

sequence 必须在发送 Add 前分配，以便 envelope、appendId 和 ordered completion 使用稳定身份。候选 allocator 可以是上层单 writer、MetadataStore range allocator 或其他经 RFC 接受的 authority。

最低要求：

- reservation 是有界区间；
- range 不重叠；
- crash 允许形成 hole，但 hole 处理不能伪造已提交记录；
- `writerEpoch` 变化不能使旧 reservation 在 successor 上重新合法；
- reservation authority 与 durable publication frontier 分离。

首版不要求 Bookie 分配全局 sequence。

## 7. appendId 与不确定结果

每个逻辑 append 或原子 application batch 必须有稳定 `appendId`。重试复用相同 appendId 和相同 payload digest；相同 appendId、不同内容必须是冲突。

客户端把结果分为：

```text
COMMITTED       可证明达到合同要求的 AQ/durability
REJECTED        可证明未被接受
OUTCOME_UNKNOWN response loss、timeout 或 takeover 竞争
```

`OUTCOME_UNKNOWN` 不能直接重发为新的 sequence，也不能直接当作失败。恢复必须通过 predecessor 的最大连续可恢复前缀、appendId 索引或权威 scan 解析。

appendId index 可以是 control entry、footer 或 RocksDB derived index；任何派生索引损坏时都必须能从 payload 重建。

## 8. Concurrent I/O 与 ordered frontier

客户端可以并发发送多个 entry，但对上层发布 durable frontier 时必须按 sequence 连续推进。

```text
reserved:  100 101 102 103
completed: yes yes no  yes
frontier:  101
```

entry 103 的网络 completion 不能让可见 frontier 跨过 102。实现必须对 inflight entry count 和 bytes 设置硬上限，避免一个 hole 导致无界缓存。

首版 durability 仅允许 `SYNC_ON_ACK`。`DEFERRED_SYNC_LEGACY` 遇 failed Bookie 时不具备一般 ensemble-change 合同，不与首版 Sequenced WAL 自动 failover 组合。

## 9. Client-only stale-writer 合同

### 9.1 禁止 same-ledger epoch takeover

错误流程：

```text
writer epoch 31 writes ledger L1
owner changes to epoch 32
epoch 32 continues writing L1
```

旧 Bookie 无法按 envelope 中的 epoch 拒绝 Add，因此此流程没有 server-side fencing 依据。

合法流程：

```text
predecessor ledger L1
    -> acquire recovery authority
    -> BookKeeper quorum fencing
    -> recover maximum provable contiguous prefix
    -> close/seal L1
    -> create and prepare successor ledger L2
    -> publish successor run ACTIVE
    -> new writer appends only to L2
```

新 writer 在 successor ACTIVE 之前不得接受上层新 append。

### 9.2 takeover 线性化边界

旧操作按其与 BookKeeper fence 的关系分类：

| 类别 | 语义 |
| --- | --- |
| fence 生效前已经取得 AQ | 合法 predecessor 成功；即使 response 晚到也不追溯撤销 |
| 与 fence 并发且结果不确定 | 由 recovery 是否纳入最大连续前缀决定 |
| fence 后才尝试形成 AQ | 不能重新达到 AQ；失败或维持 outcome unknown |

owner metadata 的变化不能追溯宣布所有旧 completion 非法。反过来，response 在 successor ACTIVE 后到达也不能证明该操作是在 ACTIVE 后才提交；必须看 predecessor AQ/recovery 事实。

精确安全目标是：

```text
after successor ACTIVE,
no predecessor operation outside the sealed recovered prefix
can become newly published committed data
```

### 9.3 Metadata watch 的边界

writer 可以缓存/watch owner metadata 来提前停止发送，但它只是 early-rejection optimization。正确性来自 BookKeeper fencing、recovery、predecessor seal 和 successor publication。

因此不要求普通成功前远程读取 root/epoch，也不能以“不访问 MetadataStore”推导 stale-writer 安全。

## 10. Recovery 与 successor publication

恢复方必须：

1. 取得唯一 recovery authority；
2. fence predecessor ledger；
3. 读取足以证明 AQ/最大连续 entry 前缀的副本；
4. 校验 sequence interval、appendId 和 payload checksum；
5. 截止在第一个不可证明连续的位置；
6. 以 BookKeeper 合同恢复/关闭 predecessor；
7. 写入可审计的 run footer 或等价 seal record；
8. 创建并准备 successor；
9. 以 MetadataStore CAS 发布 predecessor SEALED 与 successor ACTIVE 的关系。

若第 9 步 response loss，恢复方必须读取 authority 解析，不得盲目创建第二个 successor。

run footer 可以加速恢复，但不是唯一事实来源。footer 缺失或损坏时走 bounded scan/fallback；恢复上限和 fallback 成本需要 RFC-0004 闭合。

## 11. Future epoch-aware capability

如果将来要求 Bookie 解析 `writerEpoch`、拒绝 stale writer 并允许 same-ledger epoch change，必须定义独立能力：

```text
EPOCH_AWARE_ADD_V1
```

它至少需要：

- durable epoch install/fence authority；
- Add 携带 epoch 与 instance/hash；
- Bookie 对 epoch 单调性和冲突的 fail-closed 校验；
- recovery、ensemble replacement 和 mixed-version 合同；
- 独立形式化模型。

该能力不属于 client-only Profile，也不在本 RFC 首版范围内。

## 12. Derived sequence index

允许的加速结构：

- ledger 内 packed range-index control entry；
- run footer；
- Bookie installed Profile 下的 derived RocksDB index；
- client-side cache。

所有加速结构必须满足：

- 不改变 ACK 事实；
- 不把缺失索引解释为缺失 payload；
- 能从权威 entry envelope 重建；
- locator 包含 ledger instance，避免 ledgerId 重用 ABA；
- rebuild 与 foreground I/O 有明确 QoS。

## 13. 安全不变量

1. 同一 SequenceDomain 中两个已发布 payload interval 不重叠。
2. ordered durable frontier 只跨过连续且可证明 committed 的 sequence。
3. 相同 appendId 不得映射到不同 payload digest 或两个 committed interval。
4. successor ACTIVE 之前 predecessor 已 fence、恢复并 seal。
5. successor ACTIVE 后，predecessor sealed prefix 之外的数据不得新进入 published committed state。
6. fence 前已取得 AQ 的合法结果不被 owner metadata 追溯撤销。
7. recovery 只发布最大可证明连续前缀。
8. 删除全部 derived index 不改变可恢复结果。
9. client-only Profile 不依赖旧 Bookie 解释 writerEpoch。

## 14. Model B 最低场景

形式化模型至少覆盖：

- 两个 writer 竞争 recovery authority；
- 并发 Add、乱序 completion 和 response loss；
- fence 前 AQ、fence 并发、fence 后到达；
- client crash before/after reservation；
- recovery crash and retry；
- successor publication CAS response loss；
- Bookie crash 和 ensemble change；
- appendId 相同内容重试与不同内容冲突；
- 3/3/2、3/3/3、3/2/2 以及一般 `E > W` 的协议结构。

模型不包含 blob store。

## 15. 接受 Gate

本 RFC 进入 Accepted 前必须：

- takeover 的 recovery authority、metadata schema 和 CAS 线性化点冻结；
- successor 起始 sequence 和 reservation hole 规则冻结；
- outcome-unknown 解析有确定算法和有界 fallback；
- client-only mixed-version 集成测试证明不依赖 server epoch 解析；
- Model B 无 safety counterexample；
- first-version API 明确只支持 `SYNC_ON_ACK`；
- derived index 删除/rebuild 不改变恢复结果。

任一测试出现两个 ACTIVE successor、sealed prefix 外旧数据被发布、sequence overlap 或 appendId 内容冲突被接受，本 RFC 不得提升状态。

## 16. 开放问题

- SequenceDomain 与 run metadata 的 authority/schema；
- recovery authority 的取得、租约失效和 CAS 细节；
- sequence allocator 选择、range 大小和 hole policy；
- successor 的起始 sequence 与 predecessor footer 的绑定；
- application batch 的原子边界；
- appendId retention、索引大小和冲突窗口；
- recovery scan 的最大窗口与超限处理；
- installed Profile 的 server-side sequence capabilities 是否拆成多个 feature bit；
- pooled lane 是否永远 deferred，或另立专门 RFC。

这些问题关闭且 Model B 通过前，Sequenced WAL 仍是 RFC Required。
