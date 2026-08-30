# Spike B：Allocator、Block 与 100k Ledger 资源模型否证规范

> 状态：**Planned / Not Executed**<br>
> 对应 RFC：[RFC-0003](../RFC-0003-segment-storage-allocator.md)<br>
> 性质：否证型；PASS 最多解锁 Segment shadow implementation

## 1. 要回答的问题

本 Spike 验证候选 `ArenaControlLog + shared slab + dedicated extent` 是否能同时满足：

- crash 后无 double allocation；
- generation reuse 后无 stale read；
- 已允许参与 ACK 的本地 payload 不因 allocator authority 恢复而丢失；
- 100k idle ledger 不产生固定 extent 或 per-ledger block buffer；
- allocator checkpoint、reclaim 和 compaction 对 foreground p99 的影响可控。

该 Spike 的目的不是证明生产性能，而是尽早否证 allocator authority 和资源模型。

## 2. 非目标

- 不实现完整 BookKeeper quorum 或 AutoRecovery；
- 不证明 Profile install；
- 不决定最终生产 extent size；
- 不把 page cache 或 tmpfs 结果当作 NVMe direct-I/O 证据；
- 不把 shadow-writer PASS 当作 Segment ACK authority。

## 3. 运行前锁定 manifest

以下字段不得为 `TBD`：

```text
sourceCommit
RFC0003Revision
prototypeCommit
hardwareHostId
CPU and NUMA topology
memory size
NVMe model, firmware and namespace
filesystem and mount options
I/O API and alignment
JDK and JVM flags
shard count
control-log/checkpoint region sizes
block sizes under test
extent sizes under test
ledger state implementation
fault injector version
workload generator version
warmup, duration and repetitions
baseline definition
artifact output directory
```

正式测试使用独占或可证明隔离的设备区域；不得指向生产数据盘。输出目录必须全新且 immutable。

## 4. 最小原型

必须实现真实可崩溃恢复的：

- superblock A/B；
- `ArenaControlLog`；
- allocator checkpoint A/B 与 rotation；
- `ALLOC/ALLOC_POOL`；
- shared active slab blocks；
- dedicated extents；
- block/record framing 与 checksums；
- locator 的 generation + ledger instance 校验；
- durable `DELETE_TOMBSTONE`；
- durable `FREE_AND_BUMP`；
- same-Arena conditional durable `MOVE_COMMIT`；
- Bookie/shard restart replay；
- 100k idle ledger state；
- 可删除并重建的最小 derived index。

内存模拟 control log 或跳过 fsync 的原型只能用于开发，不能产生正式 PASS。

## 5. Authority Oracle

每次运行保留三份独立事实：

1. workload intent/ACK journal：哪些 record 被允许视为 local durable success；
2. 原始 device image 或故障时快照：control、checkpoint、data bytes；
3. restart 后 allocator/rebuild 输出。

离线 checker 必须验证：

```text
one live owner per slot/generation
monotonic generation per slot
every local-success record has durable allocation and valid payload
no locator crosses ledger instance or generation
free list excludes every still-live allocation
checkpoint + suffix replay is deterministic
every committed move chain has one unique authoritative successor
uncommitted relocation copy never becomes authoritative
old allocation is not reused before move cutover and reader drain
```

checker 与在线 allocator 使用不同代码路径或至少独立解析实现，避免同一 bug 自证正确。

## 6. 功能场景

### B1：Control-first allocation

在 ALLOC append 前、append 后 fsync 前、fsync 后、空间 publish 前注入 crash。

Oracle：未 durable allocation 的空间不能包含被视为 local success 的 DATA；durable allocation 可以在 restart 后成为 allocated 或安全回收，但不能被两个 owner 使用。

### B2：DATA durability 与 local success

在 block header、payload、checksum/commit marker、data fsync、local success publication 各边界 crash。

Oracle：local-success journal 中的每条 record restart 后可验证恢复；torn/uncommitted tail 不被当作成功。

### B3：ALLOC pool refill

反复 refill、部分使用、shard crash、Bookie crash、未使用 pool 回收。

Oracle：pool ownership 不重叠；restart 后 unused/live classification 确定且可重复。

### B4：Shared slab 多 ledger

多个 ledger 交错写同一 block，分别删除一个、部分、全部 ledger。

Oracle：单 ledger logical delete 不影响其他 record；block 全死前不进入 free list；全死后按 durable generation bump 回收。

### B5：Hot promotion

冷 ledger 在阈值边界晋升 dedicated extent，crash 于 promotion 决策、allocation、首条 dedicated write 各点。

Oracle：shared 旧数据和 dedicated 新数据可组成唯一 ledger history；不要求迁移，不重复/丢失 local-success entry。

### B6：Delete、reader drain 与 reuse

保持 reader/pin，触发 delete；在 tombstone、invalidate、drain、FREE_AND_BUMP、新 owner allocation 各点 crash。

Oracle：reader 未 drain 时 slot 不复用；旧 locator 在新 generation 上明确失败；新 owner 数据不被旧 ledger 读取。

### B7：Checkpoint A/B rotation

在 checkpoint build、fsync、commit、inactive superblock update、fsync、active generation switch、old-log reclaim 各点 crash。

Oracle：restart 选择一个完整 authority；不得选择损坏的较新 checkpoint；不得回收唯一必要 suffix。

### B8：Control authority corruption

分别损坏单个 superblock、单个 checkpoint、control tail、必要 control suffix、A/B 全部 authority。

Oracle：可证明时降级恢复；authority 无法证明时设备 FAILED/QUARANTINED，绝不扫描 data 后猜 free list 并 writable。

### B9：Derived index deletion

完整删除 RocksDB/locator index，重启 rebuild。

Oracle：恢复的 ledger/entry/locator 集合与权威 payload 一致；stale generation 不进入新 index。

### B10：Compaction copy

对部分死亡 shared block 执行 same-Arena compact，在新 allocation、copy、DATA durability、conditional `MOVE_COMMIT` append/durability/response loss、locator publish、new-pin 阻断、reader drain、old free 各点 crash。

必须覆盖：

- 同一 predecessor 的两个 concurrent moves；
- move chain `A -> B -> C`；
- batch/group-commit torn tail；
- new payload digest mismatch；
- 每个边界删除 derived index 后重建；
- 只迁移部分 records 时尝试 whole-block free；
- old reader/pin 跨越 cutover；
- checkpoint 覆盖/未覆盖 `MOVE_COMMIT` 时 rotation crash。

Oracle：无 commit 时 old authoritative、new copy 只是 orphan；durable commit 后 new authoritative 且 index 可重建；同一 predecessor 只有一个 winning successor；每个 live record 至少一个权威副本且最多一个 active locator；old block 只有在全部 live records moved/dead、new pin 被阻断、既有 reader drain 和 durable free 后回收。relocation 不新增 local-success fact。

## 7. 资源规模场景

### B11：100k idle ledger

创建并安装原型级 100k ledger state，不写 payload或仅每 ledger 写极低速记录。

必须测量：

```text
Java heap retained bytes
direct memory
native allocator bytes
thread stacks
RocksDB memtable/block cache
open file descriptors
active block buffers
reserved data-arena bytes
control metadata bytes
```

硬 Gate：

```text
fixed extent reservation per idle ledger = 0
per-ledger block buffer                  = 0
active block buffers                    = O(shards)
idle ledger state                       <= 1 KiB per ledger
total ledger metadata resident memory   <= 128 MiB per Bookie
```

目标值：idle ledger state 不高于 512 bytes。目标未达但硬 Gate 达到可以记录为待优化；硬 Gate 失败即 FAIL。

测量必须给出 accounting equation，不能用 RSS 单值同时代表 ledger metadata。

### B12：Cold shared write

1k、10k、100k low-rate ledgers 交错写，验证 active buffer 数量不随 ledger 线性增长，并记录 group commit、fragmentation 和 dead bytes。

### B13：Hot dedicated write

1、10、100 个高吞吐 ledger，比较候选 extent size 的 allocation rate、tail waste、write amplification 和 recovery scan。

此场景用于选择 RFC 参数，不设“某个 extent size 必须胜出”的事后 Gate。

## 8. 性能场景

entry sizes：

```text
1 KiB
4 KiB
32 KiB
```

负载：锁定 baseline saturation 的 30%、60%、80%。至少包含：

- no-background baseline；
- control pool refill；
- checkpoint rotation；
- shared-block reclaim；
- compaction；
- full derived-index rebuild。

核心硬 Gate：

```text
allocator background activity foreground p99 regression <= 5%
```

比较必须在 matched offered load、相同 durability、相同 shard 和重复次数下进行，并报告 confidence interval/原始样本。无法稳定复现为 INCONCLUSIVE，不按 PASS。

## 9. 写放大与空间指标

必须记录：

```text
host payload bytes
device bytes written
control-log bytes
checkpoint bytes
data padding bytes
compaction copied bytes
move control-log bytes and durability barriers
dead but unreclaimed bytes
dedicated tail waste
```

本 Spike 不把 `<= 1.25x` 作为所有原型 workload 的唯一 Gate，但必须证明计量方法可用于后续 production-candidate Gate。不得把 control/checkpoint/compaction bytes 排除后宣称总写放大。

## 10. 故障矩阵

每个 B1-B10 场景至少执行：

- deterministic crash at every named boundary；
- process kill；
- simulated torn sector/block where injector supports；
- response/local-success publication loss；
- `MOVE_COMMIT` durability/response loss、concurrent move 与 reader-pin cutover；
- repeated restart；
- fixed-seed random operation/fault sequences。

最低随机矩阵：

```text
seeds >= 1000 before formal run lock, or a reviewed equivalent budget
operations per seed fixed in manifest
all failing seeds preserved and replayable
```

预算可以在运行前评审调整；不能看到失败后减少 seed 数并宣称 PASS。

## 11. 硬 Safety Gate

PASS 必须满足：

```text
double allocation                             = 0
stale-generation successful read              = 0
cross-ledger-instance successful read          = 0
local-success payload lost after recovery      = 0
FREE/reuse before durable generation bump      = 0
reader-pinned slot reused                      = 0
checkpoint replay authority divergence         = 0
authority-loss device resumed writable         = 0
derived-index rebuild changed payload facts    = 0
uncommitted move copy became authoritative     = 0
committed move lost after index deletion       = 0
multiple winning successors per predecessor    = 0
source freed before move commit/reader drain    = 0
move created new local-success fact             = 0
unreplayable executed failure                  = 0
```

外加 B11 资源硬 Gate 和 foreground p99 regression Gate 全部达到。

## 12. 立即停止条件

任一 safety violation 立即：

1. 停止扩大 workload；
2. 冻结 device image、seed、logs 和 checker output；
3. 标记 Spike FAIL；
4. 回到 RFC-0003 修正 authority/ordering；
5. 使用新 immutable run 验证修正。

不得把 counterexample 标成 flaky 后删除，不得通过跳过 fault point 或扫描猜测 free list 继续。

## 13. 必交 artifacts

```text
manifest.json
results.json
gate-summary.json
resource-accounting.md
performance-raw/
device-images-or-snapshots/
control-log-dumps/
checkpoint-dumps/
rebuild-dumps/
fault-injection-log/
failed-seed-reproducers/
checksums.txt
README.md
```

敏感或过大的 device image 可用内容寻址存储，但 manifest 必须保留不可变 digest 和取证位置。

## 14. 结果解释

- PASS：未在锁定矩阵中否证 allocator/资源模型，只解锁 RFC-0003 接受评审和 shadow implementation。
- FAIL：存在 safety、resource 或 locked performance Gate 失败，Segment 保持 P0 Blocked。
- INCONCLUSIVE：证据不全、fault 未命中、环境漂移或结果不可重放。

PASS 不授权 Segment 成为 ACK authority，不授权生产 canary，也不证明 cluster delete。
