# Spike B：Allocator、Block 与 100k Ledger 资源模型否证规范

> 状态：**Planned / Not Executed**<br>
> 对应 RFC：[RFC-0003](../RFC-0003-segment-storage-allocator.md)<br>
> 性质：否证型；PASS最多解锁不会污染Classic cohort的isolated/discardable shadow prototype，不解锁live shadow

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
RFC0005Revision
prototypeCommit
supported stock old Bookie commits/exact built artifacts/startup modes/tools
sameScopeCandidate=BKPF1 metadata/local Cookie sentinel exact bytes
newScopeFallback=BookieId/roots/incarnation/OS-service-credential-ACL manifest
Cookie/layout/version and auto-stamp policy
storage incarnation/device-manifest schema
Arena superblock/mandatory-feature/migration schema
persistent ProfileRegistrationStore + ephemeral registration adapter revision
migration/rollback tooling revision
hardwareHostId
CPU and NUMA topology
memory size
NVMe model, firmware and namespace
filesystem and mount options
I/O API and alignment
JDK and JVM flags
shard count
control-log/checkpoint region sizes
conditional API revision
sequencer queue/waiter/idempotency hard caps
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
- Round 7 `BKPF1` metadata/local Cookie sentinel candidate及atomic local publication；same-scope仍BLOCK，原型结果不能写成accepted format；
- new BookieId/new journal/ledger/index/Arena roots/new incarnation/new OS-service credential/ACL scope fallback，且旧service无法打开或写新scope；
- storage incarnation、required-device manifest、migration generation与versioned registration readiness；
- compatibility hook位于`EmbeddedServer`创建任何可能触碰Profile/Segment storage的component之前；
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
- per-Arena conditional apply/result、operation identity、assigned control sequence 与 group `durableThrough`；
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
checkpoint current selector + anti-ABA state equals full-chain oracle
condition failure changed no authority state
durable apply result covered its own control sequence
duplicate operation produced no second winner/generation bump
uncommitted relocation copy never becomes authoritative
old allocation is not reused before move cutover and reader drain
orphan new location has no lookup/local-success authority dependency,
while the logical entry may retain its existing success at the current selector
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

在 control cut `S`、bounded checkpoint chunks、fsync、`CHECKPOINT_COMMIT(generation,S,identity)`、inactive superblock update/fsync、active generation switch、fallback-dependency verification和 old-log reclaim各点 crash。把 `MOVE_COMMIT` 放在 `S-1/S/S+1`，并覆盖 current selector已保存但old source尚未free、old free后historic chain被压缩、A/B fallback仍依赖旧suffix等状态。

Oracle：restart选择一个完整authority，`checkpoint through S + complete suffix >S`与full-chain replay产生相同allocation/current selector/retiring/anti-ABA状态；不得选择损坏的较新checkpoint或回收任一fallback仍依赖的唯一suffix。

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
- checkpoint后stale old operation retry；
- logical entry已有local-success，但uncommitted new location从未成为success/lookup location；
- orphan GC与late commit在同/不同group竞争，分别覆盖free先赢与commit先赢；
- shared allocation一条orphan、一条live，以及writer-generation/pin quiescence race。
- 同一operation在enqueue前、append后force前、force后response loss重试；
- group内独立condition一成一败、`durableThrough < ownSequence`、middle gap/torn/unknown mandatory record；
- idempotency summary compact后的extremely-late retry，以及queue/waiter/future cap压力。

Oracle：无 commit 时 old authoritative、new copy 只是 orphan；durable commit 后 new authoritative 且 index 可重建；同一 predecessor 只有一个 winning successor；每个 live record 至少一个 authoritative lookup locator（允许cut前old reader pin）且不能有两个new lookup winners；old block只有在全部live records moved/dead、new pin被阻断、既有reader drain和durable free后回收。relocation不新增local-success fact；清理orphan new location不删除logical entry在current location承载的既存success。pending/admitted append不授予authority，condition failure无副作用；duplicate只返回same durable result或stale/conflict。

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

### B14：Stock old binary pre-replay downgrade fence

步骤：用manifest锁定的每个真实stock old binary/release/commit和启动模式/tool/storage-expansion入口，打开分别包含metadata/local `BKPF1` sentinel、仅Cookie version bump、仅Cookie optional property、仅new superblock/file、仅unknown negative Journal meta-entry、仅registration property的scope；覆盖data-integrity enabled/disabled、Cookie auto-stamp、旧进程已运行/未完全退出、stale exclusive lock、BookieId reuse、Journal replay与writable registration。instrument old process在退出前触碰的每个file/path/byte和write/open动作。

Oracle：唯一可接受same-scope candidate必须让每个supported old binary在任何Profile storage open、Journal replay、Arena/data writer、handle/lazy storage、registration与write前确定退出，且不stamp over sentinel。仅“最终未注册”不足；data-integrity路径先创建LedgerStorage时任何不允许的file touch都使candidate FAIL。Cookie optional/version、registration、new file/superblock或unknown Journal record被忽略/restamp/skip必须记录为否证。若无candidate可证明，正式结果锁new BookieId/new roots/new credentials/ACL fallback，原scope不可写且旧service无access。

### B15：Partial migration、device manifest 与 rollback

步骤：按drain/connection close → exclusive storage lock → persistent PREPARED/Cookie sentinel CAS → 每required directory atomic local sentinel → device superblock → control/route recovery → delete catch-up → durable local readiness → persistent `ProfileRegistrationStore` CAS → matching BookieServiceInfo/ephemeral registration，在每个边界crash/response loss；覆盖多device子集、missing/corrupt/unknown mandatory、device replacement、stale registration、rollback/reverse/wipe/new-incarnation，以及new BookieId/new-scope fallback的access-denied与placement/readiness。

Oracle：任何partial/mismatch/unknown/corrupt状态整个Bookie non-writable并重试同一migration generation；persistent readiness CAS先于ephemeral registration，response loss重读两层且generation/incarnation mismatch demote。存在任一local success/route/activation/fence/grant/tombstone/Arena authority/durability unknown时same-scope old-binary rollback拒绝；恢复只可roll-forward、verified export/rebuild、irreversible wipe/decommission或new incarnation。new-scope fallback只在旧BookieId drained/readonly/decommissioned且旧credential不能访问时可writable。

Wave 0已完成一个不访问真实filesystem、OS权限、registration backend或外部目标的typed reference implementation与immutable receipt；17项普通测试机械覆盖上述ordering、CAS/response loss、九个crash cut、device负向状态、stale demotion、rollback拒绝和new-scope access-isolation语义。它只证明reference state machine和内存adapter满足当前合同，不能替代本节要求的真实stock binary、file-touch、multi-device、physical durability、startup raw metrics或formal Spike运行；Spike状态继续是`Planned / Not Executed`。

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

另设独立startup/read-amplification矩阵，不把它混入Add热路径：

```text
cold and warm Classic-only startup baseline
cold and warm Segment startup
1 and manifest-maximum required-device counts, plus intermediate points
compatibility-fence read bytes and I/O count
pre-storage-open hook latency and files/bytes touched
device-manifest + per-Arena superblock read bytes and I/O count
allocator/route/delete recovery and readiness/registration phase latency
persistent readiness CAS + ephemeral registration latency
total time to read-only and writable readiness
```

每个phase报告原始样本、分布、device-count scaling与matched baseline；exact latency/read-amplification threshold保持OPEN，不能看到结果后补Gate。最低证据要求是所有计数可独立归因，Classic-only startup不会执行Profile/Arena验证或连接handshake，且format/readiness检查只发生在startup/migration/registration，normal Add中的相关read/remote-I/O计数为0。

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
sequencer queue depth and waiter/token count
conditional retries and condition-failure counts
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
- checkpoint current-selector compaction、orphan free/late commit与durable-through gap；
- conditional enqueue/append/force/result、duplicate response loss、unknown mandatory record与selector/pin acquire cut；
- repeated restart；
- fixed-seed random operation/fault sequences。

B14/B15必须另执行完整stock binary/boot/migration matrix；模拟parser或mock registration不能替代正式结果。

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
orphan GC removed current local-success payload = 0
checkpoint selector differed from full chain    = 0
late commit and orphan free both succeeded      = 0
conditional failure mutated authority state     = 0
durable result before own sequence durability   = 0
duplicate operation created second winner/bump  = 0
unknown mandatory record skipped writable       = 0
selector cut allowed new old-location pin        = 0
queue/waiter/idempotency hard-cap violations     = 0
unreplayable executed failure                  = 0
supported old binary crossed compatibility fence into storage-open/replay/write/registration = 0
supported old binary touched Profile authority/Arena before fail-stop = 0
supported old binary stamped over BKPF1 candidate      = 0
Cookie/new-file/registration-only false downgrade gate = 0
partial required-device migration became writable = 0
unknown/corrupt mandatory format became writable = 0
stale readiness/registration generation became writable = 0
persistent readiness missing/mismatch registered ephemeral writable = 0
unsafe old-binary rollback accepted             = 0
old service credential opened/wrote new-scope fallback = 0
missing startup/read-amplification raw metrics   = 0
format/readiness validation executed on normal Add = 0
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

stock old binary越过candidate fence触碰Profile storage、进入Journal replay/registration/write、stamp over mandatory sentinel，partial migration变成writable，或旧service credential能打开new-scope fallback，均属于同等级立即停止的safety violation。

## 13. 必交 artifacts

```text
manifest.json
results.json
gate-summary.json
resource-accounting.md
performance-raw/
startup-performance-raw/
device-images-or-snapshots/
control-log-dumps/
checkpoint-dumps/
rebuild-dumps/
fault-injection-log/
failed-seed-reproducers/
stock-old-binary-boot-matrix/
cookie-autostamp-results/
pre-storage-open-file-touch-traces/
new-bookieid-new-scope-access-matrix/
migration-crash-matrix/
registration-readiness-history/
rollback-proof-results/
checksums.txt
README.md
```

敏感或过大的 device image 可用内容寻址存储，但 manifest 必须保留不可变 digest 和取证位置。

## 14. 结果解释

- PASS：未在锁定矩阵中否证allocator/资源模型，只解锁RFC-0003接受评审和isolated/discardable shadow prototype；若same-scope candidate失败但new-scope fallback通过，PASS必须明确记录该限定，不能继续宣称原地格式兼容。
- FAIL：存在 safety、resource 或 locked performance Gate 失败，Segment 保持 P0 Blocked。
- INCONCLUSIVE：证据不全、fault 未命中、环境漂移或结果不可重放。

PASS 不授权 Segment 成为 ACK authority，不授权生产 canary，也不证明 cluster delete。
