# Round 3：Local Durable Authority、Recovery Fallback 与 Repair Reset

> Reviewer：复用 Round 1/2 的固定 `gpt-5.6-sol / max` agent<br>
> 源码与文档基线：`4849e7559ad816cbea7ac5e76885138ab12cb9df`<br>
> 评审模式：正确性、热路径性能、复杂度和过度设计联合审查<br>
> 记录规则：以下 reviewer 回复未经删节；仅将 Markdown heading 统一下移一级，以保持文档单一 H1。规范是否采纳以对应 RFC 的集成结果为准。

## 本轮 Questions

### Q1：Segment compaction relocation authority

决定同一 Arena 内 compaction copy 的唯一 durable cutover authority、reader drain、old allocation free 与 group commit 边界；cross-Arena/device relocation 不得被本轮机制暗自覆盖。

### Q2：Range/TailSummary/BatchRecovery fallback

决定 fast-path unsupported、stale、partial 或局部预算耗尽后的 deterministic point-read fallback；区分 attempt deadline、transient unavailable、corruption 与 evidence exhausted。

### Q3：DeleteJournal / offline rejoin

把 per-ledger `deleteEpoch` 与 delivery progress 分开，冻结 no-hole stream、durable cursor、snapshot+suffix、bounded assignment、storage incarnation 与 writable registration 的最低合同，同时保持物理拓扑 OPEN。

### Q4：Permanent-loss repair reset

冻结 `F` budget 的 range-scoped reset proof，区分 target local durability、membership CAS、normal activation 与完整 `F+1` failure-domain coverage，禁止 ledger-wide 无条件清零。

## Reviewer 完整回复

<!-- BEGIN VERBATIM REVIEW -->

## Round 3 独立审查结论

基线已核对为 `turbo-bk@4849e7559ad816cbea7ac5e76885138ab12cb9df`，与 `origin/turbo-bk` 为 `0/0`。工作区只有未跟踪的 `BtrLog Low-Latency Logging.pdf`，本次未修改任何文件。

本轮总判定：

| 问题 | 结论 |
| --- | --- |
| Q1 Segment compaction relocation authority | **REVISE → 收窄为同一 Arena authority domain 后可 LOCK** |
| Q2 Range/TailSummary/BatchRecovery fallback | **REVISE → deterministic fallback 可 LOCK；终止分类必须收紧** |
| Q3 DeleteJournal / offline rejoin | **REVISE → 加入 bounded stream assignment、storage incarnation 和 registration linearization 后可 LOCK** |
| Q4 permanent-loss repair reset | **REVISE → range-scoped coverage proof 可 LOCK；membership/activation/local durability 单独都不够** |

现行文档和源码中的关键事实：

- RFC-0003 已把 durable relocation authority 归自己，但仍把 compaction locator switch 留为 OPEN；`ArenaControlLog` 当前候选 record 中没有 `MOVE_COMMIT`。
- Spike B 的 B10 只要求“新 locator durable/authoritative”，没有定义 durable authority 是什么。
- derived locator/RocksDB 明确是可重建结构，不能决定两个 durable payload 副本谁 authoritative。
- RFC-0004 第 204 行目前把所有 count/bytes/wall-clock/retry 超限直接变成 non-promotable/人工状态，这与本轮要求的 deterministic fallback 冲突。
- 当前 `LedgerHandle.asyncBatchReadEntries` 在不支持 batch 时已经回退 point read；`LedgerFragmentReplicator` 也有 point-read + single-entry recovery-add 基线，但新 general range 的 partial result、hole、TailSummary 和 budget fallback 尚无统一合同。
- RFC-0004 当前把 `deleteEpoch` 同时用于 per-ledger delete identity 和 Bookie catch-up watermark；二者不是同一维度。
- Spike C Model D 只有抽象的 local apply watermark，没有 delete stream、assignment、snapshot 或 Bookie storage-incarnation 语义。
- Round 1 已锁定 failure-domain budget，但 Spike C 只有 manifest 字段 `repair/re-replication budget reset point` 和 A-REPAIR-LOSS 场景名，没有实际 reset action、coverage proof 或 invariant。
- Round 2 的 RepairIntent `COMMITTED` 目前只表达 copy + ensemble CAS 完成，尚不足以证明所有 ACKed entries 已恢复到 `F+1` distinct domains。
- 写期 replacement 路径明确不复制历史 fragment，所以 membership CAS + `NORMAL_ACTIVE` 绝不能自动解释为历史 ACKed entries 已修复。

---

### Q1 — Segment compaction relocation durable authority

#### 结论：REVISE → 同一 ArenaControlLog authority domain 内可 LOCK

候选 `MOVE_COMMIT` 是解决“双份 durable payload、derived locator 丢失后谁 authoritative”所需的最小 durable selection authority。没有这一事实，restart scan 只能看到 old/new 两份合法 payload，无法安全选择。

但要补三条不可省略的限制：

1. `MOVE_COMMIT` 必须是条件化的 authority transition，不只是“新位置存在”的通知。
2. 首版合同必须限制 old/new allocation 受同一个可线性化的 `ArenaControlLog` authority domain 管理。
3. shared block 的某个 record 完成 move 不等于整个 old block 可以 `FREE_AND_BUMP`。

跨 device/Arena relocation 会涉及两个独立 control authority；单边 `MOVE_COMMIT` 不能解决另一设备丢失、双边 response loss 和双重 ownership。首版应明确：

```text
cross-Arena/device relocation = unsupported / KEEP OPEN
```

不能为了本轮引入通用分布式事务。

#### 最小不可删不变量

1. 新 allocation authority 必须在新 payload 使用前 durable。
2. 新 payload、ledger instance、logical identity、generation 和 digest 必须在 `MOVE_COMMIT` 前 durable 且可验证。
3. `MOVE_COMMIT` 是唯一 locator-authority cutover 点。
4. `MOVE_COMMIT` 必须条件化绑定 expected authoritative predecessor：
   - ledger instance；
   - logical entry/range identity；
   - old location + generation；
   - new location + generation；
   - payload digest；
   - move operation identity/generation。
5. 同一 predecessor 只能有一个 winning successor。并发 loser 的新副本只是 orphan garbage。
6. 无 durable `MOVE_COMMIT` 时 old location 仍 authoritative；不能因新 payload generation 更大、mtime 更新或 derived index 已指向新位置而切换。
7. durable `MOVE_COMMIT` 后，新 lookup 必须走 new location；已有 old-location reader/pin 可以完成，但不能再创建新的 old pin。
8. old allocation 只有在：
   - 所有 live records 已 move/dead；
   - old locator/cache 已切断新引用；
   - 既有 reader、zero-copy、async-I/O pins 已 drain；
   - `FREE_AND_BUMP` durable；

   之后才能复用。
9. `MOVE_COMMIT` 不产生新的 BookKeeper local success、AQ 或 ACK；它只保持既有 payload authority。
10. 新 authoritative payload digest 验证失败时必须 fail closed/进入恢复，不能静默回退 old copy。旧副本若仍存在，只能作为显式后续 repair/move 的候选 evidence，不能自行夺回 authority。
11. allocator checkpoint 必须包含或可由保留 suffix 重建所有仍有效 relocation cutover；checkpoint rotation 不能丢掉唯一 `MOVE_COMMIT` authority。

#### 推荐顺序与线性化点

```text
0. choose immutable live source record/range
1. append and durably commit ALLOC/ALLOC_POOL for new allocation
2. copy payload + full identity to new allocation
3. complete new DATA durability barrier and verify checksum/digest
4. append conditional MOVE_COMMIT(expectedOld -> new, moveGeneration)
5. make MOVE_COMMIT durable
   -- relocation authority linearizes here --
6. publish/rebuild derived locator to new location
7. prevent new old-location pins; drain pre-cutover readers/pins
8. when the whole old allocation is reclaimable:
   append durable FREE_AND_BUMP
9. expose bumped generation to allocator
```

`MOVE_PREPARE` 可以记录 copy intent、加速 orphan discovery 或 QoS accounting，但不是 safety 必需。没有 `MOVE_PREPARE` 时，GC 必须通过 replay 完整 control authority、operation generation 和 allocation ownership，证明新副本从未 commit，才能回收；不能仅凭超时删除。

#### Crash / response-loss

- crash before new DATA durable：old authoritative；new allocation/torn payload 可清理。
- new DATA durable、`MOVE_COMMIT` 前 crash：old authoritative；new payload 是 orphan。
- `MOVE_COMMIT` durable 但 response loss：replay相同 operation identity；不得重做成第二个 competing move。
- `MOVE_COMMIT` durable、derived locator publish 前 crash：restart 从 control authority 重建 new locator。
- locator publish 后、reader drain/free 前 crash：restart 仍选 new；old allocation继续保持 allocated，直到重新证明可 free。
- `FREE_AND_BUMP` response loss：按 slot、old/new generation 和 operation identity replay；不得重复 bump 或复用错误 generation。
- 两个 move 并发：只有 expected predecessor 匹配的条件化 commit 能赢；loser 不得覆盖 winner。
- move chain `A -> B -> C`：每个 commit 绑定直接 predecessor 和单调 move generation；replay不能只按“最大物理 generation”猜测。

#### 性能与复杂度预算

不能要求每个 moved entry 一次独立 control fsync。允许：

- 多个独立 `MOVE_COMMIT` records 共用一次 group-commit durability barrier；
- 一个有界 `MOVE_COMMIT` 覆盖一组 records/range；
- 每个 record 保留独立 logical identity/result，但 exact packing、range bytes 和 batch 数保持 OPEN；
- durability completion 只在覆盖该 move 的 barrier 完成后触发 locator cutover。

这会产生 background control bytes，但不是 normal Add 的 per-entry control fsync。必须计入总 host/device write amplification，不能从 Spike B 的 1.25x accounting 中剔除。

Compaction 需要 foreground QoS；必须测：

- group size 与 control bytes；
- move copy bytes；
- locator publish contention；
- reader-drain tail；
- foreground p99；
- orphan bytes与清理延迟。

#### Spike B / C 必须增加的 falsification

Spike B：

- B10 明确加入 `MOVE_COMMIT` 前后、durability response loss、locator publish、reader drain、old free各 crash point。
- concurrent moves from same predecessor。
- chained moves。
- batch/group commit torn tail，只允许完整 record prefix生效。
- new data digest mismatch。
- derived index在每个边界删除并重建。
- shared block仅部分 records moved时禁止 whole-block free。
- old reader跨 cutover持 pin。
- checkpoint覆盖/未覆盖 `MOVE_COMMIT` 时 rotation crash。
- 验证 `MOVE_COMMIT` control bytes/fsync与 foreground p99。

Spike C Model C 增加：

```text
CopyForMove
DurabilizeMovedPayload
AppendMoveCommit
DurabilizeMoveCommit
PublishMovedLocator
DrainOldReader
FreeMovedSource
```

至少加入：

```text
MoveCommitSelectsUniqueAuthority
UncommittedCopyNeverBecomesAuthoritative
CommittedMoveSurvivesIndexLoss
NoFreeBeforeMoveCommitAndReaderDrain
MoveDoesNotCreateLocalSuccess
```

---

### Q2 — Range/TailSummary/BatchRecovery deterministic fallback

#### 结论：REVISE → fallback 核心可 LOCK

推荐方向正确，但终止条件需要两处收紧：

1. 单个 corrupt replica 不足以把整个 ledger 判为 terminal；该 evidence 应失效并继续从其他 replica取证。只有 valid evidence exhausted 或出现不可调和的 identity/quorum-proof conflict 才能宣告 DATA_LOSS/terminal。
2. global deadline/cancellation 只能终止当前 attempt，不能单独证明 ledger 永久 non-promotable。它应返回明确 `CANCELLED/DEADLINE_EXCEEDED/INCOMPLETE`，保留后续重试可能。

还必须区分：

```text
fast-path local budget
    -> deterministic fallback

authoritative fallback temporary unavailability
    -> RETRYABLE / DEFERRED

caller attempt deadline/cancellation
    -> attempt incomplete, not data-loss proof

irreconcilable proof conflict or valid evidence exhausted
    -> terminal quarantine / DATA_LOSS
```

#### 最小不可删不变量

1. TailSummary 始终只是 hint，除非未来另行接受 quorum-proof合同。
2. unsupported capability、stale/invalid TailSummary、partial response、fast-path count/bytes/retry budget exhaustion 都不能改变 recovery correctness。
3. fallback 必须使用同一 immutable `RecoveryContext`：
   - ledger instance；
   - sealed/fenced metadata version/digest；
   - ensemble history；
   - E/W/A 和 entry→write-set mapping；
   - fence/recovery generation；
   - RepairIntent/delete control generation。
4. authority context 变化时不能继续使用旧 continuation；必须重读并证明等价，或在新 generation 下重新开始。
5. partial range 中每个 entry 独立验证。最小 unresolved coordinate 阻止连续 recovery frontier越过。
6. later entry即使已验证，也不能让 recovery跳过更早 hole。
7. 已验证 evidence 若因内存回收或 coordinator crash 丢失，可以安全重读；禁止的是把“本地没保留 proof”解释成 quorum absence，或在没有 proof时跳过。
8. fallback 从最早 unresolved coordinate开始，以 bounded concurrency执行现有 point-read/evidence merge，再走 single-entry recovery-add。
9. target recovery write仍受 live RepairIntent、recovery-only authority、instance、range和delete gate约束。
10. fast path和fallback的最终最大连续可证明前缀必须等价于全 point-read oracle。
11. partial BatchRecoveryAdd success按 entry精确重试；batch不得被当作原子。
12. transient timeout/no quorum 在证据未耗尽时不能伪造成 DATA_LOSS。

#### 推荐顺序

```text
1. freeze/read immutable RecoveryContext
2. read TailSummary only as upper-bound/priority hint
3. execute bounded range fast path
4. independently validate every returned coordinate and proof
5. maintain bounded in-memory:
   - proven coordinates/results
   - earliest unresolved coordinate
   - copied/target-durable results
6. on unsupported/stale/partial/fast-budget:
   switch to point-read fallback at earliest unresolved coordinate
7. reuse valid later evidence only while it remains bound to the same RecoveryContext;
   otherwise reread it
8. perform idempotent single-entry recovery-add for unresolved entries
9. never publish completion/frontier across a hole
10. after full required coverage, continue existing ensemble/close/receipt protocol
```

#### Continuation/checkpoint durability

Safety 不要求 durable per-entry continuation 或 durable range checkpoint。

默认最小合同应是：

- continuation、bitmap、later-entry proof cache 可以是 bounded volatile state；
- coordinator crash 后从 durable RepairIntent和 immutable RecoveryContext重读；
- 已写 target payload通过 entry identity/checksum实现幂等；
- 不向 MetadataStore写 per-entry progress；
- 不给每个 recovered entry增加 control fsync。

如果 benchmark证明长 fragment重复扫描不可接受，可增加 operation-scoped bounded checkpoint，但它仍只是优化：

- 必须绑定 RepairIntent generation 和 RecoveryContext digest；
- 丢失不改变结果；
- stale checkpoint只能被拒绝或忽略；
- exact interval、packing、durability和retention保持 OPEN。

#### Crash / response-loss

- TailSummary response loss：忽略 hint，进入 fallback。
- range完全失败：从该 request最小 coordinate fallback。
- partial result：从最早 unresolved coordinate fallback；later verified proof可保留或重读。
- range response收到后 coordinator crash：volatile proof可以丢失；restart从 durable operation context重读，不得宣告 absence。
- recovery-add response loss：读取 target或幂等重发相同 entry/digest。
- fallback期间 metadata/delete generation变化：停止旧 attempt并重新解析 authority。
- deadline/cancel：停止发新 I/O，drain/release已开始资源，返回 attempt incomplete；不写 DATA_LOSS。
- 单 replica corruption：排除该 evidence并继续。
- 不同 payload都通过身份/checksum校验或 metadata/write-set proof互相矛盾：进入 explicit corruption/quarantine，不能任取一份。
- 所有合法候选 evidence耗尽：明确 DATA_LOSS，永不返回成功。

#### 性能与复杂度预算

该合同只影响 recovery冷路径，不影响 normal Add。

需要控制：

- point-read fallback 的内存和并发必须有硬上限；
- 不能重新创建一个包含整个剩余 fragment的无界 list；
- fast path切换fallback不得重新读取已经可以安全复用的大量 later evidence，除非内存预算或 crash迫使重读；
- 所有重读/重写必须计入 recovery bytes、RPC、latency；
- fast path预算与 global attempt deadline必须是两个独立指标。

exact count/bytes/batch/retry数值仍 OPEN。

#### Spike C Model E / executable tests 必须增加

- unsupported before first response。
- unsupported after partial valid response。
- stale TailSummary过高、过低、错误 instance/generation。
- response中间 hole但后续 entries有效。
- fast count/bytes/retry budget恰好耗尽。
- fallback与range response/recovery-add response loss并发。
- coordinator crash丢失volatile continuation。
- delete/control generation在fallback前后变化。
- 单 replica corrupt但其他 evidence可恢复。
- irreconcilable payload/identity conflict。
- all evidence exhausted。
- cancellation/deadline不能写永久 DATA_LOSS。
- fast+fallback最终结果与全 point-read oracle完全一致。
- 内存峰值保持在manifest锁定的bounded concurrency内。

需要新增或等价 invariant：

```text
FastPathFailureDoesNotChangeRecoveryTruth
FallbackStartsAtEarliestUnprovenCoordinate
RecoveryNeverSkipsHole
AttemptDeadlineDoesNotProveDataLoss
FastAndPointOracleEquivalent
```

---

### Q3 — DeleteJournal / offline rejoin

#### 结论：REVISE → 加入 assignment/incarnation 后可 LOCK

候选 stream/cursor/snapshot方向正确。`deleteEpoch` 必须保留为 per-ledger-instance delete fencing/version，不能再用于比较不同 ledger的全局进度。

但仅有“每 stream cursor”仍不足够：Bookie还必须能证明它没有漏掉一个本应订阅、但本地完全不知道的 stream。因此必须增加 bounded、durable stream applicability/assignment authority。

#### 最小不可删不变量

1. 每个 delivery stream 有稳定 identity、stream generation和单调 committed sequence。
2. sequence无洞是对 committed stream而言；Bookie不能跨越 unexplained sequence。
3. cursor推进到 N 意味着 `<=N` 的每个 sequence都已经：
   - 对该 Bookie/storage incarnation完成 durable delete effect；或
   - 由可验证 routing/membership proof判定为不适用。
4. local tombstone、reader drain要求和必要 generation/free effect必须先 durable，cursor才能推进。
5. cursor绑定：
   - Bookie stable identity；
   - storage/device incarnation；
   - stream identity/generation；
   - applied-through sequence。
6. 新磁盘、重装或伪造新 Bookie incarnation不能继承旧 cursor。必须 verified bootstrap，或有 durable wipe/decommission proof。
7. 集群维护一个带 generation 的 finite applicable-stream assignment。Bookie不能仅报告自己知道的 stream。
8. registration时必须从cluster-authoritative assignment取得：
   - assignment generation；
   - finite applicable stream set；
   - 每个 stream required-through committed sequence。
9. applicable stream数量必须有配置前冻结的有限上限；超过上限或assignment无法证明时fail closed。
10. assignment移除一个 stream前，必须证明：
    - 其delete effects已被新assignment/snapshot接管；或
    - 旧Bookie/storage incarnation被不可逆decommission/wipe，不能返回writable。
11. 普通 ensemble replacement不是 decommission proof；它不会删除旧Bookie本地数据，也不会阻止旧incarnation重返。
12. snapshot必须绑定：
    - stream identity；
    - snapshot generation；
    - covered-through sequence；
    - assignment/membership generation；
    - 内容或效果 proof digest。
13. bootstrap只能是 verified snapshot + 无洞完整 suffix。
14. journal prefix只有在所有仍支持的bootstrap路径都有有效snapshot或terminal decommission proof后才能compact。
15. writable registration必须与新 delete publication线性化：
    - delete先于registration cut：必须包含在required-through；
    - registration先赢：后续delete必须把该Bookie当在线适用target并执行正常gate。
16. local cursor或snapshot丢失时Bookie保持RECOVERING/READ_ONLY，不能通过扫描到一些tombstone后猜测已catch up。

#### 推荐逻辑模型

```text
DeleteDeliveryAssignment {
    assignmentGeneration
    bookieIdentity
    storageIncarnation
    applicableStreams[]    // finite and bounded
    requiredThroughByStream
}
```

不冻结物理schema。

每个stream：

```text
CommittedDeleteEvent(streamId, streamGeneration, sequence, ledgerInstance, deleteAuthorityDigest)
```

Bookie apply：

```text
1. fetch verified assignment + required-through
2. for each applicable stream:
   a. verify local cursor/snapshot identity
   b. if bootstrap:
      apply verified snapshot covered-through N
   c. fetch complete suffix N+1..requiredThrough
   d. for each sequence:
      durably apply delete effect or verify explicit non-applicability
      then durably advance cursor
3. reconcile RepairIntent/device state
4. registration CAS/fence validates same assignment generation and cursors
5. become writable
```

#### Boundedness 与拓扑

为避免无限 cursor vector，立即锁定：

```text
applicable stream count per Bookie/storage incarnation
    <= finite manifest-locked maximum
```

同时禁止“每 ledger一个长期 stream”。

可选实现仍保持 OPEN：

- 每 Bookie inbox：cursor数接近 O(1)，fanout/storage成本高；
- 固定数量global shards：cursor数 O(configured shards)，需要明确routing/non-applicable proof；
- hierarchical snapshot + bounded suffix；
- 其他有同等boundedness的拓扑。

不需要一个全局串行 sequence。sequence只要求在各自 stream内单调，因此无需为了正确性制造全局 metadata bottleneck。

如果使用共享shard，Bookie不能简单跳过不属于自己的sequence；必须有stream中明确记录或可验证routing proof，使cursor可连续推进。

#### Crash / response-loss

- local tombstone durable、cursor前 crash：重放event，幂等完成并推进cursor。
- cursor先durable、delete effect未durable：safety violation。
- cursor response loss：重读本地cursor，不重复非幂等free。
- duplicate相同 sequence/digest：幂等。
- 相同 sequence不同内容：stream corruption，fail closed。
- suffix缺口：保持non-writable。
- snapshot torn/digest错误/generation回退：拒绝snapshot。
- snapshot applied、suffix前 crash：重读snapshot/cursor并从下一sequence继续。
- assignment变更 response loss：registration按cluster generation重读；旧assignment不能自行注册。
- Bookie长期离线到journal prefix已compact：必须有可验证snapshot；否则只能wipe/decommission，不能writable。
- decommission response loss：重读cluster proof；旧storage incarnation在proof未成立前仍受catch-up约束。
- registration与new delete竞争：由registration/assignment generation CAS决定先后，不能靠watch callback时序。

#### 性能与复杂度预算

该路径是delete delivery和Bookie registration冷路径：

- event append、Bookie tombstone/cursor可batch/group commit；
- cursor不要求每ledger独立fsync；
- snapshot减少离线catch-up和retention；
- normal Add不读取DeleteJournal远程状态；Bookie只有完成registration后依赖本地gate；
- exact stream数、batch、snapshot大小、retention时间保持 OPEN。

必须测：

- delete publish contention；
- fanout bytes；
- cursor group-commit；
- snapshot build/verify成本；
- 长期离线bootstrap latency；
- registration time；
- stream数增加时local state与metadata大小。

#### Spike C Model D 必须增加

状态：

```text
delete streams and committed heads
stream assignment generation
Bookie/storage incarnation
per-stream durable applied cursor
snapshot generation/covered-through/digest
required-through registration cut
```

动作：

```text
AppendDeleteEvent
DurabilizeDeleteEffect
AdvanceDeleteCursor
LoseCursorResponse
BuildDeleteSnapshot
CompactJournalPrefix
ChangeStreamAssignment
ReplaceStorageIncarnation
FetchRequiredThrough
RegisterWritable
```

场景：

- two streams交错。
- missing middle sequence。
- duplicate和same-sequence conflict。
- cursor-before-effect负向场景。
- snapshot每个crash边界。
- snapshot + suffix缺口。
- assignment增加/移除stream与rejoin竞争。
- old storage incarnation伪装新节点。
- registration与delete publish竞争。
- compact prefix后极晚rejoin。
- ordinary ensemble replacement不能替代decommission。
- applicable stream上限超限时fail closed。

不变量：

```text
DeleteCursorImpliesDurableEffects
NoCursorAdvanceAcrossUnexplainedGap
WritableImpliesAllApplicableStreamsCaughtUp
AssignmentRemovalCannotLoseDeleteObligation
SnapshotPlusSuffixIsComplete
OldStorageIncarnationCannotBypassCatchup
```

---

### Q4 — permanent-loss budget F 的 repair reset

#### 结论：REVISE → range-scoped reset 可 LOCK

候选方向正确，但必须明确：

```text
target local durability
    != all ACKed entries repaired

standard ensemble CAS
    != all ACKed entries repaired

NORMAL_ACTIVE
    != all ACKed entries repaired

generic RepairIntent COMMITTED
    != budget reset proof
```

reset必须是对一个明确、不可变、可验证 coverage interval 的事实，不能是ledger级无条件清零。

#### 最小不可删不变量

1. reset范围绑定 ledger instance和bounded fragment/range identity。
2. range必须绑定 immutable recovery context：
   - sealed metadata version/digest；
   - ensemble/write-set history；
   - Profile failure-domain policy/generation；
   - repair operation/generation；
   - coverage cut/end coordinate。
3. claimed range中的每个 ACK-eligible entry都必须有matching payload/identity evidence；任何hole都禁止该range reset。
4. 在proof cut上，每个entry的有效durable evidence覆盖至少 `F+1` distinct declared permanent-failure domains。
5. Bookie属于某domain必须由稳定、可审计的domain identity/policy generation证明；不能通过重新标注同一物理failure domain制造 `F+1`。
6. source read成功和target local durable各自只是coverage输入；只有完整range coverage proof才能reset。
7. membership CAS必须已经发布exact replacement mapping。
8. repair completion authority必须可重放、幂等，并绑定：
   - RepairIntent；
   - range/cut；
   - metadata CAS result；
   - evidence/domain coverage digest或等价receipt；
   - observed loss/policy generation；
   - completion generation。
9. completion response loss不产生第二次reset或新的repair epoch。
10. completion proof cut之后发生的永久loss必须计入新window；迟到旧completion不能抹掉已经发生/声明的loss。
11. overlapping range reset必须单调合并或按control generation串行；不能通过重叠operations重复“减掉”loss。
12. no evidence / coverage不足时不能reset；超过budget时进入明确DATA_LOSS，不伪造成功。
13. physical metadata可以按fragment/range压缩，不要求per-entry MetadataStore record。

建议区分两个事实，但不锁enum名称：

```text
REPLICA_COMMITTED
    copy complete + membership CAS published

LOSS_BUDGET_RESET_PROVEN
    complete F+1-domain coverage proof for a bounded range
```

现行 `RepairIntent COMMITTED` 只有在语义被增强为第二种proof时才能直接用于reset。否则必须保持两个事实，不可因为名字相同而混用。

#### 推荐顺序与reset线性化点

```text
1. create durable RepairIntent
2. freeze bounded repair range/cut and recovery context
3. grant target RECOVERY_ONLY
4. stream/read every required entry and validate identity/digest
5. make replacement payload/identity durable
6. collect/derive bounded evidence that every entry in range has
   F+1 distinct valid domains at the proof cut
7. reread authority and CAS standard ensemble replacement
8. transition target role:
   - historical/closed: COMMITTED_REPLICA/READABLE
   - current writable membership: post-CAS NORMAL_ACTIVE if it will accept future normal Adds
9. CAS durable repair completion authority conditioned on:
   - exact membership mapping
   - same repair/control generation
   - same failure-domain policy/loss generation
   - complete range coverage proof
   -- loss-budget reset for that range linearizes here --
10. retain/compact receipt only under existing delete-history retention rules
```

如果新的known permanent loss在coverage proof与completion CAS之间出现，generation条件必须使旧completion失败或把该loss计入proof cut之后的新window，不能无条件归零。

#### Closed / historical fragment

`standard ensemble CAS + RepairIntent COMMITTED/receipt` 可以足够，但只有在该 COMMITTED/receipt 已经语义上证明：

- claimed closed range完整；
- 每个ACK-eligible entry无洞；
- payload/identity验证通过；
- 当前有效evidence覆盖 `F+1` distinct domains；
- exact metadata mapping已发布；
- completion authority durable且可重放。

closed/historical target不需要、也不应获得 `NORMAL_ACTIVE`。要求normal activation会扩大接受集合，属于错误复杂度。

#### Current writable fragment

`NORMAL_ACTIVE` 是让replacement参与未来normal ACK所必需的，但它不是历史repair证明。

必须拆开两类范围：

1. **旧的immutable prefix/旧fragment**

   以 `LAC+1` membership boundary、fence或另一个durable cut冻结。它可以在ledger总体仍OPEN时按historical range修复；该range的reset依赖coverage receipt，不依赖target normal-active。

2. **CAS后新的current writable fragment**

   replacement必须完成post-CAS `NORMAL_ACTIVE` 后才能接收normal Add；此后每个新成功ACK自身重新满足 `F+1` domain合同。

如果要宣称一个包含CAS前已ACK entries的current range已经reset，则必须同时满足：

- bounded prefix coverage proof；
- membership CAS；
- 若target还承担future normal writes，则post-CAS `NORMAL_ACTIVE`；
- durable repair completion authority。

因此答案是：

```text
post-CAS NORMAL_ACTIVE 对 current writable authority 是必要条件，
但对历史ACKed payload的budget reset绝非充分条件。
```

写期replacement本身不复制历史fragment，所以不能仅凭 `install -> CAS -> activate -> resend` 清零旧loss window。

#### Crash / response-loss

- target部分copy后crash：无completion，无reset；RepairIntent继续可枚举。
- target全copy但coverage存在一个hole：无reset。
- target durable、metadata CAS前crash：无reset。
- metadata CAS durable但response loss：重读exact mapping；仍需completion proof。
- role transition完成但completion前crash：无reset。
- completion durable但response loss：重读同一operation/range generation，视为已reset一次。
- completion receipt响应到达前发生新loss：不能用迟到response清掉该loss。
- concurrent overlapping repair：不同generation或disjoint ranges；冲突range不得各自全量reset。
- delete先赢：repair不能completion；target进入delete/cleanup路径。
- repair completion先赢：delete history必须保留receipt/target。
- detected corruption使某evidence失效；只有剩余coverage仍满足 `F+1` 才可completion。

#### 性能与复杂度预算

不要求：

- stop-the-world；
- 全ledger重新复制；
- per-entry MetadataStore update；
- 每entry control fsync；
- unbounded ACK receipt history。

允许：

- 按fragment/range的bounded repair operation；
- 流式、bounded-memory coverage计算；
- target payload复用existing recovery-add；
- 本地range digest/bitmap/Merkle-like receipt作为候选，但exact encoding保持 OPEN；
- 一个operation completion CAS；
- 相邻同generation coverage interval的安全压缩。

必须测：

- repair read/write amplification；
- coverage验证CPU；
- receipt大小；
- fragment很小时metadata CAS放大；
- failure-domain验证延迟；
- current writable activation stall；
- repeated loss/repair的state growth。

#### Spike C 必须增加

Model A state：

```text
per-range repair generation
repair coverage cut
valid evidence domains per coordinate
failure-domain policy generation
loss generation since proof cut
durable repair completion authority
```

动作：

```text
BeginBoundedRepair
DurabilizeReplacementEvidence
VerifyRangeCoverage
PublishRepairMembership
ActivateCurrentReplacement
PublishRepairCompletion
LoseRepairCompletionResponse
PermanentLossAfterRepairProof
```

场景：

- partial target copy。
- all entries except one hole。
- target durable但domains少于 `F+1`。
- membership CAS without full copy。
- activation without historical copy。
- closed fragment reset without NORMAL_ACTIVE。
- current writable target未activate。
- current target active但历史range不完整。
- `E > W` 中不同entry write set/domain coverage。
- repair proof后loss、response loss、第二次loss。
- delayed old completion跨越new loss generation。
- overlapping repair ranges。
- detected corruption。
- membership-only或local-durability-only伪造reset。

不变量：

```text
RepairResetImpliesCompleteRangeCoverage
RepairResetImpliesFPlusOneDistinctDomains
MembershipAloneNeverResetsLossBudget
ActivationAloneNeverResetsLossBudget
LocalTargetDurabilityAloneNeverResetsLossBudget
LossAfterProofCountsAgainstNewWindow
ClosedRepairResetDoesNotRequireNormalActive
CurrentNormalWritesRequirePostCasNormalActive
```

Spike B只需证明Segment target local durable evidence和restart replay不会伪造range receipt；distributed F/reset仍由Spike C Model A负责。

---

## 1. 本轮可立即 LOCK 的精确合同

1. 同一 Arena authority domain内，durable `MOVE_COMMIT` 是compaction relocation的唯一authoritative locator cutover。
2. `MOVE_COMMIT` 条件化绑定expected old authority、new location/generation、logical identity、payload digest和move generation。
3. 无 `MOVE_COMMIT` 时old authoritative；new copy只是orphan。
4. durable commit后derived locator可发布/重建；old free晚于cutover、new-pin阻断、reader drain和durable `FREE_AND_BUMP`。
5. `MOVE_PREPARE` 不是safety必需。
6. `MOVE_COMMIT` 可bounded batching/group commit，不要求per-entry fsync。
7. fast range/TailSummary/batch失败或局部预算耗尽必须deterministic fallback，不能改变recovery truth。
8. fallback从earliest unresolved coordinate开始，不能越过hole。
9. fallback复用同一immutable recovery context；authority generation变化则重读/重启。
10. continuation默认可volatile；crash后允许幂等重读，不要求per-entry durable checkpoint。
11. 单replica corruption不自动等于ledger terminal；valid evidence exhausted或irreconcilable proof conflict才可DATA_LOSS/terminal。
12. deadline/cancellation只终止attempt，不单独证明永久non-promotable。
13. `deleteEpoch` 只属于per-ledger-instance delete fencing/version。
14. delete delivery progress使用per-stream monotonic no-hole sequence和durable per-stream cursor。
15. cursor advance隐含对应delete effect或可验证non-applicability已经durable。
16. stream assignment、Bookie/storage incarnation和registration required-through必须cluster-authoritative。
17. 每Bookie适用stream集合有限且有硬上限；exact topology开放。
18. snapshot必须绑定stream/generation/covered-through/assignment和内容或效果digest；bootstrap是snapshot + complete suffix。
19. ordinary ensemble replacement不能替代old storage incarnation的decommission/wipe proof。
20. loss-budget reset按bounded immutable range证明，不能ledger-wide无条件清零。
21. reset要求claimed range每个ACK-eligible entry重新具备至少 `F+1` distinct declared domain的valid durable evidence。
22. standard membership CAS、local target durability、NORMAL_ACTIVE或generic COMMITTED单独都不能reset。
23. closed/historical range不需要NORMAL_ACTIVE；strong repair completion receipt可以reset。
24. current writable replacement若承担未来normal writes必须post-CAS NORMAL_ACTIVE，但历史prefix仍需独立coverage proof。
25. repair completion response loss必须幂等可重放；proof cut之后的loss计入新window。
26. repair/reset metadata成本为per-operation/per-range，不做per-entry MetadataStore update或control fsync。

## 2. 必须保持 OPEN

- `MOVE_COMMIT` exact record bytes、checksum、packing、batch/range大小。
- `MOVE_PREPARE` 是否实现及orphan GC策略。
- cross-Arena/device evacuation/relocation协议；首版应unsupported。
- compaction scheduler、lifetime policy、QoS和数值Gate。
- range/BatchRecovery exact wire schema、continuation token编码。
- general E/W/A merge伪代码。
- volatile proof cache和可选durable operation checkpoint的具体结构。
- fast-path及global attempt count/bytes/retry/deadline数值。
- delete stream拓扑：global shard、per-Bookie inbox或其他。
- stream数、assignment存储、event batching和snapshot物理schema。
- snapshot内容表示、签名/proof、retention和最大rejoin window。
- decommission/wipe/replacement terminal proof的exact administrative流程。
- repair coverage receipt的encoding、range digest/bitmap/Merkle选择。
- failure-domain exact定义、默认F、domain identity来源和policy更新流程。
- loss-generation authority的物理namespace和retention。
- overlapping repair coverage map的压缩/compaction实现。
- 所有性能阈值。

## 3. 必须同步修改的文档/章节

#### RFC-0003

- §5 `ArenaControlLog`：增加 `MOVE_COMMIT` semantic record。
- §5.1 Authority：加入conditional predecessor/replay规则。
- §10 Delete/Free/Reuse：拆分record relocation和whole-block free条件。
- §11 checkpoint：保证relocation authority被checkpoint或suffix覆盖。
- §12 crash matrix：加入move commit/response loss/cutover/drain/free。
- §15 Derived index：明确从 `MOVE_COMMIT`重建。
- §17 invariants、§18 Gates、§19 OPEN：加入本轮LOCK与cross-Arena OPEN。

#### RFC-0004

- §6 TailSummary：补deterministic fallback和单replica corruption边界。
- §7 Recovery Range：修正第204行“任何budget超限直接non-promotable”的冲突。
- §7.1 BatchRecoveryAdd：加入partial result→earliest unresolved fallback。
- §9.1 RepairIntent：区分 replica committed 与 loss-budget reset proven；加入coverage proof。
- §12 offline rejoin：删除 `localDeleteAppliedThrough >= clusterRequiredDeleteEpoch`，改为stream/assignment/cursor/snapshot合同。
- §14 concurrency：加入registration vs delete stream publication、repair completion vs loss generation。
- §16 invariants、§17 Model D/E、§18 Gate、§19 OPEN同步。

#### RFC-0005

- §2 owner边界保持不变。
- §9 restart：消费RFC-0003 relocation commit和RFC-0004 stream cursors。
- §10 performance：明确move group commit不是normal Add fsync。
- §11 invariants：加入relocation cutover不改变local success。
- §13 relocation OPEN项：替换为本轮已LOCK的消费顺序，仅保留exact packing/cross-device开放。

#### Spike B

- prototype加入 `MOVE_COMMIT`。
- authority checker加入unique committed move chain。
- B10扩展所有crash/response-loss/concurrent move/derived-index rebuild场景。
- safety gate加入uncommitted-copy authority、free-before-drain等计数。
- write-amplification accounting包含move control bytes。

#### Spike C

- Model C加入relocation actions/invariants。
- Model D加入delete streams、assignment、incarnation、snapshot、cursor。
- Model E加入deterministic fallback equivalence。
- Model A加入range-scoped repair completion和loss reset。
- A-REPAIR-LOSS不再只是场景名，必须覆盖具体reset proof和delayed response。

#### turbo-bk.md

- Authority层次中把 `ArenaControlLog` 扩充为allocation/generation/relocation selection authority。
- 跨RFC安全不变量加入：
  - committed move cutover；
  - deterministic recovery fallback；
  - no-hole delete stream rejoin；
  - range-scoped repair reset。
- Model A/C/D/E范围同步。
- 当前开放问题删除已锁语义，仅保留exact topology/encoding/threshold。

#### Round 3 grill 归档

- 新增独立 Round 3 文档，完整保存本回复。
- README在integration audit完成前保持 `Review Complete / Integration In Progress`。

## 4. 新的 P0 依赖或矛盾

存在四个当前必须修复的跨文档矛盾：

1. RFC-0003 声称自己拥有durable relocation authority，但没有selection record；Spike B却使用“authoritative locator”作为oracle。`MOVE_COMMIT`关闭该循环。
2. RFC-0004 一边说TailSummary失效fallback，一边把所有fast budget超限直接写成non-promotable。必须区分fast budget和global attempt outcome。
3. RFC-0004 用per-ledger `deleteEpoch`作为跨ledger Bookie watermark，类型/序关系不成立。
4. Round 1/Spike C要求“last proven repair”，但RFC-0004当前 `COMMITTED`没有F+1 range coverage语义，写期replacement又不复制历史数据。

不需要新增独立RFC，但出现一个必须明确的P0 owner补充：

- RFC-0004拥有cluster repair completion与loss-budget reset proof语义；
- RFC-0001/Profile拥有F与declared failure-domain policy；
- RFC-0005只提供/消费Bookie local durable evidence和角色；
- Spike C验证distributed coverage/reset。

cross-device relocation不是首版P0新增依赖：只要首版明确限制同一 Arena authority domain。若首版要求device evacuation，则它成为新的P0协议，不能由本轮 `MOVE_COMMIT`自动覆盖。

## 5. 下一轮最值得 grill 的 frontier

按依赖顺序，下一轮应聚焦“proof与bounded compaction”，而不是提前选bytes：

1. **Repair coverage receipt / loss generation authority**
   - 如何以bounded range receipt证明F+1；
   - failure-domain identity/policy generation如何固定；
   - delayed loss与completion如何排序；
   - overlapping intervals如何compact而不无界增长。

2. **Delete stream assignment / snapshot trust**
   - assignment变更与registration CAS；
   - snapshot必须承载哪些delete effects；
   - old storage incarnation的wipe/decommission proof；
   - 在不选最终拓扑前冻结boundedness Gate。

3. **ArenaControlLog relocation checkpoint/GC**
   - `MOVE_COMMIT` chain如何进入checkpoint；
   - orphan new allocation如何安全回收；
   - same-Arena首版与cross-Arena unsupported边界；
   - group-commit completion与reader cutover接口。

4. **Recovery outcome taxonomy**
   - `RETRYABLE/DEFERRED/CANCELLED/QUARANTINED/DATA_LOSS`的最小语义；
   - fast budget与global attempt budget；
   - Model E全 point-read oracle及可选checkpoint边界。

本轮完成后，四条方向都能进入规范集成，但还不能宣称整体 Implementation Ready；下一轮至少需要关闭上述proof owner与bounded-state frontier。

<!-- END VERBATIM REVIEW -->


## Integration Audit 1 完整回复

<!-- BEGIN VERBATIM INTEGRATION AUDIT 1 -->

## Round 3 Integration Audit 1

**结论：PASS。**

当前未提交集成准确实现了 Round 3 的 LOCK/REVISE 结论，没有发现必须修复的正确性缺口、热路径性能回退、复杂度膨胀或过度设计。可以完整归档本回复，将 Round 3 更新为 `Reviewed / Integrated`，随后只提交本轮 9 个文档并推送。

### 1. 基线与工作区边界

已核对：

- HEAD：`4849e7559ad816cbea7ac5e76885138ab12cb9df`
- `HEAD...origin/turbo-bk`：`0 / 0`
- 变更范围：8 个 tracked 文档和新增 Round 3 归档，共 9 个本轮文件。
- 无关 `BtrLog Low-Latency Logging.pdf` 仍为未跟踪文件，未被读取、修改或纳入范围。
- 未发现其他源码、生成物或文档变更。

### 2. RFC-0001：PASS

`permanentLossBudgetF` 与 failure-domain policy 被正确放在 immutable Profile safety contract 中：

- normal ACK 至少覆盖 `F + 1` 个 distinct declared domains；
- policy identity/generation 不能在 repair proof 中被重新标注，以伪造覆盖；
- RFC-0004 只有在 bounded range 完整恢复并发布强 completion proof 后才能重置 loss window。

没有提前冻结：

- failure-domain 的 exact 定义；
- domain identity 来源；
- 默认 `F`；
- policy evolution 与 compatibility rules；
- 最终 descriptor wire fields。

这些均仍明确位于 OPEN。RFC-0001 只拥有不可变 policy 语义，没有侵入 repair protocol 或 proof encoding。

### 3. RFC-0003：PASS

同一 Arena relocation 合同完整且最小：

- 首版只支持 old/new allocation 属于同一线性化 `ArenaControlLog` authority domain；
- cross-Arena/device relocation 明确 unsupported / OPEN；
- 没有把单边 `MOVE_COMMIT` 扩展为隐式分布式事务；
- `MOVE_COMMIT` 条件绑定 predecessor、ledger instance、logical identity/range、old/new location+generation、payload digest、operation identity/generation；
- 同一 predecessor 只有一个 winning successor；
- new payload 必须先 durable，`MOVE_COMMIT` durability 才是 locator authority cutover；
- 无 commit 时 old authoritative，new copy 只是 orphan；
- commit 后 derived index 丢失仍可从 durable move chain 重建；
- old allocation 的 whole-allocation free 晚于 cutover、阻断新 old pin、reader drain 和 durable `FREE_AND_BUMP`；
- 一个 record move 不会被误解释为整个 shared allocation 已可回收；
- checkpoint 必须包含 relocation authority，或保留能唯一重建 move chain 的 suffix。

Crash/response-loss 行为也一致：

- payload durable、commit 前 crash：old 仍 authoritative；
- commit durable、response loss：按 operation identity 重放，不能生成 competing move；
- commit durable、locator publication 前 crash：restart 从 control authority 重建；
- new payload digest/identity 无法验证：fail closed，不能让 old copy自行夺回 authority。

性能边界正确：有界 move records 可以共用 group-commit barrier，不要求 moved entry 各自 fsync；relocation 不产生新的 local success、AQ 或 ACK。没有给 normal Add 增加控制日志 I/O。

### 4. RFC-0004 fallback：PASS

fast-path 与 recovery truth 已正确分离：

- fast-path count/bytes/retry 是局部预算；
- 局部预算耗尽只触发 deterministic fallback；
- caller deadline/cancellation 只终止本次 attempt；
- authoritative fallback 暂时不可用返回 retryable/deferred；
- 只有 valid evidence exhausted 或不可调和的 identity/quorum proof conflict 才能进入 terminal quarantine / DATA_LOSS；
- 单个 corrupt replica 只使该份 evidence 无效，不能直接判 ledger 不可恢复。

Fallback 合同完整：

- 使用同一个 immutable `RecoveryContext`；
- 从 earliest unresolved coordinate 开始；
- 复用或重读同 context 下已验证 evidence；
- 不跨越较早 hole 发布 frontier；
- 回到 bounded-concurrency point read 与 idempotent single-entry recovery-add；
- authority generation 变化时重读并证明 context 等价，或在新 generation 下重新开始。

Continuation、bitmap 和 proof cache 默认是 bounded volatile state。coordinator crash 最多导致重读，不会把未缓存 evidence 当成 absence；没有引入 per-entry MetadataStore progress、durable checkpoint 或 control fsync。operation-scoped checkpoint 仍只是 benchmark 证明需要后才考虑的可丢弃优化。

### 5. RFC-0004 loss reset：PASS

replica/membership `COMMITTED` 与 `LOSS_BUDGET_RESET_PROVEN` 已明确分离。下列任一单独事实均不能 reset：

- ensemble membership CAS；
- replacement target local durability；
- `NORMAL_ACTIVE`；
- generic `COMMITTED`。

Reset proof 正确绑定：

- ledger instance；
- bounded immutable range/cut；
- sealed/fenced metadata 和 ensemble/write-set history；
- failure-domain policy generation；
- repair operation/generation；
- observed loss generation；
- exact membership mapping；
- complete per-entry payload/identity evidence；
- proof cut 上每个 ACK-eligible entry 的 `F + 1` distinct-domain coverage。

角色区分正确：

- closed/historical fragment 不需要 `NORMAL_ACTIVE`，但必须有完整 coverage 与 durable strong completion；
- current writable replacement 需要 post-CAS `NORMAL_ACTIVE` 才能承接未来 Adds；
- activation 只授权未来写入，不证明旧 ACK prefix 已修复；
- write-time replacement 不复制旧 fragment，因此不能自动清零旧 loss window。

Reset 线性化在条件化 durable repair completion CAS。response loss 可幂等重放；proof cut 后的新 loss 进入新 window；迟到 completion 与 overlapping repair 不能抹掉新 loss。证明保持 operation/range scoped、bounded streaming 和一个 completion CAS，没有 stop-the-world、全 ledger 重复制、per-entry metadata、per-entry fsync 或无界 proof history。

### 6. Delete stream / offline rejoin：PASS

`deleteEpoch` 已正确限制为 per-ledger-instance fencing/version，不再兼任 catch-up cursor。

最低合同完整：

- stream sequence 单调、无洞；
- cursor 绑定 stream identity/generation、Bookie stable identity 和 storage/device incarnation；
- delete effect durable 后才能推进 cursor；
- unexplained missing sequence 阻止 writable；
- cluster-authoritative assignment 给出有限 applicable stream set 和 required-through vector；
- applicable stream 数存在 manifest-locked finite maximum；
- 禁止每 ledger 一个长期 stream；
- 新 storage incarnation 不能继承旧 cursor；
- ordinary ensemble replacement 不能替代 irreversible wipe/decommission proof；
- snapshot 绑定 stream、snapshot generation、covered-through、assignment/membership generation 和 effect/proof digest；
- bootstrap 只能是 verified snapshot + complete no-hole suffix；
- registration CAS 校验同一 assignment generation、incarnation 和 required-through vector；
- delete publication 与 registration 有明确线性化 cut。

这既避免了无限 cursor vector，也没有强制一个全局 sequence。per-Bookie inbox、固定 shard 或 hierarchical snapshot 等物理拓扑仍保持 OPEN。

### 7. RFC-0005、Spike B/C 与总体 RFC：PASS

同步完整：

- RFC-0005 只消费 RFC-0003 relocation authority 和 RFC-0004 delete/recovery authority，不复制其 schema；
- restart 按 durable move chain 重建 locator，并在 delete stream/incarnation reconciliation 完成后才注册 writable；
- RFC-0005 性能边界明确禁止 Add-time per-entry control fsync 和 remote metadata I/O；
- Spike B 覆盖 concurrent/chained move、torn group、digest mismatch、response loss、derived index deletion、checkpoint rotation、partial-block free 和 reader pin；
- Spike B accounting 包含 move control bytes、durability barriers、foreground p99，且 move 不创造 local success；
- Spike C 的 Model A/C/D/E 已加入 loss reset、move authority、delete stream/incarnation、fallback oracle equivalence 所需 state/action/invariant；
- Model E 明确验证 fast path + fallback 与全 point-read oracle 等价；
- `docs/turbo-bk.md` 的 authority、cross-RFC invariants、模型范围、Stage 边界和 OPEN 项均已同步；
- 没有把 Round 3 集成误写成 RFC Accepted、feature enabled、canary ready 或 implementation ready；
- section numbering 连续，没有重复章节或新 owner 冲突。

### 8. Round 3 归档与文档验证：PASS

Round 3 文件：

- 919 行、43,375 bytes；
- `BEGIN VERBATIM REVIEW` / `END VERBATIM REVIEW` 各一个；
- Q1–Q4 的结论、最小不变量、顺序、crash/response-loss、性能、复杂度、Spike falsification、LOCK/OPEN/依赖/frontier 均存在；
- 唯一规范化是 heading level 统一下移，且文件显式披露了该处理；
- 主 agent 报告的原 session JSONL 32,461-character 内容比较为 PASS。

因此这满足“不丢失 reviewer 反馈”。机械 heading 调整和 trailing-whitespace 清理不属于反馈删节。

README 当前仍是 `Review Complete / Integration In Progress`，在本次 audit 完成前状态正确。

验证结果：

- `git diff --check`：PASS；
- Round 3 新文件 whitespace check：PASS；
- 13 个 Markdown 文件单 H1：PASS；
- fenced code block：PASS；
- repo-root/relative link validation：PASS；
- branch ancestry：`0 / 0`。

### 性能、复杂度与过度设计总评

没有发现实质性能回退：

- normal Add 无新增 MetadataStore I/O；
- normal Add 无新增 per-entry control fsync；
- recovery payload 无 per-entry metadata round trip；
- relocation 允许 bounded batching/group commit；
- loss reset 是 operation/range 级持久化；
- fallback 的内存和并发要求保持 bounded；
- delete stream 不要求全局串行 sequence。

没有引入不必要的复杂度：

- relocation 复用单 Arena authority，没有新增分布式事务；
- repair reset 复用 RepairIntent、membership CAS 和有界 completion authority；
- delete rejoin 只增加安全所必需的 assignment/incarnation/cursor/snapshot 事实；
- continuation 默认不 durable；
- exact topology、encoding、packing 和数值阈值均未提前冻结。

### 仍需保持 OPEN 的内容

以下内容没有被误写成最终合同：

- `MOVE_COMMIT` exact bytes、checksum、packing、batch/range size、`MOVE_PREPARE` 与 orphan GC；
- cross-Arena/device evacuation/relocation protocol；
- fast-path/global-attempt 数值预算、exact error mapping；
- optional operation checkpoint encoding/durability；
- failure-domain exact definition、identity source、默认 `F`、policy evolution；
- repair receipt encoding、loss-generation namespace、overlap interval compaction；
- delete stream topology/count、assignment storage、event batching、snapshot encoding/proof、retention；
- decommission/wipe authorization 和 durable proof；
- exact performance thresholds。

两个非阻塞措辞建议可留到后续编辑：

- RFC-0004 的 owner 列表未来可显式把“range-scoped completion/loss reset proof”补到 RepairIntent/RFC-0004 owner 描述中；当前 §9.2 已有完整规范，未形成 ownership 歧义。
- Spike B 的 “active locator” 后续可澄清为 “authoritative lookup locator”，避免与已存在的 old reader pin 混淆；当前 oracle 和 reader-drain 顺序已经排除了错误解释。

二者都不是本次提交阻塞项。

### 发布许可

可以执行：

1. 将本回复完整追加到 Round 3 归档；
2. 将 README 的 Round 3 状态改为 `Reviewed / Integrated`；
3. 只暂存以下 9 个文件：
   - RFC-0001
   - RFC-0003
   - RFC-0004
   - RFC-0005
   - grill README
   - Round 3 归档
   - Spike B
   - Spike C
   - `docs/turbo-bk.md`
4. 明确排除 `BtrLog Low-Latency Logging.pdf`；
5. 重新执行 whitespace/link/ancestry 检查后 commit 并 push。

本次 PASS 只表示 **Round 3 reviewer 结论已准确集成**，不提升任何 RFC、Spike、canary 或实现就绪状态。

<!-- END VERBATIM INTEGRATION AUDIT 1 -->
