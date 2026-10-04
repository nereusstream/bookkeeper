# Spike C 有限模型实施

状态：EXPERIMENTAL / NON_PROMOTABLE / NO_AUTHORITY / FINITE_SLICE_PASS。当前Model A Bookie权限快照及Model C既有切片、native MOVE覆盖和合批compaction投影共19个完整正向配置、48个指定性质反例；当前回执见末节。不是完整Model C、A+C或Block G验收，历史回执保留当时范围。

合同由[RFC-0003](../../RFC-0003-segment-storage-allocator.md)、[RFC-0005](../../RFC-0005-segment-bookie-state.md)及[Spike C](../../spikes/SPIKE-C-no-object-tla.md)拥有。`UnifiedWalTypes.tla`仅定义纯函数；`SegmentAllocator.tla`按可读写入、文件barrier、连续前缀发布、控制durable/apply、CLOSE和恢复阶段分别建模；`ArenaCheckpoint.tla`单独检查applied快照、payload force、控制commit、A/B发布及保留完整日志时的恢复；`BookieCheckpoint.tla`检查固定权限轨迹的快照、后缀及已发布image覆盖的prefix回收；`ArenaRotation.tla`检查双根、新文件/目录同步及旧prefix回收，`DerivedCoverage.tla`检查重建代/单CF flush后才发布coverage，`IndexMaintenance.tla`检查有界hot与共享DATA执行中的索引停顿/失败、query-visible接管/热淘汰、坐标占用及控制预留，`RuntimeCoverage.tla`另检查同一index generation内冻结cut、flush、A/B发布及连续publication序号；`TailRecovery.tla`检查内容匹配的持久覆盖、固定头/缺口、必要tail同步及当前tombstone选择；`AllocationOrder.tla`检查原分配顺序与旧尾部隔离，`WholeAllocationFree.tla`检查全死proof、实际I/O/pin排空、提交保护与FREE barrier/generation；`WholeAllocationReuse.tla`检查实际旧I/O终结、FREE前序、当前grant与健康CLOSED末端区分旧残留；`TerminalIndexCleanup.tla`检查捕获准确终态、实际旧Put/读义务、native删除先于坐标释放、取消及有界容量恢复；`RelocationCompletion.tla`检查copy实际barrier、连续前缀、内部结果与原池两方终结；`ConditionalMove.tla`检查COPY事实、前驱条件、CONTROL durable/apply、同批双坐标原子选择、读保护与metadata-only重放；`MoveFreeReuse.tla`另检查当前目标、selector cut、源/orphan退役、grant generation复用与控制定位恢复；不使用对象存储。

## 复现与输入绑定

```bash
python3 scripts/unified-wal/run-storage-models.py --tool /path/to/tla2tools.jar
```

工具固定为[TLA+ 1.7.4 官方发行](https://github.com/tlaplus/tlaplus/releases/tag/v1.7.4)的`tla2tools.jar`，发行SHA-1为`bee4a54f3ee3d4afc347c3240ec2d9e93b075104`；runner验证它并记录实际SHA-256，不自动下载或改Maven依赖。本次取得的完整文件为2,274,532 bytes，SHA-256为`936a262061c914694dfd669a543be24573c45d5aa0ff20a8b96b23d01e050e88`，manifest记录工具版本和checksum。使用当前Java，1 worker、512 MiB heap、64 MiB direct上限、固定fingerprint polynomial 0与seed 1；每config最多120秒，无symmetry或额外state constraint。安全性检查不增加fairness；有界停止/隔离状态允许无后继，未检查deadlock或liveness。

每次运行在独立临时目录冻结modules/configs，保存manifest、命令、JDK/平台、源码哈希、父提交处的合同哈希、checker输出、完整负向trace、coverage-summary及checksums。统计记录generated/distinct states、剩余queue、searchDepth、仅完整探索时的diameter、elapsed及`time`测得的进程peak RSS。负向检查在发现反例时停止，其搜索深度不是完整图直径。超时终止本次checker进程组并标INCONCLUSIVE；语法/工具错误及未完整探索都不是PASS。正式配置或源码改变须新建run，不能改绑定旧结果。

## 矩阵与范围

| 配置 | 有界结构与目的 |
| --- | --- |
| `c-relocation-completion` | 2个copy batch的实际IO、原owner结果、连续前缀与原池释放；copy不发布普通定位 |
| `ce-relocation-early-facts` | 故意force即返回内部事实；必须违反`CopyFactsHaveBarrierAndPrefix` |
| `ce-relocation-ordinary` | 故意将copy发布为普通定位；必须违反`CopyDoesNotPublishOrdinary` |
| `ce-relocation-cancel` | 故意取消即释放池；必须违反`BuffersWaitForBothTerminals` |
| `c-terminal-index` | 2个旧坐标/2行native、0/1旧Put/读/实际删除I/O、1个后继写，固定终态cut后真实删除与owner释放 |
| `ce-terminal-index-late-put` | 故意忽略旧Put；必须违反`ReleasedCoordinatesHaveNoNativeRows` |
| `ce-terminal-index-before-delete` | 故意先释放坐标；必须违反`ReleasedCoordinatesHaveNoNativeRows` |
| `ce-terminal-index-new-tombstone` | 故意按后来的终态集合释放；必须违反`OnlyCapturedTerminalCoordinatesRetire` |
| `ce-terminal-index-cancel` | 故意取消即解冻；必须违反`MaintenanceGateHeldThroughActualDelete` |
| `c-whole-reuse` | 1个整块、0/1实际旧/新I/O、0/1当前prefix、1次crash，只有健康CLOSED可以恢复当前数据 |
| `ce-whole-reuse-before-free` | 故意提前复用；必须违反`ReuseHasTerminalOldIoAndFree` |
| `ce-whole-reuse-before-io` | 故意在新I/O终结前CLOSE；必须违反`ClosedTailHasActualDurableIo` |
| `ce-whole-reuse-old-bytes` | 故意把旧suffix当当前记录；必须违反`OldBytesNeverBecomeCurrent` |
| `c-whole-free` | 2个共享occupant、1次旧I/O/reader pin、1次crash；实际终态与quiescence proof后持有gate至CONTROL durable/原owner apply |
| `ce-whole-free-partial-death` | 故意漏检活record；必须违反`EveryOccupantWasTerminal` |
| `ce-whole-free-early-release` | 故意提前close提交保护；必须违反`SubmittedFreeRetainsProtection` |
| `ce-whole-free-reboot-pins` | 故意以重启计数替代旧实际I/O/未关闭会话判断；必须违反`RebootCountsDoNotProveOldIoTerminal` |
| `ce-whole-free-before-force` | 故意在CONTROL barrier前bump；必须违反`FreeAuthorityHasBarrier` |
| `c-tail-recovery` | 3个DATA位置、cut 0..3、匹配/缺失/后来改变的index内容、正常/有物理缺口及tombstone；覆盖内只查header、其他body检查后sync |
| `ce-tail-recovery-invalid-index` | 故意用不匹配的index授权跳过；必须违反`SkippedBodyHasDurableMatchingIndex` |
| `ce-tail-recovery-before-sync` | 故意在必要DATA sync前开放；必须违反`ReadyHasRequiredDataSync` |
| `ce-tail-recovery-ignore-gap` | 故意允许物理序号缺口开放；必须违反`ReadyHasContiguousPhysicalPrefix` |
| `c-runtime-coverage` | 3个单stream DATA坐标、最多3 publication、1次crash；冻结cut后暂停Put，实际flush后发布 |
| `ce-runtime-before-flush` | 故意在flush前发布；必须违反`RuntimeCoverageWasFlushed` |
| `ce-runtime-clock-reset` | 故意由旧contents是否匹配重置publication序号；必须违反`PublicationClockAdvances` |
| `ce-runtime-late-cut` | 故意以发布时的新DATA范围替换冻结cut；必须违反`RuntimeCoverageStopsAtCapturedData` |
| `c-index-maintenance` | 3个坐标占用容量、2个hot容量、2个DATA票据、1次索引暂停；query-visible之后淘汰hot，保留定位与控制额度 |
| `ce-index-control-credit` | 故意让索引占控制credit；必须违反`FenceCanUseReservedLane` |
| `ce-index-early-eviction` | 故意在Put前淘汰hot；必须违反`LocalSuccessKeepsLocatorAuthority` |
| `c-derived-coverage` | 2个已选定DATA entry、最多3 index代、1次故障；先退休旧coverage、完整flush再force发布 |
| `ce-derived-before-flush` | 故意在完整Put后、flush前发布；必须违反`CoverageWasPersisted` |
| `c-managed-shared` | 2 stream × 2 batch、共享1个DATA文件、最多2 epoch/2次进程退出/2次掉电；当前合同的正向检查 |
| `c-managed-split` | 相同结构分属2个DATA文件；当前合同的正向检查 |
| `ce-data-before-barrier` | 故意允许完整write替代DATA barrier；必须违反`DataSuccessHasBarrier` |
| `ce-control-before-sync` | 故意允许未同步可读控制记录直接apply；必须违反`ControlHasBarrier` |
| `ce-open-replay-after-error` | 故意允许OPEN在校验/sync后直接取得恢复资格；必须违反`KnownErrorRemainsIsolated` |
| `c-arena-rotation` | 1次物理rotation/1次掉电/1次image或publication损坏，双恢复根与新文件及目录barrier后回收，保留已确认tail |
| `ce-arena-rotation-early-reclaim` | 故意允许仅一份恢复根后回收，随后该页损坏，必须违反`SurvivingAuthority` |
| `c-checkpoint` | 3条控制命令（第2条条件失败）、2个slot、最多3代checkpoint/2次进程退出/1次掉电；完整控制日志保留 |
| `ce-checkpoint-durable-cut` | 故意取durable cut替代applied cut；必须违反`SnapshotCutWasApplied` |
| `ce-checkpoint-early-publication` | 故意在控制commit持久前发布；必须违反`PublishedImageHasDurableCommit` |
| `a-bookie-checkpoint` | 1 ledger、固定10条已认证控制记录、2次snapshot/2次process crash；恢复权限与完整applied历史一致 |
| `ce-bookie-checkpoint-terminal` | 故意在image丢失fence/tombstone；必须违反`BookieTerminalPermissionsRemainDenied` |
| `a-bookie-rotation` | 同一1 ledger/10记录/2snapshot/2crash，允许回收image覆盖的prefix；权限仍等于完整applied历史 |
| `ce-bookie-rotation-terminal` | 丢失image terminal字段，回收范围中的恢复权限必须违反`RetiredBookieTerminalPermissionsRemainDenied` |

负向配置是被否证的候选规则，不是生产功能开关，也不是实际Java源码变异。它们的EXPECTED_COUNTEREXAMPLE用于检查性质能否发现指定错误；不能计入正向PASS。完整trace保存模型oracle`faultSeen`：它在进程退出后仍记录先前I/O错误，而恢复进程只能读取控制与DATA镜像。该oracle不是实现新增的持久日志。恢复同步只建立当前bytes的durability，不自动证明先前错误已经解析。

模型假设输入grant、storage identity、Bookie权限及framing已验证，旧进程退出已终结模型中的提交者；batch冻结后不可变、旧授权范围不覆盖/复用。durable bytes在掉电时保留，可读未同步bytes可丢失。有限模型不证明真实fsync、设备隔离、目录同步、CRC、字节预算或NUMA。未展开allocation条件命令全集、generation reuse、MOVE/FREE、完整在线rotation、长期历史压缩及一般MOVE/FREE覆盖恢复、worker冻结与额度协调、损坏/部分framing、任意Bookie route变更/跨ledger竞争、quorum/客户端、删除和进展；它们仍须后续Model A/C/D及A+C/C+D配置验证。

性质和真实实现/测试的映射分别见[invariant mapping](invariant-mapping.md)及[action mapping](action-to-test-mapping.md)。执行结果由本页后续验证记录及各独立run拥有，不能以此切片提升Block G或生产authority。

## 当前验证状态

起步记录：2026-10-03提交`cb5b4cc8fe`时工具下载失败，只有Python AST、配置与文档静态检查，状态为UNPARSED / NOT_EXECUTED。缺工具时的ClassNotFoundException属于准备失败，不是模型结果。

管理会话首次回执为`unified-wal-model-jqvpwfsw`，在macOS/aarch64、Corretto 21.0.10上运行。模型、配置及7个Java输入来自`cb5b4cc8fea199365ddf4cf8e0b57e39755500b6`；runner增加searchDepth并仅在完整探索时记录diameter。manifest绑定全部15个实际输入的SHA-256和父提交处合同，最终输入及回执checksums已逐项核对。没有修改模型规则、有限参数或Java恢复行为，也未重跑无变化的Java测试/style。

| 配置 | 结果 | generated / distinct | 剩余queue | 搜索深度 / 完整图直径 | elapsed秒 | peak RSS bytes |
| --- | --- | --- | --- | --- | --- | --- |
| `c-managed-shared` | PASS | 95,530 / 44,007 | 0 | 37 / 37 | 1.559 | 281,198,592 |
| `c-managed-split` | PASS | 125,075 / 50,859 | 0 | 38 / 38 | 1.762 | 281,247,744 |
| `ce-data-before-barrier` | EXPECTED_COUNTEREXAMPLE | 45 / 40 | 19 | 7 / — | 0.800 | 113,328,128 |
| `ce-control-before-sync` | EXPECTED_COUNTEREXAMPLE | 17 / 16 | 7 | 5 / — | 0.809 | 112,328,704 |
| `ce-open-replay-after-error` | EXPECTED_COUNTEREXAMPLE | 3,263 / 1,863 | 844 | 12 / — | 0.922 | 153,550,848 |

两个正向输出均确认complete exploration、无性质错误，18个Next动作的coverage均非零。负向完整trace及checker原始输出由回执保存：

- DATA候选规则在`WriteComplete → PublishLive`后令`successes={<<1,1>>}`而`dataDurable={}`，违反`DataSuccessHasBarrier`。
- 控制候选规则在`AppendOpen → CrashProcess → StartRecovery → RecoverControl`后令applied=1而durable=0，违反`ControlHasBarrier`。
- OPEN候选规则在`Submit → FailData → CrashProcess → RecoverControl → ValidateData → RecoverDataBarrier → QualifyRecovered`后令phase=ready、faultSeen=TRUE、faultDurable为空，违反`KnownErrorRemainsIsolated`。最短反例没有已写DATA；它检查错误历史的丢失及错误资格提升，不是已成功entry掉电丢失的物理证明。

这些结果只覆盖上述有限切片。未正常关闭会话的故障解析证据/合同仍待确认；旧I/O错误不能因后次同步而被视为已解析。完整Model A/C/D、组合模型、真实掉电、checkpoint/index/回收及服务级恢复仍未完成，Block G及production authority保持不变。


### Arena checkpoint 增量验证

此前本轮回执`unified-wal-model-lfdt0ywk`的临时目录已不可用，历史记录不重绑；重新取得同checksum工具并以独立run验证。

2026-10-03新回执`unified-wal-model-thb7_nqa`运行全部8个配置，得到3个正向PASS和5个EXPECTED_COUNTEREXAMPLE。父提交为`f11660524b5439bf0ea8f88d017441b18421c9d4`；全部20个实际model/config/runner/Java输入由manifest哈希绑定，不重绑上述历史回执。主机为macOS arm64，TLC输出记录JDK为Corretto 21.0.10 x86_64；运行参数、工具checksum及超时保持不变。

| 新配置 | 结果 | generated / distinct | 剩余queue | 搜索深度 / 完整图直径 | elapsed秒 | peak RSS bytes |
| --- | --- | --- | --- | --- | --- | --- |
| `c-checkpoint` | PASS | 476,073 / 179,279 | 0 | 44 / 44 | 3.756 | 378,699,776 |
| `ce-checkpoint-durable-cut` | EXPECTED_COUNTEREXAMPLE | 20 / 19 | 12 | 4 / — | 0.720 | 114,884,608 |
| `ce-checkpoint-early-publication` | EXPECTED_COUNTEREXAMPLE | 73 / 56 | 31 | 5 / — | 0.768 | 116,064,256 |

checkpoint正向17个Next动作coverage均非零。第一个负向trace为`Append → ForceLog → Freeze`，捕获的through大于capturedApplied；第二个为`Freeze → WriteImage → ForceImage → Publish`，发布时没有durable commit。当前Java分别从appliedThrough/appliedOffset冻结并在CHECKPOINT_COMMIT持久、顺序apply后发布，负向配置不是Java源码变异或生产开关。

模型的cut是命令数量（0表示空），Java为控制sequence（-1表示空）；模型slot为1/2，Java为0/1。控制framing、storage identity、文件完整性判定与force本身作为已验证输入/动作抽象，损坏以image valid=false表示；固定条件命令只包含两个成功grant和一个no-op，不证明完整allocator predicate。恢复oracle比较checkpoint+完整后缀与全历史结果，选择最高完整、forced、有匹配commit的发布代，并在暴露前同步发布页；不可用时从仍完整保留的日志恢复。模型未展开字节CRC/SHA、瞬时内存/磁盘预算、Bookie checkpoint、在线调度、日志前缀回收、MOVE/FREE/index或真实掉电。这些仍是后续实现与Gate，不从本切片推出长期运行或完整Model C完成。


### Bookie checkpoint 权限恢复切片

2026-10-03回执`unified-wal-model-pz77hf26`在父提交`6402c1c64ecfa991ffff5576f88f285e00179148`上绑定24个实际输入（4个TLA module、10个cfg、runner、9个Java），保留父提交处3份合同哈希；历史Arena回执不重绑。全部10个配置得到4个完整正向PASS、6个指定性质反例；原8个配置保持原状态/计数，新配置如下。仍为macOS arm64主机、Corretto 21.0.10 x86_64、原固定tool checksum/worker/heap/fp/seed/120秒上限。

| 新配置 | 结果 | generated / distinct | 剩余queue | 搜索深度 / 完整图直径 | elapsed秒 | peak RSS bytes |
| --- | --- | --- | --- | --- | --- | --- |
| `a-bookie-checkpoint` | PASS | 90,586 / 28,292 | 0 | 31 / 31 | 1.561 | 277,725,184 |
| `ce-bookie-checkpoint-terminal` | EXPECTED_COUNTEREXAMPLE | 15,393 / 6,441 | 1,861 | 16 / — | 1.045 | 194,166,784 |

正向8个Next动作coverage均非零。模型固定install→activate→两次LAC→grant→close→fence→tombstone→旧activation/install identity重试，保存installed/active/fenced/tombstone、grant与已提交close事实、最新LAC和local generation的状态投影。Java image保存首次命令/实际条件结果、FENCE/commit及最新LAC，按原顺序重建；模型直接冻结上述权限字段，不证明全部conditional predicate或字节序列化。typed authority、scope/credential绑定、完整framing与C模型的payload/commit/publication barrier作为输入假设。

负向最短trace在7条可读控制记录经process crash、恢复sync/replay建立fenced状态后，`Freeze → Publish → Crash → RecoverySync → Recover`使image丢失terminal gate并恢复active，违反`BookieTerminalPermissionsRemainDenied`。该配置是被否证的候选序列化规则，未修改Java或生产开关。初次新增cfg因错误注释语法只得到INCONCLUSIVE，修正为TLC支持的注释后重新独立运行；未缩小常量或放松性质。

此切片只验证固定单ledger权限投影、已apply cut及保留完整日志的后缀恢复；Publish合并了由C切片单独检查的barrier步骤，不是A+C组合证明。没有展开多ledger/多Arena竞争、在途ticket/pending gate、冷metadata与TLS、credential加解密、CRC/SHA、双owner发布页I/O、tail分类、prefix reclaim/rotation、完整Model A/C/D或真实掉电。对应真实文件回归与范围见[存储实现](../local-storage/README.md)；服务readiness、Block G及production authority保持不变。

### Arena双根rotation切片

新回执`unified-wal-model-t1waf2bm`绑定父提交`00edeb8f`，冻结29个实际模型/config/runner/Java源码SHA-256及父提交处RFC输入。旧`jqvpwfsw`、`thb7_nqa`、`pz77hf26`的输入与结论不变。矩阵12配置实际为5个正向完整PASS、7个指定性质反例。

| 新配置 | generated / distinct / queue | depth / 完整diameter | elapsed / peak RSS | 结论 |
|---|---|---|---|---|
| c-arena-rotation | 332 / 161 / 0 | 19 / 19 | 0.850 s / 122,044,416 bytes | PASS |
| ce-arena-rotation-early-reclaim | 116 / 68 / 21 | 10 / 不适用 | 0.830 s / 112,115,712 bytes | EXPECTED_COUNTEREXAMPLE: SurvivingAuthority |

正向15个Next动作coverage均非零。模型只有一次rotation、一次掉电与一次image或publication损坏，两个cut的普通authority相同；内部commit identity/相邻sequence、条件结果和字节CRC/SHA由真实Java验证。先force image、日志及目录，再force两根才允许reclaim与普通tail ACK。每个发布根都绑定两份image，模型允许通过任一有效根使用任一有效image；负向在仅force一份根后Reclaim，随后该根损坏使唯一authority消失，checker实际捕获指定反例，没有修改Java开关。投影不检查重复rotation、动态操作语义、文件锁、I/O unknown、坏framing、Bookie组合、状态预算压缩或liveness，不构成完整Model C/A+C与真实掉电证明。

### Bookie回收prefix权限投影

2026-10-03新回执`unified-wal-model-bu9e_n9l`绑定父提交`43bfd80b603b500843fba067ff8b847320ebf116`，冻结31个实际输入（5个module、14个cfg、runner、11个Java）及父提交处3份合同哈希；参数及官方tool checksum不变。全部14个配置实际得到6个完整正向PASS、8个指定性质反例，历史回执不重绑。

| 配置 | generated / distinct / queue | depth / 完整diameter | elapsed / peak RSS | 结论 |
|---|---|---|---|---|
| a-bookie-rotation | 159,578 / 48,763 / 0 | 31 / 31 | 2.272 s / 293,011,456 bytes | PASS |
| ce-bookie-rotation-terminal | 26,865 / 10,883 / 3,134 | 17 / 不适用 | 1.362 s / 208,044,032 bytes | EXPECTED_COUNTEREXAMPLE: RetiredBookieTerminalPermissionsRemainDenied |
| a-bookie-checkpoint | 91,290 / 28,292 / 0 | 31 / 31 | 1.704 s / 286,670,848 bytes | PASS |
| ce-bookie-checkpoint-terminal | 15,524 / 6,441 / 1,861 | 16 / 不适用 | 1.307 s / 203,128,832 bytes | EXPECTED_COUNTEREXAMPLE: BookieTerminalPermissionsRemainDenied |

Bookie新增`RetirePrefix`常量和`retiredThrough`；只有已发布image覆盖的applied prefix才可退休，退休后恢复不允许全历史日志fallback。正向9个Next动作coverage均非零，`ReclaimPrefix`为6,864:10,725。已有checkpoint配置显式`RetirePrefix=FALSE`，distinct states与历史相同；增加禁用动作的评估改变generated统计，旧90,586/15,393统计仍由旧输入回执拥有。

正向检查全部权限等于完整applied历史、terminal拒绝和退休cut仍被checkpoint覆盖。负向最短trace先重放7条控制记录取得fenced状态，以丢失terminal字段的image再次恢复出active状态，最后`ReclaimPrefix`令退休域性质生效并捕获反例；不能将该trace表述为先回收、再掉电的物理故障顺序。负向仍是候选序列化规则，不是Java变异或生产开关。

此投影假设payload/commit/publication及双恢复根/目录barrier已经成立；共享物理协议由`ArenaRotation.tla`独立检查，Java分别验证两owner的identity/magic、原commit UUID、sealed credential、条件结果及mandatory suffix。两个有限模型未组合为A+C，也没有展开ticket生命周期、多ledger竞争、完整body密码学、重复rotation历史压缩、真实掉电或liveness。实际实现与回执见[本地Bookie rotation](../local-storage/README.md#bookie停写rotation与共享物理协议)，完整Model A/C/D、Block G及production authority仍未完成。


### 派生索引覆盖投影

2026-10-03回执`unified-wal-model-p_83_s8r`绑定父提交`78100b5ad9c999651cdb4eb577b444c895f0d549`，冻结36个实际输入（6个module、16个cfg、runner、13个Java）及父提交处3份合同哈希。全部16个配置得到7个完整正向PASS、9个指定性质反例；历史回执不重绑，工具及有限运行参数保持不变。

| 新配置 | generated / distinct / queue | depth / 完整diameter | elapsed / peak RSS | 结论 |
|---|---|---|---|---|
| c-derived-coverage | 72 / 53 / 0 | 16 / 16 | 0.805 s / 111,837,184 bytes | PASS |
| ce-derived-before-flush | 25 / 20 / 5 | 7 / 不适用 | 0.858 s / 111,575,040 bytes | EXPECTED_COUNTEREXAMPLE: CoverageWasPersisted |

正向8个Next动作coverage均非零。`BeginRebuild`在同一CF WriteBatch中更换index generation并删除旧定位，立即退休旧coverage；`ForceReset`持久化该批。`Write`只建立可查询定位，`Flush`建立完整持久内容，再由`Publish/ForcePublication`发布coverage。故障丢弃未同步内容，恢复只接受匹配当前index generation的覆盖；索引不可用时从同一份已选定DATA重新构建。负向最短轨迹为`BeginRebuild → ForceReset → Write → Write → Publish → ForcePublication`，缺少`Flush`，实际违反指定性质。

有限参数只有2个entry、最多3个index generation、1次故障；DATA/route选择和framing有效、单CF WriteBatch原子性及flush持久屏障是输入假设。模型不证明真实JNI、目录/设备同步、CRC、控制并发、RELOCATION_COPY选择、在线hot handoff、index stall、扫描跳过或全局资源预算。真实RocksDB、独立字节向量和三个子进程退出阶段由[本地存储验证](../local-storage/README.md#rocksdb冷重建与持久覆盖切片)独立记录，进程退出不代表真实掉电。完整Model A/C/D及组合、Block G和production authority仍未完成。


### 索引维护与控制预留投影

2026-10-03回执`unified-wal-model-tn4d2l9o`绑定父提交`56c6d8c1fe5fe0c46dc0d5eb7603aa3c364c01d2`，冻结41个实际输入（7个module、18个cfg、runner、15个Java），保留父提交处3份合同哈希。全部18配置为8个完整正向PASS、10个指定反例；工具、参数及历史回执不变，55个回执文件checksum已核对。

| 新配置 | generated / distinct / queue | depth / 完整diameter | elapsed / peak RSS | 结论 |
|---|---|---|---|---|
| c-index-maintenance | 1,125 / 482 / 0 | 15 / 15 | 0.818 s / 132,497,408 bytes | PASS |
| ce-index-control-credit | 29 / 21 / 12 | 4 / 不适用 | 0.787 s / 107,790,336 bytes | EXPECTED_COUNTEREXAMPLE: FenceCanUseReservedLane |

正向10个Next动作coverage均非零。模型区分hot准入、已durable DATA/热结果、维护排队、实际Put、query-visible后原owner终结及暂停/恢复/后台失败；保留hot而不淘汰，索引只能使用有界DATA票据。控制目标无未终结pre-cut DATA，必须始终可使用独立预留lane；负向最短轨迹`Submit → ForceData → StartIndex`将该credit交给索引，即违反`FenceCanUseReservedLane`，没有修改Java开关。

参数固定3个entry、2个hot容量/2个DATA票据、最多1次暂停。DATA barrier与热发布合并为已由独立DATA切片/真实Java检查的动作；共享执行顺序、native原子性、真实权限和控制force为假设。本模型不证明byte总账、JNI/设备、跨ledger pre-cut排空、hot淘汰/fresh lookup、在线覆盖、MOVE/delete或liveness。初始回执`unified-wal-model-sciqsu_x`的新cfg未配置有限terminal状态策略，checker在后台失败且fence完成、没有新准入的终态报deadlock，结果为INCONCLUSIVE；最终与既有有限切片一致设置`CHECK_DEADLOCK FALSE`，不改变安全性质或常量，不宣称死锁自由/最终进展。

真实Put暂停、后台冲突、预算拒绝、native锁与关闭排空由[存储回归](../local-storage/README.md#异步派生定位追加切片)独立验证。完整Model A/C/D与组合、真实掉电、Block G、步骤8及生产authority仍未完成。

### Query-visible接管与坐标占用

2026-10-03回执`unified-wal-model-2qh8pivn`绑定父提交`2e5978ab836109574f23cbf429859571928b67e9`，冻结43个实际输入（7 module、19 cfg、runner、16 Java），保留父提交处3份合同哈希；58个回执文件checksum核对通过。全部19配置为8个完整正向PASS、11个指定反例，既有历史回执及工具参数不变。

| 当前配置 | generated / distinct / queue | depth / 完整diameter | elapsed / peak RSS | 结论 |
|---|---|---|---|---|
| c-index-maintenance | 2,131 / 858 / 0 | 18 / 18 | 0.898 s / 127,451,136 bytes | PASS |
| ce-index-control-credit | 29 / 21 / 12 | 4 / 不适用 | 0.838 s / 109,887,488 bytes | EXPECTED_COUNTEREXAMPLE: FenceCanUseReservedLane |
| ce-index-early-eviction | 35 / 21 / 12 | 4 / 不适用 | 0.826 s / 109,920,256 bytes | EXPECTED_COUNTEREXAMPLE: LocalSuccessKeepsLocatorAuthority |

正向10个Next动作coverage均非零。`Submit`按未完成DATA与hot积压收费，完整坐标上限为3；`CompleteIndex`在实际query-visible之后移除hot但保留admitted坐标，允许hot容量复用。`LocalSuccessKeepsLocatorAuthority`检查已成功entry始终有hot或可查询row，`OccupiedCoordinatesSurviveEviction`检查已durable坐标不会因淘汰丢失。两个负向最短轨迹均为`Submit → ForceData → StartIndex`，分别否证索引占控制credit与提前淘汰。它们不是Java功能开关。

没有展开fresh Get顺序、捕获定位/read pin、native句柄、真实票据续接或关闭流程；以上由[独立存储回归](../local-storage/README.md#query-visible-热定位接管切片)验证。DATA barrier/发布、单CF查询可见性为输入假设，有限配置继续`CHECK_DEADLOCK FALSE`；不证明liveness、全部byte/native/disk预算、在线coverage或MOVE/FREE，也不提升完整A/C/D/组合、步骤8或生产Gate。


### 运行期覆盖发布切片

2026-10-03 回执`unified-wal-model-47t1o26s`冻结48个实际输入（8 module、23 cfg、runner、16 Java）并绑定父提交`8ab07e437406aa921defb61466e7bb4e6080b944`处3份合同哈希，70个回执文件checksum已逐项核对。全部23配置为9个完整正向PASS与14个指定反例；旧回执不改绑定。

| 新配置 | 结果 | generated / distinct | 剩余queue | 搜索深度 / 完整图直径 | elapsed秒 | peak RSS bytes |
|---|---|---|---|---|---|---|
| `c-runtime-coverage` | PASS | 7,150 / 3,102 | 0 | 22 / 22 | 0.9103 | 151,846,912 |
| `ce-runtime-before-flush` | EXPECTED_COUNTEREXAMPLE | 100 / 64 | 32 | 6 / — | 0.8702 | 111,575,040 |
| `ce-runtime-clock-reset` | EXPECTED_COUNTEREXAMPLE | 635 / 326 | 92 | 10 / — | 0.9058 | 127,631,360 |
| `ce-runtime-late-cut` | EXPECTED_COUNTEREXAMPLE | 104 / 63 | 27 | 6 / — | 0.8413 | 112,934,912 |

正向11个Next动作coverage均非零。模型分别保存已durable DATA、query-visible定位、flush后的内容、冻结cut、可读页与durable页；contents数量代表完整内容快照，stream范围用单stream每条DATA对应一定位的有限投影表达。Prepare要求定位已追平DATA并暂停Put，但允许新DATA继续前进；它们必须等Resume后再入库。最高页只有contents与当前DB匹配才可使用；publication时钟取有效同代页最大值，不以当前contents匹配为前提。crash恢复只保留durable页及flush内容，未持久页的时钟不会形成恢复承诺。

初次回执`unified-wal-model-52pnos7o`未通过负向敏感性检查：clock oracle赋值缺少括号，被解释为限制动作的条件，错误地挡住候选规则，不能计为PASS。修正oracle后增加独立stream cut/contents字段与late-cut负向，取得当前新回执；未缩小常量或削弱性质。历史失败不重绑。

DATA barrier、当前原owner连续前缀、身份/权限及framing仍作为其他切片已验证的输入；未建模多worker冻结/原票据协调、RocksDB真实flush/内容摘要、CRC、native锁、真实文件系统或掉电。Java另验证真实多worker/多stream cut、页发布暂停中的DATA/控制推进、取消排空、force失败及部分DATA额度拒绝。此模型不实现或证明coverage跳过DATA扫描、MOVE/FREE、完整A+C/C+D或liveness，步骤8与生产authority保持未完成。


### 持久覆盖与尾部恢复投影

2026-10-03回执`unified-wal-model-h1roqnsp`冻结53个实际输入（9 module、27 cfg、runner、16 Java），绑定父提交`094a5c709629ba6cb15a2fdbe11b66972a231f7f`处3份合同哈希；82个回执文件checksum已核对。27配置为10个完整正向PASS与17个指定反例，原模型回执不重绑。

| 新配置 | 结果 | generated / distinct | 剩余queue | 搜索深度 / 完整图直径 | elapsed秒 | peak RSS bytes |
|---|---|---|---|---|---|---|
| `c-tail-recovery` | PASS | 280 / 280 | 0 | 8 / 8 | 0.8471 | 115,490,816 |
| `ce-tail-recovery-invalid-index` | EXPECTED_COUNTEREXAMPLE | 83 / 83 | 39 | 3 / — | 0.8263 | 110,493,696 |
| `ce-tail-recovery-before-sync` | EXPECTED_COUNTEREXAMPLE | 242 / 242 | 20 | 7 / — | 0.8241 | 112,656,384 |
| `ce-tail-recovery-ignore-gap` | EXPECTED_COUNTEREXAMPLE | 301 / 281 | 39 | 8 / — | 0.7924 | 113,491,968 |

正向5个Next动作coverage均非零。3个单stream位置、cut 0..3涵盖无/部分/全部覆盖；持久index可匹配、缺失或被后来Put改变，当前tombstone可排除旧坐标。Start仅在完整内容匹配时保留index定位，其他情况全量body重建；InspectHeader无论是否覆盖都检查物理顺序，仅对有效覆盖跳过body。物理缺口阻止Force/Expose，正常范围在必要DATA sync后开放，并保留全部当前可读坐标。

控制重放/身份/grant/合法header与各未跳过body的CRC验证是输入假设；db集合表示已验证的实际持久内容，匹配用集合相等替代Java完整key/value摘要。模型不检查CRC/SHA碰撞、真实RocksDB/文件系统、读取bytes、source目录绑定、native增量WriteBatch、多个stream或真实掉电，以上由独立Java回归或待执行Gate拥有。实际Java仍检查固定头与有界物理连续前缀、匹配的index batch位置及当前Bookie后缀，不把跳过body当payload成功；必要sync失败不能开放。

本切片没有解除未正常关闭会话隔离，没有一般MOVE/FREE、回收后current selector、跨Arena/ledger竞争、完整A+C/C+D或liveness。对应实现与真实5项恢复回归见[本地存储](../local-storage/README.md#持久覆盖辅助的正常恢复切片)，步骤8与production authority仍未完成。


## 分配顺序与旧尾部隔离投影

2026-10-03，`unified-wal-model-p7nk5a81`绑定父提交`c371c40d8d4a871e39d2ff8cc987da611cb88632`，冻结57个实际输入（10 module、30 cfg、runner、16 Java）及父提交处3份合同哈希；91个回执checksum逐项核对。30个配置为11个完整正向PASS和19个指定性质反例，旧回执不改绑定。复现沿用`python3 scripts/unified-wal/run-storage-models.py --tool /path/to/tla2tools.jar`。

| 配置 | 结果 | generated / distinct | queue | depth / diameter | elapsed s | peak RSS bytes |
|---|---|---|---|---|---|---|
| `c-allocation-order` | PASS | 630 / 302 | 0 | 11 / 11 | 0.8197 | 110,772,224 |
| `ce-allocation-file-order` | EXPECTED_COUNTEREXAMPLE: NewerAllocationIsNotBlockedByFileOffset | 10 / 10 | 5 | 4 / — | 0.7609 | 105,431,040 |
| `ce-allocation-old-tail` | EXPECTED_COUNTEREXAMPLE: OldAllocationsCannotBeReentered | 17 / 11 | 6 | 4 / — | 0.8056 | 105,496,576 |

固定3个成功applied grant、每grant 2个位置、最多1次restart，grant 2在共享文件中的地址低于grant 1。检查可进入后来grant、进入后继不返回旧尾，以及restart不重新使用任何已知旧grant尾部。TLC实际展开的`Grant`、`Consume`、`Restart` coverage均非零（21:73、240:480、40:76）；Reserve/Cancel两个包装动作共用Consume并以lastKind区分，未分别统计包装动作次数。Cancel只投影预留地址不返还，真实I/O未知/故障隔离由既有管理会话模型及Java负责。

成功grant、原控制sequence/身份已持久且可重放为假设；不是control fsync/apply或checkpoint新的证明。没有建模实际offset bytes、CRC、扫描各allocation区间拼接、退休range、FREE/generation reuse、reader/writer I/O drain、全局预算或liveness。实际控制序号跨rotation、全量/覆盖辅助恢复和必要gap/duplicate拒绝由[Java真实文件回归](../local-storage/README.md#分配控制顺序与低地址追加)另证，不能据此宣布步骤9完成。


## 整allocation FREE 投影

2026-10-04，`unified-wal-model-k5u2v1c6`绑定父提交`8ac860736695b9cedec43a25f2ecc586d03f0689`，冻结64个输入（11 module、35 cfg、runner、17 Java）及父提交处3份合同哈希，106个receipt checksum已逐项核对。35配置为12个完整正向PASS和23个指定反例，旧回执不重绑。复现使用既有runner与官方TLC 1.7.4。

| 配置 | 结果 | generated / distinct | queue | depth / diameter | elapsed s | peak RSS bytes |
|---|---|---|---|---|---|---|
| `c-whole-free` | PASS | 217 / 68 | 0 | 9 / 9 | 0.8047 | 111,804,416 |
| `ce-whole-free-partial-death` | EveryOccupantWasTerminal | 207 / 72 | 22 | 5 / — | 0.9102 | 113,164,288 |
| `ce-whole-free-early-release` | SubmittedFreeRetainsProtection | 170 / 56 | 12 | 6 / — | 0.8595 | 112,558,080 |
| `ce-whole-free-reboot-pins` | RebootCountsDoNotProveOldIoTerminal | 254 / 92 | 16 | 9 / — | 0.9174 | 111,476,736 |
| `ce-whole-free-before-force` | FreeAuthorityHasBarrier | 169 / 55 | 11 | 6 / — | 0.9054 | 111,771,648 |

固定2个共享occupant、0/1实际旧I/O与reader pin、至多1次crash。单调准确终态、成功grant和DATA验证为输入合同；Prepare合并Bookie entry准入冻结与文件gate，复用同一真实CONTROL force/顺序apply抽象。可见I/O计数与实际旧I/O分开；正常crash后不凭可见0解除未关闭writer隔离。重复操作不再次bump，取消等待不释放维护义务，control unknown只在原owner解析后释放临时gate且保持不可写。

正向Tombstone、StartIo/FinishIo、Pin/Unpin、Prepare、Submit、Force/ForceUnknown、Apply、OwnerResolve、CancelWait、Crash有非零coverage；Duplicate为stuttering幂等投影，EarlyClose在正向恒禁用，不宣称这两个动作的非零状态扩展。有限终态保留CHECK_DEADLOCK FALSE，不证明liveness。实际writer/read quiescence、barrier与延迟owner、固定body/CRC、checkpoint/rotation后跳过退役字节、空range及全量/覆盖storage恢复见[Java证据](../local-storage/README.md#整-allocation-全死-free-与退役前缀)。

没有展开多个victim/并发FREE竞争、控制checkpoint状态图与Bookie完整轨迹组合、真实buffer/queue预算、DATA parser、retired prefix区间拼接、native索引删除、MOVE/selector或旧字节物理reuse；有限投影与Java局部验证不能宣布步骤9整体、完整A+C/C+D或生产Gate完成。


## 健康 CLOSED 整块复用投影

2026-10-04回执 `unified-wal-model-r5vaebxq`，父提交 `8fad048db711b9537ce84cd4dca740841882dac3`，冻结69个输入（12 module、39 cfg、runner、17 Java）和父提交处3份合同哈希，118个回执checksum逐项核验。总矩阵13个正向完整PASS、26个指定反例。仅本增量统计如下，旧回执保持原绑定：

| 配置 | 结果 | generated / distinct | 剩余queue | 搜索深度 / 完整图直径 | elapsed秒 | peak RSS bytes |
|---|---|---|---|---|---|---|
| `c-whole-reuse` | PASS | 23 / 20 | 0 | 9 / 9 | 0.7414 | 104,267,776 |
| `ce-whole-reuse-before-free` | EXPECTED_COUNTEREXAMPLE | 3 / 3 | 1 | 2 / — | 0.7966 | 100,941,824 |
| `ce-whole-reuse-before-io` | EXPECTED_COUNTEREXAMPLE | 15 / 13 | 4 | 6 / — | 0.7931 | 104,382,464 |
| `ce-whole-reuse-old-bytes` | EXPECTED_COUNTEREXAMPLE | 21 / 18 | 3 | 7 / — | 0.8145 | 104,841,216 |

正例完整探索20个distinct states，queue=0。FinishOld/Free/Reuse/StartWrite/Force/WriteUnknown/Close/Crash/Recover均有coverage；模型中的FREE是已有准确全死proof、控制durable/apply和原owner保护的抽象，不重新实现完整FREE模型。CLOSED尾部以0/1 prefix代表实际末端；新I/O unknown不得CLOSE，crash不丢掉实际在途I/O计数。末端后的旧字节存在与CRC有效性都不授予当前记录authority。三个负例分别去掉FREE/旧I/O前序、实际新I/O终结检查、旧suffix排除。

此投影仅单extent/一次reuse、一次crash，健康CLOSED restart；没有实际body/CRC、control snapshot组合、stream退休区间拼接、多个generation/多个victim/竞争、native index、buffer与queue预算或liveness。真实同块两轮FREE、完整及覆盖辅助重启、旧tail垃圾/当前prefix损坏、短写/force失败和独立向量由[Java切片](../local-storage/README.md#整-allocation-复用与健康关闭末端)拥有；不推出完整步骤9、长期净回收或生产权威。


## 终态索引清理投影

2026-10-04，回执 `unified-wal-model-ns9qmh_m` 绑定父提交 `c9463f2ed1fae6c611a325a2b8a702871190f8d6`，冻结75个输入（13 modules、44 configs、runner、17 Java）及父提交处3份合同哈希，133个回执checksum逐项核验。矩阵14个完整正向PASS、30个指定反例；历史结果不重绑。仅新增配置如下：

| 配置 | 结果 | generated / distinct | 剩余queue | 搜索深度 / 完整图直径 | elapsed秒 | peak RSS bytes |
|---|---|---|---|---|---|---|
| `c-terminal-index` | PASS | 517 / 244 | 0 | 13 / 13 | 0.8505 | 116,752,384 |
| `ce-terminal-index-late-put` | EXPECTED_COUNTEREXAMPLE | 433 / 204 | 82 | 8 / — | 0.9520 | 114,966,528 |
| `ce-terminal-index-before-delete` | EXPECTED_COUNTEREXAMPLE | 92 / 44 | 12 | 6 / — | 0.7822 | 110,379,008 |
| `ce-terminal-index-new-tombstone` | EXPECTED_COUNTEREXAMPLE | 208 / 110 | 38 | 8 / — | 0.8040 | 113,049,600 |
| `ce-terminal-index-cancel` | EXPECTED_COUNTEREXAMPLE | 109 / 54 | 16 | 6 / — | 0.7938 | 111,575,040 |

2个旧key与2个native行，0/1实际旧Put与读义务、0/1删除I/O、最多1个新key，终态单调且准确。Prepare代表全部原owner已暂停/index追平并取得Bookie entry lease，captured集合从该cut固定；晚到终态不改变本次native/owner集合。native删除失败可只删subset但不释放坐标；coverage发布unknown发生在安全native删除及owner释放之后，可以在实际终结解冻后使用容量，不能把它当coverage成功。取消等待保留gate到实际删除终结；旧Put不能在新坐标释放后重新插入旧行。

正例244 distinct states、queue 0、depth/diameter 13，Tombstone/FinishOldPut/FinishRead/Prepare/StartDelete/Delete/DeleteUnknown/Prune/Publish/PublicationUnknown/Finish/CancelWait/NewWrite均有coverage。输入framing、准确Bookie T、真实native WriteBatch与本地IO屏障为抽象；Publish沿用既有coverage合同，不展开其完整A/B状态图。该投影没有多worker部分refusal/失败时序、真实native批次内存/字节、restart/crash、metadata保留、SST/磁盘净回收、FREE/MOVE组合或liveness。真实六类新路径及1/4 worker小容量四轮复用由[Java切片](../local-storage/README.md#运行期终态索引与坐标回收)拥有，不等于长期运行验收或完整A+C/C+D。


## relocation 内部完成投影

2026-10-04，回执 `unified-wal-model-a3_49iho` 绑定父提交 `de6f78fdb1acb5965f549835eb281105619390cc`，冻结80个输入（14 modules、48 configs、runner、17 Java）及父提交处3份合同哈希，145个回执checksum逐项核验。15个完整正向PASS、33个指定反例；历史回执不重绑。新增配置如下：

| 配置 | 结果 | generated / distinct | 剩余queue | 搜索深度 / 完整图直径 | elapsed秒 | peak RSS bytes |
|---|---|---|---|---|---|---|
| `c-relocation-completion` | PASS | 580 / 230 | 0 | 10 / 10 | 0.7362 | 112,902,144 |
| `ce-relocation-early-facts` | EXPECTED_COUNTEREXAMPLE | 2 / 2 | 0 | 2 / — | 0.7095 | 102,580,224 |
| `ce-relocation-ordinary` | EXPECTED_COUNTEREXAMPLE | 7 / 7 | 4 | 3 / — | 0.7326 | 107,855,872 |
| `ce-relocation-cancel` | EXPECTED_COUNTEREXAMPLE | 6 / 6 | 4 | 2 / — | 0.7215 | 103,186,432 |

固定2个已提交copy batch，实际IO、durable、owner接收、prefix与最终owner terminal分开。后批force不越过缺口；copy内部结果不授予普通定位。错误可让pending先收到失败，但其buffer仍等自身实际IO及原owner都终结。取消只停止等待，原资源与内部结果义务保留。

正向230 distinct states，queue 0、depth/diameter 10；Force/ForceUnknown/Receive/OwnerTerminal/Release/CancelWait全部有coverage。framing、CRC、SHA-256、grant及copy来源为输入假设；字节/池真实路径由[五个Java测试](../local-storage/README.md#relocation-副本内部持久化完成)覆盖。投影没有MOVE/predecessor/selector、Bookie授权、orphan FREE、多个victim/源冻结、native覆盖、crash/restart或liveness，不证明完整A+C/C+D及步骤9验收。


## 条件 MOVE 与读取保护投影

2026-10-04，`unified-wal-model-jfi2r8do` 绑定父提交 `f66b0a8ed5b31c1e84ee84a5d6f5fbfa7cfa6210`，冻结87个输入（15 modules、52 configs、runner、19 Java）及父提交处合同哈希；157个checksum逐项核验。16个完整正向PASS、36个指定反例，历史绑定不改。

| 配置 | 结果 | generated / distinct | queue | depth / diameter | elapsed秒 | peak RSS bytes |
|---|---|---|---|---|---|---|
| `c-conditional-move` | PASS | 776 / 358 | 0 | 19 / 19 | 0.7894 | 122486784 |
| `ce-move-before-force` | EXPECTED_COUNTEREXAMPLE | 75 / 38 | 18 | 5 / — | 0.8006 | 111755264 |
| `ce-move-ignore-predecessor` | EXPECTED_COUNTEREXAMPLE | 371 / 170 | 22 | 11 / — | 0.8458 | 121847808 |
| `ce-move-cancel` | EXPECTED_COUNTEREXAMPLE | 75 / 38 | 18 | 5 / — | 0.8523 | 109920256 |

固定双坐标与3个候选，前两个竞争初始前驱，第三个以首个成功selector为前驱；copy事实不切换selector。AppendControl、Force、Apply分开；guard排空旧read并持有到apply/实际owner结束，cancel只停止waiter；CrashReplay使用durable有序前缀，不访问copy payload。正向各动作均有coverage，有限模型不声称liveness。

copy实际持久/前缀、源/目标framing/CRC/hash与grant/Bookie权限是输入假设；Java另验证真实文件及原owner生命周期。投影不包含源/orphan FREE、generation reuse、native Put/coverage、全量checkpoint状态图、完整A+C/C+D、实际byte预算或掉电；不能代替步骤9验收。初次新模型动作名称与Sequences.Append冲突，四配置为INCONCLUSIVE；重命名动作后新建完整run，没有把语法错误算PASS。


## MOVE/FREE/reuse 组合投影

2026-10-04，`unified-wal-model-eggymj0z` 绑定父提交 `e83194428e8955180f4a35991b98c72d608fb622`，冻结93个输入（16 modules、57 configs、runner、19 Java）和父提交合同哈希；172个checksum逐项核验。矩阵17完整正向PASS、40指定反例；既有receipt绑定保持。

| 配置 | 结果 | generated / distinct | queue | depth / diameter | elapsed秒 | peak RSS bytes |
|---|---|---|---|---|---|---|
| `c-movefree-reuse` | PASS | 3135 / 2398 | 0 | 33 / 33 | 0.9396 | 127942656 |
| `ce-movefree-current` | EXPECTED_COUNTEREXAMPLE | 156 / 118 | 58 | 5 / — | 0.8426 | 112476160 |
| `ce-movefree-late-target` | EXPECTED_COUNTEREXAMPLE | 441 / 368 | 95 | 10 / — | 0.9785 | 116834304 |
| `ce-movefree-stale-cut` | EXPECTED_COUNTEREXAMPLE | 144 / 119 | 39 | 6 / — | 0.8294 | 112754688 |
| `ce-movefree-old-bytes` | EXPECTED_COUNTEREXAMPLE | 1147 / 941 | 156 | 15 / — | 0.9469 | 122765312 |

固定A/B/C，B/C为两份copy候选、A/B可FREE并最多各复用一次；MOVE预期A→B→C，copy facts本身不选择目标。CONTROL append、force与apply分开；FREE记录proof cut，允许一次其他control变化检验陈旧proof拒绝。目标与前驱必须仍有原grant generation，源/orphanFREE不能释放current selector，restart选择控制元数据，不从旧generation字节复活原坐标。copy/barrier及准确grant/hash proof、排空旧read与I/O为其他模型/Java验证的输入假设。

正向Copy/PrepareMove/PrepareFree/PrepareReuse/AdvanceControl/AppendControl/Force/ApplyMove/ApplyFree/ApplyReuse/Restart均有coverage；没有state constraint、symmetry或liveness检查。初次组合运行完整通过但未显式展开复用目标generation predicate，随后补齐旧copy不能重新进入reused generation的条件/不变量并重跑，最终只引用新run。终态Bookie准确T/selector删除、真实字节CRC/SHA与DATA/CLOSED范围、native索引/票据由[真实回收切片](../local-storage/README.md#搬迁源与-orphan-的整块回收)拥有；该投影不证明完整checkpoint图、native coverage、字节总账、多victim批次、完整A+C/C+D、掉电或进展。


## Native MOVE coverage 投影

2026-10-04，`NativeMoveCoverage.tla` 对原 owner 旧 Put 排空、受保护 native MOVE 更新、flush/coverage 发布、取消及覆盖恢复作有限组合投影。一个坐标/三个候选位置，最多一次 crash；普通 DATA、COPY durability、grant、Bookie 权限、已成功 apply 的 MOVE 和目标 CRC/SHA 判定是输入假设。恢复先检查实际 native 内容与当前 selector 是否匹配，不匹配则全量重建；匹配覆盖仍须校验当前目标字节。native 失败保留 selector，等待方取消不结束维护。`CHECK_DEADLOCK FALSE` 只检查安全性质，不宣称最终进展。

运行 `python3 scripts/unified-wal/run-storage-models.py --tool /tmp/unified-wal-tla2tools-174.jar` 的最终回执 `unified-wal-model-75i1oty1` 绑定父提交 `e8589da6ab61a98f0cc1ef4e28700eeb1647ffe4`：17个TLA模块、62个cfg、runner及19个Java实际输入，共99项/187个checksum。完整矩阵18正向PASS、44指定反例；新投影如下：

| 配置 | 结果 | generated/distinct | 队列 | 深度/直径 | 秒 | peak RSS bytes |
| --- | --- | --- | --- | --- | --- | --- |
| c-move-index | PASS | 188/108 | 0 | 15/15 | 0.855 | 110034944 |
| ce-move-index-old-put | CapturedOwnersHadNoOldPut | 7/7 | 4 | 3/— | 0.837 | 108855296 |
| ce-move-index-cancel | MaintenanceKeepsProtection | 24/19 | 7 | 5/— | 0.844 | 107675648 |
| ce-move-index-stale-row | RecoveredLocatorUsesSelector | 17/13 | 5 | 4/— | 0.791 | 108101632 |
| ce-move-index-missing-bytes | CoveredReadyValidatesCurrentBytes | 117/78 | 18 | 10/— | 0.840 | 111460352 |

负向配置分别去掉旧 Put 排空、在取消时释放 gate、信任陈旧 native 行、跳过当前 target 验证，均产生指定 invariant 反例。模型不展开真实字节、RocksDB/JNI 原子性、256行批次、多个原 worker 的 ticket/lease 协调、实际 I/O/force/目录窗口、native 部分失败、FREE/reuse 的完整组合、全资源总账、真实进程退出或掉电；旧 Put 防止普通 append 替换的 Java 检查仍保留，反例只验证捕获 cut 的排空前提。真实维护/覆盖回归见[本地状态](../local-storage/README.md#native-move-索引维护与覆盖恢复)，完整A/C/D与组合、Block G、步骤8/9和生产Gate不提升。先前各回执不重绑。


### 有界合批 compaction 投影

`CompactionGroup.tla`投影一个或两个4单位source、每块1单位live payload和一块4单位destination；因此单块无净收益，双块有严格正净收益。COPY write/force、MOVE force/apply、native发布、FREE proof/control force、两个成员顺序apply与各自finish独立建模。v2 proof cut分别为7/8；取消只撤回复，所有实际claimed成员终结前保留共享保护。

| 配置 | 检查 |
| --- | --- |
| `c-compaction-group` | 正确allocation净成本、durable后继、native前序、每成员cut与共享保护 |
| `ce-compaction-allocation-cost` | 忽略向上取整destination成本，违反`SelectionHasAllocationNetGain` |
| `ce-compaction-early-free` | MOVE前释放source，违反`FreedSourcesHaveDurableSuccessor` |
| `ce-compaction-shared-cut` | 全部FREE使用同一cut，违反`AppliedFreeUsesOwnCut` |
| `ce-compaction-cancel` | 取消即释放gate，违反`PendingMembersKeepProtection` |

有限正向投影不包含全T组、multi-batch、crash/recovery、跨stream/多Arena、并发源选择、自动orphan清理或资源调度/进展。真实source冻结/输入预算、framing/CRC/SHA、native flush、完整whole-allocation successor proof作为已验证输入；其他既有切片和Java回归分别拥有这些语义，不将35个distinct状态投影称为完整组合证明。


2026-10-04最终回执 `unified-wal-model-inrar5jz` 绑定父提交 `97e25c317e044d04c7c0cf6c6eb9bf2a8aa92f17`、107个实际输入（18个TLA module、67个cfg、runner、21个Java）及202个checksum；原tool/平台/worker/heap/fp/seed/120秒参数不变。全部67配置为19完整正向PASS和48指定性质反例，既有模型状态计数保持不变。初次新module的混合逻辑表达式缺少括号导致解析INCONCLUSIVE，修正后完整探索；未通过run不计为模型结果，格式修正后的最终源码重新绑定。

| 配置 | 结果 | generated / distinct | 剩余queue | 深度 / 完整图直径 | elapsed秒 | peak RSS bytes |
| --- | --- | --- | --- | --- | --- | --- |
| `c-compaction-group` | PASS | 50 / 35 | 0 | 16 / 16 | 0.788089 | 109,559,808 |
| `ce-compaction-allocation-cost` | EXPECTED_COUNTEREXAMPLE | 2 / 2 | 0 | 2 / — | 0.830970 | 102,596,608 |
| `ce-compaction-early-free` | EXPECTED_COUNTEREXAMPLE | 14 / 12 | 2 | 6 / — | 0.736586 | 107,675,648 |
| `ce-compaction-shared-cut` | EXPECTED_COUNTEREXAMPLE | 37 / 27 | 1 | 12 / — | 0.809815 | 111,460,352 |
| `ce-compaction-cancel` | EXPECTED_COUNTEREXAMPLE | 30 / 23 | 4 | 10 / — | 0.752421 | 108,871,680 |

四个trace分别在选择单source、COPY后提前FREE、第二FREE apply和claimed组取消时出现指定性质错误；都是候选规则反例，不是Java源码变异或生产开关。对应真实Java七类282项，277通过、5平台跳过；完整模型/Block G/生产authority未提升。
