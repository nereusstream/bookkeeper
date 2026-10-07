# Spike C 有限模型实施

状态：EXPERIMENTAL / NON_PROMOTABLE / NO_AUTHORITY / FINITE_SLICE_PASS。当前 Model A Bookie权限快照、registration/readiness、authority cold-read、本地 session 准入、startup DATA 排他、resumable all-E availability、initial READY/birth CAS/retention、初始all-E metadata门槛、physical INSTALL、PREPARING reservation authority 与 Profile 标准metadata创建、普通 Add/FENCE/恢复分发与backing额度投影及 Model C 既有切片，共 59 个完整正向配置、237 个指定性质反例，并包含固定初始 SDK bridge、guarded membership CAS、replacement lifecycle、inactive preparation/UIC2及原SDK replacement整体gate投影；当前回执见末节。不是完整 Model A/C/D、A+C 或 Block G 验收，历史回执保留当时范围。

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
| `a-profile-initial-availability` | persistent PREPARING/candidate/READY/DELETE、target五类结果、required metadata及最后authority事实；all-E当前结果才AVAILABLE |
| `ce-profile-initial-availability-ack` | A代替E，须违反 `AvailabilityWaitsForAllE` |
| `ce-profile-initial-availability-binding` | 接受wrong binding，须违反 `AvailabilityHasCurrentBoundResults` |
| `ce-profile-initial-availability-unknown` | 把UNKNOWN当完成，须违反 `AvailabilityDoesNotCountUnknown` |
| `ce-profile-initial-availability-history` | 把已fenced的historical activation当当前active，须违反 `AvailabilityDoesNotCountHistoricalFence` |
| `ce-profile-initial-availability-rebuild` | 既有page引用的metadata缺失时重建，须违反 `RequiredMetadataIsNeverRebuilt` |
| `ce-profile-initial-availability-delete` | 忽略最后DELETE事实，须违反 `AvailabilityObservesReadyNotDelete` |
| `ce-profile-initial-availability-metadata` | 忽略最后metadata事实，须违反 `AvailabilityHasMatchingMetadataFact` |
| `a-profile-ready-lifecycle` | 一个head和page、READY/DELETE publisher read/CAS、root/page重建与metadata出生身份，published reference后才授予activation，delete后保留targets |
| `ce-profile-ready-lifecycle-inert` | 信任inert page，须违反 `ActivationHasPublishedReady` |
| `ce-profile-ready-lifecycle-head-birth` | CAS仅比较重置的version，须违反 `BirthChecksProtectTheCurrentRoot` |
| `ce-profile-ready-lifecycle-page-birth` | 忽略重建page的birth，须违反 `ReadyReferencesActualPageBirth` |
| `ce-profile-ready-lifecycle-metadata-birth` | 忽略同version0的新metadata出生身份，须违反 `ReadyValidatedMetadataBirth` |
| `ce-profile-ready-lifecycle-delete` | READY绕过delete cut，须违反 `DeleteCutNeverRollsBack` |
| `ce-profile-ready-lifecycle-retention` | DELETE丢已发布target引用，须违反 `PublishedTargetsSurviveDelete` |
| `ce-profile-ready-lifecycle-all-e` | page缺少全部E的receipts，须违反 `ReadyRequiresAllEReceipts` |
| `a-profile-initial-metadata` | E/W/A=3/3/2、计划有效/无效、每target四类结果、root变化/删除与fresh read，全部E匹配才提交metadata |
| `ce-profile-initial-metadata-ack` | 只等A个结果就推进，须违反 `MetadataWaitsForAllE` |
| `ce-profile-initial-metadata-binding` | 接受错绑定success，须违反 `MetadataHasExactDurableResults` |
| `ce-profile-initial-metadata-root` | 忽略root version变化，须违反 `MetadataHasSameRootSnapshot` |
| `ce-profile-initial-metadata-absence` | root missing仍推进，须违反 `MetadataHasCommittedReservation` |
| `ce-profile-initial-metadata-plan` | 错计划仍写reservation，须违反 `InvalidPlanHasNoReservation` |
| `ce-profile-initial-metadata-writer` | metadata alone返回writer，须违反 `MetadataAloneNeverReturnsWriter` |
| `ce-profile-install-dispatch-generation` | INSTALL receipt用推进后的current generation，须违反 `InstallReceiptKeepsBirthGeneration` |
| `a-profile-install-dispatch` | 一次typed INSTALL的control提交、write/force/owner apply、可选停止等待与实际completion，成功晚于barrier/owner且不改变normal状态 |
| `ce-profile-install-dispatch-exact` | 忽略exact authority/physical target，须违反 `AdmissionHasExactAuthorityAndTarget` |
| `ce-profile-install-dispatch-barrier` | 提交即成功，须违反 `InstallSuccessHasBarrier` |
| `ce-profile-install-dispatch-owner` | I/O terminal即成功、尚无owner apply，须违反 `InstallSuccessHasOwnerApply` |
| `ce-profile-install-dispatch-stop` | 停止等待即释放ownership，须违反 `SubmittedOwnershipWaitsForBothTerminals` |
| `ce-profile-install-dispatch-normal` | INSTALL激活normal，须违反 `InstallPreservesNormalState` |
| `ce-profile-install-dispatch-unknown` | 提交后timeout清成NONE，须违反 `SubmittedUnknownNeverBecomesNone` |
| `a-profile-reservation-authority` | 一次PREPARING root cold read、可选后续root替换及INSTALL投影，检查exact key/计划目标/严格root及同snapshot版本 |
| `ce-profile-reservation-authority-key` | 忽略instance/domain/purpose key，须违反 `GrantedMatchesRequestedKey` |
| `ce-profile-reservation-authority-target` | 忽略计划目标，须违反 `GrantedTargetIsPlanned` |
| `ce-profile-reservation-authority-corrupt` | 将坏/不完整/超限/ephemeral root当成可用，须违反 `GrantedHasValidCommittedRoot` |
| `ce-profile-reservation-authority-normal` | PREPARING授予ACTIVATE，须违反 `PreparingOnlyAuthorizesInstall` |
| `ce-profile-reservation-authority-version` | 新version配旧scope，须违反 `ProjectionKeepsReadVersion` |
| `a-profile-metadata-create` | 一次standard metadata create、一次resolving snapshot read及可选delete/recreate/update，检查完整instance/reference/初始内容和UNKNOWN |
| `ce-profile-metadata-create-instance` | 忽略instance，须违反 `SuccessMatchesInstance` |
| `ce-profile-metadata-create-sidecar` | 忽略opaque sidecar reference，须违反 `SuccessMatchesSidecar` |
| `ce-profile-metadata-create-payload` | 忽略初始内容，须违反 `SuccessMatchesInitialPayload` |
| `ce-profile-metadata-create-absence` | 将 resolving NONODE 当成无effect，须违反 `AbsenceDoesNotProveNoEffect` |
| `ce-profile-metadata-create-ack` | 创建成功回调后跳过实际read，以提交对象形成成功，须违反 `SuccessHasActualMetadataSnapshot` |
| `a-startup-data-exclusion` | 一次 graph restart 与可选 foreign DATA writer，恢复前排他和同锁移交，清理只释放自己已终结的资源 |
| `ce-startup-data-exclusion-order` | 恢复先于 DATA 锁，须违反 `RecoveryStartsWithOwnExclusion` |
| `ce-startup-data-exclusion-handoff` | 移交时释放 native 锁，须违反 `FileHandoffKeepsExclusion` |
| `ce-startup-data-exclusion-cleanup` | 清理终结却保留自己占有的锁，须违反 `ClosedAttemptHasNoOwnedScope` |
| `ce-startup-data-exclusion-foreign` | 清理误释放 foreign 锁，须违反 `ForeignWriterKeepsItsScope` |
| `a-registration-admission` | 一次 owned reconciliation、一次连接丢失/观察/重连、一次已提交 DATA I/O，退休与原 I/O/owner 终结分离 |
| `ce-registration-admission-session` | 忽略当前 session/SDK 连接检查，须违反 `AdmissionUsesOriginalConnectedSession` |
| `ce-registration-admission-reconnect` | 连接恢复自动重开旧 gate，须违反 `RetiredAttemptNeverReopens` |
| `ce-registration-admission-late` | 迟到 reconciliation 重开退休 gate，须违反 `RetiredAttemptNeverReopens` |
| `ce-registration-admission-release` | 退休提前释放已提交 I/O ownership，须违反 `OutstandingOwnershipWaitsForActualTerminals` |
| `a-authority-read` | 八种单 view 结果、一次 concurrent update、sync/get/callback/deadline，合法旧 snapshot 保持自己的 payload/version |
| `ce-authority-read-key` | 忽略 derived key，须违反 `FoundMatchesRequestedKey` |
| `ce-authority-read-invalid` | 允许无效/incomplete/ephemeral/wrong-state view，须违反 `FoundHasValidPersistentCompleteRecord` |
| `ce-authority-read-error` | 错误映射为不存在，须违反 `AbsentOnlyFromActualNoNode` |
| `ce-authority-read-version` | 用更新后的版本配旧 payload，须违反 `FoundKeepsOneAtomicSnapshot` |
| `c-compaction-tail` | 两source与一个current COPY grant已占满，最新有效tail证明后复用剩余范围；保留目的地不FREE |
| `ce-compaction-tail-boundary` | 故意忽略全批COPY空间，须违反 `TailWritesUseUnusedRange` |
| `ce-compaction-tail-retained` | 故意将保留目的地加入FREE，须违反 `RetainedCurrentCopyNeverFreed` |
| `c-compaction-capture` | 2批COPY、2..4行计划/上限3、1..2原池buffer；capture及COPY I/O/owner终结与测量身份检查 |
| `ce-compaction-capture-cancel` | 故意取消即归还buffer，须违反 `CaptureAndCopyKeepOwnedBuffers` |
| `ce-compaction-capture-measurement` | 故意忽略测量身份变化，须违反 `CopyMatchesMeasurement` |
| `c-compaction-sweep` | 1固定文件、3稳定grant、每窗口1 grant、至多2后来grant及6个已准入请求，固定上限保证有限轮覆盖与回访 |
| `ce-compaction-sweep-reset` | 故意每请求重置游标，须违反 `ThreeAdmittedCallsCoverStableRound` |
| `ce-compaction-sweep-expanding` | 故意每请求扩大轮上限，须违反 `FourthAdmittedCallRevisitsOldestGrant` |
| `c-compaction-selection` | 3个current候选、各0..3 live记录、0/1 spare；正向COPY不占整组输入，真实测量优先dead并保留最终proof/barrier；1..3输入预算只用于错误整组捕获候选 |
| `ce-compaction-selection-input` | 故意选择前捕获全部输入，必须违反 `DeadCandidatesAvoidInputRefusal` |
| `ce-compaction-selection-index` | 故意以native缺行测量死亡，必须违反 `SelectedSourcesUseActualData` |
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


### 多批 compaction 投影

`CompactionGroup.tla`扩展为1..3个4单位source、1..3个各2单位aligned COPY batch；destination容量为aligned总成本向上取整到4单位，不再只扣首批。每批COPY force、MOVE force/apply分别累计，全部moved后才能发布native和FREE；增加后批CopyFailure，先前moved事实不回滚，也不允许whole-source FREE。FREE自身cut/全部claimed成员保护性质继续保留。

新增 `ce-compaction-incomplete-group` 故意允许在首批MOVE后发布native并释放整组，必须违反 `FreedSourcesHaveDurableSuccessor`。原五个配置随新有限域得到新的结果，历史35-state回执不重绑。有限投影仍不包含全T、实际CRC/SHA/operation identity、native flush、snapshot/队列资源、真实crash/reuse、跨stream/多Arena或进展；这些来自输入假设、其他切片及实际Java验证。


2026-10-04最终回执 `unified-wal-model-e8rktusu` 绑定父提交 `95fc090067b084f9cc50365df29037fac673b91a`、108个输入（18个TLA module、68个cfg、runner、21个Java）与205个checksum，全部68配置为19个完整正向PASS及49个指定性质反例。既有tool/平台/JDK、1 worker、512/64 MiB、fp/seed和120秒参数保持不变。原其他切片状态计数未变；下面为本次扩展投影的新结果。

| 配置 | 结果 | generated / distinct | 剩余queue | 深度 / 完整图直径 | elapsed秒 | peak RSS bytes |
| --- | --- | --- | --- | --- | --- | --- |
| `c-compaction-group` | PASS | 378 / 259 | 0 | 28 / 28 | 0.800760 | 113,475,584 |
| `ce-compaction-allocation-cost` | EXPECTED_COUNTEREXAMPLE | 4 / 4 | 2 | 2 / — | 0.793457 | 103,071,744 |
| `ce-compaction-early-free` | EXPECTED_COUNTEREXAMPLE | 63 / 53 | 15 | 6 / — | 0.791054 | 109,641,728 |
| `ce-compaction-shared-cut` | EXPECTED_COUNTEREXAMPLE | 201 / 144 | 15 | 12 / — | 0.837998 | 112,738,304 |
| `ce-compaction-cancel` | EXPECTED_COUNTEREXAMPLE | 156 / 114 | 14 | 10 / — | 0.805309 | 112,934,912 |
| `ce-compaction-incomplete-group` | EXPECTED_COUNTEREXAMPLE | 214 / 151 | 19 | 11 / — | 0.815862 | 112,164,864 |

新增完整trace为两批任务完成首批MOVE后，错误PublishNative→PrepareFree→ForceFree→ApplyFree；此时还有未搬迁批次。Java的完整whole-source successor proof不由计数替代。真实四项回归及七类281通过/5平台跳过分别验证storage语义；没有执行JVM halt或掉电，不提升完整模型/Block G/生产authority。


## 有界 source 选择投影

`CompactionSelection.tla`投影3个已固定候选及其各0..3个current live记录、1..3个输入单位和0/1可用destination。真实DATA测量在选择前不占payload输入；有dead时只选dead，没有dead时选择窗口整组，再判输入和目的地额度。部分live路径保留COPY/MOVE成功或失败，FREE仍需要独立whole-allocation当前后继proof与CONTROL barrier。source/target实际CRC、身份、Bookie权限、稳定cut及资源guard均为实现输入假设；候选窗口排序、16上限、allocation取整净成本由Java及已有group投影分别验证。

两个敏感性候选分别是选择前捕获全部输入，以及用native缺行代替current DATA测量。模型明确把全候选捕获成本与选中组COPY需求分开：无关payload只影响错误的输入捕获，不能使零live组产生destination需求。后者反例仅证明候选选择违约，不宣称错误候选已经通过最终FREE proof。无spare的dead直接回收只在本有限域与输入假设下检验，不是整体存储进展、完整内存界限、liveness或crash/复用证明。

| 配置 | 结果 | generated / distinct / queue | 深度 / 直径 | 秒 | peak RSS bytes |
| --- | --- | --- | --- | --- | --- |
| `c-compaction-selection` | 完整PASS | 10558 / 10558 / 0 | 8 / 8 | 0.972378 | 180174848 |
| `ce-compaction-selection-input` | 指定 `DeadCandidatesAvoidInputRefusal` 反例 | 6178 / 6178 / 2057 | 4 / 不适用 | 0.863914 | 147275776 |
| `ce-compaction-selection-index` | 指定 `SelectedSourcesUseActualData` 反例 | 4118 / 4118 / 2057 | 3 / 不适用 | 0.890857 | 136691712 |

回执 `unified-wal-model-q9uihppa` 绑定父提交 `a3da98d3c0a734b61bb7fa06c53ecb6c6aac5840`、112个模块/config/runner/实际Java输入和214个checksum，完整矩阵20正向PASS/51指定反例；输入及checksum逐项验证。父提交合同哈希保留父提交原值，不改绑定历史回执；Java七类288项、283通过/5平台跳过为独立真实路径证据，见[本地选择实现](../local-storage/README.md#固定-shard-的有界回收选择)。仍无完整A/C/D/组合、Block G、掉电或生产authority。


## 有界窗口轮转投影

`CompactionSweep.tla`投影一个固定shard/file、3个持续可用grant、每窗口1个grant、至多2个后来grant与6个已准入尝试；一次拒绝准入动作不改变游标。模型sequence从1开始，实现控制首sequence为0/初始游标-1，实际每窗口最多16个grant由Java真实17个grant验证。候选是否dead、有净收益或资源不足不改变已准入窗口的尝试进度；真实DATA/权限/当前grant及正常资源终结为外部输入，本模块不证明MOVE/FREE正确性。

`ThreeAdmittedCallsCoverStableRound`检查3个持续可用旧候选在3次已准入尝试中均被访问；`FourthAdmittedCallRevisitsOldestGrant`检查固定本轮上限后下一次重开从旧候选开始，而非不断把后来grant纳入当前轮。两个故意错误候选分别每次从头，以及每次扩大上限；前者漏查后续旧grant，后者延迟已变化旧source的回访。未增加调度fairness或deadlock/liveness检查，性质只在上述有限域、持续候选和调用方继续已准入尝试的条件下成立。

| 配置 | 结果 | generated / distinct / queue | 深度 / 直径 | 秒 | peak RSS bytes |
| --- | --- | --- | --- | --- | --- |
| `c-compaction-sweep` | 完整PASS | 113 / 60 / 0 | 10 / 10 | 0.787118 | 109789184 |
| `ce-compaction-sweep-reset` | 指定 `ThreeAdmittedCallsCoverStableRound` 反例 | 24 / 15 / 5 | 4 / 不适用 | 0.822043 | 108871680 |
| `ce-compaction-sweep-expanding` | 指定 `FourthAdmittedCallRevisitsOldestGrant` 反例 | 60 / 38 / 11 | 6 / 不适用 | 0.778721 | 108593152 |

回执 `unified-wal-model-vzkxsq35` 绑定父提交 `c7832d4c0695bb27b8ec049df488b1b3273a04f7`、116个输入/223个checksum、21完整正向PASS/53指定反例。历史设计输入仍取父提交哈希，旧回执不重绑；本轮[真实容量回归](../local-storage/README.md#有界回收窗口轮转与有限容量循环)分别验证实际16上限、满容量/ordinary refill/准确T/多代复用，未做长期负载、性能、多Arena、掉电或生产authority验收。


## 池化 compaction 捕获投影

`PooledCompactionCapture.tla`将当前两批COPY的scratch与冻结buffer分开：1..2个原owner buffer、2..4行测量计划及3行上限，source payload占用SegmentEntryInputs为0。超行数先拒绝，只有1个buffer时归还已取得scratch；捕获I/O终结后由原owner释放scratch并转交冻结COPY，实际COPY I/O及owner都终结后才归还COPY。取消只撤销等待，已取得资源保持到相应终结。测量后源身份变化使下一批拒绝，不生成新的COPY/MOVE；FREE依赖两批durable MOVE，native覆盖和整块proof仍是已有group/Java的独立输入。

`CompactionSelection.tla`的正向Capture同步改为不占整组源输入；原错误 `CaptureAllFirst` 仍占全部候选payload并产生指定反例。目的地需求继续由选中live数量判断，与错误输入算法分开。完整正向选择图现在13758 generated/distinct、队列0、深度/直径8；此前10558状态的历史回执不改绑。

| 配置 | 结果 | generated / distinct / queue | searchDepth / diameter | elapsed秒 | peak RSS bytes |
| --- | --- | --- | --- | --- | --- |
| `c-compaction-capture` | 完整PASS | 248 / 188 / 0 | 19 / 19 | 0.895649 | 110755840 |
| `ce-compaction-capture-cancel` | 指定 `CaptureAndCopyKeepOwnedBuffers` 反例 | 37 / 33 / 7 | 5 / 不适用 | 0.974862 | 107773952 |
| `ce-compaction-capture-measurement` | 指定 `CopyMatchesMeasurement` 反例 | 57 / 47 / 9 | 7 / 不适用 | 0.948871 | 107282432 |

最终回执 `unified-wal-model-uzyhx2mp` 绑定父提交 `cd2b4e4facb7fca36eb1a9bda19eec80fd0240b5`、121个输入和232个checksum；22完整正向PASS/55指定反例。新增共同writer `SegmentBatchFormat.java` 的输入绑定，原父提交合同哈希与历史回执保持。先验证模型后修正两处Java长行，因此最终run按最终源码重新冻结；中间run不重绑。本轮[真实路径](../local-storage/README.md#池化分批-compaction-捕获)独立验证hash/cut变化、池不足、三批与恢复。

本模型的valid只代表guard下真实身份复核结果，不计算SHA/CRC或模型化文件字节；坐标计数不证明完整heap预算，有限进程切片不证明调度、long-run、掉电、完整A/C/D组合、Block G或生产authority。


## 保留 compaction tail 投影

`RetainedCompactionTail.tla`投影两个source及一个保留current COPY grant占满文件：目的地容量4单位、当前used end为2、剩余新COPY需求1..3单位，最新grant与current COPY两个布尔事实代表Java受保护证明。需求不适配全部剩余范围即拒绝；接受后只在oldEnd之后追加COPY，实际barrier→MOVE逐批成功，再native/whole proof→FREE，只退役两个source。中途停止保留旧目的地和全部source，无新增allocation容量。

| 配置 | 结果 | generated / distinct / queue | searchDepth / diameter | elapsed秒 | peak RSS bytes |
| --- | --- | --- | --- | --- | --- |
| `c-compaction-tail` | 完整PASS | 42 / 42 / 0 | 11 / 11 | 0.799228 | 105627648 |
| `ce-compaction-tail-boundary` | 指定 `TailWritesUseUnusedRange` 反例 | 49 / 49 / 1 | 9 / 不适用 | 0.845209 | 108068864 |
| `ce-compaction-tail-retained` | 指定 `RetainedCurrentCopyNeverFreed` 反例 | 38 / 38 / 1 | 8 / 不适用 | 0.771254 | 105398272 |

最终回执 `unified-wal-model-63bfz3qg` 绑定父提交 `6d05ff02399be4218ae776bd9d851e2253be521b`、125个输入及241个checksum，23个完整正向PASS/57个指定反例。最后修正结果中的实际目的地ID计数后，按最终Java输入重新冻结模型run；历史回执与父提交合同哈希不重绑。[真实回归](../local-storage/README.md#满容量部分-move-的保留-tail-续写)独立覆盖1/4 worker、gen2残留tail、健康重启、源复用、全批容量不足及坏字节拒绝。

本模型的latest/currentCopy是已验证的输入，不枚举原始record、SHA/CRC、selector/Bookie cut或实际CLOSED witness；有限计数/阶段不证明完整预算、I/O、调度liveness、一般OPEN错误恢复、long-run、多Arena、完整A/C/D组合或生产authority。`AllocationOrder`原投影继续验证进入后继grant后不回旧grant；新显式最新tail证明并不授权更旧allocation重入。


## 维护容量预留投影

`MaintenanceReserve.tla` 声明两个固定DATA文件、每文件7个whole blocks、维护目的地上限/预留1或2、一批3个跨文件请求（各1..2 blocks）及单compaction。Plan逐个扣减同文件剩余容量，Apply才成为占用；restart按已有grant重建，未apply计划不成为空间权威。维护可使用保留容量，但全目的地成本不得超过声明上限；经过独立真实FREE证明后恢复内部容量，PARTIAL仍保留已占用目的地。模型只表示计数，不模拟extent split/coalesce、actual DATA、FREE proof、fsync或filesystemavailable。

| 配置 | 结果 | generated/distinct/queue | depth/diameter | elapsed s | peak RSS bytes |
| --- | --- | --- | --- | --- | --- |
| `c-maintenance-reserve` | PASS | 3872/3872/0 | 9/9 | 0.916933 | 146767872 |
| `ce-maintenance-reserve-foreground` | EXPECTED_COUNTEREXAMPLE | 898/898/383 | 4/— | 0.897642 | 117555200 |
| `ce-maintenance-reserve-merged` | EXPECTED_COUNTEREXAMPLE | 898/898/383 | 4/— | 0.841703 | 117669888 |
| `ce-maintenance-reserve-workset` | EXPECTED_COUNTEREXAMPLE | 1540/1539/385 | 6/— | 0.857535 | 122044416 |
| `ce-maintenance-reserve-restart` | EXPECTED_COUNTEREXAMPLE | 1537/1537/383 | 6/— | 0.850071 | 120717312 |

敏感性分别允许前台消耗保留、同批每请求重置预算、忽略目的地工作集上限、restart忘记已有grant，得到指定性质反例。正向完整终结、queue=0，没有state constraint或symmetry reduction；不能从有限安全性质推出维护调度或长期进展。

最终回执 `unified-wal-model-dzggb6ip` 绑定父提交 `10f7eccccf6284d6dcf352f05c600084da1b57fb`、131个输入及256个checksum；完整矩阵24正向PASS/61指定反例，历史回执和父提交合同哈希不重绑。修正Java格式及新增模型文件的EOF空行后，按最终实际输入重新冻结本回执；较早模型run不用于当前绑定。[真实回归](../local-storage/README.md#前台-refill-的维护容量预留)独立验证前台拒绝/恢复、两文件合批、1/2 blocks工作集、健康重启和读/幂等。

Source死亡/selector/generation、CLOSED witness、真实内存、I/O terminal与filesystem/SST/control/checkpoint余量继续由独立合同及测试承担；碎片和无净收益组可以拒绝。本切片不关闭B17、完整Model C/A+C/D或生产Gate。

## 运行期控制日志轮转投影

`RuntimeControlRotation.tla` 只展开一份 graph 维护请求、两个 driver 排他名额、全 DATA 保护、独立 Bookie/Arena 完成 root 以及原 owner 返回。每种已有 admission/read pin 只有0或1；实际排空后才能取得保护，否则当前尝试拒绝。取消不取消 I/O，I/O 终结不代替原 owner 终结；OPEN session 与既有 cursor 保留，尝试失败停止图准入。原 owner resolve 的 TRANSIENT_UNAVAILABLE、ROTATED 和 UNKNOWN 分别保留对应边界，不假设跨 owner 原子发布。

每个 Next 动作在正向配置都有非零 coverage。物理 image/commit/header/双 root/目录 force 协议继续由既有 ArenaRotation/BookieCheckpoint 模型及真实文件/JVM退出回归独立承担；本投影接受单 owner 的成功或不确定完成为输入，不是完整 A+C 组合或实际掉电证明。

| 配置 | generated / distinct / queue | depth / diameter | 时间 / peak RSS | 结果 |
| --- | --- | --- | --- | --- |
| c-runtime-control-rotation | 81 / 44 / 0 | 7 / 7 | 0.796 s / 109,379,584 bytes | PASS |
| ce-runtime-control-rotation-cancel | 14 / 10 / 4 | 3 / 不适用 | 0.843 s / 101,531,648 bytes | EXPECTED_COUNTEREXAMPLE: RotationKeepsReservations |
| ce-runtime-control-rotation-owner | 28 / 19 / 9 | 4 / 不适用 | 0.841 s / 102,367,232 bytes | EXPECTED_COUNTEREXAMPLE: GuardWaitsForOriginalOwner |
| ce-runtime-control-rotation-session | 13 / 9 / 3 | 3 / 不适用 | 0.903 s / 101,777,408 bytes | EXPECTED_COUNTEREXAMPLE: OpenSessionAndCursorRemain |
| ce-runtime-control-rotation-failure | 54 / 31 / 11 | 5 / 不适用 | 0.788 s / 108,871,680 bytes | EXPECTED_COUNTEREXAMPLE: UnknownStopsGraph |

回执 `unified-wal-model-2b0qki3w` 绑定父提交 `f2984c57f65d833d99f95149bc87bf9404e29ed8`、138个输入和271个checksum；25正向配置完整PASS、65指定反例。源码输入新增 Bookie control driver 的实际维护排他路径，历史sourceCommit/designInputs/receipt不重绑。提前取消释放、I/O结束就释放guard、伪造CLOSED/cursor重置和 UNKNOWN 后重新开放四个候选分别被指定性质否证。[真实运行期回归](../local-storage/README.md#运行期控制日志轮转)独立验证容量满额/连续续写、真实pin、force暂停、取消、失败及健康重启。

有限终态保持 CHECK_DEADLOCK FALSE，不证明liveness；真实buffer总账、quarantine通知队列、全部控制并发、自动容量调度、filesystem/SST临时空间、长期负载、多Arena、OPEN错误恢复和完整模型/生产Gate仍未闭合。

## 控制容量恢复投影

`ControlCapacityRecovery.tla` 展开一次容量拒绝、一个fresh-fit判据、0/1个旧entry与pin/control阻塞、一次新entry尝试和一份自动CONTROL I/O。pressure拒绝新entry，旧资源终结后既有owner pump才启动轮转；暂不可用时保留需求。物理终结后仍保留graph pause到原owner，失败则保留诊断并停止准入。新的journal与真实原owner协议继续由RuntimeControlRotation和既有双恢复根模型独立承担，本投影不合并这些字节事实为完整A+C。

| 配置 | generated / distinct / queue | depth / diameter | 时间 / peak RSS | 结果 |
| --- | --- | --- | --- | --- |
| c-control-capacity-recovery | 172 / 62 / 0 | 6 / 6 | 0.786 s / 108,937,216 bytes | PASS |
| ce-control-capacity-oversize | 17 / 17 / 15 | 2 / 不适用 | 0.736 s / 101,171,200 bytes | EXPECTED_COUNTEREXAMPLE: OversizeNeverStartsRotation |
| ce-control-capacity-pause | 59 / 34 / 15 | 3 / 不适用 | 0.822 s / 108,068,864 bytes | EXPECTED_COUNTEREXAMPLE: PressureRefusesNewEntries |
| ce-control-capacity-demand | 66 / 38 / 17 | 3 / 不适用 | 0.777 s / 106,037,248 bytes | EXPECTED_COUNTEREXAMPLE: BlockedDemandRemains |

正向全部8个Next动作有非零coverage。回执 `unified-wal-model-tqstjlab` 绑定父提交 `59d1c4cfe0cb4e03ee95e594fa81310301fdbb3a`、143个输入和283个checksum；26完整正向PASS、68指定反例。超大请求触发轮转、pressure下继续接受新entry、资源暂不可用便忘记需求三个候选分别被指定性质否证。[真实容量恢复](../local-storage/README.md#控制容量压力的自动轮转)另证真实journal字节、pin解除、同步quarantine拒绝的唤醒、原pool续写及错误可见性。

fresh-fit与实际terminal/轮转结果为输入假设，不能推出真实CRC/目录force、所有线程/通知队列或filesystem预算正确。有限终态保留CHECK_DEADLOCK FALSE，不证明一般liveness、低水位调度或持续负载；历史receipt和父提交designInputs哈希不重绑，完整A/C/D及组合、B17和生产Gate未完成。

## 冷启动控制容量投影

`ColdControlStartup.tla` 展开一次冷启动，将真实CLOSED、同一writer排他、当前必要DATA有效性、恢复sync与OPEN/CLOSE/双root的fresh-fit作为有限输入。已有容量够时直接下一OPEN，不足且fresh-fit时只轮转Arena再OPEN，无法恢复时失败；真正成功的OPEN后才发布ready。Bookie cut和双root字节协议分别由真实恢复/既有ArenaRotation负责，本投影不是它们的完整组合或物理崩溃重演。

| 配置 | generated / distinct / queue | depth / diameter | 时间 / peak RSS | 结果 |
| --- | --- | --- | --- | --- |
| c-cold-control-startup | 148 / 120 / 0 | 6 / 6 | 0.809 s / 107,757,568 bytes | PASS |
| ce-cold-control-startup-guard | 83 / 61 / 35 | 2 / 不适用 | 0.795 s / 106,774,528 bytes | EXPECTED_COUNTEREXAMPLE: RotationHasValidatedProtectedClosedData |
| ce-cold-control-startup-open | 135 / 107 / 20 | 4 / 不适用 | 0.792 s / 107,429,888 bytes | EXPECTED_COUNTEREXAMPLE: OpenHasRecoveredCapacity |
| ce-cold-control-startup-failure | 97 / 69 / 35 | 3 / 不适用 | 0.711 s / 106,987,520 bytes | EXPECTED_COUNTEREXAMPLE: ReadyHasDurableOpenAndNoFailure |

正向六个生产动作Recover/Capacity/Rotation/Open/Publish/Fail的coverage均非零；`PrematureRotation` 是只在敏感性配置启用的错误候选，正向为零。回执 `unified-wal-model-4g96l4uk` 绑定父提交 `b03b1125ea0c34dacbdd4bf1447aaa6ca7e0a442`、148个实际输入和295个checksum；27正向完整PASS、71指定反例。绕过恢复/保护、容量未恢复先OPEN和失败发布ready分别被指定性质否证。[真实冷启动路径](../local-storage/README.md#满控制日志的健康冷启动)独立提供满日志、锁、CRC/force、恢复数据、新grant和未知OPEN证据。

此投影不模拟CRC、目录force、真实排他锁实现或全部reuse witness字节上限；这些均为输入边界，不因模型PASS就推广为全部持久化/并发正确。有限终态CHECK_DEADLOCK FALSE，liveness、掉电、filesystem工作集、完整A/C/D与组合、B17和生产Gate仍未完成；历史receipt/designInputs保持原绑定。


## Arena 空间压力维护投影

`ArenaSpaceMaintenance.tla` 有限展开同一文件的两个shard窗口、一次tombstone与一次已知控制容量拒绝。资源和受保护compaction结果是输入：控制rotation先行，实际buffer/pin条件满足后才开始，冻结cut不变且无净收益时不重复窗口；控制拒绝重置cursor以重新保护，UNKNOWN停止DATA准入，实际容量恢复后才放行。真实COPY/MOVE/FREE、dual-root、native覆盖和字节成本仍由既有路径与独立模型负责，此投影不是物理语义或完整组合证明。

| 配置 | generated / distinct / queue | depth / diameter | 时间 / peak RSS | 结果 |
| --- | --- | --- | --- | --- |
| c-arena-space-maintenance | 107 / 85 / 0 | 15 / 15 | 0.791 s / 109,903,872 bytes | PASS |
| ce-arena-space-maintenance-resources | 10 / 9 / 7 | 2 / 不适用 | 0.781 s / 106,397,696 bytes | EXPECTED_COUNTEREXAMPLE: MaintenanceStartsWithActualResources |
| ce-arena-space-maintenance-repeat | 84 / 63 / 9 | 8 / 不适用 | 0.807 s / 110,706,688 bytes | EXPECTED_COUNTEREXAMPLE: UnchangedFullLiveWindowsVisitedOnce |
| ce-arena-space-maintenance-refusal | 25 / 16 / 5 | 3 / 不适用 | 0.795 s / 106,708,992 bytes | EXPECTED_COUNTEREXAMPLE: KnownRefusalKeepsFreshProtectedWork |
| ce-arena-space-maintenance-unknown | 36 / 24 / 6 | 4 / 不适用 | 0.806 s / 108,363,776 bytes | EXPECTED_COUNTEREXAMPLE: UnknownStopsDataAdmission |

正向全部10个Next动作均有非零执行coverage。回执 `unified-wal-model-ez7490vu` 绑定父提交 `860aac1ea5b9b34af1d077edcda7f1aa409e6fcd`、155实际输入和310个checksum，28正向完整PASS、75指定反例；新增四个错误候选分别被指定性质否证。运行命令仍为 `python3 scripts/unified-wal/run-storage-models.py --tool /tmp/unified-wal-tla2tools-174.jar`，官方1.7.4工具身份、单worker、固定seed/fp、无状态约束或对称化均保留。真实行为证据见[自动空间回收](../local-storage/README.md#数据空间压力的自动回收)。

CHECK_DEADLOCK FALSE仅允许有限终态，不证明公平调度、全部park/unpark或长期liveness。该投影没有filesystem/SST工作集、完整partial收敛、多Arena、实际掉电或正式性能；B17、完整A/C/D及组合和生产Gate仍未完成，历史receipt/designInputs保持原绑定。

## 内部 checkpoint 历史投影

`InternalCheckpointHistory.tla` 固定 grant/credential/tombstone 业务结果、八个内部 checkpoint generation 和一个保留 image，检查当前物理 root 加最新两个内部 commit 的有界集合。Rotate 的双 root 持久化/回收协议已由独立切片承担；真实 framing/hash、commit 绑定、业务条件及实际预算在本投影中是输入，不冒充完整 Model A+C。

| 配置 | generated / distinct / queue | depth / diameter | wall / peak RSS | 结果 |
| --- | --- | --- | --- | --- |
| c-internal-checkpoint-history | 847 / 453 / 0 | 14 / 14 | 0.813 s / 114,524,160 bytes | PASS |
| ce-internal-checkpoint-history-unbounded | 65 / 45 / 20 | 7 / 不适用 | 0.853 s / 109,297,664 bytes | EXPECTED_COUNTEREXAMPLE: ImageHasBoundedInternalHistory |
| ce-internal-checkpoint-history-root | 49 / 36 / 18 | 6 / 不适用 | 0.862 s / 109,215,744 bytes | EXPECTED_COUNTEREXAMPLE: SnapshotRetainsCurrentRootIdentities |
| ce-internal-checkpoint-history-legacy | 78 / 55 / 28 | 7 / 不适用 | 0.792 s / 109,494,272 bytes | EXPECTED_COUNTEREXAMPLE: LegacyReadyHasCompleteCheckpointHistory |
| ce-internal-checkpoint-history-business | 2 / 2 / 0 | 2 / 不适用 | 0.749 s / 101,711,872 bytes | EXPECTED_COUNTEREXAMPLE: SnapshotKeepsBusinessResults |

五个正向 Next 动作均有非零执行 coverage：Checkpoint 29:171、Rotate 28:224、MarkLegacy 57:113、CorruptPredecessor 112:112、Restore 226:226（保留 coverage 原始双计数）。四个错误候选分别否证无界内部保留、遗漏当前 root、旧 v1 放行缺失历史与丢弃业务结果。恢复还检查准确 predecessor；非零首 sequence 及 generation 间隔不授予无 commit 或缺少必要 suffix 的恢复权威。

最终回执 `unified-wal-model-_nzaxgir` 绑定父提交 `386478b6749250b737ea8dcc358d7e7414715b03`、161 实际输入与 325 个 checksum；29 个正向完整 PASS、79 个指定反例，108 配置全部符合预期。复现命令仍为 `python3 scripts/unified-wal/run-storage-models.py --tool /tmp/unified-wal-tla2tools-174.jar`，官方 1.7.4、单 worker、固定 seed/fp、无状态约束或对称化保留。真实行为、旧 v1/新 v2 向量与恢复证据见[有界内部历史](../local-storage/README.md#内部-checkpoint-历史的有界保留)。

CHECK_DEADLOCK FALSE 仅允许有限终态，没有 liveness、公平调度、物理 durability 或性能结论。业务/会话历史压缩、持续创建删除、filesystem 临时工作集、多 Arena、掉电、完整 A/C/D 与组合及生产 Gate 未由本投影闭合；历史 receipt/designInputs 继续保留原绑定。

## 原 journal 的压缩 checkpoint 恢复投影

2026-10-05，既有 `InternalCheckpointHistory.tla` 增加 original/rotated 两种恢复入口，仍固定八个内部 generation 与 grant/credential/tombstone 业务角色。合法 v2 在两种入口采用同一 predecessor 与条件结果门槛，original 不能因已替代内部历史被省略而拒绝完整 image。`RejectOriginalGap` 敏感性候选专门模拟遗漏，要求在指定性质上失败；不借完整日志 fallback 掩盖合法最高 image 未被选中的问题。

| 配置 | generated / distinct / queue | depth / diameter | wall / peak RSS | 结果 |
| --- | --- | --- | --- | --- |
| c-internal-checkpoint-history | 1694 / 906 / 0 | 14 / 14 | 0.897 s / 117,178,368 bytes | PASS |
| ce-internal-checkpoint-history-unbounded | 129 / 89 / 41 | 7 / 不适用 | 0.736 s / 110,592,000 bytes | EXPECTED_COUNTEREXAMPLE: ImageHasBoundedInternalHistory |
| ce-internal-checkpoint-history-root | 79 / 59 / 30 | 6 / 不适用 | 0.736 s / 109,543,424 bytes | EXPECTED_COUNTEREXAMPLE: SnapshotRetainsCurrentRootIdentities |
| ce-internal-checkpoint-history-legacy | 142 / 99 / 49 | 7 / 不适用 | 0.722 s / 110,346,240 bytes | EXPECTED_COUNTEREXAMPLE: LegacyReadyHasCompleteCheckpointHistory |
| ce-internal-checkpoint-history-business | 3 / 3 / 1 | 2 / 不适用 | 0.753 s / 101,761,024 bytes | EXPECTED_COUNTEREXAMPLE: SnapshotKeepsBusinessResults |
| ce-internal-checkpoint-history-original | 65 / 51 / 27 | 6 / 不适用 | 0.815 s / 109,969,408 bytes | EXPECTED_COUNTEREXAMPLE: OriginalRestoresValidCompactedImage |

正向五个 Next 动作均有非零 coverage：Checkpoint 58:342、Rotate 56:448、MarkLegacy 114:226、CorruptPredecessor 224:224、Restore 452:452（保留工具原始双计数）。两种 recoveryMode 均由 Init 可达。回执 `unified-wal-model-4gs7yzfe` 绑定父提交 `bb18e0fc6e9473c742ef01d38ac25af5dfbff35b`、162 实际输入与 328 个 checksum；109 配置中 29 个正向完整 PASS、80 个指定反例全部符合预期。新增反例在 `OriginalRestoresValidCompactedImage` 失败；既有 v1、root 保留、业务结果及 predecessor 性质继续检查，旧 receipt 不改绑定。

复现仍使用 `python3 scripts/unified-wal/run-storage-models.py --tool /tmp/unified-wal-tla2tools-174.jar`。本轮原临时工具缺失，curl 下载超时、续传 TLS 失败均没有执行模型或产生 PASS；恢复官方 1.7.4 完整文件并核对锁定 SHA-1/SHA-256 后才运行。单 worker、固定 seed/fp、无状态约束或对称化保持不变；真实路径见[原 journal 恢复](../local-storage/README.md#原-journal-的压缩-checkpoint-恢复)。

本模型把物理原日志/轮转、commit/framing/hash、业务条件及实际同步作为输入；CHECK_DEADLOCK FALSE 仅允许有限终态，不证明 liveness、公平调度或物理 durability。业务/会话历史压缩、完整 filesystem 临时工作集、长期负载、多 Arena、完整 A/C/D 与组合、B17 和生产 Gate 仍未闭合。


## 退役 DATA 序号区间投影

2026-10-05，`RetiredSequenceRanges.tla` 固定三个已有成功退役事实、两个抽象 stream、原 allocation 控制顺序 1/3/5、current grant 边界 2/4、连续或缺口物理序号，以及可能没有物理范围的空 FREE 必要 cut。准确 shard/generation/start 与成功 FREE 是输入；CheckBookieCut 先保留全部必要 cut，再 Advance 折叠连续区间，Finish 进入有限终态。覆盖集合必须相同，当前 grant 的顺序不能被桥接，合并保留最大 cut；空 FREE 不得因未进入区间而失去 cut。

| 配置 | generated / distinct / queue | depth / diameter | wall / peak RSS | 结果 |
| --- | --- | --- | --- | --- |
| c-retired-sequence-ranges | 192 / 192 / 0 | 6 / 6 | 0.914 s / 127,696,896 bytes | PASS |
| ce-retired-sequence-ranges-gap | 98 / 98 / 31 | 4 / 不适用 | 0.841 s / 113,917,952 bytes | EXPECTED_COUNTEREXAMPLE: RetiredCoverageIsExact |
| ce-retired-sequence-ranges-live | 101 / 101 / 31 | 4 / 不适用 | 0.838 s / 115,245,056 bytes | EXPECTED_COUNTEREXAMPLE: CurrentAllocationOrderIsNotBridged |
| ce-retired-sequence-ranges-cut | 97 / 97 / 31 | 4 / 不适用 | 0.857 s / 117,112,832 bytes | EXPECTED_COUNTEREXAMPLE: RequiredBookieCutIsRetained |
| ce-retired-sequence-ranges-empty | 49 / 49 / 31 | 2 / 不适用 | 0.848 s / 110,821,376 bytes | EXPECTED_COUNTEREXAMPLE: EmptyRetirementKeepsRequiredBookieCut |

正向三个 Next 动作均有非零 coverage：CheckBookieCut 32:32、Advance 96:96、Finish 32:32（保留工具双计数）。四个错误候选分别忽略序号缺口、current grant 顺序边界、区间最大 cut 或空 FREE 的 cut，均在指定性质上产生反例。

初次候选回执 `unified-wal-model-7kzdbnvc` 的空 FREE 敏感性错误在 Init 即违反性质，没有 runner 要求的 Next 轨迹，因此整轮 exit 1、该项 INCONCLUSIVE，不计 PASS。将 cut 计算表示为独立 CheckBookieCut 步骤后保持同一性质与失败判定，再运行得到上述明确反例；没有修改 runner 的通过标准。

最终回执 `unified-wal-model-67aozqxn` 绑定父提交 `efbcacc4e0164b548cdca160dfbdf43c031e85b8`、168 实际输入与 343 个 checksum；114 配置为 30 正向完整 PASS、84 指定反例，全部符合预期。复现仍为 `python3 scripts/unified-wal/run-storage-models.py --tool /tmp/unified-wal-tla2tools-174.jar`，官方 1.7.4、单 worker、固定 seed/fp、无状态约束或对称化保留。真实复用、未写 current grant 与缺失 Bookie authority 的验证见[区间合并](../local-storage/README.md#退役-data-序号区间的合并)。

CHECK_DEADLOCK FALSE 仅允许有限终态。本模型不新增 FREE/字节/权限或持久化权威，不证明历史可以安全删除、长期工作集有界、liveness、一般 OPEN 错误恢复、完整 A/C/D 与组合、B17 或生产 Gate；原历史 receipt/designInputs 保持原绑定。


## Arena 当前状态索引投影

2026-10-05，`ArenaStateIndices.tla` 固定四个正 allocation ID 和六个已应用命令，区分当前 grant、历史已用身份闭区间及成功高水位。Allocate 的 slot/stream 合法性与 durable 顺序 apply 是输入；独立 reference set 与首次成功序号检查区间覆盖和原 grant order。Free 删除 current，不删除已用身份。有限范围内枚举失败分配、未用低 ID 空洞、退役身份与不同操作顺序；不以 planner 的连续 ID 假定所有低 ID 都已使用。

| 配置 | generated / distinct / queue | depth / diameter | wall / peak RSS | 结果 |
| --- | --- | --- | --- | --- |
| c-arena-state-indices | 23517 / 7411 / 0 | 7 / 7 | 0.896 s / 251,920,384 bytes | PASS |
| ce-arena-state-indices-free | 26 / 16 / 12 | 3 / 不适用 | 0.736 s / 109,527,040 bytes | EXPECTED_COUNTEREXAMPLE: IssuedRangesAreExact |
| ce-arena-state-indices-hole | 31 / 18 / 13 | 3 / 不适用 | 0.832 s / 108,937,216 bytes | EXPECTED_COUNTEREXAMPLE: FeasibleUnissuedIdentityIsAccepted |
| ce-arena-state-indices-high | 2 / 2 / 0 | 2 / 不适用 | 0.755 s / 109,838,336 bytes | EXPECTED_COUNTEREXAMPLE: HighWatermarkTracksOnlySuccessfulIdentity |
| ce-arena-state-indices-order | 6 / 3 / 1 | 2 / 不适用 | 0.720 s / 106,807,296 bytes | EXPECTED_COUNTEREXAMPLE: CurrentGrantKeepsOriginalOrder |

两个正向 Next 动作均有非零 coverage：Allocate 3630:18800、Free 3780:4716（保留工具原始双计数）。四个指定错误分别遗忘 FREE 后的已用 ID、用高水位拒绝未用空洞、由失败高 ID 推进高水位、丢失原 grant order，均在指定性质上产生反例。区间精确性、互不重叠及成功高水位同时检查；这里没有模拟实际物理 slot、请求 UUID/请求代、session 或 checkpoint 字节恢复。

既有 `RetiredSequenceRanges.tla` 增加 RetireEmpty：当前空 grant 的 order 2 或 4 移除后，以先前折叠的范围作为输入再次 Advance，不重新扫描原 FREE 历史。保留物理覆盖、stream、最大 cut 和剩余 current order 边界，新增性质要求有限终态中不残留可合并的相邻范围。拒绝再折叠的错误候选单独否证这个行为。

| 配置 | generated / distinct / queue | depth / diameter | wall / peak RSS | 结果 |
| --- | --- | --- | --- | --- |
| c-retired-sequence-ranges | 492 / 476 / 0 | 12 / 12 | 0.796 s / 132,382,720 bytes | PASS |
| ce-retired-sequence-ranges-gap | 194 / 194 / 63 | 4 / 不适用 | 0.764 s / 115,556,352 bytes | EXPECTED_COUNTEREXAMPLE: RetiredCoverageIsExact |
| ce-retired-sequence-ranges-live | 197 / 197 / 63 | 4 / 不适用 | 0.826 s / 116,178,944 bytes | EXPECTED_COUNTEREXAMPLE: CurrentAllocationOrderIsNotBridged |
| ce-retired-sequence-ranges-cut | 193 / 193 / 63 | 4 / 不适用 | 0.765 s / 114,999,296 bytes | EXPECTED_COUNTEREXAMPLE: RequiredBookieCutIsRetained |
| ce-retired-sequence-ranges-empty | 81 / 81 / 63 | 2 / 不适用 | 0.743 s / 111,493,120 bytes | EXPECTED_COUNTEREXAMPLE: EmptyRetirementKeepsRequiredBookieCut |
| ce-retired-sequence-ranges-empty-barrier | 385 / 385 / 59 | 7 / 不适用 | 0.754 s / 119,488,512 bytes | EXPECTED_COUNTEREXAMPLE: EmptyBarrierRemovalIsFolded |

扩展后的四个 Next 动作都有非零 coverage：CheckBookieCut 80:80、Advance 236:236、Finish 80:80、RetireEmpty 16:32。当前 grant 的控制顺序、已成功 FREE、准确 stream 与必要 Bookie cut 仍是输入；物理证明、同步与 fallback 的派生状态清空由[真实文件验证](../local-storage/README.md#arena-当前授权与历史身份的分离)承担，不能把该有限模型当作真实恢复证明。

最终回执 `unified-wal-model-0ygt08z3` 绑定父提交 `ddf5e2700afcee6bbb0e25e130edfc7ec9d15a80`、175 实际输入与 361 个 checksum；120 配置为 31 正向完整 PASS、89 指定反例，全部符合预期。复现仍为 `python3 scripts/unified-wal/run-storage-models.py --tool /tmp/unified-wal-tla2tools-174.jar`；官方 1.7.4、单 worker、固定 seed/fp、无状态约束或对称化不变。全部 source hash 与最终代码一致，designInputs 仍绑定父提交；历史 receipt 不改绑定。

CHECK_DEADLOCK FALSE 只允许有限终态，没有公平调度、liveness、真实持久化或性能结论。稀疏 ID 与序号缺口的范围数没有常数上限，UUID/业务/会话历史尚未压缩；完整业务 image、请求代、长期负载、多 Arena、filesystem 临时工作集、掉电、完整 A/C/D 与组合、B17 及生产 Gate 仍未闭合。


## 当前 COPY 证明投影

2026-10-05，`CurrentCopyFacts.tla` 固定三条 COPY、两坐标、两 allocation、原 generation 1 与一次 generation 2 复用。成功 MOVE reference set 保留历史；查询索引只保留当前 allocation 的成功 COPY，selector 更新不删尚在当前 allocation 内的旧成功证明。Free 在 terminal/current selector 条件允许时清空该 allocation 的证明和 selector，Reuse 不复活原 generation。实际 COPY 字节、准确 location、MOVE 前驱和 Bookie 合法性、durable ordered apply 都是输入，不由有限状态自行授予。

| 配置 | generated / distinct / queue | depth / diameter | wall / peak RSS | 结果 |
| --- | --- | --- | --- | --- |
| c-current-copy-facts | 22424 / 1266 / 0 | 12 / 12 | 0.852 s / 157,171,712 bytes | PASS |
| ce-current-copy-facts-superseded | 98 / 33 / 27 | 3 / 不适用 | 0.765 s / 110,084,096 bytes | EXPECTED_COUNTEREXAMPLE: CurrentCopyProofIsExact |
| ce-current-copy-facts-rejected | 2 / 2 / 0 | 2 / 不适用 | 0.714 s / 101,466,112 bytes | EXPECTED_COUNTEREXAMPLE: CurrentCopyProofIsExact |
| ce-current-copy-facts-retired | 690 / 119 / 85 | 4 / 不适用 | 0.724 s / 111,280,128 bytes | EXPECTED_COUNTEREXAMPLE: CurrentCopyProofIsExact |
| ce-current-copy-facts-identity | 106 / 38 / 32 | 3 / 不适用 | 0.704 s / 110,821,376 bytes | EXPECTED_COUNTEREXAMPLE: QueriesBindCurrentIdentityAndDigest |

五个 Next 动作都有非零执行 coverage：ApplyMove 29:758、Tombstone 90:1020、Free 699:4876、Reuse 447:577、Query 0:15192（保留工具双计数）。正确 Query 只检查结果，保持业务和校验状态，所以不生成新 distinct 状态；敏感性候选分别丢弃仍占当前 allocation 的已替代 COPY、为失败 MOVE 发布证明、FREE 遗留证明、查询忽略 UUID/generation/digest，均通过 Next 轨迹在指定性质上失败。查询模型的三个错误参数各抽象一种身份或字节差异，实际原始记录字段与 SHA-256 由真实测试验证。

最终回执 `unified-wal-model-qx5snprk` 绑定父提交 `c23f50a4059d8853258aec36073ee8bbb4a99dda`、181 实际输入与 376 个 checksum；125 配置为 32 正向完整 PASS、93 指定反例，全部符合预期。复现仍为 `python3 scripts/unified-wal/run-storage-models.py --tool /tmp/unified-wal-tla2tools-174.jar`，官方 1.7.4、单 worker、固定 seed/fp、无状态约束或对称化保留。当前 source hash 和全部 receipt checksum 已核对，designInputs 保持父提交绑定，历史结果不改绑定。实际 image/fallback/rotation 与旧/current/失败 COPY 验证见[当前 COPY 与 selector 状态](../local-storage/README.md#当前-copy-证明与-selector-状态)。

CHECK_DEADLOCK FALSE 只允许有限终态。本投影不证明物理持久化、恢复协议、请求代或 UUID/MOVE 历史可以裁剪，也不证明全部业务 image、总内存/磁盘/临时工作集有界、长期负载、liveness、多 Arena、掉电、性能、完整 A/C/D 与组合、B17 或生产 Gate。


## ZooKeeper readiness CAS 投影

2026-10-05，`ReadinessCas.tla` 是 Model A 的持久 registration 条件更新投影：两个 birth、readiness generation 1～3、dataVersion 0～2、一次并发更新/删除重建，捕获→提交→原子条件应用→回复→fresh query 的阶段 0～5。重建改变 birth 并使 dataVersion 回到 0；成功不能跨 birth 或忽略 expected semantic generation。丢回复保留 UNKNOWN，查询以 generation 2 抽象完整目标 equality。公开 mzxid 的实际双版本比较、codec/ACL、persistent/空锚点、整次 deadline 由[真实 ZK 测试](../startup-readiness/README.md#步骤-12真实-zookeeper-readiness-cas-增量)验证，模型不展开这些字节和 backend 机制。

| 配置 | generated / distinct / queue | depth / diameter | wall / peak RSS | 结果 |
| --- | --- | --- | --- | --- |
| a-readiness-cas | 85 / 67 / 0 | 8 / 8 | 0.789 s / 109,625,344 bytes | PASS |
| ce-readiness-cas-birth | 23 / 21 / 9 | 5 / 不适用 | 0.815 s / 107,921,408 bytes | EXPECTED_COUNTEREXAMPLE: ReadinessCasPreservesBirthAndExpectedGeneration |
| ce-readiness-cas-generation | 20 / 18 / 9 | 4 / 不适用 | 0.814 s / 107,249,664 bytes | EXPECTED_COUNTEREXAMPLE: ReadinessCasPreservesBirthAndExpectedGeneration |
| ce-readiness-cas-unknown | 29 / 27 / 13 | 5 / 不适用 | 0.823 s / 106,037,248 bytes | EXPECTED_COUNTEREXAMPLE: UnresolvedLostReplyNeverReportsApplied |

七个 Next 动作都有非零 coverage：Capture 2:2、Submit 1:5、Replace 8:15、ConcurrentUpdate 8:15、Commit 5:5、Reply 18:18、Query 23:23。三个错误规则分别取消 birth 检查、忽略 expected readiness generation、将丢回复直接提升成功，都在指定性质上给出 Next 轨迹反例。atomic multi、唯一不可变 birth、有效 record/scope/ACL 及有序修改是输入假设；它不是 MetadataStore quorum、服务生命周期或 ZooKeeper 内部协议证明。

最终回执 `unified-wal-model-o4tiypiw` 绑定父提交 `aee4bed4060d6f52097fc113978c462662b314e5`、224 source inputs 与 388 个 checksum；129 配置为 33 正向完整 PASS、96 指定反例，全部符合预期。复现命令 `python3 scripts/unified-wal/run-storage-models.py --tool /tmp/unified-wal-tla2tools-174.jar`，仍使用官方 TLC 1.7.4、单 worker、固定 seed/fp、无状态约束或对称化。全部 source/checksum 已核对；5 份 designInputs 始终绑定父提交，历史回执不改绑定。

CHECK_DEADLOCK FALSE 只允许有限终态；无 fairness/liveness、真实 quorum/持久化、ACL、安装/激活、startup 与 ephemeral 生命周期、性能或生产 authority 结论。完整 A/C/D 与组合、Block G、长期负载和服务 readiness 仍未完成。


## Session 绑定的 registration publication 投影

2026-10-05，`RegistrationPublication.tla` 为 Model A 的单 Bookie/两代 publisher 局部投影。authority 1→2 一次变化，service/writable 各用 0/1/2 表示 absent/旧 projection/刷新或替代 projection，节点版本 0～3；允许一个 info refresh、old demote capture 和 nonce 2 replacement。实际原子 multi、已验证的记录/ACL/scope、唯一 immutable session marker、demote/失连后 local gate 关闭为输入。它检查 authority 查读后变化、两种 info/writable 竞争、同版本新 nonce 与旧删除、丢回复保持 UNKNOWN，不展开 protobuf/字节、ZooKeeper 内部协议或真实 startup。

| 配置 | generated / distinct / queue | depth / diameter | wall / peak RSS | 结果 |
| --- | --- | --- | --- | --- |
| a-registration-publication | 1793 / 868 / 0 | 13 / 13 | 0.899 s / 117,899,264 bytes | PASS |
| ce-registration-publication-authority | 5 / 5 / 1 | 4 / 不适用 | 0.809 s / 108,396,544 bytes | EXPECTED_COUNTEREXAMPLE: PublishedOnlyAgainstMatchingAuthority |
| ce-registration-publication-session | 303 / 197 / 102 | 8 / 不适用 | 0.796 s / 117,047,296 bytes | EXPECTED_COUNTEREXAMPLE: OldDemotionCannotRemoveReplacement |
| ce-registration-publication-service | 103 / 74 / 39 | 7 / 不适用 | 0.799 s / 111,443,968 bytes | EXPECTED_COUNTEREXAMPLE: WritableProjectionMatchesService |
| ce-registration-publication-unknown | 50 / 39 / 22 | 6 / 不适用 | 0.801 s / 108,838,912 bytes | EXPECTED_COUNTEREXAMPLE: UnresolvedPublicationNeverReportsApplied |

12 个 Next 动作都有非零 coverage：CaptureInfo 1:1、AuthorityChange 13:264、PublishInfo 2:2、CaptureWritable 6:18、CaptureRefresh 4:8、Refresh 12:176、CreateWritable 27:68、Reply 76:296、Query 190:296、CaptureDemote 58:58、Replace 151:278、Demote 327:327。四个错误规则分别省略 authority 条件、取消旧 session marker 检查、不递增相同 service-info 字节的版本、直接提升丢回复，均在指定性质给出 Next 轨迹反例。

最终回执 `unified-wal-model-s992kax7` 绑定父提交 `8e2d6668168e9983a270781d027f714159908c89`、234 source inputs 与 403 checksum；134 配置为 34 正向完整 PASS、100 指定反例，全部符合预期。官方 TLC 1.7.4、单 worker、固定 seed/fp、无状态约束或对称化；复现仍用 `python3 scripts/unified-wal/run-storage-models.py --tool /tmp/unified-wal-tla2tools-174.jar`。全部 source/hash 校验已执行，5 份 designInputs 保持父提交绑定，旧结果不改绑。实际 server session expiration、ACL/codec 和发布交错另见[真实 registration 验证](../startup-readiness/README.md#步骤-12真实-ephemeral-发布增量)。

CHECK_DEADLOCK FALSE 只允许有限终态。当前性质不要求一个以前成功的 ephemeral hint 在 authority 后续变化后仍有效；实际读取仍需 fresh authority/reference，持续服务准入必须单独接失连/降级事件。模型不证明 liveness/fairness、quorum、真实持久化或 local receipt、全部工作集、listener/placement、生产 registry、安装激活、完整 A/C/D/组合、Block G、掉电或性能。


## 本地 readiness receipt 发布投影

2026-10-05，`LocalReadinessPublication.tla` 描述既有 receipt 1、不同内容的目标 receipt 2、请求 generation 1/2、一次 write/rename/回复/退出/cold reopen。exclusive writer、严格 scope/codec、atomic rename 与成功文件/目录 barrier 的 crash 保留规则是输入。temp 不参与恢复；新实例在文件与目录恢复同步后才提供 receipt。对当前 UNKNOWN 实例的读则直接失败，不能因新文件已可读而解析成功。这里没有新建 recovery 状态或改变 DATA/control 的隔离合同。

| 配置 | generated / distinct / queue | depth / diameter | wall / peak RSS | 结果 |
| --- | --- | --- | --- | --- |
| a-local-readiness-publication | 63 / 56 / 0 | 11 / 11 | 0.739 s / 109,740,032 bytes | PASS |
| ce-local-readiness-publication-file | 39 / 34 / 5 | 7 / 不适用 | 0.737 s / 111,362,048 bytes | EXPECTED_COUNTEREXAMPLE: AppliedHasFileAndDirectoryBarriers |
| ce-local-readiness-publication-directory | 39 / 34 / 5 | 7 / 不适用 | 0.735 s / 111,034,368 bytes | EXPECTED_COUNTEREXAMPLE: AppliedHasFileAndDirectoryBarriers |
| ce-local-readiness-publication-unknown | 36 / 33 / 5 | 7 / 不适用 | 0.740 s / 109,772,800 bytes | EXPECTED_COUNTEREXAMPLE: UncertainInstanceNeverReturnsReceipt |
| ce-local-readiness-publication-pending | 24 / 22 / 7 | 5 / 不适用 | 0.734 s / 108,691,456 bytes | EXPECTED_COUNTEREXAMPLE: UnpublishedTempNeverBecomesReceipt |
| ce-local-readiness-publication-generation | 21 / 20 / 9 | 5 / 不适用 | 0.734 s / 110,231,552 bytes | EXPECTED_COUNTEREXAMPLE: OneGenerationBindsOneReceipt |

11 个 Next 动作均有非零 coverage：Begin 2:2、WriteTemp 1:1、SyncFile 2:2、Rename 1:1、SyncDirectory 2:2、Reply 1:1、Query 5:5、Crash 10:17、Reopen 10:10、RecoveryFailure 10:10、ReadRecovered 10:10。五个错误规则分别跳过文件同步、目录同步、UNKNOWN 读隔离、采用未发布 pending、允许同代不同内容覆盖；均沿 Next 轨迹违反指定性质。真实文件、writer lock、codec 和子进程退出验证见[本地 receipt 实现](../startup-readiness/README.md#步骤-12真实本地-readiness-receipt)。

最终回执 `unified-wal-model-4h3zm7ae` 绑定父提交 `4f9c2c24151cbef9ddb8c7da9b6fc86e9ec10fc7`、245 source inputs 和 421 checksum；140 配置为 35 正向完整 PASS、105 指定反例，全部符合预期。47 Java 输入均包含于该 source set，全部 source/checksum 及 5 个父提交 designInputs 已核对。复现命令保持 `python3 scripts/unified-wal/run-storage-models.py --tool /tmp/unified-wal-tla2tools-174.jar`，官方 TLC 1.7.4、单 worker、固定 seed/fp，无状态约束或对称化，旧回执不改绑定。

CHECK_DEADLOCK FALSE 只允许有限终态。物理 FileChannel 和进程退出证据不等于掉电，输入的 fsync/atomic-rename 假设也不由 TLC 自证。真实 recovery/device/delete catch-up、生产 startup/registry、持续失连准入、quorum、长期工作集、性能、完整 A/C/D 与组合、Block G 和生产 Gate 均未由该 receipt 投影闭合。


## 控制权威 cold read 投影

2026-10-05，`AuthorityRead.tla` 为 [RFC-0001 fixed-key authority read](../../RFC-0001-profile-capability-install.md#66-control-authority-与-protected-local-binding) 的单次读取投影：一个 requested key、八种结果（valid/foreign/corrupt/incomplete/ephemeral/wrong-state/NONODE/backend error）、generation/version 1→2 一次更新、sync/get/captured callback/timeout。它允许 snapshot 捕获后发生更新以及延迟交付；FOUND 必须仍使用同一 snapshot 的 payload/version，不能误装新版本。只有实际 NONODE 返回 absent，其余错误保持 unavailable，无效记录 quarantine。

| 配置 | generated / distinct / queue | depth / diameter | wall / peak RSS | 结果 |
| --- | --- | --- | --- | --- |
| a-authority-read | 168 / 120 / 0 | 5 / 5 | 0.768 s / 108,347,392 bytes | PASS |
| ce-authority-read-key | 87 / 62 / 25 | 4 / 不适用 | 0.789 s / 107,003,904 bytes | EXPECTED_COUNTEREXAMPLE: FoundMatchesRequestedKey |
| ce-authority-read-invalid | 92 / 66 / 26 | 4 / 不适用 | 0.778 s / 107,855,872 bytes | EXPECTED_COUNTEREXAMPLE: FoundHasValidPersistentCompleteRecord |
| ce-authority-read-error | 10 / 10 / 8 | 2 / 不适用 | 0.781 s / 104,202,240 bytes | EXPECTED_COUNTEREXAMPLE: AbsentOnlyFromActualNoNode |
| ce-authority-read-version | 121 / 89 / 31 | 5 / 不适用 | 0.811 s / 107,806,720 bytes | EXPECTED_COUNTEREXAMPLE: FoundKeepsOneAtomicSnapshot |

六个 Next 动作均有非零 coverage：SyncSuccess 8:16、SyncFailure 8:16、Capture 16:16、ConcurrentUpdate 32:48、Reply 24:24、Timeout 24:40。四个错误规则分别忽略 key、放行 invalid view、将错误视为不存在、混用更新后的 store version，均沿 Next 轨迹违反指定性质。atomic ZooKeeper get 的 bytes/Stat 一致性、实际解析/CRC/ACL/Stat 校验以及 deadline 为输入假设；这些机制由[真实 ZK 读取测试](../profile-control/README.md#步骤-12真实-zk-控制权威读取)分别验证。该模型不展开字节、真实会话内部状态或更新的发布协议。

最终回执 `unified-wal-model-9jv2zpi_` 绑定父提交 `0231ac595a4d881488c913002dcb3002a4564484`、285 source inputs 和 436 checksum；145 配置为 36 正向完整 PASS、109 指定反例，全部符合预期。Java run 的 81 输入均为该 source set 子集，全部 source/checksum 及 5 个父提交 designInputs 已核对。复现仍为 `python3 scripts/unified-wal/run-storage-models.py --tool /tmp/unified-wal-tla2tools-174.jar`，官方 TLC 1.7.4、单 worker、固定 seed/fp，无状态约束或对称化；旧回执保持原绑定。

CHECK_DEADLOCK FALSE 只允许有限终态，无 fairness/liveness 结论。FOUND 是一次读取 snapshot，没有连续 authority 租约；无全局 sidecar/membership、backlink、fanout/page、publication CAS、安装激活、实际 startup/delete catch-up、生产 registry/连续准入、quorum、SDK/socket 总预算、长期工作集、性能或掉电结论。完整 A/C/D、组合与 Block G 继续 OPEN，读取投影不提升步骤 12 或生产 Gate。

## Session 绑定的本地 DATA 准入投影

2026-10-05，`RegistrationAdmission.tla` 投影一个 bound session/nonce、exact owned view 与本地 identity、一次 final reconciliation、一次连接丢失/事件观察/重连和一次实际已提交 DATA I/O。startup 检查完成及 backend read 的准确性是输入，不由该模型产生。transport loss 与观察事件分开；gate 热检查还验证可见连接/session，但不提供瞬时网络故障检测或远端 authority 租约。

| 配置 | generated / distinct / queue | depth / diameter | wall / peak RSS | 结果 |
| --- | --- | --- | --- | --- |
| a-registration-admission | 466 / 220 / 0 | 12 / 12 | 0.854 s / 113,623,040 bytes | PASS |
| ce-registration-admission-session | 194 / 117 / 36 | 6 / 不适用 | 0.849 s / 112,050,176 bytes | EXPECTED_COUNTEREXAMPLE:AdmissionUsesOriginalConnectedSession |
| ce-registration-admission-reconnect | 103 / 72 / 36 | 5 / 不适用 | 0.841 s / 109,117,440 bytes | EXPECTED_COUNTEREXAMPLE:RetiredAttemptNeverReopens |
| ce-registration-admission-late | 135 / 92 / 41 | 5 / 不适用 | 0.858 s / 110,575,616 bytes | EXPECTED_COUNTEREXAMPLE:RetiredAttemptNeverReopens |
| ce-registration-admission-release | 200 / 122 / 38 | 6 / 不适用 | 0.839 s / 112,017,408 bytes | EXPECTED_COUNTEREXAMPLE:OutstandingOwnershipWaitsForActualTerminals |

11 个 Next 动作 coverage 均非零：Publish 2:4、CaptureRead 2:10、CompleteRead 4:36、TransportLoss 18:48、ObserveLoss 19:100、Reconnect 36:98、Close 81:112、Admit 2:2、FinishIo 18:18、FinishOwner 18:18、Release 18:18。四个 mutation 分别忽略原 session/连接、重连重开、迟到读取重开及退休即释放 I/O ownership，均沿真实 Next 轨迹违反指定不变量。源输入在复制后可提前释放、已知 pre-I/O refusal 及多请求/SDK 自动重建不在该单已提交 I/O 投影内。

最终 `unified-wal-model-1jom7_es` 绑定父提交 `f7aebbd97412d0ccd78c37cb9d6dad7fcbf6c37a`、293 source inputs/451 checksum：150 配置为 37 正向完整 PASS、113 指定反例，全部符合预期。Java `unified-wal-registration-admission-final-qdyeu_q7` 的 106 输入均在该 source set 中；source/checksum、五个父提交 designInputs 已逐项核对。官方 TLC 1.7.4、单 worker、固定 seed/fp、无 symmetry/state constraint，复现命令不变，历史回执不重绑定。

[实际 watcher/session 与 storage I/O 测试](../startup-readiness/README.md#步骤-12session-绑定的本地-data-准入)覆盖真实 server 断连/同 session 重连、expiry、延迟实际回调以及正常 DATA force/排空/重启；它们与模型输入分别验证。CHECK_DEADLOCK FALSE 只允许有限终态，无 fairness/liveness、quorum、瞬时 lease、持续 remote authority、总 SDK/socket 工作集、真实 startup/delete facts、生产服务接线、掉电或性能结论；完整 A/C/D、A+C/C+D、Block G 与步骤 12/生产 Gate 继续 OPEN。


## Startup DATA 排他投影

2026-10-05，`StartupDataExclusion.tla` 为一次 graph restart 的资源顺序投影：初始 scope 为 none 或 foreign，acquire 后才恢复控制，files 接管同一排他，再 bootstrap；失败先进入清理，actual startup cleanup 终结才释放自己资源。foreign writer 的外部释放是独立动作。Native writer exclusion、scope/format 校验、recovery force 及 cleanup 的真实终结是输入，不由该模型证明；没有兼容/删除/readiness 成功常量。

| 配置 | generated / distinct / queue | depth / diameter | wall / peak RSS | 结果 |
| --- | --- | --- | --- | --- |
| a-startup-data-exclusion | 28 / 20 / 0 | 6 / 6 | 0.840 s / 106,889,216 bytes | PASS |
| ce-startup-data-exclusion-order | 4 / 4 / 2 | 2 / 不适用 | 0.777 s / 99,926,016 bytes | EXPECTED_COUNTEREXAMPLE:RecoveryStartsWithOwnExclusion |
| ce-startup-data-exclusion-handoff | 11 / 11 / 4 | 4 / 不适用 | 0.845 s / 106,905,600 bytes | EXPECTED_COUNTEREXAMPLE:FileHandoffKeepsExclusion |
| ce-startup-data-exclusion-cleanup | 13 / 12 / 4 | 4 / 不适用 | 0.790 s / 106,102,784 bytes | EXPECTED_COUNTEREXAMPLE:ClosedAttemptHasNoOwnedScope |
| ce-startup-data-exclusion-foreign | 8 / 8 / 3 | 3 / 不适用 | 0.795 s / 100,040,704 bytes | EXPECTED_COUNTEREXAMPLE:ForeignWriterKeepsItsScope |

九个 Next 动作均产生后继：Acquire 2:2、RejectBusy 1:1、Recover 2:2、AdoptFiles 2:2、Bootstrap 2:2、Fail 4:6、Cleanup 2:6、Close 0:2、ForeignRelease 3:3。Close 的 0:2 表示生成的 closed 状态已由其他路径发现，不表示未执行；不为改变统计增加状态。四个 mutation 对应上述顺序、移交、泄漏和 foreign ownership，均触发指定性质。

最终 `unified-wal-model-9t0slz3k` 绑定父提交 `a0054d5771f9f40bd1447f324380bc73672ca12f`、299 source inputs/466 checksum；155 配置为 38 正向完整 PASS、117 指定反例，全部符合预期。Java `unified-wal-startup-exclusion-final-8m0a_enj` 单独冻结 288 输入，106 重合输入准确匹配，481 个去重输入与五个父提交 designInputs 已核对。官方 1.7.4 tool、单 worker、固定 seed/fp、无 symmetry/state constraint，复现命令不变。首次 `unified-wal-model-h_emm1xt` 新模块因运算符优先级解析失败，补齐括号后重建冻结输入；旧失败输出保留，不计 PASS，也不改绑定。

[真实 startup 锁回归](../local-storage/README.md#restart-的-data-排他与控制恢复顺序)与既有所有文件恢复路径分别验证 native 机制。CHECK_DEADLOCK FALSE 只允许有限终态；无 fairness/liveness、一般未 CLOSED 恢复、真实 compatibility/device/delete facts、服务 readiness、生产 listener/registry、quorum、长期总工作集、掉电或性能结论。完整 A/C/D、A+C/C+D、Block G 及生产 Gate 仍 OPEN。

## Profile 普通 Add 分发投影

2026-10-06 `ProfileNormalDispatch`覆盖一个normal请求、同步source交接、真实I/O/owner终结、排到event loop的完成对象及一个真实下游响应buffer。输入假设是合法TLS、canonical frame、实际local credential/activation校验、已计费同步复制及真实I/O终结；Java socket回归分别验证这些具体实现。模型不证明TLS/socket总bytes、实际fsync、readiness、网络送达、时间、公平性、quorum或完整A/C。

性质为：超限header不能分配body；成功晚于实际durability和owner终结；实际DATA unknown不能变NONE；取消不能提前释放I/O资源；已排队completion和下游response在各自终结前保持frame与响应额度；已提交源已经复制。响应等待取消与实际终结分别建模，关闭连接不会抹掉旧I/O。五个mutation逐个破坏header、早成功、unknown映射、I/O保护、排队/下游响应保护。

| 配置 | generated / distinct / queue | depth / diameter | elapsed / peak RSS | 结果 |
| --- | --- | --- | --- | --- |
| a-profile-normal-dispatch | 94 / 69 / 0 | 11 / 11 | 0.849 s / 111,083,520 bytes | PASS |
| ce-profile-normal-dispatch-header | 5 / 5 / 3 | 2 / 不适用 | 0.803 s / 103,661,568 bytes | EXPECTED_COUNTEREXAMPLE:HeaderBoundPrecedesBody |
| ce-profile-normal-dispatch-early | 25 / 25 / 5 | 5 / 不适用 | 0.851 s / 108,953,600 bytes | EXPECTED_COUNTEREXAMPLE:SuccessRequiresActualDurability |
| ce-profile-normal-dispatch-unknown | 56 / 48 / 8 | 8 / 不适用 | 0.780 s / 109,625,344 bytes | EXPECTED_COUNTEREXAMPLE:NoneIsNotUnknownData |
| ce-profile-normal-dispatch-io | 25 / 25 / 5 | 5 / 不适用 | 0.804 s / 109,051,904 bytes | EXPECTED_COUNTEREXAMPLE:IoProtectionWaitsForActualTerminals |
| ce-profile-normal-dispatch-response | 27 / 27 / 5 | 6 / 不适用 | 0.809 s / 109,264,896 bytes | EXPECTED_COUNTEREXAMPLE:ResponseProtectionWaitsForWriteTerminal |

最终 `unified-wal-model-2o8mhfyh` tool process exit 0，161配置为39正向完整PASS/122指定反例；冻结332 inputs/485 checksums，父提交 `25343736717180ed6e05818f44cfa88f02409600`及五个designInputs保持父提交绑定。Java `unified-wal-local-uA3Q6Jy1` 922输入与模型116重合hash准确一致，去重union 1138已核验；其14类383项为378通过/5 macOS Linux-affinity跳过。官方tool 1.7.4、单worker、固定seed/fp、无symmetry/state constraint，CHECK_DEADLOCK FALSE不声称活性证明。先前 `unified-wal-model-9awbrno9` 是尚无queued阶段、测试fixture未最终修正的探索运行，保留原输入，不充当本轮最终回执。

[真实隔离socket与取消红绿](../profile-wire/README.md#真实隔离普通-add-接线)验证具体网络/文件/额度交错。生产listener、真实控制授权、TLS/socket/native与存储总账、startup readiness、完整A/C/D、A+C/C+D、Block G、长期负载、掉电和性能仍未完成。

## Profile FENCE 分发投影

2026-10-06 `ProfileFenceDispatch`覆盖一个完整FENCE、其真实控制I/O/owner终结、排队completion和响应；DATA预算已满，控制建立后可另耗尽UNCLASSIFIED名额。exact TLS/context/credential、既有pre-cut drain、实际force与owner终结是实现输入，不是密码学、fsync或完整Model A/C证明。性质为授权与原子workspace换账先于CONTROL/效果；success晚于实际force与apply；断连保留在途控制副本及queued/write额度；实际UNKNOWN不能变为本次pre-I/O NONE；后续错凭据不成功，未分类满额不阻止已建立控制lane。六个mutation分别破坏这些边界。

| 配置 | generated / distinct / queue | depth / diameter | elapsed / peak RSS | 结果 |
| --- | --- | --- | --- | --- |
| a-profile-fence-dispatch | 186 / 126 / 0 | 10 / 10 | 0.851 s / 113,704,960 bytes | PASS |
| ce-profile-fence-dispatch-auth | 13 / 13 / 9 | 2 / 不适用 | 0.787 s / 107,069,440 bytes | EXPECTED_COUNTEREXAMPLE:ControlRequiresExactAuthorization |
| ce-profile-fence-dispatch-workspace | 16 / 15 / 10 | 2 / 不适用 | 0.861 s / 106,725,376 bytes | EXPECTED_COUNTEREXAMPLE:ControlWorkspaceMovesAtomically |
| ce-profile-fence-dispatch-early | 56 / 54 / 22 | 4 / 不适用 | 0.886 s / 113,033,216 bytes | EXPECTED_COUNTEREXAMPLE:SuccessRequiresFenceForceAndOwner |
| ce-profile-fence-dispatch-terminal | 56 / 54 / 22 | 4 / 不适用 | 0.922 s / 110,379,008 bytes | EXPECTED_COUNTEREXAMPLE:ControlCopiesWaitForActualTerminal |
| ce-profile-fence-dispatch-unknown | 114 / 93 / 16 | 6 / 不适用 | 0.924 s / 112,066,560 bytes | EXPECTED_COUNTEREXAMPLE:UnknownIsNotPreIoNone |
| ce-profile-fence-dispatch-unclassified | 152 / 113 / 13 | 7 / 不适用 | 0.860 s / 114,556,928 bytes | EXPECTED_COUNTEREXAMPLE:ExistingControlSurvivesUnclassifiedFull |

最终 `unified-wal-model-g0cie706` tool exit 0，168配置为40正向完整PASS/128指定反例；冻结340 inputs/506 checksums，父提交 `66644a6021bf9e4ea2e6fdcbec4d5ceadd465daf`与五个designInputs绑定父提交，未改历史回执。Java `unified-wal-local-P3zaRCUQ`的922输入与模型116重合hash精确一致，union 1146核验；14类391项为386通过/5 macOS Linux-affinity跳过。官方tool 1.7.4、单worker、heap512MiB/direct64MiB、seed1/fp0、无symmetry/state constraint；CHECK_DEADLOCK FALSE，不声称活性。

首次 `unified-wal-model-t7u8k4qc`发现新模块语法错误后被明确终止，exit130且未产生完整最终回执，不能计PASS；修正后 scoped `unified-wal-model-c23rx1hb`七配置通过，再扩大为上述最终矩阵。统计是固定有限配置验证记录，不能替代 [真实socket与文件交错](../profile-wire/README.md#真实隔离-fence-与控制额度)，不证明TLS/socket/native/storage总量、fresh readiness、production listener、完整control-plane、quorum、时间、掉电或性能；步骤11及整体Gate仍未完成。


## Profile ADD_RECOVERY 分发投影

2026-10-06 `ProfileRecoveryDispatch`复用既有模型runner，检查一个已安装本地grant下的单条恢复写入：exact route/credential/intent及代际/range/BK坐标为输入，源可用额度为有限布尔条件；成功必须在实际I/O与owner结果判定之后且grant在判定时仍有效，恢复不得激活normal，已开始DATA的unknown不得变NONE，取消不得提前释放实际I/O保护。

`submitted`抽象已开始的物理DATA尝试；已知未执行I/O的解析/owner拒绝统一压缩为Refuse，不枚举原队列全部中间状态。源复制与实际提交合为一个动作，CopyRequiresSourceCredit只检查先有源额度的前置条件，不证明allocator或TLS/socket实际总内存。排队completion/下游响应保护沿用ProfileNormalDispatch投影与真实socket回归。grant在owner结果判定后关闭不撤销已经判定的成功；模型分别保留grantAtResult与当前grantOpen。

| 配置 | generated / distinct / queue | depth / diameter | elapsed / peak RSS | 结果 |
| --- | --- | --- | --- | --- |
| a-profile-recovery-dispatch | 165 / 104 / 0 | 8 / 8 | 0.847 s / 110,444,544 bytes | PASS |
| ce-profile-recovery-dispatch-auth | 12 / 12 / 8 | 2 / 不适用 | 1.010 s / 106,217,472 bytes | EXPECTED_COUNTEREXAMPLE:SubmittedRequiresExactOpenGrant |
| ce-profile-recovery-dispatch-source | 9 / 9 / 6 | 2 / 不适用 | 0.956 s / 107,986,944 bytes | EXPECTED_COUNTEREXAMPLE:CopyRequiresSourceCredit |
| ce-profile-recovery-dispatch-close | 97 / 70 / 13 | 6 / 不适用 | 0.975 s / 107,118,592 bytes | EXPECTED_COUNTEREXAMPLE:SuccessRequiresCurrentGrantAtDecision |
| ce-profile-recovery-dispatch-activate | 61 / 47 / 16 | 4 / 不适用 | 0.826 s / 109,740,032 bytes | EXPECTED_COUNTEREXAMPLE:RecoveryDoesNotActivateNormal |
| ce-profile-recovery-dispatch-unknown | 91 / 66 / 16 | 5 / 不适用 | 0.906 s / 110,592,000 bytes | EXPECTED_COUNTEREXAMPLE:NoneIsNotSubmittedData |
| ce-profile-recovery-dispatch-io | 38 / 32 / 17 | 3 / 不适用 | 0.793 s / 106,676,224 bytes | EXPECTED_COUNTEREXAMPLE:IoProtectionWaitsForActualTerminals |

六个mutation分别跳过exact OPEN tuple、在没有源额度时复制、忽略close后的资格、恢复激活normal、把unknown/stale映成NONE、取消提前释放I/O资源，均命中指定性质。首次 scoped `unified-wal-model-9d0m0sbd`七配置通过，再执行最终既有矩阵；没有改变旧模型或重绑旧receipt。

最终 `unified-wal-model-vuhtsh6x` tool exit0，175配置为41正向完整PASS/134指定反例；冻结348 source inputs/527 checksums，父提交 `6f44a97fc9c679cda0e0919663497c4dde1d4715`与五个designInputs保持父提交绑定。Java `unified-wal-local-WaFfvigz`922输入、14类398项为393通过/5 macOS Linux-affinity跳过；116共享hash精确匹配，1154去重union与20份Java回执checksum已核验，Spotless/Checkstyle通过。官方tool1.7.4、单worker、heap512MiB/direct64MiB、seed1/fp0、无symmetry/state constraint，CHECK_DEADLOCK FALSE，复现命令不变。

[真实恢复socket与文件交错](../profile-wire/README.md#真实隔离-add_recovery-接线)另验CRC、固定头N、实际源占用、force、关闭grant、断连/native锁及restart重复点读。该有限投影不证明实际mTLS/密码学、fsync、确定missing、真实RepairIntent授权/CLOSED、coordinator/client quorum、生产listener/readiness、总内存、业务timer、公平性/活性、掉电或性能；完整A/C/D、组合、Block G及步骤11/14/Gate仍OPEN。


## Profile backing 额度投影

2026-10-06 [ProfileBackingBudget](modules/ProfileBackingBudget.tla)投影一个连接、两个根backing、每根至多两个引用、capacity 1..3、每lane额度6/每连接额度8。真实授权转账是既有FENCE分发投影的输入；模型保留原read backing的allocation lane。扩容显式保留旧根及新temporary，完成或失败后才归还对应份额；response从已计费workspace拆出独立backing，断连释放名额/base但保留仍持有的根和workspace，最后引用才释放根。

六个mutation分别跳过根分配计费、扩容只计新旧差额、先释放一个view就归还全部backing、断连提前归还backing、拆出response却继续重复计workspace，以及转账时搬走原read backing；均命中各自指定性质。首次scoped `unified-wal-model-9w4exq9h`七配置通过，正向11,482 generated / 2,404 distinct / queue0、depth/diameter9。调用runner时一次Python参数错误与一次非可执行脚本调用不计模型结果，未修改文件权限。

该投影不枚举实际ByteBuf字节、heap/native分配器、多个连接调度、每次缩容、TLS密码学/engine内部内存、kernel buffers、credit等对象开销或storage统一总预算。对应[Java allocator及真实socket回归](../profile-wire/README.md#netty-backing-buffer-额度接线)另验实际capacity/refcount、缩容/拒绝后数据/index、TLS拒绝和相同port控制可达性。模型不证明fsync、quorum、时间、公平性/活性、生产listener/readiness、掉电或性能；完整A/C/D、组合、Block G和步骤11/Gate仍OPEN。

最终 `unified-wal-model-wci6ds2z` tool exit0，42正向完整PASS/140指定反例、358冻结inputs/548 checksums。Java `unified-wal-local-V4ePSx6Q`924 inputs、15类405项为400通过/5平台跳过及style通过，118共享输入与1164去重union精确匹配；五个designInputs绑定父提交 `15df420b42a69b88c1b9c03372d920d54ea49bf5`，历史回执不变。官方tool1.7.4、单worker、heap512MiB/direct64MiB、seed1/fp0、无symmetry/state constraint，CHECK_DEADLOCK FALSE。以下统计来自最终矩阵：

| 配置 | generated / distinct / queue | depth / diameter | elapsed / peak RSS | 结果 |
| --- | --- | --- | --- | --- |
| a-profile-backing-budget | 11482 / 2404 / 0 | 9 / 9 | 0.972 s / 156,958,720 bytes | PASS |
| ce-profile-backing-budget-allocation | 3 / 3 / 1 | 2 / 不适用 | 0.861 s / 111,771,648 bytes | EXPECTED_COUNTEREXAMPLE:BackingsKeepFullCredit |
| ce-profile-backing-budget-resize | 15 / 15 / 12 | 3 / 不适用 | 0.849 s / 112,443,392 bytes | EXPECTED_COUNTEREXAMPLE:ResizeKeepsOldAndNewCredit |
| ce-profile-backing-budget-view | 96 / 69 / 55 | 4 / 不适用 | 0.908 s / 114,311,168 bytes | EXPECTED_COUNTEREXAMPLE:BackingsKeepFullCredit |
| ce-profile-backing-budget-close | 22 / 22 / 19 | 3 / 不适用 | 0.838 s / 112,295,936 bytes | EXPECTED_COUNTEREXAMPLE:BackingsKeepFullCredit |
| ce-profile-backing-budget-response | 2 / 2 / 0 | 2 / 不适用 | 0.800 s / 110,690,304 bytes | EXPECTED_COUNTEREXAMPLE:ResponseSplitConservesReservation |
| ce-profile-backing-budget-lane | 21 / 21 / 18 | 3 / 不适用 | 0.848 s / 111,493,120 bytes | EXPECTED_COUNTEREXAMPLE:ReadBackingKeepsAllocationLane |


2026-10-06 [接收至存储源副本](../profile-wire/README.md#接收至存储源副本交接)沿用现有ProfileBackingBudget两个根的capacity/refcount/关闭投影及ordinary/recovery分发的I/O/owner终结投影重新验证。Java接线把源副本实际交给该allocator，所以第二根可以对应source；原模型仍不枚举source子额度、对齐池交接或完整组合，未新增性质或扩大有限模型声明。真实socket另验复制前后两份账本、断连与实际force/native锁的生命周期。

当前源副本交接最终矩阵 `unified-wal-model-37pz7m69` tool exit0，42正向配置完整PASS/140指定反例，358冻结inputs/548 checksums；Java `unified-wal-local-1PlKPtbk`924 inputs、15类407项为402通过/5平台跳过，Spotless/Checkstyle `unified-wal-source-style-Thj6khH7`通过。118共享输入与1164去重union精确匹配，五个designInputs绑定父提交 `b61887fd56b2037b886114ef924c40cbf26460ad`；此前矩阵及统计保留各自原运行身份，未新增模型或性质，也不声明上述独立切片已经形成source/pool/TLS组合证明。

## 标准 metadata backlink 创建投影

2026-10-06 `ProfileMetadataCreate`仅建模一个ledger key的一次create-if-absent、创建成功或失败后的一次实际resolve read、可选删除/不同instance重建及初始内容更新。kind为absent、matching、other-instance、other-sidecar、other-initial-payload、corrupt、legacy或read-error八类；dataVersion仅0/1，重建再次为0。创建成功回调只进入read阶段，不能以提交对象形成成功；实际read取得的snapshot/version保留到结果判断。成功只陈述自身snapshot，read之后的删除或更新不使结果成为持续租约。

五个mutation分别取消instance/reference/初始内容匹配、把resolving absence改为NONE，或创建成功后跳过实际read。正向完整遍历152 generated/137 distinct/queue0、search depth与diameter均8；五个mutation各命中指定性质。原子backend create/read和strict codec是输入，创建callback本身不提供完整snapshot证明；52-byte编码/CRC/ID/用户字段复制/真实client行为由Java验证。没有reservation/全E安装、READY或delete cut、后续mutation CAS、完整sidecar/retention、quorum、liveness、性能或完整A/C/D组合证明。

最终 `unified-wal-local-asvuwC2b` tool exit0，981冻结source inputs，相关6类70/70通过、0 failure/error/skip；`unified-wal-metadata-style-RCI77e1E` Spotless/Checkstyle exit0，source集一致，12份Java回执checksums已核验。模型 `unified-wal-model-7mo2zzed` tool exit0，188配置为43正向完整PASS/145指定反例，414冻结inputs/566 checksums；179共享输入及1216去重union一致，五个designInputs绑定父提交 `ffc3e8cdea081280a8f21da8c63dea3c235dfbd7`。此前不存在的isOpen接口、初次style超长行与上述实际snapshot回归失败均不计PASS；最终验证在实际read修复之后，旧运行保留原输入。验证耗时不是性能前后对照。 官方tool1.7.4、单worker、heap512MiB/direct64MiB、seed1/fp0，无symmetry/state constraint，CHECK_DEADLOCK FALSE；历史矩阵和Wave0回执保持原绑定。

| 配置 | generated / distinct / queue | depth / diameter | elapsed / peak RSS | 结果 |
| --- | --- | --- | --- | --- |
| a-profile-metadata-create | 152 / 137 / 0 | 8 / 8 | 0.829 s / 111,493,120 bytes | PASS |
| ce-profile-metadata-create-instance | 48 / 41 / 17 | 4 / 不适用 | 0.859 s / 110,739,456 bytes | EXPECTED_COUNTEREXAMPLE:SuccessMatchesInstance |
| ce-profile-metadata-create-sidecar | 50 / 43 / 18 | 4 / 不适用 | 0.803 s / 111,149,056 bytes | EXPECTED_COUNTEREXAMPLE:SuccessMatchesSidecar |
| ce-profile-metadata-create-payload | 52 / 45 / 19 | 4 / 不适用 | 0.842 s / 110,460,928 bytes | EXPECTED_COUNTEREXAMPLE:SuccessMatchesInitialPayload |
| ce-profile-metadata-create-absence | 84 / 73 / 22 | 6 / 不适用 | 0.858 s / 110,526,464 bytes | EXPECTED_COUNTEREXAMPLE:AbsenceDoesNotProveNoEffect |
| ce-profile-metadata-create-ack | 17 / 17 / 7 | 3 / 不适用 | 0.810 s / 107,347,968 bytes | EXPECTED_COUNTEREXAMPLE:SuccessHasActualMetadataSnapshot |


## PREPARING reservation authority 投影

2026-10-06 `ProfileReservationAuthority`验证一次真实root冷读之后的INSTALL authority选择。kind为absent、matching、错误instance/domain key、目标未计划、坏/不完整/超限/ephemeral root及unavailable六类；purpose分别INSTALL与ACTIVATE，root version为0/1，可在read前后发生一次替换。返回保留read的scope与版本，不把后来的root/version拼入旧snapshot，也不声称read之后仍持有持续authority。

五个mutation分别忽略key、忽略计划目标、接受坏root、PREPARING授予normal activation以及混用后来的version。原子bytes/Stat read和严格codec是输入；真实root创建的完整payload/instance/reference匹配与UNKNOWN边界沿用既有metadata-create投影，加上[真实ZK root与接口回归](../profile-control/README.md#步骤-12真实-preparing-reservation-与-install-冷读)。此处没有root publication CAS、all-E local durability/receipt、READY/child-page/delete cut、birth lineage、orphan GC/retention、quorum、公平性/活性、fsync、实际TLS、掉电、性能或完整A/C/D组合证明。最终 `unified-wal-local-K2IDKqqY` tool exit0，985冻结source inputs，相关7类80/80通过、0 failure/error/skip；`unified-wal-reservation-style-aHMhoBh8` Spotless/Checkstyle exit0，source集精确一致，13份Java回执checksums已核验。最终模型 `unified-wal-model-2iaijevf` tool exit0，194配置为44正向完整PASS/150指定反例，425冻结inputs/584 checksums；183共享输入及1227去重union一致，五个designInputs绑定父提交 `2877d79cc2a4c61db1f9fae272c7b171592f4abe`。官方tool1.7.4、单worker、heap512MiB/direct64MiB、seed1/fp0，无symmetry/state constraint，CHECK_DEADLOCK FALSE，不声称活性、性能或生产authority。历史回执保持原绑定。

新正向投影完整遍历76 generated/66 distinct/queue0，search depth和diameter均4；五个mutation命中各自指定性质。

| 配置 | generated / distinct / queue | depth / diameter | elapsed / peak RSS | 结果 |
| --- | --- | --- | --- | --- |
| a-profile-reservation-authority | 76 / 66 / 0 | 4 / 4 | 0.791 s / 107,118,592 bytes | PASS |
| ce-profile-reservation-authority-key | 43 / 33 / 16 | 3 / 不适用 | 0.850 s / 105,857,024 bytes | EXPECTED_COUNTEREXAMPLE:GrantedMatchesRequestedKey |
| ce-profile-reservation-authority-target | 45 / 35 / 17 | 3 / 不适用 | 0.792 s / 107,118,592 bytes | EXPECTED_COUNTEREXAMPLE:GrantedTargetIsPlanned |
| ce-profile-reservation-authority-corrupt | 47 / 37 / 18 | 3 / 不适用 | 0.790 s / 106,004,480 bytes | EXPECTED_COUNTEREXAMPLE:GrantedHasValidCommittedRoot |
| ce-profile-reservation-authority-normal | 54 / 44 / 21 | 3 / 不适用 | 0.839 s / 106,692,608 bytes | EXPECTED_COUNTEREXAMPLE:PreparingOnlyAuthorizesInstall |
| ce-profile-reservation-authority-version | 65 / 55 / 24 | 4 / 不适用 | 0.804 s / 104,677,376 bytes | EXPECTED_COUNTEREXAMPLE:ProjectionKeepsReadVersion |


## Physical INSTALL 分发投影

2026-10-06 `ProfileInstallDispatch`投影一个typed INSTALL通过预留control driver的提交、write、force、owner apply和completion。输入为已验证exact principal/committed scope/descriptor/physical target、control容量、route为absent/same/conflicting及实际I/O结果；已有route的normal状态可以true或false，INSTALL都保持原状态。首次absent install仍为inactive，不能借INSTALL越过READY/activation。

停止等待只产生UNKNOWN，不取消已经提交的任务；held资源保留至实际I/O和owner都终结。callback成功必须有当前barrier和owner apply；failure仍由owner终结并释放资源。六个mutation分别取消exact check、提交即成功、仅I/O终结即成功、停止等待提前释放、INSTALL激活normal与UNKNOWN映NONE，检查各自指定性质。

本投影不枚举真实credential/加密、文件framing/byte buffers、全E并发/receipt coordinator、local generation历史、READY/CAS/delete cut、业务timer、公平性/活性、实际TLS、fsync硬件、quorum、掉电、性能或完整A/C/D组合；[真实FileChannel与ZK/typed endpoint验证](../profile-control/README.md#步骤-12preparing-接线真实-local-inactive-install)分别提供实际对应证据。旧source/config/run不重绑定，最终 `unified-wal-local-xzOtn4EW` tool exit0，987冻结source inputs，相关10类102项为97通过/5 macOS Linux-affinity跳过、0 failure/error；`unified-wal-install-style-mVFZelYE` Spotless/Checkstyle exit0，source集精确一致，16份Java回执checksums已核验。最终模型 `unified-wal-model-nbfk9uf2` tool exit0，201配置为45正向完整PASS/156指定反例，435冻结inputs/605 checksums；185共享输入及1237去重union一致，五个designInputs绑定父提交 `eb8408cb5cac686e0e7e9a72cd706877c067c1ce`。官方tool1.7.4、单worker、heap512MiB/direct64MiB、seed1/fp0，无symmetry/state constraint，CHECK_DEADLOCK FALSE，不声称活性、性能或生产authority。

新正向投影完整遍历200 generated/170 distinct/queue0，search depth和diameter均7；六个mutation命中各自指定性质。

| 配置 | generated / distinct / queue | depth / diameter | elapsed / peak RSS | 结果 |
| --- | --- | --- | --- | --- |
| a-profile-install-dispatch | 200 / 170 / 0 | 7 / 7 | 0.791 s / 112,230,400 bytes | PASS |
| ce-profile-install-dispatch-exact | 43 / 43 / 39 | 2 / 不适用 | 0.852 s / 109,035,520 bytes | EXPECTED_COUNTEREXAMPLE:AdmissionHasExactAuthorityAndTarget |
| ce-profile-install-dispatch-barrier | 44 / 44 / 39 | 2 / 不适用 | 0.838 s / 108,937,216 bytes | EXPECTED_COUNTEREXAMPLE:InstallSuccessHasBarrier |
| ce-profile-install-dispatch-owner | 110 / 107 / 19 | 4 / 不适用 | 0.812 s / 109,920,256 bytes | EXPECTED_COUNTEREXAMPLE:InstallSuccessHasOwnerApply |
| ce-profile-install-dispatch-stop | 82 / 82 / 37 | 3 / 不适用 | 0.861 s / 109,330,432 bytes | EXPECTED_COUNTEREXAMPLE:SubmittedOwnershipWaitsForBothTerminals |
| ce-profile-install-dispatch-normal | 140 / 127 / 19 | 5 / 不适用 | 0.797 s / 110,952,448 bytes | EXPECTED_COUNTEREXAMPLE:InstallPreservesNormalState |
| ce-profile-install-dispatch-unknown | 82 / 82 / 37 | 3 / 不适用 | 0.792 s / 107,741,184 bytes | EXPECTED_COUNTEREXAMPLE:SubmittedUnknownNeverBecomesNone |


## 初始 all-E metadata 门槛投影

2026-10-06 `ProfileInitialMetadata`固定E/W/A=3/3/2、三个nonblocking target、有效/无效初始计划、每target exact durable/known refusal/UNKNOWN/错绑定success四类结果。发送整个E后按任意顺序收结果；只允许全部matching durable及fresh root snapshot相同且committed的路径提交metadata。read之后的root变化/删除仍允许metadata形成inert orphan：模型明确不把root read和standard metadata create视作跨key原子事务，也不从metadata alone返回writer。

六个mutation分别用A代替E、接受错binding、忽略root version、接受missing root、忽略初始plan和从metadata alone返回writer。实际target force/结果关联、严格descriptor/HELLO/password-credential/metadata codec及SDK原子操作为输入，分别由[真实协调门槛与receipts测试](../profile-control/README.md#步骤-12all-e-后的初始-metadata-协调门槛)、原physical INSTALL/metadata-create投影验证；不枚举所有byte/crypto、persistent receipt/target page、READY/delete cut CAS、全E activation、完整restart/client quorum、actual INSTALL TLS/wire、活性/公平性、掉电/性能或完整A/C/D组合。

既有ProfileInstallDispatch另保留birthGeneration1与可推进的currentGeneration1/2，新增`InstallReceiptKeepsBirthGeneration`；既有正向及六个敏感性配置保持原性质，新增current-gen-as-install mutation命中该性质。真实activation后same INSTALL retry和文件replay回归支持此映射，未改control record schema。历史run/source/config统计保留原绑定，最终 `unified-wal-local-CgLIZ88L` tool exit0，991冻结source inputs，相关10类351项为346通过/5 macOS Linux-affinity跳过、0 failure/error，含完整Bookie控制log 245项；`unified-wal-initial-style-E82C4Awl` Spotless/Checkstyle exit0，source集精确一致，16份Java回执checksums已核验。最终模型 `unified-wal-model-ufcpvr46` tool exit0，209配置为46正向完整PASS/163指定反例，448冻结inputs/629 checksums；189共享输入及1250去重union一致，五个designInputs绑定父提交 `60a0bab6e5793cff79216ea6c99de080bf60ae0f`。官方tool1.7.4、单worker、heap512MiB/direct64MiB、seed1/fp0，无symmetry/state constraint，CHECK_DEADLOCK FALSE，不声称活性、性能或生产authority。

新正向gate投影完整遍历6168 generated/2767 distinct/queue0，search depth和diameter均12；六个gate及一个birth-generation mutation均命中指定性质。

| 配置 | generated / distinct / queue | depth / diameter | elapsed / peak RSS | 结果 |
| --- | --- | --- | --- | --- |
| a-profile-initial-metadata | 6168 / 2767 / 0 | 12 / 12 | 0.904 s / 141,115,392 bytes | PASS |
| ce-profile-initial-metadata-ack | 6324 / 3094 / 783 | 8 / 不适用 | 0.967 s / 153,305,088 bytes | EXPECTED_COUNTEREXAMPLE:MetadataWaitsForAllE |
| ce-profile-initial-metadata-binding | 6121 / 2710 / 200 | 9 / 不适用 | 0.958 s / 145,883,136 bytes | EXPECTED_COUNTEREXAMPLE:MetadataHasExactDurableResults |
| ce-profile-initial-metadata-root | 6157 / 2761 / 69 | 10 / 不适用 | 0.901 s / 149,749,760 bytes | EXPECTED_COUNTEREXAMPLE:MetadataHasSameRootSnapshot |
| ce-profile-initial-metadata-absence | 6157 / 2761 / 68 | 10 / 不适用 | 0.923 s / 150,994,944 bytes | EXPECTED_COUNTEREXAMPLE:MetadataHasCommittedReservation |
| ce-profile-initial-metadata-plan | 129 / 129 / 127 | 2 / 不适用 | 0.796 s / 110,034,944 bytes | EXPECTED_COUNTEREXAMPLE:InvalidPlanHasNoReservation |
| ce-profile-initial-metadata-writer | 6162 / 2763 / 67 | 10 / 不适用 | 0.914 s / 146,079,744 bytes | EXPECTED_COUNTEREXAMPLE:MetadataAloneNeverReturnsWriter |
| ce-profile-install-dispatch-generation | 185 / 157 / 19 | 6 / 不适用 | 0.863 s / 113,147,904 bytes | EXPECTED_COUNTEREXAMPLE:InstallReceiptKeepsBirthGeneration |


## Initial READY lifecycle 投影

2026-10-06 `ProfileReadyLifecycle`投影initial PREPARING/READY/DELETE head和一份有界page、publisher的read及conditional transaction、root/page出生身份0/1与dataVersion重置、metadata birth0/1但dataVersion均0。opaque head version与semantic control generation分别保留；READY只推进version/sequence，DELETE才推进control generation。metadata check是一次snapshot，之后变化不把pub结果变成lease；activation的READY proof也是读取时事实，未模型化瞬时跨store撤权。

七个mutation分别信任inert page、忽略head birth、忽略page birth、忽略metadata birth、越过delete cut、删除target reference以及不要求all-E。strict codec、ACL/immutable marker、SDK atomic transaction/metadata result/local barrier为输入；[实际ZK及all-E/files组合](../profile-control/README.md#步骤-12initial-ready目标留存与-delete_intent-cas)另验证这些映射。旧initial-metadata/INSTALL投影保持其各自范围；实际physical activation/barrier/fence由原control log及Java组合验证。

此投影不是full lifecycle：没有replacement/repair/大量pages/GC、broker crash/restart coordinator、actual INSTALL/ACTIVATE TLS、key/certificate、完整delete/drain或normal writer availability，也未检查fairness/liveness/quorum/powerloss/performance及full A/C/D组合。原统计/run/source不改绑；最终 `unified-wal-local-qlSVYzAe` tool exit0，995冻结source inputs，相关11类364项为359通过/5 macOS Linux-affinity跳过、0 failure/error，含完整Bookie控制log 245项；`unified-wal-ready-style-j5vX2UG6` Spotless/Checkstyle exit0，source集精确一致，17份Java回执checksums已核验。最终模型 `unified-wal-model-2cqx367s` tool exit0，217配置为47正向完整PASS/170指定反例，461冻结inputs/653 checksums；193共享输入及1263去重union一致，五个designInputs绑定父提交 `9a48e6346453f85c5d02615ef9e4503f30ce13c6`。官方tool1.7.4、单worker、heap512MiB/direct64MiB、seed1/fp0，无symmetry/state constraint，CHECK_DEADLOCK FALSE，不声称活性、性能或生产authority。

新正向lifecycle投影完整遍历355 generated/218 distinct/queue0，search depth/diameter均10；七个mutation各命中指定性质。

| 配置 | generated / distinct / queue | depth / diameter | elapsed / peak RSS | 结果 |
| --- | --- | --- | --- | --- |
| a-profile-ready-lifecycle | 355 / 218 / 0 | 10 / 10 | 0.782 s / 112,574,464 bytes | PASS |
| ce-profile-ready-lifecycle-inert | 22 / 19 / 12 | 3 / 不适用 | 0.794 s / 105,054,208 bytes | EXPECTED_COUNTEREXAMPLE:ActivationHasPublishedReady |
| ce-profile-ready-lifecycle-head-birth | 35 / 26 / 14 | 4 / 不适用 | 0.821 s / 109,379,584 bytes | EXPECTED_COUNTEREXAMPLE:BirthChecksProtectTheCurrentRoot |
| ce-profile-ready-lifecycle-page-birth | 87 / 61 / 28 | 5 / 不适用 | 0.899 s / 110,100,480 bytes | EXPECTED_COUNTEREXAMPLE:ReadyReferencesActualPageBirth |
| ce-profile-ready-lifecycle-metadata-birth | 117 / 76 / 35 | 5 / 不适用 | 0.820 s / 109,887,488 bytes | EXPECTED_COUNTEREXAMPLE:ReadyValidatedMetadataBirth |
| ce-profile-ready-lifecycle-delete | 135 / 87 / 35 | 6 / 不适用 | 0.831 s / 110,821,376 bytes | EXPECTED_COUNTEREXAMPLE:DeleteCutNeverRollsBack |
| ce-profile-ready-lifecycle-retention | 140 / 93 / 37 | 6 / 不适用 | 0.806 s / 110,870,528 bytes | EXPECTED_COUNTEREXAMPLE:PublishedTargetsSurviveDelete |
| ce-profile-ready-lifecycle-all-e | 46 / 32 / 20 | 4 / 不适用 | 0.765 s / 106,217,472 bytes | EXPECTED_COUNTEREXAMPLE:ReadyRequiresAllEReceipts |


## Initial availability 恢复投影

2026-10-06 `ProfileInitialAvailability`投影一次fresh/PREPARING/candidate/READY/DELETE恢复round、E=3及A=2，每target current active/known refusal/UNKNOWN/wrong binding/historical fenced五类结果。既有candidate/READY代表metadata曾存在，missing required record不能重建；提交整个E、收集结果、metadata取证和最后root取证分别推进。Delete/metadata移除可与round交错；final metadata和root是各自读取时的facts，不是continuing lease或cross-key/target atomic snapshot。

七个mutation各取消all-E、bound、known、current active、required metadata不重建、final delete及final metadata边界。strict codec/peer/correlation、actual barrier/current local state、metadata/root出生身份和前序publication为inputs，由既有模型及[实际resume/replay测试](../profile-control/README.md#步骤-12可恢复初始协调与-all-e-availability-snapshot)分别验证。不是actual control TLS/wire、SDK writer、complete process restart/repair/replacement/full delete/GC、公平性/活性/quorum/powerloss/performance或full A/C/D组合证明；旧run/source/统计不改绑，最终 `unified-wal-local-v7KaiG3E` tool exit0，996冻结source inputs，相关11类373项为368通过/5 macOS Linux-affinity跳过、0 failure/error，含完整Bookie控制log 245项；`unified-wal-availability-style-p5JRNimT` Spotless/Checkstyle exit0，source集精确一致，17份Java回执checksums已核验。最终模型 `unified-wal-model-bf63gmga` tool exit0，225配置为48正向完整PASS/177指定反例，471冻结inputs/677 checksums；194共享输入及1273去重union一致，五个designInputs绑定父提交 `54f47dbd4ced724587fc9e8fadf91e1e054ef575`。官方tool1.7.4、单worker、heap512MiB/direct64MiB、seed1/fp0，无symmetry/state constraint，CHECK_DEADLOCK FALSE，不声称活性、性能或生产authority。

新正向availability投影完整遍历53500 generated/24750 distinct/queue0，search depth/diameter均10；七个mutation命中各自指定性质。

| 配置 | generated / distinct / queue | depth / diameter | elapsed / peak RSS | 结果 |
| --- | --- | --- | --- | --- |
| a-profile-initial-availability | 53500 / 24750 / 0 | 10 / 10 | 1.185 s / 220,348,416 bytes | PASS |
| ce-profile-initial-availability-ack | 35006 / 18004 / 6501 | 7 / 不适用 | 1.176 s / 203,587,584 bytes | EXPECTED_COUNTEREXAMPLE:AvailabilityWaitsForAllE |
| ce-profile-initial-availability-binding | 44121 / 19558 / 2984 | 8 / 不适用 | 1.151 s / 200,704,000 bytes | EXPECTED_COUNTEREXAMPLE:AvailabilityHasCurrentBoundResults |
| ce-profile-initial-availability-unknown | 44081 / 19539 / 2989 | 8 / 不适用 | 1.185 s / 198,721,536 bytes | EXPECTED_COUNTEREXAMPLE:AvailabilityDoesNotCountUnknown |
| ce-profile-initial-availability-history | 44161 / 19577 / 2979 | 8 / 不适用 | 1.152 s / 207,126,528 bytes | EXPECTED_COUNTEREXAMPLE:AvailabilityDoesNotCountHistoricalFence |
| ce-profile-initial-availability-rebuild | 1002 / 1002 / 999 | 2 / 不适用 | 0.883 s / 113,868,800 bytes | EXPECTED_COUNTEREXAMPLE:RequiredMetadataIsNeverRebuilt |
| ce-profile-initial-availability-delete | 49001 / 21876 / 2373 | 9 / 不适用 | 1.306 s / 219,578,368 bytes | EXPECTED_COUNTEREXAMPLE:AvailabilityObservesReadyNotDelete |
| ce-profile-initial-availability-metadata | 49006 / 21880 / 2374 | 9 / 不适用 | 1.189 s / 225,804,288 bytes | EXPECTED_COUNTEREXAMPLE:AvailabilityHasMatchingMetadataFact |


## 初始 control socket 分发投影

2026-10-06 `ProfileInitialSocketDispatch`投影一个有界初始control请求的cold任务、actual capability/权限与容量输入、event-loop原子转账、实际force/owner终结、cold等待超时、回复write及断连。等待EL的future已超时就不能迟到提交；已提交physical UNKNOWN仍保留原副本/I/O custody，回复完成也不是I/O完成。capability不授予权限，workspace与CONTROL同账移动。实际证书/字节/crypto/完整ZK READY及all-E coordinator不在该有限状态中展开，由[声明范围内的真实socket回归](../profile-wire/README.md#真实隔离初始-control-接线)分别验证；不证明SDK/quorum、完整A/C/D组合、活性/公平性、总内存、fsync硬件、长期/掉电/性能或生产authority。

首轮 `unified-wal-model-2b4n23xf` exit1：原投影允许cold timeout已回复终结后再次Reject排队，违反ColdAndIoCopiesRemainCharged；修正为单次cold拒绝/owner不重复发送原已返回结果，未删除或弱化性质。其余旧输入、反例和原结果目录保留。focused `unified-wal-initial-model-focused-j6hz734t`正向完整PASS。

最终 `unified-wal-local-j9MoHs4Q` tool exit0：1002冻结输入，相关17类417项为412通过/5 macOS Linux-affinity跳过，0失败/错误；Bookie控制log完整252项包括七项新socket回归。`unified-wal-initial-socket-style-9ga4n66n` Spotless/Checkstyle exit0，142冻结Java source与test inputs一致；23份Java/style回执checksums已核验。最终模型 `unified-wal-model-0tgx4f8_` tool exit0：233配置为49正向完整PASS/184指定反例，483冻结输入/701 checksums，197共享输入及1288去重union一致；五个designInputs仍绑定运行父提交 `d7a68ffc03eb9a21bd91c1250fb6fb7b42d61f5e`。原始receipt/source/config/run不重绑定。官方tool1.7.4、单worker、heap512MiB/direct64MiB、seed1/fp0、无symmetry/state constraint、CHECK_DEADLOCK FALSE；有限PASS不代替实际TLS、fsync/掉电、内存、活性/公平性或完整A/C/D组合证据。

新正向完整1054 generated/546 distinct/queue0，depth与diameter均11；七个mutation各命中指定性质。

| 配置 | generated / distinct / queue | depth / diameter | elapsed / peak RSS | 结果 |
| --- | --- | --- | --- | --- |
| a-profile-initial-socket | 1054/546/0 | 11 / 11 | 0.858 s / 119,390,208 bytes | PASS |
| ce-profile-initial-socket-capability | 123/91/55 | 3 / 不适用 | 0.856 s / 110,444,544 bytes | EXPECTED_COUNTEREXAMPLE:ControlRequiresNegotiation |
| ce-profile-initial-socket-authority | 114/86/53 | 3 / 不适用 | 0.816 s / 111,329,280 bytes | EXPECTED_COUNTEREXAMPLE:ControlRequiresExactAuthority |
| ce-profile-initial-socket-workspace | 146/98/59 | 3 / 不适用 | 0.918 s / 108,118,016 bytes | EXPECTED_COUNTEREXAMPLE:WorkspaceMovesWithControl |
| ce-profile-initial-socket-terminal | 21/19/17 | 2 / 不适用 | 0.827 s / 107,839,488 bytes | EXPECTED_COUNTEREXAMPLE:ColdAndIoCopiesRemainCharged |
| ce-profile-initial-socket-barrier | 492/304/113 | 5 / 不适用 | 0.798 s / 111,591,424 bytes | EXPECTED_COUNTEREXAMPLE:SuccessRequiresActualForceAndOwner |
| ce-profile-initial-socket-unknown | 491/303/112 | 5 / 不适用 | 0.851 s / 112,951,296 bytes | EXPECTED_COUNTEREXAMPLE:SubmittedUnknownIsNeverNone |
| ce-profile-initial-socket-late | 231/141/77 | 4 / 不适用 | 0.859 s / 110,592,000 bytes | EXPECTED_COUNTEREXAMPLE:TimedOutAdmissionNeverSubmits |


## 初始 client 关联与 slot 投影

2026-10-06 `ProfileInitialClientCorrelation`投影同一channel两个请求，quota为1或2；peer/binding覆盖TLS/HELLO Bookie/incarnation/engine/capability及完整frame/body字段的已验证输入。enqueue先持有quota，写入与wire未来分别终结，取消已发送future不会撤销server命令或复活future。correlated拒绝不会变durable成功，网络UNKNOWN不能映NONE，frame ID不重用。bad peer/binding、取消或reply先释放slot、重用frame ID、UNKNOWN映NONE及取消future复活七个mutation各命中指定性质。

实际server成功的force/owner证据来自现有initial socket/physical INSTALL投影及[真实文件/ZK/mTLS组合](../profile-wire/README.md#非阻塞初始-control-client)，本projection只抽象已验证输入；不枚举byte/crypto、全部三目标/metadata生命周期、NIO内部队列/timer/DNS、统一总内存、所有failure交错、SDK/quorum、活性、公平性或完整A/C/D组合。caller提供resolved地址，client拒绝unresolved，不在EL新增解析层。

最终 `unified-wal-local-4qDodbai` tool exit0，1004冻结inputs，相关18类425项为420通过/5 macOS Linux-affinity跳过、0 failure/error；包含8项真实initial client/ZK/file/socket组合。`unified-wal-client-final-style-tyd9etl4` Spotless/Checkstyle exit0，200冻结Java source与去重union精确一致；24份Java/style回执checksums已核验。最终模型 `unified-wal-model-e10jjm5g` tool exit0，241配置为50正向完整PASS/191指定反例、494冻结inputs/725 checksums；199共享inputs及1299去重union一致，五个designInputs绑定父提交 `57a614c304e72985d850033c36dcff3f7232cdd5`。官方tool1.7.4，单worker、heap512MiB/direct64MiB、seed1/fp0，无symmetry/state constraint，CHECK_DEADLOCK FALSE；有限模型不替代实际crypto/fsync硬件、活性、公平性、整个内存总量、完整A/C/D、掉电、长期、性能或生产authority。历史source/config/run/receipt保持原绑定。

新正向完整2775 generated/1281 distinct/queue0，depth与diameter均15。

| 配置 | generated / distinct / queue | depth / diameter | elapsed / peak RSS | 结果 |
| --- | --- | --- | --- | --- |
| a-profile-initial-client | 2775 / 1281 / 0 | 15 / 15 | 0.923 s / 134,070,272 bytes | PASS |
| ce-profile-initial-client-peer | 22 / 22 / 11 | 4 / 不适用 | 0.905 s / 110,755,840 bytes | EXPECTED_COUNTEREXAMPLE:RequestRequiresVerifiedPeer |
| ce-profile-initial-client-binding | 36 / 35 / 18 | 5 / 不适用 | 0.851 s / 111,591,424 bytes | EXPECTED_COUNTEREXAMPLE:SuccessHasBoundReply |
| ce-profile-initial-client-cancel | 17 / 17 / 6 | 4 / 不适用 | 0.914 s / 108,560,384 bytes | EXPECTED_COUNTEREXAMPLE:SlotsWaitForBothTerminals |
| ce-profile-initial-client-write | 36 / 35 / 18 | 5 / 不适用 | 0.801 s / 112,410,624 bytes | EXPECTED_COUNTEREXAMPLE:SlotsWaitForBothTerminals |
| ce-profile-initial-client-id | 26 / 26 / 12 | 4 / 不适用 | 0.794 s / 108,331,008 bytes | EXPECTED_COUNTEREXAMPLE:FrameIdsNeverAlias |
| ce-profile-initial-client-unknown | 32 / 31 / 15 | 5 / 不适用 | 0.854 s / 110,837,760 bytes | EXPECTED_COUNTEREXAMPLE:NetworkUncertaintyNeverMeansNone |
| ce-profile-initial-client-revive | 95 / 80 / 46 | 6 / 不适用 | 0.866 s / 113,328,128 bytes | EXPECTED_COUNTEREXAMPLE:CancelledFutureIsNotRevived |


## 普通 client 的 source 与 output 投影

2026-10-06 ProfileNormalClientSource区分caller source snapshot、outbound、write promise、wire等待和quota。quota在retain/clone前取得，queued source等实际EL任务决定encode或abort；copy完成即可归还source，output只在write终结后归还，slot同时等write/wire；cancel不复活future，NONE/UNKNOWN不能成为durable ACK。bound/capacity/server outcome为有限输入，TLS/完整关联/server force/owner依赖既有切片和[当前实际回归](../profile-wire/README.md#普通-data-client-与-sdk-接线边界)。不展开byte/pool/crypto/kernel、原SDK callback/replacement/close、全部timer交错、真实总预算、quorum算法、活性、公平性或完整A/C/D。

最终 unified-wal-local-yMsEARYx tool exit0：1007冻结inputs，相关18类431项为426通过/5 macOS Linux-affinity跳过，0 failure/error；原8项initial及新增6项ordinary DATA组合通过。unified-wal-normal-client-style-n2bhqnuh Spotless/Checkstyle exit0，203冻结Java source与union一致；24份Java/style回执checksums已核验。模型 unified-wal-model-5720ce65 tool exit0：247配置为51正向完整PASS/196指定反例，506冻结inputs/743 checksums，204共享及1309去重union一致，5个designInputs绑定父提交8f6bf6f9501741ca98d968ae2efc2ea8b8563fc5。官方tool1.7.4、单worker、heap512MiB/direct64MiB、seed1/fp0，无symmetry/state constraint，CHECK_DEADLOCK FALSE；不证明crypto/fsync硬件、活性、公平性、整个内存、完整A/C/D、长期/掉电/性能或production authority。历史source/config/run/receipt不重绑定。

正向完整85 generated/66 distinct/queue0，depth与diameter均7；5个mutation命中指定性质。

| 配置 | generated / distinct / queue | depth / diameter | elapsed / peak RSS | 结果 |
| --- | --- | --- | --- | --- |
| a-profile-normal-client-source | 85 / 66 / 0 | 7 / 7 | 0.998 s / 111,476,736 bytes | PASS |
| ce-profile-normal-client-source-quota | 16 / 16 / 11 | 2 / 不适用 | 0.962 s / 101,335,040 bytes | EXPECTED_COUNTEREXAMPLE:SourceSnapshotRequiresQuota |
| ce-profile-normal-client-source-cancel | 25 / 25 / 8 | 3 / 不适用 | 0.865 s / 107,347,968 bytes | EXPECTED_COUNTEREXAMPLE:QueuedSourceWaitsForTaskDecision |
| ce-profile-normal-client-source-bound | 26 / 26 / 10 | 3 / 不适用 | 0.892 s / 105,529,344 bytes | EXPECTED_COUNTEREXAMPLE:SubmittedEntryFitsInstalledBound |
| ce-profile-normal-client-source-status | 39 / 39 / 10 | 4 / 不适用 | 0.777 s / 103,841,792 bytes | EXPECTED_COUNTEREXAMPLE:SuccessRequiresDurableStatus |
| ce-profile-normal-client-source-write | 34 / 34 / 7 | 4 / 不适用 | 0.774 s / 106,577,920 bytes | EXPECTED_COUNTEREXAMPLE:OutputWaitsForActualWrite |

### 固定初始 SDK bridge 与当前验证

2026-10-06，ProfileSdkBridge 在已有 DATA client/source/dispatch 切片之上验证一个原 SDK entry、三个目标、A=2 和最多两次尝试；NONE/UNKNOWN、backoff、deadline、原 quorum、单次 callback、本地 stop 和 metadata OPEN 分开建模。五个单独变异分别覆盖遗忘 UNKNOWN、丢失重试字节、改变坐标、重复回调与错误发布 Classic CLOSED。原 LedgerHandle/PendingAddOp 及新 adapter/context/config/stats 的源码纳入当前 source hash；乱序双 entry 连续回调由真实 socket test 与原 PendingAddOp regressions 检验，未将一 entry 模型扩大成两 entry 的证明。

当前 unified-wal-model-5b1j8vic exit0，253配置为52个完整正向PASS/201个指定反例；521实际输入、761回执checksum逐项一致。新正向为9613 generated/2476 distinct、queue0、depth/diameter12，elapsed约0.875s、peak RSS158597120 bytes；这只是有限检查器成本。Java unified-wal-local-lHxRMIwp 实际13类418项、413通过/5 macOS Linux-affinity跳过、零失败/错误，包含19项真实 socket及19项原 PendingAddOp。135个 server Java 的 style快照匹配，19份 Java/style checksums核验；212共享输入、1318去重union一致，5个designInputs绑定父提交6df172fb5e664fe12ba642ebd85960acee84343b，历史回执不重绑。完整实际命令见[SDK实现记录](../profile-wire/README.md#固定初始-ensemble-的原-sdk-写入接线)。

运行仍用官方 tool1.7.4、单worker、heap512 MiB/direct64 MiB、seed1/fp0、无symmetry/state constraint，CHECK_DEADLOCK FALSE。模型没有展开真实时钟/网络、byte/kernel总量、持续 metadata lease、换组/恢复/close、liveness/fairness、完整A+C+D或生产Gate；模型PASS不替代TLS/fsync/掉电/长期和性能证据。

### 标准 membership 出生绑定 CAS 与当前验证

2026-10-06，ProfileMembershipCas 抽象一次metadata mutation/一次UNKNOWN retry、两个physical parent birth、一个只读child和一个额外version。允许同metadata bytes/token被恢复为新parent，也允许freeze/更高版本同mapping在真实cold phase之间竞争。最后guard+version共同约束write；同一次replacement始终复用captured birth，成功必须有exact mapping/next version，creator与user字段不变。七个变异均得到指定性质反例；namespace覆盖的性质检查实际状态，不能靠最后返回CONFLICT掩盖已经发生的覆盖。

最终 unified-wal-model-q8ngbbve exit0：261配置为53个完整正向PASS/208个指定反例，530输入、785checksum逐项一致。新正向136 generated/94 distinct、queue0、depth/diameter10，elapsed约0.790s、peak RSS114196480 bytes；仅是检查器成本。Java unified-wal-local-JohSldiz 实际8类116项全部通过、零失败/错误/跳过，1009冻结输入；style匹配135 server Java，14份Java/style checksums。212共享输入与1327去重union一致，5个designInputs仍绑定父提交15017d22994f0d990bb4842e792f5c804778bade。首轮 mps2xdo9 的新配置是parse-error INCONCLUSIVE，总体exit1；最终修正另建run，历史回执不改绑定。命令与真实guard RED见[metadata实现记录](../profile-control/README.md#标准-membership-的出生绑定-cas)。

参数仍为官方tool1.7.4、单worker、heap512 MiB/direct64 MiB、seed1/fp0、无symmetry/state constraint、CHECK_DEADLOCK FALSE；没有证明ZK真实多节点quorum、ACL安全/legacy隔离、bytes/schema、clock/liveness、target history、activation、writer exposure/cleanup或full A+C+D。metadata结果是观测时snapshot，不是持续authority lease。

### Replacement lifecycle 与当前验证

2026-10-06，ProfileReplacementLifecycle新增两个sequential same-start publication、有界candidate、metadata/root/page条件同一次CAS、DELETE cut、保留prefix及current authority。grants和discovery只证明其read cut，不能投影为持续lease或即时本地撤权。八个单独mutations均违反预期性质。实际typed endpoint+文件driver支持ACTIVATE_REPLACEMENT，但new-peer INSTALL准备、actual replacement wire/TLS client和original SDK gated resend未接通。

最终 unified-wal-model-772_q39j exit0：270配置为54完整正向PASS/216指定反例；541输入、812checksum逐项一致。新正向1128 generated/646 distinct、queue0、depth/diameter15，elapsed约0.891s、peak RSS120471552 bytes，仅是检查器成本。Java unified-wal-local-3apvD5t0 实际9类147项全部通过，1010冻结输入；20个ZK lifecycle用例、23个typed/file INSTALL及19个actual socket/19个原PendingAddOp分别保留对应范围。Style匹配136 server Java，15份Java/style checksums；213共享输入/1338去重union一致，5个designInputs绑定父提交186d00e87beb6b1ac3c893f8bc5e7fe6fa9523bd。旧source/run/config/receipt不改绑定，actual metadata-check RED见[实现记录](../profile-control/README.md#replacement-授权与目标留存)。

仍用官方tool1.7.4、单worker、heap512 MiB/direct64 MiB、seed1/fp0、无symmetry/state constraint，CHECK_DEADLOCK FALSE。模型不证明actual ZK quorum、CRC/schema bytes、ACL security、TLS、file barriers、SDK resend/completion、cleanup、clock/liveness、fairness或full A+C+D；不是步骤13/生产验收。

### Replacement inactive preparation 与 UIC2 当前验证

2026-10-06，ProfileReplacementInstall验证一个frozen next slot与ACTIVATE_REPLACEMENT，predecessor/exact committed/changed metadata三态，peer/capability/purpose/schema条件以及INSTALL始终inactive、force与owner apply完成后ACK。7个mutations为忽略prepare/delete/metadata、INSTALL开放normal、UIC2/schema忽略及force前ACK。type/security及bytes逻辑由Java真实combo和严格codec检验，既有root/page birth与body custody投影继续保留。

最终 unified-wal-model-pbctfmsp exit0：278配置为55完整正向PASS/223指定反例，551 inputs/836 checksums一致；new165 generated/120 distinct、queue0、depth/diameter11，elapsed约0.788s、peak RSS110395392 bytes，仅为TLC成本。Java unified-wal-local-z8a0uqA6 实际13类171项全部通过，1011冻结inputs；common/server style匹配215 Java，19份Java/style checksums。214共享/1348去重union一致，5个designInputs保持父提交2a990a1022acb139b29440de24c91b8f8d30687b；historical source/run/config/receipt不重绑。actual四节点new-peer chain、loss-response sameop reconcile、independent preparation vector及UIC1/UIC2拒绝边界见[实现记录](../profile-control/README.md#new-peer-inactive-install-与-replacement-wire)。

仍用tool1.7.4、单worker、heap512 MiB/direct64 MiB、seed1/fp0、无symmetry/state constraint，CHECK_DEADLOCK FALSE；没有证明TLS/byte/refcount/quorum、CRC/schema兼容、ACL、真实ZK durability、clock/liveness、whole memory、SDK mapping/ACK/resend、cleanup、full A+C+D、longrun/掉电/性能或production authority。

### 原 SDK replacement 整体 gate 当前验证

2026-10-07，`ProfileSdkReplacement` 有限投影两个 pending entry、E=W=3/A=2、两个受影响 slot，以及 inactive install、membership、authority、activation、mapping、逐 slot ACK 撤销、必要重发和连续回调。`AvailabilityIncludesActivation` 阻止中间 publication 被当作可用；gate 必须等全部受影响 slot 更新，旧 request terminal 独立推进且旧 ACK 不进入新 mapping。七个独立 mutation 分别跳过 activation、提前开放 gate、接受旧副本、改变 source/coordinate、重发未变副本或重复原 callback。

实际控制 barrier、TLS/HELLO/关联、ZK birth/CAS 与资源 custody 复用既有投影及[当前真实组合测试](../profile-wire/README.md#原-sdk-写期换组与整体回调门)。该模型不展开同 ID rejoin、真实时钟/timer、全部 quorum 配置、物理字节/refcount、进程恢复、活性/公平性、完整 A+C+D 或生产权限。最终完整检查31889 generated/8488 distinct、queue0、depth27。

最终 `unified-wal-model-_ugmmp39` exit0：286配置为56完整正向PASS/230指定反例、561输入/860 checksums；Java `unified-wal-local-QcOf8k2C` 实际8类373项全部通过，1012输入；style匹配213共享Java，Java/style共14 checksums。215共享/1358去重union与最终源码一致，5个designInputs保持父提交78135bdd2efbbca2e13e54c5f5b2c6fe9e04daaa。具体命令、真实RED、旧 capability测试合同更新和剩余范围见[当前实现记录](../profile-wire/README.md#原-sdk-写期换组与整体回调门)，历史回执不重绑。

### SDK A=3 与连续两次同起点变更

2026-10-07，复用ProfileSdkReplacement，AckQuorum参数为2或3、MaxChanges为1或2。第二次Start只允许两个entry仍pending，保留未变slot0的ACK与原source/coordinate，重置本次install/authority/activation事实，在整体gate下撤销当前被替换slot，再重发；首次及更早退役请求均可独立终结，不计入新mapping。新增正向仅为A=3一次变更、A=3连续两次变更，既有七个独立mutation保持；不是完整quorum、timer/clock、byte/refcount、进程恢复、同ID rejoin、活性/公平性或full A+C+D。

实际五节点回归只变更一个slot两次；有限模型变更两个slot两次，使用最多七个逻辑目标。模型和实际测试是各自明确范围的证据，不声称整体系统refinement，控制barrier及ZK/birth条件依赖既有切片与[当前实际测试](../profile-wire/README.md#333-与同起点连续换组验证)。最终回执见后文。

最终 `unified-wal-model-nd4nuuor` exit0：288配置为58完整正向PASS/230指定反例、563 inputs/864 checksums。新增A=3一次为26639 generated/7162 distinct、depth27；两次为672783 generated/119418 distinct、depth44，均queue0。Java `unified-wal-local-w7DpFvzI` 实际2类46项全通过，1012输入；style匹配213共享Java，Java/style共8 checksums。215共享/1360去重union一致，5个designInputs绑定父提交903f2bd8dccf239b272b107e6c42206d8116d390；命令、测试断言依据与剩余范围见[当前实际记录](../profile-wire/README.md#333-与同起点连续换组验证)。历史回执不重绑。

### 真实点读分发的有限投影

2026-10-07 ProfilePointReadDispatch投影一个固定query、一个native Get/read和128组有限输入：协商/权限/binding、found、coverage/fenced/stable。OwnerEnd只能在IO终结后生成结果，取消只停等待，queued成功loan保留到消费，output保留到实际write终结；UNKNOWN不提升为MISSING。7个mutation独立取消auth、missing proof、cancel credit、terminal等待、queued loan、output write或reply binding。实际proof生产、bytes/codec/TLS/CRC、clock/完整budget、其他读API/恢复和活性不在此投影。

最终unified-wal-model-p92naucz exit0：296配置为59正向完整PASS/237指定反例，575 inputs/888 checksums；新896 generated/704 distinct、queue0、depth7。Java unified-wal-local-cl29WO9o 实际5类77项全部通过，1018 inputs；style匹配216共享Java，Java/style11 checksums。218共享/1375去重union一致，5个designInputs保持父提交4868ff4d64的完整SHA。实际命令、malformed/权限及timeout证据见[当前实现记录](../profile-wire/README.md#read_entry-接线与验收范围)；历史source/run/config/receipt不重绑，不提升full A+C+D或生产权限。

### 网络FENCE客户端当前绑定

2026-10-07，unified-wal-model-j95f2rfy exit0保持296配置（59完整正向/237指定反例），575 inputs/888 checksums；复用既有ProfileFenceDispatch与ProfileInitialClientCorrelation/PointReadDispatch等切片，不新增或伪称quorum、LAC/length锚点、fencing read和CLOSED模型。Java unified-wal-local-LewZ3Uap实际3类59项通过，1018 inputs；style匹配216共享Java、Java/style9 checksums；218共享/1375union一致，5个designInputs保持父提交e1e6d4e704完整SHA。真实UNKNOWN/幂等及命令见[实现记录](../profile-wire/README.md#真实control客户端fence前置)；历史回执不重绑，full A+C+D/生产权限仍未获得。

### READ_LAC源取证的有限投影

ProfileLacSource使用1024组有限输入与单次回复，检查源样本身份/实际验证、独立权限、整ledgerEMPTY及explicit LAC排除、sourceLength不变成prefixLength；Get/read/loan/output的职责复用ProfilePointReadDispatch。六个单独mutation违反指定性质。LAC/source metadata只代表一份已验证原entry，不承诺maximum/explicit LAC、quorum覆盖、length锚点或matching CLOSED。

最终源码回执：Java `unified-wal-local-CQpzBTsP` exit0，实际7类88项全部通过（common8/server80：socket37、PendingAddOp19、decoder21、Bookie读/explicit LAC3），1022冻结输入；common/server style `unified-wal-lac-source-style-ge08g8nc` exit0，374个Java输入与Java回执匹配，其中218与模型共享，Java/style13份checksums核验。模型 `unified-wal-model-a9lunm3r` exit0：303配置为60完整正向PASS/243指定反例、585 inputs/909 checksums；220共享/1387去重union与当前源码一致，五个designInputs保持父提交`4eca18453bfbaf1a34f69ff66f9669eb384fb0c8`绑定。新增LAC投影2048 generated/2048 distinct、queue0、depth2，六个mutation命中指定性质。tool1.7.4/单worker/heap512/direct64MiB/seed1/fp0，无symmetry或state constraint，CHECK_DEADLOCK FALSE；有限结果不证明最大LAC、CRC/crypto、完整A/C/D、时钟/活性/总内存、掉电或生产权限。

实际路径、固定向量与重现命令见[实现记录](../profile-wire/README.md#read_lac-源取证)。历史source/design/run回执保持原绑定；步骤14/full A+C+D和生产Gate仍OPEN。

### SDK LAC覆盖与length锚定的有限投影

ProfileLacEvidence投影E=W=3、A=2/3、一个源fan-in与一个必要边界读取。SOURCE在digest之后才计coverage；源100/LAC99/length1100不能替代entry99/length1000锚点。坏digest/UNKNOWN不算有效，缺锚点、错坐标、矛盾或本handle metadata变化不成功。caller取消保留当前聚合查询名额，child socket/实际远端I/O的独立custody仍由既有切片负责。只输出快照证据，不改变writer状态或发布CLOSED，不证明完整前缀、payload一致性、fresh远端birth/context或基础恢复关闭。

最终Java `unified-wal-local-ciPAMd31` exit0，实际7类92项全部通过（common8/server84：socket41、PendingAddOp19、decoder21、Bookie3），1023冻结输入；common/server Spotless/Checkstyle实际exit0，记录`unified-wal-lac-evidence-style-b6jl_1qq`保留完成chunk c52c45及空输出，375个Java输入与Java回执一致、其中219与模型共享，Java/style14份checksums已核验。最终模型`unified-wal-model-n_xpb0fl` exit0：311配置为62完整正向PASS/249指定反例，595 inputs/932 checksums；221共享/1397去重union与当前源码一致，五个designInputs保持父提交`a72a48394392cb390735e6d5d4c17d96a854de54`绑定。新增A2为4328 generated/3872 distinct、A3为4808/4256，均queue0/depth6；六个mutation命中指定性质。tool1.7.4、单worker、heap512/direct64MiB、seed1/fp0，无symmetry/state constraint，CHECK_DEADLOCK FALSE。最初5qu67oxn因新模块TLA语法优先级错误INCONCLUSIVE并停止，修正及独立检查后才生成本最终回执；不把语法失败算实现RED或模型PASS，历史回执不重绑。

真实场景与重现命令见[实现记录](../profile-wire/README.md#原sdk的lac覆盖与length锚定)。full A+C+D、fencing/required frontier与生产Gate保持OPEN。

### 恢复context与fencing前置的有限投影

ProfileRecoveryPrelude消费一份OPEN predecessor、published authority、两种物理birth及E=W=3 A2/A3的durable peer-bound fence fan-in；metadata/lifecycle的version和UUID相同不代表同一physical birth。一次同记录CAS保留creator/operation；UNKNOWN不投有效fence票，scope变更不能得到FENCED_CONTEXT，prelude不发布CLOSED。九个指定mutation分别违反这些性质。byte/CRC/SHA、真实ZK及实际force由Java生产，full recovery/close、总预算和活性不在此投影。

最终Java `unified-wal-local-ZluCKxGo` exit0，实际9类143项全部通过（common8/server135：socket43、lifecycle30、metadata19、PendingAddOp19、decoder21、Bookie3），1026冻结输入；common/server style `unified-wal-recovery-context-style-uudk7s_c` exit0，377个Java输入匹配，其中221与模型共享，Java/style15份checksums核验。模型`unified-wal-model-3b4kjjzo` exit0：322配置为64完整正向PASS/258指定反例，609 inputs/964 checksums；223共享/1412去重union均匹配，五个designInputs保持父提交`6661b7049db2482b063731ef2aebcc4543e3be6c`绑定。新A2/A3各12744 generated/9072 distinct、queue0/depth7，九个mutation违反指定性质。tool1.7.4/单worker/heap512/direct64MiB/seed1/fp0，无symmetry或state constraint，CHECK_DEADLOCK FALSE；不是crypto/byte producer、general E>W、frontier/grant/close、full A+C+D、clock/活性、总memory、掉电/长期/性能或生产权限。历史source/design/run回执不重绑。

实际用例、固定向量及命令见[实现记录](../profile-wire/README.md#恢复上下文与fencing前置)。步骤14、full A+C+D及生产Gate保持OPEN。

### fresh-context readonly recovery observation

ProfileRecoveryObservation投影同一上下文的一次readonly point probe，E=W=3 A2/A3。definitive missing必须有对应source的durable fence和identity binding；UNKNOWN不能投票，预算不是tail，context变化不能报告有效样本；禁止Add、caller取消时放permit，以及把观察当close。七个指定mutation分别违反这些性质。Java原recovery读算法仍负责实际write-set聚合，此投影不替代byte/CRC/force、完整required authority/frontier/close或总预算验证。

最终Java `unified-wal-local-E2Od7BZC` exit0，实际9类146项全部通过（common8/server138：socket46、lifecycle30、metadata19、PendingAddOp19、decoder21、Bookie3），1027冻结输入；common/server Spotless/Checkstyle实际exit0，记录`unified-wal-recovery-observation-style-23phd7ct`保留完成chunk177207与空输出，378个Java输入匹配/222与模型共享，Java/style16 checksums核验。最终模型`unified-wal-model-jfaltg9m` exit0：331配置66完整正向PASS/265指定反例、620 inputs/990 checksums；224共享/1423去重union匹配，五个designInputs保持父提交`62aef445dd57cd5df6f0b05531cddcd733319f1d`绑定。新增A2/A3各58752 generated/27648 distinct、queue0/depth4，七个mutation命中指定性质。tool1.7.4/单worker/heap512/direct64MiB/seed1/fp0，无symmetry/state constraint，CHECK_DEADLOCK FALSE；不是full A+C+D、完整required frontier/explicit LAC/grant/close、general E>W、crypto producer、clock/活性、总memory、掉电/长期/性能或生产权限。历史回执不重绑。

真实fixture和命令见[实现记录](../profile-wire/README.md#fresh-context只读取证)，步骤14和生产Gate仍OPEN。

### RepairIntent admission与recovery grant有限投影

ProfileRepairGrant投影一个固定inert槽的inactive INSTALL、birth/delete检查、admission及目标留存、exact授权、actual force后的grant与恢复DATA；不发布membership、grant close或CLOSED。九个mutation分别跳过admission/install/delete/birth/auth/tuple/force/retention或激活normal权限，并违反指定性质。cold读观察到的delete与grant之间的有限次序不证明全部真实竞态线性化；byte/CRC/SHA、TLS及实际持久化由Java路径生产，模型消费这些事实。

最终`unified-wal-model-k2fr4zox` exit0：341配置67完整正向PASS/274指定反例，632 inputs/1020 checksums；新增173 generated/109 distinct、queue0/depth4。Java `unified-wal-local-656ohwXB`为9类151项通过/1030 inputs，style `unified-wal-repair-grant-style-h6yxeece` exit0/379 Java输入，Java/style15 checksums；225共享/1437去重union匹配，style/model223共享。五个designInputs保持父提交`7a1f8bb1239a55eb1df667bfa53e20d0319db154`绑定，tool1.7.4/单worker/heap512/direct64MiB/seed1/fp0，无symmetry/state constraint，CHECK_DEADLOCK FALSE。

真实场景、向量与命令见[实现记录](../profile-wire/README.md#repairintent-admission与实际恢复写回)。required frontier、grant close/membership/replica-readable/CLOSED、full A+C+D、总预算、活性、crypto、长期/掉电/性能与生产Gate仍OPEN，历史回执不重绑。

### UIC4 recovery close的有限消费投影

ProfileRecoveryCloseWire以独立committed close authority为输入，投影capability/principal/exact授权、grant binding、pre-cut ticket、控制force、actual terminal及caller取消的custody。九个mutation分别跳过capability/authority/binding/pre-cut/force、取消时释放custody、激活normal、允许cut后恢复Add或提前发送durable reply。它不证明close-authority/membership发布器、真实byte/crypto/force本身、matching CLOSED、全局预算或活性。

最终Java `unified-wal-local-cs02h4mj` exit0：9类161项全部通过，common10/server151（socket51、lifecycle33、metadata19、PendingAddOp19、decoder22、Bookie7），1031冻结输入。Spotless/Checkstyle实际session1107完成chunk d22dc9/exit0/空输出，归档`unified-wal-recovery-close-style-9xx5ba13`的379 Java输入匹配/223与模型共享，Java/style15 checksums核验。最终模型`unified-wal-model-1_ferwu3` exit0：351配置68完整正向PASS/283指定反例、643 inputs/1050 checksums；225共享/1449去重union匹配，五个designInputs保持父提交`d6d78ded151c21987908f02d3f7164fe4eafd41b`绑定。新增close投影179 generated/138 distinct、queue0/depth6，九个mutation命中指定性质；tool1.7.4/单worker/heap512/direct64MiB/seed1/fp0，无symmetry/state constraint，CHECK_DEADLOCK FALSE。 历史回执不重绑。

实际socket/file/固定头验证与命令见[实现记录](../profile-wire/README.md#uic4-recovery-grant-close接线)，步骤14和生产Gate保持OPEN。

### 历史repair membership与恢复context采用投影

ProfileRepairMembership消费caller完成copy的输入，有限投影两次历史fragment CAS、metadata/intent physical birth、admission/delete cut、atomic mapping/head publication、planned slot、原context及history保留和proof缺失拒绝。十个mutation分别跳过metadata/intent birth、delete、admission、破坏原子发布、错slot、丢context、丢history、忽略proof或接受unbounded fragment。byte/CRC/SHA、真实copy与TLS/force由Java路径生产；它不证明copy coordinator、last-fragment frontier、strong reset、CLOSED、活性或full A+C+D。真实场景及重现见[实现记录](../profile-wire/README.md#历史fragment-repair-membership与close授权)。

最终Java `unified-wal-local-BEO0Fee6` exit0：9类167项全部通过，common10/server157（socket52、lifecycle38、metadata19、PendingAddOp19、decoder22、Bookie7），1034冻结输入。Spotless/Checkstyle实际session54772完成chunk d7c59d/exit0/空输出，归档`unified-wal-repair-membership-style-tbaqb0r0`的380 Java输入匹配/224与模型共享，Java/style15 checksums核验。最终模型`unified-wal-model-lfhel3f9` exit0：362配置69完整正向PASS/293指定反例、656 inputs/1083 checksums；226共享/1464去重union匹配，五个designInputs保持父提交`9ec110346f3bb9c210b3819db4968b9ea4503064`绑定。新增投影121 generated/94 distinct、queue0/depth8，十个mutation命中指定性质；tool1.7.4/单worker/heap512/direct64MiB/seed1/fp0，无symmetry/state constraint，CHECK_DEADLOCK FALSE。历史回执不重绑。

### 历史fragment copy协调投影

ProfileRepairCopy消费verified source/fence/grant及RPC实际终结事实，有限投影两个entry、一个attempt permit、caller停止、durable计数、全部copy后publication与published-history resume无recopy。九个mutation分别跳过source/grant、提前publish、计UNKNOWN、取消时放permit、published recopy、并发多entry、取消后继续或激活normal。单模型不生产crypto/TLS/force、underreplication scheduling、last-fragment frontier/CLOSED、full A+C+D、总预算或活性证据。真实路径及命令见[实现记录](../profile-wire/README.md#历史fragment完整copy协调与同操作恢复)。

最终Java `unified-wal-local-ZDl8kEhW` exit0：9类171项全部通过，common10/server161（socket56、lifecycle38、metadata19、PendingAddOp19、decoder22、Bookie7），1034冻结输入。Spotless/Checkstyle实际session88830完成chunk c3b000/exit0/空输出，归档`unified-wal-repair-copy-style-83zwqmp9`的380 Java输入匹配/224与模型共享，Java/style15 checksums核验。最终模型`unified-wal-model-0_bda69p` exit0：372配置70完整正向PASS/302指定反例、667 inputs/1113 checksums；226共享/1475去重union匹配，五个designInputs保持父提交`21081423bcf778c450452089a63e86cc17e53a8b`绑定。新增投影67 generated/51 distinct、queue0/depth10，九个mutation命中指定性质；tool1.7.4/单worker/heap512/direct64MiB/seed1/fp0，无symmetry/state constraint，CHECK_DEADLOCK FALSE。历史回执不重绑。
