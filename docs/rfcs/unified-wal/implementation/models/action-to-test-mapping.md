# 当前 Model C 动作与真实路径映射

2026-10-03，`unified-wal-model-jqvpwfsw`的两个正向配置中，以下18个Next动作的TLC coverage均非零，包含CLOSE后的恢复资格与恢复发布；完整统计由checker-output保存。动作可达性不证明抽象与物理实现等价，详见[实际运行结果](README.md#当前验证状态)。

| 动作 | 当前实现/已有定向测试 |
| --- | --- |
| `AppendOpen/ForceControl/ApplyControl/AppendClose` | Arena `DataSession`与文件writer生命周期；`dataSessionReservesItsCleanCloseAndReopensWithANewEpoch`、`dataSessionUnknownBeginAndCloseHaveDifferentRecoveryOutcomes` |
| `Submit/WriteComplete/CoverFileBarrier/PublishLive` | DATA driver与`SegmentPhysicalPrefix`；`physicalDriverPublishesOnlyContiguousForcedCandidates`、`laterDurableBatchWaitsForTheEarlierPhysicalStreamPrefix`、`completionWithoutTheReservationForceCannotAdvanceThePhysicalPrefix` |
| `FailData/PersistFault` | DATA失败共享通知与Arena `FileQuarantine`；`physicalDriverForceFailureKeepsCoordinatesAndReportsNoCandidates`、`failedOrUnsubmittedDataSessionNeverProducesACleanClose` |
| `CrashProcess` | 真实子进程`Runtime.halt`；`abruptExitBeforeNormalDataBarrierNeedsRestartSync`、`unclosedSessionRefusesRestartAfterAbruptDataFailureWithoutQuarantineRecord` |
| `StartRecovery/RecoverControl/ValidateData/RecoverDataBarrier` | 双控制重放、DATA完整扫描/sync；`cleanRestartRestoresOwnerPrefixAndDuplicateAuthorityBeforeAppending`、`cleanRestartSyncFailureAndChangedBookieCutDoNotPublishAnOwner` |
| `QualifyRecovered/PublishRecovered` | 正常关闭local-result恢复和原owner bootstrap；`storageGraphBootstrapsAllShardsAndRestartsLocalResults` |
| `PowerLoss` | 模型丢弃未同步bytes的动作；真实掉电测试尚未执行，不用Java进程退出冒充 |

这些映射指出现有验证入口，不宣称每个模型动作已经获得物理等价证明。模型未展开的损坏、部分framing、allocator全部条件命令、selector/回收、index及分布式边界保持未完成。已有Java验证来源为本地存储状态的独立历史回执；模型运行不会重绑它们。


Arena checkpoint增量的17个Next动作在`unified-wal-model-thb7_nqa`正向配置中均非零，映射如下；模型仍是小型状态投影。

| 动作 | 当前实现/真实测试 |
| --- | --- |
| `Append/ForceLog/Apply/Freeze` | Arena有界控制日志及applied状态；`checkpointCapturesAppliedCutWhileLaterBatchIsDurableAndDoesNotHoldTheGrantLock` |
| `WriteImage/ForceImage/WriteCommit/ForceCommit/Publish/ForcePublication/Finish` | `SegmentControlCheckpoint`及Arena CHECKPOINT_COMMIT；独立固定向量、`checkpointFailuresNeverAcknowledgePublicationOrDiscardTheOldAuthority` |
| `Corrupt` | image无效的抽象；`checkpointRestoresAppliedResultsAndSuffixAndFallsBackAfterCorruption` |
| `Crash/RecoverySync/Recover/Resume` | `checkpointRestartAfterAbruptExitAtEachPublicationBarrier`四个真实JVM halt边界；`arenaCheckpointPreservesTheRealStorageGraphReadDuplicateAndAppendAcrossRestart` |
| `PowerLoss` | 丢弃未同步内容的模型动作；没有执行真实掉电 |

`checkpointPreservesUnclosedSessionAndDurableQuarantine`另验证状态镜像不解除现有OPEN/隔离门槛；该真实合同不是固定grant模型的证明范围。缺失/损坏完整旧日志仍拒绝恢复，不能用checkpoint跳过其当前必需依赖。


Bookie增量回执`unified-wal-model-pz77hf26`正向8个Next动作均非零：

| 动作 | 当前实现/真实测试 |
| --- | --- |
| `Append/ForceLog/Apply` | 原Bookie控制排他、group durability及顺序apply；新增commit用同一写入排他/force义务 |
| `Freeze` | `bookieCheckpointCapturesAppliedCutWhileTombstoneIsDurableButUnapplied`；只取applied状态及真实字节cut，慢force不持查询锁 |
| `Publish` | 此A切片合并C barrier假设；真实`bookieCheckpointFailureDoesNotAcknowledgePublicationAndRecoverySyncsCompletePage`、固定向量和双owner页面隔离分别验证文件路径 |
| `Crash/RecoverySync/Recover` | `realBookieCheckpointProcessExitAtEachPublicationBarrierRetainsFence`及权限/幂等/LAC/close/image回退测试；既有DATA图扩展为两个控制owner checkpoint后正常restart |

本投影没有power-loss动作或ticket lifecycle证明；真正的掉电、完整Model A及A+C仍须独立证据。


Arena rotation回执`unified-wal-model-t1waf2bm`正向15个Next动作均非零：

| 动作 | 当前实现/真实测试 |
|---|---|
| `WriteImages/ForceImages` | 连续两个applied checkpoint；固定字节向量与首次条件结果恢复 |
| `WriteFile/ForceFile/ForceDirectory` | 新日志身份/header、force及真实目录同步；候选force失败清理 |
| `WriteRoot/ForceRoot/Reclaim` | 两份页绑定同一物理代及两条原commit，之后marker/旧代退休；7个JVM halt阶段 |
| `WriteTail/ForceTail/Acknowledge` | rotation后新代framing及正常控制force/apply；三轮容量耗尽/重开与DATA图restart |
| `Corrupt/CorruptRoot/Crash/Recover` | 单image/page回退、当前suffix损坏拒绝；模型Crash丢弃未同步内容，真实进程退出不证明掉电 |

真实跨JVM排他和Bookie身份读取测试独立验证sidecar锁，不由上述模型动作推出。重复rotation与全部操作结果、坏header/CRC/序号及资源预算仍需真实路径；完整Model C/Bookie组合和liveness保持未完成。

Bookie回收权限回执`unified-wal-model-bu9e_n9l`正向9个Next动作均非零，前8个仍对应上述Bookie checkpoint路径，新增映射：

| 动作 | 当前实现/真实测试 |
|---|---|
| `ReclaimPrefix` | `SegmentBookieControlLog.rotate()`；`bookieRotationReclaimsOrdinaryBudgetAndRestoresSealedPermissionsAfterEachCycle`与`bookieCheckpointPreservesPermissionsOutcomesLacAndClosedRecoveryGrant` |
| `Freeze/Publish/Recover`（退休域） | `bookieRotationFallbackPreservesFenceAndRequiresTheCurrentJournal`、独立Bookie header/第三页向量、两个owner rotation后的DATA图restart |

共享物理`WriteFile/ForceFile/ForceDirectory/WriteRoot/ForceRoot/Reclaim`仍由独立C模型检查；Bookie真实`bookieRotationProcessExitKeepsAuthorityAtFileAndDualRootBarriers`验证7个JVM退出阶段，sidecar及慢I/O票据门槛由独立测试覆盖。权限模型合并这些屏障为输入假设，不是两个模型的组合证明，也不把进程退出当成掉电。


派生索引覆盖回执`unified-wal-model-p_83_s8r`正向8个Next动作均非零：

| 动作 | 当前实现/真实测试 |
|---|---|
| `BeginRebuild/ForceReset` | `SegmentDerivedIndex.replace`先退休旧coverage再更新定位；`derivedIndexReplacementRetiresOldCoverageBeforeDestructiveEdits` |
| `Write/Flush/Publish/ForcePublication` | `derivedIndexPutIsQueryableButCoverageRequiresActualFlushAndPublication`；`derivedIndexKeyLocationAndCoverageMatchIndependentByteVectors`及`derivedCoverageWriteOrForceFailureNeverReportsCheckpointSuccess` |
| `Crash/Recover` | `realDerivedIndexProcessExitBeforeFlushAndAtCoveragePublicationRebuildsData`的Put后、发布force前及发布完成三个真实JVM halt阶段；模型故障丢弃未同步内容，真实进程退出不证明掉电 |
| 新一轮`BeginRebuild` | `deletedDerivedDatabaseRebuildsFromDataWithoutRevivingTombstones`及`storageGraphRebuildsDeletedDerivedIndexBeforeCleanRestartRequests` |

当前只接入冷启动完整DATA扫描之后、开放请求之前的重建。在线hot handoff、index stall背压、扫描跳过及完整A+C/D模型均未完成；独立模型与实际测试不互相代替证据。


索引维护回执`unified-wal-model-tn4d2l9o`正向10个Next动作均非零：

| 动作 | 当前实现/真实测试 |
|---|---|
| `Submit/ForceData` | 既有有界组批/真实DATA force/连续前缀/热发布；新增热容量拒绝发生于DATA预留前 |
| `StartIndex/StartPut/Put/CompleteIndex` | `SegmentHotDirectory`的固定owner pump与既有DATA ticket/I/O，`SegmentDerivedIndex.append`真实WriteBatch后回原owner终结 |
| `Stall/Resume/Fence` | `asynchronousDerivedIndexStallKeepsHotSuccessAndReservedFenceProgress`；暂停真实索引job，使用同一DATA/控制I/O服务，先热成功再durable fence、预算拒绝及恢复已接受写 |
| `IndexFailure` | `asynchronousIndexConflictRetainsHotReadAndRefusesNewDataBeforeReservation`；真实冲突派生行使后台拒绝覆盖，热读仍成功，新DATA不预留/force |

`asynchronousDerivedIndexDrainRetainsNativeLockUntilRealIoTerminates`另验证关闭超时保留实际native排他，实际I/O及原owner票据终结后才排空；模型未证明native句柄或关闭顺序。共享DATA I/O停顿可能延迟点读/已接受DATA，控制预留独立；不从该有限投影推出完整系统的进展或生产authority。

当前接管回执`unified-wal-model-2qh8pivn`仍使10个Next动作coverage全部非零，`CompleteIndex`在query-visible后淘汰hot，`Submit`复用hot额度但保留累计坐标占用。

| 变化/实际风险 | 当前真实测试 |
|---|---|
| `CompleteIndex`接管及在途定位 | `indexHandoffKeepsCapturedReadAndDrainsCancelledFreshGet`；`indexHandoffKeepsCapturedDuplicateAndChecksFreshNormalAndRecoveryDuplicates` |
| 坐标占用、hot/恢复容量独立 | `storageGraphWritesBeyondHotCapacityAndRebuildsAllCoordinatesIncludingHoles`；单shard超过hot容量、乱序填洞及删库全重建 |
| 模型未展开的同票据多段I/O | `ioContinuationKeepsOneTicketAcrossDrainFailureAndSecondIoRejection`；真实两个I/O阶段、owner异常/拒绝及恰好一次终结 |

模型新增Put前淘汰反例，不替代真实Get/CRC/权限/pin、native锁或关闭流程；运行时query-visible仍不推进持久覆盖。


## 运行期覆盖动作

`RuntimeCoverage.tla`正向11个Next动作coverage均非零，当前真实接线如下：

| 模型动作 | 实际路径及验证 |
|---|---|
| `Add/Put` | DATA barrier/连续前缀/hot后异步WriteBatch，沿用既有真实文件回归 |
| `Prepare` | `IsolatedSegmentStorage.checkpointIndex`每原worker一份DATA票据冻结其shard；`runtimeIndexCheckpointUsesAllOriginalWorkersAndMonotoneSameGenerationPublications`覆盖1/4 worker |
| `Flush/Publish/ForcePublication` | `checkpointRuntime`校验全部owner cut/实际native行，复用真实flush及A/B页write/force/目录sync/重读；同代三次publication回归 |
| `Resume/Abort` | 全原worker解冻后返回结果；`runtimeIndexPartialDataTicketRefusalResumesPreparedWorkersBeforeRetry`检查部分额度拒绝后可重试 |
| `BackgroundFlush` | RocksDB可能自行flush的抽象，不独立建立新coverage；Java不控制其内部时序 |
| `Crash/Recover` | flush/页持久性的有限投影；原冷覆盖JVM退出测试继续保留，当前在线页未新增实际JVM退出或掉电验证 |

`runtimeIndexCheckpointPausedPublicationKeepsNewHotDataFenceAndCancellationDrain`检查冻结后新DATA不进入此次索引cut、原owner/独立控制I/O推进、请求取消不提前归还DATA票据/native锁。它显式配置2条DATA I/O及1条CONTROL I/O；仅1条DATA I/O时checkpoint占用会延迟新的DATA I/O，不据此声称无限进展。

`runtimeIndexPublicationFailureDoesNotPoisonQueryableRowsOrReportSuccess`检查真实页force失败返回UNKNOWN且没有成功coverage，既有query-visible读/重复比较仍可用；同代有效未force页在后续publication clock中仍计数。模型不替代真实多worker协调、票据最后使用、CRC、权限或存储排空验证。


## 持久覆盖恢复动作

`TailRecovery.tla`正向5个Next动作coverage全部非零：

| 动作 | 当前实现及真实验证 |
|---|---|
| `Start` | `recoveryCoverage`验证真实native摘要和page/当前authority；`coveredRestartInvalidPageContentsOrDeletedDbFallsBackToFullDataRebuild`覆盖坏页、内容后来变化及删库 |
| `InspectHeader` | 独立batch header parser、连续性、index匹配位置及授权tail完整parse；`coveredRestartReusesNativeGenerationAndValidatesOnlyUncoveredBodies`验证1个覆盖/2个tail及多shard |
| `Finish` | 稳定Bookie/Arena cut、全部必要头、连续prefix/唯一坐标；`coveredRestartRejectsBadRequiredTailAndCoveredReadsStillValidateRecords`覆盖尾部CRC、覆盖头缺失及覆盖body读CRC |
| `ForceData` | 所有必要DATA文件force仍必需；同一有效cut回归注入真实force失败，确认未开始native/session恢复，再重试成功 |
| `Expose` | `updateFromRecovery`保留index代、批量补tail/清除不可读行、flush/发布后startSession与原owner bootstrap；storage图真实重启/读取/重复/追加 |

`coveredRestartReplaysTombstoneSuffixWithoutRevivingIndexedRows`另检查控制后缀过滤旧native行与新tail；`coveredRecoveryRejectsSeedFromAnotherDataDirectoryBeforeSync`从两个真实独立目录取得证据，并在错误目录sync前拒绝。source目录绑定及真实native写入不在有限模型中；不由该投影推出任意权限/selector、实际掉电或liveness。


## 分配顺序与低地址追加

`AllocationOrder.tla`实际展开的Grant/Consume/Restart均有非零coverage；Reserve与Cancel共用Consume，lastKind区分但没有独立包装动作计数。

| 动作 | 当前代码与真实回归 |
|---|---|
| `Grant` | `SegmentArenaControlLog`原成功operation顺序；`allocationControlOrderSurvivesRotationAndBothFullAndCoveredStorageRestart`完成实际Arena rotation后重启 |
| `Reserve` / `Cancel`（Consume） | `IsolatedSegmentFiles.reservePhysical`与`appendAddress`的控制序号/offset游标；`laterAppliedAllocationCanAppendBelowTheSameStreamsPreviousFileOffset`验证高→低及旧grant拒绝；`authorizedStreamSkipsAndCancelledReservationsCannotBeReused`保留取消不归还及raw前缀保护 |
| `Restart` | `restartSkipsOldAllocationTailsButAcceptsANewLowerGrant`验证两轮物理restart，新低地址grant可写但旧grant尾不可返还；storage图另验证全量/同代覆盖重启、读、重复和新追加 |

`allocationOrderedRecoveryStillRejectsCrossAllocationSequenceGapsAndDuplicates`使用真实有效DATA framing分别制造区间间gap和duplicate，不能借排序容忍无效前缀。每allocation连续区间、CRC/文件内容、scope及rotation原序号由Java另证；有限分配顺序投影不建模这些字节行为，也不证明FREE、旧I/O/pin排空或liveness。


## 整allocation FREE

`WholeAllocationFree`投影使用准确、单调的终态tombstone作为输入合同；普通DATA内容、CRC、allocation身份与Bookie cut不在模型中解析。真实实施映射：

| 动作 | 实际路径与必要测试 |
|---|---|
| `Tombstone/Prepare` | Bookie `ReclamationLease`先排空/关闭entry admission，文件`ReclamationGuard`再拒绝新reservation/read lease；`wholeDeadAllocationFreePreservesLiveSharedFileDataAcrossFullAndCoveredRestart`、`wholeAllocationFreeRejectsInactiveRecordsAndUnverifiableDeadData` |
| `StartIo/FinishIo/Pin/Unpin` | 实际DATA reservation/write/force与read lease计数；`wholeAllocationFreeRequiresActualWriterAndReadQuiescence`同时验证有旧entry admission时不妨碍旧写推进 |
| `Submit/Force/Apply/OwnerResolve` | 现有Arena driver CONTROL I/O与原owner apply；`submittedFreeKeepsProtectionUntilControlIoAndOriginalOwnerFinish`暂停真实force及owner，验证durable未apply仍不开放；`runtimeWholeAllocationFreeUsesExistingStorageGraphAndRestoresRetiredPrefix`验证实际图 |
| `ForceUnknown/Crash` | `wholeAllocationFreeForceUnknownCannotPublishHealthyCloseOrReopen`验证真实barrier异常、OPEN保留及restart拒绝；模型crash不是新的Java进程退出或掉电实验证据 |
| `Duplicate` | 真实重复FREE结果相同、generation仍为2；独立向量`wholeAllocationFreeMatchesIndependentBytesAndCannotForgeARuntimeProof`禁止仅靠decoded事实构造新runtime proof |
| `CancelWait/EarlyClose` | stopWaiting只丢弃响应；真实提交期间close与重复claim不能解除保护。EarlyClose仅在敏感性配置启用，正向coverage不要求它可达 |

metadata-only退役范围恢复、空range不虚增序号、rotation及native覆盖由真实Java路径验证，未被这个小模型展开。旧字节物理复用、MOVE/selector、runtime索引/coordinate回收、全预算、完整A+C/C+D及liveness仍未完成。


## 健康 CLOSED 整块复用

| 动作 | 实际路径与必要测试 |
|---|---|
| `FinishOld/Free/Reuse` | 已有reclamation gate与Arena条件apply、原allocator demand refill；`runtimeAllocatorReusesOnePhysicalBlockAcrossTwoTerminalFreeGenerations`、`reusedAllocationKeepsOldTailOutsideRequiredDataAcrossFullAndCoveredRestart` |
| `StartWrite/Force/WriteUnknown` | 原DATA reservation/FileChannel write/force；`reusedPartialWriteAndForceFailureKeepOpenSessionUnrecoverable`保持OPEN及拒绝重启 |
| `Close/Crash/Recover` | 真实writer关闭且I/O终结后保存CLOSED v2 tails；`unwrittenReusedGrantDoesNotReviveOldGenerationOrInventSequence`、`damagedRequiredReusedPrefixCannotBeClassifiedAsUnusedOldTail`及共享文件完整/同UUID覆盖恢复 |

独立ALLOCreuse/CLOSEDv2向量通过 `reusedAllocationAndClosedTailMatchIndependentBytesWithoutGrantingRuntimeAuthority`；模型假设输入framing正确，不负责字节CRC、真实file force或外部decoded命令拒绝。模型crash是逻辑投影，没有新增JVM halt或掉电证据。


## 终态索引与坐标清理

| 动作 | 实际路径与必要测试 |
|---|---|
| `Tombstone/FinishOldPut/FinishRead/Prepare` | Bookie准确context snapshot与entry quiescence、各原hot/index pause；`nativeTerminalCleanupUsesExactCapturedTombstonesAndBoundedDeleteBatches`、`terminalCleanupWaitsForActualLateNativePutBeforeReleasingCapacity` |
| `StartDelete/Delete/DeleteUnknown/Prune` | 原DATA I/O native iterator/256-key WriteBatch，成功后原owner occupied删除；`nativeTerminalCleanupFailureDoesNotReleaseOccupiedCoordinates`保留坐标，真实后继写仍背压 |
| `Publish/PublicationUnknown/Finish` | 原runtime checkpoint的flush/A/B force与全原worker终结；`terminalCleanupPublicationFailureKeepsNativeDeletionAndRestoresAdmission` |
| `CancelWait` | request.stopWaiting只丢响应、实际I/O/lease保留；`cancelledTerminalCleanupHoldsActualIoAndDefersLaterTombstones`暂停实际page force，close短超时不完成，晚到T由下一次/恢复处理 |
| `NewWrite` | `runtimeTerminalCleanupRecyclesEveryOriginalOwnersCoordinateAndPhysicalBlock`在1/4 worker、四坐标/四块下验证四轮清理/FREE/复用后保留四个第五代live entry及restart |

模型不代替实际native、原worker通知或资源释放验证；没有新增进程退出/掉电实验。native生命周期故障与Put暂停仅在测试中通过反射取得实际资源，生产设计未增加mock层或测试开关。


## relocation 内部持久化完成

| 动作 | 实际路径与必要测试 |
|---|---|
| `Force/ForceUnknown` | `submitRelocation`复用真实write/force；`relocationWriteAndForceUnknownNeverReturnDurableCopyFacts`覆盖部分写/force unknown，不返回副本事实 |
| `Receive` | 原owner连续prefix后才构造内部结果；`laterCopyForceCannotReturnFactsAcrossAnEarlierPhysicalHole`以两个实际DATA IO worker验证后批先完成仍等待缺口 |
| `OwnerTerminal/Release/CancelWait` | 原physical obligation等待实际IO与owner两方才归还pool；`cancelledRelocationWaitKeepsActualForceAndOriginalPoolUntilTerminal`取消future、实际force暂停及短close保留资源 |
| copy-only输入/内部结果 | `relocationDurabilityReturnsBoundCopyFactsWithoutOrdinaryPublication`验证绑定、原始copy可校验但无普通定位、后继普通同坐标写与恢复来源选择；`relocationValidationConsumesPooledBatchesWithoutIoOrCoordinatePublication`验证mixed origin/move、重复坐标及错误shard拒绝并归还池 |

没有引入MOVE完成、普通Add成功或selector winner的模型断言。乱序测试暂停前批进入实际IO；同文件write/force串行保证保留，测试不通过暂停持锁force制造不可达交错。


## 条件 MOVE

| 动作 | 实际路径与回归 |
|---|---|
| `Copy/Prepare` | 实际副本结果与 `SegmentArenaMove.prepare` 的源/目标独立点读；`moveProofRejectsHeldReadsWrongBytesAndFutureSelectorsWithoutLeakingProtection` 验证读保护、不同entry bytes、未来引用与scratch失败归还 |
| `AppendControl/Force/Apply` | 既有 CONTROL I/O / Arena 顺序条件；`moveWaitsForApplyAndRedirectsOldHotReadsAndDuplicateChecks` 验证 durable未apply、decoded无新proof及当前目标点读/幂等 |
| `Apply/CrashReplay` | `moveChainRejectsStalePredecessorAndReplaysSelectorsThroughRotation` 验证双坐标全批条件、loser、后继与旧重试、完整历史及rotation selector恢复、当前target缺失隔离；`moveControlForceUnknownDoesNotCutOverBeforeApplyButReplaysDurableFacts` 验证未知barrier与未关闭会话 |
| `ReadStart/ReadFinish/Finish/CancelWait` | `cancelledMoveWaitRetainsProtectionThroughActualForceAndOwnerApply` 保持文件/Bookie gate到实际force与owner apply；已有业务read admission/file pin保持两方终结 |
| wire bound | `moveFormatMatchesIndependentVectorAndBoundsAuthorityReferences` 独立416-byte向量、256/257行、未知版本/未来前驱/geometry/header拒绝 |

真实原owner池/CONTROL service复用现有组件。以上不包括graph compaction、源FREE/reuse与native maintenance cut。


## MOVE/FREE/reuse

| 动作 | 实际路径与测试 |
|---|---|
| `PrepareFree/ApplyFree` | protected整块parser校验T或当前MOVE后继、实际目标点读与v2 Arena cut；`movedSourcesFreeAndReuseWithoutNewCheckpointRecoverCurrentCopy`覆盖当前COPY拒绝、坏C阻止源退役与A/B整块FREE |
| `PrepareReuse/ApplyReuse/Restart` | 既有allocator/free generation和CLOSED末端；同一真实测试将A/B以generation2写入其他坐标，再做无新checkpoint全量/rotation storage恢复与当前C缺失隔离 |
| `AdvanceControl/ApplyFree/PrepareMove` | `orphanCopyFreeRevokesLateMoveAndStaleSelectorCutCannotRetireAllocation`验证陈旧proof整批条件失败、orphan FREE撤销grant、迟到MOVE拒绝，原普通坐标仍SAME_DURABLE |
| 准确T/selector退役 | `terminalCurrentCopyFreeRetiresSelectorWithoutRestoringOldPermission`验证当前COPY有T后v2退役、普通T源v1保留、重启后的NOT_READY/无结果 |
| framing | `moveAwareFreeFormatBindsEarlierSelectorCutAndKeepsV1Bytes`验证280-byte独立v2 oracle、错误cut与decoded新proof拒绝、v1字节保留 |

不包含在线MOVE native Put/coverage协调或graph compaction request。


Native MOVE coverage 增量回执 `unified-wal-model-75i1oty1` 的动作映射：

| 动作 | 实现及真实验证 |
| --- | --- |
| Move | 前置真实 COPY/MOVE 切片；本投影假设已 durable/apply 的 selector |
| OldPut/Prepare/NativeUpdate/Flush/ForcePage | 原 worker checkpoint 暂停及真实 RocksDB更新/flush/页force；nativeMoveMaintenanceReplacesDrainedRowsAndCoveredRestartVerifiesCurrentBytes |
| NativeFailure/Finish | failedNativeMoveValidationKeepsSelectorAndRequiresFullRecovery；graphMoveIndexPublicationFailureRetainsCurrentReadsAndCanRepublish |
| Cancel | cancelledGraphMoveCheckpointHoldsProtectionUntilActualPublicationAndOwnerDrain，1/4 worker、短关闭与实际排空 |
| Crash/CorruptTarget/Recover | 首项真实正常关闭、覆盖恢复与坏CRC/零化target；模型crash不代表新执行了JVM halt或掉电 |

实际读取量包含覆盖 target 的点读；多worker/runtime票据协调仍由Java回归负责，不从这一个坐标投影推出完整MOVE/FREE组合或进展。


## 有界合批 compaction

| 动作 | 当前真实路径与测试 |
| --- | --- |
| `Select/Allocate/WriteCopy/ForceCopy/ForceMove/ApplyMove/PublishNative` | `graphCompactionRequiresGroupNetGainPreservesSharedLiveDataAndReusesFreedSource`：真实单块拒绝、双块合批、OPEN ledger、准确T、覆盖及generation2复用恢复 |
| `PrepareFree/ForceFree/ApplyFree/FinishMember` | 同一graph回归验证两个v2 cut逐条递增；`protectedFreeBatchSharesOneBarrierAndWaitsForEveryClaimedMember`验证一次真实force、重复finish及保护到末成员 |
| `NativeFailure` | `graphCompactionPublicationFailureKeepsMovedDataAndSourcesForLaterFree`：MOVE后真实页force失败不释放source，新operation无COPY地继续回收 |
| `Cancel` | `cancelledGraphCompactionHoldsResourcesUntilPublicationAndCanReclaimAfterRestart`：1/4 worker，实际发布暂停、短close锁保留、重启读当前copy及继续FREE |
| 投影外输入 | `graphCompactionTerminalGroupNeedsNoCopyAllocation`验证全T无需destination；`preparedResolvedMoveRetryReleasesItsUnclaimedProtection`验证同一guard的零claim已解析MOVE重试 |

没有在本轮新执行掉电或JVM halt；健康restart/实际storage字节回归与有限模型动作分别说明证据。


## 多批 compaction

| 动作 | 当前真实路径与测试 |
| --- | --- |
| Select/NextCopy/Allocate/ForceCopy/ForceMove/ApplyMove | `graphCompactionSplitsByBufferAndMoveRowsAndRecoversEveryBatchAfterSourceReuse`：1/4 worker、40/402坐标、3/2批、不同COPY身份、当前读/幂等、覆盖及source generation2复用后的全量恢复 |
| CopyFailure | `laterCompactionCopyForceUnknownRetainsSourcesAndOnlyEarlierMoveSelectors`：第二实际force unknown，只保留首19个MOVE，无FREE；控制重放保留source generation和quarantine，业务restart拒绝 |
| 全批allocation成本 | `multiBatchCompactionIncludesEveryFrameInAllocationNetCost`：payload小于一块，但各批framing/padding合计后两块destination，source组净零不产生ALLOC/COPY/MOVE |
| 池化分批/归还 | 当前 `multiBatchCompactionUsesPooledChunksBeyondSourceInputBudget` 取代整组预算拒绝契约；真实池不足回归见下节，历史回执保留 |

新模型不称为实际IO失败恢复或掉电证明；source已搬迁部分在旧source整块FREE前仍必须满足全范围proof。


## 有界 source 选择

| 动作 | 实际路径与风险验证 |
| --- | --- |
| Measure/Select/Capture | `SegmentArenaControlLog.compactionCandidates` 固定shard/唯一stream、最多16个最早同文件current grant；`SegmentDataRecovery.inspectCompaction` 同guard真实DATA测量，再捕获选中组；不使用index缺行 |
| 死source直接Prove/ForceFree/ApplyFree | `shardCompactionReclaimsAtFullCapacityBeforeCapturingUnrelatedLiveInputs`：3个grant无空闲、无关live payload超过源预算，直接FREE两旧source，无COPY；保留当前MOVE并实际generation2复用及重启 |
| 固定窗口、共享活数据 | `shardCompactionBoundsOldestWindowAndRetainsYoungerSharedLiveData`：17 grant前16个T回收、年轻live保留，下一窗口净零拒绝，跨worker重启仍读原数据 |
| 部分live路径Move | 既有 `graphCompactionRequiresGroupNetGainPreservesSharedLiveDataAndReusesFreedSource` 与 `graphCompactionSplitsByBufferAndMoveRowsAndRecoversEveryBatchAfterSourceReuse` 改经shard入口，复用真实共享T/净成本、多批、覆盖及复用验证 |

有限投影只检查选择与输入/目的地准入顺序，不能替代真实whole-allocation proof、I/O、资源终结或持续负载验收。


## 有界窗口轮转

| 动作 | 真实实现与风险回归 |
| --- | --- |
| Admit/固定cut/顺序窗口 | `compactShard` 每shard进程内after/through；`compactionCandidates` 既有固定文件、原ALLOC sequence区间及16上限；扫描cut不授予FREE |
| 继续下一窗口 | `shardCompactionAdvancesPastFullLiveWindowToRecoverDestinationSupply`：17 grant满文件，首16 live目的地无空间，下一dead直接FREE，再搬迁17条并FREE16源、generation2目的地及重启 |
| Append/回访与空窗 | `shardCompactionFreezesScanRoundAndRevisitsEarlierSourcesAfterRefill`：真实ordinary refill新grant、准确T后先回访原16、再处理新grant，全空返回false、低地址gen2再写与重启 |
| 资源循环输入 | `runtimeTerminalCleanupRecyclesEveryOriginalOwnersCoordinateAndPhysicalBlock` 改经自动shard选择；1/4 worker、4 shard、4轮FREE、gen1→5及最终覆盖/重启；有限次数，不是long-run测量 |

模型每窗口1个grant只投影推进顺序；Java实际16上限与byte/资源语义由真实路径验证，不能从游标推出数据死亡或FREE authority。


## 池化 compaction 捕获

| 动作 | 实际路径与行为验证 |
| --- | --- |
| Measure/AcquireScratch/AcquireCopy | `inspectCompaction`只保留限额内source/hash，`CompactionOperation`从原owner取scratch与COPY池；`compactionSingleOwnerBufferRefusalReturnsScratchAndReclaimsUnusedDestination`检验实际1 buffer拒绝、读/幂等及未使用destination回收 |
| CaptureIoEnd/CaptureOwnerEnd | guard下 `captureCompaction`重新点读真实source并复核hash/selector/Bookie cut；`RelocationBuilder.append`同步复制借用视图；`compactionRecapturesMeasuredSourceBytesAndBookieCutBeforeLaterCopy`分别保持CRC有效改变后批payload/实际其他ledger控制cut，仅前19 MOVE、1 COPY、0 FREE，并恢复读/重启 |
| WriteIoEnd/WriteOwnerEnd/Move | 复用 `submitRelocation`实际DATA/prefix与原owner两终结；`multiBatchCompactionUsesPooledChunksBeyondSourceInputBudget` 1/4 worker、40条/1760 bytes大于1060输入额度、3次COPY、全部读/幂等/native覆盖与restart；既有后批force unknown与取消生命周期继续覆盖 |
| Free | 全批MOVE后既有native/whole-allocation proof与共享CONTROL barrier；group模型及真实source复用回归继续独立验证，计划和moved计数不授予FREE |

新有限模型只投影资源阶段与测量检查结果，不能把valid布尔值或有限行数当成实际字节/完整内存/掉电证据。


## 保留 compaction tail

| 动作 | 实际实现与风险验证 |
| --- | --- |
| Measure | 新grant无空间后，`latestAllocationForShard`取得同stream最新grant；guard `inspectCompactionTail`实际完整CRC/selector/permission/prefix/tail及全批空间检查，只接受含current COPY且不属于显式source组的目的地 |
| Copy/ForceCopy/Move | `resumeRetainedTail`在原owner、原双buffer/driver中续写；`shardCompactionResumesPartialMoveAtFullCapacityAfterHealthyRestart`：实际PARTIAL→健康CLOSE/restart，1/4 worker、gen1/gen2残留tail、21条/两COPY、全部旧数据/幂等及source gen2新普通写/再重启 |
| 拒绝与StopBeforeNextCopy输入 | `retainedCompactionTailRejectsInsufficientRangeAndCorruptExistingBytes`：只剩一批但需要两批时0COPY/0FREE；破坏已有终态record后0COPY，恢复原字节仍可读/重发；已有cut变化/force unknown/取消回归继续覆盖各自资源路径 |
| Native/ForceFree/Free | 原native/coverage和完整source proof，保留目的地排除FREE；实际source净FREE2 MiB、destinationBytes=0但ID为实际保留grant；未apply ALLOC不报告实际ID |

模型只接受Java已证明的latest/currentCopy为输入；阶段计数不代替真实whole proof或持久化。


## 维护容量预留

| 动作 | 实际路径与风险验证 |
| --- | --- |
| Plan/Apply | `planPools(requests, maintenanceBlocks)` 按固定文件汇总fresh/retired whole blocks，同批成功计划扣减；`mergedForegroundPoolsShareReservePerFileAndMaintenanceCanUseItAfterReplay` 验证两文件合批、控制apply、replay、前台拒绝及维护末块准入 |
| StartMaintenance/VerifiedFree | `Limits.maintenanceBlocks` 是明确目的地工作集上限，compaction实际全aligned测量后在ALLOC前检查；`declaredTwoBlockMaintenanceWorksetCompactsWhileSmallerBoundRefusesBeforeAlloc` 验证声明1拒绝、2实际COPY/MOVE/native/FREE及净收益 |
| Partial/Restart | `foregroundRefillPreservesCompactionCapacityAndRestoresAdmissionAfterRestart` 由普通路径填两个source，拒绝最后refill后维护恢复容量，1/4 worker及健康重启的gen2低地址写入不消耗拒绝的sequence；partial已用容量继续由真实grant占用 |

FREE proof、extent几何、并发I/O终结及filesystem预算是其他模型/真实路径的独立义务；本投影不声明长期进展。

## 运行期控制日志轮转

| 动作 | 实际路径与风险验证 |
| --- | --- |
| Admit/Protect/Refuse/DataTerminal/Unpin | `rotateControls` 复用两个 driver 排他、Bookie admission lease 与文件保护；`runtimeControlRotationWaitsForDataReadPinsAndCancelledIoOwnerResolution` 实际文件pin下零rotation I/O，释放后可重试 |
| BookieDone/ArenaTerminal | 两个已有 `rotate` 物理协议，Arena新入口要求准确writer保护；`automaticControlRotationReclaimsFullLogsPreservesOpenSessionAndOriginalAppendCursor` 的1/4 worker满额后两轮原pool续写、OPEN epoch/游标/读/幂等/健康重启 |
| CancelWait/OwnerResolve | 原 CONTROL ticket 将实际I/O完成返回原owner，取消只撤回复；force暂停、缓存控制driver和DATA拒绝、短shutdown保持保护至实际排空 |
| BookieUnknown/ArenaTerminal失败 | `runtimeControlRotationForceFailureStopsGraphAndReportsOnlyCompletedOwnerRoots` 分别注入真实force失败，UNKNOWN停止准入，Bookie已完成root不被误报为双owner成功，Arena OPEN仍拒绝恢复 |

正向全部10个Next动作有非零coverage；既有物理发布、损坏与JVM退出测试仍独立保留。模型不自行证明字节、真实force、全部通知队列或长期进展。

## 控制容量恢复

| 动作 | 实际路径与风险验证 |
| --- | --- |
| CapacityRefusal/EntryAttempt | Bookie `requireCapacity`、Arena append/quarantine预留标记fresh-fit压力；图新准入保持false与caller buffer所有权；`automaticControlRotationReclaimsFullLogsPreservesOpenSessionAndOriginalAppendCursor` 的旧生产红/新生产绿、1/4 worker两轮原pool续写及健康restart |
| DataTerminal/Unpin/ControlTerminal/Pump | 每原owner `attachControlMaintenance`，只在实际entry/I/O/pin排空时用既有CONTROL ticket；`arenaQuarantineCapacityRefusalWakesAutomaticRotationWithoutControlFailure` 检验没有control错误回调时的同步DATA拒绝唤醒 |
| IoTerminal/OwnerResolve | 既有受保护双owner协议与原ownerterminal；`automaticControlRotationFailureIsObservableAndStopsGraphAdmission` 用真实无效候选验证cause与停止准入，原force/取消/drain回归保留 |
| fresh-fit拒绝分支 | `oversizedControlBatchDoesNotTriggerUnproductiveAutomaticRotation`：300项合法batch数量的真实字节超限、没有新journal、DATA继续成功；不是用非法batch数量替代容量验证 |

模型只检查有限安全关系，不证明所有park/unpark或真实事件最终完成；这些真实回归仍保持局部、可复现的deadline与证据边界。
