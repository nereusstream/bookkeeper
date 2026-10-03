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
