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
