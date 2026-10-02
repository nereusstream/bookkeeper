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
