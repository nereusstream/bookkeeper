# 当前 Model C 性质映射

2026-10-03，`unified-wal-model-jqvpwfsw`的共享/分文件正向配置均完整探索并检查以下7项性质；三个负向配置分别违反指定性质，详见[实际运行结果](README.md#当前验证状态)。这是有限模型证据，不替代下列真实路径的验证。

| 性质 | 当前合同与实现 | 证据边界 |
| --- | --- | --- |
| `TypeOK` | 配置固定stream/batch/epoch/故障次数，控制cut及集合保持类型/容量范围 | 不证明Java内存字节总账 |
| `ControlHasBarrier` | RFC-0003 §5.3/12；Arena控制日志appendDurably后顺序apply，恢复可读控制内容先同步 | 只抽象完整会话控制记录；条件命令全集/rotation未展开 |
| `DataSuccessHasBarrier` | RFC-0003 §6.1；完整DATA write和对应文件barrier分别完成后才能成功 | 不把模型barrier当真实fsync证据 |
| `PhysicalSuccessIsContiguous` | `SegmentPhysicalPrefix`各stream连续through，乱序完成不能跳洞成功 | 固定不复用range；generation/MOVE/FREE仍未建模 |
| `KnownErrorRemainsIsolated` | RFC-0003 §12.9；Arena DEVICE_STATE隔离及OPEN/CLOSE管理会话阻止旧错误被后次sync抹除 | `faultSeen`为历史oracle，非新进程可直接读取的证据；故障解析仍未实现 |
| `RecoveredDataIsDurable` | 完整候选验证、控制依赖及必要DATA恢复同步后才给local-result恢复资格 | framing、Bookie selector及字节验证由真实路径负责 |
| `LiveWriterHasOpenAuthority` | `IsolatedSegmentFiles.startSession`先持久OPEN并apply；`IsolatedSegmentStorage`全shard bootstrap后开放请求 | 模型只抽象Arena会话，不证明服务readiness/registration |

原调用方是否收到结果、原请求quorum是否成功、最终ledger prefix及LAC均不由上述本地性质推出。模型中的`successes`是外部验收oracle，不新增逐entry ACK日志。
