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


Arena checkpoint增量回执`unified-wal-model-thb7_nqa`另检查以下性质；原7项管理会话性质在同次运行中仍由原配置检查。

| 性质 | 当前合同与实现 | 证据边界 |
| --- | --- | --- |
| `TypeOK`（ArenaCheckpoint） | 固定3命令/2slot/3代/2crash/1power loss的cut、image与commit类型 | 不证明实际文件或资源总账 |
| `SnapshotCutWasApplied` | RFC-0003 §11；冻结appliedThrough及其字节边界，不取未apply的durable上限 | 模型为命令计数，Java为sequence；不包含MOVE/FREE |
| `PublishedImageHasDurableCommit` | payload force/验证后，CHECKPOINT_COMMIT持久并apply，再写inactive发布页 | 文件framing/SHA及barrier由真实测试负责 |
| `RecoveredAuthorityMatchesFullHistory` | 验证提交/发布/image，重放从快照cut之后的完整后缀；两份不可用时全量重放 | 固定grant/no-op投影，完整旧日志仍必需；不允许prefix reclaim |
| `RecoveredPublicationWasSynced` | 完整可读发布页在恢复暴露前force | 不把模型动作当设备掉电证据 |


Bookie增量回执`unified-wal-model-pz77hf26`增加以下Model A局部投影；原Model C性质在同次运行中保持验证。

| 性质 | 当前合同与实现 | 证据边界 |
| --- | --- | --- |
| `TypeOK`（BookieCheckpoint） | 1 ledger/10条固定记录/2snapshot/2crash，权限与cut类型 | 不证明文件或整个资源预算 |
| `BookieAuthorityMatchesAppliedPrefix` | RFC-0005 §5.2/§9；applied image加完整后缀恢复同一权限、close事实、最新LAC和generation | 直接状态投影；Java按首次命令/结果重建由真实文件测试验证 |
| `BookieTerminalPermissionsRemainDenied` | fence/tombstone不因snapshot丢失而恢复normal/read权限；旧activation/install identity重试不改变权限 | 单ledger固定轨迹；冷authority/credential/barrier输入已验证的假设，不是A+C或整体ACK证明 |


Arena rotation回执`unified-wal-model-t1waf2bm`增加以下有限投影：

| 性质 | 当前合同与实现 | 证据边界 |
|---|---|---|
| `TypeOK`（ArenaRotation） | 单次rotation/双root/单tail/单故障的阶段、集合和计数 | 不证明字节或资源总账 |
| `SurvivingAuthority` | RFC-0003 §11；force新文件与目录、两份完整根后才回收旧prefix | 两root保存相同普通authority；不展开一般conditional state |
| `AcknowledgedTailSurvives` | 切换后普通请求在新代force后成功，旧代不能替代current tail | 不证明实际fsync或坏framing分类 |
| `RecoveryKeepsAcknowledgements` | 合法恢复根加当前必要tail保留全部既存确认 | 单次掉电/单image损坏；不是A+C组合 |

Bookie回收权限回执`unified-wal-model-bu9e_n9l`另检查：

| 性质 | 当前合同与实现 | 证据边界 |
|---|---|---|
| `BookieRetiredPrefixNeedsCheckpoint` | RFC-0005 §5.2；旧prefix回收后仍有已提交image覆盖退休cut | 双根/barrier由独立C切片作为假设，不是A+C组合 |
| `RetiredBookieTerminalPermissionsRemainDenied` | 被回收权限历史中的fence/tombstone仍禁止normal/read恢复，close/LAC由完整权限相等性质检查 | 单ledger固定轨迹；指定负向在错误image恢复后进入退休域才触发 |

当前权限投影9个动作完整探索48,763 distinct states；字节、密钥、锁、真实文件和票据门槛由独立Java测试验证，模型不授予生产authority。


派生索引覆盖回执`unified-wal-model-p_83_s8r`增加以下有限投影：

| 性质 | 当前合同与实现 | 证据边界 |
|---|---|---|
| `CoverageWasPersisted` | RFC-0003 §15；同一index generation的完整实际内容flush后，才force持久coverage | 固定2个已选定DATA entry；CF原子性、flush和发布屏障为输入假设，不证明JNI或设备 |
| `RecoveredCoverageHasLocations` | 恢复检查当前index generation和完整实际定位；新重建代使旧coverage失效 | 已选定DATA/route作为输入；不证明在线handoff、扫描跳过或完整恢复协议 |

正向完整探索53个distinct states，指定负向缺少flush即违反`CoverageWasPersisted`。实际RocksDB读取、内容摘要、generation绑定、字节向量及索引全丢失重建另由Java验证，不从此投影推出完整Model A/C或生产authority。
