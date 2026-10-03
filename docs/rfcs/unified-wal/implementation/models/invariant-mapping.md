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


索引维护回执`unified-wal-model-tn4d2l9o`增加以下有限性质：

| 性质 | 当前合同与实现 | 证据边界 |
|---|---|---|
| `BacklogBudget` | RFC-0003 §13/15；hot记录提前计费，每shard一个索引批次复用DATA票据 | 3 entry/2 hot/2 ticket；不证明全部byte/native/disk预算 |
| `LocalSuccessKeepsHotAuthority` | 后台Put暂停或失败不撤销已durable DATA/热定位结果，不提前淘汰热记录 | DATA barrier/发布为输入假设，非完整A+C证明 |
| `IndexRowsCameFromDurableData` | 只有live-durable热定位进入普通派生追加，query-visible不代表新的持久覆盖 | 本地定位投影；完整权限/selector由实际控制和读路径负责 |
| `FenceCanUseReservedLane` | 维护不占控制票据或控制I/O，已排空pre-cut目标的fence仍可启用 | 检查可启用性，不证明fairness/实际force/liveness；一般pre-cut等待仍允许 |

正向完整探索482个distinct states，负向索引占控制credit得到指定性质反例。真实暂停和失败的独立Java回归验证当前I/O/owner接线；hot淘汰及在线覆盖仍未接入。

当前接管回执`unified-wal-model-2qh8pivn`在上述历史维护投影基础上完整探索858 distinct states：

| 当前性质 | 当前合同与实现 | 证据边界 |
|---|---|---|
| `LocalSuccessKeepsLocatorAuthority` | query-visible owner接管前保留hot，之后仍有当前DB定位；过早淘汰负向实际违反 | 不证明真实Get/selector/pin，Java独立校验 |
| `OccupiedCoordinatesSurviveEviction` | hot淘汰只释放定位容量，已接纳/unknown坐标仍由有限occupancy保留 | 3坐标、2hot/2ticket；完整恢复容量独立，不证明长期/native总账 |

`BacklogBudget`现在检查pending DATA与hot合计而非历史累计admitted；`IndexRowsCameFromDurableData`与`FenceCanUseReservedLane`保留。正向、两个指定负向及真实文件回归独立记录，在线coverage与完整模型仍未完成。


## 运行期覆盖发布

`unified-wal-model-47t1o26s`新增有限投影及三个指定错误规则：

| 性质 | 当前实现与合同 | 证据边界 |
|---|---|---|
| `RuntimeCoverageWasFlushed` | `SegmentDerivedIndex.checkpointRuntime`验证原owner cut和当前行，真实单CF flush后才force覆盖页 | 模型flush为独立动作，contents为有限计数；真实native/CRC另由Java验证 |
| `RuntimeCoverageStopsAtCapturedData` | `pauseIndexForCheckpoint`冻结当前单stream连续前缀；暂停期间新DATA只保留hot，不扩大本次cut | 单stream每DATA一定位投影；不是一般MOVE/FREE或多worker证明 |
| `PublicationClockAdvances` | `nextPublication`取scope/CRC/UUID有效页的最大序号，即使旧内容已不匹配；同代重试递增 | 无坏CRC/部分字节；crash不承诺未持久页时钟，真实force失败另测 |

正向完整探索3,102 distinct states，三个候选规则均违反指定性质。覆盖仅说明被冻结且实际flush内容的派生定位范围，不替代Bookie权限、DATA或恢复同步；该历史版本restart仍全量扫描；后续覆盖辅助恢复另见下节。


## 持久覆盖辅助的尾部恢复

`unified-wal-model-h1roqnsp`正向完整探索280 distinct states，保留如下性质：

| 性质 | 当前实际路径 | 证据边界 |
|---|---|---|
| `SkippedBodyHasDurableMatchingIndex` | `SegmentDerivedIndex.recoveryCoverage`验证真实contents/page/scope/UUID/current cut/stream/grant；无效proof全扫描 | 集合相等代表内容摘要；真实CRC/身份/目录绑定由Java验证 |
| `ReadyHasRequiredDataSync` | `scanWhileLocked`保留DATA恢复force，返回inventory及native恢复/新session之前不能失败 | 模型sync为动作；真实失败注入另测 |
| `ReadyHasContiguousPhysicalPrefix` | 无论covered与否都读取独立parser验证的固定头，确认stream连续性和覆盖内必要头 | 单stream 3位置且假设扫描顺序；真实allocation区间拼接、多stream/grant/index batch位置由Java另证 |
| `ReadyRetainsExactlyReadableCoordinates` | 覆盖定位与完整tail合并、唯一坐标检查；当前Bookie tombstone过滤，native仅补tail/清除不可读行 | 无MOVE/FREE；不授予payload成功或确定absence，点读仍校验record |

三个指定负向分别针对无效proof授权、跳过sync及忽略物理缺口。有限模型没有RocksDB增量写入、native预算、真实body CRC/目录锁或任意selector；旧cold/运行期覆盖证明继续独立拥有原证据。


## 分配顺序与旧尾部隔离

`unified-wal-model-p7nk5a81`的分配顺序投影完整探索302 distinct states，两个指定候选均取得反例。

| 性质 | 当前实际路径 | 证据边界 |
|---|---|---|
| `NewerAllocationIsNotBlockedByFileOffset` | `appliedAllocationOrder`取原成功operation序号；`appendAddress`和`reservePhysical`按control order选择/前进，后来grant可处于低地址 | 已成功grant/原序号可信；真实grant、CRC和rotation由Java负责 |
| `OldAllocationsCannotBeReentered` | 每stream AppendCursor保留已进入allocation序号和offset，进入后继便拒绝旧grant及已取消预留 | 不代表旧writer I/O已终结，不授予FREE或generation复用 |
| `RestartDoesNotRestoreOldTail` | writer锁下冻结已重放Arena applied cut，排除全部旧grant尾部；新控制grant不被全文件高水位误拒绝 | 仅现有正常/physical restart准入范围；不修复未关闭会话 |

恢复`StreamProgress`按实际allocation原控制顺序拼接一个连续区间，仍拒绝区间内/区间间gap与重复；这一真实解析路径由Java验证，当前有限模型没有区间拼接或媒体损坏状态。现有TailRecovery模型的有序3位置投影不替代该证据。


## 整allocation FREE投影

| 性质 | 实施合同及实际边界 |
|---|---|
| `EveryOccupantWasTerminal` | 独立解析完整allocation的每个普通record，准确终态tombstone才证明dead；inactive/unknown/index miss及共享块部分删除不够 |
| `SubmittedFreeRetainsProtection` | 具体命令持有Bookie/file准入gate，控制force与原owner apply未终结时不能close；runtime stopWaiting不释放原票据 |
| `RebootCountsDoNotProveOldIoTerminal` | FILE实际reservation/write/read lease及Bookie entry admission都必须排空，未关闭OPEN仍拒绝；模型可见计数与实际I/O刻意分开 |
| `FreeAuthorityHasBarrier` | 当前前序grant/expected generation在durable完整命令后顺序apply才撤销/bump；checkpoint/rotation保留必要identity/序号事实 |

模型假设完整grant/准确终态与真实DATA验证，不代替Java格式/CRC或文件同步。未建模物理reuse、多个FREE/victim竞争、MOVE、完整权限组合、memory预算或liveness；正例68状态不是步骤9整体或生产权威证明。


## 健康 CLOSED 整块复用投影

| 性质 | 实施合同及实际边界 |
|---|---|
| `ReuseHasTerminalOldIoAndFree` | 先前FREE必须实际排空旧I/O/pin并durable apply；新ALLOC精确匹配整个retired extent/当前FREE generation |
| `ClosedTailHasActualDurableIo` | 实际关闭writer、reservation/I/O/read lease全部终结且无错误才保存CLOSED末端；取消请求或可见计数不代替真实终结 |
| `OldBytesNeverBecomeCurrent` | 当前reused grant与已应用健康CLOSED末端定义必要prefix；旧suffix不解析，prefix损坏或缺末端仍隔离 |

单extent一次reuse的有限结果不涵盖OPEN恢复、真实parser/native索引、多个FREE/MOVE、内存/磁盘预算或liveness。Java两轮generation复用是单独真实路径证据。


## 终态索引清理投影

| 性质 | 实际合同及边界 |
|---|---|
| `OnlyCapturedTerminalCoordinatesRetire` | native/owner只使用持有的同一准确终态snapshot，inactive/unknown和晚到T不扩大集合 |
| `ReleasedCoordinatesHaveNoNativeRows` | 全部旧Put/index义务终结后，实际native删除成功才允许原owner释放占用；native失败不释放 |
| `MaintenanceGateHeldThroughActualDelete` | Bookie entry lease与原index pause跨实际IO及owner终结，取消等待不放弃后台义务 |
| `CapacityBounded` | 固定coordinate/native行上限，释放后新写仍通过原预算；未推导完整memory/SST/临时磁盘预算 |

有限snapshot/集合投影不包含所有原worker时序、control/coverage完整状态机或FREE/MOVE组合。发布失败可保持安全删除且恢复准入，但不能把UNKNOWN或模型PASS当持久覆盖/生产authority。


## relocation 内部完成投影

| 性质 | 实际合同及边界 |
|---|---|
| `CopyFactsHaveBarrierAndPrefix` | 实际DATA barrier及原owner完整连续prefix后才构造副本事实，不从单个force或最大sequence推断 |
| `CopyDoesNotPublishOrdinary` | copy内部completion与普通Add/recovery结果分离，既有hot/index继续排除copy；MOVE是另需持久授权的阶段 |
| `BuffersWaitForBothTerminals` | 实际IO及原owner结果均终结后归还原池；取消等待或pending先失败不提前释放 |

副本字节输入及准确generation/grant假定由真实parser/allocator验证；该有限投影不决定MOVE/selector或FREE胜者，不推导完整内存/磁盘预算或恢复authority。


## 条件 MOVE 与读取保护

| 性质 | 实际合同与范围 |
|---|---|
| `SelectorHasBarrier` | Arena只对已force且按顺序到达的控制批次apply；copy durable本身不成为selector |
| `SelectorMatchesReplay` | 双坐标全部前驱条件成立才切换，旧赢家重试不覆盖后继，checkpoint/日志重放同一apply |
| `PendingMoveKeepsProtection` | runtime proof guard直到实际CONTROL I/O与原owner terminal才释放，cancel不提前归还 |
| `ReadPinKeepsSelector` | 初版全局Bookie/file drain排空实际读，阻止新read跨MOVE切点；consumer独立buffer可继续持有 |
| `CopyNeverSelects` | 未成功MOVE的copy仍是orphan物理事实，不发布普通坐标/ACK |

控制 metadata-only 重放与DATA当前目标验证分开；full scan仍检查未退役历史allocation。MOVE/FREE/reuse/native coverage组合和每allocation在线cutover尚未启用。
