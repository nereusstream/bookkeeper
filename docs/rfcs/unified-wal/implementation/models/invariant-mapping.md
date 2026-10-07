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


## MOVE/FREE/reuse

| 性质 | 实际合同与范围 |
|---|---|
| `SelectedAllocationRemainsLive` | 全victim proof排除活current target，FREE成功撤销grant；orphan目标FREE后迟到MOVE不可取得原grant |
| `AppliedFreeHadExactSelectorCut` | v2 proof引用Arena applied cut，首次apply要求等于本FREE前序sequence；不以digest或索引缺失代替selector |
| `SelectorNeverReentersReusedGeneration` | 原成功ALLOC序号/full generation与当前grant复核，旧COPY不进入新owner allocation |
| `RestartKeepsControlSelector` | 有序MOVE/FREE/ALLOC元数据重放后只验证current DATA与健康CLOSED末端，不重新扫描已退役源来选择坐标 |

准确Bookie T、真实目标点读/CRC/SHA和保护生命周期由Java/原模型拥有；完整native maintenance/coverage和liveness仍未展开。


Native MOVE coverage 回执 `unified-wal-model-75i1oty1` 的新增性质：

| 性质 | 当前实现及证据边界 |
| --- | --- |
| CapturedOwnersHadNoOldPut | 全部原 owner 在无旧 Put/候选/物理 reservation 时暂停；实际旧 Put stall/拒绝继承已有checkpoint回归，本投影无真实队列 |
| MaintenanceKeepsProtection | Bookie/file gate 与 scratch 持续到实际I/O及全原owner终结；等待方取消只撤销回复 |
| RecoveredLocatorUsesSelector | 陈旧 native locator 无法成为当前 coverage；无效覆盖改用完整DATA重建 |
| CoveredReadyValidatesCurrentBytes | seed内当前可读MOVE target仍实际点读并检查operation/generation/CRC/SHA，坏或缺失副本拒绝恢复 |

COPY/MOVE durability、grant与权限为输入假设；有限PASS不等于实际flush、字节验证、完整资源或掉电证明。


## 有界合批 compaction

| 性质 | 当前实现及边界 |
| --- | --- |
| `SelectionHasAllocationNetGain` | 实际source容量减COPY含framing/padding且向上取整的destination容量；单块零收益不分配 |
| `MovedTargetHasDurableCopy` | 新applied grant、既有COPY实际force/prefix结果后才提交条件MOVE |
| `FreedSourcesHaveDurableSuccessor` | MOVE及native覆盖之后，整组重新验证whole-allocation当前后继再durable/apply FREE |
| `AppliedFreeUsesOwnCut` | 起始applied cut加前序成员数，最终apply仍等于自身sequence-1 |
| `PendingMembersKeepProtection` | 同一guard覆盖所有实际claimed成员；取消仅撤销回复，重复finish不提前解锁 |

此投影未展开全T、真实字节/资源、native失败恢复、crash/reuse或进展；这些由Java和既有有限切片分别验证。


## 多批 compaction

`SelectionHasAllocationNetGain`现在扣除全部aligned COPY batch的总成本并按目的地allocation取整；`MovedTargetHasDurableCopy`累计各批的COPY/MOVE force事实；`FreedSourcesHaveDurableSuccessor`要求完成全部批次，再经native发布及FREE barrier。共享guard和各FREE前序cut的已有性质继续适用。`FreeIncompleteGroup`是被否证候选规则，whole-allocation当前successor proof在Java中仍独立重验，不能由moved计数替代。字节/身份/资源与进展不在有限计数投影内证明。


## 有界 source 选择

| 性质 | 实现与证据边界 |
| --- | --- |
| `SelectedSourcesUseActualData` | 有dead时只选真实当前DATA零live候选；native缺行不决定death；窗口16与control顺序由Java验证 |
| `DeadCandidatesAvoidInputRefusal` | 测量不捕获payload，有dead时仅捕获选中组；零live无需destination；满容量/合法小源预算实际回归 |
| `FreedSourcesHaveWholeProof` | 候选测量不授予FREE，实际整allocation后继字节/准确T proof及CONTROL force仍必需 |
| `FreedCurrentRowsHaveSuccessor` | live路径复用已durable COPY/MOVE，最终重新证明；模型不自行证明实际target bytes/权限正确性 |

这是有限输入假设下的选择约束及准入性质，不是全存储liveness、窗口外公平性、持续容量或完整resource模型。


## 有界窗口轮转

| 性质 | 实现与边界 |
| --- | --- |
| `ThreeAdmittedCallsCoverStableRound` | 固定本轮上限、从最后尝试的原ALLOC sequence继续；零净收益/资源不足结果也推进已准入窗口，拒绝准入不推进；模型3候选/窗口1，仅检验继续发起已准入尝试且候选稳定时的有限覆盖 |
| `FourthAdmittedCallRevisitsOldestGrant` | 老轮空窗后最多一次刷新上限并从-1开始；后来grant不扩大未完成轮；实际ordinary refill后准确T回访验证 |

cursor没有持久授权语义，重启从头不影响selector/T/generation；真实FREE继续独立whole-allocation proof与CONTROL cut/barrier。未模型化调度、完整预算、失败存储或全系统liveness。


## 池化 compaction 捕获

| 性质 | 实现与证据边界 |
| --- | --- |
| `TypeOK` | 原owner计划使用既有indexedEntries数量上限，不持整组payload；单批最多256行/buffer容量；有限模型行计数不证明实际heap字节上界 |
| `CaptureAndCopyKeepOwnedBuffers` | capture I/O结束及原owner接续前持有scratch/COPY两个lease；转交driver后COPY等实际I/O与原owner终结；取消只停响应 |
| `CopyMatchesMeasurement` | 每批guard下复核真实source SHA/CRC/selector/permission及测量Bookie cut、冻结长度；有效CRC的payload变化与其他ledger实际控制变化均使后批PARTIAL |
| `FreeHasAllDurableMoves` | 先全部COPY/MOVE再native覆盖和最终真实whole proof；模型仅计数，不以计数替代实现证明 |

正向选择模型不再消耗整组payload输入；错误capture-all反例保持。source原batch适配scratch、真实byte/hash、完整资源和持久化语义由Java/既有切片验证，未提升完整模型或生产Gate。


## 保留 compaction tail

| 性质 | 实现与边界 |
| --- | --- |
| `TailWritesUseUnusedRange` | guard下扫描当前generation实际末端，校验最新grant/原owner已排空prefix，完整aligned COPY总需求须适配剩余tail；gen2按CLOSED used-end排除旧残留，已有cursor不得越过或与末端矛盾 |
| `RetainedCurrentCopyNeverFreed` | 自动组排除保留grant，显式请求只使用组外目的地；共享source proof/CONTROL FREE始终只退役原source，最新目的地中已MOVE副本保持当前 |
| `FreeHasCompleteCurrentSuccessor` | 全部新COPY barrier/MOVE/native覆盖和真实whole proof后FREE；有限model以计数/证明输入表示，不能自证actual DATA/CRC/权限 |

健康恢复默认仍跳过旧tail；只显式开启已验证最新current COPY tail，不回到更早grant。无合格tail或不足时继续拒绝；不宣称一般partial、OPEN错误、调度、长期容量或全资源闭合。


## 维护容量预留

| 性质 | 实现与边界 |
| --- | --- |
| `WholeBlocksAccountOnce` | fresh高水位后的whole blocks加每个retired slot，已apply grant仍占用；restart重新推导，没有新增持久预算日志。FREE只增加内部reusable，不增加filesystem available |
| `PlannedForegroundKeepsReserve` / `ForegroundKeepsReserve` | 同文件多个shard的成功计划逐个扣减，容量拒绝不新增DATA sequence或UNKNOWN；仅限制新pool分配，旧grant可用tail不计新空间 |
| `DestinationFitsDeclaredWorkset` | 单compaction并发，声明每文件维护blocks同时是全aligned目的地最大值；超过限制在ALLOC前拒绝，全死直接FREE仍可推进 |

计数不能证明连续extent可用、source死亡或系统liveness。现有同尺寸whole reuse、无split/coalesce，SST/control/checkpoint临时工作集和共享filesystem尚未关闭。

## 运行期控制日志轮转

| 性质 | 实现与边界 |
| --- | --- |
| RotationKeepsReservations / GuardWaitsForOriginalOwner | 全图pause、两个driver维护排他和真实file/Bookie保护一直保留到原owner的terminal action；stopWaiting仅撤回复，实际CONTROL I/O返回前不释放 |
| ProtectedDataIsTerminal | Bookie lease拒绝未终结entry，文件guard拒绝实际DATA I/O、预留和read pin；不是所有网络buffer或native索引工作集证明 |
| OpenSessionAndCursorRemain | 受保护Arena rotation保存原OPEN/epoch/append cursor，保留两commit以外的真实CLOSE空间；不伪造CLOSED或归还已使用tail |
| ArenaRootHasCompletedBookie / SuccessHasBothRoots | 两owner顺序各自提交，返回各自实际root，只有全部完成才ROTATED；无跨owner原子提交承诺 |
| UnknownStopsGraph | 任一已尝试轮转失败报告UNKNOWN，停止新准入；Arena失败仍需已有OPEN恢复规则，不靠checkpoint宣称ready |

物理双恢复根、CRC/identity/目录持久化是既有独立模型与真实回归的输入合同；本投影不关闭完整A+C、B17、liveness或生产Gate。

## 控制容量恢复

| 性质 | 实现与边界 |
| --- | --- |
| OversizeNeverStartsRotation | 当前容量不足且请求连同保留量能适配新journal时才标记pressure；本身超大请求保持拒绝，不靠无限轮转制造容量 |
| PressureRefusesNewEntries / BlockedDemandRemains | 新graph准入在pressure期间拒绝；driver/ticket/guard暂不可用不清除日志需求，旧义务仍终结，不存储业务重试队列 |
| AutomaticIoHasTerminalResources | Bookie与files预检查避免反复投递无收益I/O，实际driver排他和guard仍二次取得；不自证真实pin或全部并发 |
| PauseWaitsForOwner / FailureIsObservableAndStopped | 实际I/O终结不提前解冻图；UNKNOWN原owner保存cause、停止准入，不因日志failed使pressure谓词为false就伪造恢复 |

资源总账、字节/force、全notify路径与长期liveness仍由相应真实验证/完整模型承担，本有限投影不关闭B17或生产Gate。

## 冷启动控制容量

| 性质 | 实现与边界 |
| --- | --- |
| RotationHasValidatedProtectedClosedData | 当前必要DATA检查/同步及同一writer排他先于Arena冷轮转，实际会话保持CLOSED；模型把锁与恢复结果作为输入，真实暂停/冲突/失败测试独立验证 |
| OpenHasRecoveredCapacity / FreshJournalTooSmallNeverRotates | 旧日志不足才检查fresh-fit，包含下一CLOSE真实reuse witness及双root保留量；适配后轮转先于下一OPEN，不能恢复容量时不消费checkpoint |
| ReadyHasDurableOpenAndNoFailure | sessionReady只在OPEN成功后设置，owner bootstrap后再开放图；checkpoint/候选/OPEN force失败不发布成功，真实未知OPEN继续拒绝恢复 |

有限终态CHECK_DEADLOCK FALSE；byte/force、崩溃、完整控制并发、liveness及生产Gate不由本投影闭合。


## Arena 空间压力维护

| 性质 | 实现与边界 |
| --- | --- |
| MaintenanceStartsWithActualResources / ControlRotationRetainsPriority | 既有owner pump先恢复控制容量，实际Bookie/file drain与各原池buffer额度再允许job；诊断不代替真实guard和原owner借池 |
| UnchangedFullLiveWindowsVisitedOnce | 每文件冻结cut、同文件固定shard逐窗口，只有准入成功才前进，无净收益轮结束等待新的控制事实；不证明全局公平或长期deadline |
| KnownRefusalKeepsFreshProtectedWork | 仅写入开始前typed容量拒绝可保留压力、清除cursor、先rotation再重新检查；已写COPY/MOVE仍保留真实事实，不重放旧业务 |
| UnknownStopsDataAdmission / RestoredAdmissionHasActualCapacity | 实际未知失败保留cause、停止图，新pool需要实际同尺寸extent和预留；模型把真实whole-block事实作为输入，不证明物理回收 |

有限终态没有liveness、公平调度或共享filesystem预算证明；完整A/C/D与组合、B17和生产Gate仍未关闭。

## 内部 checkpoint 历史

| 性质 | 实现与边界 |
| --- | --- |
| ImageHasBoundedInternalHistory / SnapshotRetainsCurrentRootIdentities | 保留当前双 root 加最新两个成功内部 commit，image 内合计至多四项；成功物理回收后才清理 live 表，当前 suffix 不计为 image 项也不绕过 journal 预算 |
| SnapshotKeepsBusinessResults | 仅省略过时内部成功 commit，原业务及失败条件照常保留；固定 grant/credential/T 的模型角色不能代替所有真实业务重放验证 |
| LegacyReadyHasCompleteCheckpointHistory | v1 不采用内部 generation 跳跃恢复；v2 版本绑定 metadata CRC，旧 v1 字节与读取规则保留，未知版本仍拒绝 |
| ReadyHasCheckpointPredecessor | v2 保留内部 commit 严格递增且小于目标代，正常重放条件结果，最终 generation 必须恰为目标 predecessor；真实 commit/image/reference 和完整必要 suffix 仍不可缺失 |

有限模型不证明业务历史可无限创建删除、共享 filesystem 工作集、长期 liveness 或完整 A/C/D；B17、步骤 8/10 与生产 Gate 仍未关闭。

## 原 journal 的压缩 checkpoint 恢复

| 性质 | 实现与边界 |
| --- | --- |
| OriginalRestoresValidCompactedImage | original 与 rotated 使用相同 image 语义，完整 v2 不因缺少已替代的内部 history 被拒绝；模型固定两种 recoveryMode 和八个内部 generation，真实测试核对最高已发布恢复根 |
| ReadyHasCheckpointPredecessor / LegacyReadyHasCompleteCheckpointHistory | 同一验证入口仍要求准确 predecessor，v1 完整历史、原条件结果及必要 suffix 不放宽；模型门槛不代替 commit/hash/真实同步 |

完整日志回退保留，但不能掩盖合法 checkpoint 的恢复遗漏；本增量不证明业务历史压缩、liveness、完整模型或生产 Gate。


## 退役 DATA 序号区间

| 性质 | 实现与边界 |
| --- | --- |
| RetiredCoverageIsExact | 同准确 shard/generation/start 且物理序号连续才合并，合并前后覆盖集合相同；有缺口或不同 stream 则保持分离 |
| CurrentAllocationOrderIsNotBridged | current grant 的原成功控制序号作为边界，包括无 DATA 的 grant；不能按 file offset 或仅按物理序号忽略它 |
| RequiredBookieCutIsRetained | 合并区间携带成员最大必要 cut；模型输入已有成功 FREE，不授予新的业务权限 |
| EmptyRetirementKeepsRequiredBookieCut | 空 FREE 无序号范围但仍保留必要 Bookie cut；恢复检查所有退役事实的最大 cut，不只检查非空投影 |

原 request-id/allocation/FREE/session 等历史仍保留，排序工作集仍随历史增长；有限投影不证明完整业务压缩、有界长期资源、liveness 或生产 Gate。


## Arena 当前授权与历史身份

| 性质 | 实现与边界 |
| --- | --- |
| IssuedRangesAreExact / IssuedRangesAreDisjoint | 独立 issued ID 闭区间只覆盖实际成功分配，左右相邻区间合并；FREE 不移除已用身份，任意 ID 空洞不是已使用 |
| FeasibleUnissuedIdentityIsAccepted | 不以 ID 低于高水位代替 issued 查询；已满足实际 slot/stream 条件的未用低 ID 可成功，模型将这些前置条件作为输入 |
| HighWatermarkTracksOnlySuccessfulIdentity | planner 从独立成功高水位产生新 ID，失败分配不推进；实际边界/溢出仍按既有 API，模型固定四个 ID |
| CurrentGrantKeepsOriginalOrder | 当前 grant 保留首次成功序号，MOVE/FREE/append 从当前授权读取；准确重试不重新定义顺序 |
| EmptyBarrierRemovalIsFolded | 空 FREE 移除 current grant 后重新折叠已有范围；新的可合并邻居不能永久保留过时边界 |

新状态在每次恢复候选前清空、原完整命令 replay 重建；该真实恢复门槛不由输入型有限 indices 模型自证。整体幂等历史、compact image、长期资源与生产 Gate 尚未闭合。


## 当前 COPY 证明

| 性质 | 实现与边界 |
| --- | --- |
| CurrentCopyProofIsExact | 当前 allocation 的索引包含全部且仅包含已成功 MOVE 的 COPY；selector 更换不丢弃仍占当前 allocation 的旧成功 COPY，FREE 一次删除该 allocation 的全部证明 |
| CurrentSelectorHasCopyProof | 当前 selector 来自实际成功 MOVE 的 target 和身份，不由 COPY durable 或失败条件生成；真实成功前驱、源与目标 grant 和 Bookie cut 是输入，不由该投影证明 |
| QueriesBindCurrentIdentityAndDigest | 来源判断要求准确 location/UUID/generation/digest；失效 allocation 的历史成功、错误身份或不同字节都不能成为当前 COPY 证明 |

模型固定三 COPY、两坐标、两 allocation 与一次复用；独立成功 reference set 保留历史，而查询投影只取当前 generation。实际记录/校验和/同步与 A/B fallback 在真实文件测试中验证，有限模型不等于完整恢复、请求历史裁剪或总工作集有界。


## ZooKeeper readiness CAS

| 性质 | 当前合同与实现 | 证据边界 |
| --- | --- | --- |
| TypeOK（ReadinessCas） | 两个 birth、三代 readiness、dataVersion 0～2、一次并发修改/重建、phase 0～5 | 不是 ZooKeeper zxid 完整范围、client 队列或内存预算证明 |
| ReadinessCasPreservesBirthAndExpectedGeneration | 先检查 exact mzxid/readiness generation，提交时原子检查不可变 birth 与 dataVersion；重建后不能以 version 0 接受旧 CAS | 模型抽象 birth 唯一且不可变、scope/ACL 有效及 atomic multi；删除重建与双版本比较另由真实 server 测试验证 |
| UnresolvedLostReplyNeverReportsApplied | 提交后丢回复/超时不转为成功，保留 RESPONSE_LOST，fresh exact-read 才解析 | 模型不证明实际网络或 deadline 实现，实际已提交回调故障由 Java 测试注入 |
| ReconciledSuccessMatchesCurrentRecord | Query 仅在当前目标相等时得到等价结果，后续修改可使旧目标不再匹配 | 模型只以 generation 2 代表完整目标；完整 record 的 equality 另由实际 codec/多字段测试验证 |

三个错误规则分别跳过 birth、忽略 expected readiness generation、直接提升丢回复，必须从 Next 轨迹违反指定性质。有限终态允许 `CHECK_DEADLOCK FALSE`；不从此推导 liveness、实际持久化、安装/激活、service-info、ephemeral registration 或生产 authority。


## Session 绑定的 publication 性质

| 性质 | 合同与实现 | 边界 |
| --- | --- | --- |
| TypeOK（RegistrationPublication） | 两代 authority/nonce/projection，serviceVersion 0～3，phase 0～6 | 不代表 Java 内存/客户端队列或长期预算 |
| PublishedOnlyAgainstMatchingAuthority | info/writable 提交均 check 原 birth/dataVersion，必须匹配 READY、incarnation/gen/mzxid/hints | 原子 backend 与有效输入是假设，真实查读后修改由双 client 验证 |
| OldDemotionCannotRemoveReplacement | retired nonce 不复用，旧删除 multi 检查原 session marker；新注册即使节点版本相同也不能被删除 | 唯一 marker/当前 session 为输入，不证明恶意管理员复制 marker |
| WritableProjectionMatchesService | writable 创建原子条件更新相同 service-info 字节以 bump 版本，排除并发旧 refresh | 实际 envelope/Stat/endpoint equality 由 Java 验证；不授予持续 local 服务准入 |
| UnresolvedPublicationNeverReportsApplied | post-submit timeout/loss 仍 RESPONSE_LOST，owned exact read 才解析；demote unknown 不重新开放 nonce | 模型不证明实际网络、client deadline 或 watcher；真实回复故障由 server 事务后注入 |

generation 变化后既有 ephemeral hints 可以成为 stale；新读取/服务准入必须重新匹配，不能把过往 publish PASS 视为 lease。当前有限投影不是 service lifecycle、安装/激活、quorum 或完整 A+C/D 证明。

## 本地 readiness receipt

| 性质 | 实现合同与实际验证 | 模型边界 |
| --- | --- | --- |
| TypeOK（LocalReadinessPublication） | 两份不同内容的 receipt、请求 generation 1/2、一次 publication/进程退出/cold reopen | 独占 writer、严格 scope/codec、原子 rename 和成功 barrier 的 crash 保留规则是输入 |
| AppliedHasFileAndDirectoryBarriers | APPLIED 晚于 temp force、atomic rename 与 parent force；文件或目录 force 失败使实例 UNKNOWN | 真实 FileChannel 与有限故障点验证顺序，不证明文件系统或硬件 barrier 实现 |
| UncertainInstanceNeverReturnsReceipt | UNKNOWN 后当前实例的 read 抛异常、write 继续 UNKNOWN；不能用可读 renamed bytes 解析成功 | cold reopen 可完成新的 receipt 文件/目录同步，但不解除 DATA/control 已知错误 |
| UnpublishedTempNeverBecomesReceipt | load 只读取已发布名字，pending 从不成为恢复 authority | 不需要临时文件有有效 checksum；物理清理/覆盖仅针对该 scope 的固定 pending 路径 |
| OneGenerationBindsOneReceipt | 同完整记录等价成功、严格更高 readiness generation 才可替换；同代冲突及旧代拒绝 | 不裁剪 Arena/Bookie 请求历史，不建立业务 requestEra |
| ConfirmedReceiptSurvivesCrash | 成功文件和目录 barrier 后的 receipt 在抽象 crash 中保留；重开再次同步后才可读 | 实际两轮子进程退出覆盖 publish/reopen，不冒充掉电或整个 startup 的恢复证明 |

五个指定反例分别取消文件同步、目录同步、隔离读门槛、采用 pending 或允许同代覆盖。CHECK_DEADLOCK FALSE 仅允许有限终态。compatibility/device/recovery/delete catch-up、ZK registration、持续准入、quorum、liveness、性能及完整 A/C/D 组合不由该投影证明。

## 标准 metadata 创建

| 性质 | 合同与实际实现 | 模型边界 |
| --- | --- | --- |
| SuccessMatchesInstance / SuccessMatchesSidecar | 同ledger/instance/opaque reference才解析既有create；dataVersion重建同值不替代identity | 固定marker的长度/CRC/非零ID由Java独立向量验证，尚无后续mutation CAS |
| SuccessMatchesInitialPayload | E/W/A、ordered ensemble、digest/password、全部custom entries及显式持久ctime相同；随机creator token和backend推导ctime为观测 | 模型将所有不匹配初始内容投影为kind4，不枚举secret或全部metadata字节 |
| SuccessfulSnapshotIsValid / SuccessKeepsCapturedVersion | 仅合法匹配snapshot形成APPLIED/ALREADY_APPLIED，实际read保持自己的metadata/version | 原子create/read是backend输入，不代表结果之后仍有authority |
| SuccessHasActualMetadataSnapshot | 创建成功回调返回提交对象不足以证明成功，必须实际read；真实回调延迟期间修改quorum的回归先红后绿 | 模型显式区分创建回调与实际read，不枚举backend内部token比较逻辑 |
| AbsenceDoesNotProveNoEffect | 创建reply丢失后，即使实际节点被删除且fresh read NONODE仍UNKNOWN | 不证明先前Add/其他operation outcome，也不引入自动重建或重试 |

五个指定反例各取消一条instance/reference/content/unknown/实际snapshot边界。reservation、all-E durable receipts、sidecar READY及目标留存、delete cut、后续birth-bound mutation CAS、quorum、liveness和完整模型组合仍未覆盖。


## PREPARING reservation authority

| 性质 | 合同与实际实现 | 模型边界 |
| --- | --- | --- |
| GrantedHasValidCommittedRoot | 只有完整persistent且严格解码的PREPARING root形成authority；缺失/坏root/不可用均不放行 | 编码/CRC/count上限及实际ZK操作由Java验证 |
| GrantedMatchesRequestedKey | ledger/instance/purpose/domain exact key匹配，不能由caller选任意backend path | descriptor/完整operation/payload/principal继续由原endpoint exact AuthZ验证 |
| GrantedTargetIsPlanned | reader固定本地target，并仅选有序计划集合中的该target/incarnation | 不证明E个Bookie实际已完成inactive install |
| PreparingOnlyAuthorizesInstall | PREPARING只生成INSTALL authority，不能ACTIVATE或normal Add | 不包含READY publication及local ACTIVE状态 |
| ProjectionKeepsReadVersion | 同一次read的scope与Stat.mzxid对应，后续root替换不改旧snapshot | snapshot不是持续authority租约，也没有birth-bound mutation CAS |

五个敏感性反例各破坏以上一条边界。创建/重试/response-loss的完整内容和UNKNOWN性质沿用metadata-create投影及真实server测试；root/child CAS、delete cut、retention、quorum、完整模型组合、活性及性能仍未覆盖。


## Physical INSTALL

| 性质 | 合同与实际实现 | 模型边界 |
| --- | --- | --- |
| AdmissionHasExactAuthorityAndTarget | 原endpoint AuthZ/descriptor及adapter的实际target/incarnation验证晚于committed root读取、早于control提交 | principal/严格解析为输入，不证明TLS密码学 |
| InstallSuccessHasBarrier / InstallSuccessHasOwnerApply | 原driver的实际force与owner apply后才返回durable安装结果；barrier失败UNKNOWN | FileChannel测试验证顺序，不证明设备/掉电barrier实现 |
| SubmittedOwnershipWaitsForBothTerminals | timeout/interruption只结束等待，busy/control ticket/实际I/O及writer隔离由原driver终结 | 模型held聚合ownership，不证明所有TLS/socket/native内存总预算 |
| InstallPreservesNormalState | absent安装inactive，已active同内容retry也保持原状态，不用INSTALL激活或撤销normal | local READY/activation/fence/tombstone组合仍由既有/后续模型验证 |
| SubmittedUnknownNeverBecomesNone | 已提交请求timeout不能解释为未执行或取消成功；精确retry解析实际状态 | 更早UNKNOWN的跨attempt保持由调用方负责，未建全局client/coordinator |

六个指定反例各破坏上述exact/barrier/owner/ownership/normal/unknown边界；全E完成、receipt discovery、READY/CAS/delete cut、quorum、完整模型、活性与性能仍未由该投影证明。


## 初始 all-E metadata 门槛

| 性质 | 合同与实际实现 | 模型边界 |
| --- | --- | --- |
| InvalidPlanHasNoReservation | descriptor/metadata quorum及ordered E、credential与peer HELLO不匹配时在root写前拒绝 | 计划有效性为有限输入，具体codec/密码派生由Java验证 |
| MetadataWaitsForAllE / MetadataHasExactDurableResults | 收齐全部E的durable、正确关联结果，不能由A/W或错binding代替；UNKNOWN不会变NONE | 每target真实barrier/owner与correlation为输入，physical INSTALL另验 |
| MetadataHasSameRootSnapshot / MetadataHasCommittedReservation | all-E后sync/get与最初root的payload/mzxid一致且committed；缺失/不可用不放行 | read后的跨key窗口可产生inert orphan，后续READY/delete CAS仍待实现 |
| MetadataAloneNeverReturnsWriter | coordinator仅返回actual metadata及bounded receipts，不授予READY/ACTIVE | 没有完整writer availability、activation或quorum证明 |
| InstallReceiptKeepsBirthGeneration | current local generation推进不改变INSTALL birth；同op retry/文件replay恢复固定birth | generation取1/2，物理framing/schema由原控制log拥有 |

六个coordinator mutation和一个generation mutation命中各自边界；receipt capability digest及redacted字符串由真实/独立vector Java回归覆盖。未证明cryptographic HELLO、persistent receipt retention、full lifecycle CAS、完整模型组合、活性及性能。


## Initial READY / DELETE

| 性质 | 合同与实际实现 | 模型边界 |
| --- | --- | --- |
| ActivationHasPublishedReady | candidate page只durable不grant，必须matching READY head/reference/metadata；DELETE后无新authority | 冷读snapshot不是跨store/local即时撤权，local fence/drain仍由后续协议负责 |
| BirthChecksProtectTheCurrentRoot | Random lifecycle birth及只读marker同root创建，CAS不会只凭重置的dataVersion修改新root | marker/ACL/实际backend transaction由真实双client测试验证 |
| ReadyReferencesActualPageBirth | page独立birth marker保护旧candidate CAS，Head的mzxid/content reference不能指向重建的新page | 只有initial固定slot，没有full page/history GC |
| ReadyValidatedMetadataBirth | metadata version0还须matching creator token、instance/backlink、public ensemble digest | metadata read后可变化，后续ACTIVATE再次取证，非跨key原子事务 |
| DeleteCutNeverRollsBack / PublishedTargetsSurviveDelete | DELETE与READY同lifecycle顺序，global generation只cut推进；published targets retained | 不证明全部targets已经fenced/tombstoned或data physically deleted |
| ReadyRequiresAllEReceipts | 有序all-E scope/engine/capability digest/install birth facts在不可变page及head引用中retained | receipt crypto/actual local install由prior Java chain验证，不模型化untrusted caller/Byzantine admin |

七个指定反例命中各自界限，full A/D/C组合、replacement/repair、actual control wire、writer availability、活性及性能仍未由该投影证明。


## Initial availability

| 性质 | 合同与实际实现 | 模型边界 |
| --- | --- | --- |
| RequiredMetadataIsNeverRebuilt | candidate/READY已有durable metadata reference，actual metadata-server NONODE隔离且不create | page/ref/birth/codec验证为prior inputs，不替代full cleanup/rejoin |
| AvailabilityWaitsForAllE / AvailabilityHasCurrentBoundResults | 全E matching ACTIVATE/ledger/op/control generation/positive current result才能initial available；A不足、错binding拒绝 | exact peer、actual RPC/current state及local I/O由Java/prior slices拥有 |
| AvailabilityDoesNotCountUnknown / AvailabilityDoesNotCountHistoricalFence | UNKNOWN不升级；historical APPLIED在已fenced时仅STALE，不证明当前active | owner观测是snapshot，不能承诺以后不会fenced或offline |
| AvailabilityHasMatchingMetadataFact / AvailabilityObservesReadyNotDelete | 全回复后actual metadata/page及最后READY reference/birth取证，DELETE阻止报告availability | 非cross-key/cross-target atomic lease，没有writer authority/canary证明 |

七个敏感性反例分别命中上述all-E/binding/known/current/rebuild/metadata/delete边界，full client/recovery/delete/GC、完整模型组合、活性/性能仍未覆盖。


## 初始 socket control 身份与所有权

新增`ControlRequiresNegotiation`、`ControlRequiresExactAuthority`、`WorkspaceMovesWithControl`、`ColdAndIoCopiesRemainCharged`、`SuccessRequiresActualForceAndOwner`、`SubmittedUnknownIsNeverNone`及`TimedOutAdmissionNeverSubmits`；七个mutation分别取消capability/授权/工作区转账、终结等待即释放、force前回复、UNKNOWN映NONE及允许迟到EL提交。新正向1054 generated/546 distinct/queue0完整PASS；当前矩阵49正向/184指定反例，不以有限输入证明实际TLS、fsync硬件、SDK/quorum或全系统总预算。[映射与最终绑定](README.md#初始-control-socket-分发投影)。


## Initial client 的 peer、关联与输出 ownership

新增`RequestRequiresVerifiedPeer`、`SuccessHasBoundReply`、`SlotsWaitForBothTerminals`、`FrameIdsNeverAlias`、`NetworkUncertaintyNeverMeansNone`、`CancelledFutureIsNotRevived`及`CapacityIsBounded`；peer验证及server成功的physical证据是外部已验证输入。正向2775 generated/1281 distinct/queue0完整PASS，七个mutations命中指定性质；实际mTLS/NIO/FileChannel/ZK和schema字节由声明范围Java测试分别支持，不证明完整系统、TLS内部/kernel总账或可持续性能。[投影范围及source/run](README.md#初始-client-关联与-slot-投影)。


## Ordinary source/output ownership

新增SourceSnapshotRequiresQuota、QueuedSourceWaitsForTaskDecision、SubmittedEntryFitsInstalledBound、SuccessRequiresDurableStatus、OutputWaitsForActualWrite和SlotWaitsForWriteAndWire。5个mutation取消quota、queued source持有、entry bound、将NONE/UNKNOWN当成功和reply先归还output quota，命中指定性质；正向85 generated/66 distinct/queue0。实际digest/buffers/TLS/force与原AckSet由当前声明范围测试支持，不证明整个SDK、总内存或完整A/C/D。[范围及绑定](README.md#普通-client-的-source-与-output-投影)。

### 固定初始 SDK bridge 性质

ProfileSdkBridge 检查 KnownNoneNeverClearsUnknown、RetryKeepsSource、RetryKeepsCoordinate、OneOriginalCallback、SuccessRequiresOriginalQuorum、LocalStopNeverPublishesClosed；对应 ProfileBookieClient 的历史 UNKNOWN、保留打包字节/坐标、terminal CAS、原 AckSet 及 ProfileLedgerHandle 的本地停止。五个单独变异分别清除 UNKNOWN、提前释放 source、换 retry 坐标、重复 callback、发布 Classic CLOSED，必须给出指定性质反例。attempt 常量为2、target为3、entry为1，仅验证有限分类/控制状态；下层实际 TLS/write/wire 和 server force/owner 仍由已有切片及真实测试验证，不代表全系统 refinement、活性或完整 A+C+D。

### 标准 metadata membership CAS 性质

ProfileMembershipCas 检查 NewPhysicalBirthIsNeverOverwritten、CapturedBirthIsFrozen、FrozenMembershipIsNeverWritten、MetadataFieldsArePreserved、SuccessHasExactMappingAndVersion。七个变异分别取消最后marker检查、UNKNOWN后重捕获birth、忽略freeze、丢失creator token、丢失user字段、把未改变的predecessor提升为成功、接受更高version的同mapping。损坏新birth的不变量检查实际状态，不因最后返回CONFLICT而通过。有限范围是一次CAS/一次retry、两个parent birth、一个marker和一个额外version；成功是观测当时的snapshot，不是持续lease，不证明真实ACL、ZK多节点quorum、字节schema、clock/liveness、activation/cleanup或完整A+C+D。

### Replacement lifecycle 性质

ProfileReplacementLifecycle检查PublicationHasSameMetadataAndPageBirth、MutationUsesObservedRootBirth、DeleteCutNeverReopens、GrantedTargetsAreRetained、GrantRequiresCurrentPublishedAuthority、DiscoveryIncludesPublishedTargets及DiscoveryIsCompleteAtRead；两个sequential same-start目标、一个metadata freeze、两个root/page birth、完整保留prefix。八个变异分别信任inert candidate、忽略delete cut、跳过metadata条件、跳过page/root birth、丢弃前驱、仅发现当前target、接受旧authority。Grant/discovery均为采样时事实；已获本地权限不会被远程cut瞬间撤销。投影不证明codec/CRC/ACL、实际ZK quorum、force、replacement wire、SDK callback/resend、cleanup、clock/liveness或full A+C+D。

### Replacement inactive INSTALL 与 schema 性质

ProfileReplacementInstall检查InstallHasBoundLivePreparation、InstallNeverActivatesNormal、ActivationHasNegotiatedBoundSchema、AckHasActualForceAndOwnerApply。七个独立mutations对应跳过preparation、delete、metadata generation，INSTALL错误开放normal，UIC2/schema2忽略，以及force/apply前确认。一个冻结slot/一个ACTV、metadata predecessor/committed/changed三态、peer/capability/schema可用性；实时时钟/bytes/TLS/元数据quorum与whole memory由实际tests及其他slice负责。grants是读取时snapshot；远程cut不自动撤销已接纳的本地I/O，不证明liveness/fairness、SDK重发或完整A+C+D。

### 原 SDK replacement gate 性质

ProfileSdkReplacement 检查 AvailabilityIncludesActivation、GateRequiresWholeTransition、RetiredReplicaCannotAck、ResendKeepsSource、ResendKeepsCoordinate、OnlyAffectedSlotsResent、OneOriginalCallback、CallbacksStayContiguous 和 SuccessHasCurrentQuorum。七个 mutation 各命中指定性质；两个 pending entry 和两 slot 是有限边界，控制阶段依赖既有 receipt 投影，不是完整系统 refinement。同 ID rejoin、真实 refcount/crypto/fsync、timer 交错、全 quorum、controller restart、活性/公平性及 full A+C+D 仍未证明。

### SDK A=3 与第二次Start的性质

既有AvailabilityIncludesActivation、GateRequiresWholeTransition、RetiredReplicaCannotAck、ResendKeepsSource/Coordinate、OnlyAffectedSlotsResent、OneOriginalCallback、CallbacksStayContiguous、SuccessHasCurrentQuorum延伸到AckQuorum3、MaxChanges2。每次Start清除本次control完成事实，但保留未变ACK；回调仍pending才允许下一次变更，旧请求terminal独立推进。仅有限两entry/最多两次两slot变更；不涵盖任意长链、时钟/重试活性、实际内存、same-ID incarnation或full A+C+D。

### READ_ENTRY性质

ProfilePointReadDispatch检查AdmissionRequiresPermission、MissingRequiresCompleteStableFence、InFlightKeepsCredit、ObserverWaitsForActualTerminal、QueuedConsumerOwnsLoan、OutputWaitsForWrite、ReplyRequiresBinding。MISSING的coverage/stability是实现中真实producer需满足的输入事实，不是模型制造的durability证明；权限与缺失是采样切点，非持续lease。七个独立mutation各违反指定性质。单query有限投影不包括full recovery/readonly open、schema/CRC/crypto、clock、真实总预算、活性、公平性或full A+C+D。

### 网络FENCE客户端证据边界

复用已有的exact local fence/precut wait/force-owner及socket frame关联、source/output custody性质；本增量没有新操作body或durability算法。实际UNKNOWN不会撤销force，同operation重试不改变上下文或恢复旧future。模型与socket测试不能替代每write-set的集群coverage、逐missing来源fenced、LAC/source/length锚点和matching durable CLOSED，后续基础恢复仍须闭合。

### READ_LAC源取证性质

ProfileLacSource检查ReplyRequiresPermission、EmptyRequiresWholeLedgerProof、SourceHasValidatedData、ReplyRequiresSourceBinding、SourceLengthIsNeverPrefixLength。六个独立mutation分别跳过权限、仅凭坐标-1缺失声称EMPTY、忽略explicit LAC、把sourceLength标为prefixLength、忽略binding或接受未验证源，并违反指定性质。它只约束单次源样本与全ledgerEMPTY的回复；完整最大/explicit LAC、来源fenced coverage、原SDK quorum及matching CLOSED仍需后续模型和真实验证。
