# Spike C 有限模型实施

状态：EXPERIMENTAL / NON_PROMOTABLE / NO_AUTHORITY / FINITE_SLICE_PASS。Model A Bookie已执行权限快照切片及Model C管理会话/前缀/barrier/错误隔离/Arena checkpoint发布切片的四个正向配置已完成有限状态探索，六个负向配置均得到指定性质的反例；不是完整 Model C、A+C 或 Block G 验收。

合同由[RFC-0003](../../RFC-0003-segment-storage-allocator.md)、[RFC-0005](../../RFC-0005-segment-bookie-state.md)及[Spike C](../../spikes/SPIKE-C-no-object-tla.md)拥有。`UnifiedWalTypes.tla`仅定义纯函数；`SegmentAllocator.tla`按可读写入、文件barrier、连续前缀发布、控制durable/apply、CLOSE和恢复阶段分别建模；`ArenaCheckpoint.tla`单独检查applied快照、payload force、控制commit、A/B发布及保留完整日志时的恢复；`BookieCheckpoint.tla`检查固定权限轨迹的快照与后缀，不使用对象存储。

## 复现与输入绑定

```bash
python3 scripts/unified-wal/run-storage-models.py --tool /path/to/tla2tools.jar
```

工具固定为[TLA+ 1.7.4 官方发行](https://github.com/tlaplus/tlaplus/releases/tag/v1.7.4)的`tla2tools.jar`，发行SHA-1为`bee4a54f3ee3d4afc347c3240ec2d9e93b075104`；runner验证它并记录实际SHA-256，不自动下载或改Maven依赖。本次取得的完整文件为2,274,532 bytes，SHA-256为`936a262061c914694dfd669a543be24573c45d5aa0ff20a8b96b23d01e050e88`，manifest记录工具版本和checksum。使用当前Java，1 worker、512 MiB heap、64 MiB direct上限、固定fingerprint polynomial 0与seed 1；每config最多120秒，无symmetry或额外state constraint。安全性检查不增加fairness；有界停止/隔离状态允许无后继，未检查deadlock或liveness。

每次运行在独立临时目录冻结modules/configs，保存manifest、命令、JDK/平台、源码哈希、父提交处的合同哈希、checker输出、完整负向trace、coverage-summary及checksums。统计记录generated/distinct states、剩余queue、searchDepth、仅完整探索时的diameter、elapsed及`time`测得的进程peak RSS。负向检查在发现反例时停止，其搜索深度不是完整图直径。超时终止本次checker进程组并标INCONCLUSIVE；语法/工具错误及未完整探索都不是PASS。正式配置或源码改变须新建run，不能改绑定旧结果。

## 矩阵与范围

| 配置 | 有界结构与目的 |
| --- | --- |
| `c-managed-shared` | 2 stream × 2 batch、共享1个DATA文件、最多2 epoch/2次进程退出/2次掉电；当前合同的正向检查 |
| `c-managed-split` | 相同结构分属2个DATA文件；当前合同的正向检查 |
| `ce-data-before-barrier` | 故意允许完整write替代DATA barrier；必须违反`DataSuccessHasBarrier` |
| `ce-control-before-sync` | 故意允许未同步可读控制记录直接apply；必须违反`ControlHasBarrier` |
| `ce-open-replay-after-error` | 故意允许OPEN在校验/sync后直接取得恢复资格；必须违反`KnownErrorRemainsIsolated` |
| `c-checkpoint` | 3条控制命令（第2条条件失败）、2个slot、最多3代checkpoint/2次进程退出/1次掉电；完整控制日志保留 |
| `ce-checkpoint-durable-cut` | 故意取durable cut替代applied cut；必须违反`SnapshotCutWasApplied` |
| `ce-checkpoint-early-publication` | 故意在控制commit持久前发布；必须违反`PublishedImageHasDurableCommit` |
| `a-bookie-checkpoint` | 1 ledger、固定10条已认证控制记录、2次snapshot/2次process crash；恢复权限与完整applied历史一致 |
| `ce-bookie-checkpoint-terminal` | 故意在image丢失fence/tombstone；必须违反`BookieTerminalPermissionsRemainDenied` |

负向配置是被否证的候选规则，不是生产功能开关，也不是实际Java源码变异。它们的EXPECTED_COUNTEREXAMPLE用于检查性质能否发现指定错误；不能计入正向PASS。完整trace保存模型oracle`faultSeen`：它在进程退出后仍记录先前I/O错误，而恢复进程只能读取控制与DATA镜像。该oracle不是实现新增的持久日志。恢复同步只建立当前bytes的durability，不自动证明先前错误已经解析。

模型假设输入grant、storage identity、Bookie权限及framing已验证，旧进程退出已终结模型中的提交者；batch冻结后不可变、旧授权范围不覆盖/复用。durable bytes在掉电时保留，可读未同步bytes可丢失。有限模型不证明真实fsync、设备隔离、目录同步、CRC、字节预算或NUMA。未展开allocation条件命令全集、generation reuse、MOVE/FREE、控制前缀回收/rotation及index覆盖、损坏/部分framing、任意Bookie route变更/跨ledger竞争、quorum/客户端、删除和进展；它们仍须后续Model A/C/D及A+C/C+D配置验证。

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
