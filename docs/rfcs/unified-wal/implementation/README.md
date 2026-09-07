# Unified WAL Implementation Wave 0

> 状态：**EXPERIMENTAL / NON-PROMOTABLE / NO AUTHORITY / DISCARDABLE**

本目录保存 Unified WAL Implementation Wave 0 的可执行实现边界与机器回执。Wave 0 只把已冻结合同变成 reference implementation、compatibility harness、形式化模型或隔离 prototype；它不授予 stable wire、stable on-disk、live shadow、Segment ACK 或生产注册 authority。

当前已实现的完整模块：

- [`ProfileDescriptor` reference implementation](profile-descriptor/README.md)
- [`Profile control/auth typed reference endpoint`](profile-control/README.md)：已完成interfaces、isolated endpoint、12项普通功能测试、机器回执与最终scope audit；仅消费transport facts与semantic store interfaces，不接入production listener/store/route/Add/ACK。
- [`Bookie startup/readiness/new-scope reference harness`](startup-readiness/README.md)：typed immutable facts、固定fail-closed startup顺序、durable local readiness、persistent CAS/response-loss重读、matching service-info、stale demotion、ephemeral writable registration、new-scope access isolation、九个crash/restart边界、17项普通测试、机器manifest/receipt/test summary与最终scope audit已完成；不提升Spike/Gate/production状态。

已冻结、延期验证的模块：

- [`Experimental Profile wire 与冻结的 old-decoder/stock-binary harness`](profile-wire/README.md)：Block D scope guard、pure codec 与610-vector corpus 已实现；真实4.14.8 decoder发现并冻结原`0x0FFE4250`反例，replacement `0x0FF04250`与authoritative corpus已通过new-codec定向回归。released-decoder/localhost stock-binary矩阵为`DEFERRED_NOT_RUN`，harness与corpus只保留、不进入当前执行，G1保持`BLOCKED_UNVERIFIED`。

输入锁见 [`manifest.json`](manifest.json)。每个模块自己的source/test/artifact范围、复现命令、证据结论和剩余BLOCK由模块目录拥有。实现证据不能自动提升四份owner RFC或三个Spike的状态。

## 下一阶段实施清单

2026-09-08收敛修订；规划基线`7e62fadeb9418b5aa0e8ac216169228dfb7e9da6`。以下工作全部为**PLANNED / NOT EXECUTED**，没有新增修复、存储写入路径、JUnit/正式run或PASS receipt。本轮把创建/读取/恢复、CRC32C entry布局、可恢复DATA成功边界、delete-applied与物理回收、全路径资源预算直接合入现有owner RFC。客户端维护与reference codec oracle的既有安排保留；尚未实现不作为文档阶段的新缺陷，也不要求先完成所有维护代码才完善文档或推进隔离切片。

| 工作项 | 实施与完成条件 | Owner / 验证入口 |
| --- | --- | --- |
| CLIENT-1 先修ACK状态 | 现有PendingAddOp换组同步撤销旧Bookie成功；数量和启用policy覆盖共同重算completed，优先同一有效ACK事实派生；兼顾timeout统计与关闭policy成本 | [RFC-0001 §9.5](../RFC-0001-profile-capability-install.md)、[Spike A A29](../spikes/SPIKE-A-profile-install.md) |
| CLIENT-2 先修对象回收 | 旧地址响应分支补安全maybeRecycle；最后旧响应/当前响应/callback未完成均不遗漏、提前或重复回收 | RFC-0001 §9.5、Spike A A30 |
| UW-1 普通删除竞争 | 保留admission/freeze、完整targets与authoritative tombstone的普通logical success；本地durable tombstone及可重建清理义务后推进delete-applied cursor，rejoin不等drain/compaction/free；共享L1/L2删除可连续应用，访问屏障和physical结果独立 | [RFC-0004 §10—14](../RFC-0004-range-recovery-delete.md)、Spike A A28、B4/B6、[Spike C D-LOGICAL/D-ACCESS/CD-REUSE](../spikes/SPIKE-C-no-object-tla.md) |
| UW-2 普通Add与identity | 全E install/activation只约束初始normal writer，读取/恢复各自授权；immutable capability/manifest绑定首批CRC32C布局，拒绝不支持类型；同坐标payload/累计length冲突，合法LAC/digest变化幂等且必要时核对bytes；复用热尾/索引与bounded pending，无每entry RocksDB/额外SHA-256 | [RFC-0001 §5.4/6.4/9](../RFC-0001-profile-capability-install.md)、[RFC-0005 §6.1](../RFC-0005-segment-bookie-state.md)、A16/A26/A27/B19 |
| UW-3 最小真实写入路径 | ByteBuf受控view、固定shard与预分配；DATA提交即冻结，完整write及覆盖本批的barrier→同stream连续physical durable前缀→readable→success；Bookie/Arena控制日志预建条件；真实A/B checkpoint/restart，后续补多Arena矩阵 | RFC-0003 §6.1/9、RFC-0005 §5.2/10.1、[Spike B B2/B16/B18](../spikes/SPIKE-B-allocator-block.md) |
| UW-4 allocator恢复与复用 | 三类tail oracle、物理批次乱序完成与前缀恢复一致，已允许成功的数据和move目标可发现；pool/shard ownership、old writer I/O终结/隔离、原子selector/pin cut与generation reuse；cursor不代替reuse条件 | [RFC-0003 §5.4/6/10](../RFC-0003-segment-storage-allocator.md)、Spike B B3/B6/B8/B9/B10 |
| UW-5 空间耗尽与进展 | 每Arena维护保留预算、限流/拒绝/恢复、维护调度份额；DATA满额仍保留fence/tombstone处理和控制持久化容量；有界live set下长期debt稳定并按deadline排空 | RFC-0003 §13/13.1、Spike B B17/B18 / C C-SPACE |
| UW-6 最小集群可恢复性 | 实验3/3/2与3/3/3、Bookie故障域F=1；基础point recovery/durable close/restart、fenced-close普通删除；不等待strong-publication、不reset loss window；旧binary/稳定协议依赖保持BLOCK | RFC-0004 §7.5/10/18、RFC-0001 §9.4、Spike C A-POINT/D-LOGICAL |
| UW-7 全量重建与总资源 | 正常restart和full-index-loss分开扫描/计量；全部live范围覆盖；route/pool/selector/grant/pending/buffer/checkpoint/index完整账本；请求从准入到I/O、prefix/locator和响应持续计费，最老deadline不重置，大entry有界路径或拒绝，重复核对读盘不阻塞shard | RFC-0003 §13/15、RFC-0005 §10.1、Spike B B9/B11/B16/B18 |

实施顺序：

1. **CLIENT-1/2先修**：独立Classic维护补丁与确定性回归，不等待Segment模型；不借此修改新存储ACK authority。回归复现旧ACK残留、数量够但域不足和最后旧响应回收，不能仅以代码审查当作修复完成。
2. **Block H与Block G并行**：H先交付UW-2/3一个Arena的隔离写入切片，再加入UW-4/5本地安全回收；G只展开实际启用路径的A/C及必要D/组合，仍保留`E>W`、response loss、offline/rejoin、generation reuse和逐CAS/local durability边界。两者各锁source/config和独立证据，不等待全部延期增强模型；模型不能证明真实fsync/CPU，性能run也不能证明quorum安全。
3. **先得到可解释的测量**：固定硬件、durability、TLS范围、E/W/A及负载，分别测低负载延迟、目标吞吐/尾延迟、过载有界拒绝、写入与回收并行、重启/故障恢复；统一记录allocation/copy bytes/entry、CPU/entry、entries/durability barrier、padding和全部磁盘写放大、全阶段资源峰值及compaction debt。write、durability、physical prefix和locator等待分别计量，不能把同shard前缀等待归为NVMe延迟；另观察线程hop/锁和控制日志fsync。局部无网络结果不作为端到端增益，尚无replacement等场景分别标NOT_EXECUTED/未覆盖，不新建庞大压测框架。
4. **集群入口补足安全/资源证据**：UW-7全量重建、UW-3多Arena及UW-4/5故障/进展等实际所需Gate闭合后，推进UW-6。基础point recovery及普通删除不能随高级Range延期；缺少相关证据的实验仍只能用discardable数据，完整Gate和canary证据不得后补。

首批不扩大为强访问撤销、strong completion/loss reset、online delete、general streaming Range/TailSummary/BatchRecoveryAdd、rack/AZ多策略或cross-Arena迁移。普通删除不等待所有历史target在线；强撤权须独立全目标屏障。strong reset后续仅限整个ledger fenced+CLOSED，基础close/membership不创建pendingPublication、不自动续期loss budget；旧token仍按原协议解析，不得强删。每项后续能力有实际需求和独立证据后再启用。same-scope旧binary和released-decoder矩阵保持原BLOCK/DEFERRED，本清单不是执行或生产授权回执。

本轮只在既有UW-1/2/3/4/5/7和Spike A/B/C场景内补充约束与确定性反例，不新增工作流平台、全局sequencer、分布式去重库、逐entry成功日志或在线权限lease。首批仍为一Arena、固定shard和少量已声明E/W/A/Bookie故障域配置，数值阈值由同一实验manifest在run前冻结。文档完成只表示接受条件、校验布局、DATA恢复边界、applied cursor及预算归还点已有一致定义。

`manifest.json`中的`sourceCommit`、`designInputsAtSourceCommit`和各模块receipt继续绑定原历史源码，不能改成当前文档hash来伪造新证据。`plannedWork`只导航本清单；正式实施时必须以新的source/config/run identity记录本轮合同验证，旧12/17项reference测试、wire corpus和codec clone oracle不证明上述协议已经实现。

CLIENT-1/2是单独列出的Classic修复计划，不纳入Wave 0历史scope/receipt；该计划不豁免Segment安全门槛。Wave 0仍禁止触碰：

- `BookieImpl` 普通 Add 成功路径；
- 通过`PendingAddOp`接入Profile/Segment ACK决策；上述独立Classic缺陷修复不借Wave 0证据取得authority；
- `LedgerStorage`、`Journal`、`EntryLog` 的生产 authority 或格式；
- 现有 `bookie-rpc` fallback；
- 生产 Cookie、registration、AutoRecovery 和 delete；
- 任何真实 producer 的 ACK authority。
