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

2026-09-08收敛修订；规划基线`635c383e80ef353753712a7117aef32fd6b8f313`。以下工作全部为**PLANNED / NOT EXECUTED**，没有新增源码修复、存储实现、故障测试或PASS receipt。本轮补齐CLIENT-1多slot本地ACK切换与回调gate、共享DATA文件的stream逻辑后缀/旧generation残留、normal/recovery/read/storage统一entry上限，以及Arena内部与filesystem实际空间预算。此前activation目标留存、异步索引/持久coverage、背压重试、读路径分层、CRC32C、DATA冻结/恢复前缀、delete-applied及延期范围保持。reference codec oracle与历史证据不重绑，尚未实现不作为文档阶段的新缺陷。

| 工作项 | 实施与完成条件 | Owner / 验证入口 |
| --- | --- | --- |
| CLIENT-1 ACK状态与切换顺序 | 复用现有completion gate，remote phase结束后继续阻塞callback；全部受影响pending/被替换slot映射及ACK/故障域状态更新、资格重算后才恢复连续回调。保留未变identity/incarnation的有效ACK，只重发必要副本；兼顾原残留集合、timeout统计及policy关闭成本 | [RFC-0001 §9.3/9.5](../RFC-0001-profile-capability-install.md)、[Spike A A29](../spikes/SPIKE-A-profile-install.md)、Model A-ACK-RESP；4/3/2先处理write-set外slot不提前成功 |
| CLIENT-2 先修对象回收 | 旧地址响应分支补安全maybeRecycle；最后旧响应/当前响应/callback未完成均不遗漏、提前或重复回收 | RFC-0001 §9.5、Spike A A30 |
| UW-1 普通删除竞争 | 初始/写期activation authority发布时持久留存target/incarnation，并与delete共享lifecycle cut；同起点membership覆盖、snapshot压缩/restart不丢清理目标。完整target/tombstone后普通logical success；delete-applied cursor/rejoin不等drain/compaction/free，屏障和physical结果独立 | [RFC-0001 §6.3/9.1](../RFC-0001-profile-capability-install.md)、[RFC-0004 §10—14](../RFC-0004-range-recovery-delete.md)、A9/A10/A16/A28、B4/B6、[Spike C D-RACE/D-LOGICAL/D-ACCESS/CD-REUSE](../spikes/SPIKE-C-no-object-tla.md) |
| UW-2 普通Add与identity | 保留CRC32C、读路径分层、逐entry结果及unknown/重试身份；创建/安装缓存normal/recovery/read response/Segment四项最小maxPayload，首次写入执行统一限制。N-1/N/N+1和44-byte区间闭合，runtime新写阈值降低不损害旧entry读/恢复；与CLIENT-1整体gate及完整status分类衔接 | [RFC-0001 §8/9/11](../RFC-0001-profile-capability-install.md)、[RFC-0005 §6.1/8/10.1](../RFC-0005-segment-bookie-state.md)、A26/A27/A29/B18/B19 |
| UW-3 最小真实写入路径 | ByteBuf、固定shard/shared block及预分配；统一大小/权限校验→完整write/barrier→物理连续前缀→有界热定位→逐entry成功，索引异步接管不等flush。共享文件的恢复按stream/generation/sequence判定，offset不定义跨stream后缀；DATA预分配总量服从filesystem维护预算 | RFC-0003 §6.1/7/9/13/15、RFC-0005 §5.2/6.1/10.1、[Spike B B2/B9/B16/B18](../spikes/SPIKE-B-allocator-block.md) |
| UW-4 allocator恢复与复用 | 三类tail及成功DATA/move可发现；S0不完整尾部不得缩短文件、覆盖或忽略高offset的S1成功DATA。generation 7→8后未写/部分写crash，旧合法header/CRC不复活，unused须有当前authority/依赖证明，不靠整块清零。保留I/O unknown隔离、pool/selector/pin及conditional FREE/bump | [RFC-0003 §5.4/6/9/10/12](../RFC-0003-segment-storage-allocator.md)、B2/B3/B6/B8/B9/B10/B19、C-TAIL/C-REUSE |
| UW-5 空间耗尽与进展 | 每Arena内部维护块与Bookie按实际filesystem去重的磁盘预算分开；限制DATA预分配/增长，为SST/flush/compaction、控制rotation/checkpoint及必要诊断保留并发维护空间。free slot不等于filesystem available，低水位先限制新DATA，维护有份额；内部FREE不自动解除文件系统压力 | RFC-0003 §13/13.1/15、B17/B18、C-SPACE；无每次删除hole punch/文件收缩前置 |
| UW-6 最小集群可恢复性 | 实验3/3/2与3/3/3、Bookie故障域F=1；基础point recovery/durable close/restart、fenced-close普通删除；不等待strong-publication、不reset loss window；旧binary/稳定协议依赖保持BLOCK | RFC-0004 §7.5/10/18、RFC-0001 §9.4、Spike C A-POINT/D-LOGICAL |
| UW-7 全量重建与总资源 | 保留纯派生索引关闭WAL、真实persisted coverage及normal/full rebuild；共享文件按完整allocator枚举当前各stream/代际，不漏其他stream或复活旧残留。hot/异步index/memtable/读buffer/pin有界，另记SST/控制维护磁盘峰值，多个Arena不重复预留同一filesystem容量 | RFC-0003 §8/9/12/13/15、RFC-0005 §8/10.1、B2/B3/B9/B11/B12/B17/B18，dedicated仍另验B5/B13 |

实施顺序：

1. **CLIENT-1/2先修**：独立Classic维护补丁与确定性回归，不等待Segment模型。CLIENT-1同时验证旧ACK/故障域残留与多slot整体gate，包括4/3/2先处理write-set外slot、新响应交错及未变ACK保留；CLIENT-2保持最后旧响应回收。仅修改文档不算修复，不借此取得新存储ACK authority。
2. **Block H与Block G并行**：H先交付UW-2/3一个Arena、固定shard和shared block/共享DATA文件的隔离切片，再加入UW-4/5本地安全回收；dedicated extent不作为该切片前置或性能结论。G只展开实际启用路径的A/C及必要D/组合，保留`E>W`、response loss、offline/rejoin、generation reuse和逐CAS/local durability边界。两者各锁source/config和独立证据，不等待全部延期增强模型；模型不能证明真实fsync/CPU，性能run也不能证明quorum安全。
3. **先得到可解释的测量**：固定硬件、stream/file/filesystem映射、durability、TLS范围、E/W/A及负载，保留原写入/点读/重建/回收、index coverage/拒绝/retry/replacement与activation留存指标。另分列Arena reusable、filesystem available、derived-index disk bytes、maintenance temporary-space peak，以及换组保留ACK/必要resend和复用写放大。统一N和各路径开销、预分配/并发维护/低水位由同一manifest事先冻结；局部无网络结果不作为端到端增益，未运行范围保持NOT_EXECUTED，不新建平台。
4. **集群入口补足安全/资源证据**：UW-7全量重建、UW-3多Arena及UW-4/5故障/进展等实际所需Gate闭合后，推进UW-6。基础point recovery及普通删除不能随高级Range延期；缺少相关证据的实验仍只能用discardable数据，完整Gate和canary证据不得后补。

首批不扩大为强访问撤销、strong completion/loss reset、online delete、general streaming Range/TailSummary/BatchRecoveryAdd、rack/AZ多策略或cross-Arena迁移。普通删除不等待所有历史target在线；强撤权须独立全目标屏障。strong reset后续仅限整个ledger fenced+CLOSED，基础close/membership不创建pendingPublication、不自动续期loss budget；旧token仍按原协议解析，不得强删。每项后续能力有实际需求和独立证据后再启用。same-scope旧binary和released-decoder矩阵保持原BLOCK/DEFERRED，本清单不是执行或生产授权回执。

本轮只补既有CLIENT-1与UW-2/3/4/5/7、Spike A/B/C，不重新编号。六组最小场景：双slot替换先处理write-set外slot不提前callback；未变副本保留有效ACK；共享文件S0低offset不完整而S1高offset已成功；generation bump/重分配后未写crash仍见旧合法CRC；entry跨normal/recovery大小差异的统一N边界；Arena有free slot但filesystem不足。首批一Arena、固定shard/shared block和少量存储E/W/A配置保持；4/3/2属于CLIENT-1一般E>W兼容/模型回归，不据此扩大首批存储性能结论。优先补CLIENT-1与统一大小，再在同一原型验证共享文件恢复及双层预算；不新增ledger状态、全局sequencer、entry分片、逐entry成功日志、在线lease或集群空间协调器，文件收缩另行延期。

`manifest.json`中的`sourceCommit`、`designInputsAtSourceCommit`和各模块receipt继续绑定原历史源码，不能改成当前文档hash来伪造新证据。`plannedWork`只导航本清单；正式实施时必须以新的source/config/run identity记录本轮合同验证，旧12/17项reference测试、wire corpus和codec clone oracle不证明上述协议已经实现。

CLIENT-1/2是单独列出的Classic修复计划，不纳入Wave 0历史scope/receipt；该计划不豁免Segment安全门槛。Wave 0仍禁止触碰：

- `BookieImpl` 普通 Add 成功路径；
- 通过`PendingAddOp`接入Profile/Segment ACK决策；上述独立Classic缺陷修复不借Wave 0证据取得authority；
- `LedgerStorage`、`Journal`、`EntryLog` 的生产 authority 或格式；
- 现有 `bookie-rpc` fallback；
- 生产 Cookie、registration、AutoRecovery 和 delete；
- 任何真实 producer 的 ACK authority。
