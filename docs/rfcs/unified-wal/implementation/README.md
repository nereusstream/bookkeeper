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

2026-09-18收敛修订；规划基线`a861f470c47e12916a3c300d37d2b82131aee290`。以下全部为**PLANNED / NOT EXECUTED**，没有新增源码修复、实现/故障测试或PASS receipt。本轮将四项规则直接合入主流程：恢复候选经有效证据或必要恢复同步取得本地durability；Arena条件命令先合批持久再顺序apply；恢复close绑定同一前缀的P/length；点读先hot、miss后fresh DB Get。此前逐来源恢复否定证据、完成工作集、compaction净收益及Bookie/Arena职责保持，CLIENT-1/2和首批范围不扩大，reference codec oracle与历史证据不重绑。

| 工作项 | 实施与完成条件 | Owner / 验证入口 |
| --- | --- | --- |
| CLIENT-1 ACK状态与切换顺序 | 复用现有completion gate，remote phase结束后继续阻塞callback；全部受影响pending/被替换slot映射及ACK/故障域状态更新、资格重算后才恢复连续回调。保留未变identity/incarnation的有效ACK，只重发必要副本；兼顾原残留集合、timeout统计及policy关闭成本 | [RFC-0001 §9.3/9.5](../RFC-0001-profile-capability-install.md)、[Spike A A29](../spikes/SPIKE-A-profile-install.md)、Model A-ACK-RESP；4/3/2先处理write-set外slot不提前成功 |
| CLIENT-2 先修对象回收 | 旧地址响应分支补安全maybeRecycle；最后旧响应/当前响应/callback未完成均不遗漏、提前或重复回收 | RFC-0001 §9.5、Spike A A30 |
| UW-1 普通删除竞争 | 初始/写期activation authority发布时持久留存target/incarnation，并与delete共享lifecycle cut；同起点membership覆盖、snapshot压缩/restart不丢清理目标。完整target/tombstone后普通logical success；delete-applied cursor/rejoin不等drain/compaction/free，屏障和physical结果独立 | [RFC-0001 §6.3/9.1](../RFC-0001-profile-capability-install.md)、[RFC-0004 §10—14](../RFC-0004-range-recovery-delete.md)、A9/A10/A16/A28、B4/B6、[Spike C D-RACE/D-LOGICAL/D-ACCESS/CD-REUSE](../spikes/SPIKE-C-no-object-tla.md) |
| UW-2 普通Add与identity | 保留CRC32C、读路径分层、逐entry结果及unknown/重试身份；创建/安装缓存normal/recovery/read response/Segment四项最小maxPayload，首次写入执行统一限制。N-1/N/N+1和44-byte区间闭合，runtime新写阈值降低不损害旧entry读/恢复。重启可读候选不直接幂等返回durable success，须有效证据或恢复同步；与CLIENT-1整体gate及完整status分类衔接 | [RFC-0001 §8/9/11](../RFC-0001-profile-capability-install.md)、[RFC-0005 §6.1/8/10.1](../RFC-0005-segment-bookie-state.md)、A26/A27/A29/B2/B18/B19 |
| UW-3 最小真实写入路径 | 保留ByteBuf、固定shard/shared block、预分配及完整write/barrier→物理前缀→热定位→逐entry成功。DATA准入前保障共享提交buffer和定位/完成容量；索引异步接管不等flush，stall先背压新DATA。Arena条件命令按入队→合批durable→顺序原子apply→结果执行，不另写结果日志；Bookie持有route/fence/grant/tombstone | RFC-0003 §5.3/6.1/13/15、RFC-0005 §5.2/10.1、[Spike B B2/B3/B9/B16/B18](../spikes/SPIKE-B-allocator-block.md)；满载完成工作集及同批竞争ALLOC |
| UW-4 allocator恢复与复用 | 先验证完整控制/DATA候选与依赖，无独立durability证据时按文件恢复同步；再开放相关写入/幂等成功，已知错误不凭后来sync清除。checkpoint仅取连续appliedThrough，durable未apply命令按序重放；FREE整单元quiescence gate提交前建立并保持到结果解析。共享文件逻辑尾部、旧generation、冻结source/成组compaction及selector/pin合同保持 | [RFC-0003 §5.3/10—12](../RFC-0003-segment-storage-allocator.md)、B2/B3/B4/B6/B7/B9/B10/B16、C-CKPT/C-COND/C-TAIL/C-REUSE/C-MOVE、AC-LOCAL-STORE |
| UW-5 空间耗尽与进展 | 保留Arena内部/Bookie filesystem双层预算和维护份额；按有界victim组计算释放source减实际destination占用，计framing/padding，跨source复制/group commit可合批。低水位无获益候选时背压，等待真实删除/容量恢复，不反复搬近全live块；不靠hole punch或收缩解围 | RFC-0003 §10/13.1/14/15、B10/B17/B18、C-SPACE；copied bytes与net reusable bytes gained分列 |
| UW-6 最小集群可恢复性 | 实验3/3/2与3/3/3、Bookie故障域F=1；复用Classic write-set/fencing/W-A+1/顺序write-back及逐来源否定条件。保留提示源entryId/LAC/累计length，取得匹配最终P的L后发布并重读核对CLOSED(P,L,context)；空ledger为(-1,0)，缺锚点defer、矛盾conflict，必要时只补边界点读。恢复sync不推导AQ/LAC | [RFC-0004 §7.3—7.5](../RFC-0004-range-recovery-delete.md)、RFC-0005 §6—8、A16/A26/B19、A-POINT/D-LOGICAL |
| UW-7 全量重建与总资源 | 保留纯派生索引WAL关闭、真实persisted coverage与完整重建；索引/readiness不替代DATA sync。点读先hot并保护命中对象，miss后fresh current DB Get，再核对selector/generation/pin；旧snapshot/iterator/negative cache不能裁定不存在，未知覆盖not-ready。hot/async index/memtable/读buffer/pin及提交/完成工作集同一总账；SST/控制磁盘峰值和filesystem去重保持 | RFC-0003 §9/12/13/15、RFC-0005 §8/10.1、B2/B3/B9/B11/B12/B17/B18、C-REBUILD，dedicated仍另验B5/B13 |

实施顺序：

1. **CLIENT-1/2先修**：独立Classic维护补丁与确定性回归，不等待Segment模型。CLIENT-1同时验证旧ACK/故障域残留与多slot整体gate，包括4/3/2先处理write-set外slot、新响应交错及未变ACK保留；CLIENT-2保持最后旧响应回收。仅修改文档不算修复，不借此取得新存储ACK authority。
2. **Block H与Block G并行**：H先交付UW-2/3一个Arena、固定shard和shared block/共享DATA文件的隔离切片，再加入UW-4/5本地安全回收；dedicated extent不作为该切片前置或性能结论。G只展开实际启用路径的A/C及必要D/组合，保留`E>W`、response loss、offline/rejoin、generation reuse和逐CAS/local durability边界。两者各锁source/config和独立证据，不等待全部延期增强模型；模型不能证明真实fsync/CPU，性能run也不能证明quorum安全。
3. **先得到可解释的测量**：固定硬件、stream/file/filesystem映射、durability、TLS范围、E/W/A及负载，保留写/读/重建/回收、index coverage、拒绝/retry/replacement、内存与四项磁盘指标；compaction分列copied bytes与扣除destination后的net reusable bytes gained，关联debt/p99。复用同一测量记录恢复sync的文件/次数/等待、control commands/barrier与apply lag、必要恢复边界点读数；满载暂停/恢复下游记录已准入完成及资源归还。统一N、工作集、victim/净收益及维护/预分配上限由同一manifest冻结；未运行范围保持NOT_EXECUTED，不新建平台。
4. **集群入口补足安全/资源证据**：UW-7全量重建、UW-3多Arena及UW-4/5故障/进展等实际所需Gate闭合后，推进UW-6。基础point recovery及普通删除不能随高级Range延期；缺少相关证据的实验仍只能用discardable数据，完整Gate和canary证据不得后补。

首批不扩大为强访问撤销、strong completion/loss reset、online delete、general streaming Range/TailSummary/BatchRecoveryAdd、rack/AZ多策略或cross-Arena迁移。普通删除不等待所有历史target在线；强撤权须独立全目标屏障。strong reset后续仅限整个ledger fenced+CLOSED，基础close/membership不创建pendingPublication、不自动续期loss budget；旧token仍按原协议解析，不得强删。每项后续能力有实际需求和独立证据后再启用。same-scope旧binary和released-decoder矩阵保持原BLOCK/DEFERRED，本清单不是执行或生产授权回执。

本轮归入既有UW-2/3/4/6/7与Spike A/B/C，CLIENT-1/2及UW-1/5保留，不重新编号。四组场景：write未barrier退出→重启重试→再次掉电；同组竞争ALLOC、apply中途crash及重复FREE；LAC落后/fragment提升/无尾部/空ledger的P-length绑定；hot→DB接管与fresh fallback交错。执行顺序直接归入restart、conditional apply、close与点读主流程，原一Arena/固定shard/shared block及E/W/A范围不变；不新增控制服务、推测回滚状态、跨索引锁、LAC-length持久映射或逐entry日志。

`manifest.json`中的`sourceCommit`、`designInputsAtSourceCommit`和各模块receipt继续绑定原历史源码，不能改成当前文档hash来伪造新证据。`plannedWork`只导航本清单；正式实施时必须以新的source/config/run identity记录本轮合同验证，旧12/17项reference测试、wire corpus和codec clone oracle不证明上述协议已经实现。

CLIENT-1/2是单独列出的Classic修复计划，不纳入Wave 0历史scope/receipt；该计划不豁免Segment安全门槛。Wave 0仍禁止触碰：

- `BookieImpl` 普通 Add 成功路径；
- 通过`PendingAddOp`接入Profile/Segment ACK决策；上述独立Classic缺陷修复不借Wave 0证据取得authority；
- `LedgerStorage`、`Journal`、`EntryLog` 的生产 authority 或格式；
- 现有 `bookie-rpc` fallback；
- 生产 Cookie、registration、AutoRecovery 和 delete；
- 任何真实 producer 的 ACK authority。
