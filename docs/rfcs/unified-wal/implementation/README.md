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

2026-09-18收敛修订；规划基线`87c346ff031c648747dd8e3126554e9589aeb484`。以下全部为**PLANNED / NOT EXECUTED**，没有新增源码修复、实现/故障测试或PASS receipt。本轮直接修改恢复、framing/completion及网络入口流程：先重演完整控制历史再验证当前必要DATA；持久区分entry与relocation copy，搬迁完成不直接发布普通定位；接收预算覆盖TLS/半包并保留有界控制通路。此前恢复同步、条件命令durable后顺序apply、P/length绑定和hot-first保持，CLIENT-1/2与首批范围不扩大，reference codec oracle与历史证据不重绑。

| 工作项 | 实施与完成条件 | Owner / 验证入口 |
| --- | --- | --- |
| CLIENT-1 ACK状态与切换顺序 | 复用现有completion gate，remote phase结束后继续阻塞callback；全部受影响pending/被替换slot映射及ACK/故障域状态更新、资格重算后才恢复连续回调。保留未变identity/incarnation的有效ACK，只重发必要副本；兼顾原残留集合、timeout统计及policy关闭成本 | [RFC-0001 §9.3/9.5](../RFC-0001-profile-capability-install.md)、[Spike A A29](../spikes/SPIKE-A-profile-install.md)、Model A-ACK-RESP；4/3/2先处理write-set外slot不提前成功 |
| CLIENT-2 先修对象回收 | 旧地址响应分支补安全maybeRecycle；最后旧响应/当前响应/callback未完成均不遗漏、提前或重复回收 | RFC-0001 §9.5、Spike A A30 |
| UW-1 普通删除竞争 | 初始/写期activation authority发布时持久留存target/incarnation，并与delete共享lifecycle cut；同起点membership覆盖、snapshot压缩/restart不丢清理目标。完整target/tombstone后普通logical success；delete-applied cursor/rejoin不等drain/compaction/free，屏障和physical结果独立 | [RFC-0001 §6.3/9.1](../RFC-0001-profile-capability-install.md)、[RFC-0004 §10—14](../RFC-0004-range-recovery-delete.md)、A9/A10/A16/A28、B4/B6、[Spike C D-RACE/D-LOGICAL/D-ACCESS/CD-REUSE](../spikes/SPIKE-C-no-object-tla.md) |
| UW-2 普通Add与identity | 保留CRC32C、统一maxPayload/N边界、读路径分层、逐entry结果及unknown身份，收紧新写不损害旧entry读/恢复。普通/合法恢复Add由内部设置ENTRY_DATA；存储来源不证明ACK，也不能由客户端伪造relocation身份。重启幂等成功仍须有效durability证据或恢复同步，完整status/CLIENT-1 gate保持 | [RFC-0001 §8/9/11.6](../RFC-0001-profile-capability-install.md)、[RFC-0003 §9](../RFC-0003-segment-storage-allocator.md)、[RFC-0005 §6.1/8/10.1](../RFC-0005-segment-bookie-state.md)、A26/A27/A29/B2/B9/B18/B19 |
| UW-3 最小真实写入路径 | 保留固定shard/shared block/预分配及entry的barrier→物理前缀→热定位→成功。framing持久区分ENTRY_DATA/RELOCATION_COPY及move/target身份；同一I/O的copy完成只进内部MOVE路径，不直接发布普通定位、不覆写提交位。网络固定header后预留额度再收大body；DATA准入仍保障提交/定位工作集，Arena命令仍合批durable后顺序apply | RFC-0001 §11.6、RFC-0003 §5.3/6.1/9/15、RFC-0005 §5.2/6.1/10.1、[Spike B B2/B3/B9/B10/B16/B18](../spikes/SPIKE-B-allocator-block.md) |
| UW-4 allocator恢复与复用 | 先验证checkpoint/完整后缀并重演metadata predicate，再验证最终selector及未完成操作仍需DATA；合法退出的历史MOVE副本不再要求存在，不因日志完整就判所有命令成功。必要恢复同步后开放服务，C坏/后缀缺失仍隔离且不回退旧位置。FREE前受保护回收/gate、applied checkpoint cut及原共享文件/reuse/compaction合同保持，不强制每FREE一次checkpoint | [RFC-0003 §5.3/10—12](../RFC-0003-segment-storage-allocator.md)、B2/B3/B4/B6/B7/B9/B10/B16、C-CKPT/C-COND/C-TAIL/C-REUSE/C-MOVE、AC-LOCAL-STORE |
| UW-5 空间耗尽与进展 | 保留Arena内部/Bookie filesystem双层预算和维护份额；按有界victim组计算释放source减实际destination占用，计framing/padding，跨source复制/group commit可合批。低水位无获益候选时背压，等待真实删除/容量恢复，不反复搬近全live块；不靠hole punch或收缩解围 | RFC-0003 §10/13.1/14/15、B10/B17/B18、C-SPACE；copied bytes与net reusable bytes gained分列 |
| UW-6 最小集群可恢复性 | 实验3/3/2与3/3/3、Bookie故障域F=1；复用Classic write-set/fencing/W-A+1/顺序write-back及逐来源否定条件。保留提示源entryId/LAC/累计length，取得匹配最终P的L后发布并重读核对CLOSED(P,L,context)；空ledger为(-1,0)，缺锚点defer、矛盾conflict，必要时只补边界点读。恢复sync不推导AQ/LAC | [RFC-0004 §7.3—7.5](../RFC-0004-range-recovery-delete.md)、RFC-0005 §6—8、A16/A26/B19、A-POINT/D-LOGICAL |
| UW-7 全量重建与总资源 | 全量重建由最终控制状态及受校验来源选择位置，未发布copy不因index/task丢失提升；保留hot-first/fresh Get与真实persisted coverage。TLS/header/read、半包/已解码未准入请求加入既有总账，交接credit不漏计/永久重复；同Profile listener保留有界独立控制连接及接收/执行份额，DATA暂停仍可达且不豁免认证/总量。原内存/双层磁盘及完成工作集保持 | RFC-0001 §11.6、RFC-0003 §9/12/13/15、RFC-0005 §8/10.1、A26/B2/B3/B9/B10/B11/B12/B17/B18、C-REBUILD，dedicated仍另验B5/B13 |

实施顺序：

1. **CLIENT-1/2先修**：独立Classic维护补丁与确定性回归，不等待Segment模型。CLIENT-1同时验证旧ACK/故障域残留与多slot整体gate，包括4/3/2先处理write-set外slot、新响应交错及未变ACK保留；CLIENT-2保持最后旧响应回收。仅修改文档不算修复，不借此取得新存储ACK authority。
2. **Block H与Block G并行**：H先交付UW-2/3一个Arena、固定shard和shared block/共享DATA文件的隔离切片，再加入UW-4/5本地安全回收；dedicated extent不作为该切片前置或性能结论。G只展开实际启用路径的A/C及必要D/组合，保留`E>W`、response loss、offline/rejoin、generation reuse和逐CAS/local durability边界。两者各锁source/config和独立证据，不等待全部延期增强模型；模型不能证明真实fsync/CPU，性能run也不能证明quorum安全。
3. **先得到可解释的测量**：固定硬件、stream/file/filesystem映射、durability、TLS范围、E/W/A及负载，保留写/读/重建/回收、index coverage、拒绝/retry/replacement、内存与四项磁盘指标；compaction分列copied bytes与扣除destination后的net reusable bytes gained，关联debt/p99。复用同一测量记录恢复sync的文件/次数/等待、control commands/barrier与apply lag、必要恢复边界点读数；满载暂停/恢复下游记录已准入完成及资源归还。统一N、工作集、victim/净收益及维护/预分配上限由同一manifest冻结；未运行范围保持NOT_EXECUTED，不新建平台。
4. **集群入口补足安全/资源证据**：UW-7全量重建、UW-3多Arena及UW-4/5故障/进展等实际所需Gate闭合后，推进UW-6。基础point recovery及普通删除不能随高级Range延期；缺少相关证据的实验仍只能用discardable数据，完整Gate和canary证据不得后补。

首批不扩大为强访问撤销、strong completion/loss reset、online delete、general streaming Range/TailSummary/BatchRecoveryAdd、rack/AZ多策略或cross-Arena迁移。普通删除不等待所有历史target在线；强撤权须独立全目标屏障。strong reset后续仅限整个ledger fenced+CLOSED，基础close/membership不创建pendingPublication、不自动续期loss budget；旧token仍按原协议解析，不得强删。每项后续能力有实际需求和独立证据后再启用。same-scope旧binary和released-decoder矩阵保持原BLOCK/DEFERRED，本清单不是执行或生产授权回执。

本轮归入既有UW-2/3/4/7，CLIENT-1/2与UW-1/5/6保持，不重新编号。B9/B10及C-MOVE/C-REBUILD覆盖A→B→C中A/B复用后无新checkpoint重启、最终C坏/控制缺失，以及copy durable但无成功MOVE时清空索引；B18/A26覆盖多连接半包、ownership交接及DATA暂停时控制可达。先修恢复追读历史副本的冲突，再补持久来源和接收预算；真实framing/TLS/网络由既有原型测量，模型不替代实测。沿现有指标分列当前必要DATA读取、TLS/decoder/半包bytes与receive reserved/held峰值、控制接收/完成等待；原一Arena/固定shard/shared block及E/W/A不变，不新增listener、协议、集中流控服务或逐entry日志。

`manifest.json`中的`sourceCommit`、`designInputsAtSourceCommit`和各模块receipt继续绑定原历史源码，不能改成当前文档hash来伪造新证据。`plannedWork`只导航本清单；正式实施时必须以新的source/config/run identity记录本轮合同验证，旧12/17项reference测试、wire corpus和codec clone oracle不证明上述协议已经实现。

CLIENT-1/2是单独列出的Classic修复计划，不纳入Wave 0历史scope/receipt；该计划不豁免Segment安全门槛。Wave 0仍禁止触碰：

- `BookieImpl` 普通 Add 成功路径；
- 通过`PendingAddOp`接入Profile/Segment ACK决策；上述独立Classic缺陷修复不借Wave 0证据取得authority；
- `LedgerStorage`、`Journal`、`EntryLog` 的生产 authority 或格式；
- 现有 `bookie-rpc` fallback；
- 生产 Cookie、registration、AutoRecovery 和 delete；
- 任何真实 producer 的 ACK authority。
