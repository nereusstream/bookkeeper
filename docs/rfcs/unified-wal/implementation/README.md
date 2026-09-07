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

2026-09-08收敛修订；规划基线`63ff8e0876dcd7bbb229ff0712891b06c0182d06`。以下工作全部为**PLANNED / NOT EXECUTED**，没有新增修复、存储写入路径、JUnit/正式run或PASS receipt。本轮将读可见性/物理候选分层、逐entry与batch结果分离、已提交坐标及I/O异常退出、独立record点读、shared-only首批布局与stream/file映射合入现有owner RFC。此前创建/激活、CRC32C、DATA冻结/恢复前缀、delete-applied与全路径预算保持。客户端维护及reference codec oracle安排不变，尚未实现不作为文档阶段的新缺陷。

| 工作项 | 实施与完成条件 | Owner / 验证入口 |
| --- | --- | --- |
| CLIENT-1 先修ACK状态 | 现有PendingAddOp换组同步撤销旧Bookie成功；数量和启用policy覆盖共同重算completed，优先同一有效ACK事实派生；兼顾timeout统计与关闭policy成本 | [RFC-0001 §9.5](../RFC-0001-profile-capability-install.md)、[Spike A A29](../spikes/SPIKE-A-profile-install.md) |
| CLIENT-2 先修对象回收 | 旧地址响应分支补安全maybeRecycle；最后旧响应/当前响应/callback未完成均不遗漏、提前或重复回收 | RFC-0001 §9.5、Spike A A30 |
| UW-1 普通删除竞争 | 保留admission/freeze、完整targets与authoritative tombstone的普通logical success；本地durable tombstone及可重建清理义务后推进delete-applied cursor，rejoin不等drain/compaction/free；共享L1/L2删除可连续应用，访问屏障和physical结果独立 | [RFC-0004 §10—14](../RFC-0004-range-recovery-delete.md)、Spike A A28、B4/B6、[Spike C D-LOGICAL/D-ACCESS/CD-REUSE](../spikes/SPIKE-C-no-object-tla.md) |
| UW-2 普通Add与identity | 保留初始writer激活与CRC32C/逻辑identity合同；客户端confirmed范围、Bookie物理点读、恢复候选分层，local LAC不截断合法点读；同批逐entry权限/结果独立，提交后timeout不抹除坐标，占位解析前不同payload不成为winner | [RFC-0001 §5.4/6.4/9](../RFC-0001-profile-capability-install.md)、[RFC-0005 §6.1/8](../RFC-0005-segment-bookie-state.md)、RFC-0004 §7.5、A16/A26/A27/B19 |
| UW-3 最小真实写入路径 | ByteBuf、固定shard、预分配和shared DATA block；冻结→完整write/覆盖barrier→物理连续前缀→逐entry检查/定位/成功；非空封包才分配sequence；共享DATA文件映射与barrier成本明确，record外层身份可独立校验；真实checkpoint/restart | RFC-0003 §6.1/7/9、RFC-0005 §5.2/10.1、[Spike B B2/B9/B16/B18](../spikes/SPIKE-B-allocator-block.md) |
| UW-4 allocator恢复与复用 | 三类tail及已成功DATA/move目标可发现；写错误/unknown暂停受影响stream/file，必要时升级既有non-writable/quarantine，不以跳号/新stream/重复sync成功绕过；I/O终结与坐标保护交接后才释放资源，保留pool/selector/pin/reuse规则 | [RFC-0003 §5.4/6/10](../RFC-0003-segment-storage-allocator.md)、Spike B B2/B3/B6/B8/B9/B10/B19 |
| UW-5 空间耗尽与进展 | 每Arena维护保留预算、限流/拒绝/恢复、维护调度份额；DATA满额仍保留fence/tombstone处理和控制持久化容量；有界live set下长期debt稳定并按deadline排空 | RFC-0003 §13/13.1、Spike B B17/B18 / C C-SPACE |
| UW-6 最小集群可恢复性 | 实验3/3/2与3/3/3、Bookie故障域F=1；基础point recovery/durable close/restart、fenced-close普通删除；不等待strong-publication、不reset loss window；旧binary/稳定协议依赖保持BLOCK | RFC-0004 §7.5/10/18、RFC-0001 §9.4、Spike C A-POINT/D-LOGICAL |
| UW-7 全量重建与总资源 | normal/full-index-loss分别覆盖全部已启用live布局；全阶段预算继续包含异常pending、点读I/O/cache、共享父buffer/小副本和pin；小entry按必要对齐范围独立读，坏length不放大分配，慢读者不无界占用；dedicated启用后池/文件/线程另计 | RFC-0003 §8/9/13/15、RFC-0005 §8/10.1、Spike B B9/B11/B12/B18，dedicated另验B5/B13 |

实施顺序：

1. **CLIENT-1/2先修**：独立Classic维护补丁与确定性回归，不等待Segment模型；不借此修改新存储ACK authority。回归复现旧ACK残留、数量够但域不足和最后旧响应回收，不能仅以代码审查当作修复完成。
2. **Block H与Block G并行**：H先交付UW-2/3一个Arena、固定shard和shared block/共享DATA文件的隔离切片，再加入UW-4/5本地安全回收；dedicated extent不作为该切片前置或性能结论。G只展开实际启用路径的A/C及必要D/组合，保留`E>W`、response loss、offline/rejoin、generation reuse和逐CAS/local durability边界。两者各锁source/config和独立证据，不等待全部延期增强模型；模型不能证明真实fsync/CPU，性能run也不能证明quorum安全。
3. **先得到可解释的测量**：固定硬件、stream/file映射、durability、TLS范围、E/W/A及负载，分别测低负载延迟、目标吞吐/尾延迟、过载有界拒绝、写入与回收并行、重启/故障恢复。记录allocation/copy bytes/entry、CPU/entry、按文件的barrier数量/覆盖/等待、padding和全部写放大、全阶段资源峰值及debt；点读另记实际读取bytes/返回bytes、I/O数、cache hit、父buffer与pin时长。write/durability/prefix/locator等待分列，不把共享文件成本当作shard独立flush，也不将其统称NVMe延迟。局部无网络结果不作为端到端增益，未运行的replacement/dedicated等分别标NOT_EXECUTED/未覆盖，不新建压测平台。
4. **集群入口补足安全/资源证据**：UW-7全量重建、UW-3多Arena及UW-4/5故障/进展等实际所需Gate闭合后，推进UW-6。基础point recovery及普通删除不能随高级Range延期；缺少相关证据的实验仍只能用discardable数据，完整Gate和canary证据不得后补。

首批不扩大为强访问撤销、strong completion/loss reset、online delete、general streaming Range/TailSummary/BatchRecoveryAdd、rack/AZ多策略或cross-Arena迁移。普通删除不等待所有历史target在线；强撤权须独立全目标屏障。strong reset后续仅限整个ledger fenced+CLOSED，基础close/membership不创建pendingPublication、不自动续期loss budget；旧token仍按原协议解析，不得强删。每项后续能力有实际需求和独立证据后再启用。same-scope旧binary和released-decoder矩阵保持原BLOCK/DEFERRED，本清单不是执行或生产授权回执。

本轮只补既有UW-2/3/4/7和Spike A/B/C，工作项不重新编号。六组最小场景为：LAC 99/候选100；同批L1 fence/L2合法；K/X已提交timeout再到K/Y；batch 11 unknown/12完成；大batch中小entry独立点读；大量hot ledger/慢reader的有界buffer与pin。复用现有batch table、pending、locator及恢复流程，不新增控制层、在线补洞/跨设备迁移、flush调度框架或逐entry成功日志。首批一Arena及少量E/W/A/Bookie故障域配置保持，数值阈值仍在同一实验manifest中提前冻结。

`manifest.json`中的`sourceCommit`、`designInputsAtSourceCommit`和各模块receipt继续绑定原历史源码，不能改成当前文档hash来伪造新证据。`plannedWork`只导航本清单；正式实施时必须以新的source/config/run identity记录本轮合同验证，旧12/17项reference测试、wire corpus和codec clone oracle不证明上述协议已经实现。

CLIENT-1/2是单独列出的Classic修复计划，不纳入Wave 0历史scope/receipt；该计划不豁免Segment安全门槛。Wave 0仍禁止触碰：

- `BookieImpl` 普通 Add 成功路径；
- 通过`PendingAddOp`接入Profile/Segment ACK决策；上述独立Classic缺陷修复不借Wave 0证据取得authority；
- `LedgerStorage`、`Journal`、`EntryLog` 的生产 authority 或格式；
- 现有 `bookie-rpc` fallback；
- 生产 Cookie、registration、AutoRecovery 和 delete；
- 任何真实 producer 的 ACK authority。
