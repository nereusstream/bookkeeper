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

2026-09-07修订；规划基线`030c6d75013acb72a0b530149b495f3b3a7f6efe`。以下工作全部为**PLANNED / NOT EXECUTED**，没有新增实现、正式run或PASS receipt。安全要求、候选算法和验收条件直接写入现有owner RFC，不建立另一套设计基线。

| 工作项 | 实施与完成条件 | Owner / 验证入口 |
| --- | --- | --- |
| UW-1 删除竞争协议 | 关闭admission、标准metadata同记录freeze、domain prepare/lifecycle publish/resolve、已有handle/grant访问屏障；逐cut crash/response loss可恢复 | [RFC-0004 §10/14](../RFC-0004-range-recovery-delete.md)、[Spike A A28](../spikes/SPIKE-A-profile-install.md)、[Spike C D-PUBLISH/D-ACCESS](../spikes/SPIKE-C-no-object-tla.md) |
| UW-2 普通Add闭环 | 同坐标去重/冲突、DATA后readable publication、unknown后正式换组、旧ACK排除、实际ACK故障域及连续前缀 | [RFC-0001 §9.3/9.4](../RFC-0001-profile-capability-install.md)、[RFC-0005 §6.1](../RFC-0005-segment-bookie-state.md)、Spike A A27 / B B2 |
| UW-3 真实本地控制存储 | 独立Bookie控制日志+A/B checkpoint；多Arena部分durability与restart不扩张权限，无normal Add控制fsync/重复DATA写 | [RFC-0005 §5.2](../RFC-0005-segment-bookie-state.md)、[Spike B B16](../spikes/SPIKE-B-allocator-block.md) |
| UW-4 allocator恢复与复用 | 三类tail oracle、pool/shard ownership、old writer I/O终结/隔离、原子selector/pin cut与generation reuse | [RFC-0003 §5.4/6/10](../RFC-0003-segment-storage-allocator.md)、Spike B B3/B6/B8/B10 |
| UW-5 空间耗尽与进展 | 每Arena维护保留预算、限流/拒绝/恢复、维护调度份额；有界live set下长期debt稳定并按deadline排空 | RFC-0003 §13.1、Spike B B17 / C C-SPACE |
| UW-6 最小集群可恢复性 | 实验3/3/2与3/3/3、Bookie故障域F=1；基础point recovery/durable close/restart重验、fenced-close受限删除及屏障；旧binary/稳定协议依赖保持BLOCK | RFC-0004 §7.5/10/18、RFC-0001 §9.4、Spike C A-POINT/D-ACCESS |
| UW-7 全量重建与总资源 | 正常restart和full-index-loss分开扫描/计量；全部live范围覆盖；route/pool/selector/grant/pending/buffer/checkpoint/index完整账本 | RFC-0003 §13/15、Spike B B9/B11/B16 |

实施顺序：

1. **Block G**先把UW-1/UW-2的候选并发协议及UW-3/UW-4的跨层cut展开为Model A/C/D和A+C/A+D/C+D组合，保留`E>W`、response loss、offline/rejoin和generation reuse。锁定TLC/tool/config，冻结反例与独立receipt；不把多个CAS及local durability假设成一个原子动作。
2. **Block H隔离存储原型**承接UW-2/3/4/5/7，真实control/DATA I/O与独立镜像oracle，先冻结record framing、保留预算和资源/进展阈值。模型不能证明真实fsync/framing，内存reference也不能替代物理实验。
3. 在所需模型、真实存储、兼容和feature gates实际闭合后，推进UW-6最小集群。基础恢复不能随高级Range延期；没有该证据的Add/ACK实验只允许discardable数据。

首批不扩大为online delete、general streaming Range/TailSummary/BatchRecoveryAdd、rack/AZ多策略或cross-Arena迁移；每项后续能力独立Gate。same-scope旧binary以及当前延期的released-decoder矩阵保持原BLOCK/DEFERRED状态，本清单不构成执行这些实验或启用生产路径的回执。

`manifest.json`中的`sourceCommit`、`designInputsAtSourceCommit`和各模块receipt继续绑定原历史源码，不能改成当前文档hash来伪造新证据。新增`plannedWork`只导航本清单；正式实施时必须以新的source/config/run identity记录本轮合同验证，旧12/17项reference测试不证明上述协议已经实现。

Wave 0 禁止触碰：

- `BookieImpl` 普通 Add 成功路径；
- `PendingAddOp` ACK 决策；
- `LedgerStorage`、`Journal`、`EntryLog` 的生产 authority 或格式；
- 现有 `bookie-rpc` fallback；
- 生产 Cookie、registration、AutoRecovery 和 delete；
- 任何真实 producer 的 ACK authority。
