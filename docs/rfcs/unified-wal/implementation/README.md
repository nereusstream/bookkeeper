# Unified WAL Implementation Wave 0

> 状态：**EXPERIMENTAL / NON-PROMOTABLE / NO AUTHORITY / DISCARDABLE**

本目录保存 Unified WAL Implementation Wave 0 的可执行实现边界与机器回执。Wave 0 只把已冻结合同变成 reference implementation、compatibility harness、形式化模型或隔离 prototype；它不授予 stable wire、stable on-disk、live shadow、Segment ACK 或生产注册 authority。

当前已实现的完整模块：

- [`ProfileDescriptor` reference implementation](profile-descriptor/README.md)

已冻结、延期验证的模块：

- [`Experimental Profile wire 与冻结的 old-decoder/stock-binary harness`](profile-wire/README.md)：Block D scope guard、pure codec 与610-vector corpus 已实现；真实4.14.8 decoder发现并冻结原`0x0FFE4250`反例，replacement `0x0FF04250`与authoritative corpus已通过new-codec定向回归。released-decoder/localhost stock-binary矩阵为`DEFERRED_NOT_RUN`，harness与corpus只保留、不进入当前执行，G1保持`BLOCKED_UNVERIFIED`。

正在执行的模块：

- [`Profile control/auth 与隔离 bookie-profile endpoint`](profile-control/README.md)：已建立独立scope guard；typed cold-path interfaces、isolated endpoint tests与机器回执正在执行，不接入生产Bookie或normal Add。

输入锁见 [`manifest.json`](manifest.json)。每个模块自己的 source/test/artifact 范围、复现命令、证据结论和剩余 BLOCK 由模块目录拥有。实现证据不能自动提升五份 owner RFC 或三个 Spike 的状态。

Wave 0 禁止触碰：

- `BookieImpl` 普通 Add 成功路径；
- `PendingAddOp` ACK 决策；
- `LedgerStorage`、`Journal`、`EntryLog` 的生产 authority 或格式；
- 现有 `bookie-rpc` fallback；
- 生产 Cookie、registration、AutoRecovery 和 delete；
- 任何真实 producer 的 ACK authority。
