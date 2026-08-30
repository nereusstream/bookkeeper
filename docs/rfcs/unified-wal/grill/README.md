# Unified WAL P0 Grill 记录

本目录保存 Unified WAL RFC 在进入实现前的逐轮合同审查。

规则：

- 每轮只审查 design tree 当前 frontier；
- reviewer 固定为同一个 `gpt-5.6-sol / max` agent；
- reviewer 的完整反馈原样保存，不用摘要替代；
- `LOCK` 或经收紧后可 `LOCK` 的结论同步进入 Proposed RFC；
- `OPEN`、性能参数和待 Spike 验证项只进入开放问题，不伪装成最终合同；
- 每轮文档独立校验、commit 和 push 后才进入下一轮。

| Round | 主题 | 基线 | 状态 |
| --- | --- | --- | --- |
| [Round 1](ROUND-01-root-contracts.md) | P0 根合同 | `3a476baa6005881e34de3ac876a4f4294487d645` | Reviewed / Integrated |
| [Round 2](ROUND-02-control-plane-authority.md) | Control-plane authority、activation 与 repair | `cc7e03c81571b01b7f9da97a4c9bf1154d743c1c` | Reviewed / Integrated |
| [Round 3](ROUND-03-local-authority-recovery-reset.md) | Local relocation、recovery fallback、delete rejoin 与 repair reset | `4849e7559ad816cbea7ac5e76885138ab12cb9df` | Reviewed / Integrated |
