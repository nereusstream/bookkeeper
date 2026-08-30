# Unified WAL P0 Grill 记录

本目录保存 Unified WAL RFC 在进入实现前的逐轮合同审查。

规则：

- 每轮只审查 design tree 当前 frontier；
- reviewer 固定为同一个 `gpt-5.6-sol / max` agent；
- reviewer 的完整反馈原样保存，不用摘要替代；
- `LOCK` 或经收紧后可 `LOCK` 的结论同步进入 Proposed RFC；
- `OPEN`、性能参数和待 Spike 验证项只进入开放问题，不伪装成最终合同；
- 每轮文档独立校验、commit 和 push 后才进入下一轮。
- grill文件是逐字历史归档，可能保留当轮使用但现已废弃的内部代际命名；不得据此恢复项目代际线或覆盖当前owner RFC。
- 当前规范只维护单一持续演进基线；后续改造直接更新总纲、owner RFC、Spike与实现，technical wire discriminator不等于产品版本。

| Round | 主题 | 基线 | 状态 |
| --- | --- | --- | --- |
| [Round 1](ROUND-01-root-contracts.md) | P0 根合同 | `3a476baa6005881e34de3ac876a4f4294487d645` | Reviewed / Integrated |
| [Round 2](ROUND-02-control-plane-authority.md) | Control-plane authority、activation 与 repair | `cc7e03c81571b01b7f9da97a4c9bf1154d743c1c` | Reviewed / Integrated |
| [Round 3](ROUND-03-local-authority-recovery-reset.md) | Local relocation、recovery fallback、delete rejoin 与 repair reset | `4849e7559ad816cbea7ac5e76885138ab12cb9df` | Reviewed / Integrated |
| [Round 4](ROUND-04-bounded-proof-interfaces.md) | Bounded proof、assignment handoff、relocation checkpoint 与 recovery outcome | `dbc0f7fee412e68d1cecd40efbf6d80edfdc8af0` | Reviewed / Integrated |
| [Round 5](ROUND-05-implementation-interfaces.md) | Sidecar、ArenaControlLog、local durable authority 与 recovery compatibility 接口 | `ba820b28babd7683e8dfe6a724d1b8aeecf7c2e1` | Reviewed / Integrated |
| [Round 6](ROUND-06-profile-security-wire-downgrade.md) | Profile canonical/security、wire compatibility 与 format downgrade gate | `1c2fc1b30aa69b55cbfac4116609c4f4e78fb2b5` | Reviewed / Integrated |
| [Round 7](ROUND-07-exact-implementation-manifest.md) | Descriptor/control/wire/format exact implementation manifest 与实施解锁边界 | `9885dbda26f55dbe3c20c530a36b22109d76b7e2` | Reviewed / Integrated |
