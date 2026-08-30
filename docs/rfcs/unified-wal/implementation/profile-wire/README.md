# Experimental Profile Wire — Wave 0 Block D

> **EXPERIMENTAL_TEST_MANIFEST / NOT_STABLE_WIRE / NON_PROMOTABLE / NO_AUTHORITY / DISCARDABLE**

本目录拥有 Unified WAL Wave 0 Block D 的实现锁、byte corpus、真实旧 decoder/stock binary 兼容证据与机器回执。Block D 只实现纯字节 codec 和隔离 compatibility harness；不实现 listener、endpoint、TLS 服务，不接入生产启动、Bookie Add/ACK、storage、registration、recovery 或 delete authority。

当前状态：**IN PROGRESS — scope guard installed; codec/corpus/old-binary evidence pending**。

## Scope guard

从 Block D 起点 `be3d3d55b6787b84698fe428705b398bda57a1de` 检查所有 committed、tracked 和 untracked change：

```bash
scripts/unified-wal/check-wave0-block-d-scope.sh
```

guard 只允许本模块的 pure codec、test corpus、隔离 harness 和 owner documentation；任何 `BookieImpl`、`PendingAddOp`、`LedgerStorage`、`Journal`、`EntryLog`、现有 `bookie-rpc` decoder/fallback、Cookie/registration、AutoRecovery/delete 或生产 authority 路径都会因不在 allowlist 中而 fail closed。未跟踪的 `BtrLog Low-Latency Logging.pdf` 永久排除，不进入实现或提交。

代码、corpus、运行锁、raw artifact index、test summary、machine receipt 和 G1 结论会在后续里程碑同步写入本目录。代码完成不等于 wire stable，也不等于 G1 PASS。
