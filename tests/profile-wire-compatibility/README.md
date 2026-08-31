# Deferred Profile Wire Compatibility Harness

本目录保留 Wave 0 Block D 尚未完成的 released-decoder/stock-binary harness、artifact lock 与源代码，当前状态为 `DEFERRED_NOT_RUN`。

- 不属于常规 Maven reactor 或 CI；
- 当前实现与后续独立模块不得启动这里的 probe、旧版 Bookie/ZooKeeper 或 raw-corpus 回放；
- 保留文件不构成兼容性证据，G1保持 `BLOCKED_UNVERIFIED`；
- 不得删除冻结反例、缩小Oracle或把延期状态解释为PASS；
- 若未来单独显式授权恢复认证，必须使用fresh run identity并重新产生完整、不可变证据。

权威状态与边界见 `docs/rfcs/unified-wal/implementation/profile-wire/README.md` 和 `candidate.json`。
