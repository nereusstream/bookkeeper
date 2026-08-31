# Experimental Profile Wire — Wave 0 Block D

> **EXPERIMENTAL_TEST_MANIFEST / NOT_STABLE_WIRE / NON_PROMOTABLE / NO_AUTHORITY / DISCARDABLE**

本目录拥有 Unified WAL Wave 0 Block D 的实现锁、byte corpus、冻结的旧 decoder 反例与保留但不再执行的 compatibility harness。Block D 只实现纯字节 codec；不实现 listener、endpoint、TLS 服务，不接入生产启动、Bookie Add/ACK、storage、registration、recovery 或 delete authority。

当前状态：**REPLACEMENT CORPUS PASS / COMPATIBILITY MATRIX DEFERRED_NOT_RUN — 原 candidate `0x0FFE4250` 的反例已冻结；replacement `0x0FF04250` 未获得 released-decoder/localhost stock-binary 兼容结论，G1保持 `BLOCKED_UNVERIFIED`**。

## Scope guard

从 Block D 起点 `be3d3d55b6787b84698fe428705b398bda57a1de` 检查所有 committed、tracked 和 untracked change：

```bash
scripts/unified-wal/check-wave0-block-d-scope.sh
```

guard 只允许本模块的 pure codec、test corpus、冻结 harness 和 owner documentation；任何 `BookieImpl`、`PendingAddOp`、`LedgerStorage`、`Journal`、`EntryLog`、现有 `bookie-rpc` decoder/fallback、Cookie/registration、AutoRecovery/delete 或生产 authority 路径都会因不在 allowlist 中而 fail closed。路径处于 allowlist 不构成执行 compatibility harness 的授权；当前执行处置见下文。未跟踪的 `BtrLog Low-Latency Logging.pdf` 永久排除，不进入实现或提交。

## Pure codec milestone

`bookkeeper-common` 的内部 `org.apache.bookkeeper.common.profile.wire` 包现已实现：

- `ProfileFrame`、`ProfileFrameHeader`、`ProfileFrameCodec` 的4-byte BE outer length、固定32-byte header、magic/protocol/header/flags/reserved/length严格拒绝和allocation-before-body界限；
- pre-HELLO 4096、control 65536、absolute 5242880三个hard max；
- `ProfileHello` 的byte-exact client/server body、unsigned capability order、UTF-8/NUL/bounds；
- `ProfileStatus` 的12 status class、5 retry disposition、4 durable result；
- 60-byte `ProfileLedgerContext` 和 distinct normal/recovery/read/fence/LAC/force/list data bodies；
- `RANGE_READ`/`BATCH_RECOVERY_ADD` 只接受空request body并产生 typed `UNSUPPORTED`；
- 七个control subtype的未冻结tail明确返回 `BLOCKED_UNFROZEN_CONTROL_BODY`，没有猜测或实现schema。

## Authoritative byte corpus milestone

`bookkeeper-common/src/test/resources/profile-wire/` 冻结保留610个authoritative vectors：259 valid、86 invalid、9 adversarial old-protocol/TLS prefixes、256 fixed-seed fuzz。全部vector共享同一 `fixtures.tsv`，逐项固定phase、frame accept/reject reason、typed decode/status、close结果和byte count；`checksums.sha256` 锁定每个binary、fixture和manifest。

覆盖包括：每个subtype、client/server HELLO、全部240个status/retry/durable组合、normal/recovery distinct bodies、magic逐byte flip、1..31-byte header truncation、body truncation、outer/body mismatch、三个oversize边界、version/header/flags/reserved/subtype、malformed HELLO/status/data、normal/recovery interchange、TLS ClientHello、v3/pre-v3 adversarial prefixes、Profile后接legacy-like ADD prefix，以及seed `0x505746555a5a0001` 的256个arbitrary vectors。strict new-codec corpus test同时断言全部frame reject在body allocation前发生。

replacement `0x0FF04250` 已重建同一组610个vector及全部checksums；新增的确定性回归要求pre-v3 opcode byte `0xf0`、其XOR `0xff`结果`0x0f`及全部single-bit flips均保持在Classic opcode `1..7`之外。本阶段16个wire/corpus单测通过。仓库记录的 stock old Bookie 支持列表为 `4.14.8 / 4.15.5 / 4.16.7 / 4.17.2`，但 replacement 的 released decoder 与 localhost stock binary raw-corpus matrix 明确为 `DEFERRED_NOT_RUN`，没有 raw artifacts、machine receipt 或兼容结论。代码和new-codec corpus完成不等于 wire stable，也不等于 G1 PASS。

## Deferred compatibility execution

当前 Wave 0 不再执行 released-decoder/stock-binary 层：不解析或启动锁定的旧版 BookKeeper artifact，不启动 Bookie/ZooKeeper 进程，不打开 probe socket，也不向旧 decoder/binary 回放610个 raw vectors。`tests/profile-wire-compatibility/`、现有 corpus 与 frozen counterexample 只作为未完成工作的可追溯材料保留，不接入常规 Maven reactor、CI 或后续模块任务。

`DEFERRED_NOT_RUN` 不是 PASS、FAIL 或 waiver；它表示该层没有当前结果。G1 因此保持 `BLOCKED_UNVERIFIED`，candidate 继续 `NOT_STABLE_WIRE / NON_PROMOTABLE / NO_AUTHORITY`。不依赖 G1 的 pure codec、reference implementation 和 isolated/discardable prototype 可以继续，但不得据此接入 production listener、Classic fallback、ACK 或其他 authority。若未来单独恢复兼容认证，必须显式授权、使用新的 run identity 并从零生成证据；不得复用当前延期状态充当结果。

## Frozen counterexample

首个真实 released decoder run 在 Apache BookKeeper `4.14.8` 上发现 [`CE-20260831T105059Z`](counterexamples/ce-20260831T105059Z-4.14.8-magic-flip-1/README.md)：`0x0FFE4250` 的第二个magic byte被corpus逐byte XOR `0xff` 后成为 `0x01`，真实pre-v3 decoder把它识别为`ADDENTRY`并产生`ParsedAddRequest`。因此原候选硬 FAIL，raw bytes/artifact digest/result/repro已冻结。replacement 的G1保持 `BLOCKED_UNVERIFIED`；若未来显式恢复兼容认证，只能保留deterministic regression并用新run identity重跑全部四个真实released decoder和stock binary，不能删除vector、覆盖反例或放宽Oracle。

replacement candidate 的机器锁见 [`candidate.json`](candidate.json)。它只记录当前实验候选、延期执行状态与G1阻塞，不覆盖或重写上述counterexample。
