# Experimental Profile Control/Auth — Wave 0

> **EXPERIMENTAL / NON-PROMOTABLE / NO AUTHORITY / DISCARDABLE**

本目录拥有 Unified WAL Wave 0 的 Profile control/auth interfaces、隔离 `bookie-profile` endpoint reference implementation 与机器回执。typed cold-path implementation 与12项普通功能测试已经完成；本次里程碑之后只生成绑定已提交源码的机器回执并执行最终范围审计。

该模块不得接入生产 Bookie 启动、legacy `bookie-rpc`、普通 Add/ACK、`LedgerStorage`、`Journal`、`EntryLog`、Cookie/registration、AutoRecovery/delete或任何生产authority。released-decoder/stock-binary compatibility matrix继续为`DEFERRED_NOT_RUN`，G1继续为`BLOCKED_UNVERIFIED`，本模块不读取或执行其harness、corpus或旧artifact。

## Scope guard

从本模块基线 `761f65a9b93626afaf16484a8d270df06989f5b3` 检查所有committed、tracked与untracked change：

```bash
scripts/unified-wal/check-wave0-profile-control-scope.sh
```

guard只允许新的`bookkeeper-common` experimental control package、对应测试、本目录和owner documentation。未跟踪的 `BtrLog Low-Latency Logging.pdf` 永久排除。

## Typed reference boundary

实现位于 `org.apache.bookkeeper.common.profile.control`，只提供以下语义边界：

- `ProfilePrincipal`、`ProfileTransportContext`、`ProfileControlOperation`与immutable `ProfileControlScope`；
- `ProfileStaticOperationAuthorizer`与exact `ProfileControlAuthorizer`；
- 由ledger/instance/purpose/domain派生、caller不能指定任意backend path的`ProfileAuthorityKey`；
- one-shot cold `ProfileControlStore.read(...)`与strict `CommittedProfileAuthority`；
- fixed kind-1/20-byte、constant-time compare、无raw accessor且所有public text固定脱敏的`ProtectedProfileCredential`；
- physical owner/record packing仍BLOCK的`ProtectedProfileStateStore`语义接口；
- 消费已完成transport facts的`IsolatedProfileControlEndpoint`与secret-free result。

endpoint顺序固定为 immediate TLS1.3/mTLS事实检查 → static operation precheck → fixed-key committed authority cold read → exact principal/scope/authority AuthZ → exact tuple/state/Engine/descriptor/credential检查 → conditional local semantic transition → durable-only success。dependency异常、unknown/corrupt authority、tuple冲突和非durable success全部fail closed。

该类不监听socket、不终止TLS、不访问MetadataStore或生产local store，也不拥有route、allocation、journal、entrylog、registration、delete或ACK。normal Add没有任何调用点，因此remote MetadataStore、descriptor/auth hash、KMS、certificate/signature与control fsync增量均为0。

## Ordinary local verification

实现测试覆盖12项：合法INSTALL顺序与single cold read、TLS/mTLS负向、static/exact denial zero effect、authority unavailable/quarantine、exact tuple与purpose/state replay、strict descriptor/Engine、durable-only success、status、credential redaction/constant-time surface、immutable bounds/range scope及dependency fail-closed。

定向命令只编译当前module并直调Surefire，不执行released-decoder/stock-binary matrix：

```bash
mvn -o -pl bookkeeper-common \
  -DskipTests=false \
  -Dtest=IsolatedProfileControlEndpointTest \
  -Dsurefire.rerunFailingTestsCount=0 \
  compiler:compile compiler:testCompile surefire:test
```

## Remaining OPEN/BLOCK

- real listener、TLS provider/config、X509 extraction与trust rotation仍OPEN；
- concrete allowlist/backend path、store adapter和timeout/retry policy仍OPEN；
- physical protected-state owner、crash framing、group commit、at-rest protection与secure deletion仍BLOCK；
- production secret-leak/TLS/auth negative matrix和Spike A A24仍未通过；
- stable control wire、Profile wire compatibility matrix、G1、production integration与任何authority均未解锁。
