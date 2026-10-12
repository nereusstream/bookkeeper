# Profile Wire Compatibility Harness

本目录保留 Wave 0 Block D 的 released-decoder/stock-binary probe、artifact lock 与源代码。2026-10-13 按完整16步持续目标恢复了隔离验证；四个版本的610-vector decoder与loopback stock网络矩阵均通过原probe Oracle，状态为 `PASS_SCOPED`。G1仍为 `BLOCKED_UNVERIFIED`：内部 handle/master-key/allocation effect 尚无直接观测，不能把磁盘和metadata快照不变解释成完整G1通过。

本harness仍不属于常规Maven reactor或CI。候选、610-vector corpus、probe Oracle和冻结反例均未修改。每次执行使用fresh run identity、私有loopback端口/ZooKeeper namespace/可丢弃数据目录；stock classpath只来自锁定released artifact，不包含当前checkout的classes。

本次源提交 `ea98f8b20bebcf639c3ddf07effdeae2aebfd7e3`，run `uw-stock-compatibility-Mftgib2k`，证据目录 `/Users/liusinan/.codex/artifacts/unified-wal/stock-compatibility-evidence-ajegDRwU` 保存636个原始source/corpus输入、全部classpath JAR SHA256、server JAR SHA256/SHA512、Zulu17.0.6 executable SHA、class-load来源、逐vector JSONL、命令/退出码、配置、namespace/storage前后快照、进程终态、原始失败诊断及本次辅助脚本。`verified-results.json`汇总已逐项核验的范围，`raw-sha256.json`锁定原始输出。原冻结反例在全部四个decoder上各识别一个 `ParsedAddRequest`、exit1，作为预期失败控制。

复现decoder层（先建立新的 `UW_COMPAT_RUN`；保留现有输出）：

```bash
UW_COMPAT_RUN=$(mktemp -d /tmp/uw-stock-compatibility-XXXXXXXX)
UW_COMPAT_JDK=/Library/Java/JavaVirtualMachines/zulu-17.jdk/Contents/Home
for UW_COMPAT_VERSION in 4.14.8 4.15.5 4.16.7 4.17.2; do
  mkdir -p "$UW_COMPAT_RUN/$UW_COMPAT_VERSION/classes"
  mvn -o -q -f tests/profile-wire-compatibility/pom.xml \
    -Dbookkeeper.version="$UW_COMPAT_VERSION" dependency:build-classpath \
    -Dmdep.outputFile="$UW_COMPAT_RUN/$UW_COMPAT_VERSION/classpath.txt" -DincludeScope=runtime
  UW_COMPAT_CP=$(cat "$UW_COMPAT_RUN/$UW_COMPAT_VERSION/classpath.txt")
  "$UW_COMPAT_JDK/bin/javac" -cp "$UW_COMPAT_CP" \
    -d "$UW_COMPAT_RUN/$UW_COMPAT_VERSION/classes" tests/profile-wire-compatibility/src/*.java
  "$UW_COMPAT_JDK/bin/java" -Xmx256m -XX:MaxDirectMemorySize=64m \
    -cp "$UW_COMPAT_RUN/$UW_COMPAT_VERSION/classes:$UW_COMPAT_CP" StockDecoderProbe \
    "$UW_COMPAT_VERSION" bookkeeper-common/src/test/resources/profile-wire \
    bookkeeper-common/src/test/resources/profile-wire/fixtures.tsv \
    "$UW_COMPAT_RUN/$UW_COMPAT_VERSION/decoder.jsonl" \
    "$UW_COMPAT_RUN/$UW_COMPAT_VERSION/decoder-summary.json"
done
```

stock层使用真实 `org.apache.bookkeeper.server.Main -c <private-bookie.conf>`，显式Classic `InterleavedLedgerStorage`。历史lock中的 `localbookie 1` 不是Main支持的CLI，本次未执行该参数，也未改写历史lock；实际command/config单独锁定。每版本先启动私有ZooKeeper、确认PID拥有监听端口，再运行 `BookieShell -conf <private-bookie.conf> initnewcluster` 和Main。ZooKeeper server-only metrics/snappy依赖按对应released ZK父POM锁定，仅添加到ZK classpath。不得用共享2181/5000端口或 `/tmp/localbookies-config` 替代本次隔离条件。

在每个健康stock Bookie上先用对应released SDK写入并关闭37-byte Classic sentinel，记录ledger0/LAC0/payload SHA；取得journal/ledger文件及完整私有namespace快照后运行：

```bash
"$UW_COMPAT_JDK/bin/java" -Xmx256m -XX:MaxDirectMemorySize=64m \
  -cp "$UW_COMPAT_RUN/$UW_COMPAT_VERSION/classes:$UW_COMPAT_CP" StockBinaryProbe \
  "$UW_COMPAT_VERSION" 127.0.0.1 "$UW_COMPAT_BOOKIE_PORT" \
  bookkeeper-common/src/test/resources/profile-wire \
  bookkeeper-common/src/test/resources/profile-wire/fixtures.tsv \
  "$UW_COMPAT_RUN/$UW_COMPAT_VERSION/binary.jsonl" \
  "$UW_COMPAT_RUN/$UW_COMPAT_VERSION/binary-summary.json" 200
```

随后比对快照、以released SDK重读sentinel，关闭并核验自己启动的精确PID/端口。`StockBinaryProbe`的 `ackOrOk` 保守计数任何非空响应，PASS同时要求零response bytes、零该计数、零connection failure；200ms read timeout不是成功ACK或实际服务排空证明。辅助源码、实际配置和完整执行命令在上述证据的 `raw/` 中，路径/端口是本次run身份，复现必须重新分配。4.14.8首次ZK缺metrics启动失败、其cluster init失败及其后首次只比对storage的run也已保留；最终matrix另取得namespace快照，不将失败诊断计PASS。部分released client关闭打印worker-pool/session warning，helper exit0和实际子进程终态分别保存，不声称旧SDK已无清理问题。

权威状态与剩余G1边界见 `docs/rfcs/unified-wal/implementation/profile-wire/README.md` 和 `candidate.json`。
