# Profile Wire Compatibility Harness

本目录保留 Wave 0 Block D 的 released-decoder/stock-binary probe、artifact lock 与源代码。2026-10-13 按完整16步持续目标恢复了隔离验证；当前四版本665-vector（含完整实验control请求）decoder、loopback stock网络与内部effect观测均通过，状态为 `PASS_SCOPED`。先前610/611-vector矩阵保留原身份。当前加入两个实际control codec的9个完整请求及45个magic/truncation变体；四版本released resolver协议选择也已通过纯逻辑检查。完整G1仍为 `BLOCKED_UNVERIFIED`：固定corpus与Classic Interleaved backend结果不代表mixed rolling versions/auth/semantic/startup及general stable control-body接受完成。

本harness仍不属于常规Maven reactor或CI。候选、原probe Oracle和冻结反例未修改；现有生成器补齐RECOVERY_TERMINAL_CLOSE并增加实际control codec完整请求后当前corpus为665个，原611个raw bytes保持一致。每次执行使用fresh run identity、私有loopback端口/ZooKeeper namespace/可丢弃数据目录；stock classpath只来自锁定released artifact，不包含当前checkout的classes。

本次源提交 `ea98f8b20bebcf639c3ddf07effdeae2aebfd7e3`，run `uw-stock-compatibility-Mftgib2k`，证据目录 `/Users/liusinan/.codex/artifacts/unified-wal/stock-compatibility-evidence-ajegDRwU` 保存636个原始source/corpus输入、全部classpath JAR SHA256、server JAR SHA256/SHA512、Zulu17.0.6 executable SHA、class-load来源、逐vector JSONL、命令/退出码、配置、namespace/storage前后快照、进程终态、原始失败诊断及本次辅助脚本。`verified-results.json`汇总已逐项核验的范围，`raw-sha256.json`锁定原始输出。原冻结反例在全部四个decoder上各识别一个 `ParsedAddRequest`、exit1，作为预期失败控制。

后续源父提交 `55388d6da00d9662cfaa890ede2dd24abde64769`，run `uw-stock-effect-611-oc3r_rd3`；新observer/生成资源以 `stock-effect-evidence-aakgx616/stage3-preflight.json` 的639个输入SHA绑定，证据目录 `/Users/liusinan/.codex/artifacts/unified-wal/stock-effect-evidence-aakgx616`。四个decoder各611项PASS、原反例各预期FAIL；真实stock各611连接、零响应、零内部effect、文件和namespace快照相同，前后Classic sentinel正常。当前Profile五类37项通过；此前三项corpus失败证明生成器要求新增subtype而committed corpus缺失，修复仅从现有生成器补资源，没有弱化测试。第一次observer Checkstyle发现default-package文档缺失；新增 `package-info.java` 后定向9.3检查两文件均零违规。两处行折叠的observer编译bytecode与被测stage2逐项一致，最终611矩阵又使用最终编译版本。

`StockEffectObserver`使用JDK17自带JDI；只允许连接本run拥有的loopback JDWP端口。以 `-agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=127.0.0.1:<private-port>` 启动真实Main，外部observer注册class-prepare与方法入口断点，随后恢复VM；不替换类、不mock route/store、不访问参数中的credential/payload。4.14.8观测Bookie，后三版观测BookieImpl；记录精确signature覆盖processRequest、read/write HandleFactory、descriptor/FileInfo构造、Interleaved/LedgerCache master-key、Bookie/Interleaved Add及Journal logAddEntry。前后正常写/读必须实际触发这些路径，才能把corpus阶段零计数作为证据。

```bash
"$UW_COMPAT_JDK/bin/javac" --add-modules jdk.jdi -d "$UW_COMPAT_RUN/observer-classes" \
  tests/profile-wire-compatibility/src/StockEffectObserver.java \
  tests/profile-wire-compatibility/src/package-info.java
"$UW_COMPAT_JDK/bin/java" --add-modules jdk.jdi \
  -cp "$UW_COMPAT_RUN/observer-classes" StockEffectObserver \
  "$UW_COMPAT_JDWP_PORT" "$UW_COMPAT_RUN/effect-events.jsonl"
```

observer标准输入接受 `phase:<unique-name>`、`snapshot`、`finish`；每条返回一行JSON。phase cut会暂停VM、消费已排队debug事件，并拒绝跨越尚存channel context；channelActive/channelInactive的context identity配对另记，不能混入Classic effect。driver保持corpus阶段直到611 opened/611 closed和零尚存context，随后才开始控制请求；前后正向effect hits分别为4.14/4.15的18/21和4.16/4.17的17/20。`finish`也拒绝尚存连接，异常/VM终止不可计PASS。该JDI运行只验证行为，暂停/断点开销不用于性能测量。run-owned完整driver/helper/config/命令与原始阶段tar、SHA在证据中；前两个610阶段分别验证方法effect及补强channel drain，最后611阶段同时重跑decoder、冻结控制和完整网络/effect组合。

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

当前665-vector run为 `uw-control-compat-4grpqg8o`，父提交 `365dcb802e0bcea1e8f4be64f40c86f480ec5a08`，证据 `/Users/liusinan/.codex/artifacts/unified-wal/control-compat-evidence-yr6r7iu4`。新增control frames用client当前两个codec编码，由对应decoder及round-trip验证；每frame的四个magic flip和末尾truncation也进入同一fixtures.tsv。原611 binary、probe Oracle与冻结反例不改，历史610/611 run不重绑定。七类47项当前Profile测试通过；四版665-vector stock decoder/network/JDI/native快照验证均PASS，channel665/665/zero remaining；原反例各expected FAIL。

新resolver检查使用对应released DefaultBookieAddressResolver和标准service-info future fixture，四版各4案例通过：endpoint两种排序只选bookie-rpc；known Profile-only的opaque/legacy-shaped ID均拒绝。它验证pure protocol selection，registration I/O/publication/advertisement不在该probe范围。全部JAR/JDK/class-load/SHA、实际命令/config/退出码/原始输出在上述证据；resolver与stock有独立input身份。

复现resolver：以对应released classpath编译StockResolverProbe.java，并以该class目录加released classpath运行 `StockResolverProbe <version> <fresh-summary.json>`。JDK仍为锁定Zulu17.0.6；不要添加当前checkout target/classes。实际完整compile/run命令和class-load来源见run归档。


## Released SDK to current Classic

2026-10-28增加独立的 `src/StockClassicClientProbe.java` 与 `current/src/CurrentClassicEpisodeProbe.java`。current-only helper单独放置，保留原released `src/*.java`编译入口；旧Oracle、observer、corpus及lock不改。实际四版12进程、current健康同roots重启及JDI正向effect已通过，当前服务端binary绑定28e4c48138；完整G1仍BLOCKED_UNVERIFIED，范围与原始证据由[当前Classic互操作记录](../../docs/rfcs/unified-wal/implementation/profile-wire/README.md#released-sdk-与当前-classic-bookie-的互操作)拥有。

```text
CurrentClassicEpisodeProbe <fresh|existing> <root> <metadata-uri> <port> <nonce>
StockClassicClientProbe <version> <create|read> <metadata-uri> <receipt.json> [ledger-id]
```

fresh仅接受不存在的私有root和matching `/uw-classic-interop-<nonce>` namespace，使用非强制format；existing要求前episode健康close、PID已退出且cookie/roots/URI/BookieId匹配，不format。服务端stdin依次接受 `CLOSE <nonce>`、`EXIT <nonce>`，二者分别验证。客户端运行必须仅含对应锁定released classpath与其独立helper classes；current server只使用冻结的当前classpath。复现脚本、实际完整javac/java命令、JDK/JAR锁及资源来源在上述run的 `run.py`、`inputs/manifest.json` 和各process receipt；复现必须生成新的run/namespace/port身份，不能覆盖已有结果。


## Current Profile client to released Classic

`current/src/CurrentProfileHandshakeProbe.java`通过公开CONTROL/DATA client入口检查实际连接，不修改生产类。仅`positive`模式创建一个只接HELLO的真实mTLS peer；`reject`模式必须由fresh隔离driver提供其实际stock端口。反射只观察并排空真实失败连接owner，不替换TLS、store或transport。原消息/promise继续沿原pipeline传递。

```text
CurrentProfileHandshakeProbe <positive|reject> <port> <cert-root> <fresh-receipt.json>
```

positive要求port=0、使用声明的测试证书；reject要求本run拥有的loopback端口。新helper单独用current冻结classpath编译，不加入released编译目录或常规reactor/CI。四版实际负向路径、前后Classic控制、JDI零effect、native快照与完整命令由[当前拒绝记录](../../docs/rfcs/unified-wal/implementation/profile-wire/README.md#实际-profile-client-对-released-classic-的拒绝)拥有；完整G1保持BLOCKED_UNVERIFIED。


## Actual mixed initial E

`current/src/CurrentProfileMixedCreateProbe.java`在standalone current JVM中复用原真实SDK/file/ZK/mTLS Fixture，driver再启动对应released Classic Main与原observer。该实验留在reactor/CI之外，不要求常规Java测试自行下载或启动released binary。

```text
CurrentProfileMixedCreateProbe <nonce> <fresh-receipt-root> [missing|misdirected]
```

helper依次接受matching `PREPARE <nonce> <owned-old-BookieId>`、`REJECT <nonce>`、`POSITIVE <nonce>`、`EXIT <nonce>`；driver必须证明其私有ZK/Bookie/JDWP端口由相应PID拥有，并在EXIT前终止旧服务。17个冻结项目JAR前置四个精确测试证书的file-resource目录，供原fixture的file URL读取；helper/fixture字节码未替换。完整命令、原始输入与四版本scope由[真实mixed E记录](../../docs/rfcs/unified-wal/implementation/profile-wire/README.md#真实-mixed-e-的公开-sdk-创建拒绝)拥有，完整G1保持BLOCKED_UNVERIFIED。

默认`missing`保持第三个E为旧Classic身份、缺少Profile readiness的原模式。`misdirected`保留第三个当前Profile身份，由原生publisher发布完整readiness/service-info/writable，但将其endpoint指向driver拥有的旧Classic端口；拒绝后由原publisher公开`demote`并以新nonce发布正确endpoint，再执行同一fixture的公开create/Add/close/只读控制。四版误指场景与一版默认模式回归的独立源码/run绑定由[误指hint记录](../../docs/rfcs/unified-wal/implementation/profile-wire/README.md#误指旧-endpoint-的真实-profile-hint-拒绝)拥有；这些发现输入不构成Main/commissioning authority。
