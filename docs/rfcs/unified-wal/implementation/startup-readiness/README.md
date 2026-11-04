# Unified WAL Wave 0 startup/readiness/new-scope reference harness

> 状态：**EXPERIMENTAL / NON-PROMOTABLE / NO AUTHORITY / DISCARDABLE**

本目录记录 Bookie startup/readiness/new-scope reference harness 的实现与机器证据。模块基线为 `df122c524ddc1739ef58e1f1ede3d8140ee0de86`，完成普通本地测试的实现提交为`fa608accc64222c1971bab6aa8fec7eafc749723`。

当前reference implementation已实现：

- strict immutable `StorageIncarnation`、generation、compatibility、required-device/superblock与allocator/route/delete recovery facts；
- `new BookieId + new journal/ledger/index/Arena roots + new incarnation + new credential scope` 的纯语义fallback manifest；
- old credential必须对new scope为`NONE`、旧scope必须drained/readonly/decommissioned、BookieId/incarnation/roots/credential必须隔离的writable eligibility；
- 任一local success、route/install、activation、fence/grant、tombstone、Arena authority或durability unknown时拒绝old-binary same-scope rollback；
- 固定phase顺序的fail-closed `StartupReadinessHarness`：compatibility → required devices → allocator → route → delete → durable local readiness → persistent readiness CAS → matching service-info → ephemeral writable registration；
- domain-specific `ProfileRegistrationStore.read/compareAndSet`与确定性内存adapter，显式分离store version和readiness generation；
- CAS response loss、local durability response loss、service-info/ephemeral response loss均通过重读exact record解析；conflict、unavailable、corrupt、generation/incarnation mismatch和stale ephemeral state均demote/non-writable；
- 九个命名crash边界可重复注入，使用同一内存durable state restart后必须恢复为exact matching writable或保持fail-closed；
- cold-path计数独立归因，normal Add的format/readiness read、remote I/O、hash、TLS、KMS/certificate与control fsync计数固定为0；
- 独立scope guard与17项定向普通测试。

模块状态是`REFERENCE_IMPLEMENTATION_COMPLETE`。不可变source/test Git objects与边界见[`receipt.json`](receipt.json)，运行输入与锁定合同见[`manifest.json`](manifest.json)，17项测试和独立检查见[`test-results/summary.json`](test-results/summary.json)。该结果只完成reference implementation，不能写成Spike B PASS、Gate通过、stable format或production readiness。

该模块只处理调用方提供的typed facts，不读取或修改真实OS权限、Cookie、superblock bytes、storage roots、registration backend或外部目标。same-scope `BKPF1`、真实old binary/startup/file-touch probe、physical format、migration/wipe工具与production integration均不在本模块中。

定向验证命令：

```bash
mvn -o -pl bookkeeper-common \
  -DskipTests=false \
  -Dtest=StartupReadinessFactsTest,StartupReadinessHarnessTest \
  -Dsurefire.rerunFailingTestsCount=0 \
  compiler:compile compiler:testCompile surefire:test

scripts/unified-wal/check-wave0-startup-readiness-scope.sh
```

命令不运行Maven完整lifecycle，不读取或执行延期compatibility harness/corpus/counterexample。

格式、静态和范围检查：

```bash
mvn -o -pl bookkeeper-common -DskipTests spotless:check checkstyle:check
scripts/unified-wal/check-wave0-startup-readiness-scope.sh
git diff --check
```

`bookkeeper-common` module-wide verify与RAT均为`NOT_RUN_BY_DESIGN`，不能写成PASS。released decoder/stock binary、same-scope、真实startup/file-touch、physical format、真实credential/ACL与formal Spike B均保持未运行或BLOCK。


## 步骤 12：真实 ZooKeeper readiness CAS 增量

2026-10-05，新增隔离的 [`ZKProfileRegistrationStore`](../../../../../bookkeeper-server/src/main/java/org/apache/bookkeeper/discover/profile/ZKProfileRegistrationStore.java)，直接实现已有 `ProfileRegistrationStore`，保留 store version 与 semantic readiness generation 的区别。调用方提供并持有 ZooKeeper client、已配置的非根 namespace、ACL 与正的整次操作期限（至多一分钟）；adapter 不关闭 client，不创建线程、重试层或自动注册。它是阻塞冷路径，不能从 ZooKeeper event thread 或 DATA append owner 调用。

每个 Bookie 的路径是 `root/base64url-without-padding(UTF8(bookieId))`，name 保留现有 1～255 byte 上限。父节点为 persistent readiness envelope，创建时用一个原子 multi 同时创建带新 UUID 的空 persistent `birth-UUID` 子节点；子节点仅保留调用方 reader 身份的 READ ACL。ACL 由调用方配置，测试中的 OPEN_ACL_UNSAFE 仅用于管理故障注入。namespace 管理者不得复制旧 birth 或恢复旧锚点来伪造新实例；adapter 自身不删除父节点或锚点、不修改锚点。

读与 CAS 先 `sync(root)`，再取得 fresh data/stat，不用 watch/cache。公开 store version 使用 `Stat.mzxid`；CAS 同时比较它和记录中的 readiness generation，更新提交为 `check(birth-UUID, version=0) + setData(parent, expected dataVersion)`。新实例的 birth 与原子条件一起防止“删除再创建使 dataVersion 回到 0”的 ABA，单靠节点 dataVersion 不足。校验还要求 persistent owner、正确 Bookie scope、非负版本和空/version-0 锚点；dataVersion 达到 Integer.MAX_VALUE 时拒绝更新。原子 multi、版本条件与 ACL 假设见 [ZooKeeper 3.9.5 Programmer's Guide](https://zookeeper.apache.org/doc/r3.9.5/zookeeperProgrammers.html)。

整次操作使用一个 monotonic deadline。未提交时超时为 UNAVAILABLE；提交后的超时、连接故障或中断保持 RESPONSE_LOST，不能推测成功或失败，后续由调用方 fresh-read exact record 解析。已确定的版本/存在性冲突为 CONFLICT，明确 ACL 等拒绝为 UNAVAILABLE。损坏或未知版本不修复、不写 READY；中断保留中断标志。adapter 不持有 local receipt，也不凭一次 CAS 授予服务可写资格。

### 实验 readiness envelope v1

[`ProfileReadinessCodec`](../../../../../bookkeeper-server/src/main/java/org/apache/bookkeeper/discover/profile/ProfileReadinessCodec.java) 使用 big-endian、最大 65,536 byte，总长度为 `148 + nameBytes + 4 * hintCount`。字段顺序如下：

| 字段 | 字节与约束 |
| --- | --- |
| magic / schema / birth | `UWRD` 4、版本 1 的 int32、非零 UUID 16 |
| Bookie scope | int32 UTF-8 byte length，随后 1～255 byte canonical UTF-8 |
| storage / Engine / protocol | incarnation 16、Engine int32（1/2/3）、major/minor 各 int32 |
| capability / local format | 各 int64 generation |
| device manifest | identity 32、manifest generation int64 |
| assignment / local readiness | 各 int64 generation |
| minimum reader / writer | 各 int32 |
| capability hints | int32 count，随后原顺序的正 int32 hints，受完整长度及既有 record 约束 |
| state / checksum | READY=1、NON_WRITABLE=2 的 int32；此前全部字节的 CRC32C int32 |

解码严格拒绝未知 schema/Engine/state、坏 checksum、非法 scope/count 和尾随字节；既有 typed facts 继续验证 generation、manifest 等字段。这个格式只用于上述实验节点，不修改 descriptor/wire、DATA、控制日志或 checkpoint image，也没有长期兼容承诺。

该增量仍为 EXPERIMENTAL / NON-PROMOTABLE。尚未接入真实 Bookie startup、本地恢复/delete catch-up、service-info、ephemeral registration、失连降级、initial install/activation、ProfileControlStore 或 metadata sidecar/page。Wave 0 reference 的 manifest/receipt/test summary 仍冻结在原 source；真实 ZK 增量不能改写它们，也不表示步骤 12 完成。


### 本增量实际验证与边界

最终 Java 运行 `unified-wal-readiness-2owbwpc_` 绑定父提交 `aee4bed4060d6f52097fc113978c462662b314e5` 和 38 个 source inputs：8 项真实 server 测试及原 reference 的 17 项测试，25/25 通过，无失败、错误或跳过；Spotless/Checkstyle 通过。源码摘要与最终实现一致，test XML、完整 Maven/style 日志和 exit code 保存在独立运行目录。复现：

```bash
mvn -o -P '!cargo-zigbuild' -pl bookkeeper-server -am \
  -Dtest=ZKProfileRegistrationStoreTest,StartupReadinessFactsTest,StartupReadinessHarnessTest \
  -Dsurefire.failIfNoSpecifiedTests=false -DtestRetryCount=0 test
mvn -o -P '!cargo-zigbuild' -pl bookkeeper-server -am spotless:check checkstyle:check
python3 scripts/unified-wal/run-storage-models.py --tool /tmp/unified-wal-tla2tools-174.jar
```

真实测试覆盖完整 record roundtrip、store/readiness 双版本、同前驱双 client 单 winner、创建/更新后丢回复、删除重建的版本 ABA、坏 schema/checksum/count/尾随字节、锚点写权限及非法变更、真实 ACL 拒绝、实际提交后回调超时和 fresh-read 原结果。有限 Model A registration 投影检查 67 distinct states，三个指定反例；本次完整模型矩阵 33 正向完整通过、96 指定反例，224 source inputs/388 checksum，详见[模型实际结果](../models/README.md#zookeeper-readiness-cas-投影)。

失败证据保留：`unified-wal-readiness-neClE2oJ` 中 reference 17 通过、server 1 通过/6 setup error，原因是 fixture 绑定 127.0.0.1 而 localhost 被本环境解析到非 loopback；只在新 fixture 明确使用已绑定的 127.0.0.1，不改变系统 DNS 或共享 helper。`unified-wal-readiness-fnhy3ju7` 为 24 通过/1 error，ZooKeeper 的 `setACL` 校验调用 `contains(null)` 与 Java `List.of` 不兼容；改为支持该检查的 Arrays.asList，未弱化 ACL 断言。`unified-wal-readiness-ffsihmh2` 的 Java 25 通过、Checkstyle 8 项失败（新包 package-info、行宽/导入），修正后取得上述最终绑定结果。三者不冒充算法 red/green、后端事务失败或最终全部通过。

历史 Wave 0 `check-wave0-startup-readiness-scope.sh` 本次对累计工作返回 BLOCK（334 个超出冻结 reference allowlist 的路径，含后续已授权的实际存储与新 adapter）；日志在 `unified-wal-readiness-ffsihmh2/wave0-scope.log`。它仍作为原 reference 边界证据保存，没有扩充 allowlist 或改写旧 PASS。这不是本轮当前 scope 的通过记录。原 reference Java/receipt、生产 service 入口和 normal Add 没有改动，当前 adapter 仅被新的实际 ZK 测试构造。

未运行 ZooKeeper quorum 多节点/节点重启、真实网络断开/会话过期、跨进程 registration 生命周期或实际部署；没有持久化故障/掉电、吞吐/性能或生产 readiness 结论。完整 ProfileControlStore、install/activation、local readiness receipt→persistent→matching service-info→ephemeral 链及失连降级仍是步骤 12 的未完成事项。


## 步骤 12：真实 ephemeral 发布增量

2026-10-05，[`ZKProfileEphemeralRegistrationStore`](../../../../../bookkeeper-server/src/main/java/org/apache/bookkeeper/discover/profile/ZKProfileEphemeralRegistrationStore.java) 实现已有 `EphemeralProfileRegistrationStore`，复用持久 adapter 的 scope、fresh snapshot、有界 I/O 与 ACL。每个实例绑定一个 Bookie、已连接的 caller-owned client/session 和随机 nonce。它是一次 cold startup attempt 的发布者；不安装 watcher、不关闭 client、不重连、不自动重试，也不成为普通 Add 的远端门槛。

在已有 persistent `root/base64url(bookieId)` 下创建 `session-UUID`、`service-info` 和 `writable` 三类 ephemeral 子节点；保留原 persistent `birth-UUID`。session marker 是空/version-0/read-only ACL。service-info 首次发布通过 atomic multi 检查 readiness birth/dataVersion，同时创建 marker 与 service-info；必须匹配 READY、incarnation、readiness generation、mzxid 及 capability hints。创建 writable 再检查相同 authority/marker、条件更新原 service-info 的相同字节（递增它的版本），再创建相同 projection 的 writable。这样，先查到“尚无 writable”的 service refresh 与 writable 创建竞争时，版本条件使一方失败，不能把新 info 与旧 writable 混合。

解析同时校验 ephemeral owner、Bookie scope、nonce、marker 与 checksum。read 方法只供该发布者解析自身 attempt，不是跨会话 placement/discovery API；foreign session/nonce、不可用或损坏显式抛异常，不能被当作 absent，更不能让原 harness 的 conflict→exact-read 路径认领另一会话的相同记录。仅明确 NONODE 返回 empty。writable 读还要求当前 persistent READY/reference 与 service-info 一致。

`demote` 先把实例永久置为 retired，再以 session marker 和实际节点版本原子删除 owned writable/info/marker。退休的 nonce 不复用，包括删除回复丢失时；新 attempt 必须新建实例/nonce。旧删除请求在同名新节点都恢复相同版本后仍因原 marker 缺失而失败。demote 未确认时抛异常、local gate 保持关闭；它不保证不可用时远端已经删除。调用方负责停止 local admission 和 client 生命周期。发布的 OK 回调交付后再检查原 session 与 retired gate；真实提交后会话过期、再交付成功回调也只返回 RESPONSE_LOST。连接非 CONNECTED、session ID 改变或过期均拒绝该实例继续发布/返回 writable，server 的实际 session expiration 删除三类 ephemeral 并保留 persistent readiness。

### 实验 ephemeral envelope v1

[`ProfileEphemeralCodec`](../../../../../bookkeeper-server/src/main/java/org/apache/bookkeeper/discover/profile/ProfileEphemeralCodec.java) 使用 big-endian、最大 65,536 byte，总长度 `80 + nameBytes + endpointBytes + 4 * hintCount`：magic `UWEP` int32、schema 1 int32、nonce UUID16、session ID int64、Bookie name int32 byte length/canonical UTF-8、incarnation16、readiness generation int64、persistent mzxid int64、endpoint int32 byte length/canonical UTF-8、hint count int32/原顺序 int32 hints、此前全部字节 CRC32C int32。name 与 endpoint 各 1～255 byte，nonce/session 非零，typed facts 与完整长度继续严格校验；未知 schema、坏 checksum/count、非法 scope 或尾随 bytes 均拒绝。该 schema 仅用于实验 namespace，未改变 Profile wire、descriptor、控制/DATA/image 格式，也不是现有 BookieServiceInfo protobuf。

### 本轮验证

Java `unified-wal-registration-yqio47xw` 绑定父提交 `8e2d6668168e9983a270781d027f714159908c89` 与 42 source inputs：新 publisher 12、原真实 persistent CAS 8、原 reference 17，共 37/37 通过，0 failure/error/skip；Spotless/Checkstyle 通过。两个测试类共享既有真实 server/双 client fixture；故障仅影响 pre-submit 交错或真实提交后的回复，不模拟后端成功。

真实覆盖包括 persistent-before-info-before-writable、owned stat 与 exact fields、marker READ ACL、lost info/writable/demote reply、原删除请求对同版本的新 nonce 重投、两个 publication boundary 的 readiness 变化、service refresh/writable 两种交错、相同字段的 foreign session 拒绝、实际 server session expiration/new publisher、已提交的 writable 回调超时、实际过期后的成功回调不提升资格、坏 envelope/wrong scope、真实 parent ACL 拒绝无部分创建，以及既有 harness 配上真实 persistent/ephemeral stores。组合场景仍使用 typed recovery/compatibility facts 与 `InMemoryLocalReadinessStore`；其 WRITABLE 仅为该 reference 状态机结果，不能证明真实文件恢复、local receipt fsync 或 Bookie 服务可写。

```bash
mvn -o -P '!cargo-zigbuild' -pl bookkeeper-server -am \
  -Dtest=ZKProfileRegistrationStoreTest,ZKProfileEphemeralRegistrationStoreTest,StartupReadinessFactsTest,StartupReadinessHarnessTest \
  -Dsurefire.failIfNoSpecifiedTests=false -DtestRetryCount=0 test
mvn -o -P '!cargo-zigbuild' -pl bookkeeper-server -am spotless:check checkstyle:check
python3 scripts/unified-wal/run-storage-models.py --tool /tmp/unified-wal-tla2tools-174.jar
```

模型 `unified-wal-model-s992kax7` 绑定同一父提交、234 source inputs/403 checksum，134 配置为 34 正向完整 PASS、100 指定反例；新 registration publication 投影 868 distinct states、12 动作 coverage 非零、4 指定反例，详见[模型结果](../models/README.md#session-绑定的-registration-publication-投影)。所有 source hash 与本轮实现一致，旧 receipt/source/design 绑定保持原样；上面 CAS 25 测试与 reference receipt 是历史增量，不改为本轮结果。冻结 Wave 0 scope guard 的历史 BLOCK 保持，脚本/allowlist 未修改，本轮不重复当作当前 backend 验收。

本轮只完成独立实验 namespace 的真实发布协议。生产 BookieServiceInfo/现有 available registry、真实 startup/local receipt 与 delete catch-up、listener/placement/本地 admission 的持续连接事件接线、watcher 与失连立即降级、实际 ZooKeeper quorum/重启、BookKeeper wrapper 全自动重建/重投组合、ProfileControlStore/sidecar/安装激活仍未完成。会话自动过期清理不等于临时断连立即删除远端节点；一个 cold check 也不证明持续服务 readiness。没有 production authority、掉电、性能、长期工作集或完整 A/C/D 与组合结论，步骤 12/Gate 保持 PARTIAL。

## 步骤 12：真实本地 readiness receipt

2026-10-05，[`SegmentLocalReadinessStore`](../../../../../bookkeeper-server/src/main/java/org/apache/bookkeeper/bookie/storage/segment/SegmentLocalReadinessStore.java) 实现既有 `LocalReadinessStore`。调用方提供已创建、受保护的实验 Bookie 目录、BookieId 与 incarnation；复用 `SegmentDataFileLock` 的进程内 scope 保留及实际 OS writer 锁，并复用 `SegmentFileIo` 的完整短写、有限零进度与 force 路径。它只持久化已提供的结论，不读取或证明 compatibility、required-device、allocator/route recovery、delete assignment/cursor；这些仍须在每次 startup 新鲜验证，旧 receipt 不能代替恢复事实。

目录只使用三个固定名字：`local-readiness.lock`、已发布的 `local-readiness`、未发布的 `local-readiness.pending`。相同完整记录返回 ALREADY_APPLIED；同 incarnation 的严格更高 readiness generation 才可替换，旧代或同代不同内容返回 CONFLICT。当前 payload 之外不保存历史，100 代真实替换后仍只有 receipt 与 lock 两个文件。此结论只涉及 receipt，不扩展到既有 Arena/Bookie 业务历史或整个存储工作集。

发布顺序是完整 temp write → temp `force(true)` → 同目录 `ATOMIC_MOVE + REPLACE_EXISTING` → parent directory `force(true)` → APPLIED。没有原子替换能力时直接保留 UNKNOWN，不退化为普通 move。write、文件同步、替换或目录同步的 I/O 失败均永久隔离当前实例：后续 write 仍 DURABILITY_UNKNOWN，read 明确抛异常，不能把已可读的 renamed bytes 当作已持久成功，也不能当作 absent。调用方不得通过这种异常开放本地服务。

新 cold attempt 在独占锁内严格解析已发布文件、核对 Bookie/incarnation、同步该文件及目录之后，才返回 receipt；缺少正式文件时返回 empty。未发布的 pending 永不用于恢复，后续明确的新 write 可在这个固定临时路径重写。此恢复同步只确认 receipt 文件，不能清除 DATA/control 的已知 I/O 错误、未 CLOSED 会话或未完成删除追赶。constructor 任一校验/同步失败均保持未开放，并尝试释放 writer scope；释放失败作为 suppressed exception 保留，不伪造清理成功。

close 先关闭当前实例的读写资格，再释放实际 writer；即使释放抛异常，该实例也不再读写，后续 close 仍可重试资源清理。真实重复关闭与关闭后拒绝已验证；native lock/channel close 失败未实际注入。

### 实验本地 envelope v1

[`SegmentLocalReadinessCodec`](../../../../../bookkeeper-server/src/main/java/org/apache/bookkeeper/bookie/storage/segment/SegmentLocalReadinessCodec.java) 的 big-endian 格式总长度 `112 + nameBytes`，范围 113～367 bytes：8-byte magic `UWLRD001`、schema int32=1、total length int32、Bookie name int32 byte length/canonical UTF-8、incarnation16、manifest identity32、device/format/effective-delete/readiness 四个 int64 generation、status int32（1 READY/2 QUARANTINED）、durable flag int32（0/1）、前面全部字节的 CRC32C int32。READY 必须 durable=true，所有 generation 严格为正。未知 magic/schema/状态、非规范 UTF-8/NUL、零 identity/generation、错误长度/校验和及尾随字节均拒绝；先检查文件大小再分配。现有 control、DATA、image、wire 及冻结 reference 格式均未变化。

### 本轮验证与边界

Java `unified-wal-local-readiness-0bt45og9` 绑定父提交 `4f9c2c24151cbef9ddb8c7da9b6fc86e9ec10fc7` 和 47 source inputs：本地 store 10、ephemeral 13、persistent CAS 8、原 reference 17，共 48/48 通过，0 failure/error/skip；Spotless/Checkstyle 通过。真实覆盖短写及部分失败、文件/目录 barrier 失败后的不可解析成功、恢复 barrier 失败、unpublished temp、固定字节向量、损坏及大小/scope/符号链接拒绝、运行中损坏不覆盖、目录别名重复打开后跨进程 writer 仍互斥，以及三个发布边界退出后各再发布/退出/重开一轮。

子进程退出点分别为 temp write 后/force 前、temp 已 force 且已 rename 后/directory force 前，以及完整发布返回 APPLIED 后；第二次退出后仍保留新 receipt，旧 generation 不再成功。它是进程退出与实际 FileChannel 证据，不是掉电或文件系统故障模型的实证。

`referenceStartupUsesRealFileReceiptAndBothZooKeeperStores` 让原 harness 使用真实文件 receipt 和两个真实 ZK stores，处理 persistent reply loss，再 demote、重开 receipt、新 nonce 重跑九个阶段。原 typed recovery/compatibility/device/delete facts 未变成真实生产恢复；WRITABLE 仍只描述该组合测试的 reference 结果，普通 Add 冷操作计数保持 0。此前使用内存 local store 的历史场景与回执保留。

```bash
mvn -o -P '!cargo-zigbuild' -pl bookkeeper-server -am \
  -Dtest=SegmentLocalReadinessStoreTest,ZKProfileRegistrationStoreTest,ZKProfileEphemeralRegistrationStoreTest,StartupReadinessFactsTest,StartupReadinessHarnessTest \
  -Dsurefire.failIfNoSpecifiedTests=false -DtestRetryCount=0 test
mvn -o -P '!cargo-zigbuild' -pl bookkeeper-server -am spotless:check checkstyle:check
python3 scripts/unified-wal/run-storage-models.py --tool /tmp/unified-wal-tla2tools-174.jar
```

本轮模型 `unified-wal-model-4h3zm7ae` 绑定同一父提交、245 source inputs/421 checksum；140 配置为 35 正向完整 PASS、105 指定反例。本地投影 56 distinct states、11 个 Next 动作 coverage 非零、5 个指定反例。有限 publication 投影及完整模型结果见[本地 receipt 模型](../models/README.md#本地-readiness-receipt-发布投影)。本轮不接入 EmbeddedServer 的 pre-replay fence、真实 recovery/delete catch-up、生产 BookieServiceInfo/available registry 或连续连接事件的本地 admission；ProfileControlStore、sidecar、安装激活、完整 A/C/D 组合、quorum、掉电和性能也仍未完成。步骤 12 与生产 Gate 保持 PARTIAL；原 scope guard/receipt/design source 绑定未改变。


2026-10-05 新增前置 [真实 ZK 控制权威读取](../profile-control/README.md#步骤-12真实-zk-控制权威读取)，73 项相关测试和 style 通过；它只读取 provisioned 单 view，并与原 reference endpoint 的 exact AuthZ/scope 检查组合，不发布控制 authority，也不改变本目录恢复输入。实际文件 recovery、required-device/compatibility 与 delete assignment/cursor 尚未产生完整 readiness facts；不得以 typed 成功、可读 authority 或 receipt 替代真实恢复。pre-replay startup、生产 registry/连续连接准入、完整 sidecar/安装激活与步骤 12 仍未完成。历史 reference/真实文件/ZK receipts 保持原绑定。

## 步骤 12：session 绑定的本地 DATA 准入

2026-10-05，`ZKProfileEphemeralRegistrationStore.watchAdmission` 在该 attempt 第一次 cold 操作前，绑定借用 client 的既有 `ZooKeeperWatcherBase`，不新建 client、线程、重试或 watcher manager。gate 最初关闭；调用方完成本地 startup 检查并发布后，`openAfterReconciliation` 再读匹配的 persistent/info/writable/owned marker 与原 session，完整 tuple 相符才开放。缺失或不匹配退休该 attempt；读取故障退休并传播原因，已并发退休的 attempt 返回 false。该方法不产生或验证真实 recovery/device/delete facts，不能用发布成功替代这些前置检查。

连接事件除 SyncConnected/SaslAuthenticated 外均永久退休原 session/nonce；热检查还要求 SDK 当前为 CONNECTED 且 session 未变。临时断连后的同 session 重连、session 过期、显式 close/demote 及迟到 cold 回调都不能重开旧 gate。新 nonce 是显式的新 attempt，仍须重新满足 startup 与 reconciliation；连接恢复本身不开放服务。绑定一次且先于 cold 操作，退休先关闭 gate 再移除 observer；gate close 不关闭借用 client，也不假装远端 ephemeral 已删除。

`IsolatedSegmentStorage.bindProfileRegistration` 仅允许启动前绑定一次，Bookie scope 必须一致；每次普通/恢复 DATA 接纳再检查已开放 registration 的 Bookie 与本地 incarnation。热路径不做 metadata RPC，不增加按 entry 的 handshake/proof 读取。拒绝不取得调用方 buffer ownership；控制、读取及维护沿用原路径，已提交工作沿用原 I/O/owner 终结协议。graph close 先关闭接收及注册 gate，再排空；超时不能提前释放实际 I/O buffer、writer lock 或文件描述符。未绑定的 graph 仍只是既有 local-only 实验入口，不能据此发布 Profile 服务。

这里的失连切点是可观察的 connection event/SDK state。网络中断在 SDK 发现前可能尚未反映，本 gate 不是瞬时故障检测器，也不 watch 远端 persistent authority 的持续变化或提供 authority 租约。AuthFailed/ConnectedReadOnly 被代码拒绝，但本轮没有实际后端触发这两类状态；没有扩展为 quorum/SDK 自动重建实证。

### 本轮验证与边界

最终 Java `unified-wal-registration-admission-final-qdyeu_q7` 绑定父提交 `f7aebbd97412d0ccd78c37cb9d6dad7fcbf6c37a` 与 106 Java/POM 输入：八类共 296/296 通过，0 failure/error/skip，Spotless/Checkstyle 通过。publisher 的 17 项包含四项新增回归：exact final read 与 scope/incarnation、1,000 次热检查不新增真实 get、实际 server 断连/同 session 重连、新 attempt、真实 expiry、缺失/错 tuple/坏记录及 held actual reconciliation callback。共享 ZK fixture 使用实际 watcher manager，原 control/readiness 回归一并执行。

真实 storage 组合测试先验证 pending gate 拒绝，再在实际 DATA write 后、native force 前暂停 I/O，关闭实际 ZK client。后续普通/恢复 DATA 均拒绝，调用方仍持有输入；原输入已复制可正常归还，I/O 对齐池 lease 与 OS writer lock 则保留。短期限 close 返回 false；释放 I/O 后原请求为 LOCAL_DURABLE，排空成功且真实 restart 读回 entry。它证明已提交本地 I/O 的资源与结果，不承诺尚未提交 I/O 的排队请求必然成功，也不是生产 ACK。

最小 publisher/组合验证通过后，因共享 fixture 和存储接纳/关闭契约扩大至完整八类。中间失败包括 API/格式及 fixture 构造问题，另有把 owner 排队当成已提交 I/O、把已复制的原输入当成 I/O buffer 的错误预期；已按真实提交切点和原池 ownership 修正，没有削弱生产拒绝、持久化或关闭契约。失败日志保留且不计 PASS。

```bash
mvn -o -P '!cargo-zigbuild' -pl bookkeeper-server -am \
  -Dtest=ZKProfileControlStoreTest,IsolatedProfileControlEndpointTest,SegmentLocalReadinessStoreTest,ZKProfileRegistrationStoreTest,ZKProfileEphemeralRegistrationStoreTest,StartupReadinessFactsTest,StartupReadinessHarnessTest,SegmentBookieControlLogTest \
  -Dsurefire.failIfNoSpecifiedTests=false -DtestRetryCount=0 test
mvn -o -P '!cargo-zigbuild' -pl bookkeeper-server -am -DskipTests spotless:check checkstyle:check
python3 scripts/unified-wal/run-storage-models.py --tool /tmp/unified-wal-tla2tools-174.jar
```

模型 `unified-wal-model-1jom7_es` 绑定同一父提交、293 source inputs/451 checksum：150 配置为 37 正向完整 PASS、113 指定反例；106 Java 输入均为其子集，source/checksum 与五个父提交 designInputs 已核对。新投影 220 distinct states、11 个 Next 动作 coverage 非零及四个指定反例，见[本地准入模型](../models/README.md#session-绑定的本地-data-准入投影)。旧 receipt、设计输入与冻结 scope guard 不改绑。

这一步接上隔离 graph 的 session 事件与本地 DATA gate，没有接入真实 Bookie listener、生产 BookieServiceInfo/available registry、fresh recovery/compatibility/device/delete facts 或 pre-replay startup。远端 authority 持续变化、完整 sidecar/backlink/安装激活、真实 quorum、掉电/性能、完整 A/C/D 及组合、Block G 与步骤 12/生产 Gate 仍未完成。

2026-10-05 启动排他增量：[restart DATA 排他前置](../local-storage/README.md#restart-的-data-排他与控制恢复顺序)先取得真实 DATA 目录 writer lock，再恢复控制及 index，把同一 native lock 交给 files，保留原全部 header 校验/恢复同步与 CLOSED 限制。实际子进程外部锁红绿、控制打开/DATA 恢复失败清理及成功移交/读回验证完成；340 项中 335 通过、5 项 macOS 上的 Linux 亲和性跳过，style 通过。有限矩阵 38 正向完整 PASS/117 指定反例。只补本地 ownership 前置，compatibility/device/delete 权威、真实 startup/readiness、生产接线及完整模型仍待完成。

## 消费者发现与再次复核

2026-10-09，AutoRecovery动态pool接线前补齐既有ZK registration的真正消费者读取。原ZKProfileEphemeralRegistrationStore仍是publisher adapter，只接受自身session/nonce；不能用它发现其他进程。ZKProfileRegistrationStore新增readPeer，允许独立reader session观察另一个publisher，不改变发布、demote或本地admission语义，不新增metadata格式、namespace、watch/retry或工作项日志。

一次冷读取有同一个最多一分钟deadline：先sync并读取持久READY及其birth anchor，再读取service-info/writable两份严格UWEP envelope；两份必须完整相同，实际Stat owner必须非零且等于envelope的publisher session，scope/incarnation/readiness generation/persistent mzxid/capability hints与READY一致，session-nonce marker必须同owner、version0和empty bytes。末次再次sync并读取READY、两份projection及marker，比较全部czxid/mzxid/version/owner和字节，reader本身也必须保持原connected session。NOT_FOUND用于缺注册或非READY；合法更新窗口/不可读返回UNAVAILABLE；损坏、persistent假注册或owner错配返回CORRUPT，均不携带peer。

RegisteredPeer只有私有构造，持有不可变projection/readiness和出生身份，属于发现观察结果，不是transport认证或ledger mutation authority。revalidatePeer重新执行真实冷读取；同内容重建、publisher episode/READY/endpoint改变时不会静默换成新peer。之后仍须按既有TLS1.3+mTLS/HELLO校验expected BookieId/incarnation/实际capability，再做冷复核；新reader不能凭capability hints授予normal/recovery/ACK。该观察cut不保证之后的瞬时强撤权，也不实现same-ID rejoin。

真实ZK双session测试验证消费者读取、只读不修改Stat、原publisher-only adapter仍拒绝foreign caller、demote和实际session expiry、stale READY、同字节writable重建、错误owner/坏codec/persistent注册/缺marker，以及持有第一条真实READY回复期间发生更新时末次复核拒绝。ProfilePeerDiscovery有限投影检查READY/owner/pair/marker和generation/birth一致性；这些guard事实由Java生产，模型不生产ZK、TLS、crypto或HELLO。

复现：

```bash
bash scripts/unified-wal/run-local-storage-tests.sh 'ZKProfileRegistrationStoreTest,ZKProfileEphemeralRegistrationStoreTest,ZKProfileControlStoreTest'
mvn -o -P '!cargo-zigbuild' -pl bookkeeper-server -am -DskipTests spotless:check checkstyle:check -q
python3 scripts/unified-wal/run-storage-models.py --tool "$UW_TLC_JAR" --config-prefix a-profile-peer-discovery --config-prefix ce-profile-peer-discovery --config-prefix a-readiness-cas --config-prefix ce-readiness-cas --config-prefix a-registration-publication --config-prefix ce-registration-publication --config-prefix a-registration-admission --config-prefix ce-registration-admission
```

当前只实现消费者发现与再次复核这一必要前置。AutoRecoveryMain默认仍不启用Profile；服务bootstrap、动态TLS连接/重连/总pool预算及pool与原task实际drain的资源所有权继续OPEN。生产Bookie listener、完整startup/pre-replay权威、离线追赶/rejoin/delete/GC与综合Gate仍未完成，步骤12/14保持PARTIAL，不提升生产权限或历史receipt。

Java `unified-wal-local-GDbVx4DD` exit0：3类42项全部通过（persistent consumer/codec/CAS13、ephemeral publisher17、control store12），1052冻结inputs/6 artifact checksums；随后只修正import排序。最终 `unified-wal-local-Jh70Lafn` exit0：5项消费者定向PASS、1052当前inputs/4 checksums。最终style `unified-wal-peer-final-style-lFNGo2TX` exit0/2 checksums；root RAT `unified-wal-peer-checks-IT8HTN1T` exit1/443 unapproved路径，全部属于父HEAD且与上一里程碑集合相同，5 checksums保留首次ImportOrder格式失败；没有新增许可失败、不改规则、不计CI全绿。

模型相关广度 `unified-wal-model-i3w2dufn` exit0：21配置4完整正向/17指定反例、827 inputs/632 checksums；在增加expiry测试及最后import/scope说明前。最终 `unified-wal-model-8z5elnia` exit0：7配置1完整正向/6指定反例、827当前inputs/607 checksums；Java/model共享232/去重1647 inputs与五个父HEAD designInputs全部核对。新正向97 generated/81 distinct、queue0、depth/diameter4、0.775秒/peak RSS104448000 bytes（广度run），是有限模型消耗，非服务或存储性能。官方tool1.7.4、单worker、heap512/direct64MiB、seed1/fp0、无symmetry/state constraint、CHECK_DEADLOCK FALSE；catalog515和完整A+C+D未全跑，历史source/config/run及回执不重绑。

2026-10-11消费者观察已接[显式注册连接入口](../profile-wire/README.md#注册观察驱动的连接与实际本地排空)：原观察→cold复核→actual mTLS/HELLO→cold再次复核→交付；失败/取消的cold与本地transport实际排空可等待。单连接不替代动态pool与service bootstrap/总预算，步骤12/14仍PARTIAL。

2026-10-11consumer/registered connection已由[配置cohort peer pool与原worker lease](../profile-wire/README.md#注册-cohort-peer-pool-与实际-worker-lease)消费；不是无界registry枚举，也不授予ledger authority或自动rejoin，默认service bootstrap仍OFF。

2026-10-11 registered pool由[显式owned AutoRecovery service](../profile-wire/README.md#显式-autorecovery-service-bootstrap-与-owned-资源)实例化，SDK/ZK authority context一致、failure/shutdown实际排空后关闭owner；默认service入口OFF，完整Profile audit/startup与生产Gate未接受。

2026-10-11 [SDK关闭期间的新中断](../profile-wire/README.md#sdk-关闭过程中的新中断)已真实RED/GREEN验证并修复：原Main使用独立一次性SDK closer，caller中断只记账并等实际procedure结束，随后回收cold/group并恢复signal。48项Java与相关54模型配置得到预期结果；Profile availability/audit、全字节预算及步骤12/14/生产Gate仍PARTIAL/OPEN。

2026-10-11 [Auditor实际排空与中断](../profile-wire/README.md#auditor-排空与-owned-sdk-关闭)已真实RED/GREEN验证原audit executor等待中的interrupt不再提前进入SDK关闭；Auditor/elector ownBkc复用one-shot closer。51项Java（含Classic auditor关闭/连续换选举）与相关55模型配置得到预期结果，runner绑定实际owner源码；Profile availability/audit及步骤12/14/生产Gate仍PARTIAL/OPEN。

2026-10-11 [Profile成员故障发现](../profile-wire/README.md#profile-成员故障发现与原-auditor-接线)已接原auditor启动/cohort watch/periodic扫描与原任务生成，按descriptor/instance/retained incarnation和fresh cut核验；Classic同名registry不掩盖Profile缺失，UNKNOWN/CORRUPT不计缺失。真实自动任务→COPY/成员替换→全W复核/排空/marker清理通过；注册观察不授予repair/normal权限。周期DATA审计、总预算、rejoin/delete/综合Gate仍OPEN，步骤12/14保持PARTIAL。

2026-10-11 [共享索引与legacy退役拒绝](../profile-wire/README.md#共享引用索引与-legacy-退役拒绝)修复Profile过滤影响共享BookKeeperAdmin的问题：公开索引恢复全引用，仅Classic auditor过滤Profile；legacy退役对已知ensemble/marker中的Profile引用明确拒绝，避免无marker时提前成功。真实退役RED/GREEN与公开index/Classic实际任务范围验证通过；完整Profile退役/retained-history清理仍未实现，生产Gate不提升。

2026-10-11 [CLOSED周期DATA审计](../profile-wire/README.md#profile-closed-data-周期审计)已复用原replica-check CLOSED范围、streamed metadata与真实全W验证，共用单peer pool/coordinator预算，actual health/lease drain与最后fresh cut后只生成检查任务。真实历史DATA hole→原worker修复/全W/marker闭环、健康/权限拒绝不clear、deadline/关闭保留actual资源通过。OPEN/正常writer close、payload/placement、总字节预算、离线/rejoin/delete与生产Gate仍OPEN，步骤12/14保持PARTIAL。


## 初始 delete required-through 与实际恢复 cursor

2026-10-12，explicit `bindDeleteDelivery`将原graph的healthy recovered control cursor接到既有ephemeral publisher，fresh writable multi同时检查initial assignment/birth/head版本；仅caller输入DELETE_RECOVERED或persistent READY不够。`catchUpProfileDeletes`实际冷读完整event/authority并经原force/apply推进no-hole cursor；LIMIT/UNKNOWN不开放gate，UNKNOWN保留actual local completion。已用真实磁盘restart/DLR/三节点与两ZK客户端验证seq2抢在注册前提交、继续追赶与lost reply同session解析。[接线记录](../profile-wire/README.md#初始-required-through-cut-与真实-rejoin-追赶)拥有源码、范围与证据。缺失assignment不默认empty；首次empty commissioning是显式动作。ordinary head增长不是assignment变化；后续在线消费、snapshot/handoff、其他真实compatibility/device/required-authority producer与默认bootstrap尚未闭合，不把此cursor切片升级为完整startup/Gate。


2026-10-12 [全目标 source与原注册consumer](../profile-wire/README.md#全目标-publication-与在线节点消费)在logical前使全部target head的required-through可见，PREPARED阻塞追赶而不授权删除；新reference持久绑定完整publication prefix。原registered graph显式借用cold/scheduler消费后续CLOSED effects，ordinary head增长不demote；stop/session退休保持local gate关闭并等待actual I/O，未排空close保留locks。该接线仍要求既有真实startup前置/default bootstrap等其余facts，snapshot/handoff及完整Gate不提升。


## 实际恢复 graph 与原 startup 发布桥接

2026-10-12，`IsolatedSegmentStorage.recoveredStartupFacts` 在原 opened graph 上产生 device/allocator/route/delete 观察，`reconcileReadiness` 将它们接到已有 `StartupReadinessHarness`，没有另建状态机、线程、队列或持久格式。入口是显式、有界的 cold 启动步骤，调用方须持有原 graph 的启动编排，先绑定 pending registration admission，再完成 `startNew`/健康 `restart`，期间不交付其他 graph 操作。

初始单 Arena 的 expected manifest identity 是域分离 SHA-256 加原完整 immutable version-1 header；包含声明的全部 DATA 文件拓扑、Bookie/incarnation/Arena 和持久格式，不从当前可读目录子集猜测 required set。它不受执行线程数、placement 或目录位置影响。compatibility policy 和 expected device 要求须与本 graph 的固定 scope/generation/format 一致；错 manifest 或不支持的要求在 delete apply 前拒绝。内部 reference feature 1 表示 mandatory experimental version-1 header/control/framing base，不新增磁盘 feature bit、wire capability 或已发布格式承诺。

实际 graph 打开仍复用原全部 DATA header、双控制日志、DATA recovery sync、派生定位恢复和固定 owner bootstrap；必需控制文件的 open 已不使用 CREATE，不因文件缺失补造权威。产生 facts 时再次检查同一健康 scope、无未决 Bookie 控制/entry义务、无未决或quarantine Arena状态及原 managed session。bounded delete catch-up 必须从 committed source 达到当前 shared assignment/head；LIMIT/UNKNOWN/UNAVAILABLE/QUARANTINED 均不产生可发布 input。观察覆盖 allocator/route/delete RECOVERED、实际 READY device 和当前 assignment generation； supplied device status/recovery conclusion 被实际观察替换。rollback facts 从已恢复 control 的 install/activation/fence/grant/tombstone 和 Arena authority 取得；没有 success receipt 不能证明曾经没有成功，仍不授予回退权限。

publisher 要求原 admission 对象、同一个 ZK client/registration namespace、已绑定的 reservation source 与本 incarnation。原 Harness 顺序仍是 DLR 文件/目录持久化 → persistent readiness CAS → service-info → writable → exact final read；原 writable multi 继续消费 delete required-through cut。随后 `openAfterReconciliation` 再做原 session/nonce/tuple复核，并启动已有 `SegmentProfileDeleteFollower`。调度器拒绝、attempt退休或consumer失败不返回可用 WRITABLE，撤销本地准入并demote原发布；graph关闭仍按原consumer实际排空协议。completion 访问器只返回副本，其含义是已提交本地delete I/O/owner完成，不是远端ZK操作取消或整个graph排空。

兼容性 fence/migration 状态、reader/writer门槛、new-scope roots/独立credential/access/disposition 仍是上游提供的 policy，当前桥接只核对其类型合同与本地scope，尚未提供其真实 issuer 或操作系统/service身份隔离证明。测试中的 old/new roots、credentials、FORMAT_READY和endpoint均是reference输入；不能把本次组合 WRITABLE 当成真实兼容迁移、生产endpoint或部署验收。默认 EmbeddedServer/bootstrap、SDK routing、完整pre-replay fence与其余required authority producer继续OPEN。

真实组合测试先通过既有mTLS socket/coordinator把ledger44恢复为fenced CLOSED并逻辑删除，再关闭目标0、原目录/身份restart，在新ZK publishing client上追赶原committed delete。即使输入device为CORRUPT、recovery为MISSING，输出也必须来自实际恢复结果；错manifest在effect前拒绝，错publishing client在DLR前拒绝。正向读回真实durable DLR、persistent manifest和owned writable，并验证本地tombstone读拒绝、graph关闭退休准入、closed graph不能产生facts；另一轮用已关闭scheduler验证consumer接线拒绝与demote。它没有注入新的掉电、compatibility fence或物理credential故障。

复现：

```bash
bash scripts/unified-wal/run-local-storage-tests.sh 'StartupReadinessHarnessTest,StartupReadinessFactsTest,ZKProfileEphemeralRegistrationStoreTest,ZKProfileRegistrationStoreTest,SegmentLocalReadinessStoreTest,SegmentProfileInitialSocketClientTest#readinessBridge*+automaticDeletePublication*+preparedDeleteCancellation*+committedDeleteDeliver*+*Registration*+*Registered*+registered*+registration*+*Rejoin*'
bash scripts/unified-wal/run-local-storage-tests.sh 'SegmentProfileInitialSocketClientTest#readinessBridgeUsesActualRecoveredFactsAndOriginalPublicationSequence'
python3 scripts/unified-wal/run-storage-models.py --tool "$UW_TLC_JAR" --config-prefix a-local-readiness-publication --config-prefix ce-local-readiness-publication --config-prefix a-readiness-cas --config-prefix ce-readiness-cas --config-prefix a-registration-publication --config-prefix ce-registration-publication --config-prefix a-registration-admission --config-prefix ce-registration-admission
mvn -o -P '!cargo-zigbuild' -pl bookkeeper-server -am spotless:check checkstyle:check
mvn -o -P '!cargo-zigbuild' apache-rat:check
```

父源码 `3e82a21ed9d540740db763acb38ab497225d6387`；广度 `LO15wlP1` 六类77项（common facts6/harness11、persistent13/ephemeral19、local DLR10/socket18）零failure/error/skip，在最终test格式及等价简名调整前，保留原stage输入；`SakRPo3Z`同选择早期运行不叠加计覆盖。最终 `bTjwd1kg` 一项组合测试（包含正常与scheduler拒绝两轮）零failure/error/skip，1073当前source inputs全部匹配。首次 `gTV7Lb7B` 因fixture给出空mandatory feature集合，被原类型合同拒绝；是fixture失败，不冒充生产bug红绿。中间正向与格式失败日志均保留。最终Spotless/Checkstyle exit0；root RAT exit1/443 unapproved，逐路径均属于父HEAD，没有新增，未改许可规则，不称CI全绿。

相关既有有限投影 `u5cp9gcl` exit0，20配置为4正向完整探索/16指定反例；970 inputs仅上述test文件的最终格式/等价简名调整与run阶段不同，其余匹配，原manifest不移绑。投影检查DLR barriers/UNKNOWN、persistent CAS birth/generation、session发布和本地准入；没有新增模型或把guard输入宣称为真实fence/credential/TLS/fsync证明。TLC1.7.4、单worker、heap512/direct64MiB、seed1/fp0、无symmetry/state constraint、CHECK_DEADLOCK FALSE；完整A+C+D及综合模型未全跑。持久证据 `/Users/liusinan/.codex/artifacts/unified-wal/startup-bridge-evidence-r0papqio` 保留原command/source/config/stage/reports/checksums。

本切片接通实际恢复事实与原显式发布/准入/consumer，步骤12/14/15及生产Gate保持PARTIAL/OPEN。snapshot/handoff/history GC/decommission、OPEN/payload/placement、full-bytes、完整兼容/正式性能/运维与全16步综合验收仍需推进；Linux NUMA、真实掉电与正式性能本轮NOT_EXECUTED，持续目标保持active。


## 持久 startup requirements 与冻结发布条件

2026-10-12，`ZKProfileStartupRequirementsStore` 在调用方预先配置的独立namespace保存初始单Arena要求：完整 `BookieCompatibilityFact` 与 expected `RequiredDeviceManifest`，由一次真实ZK multi原子创建record及唯一birth子节点。它是required格式/身份/设备声明的持久来源，独立于readiness；不是stock old-binary fence、OS/service credential隔离证明，也不授予ledger INSTALL/normal/recovery权限。namespace/ACL仍由部署方提供，接口不创建生产scope或修改OS权限。

`provision` 只创建、不更新：same完整内容返回ALREADY_APPLIED，不同内容CONFLICT；existing corrupt/missing birth不覆盖或补造。提交后丢回复/timeout保留RESPONSE_LOST，调用方从实际source重新取得观察。`read` 先sync，只有实际NONODE是NOT_FOUND；坏格式、ephemeral、非version0、错scope、缺/坏birth或parent/birth不在同一creation zxid为CORRUPT，读故障为UNAVAILABLE。Observation只由原store私有构造，冻结原envelope/czxid/mzxid；不能从调用方typed事实或readiness提示产生。

实验UWSQ/schema1最多4096 bytes，仅一个required Arena，每份严格递增mandatory feature列表最多64个ID。字节顺序为magic/schema/birth UUID、length-prefixed UTF-8 BookieId、incarnation/Engine/wire/hash suite、mandatory features、manifest identity/generation、local format/effective assignment、minimum reader/writer、migration state/generation，再接device identity/incarnation/manifest generation、Arena/control/checkpoint formats、superblock generation/features及CRC32C。migration的显式值1/2/3/4分别为PREPARED/FORMAT_READY/QUARANTINED/UNKNOWN_MANDATORY，不依赖Java enum ordinal；未知schema/state、长度/UTF-8/feature不合法、trailing bytes与非canonical编码拒绝。device READY表示要求，不能证明磁盘已恢复；CRC32C不构成授权或old-binary可见sentinel。原DATA/control/wire bytes不变，未作stock binary兼容声明。

显式调用先read source，再在任何新graph构造/open前执行 `IsolatedSegmentStorage.startupPolicy`：用观察替换caller compatibility/device要求，匹配原完整header的deterministic manifest、Bookie/incarnation/Arena/generation/formats及mandatory base feature。PREPARED/不支持scope或minimum版本拒绝。reader/writer支持固定为本实现实际version1，不能由caller自报runtime99越过minimum2；恢复facts也产生实际version1/base feature1。new-scope policy仍须满足原typed roots/身份/access/disposition合同，但其物理真实性未由此验证。随后走原恢复graph、实际delete catch-up及原Harness顺序；同scope迁移、PREPARED→FORMAT_READY工具及default EmbeddedServer hook仍未接入。

publisher在首次cold操作前一次性 `bindStartupRequirements`，要求同一ZK client、原source对象/私有Observation及Bookie scope，不能在运行中重绑。graph的 `reconcileReadiness` 拒绝用另一份caller compatibility/device替换已绑定source；writable冷读取再复核source，writable创建的同一个multi加入原required record version0与唯一birth version0检查，同时消费已有readiness/info/session及delete required-through条件。即使要求内容相同，正常重新provision的物理birth也不能替代原attempt；preflight后变化由最终事务拒绝。回复丢失可从原source/owned registration解析，source变化不能被新hint或新观察悄悄吸收。没有添加按entry读、持续远端authority lease或强reset；不支持管理者恢复旧birth ID/全metadata重置，也不把registration投影当成兼容性fence。

真实ZK测试覆盖并发不同要求single winner、原source Stat不被同操作重写、commit reply loss、held actual reply/timeout、parent ACL拒绝、损坏/缺birth/ephemeral与不补造；冻结publisher在source重建后拒绝，source在最后preflight与真实multi之间重新provision时无writable effect，原info保留；foreign source/client观察不能绑定。固定226-byte独立字段向量、CRC/截断/未知schema与有正确CRC的非法UTF-8均验证。原实际mTLS/SDK/CLOSED/delete/磁盘restart组合改为先读取source、检查后才构造replay graph；prepared和minimum2拒绝，即使caller自报runtime99也拒绝，caller替换要求在DLR前失败；正常发布/consumer及scheduler拒绝后的demote继续验证。

复现：

```bash
bash scripts/unified-wal/run-local-storage-tests.sh 'ZKProfileStartupRequirementsStoreTest,StartupReadinessHarnessTest,StartupReadinessFactsTest,ZKProfileEphemeralRegistrationStoreTest,ZKProfileRegistrationStoreTest,SegmentLocalReadinessStoreTest,SegmentProfileInitialSocketClientTest#readinessBridge*+automaticDeletePublication*+preparedDeleteCancellation*+committedDeleteDeliver*+*Registration*+*Registered*+registered*+registration*+*Rejoin*'
bash scripts/unified-wal/run-local-storage-tests.sh 'ZKProfileStartupRequirementsStoreTest,StartupReadinessHarnessTest,StartupReadinessFactsTest,SegmentProfileInitialSocketClientTest#readinessBridgeUsesActualRecoveredFactsAndOriginalPublicationSequence'
python3 scripts/unified-wal/run-storage-models.py --tool "$UW_TLC_JAR" --config-prefix a-startup-requirements --config-prefix ce-startup-requirements --config-prefix a-registration-publication --config-prefix ce-registration-publication --config-prefix a-readiness-cas --config-prefix ce-readiness-cas --config-prefix a-registration-admission --config-prefix ce-registration-admission
mvn -o -P '!cargo-zigbuild' -pl bookkeeper-server -am spotless:check checkstyle:check
mvn -o -P '!cargo-zigbuild' apache-rat:check
```

父源码 `a048b29cae32fada0753a7c95ecf2fefc9a21a39`；阶段 `qy6yYYSK` 七类85项全部通过，在最终caller替换/UTF-8 negative和actual runtime guard前，按原stage保留。`kWs6a5Y9`/`lBeE5Og3`/`ZoQhNwYU` 各9项定向按原输入保存，不叠加计覆盖；首次 `v1qdFr3L` 的一项error是fixture撤去自身ADMIN后尝试恢复ACL，已改用独立只读拒绝目录，未改变生产ACL契约。最终 `KGzRhsIE` 四类26项（requirements8/socket1/harness11/facts6）零failure/error/skip，1076当前inputs全部匹配，actual runtime version guard和新negative已执行。最终style exit0；初次RAT448中的新增5个模型header补全真实ASF许可后，最终RAT仍exit1/443父HEAD已有路径，无新增，不改规则、不称CI全绿。

相关模型 `hb9tihtt` 18配置为4完整正向/14指定反例，在最后header/test/runtime变化前，保留原输入。runtime guard阶段 `fn17vvad` 新投影4配置为1完整正向/3指定反例，978当时inputs匹配；检查unsupported pre-open、caller替换和writable遗漏frozen source birth三个性质。22 generated/19 distinct、queue0、depth/diameter6、0.829秒/peak RSS104546304 bytes属于有限checker消耗，非存储性能。官方TLC1.7.4、单worker、heap512/direct64MiB、seed1/fp0、无symmetry/state constraint、CHECK_DEADLOCK FALSE；模型把完整解析、唯一immutable birth、ZKmulti原子性和actual graph readiness作为输入，不生产TLS/fsync/物理credential/fence或完整活性。catalog628与完整A+C+D未全跑。证据 `/Users/liusinan/.codex/artifacts/unified-wal/startup-requirements-evidence-udj556wk` 保留各stage原source/config/run/command/失败/当前snapshot与checksum；历史回执不移绑。

当前增加持久required格式/设备来源与显式pre-construction/frozen publication guard；真正old-binary fence和new-scope/credential物理证明、default bootstrap/SDK routing、snapshot/handoff/history GC/decommission、full-bytes及完整兼容/性能/运维仍OPEN。步骤12/14/15和生产Gate保持PARTIAL/OPEN，Linux NUMA/真实掉电/正式性能本轮NOT_EXECUTED，完整目标继续active。

最后仅清理五个新模型许可头的尾随空白，最终iou2vxf6四配置为1完整正向/3指定反例，978当前inputs匹配；完整模型语义未改，原fn17vvad和Java KGzRhsIE回执保持stage绑定。最终staged whitespace检查与本地提交一并核对。


## 实际 Unix credential 的 new-scope access 快照

2026-10-12，新增admin实验工具 `scripts/unified-wal/probe-new-scope-access.py`，输入oldUser和四类old/new canonical roots，实际运行当前身份及 `/usr/bin/sudo -n -u oldUser -- <same Python> -I -B <same script> --worker`。这里只处理调用方明确提供的隔离目录；工具不创建账户、不修改已有目录/文件权限、不改现有storage bytes，不启动/停止真实部署服务。旧child cwd为`/`、close_fds为true，仅继承stdio；报告native effective UID/GID/supplementary groups、nonce、script/input SHA、原command与actual exit，避免仅比较用户名或caller自述access等级。

每个root实际执行directory read与私有nonce文件的create/write/fsync；对现有regular files及新建的0666可读nonce marker，再执行read-open/write-open。现有文件只open/close、不读取payload、不写入。marker检查用于空目录和execute-only目录；它不是真实authority文件，权限只修改在本工具创建的文件上。完整现有清单有128-file/128-directory界限，输入16KiB、worker/输出256KiB，超限不取子集成功。symbolic alias、hardlink/特殊文件、old/new root相等或祖先重叠、missing root、old UID0或同UID均不产生VERIFIED；known errno EACCES/EPERM才是DENIED，其他错误是UNAVAILABLE。前后核对root与文件identity/owner/mode/size，重叠new roots中的同一路径去重，不能以可读子集代替整份清单。

new身份须能read/create/write/sync并打开现有文件；old身份必须对directory/create及全部known-file read/write-open均被实际拒绝。结束后只删除同device/inode的本工具nonce文件；partial marker写入失败也在实际调用结束后检查其身份再清理。成功的旧child已实际退出才返回VERIFIED，超时/不可运行/错身份/错nonce/缺清单均不产生可导入access事实。这里不承诺中断一个旧binary或远端操作，也不提供权限持续lease。

测试先真实否证了早期directory-only规则：目录mode0711使old身份不能列举或创建，但四个0644 `known-authority` 文件仍可由actual old UID的`head`读出。red `44EZ9JGG` 一项failure是REJECTED期望却得到VERIFIED；不是mock或fixture失败。增加existing-file清单及0666 marker后，该场景返回REJECTED。额外验证private roots正向、0777公开roots拒绝、物理alias/missing/root-UID拒绝；可读marker和actual-file检查不能被“目录名不同”替代。

原真实SDK/mTLS/ZK/CLOSED/delete/target restart组合的 `physicalCredentialAccessFeedsOriginalReadinessBridge` 在构造replay graph前运行probe，将实际canonical paths、effective UID和NONE/READ_WRITE access导入原NewScopeFallbackManifest，再复用原source/harness/admission/consumer。两个成功snapshot分别包含实际Arena/DATA/control/derived-index的16个现有文件，scheduler拒绝仍demote。当前单Arena图不使用Classic journal/ledger目录，这两类采用真实隔离guard目录；oldBookieId/incarnation、old roots与old-service DRAINED仍为外部reference声明，工具明确 `oldServiceDisposition=NOT_VERIFIED`，未证明旧服务曾运行或已真实排空/退役。

本机实际new UID501、old `nobody` UID4294967294，native old身份包括其真实supplementary groups。最终八份receipt为3 access VERIFIED、2 REJECTED、3 UNAVAILABLE；原始inode/path/UID/errno/command保留在XML与提取的access-receipts.json。它证明这台macOS和这些隔离scope上的访问snapshot，不证明全部旧service凭据、privilege escalation、所有aliases/copied exports、权限后续不变或stock binary矩阵。`authority=ACCESS_SNAPSHOT_ONLY`、`stockBinaryMatrix=NOT_EXECUTED` 固定保留，不能从VERIFIED推导生产WRITABLE或整个fallback Gate通过。

复现（Unix系统Python、`nobody`及noninteractive sudo实际可用；不可用时physical tests为NOT_EXECUTED/skip，原无特权reference测试仍执行）：

```bash
bash scripts/unified-wal/run-local-storage-tests.sh 'SegmentNewScopeAccessTest,StartupReadinessFactsTest,StartupReadinessHarnessTest,ZKProfileStartupRequirementsStoreTest,SegmentProfileInitialSocketClientTest#physicalCredentialAccessFeedsOriginalReadinessBridge+readinessBridgeUsesActualRecoveredFactsAndOriginalPublicationSequence'
/usr/bin/python3 -I -B scripts/unified-wal/probe-new-scope-access.py --timeout 10 < isolated-scope-request.json
mvn -o -P '!cargo-zigbuild' -pl bookkeeper-server -am spotless:check checkstyle:check
mvn -o -P '!cargo-zigbuild' apache-rat:check
```

request包含 `oldUser`、`oldRoots`/`newRoots` 两份各四项 `{kind: JOURNAL|LEDGER|INDEX|ARENA, path: <absolute canonical directory>}`；所有目录必须已经存在。命令中的JSON是操作者提供的隔离输入，本轮未新增或生成生产配置。

父源码 `7f469625894fcb82c108074429c6cc8e83004be2`。初始qLtUZ5HV五项在known-file反例前；之后green六项在最后partial marker清理前，保留stage绑定，不叠加计覆盖。最终N9E8hDcb五类31项（access4/socket2/harness11/facts6/requirements8）零failure/error/skip，1078当前inputs全部匹配，实际Unix权限和原startup组合均执行。Spotless/Checkstyle exit0、Python源码compile通过且移除本次bytecode；root RAT仍exit1/443父HEAD历史路径、无新增。原模型语义未改，不新增/重跑TLC，不把旧model PASS移绑为Unix proof。archive `/Users/liusinan/.codex/artifacts/unified-wal/new-scope-access-evidence-hnn6m1xl` 保留原run/command/receipt/stage；red probe及test源码已按原hash恢复，1078 red inputs全部核对，可重现directory-only假成功。

完整old-service disposition/OS-service范围issuer、真实stock old binary与pre-storage-open全矩阵、default EmbeddedServer/bootstrap/SDK routing和其余综合验收继续OPEN。B14/B15整体仍未接受；步骤12/14/15与生产Gate保持PARTIAL/OPEN。Linux/NUMA、真实掉电、正式性能本轮NOT_EXECUTED，完整目标继续active。


## 实际旧 Classic episode 的有序关闭与 new-scope 组合

2026-10-12，`SegmentClassicScopeEpisodeTest` 用actual old UID的独立JVM运行原 `EmbeddedServer`/Classic Bookie，隔离loopback端口、ZKnamespace及old journal/ledger/index roots。原metadata driver、registration manager、BookieNettyServer、request processor、Bookie/Journal/SyncThread/ledger storage与lifecycle stack均走当前真实实现，没有mock server或伪造注册。old Arena为Classic不使用的guard目录。new scope仍使用另一UID和前节实际access probe，本轮不创建系统账户、修改生产路径或启动部署服务。

child真实注册后输出完整nonce/PID/UID/BookieId、registration path/original session owner/czxid、四类canonical roots与实际加载的EmbeddedServer/BookieImpl class SHA。parent再次读取actual registration并匹配birth/owner；原SDK在该Bookie完成E/W/A=1的CRC32C Add及读取，成功确认后关闭ledger/client。child接收到同nonce的CLOSE才调用原stack.close，再关闭明确自持有的metadata driver；server与Bookie running均false且exit code0后才原子发布CLOSED观察。它在CLOSED后有意继续等待EXIT，验证component close/registration消失并非actual process terminal。

只有原wrapper正常exit0、captured actual Java ProcessHandle也已退出、同nonce/binding CLOSED存在且原writable/readonly registry均fresh absent，owned Episode才能返回drained观察。仍存活的closed JVM不能取得DRAINED；caller不能把缺registration或自己填写的enum当成它。异常路径让该实际JVM在已确认写入后halt37，不执行有序close或产生CLOSED；ZooKeeper TCP断连仍可保留ephemeral至session过期，测试等待真实expiry再fresh读absence，仍拒绝DRAINED。absence不是永久停写、wipe、访问屏障或decommission证明。

`actualOldClassicDrainAndCredentialAccessFeedOriginalReadinessBridge` 将该owned observation的actual old BookieId/roots/UID和有序关闭结果，连同actual access NONE/READ_WRITE导入原fallback输入，再read/check冻结startup source、构造actual replay graph并运行原Harness/publisher/admission/consumer。两个rejoin实例各先完成old episode关闭，oldScope未真实排空时不能进入该转换；正常consumer与rejected scheduler/demote均有验证。原new target是既有隔离Profile dataset，首次创建与初始安装发生在fixture setup，本组合只证明old关闭后这次replay启动顺序，不冒充完整migration或首次new-scope bootstrap。

Classic没有本原型的native Segment storage-incarnation字段；转换用本次实验分配并绑定old roots/BookieId/session/PID的episode UUID作为admin old-scope标识，不把它写入Classic格式或宣称由旧Cookie原生提供。该受控旧实例的DRAINED观察由程序自身持有的process lifetime产生；没有推广到外部进程、所有service accounts/aliases、部署管理器、released stock binary或旧数据永久不可重启。前节access工具仍固定oldServiceDisposition=NOT_VERIFIED，因为它只验证文件访问；actual old episode由本节额外提供，层次不得混同。default EmbeddedServer Profile hook与production authority issuer继续OPEN。

复现（与前节相同的真实Unix身份前置，不可用则明确NOT_EXECUTED/skip）：

```bash
bash scripts/unified-wal/run-local-storage-tests.sh 'SegmentClassicScopeEpisodeTest,SegmentNewScopeAccessTest,StartupReadinessFactsTest,StartupReadinessHarnessTest,ZKProfileStartupRequirementsStoreTest,SegmentProfileInitialSocketClientTest#actualOldClassicDrainAndCredentialAccessFeedOriginalReadinessBridge+physicalCredentialAccessFeedsOriginalReadinessBridge+readinessBridgeUsesActualRecoveredFactsAndOriginalPublicationSequence'
mvn -o -P '!cargo-zigbuild' -pl bookkeeper-server -am spotless:check checkstyle:check
mvn -o -P '!cargo-zigbuild' apache-rat:check
```

父源码 `25b7913260144d0e96874ec865db934afb322501`。初始EvBvsNwx的一项失败发生在已完成主体关闭后的fixture cleanup：old UID可清空old目录，却不能移除其new UID持有的parent entry；改为old UID仅清空自有内容、new UID移除自持有parent，不改变生产权限。单episode green保留原stage；BePxQPHW含三个组合PASS，另abrupt场景因过早假设ephemeral随TCP退出而立即消失失败，已按实际session expiry等待，未删除有效判定。corrected五项通过；最终6ZXduv7I六类34项（episode2/access4/socket3/harness11/facts6/requirements8）零failure/error/skip，1094当前inputs全部匹配，collector包含实际classic/lifecycle/registry producer源码。

最终五份drain receipt来自三个不同BookieId/nonce/actual process episode，重复查询不计新覆盖；所有old native UID为本机4294967294，class字节身份和original registration owner/birth保留。另有10份access观察。Spotless/Checkstyle exit0；RAT仍exit1/443父HEAD已有路径，无新增。原semantic模型不变，本轮未重跑TLC；实际PID/UID/SDK/journal/driver退出不能由旧模型PASS生产。归档 `/Users/liusinan/.codex/artifacts/unified-wal/classic-scope-episode-evidence-y39dj4hz` 保留所有stage、原source/command/receipts、失败child日志和checksum，不移绑历史PASS。

这是owned isolated current-build Classic episode的关闭/排空与新scope replay组合，released-old-binary全矩阵、外部old-service authority/永久退役、default bootstrap/SDK routing与综合验收继续OPEN。B14/B15整体仍未接受，步骤12/14/15及生产Gate保持PARTIAL/OPEN；Linux/NUMA、真实掉电、正式性能本轮NOT_EXECUTED，完整目标保持active。


2026-10-12 [显式Profile listener与actual network drain](../profile-wire/README.md#显式-profile-listener-与实际网络-credit-排空)已复用original graph/pipeline并管理native parent/child、worker handoff与receive ledger终态；cold/frame/backing保留至实际归还，caller future不能伪造drain，borrowed graph/cold/loops由caller续接。actual mTLS/held cold/bind failure/queued accepted socket验证完成，46项Java/13配置模型得到预期结果、style通过，RAT443历史问题无新增。它是full startup owner的必要接线，尚未把required/physical proof、端口和registration组成default服务入口，生产Gate继续OPEN。

## 显式 Profile 服务启动与 owner 排空

2026-10-12，`SegmentProfileService` 将原 graph、DLR receipt、session admission、listener 和 delete consumer 接成一个显式隔离服务 episode。构造 owner 不打开目录、创建 graph 或绑定端口；`start(existing)` 由外部 cold caller 执行，先读取原 immutable requirements、运行原 version/scope/new-scope 检查，再核对 fallback 的 ARENA/INDEX 根与将要打开的原生目录。已存在的祖先通过 real path 解析，未创建的后缀保持计划路径；错根、相对根及与任一 old root 的祖先/后代重叠拒绝。此检查只绑定路径，不生产 Unix access、old-service disposition 或永久隔离事实，物理 proof issuer 仍由调用方提供。

检查通过后才构造原 `IsolatedSegmentStorage`，冻结 requirement birth、绑定原 delete source 和 pending admission、验证相同 publisher stores，然后创建或恢复真实 graph。DLR owner 在 storage open 后创建；实际 native listener bind 成功后，用实际 IP/端口产生 `tls://` endpoint（支持随机端口），再执行原 catch-up → recovered facts → Harness/DLR/persistent/info/writable → admission → follower。调用方填写的 endpoint 不代替实际 bind 地址；HELLO 与注册使用同一 readiness generation。实际 bind 失败不发布 readiness；失败启动走同一 owner 的关闭，未实际排空时保留 owner 和 locks，不用失败/cancel/timeout 表示清理成功。

`close(timeout)` 先退休 episode、原 admission 和 receive ledger。启动仍在执行时返回 false，启动线程保留自己的真实工作，晚到结果不能重新开放该 attempt；它终结后才进入清理。正常关闭先尝试 demote owned registration，再等待 listener 的 native parent/child/worker handoff 及 cold/frame/output/backing credit，随后执行原 storage follower、owner、DATA/I/O/pool drain，最后关闭 DLR writer lock。false 保留实际存储锁供后续 drain；demotion 未确认或实际 drain 出错保留异常，不返回成功。timeout 是本地 drain 截止时间，metadata demotion 另受原 store timeout 约束。ZK client、TLS、cold executor、scheduler 和 event loops 均借用，调用方必须等真实 drain 后才能关闭它们；没有新增线程、重试队列或同名持久化框架。

四项新增真实组合测试覆盖：missing/PREPARED/minimum reader2/UNKNOWN old disposition/错 Arena proof 在 graph 构造与目录创建前拒绝；占用端口导致 bind 失败后可真实重新打开 graph；三个首次创建的隔离服务经实际注册发现、mTLS/HELLO、原 initial coordinator 和 SDK 完成 Add/read。阻塞实际 DATA force 时，关闭超时仍保留 native lock，其他副本的合法 quorum ACK 不被误称目标已排空；实际 force 终结后关闭并以 generation2 restart，再经注册连接读取已确认 entry。另一测试在原 ZK publication multi 前暂停，stop 返回 false、保留锁且退休 admission，原 RPC 晚到后不能产生 writable；真实终结后锁可重新获得。

前三项以显式 typed fallback 输入验证机械接线，不能证明物理授权。第四项先运行实际 old UID 的当前 Classic JVM/SDK 写读、有序 stack/driver close、actual process exit 和原 registration 终态，再用真实双 UID access probe 的 canonical roots 接入此服务 owner 的 **existing dataset restart**。fixture 的初始 dataset 仍先前创建；该组合不是首次 migration、外部服务永久退役或 released old-binary 矩阵。Classic episode UUID 仍为实验 admin scope 标识，access 仍为 ACCESS_SNAPSHOT_ONLY。

复现：

```bash
bash scripts/unified-wal/run-local-storage-tests.sh 'ZKProfileStartupRequirementsStoreTest,ZKProfileEphemeralRegistrationStoreTest,SegmentLocalReadinessStoreTest,SegmentProfileInitialSocketClientTest#ownedProfile*+ordinaryDataClient*+ordinarySourceQuota*+ordinaryDataRejection*+ordinaryTimeout*+ordinaryServerForce*+ordinaryReply*+writerStop*+ordinaryWriterClose*+readinessBridge*+automaticDeletePublication*'
mvn -o -P '!cargo-zigbuild' -pl bookkeeper-server -am -DskipTests spotless:check checkstyle:check
mvn -o -P '!cargo-zigbuild' apache-rat:check
```

父源码 `08a15da7b712c880ebb1400b348959e78791b6e0`。最终 `vlh2i7CZ` 四类58项（requirements8/ephemeral19/DLR10/socket21）零 failure/error/skip，1096 inputs 全部匹配最终源码；实际 old UID、access 和 SDK 组合已执行。最初2项、随后3项的 green 保留原 stage；两次阶段失败来自新增 negative fixture 把 case3 当作三节点列表索引，已修正，不计生产 bug 红，未弱化断言。最终 Spotless/Checkstyle exit0；root RAT exit1/443 unapproved 全为父 HEAD 已有路径，无新增，不称 CI 全绿。原语义模型未改，本轮未重跑 TLC，也没有从旧模型移绑服务或原生路径证明。

archive `/Users/liusinan/.codex/artifacts/unified-wal/profile-service-evidence-c0hy9i94` 保留原 stage inputs/log/reports、最终1096份源码快照、实际 UID/process/access receipt、compiled service class SHA、style/RAT 和 checksum。当前接通显式服务启动/发布/SDK发现/关闭/restart；default EmbeddedServer/配置入口、完整外部 physical authority、released stock/pre-storage-open 矩阵、snapshot/handoff/GC/decommission、full-bytes/兼容/正式性能/运维与综合验收继续 OPEN。默认 OFF/SYSTEM/PARK 和其他 disabled 能力不变；步骤11/12/14/15及生产 Gate 保持 PARTIAL/OPEN，Linux NUMA/真实掉电/正式性能本轮 NOT_EXECUTED，完整目标 active。

## EmbeddedServer 显式 Profile 分支与 lifecycle

2026-10-12，原 `EmbeddedServer.Builder.build()` 在构造 stats/Classic metadata driver、registration、ledger manager、目录、cookie、ledger storage 之前选择显式 `experimentalProfileService(owner, existingStorage)` 分支。没有 opt-in 时仍是原 Classic 默认；没有增加默认启用的配置属性或将 Profile graph 塞入 Classic LedgerStorage。Profile 分支只组装原 stats service 与同一 owner 的 lifecycle component，构造 stack 不打开 Profile storage；`stack.start()` 调用前节真实 pre-open/recovery/bind/readiness 路径。该模式的 Classic BookieService/registration/ledger-manager/directory getter 为 null；不得用于 Classic admin/storage API。

显式分支拒绝混入 caller-supplied Classic resources、Classic auto-recovery/data-integrity/HTTP/scrubber/read-only 和 extra components，不能静默忽略这些已配置能力。stats provider 的原 borrowing 规则保留；Profile 的 TLS/ZK/cold/scheduler/loops 仍由 caller 提供并持有至实际 drain。当前 owner 构造仍是内部实验装配；配置文件/bootstrap factory、外部 authority issuer 和 Main 命令入口的完整装配尚未交付，不能把这个 builder hook 宣称为默认可部署服务。

Profile component 复用原 LifecycleComponent/stack/listener 接口，但 STARTED 仅在原 owner 成功且 episode 未退休后成立。启动失败通知所配置 exception handler 后仍抛出失败，即使 handler 返回也不能让 stack 继续启动后续组件。stop/close 先走原 owner 实际排空；timeout、启动尚未终结或 demotion/drain 异常向上传递，不提前标 STOPPED/CLOSED，也不吞掉异常。原 stack 的 reverse close 因此停在未终结的 Profile component，stats 及更早资源不能继续释放；实际终结后可重试。一个 owner 只绑定一次 lifecycle，stop 后不能重启同 episode，restart 使用新 owner/publisher 与原 existing dataset。

三项新增真实组合验证：在不可用的 Classic metadata URI 和未创建的 Classic journal/ledger/index 路径下，组装/start 三个 Profile stack、注册发现、mTLS、原 initial coordinator、SDK Add/read、generation2 restart；held DATA force 时 stack close 真实超时并保留 STARTED/native lock/stats，force 终结后才 CLOSED。missing requirements 在 storage constructor/目录创建前拒绝，exception handler 返回仍非 STARTED；冲突 auto-recovery 配置在 build 前拒绝。真实 publication multi 暂停时，stop 不能取消实际 RPC或标终态，晚到结果不能开放 writable/STARTED，实际终结后才能关闭。原四项 EmbeddedServer 单测保留，当前真实 old-UID Classic episode 路径也在同次 Profile 组合回归中执行。

复现：

```bash
bash scripts/unified-wal/run-local-storage-tests.sh 'TestEmbeddedServer,SegmentProfileInitialSocketClientTest#embeddedProfile*+ownedProfile*+ordinaryDataClient*+ordinarySourceQuota*+ordinaryDataRejection*+ordinaryTimeout*+ordinaryServerForce*+ordinaryReply*+writerStop*+ordinaryWriterClose*+readinessBridge*+automaticDeletePublication*'
mvn -o -P '!cargo-zigbuild' -pl bookkeeper-server -am -DskipTests spotless:check checkstyle:check
# 等构建终结后再扫描，避免 target class 正在更新。
mvn -o -P '!cargo-zigbuild' apache-rat:check
```

父源码 `9796c5c77d8809fbcadc0fde1ea31d77ad062581`。最初2项 green 保留原 stage；扩大28项的一次失败是测试把 A=2 ACK 当成指定第三个目标已完成。测试改为以同坐标/同字节取得该目标自己的 durable reply 后再断言读取，保留原 quorum 及 FOUND 断言，不降低生产 ACK 契约。最终 `XM6r1Dpb` 两类28项（原 EmbeddedServer4/socket24）零 failure/error/skip，1100 inputs 全部匹配最终源码；最终 Spotless/Checkstyle exit0。首个并行 RAT 因编译更新 class 的 NoSuchFileException 未形成许可结论；构建终结后独立 RAT exit1/443，全为父 HEAD 路径，无新增。原语义模型未改，本轮不重跑 TLC，不移绑旧 PASS 为 native startup/lifecycle 证明。

archive `/Users/liusinan/.codex/artifacts/unified-wal/embedded-profile-evidence-28m1cmu4` 保留 stage 输入/log/reports、最终源码与当前 Classic/UID/access receipt、style/RAT/checksum。该里程碑交付显式 EmbeddedServer builder/lifecycle hook，配置 bootstrap、完整外部 physical authority、released stock矩阵、snapshot/handoff/GC/decommission、full-bytes/正式兼容/性能/运维及全16步综合验收继续 OPEN。默认 OFF/SYSTEM/PARK 与 disabled 能力不变，步骤11/12/14/15及生产 Gate 保持 PARTIAL/OPEN，Linux NUMA/真实掉电/正式性能本轮 NOT_EXECUTED，完整目标 active。

## 配置转换与显式 bootstrap

2026-10-12，`SegmentProfileService.fromConfiguration(ServerConfiguration, key, validator, startup, transport)` 将原配置对象中的显式本地参数冻结为原 IsolatedSegmentConfig/ExecutionGroup.Settings/Storage.Limits/BatchLimits，再构造同一服务 owner。调用不打开 metadata、目录、文件或端口；Bookie/incarnation/单 Arena UUID 从 supplied startup facts 取得，实际 start 仍需原 persistent requirements/source birth、完整 header identity、native root binding、recovery 和 readiness 校验。配置文本不能生成 physical access、old-service disposition、credential 或 committed operation authority。

`Startup`/`Transport` 与原 control/read 配置现在提供公开的实验装配接口，复用原 typed stores、authorizers、TLS、receive budget 和有界 cold/loops；没有增加另一套 backend、持久化格式或线程框架。配置对象在 factory 返回后再修改不改变该 owner 的布局/执行/预算。所有数值要求整数、精确 int/long 范围；拒绝多值、未知 `experimentalSegment*` 键、未知 mode、相对目录、missing mandatory bounds、非法单项及跨 layout/worker/buffer/batch 组合。prefix 检查遍历实际 keys 后显式匹配，不能使用 Commons `getKeys(prefix)` 把平铺键漏掉。

字段名均在下表短名之前添加 `experimentalSegment`，例如 `experimentalSegmentMode`。各预算是原组件预算，不是 full-bytes 整体验收。

| 短名 | 要求/默认 |
|---|---|
| Mode | 默认 OFF；factory 只接受显式 ISOLATED |
| Directory | 必需的独立绝对目录 |
| LogicalShards / DataFiles / BytesPerFile | 原默认 4 / 2 / 16777216；持久布局在启动时匹配原 manifest/header |
| MaxPayloadBytes / MaxPendingEntries | 原默认 1048576 / 256 |
| Backend / Affinity / NumaPolicy / WaitStrategy | 原默认 FILE_CHANNEL / OFF / SYSTEM / PARK；沿用既有支持范围，unsupported 显式拒绝 |
| Workers / IoThreads / ControlSlots / Buffers / BufferBytes | 均必需，沿用原 execution bounds；worker 不超过固定 shards/pending |
| InputBudgetBytes / BufferBudgetBytes / StartupTimeoutMillis | 均必需；buffer budget 覆盖原 aligned buffers 与 input budget |
| ControlLogBytes / HotEntries / IndexedEntries | 均必需，沿用原 control/hot/index bounds |
| PoolBlocks / MaintenanceBlocks | 均必需，合计不超过单 DATA file 的真实块容量 |
| BatchBytes / BatchEntries / BatchDelayMillis | 均必需，沿用原 batch limits，并匹配 buffer/最大 entry envelope |

正常使用顺序是：外部 issuer/调用方提供已验证 startup/authority 与借用的 transport 环境，调用 factory，再向原 EmbeddedServer builder 显式提交该 owner。无 Mode/默认 OFF 不启用 factory；原手工 owner opt-in 仍为独立显式接口。配置为 ISOLATED 却未提交明确 owner，或者有未知 mode/key、多值参数时，原 `EmbeddedServer.build()` 在全部 Classic 构造之前拒绝，不能从缺失 authority 自动退回 Classic。**当前 Main 的裸配置入口仍不能自行生产外部 proof/resource 环境，ISOLATED 会 fail closed**；完整 Main 装配、外部 authority issuer 和 owned borrowed-resource bootstrap 继续 OPEN。

原服务组合测试现实际写出 properties 文件，经 ServerConfiguration.loadConf 加载后走此 factory：startup rejection、真实三节点 EmbeddedServer/注册发现/mTLS/原 coordinator/SDK/read、held force/stop、generation2 restart 和 actual old Classic/双 UID access replay 均保留。新增负向覆盖错误 worker/pool/buffer/input/hot/batch、非整数、unsupported backend/spin、relative path、missing indexed bound、unknown advanced Range 键及多值 mode；未构造 graph/未创建目录。改变原配置的 BufferBytes/Affinity 后，已冻结 owner 仍按 OFF/SYSTEM/PARK 真正启动并发布。

复现：

```bash
bash scripts/unified-wal/run-local-storage-tests.sh 'TestEmbeddedServer,SegmentProfileInitialSocketClientTest#profileConfiguration*+embeddedProfile*+ownedProfile*+ordinaryDataClient*+ordinarySourceQuota*+ordinaryDataRejection*+ordinaryTimeout*+ordinaryServerForce*+ordinaryReply*+writerStop*+ordinaryWriterClose*+readinessBridge*+automaticDeletePublication*'
mvn -o -P '!cargo-zigbuild' -pl bookkeeper-server -am -DskipTests spotless:check checkstyle:check
# 构建终结后单独执行。
mvn -o -P '!cargo-zigbuild' apache-rat:check
```

父源码 `6a043e7e45bf4b63268e2ffc3df7f45f39a74414`。初始16项中两个负向真实失败：Commons namespace prefix iterator 未遍历这些 flat keys，多值 mode 与未知键绕过检查；改为实际 key 枚举，原断言保留，16项 green。随后实际 properties 加载与扩大回归的最终 `1fAkbzBj` 两类30项（原 EmbeddedServer及配置5/socket25）零 failure/error/skip，1100 inputs 全部匹配最终源码；19份 settings/properties SHA 回执记录不同测试实例，不计额外覆盖。最终 Spotless/Checkstyle exit0，独立 root RAT exit1/443，全为父 HEAD 路径，无新增。模型语义不变，本轮不重跑 TLC，不移绑旧模型为配置/物理证明。

archive `/Users/liusinan/.codex/artifacts/unified-wal/profile-bootstrap-evidence-o6zzfc8e` 保留 red/green/最终 stage inputs/log/reports、最终源码、实际 config/Classic/UID/access receipt、style/RAT/checksum。当前完成配置转换和可调用的显式 programmatic bootstrap；Main/resource/environment 完整装配、外部 authority/stock矩阵、snapshot/handoff/GC/decommission/full-bytes/正式兼容/性能/运维与全16步验收继续 OPEN。默认 OFF/SYSTEM/PARK 和 disabled 能力不变，步骤11/12/14/15及生产 Gate 保持 PARTIAL/OPEN，Linux NUMA/真实掉电/正式性能本轮 NOT_EXECUTED，完整目标 active。

## 同一服务 episode 的 owned 资源初始化与终止

当前 TLS ownership 增量见[Owned Profile TLS 的 native 引用与服务终态](#owned-profile-tls-的-native-引用与服务终态)；下述2026-10-12回执保留原实现及输入绑定。

2026-10-12，`SegmentProfileService.withOwnedResources` 在原参数快照后返回一个尚未打开资源的 owner。`start` 才初始化同一次 `SegmentProfileOwnedResources`：一个 native ZooKeeper/原 watcher manager，原 ZKMetadataClientDriver/ledger-manager factory/AbstractZkLedgerManager，bounded cold executor、delete scheduler 和独立 acceptor/worker loops。原 driver 显式借用该 exact ZK client，后续 requirements/persistent/publisher/reservation/metadata store 全用同一 client 与 namespace；没有构造 Classic registration manager、目录、cookie 或 LedgerStorage。Metadata URI/session timeout 使用原配置，三个 Profile namespaces/ACL、实际 startup policy、有限 worker/queue/poll bounds 由明确 Settings 提供；required source/delete assignment 必须已 commissioned，不在启动中补造。

控制路径复用原 install/activation/replacement/repair/delete authority readers，按原 purpose 选取，沿用 caller 的 static/exact Authorizer 与 nonblocking read policy；不是 auth-only 或 caller enum 授权。原 owner 的 native root/header/recovery/catch-up/Harness/registration/admission/consumer/SDK 路径继续使用。TLS factory 仍明确借用，receive budget 归这个 listener episode；secret key与physical/old-service proof仍来自外部 issuer。没有新重试层、每 entry MetadataStore 或另一套 control/data 格式。

初始化发生在 caller 已持有的 owner 内；部分 metadata/连接初始化失败，或者已初始化但缺 required source，原 `start` finally 会退休并走同一清理路径，失败/未排空时保留 handle 与异常。close-before-start 不创建线程/连接，旧 borrowed-resource factory 不关闭 caller 的资源。真实服务 drain 完成后才 shutdown cold/scheduler并等待 actual termination；`AbstractZkLedgerManager.closeAndAwaitTermination(timeout)` 确認其原 scheduler，保留 legacy close 的原非阻塞行为。随后确认原 factory/driver close、原 `ZooKeeper.close(wait)` 的 send/event-thread terminal，再等待 worker/acceptor termination；false/异常保留后续资源与状态供重试，不能先释放 ZK 或网络 loops。

原 ZK 3.9.5 的 close(wait) 会先调用 synchronous close，再分别 join/check send/event threads；其 closeSession 与原 sender join 在 bounded wait 检查之前。因此本接口的 deadline 不能宣称为所有底层 RPC/join 的硬总截止时间；metadata demotion同样有原 store timeout。实际完成仍必须得到各原资源的终态，不用调用 close、caller timeout/cancel、ZK状态或内部 flag 单独证明排空。SDK/目录权限/current-build Classic证据不升级 released stock、永久服务退役或full-bytes。

三项新增真实测试：三 owned Bookie episode 用原 properties/metadata URI、独立 ZK与原 LM、各自 cold/scheduler/loops完成 mTLS/initial coordinator/原 SDK Add/read与generation2 restart；held DATA force 时网络/storage owner未排空，全部外部资源仍未终止，实际 force终结后才完成全资源关闭。另在原 metadata scheduler 上保留一个实际未终结任务，close 返回 false、ZK/worker未关闭；该任务真正退出后同 owner重试成功。missing requirements与原 TCP/ZK handshake timeout均在无 storage constructor/目录创建时失败，caller仍持有handle，已创建的真实资源终结。fixture改为对已 native closed 的 listener读取closeFuture，不在其event loop已经终止后再次投递close。

复现：

```bash
bash scripts/unified-wal/run-local-storage-tests.sh 'TestEmbeddedServer,SegmentProfileInitialSocketClientTest#profileConfiguration*+embeddedProfile*+ownedProfile*+ordinaryDataClient*+ordinarySourceQuota*+ordinaryDataRejection*+ordinaryTimeout*+ordinaryServerForce*+ordinaryReply*+writerStop*+ordinaryWriterClose*+readinessBridge*+automaticDeletePublication*'
mvn -o -P '!cargo-zigbuild' -pl bookkeeper-server -am -DskipTests spotless:check checkstyle:check
# 等构建结束后执行。
mvn -o -P '!cargo-zigbuild' apache-rat:check
```

父源码 `1e7c0c37a9ea73767dd29f5a3b5fc3f53e4ef42f`。最初 compile 失败是cleanup再次调用getFactory的checked MetadataException；改为保留实际已创建factory，未创建时不重试初始化。首个 SDK测试完成实际关闭后在fixture重复close已终结event-loop的channel时error，按原closeFuture修正，未弱化drain断言。最小三项green后，最终 `uuTpSN6F` 两类33项（原 EmbeddedServer/配置5/socket28）零 failure/error/skip，1103 inputs全部匹配，collector补齐metadata client driver和watcher producer。最终Spotless/Checkstyle exit0，独立root RAT exit1/443父HEAD路径，无新增；模型语义未改，本轮不重跑TLC，不移绑旧PASS为原生资源事实。

archive `/Users/liusinan/.codex/artifacts/unified-wal/profile-owned-evidence-7vj7l7nd` 保留stage输入/log/reports、最终源码、actual config/Classic/UID/access receipt、ZK close字节码核对、style/RAT/checksum。当前提供可调用的 owned ZK/metadata/executor/event-loop bootstrap，完整 Main/外部 physical authority/TLS native ownership、released stock矩阵、snapshot/handoff/GC/decommission、full-bytes/正式兼容/性能/运维及全16步验收仍 OPEN。默认 OFF/SYSTEM/PARK 和 disabled 能力不变，步骤11/12/14/15及生产 Gate仍 PARTIAL/OPEN；Linux NUMA/真实掉电/正式性能本轮 NOT_EXECUTED，完整目标 active。


2026-10-12 [Assignment handoff 的 EFFECTIVE 与 G2/G3 接续](../profile-wire/README.md#assignment-handoff-的-effective-与-g2g3-接续)已把原graph gate退役、owned registration删除、NON_WRITABLE投影与native assignment/head更新放进原publisher同一ZK multi；原type15 force/apply/checkpoint/replay接续effective progress，原事件身份保留。三目标真实G2/G3 READY、SDK Add/close、新旧事件与混合snapshot、健康文件旋转重启、native丢回复/held callback与type15 UNKNOWN已有验证。79项原阶段Java（server 71/common codec 8）/最终21项补验及35有限模型配置通过，1929个最终源码输入核验一致，style通过、RAT443父HEAD无新增；完整Gate不提升。retention/offline/backup-restore、accepted terminal decommission/unrecoverable/history GC、其他required authority/default routing/full-bytes/兼容/正式性能/运维/全16步验收继续OPEN。

## 固定存储池与网络共享 DATA 预算

2026-10-13，接续父提交`6c99ec81c6ac8e3913db0e1045c86d94bd5d4711`。原ProfileService的网络quota与execution固定buffer budget分别有界，却未将固定池容量纳入同一Bookie DATA额度。现在service在requirements/root绑定复核后、IsolatedSegmentStorage构造及任何pool分配之前，复用原ProfileReceiveBudget/Credit保留`buffers * (bufferBytes + 4095)`；它覆盖原pool每个exact-capacity直接root及对齐余量，不为每entry重复预留最大buffer。额度不足以IOException终止，storage/目录不打开、不发布WRITABLE；总额度和CONTROL/UNCLASSIFIED保留份额不提高，也没有新增配置或默认路径。

原Settings.reservedBufferBytes用于该startup reservation和execution诊断，仍由已有Settings的overflow、alignment、input+buffer bound检查约束。DataWorkingSet只是原Credit的非connection owner handle，不占connection slot或授予authority；Credit capacity使用long以保留可能超过2 GiB的总固定容量，ByteBuf allocation/response split仍为原int capacity。原network snapshot继续由同一connection allocator计费，并受SegmentEntryInputs逻辑上限约束；不把input cap再次固定扣账。固定pool已保留的提交工作集不能被network DATA分配耗尽，已持有的control额度继续独立可用。

closeConnectionsAndDrain退役新准入，只等待真实connection slots以及这些connection的全部backing/workspace bytes，包括旧lane、closed transport后的retained root和cold任务。原closeAdmissionAndDrain继续等待全部lane bytes/slots，含固定storage capacity；两者均返回copy，callback在原账本monitor外完成。listener先等原accepted socket/worker交接及connection drain，service再等实际storage I/O/owner shutdown、pool leases归还和root.release；原storage/local关闭成功后才归还DataWorkingSet，owned resources仍按原全账本/metadata/threads次序关闭。caller timeout、不完整startup/cleanup、held force和中断都不能归还活pool reservation。这样不会让listener等待必须由下一阶段storage close释放的固定容量而形成关闭循环。

Java`unified-wal-local-vgpTyehj`首轮4类38项0 failure/error/skip。最终`unified-wal-local-zPPNgjaU`4类51项（allocator11/decoder23/pipeline1/socket16）0 failure/error/skip；`unified-wal-local-NjqynmQ3`补齐owned resources 2项及execution group 14项，其中5个Linux专用Assume条件跳过，11项实际通过。最终两批合计5类67项、62实际通过/5跳过，无failure/error。新增单测验证fixed与network共享DATA耗尽、CONTROL仍可用、最后retained view、connection drain先于full drain、retired拒绝和3,000,000,000-byte long容量；真实service quota满时拒绝目录/storage open。现有SDK/注册发现、EmbeddedServer、真实文件force期间close、健康restart、新旧scope/Unix access、publication stop/held metadata以及pool初始化失败/跨线程归还继续覆盖；held force时总账保留至少全pool容量，实际close后归零。

`unified-wal-model-9cju7__p`共17原配置，2完整正向/15指定反例，覆盖原root守恒与service owner投影；新增两类drain的完整状态空间未单独建模，不将TLC旧抽象当作全内存证明。1115 Java与1102模型inputs去重1946，全部实际source SHA匹配；每个stage输出及parent designInputs保持原绑定。证据`/Users/liusinan/.codex/artifacts/unified-wal/shared-storage-budget-evidence-KcAJgHAl`保存原输入/命令/XML、模型输出、冻结源码和checker诊断。最终Spotless/Checkstyle exit0；所有Java/model/checker终止后的独立RAT exit1，443 unapproved路径逐项存在父HEAD、无新增，不称CI全绿。

此增量针对原ProfileService整合入口的fixed ByteBuf root容量；单独bare storage/listener的隔离实验仍有原独立pool bounds，不称所有手工入口已共享总账。预留在partial startup期间可能尚未全部分配，used是reservation而非RSS，netty/native allocator overhead、队列/Java对象、index/hot/control持久工作集、MetadataStore/JDK/opaque TLS/kernel、caller reply与outbound cohort统一总账继续实施。full-bytes、正式性能、完整兼容和全16步生产Gate仍OPEN；默认OFF/SYSTEM/PARK及所有disabled能力不变，Linux NUMA/真实掉电/正式性能/canary本轮NOT_EXECUTED。

2026-10-13 [派生 index block cache 与共享 DATA 预算](../local-storage/README.md#派生-index-block-cache-与共享-data-预算)已将原1 MiB index block cache启用strict capacity，并在ProfileService storage open前与固定池合并预留于原DATA额度；原缓存大小与协议/格式不变。旧service只预留池的真实quota红、当前拒绝绿及最终2类34项真实Java通过；memtable/WBM并非hard native上限，所有native/对象总账与正式性能仍OPEN。本轮不重跑不覆盖该native政策的TLC，旧模型保持原绑定，默认OFF/SYSTEM/PARK及16步生产Gate不提升。

2026-10-13 [默认 SDK open 的 Profile 拒绝边界](../profile-wire/README.md#默认-sdk-open-的-profile-拒绝边界)已在原LedgerOpenOp密码/digest验证后、Classic handle/listener/recovery创建前识别mandatory Profile backlink，返回原IllegalOpException，避免当前SDK误用Classic路径。真实CLOSED旧行为红、新OPEN/CLOSED/valid-CRC future marker拒绝绿及三Bookie Classic兼容，最终2类17项通过；Main/外部issuer、默认Profile routing、released stock矩阵及全16步Gate仍OPEN，默认OFF/SYSTEM/PARK与disabled不变。

## Owned lifecycle 的实际 self-drain 拒绝

2026-10-13，父提交 `54800228d88d044972684c51e9f3e8d096ea849c`。原service的external caller检查只识别Netty loops，transport建立后还绕过owned resource检查。由owned cold/delete executor、原metadata scheduler或ZK event callback调用close时，旧逻辑先退休admission/registration/storage，再等待调用者自己的executor/thread终结；无法在该调用返回前完成，变成超时和非预期服务退休。直接AbstractZkLedgerManager.closeAndAwaitTermination在自己的scheduler上也先shutdown再等自己。

当前owned resources在已有DefaultThreadFactory上记录cold/delete实际运行Thread，结束后移除，保持原executor/queue/线程数；native ZooKeeper默认watcher仅先捕获当前event Thread再委托原ZooKeeperWatcherBase，session/child watchers合同不变。原AbstractZkLedgerManager同一single scheduler记录实际Thread，新增明确external drain caller检查；其legacy close()保持非阻塞行为。service在退休任何state前统一检查owned caller，已有transport后不绕过。由这些实际managed contexts及原Netty loops调用立即IllegalStateException；不以名称/prefix或caller枚举判断，不先demote/shutdown再表示等待失败。合法external close仍按原实际frame/storage/I/O/pool/metadata/ZK/network终态推进，UNKNOWN、held task、timeout和中断不变。

新增真实owned service回归先在旧代码 `unified-wal-local-aPMtbYaZ` RED：cold task没有抛IllegalStateException而等自身排空后返回，Future以断言失败终结。修正后最小一项通过；最终覆盖actual cold、delete scheduler、metadata scheduler和native ZK getData callback，直接metadata self-await也拒绝且不shutdown。每次非法调用后fresh真实peer仍可读、各owned resource未终结；外部调用者故意使用与metadata scheduler相同的线程名，仍能正常关闭并确认全部终态，证明检查是identity而非名字。

最终 `unified-wal-local-wYFERfkw`：metadata manager26项、真实ledger metadata创建/删除12项、selected owned socket/service6项，共44 registered/42 PASS/2既有Assume skip（ParentNodeDeletion只适用于两种hierarchical manager），零failure/error。selected范围保留三目标真实SDK/注册/mTLS/重启、held DATA force、held metadata task、部分初始化失败和Embedded lifecycle。1170个runner源码/输入SHA与最终实现逐项匹配；另补充6个与父提交相同的metadata test/base输入，合计1176份冻结源码/输入；Spotless/Checkstyle exit0；Java/checker实际终结后的独立RAT exit1，443 unapproved均在父提交、无新增。

证据 `/Users/liusinan/.codex/artifacts/unified-wal/drain-caller-evidence-htwcwae_` 保存RED/focus/final的source/input、原命令/XML、checker及诊断，旧源码与结果不移绑。没有新增线程、executor、生产队列、重试、格式或authority issuer，ordinary Add无新增metadata/hash/control调用。本次caller identity由真实Java上下文验证，现有形式模型没有这些thread identities；未重跑或移绑TLC，不声称完整A/C/D或liveness证明。

复现：

```bash
bash scripts/unified-wal/run-local-storage-tests.sh 'AbstractZkLedgerManagerTest,LedgerMetadataCreationTest,SegmentProfileInitialSocketClientTest#ownedProfileResources*+ownedProfileInitializationFailure*+ownedProfileServicePublishes*+embeddedProfileServiceRuns*'
```

完成的是同一owned服务的合法drain调用边界。Main/default SDK路由、外部required authority producer、full-bytes、backup/restore/accepted永久退役/history GC、mixed/rolling与正式性能/运维和完整16步继续推进；默认OFF/SYSTEM/PARK及disabled不变，生产/稳定Gate仍OPEN，Linux NUMA/真实掉电/正式性能/canary仍NOT_EXECUTED。


2026-10-13 [派生 index 的有界 Java 读取](../local-storage/README.md#派生-index-的有界-java-读取)已限制原native Get/iterator返回到Java的复制尺寸，正常和损坏的derived数据仍保留原语义；真实父Get分配RED与18项storage/index GREEN/style拥有实际证据。此为固定池/cache之外的Java输入边界，不把memtable/native/locator/control对象计入已闭合预算，也不提升startup或生产Gate，默认OFF/SYSTEM/PARK及disabled不变。


## Main 首版受控部署边界

2026-10-27，用户已选定首版先限已有旧服务的受控部署：已有 commissioned requirements、真实旧服务 episode 及明确外部信任输入。沿原 Main/EmbeddedServer/withOwnedResources 接线，commissioning 保持独立显式执行。旧服务处置、Unix 权限隔离、跨重启 AES key、principal/registry/ACL 等仍需要真实可信来源；配置中的 DRAINED/FORMAT_READY、注册消失或 mTLS 成功不能自行生成这些 authority。当前裸 Main 的 ISOLATED 缺 owner 拒绝继续保留，具体部署 adapter/issuer 与真实 Main 进程验收仍 OPEN；该范围选择不是可部署或生产 Gate 通过的声明。


[永久坐标根的 DATA 启动预留](../local-storage/README.md#永久坐标根的-data-预留与关闭退休)：2026-10-27父277f4d6514上，沿原单次DATA额度预留永久坐标HashSet/Key/context，缺额在storage构造/目录与index打开前拒绝；实际关闭退休共享hot/inventory根后才归还。六类618项、四种JVM布局与style验证及范围见canonical节，不提升full-bytes或生产Gate。


## Owned Profile TLS 的 native 引用与服务终态

2026-10-27，父 `3414108da6c8b30a25d5e7550b0d9b17953ba70f` 上，`SegmentProfileService.withOwnedResources` 增加明确省略 TLSContextFactory 参数的重载：冻结原 ServerConfiguration 中已有 key/certificate/trust/provider/refresh 设置，在原 owned resources.open 内创建 Profile server factory，再初始化原 ZK/metadata/executors；构造或 close-before-start 不创建 TLS/context/native loops。原接收 factory 的重载保持 borrowed，null 仍拒绝。secret key、requirements、old-service/Unix access proof、static/exact authorizer 和 read policy 继续来自原显式输入，未增加配置格式或 issuer。

私有 TLS factory 在 listener、storage I/O/owner、账本 credit、cold/scheduler、LM/driver/ZK 及 worker/acceptor 的实际终态后关闭；此前未完成的排空步骤或异常不会提前释放仍活跃的 TLS。TLS 初始化失败包装为 IOException 并保留原 checked SecurityException cause，沿原 start finally/close 清理；metadata 初始化失败同样释放已经创建的私有 native context。原 borrowed 重载关闭后 caller factory 仍能构造 handler，caller 自己等待其网络终止后再关闭。

`TLSContextFactory.closeProfileServer()` 幂等释放 factory 持有的 current context 引用并阻止再 init/造 handler，本身不宣称 engine 已排空。Profile 成功 init 后拒绝 reinit，失败 init 清空 context 并允许原 factory 重试；专用 newProfileServerTLSHandler 保留原 checked SecurityException 契约，通用 handler 在已关闭/未初始化时抛 IllegalStateException，Classic 误用此关闭 API 也明确拒绝。Classic STARTTLS/配置/refresh 路径不转移 ownership。

当前 Netty 4.2.15 的实际 OPENSSL provider 使用 OpenSslServerContext/OpenSslEngine：engine retain parent，实际 engine deallocation 释放该引用，finalizer 不能作为 drain 事实。Profile factory monitor 覆盖选择/refresh 到 engine 构造和 protocol/cipher 配置交付，成功 refresh 释放旧 factory 引用，已交付旧 engine 继续持有原 context；失败 refresh 保留当前 context；配置异常释放尚未交付 engine。无 retired-context 列表、后台 watcher、额外生产线程或普通 Add 成本。

独立刷新 contract probe 使用完全相同的 class 字节：旧 factory 原 class overlay 下实际 exit1，真实 mtime refresh 后旧 context refCnt 仍2，旧 engine 实际终结后仍1，命中 owner 泄漏断言；当前 factory 下 exit0，依次2→1→0。两次探针均显式清理 old/current native refs 至0，不依赖进程退出或 GC。基线 class/source 与原阶段输入绑定保留，未 checkout/revert 生产源码。首轮编译因新增 checked exception 接线缺失而失败、0 tests，保留原输入/源码/日志，不计产品 RED。

修正后定向17项全部通过：10项 TLS factory 与7项真实服务测试，明确 assert实际 OPENSSL context/engine，JDK回退不能计 native release PASS。成功/失败刷新仍完成实际 TLS1.3 EmbeddedChannel handshake；两个竞争测试把实际 native engine 阻塞于 context lock，验证 close/refresh 不越过 factory monitor。原三节点 SDK Add/read、健康 generation restart、held DATA force/metadata scheduler、初始化失败/close-before-start/borrowed 接口各观察相应 actual terminal 与 native refCnt。扩大五类540项，532通过、8因原Classic证书格式前置条件跳过，failures/errors均0（Bookie282、SDK socket233、Profile TLS10、入口pipeline1、Classic参数化14其中6通过/8跳过）；用例身份与1194份最终输入、native1179份输入匹配。Spotless/Checkstyle exit0；独立RAT实际exit1，仍443个既有未批准路径，路径及全部字节与父提交一致、无新增。

证据在 `/Users/liusinan/.codex/artifacts/unified-wal/owned-profile-tls-evidence-3414108d`；同一 contract probe class 的 SHA-256 为 `d6ec87db2d4e593612e2a5d2cc3a8328ea59fa68904d630030f2d7589e901811`。TLA 未改，旧模型结果不重绑；该增量不等于 TLS opaque/native/JDK 总字节已纳入预算。Main 外部 issuer/输入装载/实际部署进程、full-bytes/B11、完整G1与其余16步继续 OPEN；用户已冻结 Main 首版限已有旧服务的受控部署。生产 Gate OPEN，完整目标 active，OFF/SYSTEM/PARK 与 disabled 能力保持。


[Hot 与 restart inventory 的驻留元数据预算](../local-storage/README.md#hot-与-restart-inventory-的驻留元数据预算)：2026-10-27父d4837e0ea7上，原DATA单次启动预留加入hot/pending元数据与restart inventory，复用Layout的identity/immutable容器及direct view上界；两map高水位、各shard失败admission、旧index返回窗口计入，正常Key/context沿occupied。Inventory寿命跨terminal退休/新ledger复用，另计旧C个context，不重复Key；quota不足在storage/目录前拒绝，原actual关闭/五容器与inventory退休后退费。预算RED已复现，阶段6/600项保留原绑定；最终费用修正仅inventory函数/注释，三类260项零失败/错误/跳过，style/RAT以canonical实际回执为准。四最终JVM独立2C context图不超模型，但不证明状态数/RSS/native/完整B11。raw-unpooled backing、解析/执行器/pool其余元数据、Main/完整G1与其余16步仍OPEN；目标active、生产Gate OPEN、OFF/SYSTEM/PARK与disabled保持。


[Pooled 解析工作区与派生 pending context](../local-storage/README.md#pooled-解析工作区与派生-pending-context)：2026-10-27父5d4cd9ccda上，原DATA一次预留加入按pool几何K、owner/维护并发及P票据返回窗口计算的pooled解析工作区和PendingNormal独立context A；4KiB K20，hot重解析B不共享A。PoolSubmission在invoke前消费callback，切断旧CONTROL task到future DATA leader的回指；异常汇总/后续投递/actual drain保持。冷扫描复用page/content credit计本批元数据，按已验证n限制重读记录数，超额整批拒绝。预算与两callback实际RED已复现，6项定向通过，四JVM真实解析/Hot/Pending/A+B图不超模型；最终六类627项零失败/错误/跳过，style/RAT以canonical实际回执为准。完整read/driver/跨批扫描/compaction/pool/raw图及B11仍OPEN，不计性能/native/RSS或状态数证明；目标active、生产Gate OPEN、OFF/SYSTEM/PARK与disabled保持。


[恢复扫描的跨批工作区](../local-storage/README.md#恢复扫描的跨批工作区)：2026-10-27父9a195f5b7f上，冷路径新增一个跨批credit，按C/S与Arena实际M/R/A计entries、foundMoves、stream ranges、headers/postcoordinates及独立旧seed；indexed入口先付费再建seed，保持到实际扫描/postcheck终结，不给normal Add新增credit或扩大生产quota。目标RED复现且同方法字节GREEN，四项定向与四JVM有限图通过；初次627项整集5失败/2错误及fork超时保留；同码7项及完整SDK235项复验通过，另五类392项保持原通过，全部627用例身份匹配；style通过，RAT443原路径/原字节无新增，具体初次根因未证明，实际回执由canonical拥有。该证据不闭合真实峰值/状态数/native/RSS或B11；其余full-bytes/Main issuer/完整G1与16步仍OPEN，目标active、生产Gate OPEN、OFF/SYSTEM/PARK与disabled保持。


[点读元数据与 loan 资源退休](../local-storage/README.md#点读元数据与-loan-资源退休)：2026-10-28父c51f3ba9b5上，将 pooled reader/command/result/fresh Get 的已知元数据加入原service预留，无buffer LAC loan由原wire ResponseCredit另付；loan关闭清空三项资源，command在物理/owner双终态后退休payload。真实RED、定向/整类回执、初次cold-restart欠额与仅成功夹具同额补费、四JVM有限图及最终源码/class等价边界由canonical拥有。没有normal Add新增credit/atomic或生产quota扩容；此切片不闭合全部point-read调用者图、driver/执行器/pool、native/RSS或B11。Main具体issuer/受保护装载、完整G1及其余16步继续OPEN，目标active、生产Gate OPEN、OFF/SYSTEM/PARK和disabled能力保持。


[失败 DATA 前缀的 payload 引用退休](../local-storage/README.md#失败-data-前缀的-payload-引用退休)：2026-10-28父a08d1043d1上，actual completion 后同时摘除prefix与Hot的已终结batch/reservation根，保留未完成I/O身份、UNKNOWN坐标、原through与真实双门归还；owner失败但force成功仍UNKNOWN且无虚构quarantine。三条真实RED与同文件GREEN、相关Bookie/SDK/重启回执由canonical拥有。execution/pool已知图与终止退根、Input的U/36边界、opaque/native、Main issuer、完整G1和B11仍OPEN；完整目标active、生产Gate OPEN、OFF/SYSTEM/PARK及disabled保持。


[执行与池的固定 Java 容量和关闭退根](../local-storage/README.md#执行与池的固定-java-容量和关闭退根)：2026-10-28父c474431233上，第八项service DATA预留支付已知execution/owner/I/O/pool固定Java图及PREFER旧空容器重叠；成功native close/actual termination/input0后清多条内部执行别名，timeout/本次失败保持。旧路径两条RED、同文件定向/整类、四JVM边界与RefCnt补列的实际回执由canonical拥有。Input动态N=U/36、Thread/registry/Worker/AQS waiter、CPU/cleaner/native及完整B11仍OPEN；Main具体issuer/受保护装载、完整G1及其余16步继续推进，目标active、生产Gate OPEN、OFF/SYSTEM/PARK与disabled保持。


[Input 核心元数据与源引用退休](../local-storage/README.md#input-核心元数据与源引用退休)：2026-10-28父cab51e1c8e上，第九项service DATA预留按N=floor(U/36)支付已知Input/context/heap-root/RefCnt/NIO与claim工作区；成功release后清根并退款，Entry/bulk同步退休旧源引用。四条真实RED及同字节测试的13项GREEN由canonical记录；Driver描述符/解析图、native、Main issuer、完整G1和B11仍OPEN，持续目标active、生产Gate OPEN，OFF/SYSTEM/PARK与disabled保持。


[Driver 批次回调与重复请求引用退休](../local-storage/README.md#driver-批次回调与重复请求引用退休)：2026-10-28父2aadc71bb1上，首次deliver消费completion，duplicates/waiters在成功交接后清根；UNKNOWN与actual I/O双门、错误汇总保持。真实RED/GREEN和当前验证由canonical拥有；Driver完整容量、Main issuer、G1与B11仍OPEN，完整目标active、生产Gate OPEN，OFF/SYSTEM/PARK与disabled保持。


[Driver normal 与 recovery 的已知 Java 容量](../local-storage/README.md#driver-normal-与-recovery-的已知-java-容量)：2026-10-28父16933ee747上，第十项DATA预留支付源/preparation、独立Bookie/owner集合及整串返回窗口的已知Java图；waiter成功post后换空容器，旧九项金额保持。公式、实际验证及有限测量边界由canonical拥有；raw/compaction/JDK/native、Main issuer、G1和B11仍OPEN，目标active、生产Gate OPEN，OFF/SYSTEM/PARK与disabled保持。


## 启动前沿用既有磁盘阈值

2026-10-28在父源码`9cfe97c9102a2e67d620bfe156fe2efdeb3b790b`上，将原ServerConfiguration的diskUsageThreshold/diskUsageWarnThreshold接入显式Profile service。fromConfiguration及两个owned-resources工厂通过同一私有参数/构造路径冻结数值，caller在创建后修改原configuration不改变该episode。OFF默认和配置项不变；没有新增配置、monitor线程或资源框架。

start沿原流程先取得commissioned requirements并核对可信storage roots，再在storage、listener及readiness发布前调用原DiskChecker。新目录检查已存在parent，existing恢复检查目标目录本身；hard threshold、目录不可访问和检查失败沿原失败清理拒绝启动，警告阈值仍只使用原checker的告警，不被改成硬拒绝。此检查是本机访问/使用率观察，不产生old-service、Unix隔离、key/policy或恢复authority，Main实际issuer和受保护装载仍OPEN。

DiskChecker新增只读checkExistingDir，先检查目录/读写访问再检查使用率，不创建缺失目录。原Classic checkDir保持先检查使用率、再按原规则创建目录、再检查相同访问权限的顺序，仅抽取原权限判断共用。首次Profile接线使用原checkDir时，一个真实existing启动反例确认其意外创建缺失storage目录；改用只读入口后，该目录保持不存在，readiness/预算均不遗留。

实际测试按当前filesystem使用率选择低于使用率的hard阈值，以及有效高hard/低warn组合；未填满设备或伪造空间耗尽。原实现忽略hard配置而成功启动，真实RED为expected DiskOutOfSpaceException but nothing was thrown，1 failure、0 error/skipped。该方法自有效RED起保持原字节，同时覆盖创建后修改caller配置及warn仍可真实启动/注册。首次夹具仅1MiB而不能容纳原两个1MiB pool/maintenance block，运行停在既有layout校验；该次error独立保留、不计语义RED，改用原fixture文件容量，没有扩大生产quota。

首轮接线三项通过后，新增上述缺失existing目录反例得到真实assertFalse失败，0 error/skipped；该方法从此RED至最终源码也保持原字节。最终四项SDK定向及原TestDiskChecker整类五项共9项全部通过，零failure/error/skipped。各阶段保存自己的源码/规则、native输入、测试源码、Maven/XML，旧PASS不移绑；证据位于`/Users/liusinan/.codex/artifacts/unified-wal/disk-preflight-evidence-9cfe97c9`。

本项只交付启动时现有阈值检查：尚无持续磁盘monitor/低水位恢复、filesystem共享预算与并发维护承诺、SST/control/checkpoint保留量或ENOSPC进展验收；不会把百分比检查当作B17维护字节预留。已创建DATA增长后的余量、完整full-bytes/B11、多Arena持久路由及其余16步仍待完成，目标active、生产Gate OPEN，OFF/SYSTEM/PARK与disabled保持；Linux NUMA、真实掉电、正式性能/canary本轮NOT_EXECUTED。

最终真实SDK整类246项全部通过，与上述未修改的DiskChecker整类5项合计251个不同用例通过，0 failure/error/skipped；最终定向与SDK使用同一1206项声明输入，1179项原生输入分别为其子集，另外显式锁定本轮涉及的DiskChecker及其测试源码。原SDK方法全部保留，仅新增两个独立行为用例。Spotless/Checkstyle实际exit0；没有重跑旧compatibility corpus、released binary或TLC，原PASS不移绑。

用户2026-10-28已同时冻结后续多Arena的固定清单/显式shard→Arena映射及新incarnation变更边界，已同步RFC-0003 §4、实施清单步骤10与execution-objective；当前代码仍只允许单Arena，该决策未作为多Arena运行时验收。

独立RAT实际exit1，仍为父9cfe97c910既有434个未批准路径，路径集合及bytes全同、无新增，不称CI全绿。最终1203项非本轮修改的声明输入与父Git blob相同，两个语义RED的方法/测试字节及原Classic权限判断与创建顺序均已核对；60份相关编译class留档。六个终结run的25个文件经逐SHA/uid/gid/mode、同inode硬链接及两次无reader核对后移除原临时根，逻辑路径7403401 bytes；失败诊断、输入、日志与XML全部保留，常规文件数据块回收0 bytes。


## Main 的受控发行者与受保护装载裁决

2026-10-30，指定规划会话在`5a965e38d5272cd5b89b672fee3f53564512eb2d`只读核对后裁定：首版采用固定受控旧服务launcher发行证明、固定受保护文件loader装载，沿既有withOwnedResources → experimentalProfileService → lifecycle接入Main。此处记录实施边界，尚无新的Main实现或PASS。

launcher持有旧服务运行时建立的进程、控制通道及registration episode，只有停止准入、实际关闭、原进程终结、原registration退出且自动重启受控后才发行DRAINED；不发行DECOMMISSIONED。证明绑定旧Bookie、真实UID、commissioned旧存储scope、根和registration birth/session，并绑定目标Bookie/incarnation、commission generation、全部device/Arena/shard映射及policy/key身份。Classic没有Profile incarnation，不能把随机启动nonce替代持久scope，也不把备用ARENA guard根包装成旧Classic存储。

issuer UID、受保护输入目录及metadata信任根由外部部署指定，不能从待验文件自报。manifest、AES key、exact-scope policy有界装载并检查权限、父目录、链接/替换与完整内容绑定；properties只提供引用和运行参数。真实commissioned requirements及assignment只读验证，不在Main启动中补造。首次采用及重试绑定同一操作，健康重启验证原绑定；失败沿原owner等待实际drain，未终结保留handle，不自动恢复旧writer。

最低真实验收须由旧Classic进程完成SDK写读及真实关闭/终结后，启动真实Main进程装载受保护输入，完成Profile注册、SDK写读和健康重启，并覆盖仍存活/异常退出、换target/root、旧身份可修改、错AES key、缺commissioned输入、路径替换及未终结startup/registration关闭。多Arena及按实际引擎表达root集合仍需接入；现有typed fallback输入、访问探针JSON或单Arenafixture成功不能代替该producer/loader。Main可部署性、完整startup与生产Gate仍OPEN。


## commissioned requirements 的原生来源指纹

2026-10-31在父源码`1d8b925617d1e6c54fd053f871c1b8c072c90102`上，ZKProfileStartupRequirementsStore的真实Observation新增只读sourceIdentity，供后续固定交接loader匹配原commissioned来源。值包含实际完整节点路径、envelope原birth UUID、原生creation/modification zxid、version及原metadata字节的SHA-256；immutable version仍为0。摘要由已通过原codec及同事务birth marker核验的native读取字节计算，保持原记录与当前delete-assignment有效generation overlay分离，不重编码overlay来冒充原材料。公开值对byte[]作防御复制并按内容比较，构造该值不构成授权；只有原store的已验证读取能产生Observation。

原requirements格式、create-only provision、birth marker、publicationChecks及publisher的store/client/observation owner绑定不变。sourceIdentity getter只返回原观察内的不可变值，不执行新的metadata I/O，不改变普通Add路径。完整证明仍需绑定原native cluster/Bookie/scope及实际借用的可信store；这个来源指纹不发行旧服务DRAINED、UID/自动重启隔离、key/policy资格、bootstrap许可或服务readiness。公开SegmentClassicBootstrap、真实launcher/固定loader和Classic恢复/封口继续OPEN，完整16步目标active，Main/G1/full-bytes及生产Gate不提升，默认OFF/SYSTEM/PARK和disabled保持。

验证使用真实ZooKeeper：删除后按相同原字节和envelope birth重建node与marker，摘要/UUID/version相同而原生birth zxid不同，两个来源不相等；同一multi在不同namespace创建相同记录与marker，birth zxid/UUID/摘要相同，实际节点路径仍使来源不同。另验证真实Stat/原字节匹配、防御复制，以及原foreign-observation发布拒绝和字节codec向量。首组十项通过后增加namespace对照，最终执行`scripts/unified-wal/run-local-storage-tests.sh 'ZKProfileStartupRequirementsStoreTest,ZKProfileEphemeralRegistrationStoreTest,SegmentProfileInitialSocketClientTest#ownedProfileServiceRejectsRequirementsBeforeStorageOpenAndCleansBindFailure+ownedProfileServicePublishesBoundEndpointSupportsSdkAndRetainsDrainAcrossRestart+deleteHandoffEffectiveAllRealTargetsRejoinG2AndKeepOldEventsWithNewJournalData'`：三类33项通过（requirements 11、ephemeral registration 19、实际owned服务/SDK/重启及delete assignment handoff三项），0失败/错误/跳过；style退出0。未将先前物理Arena的421项结果移绑本源码，也不将新增来源值当作完整交接协议验收。


后续固定交接协议已由规划会话冻结，当前实现仍OPEN：固定受控launcher只管理其实际启动的受保护Classic child；先持久写入按cluster＋Bookie固定的retirement intent，再停止准入/实际排空/关闭metadata与服务/确认正常进程退出及registration消失，并完成两UID持续权限隔离后，才发布不可覆盖的证明。旧UID其他进程或已有fd、未知自动重启、异常退出和关闭未决均拒绝。loader每次取得同一supervisor guard并冷读原intent、native/Cookie/requirements、物理身份与Unix隔离；同操作重试和ENABLED重开复用原完整证明，不补造episode，不使用时钟租约。

证明固定使用JCA Ed25519（外部固定公钥、launcher独占私钥），签名覆盖`BookKeeper/CommonClassicHandoff/v1\0`与完整header/payload，总长度不超过64KiB；封套依次为`UWCHOF01`、u16 schema=1、u16 flags=0、u32 payloadLength、32-byte signerKeyId、固定顺序/严格UTF-8/有界payload、64-byte signature。payload绑定唯一OFFLINE_CLASSIC_BOOTSTRAP/native bindingId、部署/程序清单、原cluster/Bookie/Cookie/native出生与PREPARED语义、requirements原记录/birth/generations、共同incarnation/manifest/control根、按manifest角色/索引对应的完整物理成员、真实episode/退出/UID、独立32-byte AES材料identity、精确scope policy及retirement digest；未知/重复/溢出/尾随或错scope均拒绝。私有codec/loader/session不提供通用sign(ProofRecord)，完整两UID真实进程及bootstrap验收不能由codec单测或本节来源指纹替代。


证据目录`/Users/liusinan/.codex/artifacts/unified-wal/requirements-source-evidence-1d8b9256`分别保留首组十项和最终33项的原源码/日志/XML/class，style退出0。RAT实际退出1，431项未批准路径集合及各项字节与父提交和前次封存一致，无新增或解决，受保护PDF在扫描前排除。当前声明1321项源码和十项文档输入，native runner实际记录1294项，相关class快照14项；其他输入逐项对应父Git blob，不将早期getter结果移绑新增namespace字段后的源码。完成实际终态与两次独立lsof检查后，仅移除本次两个raw scope的四个目录、十个文件路径，共1,479,573逻辑字节；归档硬链接完整保留，regular-file数据块回收为0。用户PDF、默认目录和其他任务产物未触碰。本增量只交付来源身份值，不关闭完整交接证明或bootstrap验收。


## 固定 Classic 交接 codec 与精确材料绑定

2026-10-31在父源码`967ef3dee3497be377c7f2ab58d1050abf8f20e9`上，实现包内SegmentClassicHandoff固定proof/policy/retirement字节、独立32-byte AES key identity及完整材料scope匹配。具体顺序/边界与固定向量由[受控 Classic 交接材料 v1](../local-storage/format-v1.md#受控-classic-交接材料-v1)拥有，既有Segment/ROUTE/seal/native格式不变。证明最大64KiB，固定JCA Ed25519和外部已信任signer/key；对稳定原始字节验签后解析，再检查规范字节。无私钥参数、通用sign(ProofRecord)、网络信任锚下载、算法协商或JSON重编码路径。

packet复用原PREPARED native binding及SourceIdentity，绑定原cluster/Bookie/Cookie/birth/op/共同manifest/目标身份，按manifest角色与索引保存物理根而不复制路径表。原始retirement字节摘要绑定同operation及同物理identity；policy只接受本op/Bookie/incarnation/manifest/目标UID的完整相等，AES材料必须独立原始32字节。签名同时覆盖真实child关闭/正常退出和独立Unix隔离结果/回执摘要；mode与typed成功结果不自行构成这些事实。ZK Observation新增commissionedCompatibility只读原envelope兼容要求；proof requirements generations取原记录，保留当前delete-assignment overlay供原服务逻辑消费，getter不增加metadata I/O。

本增量只返回已验签的数据与材料比较结果，没有发行bootstrap session、DRAINED、readiness或registration。真实launcher固定supervisor/retirement持久状态、实际child适配器、Unix/fd/ACL完整核验、受保护材料owner/loader、当前metadata冷读，以及Classic导入/flush/seal/native ENABLED仍须串联；完整issuer与离线包仍OPEN。Main/G1/full-bytes/生产Gate及完整16步不提升，目标active；默认OFF/SYSTEM/PARK与disabled保持。


最终执行`scripts/unified-wal/run-local-storage-tests.sh 'SegmentClassicHandoffTest,ZKProfileStartupRequirementsStoreTest,ZKProfileEphemeralRegistrationStoreTest,SegmentProfileInitialSocketClientTest#ownedProfileServiceRejectsRequirementsBeforeStorageOpenAndCleansBindFailure+ownedProfileServicePublishesBoundEndpointSupportsSdkAndRetainsDrainAcrossRestart+deleteHandoffEffectiveAllRealTargetsRejoinG2AndKeepOldEventsWithNewJournalData'`：四类41项全通过（codec 8、requirements 11、ephemeral 19、实际服务/SDK/重启及handoff 3），0失败/错误/跳过；最终style退出0。独立Python struct/hashlib与OpenSSL 3.6.4生成最终policy 100、retirement 685、proof 973字节向量，OpenSSL验签成功，Java/JCA产出的签名逐字节相同。测试翻转proof全部973个字节逐一拒绝；即使重新使用测试key正确签名，未知purpose/flags/结果、坏UTF-8、溢出长度/计数、非规范时间或尾随字段仍拒绝。另验证错signer/key/policy/目标scope、完整manifest成员缺失、物理别名/重排、最大257成员、unsigned UID和不可变引用；时间戳不产生租约。真实native G2 handoff中确认Requirements.from仍取原commissioned assignment generation=1，原服务继续消费当前overlay。

首轮主编译因遗漏有界count reader退出1，没有执行测试；补齐后八项通过。对照冻结合同补入独立Unix隔离结果/回执，并增加producer签名输入与独立向量比较后，最终41项通过；未发布的937-byte草稿向量、原八项源码/class和首次style=0保留原绑定，最终973-byte输入与style-final=0另存，不将早期结果移绑。编译错误不记作产品RED，也不把codec中固定的成功结果或测试fixture当真实launcher证据。RAT实际退出1，431项未批准路径及字节与父/前次封存相同，无新增或解决，PDF在扫描前排除，不宣称全量CI通过。

证据目录`/Users/liusinan/.codex/artifacts/unified-wal/classic-handoff-codec-evidence-967ef3de`保留各阶段源码、实际日志/XML/class及两次独立向量生成脚本/输入/签名/回执。固定key为公开RFC测试材料，OpenSSL只用于向量生成，不增加运行或Maven依赖。最终声明1326项源码/资源及十项文档输入，native runner实际1299项，相关class快照67项；其他输入逐项对父Git blob核对。实际终态和两次独立lsof检查后，仅移除本次三个raw scope的六个目录、14个文件路径，共1,647,820逻辑字节；归档硬链接与必要向量生成材料保留，regular-file数据块回收为0。PDF、默认目录与其他任务产物未触碰。完整材料owner/loader、真实issuer/旧进程及两UID验收、公共bootstrap/Classic恢复桥和封口继续OPEN。

提交前审计首轮因本次审计脚本参数替换把已保存的清理字节数误写为6,779,573而退出1；依据原cleanup-result中的实际1,647,820修正参数，保留原脚本/日志/退出码后重新核对最终index、源码/文档/class及精确patch。该审计错误未修改产品源码或测试断言，不作为产品RED；最终提交以成功审计结果为准。


## 交接材料 I/O owner 与 supervisor 终态

2026-10-31在父源码`b10a7547b245009ef678e2d8bd29252de676937c`上，新增包内SegmentClassicHandoffMaterials资源子owner。构造只冻结显式引用、外部期望scope/公钥与完整manifest；initialize才检查实际目标UID、规范Unix路径与受保护材料，并取得已有supervisor guard。proof、exact policy、固定retirement intent和独立原始32-byte AES key均有界只读，不创建、修复或覆盖材料。签名与原intent完整摘要/operation/物理成员、原native PREPARED语义及requirements来源、部署/program/UID/key/policy逐项匹配；ENABLED只允许同原birth及PREPARED投影的数据比较，实际现存seal和当前metadata必须由外层另验。

supervisor文件名按clusterId、原cluster birth和Bookie固定，绑定UUID、episode、PID及尝试次数不改变名字；准确编码见[受控 Classic 交接材料 v1](../local-storage/format-v1.md#受控-classic-交接材料-v1)。本子owner只打开已有guard/intent，固定部署目录及launcher的create-only发布仍须接线。材料要求单硬链接普通文件、受信任owner及私有mode；逐层检查规范父路径、device/inode/owner/mode，并比较读取前后的叶子身份/长度/mtime/ctime和父身份。sticky公共父目录仅按实际owner保护判定。拒绝符号别名、与共同manifest全部guard的inode别名、缺失、超界和读取期间替换，不依据文件自报owner或任意true回调放行。

实际文件打开前先登记owner，原生fd打开后立即归属既有SegmentOwnedFiles；每次读均等待真实close返回，失败抹除暂存字节，原始AES输入在成功和失败路径均清零。首次native close失败不能由后续close重试覆盖，也不能发布key或释放supervisor。初始化与外部关闭串行；实际初始化线程不可排空自身，外部close须等待读取真实返回。只有所有材料fd及guard均真实终结才报告本子owner drained；原初始化错误独立保留，外层仍须先终结其全部key/scope使用者。

此处只返回包内材料数据，不发行可采用session、旧服务DRAINED、readiness或registration。NIO Unix属性与实际当前UID不证明旧UID专用/无提权、其他进程和已有fd终结、ACL有效权限或持续不可替换性；真实issuer/launcher、Unix完整核验、当前native/Cookie/requirements冷读、公开bootstrap及Classic恢复/封口继续OPEN。测试使用签名fixture和真实文件/子JVM锁，仅验证资源与材料行为，不把fixture中的成功标记当作真实交接。完整16步目标active，Main/G1/full-bytes/生产Gate不提升，默认OFF/SYSTEM/PARK及disabled保持。


首轮材料I/O八项通过；随后在同一期望scope拒绝测试中加入实际目标UID不匹配的先行拒绝及子JVM确认guard未占用。最终执行`scripts/unified-wal/run-local-storage-tests.sh 'SegmentClassicHandoffMaterialsTest,SegmentClassicHandoffTest,SegmentOwnedFilesTest,SegmentOwnedBookieControlLogTest,SegmentDataFileLockTest,ZKProfileStartupRequirementsStoreTest,ZKProfileEphemeralRegistrationStoreTest,SegmentProfileInitialSocketClientTest#ownedProfileServiceRejectsRequirementsBeforeStorageOpenAndCleansBindFailure+ownedProfileServicePublishesBoundEndpointSupportsSdkAndRetainsDrainAcrossRestart+deleteHandoffEffectiveAllRealTargetsRejoinG2AndKeepOldEventsWithNewJournalData'`：八类72项全通过（材料8、codec 8、owned files 4、Bookie control owner 9、原生锁10、requirements 11、ephemeral 19、实际SDK/服务/重启与handoff 3），0失败/错误/跳过。材料测试覆盖真实只读文件与子JVM guard竞争、缺失不补建、超长/错误权限、符号/硬链接、原生路径替换、错误AES/intent/原birth/program、原始AES缓冲清零、首次native close失败仍保留guard，以及实际初始化未返回时外部close等待；首次关闭失败仅用故障channel注入，实际fd和guard排空边界仍被独立观察。未用此测试代替实际旧服务发行者或两UID/ACL/fd隔离。


首次style实际退出1，原因是新文件的六处超长行和一个未使用import；修正后最终spotless/checkstyle退出0，最终源码72项回归重新执行并通过。首轮8项、加入实际UID拒绝后的72项，以及格式修正后的最终72项分别保存原源码/日志/XML/class，不将较早PASS移绑最终字节。独立RAT实际退出1，431条未批准路径集合及各项字节与父提交/前次封存相同，没有新增或解决；受保护PDF在扫描前排除，不宣称全量CI通过。

证据目录`/Users/liusinan/.codex/artifacts/unified-wal/handoff-material-io-evidence-b10a7547`保留上述原阶段及首次style失败、最终style成功和RAT终态。最终声明1328项源码/资源及十项文档输入，native runner实际1301项，相关class快照89项；其他源码逐项对父Git blob核对。实际终态、全部归档硬链接/内容/身份核对及两次独立lsof检查后，仅移除本次三个raw scope的六个目录、26个文件路径，共2,773,421逻辑字节；归档硬链接保留，regular-file数据块回收为0。用户PDF、默认目录与其他任务产物未触碰。本增量是材料I/O资源依赖，不关闭真实issuer、完整loader/Unix隔离或离线bootstrap验收。


## Classic bootstrap 的真实冷读与借用回调排空

2026-10-31在父源码`78a317040c7e0c7177b405194275172820f33a78`上，ZKRegistrationManager新增显式final借用入口，使用manager自己的真实ZooKeeper连接及固定native namespace，创建requirements store内构造受限的ClassicBootstrapSources。构造只做引用/客户端身份/namespace/参数检查，不执行查询，不新建客户端、线程或重试层，不暴露原生客户端getter。每次read重新查询已有native binding、真实cluster及Cookie出生/version/摘要、原commissioned requirements节点/birth/字节与代际，再确认同Bookie的writable和readonly两类registration均实际不存在。native与requirements的共同incarnation及完整manifest必须一致；NOT_FOUND/ABSENT、损坏、不兼容、超时/中断或任何未知结果均拒绝，不补造缺失authority。

冷读复用原ZKBookieRouteBindingStore与requirements parser，原prepare/complete/普通read和当前delete-assignment overlay行为保持。bootstrap返回原commissioned compatibility/devices/source identity数据，不返回可用于publisher的Observation，不读取或重编码当前assignment overlay来冒充原证明材料；当前delete catch-up/readiness仍必须走原实际authority路径。PREPARED到ENABLED的合法同birth变化由真实查询暴露；原生node按相同字节/envelope UUID重建时，新creation zxid仍可见，不能当原证明的相同出生。外层loader仍须把这些新鲜值与原签名操作及实际control seal逐项匹配，本子项没有发行可采用session。

显式借用owner给本次查询的每一个sync/getData/exists回调登记实际归属。调用方超时或中断只结束等待，不取消原查询；callback实际归还前，closeAndDrain保持pending/non-drained，不能据future返回释放上层supervisor/根或关闭借用client。原查询方法未返回时，外部关闭须等待其真实返回；实际读线程不能排空自身或重入read。查询首次失败保留并阻止新读；同步提交抛错且回调终态不明时同样保留未决，不能默认未提交。关闭只停止新查询和返回本owner的真实回调终态，永不关闭caller的ZooKeeper、manager或requirements store；后续迟到回调真实归还可使drain完成，但原读错误仍独立保留。既有非owned冷读不改变返回分类，普通Add没有新增metadata调用。

该对象只交付真实冷读及借用资源终态，不证明实际旧服务关闭/退出、Unix专用UID/提权/ACL/fd隔离、固定retirement持久发布或readiness。签名材料I/O owner与本冷读owner的完整loader接线、真实launcher、公开bootstrap/Classic恢复桥/flush/seal与Main仍须完成；完整16步目标active，Main/G1/full-bytes/生产Gate保持OPEN，默认OFF/SYSTEM/PARK和disabled保持。当前测试使用真实ZooKeeper及有限回调交付/提交故障注入，不把元数据fixture当实际旧Classic进程证据。


首轮真实冷读八项通过；随后补入native与requirements的共同incarnation/manifest拒绝，以及缺失native/requirements不补建的覆盖，扩展为九项。最终执行`scripts/unified-wal/run-local-storage-tests.sh 'ZKClassicBootstrapSourcesTest,ZKBookieRouteBindingStoreTest,ZKProfileStartupRequirementsStoreTest,ZKProfileEphemeralRegistrationStoreTest,ZKProfileRegistrationStoreTest,SegmentClassicHandoffTest,SegmentClassicHandoffMaterialsTest,SegmentProfileInitialSocketClientTest#ownedProfileServiceRejectsRequirementsBeforeStorageOpenAndCleansBindFailure+ownedProfileServicePublishesBoundEndpointSupportsSdkAndRetainsDrainAcrossRestart+deleteHandoffEffectiveAllRealTargetsRejoinG2AndKeepOldEventsWithNewJournalData'`：八类81项全通过（cold sources 9、native binding 10、requirements 11、ephemeral 19、persistent readiness 13、codec 8、材料I/O 8、实际SDK/服务/重启与handoff 3），0失败/错误/跳过。新增测试使用真实ZooKeeper、原Cookie和native侧记录，验证同原birth的PREPARED→ENABLED新鲜读取、同字节重建的native/requirements新出生可见、原Cookie同字节升version仍拒绝、两类registration存在、foreign client/namespace、scope错配、缺失/未知拒绝，以及超时/中断不取消迟到回调、实际查询期间的自排空/重入拒绝与外部close等待、未知提交终态不伪造排空。close后借用client及原requirements仍可使用。ENABLED seal只是metadata fixture，此测试不证明真实control封口；来源ABA只暴露新身份，原proof匹配与采用拒绝仍须外层loader完成。

首次style因一个测试lambda的换行退出1；修正后最终spotless/checkstyle退出0，最终源码81项重新执行并通过。首轮8项、格式前81项与最终81项分别保留原源码/日志/XML/class，首次style失败与最终成功单独留档，不将早期PASS移绑后续字节。RAT实际退出1，431条未批准路径集合及各项字节与父提交/前次封存一致，没有新增或解决；受保护PDF在扫描前排除，不宣称全量CI通过。

证据目录`/Users/liusinan/.codex/artifacts/unified-wal/classic-bootstrap-sources-evidence-78a31704`保留上述原阶段与终态；最终声明1329项源码/资源及十项文档输入，native runner实际1302项，相关class快照108项，其余源码逐项核对父Git blob。实际终态、归档硬链接/内容/身份及两次独立lsof检查后，仅移除本次三个raw scope的六个目录、26个文件路径，共3,904,880逻辑字节；归档保留，regular-file数据块回收为0。PDF、默认目录及其他任务产物未触碰。本增量只交付真实来源读取及其资源子owner，完整交接loader/issuer/Unix隔离、公开bootstrap与Classic恢复封口仍OPEN。


## 固定交接的原子不覆盖改名

2026-10-31在父源码`facda20fcc9ef6ce31202fee1ddcdc02a1d53648`上，接续规划裁决，为固定retirement/proof发布增加既有native-io的窄renameNoReplace依赖。NativeIO新增默认拒绝，旧方法集合的Java实现无需增加实现即可明确拒绝新能力；NativeIOImpl/NativeIOJni及原Rust库增加一次原子改名。Linux走libc架构常量与syscall(RENAME_NOREPLACE)，不直接链接较新的glibc renameat2符号；macOS走renamex_np(RENAME_EXCL)。不增加通用flags、I/O后端、客户端线程或自动重试，不复制/删除源、fsync或打开业务fd；只有原生明确返回0才成功，其余保留真实errno或明确报能力不可用。旧库缺符号/装载失败不回退到Java move或hardlink。JNI直接读取UTF-16并严格转换，NUL、null或未配对surrogate拒绝，不经lossy转换改名另一路径；有效非BMP/非ASCII路径保持。

原语成功只表示名字原子切换，不等于证明已经持久发布。完整顺序仍为同一已核验受保护目录及supervisor guard下：owned temp完整写入→force→首次native close明确成功→renameNoReplace→父目录同步及对应资源终结。该owned publisher接合、dirsync/close故障与完整issuer仍须完成；源码中的原语不签名、不产生旧服务DRAINED、可采用session或readiness。操作系统语义依据[Java Files.move](https://docs.oracle.com/en/java/javase/21/docs/api/java.base/java/nio/file/Files.html#move(java.nio.file.Path,java.nio.file.Path,java.nio.file.CopyOption...))、[Linux rename](https://man7.org/linux/man-pages/man2/rename.2.html)、[Apple rename](https://raw.githubusercontent.com/apple-oss-distributions/xnu/main/bsd/man/man2/rename.2)及[fsync](https://man7.org/linux/man-pages/man2/fsync.2.html)。Java ATOMIC_MOVE本身不承诺已存在目标必定拒绝，本流程不采用该替代算法。

successor恢复规则已由指定规划会话收紧：只有temp，即使完整且签名正确，也不补签或自动promote；只允许核验复用同操作原final。rename成功但目录同步失败，原调用仍失败/未决，不删除final、回滚或重签。进程退出后若实际final存在且单硬链接/身份/签名/retirement/当前native与requirements/Unix条件均通过，可完成必要的目录恢复同步后复用，不追认原调用成功。final缺失/损坏/错操作则拒绝，不能从temp重建或删除冲突目标。原材料I/O和真实冷读子owner尚未接成这个完整恢复流程，不把本次原语验收当作publisher或bootstrap验收。Main/G1/full-bytes/生产Gate保持OPEN，完整16步目标active，默认OFF/SYSTEM/PARK及disabled保持。


本机实际构建并加载macOS arm64 host JNI，runtime记录文件系统为APFS、实现来自本工作区native-io/target/classes，使用原有显式library-path覆盖。原库和新库均以Cargo.lock锁定依赖构建，JNI导出集合保留原十项，仅新增renameNoReplace。首次offline构建因本地缺libc缓存实际退出101，没有产生库或测试结果；锁定fetch后原库与新库分别构建成功，Cargo.lock/toml/build.rs/pom未改。Rust编译两次均保留原九条warning，不借本任务清理；Java样式及Rust格式检查均退出0。两个独立JVM分别用原版精确JNI库核验旧open/close仍能调用，而新符号缺失必须返回带UnsatisfiedLinkError原因的NativeIOException，源与目标不发生改名，无其他算法回退。

首组NativeIORenameNoReplaceTest八项与既有NativeIOLibraryPathTest十二项共20项通过；增加实际filesystem/implementation/library来源输出后，最终在同一显式JNI库上执行这两类及SegmentClassicHandoffTest、SegmentClassicHandoffMaterialsTest、SegmentProfileInitialSocketClientTest的三个owned SDK/服务/重启/handoff方法：五类39项全通过，0失败/错误/跳过。完整Maven命令、原源码和实际加载库路径/SHA由证据目录各阶段inputs.json保存。新八项覆盖旧Java方法集合默认拒绝、原native open/fsync/close、有效非BMP与非ASCII路径、null/NUL/未配对UTF-16拒绝且不改名replacement路径、已有普通文件/目录/有效及悬空symlink保持原inode与内容、缺失源保留errno、两个真实JVM竞争仅一个成功、改名前后实际进程cut后只留下完整单链接文件。原语cut不执行目录同步，不称作持久发布或真实掉电证明。原20项、最终39项及两次旧库子进程均保留各自原源码/class/库绑定，不把早期PASS移绑新增来源输出后的字节。

本次仅实际验证macOS/APFS；Linux运行及交叉编译、跨文件系统的真实调用仍NOT_EXECUTED，不从源码分支或一般errno处理推定通过。RAT实际退出1，431条既有未批准路径及字节与父提交/前次封存相同，无新增或解决；PDF扫描前排除，不宣称全量CI通过。完整owned temp→force/首次close→rename→dirsync发布图、目录同步/关闭失败、successor恢复同步、实际issuer/Unix隔离与离线bootstrap继续OPEN。

证据目录`/Users/liusinan/.codex/artifacts/unified-wal/atomic-publication-evidence-facda20f`保留原版/新JNI库、源、命令、实际日志/XML、导出对照、运行时来源与兼容probe。最终声明1340项源码/资源及十项文档输入，本次Maven实际来源清单同为1340项，相关class快照78项。实际终态与两次独立lsof及硬链接/字节/身份核对后，仅移除本次两个raw scope的四个目录、13个文件路径，共897,482逻辑字节；归档硬链接保留，regular-file数据块回收0。另核对两个固定JNI库与实际构建输出完全相同且独立保留后，清理本次两处私有Rust target的86目录、340文件、85,641,851逻辑字节；原库/新库、锁定依赖、源码/日志/指纹及测试证据保留，不将路径逻辑字节当作卷空间回收量。用户PDF、默认目录及其他任务产物未触碰。


## retirement 与 proof 的 owned 文件发布

2026-10-31在父源码`6e0909af442c35452c6c569693715f249c2922c4`上，实现包内SegmentClassicHandoffPublication文件资源子owner。构造只冻结固定supervisor scope/材料引用、外部UID与公钥/KeyId、原guard owner及I/O配置；initialize才检查实际issuer UID、规范私有目录/父链及同scope原guard确实持有，guard须属于目标UID。子owner不再取第二个guard、不创建缺失目录/guard，parent从旧服务启动起持原guard，须在全部实际使用者结束及本子owner排空后才释放。原guard新增requireHeld只作实际资源/固定路径检查，不发行metadata或旧服务authority。

retire将原完整operation/members固定为同一规范字节；proof只接受外部已签名且与该retirement逐项匹配的原packet，包内writer没有私钥或sign-facts入口。新文件按同目录唯一candidate、owned CREATE_NEW/WRITE→完整write→仅对本owner新文件设置目标UID→force→首次native close明确成功→原子renameNoReplace→owned final有界重读/身份比较与首次close→目录force及首次close完成。每步复核目录/父链和原guard；不truncate/替换/delete已有final，不回滚或换packet重签。成功文件退役fd不累计；失败/挂起fd保留首次实际结果，签名通过不能掩盖close失败。目标UID设置只作用于本owner新建candidate，既有材料/guard/final不修改。

受保护文件集合为CREATE_NEW＋WRITE增加原子新inode路径：先登记再open，只有原子新建有效时不预读不存在路径作inode别名比较，选项在打开前冻结；READ忽略CREATE_NEW时仍执行原保护检查，既有alias不能被打开后关闭而释放POSIX guard。此变化沿原SegmentOwnedFiles，不增线程/队列/后端。读取与发布共用原Unix路径身份检查，SegmentClassicHandoffPaths仅消除这两条路径的实质重复；原reader的key清零、native close与owned终态合同保持。

只有该owner实际创建的原intent final与其candidate inode一致且目录同步完成后，才允许其写入新的已签proof；successor看到既有intent只可复用原proof final，不能因caller传入合法签名packet而新发。恢复从原final有界读取/验签/原retirement匹配并完成目录恢复同步，不枚举或promote orphan candidate。file force失败不重开未知candidate；rename结果未决不再改名，只在原final出现且与本owner原candidate同inode/同bytes时解析；dirsync失败保留原final，可对同原packet做安全同步重试，首次错误独立保留，不追认原调用成功。首次native close失败则资源图sticky/non-drained，不能由重试同步变成成功。实际I/O未返回时close等待，实际操作线程拒绝自排空或重入；close不释放借用guard。

首组publication/files共14项通过。增加独立JVM的guard仍持有检查及实际UID probe入口后，最终执行publication九项、owned files五项、guard十项、materials八项、codec八项、owned Bookie control九项及既有真实SDK/服务/重启/handoff三个方法：七类52项，0失败/错误/跳过。新覆盖包括文件/目录force失败、原final安全同步重试、实际rename已执行但结果丢失、首次native close失败仍non-drained、实际I/O未返回时外部close等待、caller timeout不终止实际操作、冲突final不变、signed orphan不采用，以及proof file-force前与rename后目录force前的真实子进程cut；不把文件I/O进程cut当作旧服务正常排空或真实掉电。样式首次实际退出1（import顺序与长注释），修正后退出0；早期14项与最终52项分别保留原源码/class绑定。

另以三个真实JVM分别执行UID 501准备、UID 0发布、UID 501读取/恢复原proof，全部退出0；guard/retirement/proof均属501、0600、单链接。probe使用测试签名和合成operation/members，只验证新建文件归属及目标UID实际读取，不证明旧UID进程/fd退出、完整Unix隔离、metadata资格或可采用session。三次调用后的附加检查起初误把fixture父目录当作私有regular file，修正为实际materials子目录后复核原运行与文件，未重跑发布。RAT实际退出1，原431项未批准路径及父Git blob字节不变，扫描前排除用户PDF；Linux、完整launcher/Unix和离线bootstrap验收仍NOT_EXECUTED/OPEN，不宣称全量CI通过。

证据目录`/Users/liusinan/.codex/artifacts/unified-wal/owned-publication-evidence-6e0909af`保留各阶段原来源、命令、日志/XML、UID probe与首次失败；本次复用父里程碑封存的精确host JNI库，SHA-256为`619efd63ed22a72536e00d791d679aa6559edc7fdfa68621df50850bc7e5b7a2`，没有重建或改变native源码。最终声明1343项源码/资源、十项文档输入及93项相关class；Maven实际输入同为1343项。实际终态、归档硬链接/字节/身份及两次独立lsof核验后，仅移除本次两个raw scope的四目录、15文件路径、1,129,414逻辑字节；归档硬链接保留，regular-file数据块回收0。UID probe三份材料保留作原始证据，用户PDF、默认目录和其他任务产物未触碰。

本子项只报告文件发布及资源事实，不验证实际旧服务/metadata资格、其他进程与fd、Unix提权/组/ACL完整隔离或当前control seal，不发行DRAINED/可采用session/readiness。完整launcher/loader须先完成冻结的实际输入/进程/当前metadata与Unix核验，再按原owner图采用这些文件；公共bootstrap、Classic导入/flush/seal及Main仍须接线验收。完整16步目标active，Main/G1/full-bytes/生产Gate保持OPEN，默认OFF/SYSTEM/PARK及disabled保持。

## Linux conf/include 的完整原 owner 图（数据观察）

2026-11-01在父`2df3c6180c7ebea41bc5228a9dd5c217e6975f38`上接入包内verifyLinuxConfigurationGraph：root必须属于传入的原Loading.configuration，present索引绑定同一批准表的LOADER_CONFIG；所有配置声明先按1MiB准入，再做目标I/O。显式absent root只有实际保护父链下确证缺失才返回空图，未提供/未知/校验失败不作为空图。未来qualified消费者固定选择/etc/ld.so.conf；临时树入口仅data-only，没有配置/环境替换根或发行Installed/START的入口。每个匹配的logical pathname解析到批准的物理配置，再用原owner同一个hash/parser FileChannel读取、检查身份/父链/ACL及首次关闭；匹配后打不开、类型/批准不符、读取或关闭失败使整图拒绝。

按原[glibc2.39 glob](https://raw.githubusercontent.com/bminor/glibc/glibc-2.39/posix/glob.c)的dirname、literal/magic、backslash与MARK分支组成有界显式计划，不按pathname深度递归，不预先normalize或删除尾slash。每个pattern的完整逻辑返回名分别按C字节序排序，按原include word顺序逐个展开并保留重复；相对base来自本次逻辑配置名的[dirname](https://raw.githubusercontent.com/bminor/glibc/glibc-2.39/misc/dirname.c)，保留其别名/原路径上下文，不使用最终物理文件的父目录。虚拟`.`/`..`参与glob判断但不伪装成DirectoryStream物理成员。conf目录行仍只是ldconfig输入事实，不当作runtime loader搜索顺序。

规划确认固定版本MARK的边界：literal regular.conf/的递归存在分支可返回regular.conf，MARK只给is_dir者追加slash而保留普通文件名；图按实际returned name继续验证/读取，不吞成NOMATCH或加新Unsupported。该变换只来自对应glob分支；实际lookup收到link/仍要求目录。新增内部LOOKUP仅观察实际regular/dir或确证缺失，LITERAL的最终普通link仅lstat、保留dangling匹配，后续follow仍须原声明及readlink原bytes。实际普通文件不能作目录时记录已核验的非目录分支；权限、未知alias、iterator/I/O/预算/期限错误不得被捕获成零匹配。LOOKUP允许root/dot和满足原sticky规则的既有前缀；实际枚举另要求最终目录受保护且不可写，旧四种kind及普通checked/key/recipe合同保持。

每遍独立计费：root与重复文件计入65536次展开，include word计入65536次pattern，完整文件length（含注释/raw bytes）累计16MiB，活动上下文最多32且root计入。文件重复时仍实际hash/read并全长度计费；物理目录成员可复用同遍原观察，但每次检查原路径/身份，继续262144总成员/64MiB名称预算。活动cycle key使用实际dev/inode加逻辑include dirname/context，允许非活动重复及同一物理文件在不同逻辑base下重现；无法证明上下文重复时由深度/总量限制终止。A/B两遍共享原绝对deadline，完整不可变结果返回前再检查；已解析前缀不能成为部分成功。整遍比较包含有序file/include/search/match/完整目录事实及流式lookup摘要，后者仅同进程A/B比较，不是持久/可签发编码，不补认原子性或连续外部保护，也不证明整个heap/RSS上界。

新增真实文件/目录/JNI集成覆盖顺序/嵌套/重复、同physical file不同logical base、显式缺失root及membership、known non-dir/file/..、dangling literal和未批准命中、dot候选、dirname magic/转义、MARK literal与magic差异、真实cycle、32接受/33拒绝，以及重复1MiB文件在超16MiB前拒绝下次payload open（已打开15次）。另验证sticky已存在literal可读而缺失/枚举拒绝、超大声明在目标metadata I/O前拒绝、原DirectoryStream held时self-close拒绝且external close等待实际body、B遍增加unrelated非匹配成员仍拒绝；纯budget与2048层显式glob计划检查分别覆盖工作量边界和无pathname递归。首轮停在testCompile的helper InterruptedException声明错误，0项测试执行；随后24项一处夹具把newline写成literal backslash-n，修正后3类27项全过。最终串行十一类125项全部通过、0失败/错误/跳过，独立Spotless/Checkstyle退出0。首次编译失败时捕获的旧target class不作为新源码PASS，原失败及各阶段源/class/log/XML保持原绑定。

证据`/Users/liusinan/.codex/artifacts/unified-wal/classic-linux-include-evidence-2df3c618`保留四阶段各1424项输入逐项核验，最终六个code与通过test/style原快照一致；原GNU glob/dirname/ldconfig源码及license仅在artifact保存，不进Apache源码。终态RAT退出1，原431项路径及父2df3字节不变、本次无新增，扫描前排除用户PDF。四raw scope经各两次独立lsof及inode/SHA/归档硬链接核验后仅清理8目录、29文件路径、2,080,146逻辑字节；原证据保留、regular-file数据块回收0，共享target/精确JNI库未动。没有Rust重建、模型或性能结论；实际Linux/glibc2.39差分与接受路径仍NOT_EXECUTED。

本子包完成的是配置图数据观察，尚未接入新LinuxDeployment schema/provider及原声明字符串、完整候选集合/选择/诊断或qualified父子启动。旧schema1不补认新图/预算/算法资格，各整体Gate继续OPEN，完整16步active，默认OFF/SYSTEM/PARK与disabled保持。

## Linux include component 的 C 字节匹配

2026-11-01在父`228f9a12d27f796df908548e7004d4541e5b18f3`上增加包内SegmentClassicLinuxGlob，接原LinuxDirectoryObservation.matchingMembers。输入仍是原owner实际A/B观察及首次关闭后的不可变普通成员名单；筛选不查文件、不follow link、不批准候选。完整原名单保留，匹配名单按unsigned UTF8 bytes排序而非Java UTF-16顺序；不按inode/path去重，不合并多个include pattern。真实文件测试以U+E000和补充平面字符验证两种排序的差异，原dangling/unrelated成员仍仅为名称事实。

按固定C LC_CTYPE/LC_COLLATE、空环境下的[glibc2.39 matcher](https://raw.githubusercontent.com/bminor/glibc/glibc-2.39/posix/fnmatch_loop.c)实现字节`*`/`?`、转义、初始dot限制、`!`/`^`否定、range、12种ASCII POSIX class及单字节collating/equivalence。相邻star没有递归目录语义，不展开brace/tilde/extmatch或case-fold；不读取JVM默认Locale/JNU改变规则。保留unterminated bracket的literal回退以及matched-prefix扫描与unknown class/无效collating等的定义差异，不预编译成Java字符集合或笼统拒绝malformed输入。原C-collate/C-collate-seq、fnmatch及limits源码独立保存，静态核对NRULES=0、256项collseqmb=0…255、CHARCLASS_NAME_MAX=2048，以及求值/已匹配后扫描的不同边界；这些不是vendor build或Linux运行证明。

pattern/单成员最多4096 UTF8 bytes，成员事实最多262144/64MiB；字符数先验与严格编码检查先于分配字节图像，原始字节匹配拒绝NUL和slash。每次匹配/筛选和最终不可变结果构造均沿原绝对deadline，超时不返回NOMATCH；限制仍不是整个heap/RSS上界。首组两类45项通过，补最终deadline检查前后各次109项保持原source/class/XML绑定；最后冻结全部返回guard后，串行十类109项全部通过、0失败/错误/跳过，独立Spotless/Checkstyle退出0。纯语义例和实际目录消费均有覆盖，未执行Linux glibc2.39差分oracle，不以host libc近似替代。

证据`/Users/liusinan/.codex/artifacts/unified-wal/classic-linux-glob-evidence-228f9a12`保存四个原测试阶段（各1423项输入）与两份style原源，最终四个code与通过guarded test/style快照逐项相同；原RAT及cleanup的pre-guard回执独立保留，后续结果没有移绑。终态RAT退出1，原431项路径及父228f字节不变、本次无新增，扫描前排除用户PDF。四raw scope经各两次独立lsof及inode/SHA/归档硬链接核验后仅清理8目录、44文件路径、2,784,961逻辑字节，原证据保留、regular-file数据块回收0，共享target/精确JNI库未动；无Rust重建、模型或性能结论。

matchingMembers只是普通物理成员筛选，不能当完整filesystem glob。后续原owner图仍须区分literal/magic路径及实际错误、正确处理DirectoryStream未返回的`.`/`..`候选（不伪装成新物理成员）、按每个pattern的完整逻辑pathname分别排序并保留原word顺序、相对include的逻辑基目录、上下文循环及已冻结的全部图级预算。上述完整图、新schema/provider原字串、候选/选择/诊断及qualified启动仍OPEN，Linux实际接受路径NOT_EXECUTED，完整16步active，默认OFF/SYSTEM/PARK及disabled保持。

## Linux conf 的原字节解析与原句柄输入

2026-11-01在父`f672fd5b3904dc0b82aa891ee25e722c14190e09`上增加包内SegmentClassicLinuxConfiguration，并将LOADER_CONFIG接到原trust同一个SHA/metadata FileChannel及首次关闭；读取前按0…1MiB限制声明长度。只做positional读取、不移动调用方channel位置、不open/include/枚举目录或执行ldconfig。保留一份私有原bytes，先完整扫描验证，再由独立顺序cursor按需返回directive、原line/byte-column/include word序号和原行视图，不常驻大量短行对象。返回的bytes可修改副本不改变原图像；结果仍只是数据，不能产生Installed/START、文件批准或loader目录顺序。

按[glibc2.39解析规则](https://raw.githubusercontent.com/bminor/glibc/glibc-2.39/elf/ldconfig.c)处理C空白、首个`#`及大小写：include只认精确小写加space/Tab，按space/Tab拆每个pattern，不解释引号或反斜线；CRLF的CR仍可留在pattern末尾。目录行先去尾部C空白、再去尾部slash，不normalize dot/dotdot、不展开变量/tilde、不剥BOM；仅slash的空结果忽略。ignored hwcap按ASCII不区分大小写识别，保留raw字段。有效目录/pattern要求严格UTF8且最多4096字节，无关注释和ignored hwcap允许非UTF8 raw bytes；按规划裁决，任意位置出现NUL（包括注释）明确Unsupported，完整原bytes仍读取/hash，不按C字符串前缀放行。

独立242-byte固定向量SHA-256为`feabdd5dd6b447e4fd716c4b9ab4983d2c331b7b4d5a9fd95dd646f6e80958df`，原Python byte literal可复现；不是发行版conf或实际ldconfig。真实临时文件覆盖1MiB恰好524288个有效短行及一条长raw注释、4096-byte有效token/超限、空配置、期限/零进展/短读/长度变化、原位置不动，以及NUL/坏有效UTF8的拒绝。原owner验证只返回一份实际FileChannel身份且已关闭，不查include目标；原metadata read＋首次close故障用例复用于cache/config两角色，两个失败均保留且不补认drained。首轮两类46项有一夹具把Java open的两个重载调用算为两次资源获取，修为核对原返回channel身份及实际关闭后46项通过；再运行最终串行九类104项全部通过、0失败/错误/跳过，独立Spotless/Checkstyle退出0。原失败及各阶段源码/class/XML分别保留，不移绑旧PASS。

证据`/Users/liusinan/.codex/artifacts/unified-wal/classic-linux-conf-evidence-f672fd5b`保存三阶段各1421项输入与原快照/父Git字节的逐项核验，最终五个代码/资源与通过test/style快照相同。RAT退出1，原431项路径及父f672字节不变、本次无新增，扫描前排除用户PDF。三raw scope经两次独立lsof及inode/SHA/归档硬链接核验后只清理6目录、22文件路径、1,580,849逻辑字节；原证据保留、regular-file数据块回收0，共享target/精确JNI库未动。本轮没有Rust重建、模型或性能结论，Linux实际接受路径NOT_EXECUTED。

后续include/glob图仍须接原owner，不能由这个parser宣布完整conf闭合。规划已冻结每遍累计16MiB（重复include每次按完整长度计费）、65536次文件展开、65536次pattern求值、32个同时活动上下文（根计入）；A/B各自计费但共享原绝对deadline，目录总成员/名称字节预算继续。循环key须包含实际文件身份和逻辑include基目录/解析上下文，不能仅按同inode重现拒绝；非活动栈重复访问允许，无法证明上下文重复时靠深度/总展开上限终止。任何预算错误都不产生部分图、零匹配或absence成功。完整展开、C locale glob、这些图级计数、新schema/provider原字串、候选/选择/诊断及qualified启动仍OPEN，完整16步active，默认OFF/SYSTEM/PARK和disabled保持。

## Linux alias 的逐组件解析修复

2026-11-01在父`3cdcfd3fc80ecd856b09fa72f4997756e498f8dc`上修正声明resolver和原trust owner提前normalize的问题：相对target从原链接物理父目录开始，绝对target从根开始；按原顺序展开每个组件，前置链接解析后才处理后续`.`/`..`，保留尾随`/`的目录要求。实际owner观察所有经过的目录、链接及其原始target bytes，包括后来被`..`消去的组件；中间普通文件、缺失、未知链接或权限/ACL失败不再被词法消去。新组件在相关I/O前还须与原字符串形成的UTF8 file:///路径字节等价，避免JNU替换后自洽。原父链保护、lstat/readlink/JNI身份、deadline、A/B观察及原关闭合同保持；最终显式加载叶仍不接受根目录，ALIAS叶只观察而不跟随。

每个完整pathname最多跟随40次，alias表仍最多128项；同一alias在不同剩余后缀下可以合法重复，第41次才拒绝。展开队列按4096 UTF8路径上限限制，最多40次扩展及原组件数约束处理量，实际owner逐组件检查原绝对deadline。此规则依据[Linux路径解析合同](https://man7.org/linux/man-pages/man7/path_resolution.7.html)和规划会话裁决，不把macOS内核的链接遍历上限说成Linux实测。构造时去掉错误的词法self比较，真实cycle仍由follow上限拒绝；原序列化、摘要域和固定向量不变，声明模型本身不证明中间文件类型、真实路径或资格。

先在原实现执行六项反例，实际5 failure、1 error：jump/../interpreter选错真实文件、file/..被消去、合法重复alias误拒以及声明/owner第41次仍接受。修后两类47项通过；再补同一用例中的绝对target、词法self但真实非self、根上方的`..`，最终串行八类96项全部通过、0失败/错误/跳过，独立Spotless/Checkstyle退出0。真实临时文件/JNI覆盖相对与绝对target对照系统toRealPath、file/..、file/.、原始尾随slash、缺失/未知组件、重复链接及40/41边界；尾随slash用实际host symlink命令保留原bytes，避免Path.of先删除slash。40/41是组件算法的host JNI观察，不是Linux完整lookup/启动验证。旧部署及launcher字节向量继续通过，早期47项不移绑后来源码。

证据`/Users/liusinan/.codex/artifacts/unified-wal/classic-linux-alias-evidence-3cdcfd3f`保留三阶段原source/class/log/XML，每阶段1418项输入逐项匹配原快照或父Git字节，最终代码与通过test/style快照一致。RAT退出1，原431项路径及父3cdc字节不变、本次无新增，扫描前排除用户PDF。三raw scope经两次独立lsof及inode/SHA/归档硬链接核验后仅清理6目录、21文件路径、1,532,702逻辑字节，原证据保留、regular-file数据块回收0；共享target/精确JNI库未动。本增量无Rust重建、模型或性能结论；conf/include、新schema/provider原字串、完整候选/选择/诊断及qualified启动仍OPEN，Linux接受路径NOT_EXECUTED，完整16步active，默认OFF/SYSTEM/PARK与disabled不变。

## Linux new-cache 的有界原句柄解析

2026-11-01在父`80185847481561cd46896d60b7a6856b239945d5`上增加包内SegmentClassicLinuxCache数据观察，并接原trust的LOADER_CACHE角色：读取前按48 bytes…16MiB拒绝不支持长度，SHA与cache metadata使用同一原FileChannel，继续原路径/父链/ACL/单链接/身份、absolute deadline与首次关闭合同。v1程序角色解释与读取保持，新的cache字段不产生Installed/START/Unix资格；fixture仍拒绝Installed。只读positional读取，不移动/关闭调用方channel，不mmap或调用helper/loader/ldconfig，也不选实际winner。

支持受限glibc2.39 new-cache1.1 standalone格式：完整header、明确little-endian2及受限unset0、262144表项上限、绝对字符串偏移与suffix-sharing、NUL边界、扩展magic/tag/flags/范围/对齐/重叠和零padding/尾部，以及generator的非NUL结尾字节和按字节排序的glibc-hwcaps。拒绝legacy/compat/未知布局和损坏引用，不自动修复或重写cache。输出原始顺序、全部ABI flags、osversion_unused和legacy/named hwcap/ISA信息；lp64()仅为0x303 flags匹配，不是ELF机器类型、CPU可用性或加载批准。字符串使用私有原始图像的不可变有界视图，按需复制或严格UTF8解码；因此不因大量重复/共享suffix引用复制整串，也不把合法无关ABI的raw bytes替换成另一个路径。

查找顺序校验遵循支持范围内的numeric名称比较、flags降序、named hwcap优先及其名称字节顺序，不改成普通String/natural sort。规划会话冻结未验证的C回绕为明确UNSUPPORTED，既不报absence，也不跳过无关超界key：原C表达式value*10+字符-'0'的中间乘法/加ASCII字符也必须在int范围内。前导零不按长度拒绝；测试支持100个零前缀及2147483599，拒绝2147483600及INT_MAX的中间越界。新schema仍须显式绑定此比较policy，必要NEEDED查询也须遵守；确切vendor wrap profile将来有实证再扩展，当前未添加猜测分支。

独立Python写出551-byte/7项固定向量（SHA-256 `19083594b3a782789e794bc4ed946141e1441d95657281826e2f0ac4d66e3640`），包括10/2数值排序、LP64/i386/x32、named与legacy hwcap、非UTF8 raw path、suffix引用及非NUL结尾generator；不是实际部署cache。原glibc2.39头文件在artifact外置shim下由host clang独立验证header48/entry24、各offset、17+3 magic/version及扩展常量，原头/hash/编译命令/程序保留，不称Linux或loader运行。实际临时文件覆盖恰好16MiB/262144项与超限、原channel位置/期限/零进展、同原hash句柄解析、格式错误后的排空、读取失败与首次关闭失败同时保留，未用mock成功代替这些文件读取。

首轮两类41项有一个负向夹具将重复v3引用写成了合法suffix偏移，修正为实际第二索引；原1 failure保留。首次样式三项长行/unused import失败已修正。随后7项与样式误并行，虽原退出0，独立注记将其排除为最终证据；最终按串行命令，cache/trust/program/deployment/materials/publication/preload七类85项全部通过、0失败/错误/跳过，后续独立Spotless/Checkstyle退出0。三个原测试阶段各1418项源码/资源分别对照原快照或父Git字节，无缺失/错配；最终五个代码/资源与通过样式和最终test原源完全一致。不将早期C数值范围或并行回执移绑最终实现。

两个已有本地Docker镜像均不含/etc/ld.so.cache，有限定位已停止；两个任务自有容器只create/cp/inspect、从未start，按原ID/label/created状态清理且仅删除本次匿名卷，镜像及其他容器不动。第一失败记录来自原tool stderr，第二命令/错误/实际状态保留；实际发行版cache样本、Linux glibc2.39接受路径及端到端运行仍NOT_EXECUTED，没有用固定向量冒充。metadata inspect中无关环境配置未作为证据保存。

证据`/Users/liusinan/.codex/artifacts/unified-wal/classic-linux-cache-evidence-80185847`包含各原source/class/XML、独立生成器/向量、header oracle、失败与并行边界注记。终态RAT退出1，原431项路径及父801字节不变，本次无新增，扫描前排除用户PDF。三raw scope经两次lsof及原inode/SHA/归档硬链接核验后清理6目录/19文件路径/1,432,185逻辑字节，归档保留、regular-file数据块回收0，共享target和精确JNI未动。本增量无Rust重建、模型或性能；conf/include、新schema/原字符串/provider绑定、完整候选/选择/诊断及v2 qualified启动仍OPEN，各整体Gate不提升，完整16步active，默认OFF/SYSTEM/PARK与disabled保持。

## Linux 加载目录成员与逐路径字节等价

2026-11-01在父`b3bcb2f12f4eece7536e035a892a5e56d918cead`上，原SegmentClassicLaunchTrust/AuxiliaryRead增加完整成员名观察：先核验声明目录的物理路径，再持有原DirectoryStream、枚举名称并检查实际首次关闭，前后目录/父链及整组A/B必须一致；缺失和真正空目录分别表达。同一resolved目录的alias视图复用本轮名称列表。按规划会话冻结总成员262144，另用64MiB UTF8名称字节上限保护本组件，超限失败并关闭原iterator；这不是整个父进程的heap或full-bytes验收。名称包含确实无关成员，不跟随它们、不逐个读取内容，也不把枚举事实变成批准候选。异常/未知关闭保留原failure，外部close等待实际open体退出，自drain拒绝。

字节边界采用当前测试JDK Corretto21.0.10+7-LTS的原始UnixPath比较及file URI百分号解码规则。名称先quote为ASCII，显式file:///并拒绝authority/query/fragment，再由批准Unix provider构造UTF8预期Path，与原native-byte叶Path比较；不使用URI.getPath或生产Path.toUri，不用可变sun.jnu.encoding自报。首次草案new URI(file,null,...)生成file:/会走JNU fallback，这个实际缺口经匹配源码核对后改为空authority及显式前缀检查，最终验证另外绑定。相同无I/O字节检查已提前到Linux最终文件、解析后的加载路径及alias的JNI调用，原v1 hash/regular ACL入口保持。新profile仍需把原始descriptor字符串及实际default provider/Path/URI行为绑定到批准JDK，不能只验类名；当前组件不发行该资格。Corretto src.zip三份原源及SHA保留，仅作为实际测试runtime的源码依据。

首组两类41项、后续六类73项各按当时的名称预算或URI实现保存；最终UTF8边界/Unicode文件用例后的trust/program/deployment/materials/publication/preload六类74项全部通过，0失败/错误/跳过，最终Spotless/Checkstyle退出0。覆盖真实Unicode及%/#/?字面成员、未跟随的dangling member、alias与空/缺失目录、open后目录inode替换、首次iterator close失败不重试、外部close等原调用体，以及跨物理目录的总名称/总数量超限；容量反例为包装真实iterator的合成名称故障，不当作真实成员或批准阳性。内存中file:///%FF原字节向量验证有损解码拒绝、原iterator关闭；不合法的完整Path在任何Files I/O前拒绝。五阶段各1415项原源/资源分别对照快照或父Git字节，无缺失/错配；最终两个代码源和样式/最终测试完全一致。早期PASS不移绑最终实现或262144契约。

Mac文件系统创建真实非法UTF8文件名被errno92拒绝，对应DirectoryProbe JVM未启动，不计内核反例阳性；空fixture经两次lsof及身份检查后清理。两个独立Java源探针只读匹配JDK缓存charset与URI向量，默认和-Dsun.jnu.encoding=ISO-8859-1尝试均观察到UTF8缓存，不能据此声称已测试非UTF8 runtime；测试用module open不成为生产入口。没有新增JNI、Rust构建、Linux运行、模型或性能验证；实际qualified阳性仍NOT_EXECUTED。

证据`/Users/liusinan/.codex/artifacts/unified-wal/classic-linux-directories-evidence-b3bcb2f1`保存原source/class/XML、各阶段命令、JDK源/探针及容量/URI纠正边界。终态RAT退出1，原431项未批准路径及父b3字节一致，本次无新增，扫描前排除用户PDF。五个Maven raw scope经两次独立lsof及原inode/SHA/硬链接核验后清理10目录/41文件路径/2,821,018逻辑字节，归档保留、regular-file数据块回收0，共享target、原精确JNI和用户文件不动。

规划会话已冻结后续完整接受语义：批准vendor build的glibc2.39/LP64/little-endian，new cache1.1/generator/hwcaps扩展及明确支持的endian-unset；全结构验证、无关或其他ABI合法条目可保留，cache最大16MiB/262144条目。请求者ELF＋NEEDED原序号/名称绑定批准候选集合，递归未选中候选；实际winner/CPU/OS特性为运行观察。真实RPATH加载者链/RUNPATH/cache/有序default目录、NODEFLIB、ORIGIN/LIB/PLATFORM及alias加载路径上下文、v4/v3/v2/base规则都须纳入同一新schema；批准loader固定--list-diagnostics及候选批准后的--list systemctl仅为有界原进程观察/oracle，不证明后续dlopen闭合。conf为ldconfig输入，按C locale glob64的相对/绝对include与* ? []、depth32闭合完整匹配，实际零匹配允许，未知不当零；不运行ldconfig修复或用Java **/brace近似。确实无关目录成员可保留，可能参与加载/include者不得现场批准。以上与service/target合并新LinuxDeployment schema，旧schema1不补认缺失语义，旧ELF格式1的token拒绝也不悄悄改义。当前目录观察尚未完成新schema、cache/conf parser、candidate/selection solver、实际Linux接受/拒绝或v2 qualified启动；各整体Gate仍OPEN，完整16步active，默认OFF/SYSTEM/PARK及disabled不变。

## Linux 声明加载路径的物理观察

2026-11-01在父`33353b2ad6571c4566c2342a68d00576c4f8f98b`上，按规划会话只读裁决，在原SegmentClassicLaunchTrust增加包内Loading路径观察；它只核验声明alias、目录、config/cache存在或缺失和interpreter入口，返回原物理身份/缺失位置及流式父链摘要。逐段验证root或原owner的真实父目录、ACL及替换权限，再处理已声明链接；未声明/不符owner/多链接/错误target/循环或超限拒绝，目标文件仍单独走原hash/ELF合同。缺失只接受原始NoSuchFileException，且实际存在的父目录不允许其他主体创建；不把不可读、错误或悬空链接当缺失。整组观察两次必须相同，不循环刷绿，也不把A/B相同当作原子或连续保护证明。摘要只比较同进程观察，不是持久或签名编码；流式处理不保留无界父链列表。普通checked、根/key/recipe/一般文件的禁止别名及regular/dir ACL语义保持。

窄NativeIO.observeSymbolicLink返回复制的原始target byte[]和无跟随身份；复用受控JNI，固定lstat→readlink→lstat，比较dev/inode/type/owner/group/nlink/size/mtime/ctime，不比较atime；4097-byte固定buffer检测4096-byte上限，拒绝截断、未知结果/平台/文件系统/缺符号，不open/dup/close或跟随最终target。Linux支持的本地文件系统白名单排除procfs magic link，完整生产profile仍限实际ext4及既有Linux资格；Mac APFS/HFS只作组件观察，不取得Linux资格。普通Linux symlink mode0777不按regular文件写权限解释，绝不把hasExtendedAcl(symlink)的拒绝转换成无ACL。trust严格UTF8且与声明原UTF8 bytes相同后，才按批准规则解析target；一般路径读取入口不扩张。

初始native8项为7通过/1 Linux条件跳过，server三类43项全部通过；最终native/readlink/ACL/rename三类16项为15通过/1 proc magic-link的Linux条件跳过，server trust/program/deployment/materials/publication/preload六类67项全部通过、0失败/错误/跳过。覆盖实际相对多跳/loader叶链接、目录缺失层级、错误presence/target、未知和悬空链接、世界可写sticky父目录下不可接受的缺失、同字节alias替换、实际路径调用体挂起时close等待/重入拒绝及旧无别名文件读取仍拒绝。四阶段各1415项输入分别核验原快照或父Git字节，无缺失/错配；最终八个代码源与通过样式和最终native/server输入完全一致。首次两处Java长行样式失败已修正、最终Spotless/Checkstyle退出0；不存在的测试名曾在Maven前被选择预检拒绝，未计为测试执行，原记录保留。

host release和Linux x86_64交叉check初始/最终各退出0；只对新增Rust函数格式化并检查退出0，不把它扩成全Rust样式结论。最终Mac JNI SHA-256为`75a9e733c7e0635960eafb590bc9a7d6d6f41b52e8dd2270f8484edf9b7bf44c`。另有四个真实独立JVM探针均退出0：原始非法UTF8 target和含重复斜线的relative target原字节保留、trust严格拒绝非法目标、前一精确JNI的新符号缺失拒绝而原open/close保持；class/source/native精确绑定保留。Mac文件系统创建4096字节target被errno63拒绝，未取得该内核边界阳性，Java4096/4097边界是纯数据验证。build.rs在构建后从已验证不变的父Git字节补存，另外三个原生输入在各次构建前快照；不把晚采样伪称原始快照。

证据`/Users/liusinan/.codex/artifacts/unified-wal/classic-linux-paths-evidence-33353b2a`保留各原source/class/XML、命令、库、失败及探针结果。终态RAT退出1，原431项未批准路径及父333字节不变，扫描前排除用户PDF。两次独立lsof及原inode/SHA/硬链接检查后，清理四个Maven raw scope的8目录/26文件路径/1,584,691逻辑字节，归档原硬链接保留、regular-file数据块回收0；清理四个任务自有Cargo target的194目录/894文件/182,334,563逻辑字节，物理回收未测；原始探针fixture另清理1目录/2 symlink/1 regular file/21逻辑字节，原目标hex和结果保留。共享target、工具链和用户文件未清理。

当前仍是包内物理来源观察，不是完整候选目录枚举、ld.so config/cache解释、hwcap选择或传递依赖闭合；没有从这些结果签发Installed/START/Unix资格，也尚未接入launcher qualified启动。Linux实际运行阳性、service/target新schema、cgroup/dropper/v2启动/UDS/Ctl和各整体Gate仍OPEN/NOT_EXECUTED，本增量无TLA/性能；完整16步目标active，默认OFF/SYSTEM/PARK及disabled不变。

## Linux 最终文件的原句柄摘要与 ELF 观察

2026-11-01在父`0882733b0a8ba7466590ca95606bb224cf6d316f`上，SegmentClassicLaunchTrust复用原保护路径/ACL、固定owner、单链接、mode、长度、父链、流式SHA-256和SegmentOwnedFiles首次关闭合同。v1原程序角色与向量不变；包内Linux文件适配支持真实零长度配置，不借用JDK角色。ELF元数据与摘要使用同一原FileChannel，检查读取和关闭后的deadline、路径对象及父链；任何读取/解析/关闭失败保留在原launcherFailure。外部close等待实际读取体退出，重入drain拒绝，首次关闭失败不重试或伪称排空。返回值只包含观察身份及元数据，不产生Installed、START或Unix资格。

SegmentClassicLinuxElf只读有界ELF64 little-endian x86-64：程序头/LOAD文件与虚拟地址映射、entry、PT_INTERP、DYNAMIC终止与字符串、DT_NEEDED原顺序、SONAME、显式绝对RPATH/RUNPATH及批准flags。对表/范围/算术溢出、歧义映射、坏UTF8、相对/空搜索及token扩展、audit/filter/未知加载扩展明确拒绝；不打开或关闭调用方句柄，不mmap、装载、执行、搜索或自动批准依赖。它不是完整重定位/符号或运行时加载选择验证器。当前只读取普通无别名最终文件；物理系统别名、目录/config/cache/hwcap选择及完整依赖比较仍待接线。

首轮两类27项中26通过、一处独立合成夹具将DYNAMIC的虚拟地址反例写到p_paddr（不参与此格式加载）而失败；修正夹具偏移200→192，生产代码未为此变更。最终ELF/trust/program/deployment/inputs/launcher六类54项全部通过，0失败/错误/跳过；覆盖真实文件、空配置、权限/SHA/角色拒绝、关闭时同字节inode替换、首次native close失败，以及实际ELF读取体挂起时close等待/自drain拒绝。每阶段1414项源码/资源分别与原快照或父Git blob核验，无缺失/错配；最终四个代码源与通过样式快照完全一致。独立Spotless/Checkstyle退出0。

另只读既有缓存中的仓库声明Netty 4.2.15.Final Linux x86_64 ELF，106584字节、SHA-256 `422aab63e1e07cbac931d4b69d9df599b2a619bd72e9894eab6496689bb2e701`；实际parser与系统objdump的NEEDED原顺序`libdl.so.2/librt.so.1/libc.so.6`、SONAME及flags相符。该文件在macOS被复制/读取，没有被执行、装载或当作部署systemctl闭包；不计Linux实际运行阳性。终态RAT退出1，原431项未批准路径及父088字节不变，扫描前排除用户PDF。证据`/Users/liusinan/.codex/artifacts/unified-wal/classic-linux-files-evidence-0882733b`保留原source/class/XML、失败与objdump/parser输出；两个raw scope经两次独立lsof/原inode/SHA/归档硬链接核验后清理4目录、14文件路径、1,009,293逻辑字节，归档保留、regular-file数据块回收0，共享target和用户文件未清理。

规划会话已接受持续决策授权，原launcher service与批准target须分kind，新方向关系与安装链接用新LinuxDeployment schema表达，旧schema/constraint1不补认缺失关系；尚未实施该schema。实际Linux依赖闭合、连续systemd/cgroup隔离、dropper、v2启动/UDS/Ctl和Unix交接仍OPEN/NOT_EXECUTED，本增量未执行TLA、性能或qualified阳性。完整16步目标active，各整体Gate与默认OFF/SYSTEM/PARK、disabled保持原边界。

## 固定 ProcessImpl 缓存的只读 JNI 观察

2026-11-05在父`a2e52a21693a0b48fc7320195c4aee4e717ef516`上，既有NativeIO/Impl/Jni及原Rust库增加无参数`jdkProcessLaunch()`。只定位bootstrap java.base的固定ProcessImpl及LaunchMechanism，取得实际static缓存的机制name/ordinal和helperpath原bytes；不接受caller类/字段/路径/机制，不设置属性或字段，不调用start/fork/exec。未知类来源、布局、枚举或缺失数据明确失败，不回退到可变属性或推算路径。Java默认接口仍拒绝未实现能力，旧库缺新增symbol由明确NativeIOException保留原LinkageError。

helper数组先检查2..4097长度再复制，唯一末NUL和全部不透明路径bytes保留，不经过字符替换解码；数据对象在有界复制后验证内容，构造与访问均防修改。[JNI GetStaticFieldID规范](https://docs.oracle.com/en/java/javase/21/docs/specs/jni/functions.html#getstaticfieldid)允许触发类初始化；Java入口显式初始化固定类，这可能等待或失败，不能持trust monitor或以超时声称native/class-init已终结。本块尚未接入trust同步段。初始化时普通Error包装为NativeIOException，VM级致命错误和ThreadDeath继续抛出；没有改变JVM选项或module opens。参考[OpenJDK21u ProcessImpl](https://raw.githubusercontent.com/openjdk/jdk21u/jdk-21.0.10-ga/src/java.base/unix/classes/java/lang/ProcessImpl.java)只解释读取对象，不认证实际批准vendor布局。

实际构建新Mac aarch64 Rust release库，退出0；新增JNI symbol存在，库长度337024、SHA-256 `75e596843fedb8fdd5542787ee2be2b562e1a8e35827114692d1642b4b4504eb`。Amazon JDK21.0.10独立JVM在`-Xcheck:jni`下实际调用新增符号：初始化前分别选择FORK/POSIX_SPAWN，初始化后把属性改为非法值仍返回原缓存；另一个JVM初始非法选择和随后的错误初始化状态均拒绝。新增4项全部通过，0失败/错误/跳过，包含opaque bytes、复制与固定边界。首轮4项中的1失败复现本机JDK直接抛plain Error，原source/class/log/XML保留；修正后不移绑旧PASS。实际旧库探针验证旧credentials符号成功、新symbol明确拒绝。既有NativeIO七类38项中36通过、2项按平台条件跳过（Linux proc magic link和close/descriptor复用），无失败/错误；样式退出0。Linux x86_64 `cargo check`退出0，仅为编译检查，没有链接/运行Linux库。

证据位于工作区外`/Users/liusinan/.codex/artifacts/unified-wal/jdk-process-cache-evidence-a2e52a21`。三个测试阶段各自1437/1437/1438项输入与原snapshot/父Git blob无错配，最终五code与修正测试、回归及样式相同；实际JNI调用及新旧库SHA分别绑定。首份RAT输入文件误复制首轮三个旧源码SHA，原报告保留但不作当前源码绑定；修正runner并在当前源码及两份文档快照下重跑的最终RAT退出1/431，原未批准路径及字节为父HEAD既有，无新增；用户PDF在扫描前排除且未读取。三个raw scope经两次lsof/identity/SHA/archive hardlink核验后清理6目录、18文件路径、834512逻辑字节，归档保留、regular-file数据块回收0；两处本次Cargo target在独立双次核验后清理447文件路径、91265437逻辑字节，保留原构建源码/log及新库，未测量物理回收量。共享target、旧sealed库和用户文件未清理。

这份观察仅证明测试环境的实际缓存事实，不是原program批准来源、helper路径对应、backend许可或传递依赖闭合。`requireLinuxDiagnosticsProcessSource()`仍Unsupported，该观察入口不执行helper或解除生产启动拒绝；独立JVM的测试启动不计受控部署helper执行或资格，原owner的锁外初始化/前后来源绑定待后续接线。实际批准Linux/vendor阳性、本块models/性能NOT_EXECUTED；完整16步目标active，parent/child/Main/G1/full-bytes/full16/production Gate保持OPEN，默认OFF/SYSTEM/PARK及advanced disabled不变。

## D2 固定 JDK helper 的原批准材料与 PT_INTERP 关联

2026-11-05在父`01e86496e84dae85dbca47904788faf0deb39fdb`上，原trust owner增加`verifyLinuxJdkHelper(deadline)`，只从本owner原verifiedProgram raw javaHome/files与原D2取得声明，不读可变java.home/launchMechanism或查替代helper。固定原文形成`lib/jspawnhelper`位置，先匹配唯一program/member/context原索引：program成员须含JDK位，不机械要求NATIVE位；同路径D2成员必须是START_CHAIN，长度/SHA一致，context须精确匹配该file与执行logical name，不能以同physical的另一个alias替代。路径canonical语法、固定拼接4096字节及4096 program/1024 D2表上限先于目标I/O；program列表固定快照，重复匹配明确拒绝，不用findFirst掩盖冲突。

复用既有fixedLinuxContextMembers的完整context/固定interpreter引用检查，以及原private路径—owned hash＋ELF2读取。helper按START_CHAIN执行角色检查entry/权限，实际NEEDED顺序及重复与原Elf metadata一致，每个原ordinal/candidates仍由原Loading引用关系保留。实际PT_INTERP存在时必须与原loading.interpreter字符串完全相等，才读取原固定loader；另一个字符串即使指向同物理成员也不补认。PT_INTERP缺失则返回null interpreter观察，保留可能非空的NEEDED，不推导没有用户态依赖。

读取helper和适用interpreter后，分别复核两者完整path observation与owned内容identity；能拒绝helper在后续loader读取期间被同字节换inode，也能拒绝loader在helper末端复核期间被替换。没有反复全量hash或重演完整JDK树，不把这份前后关联描述成跨对象原子快照或持续不变保证。结果只有原reference索引、helper及可空interpreter观察；独立`readLinuxJdkHelper` seam只接成员列表/Loading数据，不构造完整Program或system部署，不产生Installed/ready/backend许可。原D2.physical只调整为包内可见，复用原canonical纯字符串语法，codec/role/schema不变。

当前Trust86项（含8项新增真实文件/alias/owned ELF夹具）全部通过，0失败/错误/跳过，样式退出0；包括精确原索引/NEEDED序、无PT_INTERP但非空NEEDED、不匹配解释器在其内容open前拒绝、双向替换窗口、program↔D2路径/role/长度/SHA与exact context的pre-I/O拒绝、canonical/唯一/字节及表上限/期限，以及typed缺source拒绝。原关闭/字节覆盖复用该Trust相关集；未执行helper或重新运行未改的Output/其他矩阵。1436项源码输入与原snapshot/父Git blob无错配，三code与唯一测试及style快照一致，新规划会话只读核对未发现本范围阻塞缺陷。

证据位于工作区外`/Users/liusinan/.codex/artifacts/unified-wal/classic-linux-jdk-helper-evidence-01e86496`。终态RAT退出1/431，未批准路径及字节为父HEAD既有，无新增unknown；用户PDF扫描前排除且未读取。唯一确切raw scope经两次独立lsof及inode/mode/SHA/archive hardlink核验后清理两目录、四文件路径、379,804逻辑字节，归档保留，regular-file数据块回收0。原native属性/库SHA绑定和实际JNI ACL/readlink夹具只证明本机材料读取；既有七份静态来源不作vendor/bootstrap认证。

本块不确认实际ProcessImpl已选择此helper或POSIX_SPAWN，不执行helper，也不证明其NEEDED候选和外部传递依赖已闭合。`requireLinuxDiagnosticsProcessSource()`继续明确Unsupported，不因材料观察成功解除；实际backend来源、传递加载、诊断执行/parser及qualification仍待后续。typed Linux/helper/vendor运行阳性NOT_EXECUTED，本块models/性能未执行；完整16步目标active，parent/child/Main/G1/full-bytes/full16/production Gate保持OPEN，默认OFF/SYSTEM/PARK及advanced disabled不变。

## D2 loader 诊断资源机制：PARTIAL，生产启动仍明确拒绝

2026-11-05在父`2c73c5aa18698bcd71e6910e683e82f537588fca`上，为原trust owner增加一次性诊断槽位及`verifyLinuxLoaderDiagnostics(deadline)`生命周期接线，直接复用原`SegmentClassicSystemdQuery.Output`。**当前生产入口在start claim、ProcessBuilder构造及initialize之前，由`requireLinuxDiagnosticsProcessSource()`明确Unsupported；原JDK backend及helper外部依赖准入尚未实现。本块不交付可执行的生产诊断采集。**没有增加caller许可、可变property/FORK回退、fixture登记接口、schema/role或TCB范围。

保留的A/B/C接线在monitor内登记原Output/caller/inFlight和原期限、执行已有同步文件前检及start claim，锁外initialize/read，锁内检查closing/原source/loader前后绑定并发布防修改raw结果；initializingThread只覆盖同步文件段。其他线程新trust操作在inFlight时拒绝，close不等待诊断调用重新取得monitor，只把原Output非等待物理快照及!inFlight合入原Drain。caller和两consumer不能自排空，首次失败保留，晚终结只能改变物理排空事实，不能恢复观察成功。结果类型构造器私有、stdout复制，caller bytes不能包装成生产来源；上述成功路径目前因启动链guard不可达，只有源码核对，不能写成已实测的trust并发整合。

原Output新增deadline-aware initialize重载，在consumer启动前和实际start前检查原期限；原systemd入口的命令、字节上限、解码及旧initialize行为保持。stdout64KiB/stderr16KiB、空目标env、根cwd、stdin首次关闭、原Process exit/两pipe EOF及首次close责任继续复用。非等待drained快照要求Process实际终结、两consumer退出和首次关闭成功，不以过期业务deadline误报已终结资源。通用Output真实JVM夹具验证该物理合同，不证明实际批准loader或其JDK启动链。

来源核对发现，目标loader自包含条件（ET_DYN/非零entry/无PT_INTERP/无NEEDED）不足以单独放行ProcessBuilder。[OpenJDK21u 21.0.10 ProcessImpl](https://raw.githubusercontent.com/openjdk/jdk21u/jdk-21.0.10-ga/src/java.base/unix/classes/java/lang/ProcessImpl.java)及[native spawnChild](https://raw.githubusercontent.com/openjdk/jdk21u/jdk-21.0.10-ga/src/java.base/unix/native/libjava/ProcessImpl_md.c)存在先执行固定jspawnhelper、使用parent environ再传目标env的路线；这只是参考实现，不认证原approved vendor/backend。既有verifyProgram逐项hash且verifyJdkTree完整枚举要求declared==seen，现场存在的树内helper不能从program漏列；剩余缺口是实际backend与原已核验成员的对应、helper实际PT_INTERP/NEEDED及树外传递加载依据，不能扩大为“任意JDK安装内容可信”。当前可用program/D2向量是合成数据且未含helper，不是生产输入；本机fixture为Amazon 21.0.10+7-LTS/macOS aarch64，另列而不补升Linux来源。glibc2.39的[rtld.c](https://raw.githubusercontent.com/bminor/glibc/glibc-2.39/elf/rtld.c)及[dl-diagnostics.c](https://raw.githubusercontent.com/bminor/glibc/glibc-2.39/elf/dl-diagnostics.c)只为目标诊断早退代码路径提供静态依据，不替代这条JDK启动依赖。

初版WIP的Trust77＋SystemdQuery11共88项全部通过，0失败/错误/跳过，原style0、原source/class/log/XML/输入绑定保留；其中新增Output真实JVM检查包括过期启动、consumer已启动后的期限耗尽/无native start、interrupt后晚终结、consumer自drain拒绝及首次close失败不被后续观察抹除，原env/cwd/stdin夹具也执行。Trust新增仅缺原source的pre-start拒绝、命令/自包含纯检查与真实owned前后数据绑定，没有造Installed或Linux来源。修正启动链guard后只运行两项对应pre-start拒绝检查，全部通过，最终style0；原88未重跑，也未移绑新源码，不合计成“当前源码90项全通过”。两阶段1436项输入均与各自snapshot/父Git blob无错配，当前四code仅与最后两项检查及style-final绑定。新规划会话分别核对A/B/C及最终guard，接受按PARTIAL收口。

证据位于工作区外`/Users/liusinan/.codex/artifacts/unified-wal/classic-linux-loader-diagnostics-evidence-2c73c5aa`。原RAT和修正后rat-final均退出1/431，未批准路径及字节为父HEAD既有，无新增unknown，PDF扫描前排除且未读取；两份RAT分别保留各自输入。两个确切raw scope经各自两次独立lsof及inode/mode/SHA/archive hardlink核验后清理四目录、九文件路径、789,146逻辑字节，归档保留，regular-file数据块回收0。七份glibc/JDK参考原源及SHA另存，不认证原vendor或真实Linux执行。

后续按新规划裁决先复用原program与D2 START_CHAIN/context/ELF/NEEDED，观察固定jspawnhelper材料及其PT_INTERP对应；这一材料边成功也不能撤去启动guard。实际backend来源和外部传递依赖闭合、诊断parser、CPU/platform/hwcap事实、完整搜索/winner及qualification继续待实现。trust新A/B/C并发整合、typed Linux/vendor诊断执行NOT_EXECUTED，本块models/性能未运行；完整16步目标active，parent/child/Main/G1/full-bytes/full16/production Gate保持OPEN，默认OFF/SYSTEM/PARK及advanced disabled不变。

## D2 单个普通 NEEDED base 查找点的实际批准对应

2026-11-04在父`1dd01a8dfab7944ee3cc51206148ae51d8ce7079`上，原trust owner新增`verifyLinuxNeededLookup(ruleIndex, directoryIndex, deadline)`。typed入口先过原source/provider/期限门，只从本owner原D2选择NeededRule和Directory，不接caller name、Path、alias或已有观察。单独的`readLinuxNeededLookup(files, Loading, ...)`只供独立子图真实文件夹具，复用同一private实现，不签发来源或ready。既有context全集方法的文件/引用校验和alias固定只作小提取，不重入public同步入口，首次关闭/失败留存及原owner排空语义不变。

原索引、全部context/固定interpreter引用和本片name形状在相关目标I/O前核对；仅支持非空、无`/`和`$`且不是`.`/`..`的普通名字，其他形状明确Unsupported，不冒充absence。目录raw字符串仅复用DynamicPath的尾slash整理，不经过token/list展开；dirname＋name的完整UTF8字节数在Path和目标读取前检查4096上限。冒号、分号、Unicode、目录字面`$`及内部slash/dot原文保留为精确logical查询。随后从requester原context的owned hash＋ELF2观察按原ordinal取得实际name并验证元数据一致，未以caller字符串代替真实读取。

复用原严格walker检查声明目录的实际presence/owner/ACL/保护及精确candidate probe。只有其受保护父链下的NoSuchFile事实、前后复核稳定，才返回该点缺失；权限/读取异常、目录代替文件及未声明alias均失败，不混为无候选。存在时，必须在候选内容open前同时属于本NeededRule.candidates、精确匹配原context.logicalLoadName并对应实际resolved physical成员；同physical/同SHA的另一logical或其他规则批准不能补认。匹配后才调用原private context关联和owned ELF2读取，并将其完整path及内容identity与最初probe比较，再复核candidate与目录末端观察。后一次读取自身稳定不能掩盖probe后的同字节换inode或同target换alias。结果保留原rule/directory/声明、logical、requester及本次路径/内容事实；成功返回的null candidate仅表示probe的可靠缺失，不使用selected/winner命名。

Trust类74项（含11项新增）和受影响DynamicPath 8项合计82项实际全部通过，0失败/错误/跳过，样式退出0；只执行这一相关测试阶段。新夹具为合成ELF2的真实文件/alias/owned channel/JNI ACL/readlink路径，覆盖批准命中与原索引、精确logical拒绝且未open候选、保护目录和显式目录缺失、probe后两类替换、内容关闭时目录变化、absence转present、AccessDenied/目录/未声明link，以及unsupported/越界/拼接字节/typed缺source的pre-I/O拒绝。既有Trust关闭/期限覆盖继续执行，不重复无关旧矩阵。1436项源码输入与原snapshot/父Git blob无错配，三code与该唯一测试及style快照一致；新规划会话只读检查未发现冻结范围内阻塞缺陷。

证据保留在工作区外`/Users/liusinan/.codex/artifacts/unified-wal/classic-linux-needed-lookup-evidence-1dd01a8d`，包括原源码/class/log/XML/native属性和库SHA绑定。终态RAT退出1/431，未批准路径及字节与父HEAD相同，无新增unknown；用户PDF扫描前排除且未读取。唯一确切raw scope经两次独立lsof及inode/mode/SHA/archive hardlink核验后，清理两目录、五文件路径、423,138逻辑字节；归档保留，regular-file数据块回收0。[glibc 2.39 dl-load.c](https://raw.githubusercontent.com/bminor/glibc/glibc-2.39/elf/dl-load.c)的open_path仅作为dirname＋空cap suffix＋name位置的静态依据；本块不模拟其完整接受/继续搜索策略，三份既有原源静态归档不计实际vendor执行。

交付只证明普通NEEDED在一个原声明base目录中的实际查找与批准对应，不证明该目录实际进入搜索、其他位置缺失或整体winner。所有请求/目录全集、hwcap/cache/NODEFLIB/default闭合、token/slash请求、实际namespace/l_loader链、first-loaded reuse、diagnostics/systemd2及qualification仍待后续。typed Linux阳性和实际vendor运行NOT_EXECUTED，本块models/性能未执行；完整16步目标active，parent/child/Main/G1/full-bytes/full16/production Gate保持OPEN，默认OFF/SYSTEM/PARK及advanced disabled不变。

## D2 目录搜索分支的动态标签来源阶段

2026-11-04在父`91f91b9c9e068cd40109617a63ca694e76213504`上，新增包内`SegmentClassicLinuxSearchStages.select(requesterToAncestors, main, deadline)`纯数据函数。调用方必须明确提供非空requester→l_loader祖先有序对象链和main对象；不从D2的file/logical Context或NEEDED候选图推导链。ObjectData的opaque identity与physical文件、context、SONAME及PID/inode无关，context保留原0..4095索引，不同对象共享context允许且不合并。链与链外main合计最多4096个不同identity；先有界固定输入并检查链内重复identity/循环、main完整数据冲突及总数，再选择来源。链内main数据完全一致时复用，不重复计数或补入。

requester的RUNPATH标签只要present（包括空值），就屏蔽整个RPATH链及main fallback，结果仅保留requester自己的原RUNPATH来源；不会因目录展开为空、不可用或未观察而改用RPATH。requester无RUNPATH时，按链原顺序保留各对象有效RPATH；祖先自身有RUNPATH只屏蔽自身RPATH，不截断更远祖先，也不继承祖先RUNPATH。main不在链内时最后考虑其有效RPATH。present-empty RPATH仍作为原标签来源数据保留，后续展开产生零目录；不同对象相同标签/dirname不跨对象去重。静态语义核对自[glibc 2.39 dl-load.c](https://raw.githubusercontent.com/bminor/glibc/glibc-2.39/elf/dl-load.c)、[get-dynamic-info.h](https://raw.githubusercontent.com/bminor/glibc/glibc-2.39/elf/get-dynamic-info.h)及[dl-object.c](https://raw.githubusercontent.com/bminor/glibc/glibc-2.39/elf/dl-object.c)，不是实际批准vendor运行。

输出只包含原(tag, owner identity, context, DynamicPath)引用的不可变`rpathBeforeEnvironment`和`runpathAfterEnvironment`列表。中间环境路径阶段仍未观察，不能当作已验证为空；本块不求值token或目录，不生成context×目录×候选矩阵。原deadline贯穿结构校验、来源遍历及正常返回。D2 schema/codec/LoaderPolicy和所有消费者未改；记录只供包内纯数据调用，不表达真实已加载namespace、Installed或ready资格。

六项新增纯测试全部通过，0失败/错误/跳过，覆盖present-empty RUNPATH屏蔽、祖先次序和仅屏蔽自身、main链内/链外及冲突、共享context/标签不合并、4096总对象边界、不变来源/结果及期限。首次样式因测试四行LineLength退出1，保留原source/log，只有换行修正后style-final退出0，再执行唯一六项测试；原失败不移绑。1436项源码输入逐项匹配原snapshot/父Git blob，两code与该唯一测试及style-final快照相同。旧token/DynamicPath/ELF1和消费者未改，未重跑旧矩阵或重复相同源码测试；native路径属性及SHA核验不计本阶段JNI执行。新规划会话只读核对未发现冻结范围内阻塞缺陷。

证据位于工作区外`/Users/liusinan/.codex/artifacts/unified-wal/classic-linux-search-stages-evidence-91f91b9c`，三份静态原源及各自SHA另存。终态RAT退出1/431，未批准路径及字节与父HEAD相同，无新增unknown；用户PDF扫描前排除且未读取。唯一确切raw scope经两次独立lsof及inode/mode/SHA/archive hardlink核验后清理两目录、四文件路径、363,731逻辑字节，归档保留，regular-file数据块回收0。

交付仅为已经进入目录搜索分支、base namespace普通请求上的显式数据来源阶段。真实identity/祖先/main来源、首次已加载复用、name/slash/DST分流、secure-mode/ld_inhibit_rpath、CWD/ORIGIN/platform、环境/cache/NODEFLIB/default/hwcap及实际winner仍待接线；diagnostics/systemd2与完整qualification未闭合。实际Linux/vendor阳性NOT_EXECUTED，本块models/性能未运行；完整16步目标active，parent/child/Main/G1/full-bytes/full16/production Gate保持OPEN，默认OFF/SYSTEM/PARK及advanced disabled不变。

## D2 单个 DynamicPath 的有界目录列表展开

2026-11-04在父`ef96d51d7ed4d1df5ded03b5f4c531e14e3210b9`上，新增包内`SegmentClassicLinuxDynamicPath.expand`，复用原token helper，对单个原D2 DynamicPath生成有序不可变目录数据。保留原声明对象及absent/present-empty区别；两者均不生成目录。非空raw先按冒号拆分并保留首尾/相邻空段，原始空段生成明确的Kind.CWD/空dirname，不读取或推导进程cwd。分号及替换值产生的冒号保留为dirname字面内容，不再次拆分。

非空段的AVAILABLE空或UNAVAILABLE只略过该段；遇到UNOBSERVED，整组返回该状态和空列表，不暴露已得到的prefix。未使用或被首个UNAVAILABLE短路的token不额外阻断。非空dirname只去掉多余尾slash并补足一个，root保留`/`；内部重复slash、dot/dotdot和相对拼写不normalize。最终dirname按strict UTF8字节精确相等首次顺序去重，不合并物理目录或不同Unicode拼写。原raw由DynamicPath构造检查strict UTF8/NUL/4096字节，全部显式AVAILABLE替换数据先经原token函数验证，单段输出及补尾slash后仍受4096字节上限约束；整次保留原deadline。没有Path或文件系统I/O，不产生来源、Installed或ready资格。

八项新增纯单测全部通过，0失败/错误/跳过，样式退出0；包括absent/empty/冒号、边缘空段及4096个冒号、展开后空、不可用/未观察、替换后冒号/分号、slash/首次去重、不同Unicode拼写、字节及期限边界。原token及旧消费者未改，未重复相同阶段或旧矩阵。1434项源码输入逐项匹配原snapshot/父Git blob，两项code与唯一测试及style快照一致；native库路径属性/SHA核验不计本阶段JNI执行。新规划会话`01a1a6a1-894f-7cf0-a08b-bc1332179d93`只读检查未发现本范围阻塞缺陷。

证据保留在工作区外`/Users/liusinan/.codex/artifacts/unified-wal/classic-linux-dynamic-path-evidence-ef96d51d`。终态RAT退出1/431，未批准路径及字节与父HEAD相同，无新增unknown；原PDF在扫描前排除且未读取。唯一确切raw scope经两次独立lsof及inode/mode/SHA/archive hardlink核验后清理两目录、四文件路径、363,699逻辑字节；归档保留，regular-file数据块回收0。静态依据沿用[glibc 2.39 dl-load.c](https://raw.githubusercontent.com/bminor/glibc/glibc-2.39/elf/dl-load.c)的fillin_rpath/decompose_rpath/cache_rpath，原源70348字节及SHA `26345f234ef918c866e40bb86524bdfd4c3beac487af81b3f51668379e671ae0`另存，不计实际vendor loader运行。

此为单tag纯目录数据，实际CWD、ORIGIN/LIB/PLATFORM来源、secure-mode准入、RPATH/RUNPATH优先级、祖先链、cache/hwcap/first-loaded reuse及整体候选选择仍未交付；diagnostics/systemd2及qualification保持后续范围。实际Linux/vendor loader阳性NOT_EXECUTED，本块未执行models/性能；整体parent/child/Main/G1/full-bytes/full16/production Gate保持OPEN，默认OFF/SYSTEM/PARK及advanced disabled不变，完整目标active。

## D2 token rule1 的有界单字符串机制

2026-11-04在父`9ef2ed244f95027f1d24b3e97036c8361c26fe5c`上，新增包内`SegmentClassicLinuxTokens.expand(raw, origin, lib, platform, deadline)`纯数据函数，供后续搜索逻辑共用。依据[glibc 2.39 dl-load.c](https://raw.githubusercontent.com/bminor/glibc/glibc-2.39/elf/dl-load.c)的单字符串DST识别/替换边界实现ORIGIN、LIB、PLATFORM及其curly形式；ASCII标识符尾边界精确处理，未知或不完整的当前`$`按字面保留后继续扫描。替换值不递归扫描，冒号/分号、内部slash和dot字串保持，不作shell、列表或Path处理。

不可变Value只有AVAILABLE、UNAVAILABLE、UNOBSERVED三态；AVAILABLE必须携带非null text且允许空，另外两态不带text。这些只是数据，不证明运行来源或ready。只有实际识别到的已知token消费其Value，左到右首个阻塞项返回原状态，不返回prefix或空路径，也不继续把后续状态推断成整体已观察；未使用的UNOBSERVED不阻断literal输入。原raw空及AVAILABLE空与不可用/未观察明确区分。

完整raw及全部AVAILABLE替换值在输出分配前先检查strict UTF8、NUL和4096字节上限。输出使用固定有界工作区，每次literal/replacement追加前检查展开后的UTF8字节数，超限明确失败而不截断；全部正常状态返回前复核原deadline。不读取cwd、系统属性、filesystem或runtime信息，也不自行推导origin/platform。secure-execution ORIGIN额外准入、真实值来源及完整搜索规则仍由后续消费者负责。

七项纯单测覆盖known/curly/ASCII及Unicode边界、相邻/重复和未知序列中的后续`$`、非递归、两种阻塞排列、未用UNOBSERVED、原空/AVAILABLE空、未知语法、UTF8展开边界/坏surrogate/NUL和期限，全部通过，0失败/错误/跳过；样式退出0。仅新增独立helper和测试，原ELF1及消费者未改，未重跑无变化的旧大矩阵，也未为“final”重复同一组七项。1432项输入逐项匹配原snapshot/父Git blob，两项code与该唯一测试阶段/style快照一致。此为纯逻辑证据；runner的native库路径属性及SHA核验不作本阶段JNI执行证据。

静态原源70348 bytes及SHA `26345f234ef918c866e40bb86524bdfd4c3beac487af81b3f51668379e671ae0`保留在工作区外`/Users/liusinan/.codex/artifacts/unified-wal/classic-linux-token-evidence-9ef2ed24/primary-source`，不是批准vendor loader的实际运行。终态RAT退出1/431，未批准路径及全部字节与父HEAD相同，本次无新增unknown，用户PDF扫描前排除且未读取。单个确切raw scope经两次独立lsof及原inode/mode/SHA/archive hardlink核验后，清理两目录、四文件路径、363,055逻辑字节；归档硬链接保留，regular-file数据块回收0，原测试/源码/class/命令及静态来源分开保存。

下一最小范围按规划裁决为单个D2 DynamicPath到有序目录项的纯数据展开：保留标签presence，区分原始空段的CWD语义、非空段展开为空/不可用及未观察，不按替换后冒号再拆分，按最终dirname首次顺序去重。当前token函数不构成这份列表逻辑、RPATH/RUNPATH优先级、祖先链、候选搜索闭合、cache/hwcap/first-loaded reuse、diagnostics、systemd2或qualification；实际Linux/vendor loader阳性NOT_EXECUTED，各整体Gate继续OPEN，完整16步目标active。未运行本块models/性能，默认OFF/SYSTEM/PARK及advanced disabled保持。

## D2 候选 context 全集的 owner 内关联观察

2026-11-04接续父`a87ec6e0b743d7da87efc36821f0a19ed0f22276`，原trust owner增加`verifyLinuxCandidateContexts(deadline)`：只从本owner的原D2部署取得files、Loading及全部contexts，先过原source/provider/期限门。每个原context按原序调用既有private路径—owned ELF2关联读取，随后核验固定interpreter；保留context/file原索引和raw logical name，不按physical或SONAME合并，不只读取某个预选winner分支。任何必需项失败，整次失败，不返回部分集合，也不接受caller观察结果补升来源。

NEEDED结果按原requester/ordinal从已经成功观察的Image2取得实际name，保留原NeededRule及candidate context索引；重复名称的不同ordinal独立存在。原ModuleRequest顺序、重复和candidate索引不改。全部context、固定interpreter、资源健康及原deadline通过后才构造不可变结果；每项仍要求前后完整logical路径观察及identity与同次owned hash/ELF2成员一致。当前只把原声明全集接到实际关联数据，不证明跨对象原子快照、实际搜索候选闭合或loader选择。

原D2构造器的context子图检查提成包内pure `requireContextReferences`，固定loader/interpreter检查也局部复用；完整构造器仍执行原alias/file冲突、config/default directories、systemctl/units等全部检查，wire/codec不变。独立data-only入口只接files＋Loading，先固定原file索引并限制1024项，在首次目标I/O前核对loader role/interpreter物理对应、ELF metadata覆盖和全部context/needed/module引用。Loading沿用4096 contexts、needed＋modules合计8192、candidate memberships合计262144上限。alias map只从同一Loading.aliases内部派生，不提供第二份caller alias声明；helper不返回完整deployment valid、permit或ready。

真实macOS aarch64/Corretto21.0.10+7-LTS fixture使用两个physical ELF、三个logical contexts（同loader的两个alias view）及固定interpreter，验证全集不丢项、重复NEEDED不同ordinal与module请求顺序/重复、无module请求时损坏第二NEEDED候选仍触发实际读取/首次close及整次失败，以及子图缺ordinal、错误loader role/interpreter对应、超1024 file表和缺原source在目标I/O前拒绝。fixture仅构造Loading子图，不伪造完整system/service部署；沿用既有关闭/deadline覆盖。首轮三类82项、最终串行十八类202项全部通过，0失败/错误/跳过，样式退出0；两个阶段各1430源码/资源逐项匹配原snapshot/父Git blob，最终三项code与test/style一致。

终态RAT退出1/431，未批准路径及全部字节与父HEAD相同，本次代码无新增unknown，用户PDF扫描前排除且未读取。两个确切raw scope经两次独立lsof及原inode/mode/SHA/archive hardlink核验后，清理四目录、27文件路径、1,634,102逻辑字节；归档硬链接保留、regular-file数据块回收0。工作区外证据为`/Users/liusinan/.codex/artifacts/unified-wal/classic-linux-context-universe-evidence-a87ec6e0`。

新规划会话只读检查未发现本切片阻塞缺口，下一最小范围按裁决进入D2 token rule1的有界词法识别与单字符串替换纯数据机制，不再增加集合包装。实际typed Linux阳性NOT_EXECUTED；统一快照、实际候选搜索闭合、RPATH/first-loaded reuse solver、diagnostics、systemd2、完整parent/child/Main/G1/full-bytes/16步/生产Gate继续OPEN。本块未运行models/性能，默认OFF/SYSTEM/PARK与advanced disabled保持，完整目标active。

## D2 load-context 与 interpreter 的实际路径关联观察

2026-11-04在父`f23b3712960b6b0120c1263d546458edf2e563ed`上，原trust owner新增`verifyLinuxLoadContext(contextIndex, deadline)`与`verifyLinuxInterpreter(deadline)`。前者先过原source/provider/期限门，再从原D2 context表检查索引、ELF role及引用，随后才构造目标Path和执行I/O；普通context的`-1`明确拒绝。后者固定取原`loading.loader/interpreter`，结果中的`context=-1`仅表示该固定interpreter，member仍为原loader index，不作fallback或任意context选择。

每项保留原context/file索引与raw logical load name，通过原alias表和实际readlink bytes逐组件观察logical路径，要求resolved path等于声明physical member；同一次原owned channel完成SHA＋ELF2读取及首次close。关联返回前，前后完整logical path observation（包括alias链摘要）必须相等，前后完整叶子identity还必须都等于本次owned member观察的identity。仅相同字节或相同resolved pathname不充分。原权限/ACL/父链、ELF元数据匹配、资源健康及原deadline保持，不读取依赖winner或展开祖先RPATH。

原ELF member主体抽为private观察函数，原typed member入口复用；context/interpreter在同一原owner操作中调用它，没有嵌套同步公开入口提前清除`initializingThread`。包内data-only seam可消费独立Context/File/Elf及alias声明以复用同一私有I/O实现，但不会保存来源资格、取得Installed/provider ready或向typed入口提供可采纳的既有结果。当前仅逐项关联事实，不构成统一加载环境快照、候选完整性或实际loader选择证明。

真实macOS aarch64/Corretto21.0.10+7-LTS fixture验证原索引7/4和alias/nested-dotdot原logical、引用/physical错配在内容open前拒绝、owned读取及close已完成后同字节换inode无法返回关联、首次close时alias改向拒绝、interpreter执行位与固定marker，以及实际ELF positional read期间自排空拒绝/外部close等待。原首次close失败与其他身份/元数据回归继续复用；不注入ready或伪造Installed。三类首轮80项和最终串行十八类198项全部通过，0失败/错误/跳过，样式退出0；两个阶段各1430源码/资源逐项匹配原snapshot/父Git blob，最终三项code与test/style快照一致。

终态RAT退出1/431，全部未批准路径及字节与父HEAD相同，本次代码无新增unknown；用户PDF在扫描前排除且未读取。两个确切raw scope经两次独立lsof及原inode/mode/SHA/archive hardlink核验后，清理四目录、27文件路径、1,632,767逻辑字节，归档硬链接保留、regular-file数据块回收0。工作区外证据为`/Users/liusinan/.codex/artifacts/unified-wal/classic-linux-load-path-evidence-f23b3712`，原阶段不移绑。

本块在用户2026-11-04指定的新规划会话`01a1a6a1-894f-7cf0-a08b-bc1332179d93`完成只读交接与裁决，实施仍为原唯一入口。下一最小范围为原D2候选上下文全集的owner内关联观察：只接原deadline，保留全部contexts、固定interpreter、NEEDED ordinal/名称及module请求原顺序/重复，不因SONAME/file去重丢掉logical context，不只观察预选winner的分支；任一必需项失败整次失败，不出部分集合或接caller结果补升来源。复用原D2引用和固定上限，不新建codec或通用图框架。实际typed Linux阳性仍NOT_EXECUTED，候选搜索闭合、token/祖先RPATH/first-loaded reuse solver、diagnostics、systemd2、完整parent/child/Main/G1/full-bytes/16步/生产Gate继续OPEN，未运行本块models/性能；默认OFF/SYSTEM/PARK及advanced disabled保持，完整目标active。

## D2 search directories 的整表枚举观察

2026-11-04接续父`866d3d52a8603264c40155c5f40a55e658acd65b`，原`SegmentClassicLaunchTrust.verifyLinuxSearchDirectories(deadline)`一次选择本owner原D2目录清单；原source/provider和期限检查先于目标I/O，目录、alias及预算来自内部，不收caller声明、外部budget或已有结果。观察保留原directory index、raw logical字符串、明确presence、实际路径链及完整成员名。复用原walker、DirectoryStream登记和首次close；原D1目录接口仅适配原事实，共用扫描机制。

A/B各新建一份原固定预算，同一遍全部目录共享262144个成员及64MiB UTF8名称字节上限。同一physical目录每遍只枚举一次；跨alias复用名单前检查完整叶子identity，当前logical路径和alias链还要重新观察，变化使整遍失败，不能用新identity配旧names。名单完整枚举并保留无关成员，不沿成员链接读取目标，也不据名称批准候选或选择winner。原raw alias target和readlink bytes、逐component字节检查、dot/dotdot及尾slash目录要求继续保留。

D2目录使用允许root的LOOKUP取得事实后，严格检查声明presence、实际directory、原owner/ACL及不可写条件；明确absence与存在但空目录分开。walker仅让LOOKUP/LITERAL在pending为空时按原叶子规则返回root，D1 DIRECTORY、CONFIGURATION、ALIAS、INTERPRETER的root拒绝保持。真实fixture也验证经alias指向root的旧DIRECTORY入口仍拒绝，不能以新D2能力回补旧声明合同。

共用data-only整表方法返回A/B图事实；窄单遍方法允许fixture持原固定budget，通过原retain预收费后枚举两个小型真实目录，验证第二个目录因累计member或name-byte额度拒绝、两个原iterator实际首次close、整遍不返回部分清单。budget计数字段private，typed入口自行创建预算，没有cap参数、setter或reset。该fixture只证明共享预算与异常关闭机制，未实际枚举26万文件，也不是容量或性能验收。fixture仍不能取得Installed、原D2 provider ready或typed成功。

Corretto21.0.10+7-LTS/macOS aarch64下，首轮离线因lightproto0.7.0本地artifact的repository来源记录与当前仓库ID不匹配，在Server编译前停止，0测试；原source/log及当时target class库存保留，库存不称本块编译或PASS。正常联网解析后Trust＋Graph两类69项全部通过，样式退出0；随后离线串行十八类190项全部通过，0失败/错误/跳过。七个新增方法覆盖原索引3/9/11/15与logical原文、alias复用/absence/empty、file dotdot/尾slash拒绝、第二遍实际变化、alias间换inode拒绝、上述两种预收费预算/root及无原source的pre-I/O拒绝。三个阶段各1430源码/资源逐项匹配原snapshot/父Git blob，最终两项code与test/style快照一致，原失败不移绑。

终态RAT退出1/431，全部未批准路径及字节与父HEAD相同，本次代码无新增unknown，用户PDF扫描前排除且未读取。三个确切raw scope经两次独立lsof及原inode/mode/SHA/archive hardlink核验后，清理六目录、29文件路径、1,851,322逻辑字节，归档硬链接保留、regular-file数据块回收0。工作区外证据为`/Users/liusinan/.codex/artifacts/unified-wal/classic-linux-search-directories-evidence-866d3d52`。

本块交付D2搜索目录整表的数据观察，不构成整个加载环境的统一快照、候选闭包或qualification。实际typed Linux阳性仍NOT_EXECUTED；下一最小范围按规划裁决为原D2 load-context与interpreter的实际路径关联观察，保留context/file原索引及logical load name，复用ELF2 owned reader和identity核验。祖先RPATH/first-loaded reuse/closure/solver、diagnostics、systemd2、完整parent/child/Main/G1/full-bytes/16步/生产Gate继续OPEN，未运行本块models/性能；默认OFF/SYSTEM/PARK与advanced disabled保持，持续目标active。

## D2 loader 配置与 kind 分派的原 owner 观察

2026-11-01在父`e1400835b344086eb4ebe9a60e52814995fde43e`上，按总体规划会话冻结的范围，原`SegmentClassicLaunchTrust.verifyLinuxLoaderConfiguration(ruleIndex, deadline)`只从本owner绑定的原D2 configuration表选择规则。原production/Installed、program/image/launcher来源、provider及原期限先于规则Path和目标I/O；明确按kind/format分派，独立MODULE_RUNTIME规则仍Unsupported。cache声明长度在目标I/O前限制48 bytes…16MiB；preload必须明确缺失，cache缺失也只消费原表的显式absence，不补默认。两者按原logical路径分别执行A/B，present cache用原owned channel完成SHA＋固定CACHE parser、首次close、原身份/父链/摘要及后置路径复核。结果是数据观察，不产生provider ready、winner或qualification。

LDCONFIG复用原完整include图，仅把私有扫描输入拆成原fileIndex/physical Path/length/SHA/明确kind的material与Path→原target String alias表；没有重建D1 Loading/File。旧D1入口适配原事实和原语义，筛选后不重编号。包内共用`readLinuxConfigurationGraph`只回图数据，caller提供的材料或已读图不能补升typed入口来源。A/B前固定同一份list/map；按physical Path与fileIndex同时建索引，同材料不同kind或其他冲突拒绝，不按插入顺序覆盖。

scanner在进入ldconfig文件/字节预算和hash/parser前检查匹配材料的kind。仅LDCONFIG可读；MODULE_RUNTIME被include命中使整图Unsupported，没有部分图返回，也不打开该模块内容。未命中的模块材料不套用ldconfig的1MiB上限。原16MiB完整字节、65536文件/65536pattern、32活动context、重复计费、完整glob排序/alias/dot/dotdot/尾slash语义保持。alias仍限128、target限4096 strict UTF8 bytes，原readlink bytes逐项比较；新增logical/include原串在Path前逐component检查，已绑定provider的实际Path及每次展开/JNI边界继续检查，不改变逻辑原文。

Corretto21.0.10+7-LTS/macOS aarch64真实fixture验证合法图保留索引7/19、未命中超长/非法字节MODULE_RUNTIME不读、命中在内容open及ldconfig预算前拒绝、physical/index/kind冲突不覆盖，以及同handle cache完整摘要/原元数据。fixture不能取得Installed或原D2 provider ready；无原source的typed入口在目标I/O前拒绝并保持sticky。首轮四类77项有1 failure（原超长拒绝诊断被改写）及1 error（新测试误将独立向量原有MODULE_RUNTIME当可选择规则）；恢复原诊断、修正kind断言后77项全过，原失败源码/class/XML另存。样式退出0；最终串行十八类183项全部通过，0失败/错误/跳过，三个阶段各1430源码/资源逐项匹配原source/父Git blob，最终四code与test/style快照一致。

终态RAT退出1/431，未批准路径及全部字节与父HEAD相同，本次代码无新增unknown，用户PDF扫描前排除且未读取。三个确切raw scope经两次独立lsof及原inode/mode/SHA/archive hardlink核验后，仅清理六目录、35文件路径、2,201,054逻辑字节，归档硬链接保留、regular-file数据块回收0。证据在工作区外`/Users/liusinan/.codex/artifacts/unified-wal/classic-linux-loader-config-evidence-e1400835`，各原阶段不移绑。

可交付范围是逐规则D2 loader配置与完整ldconfig子图的数据观察，不是整个加载环境的统一快照或闭合证明。实际typed Linux阳性NOT_EXECUTED；原D2 search directories完整清单/每遍共享预算及A/B接线、候选闭包/选择、diagnostics、systemd2、完整parent/child/Main/G1/full-bytes/16步/生产Gate继续OPEN。未执行本块models/性能，默认OFF/SYSTEM/PARK及advanced disabled保持，持续目标active。下一块按规划裁决一次选择原D2目录清单，共用每遍成员/名称字节预算，保留原索引、logical路径和明确absence，不逐目录重置预算或发行qualification。

## D2 ELF 成员的原句柄摘要与元数据观察

2026-11-01在父`5bada70b677841e429b1a7a9fc7cd08134f338b6`上，按总体规划会话冻结的范围，在原`SegmentClassicLaunchTrust`接入`verifyLinuxElfMember(int fileIndex, long deadline)`。入口只收原D2表索引和原期限；先要求本owner的production/Installed、原program/image/launcher来源及provider ready，再检查索引和ELF role，之后才构造成员Path、执行原physical字节门及目标I/O。不接caller Path/File/program/parser/资格布尔值，不将D2 String转换成旧File绕过门；非ELF明确拒绝，旧ELF1与cache/config kind边界保持。

原`hashFile`保留metadata/ACL、单链接、UID/mode/角色执行权限、身份/父链、长度、首次实际close、后置SHA和资源健康/期限检查。一次原`SegmentOwnedFiles`打开的channel完成完整流式SHA-256与`readVersionTwo`；只有首次close及全部后检查成功后才返回带原索引、身份/父链、摘要和Image2的观察。沿用原owner同步、sticky failure、初始化自排空拒绝和外部close等待，没有新owner、重试或authority。

为让真实fixture验证生产共用机制，仅抽出`readLinuxContent(held channel, length, fixed kind, deadline)`数据函数；从同channel起点完整hash并固定分派parser，不open/close、不保存来源或资格。纯表选择及元数据比较同由生产和fixture调用：NEEDED原顺序与重复、RPATH/RUNPATH presence/value、FLAGS/FLAGS1数值逐项相等；SYSTEMCTL的PT_INTERP原文还须匹配批准interpreter，角色type/entry规则保持。SONAME与flags tag presence继续作为原观察保留，D2无对应预期字段不表示实际缺失。本块不处理候选winner、祖先RPATH、first-loaded reuse或模块来源。

Corretto21.0.10+7-LTS/macOS aarch64上，真实owned channel读取原独立1536-byte ELF2向量，验证完整摘要、原动态字段、一次打开且数据函数不关闭，以及截断、无进展、parser失败、期限和首次native close失败。原owner缺provider/source时目标I/O检测点不可达且失败sticky；越界及非ELF的原表选择在纯数据层验证，不伪造Installed或注入私有ready。首轮三类62项全部通过；首style仅三行超长退出1，换行后样式0。最终串行十八类177项全部通过，0失败/错误/跳过；两阶段各1430源码/资源与原source/父Git blob逐项匹配，最终三code与测试/style快照一致，原style失败独立保留。

终态RAT退出1/431，路径及全部字节与父HEAD相同，本次代码无新增unknown；用户PDF扫描前排除且未读取。两次独立lsof及原身份/SHA/archive hardlink核验后，仅清理本次两个确切raw scope的四目录、27文件路径、1,631,998逻辑字节，归档硬链接保留、regular-file数据块回收0。证据在工作区外`/Users/liusinan/.codex/artifacts/unified-wal/classic-linux-member-evidence-5bada70b`。

低层真实fixture只证明同handle共用机制和数据比较；typed D2入口的实际Linux阳性仍NOT_EXECUTED。完整D2 walker、loader闭包/选择/diagnostics、systemd2、qualification、parent/child/Main/G1/full-bytes/16步/生产Gate继续OPEN，不签START/handoff或解除v2 childCommand拒绝。未运行本块models/性能；默认OFF/SYSTEM/PARK及advanced disabled保持，持续目标active。

## Linux provider 的原 owner 来源与字节门

2026-11-01在本地父`9f9b08dd67ec44cf3663817f8da49e24f4296c64`上，按总体规划会话裁决，在原`SegmentClassicLaunchTrust`增加`readLauncherInputs(deadline)`与无外来事实参数的`verifyLinuxProvider(deadline)`。原`verifyProgram`只在全部hash、完整JDK/JAR、首次close/资源健康及原preload核验成功后，保存本owner的program decode来源及原`lib/modules` hash读取所返回的身份/父链观察；摘要事实来自同一次成功hash，不用D2路径自证JDK来源。原`.launcher`的一次私有读取内完成raw decode，保存原描述符SHA、typed value和六个外层path原文。旧byte与typed入口共用原一次读取，不能用第二次读取或caller复制值补升来源。

原program codec新增`decodeWithRawPaths`，留存javaHome及逐file原String；launcher codec同名入口先返回不可变raw数据，保留dropper/supervisor/manifest/proof/policy/AES原文，`value()`才构造原typed Path。复用同一解析逻辑，不复制codec、不改变wire；旧decode仍返回原数据，原v1/v2固定字节和上限保持。D2 owner在`value()`前先检查原bootstrap已使用的实际默认filesystem；未知实现直接拒绝，不通过其provider/getPath或未知候选Path方法来探测。缺原program/image仍不能形成typed D2来源。原文不从Path.toString恢复，保留文本并不代表权限或permit。

实际对象固定为批准LinuxFileSystem/LinuxFileSystemProvider/UnixPath与URI/UnixUriUtils，按bootstrap class对象、null loader、同一boot java.base及jrt模块来源核对，不调用installedProviders搜索替代实现、不增加反射开放参数。当前属性为空不证明默认filesystem未被包装：该实例会沿用初始化结果，依据[批准Corretto FileSystems原源](https://raw.githubusercontent.com/corretto/corretto-21/21.0.10.7.1/src/java.base/share/classes/java/nio/file/FileSystems.java)并由真实包装SPI反例核验。模块名、类名、runtime/vendor属性或后置image hash单独均不构成首次启动证明；外部可信安装/原bootstrap TCB仍是前提，不能据本门追认不可信JVM。

最终门只消费owner私有保存的原program/image/launcher来源，要求production preload与Installed、原deadline、D/P2/recipe与child program匹配、实际父JVM/argv/classpath/JDK、runtime build、原image当前身份/保护父链一致。门不接caller program、Path、“verified provider”或布尔资格；最终状态只存本owner，失败进入原launcherFailure且不能重读刷绿，关闭后也不能继续。D2 dropper首次metadata/hash前还要求该状态与原typed input引用，caller构造或跨owner复制的值不能到达D2 dropper I/O。fixture始终不能得到Installed或该成功状态。

普通名称component以显式ASCII百分号编码`file:///` URI构造UTF8预期Path，对比实际String→Path的字节表示；拒绝authority/query/fragment和非三斜杠分支。物理成员及六个外层/program既有Path再核对完整Path字节、同一filesystem/class以及原字符串与实际JNI字符串，不能仅凭组件通过或字符串相等放行。logical context/目录/interpreter/alias target/default目录/安装链接等保留原String；dot/dotdot、重复分隔符、尾斜杠不提前消解，不把整个URI转换结果当原文。相关源码明确[UnixUriUtils会处理重复及末尾斜杠](https://raw.githubusercontent.com/corretto/corretto-21/21.0.10.7.1/src/java.base/unix/classes/sun/nio/fs/UnixUriUtils.java)；后续walker仍须保留实际目录要求，alias仍须比对完整原readlink bytes，每次展开后NIO/JNI交界再执行既定字节检查。当前组件检查没有目标路径metadata、枚举、open或JNI I/O。

launcher顺序为原preload/native/trust→原program/config→原descriptor来源/D2绑定→原实际父JVM检查→provider/字节门→dropper等后续D2观察。原实际JVM比较仅复用移动到program.requireCurrentLauncher，不变更旧判断；门自身也消费原program复核，不能由caller声明完成。原hashFile仍调用ELF1，未提前接ELF2或完整loader闭包/选择/Unix资格，也不解除childCommand的v2拒绝、发行START或签发handoff。

验证在Corretto21.0.10+7-LTS/macOS aarch64完成：首轮四类65项全过；新wrapped filesystem子JVM首17项有1个夹具失败，发生于TemporaryFolder.toFile、尚未到门，原source/class/XML保留。修正为父测试建目录、子JVM只读原材料后，四类66项全过；真实SPI包装初始化后清空属性，D2 raw decode仍以未知filesystem拒绝，候选getPath计数0；显式控制调用计数1，检测点有效。该反例只证明拒绝顺序，不伪造Linux阳性。首轮style的泛型名称A违反既有规则，改为T后样式0；最终串行十七类171项全过（0失败/错误/跳过），包括原program/inputs/trust、ELF/loader数据及launcher/process回归。四阶段各1429源码/资源逐项核验，最终八项code与测试/style一致，原PASS不移绑。body文本包含特殊字符、Unicode、原文被旧Path折叠后的留存、one-use/超时/失败/关闭、伪造D2 input拒绝都有相应覆盖。

终态RAT退出1/431，全部路径及字节与父9f9b08dd67相同，本次代码无新增unknown；用户PDF在扫描前排除且未读取。四个已记录raw scope经两次独立lsof/原inode/mode/SHA及archive hardlink核验后，清理8目录、39文件路径、2,559,081逻辑字节；archive保留，regular-file数据块回收0。该清理只覆盖可精确归因的raw路径；首个失败子夹具的匿名临时目录路径未记录，不按全局prefix删除未知目录。

证据位于工作区外`/Users/liusinan/.codex/artifacts/unified-wal/classic-linux-provider-evidence-9f9b08dd`，匹配JDK原源及SHA、原失败和分阶段回执均保存。本块只交付原owner来源/字节前置门；实际Linux成功路径仍NOT_EXECUTED，ELF2 owned hash、D2 walker/闭包/diagnostics/完整qualification、parent/child/Main/G1/full-bytes/16步/生产Gate继续OPEN，默认OFF/SYSTEM/PARK和advanced disabled。未运行本块models/性能，持续目标active。

## Linux ELF2 的原字符串元数据观察

2026-11-01在本地父`662b4ff439e4eb246b37d8b390744a032d2d7434`上，按规划会话最新裁决，在原`SegmentClassicLinuxElf`中增加`readVersionTwo`和窄`Image2`结果。复用原ELF64 little-endian x86-64结构、区间/映射及positional reader，不复制parser、不新增模块或通用框架。调用方持有原真实FileChannel，parser不open/close、不移动channel position、不执行或装载ELF，也不读取依赖目标。原`read`经旧约束适配，继续拒绝旧flags范围外值、非法SONAME/NEEDED、重复NEEDED及空/relative/token/noncanonical搜索路径。两个公共读取入口的所有成功分支（含无dynamic/无字符串）均在最终返回前检查原deadline。

ELF2保留原NEEDED顺序、位置与重复，PT_INTERP/SONAME原文（null为缺失、空SONAME仍为present），RPATH/RUNPATH存在性和完整原字符串，以及FLAGS/FLAGS1存在性和原数值。动态路径中的empty、dot、relative、token和分隔符只作为原文，不展开、规范化、转Path/URI或抹除被RUNPATH遮蔽的RPATH，也不从token推导/补设ORIGIN flags。原明确singleton重复、audit/filter/textrel及未支持扩展仍拒绝。固定ELF2允许mask为DF `0x1b`、DF_1 `0x08000889`，在ELF1集合上仅增加已冻结的DF_ORIGIN、DF_1_ORIGIN和DF_1_NODEFLIB；位定义依据[glibc 2.39 elf.h](https://raw.githubusercontent.com/bminor/glibc/glibc-2.39/elf/elf.h)，相关原源与SHA保留在工作区外，不是批准vendor运行证据。

128 program headers、4096 dynamic entries、每image 1024个NEEDED、每string 4096 bytes、累计retained text 1MiB共同生效。每次重复字符串引用完整计费，不按唯一字符串去重；预算还包含interpreter等其他保留文本。每image限制与D2跨context的8192请求/262144候选membership预算分开，parser成功不证明搜索闭包或实际winner。

独立Python struct生成1536-byte固定向量SHA为`894bbf508d445c4323df38818df24e56b091cf5feadd832c8b9fad192e2bebc3`，包含重复NEEDED、Unicode/路径形式名称、dot interpreter、原token/empty段、present-empty RUNPATH及新flags。定向13项全过；随后串行十三类149全过（0失败/错误/跳过），包括原ELF1/owned文件/trust/codec与相关数据回归。外部诊断的首次Probe默认stdout把Unicode替换为问号，独立NEEDED对照正确失败，parser进程exit0不冒充对照通过；原149阶段源码/class、原命令/输出/错误回执分开保留。仅将测试Probe2的输出改为显式UTF8 bytes后，定向13项及最终独立样式退出0；生产源码和resource与149阶段完全一致，不把原149项移绑到新测试源码。

最终新Probe2在同Corretto21.0.10+7-LTS上读取固定向量及原已封存的Netty4.2.15.Final Linux .so（SHA `422aab63e1e07cbac931d4b69d9df599b2a619bd72e9894eab6496689bb2e701`），实际class-load来源和class SHA绑定新13项阶段；NEEDED原序/重复、SONAME、RPATH/RUNPATH存在性/原值及FLAGS/FLAGS1存在性/数值均与本机objdump的原输出匹配。两个文件均只在macOS读取，未装载/执行；显式诊断输出编码不证明整个JVM、provider或JNU接口的UTF8资格。

三阶段各1428源码/资源输入按原阶段核验无缺失或错配，最终三项code与新13项及最终style一致。RAT首跑误继承149阶段的旧诊断测试输入表，原回执保留且不作终态；修正为新13项的实际源码表后重跑，退出1/431，路径及字节与父662b4ff439相同、本次代码/资源无新增unknown，受保护PDF在每次扫描前排除且未读取。三raw scope经两次独立lsof、原inode/mode/SHA及归档hardlink核验后清理6目录/24文件路径/1,666,824逻辑字节；archive保留，regular-file数据块回收0。

原`SegmentClassicLaunchTrust.hashFile`保持ELF1，本块未新增接收D2转换后Path的入口。接续顺序仍为原String decode与D身份绑定→原可信bootstrap/JDK下核验providerRule/实际来源→显式UTF8 file:///与原字符串字节门→逐组件路径/alias/父链/ACL→原owned同handle hash＋对应parser→完整闭包/选择与qualification；字节门须先于该路径metadata/枚举/open/JNI，不能经待验证D2路径自证provider。相关源码/输入/class/log/XML、失败及静态oracle位于`/Users/liusinan/.codex/artifacts/unified-wal/classic-linux-elf2-evidence-662b4ff4`。当前仅ELF2 parser与真实文件元数据观察；D2 qualified I/O、solver/diagnostics、实际Linux接受、parent/child/Main/G1/full-bytes/16步与生产Gate继续OPEN，默认OFF/SYSTEM/PARK及advanced disabled，持续目标active。本块未运行models/性能。

## LinuxDeployment schema2 的分型批准数据

2026-11-01在本地父`31e8cb536737ccf68ce4335347a235789b43ef4c`上，按总体规划会话的两次schema裁决及UNIT_LOOKUP补充裁决，实现同包窄wrapper与`SegmentClassicLinuxDeploymentV2`。本块只交付canonical approval data、codec及原UWCLX002承载，不执行provider/ELF2/systemd2取证，也不发行Installed、START、Unix资格或DRAINED。原UWCLD001/schema1构造校验、10个role tag、字节与v1独立域保持；新增`JDK_RUNTIME`只允许D2。旧getter对D2明确抛错，不能用空表或缺省值进入旧消费者。

D2使用`UWCLD002/schema2`、零flags、u32精确payload长度和`BookKeeper/ControlledClassicLinuxDeployment/v2\0`独立域。整个部署帧最多1MiB；原UWCLX002、P2公式及外层1MiB上限不变，完整D2仍进入D/P2，v1父输入32KiB限制不变。D2 decoder保留严格UTF8原String，只作长度、NUL、物理路径语法及引用检查，不调用Path/provider、URI或目标I/O。物理File路径要求canonical非根绝对语法；logical加载名保留alias、dot及原上下文，声明替换只作语法对应，不冒充实际lookup或原字节资格。

providerRule1绑定批准Linux默认provider/UnixPath/UnixUri实现合同、vendor/runtime build及承载java.base的image；image必须是原child `.program`的JDK角色`javaHome/lib/modules`同路径、同长度、同SHA，不能另选JDK。实际已加载实现、原source image与bootstrap TCB仍须现场核验，不能靠类名、自报属性或后置hash补认启动。相同路径child文件仍须字节一致，系统alias不放宽原child文件合同；startup chain仍以原parent Main的完整JVM argv结束。

loader policy必填ELF2及cache1.1、非溢出比较器、raw path、conf、glob、相关性、hwcaps、token、diagnostics各固定rule1，未知ID拒绝；rule1的conf/glob预算保持原1MiB单文件、4096 token、每遍65536文件/65536pattern/16MiB逻辑字节/32活跃context及原目录成员预算。批准build的default目录保留顺序且逐项有presence rule，LIB绑定原build token；CPU/OS/PLATFORM、实际winner、已加载复用与运行时来源不写入D。配置必填kind/format，分别为ldconfig、cache、明确preload-absent、模块运行配置；只有ldconfig-kind可交原conf/include图，LOADER_CONFIG角色或材料hash不代替模块来源语义。

每个物理ELF保留NEEDED原序与重复、RPATH/RUNPATH absent和present-empty、原字符串及flags。Context(file,logicalLoadName)表最多4096项，每个context按原NEEDED ordinal绑定非空、排序唯一的候选context集合；模块请求也按requester context/name绑定并保留原请求顺序和重复。两类请求合计8192，每集合4096，两类候选membership合计262144，每次引用均计费。context二元组只是加载位置；后续solver必须沿incoming edges保留具体祖先RPATH链及base namespace first-loaded reuse，递归验证未选候选，不能以“候选属于集合”替代实际选择。

Unit2只批准唯一launcher SERVICE及相关TARGET；target wire不含伪造service字段。commonRule1要求loaded、非transient、NeedDaemonReload=no、SourcePath明确空、原窗口稳定且无job；target稳定状态限inactive/dead或active/active，D保存规则而非某次实时状态或job ID。service constraint1继续UID0、显式u32 GID、Type=exec、DynamicUser=no、Restart=no及原有序执行阶段。fragment/drop-in绑定物理File，drop-in顺序保留；首版consumer要求实际FragmentPath、DropInPaths与批准File路径逐项、按有效顺序精确匹配，再完成原路径和文件核验。不能将不同logical路径resolve到同一物理文件后自动算匹配，不能去重或转成集合；实际重复应用或未承载的logical路径均明确Unsupported。

每unit完整Names包含Id，最多32，按规范字节序唯一排序、同类型、全局唯一。26个正反关系family均显式存在，empty与missing分开；保留完整原unit name列表及表外名字，不截取批准表交集。每行1024、全表65536次名称引用，跨行/正反重复也收费，整体1MiB同时生效；入表双方核对反向关系。表外名字仍是未分类事实，actual observer必须按方向核对相关incoming activation、重启与旧UID入口，不能自动标无关，也不强迫纯ordering或无关host sibling展开成全机闭包；连续Upholds、socket/timer/path及其他service驱动入口不因codec可表示而获准。

安装链接共用1024预算，分UNIT_LOOKUP/WANTS/REQUIRES/UPHOLDS，保留原path、raw target及unit归属。UNIT_LOOKUP的leaf属于Names，可等于Id；同名入口不制造manager alias或依赖边。systemd alias按获批unit lookup和优先级解释，外部linked fragment才按实际文件链核验，依赖链接也不能统一用普通realpath代替有效目标语义。数据中的依赖链接必须与对应manager关系相符；反向不要求每条manager关系都有物理链接。实际link身份/保护父链、遮蔽、有效fragment与drop-ins仍由后续owner核验，无法判定即Unsupported。

独立Python struct生成的2804-byte合成向量SHA为`e8b0f9b4d8fc0c8821e18f9b0a58e9aae6cf68a222fcc8ac8227f623488d5a64`，D2身份`b1c5c07cdef05919f47a85ff519327128e46921e92ae38c6fa9e95971fc2383f`。首轮三类24项有1个测试夹具误用非Linux recipe的error，原源码/class/XML保留；修正后三类25全过，最终串行十二类136全过（0失败/错误/跳过），独立spotless/checkstyle退出0。覆盖D1旧向量、D2规范往返/截断/未知格式、实际共享请求和候选预算的分配前拒绝、65536关系引用边界及超限、外层D/P2与无资格启动拒绝。三阶段各1427源码/资源输入及最终五项code/test/style按原阶段核验一致。RAT退出1/431，全部路径及字节与父31e8cb5367一致，本次代码/资源无新增unknown，受保护PDF在扫描前排除且未读取。三raw scope经两次独立lsof/原inode与mode/SHA/归档硬链接核验后清理6目录、27文件路径、1,802,952逻辑字节；归档保留，regular-file数据块回收0。没有实际Linux/provider/glibc2.39/systemd接受阳性；models/性能未执行，各整体Gate保持OPEN，默认OFF/SYSTEM/PARK及advanced disabled。

本块证据位于工作区外`/Users/liusinan/.codex/artifacts/unified-wal/classic-linux-deployment-v2-evidence-31e8cb53`，各阶段只绑定本阶段原输入/源码/class/log/XML，不重绑旧PASS。完整provider/ELF2/systemd2观察、候选闭合及选择/diagnostics、qualified parent/child/Main/G1/full-bytes/16步与生产验收仍未完成，持续目标active。

## Linux v2 的批准输入与父身份

2026-11-01在本地父`1108ab13b8a239b5494519e6f2559231409a4dd8`上，按规划会话最新裁决增加包内SegmentClassicLinuxDeployment数据和UWCLX002/schema2父输入。v1的UWCLX001/schema1字节、父程序v1公式、32KiB解析上限及ABI1命令保持；v2总输入上限1MiB，只允许Linux x86_64固定profile，公共dropper/path/长度/SHA/ABI2必须和部署表引用相同。D以`BookKeeper/ControlledClassicLinuxDeployment/v1\0`覆盖完整UWCLD001/schema1规范帧，P2以`BookKeeper/ControlledClassicLauncherProgram/v2\0`覆盖child program摘要、u32 UTF8长度及固定父Main、D；部署帧不含自身/P2/.launcher摘要，避免循环。P2仅是批准输入身份，不是实际程序、启动或Unix证明。

批准数据绑定平台基线及解析格式、唯一有序文件/角色/长度/SHA、PT_INTERP入口和固定系统readlink字节、搜索目录存在或缺失、config/cache明确存在或缺失、全部声明ELF的DT_NEEDED/RPATH/RUNPATH与模块引用、批准exec启动链、unit fragment/drop-in/执行和激活关系、预期u32 GID及固定root/Type=exec/DynamicUser=no/Restart=no约束版本、dropper/filter/凭据清理/cgroup与root操作入口规则。文件1024、别名128、目录64、unit32/各drop-in32、每串4096 UTF8及总1MiB有独立上限；引用/排序/角色/重复、相互矛盾的存在规则、循环/无关别名、未知schema/profile/ABI/规则和溢出均拒绝，不截断。新v2公式绑定全部规则，改变filter或GID即使文件表不变也改变D/P2；父工具文件不进入child .program，重叠文件长度/SHA须一致，系统别名不能覆盖一般child文件或私有supervisor/material路径。

systemctl的闭合依赖必须由外部受控部署预装批准，涵盖loader、传递库、动态模块、配置/cache/搜索及硬件能力候选，实际文件读取/ELF规则比较仍待实现；不会通过运行未批准程序或ldd发现并批准。preload明确必须不存在，未知不可读不当不存在；cache/config缺失要求显式表达。系统别名仅对已声明加载入口逐段替换并登记使用，支持相对多跳、cache和SONAME入口，最终批准文件仍需普通无别名合同。实际readlink/父链/owner/mode/ACL/inode/目录选择/首次关闭及运行中不变约束不能由这个纯数据解析器代替。kernel/vDSO、PID1、本地manager真实性、部署管理面和父JVM之前的可信启动链属于已冻结外部TCB；后置hash不补认不可信启动，“系统库”不免除用户态依赖清单。批准数据不携boot/InvocationID/PID/出生/实际cgroup/凭据/drain/registration事实。

独立Python向量中，Linux child program614字节、recipe197字节、基本部署1205字节、父v2输入1634字节；基本D=`490df21d62cdfc28b967084f48b72d7478edcd1fb2968fdead7518926eb36edb`，P2=`68439ec6b45fb6ef7ca34e314bb5e5f996346fe581082784b6e85b7a6a527fe1`。额外2501字节向量含非空依赖、模块、RPATH顺序、drop-in、两段启动链、相关unit、unsigned GID、相对多跳及cache/SONAME别名；其D=`be613399a1e3ced73319d736bc7e4eb19612c0ec87aa51315a4c494905e463eb`。全部路径/文件SHA和版本为合成测试数据，没有执行这些文件或发行Installed/START/资格。原始生成器、早期隐含GID版本及各阶段输入不覆盖。

首组11项及扩大29项通过，新增私有descriptor容量文件测试后的trust16项通过；随后扩展规则与多跳向量实际复现一项error（合法后继别名被单段关联判断误拒绝），修复沿声明替换链收集实际使用者，循环/无关链接及缺失目录与现存链接冲突仍拒绝。最终model/inputs/trust/launcher四类31项全部实际通过，0失败/错误/跳过；输入1412项。材料owner沿原0700/0600/ACL/首次关闭和one-use，仅扩大原字节读取上限；大于32KiB的合法v2、恰好1MiB原bytes及1MiB+1拒绝均以真实私有文件验证，fixture依旧不能取得Installed。父Main只新增child程序/表冲突检查；完整Linux资格/cgroup与listener尚未接通时，v2的childCommand直接拒绝，绝不调用旧ABI、启动child或提升handoff。

样式首次十处import/长行失败，修正后独立检查通过；尝试合并test/style时cpu-affinity的原NAR生成源码产生十条样式错误，在server前退出、零相关测试，保留原生成源及失败，未修改生成代码、构建设置或跳过检查。最终仍按原分开的相关test和独立Spotless/Checkstyle命令验证：31项及最终独立样式退出0，1412项实际源码与12项代码/资源样式快照逐项一致；六个Maven原阶段全部输入分别与原快照或父Git blob核验，无缺失/错配。终态独立RAT退出1/431，全部路径及字节与父1108ab13一致、本次无新增；扫描前按精确名称排除用户PDF。证据`/Users/liusinan/.codex/artifacts/unified-wal/classic-linux-deployment-evidence-1108ab13`包含各原source/class/XML及失败，失败组合收集到的旧target库存不算新编译证明。 六个raw scope在两次独立lsof及原inode/哈希/归档硬链接核验后，只清理12目录、30文件路径、2,395,769逻辑字节；原归档保留、regular-file数据块回收0，共享target与用户文件未清理。TLA/性能/真实Linux本增量未执行；完整extra-file/ELF/依赖核验、v2启动producer/root UDS/Ctl、固定隔离dropper、Unix交接/签发及Main/G1/full-bytes/全16步和生产Gate仍OPEN，完整目标active，默认OFF/SYSTEM/PARK及disabled保持。

## 固定 systemd 查询的原 Process 与有界管道

2026-11-01在本地父`90a0f1a0cf`上实现包内SegmentClassicSystemdQuery。固定argv只调用`/usr/bin/systemctl --system --no-pager --no-ask-password --all --property=<fixed fields> show <one literal service>`，拒绝外部路径/字段/host/user manager/通配符/控制字节或多个unit；查询命令使用空环境、固定`/` cwd和独立stdin/stdout/stderr。命名属性集含规划要求的四组及各Exec入口对应的Ex属性，字段列表固定且保留空值，UTF8/总64KiB/完整行/恰好完整集合/重复或未知字段/Id不一致均拒绝。Exec值只保留原字符串，不声称已完成批准版本的argv语法解析、unit审批或连续隔离。参考官方[show接口](https://www.freedesktop.org/software/systemd/man/latest/systemctl.html#show%20%5BPATTERN%E2%80%A6%7CJOB%E2%80%A6%5D)与[Service属性](https://www.freedesktop.org/software/systemd/man/latest/org.freedesktop.systemd1.html)。

每实例只创建一个原query Process。先登记heap owner并启动两条固定non-daemon consumer，再构造Process和附加其原pipes；调用不持生命周期gate跨Process构造、read、close、等待。原stdin首次close明确成功，stdout/stderr分别最多留64KiB/16KiB、各4KiB读取工作区，超限永久拒绝观察但继续读至实际EOF；首次pipe close失败或未知构造/附加均保留未决，不重试close、kill、打断reader或堆积新查询。取得结果须先原Process实际退出、两reader实际join、三个原pipe首次关闭明确成功，非零退出/诊断stderr/解析失败都不生产可用属性。caller deadline/中断只使原观察永久失败，后续实际退出只能完成资源终结，不能把迟到结果刷绿。原launcher仍须在initialize前完成CLI/全部实际依赖批准，持有最多一个未排空查询，并将失败保留在同episode；这些v2接线尚未完成，本组件本身不能发行Unix或签发资格。

data-only首轮三项通过；增加原Process/pipe后七项通过，随后明确核对三个原pipe的首次关闭完成位，相关七项再次实际通过（0失败/错误/跳过）。真实子JVM验证stdin EOF、两路pipe字节、两路各4MiB输出超限后继续排空、exit7拒绝，以及80ms观察超时后进程仍未排空、释放私有gate后实际结束且迟到结果仍拒绝。子JVM仅是query-pipe fixture，没有运行真实systemctl或检查systemd unit；macOS的生产Owner在创建Process前拒绝。首次样式仅一行121字符注释失败，最终删去该注释一词后Spotless/Checkstyle退出0；代码/声明/行数不变，不为此重复运行测试或移绑原七项。

证据保留于`/Users/liusinan/.codex/artifacts/unified-wal/classic-systemd-evidence-90a0f1a0`。原三阶段各1405项输入/source/测试class/XML保持原绑定；前两阶段class选择器遗漏producer class，未重构或追认缺失字节，最终阶段已从该次Maven时间窗内的实际target补采七个本组件class并保存哈希。最终样式源码与受测源码的一词注释差异另记原SHA，不覆盖原测试输入。 三个raw scope经两次独立lsof及原inode/哈希/归档硬链接核验，只移除六目录、12文件路径、1,083,882逻辑字节；原归档保留、regular-file数据块回收0，共享target及用户文件不清理。RAT/TLA/性能及真实Linux manager查询未执行；v2装载/额外文件批准、root操作入口/Ctl、固定dropper/seccomp/cgroup/权限交接/资格与所有整体Gate仍OPEN，完整16步目标active，默认OFF/SYSTEM/PARK及disabled保持。

## 固定 UDS 操作报文与有界字节交换实施中

2026-11-01在本地父`88802b2be2`上新增包内SegmentClassicLauncherOperationCodec及OperationIo，字节依赖已本地提交`90a0f1a0cf`、未push；作为既定v2/专用Ctl的必要接线，尚未接入launcher或发行实际受理。独立UWCLO001报文头16字节（magic/schema1/kind/flags0/正文长度），GREETING与STOP各带非零episode，HANDOFF另带非零bindingId，REPLY仅ACCEPTED/REJECTED；最大48字节，不携私钥、可签facts或drain结果。固定头全部验证后才分配固定正文，截断/尾随/未知码/非法identity拒绝；旧HELLO/START不能互解为操作。所有episode值仍为数据，后续原owner必须关联真实unit/Process并拒绝旧episode，不以codec解析成功授予操作。

OperationIo只在原owner已登记的一个非阻塞Unix channel与专用selector上工作；一个原absolute deadline贯穿connect或整个帧，固定头通过前不按声明长度分配。不会自动替换/重连channel，不建立线程、认证root、创建endpoint或提交业务操作；EOF/超时/中断后资源仍归原owner实际关闭，中断flag保持。selector短等待仅推进这条原连接，不能复用为通用RPC或业务事件循环。后续listener/Ctl还须核验固定路径父链/实际对象、kernel peer UID/PID和原Main出生，单命令/连接、预算、原owner关闭图及STOP/HANDOFF竞争均未接通。

独立Python五份固定向量与四项codec测试首轮通过。首轮加真实I/O的八项中四项在bind前因私有fixture追加UUID导致macOS Unix路径超长而error，原输入/日志/XML保留；只将同一私有TemporaryFolder内的endpoint改为短固定叶子，原断言不放宽。修正后的两类八项全部通过，0失败/错误/跳过；实际私有Unix连接覆盖greeting→一命令→受理字节、半头原deadline、半body EOF、超长声明未等body即拒绝，以及过期/中断不发命令且原socket继续可用。ACCEPTED字节来自fixture，不是生产launcher受理、业务drain或Linux资格阳性。原codec四项及最终两类源码的Spotless/Checkstyle均退出0；后续接线仍在进行，RAT/TLA/性能未运行。证据保留于`/Users/liusinan/.codex/artifacts/unified-wal/classic-operation-evidence-88802b2b`，本包仍WIP；v2描述文件装载、root UDS/Ctl、systemctl取证与固定隔离dropper/完整Unix资格及所有整体Gate继续OPEN，完整目标active，默认OFF/SYSTEM/PARK和disabled不变。

规划会话同时冻结systemd取证使用固定`/usr/bin/systemctl show`（精确system manager/unit/property argv、空环境、原owner持一个实际查询及两路有界pipe，失败仍实际排空），CLI及依赖另纳v2批准文件，不新增native D-Bus或追认v1批准。实际属性/两次关联顺序由execution-objective拥有；磁盘cat/list-dependencies或两次相等快照不能建立唯一启动入口与连续隔离，期间unit/reload/reexec/activation图无法受控仍拒绝。本处只同步裁决，尚无CLI实现或实际Linux阳性。

## Linux 操作通道的只读 peer 凭据依赖

2026-11-01在本地父`081e437f946435d748fc7a0e195ea90b8751d50c`上，规划会话冻结完整Unix交接首版仅支持受控Linux x86_64/systemd/统一cgroup v2/宿主namespace/专用静态旧UID/独占本地ext4；具体资格与取证顺序由execution-objective拥有，macOS完整资格为UNSUPPORTED。后续父描述文件使用UWCLX002/schema2，固定root UDS在同episode、资格建立后且child创建前绑定；专用Ctl只投递STOP或HANDOFF，ACCEPTED只表示受理。v1字节及普通关闭不升级资格；当前尚未实现v2装载、UDS/Ctl、固定隔离dropper或完整Linux资格生产者，不以新增native方法替代这些工作。

本增量复用原native-io增加只读unixPeerCredentials(SocketChannel)，仅返回Linux连接建立时、调用方namespace内的PID/无符号UID/GID。JNI只接受bootstrap/java.base的精确SocketChannelImpl及固定字段签名；持匹配JDK的原stateLock覆盖fd提取、SO_DOMAIN/TYPE/PEERCRED及状态复核，检查MonitorEnter/Exit实际结果与所有内核返回长度，不导出/缓存/dup/关闭fd或改socket。原owner仍须串行化查询/关闭；原程序/JDK批准、路径保护、实际MainPID/出生/unit及episode由后续owner核验，数值观察不发行身份、操作或持续隔离资格。旧Java默认、新Java配旧JNI、非Linux与不支持输入均拒绝，无fallback。

精确本机Amazon JDK21.0.10的SocketChannelImpl、FileDescriptor及ExtendedSocketOptions源码保留在进行中证据`/Users/liusinan/.codex/artifacts/unified-wal/classic-linux-isolation-evidence-081e437f`，核对实际stateLock覆盖close/pre-close及公共SO_PEERCRED无PID。macOS arm64的真实host构建与Linux x86_64交叉cargo check退出0；后者只证明编译，不是Linux运行、身份、关闭竞争或隔离阳性。首轮最小Java验证11项有1 error、1 Linux skip：fixture用socket路径出现代替bind完成，实际connect被拒绝；改为子进程bind返回后原pipe显式报告就绪，修正后11项为10通过、0失败/错误、1 Linux专属close/fd复用竞争skip。实际双JVM的Unix通道在本机明确拒绝peer能力后仍能双向传字节，TCP/closed/unconnected/null拒绝；旧精确真实JNI的独立JVM原currentProcessCredentials仍可用，新peer方法因缺符号拒绝。上述测试不能替代Linux双进程凭据和关闭竞争阳性。

Java Spotless/Checkstyle退出0；整Rust文件rustfmt检查退出1，原HEAD独立检查也有既有漂移，因此只格式化新增函数，新增函数范围检查退出0，不顺带重排旧代码。最终host库与最终Linux交叉cargo check均退出0，最终11项回归仍为10通过/1 Linux skip，最终Java样式退出0；三次Java阶段各1399项输入逐项与原快照或父Git blob核对，无缺失/错配，最终源与实际native构建及最终样式一致。原失败/原编译绑定分别保留；RAT/TLA/性能本增量未运行，没有新的性能结论。完整父子qualified成功、Linux实际约束、retirement/权限交接/终态签发、Main/G1/full-bytes及完整16步与生产Gate保持OPEN，目标active，默认OFF/SYSTEM/PARK和disabled保持。 三个任务raw scope经两次独立lsof与原inode/哈希/归档硬链接核验后，仅移除六目录、21文件路径、859,407逻辑字节，原归档保留、regular-file数据块回收0；独立Cargo target按相同终态/源码/精确库检查清理，物理卷空间回收不作测量结论，共享Maven target与用户文件不清理。

## 受控历史 Classic child：历史资源图实施中

2026-11-01父launcher包继续在本地`192e8c674129dc240f4b0ddf83504d10ba11f071`上实施；此前凭据及child生产消费者分别已本地提交`9652b1a0393f8855419bf01d38da486d4cdf9704`、`192e8c674129dc240f4b0ddf83504d10ba11f071`，未push。原entry部分证据已封存2708文件、42,878,138逻辑字节，manifest SHA256 `d54f337ef5322e9702c43247d95476d203fde196a9a45fa2981783c58d8ab5c5`，新consumer checkpoint `dd873bc2b2c957590b370e1c217465901c566605fbddd2f60f2ee3754278dfc8`；原native-only checkpoint未覆盖，旧阶段不移绑后续源码。

下一包按规划会话固定UID0父Main/owner、`launcher/`私有材料、批准原生exec-dropper及原Process监督；具体输入和权限边界由execution-objective拥有。首版父程序复用已批准child相同文件/JVM参数，仅固定Main不同，父批准摘要用独立域覆盖原child program摘要及父Main。新增UWCLX001/schema1/flags0有界描述文件（≤32768 bytes），固定recipeId/recipe摘要/KeyId/父程序批准摘要、平台、dropper绝对路径/长度/SHA、调用格式1、supervisor目录/target UID及manifest/proof/policy/AES固定引用；不会改写原recipe/program/HELLO/START字节。调用格式1为`<dropper> --uid <numeric> --gid <numeric> --groups <canonical comma-separated exact groups> -- <original JVM argv>`，空组为独立空字符串参数，不经shell或账号查询。私钥固定读取原recipe keyName的`launcher/<keyName>.pk8`，不由报文选择；原child只读public材料路径保持。

父输入依赖复用原trust的文件owner读父专用0700目录/0600单链接文件，保留首次关闭结果和one-use；返回字节仍不是生产Installed issuer资格。新增独立raw ZK observer入口按目标session/czxid/mode核验真实registration、其他模式缺失、原cluster/Cookie及native ABSENT，不采用该node或重试注册。child原readForServing仍要求其自身原session；普通冷读仍要求两类registration缺失。原进程关闭后的独立数据读另行允许现存commissioning，但仍要求两类registration缺失及原cluster/Cookie；它不核发新启动或handoff许可，普通启动冷读的native ABSENT要求保持。

生产`SegmentClassicLauncher`与`ControlledClassicLauncherMain`代码已接固定preload/JNI/trust/program/config、实际root身份/父程序、私有描述文件与原公私钥匹配、target-owned原supervisor及retirement缺失、唯一raw ZK独立scope、批准dropper与原ProcessBuilder/空环境/独立stderr、实际最终JVM/PID/startInstant及challenge的一次START消费者。START仅原worker私有签发，原codec只新增未签名规范字节装配；与原独立START向量逐字节一致，未签名字节不能验签为授权。签发尝试与实际flush分别记录，写入失败后停止发送并关闭原stdin；正常close先STOP flush/关闭stdin，再等原Process/reader真实退出、观察原scope与注册消失，最后关闭metadata/输入文件及guard。来源读取不持生命周期gate，避免metadata回调与观察互等。部分Process/native acquisition或首次关闭未知保留原owner/CLOSING，不重试、kill或伪造正常终态。

首组25项（3类）、随后父owner6类59项、补充header15项，以及重用未签名codec和收口关闭后的7类67项各自实际全部通过，0失败/错误/跳过，分别绑定1393/1396项输入；证据位于`/Users/liusinan/.codex/artifacts/unified-wal/classic-launcher-evidence-192e8c67`，当前仍进行中。67项包含Inputs 3、Launcher 3、Trust 15、ZK源8、Child 7、原child资源24及启动codec 7项。父Main真实Process只覆盖未安装随机recipe的失败退出，没有固定安装/有效START或生产child成功链；原child资源用例继续是组件fixture。dropper验证只支持精确平台薄64位ELF/Mach可执行header、模式与流式hash，拒绝脚本/class前缀/错误架构/库类型；合成header从未执行，不证明dropper降权或Linux实际身份语义。`requestHandoff`在完整commissioned/Unix资格生产者接通前明确拒绝，不改变owner状态或生成retirement；没有UWCHOF01签发/可采用proof发布路径。67项阶段Spotless/Checkstyle实际退出0。随后把原Process/管道所有权集中到包内`SegmentClassicLauncherProcess`，由同一父worker消费、不新增worker或公共authority面；先登记heap pipe holder再创建Process，记录原birth，只有原调用方发送/实际等待。reader在坏帧/EOF/失败或部分START写入时先禁止发送，再原子唤醒关闭方；合法早期result=1被保留为失败组件结果，不当作协议伪造；STOP/关闭stdin在waitFor之前，线程和实际Process结束后才投影组件drained。新真实Process测试首组14项及fixture cleanup修正后的14项各自通过，输入1398项；新增四项用例包含六种原JVM probe episode，验证正常STOP、早期失败/无请求的正常报告、EOF/坏帧后绝不补STOP，以及result=0后输出/reader/进程仍挂起不提前结束。测试只发送112字节全零占位body，没有有效UWCLA001、签名、START许可、Installed或生产child；fixture强制退出仅限失败清理，不计为drain。stderr继承至原Maven日志，pending Future只观察超时、不取消或中断实际owner。此前67项不移绑此重构；最终8类71项（含新增Process 4项）及重构后Spotless/Checkstyle全部实际通过，0失败/错误/跳过，1398项输入和16个代码路径逐项匹配实际通过源码与最终样式快照。该增量按局部实现里程碑保存，完整包不因这些局部证据验收完成。七个Maven原阶段的全部声明输入分别与快照或原HEAD blob核验，无缺失/哈希错配；两次独立lsof及身份/字节/归档硬链接核验后，只清理对应raw scope的14目录、52文件路径、5,646,538逻辑字节，原source/class/日志/XML和归档硬链接保留，regular-file数据块回收0，共享target及用户文件未清理。RAT/TLA/性能本增量未运行，完整父包、qualified父子成功、Unix/终态issuer/bootstrap与整体Gate继续OPEN。

2026-11-01当前entry增量（在父`f1005222da0fea6ebe5683d14b9d67d060faf98d`上完成以下局部验证）已有生产`ControlledClassicMain`与`SegmentClassicChild`消费者。create仅分配heap；固定Main先将普通stdout转stderr，原协议直接使用FileDescriptor输出并观察真实I/O错误。原owner按纯JDK preload、受控JNI、固定trust/program/config、实际Process/空环境/凭据、唯一原raw ZK与native ABSENT/Cookie/cluster完整scope核验，再生成真实challenge并验证一次性签名START；只有原worker可发行私有构造的StartContext。它绑定原桥、唯一Bookie、冻结配置及同一metadata，并在任何Classic存储副作用前消费；fixture、解析值及Installed不能生成上下文。历史replay/严格flush、实际bind和本session registration之后，重新核验原scope、实际session/czxid/mode及凭据，才开放准入并发送READY。

Main的被动await消费者已实现：只等待原episode与实际线程退出，超时继续同次观察，内部callback/reader/worker自等待拒绝。STOP允许终止已获START但尚未READY的初始化；控制EOF/坏帧保留原失败并退休准入。唯一worker沿原桥实际关闭业务/网络/metadata，保留构造未决和首次关闭失败；只在已验证START且组件确实终结时发送有界结果，再关闭stdout、等原reader实际结束并关闭输入。父必须在结果/EOF/坏帧/期限后先关闭原stdin写端，再waitFor；child不以异步close(System.in)、daemon、取消或System.exit制造完成。原raw ZK关闭按当前3.9.5实现等待原send/event线程；最后再观察组件失败，避免STOP/组件排空后的Expired被抹掉。组件结果仍不是签名DRAINED。

metadata选择已按规划裁决限制：只读现存LAYOUT，原始类名/URI/type在任何可能加载自定义类的getter或反射前检查，仅精确内建Flat、Hierarchical、LegacyHierarchical、LongHierarchical四种factory。LAYOUT v1只接受flat与hierarchical（映射当前Hierarchical），v2接受四种完整类名；manager version均为1。URI zk/zk+null推断现存布局，zk+flat/hierarchical/longhierarchical必须与其相符，不新增zk+legacyhierarchical。显式选择器须全部一致，class-check bypass即使false也拒绝；缺失/未知/变化布局不写入或迁移。factory在initialize前登记，关闭只尝试一次并保留原失败。Hierarchical自身及legacy/long两个子manager的三组scheduler均shutdown、实际等待并join原线程，三组内部线程均禁止自排空。

最终`child-entry-final-related`实际14类92项全部通过（0失败/错误/跳过），声明1390项输入；含ClassicChildResources 24、历史Journal 8、Fresh owner 16、Main/pipe 6、配置 6、Profile owned-resource 3、真实ZK源 7、Hierarchical实际scheduler 2、driver 1、原long-poll/packet 17及普通Classic SDK 2项。真实资源覆盖仍由包内fixture消费，生产Main实际进程仅验证未安装recipe的启动失败：stdout先EOF，父未关闭stdin时原reader使进程保持存活，父关闭写端后无需STOP即实际失败退出。其后只改该测试加入5字节半帧，`child-entry-main-partial-header`6项与最终Spotless/Checkstyle退出0；声明1389项输入，生产源码与92项阶段逐项一致。半帧/碎片与头部字节测试不提供有效START或fixture context，不将92项移绑后改测试。

证据`/Users/liusinan/.codex/artifacts/unified-wal/classic-child-entry-evidence-f1005222`保留各阶段原输入/源码/class/实际日志/XML。首轮Main测试真实失败于输入线程未保持存活；后续独立实际pipe诊断在本机JDK 21.0.10-amzn复现FileInputStream.readNBytes因尝试seek而抛Illegal seek，生产reader改为有界read循环后通过。首轮子进程stderr未独立归档，诊断是另次执行，不冒充该原stderr。metadata阶段首轮为错误枚举引用导致编译失败、零测试；样式首轮七项失败，各自保留，后续修正。编译失败class库存可能陈旧，不证明执行；早期runner的scope描述仍是当时模板文本，实际覆盖按逐阶段命令/快照/XML及本段界定，不改写旧inputs。原native7项和旧库probe亦保持自己的来源绑定。

本entry八个Maven raw scope在两次独立lsof与原身份/字节/归档硬链接核验后，仅删除16目录、52文件路径、6,134,956逻辑字节；归档硬链接、失败源码/class/日志/XML均保留，regular-file数据块回收0，共享target与用户文件未清理。

固定系统安装、专用旧UID与受保护exec降权程序、完整真实签名START→READY→STOP成功链、父launcher签发及Unix资格仍NOT_EXECUTED；本增量没有安装系统材料、补账号、放宽生产trust或跳过START。当前只闭合了生产消费者代码及上述局部行为，完整child包与issuer/bootstrap、Main/G1/full-bytes/16步及生产Gate保持OPEN。本增量RAT/TLA/性能未重跑；下列历史阶段保留原范围，不能把旧PASS提升为完整成功链。

2026-10-31历史processor增量（父`f1005222da0fea6ebe5683d14b9d67d060faf98d`上的未提交WIP）已在原桥实际bind与原raw ZK session create-only注册后开放包内组件SERVING，复用原V2/V3 request processor、OrderedExecutor、timer、协议encoder及Netty图。此组件状态不等于生产READY；包内启动夹具不能发行StartContext。每个已接收请求在分发前登记，初始队列、fence continuation及long-poll deferred callback在提交前登记调用体；拒绝和关闭按原具体packet释放，V2关闭channel早退也完成原read计数和请求终态。普通Classic与原Fresh维持各自入口和权限。

关闭先退休准入/监听、原连接及watch，保留callback/输出执行器；原Bookie完成Journal quiesce和实际回调后，等待已接收请求、连接pipeline与输出实际终结，再停止维护、finalSync/关闭文件，最后关闭processor/timer、join原实际线程、终结Netty图。输出在producer向event loop提交前登记，内部promise不可取消；结果回原loop排队退役，使Netty失败通知后的原write-task清理先完成，编码后的真实引用还由head handler保留至原完成。连接关闭、caller观察取消、handler移除均不单独证明输出已归还；非I/O异常保留未知输出和失败。未知部分executor构造不伪造close，也不阻止已知线程/存储/监听继续关闭，组件仍CLOSING。实际线程join不依赖当前SingleThreadExecutor反向的awaitTermination返回值，未修改该普通公共组件。

真实V2 SDK反例暴露受控FreshCall包装使Journal看不到原BookieRequestHandler：DATA已提交，但批量Add响应未flush。owned分支现于原callback清空context前取得真实handler，继续原ForceWrite批次flush，不增加逐entry flush。原首轮测试被关闭轮询掩盖为60秒超时；改为实际线程join后的同用例明确报Add超时后的NotEnoughBookies，保留两轮失败，修正后同源码测试的V2/V3写入、读取、关闭原session及新metadata episode重启读取均通过。另两项真实SDK测试分别挂起尚未提交native的响应，证明channel关闭/取消尝试后仍须实际释放引用；及观察实际已注册long-poll watch在60秒请求期限前由关闭真实取消。部分构造注入包装实际executor创建，fixture虽关闭丢返回的实例，child仍保留原unknown/cause，已知前缀实际终结且不发布registration；不把fixture清理冒充owner观测。

最终八类62项全部通过（0失败/错误/跳过），包括19项当前资源/受控SDK、16项Fresh、两项普通Classic SDK及原packet/long-poll/历史Journal回归；Spotless/Checkstyle退出0。证据`/Users/liusinan/.codex/artifacts/unified-wal/classic-child-processor-evidence-f1005222`按阶段保存原输入/源码/class/日志/XML，最终1384项输入；首个1372→1380声明数harness拒绝及误选不存在的ReadEntryProcessorV3Test均未运行Maven。部分构造测试首轮编译失败、随后关闭后再取fixture Bookie导致观察失败，均不改绑为通过；样式首轮五项失败后修正。编译失败阶段的class库存可能陈旧，不证明测试执行。该processor增量未重跑RAT/TLA/性能；当时没有生产START/外层/Main/控制通道，完整child成功与系统固定安装/专用旧UID/exec降权资格仍NOT_EXECUTED，完整包、Main/G1/full-bytes/16步及生产Gate保持OPEN。以下历史各增量保存原范围与绑定，不用旧PASS替代最新证据。

processor增量的十个Maven raw scope经两次独立lsof及原身份/哈希/归档硬链接核验，仅删除20个目录、54个文件路径、1,280,665,078逻辑字节；原失败大日志/XML及其他归档保留，regular-file数据块回收0，共享target和用户PDF未触碰。归档时57个代码路径及最终1384项输入与实际通过源码匹配；后续native凭据增量另行绑定，不移绑这62项，目录仍不代表完整child验收。

Main等待接口补充冻结为唯一被动`Status awaitTermination(Duration) throws InterruptedException, TimeoutException`：只等原episode、不启动/请求关闭/取消/创建线程；closeAndAwait先请求关闭再复用，start在READY或启动失败后返回。零期限立即检查，负值/期限换算溢出拒绝；允许多个外部等待者和终结后重复观察。原reader/worker及阻碍排空的内部callback只能requestClose、不得自等待；worker finally通知仍须原线程实际退出检查。超时/中断不修改owner或中断实际I/O，Main继续等待同episode，不据此System.exit或构造服务寿命上限；返回实际终结的失败状态不等于组件成功排空或DRAINED许可。

凭据不采用UnixSystem：其账号默认GID可能不等于进程实际GID。已在原native-io增加无外部PID/凭据参数、无修改能力的窄查询，直接getuid/geteuid/getgid/getegid/getgroups，组原始上限64，返回不可变且按集合规范化的观测。两次UID/GID读不一致拒绝；旧JNI缺符号、读取失败或超限均拒绝，不回退用户字符串/环境/账号库。生产消费者必须在受控库及原材料核验后、HELLO和StartContext消费前，核对real/effective UID均为recipe旧UID、real/effective GID均为primary GID，并精确比较原补充组集合，不补/删primary GID。saved credentials及完整不可恢复降权仍需受保护exec-dropper/父Process资格，观测值本身不发行许可。

当前macOS Rust库实际构建及Spotless/Checkstyle均退出0；两次native定向均运行7项，首轮1项失败于macOS `id -G`列出的账号组超出实际getgroups集合。修正为私有临时目录中独立编译/执行的POSIX小程序，直接比较四项ID与全部原getgroups结果，保留有效id UID/GID断言及不可变/规范化验证，7项全部通过（0失败/错误/跳过）。与原真实JNI库`42a00ac97c6877074fed27e2f0ad276c25cc55e17d93fb3b0d4096e873f0ea35`运行独立JVM，原native open/close仍成功，新凭据查询以UnsatisfiedLinkError为cause的NativeIOException拒绝，未回退。证据`/Users/liusinan/.codex/artifacts/unified-wal/classic-child-entry-evidence-f1005222`仍作为完整entry接线包的进行中目录，分别保存原源码/输入/class/构建与测试日志/XML及旧库probe；最后1385项输入。两个raw scope经两次独立lsof与原身份/哈希/硬链接核验，仅删除4目录、12文件路径、544,946逻辑字节，归档硬链接保留、数据块回收0；另在保留实际库、原源码/lock/命令/终态日志后，核验并删除自有Cargo中间树43目录、170文件路径、42,823,316逻辑字节，物理回收未测量。共享target/registry及用户文件未清理。未安装/替换系统库，Linux凭据实际验证与qualified成功尚未执行；当时未接线的生产Main/StartContext/被动等待消费者现见上文entry进度，前述62项不移绑新native。RAT/TLA/性能未重跑，完整child及整体Gate继续OPEN。

控制传输裁决已补齐（当前Main消费者见上文）：原ProcessBuilder的stdin/stdout仅供固定launcher/child；Main在日志和业务类初始化前保留可观察真实I/O异常的协议输出，将普通System.out/日志转stderr。父禁止合并stderr/stdout，须持续消费或定向受控日志；批准JVM参数不得向协议stdout输出诊断。帧固定16-byte大端头为`magic:u32=0x55574343, version:u16=1, type:u16, flags:u32=0, payloadLength:u32`，先验证头再分配有界body，不重同步或兼容混入的裸HELLO/START。

| type | 方向 | payload |
|---|---|---|
| 1 HELLO | child→parent | 原样完整UWCLH001，最多512 bytes |
| 2 START | parent→child | 原样完整UWCLA001，最多4096 bytes |
| 3 READY | child→parent | episode UUID16 + session8 + registration czxid8 + mode1 + reserved7，共40 bytes |
| 4 STOP | parent→child | episode UUID，共16 bytes |
| 5 COMPONENTS_TERMINAL | child→parent | episode UUID16 + result:u32 + reserved:u32，共24 bytes |

mode为1=WRITABLE/2=READONLY，reserved均零；result为0=组件实际终结且截至报告无episode失败，1=组件实际终结但episode已失败。每种帧最多一次，原episode/方向/状态/长度严格校验；原HELLO/START字节与签名域、Main/argv不变。READY事实由父独立核对，组件结果不发行签名DRAINED，也不提升Profile稳定wire兼容承诺。单一reader收START及同episode STOP，STOP可在初始化/READY前终止启动；原worker唯一发送HELLO/READY/结果，无EXIT往返。

父完整flush STOP后立即关闭child-stdin写端。child本地请求关闭/启动失败先退休准入并drain业务，不等待阻塞reader；只有START已验证且组件实际终结才可报告原episode的结果，尚无START不伪造episode。随后关闭协议stdout，再等reader；父收到组件结果/stdout EOF/坏帧或启动期限到达，先停止发送并关闭写端，再等Process，不能先waitFor。预期EOF只结束已关闭child的接收，不抹掉原失败；运行/等待START时非预期EOF是控制丢失。不能假定异步关闭/中断System.in可可靠唤醒阻塞读；父端不合作/端点泄漏/原reader或worker未退出仍CLOSING/超时，不能用daemon、Future cancel、System.exit或kill宣称排空。即使发出result=0，后续控制关闭失败仍使child失败，父不得签发正常DRAINED。后续用批准JDK及真实Process验证无STOP启动失败/本地关闭/半帧/父退出和held输出/reader；这些传输证据仍不替代固定安装、专用UID及qualified child验收。

规划会话2026-10-31已裁定先完成真实有历史Classic child的启动、registration、服务与实际排空，再接launcher签名/Unix资格及共同bootstrap恢复桥；配置与关闭顺序已同步execution-objective。历史资源和网络episode可分两个必要提交推进，局部factory或文件测试不代表整个child完成。Fresh的空根、NO_CLUSTER_AUTHORITY及不注册边界保持，历史child需独立真实native ABSENT/既有Cookie与完整根/metadata/固定launcher上下文，Common bootstrap另需已验证交接及PREPARED/ENABLED；资源实现复用不共享权限。

完整接线包的最新公开面为`ControlledClassicMain`、`SegmentClassicChild`和`ClassicChildResources`。外层仅公开heap-only create(recipeId)和生命周期；其私有发行的嵌套StartContext绑定原owner/桥/唯一Bookie/冻结配置/实际metadata，在START核验后、任何Classic副作用前一次消费。外层持控制通道与metadata及唯一关闭worker，桥持存储/GC/StateManager/TCP图；prepare只返回heap holder，各内部部分构造也先登记再初始化。READY要求实际listener、Bookie和本session registration及最终episode核验全部成立。STOP后reader退出接收、worker实际drain业务/网络/metadata，再发有界组件结果、关闭控制pipe及线程并终结；组件结果不是签名DRAINED，父仍必须等原Process正常退出，EOF/坏帧保持失败。fixture仅直接测试包内资源，不构造生产StartContext或提升临时材料资格；生产入口及其完整消费者是本包验收点，外部系统安装/专用UID/exec前提缺失时qualified成功明确NOT_EXECUTED。

父`f1005222da0fea6ebe5683d14b9d67d060faf98d`上的装配WIP已在包内直接消费历史资源图：原ZK native ABSENT、现存完整根及远程/本地Cookie核验后，使用真实LedgerManager/LUM接历史Journal/Interleaved/GC，replay及严格初始flush后才开放fixture存储准入；Cookie不补建/修复，Fresh仍拒绝历史根。共享生命周期基类仅传递关闭请求，不共享启动许可。历史replay新增有界header/metadata长度、fence及未知特殊entry拒绝，并在使用完真实恢复ByteBuf后释放。Cookie open已开始但没有返回handle的确定反例原先错误报告组件DRAINED；修正保留原acquisition失败及未决资源，重复观察仍CLOSING、不伪造首次关闭结果。

该反例原单项失败、初轮两类19项通过、修正及replay用例后的两类21项通过各自保留原源码/输入/class/日志/XML；最终相关八类97项全部通过，0失败/错误/跳过，Spotless/Checkstyle退出0。覆盖真实私有文件/ZK、存储Add/force/重启读取、Cookie字节/inode保持、借用metadata未被关闭及原Fresh/历史Journal/index/logger/GC相关回归。证据位于`/Users/liusinan/.codex/artifacts/unified-wal/classic-child-assembly-evidence-f1005222`，各阶段1370项输入分别绑定，不移绑旧PASS；本增量RAT未重跑。归档核验发现runner未复制FreshClassicOwner的WIP源码，但四次原inputs都已声明相同SHA；从仍完全匹配该原SHA的当前文件单独恢复，原inputs及阶段快照均未改写。两次独立lsof及硬链接/身份/哈希核验后，仅清理四个raw scope的八目录、25文件路径、2,479,084逻辑字节，归档硬链接保留，regular-file数据块回收0。当前仅包内storage fixture装配，尚无生产StartContext/外层/Main、TCP/registration及控制通道，桥的组件DRAINED不是qualified child或签名终态。本块保持未提交WIP，完整child/issuer/Unix/bootstrap及整体Gate继续OPEN。

后续受控注册增量已直接接入该历史Bookie装配：StateManager改为包内heap holder，先由Bookie登记，再初始化统计与真实RM事件；首次注册机会之前所有模式变化只更新本地状态，普通registerBookie入口不能绕过受控桥。实际网络阶段将消费一次机会并冻结模式，从排队起异模式请求立即fail-stop，不等原注册任务执行完；同模式重复通知保持合法。受控ZKRegistrationManager观察原session（包括尚无首次registration时的Expired），只做一次原session create，不调用普通切换中的path delete；成功核验原session/czxid及另一模式absence，失败/未知不重试或采用后来可见的node。Bookie保留原StateManager失败来源，组件已排空但外层metadata尚未关闭时确认Expired，仍保留SESSION_EXPIRED；已STOP也不能抹掉失败。

规划会话同时冻结受控metadata不得跨session重试。实现选用同一个实际raw ZooKeeper原连接，ZKMetadataBookieDriver仅借用它做heap装配、按有效URI取得原namespace，不预建readonly容器、layout或新连接；受控driver不能换回普通initialize或创建第二个RM。受控RM拒绝ZooKeeperClient及其他subclass，从实际调用源消除自动重建session/应用create重试；普通Classic/Fresh继续各自原合同。原session关闭完成ephemeral清理，不按路径删除可能已属于另一session的节点。当前没有生产StartContext/外层/Main及实际TCP bind，这些真实注册仅由包内资源fixture直接验证，不能将名为registerAfterBind的包内调用当作实际bind或READY。

原真实ZK反例确定失败于“storage-only图的模式切换发布服务registration”，保留原单项failure。修正后两类22项、早期retryable-client图三类28项、改为raw-session图三类28项、create终态三类31项分别保存原源码；早期28项不证明禁止跨session重试。最终六类40项全部通过（0失败/错误/跳过），覆盖原Fresh/StateManager/driver、普通RM与原启动源，实际raw-session读写/readonly首次create、同模式重复、首次注册前及组件drain后实际Expired、无新session、实际create成功后注入丢返回或挂起原调用体、在途异模式/Expired与关闭超时、迟到返回拒绝和另一session重建同路径仍保留。两项故障注入只包裹真实ZK调用，不伪造ZK成功，也不称协议级网络丢包。样式首轮四项失败（两行长度/两处import顺序），修正后Spotless/Checkstyle退出0；本增量RAT未重跑，无TLA/性能或qualified子进程验收。证据`/Users/liusinan/.codex/artifacts/unified-wal/classic-child-lifecycle-evidence-f1005222`各阶段绑定1370项输入；copied runner中“无registration”旧描述由核验记录按实际suite纠正，原inputs不改写。各原源码/父Git blob/实际XML核验后，两次独立lsof及硬链接/身份/哈希检查仅清理六个raw scope的十二目录、36文件路径、4,554,381逻辑字节，归档硬链接保留，regular-file数据块回收0；共享target及用户文件未清理。当前全为未提交WIP，下一步继续生产TCP/START/控制通道与整图实际终结，完整child/issuer/Unix/bootstrap、Main/G1/full-bytes及生产Gate仍OPEN。

后续listener阶段已由原ClassicChildResources直接持有：BookieServer及Netty先返回heap holder再登记/初始化，auth/IO/acceptor构造与原bind future均留在原owner；未返回group的部分构造失败保留原acquisition未知及cause，已知前缀继续关闭，不重试或补称DRAINED。实际accepted channel在pipeline构造前登记，listener也纳入原ChannelGroup；close等待原bind/首次channel关闭结果、event loop终结及原实际线程join。普通构造与start/shutdown路径保持，受控holder拒绝误走普通启动/10ms关闭；该已封存阶段不含processor、不开放业务，真实已bind只投影组件TCP_BOUND，既不发布registration也不是READY。listener-only drain明确拒绝带processor的图，以下processor增量另行接线和验证完整资源关闭。

该listener阶段最终三类32项通过（0失败/错误/跳过），其中14项历史资源/注册/listener、16项原Fresh及两项普通Classic客户端真实TCP Add/read/NoSuchLedger。新增验证包含真实私有loopback绑定与关闭后端口重新绑定、激活前拒绝（EOF或SO_LINGER允许的RST，超时/应用数据仍失败）、实际owned event loop自await在退休前拒绝、真实占用端口的bind失败及未知acceptor构造返回在fixture实际清理后仍CLOSING。两项普通SDK只证明普通路径，使用显式隔离无共同绑定的Null-metadata夹具；不是经受控processor的请求或生产native ABSENT。该32项不包含后续processor证据，不能移绑当前源码；生产START/外层/Main/控制通道及qualified child仍未完成，整体Gate继续OPEN，修改保持未提交WIP。

证据`/Users/liusinan/.codex/artifacts/unified-wal/classic-child-tcp-evidence-f1005222`分别保存：首轮主源码checked-catch编译失败/零测试；修正后30项中一项TCP reset观察错误；接受合法关闭后32项中两项旧普通客户端夹具被UNSUPPORTED启动检查拒绝（含原teardown次生NPE）。随后仅该夹具显式声明隔离无共同绑定并处理setup未完成的清理，生产拒绝与原客户端断言保持。早期32项的BookieClientTest未列入原1370输入或class-prefix库存；在下一次编译前单独保留父HEAD源码及仍在target的失败class作恢复证据，明确不是原声明输入，不把失败移绑为通过。新增其声明后runner先因1370→1371计数拒绝、未启动Maven，原harness失败单独保留；最终两轮32项分别绑定1371输入。样式首轮一项nested-import顺序失败，修正后的Spotless/Checkstyle与最终源码32项均通过，本增量RAT未重跑，不提升兼容/模型/性能或完整child Gate。两次独立lsof及birth/owner/inode/空目录复查，仅rmdir删除与两次失败setup时间和数量完全对应的两个自有空根；原源码/class/日志/XML及用户文件保留。五个Maven raw scope在两次独立lsof及硬链接/身份/哈希复查后仅删除十目录、26文件路径、3,478,639逻辑字节；归档硬链接保留，regular-file数据块回收0，共享target未清理。

当前在父源码`f2df1313cdb7fa82d58d56dc7e557112b6901ce8`上接历史Journal资源。包内ClassicJournalRecovery在取得任何fd/目录前由Journal保留，仅读现存单链接普通文件、有界V6记录与每Journal的lastMark副本；沿原最大有效cut及缺失副本零贡献语义，存在但长度/位置无效或不可读的副本拒绝，不吞错。原record loop由普通和owned路径共享，padding与未完成尾部保持原语义，owned路径额外限V6及配置中的frame/alignment上界。目录/读取fd记录首次实际close，未决close保留原资源且不重试；不创建缺失txn、truncate、force或改写旧历史。不同包内factory只选择资源初始化路径，不发行启动许可；原Fresh initializer仍拒绝历史，不能用调用另一个initializer放宽它。实际replay在writer启动前作为原Journal活动登记，close等待其调用体/扫描callback及文件实际终结；writer启动与replay互斥，caller timeout不取消实际I/O。

首组八项实际通过，包含真实owned Journal写入/force/关闭后读取历史并续写、普通legacy reader再读新增V6记录、已有多Journal lastMark原inode更新、坏版本/长度及链接拒绝、file/directory首次close失败、实际scan挂起时外部close等待与自排空拒绝。测试只证明Journal文件与资源行为；没有真实受控child、registration、历史index/entry logger/GC/metadata完整图或Unix资格。相关回归与完整资源图继续推进，尚未提交本块或提升任何Gate；完整16步目标active。

随后扩大到ClassicJournalRecoveryTest、FreshClassicJournalDrainTest和BookieJournalTest，实际40项中13项在进入parser前被现有ClassicRouteStartup拒绝：旧测试的Null backend没有native binding读能力，返回UNSUPPORTED。只在BookieJournalTest的隔离fixture显式声明该测试Bookie的ABSENT，保留所有原legacy格式/坏Journal/partial FileInfo断言，生产preflight、Null backend及其他测试fixture不改。辅助修复脚本曾因构造点数量断言停下，后续命令仍启动了一次原源码重复回归，同样13项错误；两次原错误、原源码/class及脚本失败分别保留，不标作修复后的验证。修复后的三类40项实际全部通过，0失败/错误/跳过；样式首次因一行121字符退出1，修正后退出0。隔离fixture的ABSENT仅用于旧parser兼容测试，不算真实ZK或受控child启动证据。

进行中的证据目录为`/Users/liusinan/.codex/artifacts/unified-wal/classic-episode-evidence-f2df1313`，四次Maven各自绑定1345项源码/资源、原source/class/日志/XML；当前Journal部分仍为worktree修改，RAT及整个历史资源图/episode验收尚未执行，不封为child交付。原阶段逐项与快照或父Git blob核验、各Maven终态与两次独立lsof及硬链接/字节/身份检查后，仅清理本次四个raw scope的八目录、22文件路径、2,411,303逻辑字节；归档硬链接保留，regular-file数据块回收0。专用旧UID/外部降权程序、完整历史index/logger/真实GC metadata回调与网络资源图仍须接线，默认OFF/SYSTEM/PARK和disabled保持。

同一worktree继续接历史entry logger/index。ClassicHistoryFiles仅抽出Journal、index、entry logger实际共有的目录/只读fd登记、single-link/NOFOLLOW身份检查及首次native close；各外层仍先等待实际initializer/scan/body，才能排空该子owner，不新增backend、线程或authority。entry logger沿原lastId hint及目录largest-gap编号选择，缺失/畸形hint可回退实际文件，读/close未决则拒绝；所有目录与hint流保持owned终态，不把close抛出的NoSuchFileException认作缺失hint。新resource factory与initializer互相固定，原Fresh仍拒绝历史。

历史index先在完整根集合中只读检查全部canonical idx/idx.rloc的现存单链接V1 header、页长度对齐及有界master-key/explicit-LAC字段，再恢复原布局；已有唯一idx保持authority并清理冗余rloc，唯一orphan rloc复用原native no-replace提升并完成父目录同步/实际close，多个不明权威副本拒绝且不猜测。每次改leaf前先关闭其iterator，不使用已修改目录的旧快照。坏header/未决read close不触发任何rloc恢复；lazy FileInfo读既有index只READ/WRITE/NOFOLLOW，不带CREATE，文件在open前消失不能补建。新增index initializer线程归属检查，拒绝自排空。FileInfo的owned header路径在分配explicit-LAC buffer前检查剩余长度，避免坏长度进入大分配；普通Classic格式与原恢复分支保持，V6/V1限制只由受控历史入口施加。

新ClassicHistoryIndexEntryLogTest七项覆盖真实已落盘entry/index在独立index根重开、读回/续写/再重开、旧log不改写、原idx/单orphan rloc语义、全量header预检拒绝坏版本/长度及模糊副本、lastId和header首次native close失败、初始化自排空拒绝与missing index不补建。与Journal八项共15项实际通过；随后执行这两类及FreshDefaultEntryLoggerTest、FreshClassicIndexDrainTest、FreshClassicJournalDrainTest、BookieJournalTest，六类83项全部通过，0失败/错误/跳过；Java样式退出0。原八项/40项及其失败与当前15项/83项分别保留源码/class，不移绑旧PASS。当前是十二项代码路径的局部历史文件图WIP，RAT、真实LedgerManager/GC metadata回调、Interleaved/Bookie完整历史owner、网络episode及真实launcher/Unix仍未闭合，不提交或封为完整child，完整16步目标active且各Gate保持OPEN。

本次15项/83项及样式独立阶段保留在同一进行中的证据目录，实际源码/资源输入1347项；旧Journal四阶段与原worktree checkpoint不覆盖。原阶段逐项与source快照或父Git blob核对后，两次独立lsof与硬链接/字节/身份核验仅清理这次两个raw scope的四目录、14文件路径、1,351,588逻辑字节，归档硬链接保留，regular-file数据块回收0。Maven实际重编译已移除旧ClassicJournalRecovery内部Read class，本任务没有手工清理共享target；当前startup读fd/目录代码由三个已有调用点共同使用，不能据这些局部文件测试关闭完整child/生产Gate。

历史资源图继续接入Interleaved及原GC：ClassicGcMetadata在真实AbstractZkLedgerManager提交前登记调用体和原future，不把timeout/cancel认作实际完成；复用专用ZkLedgerUnderreplicationManager已有guarded lock，登记实际acquire/release及首次结果。同步提交已发生而返回丢失、首次release失败（包括实际delete已发生后丢失响应）均保留未决，不盲目重试；确定NodeExists不删除外来锁。沿原GC算法和executor，callback中的permit/latch在真实unlock返回后才释放，实际executor、metadata调用体/回调/锁结束后才关闭GC map。借用的manager/LUM仍由外层episode关闭；新组件不发行启动、删除或DRAINED authority。历史Interleaved入口保留metadata后才做文件I/O，原Fresh仍空根且无远程GC authority。

ClassicHistoryGcTest七项使用实际私有ZK/HLM、既有落盘entry/index与guarded UR lock，覆盖真实metadata存活/缺失、超时及中断观察不取消原回调、真实unlock未返回时close等待、自排空拒绝、有界读取、提交结果丢失和首次释放失败。首轮七项有一项因sentinel在原GC周期前插入而失败，调整到周期结束后七项通过；首次九类95项有一项错误固定pendingReads=1：已完成future的consumer可在原GC线程执行，此时read slot已退休，真实lease/body/executor仍在。仅移除该调度假设，保留真实锁/调用体及close等待断言，最终九类95项全部通过，0失败/错误/跳过。样式首次一行121字符退出1，修正后两次实际退出0；各原失败、阶段源码/class/日志/XML独立保留，不能把旧PASS移绑新源码。当前1349项源码/资源、17项代码路径仍为未提交WIP，RAT未执行；完整Bookie owner、metadata-manager/factory/client、真实网络child/registration和终态尚未闭合。

规划会话已冻结一次性START握手：child从固定系统根读取UID0保护的公钥/recipe，自行生成challenge，parent持原guard并独立核对真实Process、程序/有效配置/原scope和native ABSENT后仅签一次；child验签及复核后才消费内部启动上下文。报文、固定trust loader、issuer及完整child仍待实施，不能用局部资源factory或任意verifier callback作为启动资格。专用账号、exec降权程序及信任材料由外部部署，不自动安装或改变系统权限；完整launcher/Unix、离线bootstrap及16步验收继续OPEN，目标保持active。

GC四个阶段的原源码/class/实际退出与XML已分别核验；两次独立lsof及硬链接/字节/身份检查后，只清理对应四个raw scope的八目录、32文件路径、3,629,559逻辑字节，归档硬链接保留，regular-file数据块回收0。history-gc-worktree-checkpoint保留当时1349项源码/资源、十项文档和125项相关class，后续修改不覆盖该checkpoint，也不移绑旧95项PASS。

启动许可依赖随后进入同一WIP：包内SegmentClassicLaunchCodec实现固定HELLO与START的有界规范解析、Ed25519独立分域验签及对本次全部观察值的逐项匹配，不提供签发或startup permit。scope采用显式canonical编码，绑定实际metadata namespace、原cluster出生、Bookie/Cookie出生/version/hash及完整有序根/角色，不使用未来共同incarnation或binding。独立Python/OpenSSL向量HELLO 211字节、START 327字节；两类15项通过，包含逐字节变更/截断、重新签名的非法字段、全部观察值错配、recipeId不能选路径和与UWCHOF01互相拒绝。这里只证明字节与密码学，不证明实际child challenge一次性消费或信任锚来源。

ZKRegistrationManager新增heap-only借用启动源reader，绑定本manager实际client及固定ledgers root，复用ProfileZKReadClient记录实际提交/原callback。每次读取前后要求native ABSENT，读取原INSTANCEID持久出生/不可变版本与现存Cookie出生/version/hash/原bytes、核对两类registration缺失；不要求或创建PREPARED/commissioned requirements，不补Cookie、写metadata或注册。返回值仅是冷读数据；实际client的chroot/完整namespace还须由固定recipe及冻结配置约束后供scope编码，不能把相对ledgers root当作完整realm证明。超时/中断不取消callback，提交结果未知保留原slot；close等待真实同步调用体且拒绝自排空，不关闭借用client。

新增真实ZK源测试与启动codec、原handoff codec、原bootstrap源及native-binding回归五类41项全部通过，0失败/错误/跳过；包括读取期间真实commissioning转FOUND后拒绝、既有registration/缺Cookie/变更cluster拒绝，以及实际callback挂起、观察中断与未知提交的资源行为。样式首次因测试一行121字符退出1，修正后退出0，原失败单独保留。当前阶段声明1354项源码/资源；前述15项阶段声明1353项，均保留各自绑定。固定系统trust loader的父链/ACL核验、实际launcher issuer/Process/一次性控制通道、完整Bookie与真实child registration仍待实现，RAT未执行；本机JDK的POSIX属性不能替代macOS ACL读取，不以mode或任意true回调放行。完整16步目标继续active，各Gate保持OPEN。

## 固定 child 信任材料与只读 ACL 前置能力

父源码`f2df1313cdb7fa82d58d56dc7e557112b6901ce8`上的同一WIP新增native-io.hasExtendedAcl。查询不改权限、不打开或移交业务fd，也不自行授予信任；只接受现存普通文件/目录，保留实际errno，空/坏编码、符号链接、special file、未知文件系统或无法读取均拒绝。macOS在本地APFS/HFS上通过lstatx_np/filesec查询FILESEC_ACL presence，同时核对实际inode/mode/UID/GID/ctime稳定；避免把acl_get返回的ENOENT直接当成“没有ACL”。Linux实现限定EXT4/XFS/Btrfs/tmpfs的access/default POSIX ACL xattr，实际运行/跨编译仍NOT_EXECUTED；不能据此宣称Linux或完整Unix资格。旧Java实现默认拒绝，旧JNI缺新符号明确拒绝，没有mode-only或返回false的fallback。

本机macOS arm64/APFS实际构建首次因错误选择lstatx64_np符号退出101，按SDK的arm64/inode64 ABI修正后退出0，原失败保留；没有改变依赖版本或构建设置。新host库SHA-256为`42a00ac97c6877074fed27e2f0ad276c25cc55e17d93fb3b0d4096e873f0ea35`。新ACL与原rename/open/force/close两类首轮及最终各12项通过，含实际添加ACL后POSIX mode仍为私有而查询返回存在、文件与目录、Unicode/坏路径和原原子发布ABI；原阶段绑定分开保留。两次独立JVM使用前一精确JNI（SHA `619efd63ed22a72536e00d791d679aa6559edc7fdfa68621df50850bc7e5b7a2`）确认旧open/close可用、新ACL方法因缺符号拒绝，均退出0。Rustfmt及Java样式实际退出0；RAT实际退出1，仍为原431项未批准路径且逐项父Git blob字节不变，扫描前排除用户PDF；不宣称全量CI通过。

包内SegmentClassicLaunchTrust仅实施固定系统根的owned public-trust I/O：production入口只收有界recipeId，固定Linux/macOS根及UID0，检查完整无符号别名父链、不可由非受信主体替换的mode、实际无额外ACL、单链接普通文件/上界、读前后原inode/字节长度身份及首次native close。recipe UWCLR001/schema1/flags0总长≤4096，独立197字节Python向量绑定部署、固定key文件名/keyId、程序与实际有效配置及原scope摘要、非零旧UID/GID与有序有界groups；公钥仅从recipe固定选择的32字节原始Ed25519文件读取。内容不会被报文自带key替换，不生成配置、key、系统目录或材料。固定根的Installed类型只有成功生产loader可建；包内临时树入口只能返回Parsed，不能提升为Installed，而Installed也不证明程序/JVM参数、当前metadata、实际进程或完整Unix资格。

新trust与原handoff materials首轮16项中有一个测试夹具error：同时拦截JDK嵌套的两个open重载而重复包裹同一真实fd，清理时又触发原close故障。只限定实际三参数打开重载，并强化相同字节替换后必须报告identity变化的断言，生产检查不放宽；修正后16项通过。最终新trust、启动codec、原materials及publication四类32项全部通过，0失败/错误/跳过，覆盖坏recipe/组/路径选择/缺材料、真实ACL/不安全父链/链接、原fd替换、首次close失败持续non-drained，以及实际open调用体未返回时自排空拒绝/外部close等待；临时材料不能取得Installed。这些是文件/ACL及字节证据，没有实际生产系统信任安装、launcher签发、child Process/challenge/控制通道或真实registration成功。历史Bookie/网络/Unix/共同bootstrap完整链仍OPEN，目标保持active，各Gate不变。

进行中的证据为`/Users/liusinan/.codex/artifacts/unified-wal/classic-launch-trust-evidence-f2df1313`，各实际源码/class、日志/XML及首次失败分别保存。native首轮声明1355项源码/资源，后续trust各阶段及native最终声明1358项；旧1354/41项与1349/95项checkpoint不覆盖、不移绑。native只读能力是单独可提交的必要依赖；其提交不交付仍在worktree中的历史资源/trust完整child，也不关闭Main/G1/full-bytes/生产Gate。

全部阶段终态及原source/父Git blob绑定核验后，两次独立lsof与硬链接/身份/哈希检查只移除本次五个raw scope的十目录、27文件路径、1,862,403逻辑字节，归档硬链接保留、regular-file数据块回收0。另核验独立Rust target无人使用及170文件原身份/哈希后移除其43目录、42,821,909逻辑字节；精确host库、所有原build源码/日志（含101）、命令、class与测试证据保留，不宣称实际卷空间回收数。共享Maven target、用户PDF和其他任务路径未触碰。

native前置依赖已本地提交`2658747716c369722e3ad2348a49e96a97e4ce2f`，仅六项native代码与两项既有文档，未push；历史资源图、codec/ZK源/trust owner仍为未提交WIP。原阶段source map仍绑定父`f2df1313`，没有追认该提交交付完整child。规划会话随后冻结固定program/conf的完整装载边界，详见execution-objective：特别是解析前禁include/插值/System/Environment源、固定实际动态默认，以及首次JNI前的可信预装基线及program/hash核验。该阶段的production trust I/O尚使用普通NativeIO装载backend，材料测试不能证明早期启动链；后续一次性装载依赖已新增production前置拒绝，完整受控main与可消费START上下文仍未接线。

在等待该裁决期间补上独立的本地Cookie读取：ClassicHistoryFiles.readCookie复用原owned fd/首次关闭和现有Cookie parser，读取现存current/VERSION，最多1MiB并拒绝坏UTF8；不补建、修复、改写或推断metadata/完整根资格。新增两项真实文件验证包含原字节/inode保持、缺失/超界/坏编码拒绝，以及首次native close失败不返回解析值且持续non-drained。首次九项中新增两项在parser前因夹具未指定BookieId、默认loopback禁止而error；只给各隔离夹具指定明确测试BookieId，生产网络规则不改，修正后该类九项全部通过，0失败/错误/跳过。原九项失败与修正后的源码/class/日志独立保留，不能据此提升真实ZK/Cookie来源或完整child Gate。证据阶段位于`/Users/liusinan/.codex/artifacts/unified-wal/classic-child-inputs-evidence-26587477`，仍处于实施中。

Cookie两个阶段各声明1358项源码/资源；原source/父`2658747716` Git blob与各自class/XML核验后，初轮及最终Java样式均退出0。两次独立lsof及硬链接/身份/哈希检查仅清理两个raw scope的四目录、八文件路径、716,752逻辑字节；归档硬链接保留，regular-file数据块回收0。未重跑旧95/41/32/native12项来追认新源码，当前RAT未重跑；完整历史Bookie、程序/配置装载及JNI启动基线、真实Process握手与各整体Gate继续OPEN。


固定program的数据校验继续在同一WIP推进：UWCLP001/schema1/flags0声明有界OS/arch/JDK、固定child主类、绝对Java/JNI、完整有序普通JAR classpath、固定允许集合的JVM参数，以及受保护文件的角色/长度/SHA256。独立Python向量684字节；程序身份按独立域覆盖完整原字节。trust owner以64KiB工作区流式核对全部文件并枚举完整JDK树，拒绝未声明文件、别名/链接、可替换父链或ACL；实际JarFile检查manifest Class-Path与JAR index，防止隐式依赖。一次调用只使用caller给定的同一deadline，不逐文件重置；timeout不取消实际I/O或证明关闭。目录/JAR/manifest流在实际打开前登记，首次close失败不重试；JDK ZIP构造失败可能吞掉底层close异常，因此无返回handle的构造失败保留unknown资源而不报告drained。正常JarFile.close只证明本owner释放引用，完整JVM文件终结仍须实际进程正常退出。

最终SegmentClassicLaunchProgramTest与SegmentClassicLaunchTrustTest两类19项全部通过（0失败/错误/跳过），初轮18项属于增加deadline用例之前的独立源码绑定；两次Java样式均退出0。测试使用真实临时文件、JAR与合成JDK形状的文件树，覆盖文件字节/顺序、未声明文件、JAR隐式依赖、参数/环境注入、过期deadline、首次native/iterator close失败、未知ZIP构造关闭，以及实际open调用体挂起时外部close等待；没有启动该合成JDK或加载其JNI，不证明实际程序/运行时、生产Installed资格、START或完整child。当前阶段1361项源码/资源及31项代码路径仍未提交，原18项/19项source/class/XML在classic-child-inputs-evidence-26587477分别保存，不覆盖Cookie checkpoint。配置清洁装载、首次JNI前可信路径、实际Process/challenge与Bookie/网络完整资源图继续OPEN，目标active且各整体Gate不提升。


program两次测试的原输入/source/class/XML核验后，两次独立lsof与硬链接/身份/哈希检查仅清理对应两个raw scope的四目录、十文件路径、797,881逻辑字节；归档硬链接保留，regular-file数据块回收0。program-worktree-checkpoint保留当时1361项源码/资源、十项既有文档及216项相关class，后续配置修改不覆盖该checkpoint。

同一父`2658747716`上的SegmentClassicLaunchConfiguration随后接入固定trust owner的`<recipeId>.conf`读取：原owned fd/父链/ACL/单链接/替换/首次关闭语义保持，输入1..256KiB、最多4096个逻辑属性；含凭据文件拒绝other权限，production另要求recipe中旧UID预期组可读，完整组成员/权限隔离仍须外部Unix资格。解析直接复用Commons 2.12 PropertiesReader的转义/续行及原逗号列表转换，不经过include/URL装载器；解析前关闭includes/插值，原文本及解码后的key/value显式拒绝`${`，包括转义/续行构造，include/includeOptional大小写与转义形式均拒绝。新ServerConfiguration明确移除全部非内存继承配置并安装空interpolator，再载入本次属性，不先展开系统/环境或读第二配置文件。

固定有效值使用实际getter校验并写回typed snapshot：IO、add/read/long-poll/high-priority/Journal callback线程、allocator并发、Journal内存、Interleaved pageLimit、BookieId、数值广告地址与非零固定端口、完整三类有序绝对根以及有效metadata URI；不依赖parent CPU、JVM direct-memory、DNS或旧root/zkServers别名补值。其他常量默认由批准program版本约束，整份剩余属性也进入摘要，没有只hash几个开关。最终快照包含不可变排序map及不可变有序list，不暴露可修改ServerConfiguration。它仍是数据；实际资源图消费同一快照的接线及历史运行策略校验尚未完成，不能据此取得START。

规范摘要域为`BookKeeper/ControlledClassicConfiguration/v1\0`，覆盖UWCLC001、u16 schema1/flags0、u32属性数；key按Java UTF16 code-unit词典序排列并以u32 UTF8长度编码，value带明确tag：1/string、2/boolean、3/i32、4/i64、5/有限f64、6/有序扁平list（u32数量及逐项scalar）；不支持对象/嵌套list/非法UTF8/NUL/非有限值均拒绝，规范流最多1MiB。原properties产生string/list，显式有效字段经实际getter转换为对应数值类型。独立Python 170字节向量覆盖非ASCII键及类型/列表顺序，SHA256为`954c53da6c0c7c1368be118c6a693a0df2235c1ffc8a91bc880c20a5e76d5d71`，没有以toString替代编码。

首轮配置/trust两类16项有一项实际失败：空journalDirectories触发旧getter回退默认目录，使新入口接受不完整根；未发生目录I/O。修正为先验证原始列表再调用getter，原断言保留，16项全部通过。配置/程序/trust/启动codec四类首轮34项通过；随后沿实际Interleaved调用补齐pageLimit的显式固定，最终同四类34项全部通过（0失败/错误/跳过），两次样式退出0。覆盖实际固定conf读取与原字节后改、整个map摘要/列表顺序/typed编码、include/插值/坏输入/缺动态值/旧别名拒绝、首次native close失败持续non-drained，以及两个独立真实JVM在静态系统属性继承关闭/开启时得出相同冻结结果。两个JVM仅运行配置probe，不是受控child或START/registration验收。原失败、16项修正、前后34项各自保存1363项输入与实际source/class/XML，不移绑旧PASS；配置阶段证据仍位于classic-child-inputs-evidence-26587477，RAT未重跑、未提交，完整16步目标active且Main/G1/full-bytes/生产Gate保持OPEN。


规划会话随后裁定首次JNI装载采用固定安装库及普通/受控共用的一次性状态，限定同一system/application ClassLoader装载域；普通已开始不能补升，受控重入/竞争失败不得因load迟到返回补发成功。早期纯JDK preload只核对外部可信预装基线，原deadline/首次读取关闭保持；同一库装载后仍须核对同一份recipe/program、ACL/身份与完整配置，再继续metadata/实际Process及START。库层不授Installed/启动/排空资格，不fallback、不重置、不持锁跨I/O或load；精确边界已同步execution-objective。该裁决记录时尚未实现；下节记录后续独立装载原语与production前置拒绝，完整早期启动链仍OPEN。配置四阶段原source/父Git blob及各class/XML核验后，两次独立lsof与硬链接/身份/哈希检查仅清理四个raw scope的八目录、24文件路径、1,782,323逻辑字节；归档硬链接保留，regular-file数据块回收0。配置checkpoint保留1363项源码/资源、十项当时文档及220项相关class；共享target与用户原文件未清理。


## 受控 JNI 的一次性装载依赖

父源码`2658747716c369722e3ad2348a49e96a97e4ce2f`上，native-io新增NativeIOBootstrap，公开受控入口只接期望长度、复制的SHA256及原本地deadline，不接路径/loader/backend/回调；生产只选既定固定UID0系统安装库。原-D/env覆盖即使同路径也拒绝，无候选搜索/解压/fallback。纯JDK检查完整POSIX父链、现存单链接普通文件/UID/mode/长度，64KiB工作区逐块hash，原fd首次close成功后才System.load；保留原inode/时间/父链，装载后及生产结果读取时再核对，不能以同字节替换接受新inode。这些POSIX/hash事实依赖外部可信预装JDK/固定child/ACL JNI，不替代首次装载前的部署信任或后置ACL证明。

NativeIOJni静态块现在与受控入口共用一次性状态：普通先开始不能补升；受控期间并发/重入/普通JNI初始化令原尝试失败，原调用体继续实际终结，load迟到返回不补成功。状态锁不跨I/O、System.load或等待自身类初始化；仅支持helper/Impl/Jni同一内置JDKAppClassLoader域，其他loader副本不能重置资格，不称任意JVM全局native证明。原ordinary属性/env/JAR候选及异常行为沿原选择器保留；受控已装载后JNI复用原库，不再读取ordinary覆盖。各阶段沿同一deadline，过期或首次关闭/读取/装载失败保留原cause及实际loadEntered/loadReturned事实，无卸载、重试、强行中断或DRAINED；库层不自行退出JVM。失败首次close的原handle保持保留，不因测试手工清理而改为成功。

返回Library仅为原库数据，fixture路径仅包内测试使用，标记不能取得production controlledLibrary结果。production trust准备现已在材料I/O前拒绝普通/未决/fixture装载；program核验同时比较实际原库的固定路径、长度/SHA与原文件/父链身份。它仍不发行实际程序/JVM/Process/START资格：纯JDK的固定recipe/program preload及与后置同份材料的关联、早期JVM参数检查、实际Main/Process/challenge/metadata/控制通道、一次性context与完整历史Bookie仍须接线。

NativeIOBootstrapTest首轮四项、18个独立JVM通过；其后补原库身份留存/同字节替换与实际坏库load失败，四类28项通过，Bootstrap类为19个独立JVM。最终native四类28项与configuration/program/trust/启动codec四类35项合计八类63项全部通过，0失败/错误/跳过，包含production材料入口拒绝普通backend。独立JVM覆盖普通成功/失败、其他loader副本、真实hash/长度/别名/原fd替换/读取/首次close失败、受控一次真实装载与JNI复用、真实挂起I/O期间的并发及普通初始化、load重入/超期返回和实际UnsatisfiedLinkError；JDK17/21的测试permission hook只用于拦截真实I/O边界，不增加生产验证回调或reset。测试使用macOS arm64/APFS上的真实库副本及当前host JNI，均不安装系统信任材料、不变更账号或发行Installed/START；Linux实际运行/跨编译仍NOT_EXECUTED。Java样式实际退出0；终态独立RAT实际退出1，431未批准路径及全部字节与父HEAD一致、无新增，扫描前排除受保护PDF，不称CI全绿。没有新增TLA语义或重绑旧模型，也没有性能结论。

证据`/Users/liusinan/.codex/artifacts/unified-wal/native-bootstrap-evidence-26587477`保留三次原源码/class/1365项输入/XML及命令、style/RAT、native组件清单与提交后字节核验；首轮四项与28项不追认最终源码。原三个raw scope在两次独立lsof与硬链接/身份/哈希检查后仅删除六目录、22文件路径、1,076,251逻辑字节，归档硬链接保留、regular-file数据块回收0，共享target与用户文件未清理。必要native依赖本地提交`f1005222da0fea6ebe5683d14b9d67d060faf98d`仅包含Bootstrap/Jni/Test三项Java路径，父为2658747716、未push；原测试仍绑定其执行时的父HEAD加WIP，提交后仅核验三文件及全部1365源码/80相关class字节一致，不改写原PASS。其余历史资源图、codec/ZK源/program/config及trust server修改保持未提交WIP。完整16步目标active，Main/G1/full-bytes/Unix/bootstrap/生产Gate继续OPEN，默认OFF/SYSTEM/PARK及disabled保持。


## 固定程序材料的纯 JDK preload

父源码`f1005222da0fea6ebe5683d14b9d67d060faf98d`上的WIP新增包内SegmentClassicLaunchPreload。prepare只冻结固定recipeId、本JVM原deadline及系统根；initialize在任何native-io调用前，通过可信JDK读取固定公共`.recipe`/`.program`，保留原fd前即登记owner，验证现存普通文件、UID0/单链接/NOFOLLOW及完整POSIX父链、上界/长度与读后身份。复用现有纯数据codec，检查recipeId及program完整原字节摘要；production在文件读取前拒绝已禁环境/ordinary native覆盖及不支持的实际JVM参数，随后核对program平台、固定JNI引用和实际参数相等。没有把POSIX或同目录hash当ACL/安装信任：JDK、固定child、ACL JNI仍依赖外部可信预装基线，实际classpath/主入口/JDK/Process完整资格仍待接线。

初始化所有检查完成后才可取得program数据。生产load只能由同一原owner使用声明的长度/SHA及原deadline调用NativeIOBootstrap一次；原材料前后核对、首次close或native失败均不换材料、不续期限/重试。production trust准备改为消费该preload owner的同次native结果并一次借用，去掉独立recipeId绕过早期原材料的准备路径。后置recipe/program读取逐项比较原bytes、原inode/父链，program/config验证结束再次检查原身份，并要求调用方保留完全相同的deadline。preload与trust的资源归属分开，外层先关闭trust再关闭借用的preload；关闭或过期的preload不能继续取得材料。initializer/load/采用/比对/终检的实际调用体均受同一owner保护，外部close等待实际返回，自排空和重入拒绝；timeout观察不取消底层I/O。

纯JDK fd首次close失败保留原handle/错误且不重试；open已开始但未返回handle时不编造native close结果。preload的drain只描述本组件调用/读取，失败的native bootstrap保持未决，不能据此发行整个child的DRAINED。fixture只读临时树，可返回data及供同份后置I/O验证；即使UID参数为0也不能load生产库或取得Installed/START。没有新增公开配置/签发接口、主类stub、外部安装或账号/系统权限修改。

首轮因新增测试错误继承final FaultChannel而testCompile退出1，零测试执行，原源码/日志及当时target class库存保留，库存不当作新编译或测试通过；改用现有Mockito包裹真实通道后，三类30项全部通过。随后补齐元数据调用体的自排空/重入拒绝与验证结束的原身份复查，最终preload/program/trust/configuration/启动codec五类43项全部通过，0失败/错误/跳过。包含真实临时文件/首次关闭失败/未知open返回、同字节inode替换、同一fd读回字节不符、实际open挂起时外部close等待，以及真实临时文件/JAR和合成JDK形状的program与同份conf/相同deadline接续。独立真实JVM以不可用ordinary native路径成功完成早期fixture读取，随后改回实际JNI并成功执行ACL查询，确认preload未提前初始化native-io；该probe不运行受控Main或发行资格。样式首次一行121字符退出1，换行后Spotless/Checkstyle退出0；RAT本增量未重跑，未把前轮63项或RAT移绑本轮源码。

证据`/Users/liusinan/.codex/artifacts/unified-wal/classic-preload-evidence-f1005222`保存每阶段1367项原源码/资源输入、source/class/实际XML/日志/命令及样式失败；复制runner中的旧scope描述在核验记录中按实际suite纠正，原inputs不改写。三个raw scope经原输入/父Git blob、两次独立lsof及硬链接/身份/哈希核验后，仅清理六目录、17文件路径、1,295,273逻辑字节，归档硬链接保留，regular-file数据块回收0；前轮sealed native证据330文件checksum再次核验一致，共享target和用户文件未清理。当前preload及其server接线仍为未提交WIP，HEAD保持f1005222da、未push。系统UID0固定安装的成功入口、真实Main/实际JVM/Process/challenge/控制通道与一次性START、完整历史Bookie/registration/网络排空、issuer/Unix及共同bootstrap仍OPEN；完整16步目标active，Main/G1/full-bytes/生产Gate和默认OFF/SYSTEM/PARK及disabled保持。
