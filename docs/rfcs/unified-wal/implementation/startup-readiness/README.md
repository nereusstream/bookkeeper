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
