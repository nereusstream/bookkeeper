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

首版证明固定使用JCA Ed25519（外部固定公钥、launcher独占私钥），签名覆盖`BookKeeper/CommonClassicHandoff/v1\0`与完整header/payload，总长度不超过64KiB；封套依次为`UWCHOF01`、u16 schema=1、u16 flags=0、u32 payloadLength、32-byte signerKeyId、固定顺序/严格UTF-8/有界payload、64-byte signature。payload绑定唯一OFFLINE_CLASSIC_BOOTSTRAP/native bindingId、部署/程序清单、原cluster/Bookie/Cookie/native出生与PREPARED语义、requirements原记录/birth/generations、共同incarnation/manifest/control根、按manifest角色/索引对应的完整物理成员、真实episode/退出/UID、独立32-byte AES材料identity、精确scope policy及retirement digest；未知/重复/溢出/尾随或错scope均拒绝。私有codec/loader/session不提供通用sign(ProofRecord)，完整两UID真实进程及bootstrap验收不能由codec单测或本节来源指纹替代。 Linux 目标消费的新版本见[Linux 签名宿主视图与采用前拒绝](#linux-签名宿主视图与采用前拒绝)。


证据目录`/Users/liusinan/.codex/artifacts/unified-wal/requirements-source-evidence-1d8b9256`分别保留首组十项和最终33项的原源码/日志/XML/class，style退出0。RAT实际退出1，431项未批准路径集合及各项字节与父提交和前次封存一致，无新增或解决，受保护PDF在扫描前排除。当前声明1321项源码和十项文档输入，native runner实际记录1294项，相关class快照14项；其他输入逐项对应父Git blob，不将早期getter结果移绑新增namespace字段后的源码。完成实际终态与两次独立lsof检查后，仅移除本次两个raw scope的四个目录、十个文件路径，共1,479,573逻辑字节；归档硬链接完整保留，regular-file数据块回收为0。用户PDF、默认目录和其他任务产物未触碰。本增量只交付来源身份值，不关闭完整交接证明或bootstrap验收。


## 固定 Classic 交接 codec 与精确材料绑定

2026-10-31在父源码`967ef3dee3497be377c7f2ab58d1050abf8f20e9`上，实现包内SegmentClassicHandoff固定proof/policy/retirement字节、独立32-byte AES key identity及完整材料scope匹配。具体顺序/边界与固定向量由[受控 Classic 交接材料 v1](../local-storage/format-v1.md#受控-classic-交接材料-v1)拥有，既有Segment/ROUTE/seal/native格式不变。证明最大64KiB，固定JCA Ed25519和外部已信任signer/key；对稳定原始字节验签后解析，再检查规范字节。无私钥参数、通用sign(ProofRecord)、网络信任锚下载、算法协商或JSON重编码路径。

packet复用原PREPARED native binding及SourceIdentity，绑定原cluster/Bookie/Cookie/birth/op/共同manifest/目标身份，按manifest角色与索引保存物理根而不复制路径表。原始retirement字节摘要绑定同operation及同物理identity；policy只接受本op/Bookie/incarnation/manifest/目标UID的完整相等，AES材料必须独立原始32字节。签名同时覆盖真实child关闭/正常退出和独立Unix隔离结果/回执摘要；mode与typed成功结果不自行构成这些事实。ZK Observation新增commissionedCompatibility只读原envelope兼容要求；proof requirements generations取原记录，保留当前delete-assignment overlay供原服务逻辑消费，getter不增加metadata I/O。

本增量只返回已验签的数据与材料比较结果，没有发行bootstrap session、DRAINED、readiness或registration。真实launcher固定supervisor/retirement持久状态、实际child适配器、Unix/fd/ACL完整核验、受保护材料owner/loader、当前metadata冷读，以及Classic导入/flush/seal/native ENABLED仍须串联；完整issuer与离线包仍OPEN。Main/G1/full-bytes/生产Gate及完整16步不提升，目标active；默认OFF/SYSTEM/PARK与disabled保持。


最终执行`scripts/unified-wal/run-local-storage-tests.sh 'SegmentClassicHandoffTest,ZKProfileStartupRequirementsStoreTest,ZKProfileEphemeralRegistrationStoreTest,SegmentProfileInitialSocketClientTest#ownedProfileServiceRejectsRequirementsBeforeStorageOpenAndCleansBindFailure+ownedProfileServicePublishesBoundEndpointSupportsSdkAndRetainsDrainAcrossRestart+deleteHandoffEffectiveAllRealTargetsRejoinG2AndKeepOldEventsWithNewJournalData'`：四类41项全通过（codec 8、requirements 11、ephemeral 19、实际服务/SDK/重启及handoff 3），0失败/错误/跳过；最终style退出0。独立Python struct/hashlib与OpenSSL 3.6.4生成最终policy 100、retirement 685、proof 973字节向量，OpenSSL验签成功，Java/JCA产出的签名逐字节相同。测试翻转proof全部973个字节逐一拒绝；即使重新使用测试key正确签名，未知purpose/flags/结果、坏UTF-8、溢出长度/计数、非规范时间或尾随字段仍拒绝。另验证错signer/key/policy/目标scope、完整manifest成员缺失、物理别名/重排、最大257成员、unsigned UID和不可变引用；时间戳不产生租约。真实native G2 handoff中确认Requirements.from仍取原commissioned assignment generation=1，原服务继续消费当前overlay。

首轮主编译因遗漏有界count reader退出1，没有执行测试；补齐后八项通过。对照冻结合同补入独立Unix隔离结果/回执，并增加producer签名输入与独立向量比较后，最终41项通过；未发布的937-byte草稿向量、原八项源码/class和首次style=0保留原绑定，最终973-byte输入与style-final=0另存，不将早期结果移绑。编译错误不记作产品RED，也不把codec中固定的成功结果或测试fixture当真实launcher证据。RAT实际退出1，431项未批准路径及字节与父/前次封存相同，无新增或解决，PDF在扫描前排除，不宣称全量CI通过。

证据目录`/Users/liusinan/.codex/artifacts/unified-wal/classic-handoff-codec-evidence-967ef3de`保留各阶段源码、实际日志/XML/class及两次独立向量生成脚本/输入/签名/回执。固定key为公开RFC测试材料，OpenSSL只用于向量生成，不增加运行或Maven依赖。最终声明1326项源码/资源及十项文档输入，native runner实际1299项，相关class快照67项；其他输入逐项对父Git blob核对。实际终态和两次独立lsof检查后，仅移除本次三个raw scope的六个目录、14个文件路径，共1,647,820逻辑字节；归档硬链接与必要向量生成材料保留，regular-file数据块回收为0。PDF、默认目录与其他任务产物未触碰。完整材料owner/loader、真实issuer/旧进程及两UID验收、公共bootstrap/Classic恢复桥和封口继续OPEN。

提交前审计首轮因本次审计脚本参数替换把已保存的清理字节数误写为6,779,573而退出1；依据原cleanup-result中的实际1,647,820修正参数，保留原脚本/日志/退出码后重新核对最终index、源码/文档/class及精确patch。该审计错误未修改产品源码或测试断言，不作为产品RED；最终提交以成功审计结果为准。


## Linux 签名宿主视图与采用前拒绝

2026-11-08，基线 `bdfea43986`。`UWCHOF02` 使用 schema 2、flags 0 及签名域 `BookKeeper/CommonClassicHandoff/v2\0`。在原有操作、物理成员、退出结果、Unix 回执和 retirement digest 之后追加 48 字节大端字段，依次为 boot UUID、PID 1 原始 u64 出生 ticks、u32 mount namespace inode、根的原始 u64 dev 与 u64 inode、u32 普通 mount ID。原信任锚、64 KiB 上限、签名后规范解析及退休内容绑定继续使用。`UWCHOF01` 原字节、签名域和归档验证保持兼容。Linux 材料读取要求 `UWCHOF02`。

材料 owner 在原操作和退休绑定检查后、AES 读取前冷验 boot、初始 PID/user namespace、目标进程与 PID 1 出生及调用线程自己的实际根视图。保留树挂载与同一个签名 mount namespace 关联。原 native 来源重读前后重新核验视图和材料保护，沿原读预算拒绝迟到结果。非 root 目标读取自己的视图和可见 PID 1 出生，权限失败沿原读取错误返回。

包内 Linux `SegmentClassicBootstrap` 可运行完整只读材料、保留树和来源观察。当前连续隔离资格缺失，最终在发布 `bound` 前明确拒绝。原失败保留给后续调用，原 supervisor guard 和文件继续由原 owner 排空。claim、replay、封口与 `PREPARED → ENABLED` 均须通过该边界。生产签发、实际连续管理面约束、批准 Linux 交接和生产 Gate 保持 OPEN。

Darwin arm64 共 348 项不同的定向回归通过，2 项 Linux 条件用例跳过。独立 Python 布局和 OpenSSL 验签与 Java 的 1021 字节向量一致，原 v1 向量也通过。真实文件发布和冷恢复保留同一 v2 字节，拒绝替换为 v1。首次 Checkstyle 发现一处测试行长，修正后材料测试复验及 Spotless、Checkstyle 均通过。实际 Linux 的 v1 和错 boot 早拒绝、匹配 v2 后的最终资格拒绝及原状态复核为 NOT_EXECUTED。证据位于 `/Users/liusinan/.codex/artifacts/unified-wal/classic-handoff-host-view-evidence-bdfea439`。

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


## Classic bootstrap 在 supervisor guard 下的来源绑定

在父 `9c619d579a` 上，包内 `SegmentClassicBootstrapPreparation` 把真实 ZK 冷读和材料文件接入同一个一次性离线准备过程。构造冻结原 manager、requirements store、完整 manifest、材料引用与外部信任输入。首次冷读 A 只接受匹配完整 manifest 的 PREPARED。材料 owner 使用 A 的原 native observation、immutable requirements 出生和 commissioned generations，取得原 supervisor guard 后核验签名、retirement、policy、实际目标 UID 和独立 AES key。ENABLED 重开有独立的恢复入口要求。

持同一 guard 重新读取 B，比较原 native 版本和出生、requirements 的原始来源和 commissioned 值、完整 required devices，并在两轮分别检查两类 registration 缺失。B 前后按原 manifest 的角色和序号读取 Classic、control 与全部 Arena 根目录，比较规范路径下实际 device、inode、UID、GID 和 mode 与签名成员。这个结果只记录输入关联，后续仍需实时验证旧 UID/进程/fd、ACL、重启策略与根排他。

关闭请求可在初始化 I/O 期间关闭后续准入，迟到结果沿原阶段失败收束。实际初始化返回和借用 ZK 回调全部归还后，才排空材料并释放 supervisor。材料首次关闭失败保留原资源和原因，借用的 client、manager 与 store 由原 owner 持有。该次实现沿用当时材料读取对所有 guard 文件的存在性要求，首次 PREPARED 的规则见下一节。

使用真实 ZooKeeper、测试签名与现存文件验证六项组合场景。实际重现的只读注册及同字节重建的 requirements 出生均被第二轮拒绝，同路径替换 Arena 目录被 inode 检查拒绝。正向保持原 PREPARED、Journal 与 index 字节，未创建 marker 或打开 control。独立进程观测了实际读关闭、迟到回调与首次文件关闭失败期间的 guard。首轮测试编译因误用包内 ZK 路径方法失败，改用原 manager 的完整原生观察后六项通过。最终材料 14、交接 codec 8、ZK 冷读 9、owned 文件 7、文件锁 10 共 48 项通过，Java Spotless 与 Checkstyle 通过。沿用原 acf9afec… native 库，证据位于 `/Users/liusinan/.codex/artifacts/unified-wal/classic-bootstrap-preparation-evidence-9c619d57`。

真实旧服务退役、实时 Unix 采用、完整 bootstrap owner、受保护导入、replay/flush、封口和批准 Linux 运行继续待验收。隔离测试的签名与退役数据来自 fixture。

## 首次 PREPARED 的 Classic 锁存在性

在父 `02ae3330bf` 上，持原 supervisor guard 的材料 owner 允许初次 PREPARED 的 Classic `route-claim.lock` 尚未创建。先核验 manifest 所列原根与 `current` 是实际的规范目录，叶子用 NOFOLLOW 查询。只接受明确指向该叶子的缺失异常，并复核原目录身份。control 和所有 Classic 的 binding marker 均须明确缺失。marker 存在、权限或 I/O 失败、父目录缺失、符号链接和不明确的状态均不能取得缺失豁免。

每次材料 native open 前，新出现或原已存在的 Classic 锁都必须参与同文件排除。以 manifest 顺序保留每个已观察锁的 file key 和原目录身份。已观察的锁消失、替换或变更类型则拒绝。发布解析结果前以及准备阶段第二轮 ZK 冷读后再次复核。supervisor、Bookie control 和 Arena guard 沿原严格文件保护。此处只观察锁的存在性和别名，实际建锁仍由原根 owner 在实时 Unix 采用成功后执行。

父源码上，真实 PREPARED 和签名文件的组合测试在原 `Files.isSameFile` 处复现 `NoSuchFileException`。修正后，同一用例通过，原 Classic 锁和 marker 仍缺失，原 native 观察未变。另验证了必需 guard 缺失、marker 出现、父目录或叶子别名、查询不确定、同文件与硬链接别名，以及锁出现、消失和替换。实际持有的 Classic 锁经独立进程探测保持排他。七项定向通过，最终材料 21、共同根 13、Arena 11、owned 文件 7、文件锁 10 共 62 项通过，Java Spotless 和 Checkstyle 通过。证据位于 `/Users/liusinan/.codex/artifacts/unified-wal/classic-initial-guard-evidence-02ae3330`。

## ENABLED 重开时的原证明与本地封口

2026-11-07，基线 `5747c41c53`。离线准备阶段可以绑定真实 ZooKeeper 的 `ENABLED` 观察。材料读取复用原签名 `PREPARED` 证明，检查相同原生出生、不可变绑定、Cookie 和 commissioned requirements。`ENABLED` 必须已有全部 Classic guard。

本地封口核验要求同一 ZooKeeper manager、精确的 `ENABLED` 观察、持锁的原 Classic 根和已重开的 owned Bookie control。根标记必须匹配原生绑定的 `PREPARED` 内容，本地保留的 `BOOTSTRAP_COMPLETE` 必须匹配 `ENABLED` 的封口引用。持 supervisor guard 再冷读 ZooKeeper，复核完整观察、两类注册缺失和签名成员。校验失败撤销本次输入关联，原资源继续由各 owner 排空。

真实 ZooKeeper 和文件夹具在父源码复现 `ENABLED` 阶段拒绝。正向从实际控制日志重开，复用原签名、retirement 和已应用 claim，核对原 Journal、index 和 control 字节。错误的原生封口 UUID 和证明绑定后新出现的只读注册均拒绝。最终七类 93 项回归和 Spotless、Checkstyle 通过。证据位于 `/Users/liusinan/.codex/artifacts/unified-wal/classic-enabled-reopen-evidence-5747c41c`。

此项覆盖包内的签名、原生来源与本地封口核验。后续在同一冷重开路径增加了[既有 Classic claim 清单核验](../local-storage/README.md#离线-enabled-的既有-classic-claim-核验)。完整受保护 owner、Arena 组合、实时 Unix 采用和批准 Linux 运行继续待验收。

## Classic 交接路径的实时 ACL 复核

2026-11-07，基线 `cc403f8b42`。交接材料读取器、发布器和 launcher 复用原 JNI 的 ACL presence 查询。文件及其可信父链在查询前后比对实际 Unix 身份。检测到附加 ACL、路径变化或查询错误时拒绝。保留既有规范路径、材料单链接、可信 UID、POSIX mode 和 sticky 父目录约束。

读取器保留 supervisor guard 及 proof、policy、retirement intent、AES 文件的原身份和父链，在准备阶段发布输入关联前及后续导入、恢复和封口复核时重查。包内 preparation 对签名的 Classic、control 和 Arena 根及其父链查询 ACL，对已观察的 Classic `current` 与 guard 也重查。检测到变化时，原 owner 仍负责持有并排空 supervisor guard 和实际 I/O。

在 macOS 上用实际附加 ACL 保留相同 POSIX mode。父源码错误接受 AES 文件，并在 supervisor guard 或 Classic `current` 的权限变化后完成导入。修复后三项通过，签名 Arena 根也在绑定前拒绝。原生 ACL 4 项和服务端 99 项定向回归零失败、错误或跳过。初次 Checkstyle 发现测试 import 排序，修正后 Spotless 和 Checkstyle 通过。证据位于 `/Users/liusinan/.codex/artifacts/unified-wal/classic-handoff-live-acl-evidence-cc403f8b`。

既有数据的逐项检查见下节。

## Classic 交接保留树的逐项核验

2026-11-07，基线 `837755165d`。准备阶段在发布输入关联前，以及 `PREPARED` 导入 claim 和实际 replay 前，逐个流式核验已签名的 Classic、control 和 Arena 根。根或其受保护父链必须有一个对其他 UID 封闭的目录边界。树内各成员必须属于可信 UID 和根设备，拒绝符号链接、附加 ACL、非普通文件及多链接文件。目录深度按 Classic `current` 和两级索引目录的实际布局封顶，超出时整次拒绝。文件数不按 ledger 数截断，也不因旧 entry log 不参与 replay 而省略。

检查读取目录及 Unix 元数据。每层枚举通过原材料文件 owner 持有目录描述符，首次实际关闭成功并复核目录身份后才能推进。关闭异常保留原结果与 supervisor guard。

真实文件和 ZooKeeper 用例复现父源码接受指向闲置 entry log 的外部硬链接，以及在带附加 ACL 的闲置 entry log 存在时提交 claim。修复后两项均拒绝，保留原文件字节。另包装实际目录流注入首次关闭失败，独立进程确认 supervisor guard 持有，后续排空保持原错误。macOS 上原生 ACL 4 项和服务端 149 项定向回归零失败、错误或跳过。修正一处行长后，Spotless 和 Checkstyle 通过。证据位于 `/Users/liusinan/.codex/artifacts/unified-wal/classic-handoff-retained-tree-evidence-83775516`。

目标身份的访问检查见下节。

## Classic 交接目标身份的访问检查

2026-11-07，基线 `58898f2626`。材料读取器核对原目标的实际 real/effective UID、主 GID 和补充组，在每次保留树检查前后复核。Linux 分支还要求调用线程的 filesystem UID/GID 与实际有效凭据一致。保留树使用默认 Unix 文件系统 provider 查询路径权限，并在查询后复核实际身份。目录要求读取和搜索权限，原 PREPARED 控制写入目录及 Classic 当前存储写入目录另需写权限。

文件默认要求读取。现有锁与实际以读写打开的 Bookie 控制文件需要读写权限。PREPARED 另要求可写的现有 Classic index、可恢复 relocation、ledger `lastId` 与 `lastMark` 副本，以及已有绑定标记。旧 Journal、旧 entry log 和 Arena DATA 在本离线路径按读取检查。访问查询不另开 guard inode 的文件描述符。实际目录枚举、后续回放文件打开和关闭继续由原资源 owner 观察。

macOS 上，父源码实际接受了不可读的旧 entry log，并在既有 index 变成只读后继续提交 claim。修复后两项由 Unix 权限查询拒绝，文件字节保留。真实 ZooKeeper 和回放用例也验证只读 V6 Journal 可正常导入及恢复。首轮扩大回归中的错误 UID 诊断文本已恢复。最终 155 项定向测试全部通过，另有 9 项原生凭据和内核测试通过，2 项 Linux 专属测试跳过。Spotless 和 Checkstyle 通过。证据位于 `/Users/liusinan/.codex/artifacts/unified-wal/classic-handoff-target-access-evidence-58898f26`。

本机离线 replay 的实际创建、改名和失败后续接见[Classic replay 的 lastId 实际打开保护](../local-storage/README.md#classic-replay-的-lastid-实际打开保护)。完整 Unix 采用仍需挂载别名隔离、旧 UID 进程和已打开 fd 排除及批准 Linux 上的交接证据。公开启动装配继续待完成。

## Classic 保留根的 Linux ext4 挂载观察

2026-11-08，基线 `bbcb55b739`，按当前规划会话裁决增加 `NativeIO.linuxExt4Mount`。`SegmentOwnedFiles` 的私有 Entry 从其原路径取得严格 UTF-8 字节等价的规范绝对路径，在同一个实际 FileChannel 上读取。首次关闭及在途查询沿用原 Entry 和 JDK FileDescriptor 的互斥边界，超过原 owner deadline 的结果拒绝返回。Linux 原生查询核对目录的 `fstat`、完整 ext magic、普通 `STATX_MNT_ID` 和精确的 `ext4` mountinfo 类型。调用前后均按无符号链接的原路径重新查 dev/ino/mount ID，复核调用线程 mount namespace 和原 procfs 根，临时 fd 结算实际首次关闭。

同一次观察从固定 procfs 读完可见的 `thread-self/mountinfo`，总量上限 1 MiB，单行上限 64 KiB。逐行解码路径原字节，以组件边界检查范围。所选行必须唯一，挂载根为 `/`，且挂载点覆盖原目录。拒绝同设备的其他 attachment、根内或根上的其他挂载，以及所选挂载点的堆叠。其他记录中的非 UTF-8 名称保留原字节。多个保留根可以共享同一个合法 ext4 挂载。

返回值只覆盖调用线程在本次采样中可见的挂载。chroot 外的挂载、其他 namespace、detached mount、旧进程 fd、设备下层访问、ID 映射和采样之间的变更，仍须由批准宿主、原 launcher 的管理面持续封锁及旧 UID/cgroup/FD 排除验证。普通 mount ID 仅在持有原根的观察期间使用。保留树的实际接线见下节。完整 Unix 采用、批准 Linux 交接和生产 Gate 继续 OPEN。[Linux mountinfo](https://man7.org/linux/man-pages/man5/proc_pid_mountinfo.5.html) · [Linux statx](https://man7.org/linux/man-pages/man2/statx.2.html) · [内核 mountinfo 实现](https://raw.githubusercontent.com/torvalds/linux/v6.17/fs/proc_namespace.c)

Darwin arm64 上原生库构建成功，301 项定向 JUnit 通过，3 项 Linux 条件用例跳过。独立 JVM 加载父 JNI 库，确认缺符号明确拒绝且原目录身份查询仍可用。宿主 Cargo 测试因既有 `-install_name` 链接参数失败，随后以原 crate 源码和 Cargo 已构建的锁定依赖直接运行 29 项 Rust 测试，含 6 项新解析器测试，全部通过。Linux x86_64 `cargo check --tests` 通过。实际 Linux JNI、bind mount 和批准宿主的阳性为 NOT_EXECUTED。证据位于 `/Users/liusinan/.codex/artifacts/unified-wal/classic-retained-mount-evidence-bbcb55b7`。

## Classic 保留树的 Linux 同挂载核验

2026-11-08，基线 `71ab9d2873`。Linux 冷开逐个获取签名 Classic、control 和 Arena 根的原 FileChannel，材料 owner 持有这些根直到排空。根的文件身份和挂载观察在全树枚举前后核对，原生来源重读和完成条件继续复核同一个原根 fd。导入 claim 和 replay 前各重扫实际保留树。每次原生查询沿用原准备 owner 的读操作预算，迟到结果拒绝返回。

原 owner 从实际枚举项生成规范相对路径，严格核对 UTF-8 字节。`linuxStorageMember` 在原根 fd 上以 `openat2`、`O_PATH` 和 `BENEATH | NO_XDEV | NO_SYMLINKS | NO_MAGICLINKS` 取证，比较设备、inode、普通 mount ID 及普通文件或目录类型，结算临时 fd 的实际首次关闭。关闭 `O_PATH` fd 不释放该 inode 上原有的进程 POSIX 锁。目录流在枚举前核对其实际 file key 与此前观察到的目录身份；原有 ACL、访问、目录深度和关闭检查继续参与验收。[Linux openat2](https://man7.org/linux/man-pages/man2/openat2.2.html) · [Linux 关闭实现](https://raw.githubusercontent.com/torvalds/linux/v6.17/fs/open.c) · [JDK SecureDirectoryStream](https://docs.oracle.com/en/java/javase/21/docs/api/java.base/java/nio/file/SecureDirectoryStream.html)

成员观察针对当前解析。后续真正打开业务文件仍由原 I/O owner 核对。批准宿主的持续管理面封锁、其他 namespace 和旧进程 fd 排除仍待独立验证。完整 Unix 采用和生产 Gate 保持 OPEN。

Darwin arm64 上原生库构建和 Java 样式通过，303 项定向 JUnit 通过，5 项 Linux 条件用例跳过。首轮实际验证发现 Darwin provider 返回普通目录流，最终分别断言普通流明确拒绝与安全目录流下的目录替换行为。独立 JVM 验证父 JNI 库缺少成员符号时明确拒绝，原目录和挂载调用保持可达。30 项宿主 Rust 测试及 Linux x86_64 `cargo check --tests` 通过。真实同 dev/ino 自 bind、Linux 下路径替换和迟到结果用例等待批准环境执行。证据位于 `/Users/liusinan/.codex/artifacts/unified-wal/classic-retained-member-evidence-71ab9d28`。

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

## 原始 JDK FileChannel 的只读 fd 身份观察

2026-11-05在父`81e43187399c7237d11b13a6dd7baf74a50d4fad`上，既有NativeIO/Impl/Jni/Rust新增fileIdentity(FileChannel)，只返回不可变raw dev/ino。默认实现与旧JNI缺符号明确拒绝，未知返回/布局及closed不变成零或absent；高位long/明确零值作为数据保留。JNI只支持exact bootstrap java.base FileChannelImpl及其固定FileDescriptor/fd布局，wrapper/null/替代实现拒绝，不递归unwrap，不接Path/raw fd/调用方结果，不用Java setAccessible/module-open。不修改SegmentOwnedFiles、Trust、Bootstrap、LoadedNames或diagnostics guard。

依据[参考JDK21 FileChannelImpl](https://raw.githubusercontent.com/openjdk/jdk21u/jdk-21.0.10-ga/src/java.base/share/classes/sun/nio/ch/FileChannelImpl.java)和[FileDescriptor同步关闭/设置](https://raw.githubusercontent.com/openjdk/jdk21u/jdk-21.0.10-ga/src/java.base/share/classes/java/io/FileDescriptor.java)，在准备固定class/field/method及错误类后，对同一真实descriptor显式MonitorEnter/Exit。锁内检查原channel final isOpen与descriptor.closed/fd，执行本平台libc::fstat，再检查open/closed/fd一致；所有正常/错误/pending异常均释放已取得monitor，取锁/退出失败不返回身份，errno先保存，结果数组在锁外分配。父流closeAll和FileChannel自身Closer均经过该descriptor monitor，不加parent-null限制，也不叠加channel closeLock或回调用户对象。JNI实参及局部引用保留原channel/descriptor；这一固定JDK生命周期的核对和host测试不认证其他vendor。

[glibc的fd file-id来源](https://raw.githubusercontent.com/bminor/glibc/glibc-2.39/sysdeps/posix/dl-fileid.h)与本原语均直接fstat原fd，仅保留dev/ino原位模式；不dup、按路径重开/stat、改position/权限、关闭/转交原fd或重试未知失败。支持Linux/macOS对应libc ABI，其余明确拒绝。原语可阻塞，没有deadline线程或取消伪造终态，返回也不保证之后channel仍开着。关闭已开始或观察中逻辑关闭时拒绝；未来owner须在原deadline/inFlight/首次关闭合同内使用，不能把本结果当close/drain。

host release构建和Linux x86_64 cargo check均退出0；后者未链接或执行，不计Linux阳性。新库353664字节、SHA-256 `70bb629fc8afedddb757729f6b79d35fec28b596406f8b6cd4a0d99e815513ee`，实际运行环境Mac OS X aarch64、Amazon JDK21.0.10+7-LTS。新4项全部通过、0跳过：真实FileChannel与当时path dev/ino对照，rename并替换pathname后仍取原id且区别新pathname、position/read/open保持；null/closed/真实wrapper拒绝；公开FileInputStream.getFD monitor内令查询实际BLOCKED，再开始close并观察逻辑关闭，释放后查询拒绝且实际close结束；另检验default拒绝与DTO原位模式。fixture不把失败/未终结线程伪称drained，无反射或生产回调。

相关native八类42项为40通过/2平台跳过，0失败/错误；跳过为Linux proc magic-link及Linux peer close/reallocation。加新4共46个当前unique用例，44通过/2跳过。独立-Xcheck:jni重复同新4项通过、无JNI warning，不再累计4个unique；另一独立JVM载原sealed旧库75e59684，实际凭据查询仍成功，新符号带LinkageError cause拒绝、原channel保持open。未改server纯算法/owner矩阵不重跑；本片未执行models/性能。首style因新wrapper15个单行布局退出1，仅修测试格式后final Spotless/Checkstyle退出0，原失败快照保留；只格式化新增Rust片段，不宣称全Rust样式通过。

证据位于工作区外`/Users/liusinan/.codex/artifacts/unified-wal/native-channel-file-id-evidence-81e43187`，两Maven阶段各1444输入与原snapshot/父Git blob无错配，当前五code与两阶段/final style一致；两native构建各4个原始Rust输入、独立probe原class/library/source绑定核验。保留host库、原source/class/日志/XML及8份参考（3在线源码＋5本机src.zip条目），静态参考不冒充批准installed vendor。两个确切raw scope双lsof/identity/SHA/archive核验清理4目录/15文件路径/591023逻辑字节，归档保留/regular-file数据块回收0；两个自有Cargo target双lsof/identity/SHA核验清理447文件路径/91357886逻辑字节，库/日志/源码保留，物理回收未测，共享target/cache/toolchain不清理。

实际owner来源边仍OPEN：SegmentOwnedFiles.open返回私有Channel包装，raw FileChannel只在Entry内部；本片没有public unwrap或把wrapper误作bootstrap channel，也未把fd身份绑定同一次hash/ELF读。实际namespace/loader/main/l_loader及BFS/祖先/DST来源、完整递归helper/backend exec、typed Linux/vendor/Unix/parent/child/Main/G1/full-bytes/full16/生产Gate仍未验收。原DT_NEEDED在dl-deps中先DST展开再传lookup name的区别保留为后续组合边界。本片不撤unconditional Unsupported，整体目标active，默认OFF/SYSTEM/PARK及advanced disabled保持。

## 私有 Entry 与同次 ELF2 读取的 fd 身份绑定

2026-11-05在父`c5cfb872ceddfc2d05483c9e253a5d454e942f80`上，SegmentOwnedFiles增加包内fileIdentity桥，仅接受本owner发行的原private Channel与对应Entry；实际查询对象始终是Entry内原resource。查询前后核对wrapper仍open、owner可用、同一resource及无close/failure，不公开raw channel/fd/unwrap或接受调用方身份结果。同步Entry monitor覆盖整个NativeIO调用体，与实际首次close互斥；JNI期间不持全局gate、不算新open。调用线程在finally清除，查询重入及自drain拒绝；外部drain可以设置closing并观察retained状态，但须等查询返回才能实际close/退休，关闭已开始的late结果拒绝。原首次关闭失败及unknown保留、不重试语义保持。

Trust私有hashFile仅在ELF2分支，于同一owned channel的SHA/解析前取实际fd dev/ino并对应本次原路径Identity，读取后及首次close前再次查询且须相同。原完整路径身份、父链、权限/ACL、长度、SHA、metadata、probe、首次关闭后检查和deadline不删减；失败保留在原launcherFailure。NONE/program、ELF1、CACHE/LDCONFIG、NativeIOBootstrap及纯static readLinuxContent没有增加fd来源许可。现有ELF2 observation的identity由这条私有读取路径获得对应，不新增permit/ready或观察注入入口。

新增OwnedFiles两项与Trust三项全部通过；最终OwnedFiles7、Trust115、Content8共130项全部通过，0失败/错误/跳过，包含同新5项。真实未包装owner阳性对照path dev/ino并保持position/read/open；foreign/raw/null/closed在operator前拒绝。真实JNI查询体挂起时自drain/重入拒绝，外部drain实际BLOCKED且无提前close/drained，释放后late结果拒绝、原fd实际关闭。另以同字节不同inode的真实fd替代声明文件open，content barrier确认在任何SHA/parser前拒绝并实际close；post-content查询原异常在实际首次close后仍保留。旧ELF2 FaultChannel故障测试只在隔离helper内取已知actual后调用真实JNI，保留全部旧故障断言；这类注入不计为未包装原始来源阳性，legacy路径不使用适配，生产不unwrap或跳过查询。

本次精确复用已封存353664字节host库、SHA-256 `70bb629fc8afedddb757729f6b79d35fec28b596406f8b6cd4a0d99e815513ee`，运行环境Mac OS X aarch64、Amazon JDK21.0.10+7-LTS；未重建native或重跑未改的native/纯加载算法矩阵。首次style一处StandardCopyOption/Open import次序失败保留，修正后Spotless/Checkstyle退出0。证据`/Users/liusinan/.codex/artifacts/unified-wal/classic-linux-owned-fd-evidence-c5cfb872`分开保留两测试阶段原source/class/日志/XML及style失败、原库checkpoint引用；各1444输入按原快照或父Git blob核对无错配，最终四code与测试和final style一致。两个确切raw scope经两次独立lsof、inode/mode/SHA/archive hardlink核验后清理4目录、11文件路径、890456逻辑字节，归档保留、regular-file数据块回收0，共享target及用户文件未清理。

本片只加强这次held fd与原路径/内容的对应；不证明查询后永久不变、先前名称未命中或真实已加载对象/namespace/l_loader历史。DST lookup-name展开、BFS/祖先链、完整递归helper/backend执行与批准Linux/vendor/Unix资格仍OPEN/NOT_EXECUTED；diagnostics unconditional Unsupported、parent/child/Main/G1/full-bytes/full16/生产Gate不提升，未执行models/性能。完整16步目标active，默认OFF/SYSTEM/PARK与advanced disabled不变。

## 普通 NEEDED 的名称与条件路径准备

2026-11-05在父`c373f783c0aac05055942e7f2937c88d4c57f822`上，包内SegmentClassicLinuxNeededName实现有界纯数据两阶段。输入借用LinuxNeededContextObservation，只作为给定的原requester context、NEEDED ordinal/raw及NeededRule声明；返回保留同一原declaration和候选批准键，不用展开名称替换它。显式ORIGIN/LIB/PLATFORM Value与secure状态也只是给定事实，不证明原ELF、实际环境/Unix资格或来源；无文件/Process/JNI、typed owner放行、Installed/ready入口，owned fd路径不变。

依据[glibc2.39 dl-deps.c的expand_dst及普通NEEDED分支](https://raw.githubusercontent.com/bminor/glibc/glibc-2.39/elf/dl-deps.c)，第一阶段仅遇到已识别DST才按SECURE拒绝、secure未观察停止；无已识别DST时未知secure不阻止字面名称。复用Tokens同一private match新增bounded hasKnownToken，完整strict UTF8/NUL/4096检查保持，原expand单次不递归、不识别语法原文保留、unused UNOBSERVED不阻止及first-blocked规则不变。已知UNAVAILABLE或替换最终空得到SKIPPED_EMPTY，不调用_map_object；实际用到的来源UNOBSERVED不给lookup name或前缀；非空LOOKUP_NAME供名称复用。raw本身须非空，slash/dot/$/colon/semicolon/Unicode不按目录列表处理，不规范化或补cwd。

依据[同版dl-load.c的名称查找与slash分支](https://raw.githubusercontent.com/bminor/glibc/glibc-2.39/elf/dl-load.c)，独立afterNameMiss须有显式NO_REUSE形状，caller仍负责建立它与本次first name的对应和先后，纯结果不是name-miss证明。无slash则DIRECTORY_SEARCH并保留first name，不再次替换；含slash才第二次调用原Tokens单次替换。第一次替换值带入的新token可在这次调用被识别，但不能提前越过name reuse；第二阶段新增未知来源不抹掉第一阶段已可复用的名称。第二次UNAVAILABLE产生空DIRECT_PATH文字，表示交给open_verify会失败的空文字，不是第一阶段skip、安全缺失或CWD；全部AVAILABLE时原literal slash必保留，available空replacement不会使slash输入整体为空。不实现一般secure-ORIGIN可信目录例外，识别DST的受支持第二阶段须NON_SECURE。结果同时保存原raw、first lookup和conditional direct text；file-id复用记名仍用first lookup，不用第二次realname。

新8项首轮为1failure/2errors：两处ObjectData空names与一处NeededRule空candidates先触发已有约束；仅修本片fixture及保留原别名的追加断言，生产未变。修正后新8全部通过；最终NeededName8、Tokens7、DynamicPath8共23项全部通过、0失败/错误/跳过，包含新8而非额外累计。覆盖原ordinal/raw独立、已知空/不可用与未观察、secure识别边界、字面/Unicode、两次单次替换的顺序、name hit不依赖后续未知事实、无slash不二次替换、first name记名及原UTF8/展开界/deadline。原始和修正后Spotless/Checkstyle均退出0；未重跑未改owner/JNI/纯cache/diagnostics/LoadedNames整套，原生库仅保留runner配置属性、不计JNI执行。

证据`/Users/liusinan/.codex/artifacts/unified-wal/classic-linux-needed-name-evidence-c373f783`分别保留失败/修正最小/最终相关三个阶段原source/class/日志/XML和两次style；各1446输入按原snapshot/父Git blob绑定无错配，最终三code与修正最小/最终相关/final style一致。原静态dl-deps.c（17592字节，SHA-256 `47a1cb0f16d2073e2992af1a17c5aa269ffdd08b606a5a7e627bbfe6988d8fb1`）及dl-load.c（70348字节，SHA-256 `26345f234ef918c866e40bb86524bdfd4c3beac487af81b3f51668379e671ae0`）单列保留，不计批准vendor运行。三个确切raw scope经两次独立lsof/identity/SHA/archive hardlink核验清理6目录、14文件路径、1196136逻辑字节，归档保留、regular-file数据块回收0，共享target/用户文件不清理。

实际token/secure及原ELF关联、namespace/main/loader/l_loader、BFS/祖先/RPATH环境、完整递归helper/backend执行和批准Linux/vendor/Unix资格仍OPEN/NOT_EXECUTED，普通base/cache/default/四位置owner入口范围没有扩大。diagnostics unconditional Unsupported、parent/child/Main/G1/full-bytes/full16/生产Gate不提升，未执行models/性能；完整目标active，默认OFF/SYSTEM/PARK及advanced disabled保持。

## 同一原请求的首名查找与候选批准

2026-11-05在父`1b4ae87842d89a27f5f0f40c3c6f1558021ac3b5`上，原SegmentClassicLinuxNeededName新增decideName，接原LinuxNeededContextObservation、稳定给定的base namespace原序表、同组ORIGIN/LIB/PLATFORM Value、secure及deadline；不接另造Lookup、外部Reuse/miss、candidate index或替换查找回调。保留同一原request/declaration/requester/ordinal/raw与首次展开名，复用prepare、LoadedNames.lookup及afterNameMiss，未新增匹配算法、状态会话、缓存或全表副本。

首次SKIPPED_EMPTY/UNOBSERVED在名称查找前停止，无Reuse/AfterMiss且不访问namespace；可用首名必须有显式表，null不当空，已知空表才能由算法产生miss。原序全表仅查一次，不按批准集合预滤/排序/合并。REUSED_NAME/REUSED_SONAME核对真正首个命中replacement.context是否属于本NeededRule.candidates，未批准立即拒绝，不继续后面的批准对象，也不转为文件搜索。批准命中保留原ordinal/opaque identity/context及既有不可变记名结果，不执行第二次DST。只有本次NO_REUSE进入旧afterNameMiss，使用同一first/Value/deadline；目录仍保留first.name，slash二次展开的UNAVAILABLE空direct文本和UNOBSERVED保持可辨。NeededRule已有非空/非负/有界升序列表保证，组合仅检查最大candidate小于4096，不补认完整D2表或真实namespace来源。

新5项与最终NeededName13、LoadedNames10、Tokens7、SearchStages11共41项全部通过，0失败/错误/跳过，新5包含其中；Spotless/Checkstyle退出0。关键反例为早期未批准SONAME、晚期已批准primary同名，必须拒绝；同raw不同ordinal可批准不同context。批准命中的context刻意不同于namespace ordinal，检查SONAME记名/输入不变，首次名还含第二层DST且第二层来源未知时仍能直接命中。显式空表、缺表、首次停止不读取表、二次available/unavailable/unobserved、candidate4095/4096及坏后项不能被早命中遮蔽均覆盖。SearchStages原request fixture现从该组合的空表miss取得AfterMiss，原条件函数和既有词法/容量矩阵未重写。

证据`/Users/liusinan/.codex/artifacts/unified-wal/classic-linux-name-decision-evidence-1b4ae878`保留两阶段原source/class/log/XML与style；各1448输入按原snapshot/父Git blob核验无错配，3个改动文件与两测试/style一致。glibc2.39 dl-deps.c与dl-load.c沿用原静态字节及SHA，分别17592/70348字节；名称查找顺序是glibc规则，命中后的候选批准是本项目D2限制。两个raw scope经双lsof/identity/SHA/archive hardlink核验清理4目录、11文件路径、871083逻辑字节，归档保留、regular-file数据块回收0，共享target及用户文件未清理。

本片是纯给定数据计算，无file/JNI/Process调用；精确原JNI库仅为runner配置，不计调用或旧PASS重绑，未重跑owner/Elf/native/部署矩阵或models/性能。命中发生在audit前，miss后的分支以audit未改写为条件；真实初始namespace/运行时顺序、secure/token/audit及helper/backend来源仍OPEN，Linux/vendor/Unix阳性NOT_EXECUTED。diagnostics unconditional Unsupported和parent/child/Main/G1/full-bytes/full16/生产Gate不提升，完整目标active，默认OFF/SYSTEM/PARK与advanced disabled保持。

## 新对象 realname 的词法 ORIGIN

2026-11-05在父`251f1433f7f010cf91d99ac9e8aa89ffc8d1560e`上，包内SegmentClassicLinuxOrigin.compute仅接原realname、显式CWD Value与原deadline，返回保存同一原realname/CWD及四种状态的不可变数据。依据[glibc2.39 _dl_new_object](https://raw.githubusercontent.com/bminor/glibc/glibc-2.39/elf/dl-object.c)，输入是创建对象的realname，不是随后可能变为空串的l_name，也不是D2 physical文件的parent。没有实际对象创建、getcwd、System.user.dir、环境、/proc、Path/filesystem、JNI/Process或typed owner接线。

Tokens仅开放原strict UTF8长度校验的包内可见性，算法与原caller不变；本片不识别或展开realname的$。raw及可用CWD先核对无NUL/strict UTF8/4096界，可用CWD须非空绝对文字。非空绝对realname不依赖CWD是否观察/可用；相对名称只有CWD AVAILABLE才按末尾已有slash与否拼接，对完整joined先检查4096界，再只移除最后slash及后缀，首slash保留。dot、重复slash、尾slash、colon/semicolon/Unicode均按原文处理：`/a/../b/lib.so`→`/a/../b`、`/a//lib.so`及`/a//`→`/a/`、`/lib.so`→`/`；没有normalize或DynamicPath.dirname的尾slash整理。即使最终origin很短，也不能绕过完整拼接界。

relative＋CWD UNAVAILABLE为已知origin不可用，UNOBSERVED不给出prefix；empty realname单列DEFERRED_GET_ORIGIN，不能由CWD/physical/helper路径补全，也没有自动toTokenValue转换。[Linux后续_get_origin来源](https://raw.githubusercontent.com/bminor/glibc/glibc-2.39/sysdeps/unix/sysv/linux/dl-origin.c)须另取/proc/self/exe或LD_ORIGIN_PATH，本片未实施或假定这些事实。给定realname词法计算不证明新对象实际创建；复用已有对象须保留原origin，不能按新发现路径重算，该组合仍待实现。

新Origin6项全部通过；最终Origin6、Tokens7、DynamicPath8、NeededName8共29项全部通过，0失败/错误/跳过，包含同新6而非再累计。覆盖绝对输入不被未知CWD阻止、根/内部dot/重复与尾slash/字面$和Unicode、相对拼接与状态、empty deferred、strict输入与完整拼接4096/deadline界；一个NeededName小组合明确logical alias realname计算结果与physical parent不同。Spotless/Checkstyle退出0。未重跑未改owner/JNI/Cache/diagnostics/LoadedNames整套，原生库仅保留runner配置属性、不计JNI执行，无models/性能。

证据`/Users/liusinan/.codex/artifacts/unified-wal/classic-linux-origin-evidence-251f1433`保留最小/最终两个阶段原source/class/日志/XML与style；各1448输入按原snapshot/父Git blob绑定无错配，最终三code与两测试/style一致。两份静态原源分开保留：dl-object.c为7929字节、SHA-256 `ebe8d023c06d2c36fdb0fd03b61ce06134ab14d044bac17f569ac18c6c968126`；Linux dl-origin.c为2251字节、SHA-256 `36723ef4224cde0739f1f522ecb16a7c4506fc263ed150b84448c5c5269cec30`，不计批准vendor运行。两个确切raw scope经两次独立lsof/identity/SHA/archive hardlink核验清理4目录、11文件路径、868533逻辑字节，归档保留、regular-file数据块回收0，共享target及用户文件不清理。

实际realname/CWD/main-origin与原owned ELF关联、namespace/main/loader/l_loader/BFS/祖先/environment、完整递归helper/backend执行和批准Linux/vendor/Unix资格仍OPEN/NOT_EXECUTED。diagnostics unconditional Unsupported、parent/child/Main/G1/full-bytes/full16/生产Gate不提升，完整目标active，默认OFF/SYSTEM/PARK与advanced disabled不变。

## 有效 library-path 列表的非 secure 数据展开

2026-11-05在父`387cde4cf023f973dc263af9ab8c83630203a3cc`上，原DynamicPath增加包内expandLibraryPath，输入仅为给定非secure条件下的effective library-path Value、初始化对象origin及LIB/PLATFORM Value、原deadline；LibraryPath结果保留原effective Value与完整/未观察状态、不可变有序Directory列表。UNAVAILABLE明确表示已知llp==NULL，AVAILABLE空表示已知整串空，两者均完整空目录且原输入可区分；UNOBSERVED没有usable prefix，Java null拒绝。所有可用raw/replacement先检查strict UTF8/无NUL/4096界，不从环境/CLI/secure或当前Java进程猜来源。

依据[glibc2.39 _dl_init_paths/fillin_rpath](https://raw.githubusercontent.com/bminor/glibc/glibc-2.39/elf/dl-load.c)，非空有效llp先按colon或semicolon分段，首/中/尾原空段是CWD，整串空不是CWD；引号/反斜线不escape分隔符。非空段才进行原Tokens单次展开，替换值中的colon/semicolon/$不重分段或递归；已知不可用/展开空跳过，不变CWD，实际用到UNOBSERVED则整份列表不发布此前prefix，沿原first-blocked规则。初始化origin须来自base main、缺main时的rtld stand-in，参数有值不证明这一关系，也不能替换成后来的NEEDED requester或physical parent。

只窄提取private parse loop的两个固定调用：原DT入口仅colon，新library-path入口colon＋semicolon；无公开separator/策略/额外budget。原DynamicPath presence/empty与仅colon语义保持，absent原本只能携空raw由原构造器保证。目录末slash整理、根、相对/内部dot/slash/Unicode、精确dirname首次去重及最终含slash4096界共用原机制，CWD/相对目录仅是数据，不补实际cwd。SearchStages/owner范围、文件批准与整体资格门没有变化。

新library6项全部通过；最终DynamicPath14（新6＋原DT8）及Tokens7共21项全部通过，0失败/错误/跳过，包含同新6而非再累计。覆盖NULL/whole-empty/未观察、两分隔符及CWD次序、quote/backslash、先split后替换、blocked不变CWD/无prefix、原目录/字节/slash/deadline与旧DT semicolon literal；一个Origin小组合保留初始化原文中的两种分隔符。Spotless/Checkstyle退出0；没有真实环境探针或owner/JNI/Cache/diagnostics/LoadedNames整套重跑，原生库只是runner配置属性、不计JNI执行，无models/性能。

证据`/Users/liusinan/.codex/artifacts/unified-wal/classic-linux-library-path-evidence-387cde4c`分别保留最小/最终两个阶段原source/class/日志/XML及style；各1448输入按原snapshot/父Git blob绑定无错配，最终两code与两测试/style一致。同版dl-load.c原静态参考70348字节、SHA-256 `26345f234ef918c866e40bb86524bdfd4c3beac487af81b3f51668379e671ae0`单列保留，不计批准vendor运行。两个确切raw scope经两次独立lsof/identity/SHA/archive hardlink核验清理4目录、9文件路径、778702逻辑字节，归档保留、regular-file数据块回收0，共享target/用户文件不清理。

实际parent环境、nonsecure、effective CLI/env选择、初始化main/rtld身份与origin来源仍OPEN，不能从给定Value或纯结果补认。完整namespace/BFS/祖先与递归helper/backend、批准Linux/vendor/Unix及parent/child/Main/G1/full-bytes/full16/生产Gate仍未验收；diagnostics unconditional Unsupported与完整目标active保持，默认OFF/SYSTEM/PARK和advanced disabled不变。

## Cache 之前的条件目录来源计划

2026-11-05在父`c459d0910c13dd16f37f1f2aac0b16bfb17175b1`上，原SearchStages新增包内plan，输入保留原NeededName.AfterMiss及原requester/ordinal/raw批准键；仅接DIRECTORY_SEARCH、非空无slash的first lookup name，链首context须匹配原requester。显式requester→ancestors链与origin map在复制前及遍历中受4096/deadline限制，所有提供Value非null、AVAILABLE文字按原strict UTF8/4096校验；selected source必须有明确origin项，未知可显式给定，未选祖先不要求提供。这些校验不证明实际name miss、l_loader历史或对象来源。

复用原SearchStages.select，不另写优先级；每个selected Source按opaque owner object identity取自己的origin，同context不同object不会合并。输出DirectoryPlan固定为RPATH blocks→一个LibraryPath→RUNPATH blocks，TagBlock保留原Source的owner/context/tag/raw declaration、给定origin及不可变目录。library-path继续使用独立初始化origin，不从requester/main origin表猜出它。原tag presence/empty、有效llp的NULL/whole-empty可辨认；每个来源内部沿原DynamicPath精确去重，跨来源即使dirname或physical相同也不合并。依据仍是[glibc2.39 dl-load.c](https://raw.githubusercontent.com/bminor/glibc/glibc-2.39/elf/dl-load.c)的pre-cache次序、tag拥有者展开及独立路径初始化。

任一必要展开UNOBSERVED时，整份计划显式UNOBSERVED，两个tag block列表为空、library=null，只保留原请求及声明头，没有可消费的目录prefix。unused UNOBSERVED、UNAVAILABLE token的省略和present-empty规则沿原helper，不新增secure判断。AVAILABLE仅表示在给定nonsecure有效library-path、链、各origin等前提下数据齐备，不认证真实name miss或实际来源，也不是尝试日志/winner。相对目录与CWD保留数据原文，不补absolute或改变realname/ORIGIN。

直接复用原LoadingDirectoryBudget跨三阶段累计，保留新block前逐dirname计费；固定262144个目录关联、64MiB累计UTF8 dirname字节，CWD计一项/0字节、跨来源重复照计，超限整次失败无prefix。这是本次计划的关联/字节界，不是总heap保证，没有新可调预算或通用计费框架。cache/default、CPU/hwcap筛选、文件lookup与实际namespace/BFS/l_loader采集均不在本片，原owner目录/文件批准不被纯plan替代。

新5项全部通过；最终SearchStages11（新5＋原选择器6）、DynamicPath14、Tokens7共32项全部通过，0失败/错误/跳过，包含同新5而非再累计。覆盖child→ancestor→main各自ORIGIN、同context不同object、独立初始化origin、RUNPATH包括空值的抑制/位置、跨source重复、unknown无whole-plan prefix、未用unknown、原requester/显式origin错误与deadline。容量反例构造4000来源×（65目录＋CWD）=264000关联；忽略CWD只260000或全局去重均不会触发上限，测试必须取得原容量异常而非deadline。首次style因六处测试长行退出1，原失败保留；仅换行后final Spotless/Checkstyle退出0。

证据`/Users/liusinan/.codex/artifacts/unified-wal/classic-linux-directory-plan-evidence-c459d091`分开保留最小/最终两阶段原source/class/日志/XML及首style失败；各1448输入按原snapshot/父Git blob绑定无错配，最终两code与两测试/final style一致。同版dl-load.c原70348字节、SHA-256 `26345f234ef918c866e40bb86524bdfd4c3beac487af81b3f51668379e671ae0`单列为静态依据，不计vendor运行。两个确切raw scope经两次独立lsof/identity/SHA/archive hardlink核验清理4目录、10文件路径、825207逻辑字节，归档保留、regular-file数据块回收0，共享target与用户文件不清理。

未扩大owner/JNI/Cache等矩阵，原生库仅为runner配置属性、不计调用；本片无models/性能。实际环境/CLI/nonsecure、main/rtld与tag对象/chain/origin来源、完整loader及递归helper/backend和批准Linux/vendor/Unix资格仍OPEN/NOT_EXECUTED。diagnostics unconditional Unsupported、parent/child/Main/G1/full-bytes/full16/生产Gate不提升，完整目标active，默认OFF/SYSTEM/PARK及advanced disabled保持。

## 无已知 DST 的绝对 NEEDED 关联观察

2026-11-05在父`e2d6082c68dfdda715ce486f179b34c908a54a0d`上，原Trust新增包内verifyLinuxNeededDirect，仅收原rule index与deadline，沿原source/provider/原deadline门后才接触目标；独立readLinuxNeededDirect数据fixture共用private主体，不接调用方Path、替换名、candidate、AfterMiss或已有观察，不产生Installed/ready。复用fixedLinuxContextMembers取原NeededRule→requester Context→原ELF metadata→NEEDED ordinal，复用Tokens.hasKnownToken的strict UTF8/无NUL/4096及同一matcher；仅支持非空首字符`/`且无已知DST。相对slash、无slash与known DST在requester/目标I/O前Unsupported，unknown/不完整/标识符边界不匹配的`$`按字面保留，不一概拒绝美元符号。

主体owned读取requester一次，核对实际ELF2同ordinal原名；原字符串作为logical/批准键保留dot、重复slash、alias、colon/semicolon、Unicode和unknown `$`，路径解析沿原loadingNativePath/LOOKUP observer，未改其语义。present候选只通过原observeLinuxNeededCandidate按本rule.candidates中的exact logical＋resolved physical context打开，同physical的其它logical批准不能借用；同次owned SHA/parser、真实fd前后身份、完整path/alias/parent/权限及首次close合同保持。结果返回前再次观察原logical path且须与初probe完全相等，再健康/deadline检查；返回保留rule/declaration/name、原requester及候选context/member。

可靠protected NoSuchFile才可candidate=null；拒绝、非目录组件、目录目标、坏ELF或未声明alias不当缺失。该观察以「原名未被已加载对象复用、未被audit改写而进入direct分支」为条件；这些前提、实际open_verify/mapping/winner均未证明，不伪造NO_REUSE，也不更新namespace。没有目录/cache/default/hwcap搜索，不扩大相对或known-DST owner范围。[glibc2.39 dl-deps.c](https://raw.githubusercontent.com/bminor/glibc/glibc-2.39/elf/dl-deps.c)与[dl-load.c](https://raw.githubusercontent.com/bminor/glibc/glibc-2.39/elf/dl-load.c)仅作静态规则依据。

新direct6项全部通过；最终Trust121、Tokens7、Elf13共141项全部通过，0失败/错误/跳过，包含同新6而非再累计。共享ElfTest.dynamicImage只开放测试包可见性、body不变，以真实可变长字符串表承载绝对路径，原9字节lookup夹具不放宽。新阳性及两替换反例均用普通owner、真实FileChannel与JNI，不依赖FaultChannel适配；验证unknown `$ORIGIN_suffix`与真实known DST的区分、原alias/dot/重复slash/Unicode原名、可靠缺失/错误logical批准前拒绝open，以及拒绝/目录/未声明alias/非目录组件、index/source/deadline先拒绝。same-byte不同inode反例实际读取并关闭新fd后须落在probe→owned绑定拒绝；terminal反例在实际fd已关后返回最后一次真实旧stat再换同target alias，须取得direct末端复查专属异常，不能冒用早期共享检查覆盖。

Spotless/Checkstyle退出0。精确复用已封存353664字节JNI库，SHA-256 `70bb629fc8afedddb757729f6b79d35fec28b596406f8b6cd4a0d99e815513ee`，新路径实际在Mac OS X aarch64、Amazon JDK21.0.10+7-LTS执行文件/NativeIO观察；旧故障测试的隔离适配不计为未包装来源阳性。未重建native或重跑未改OwnedFiles/Content/Cache矩阵，没有执行或装载该合成ELF，不计批准Linux/vendor typed阳性，无models/性能。

证据`/Users/liusinan/.codex/artifacts/unified-wal/classic-linux-direct-needed-evidence-e2d6082c`保留两测试阶段原source/class/日志/XML及style，精确原JNI checkpoint另引用而不重绑旧PASS；各1448输入按原snapshot/父Git blob核验无错配，最终三code与两测试/style一致。静态原dl-load.c（70348字节，SHA-256 `26345f234ef918c866e40bb86524bdfd4c3beac487af81b3f51668379e671ae0`）与dl-deps.c（17592字节，SHA-256 `47a1cb0f16d2073e2992af1a17c5aa269ffdd08b606a5a7e627bbfe6988d8fb1`）分别保留。两个确切raw scope双lsof/identity/SHA/archive hardlink核验后清理4目录、10文件路径、846893逻辑字节，归档保留、regular-file数据块回收0，共享target/用户文件不清理。

本片不把DirectoryPlan或fixture参数提升为来源资格；真实name reuse/audit历史、环境/ORIGIN/chain、namespace/BFS、完整递归helper/backend与实际执行仍OPEN。diagnostics unconditional Unsupported、parent/child/Main/G1/full-bytes/full16/生产Gate不提升，完整目标active，默认OFF/SYSTEM/PARK与advanced disabled不变。

## 本请求者标签与原声明目录的条件观察

2026-11-05在父`f34c9fae942092aa5ec42400c81f496d11075d49`上，原Trust新增包内verifyLinuxNeededRequesterTag，仅收原rule index与deadline；typed入口先验证原source/provider/deadline，独立原members＋Loading fixture共用private主体，不接caller Path、DirectoryPlan、chain/main、替换值或既有观察。仅支持现有普通目录NEEDED范围：非空、无slash/dollar且非点或双点。原requester的RUNPATH只要present就优先，包括present-empty；否则取present RPATH，双absent明确NONE。保留所选原tag种类及presence/value，空标签与双absent可区分，被压下的RPATH不要求token来源，NODEFLIB不抑制本对象标签。

所选tag复用DynamicPath.expand，ORIGIN/LIB/PLATFORM均显式UNOBSERVED；只消费完整AVAILABLE的绝对DIRECTORY结果。已知DST所需来源未知、CWD空段、相对项或后项无法配对均整次拒绝，unknown dollar及分号仍为字面。展开dirname按原尾slash键唯一对应loading.directories，保留tag顺序和同来源首次去重；缺失或多个原声明折成同键都拒绝，不按physical/normalize配对。无尾slash的4096字节无关声明不能匹配任何支持的dirname，不重新解释它。所有四位置拼接及4096字节边界在requester/搜索目录/候选I/O前完整预检，原default raw-exact合同保持。

随后实际owned读取requester一次，核对原NEEDED ordinal及两tag的presence/value，包括空标签和NONE。匹配目录复用原presence/protection与未过滤v4/v3/v2/base四位置观察，candidate沿原rule的exact logical＋resolved physical批准及同次ELF2/SHA/fd/首次close合同；所有目录共享该requester。读取完全部目录后再复查全部目录/位置，并复查原requester logical的alias摘要、叶identity与本次owned观察相等，最后检查健康和期限。私有directory/logicals record复用于默认目录计划，未改变默认算法或新增预算。

首轮新5项通过；只读评审指出目录表排序的固定下标断言依赖临时根位置，改为从返回原index回查path并断言tag原序及相反D2索引序。首轮test/style保留原snapshot，不将其PASS移绑修正源。最终Trust126、DynamicPath14、Tokens7共147项全部通过，0失败/错误/跳过，包含同新5；最终Spotless/Checkstyle退出0。覆盖tag顺序/去重、RUNPATH空值优先、NONE、NODEFLIB、晚项语法/映射/拼接拒绝前无目标I/O、typed source/index/deadline，以及读取第二目录期间替换原requester alias后取得新末端复查专属异常。新文件阳性及竞争用普通owner、真实FileChannel和原JNI；未改Elf构造器或重复其整类矩阵。

证据`/Users/liusinan/.codex/artifacts/unified-wal/classic-linux-requester-tag-evidence-f34c9fae`保留各阶段原source/class/log/XML，两测试阶段各1448输入按原snapshot/父Git blob核验，最终两code与final test/style一致。精确复用sealed JNI SHA-256 `70bb629fc8afedddb757729f6b79d35fec28b596406f8b6cd4a0d99e815513ee`，未重建native或移绑旧矩阵；实际运行仍为Mac OS X aarch64/Amazon JDK21。glibc2.39 dl-load.c原70348字节、SHA-256 `26345f234ef918c866e40bb86524bdfd4c3beac487af81b3f51668379e671ae0`仅作静态规则依据。两raw scope经双lsof/identity/SHA/archive hardlink核验清理4目录、10文件路径、848304逻辑字节，归档保留、regular-file数据块回收0，共享target和用户文件未清理。

该观察以原名未被复用或audit改写、已到达本对象未受inhibit-rpath抑制的阶段为条件；这些前提、实际CPU位置、open_verify/mapping/winner未证明。没有父RPATH/library-path/cache/default搜索或实际ELF执行；Linux/vendor/Unix typed阳性、完整来源/namespace/helper/backend仍OPEN/NOT_EXECUTED。diagnostics unconditional Unsupported及parent/child/Main/G1/full-bytes/full16/生产Gate保持，完整目标active，默认OFF/SYSTEM/PARK与advanced disabled不变。

## 已给定文件身份的已加载对象复用

2026-11-05在父`fcd63e2a1c6f1b1ebf2426e272e9e801cc4ebe1b`上，原LoadedNames新增FileId(device,inode)、FileObject(原ObjectData,fileId)及纯lookupFile。输入是明确原序的base-namespace对象/已给定file-id、本次已给定opened id、原map-from-fd request name和deadline；只描述普通非OPENEXEC分支的局部规则，无fd参数/操作或owner入口，不证明前面名称查找已经未命中。query继续非空strictUTF8/无NUL/≤4096，slash/$原文保留；没有mode策略、跨namespace镜像、NOLOAD或文件映射器。

[glibc2.39 _dl_map_object_from_fd](https://raw.githubusercontent.com/bminor/glibc/glibc-2.39/elf/dl-load.c)约889–935在普通分支从原fd取file id，再按namespace原序找第一个非removed匹配对象；[POSIX file-id比较](https://raw.githubusercontent.com/bminor/glibc/glibc-2.39/sysdeps/posix/dl-fileid.h)只比较设备号与inode。本算法因此仅按两个raw64位值精确相等，不比较path/hash/mtime/ctime/context/object identity；负long保留高位模式，明确(0,0)与null未观察不同。所有对象id及opened id缺失均拒绝，不补零；原路径Identity不是fd fstat证明。OPENEXEC在原代码中跳过该比较块，本方法不适用，不用人工零id模拟。

此处仅跳过removed，不额外沿用名称分支的faked排除；不同object identity的同file-id可保留，原顺序决定首个对象。命中复用原identity/context/l_name/SONAME及标志，只检查原l_libname是否包含本次原query：已包含不追加，否则在尾部追加query。即使query等于l_name或SONAME，也不能据此省掉名单检查；不追加resolved path，不改变sonameAdded。返回REUSED_FILE_ID、原ordinal及不可变名称替换对象，供后次纯查找接续，原输入不改；无匹配仍NO_REUSE，不表示新对象已创建、文件不存在或搜索结束。

复用原完整前置校验、4096对象/context、12288名称关联、单串界与deadline，仅提取共用private begin/validate/hasAlias/remember。file wrapper/null id及坏后项不能被早命中遮蔽，只有确实追加才占新关联；已有alias在满额时可复用，新alias满额拒绝。原name lookup仍逐对象direct再SONAME，direct不改flag、仅SONAME分支置flag，缺失/空SONAME和skip规则保持。纯结果不执行glibc匹配后close-fd再记名的资源动作，不能冒充close/drain；实际fd来源和首次关闭继续由原owner负责，未新增缓存/线程/通用budget或事件日志。

当前LoadedNames整类10项全部通过，0失败/错误/跳过，包含新增4项与原6项，不累计为14 unique；Spotless/Checkstyle首轮退出0，规划只读评审无新增阻塞。新增覆盖不同path/name同file-id复用并保留原identity/context、两字段任一不同不匹配及raw负/零值、removed/faked差异及多个同id原序、仅主名/SONAME相等仍追加query且flag不变、接续name lookup、输入与结果隔离、缺id/坏后项、满额已有alias成功而新增失败及deadline；原6检验共享私有提取。1443项输入与原snapshot/父Git blob无错配，两当前code与test/style一致；未改owner/Cache/Diagnostics/SearchStages/JNI矩阵未重跑，旧PASS不移绑。

证据位于工作区外`/Users/liusinan/.codex/artifacts/unified-wal/classic-linux-file-id-evidence-fcd63e2a`，保留source/class/命令/log/XML及两份glibc静态源。单个确切raw scope双lsof/identity/SHA/archive hardlink核验后清理2目录、4文件路径、365627逻辑字节，归档保留/regular-file数据块回收0。给定表和id的局部计算不证明真实fd/fstat/close、初始namespace/loader/main及l_loader来源、加载BFS/祖先链、RPATH环境或完整递归helper/backend exec；typed Linux/vendor阳性及本片models/性能NOT_EXECUTED。diagnostics unconditional Unsupported、完整16步目标active、整体Gate OPEN及默认OFF/SYSTEM/PARK、advanced disabled保持。

## 显式已加载对象表的名称复用与 SONAME 记名

2026-11-05在父`8102eb35c8e59cd4c7cdfec2ba1bebba6606a47f`上，新增包内SegmentClassicLinuxLoadedNames，仅接原顺序的显式base-namespace对象表、原lookup name和deadline，无Path/文件/Process/JNI或owner入口。每对象保留opaque identity、原context、l_name、l_libname列表、null/empty可区分的SONAME，以及added/faked/removed标志；同context不同identity不合并，重复identity拒绝，不从D2候选边、physical、SONAME或context排序推断加载顺序。

依据[glibc2.39 _dl_map_object及add_name_to_object](https://raw.githubusercontent.com/bminor/glibc/glibc-2.39/elf/dl-load.c)和[_dl_name_match_p](https://raw.githubusercontent.com/bminor/glibc/glibc-2.39/elf/dl-misc.c)，逐对象按原顺序先跳过faked/removed，再精确比较该对象l_name及名称列表；仅未直接命中且sonameAdded=false、SONAME存在时再比较其SONAME。命中立即返回，早先对象SONAME优先于后面对象主名，不能分两遍扫描名称/SONAME。strict UTF8下原字符串精确相等，不用cache数字比较，不normalize路径/Unicode、展开slash/$/token或按inode合并；查询非空、无NUL、≤4096字节，对象单串同界但空串合法，缺SONAME与present-empty保留。

direct命中不改变added或别名。仅SONAME分支追加该名字到本对象名称列表尾并置added；此前同对象名称已确认不匹配，不重复追加。返回Reuse(state, ordinal, replacement)，replacement的names是不可变副本，可按原ordinal替换后供下一次纯lookup；其他对象和顺序由调用方原表保留，原输入不变，不建owner缓存/事件日志或模拟glibc锁。sonameAdded=true不再独立补查或修复SONAME，即使给定表缺对应别名。明确NO_REUSE返回-1/null，仅表示本表没有可复用对象，不表示库不存在或后续搜索已经失败。

选择及任何列表复制/更新前核验全表：对象/context限4096，l_libname全表关联数限4096＋原D2.MAX_REQUESTS(8192)=12288，每对象原名称列表须非空（元素可空），所有对象/名字/SONAME严格检查。坏后项、重复身份或总关联超限不能被早先命中遮蔽，faked/removed对象也不免结构检查；SONAME追加不能越过总界，失败不改输入。明确空表可NO_REUSE，null表/列表不当空表，原deadline保留；无新可调预算或通用状态机。

新6项纯数据测试全部通过，0失败/错误/跳过；Spotless/Checkstyle首轮退出0，规划只读评审无新增阻塞。覆盖早SONAME/晚主名顺序、同context独立身份、首次记名后重查只保留一份alias、输入及结果隔离、direct flag不变、faked/removed、.01与.1/slash/$/Unicode字面、added/absent/empty、坏后项、4096对象/12288全表及追加界/UTF8恰4096和deadline。1443项输入与原snapshot/父Git blob无错配，两个当前code与test/style快照一致；未改owner/Cache/Diagnostics/SearchStages/JNI矩阵未重跑，旧PASS不移绑。

证据位于工作区外`/Users/liusinan/.codex/artifacts/unified-wal/classic-linux-loaded-names-evidence-8102eb35`，保留原source/class/命令/log/XML与两份glibc静态源；单个确切raw scope双次lsof/identity/SHA/archive hardlink核验后清理2目录、4文件路径、364821逻辑字节，归档保留/regular-file数据块回收0。此为稳定给定数据的算法与替换结果，不是实际namespace/并发快照。初始namespace及l_loader来源、文件映射后的inode复用、RPATH环境、DFS/BFS加载顺序、完整递归solver/helper/backend exec和typed Linux/vendor阳性仍OPEN/NOT_EXECUTED；本片models/性能未执行。diagnostics unconditional Unsupported、完整16步目标active、整体Gate OPEN及默认OFF/SYSTEM/PARK、advanced disabled保持。

## Loader diagnostics 的有界纯数据解析

2026-11-05在父`5fb81cdee7c38064780dc7c7926836239e832605`上，新增包内SegmentClassicLinuxDiagnostics，仅接supplied byte[]及原absolute deadline；在clone前检查非空及原stdout 64KiB上限，一次私有副本保留原文，raw accessor再复制。按ASCII字节与LF扫描label[hex-index].subfield=value，不宽松UTF8解码、不构造Path或做I/O/Process/JNI。canonical小写0x整数/索引拒绝前导零异写，数值保留uint64位模式；原始NUL/非打印ASCII、坏引号/escape、溢出、重复字段及残缺末行均拒绝。所有未知字段仍检查行语法并保留在原bytes，不为全部CPU/env/uname构造对象树；扫描/记录规模受原64KiB及deadline限制，无新可调预算。

只提取version.version=2.39、dl_dst_lib、dl_hwcap、dl_hwcap_important、dl_hwcap2、dl_hwcaps_subdirs、dl_hwcaps_subdirs_active、dl_platform、dl_string_platform、x86.cpu_features.isa_1及path.system_dirs。标量缺失/错类型拒绝，不补0或空值；dl_platform=0x0表示明确NULL，空字符串保留为可用空串，缺字段拒绝，其他数值替代平台字符串拒绝。subdirs须为固定v4:v3:v2完整名称串，内置active仅保留0..7原值，ISA限uint32，二者不互相推算。system_dirs按原0起连续索引保留1..64条，每条为绝对slash结尾、≤4096字节的可逆ASCII串；/合法，dot/slash/alias字面形式和顺序不变，不排序/normalize或补默认目录，其他消费单串也限4096字节。类型检查不是D2材料、编译来源或实际loader批准。

[glibc2.39字符串编码器](https://raw.githubusercontent.com/bminor/glibc/glibc-2.39/elf/dl-diagnostics.c)的两位均使用(ch >> 6) & 7，丢失原byte中间3位，0x08与0x10均会输出同一数值转义文本；因此所有已消费字符串只解码可打印ASCII及反斜线/引号两种可逆escape，遇任何三位数值escape整次明确Unsupported，不能按标准octal猜原bytes或UTF8。未知/未消费字符串的三位0..7转义仅作opaque词法保留，包含当前编码可能产生的660/777形式，不误加单byte≤0377范围，也绝不逆向解码。这只是本diagnostics解码器的支持边界，不削弱D2/cache/真实文件的原UTF8路径合同；未新增codec策略或修正glibc输出。release/2.39/master同处静态源仅作交叉核对，不认证批准vendor。

关键缺口保持显式：[dl_hwcap_important打印的是HWCAP_IMPORTANT](https://raw.githubusercontent.com/bminor/glibc/glibc-2.39/elf/dl-diagnostics.c)，[cache使用的hwcap_mask来自运行tunable](https://raw.githubusercontent.com/bminor/glibc/glibc-2.39/elf/dl-cache.c)，不能互代；内置active输出也不是应用运行prepend/mask后的[最终capstr/priority](https://raw.githubusercontent.com/bminor/glibc/glibc-2.39/elf/dl-hwcaps.c)。结果只称Parsed supplied bytes及已打印Fields，无ready/qualified/toCapabilities或实际winner便捷转换。原LinuxLoaderDiagnosticsObservation/private构造、owner入口与unconditional Unsupported guard逐字节未改；parser成功不证明capture完整、EOF、进程成功、loader来源、环境/父helper启动链或完整能力事实。

新7项独立格式数据测试全部通过，0失败/错误/跳过；Spotless/Checkstyle首轮退出0，规划只读评审无新增阻塞。覆盖uint64/uint32及active独立、平台NULL/空串/缺字段、目录原序/raw形式及不可变副本、关键字符串有损escape拒绝而unknown env/uname原文保留（含000碰撞及660/777）、坏语法/数值/重复/缺字段或索引、64目录/4096消费字节、恰64KiB与超界/deadline。没有运行loader或调用JNI，不重跑未改owner/JNI/Cache矩阵，旧PASS不移绑。1441项输入与原snapshot/父Git blob无错配，两个当前code与测试/style快照一致。

证据位于工作区外`/Users/liusinan/.codex/artifacts/unified-wal/classic-linux-diagnostics-evidence-5fb81cde`，保留原source/class/命令/log/XML及6份glibc静态源。单个确切raw scope经双次lsof/identity/SHA/archive hardlink核验后清理2目录、4文件路径、364754逻辑字节，归档保留、regular-file数据块回收0。实际hwcap_mask、最终capstr/prepend/mask、owner来源绑定、前序环境/祖先/ORIGIN、first-loaded/递归solver/helper闭包/backend exec及typed Linux/vendor阳性仍OPEN/NOT_EXECUTED；本片models/性能未执行。完整16步目标active，整体Gate OPEN，默认OFF/SYSTEM/PARK及advanced disabled保持。

## Cache 单行选择与选后 NODEFLIB 的纯数据算法

2026-11-05在父`f41ba00a625e45ca80b4d4e6c7bbccc03278a200`上，原SegmentClassicLinuxCache增加包内`select`，复用既有有界new-cache1.1 Image、原LP64 flags303及数字等价组。它只计算显式假定的raw64 dl_hwcap/hwcap_mask、platform观测值或明确缺失、uint32 isa_1、有序active内置hwcaps、requester flags1及编译system_dirs前缀；不取得实际CPU/loader事实，不接trust typed入口或执行放行。null/缺事实不能变成0/空表或NO_MATCH；明确空active合法，platform的Optional.empty表示已知缺失，未知平台值只产生不匹配。active只支持v4/v3/v2的有序无重复子序列，不从ISA或名称互相推断。

依据[glibc2.39 cache选择](https://raw.githubusercontent.com/bminor/glibc/glibc-2.39/elf/dl-cache.c)及[x86 ISA检查](https://raw.githubusercontent.com/bminor/glibc/glibc-2.39/sysdeps/x86/dl-isa-level.h)，named行先执行ISA单bit成员检查，再按原扩展名raw bytes取active优先级；较低priority优先，同级保留首行，不解码未知名称。ISA是位索引；parser仍保留0..1023，只有实际到达的named行索引≥31明确Unsupported，避免将signed-C移位未定义行为伪造成Java掩码移位结果。无关名称、其他ABI及停止后的行不执行该语义检查。已有named best时遇legacy先停止；否则按raw64能力mask、TLS bit63及platform mask筛选首个合格legacy行并停止，忽略osversion_unused。x86-64只接受haswell bit50、xeon_phi bit51，未知/明确缺失平台不匹配任何非零platform字段。

选出唯一原Entry后，才按requester flags1位0x800作[system_dirs原字节前缀排除](https://raw.githubusercontent.com/bminor/glibc/glibc-2.39/elf/dl-load.c)。显式表限1..64条，每条严格UTF8、无NUL、以/开头及结尾、≤4096字节，/本身合法；这是本profile支持边界，不声称所有glibc构建均不能含零长度项。不trim/normalize/去重或把D2默认目录认证成编译表；前缀只编码一次，raw path不逐行复制或解码。返回NO_MATCH、SELECTED或REJECTED_NODEFLIB；后者保留同一Entry/ordinal/Text，绝不重扫组内后行。/lib/不匹配/lib64/，alias/dot和文件存在性不参与纯计算，选中路径陈旧/缺失也不会使本算法改选。原related/owner cache/default/四位置方法及全部格式不变。

当前Cache16项全部通过、0失败/错误/跳过，含6个新增算法测试；Spotless/Checkstyle首轮退出0。复用原固定向量及轻量独立legacy字节镜像，经实际FileChannel parser取得Image；覆盖named优先级/同级首行、active与ISA独立、明确空active、legacy平台/TLS/mask/首行、数字等价及其他ABI、ISA30与31/32/1023执行边界、raw非UTF8名称和选中path、NODEFLIB选后排除不fallback及raw prefix/alias/dot、缺事实/空表/4096字节及deadline。规划会话只读评审无新增阻塞。1439项测试输入与原snapshot/父Git blob无错配，两个当前code与测试/style快照一致；未改owner/JNI矩阵不重跑，旧PASS不移绑。

证据位于工作区外`/Users/liusinan/.codex/artifacts/unified-wal/classic-linux-cache-selection-evidence-f41ba00a`，保留原source/class/命令/log/XML及7份glibc静态源。单个确切raw scope经双次lsof/identity/SHA/archive hardlink核验后清理2目录、4文件路径、366684逻辑字节，归档保留、regular-file数据块回收0。实际能力与编译表的原owner来源、active capstr/prepend/mask、前序环境/祖先/ORIGIN、first-loaded/递归solver/helper闭包/backend exec与typed Linux/vendor阳性仍OPEN/NOT_EXECUTED，本片未执行models/性能。diagnostics unconditional Unsupported、完整16步目标active、整体Gate OPEN及默认OFF/SYSTEM/PARK和advanced disabled保持。

## 有序默认目录阶段与真实 requester NODEFLIB

2026-11-05在父`55c51ce5ce4ca4042879f6fdf4acf9b1b7203b3b`上，原trust新增`verifyLinuxNeededDefaults(ruleIndex, deadline)`，只从本owner原D2/source/provider绑定取rule、requester/context/ordinal和LoaderPolicy.defaultDirectories。原full constructor的默认目录引用约束提取为共用纯`defaultDirectoryIndices`：按policy原raw顺序精确匹配Directory索引，缺失或歧义在目标I/O前拒绝，不按目录表排序、物理路径或normalize重排。原格式、编码及旧单目录/base/cache入口语义不变；独立read seam仍只提供声明/真实文件观察，不制造Installed/source。

声明flags1只决定预规划。未置NODEFLIB时，先预检全部原默认目录的完整v4/v3/v2/base路径及strict UTF8/4096界，然后才经一次private owned requester ELF2、原ordinal/name与完整元数据绑定；以实际ELF的flags1位`0x800`返回显式状态和presence/value事实。[glibc2.39 default分支](https://raw.githubusercontent.com/bminor/glibc/glibc-2.39/elf/dl-load.c)及[DF_1_NODEFLIB常量](https://raw.githubusercontent.com/bminor/glibc/glibc-2.39/elf/elf.h)提供静态依据。只用本requester，不传播main、祖先或候选的位。置位时返回`SUPPRESSED_NODEFLIB`，不生成默认候选路径集合，不探测默认目录/候选或访问cache；空观察列表表示阶段被抑制，不表示全部缺失。按规划澄清允许原alias字典的纯数据Path/字节准备，必要requester自身材料和所经过的alias仍完整核验。

未抑制时按policy顺序调用窄private四位置机制，每组保留policy ordinal、原Directory索引、presence/protection和全部位置/候选事实；requester和原表只绑定一次，不嵌套public同步入口。dirname/alias walker、exact logical＋physical pre-content批准、owned ELF2及first-close合同沿用。各目录原有局部终端检查保留，所有默认目录读完后再统一复核全部位置与各原目录，后面目录读取期间替换前面的候选或目录不能被局部稳定掩盖。仍是有限观察，不声称统一快照、连续保护或实际loader尝试序列；沿用64目录/每目录4位置及原路径/ELF/deadline上限，无新预算、缓存、重试或线程。

新增6项先全部通过，最终Trust112＋Deployment9共121项全部通过，0失败/错误/跳过，样式首轮退出0；121已含6项，不累计为127独立用例。夹具真实改写requester ELF DT_FLAGS_1字节清`0x800`并同步SHA/声明，候选保留该位仍不抑制本requester阶段；反向用实际带位的library requester、已清位的main、会导致完整拼接超界的未使用默认目录和目标I/O guard，确认只一次requester内容open即明确抑制，不触及default/candidate/cache。声明单独置位不能伪造跳过。另覆盖policy顺序3→2不同于目录表排序、全目录全位置/共享一次requester、最后目录base close期间同target替换早先目录alias被全组末端检查拒绝、raw引用不匹配及后续目录最长路径在任何requester/target I/O前拒绝、typed缺source。原四位置/base/cache的Trust相关覆盖及受共享约束影响的原Deployment构造/向量覆盖同时执行；未改Cache/JNI矩阵未重跑，旧PASS不移绑。

证据位于工作区外`/Users/liusinan/.codex/artifacts/unified-wal/classic-linux-default-stage-evidence-55c51ce5`。两阶段各1439项输入与原snapshot/父Git blob无错配，当前三code与6项/121项/style快照完全一致。终态RAT退出1/431，原未批准路径及字节与父HEAD相同，无新增；PDF扫描前排除且未读取。两个确切raw scope经双次lsof/identity/SHA/archive hardlink核验后清理4目录、9文件路径、795778逻辑字节，归档保留、regular-file数据块回收0；两份glibc原源静态归档不认证实际vendor运行。

结果仅表示“若查找走到default阶段”的受支持内置位置对应或实际requester位抑制，不证明first-loaded/RPATH/env/RUNPATH/cache此前均失败，也不选winner。cache方法未改：[glibc先选单条cached再按NODEFLIB作system_dirs原字节前缀拒绝](https://raw.githubusercontent.com/bminor/glibc/glibc-2.39/elf/dl-load.c)，不能先删cache默认目录行再取后行充当fallback；实际cache capability/ISA选择及选后过滤仍OPEN。实际active/capstr、前序环境/祖先/ORIGIN来源、first-loaded/递归solver/helper闭包/backend exec及typed Linux/vendor阳性仍未验收，本块models/性能NOT_EXECUTED。diagnostics unconditional Unsupported、完整16步目标active、各整体Gate OPEN、默认OFF/SYSTEM/PARK及advanced disabled保持。

## 单个声明目录的固定内置四位置候选对应

2026-11-05在父`b012a5b711d162f3f652c5cc24ec6b88718f1476`上，原trust新增`verifyLinuxNeededDirectoryPositions(ruleIndex, directoryIndex, deadline)`，原D2/source/provider/owner门后，只从原policy的hwcaps rule1、requester原NEEDED ordinal及一个原Directory导出固定v4/v3/v2/base四位置。依据[glibc2.39 x86-64内置顺序](https://raw.githubusercontent.com/bminor/glibc/glibc-2.39/sysdeps/x86_64/dl-hwcaps-subdirs.c)、[suffix生成及空base](https://raw.githubusercontent.com/bminor/glibc/glibc-2.39/elf/dl-hwcaps.c)和[open_path拼接](https://raw.githubusercontent.com/bminor/glibc/glibc-2.39/elf/dl-load.c)，依次观察`glibc-hwcaps/x86-64-v4/`、v3、v2及空suffix。该集合尚未按实际能力筛选，不是当前capstr，不接受caller suffix/CPU/mask或额外目录。独立read seam只消费声明子图及真实临时文件，不建立Installed/source。

原普通name形态、indices/policy和全部四条完整logical的strict UTF8/4096字节界在requester/目录/候选目标I/O前预检；固定suffix全为ASCII，其length按字节收费。dirname复用原尾slash规则，内部dot/slash与字面`$`原文保持，不做DST或列表展开。requester只经一次private owned ELF2/实际ordinal绑定；所选原base Directory presence/protection先后核对，三个派生位置直接经原walker检查完整父链、alias、保护及实际缺失，不要求为本方法额外增加派生Directory行，也不替代已有Directory整表观察。

每位置先probe，存在时须本NeededRule的exact logicalLoadName＋resolved physical context批准后，才复用原private候选体读取owned ELF2并关联probe/content identity；可靠NoSuchFile才记录缺失，AccessDenied、非文件、未知alias或内容/身份错误均整遍拒绝。前面有文件仍继续全部四项；返回保留原rule/directory、固定position/suffix、logical/path/absence及context/member。所有位置读取后逐项复核完整path/identity/alias-chain，再核对原目录，后面的读取不能用新事实覆盖先前已观察身份。不是原子快照或实际loader的尝试/失败继续算法；旧base-only/cache入口及共享候选体未变，不嵌套public同步方法，不增加预算、重试、线程、缓存或跨调用状态。

首6项全部通过后，只修正一行121字符测试行的换行；原style失败及6项source/class/log/XML保留。最终当前两code的Trust106项全部通过，0失败/错误/跳过，style-final退出0；106已含6新增，不相加为112独立用例。新增真实文件夹具覆盖混合存在/缺失和固定顺序、单次requester读取、v4 alias指向base实体但仍需新exact context、raw Unicode/字面`$`及dot/slash、base批准不能放行现存hwcap logical且内容未open、最后base读取close期间同target替换先前v4 alias被终端复核拒绝，以及base路径恰4096而最长suffix超界时pre-I/O拒绝、typed缺source。原base/cache的Trust相关覆盖同时执行；未改Cache独立类和JNI矩阵未重跑，其原PASS不移绑。

证据位于工作区外`/Users/liusinan/.codex/artifacts/unified-wal/classic-linux-builtin-positions-evidence-b012a5b7`。两测试阶段各1439项输入与原snapshot/父Git blob无错配，最终两code与106项/style-final快照相同；生产源也与首6项相同，首测试源仅换行前版本独立保留。终态RAT退出1/431，原未批准路径及字节与父HEAD相同，无新增，PDF扫描前排除且未读取。两个确切raw scope经双次lsof/identity/SHA/archive hardlink核验后清理4目录、8文件路径、748704逻辑字节，归档保留、regular-file数据块回收0；三份glibc原源静态归档不认证实际vendor执行。

本块只对应一个原目录的四个固定内置位置，不证明该目录实际进入搜索，也不是所有运行prepend/mask输入下的完整宇宙。实际active/capstr、prepend/mask来源、cache ISA优先、NODEFLIB、RPATH/env/default整体顺序、first-loaded/递归solver、helper闭包及backend exec仍待后续；typed Linux/vendor阳性、本块models/性能NOT_EXECUTED。`requireLinuxDiagnosticsProcessSource()`保持unconditional Unsupported，完整16步目标active，各整体Gate OPEN，默认OFF/SYSTEM/PARK及advanced disabled不变。

## 单条 NEEDED 的 cache 相关行与候选对应

2026-11-05在父`f09a1979a2bb44134b4087fd5738ae8d25e3c86e`上，原trust新增`verifyLinuxNeededCache(ruleIndex, deadline)`，只从本owner原D2/provider/source选择唯一固定`/etc/ld.so.cache`的CACHE规则；独立`readLinuxNeededCache`只供声明子图与真实临时文件，不能产生Installed或typed来源。原table/context/ordinal、cache kind/format/角色/长度和普通NEEDED名称形态在目标I/O前核对；requester经原owned ELF2读取，实际ordinal与声明必须相同。cache复用原hash/parser/实际first-close读取，明确区分受保护的cache缺失、cache存在但无相关行、行存在而目标缺失。

[glibc2.39 search_cache及_dl_cache_libcmp](https://raw.githubusercontent.com/bminor/glibc/glibc-2.39/elf/dl-cache.c)使用数字感知名称比较，`libz.so.01`和`libz.so.1`可同组；[x86-64 flags规则](https://raw.githubusercontent.com/bminor/glibc/glibc-2.39/sysdeps/x86_64/dl-cache.h)限定`0x303`。新增相关行匹配直接复用原比较器及signed-C intermediate overflow校验，请求也按同一边界拒绝。全部相关原Entry/name/path/ordinal/flags/hwcap/ISA和顺序保留，重复行不合并；不做CPU/hwcap eligibility或winner选择，无关及其他ABI的path不额外解码或查文件。原parser/cache格式接纳范围不变，参考源码不认证实际approved vendor。

相关path保留cache原始字节含义，严格UTF8和有界绝对logical检查后走原lookup，不做DST、冒号分割或dirname/name拼接。只有受保护的实际NoSuchFile可以形成stale行缺失；AccessDenied、目录、未声明alias及其他错误均拒绝。目标存在时，必须先匹配本NeededRule.candidates的exact logicalLoadName＋resolved physical member，才能打开内容；提取复用的private候选体保持原probe→批准→owned ELF2→probe/content identity对应顺序。原base-directory入口仍在之后执行其候选及目录末端复核，没有重入public同步方法清掉initializingThread。

为避免重复路径反复保留完整ELF2及parents，method-local raw Cache.Text map只共享完全相同字节路径的一份不可变lookup/内容事实，各Entry仍逐行返回。不同raw logical path即使同inode也不共享批准；不按name/normalize/resolved合并，无跨调用状态。distinct path首次在decode/Path/目标I/O/展开保留之前，按Text原byteLength向既有LoadingDirectoryBudget收费，复用262144项/64MiB累计路径UTF8字节上限；64MiB不是总堆上限，真实缺失的distinct path不套D2 4096 context数。cache再经原owned hash/parser读取并比较path/identity/parents/SHA，随后每个distinct path末端复核，最后再次检查cache路径/identity；变化、超限或任何失败均整遍退出，不返回prefix或声称跨对象原子快照。缺失行不推导loader会尝试后续行。

首轮最小12项中11通过、1错误来自新增测试误将原vector的非LP64 raw-path行期望纳入结果；夹具显式改为LP64后保留严格production过滤。首样式一行121字符失败另存，换行后样式退出0。资源共享后的13项新增检查全部通过，最终Trust100＋Cache10共110项全部通过，0失败/错误/跳过；110已包含13项，不合计为123个独立用例。覆盖数字等价/能力原文/重复行及单次content open共享事实、无关/其他ABI坏UTF8略过、相关坏path/AccessDenied/目录/未声明alias拒绝、cache缺失/空组/stale＋present区分、同physical另一raw path批准不复用、probe后的同字节换inode/同target换alias、cache A/B变化及最终cache读取期间candidate替换，以及raw预算边界/请求overflow/typed缺source。未造source、observer或Installed，未重跑未变JNI矩阵。

证据位于工作区外`/Users/liusinan/.codex/artifacts/unified-wal/classic-linux-needed-cache-evidence-f09a1979`。三个测试阶段各1439项输入与原snapshot/父Git blob匹配，最终四code与13项/110项/样式原快照一致，原失败source/class/log/XML保留而不移绑PASS。终态RAT退出1/431，原未批准路径及字节与父HEAD相同，无新增；PDF扫描前排除且未读取。三个确切raw scope双次lsof/identity/SHA/archive hardlink核验后清理6目录、15文件路径、1252374逻辑字节，归档保留、regular-file数据块回收0；三份glibc原源静态归档不作运行证据。

本块只交付一条原NEEDED的尚未按能力筛选的cache相关行与实际候选对应。typed approved Linux/vendor阳性、本块models/性能NOT_EXECUTED；first-loaded复用、RPATH/RUNPATH＋环境＋cache＋default整体顺序、CPU/hwcap选择、helper传递闭包和backend exec语义仍待后续。`requireLinuxDiagnosticsProcessSource()`保持Unsupported，完整16步目标active，各整体Gate OPEN，默认OFF/SYSTEM/PARK及advanced disabled不变。

## 原 trust owner 的缓存 backend 来源绑定

2026-11-05在父`f6f2554c5212db3b8b8642c349b748c5f187c9ae`上，原owner增加只收原deadline的`verifyLinuxJdkProcess`。A段在monitor内先核对原D2/provider/source，固定同一verifiedProgram、runtime image、provider、descriptor/source digest和原controlled JNI library引用；只用不初始化的Class.forName及原requireBootstrapType检查固定ProcessImpl/enum的bootstrap java.base来源，复核原lib/modules identity/父链及preload材料，再登记本次caller/inFlight/一次性状态。B段完全在monitor外调用原final nativeIo.jdkProcessLaunch，可能初始化或阻塞，不启动Process、不设置property/字段，也不接受caller观察、NativeIO替身或来源参数。

C段在实际调用体返回后重新进入monitor，拒绝closing、失败、过期及原来源引用变化，再核对默认provider、固定module类型、原runtime image/preload和loaded JNI文件。纯`bindLinuxJdkHelperCache`复用原canonical严格UTF8 selector及program/D2/context原索引，将原helper bytes逐字节比较为raw javaHome＋固定suffix＋唯一末NUL；不宽松解码或normalize。随后直接复用提取的private原helper owned ELF/PT_INTERP观察体，末端再次复核runtime image/preload/D2/provider/library，全部成功才留存本owner私有binding并返回私有构造的数据结果。FORK/POSIX_SPAWN/VFORK如实保留；存在helper缓存不推导实际执行了helper，FORK也不自动免除未建立的启动准入。

独立的attempted/inFlight/caller/failure/observation只服务本操作。requireReady在在途时拒绝其他线程新操作；close可标closing并观察资源，不等待或取消class-init/native，总drained还须!backendInFlight，自caller排空拒绝。首失败保留到原launcherFailure/backendFailure，finally在原调用体退出后清caller/inFlight；晚返回不恢复closing/过期后的成功。本块不引入Output、进程runner、通用任务registry或public同步入口重入。原verifyLinuxProvider的image identity检查仅提取为private复用，原helper材料入口仍保留其原数据合同。

新增3项先通过，随后因requireReady/Drain共享边界变化执行完整Trust89项，0失败/错误/跳过，样式退出0。新增检查覆盖exact bytes与原1/1/2索引、不同字节/path/NUL/坏UTF8/期限拒绝，以及真实material owner的typed缺source、首次失败留存、实际排空和生产guard保持。typed拒绝发生于原source门，未触及target I/O/ProcessBuilder；JNI在该拒绝路径之前不会调用由源码顺序核对，不把mock观察当成功的owner/JNI整合。未新增source/observer注入或反射Installed。两阶段各1439项输入与原snapshot/父Git blob无错配，两code与三项/89项/样式原快照一致。未变化的JNI原语实际运行证据仍独立绑定上一块，未重跑或移绑其PASS。

证据位于工作区外`/Users/liusinan/.codex/artifacts/unified-wal/classic-linux-jdk-backend-evidence-f6f2554c`。终态RAT退出1/431，原未批准路径及字节与父HEAD相同，无新增；PDF扫描前排除且未读取。两个确切raw scope经双次lsof/identity/SHA/archive hardlink核验后清理4目录、8文件路径、745343逻辑字节，归档保留、regular-file数据块回收0。没有原批准Linux来源，完整owner A/B/C成功及close等待class-init、迟到返回不可成功、同episode不可重用的并发整合仍NOT_EXECUTED；本机JNI PASS仅证明原观察机制。本块models/性能及Linux/vendor阳性NOT_EXECUTED，helper传递依赖/实际backend执行语义、诊断parser和qualification仍待后续。`requireLinuxDiagnosticsProcessSource()`继续Unsupported；完整16步目标active，各整体Gate保持OPEN，默认OFF/SYSTEM/PARK及advanced disabled不变。

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

## 正常 D2 launcher 的 helper 解释器前置检查

正常 D2 的原 helper 组在首轮 helper 和解释器路径身份复核后，对实际 PT_INTERP 指向的、已由原 owner 绑定的 ELF 应用与 diagnostics 共用的四项条件。解释器必须为 ET_DYN、入口非零、无自身 PT_INTERP、无 DT_NEEDED。该检查直接消费原 logical→physical、批准摘要和 owned fd 对应的 ELF，失败先于首条 cache、tag 或默认目录目标读取。helper 自身无 PT_INTERP 时，以原上下文和 ordinal 继续核验其 DT_NEEDED。组末仍复核适用的路径和身份。

在父 `83654c883b` 上沿用 acf9afec… native 库。macOS arm64 上 83 项回归和 Java 样式通过。真实临时 ELF 分别验证了解释器带 DT_NEEDED、带自身 PT_INTERP 时在候选读取前拒绝，首次失败随 owner 排空保留。通过用例验证了实际解释器满足条件后仍检查原重复 ordinal，以及 helper 缺 PT_INTERP 时会拒绝后续不支持的 NEEDED 名称。同字节解释器替换仍由组内身份复核拒绝。证据目录为 `/Users/liusinan/.codex/artifacts/unified-wal/classic-helper-interpreter-evidence-83654c88`。

本节核验固定解释器的 ELF 形状。实际初始 namespace、符号和重定位、递归依赖、加载选择及执行期间 native 环境稳定性仍待验证。批准 Linux/vendor 启动记为 NOT_EXECUTED，原 childCommand 和 diagnostics 执行门继续拒绝。

## 正常 D2 launcher 的 helper 自身 RPATH/RUNPATH 候选核验

正常 D2 仍只调用一次 `verifyLinuxJdkHelperNeededCandidates`。每条直接 `DT_NEEDED` 先完成 cache A/B，再以同一份实际 requester 核验自身 tag 和默认目录。tag 核验前后复核 requester 的原逻辑路径和身份，实际 context、member、ordinal、name 和两 tag 的 presence/value 与原声明逐项对应。组前后继续复核 helper 和 PT_INTERP，三阶段均通过才记录该条原规则索引。零直接请求时整组不选择或读取这些阶段的目标。

RUNPATH 只要存在便优先，包括空值。仅在 RUNPATH 缺失时考虑自身 RPATH，NODEFLIB 只抑制默认目录，不抑制 tag。tag 沿原顺序和同来源首次去重检查 v4/v3/v2/base，所选 tag 需要未观察的 DST、相对路径、CWD 或无对应原目录时拒绝。独立 tag 入口继续在读取 requester 前完成全部预检，并保留详细返回。启动组的 tag 与默认目录共用逐目录读取及阶段末复核，仅持有路径和身份直到末轮。cache 图和完整候选 ELF 不跨阶段累积。[glibc 2.39 的 RPATH/RUNPATH 与默认目录分支](https://raw.githubusercontent.com/bminor/glibc/glibc-2.39/elf/dl-load.c)

在父 `33725ccaed` 上沿用 acf9afec… native 库。首轮 79 项回归通过，样式报两处超长签名。换行后以最终源码独立执行，79 项与 Java style 全通过。真实临时 ELF/cache/候选验证了 RPATH 原序和去重、NODEFLIB 下 tag 仍访问、同一 requester 的读取次数，以及 RUNPATH 压下未知 DST 后重复请求的未批准候选拒绝。这里记录的是材料核验顺序，实际阶段可达性和选择仍待验证。批准 Linux 运行、递归依赖和执行期间 native 环境约束继续开放，原 childCommand 与 diagnostics 继续拒绝。

## 正常 D2 launcher 的 helper 直接依赖默认目录核验

在 `33725ccaed` 上，正常 D2 在原 helper 组入口 `verifyLinuxJdkHelperNeededCandidates` 内串行检查同一条直接 `DT_NEEDED` 的 cache 和默认目录。该入口随后按上节接入自身 tag。cache A/B 完成后只取该次实际 requester，先复核原 helper 路径和身份，再用其原 ordinal、name 及实际 ELF 标志核验默认目录。默认阶段无需重开 requester 或 cache，整组前后仍复核原 helper 与 PT_INTERP。无直接 NEEDED 时，两阶段都不选择或读取目标材料。

原 policy 的目录顺序保留，实际 requester 的 `DF_1_NODEFLIB` 抑制默认目录时不构造四位置或探测目标。其余请求依次检查 v4/v3/v2/base 的实际路径、presence、原候选批准、held fd 与摘要。cache 有匹配行时仍独立检查默认候选，因为本阶段只核验条件材料。单目录完成后，启动组仅保留用于阶段末复核的路径与身份。cache 图与完整候选 ELF 不跨阶段或原规则累积。只有两阶段都通过才记录完成索引，独立 default 数据入口仍保留预检顺序与详细返回。[glibc 2.39 默认目录分支](https://raw.githubusercontent.com/bminor/glibc/glibc-2.39/elf/dl-load.c)

在父 `91d3800755` 上沿用 acf9afec… native 库。主回归 72 项和独立 requester-tag 回归 5 项全部通过，Java style 通过。实际临时文件覆盖清除和保留 NODEFLIB 的 helper、policy 顺序、相同 requester 读取次数、重复 ordinal 的后项拒绝及最后目录读取期间替换先前候选。真实批准 Linux、阶段可达性与选择、递归依赖和执行期间 native 环境约束仍待验证，原 childCommand 和 diagnostics 继续拒绝。

## 正常 D2 launcher 的 helper 直接依赖 cache 核验

在 `91d3800755` 上，正常 D2 launcher 在第二次 native 环境采样之后、`childCommand` 之前，调用当时只接收原 deadline 的 `verifyLinuxJdkHelperNeededCache`。该入口随后扩为上节的 cache 与默认目录核验。原 owner 先核原 D2 source/provider，再从已完成的私有 `backendObservation` 取得 helper，复核它仍对应同一批准 program、成员与确切执行 context。整组读取前后复核该 helper 及其已绑定 PT_INTERP 的实际路径和身份。

每条直接 `DT_NEEDED` 按原规则索引处理，重复名称仍按各自的 ordinal 和候选集合独立观察。共享 cache reader 在打开 cache 前比较实际 requester 与已绑定 helper，随后沿原 cache A/B、相关 LP64 行、候选逻辑路径、held fd、文件摘要和首次关闭合同校验。没有直接 NEEDED 时，本次 helper 核验不选 cache 规则或打开 cache。显式缺失、空匹配和 stale 路径沿单条观察的原语义处理，slash/DST 等不支持的名称使整组失败。

每条完成后仅保留原规则索引，完整 cache 图和行随该次调用退出。测试数据入口只在非 production owner 可用，由其独立读取实际声明根和解释器。相关行属于该条批准候选集合是条件材料事实，实际 loader 何时查 cache、哪条记录获选、依赖如何递归加载仍须独立核验。[glibc 依赖加载实现](https://raw.githubusercontent.com/bminor/glibc/glibc-2.39/elf/dl-deps.c)

在父 `dc6c1e1ace` 上沿用 acf9afec… native 库。首轮 57 项中有 1 项失败和 1 项错误，原因是两个测试夹具沿用了 slash/DST 名称。修正实际 ELF 字节与批准摘要，补独立拒绝用例后 58 项和样式通过。复核发现整组保留完整图会累积内存，改为逐条完成后只留索引，最终独立 58 项与 Java style 再次通过。真实临时 ELF/cache 覆盖非零根、重复 ordinal 各自批准、零直接请求、后项未批准拒绝和同字节解释器替换。批准 Linux 路径与实际加载未执行，父 native 环境写入约束和递归依赖仍待完成，原 childCommand 与 diagnostics 拒绝继续生效。

## 正常 D2 launcher 的当前 native 环境采样

原 D2 launcher 在 backend 观察前后的两个 `checkStarting()` 之后分别调用 `verifyLinuxNativeEnvironment(deadline)`。每次先核原 D2 source/provider 与期限，再冷读当前 native 环境。返回后检查原期限和资源健康，只有 `EMPTY_SAMPLED` 通过。非空、短读、变化或无法取得样本沿原 owner 的 sticky failure 阻止后续启动。D1 和无 Linux 部署不调用新 ABI。

Linux JNI 只取固定 libc `environ` 数据符号的裸地址，通过直接 `SYS_process_vm_readv` 对 `getpid()` 逐次复制一个指针宽度。先读表指针，表非 NULL 时读首槽位，再复读表指针。NULL 表也须复读一致。每次必须复制完整宽度，表指针变化立即拒绝。Java 只收到 `EMPTY_SAMPLED` 或 `NONEMPTY_SAMPLED`，环境字符串和地址都留在 Java 边界之外。旧 JNI 库缺符号以及非 Linux 平台均明确拒绝。[process_vm_readv(2)](https://man7.org/linux/man-pages/man2/process_vm_readv.2.html)

OpenJDK 在 POSIX_SPAWN 路径把当前 `environ` 交给 helper，随后才传递目标进程的环境块。Java 环境快照与 procfs 初始环境有各自的时间边界。内核复制避免在 Rust 中解引用可能因 glibc 环境更新而失效的表，复制本身不保证原子性，复读也无法排除 ABA。空样本仅描述本次读取。执行前还需要证明获批进程在执行期间的实际 native 环境稳定，并核验 helper 的传递依赖。原 childCommand 和 diagnostics 继续拒绝。[OpenJDK helper 继承环境](https://github.com/openjdk/jdk21u/blob/jdk-21.0.10-ga/src/java.base/unix/native/libjava/ProcessImpl_md.c#L556) · [Java 环境快照](https://github.com/openjdk/jdk21u/blob/jdk-21.0.10-ga/src/java.base/unix/classes/java/lang/ProcessEnvironment.java#L61-L92) · [proc_pid_environ(5)](https://man7.org/linux/man-pages/man5/proc_pid_environ.5.html) · [glibc 2.39 环境修改](https://github.com/bminor/glibc/blob/glibc-2.39/stdlib/setenv.c#L126-L159)

在父 `cb5c25cc8e` 上，最终 Darwin 库 acf9afec… 为 354496 字节。最终 native 18 项中 14 项通过、4 项 Linux 实读跳过，server 45 项全通过。Checked JNI 重跑、旧 4e0a6d2e… 库缺符号拒绝及 Java style 通过。2 项便携测试验证空表、首槽、短读、不可读和指针变化，最终对应片段逐字节相同。首次 Linux 编译因外部可变符号取址缺少 `unsafe` 失败，修正后 Linux `cargo check --tests` 通过。Rustfmt 仅报告父版本已有差异。

隔离 Linux JVM 的测试会先取得 Java 空环境快照，再由测试专用 JNI 写入固定变量，检查实际 libc 采样返回非空。当前宿主只验证了 Darwin 拒绝与 C 夹具语法，真实 Linux syscall、快照错配用例和批准 D2 全路径待实跑。生产 Gate 保持 OPEN。

## 正常 D2 launcher 的实际 JDK 后端与 helper 关联

原 launcher worker 在专用 Main、原程序、D2 文件和身份、systemd 固定端点及 dropper 检查通过后，调用一次现有的 `verifyLinuxJdkProcess(deadline)`。同一个 schema2 判断控制该调用和先前的 D2 检查，调用前后各走一次原 `checkStarting()`。核验失败、过期或已请求关闭时，启动在 `childCommand`、私钥读取与 ZooKeeper 建连之前停止。

这个阶段明确允许专用 LauncherMain 触发 `ProcessImpl` 首次初始化。上游 OpenJDK 21u 的实现会缓存父 PATH 并设置本进程的 SIGCHLD 处置。原 trust owner 复用锁内来源绑定、锁外实际 JNI 调用和返回后的同来源与实际 helper 文件复核，完成后只保存 owner 私有观察。初始化仍由原 worker 实际执行，外部等待超时只报告资源尚未终结。[OpenJDK 21u 的 ProcessImpl 原生初始化](https://raw.githubusercontent.com/openjdk/jdk21u/jdk-21.0.10-ga/src/java.base/unix/native/libjava/ProcessImpl_md.c)

在父 `271379e769` 上沿用原 4e0a6d2e… native 库。server 42 项全部通过，覆盖原 owner 缺可信来源拒绝、helper 字节与实际文件关联及 launcher 回归。native 4 项全部通过，包含独立 JVM 的 `-Xcheck:jni` 缓存读取、初始化后的属性变化及初始化失败。Java style 通过。正常 D2 调用位置和顺序由源码核验，批准 Linux 上的 owner 全路径及初始化与关闭并发尚未实跑。

后续执行还需要确认 helper 继承的实际 native 环境与传递加载链，并绑定 systemctl 自身的连接与 manager 响应。原 childCommand 和 diagnostics 的拒绝继续生效，生产 Gate 保持 OPEN。

## 正常 D2 launcher 的 systemd private socket 对端核验

schema2 launcher 先核验原生父凭据，在末轮 boot ID、process birth 与初始 PID/user namespace 检查后，调用只接收原 deadline 的 `verifyLinuxSystemdPeer`。原 trust owner 先核验 D2 source/provider，再用已绑定的 provider 构造固定 `/run/systemd/private`。父目录和祖先沿用原物理路径、owner、ACL 与保护检查。对 leaf 用 `NOFOLLOW_LINKS` 取得原始 Unix 身份，要求 UID 0、socket 类型及精确 mode 0700。systemd v255 的系统 manager 在 PID 1 时以 umask 0077 绑定该固定路径。[systemd v255 listener 源码](https://github.com/systemd/systemd/blob/v255/src/core/dbus.c#L850-L898)

owner 复用一次非阻塞 Unix connect 和原绝对 deadline，从同一实际 channel 调用原 JNI `SO_PEERCRED`。此连接只取连接时凭据，要求 PID 1、UID 0，GID 由内核返回。channel 和专用 selector 分别登记，先关闭 selector 以解除注册，再尝试关闭 channel。任一关闭失败保留首个失败和未结算资源，已有操作异常仍为主错误。完成关闭后复核 leaf 与受保护父链、owner 健康和期限。路径、连接、凭据或关闭失败沿原 launcher failure 阻止进入 dropper、childCommand 和私钥读取。D1 与无 Linux 部署的路径保持原行为。[Linux Unix socket 凭据语义](https://man7.org/linux/man-pages/man7/unix.7.html)

本次连接在对端核验后关闭，期间没有发送认证、D-Bus 或应用字节。后续 `systemctl` 自己的连接及响应、有效 unit 属性和 manager episode 仍需独立绑定。既有 D1/D2 childCommand 与 diagnostics 拒绝继续生效。[systemd v255 CLI 连接与回退源码](https://github.com/systemd/systemd/blob/v255/src/shared/bus-util.c#L187-L213)

在父 `08c87d108c` 上使用原 4e0a6d2e… native 库，最终 server 59 项全通过，native 4 项中 3 项通过、1 项 Linux 描述符重分配用例跳过，Java style 通过。临时 socket 的 EOF、leaf 替换和 selector 关闭故障覆盖原连接的资源结算。替换用例注入 peer 结果，实际跨进程 native 用例在 Darwin 验证平台拒绝和连接可用性。首次 server 59 项中 1 项因夹具期望的异常类与实际 connect 拒绝不符而失败，修正断言后的运行保留独立输入与结果。批准的 Linux 固定端点、Linux peer 阳性及完整启动均未执行，生产 Gate 仍 OPEN。

## 正常 D2 launcher 的固定 PID 1 文件系统视图

2026-11-08，基线 `14e3c4848c`。两个固定无参 JNI 入口分别读取调用线程自身视图，以及核对它与可见 PID 1 的视图。两者从实际持有的根目录 fd 取得 dev、inode 和普通 mount ID，从经验证的 procfs 读取实际 mount namespace。PID 1 路径在原 procfs 下持有固定 `1` 目录，并对固定 `root` 叶先做无跟随身份检查，再有意跟随该内核 magic link 获取实际根。PID 1 出生以原始 ticks 读两次，临时 fd 在返回结果前结算实际首次关闭。根和 namespace 在同一观察中复验。采样之间的变更仍须由持续管理面约束覆盖。[Linux proc PID root](https://man7.org/linux/man-pages/man5/proc_pid_root.5.html) · [Linux openat2](https://man7.org/linux/man-pages/man2/openat2.2.html)

原 D2 owner 在首轮初始 PID/user namespace 检查后保存视图，并与已取得的 PID 1 出生核对。末轮固定 systemd 对端检查后再次冷读 boot、进程出生和根视图，匹配原记录后才进入 dropper 检查。目标侧签名视图比较见[Linux 签名宿主视图与采用前拒绝](#linux-签名宿主视图与采用前拒绝)。持续管理面约束和批准 Linux 阳性仍待完成。

Darwin arm64 原生库构建、231 项定向 JUnit、31 项宿主 Rust 测试和 Java 样式通过。9 项 JUnit Linux 条件用例跳过。独立 JVM 使用父 JNI 库验证两个缺失符号明确拒绝，Linux x86_64 `cargo check --tests` 通过。实际 PID 1 root 跟随、同 namespace 的 chroot 和其他 mount namespace 拒绝用例为 NOT_EXECUTED。证据位于 `/Users/liusinan/.codex/artifacts/unified-wal/classic-pid1-filesystem-view-evidence-14e3c484`。

## 正常 D2 launcher 的初始 PID 与 user namespace

正常 schema2 launcher 在两次原 process-birth 核验之后各调用一次 `verifyLinuxInitialNamespaces`，仍在独立 dropper 核验、`childCommand` 和私钥读取之前。原 source/provider 与 deadline 先行；实际读取后复查 deadline 和 owner 健康，再把两个原始值分别与初始 PID inode `0xeffffffc`、user inode `0xeffffffd` 比较，并复查 deadline。缺值、任一不匹配或读取失败沿用原 sticky failure 和排空。没有首值缓存、外部数据入口或新资格对象。D1 和无 Linux 部署路径不调用新 ABI。

native 每次从同一持有并核验的 `/proc` 根 fd 解析固定的 `thread-self/ns/pid`、`thread-self/ns/user`。根以 `O_RDONLY|O_DIRECTORY|O_NOFOLLOW|O_CLOEXEC` 打开，核对 procfs magic、目录和 inode 1。两个 leaf 用 `openat2` 的 `BENEATH|NO_XDEV|NO_MAGICLINKS` 与 `O_PATH|O_NOFOLLOW|O_CLOEXEC` 持有链接本身，允许内核普通 `thread-self` 相对链接，禁止跟随最终 namespace magic link。直接 `SYS_readlinkat(fd, "", ...)` 读取，无 libc 路径回退。18 字节缓冲拒绝截断，仅接受完整 `pid:[N]`、`user:[N]` 和规范非零 u32 十进制；每个 leaf 及根前后核对身份、同一 proc 设备与链接类型，打开的 fd 各关闭一次，首错优先，全部退出结论后才返回两个原始值。

固定 inode 取自 [Linux UAPI](https://github.com/torvalds/linux/blob/v6.17/include/uapi/linux/nsfs.h)；[thread-self 源码](https://github.com/torvalds/linux/blob/v6.17/fs/proc/thread_self.c)、[namespace 链接源码](https://github.com/torvalds/linux/blob/v6.17/fs/proc/namespaces.c)、[openat2(2)](https://man7.org/linux/man-pages/man2/openat2.2.html) 和 [readlinkat(2)](https://man7.org/linux/man-pages/man2/readlink.2.html) 分别说明普通中间链接、leaf 语义、持有 leaf 的 flags 与空路径读取。JNI 只报告调用线程 PID/user namespace 的原始 inode；只有 launcher 比较是否为初始 namespace。这不涵盖 mount/network/cgroup/time namespace、宿主 PID1、systemd manager、episode 或 Unix 隔离。

最终宿主库 4e0a6d2e… 为 354352 字节。native 18 项（15 通过、3 项 Linux 实读跳过）和 server 50 项全通过；Checked JNI 重跑、旧 e3d11649… 库缺新符号拒绝及 Java style 通过。首次 style 的行长失败和首次库结果保留独立输入；最终结果来自 syscall 收紧及测试换行后的独立构建。原 7 项便携 parser 测试通过，最终 parser 和测试片段逐字节相同；Rustfmt 仍仅报告父版本已有差异。Linux x86_64 `cargo check --tests` 通过，仅编译检查。真实 Linux syscall、批准 Linux/vendor source 阳性和 D2 整体资格均未执行；原拒绝、OFF/SYSTEM/PARK 默认值与整体 Gate OPEN 保持。

## 正常 D2 launcher 与可见 PID1 的原始出生 ticks

2026-11-05在父`94bad1e5fbedf9fd8719d66936ed392ecb70fd84`上，正常D2在第一次boot核验后冷读进程出生标记，unit/link检查后第二次boot核验之后再独立读取，均先于dropper/childCommand/私钥。typed入口先`originalLinuxDeployment`；实际native返回后依原绝对期限、资源健康、值与首record对应检查，全部通过才记录本owner首次值。每次均真正冷读，不把在途native认作已被timeout取消。data-only入口只接deadline且最先拒绝production owner，不能预置正式首次值，不造进程许可或改child的Process/Instant/HELLO。

无参`NativeIO.currentLinuxProcessBirth()`返回当前PID及其startTicks、可见PID1的startTicks。两ticks保留unsigned u64原bits的Java long，0合法，不转Instant或假定HZ。一次调用从同一held proc根经openat2读固定self/stat和1/stat；前者resolve0xb保留BENEATH/NO_XDEV/NO_MAGICLINKS，仅允许必要普通self symlink，后者0xf仍禁所有symlink。leaf NOFOLLOW/NONBLOCK、regular/同procdev，4097-byte缓冲探测4096上限并完整读EOF，前后held fd/root复核。只将原boot的根/stat/errno/首close私有体局部共用，不建通用proc框架；每个已开leaf/root各close一次，后错不盖首错，全部释放有结论后才构造JNI long[3]。Boot仍用严格0xf，共享wrong-root诊断及对应测试分类改为proc identity。

[内核self实现](https://github.com/torvalds/linux/blob/v6.8/fs/proc/self.c#L10-L24)按proc superblock的PID namespace计算当前tgid并返回普通相对symlink；self stat PID另须与本次直接getpid相等，PID1 stat须为1，视图不对应拒绝，不接caller PID/path/fd/backend。parser从有界原始字节的最后右括号后核comm尾和前22字段数字语法/范围并取field22，不整行split或取首右括号，comm的空格、多括号、换行及非UTF8不会移位。[stat字段](https://man7.org/linux/man-pages/man5/proc_pid_stat.5.html)的starttime是原始ticks；[内核输出](https://github.com/torvalds/linux/blob/v6.8/fs/proc/array.c#L554-L580)还应用time namespace boottime偏移，故这是当前可见proc/time视图标记。

宿主构建退出0，新库354224 bytes/SHA `e3d116498fff91a1a1b8e41f0c9f224823ed7649add244e8c35b2221235d41b4`。Linux x86_64 cargo check --tests退出0但未链接/执行。精确生产parser及原测试片段在宿主独立编译，仅errno/pid_t类型常量声明，无来源/backend模拟：原boot2＋新proc3共5项通过，含特殊comm、合法0/高位/完整u64、PID视图不符、缺字段、坏数字及溢出。Java native Birth4＋Boot3＋Credentials2＋Kernel3＋Cgroup2共14项为12通过/2 Linux阳性跳过；Darwin实际ENOSYS78、raw值形状/default拒绝和原ABI回归有效。server Trust24（birth5、boot5、kernel/cgroup4、unit/link10）＋Launcher5＋Inputs6＋DeploymentV2 13共48项全过。最终62个唯一JUnit用例为60通过/2跳过，同4项CheckedJNI复验不增加唯一数；精确旧189库在独立VM拒绝新birth符号，旧credential ABI前后有效。

owner mock仅证相同值仍两次调用、三字段独立错配/源失败/缺值/期限前后及原失败/drain。Linux测试仅首读建立来源前分类环境限制，不跳过未证明的openat2 EINVAL；JVM PID、独立self/PID1 stat对照及第二读均在catch外，异常须失败。真实Linux syscall、复用的根/子目录/close测试及source阳性仍NOT_EXECUTED。九code与1452原输入、class/日志/XML/style快照绑定，Java Spotless/Checkstyle退出0，Rustfmt退出1且差异逐行同父；证据在工作区外`/Users/liusinan/.codex/artifacts/unified-wal/native-process-birth-evidence-94bad1e5`。old-library probe原描述误留boot符号名，原命令/输出实为birth probe，原输入保留并另有scope-correction纠正，未重绑旧ABI证据。

正常接线及production-data guard静态核对；data mocks、portable、Darwin拒绝、交叉编译与真实Linux来源分开。PID1标记不证明它是宿主systemd或初始namespace，manager MainPID/ExecMainPID/InvocationID及cgroup归属仍须真实返回值另外关联。未改宿主/proc、挂载、namespace或Installed，不新增systemctl/ProcessImpl/helper执行；D1/noLinux不上新ABI，原childCommand/diagnostics拒绝、完整Unix及parent/child/Main/G1/full-bytes/full16/生产Gate OPEN、默认OFF/SYSTEM/PARK、advanced disabled及完整目标active保持。

## 正常 D2 launcher 的可信 procfs boot ID 冷读

2026-11-05在父`4c4c2102fe9bd4a7bf6c97d8767893fb78c65d9f`上，正常D2在原source/provider、kernel/cgroup检查之后第一次`verifyLinuxBootId(deadline)`，原unit/link检查结束后、独立dropper/childCommand/私钥之前第二次调用。每次均实际重新打开、验证并读取；owner私有首UUID只作本轮比较，不替代第二读或产生boot许可。typed入口先`originalLinuxDeployment`；包内data-only入口只接deadline且最先拒绝production owner，不能提前建立正式首次值。共享体在native返回后核原绝对期限、资源健康、非零值和一致性，全部通过才记首值；失败按原owner失败/drain合同处理，超期不等于已取消在途syscall。

固定无参`NativeIO.currentLinuxBootId`不接caller path/fd/backend，不用Files、realpath、环境或systemd线程缓存补来源。native打开字面`/proc`为O_RDONLY/O_DIRECTORY/O_NOFOLLOW/O_CLOEXEC，对**持有根fd**核fstatfs完整PROC_SUPER_MAGIC及fstat目录/PROCFS_ROOT_INO=1；依据[Linux UAPI根inode约定](https://github.com/torvalds/linux/blob/v6.17/include/uapi/linux/fs.h#L56-L64)。只从根fd用openat2解析固定`sys/kernel/random/boot_id`，how为初版三u64/mode0，BENEATH、NO_XDEV、NO_SYMLINKS、NO_MAGICLINKS全开；[解析约束](https://github.com/torvalds/linux/blob/v6.8/include/uapi/linux/openat2.h#L21-L30)连同同文件系统bind mount也不能穿越。`/proc`本身是proc挂载合法，proc子目录或普通目录不能冒充其根。leaf必须regular且在同一真实procfs/dev；不依赖proc的st_size。38-byte私有缓冲到EOF，要求36-byte小写UUID加LF、拒零/截断/过长/非法格式，与[内核boot sysctl输出](https://github.com/torvalds/linux/blob/v6.8/drivers/char/random.c#L1500-L1527)对应；读后再核held leaf/root身份。

不支持openat2、策略拒绝、错误来源或I/O错误均拒绝，不回退open或规范化路径。每个成功打开的fd由本次调用明确close一次，先leaf再root，任何较早的open/read/校验/close错误均不被后错覆盖；全部close尝试有结论后才构造JNI返回String。Java边界核canonical非零UUID，非Linux实际ENOSYS、旧库缺新符号明确拒绝。D1/noLinux不上新ABI，不初始化ProcessImpl或运行systemctl/helper/子进程，不读写或挂载宿主/proc作fixture。

实际宿主Darwin库构建退出0，库354096 bytes/SHA `189d9294c01d325d65992bc27cd62199d44a59a63ed5fdc3e2e7613d82a299b9`。Linux x86_64 `cargo check --tests`退出0，包含普通临时目录/真实proc子目录不能冒充根及close首错误测试；这些Linux syscall测试**未执行**。生产parser及原测试的精确片段在宿主独立编译，只有errno类型/常量声明，无来源或syscall后备，规范/非法/截断/过长/零值两项通过；不算整个JNI crate测试通过。Java最终native Boot3＋Credentials2＋Kernel3＋Cgroup2共10项，Darwin实际ENOSYS78及default拒绝通过，Linux阳性1项跳过；server Trust19（boot新增5、kernel/cgroup4、unit/link10）＋Launcher5＋Inputs6＋DeploymentV2 13共43项全过。最终共53个唯一JUnit用例，52通过/1跳过；同3项CheckedJNI复验无新唯一数，精确旧9406库在独立VM拒绝新符号且原credential ABI前后有效。新增owner mock仅证两次相同仍冷读、变化/源失败/期限前后/非法值及首失败生命周期，不证明proc内核来源或native取消。

初版Linux阳性测试过宽的错误分类已修正：仅首读的明确环境/来源拒绝可记NOT_EXECUTED，未独立证明环境限制的openat2 EINVAL须失败；[EINVAL也表示how参数错误](https://man7.org/linux/man-pages/man2/openat2.2.html)。独立内核值对照与第二次冷读均在环境catch之外，第一次成功后再读异常必须失败。修正前后四测试阶段及两个style阶段保留各自源码/class/XML/probe，旧PASS不移绑。最终八code与native/server/style快照、1451输入绑定；Java Spotless/Checkstyle退出0，Rustfmt退出1且全部差异与父相同，新区域已格式化。证据在工作区外`/Users/liusinan/.codex/artifacts/unified-wal/native-boot-id-evidence-4c4c2102`。

正常D2接线/生产data guard为静态核对；批准Linux/vendor/source阳性及上述Linux私有fd测试NOT_EXECUTED，交叉编译、host拒绝和portable/mock成功均不替代。两次当前可见真实内核值不证明中间挂载未变、初始PID/mount/user namespace、PID1 host systemd、MainPID/InvocationID、未来episode/跨重启连续性或cgroup归属。原childCommand/diagnostics拒绝保持，完整Unix及parent/child/Main/G1/full-bytes/full16/生产Gate继续OPEN，默认OFF/SYSTEM/PARK、advanced disabled与完整目标active保持。

## 正常 D2 launcher 父进程主 GID 对应

2026-11-05在父`d05698034399ccffa208c221e263f1c1db111b09`上，原`requireRoot()`的三个调用点（初次启动、child创建前、START签发前）不变。每次仍只调用一次原`currentProcessCredentials()`，并将同一不可变读数交给最小包内值比较体。共同real/effective UID均为0要求保持；仅schema2从原D2的`units[launcherUnit].service.group()`取long，realGid与effectiveGid各自须精确相等。D1及无Linux部署不增加主组约束。没有group=0默认、固定unit下标、账号查找、补充组或child recipe主组替代，也没有新JNI、配置、缓存或资格位。

新增2项值测试使用完整验证的D2/outer inputs，将TARGET排序在前、launcherUnit设为1，分别用20和`0xffff_fffeL`：正确双主组通过，即使supplementary不含该组；单独realGid或effectiveGid不符均拒绝，即使supplementary含批准组；两个UID单独不符仍拒绝。旧格式与D1的不同/高位主组通过，UID拒绝保持。先仅提取旧UID-only主体实跑2项，D2反例出现1个预期失败（应抛IOException而未抛），原源码/class/XML保留；修复后server Launcher5＋Inputs6＋DeploymentV2 13共24项全过。另用精确原9406191e…库执行既有`NativeIOCredentialsTest`2项，实际host取值与独立POSIX C/id oracle一致；共26唯一最终用例、0失败/错误/跳过，Java Spotless/Checkstyle退出0，JNI/库未改或重建。

证据在工作区外`/Users/liusinan/.codex/artifacts/unified-wal/classic-launcher-gid-evidence-d0569803`，三阶段1450原输入分别核验，最终两code与host/server/style绑定，旧UID-only red不移绑。三个正常调用点及每次单native读取为实际源码静态核对；host原生取值和D2纯值比较不是root/Installed/source阳性，没有修改现场UID/GID。后两个D2点仍在原childCommand资格拒绝之后，本机未动态到达。

[systemd v255的enforce_groups](https://github.com/systemd/systemd/blob/v255/src/core/exec-invoke.c#L954-L969)在有效主GID上使用setresgid，补充组另行处理；这是参考语义，不是现场manager有效Group或saved IDs证明。本节点只对应当前launcher主组与原D2声明，manager MainPID/InvocationID、saved IDs、user namespace与子进程降权仍待独立验证。批准Linux/vendor正常启动NOT_EXECUTED，原provider/source和childCommand/diagnostics拒绝不松，完整Unix及parent/child/Main/G1/full-bytes/full16/生产Gate继续OPEN，默认OFF/SYSTEM/PARK、advanced disabled和完整目标active保持。

## 正常 D2 launcher 的 systemd 安装符号链接观察

2026-11-05在父`d67206d76ce7af97d5626cc59518a209d8d7685f`上，正常schema2启动在unit文件检查之后、独立dropper/childCommand/私钥之前调用`verifyLinuxInstallLinks(deadline)`。先取owner的`originalLinuxDeployment`，再复用D2有序install path及原unit关系检查：targetUnit、sourceUnit、leaf属于目标Names、依赖目录后缀与TARGET/声明关系均在首目标I/O前核验。冻结units最多32、links最多1024，空links合法；数据入口仅units/links/deadline并共用私有体，不授生产来源或资格。

每条原link只把本叶映射到其原target，复用`observeLinuxPath(ALIAS, ..., present=true)`，不授权加载器alias或其他安装链接作为父路径。原native `observeSymbolicLink`的lstat/readlink/lstat、链接owner/单链接、原始UTF8目标字节/身份检查保持；物理parent身份及受保护祖先链在完整叶观察前后复核，最终NOFOLLOW叶身份再核。结果保留原index、kind/path/raw target/sourceUnit/targetUnit与实际path observation。目标中的相对路径和dot段按原字节比较，不因realpath相同接受不同写法；不打开目标、不要求它的realpath对应fragment，也不创建/修复链接、扫描声明外入口或产生票据。成功只说明逐条磁盘链接本次观察通过，不保证跨链接原子快照或声明外没有入口。

新增5项真实临时链接测试覆盖：UNIT_LOOKUP（含Id同名）、WANTS归属/原序、.././目标及悬空目标可观察、空表不触及目标I/O；后条链接改为同一realpath的不同raw target仍拒绝；native叶观察后父目录mode改为0777拒绝；关系与超限表pre-I/O拒绝；typed缺原source前置拒绝。新增5＋原unit5＋walker/alias替换/owner排空与保护5＋Inputs6＋DeploymentV2 13＋Launcher3共37项全过，0失败/错误/跳过，Java Spotless/Checkstyle退出0。四code及1450原输入与test/style快照、原class/日志/XML绑定，9406191e…库未改/重建；证据在工作区外`/Users/liusinan/.codex/artifacts/unified-wal/classic-install-links-evidence-d67206d7`，旧阶段不移绑。

[systemd 255](https://www.freedesktop.org/software/systemd/man/255/systemd.unit.html)区分load path内alias和外部linked unit。这里不判定manager当前采用哪个unit、lookup优先级或有效依赖；后续实际FragmentPath/有序DropInPaths、NeedDaemonReload、正反依赖及额外激活来源仍需原manager观察与A/文件/B复核。正常typed接线/先后为静态核对，动态走共用数据体；批准Linux/vendor阳性NOT_EXECUTED。不运行systemctl/daemon-reload/helper/backend/diagnostics或显式初始化ProcessImpl，原guard不松；完整Unix资格及parent/child/Main/G1/full-bytes/full16/生产Gate继续OPEN，默认OFF/SYSTEM/PARK、advanced disabled及完整目标active保持。

## 正常 D2 launcher 的 unit 主文件与有序 drop-in 观察

2026-11-05在父`56ccf5c724622240e4c19feedbc088b623b6d679`上，正常schema2启动在helper材料之后、独立dropper/childCommand/私钥之前调用`verifyLinuxUnitFiles(deadline)`。入口先复核owner的`originalLinuxDeployment`，再复用D2解码中的`requireUnitFileReferences`检查整张unit表的fragment/UNIT_FRAGMENT和每个dropIns/UNIT_DROPIN索引；这些检查全部先于目标I/O。随后按原units顺序，逐fragment及其原序drop-in读取对应member的canonical physical路径、long长度和批准SHA。结果保留unit索引、fragment的-1或drop-in位置、member索引、实读路径/摘要/identity/parents；共享member按各引用再次读取，未引用的同role文件不纳入。数据入口仅files/units/deadline并共用同一私有体，不授Installed或资格，不接caller Path、backend或loader aliases。

文件读取复用`hashFile(NONE, bindIdentity=true)`，不新建解析器：checked/chain、owner/mode/ACL/单链接、只读NOFOLLOW、完整流SHA和持有channel前后实际fd dev/ino绑定、首close、原失败/drain和期限保持。NONE的fd错误使用file诊断。每项只是本次读取的磁盘事实，不是跨unit原子快照或manager配置证明。[systemd 255](https://www.freedesktop.org/software/systemd/man/255/org.freedesktop.systemd1.html)的FragmentPath、DropInPaths及NeedDaemonReload属于manager属性；后续仍须取得实际返回值，对照原引用并按冻结的A/文件/B顺序重新核验，不用这次冷读代替。

新增5项真实临时文件测试覆盖：service和target的全部原引用及共享drop-in顺序、未引用缺失文件不读；主文件正确而后置drop-in等长损坏时整次拒绝，已打开三channel各首close/实际关闭、保留原失败并drain；后置drop-in重定向到同字节异inode的真实raw fd时拒绝并关闭；后置错误role/index在首目标I/O前拒绝；typed入口缺原source时pre-I/O拒绝。新增5＋原Trust helper8＋Inputs6＋DeploymentV2 13＋Launcher3共35项全过，0失败/错误/跳过，Java Spotless/Checkstyle退出0。四code、1450输入、原class/日志/XML及精确9406191e…库保存在工作区外`/Users/liusinan/.codex/artifacts/unified-wal/classic-unit-files-evidence-56ccf5c7`；JNI未改/重建，旧PASS未移绑。

正常D2接线及顺序为静态核对，动态测试走同一私有读取体的数据入口；批准Linux/vendor/source阳性NOT_EXECUTED。不读写宿主unit作fixture，不运行systemctl/daemon-reload/helper/diagnostics/backend或显式初始化ProcessImpl；原childCommand/diagnostics拒绝保持。manager有效配置、helper传递加载、完整Unix资格及parent/child/Main/G1/full-bytes/full16/生产Gate继续OPEN，默认OFF/SYSTEM/PARK、advanced disabled及完整目标active保持。

## 正常 D2 launcher 的固定 JDK helper 文件观察

2026-11-05在父`e615f7d893ef54b4dedb570dbabba2914fec5f57`上，正常schema2块在候选全集之后、独立dropper之前，一次调用原`verifyLinuxJdkHelper(deadline)`。只消费原owner的raw javaHome、verifiedProgram和D2表，固定`lib/jspawnhelper`须同时对应program JDK成员、D2 START_CHAIN成员及exact执行context，三索引/长度/SHA/角色先核，再复用原owned ELF2/实际fd、PT_INTERP字面匹配及helper/interpreter末端复核。原观察器未改，不从向量或宿主JDK补材料，也不推断backend实际会执行helper。

本次只接文件观察，不调用`verifyLinuxJdkProcess`/`NativeIOImpl.jdkProcessLaunch`，不显式初始化ProcessImpl或设launch机制属性。[OpenJDK21u 21.0.10 Java初始化](https://raw.githubusercontent.com/openjdk/jdk21u/jdk-21.0.10-ga/src/java.base/unix/classes/java/lang/ProcessImpl.java)调用native init；其[原生实现](https://raw.githubusercontent.com/openjdk/jdk21u/jdk-21.0.10-ga/src/java.base/unix/native/libjava/ProcessImpl_md.c)读取父PATH并通过sigaction设置SIGCHLD为SIG_DFL，是将缓存观察留到需要它的阶段的静态理由，不是目标vendor/现场信号处置证明。本节点不探测/恢复sigaction，不强制FORK、运行helper或解除任何进程guard；不把历史独立VM的backend观察记成本次运行。

既有Trust helper相关8＋Inputs6＋DeploymentV2 13＋Launcher3共30项全通过，0失败/错误/跳过，无新增测试或JNI；Java Spotless/Checkstyle退出0。真实临时文件覆盖精确三索引/NEEDED原序、PT_INTERP有无、同物理异字面值、读中同字节换inode、缺材料pre-I/O及typed缺source拒绝；Launcher独立Main的缺安装拒绝仅作原进程回归，不是backend/正式启动阳性。单阶段1450原输入按snapshot/父Git blob核验，唯一code与test/style绑定，沿用9406191e…库不重建。

证据在工作区外`/Users/liusinan/.codex/artifacts/unified-wal/classic-launcher-helper-evidence-e615f7d8`；静态ProcessImpl.java29123 bytes/SHA `3c501366c6ebfe3699da9a4bc4c68bbe8dd0df48b49d5984efb3c0954549a71f`及ProcessImpl_md.c28806 bytes/SHA `ab8186c096bc6d07d6ab072ce0c4d54379a4061769a2026d56786bcba4122df3`独立保留。正常唯一消费顺序静态核对，动态是原data/shared材料，批准Linux/vendor阳性仍NOT_EXECUTED；helper传递加载闭包、实际backend/执行语义、完整Unix资格及parent/child/Main/G1/full-bytes/full16/生产Gate OPEN，原childCommand/diagnostics拒绝、defaults/disabled及目标active保持。

## 正常 D2 launcher 的全部候选上下文观察

2026-11-05在父`8c2ac1f3ea6893304b3b9d056deec73029d2ef4a`上，正常schema2启动在整表目录检查成功后、独立dropper专项之前，一次调用原`verifyLinuxCandidateContexts(deadline)`。使用原绝对期限，不另调逐context/interpreter入口、不按物理路径合并、不缓存/转发结果。原观察器未改：source/provider门后依原顺序关联全部logical context与实际路径、physical、owned fd、批准SHA和现有ELF2读数，另读固定interpreter，从实读ELF按requester/ordinal取NEEDED名，原重复module声明保持；包括未选中候选在内任一项失败就停止，独立dropper字面解释器及终端身份检查保持。

既有Trust相关14＋Inputs6＋DeploymentV2 13＋Launcher3共36项全通过，0失败/错误/跳过，无新增测试或入口；Java Spotless/Checkstyle退出0。覆盖多logical视图/原ordinal/重复module、未选中候选损坏整次失败、引用pre-I/O及typed缺source拒绝，另含原load-path/dropper真实文件/资源回归。单测试阶段1450原输入与snapshot/父Git blob一致，唯一改动code与test/style绑定一致，沿用9406191e…库不重建。

证据在工作区外`/Users/liusinan/.codex/artifacts/unified-wal/classic-launcher-candidates-evidence-8c2ac1f3`。正常唯一调用及顺序静态核对，动态证据走原data/shared真实临时文件，正式批准Linux/vendor阳性仍NOT_EXECUTED。结果是逐项冷观察，不是跨文件原子快照、namespace/搜索序、候选winner或module已执行；module仍仅声明。本节点不调用helper/diagnostics或启动进程，不松childCommand，parent/child/Main/G1/full-bytes/full16/生产Gate仍OPEN，原defaults/disabled及持续目标active保持。

## 已有 ELF2 消费者的摘要与元数据字节对应

2026-11-05在父`98d5b317d4b3ceb5f60c870fab5ed029179dbf21`上，修正已有normal dropper及其他共享ELF2内容生产路径。原实现先stream SHA，再从同fd positional解析，内容入口可返回A摘要与等长B的NEEDED等元数据；原外层fd/ctime/mtime/path/parents会拒绝许多普通改写，本反例不宣称已绕过生产启动资格。

新`readVersionTwoHashed`先用有界私有区段解析，并按原long文件偏移登记实际读取的header/PHDR/dynamic/字符串区段。capture模式只将独立至多64KiB scratch交给channel，再复制到parser实际使用、同时登记的私有数组；原PHDR128、dynamic4096、NEEDED1024及单字符串4097读取上限保持。随后同fd只顺序扫描一次完整文件，scratch复制到私有块，同一私有块用于区段比较和SHA。区段按offset排序，游标只加入当前块前的区段，active保留跨块项并原位压缩已完成项；重复偏移、重叠、部分read和大空隙不要求每块重扫全表。任何区段不符明确拒绝，未读作元数据的payload仍进入完整摘要。

文件length/流偏移保持long，仅区段/块内差值转int；不按整文件长分配或增加ELF大小上限。只在ELF2内容入口消费配对的Image2/Digest，成功channel.position为length、原size/短读零进展/最终deadline/close归属及hashFile前后真实fd/批准SHA保持。原ELF1/D1和raw readVersionTwo走原positional读取体，不新增整文件hash或移动channel；其他kind、JNI、搜索/候选及launcher guard不扩展，也没有新增正常消费者。

父内容实现的等长有效NEEDED libz→liby实际改盘反例确定取红，A摘要断言通过而元数据变B。修正后新增四项：该边界返回内部一致A；parser后改盘并改写caller保留的所有positional dst仍因私有区段不符拒绝；160KiB合法ELF的字符串表移至65536−16，重复/重叠区段跨块且非元数据payload完整入SHA；同fixture每次边界前读都被限制到65535，真实到边界后才计数，随后仅改顺序流所见offset65536，要求由延续区段发现差异。一次临时突变将active保留条件改为region.end>fileLength，跨块拒绝用例明确失败；生产源码SHA `bfaa4823fa165e10c0aa393d2326fd792fd7551df923b752c288a4ab98a65553`逐字节恢复，突变源/class/XML另存。合法和拒绝场景允许底层合法短读，不要求首次读满。

最终Elf13＋Content15＋Trust相关23共51项全部通过，0失败/错误/跳过，包含新增4项；Java Spotless/Checkstyle退出0。最终生产class与突变前相同、与突变不同，原首3及未收紧夹具cross2的PASS保留各自原测试绑定，突变红不计生产PASS。五阶段各1450原输入按snapshot/父Git blob核验，最终三code与final51/style一致。沿用9406191e…受控库，无native重建或无关矩阵重跑。

证据在工作区外`/Users/liusinan/.codex/artifacts/unified-wal/classic-elf2-hash-binding-evidence-98d5b317`，保留原source/class/日志/XML和恢复记录。动态证据为真实临时文件及受控channel故障、原owner/fd/close/dropper材料回归，正式批准Linux/vendor启动阳性仍NOT_EXECUTED。没有测量超2GiB完整文件或性能，不将160KiB fixture提升为所有文件/平台证明；该结果只保证元数据可由送入本次SHA的同一字节序列解析，不证明实际loader执行/搜索winner/完整资格。parent/child/Main/G1/full-bytes/full16/生产Gate继续OPEN，原拒绝、OFF/SYSTEM/PARK、advanced disabled与持续目标active保持。

## 正常 D2 launcher 的整张加载目录表观察

2026-11-05在父`6f747aa7d07adc6f050f2cc5bec8834ef0acc0c7`上，正常schema2启动在固定conf图之后、dropper之前调用一次原`verifyLinuxSearchDirectories(deadline)`，成功返回才继续。不逐项调用、不遍历policy.defaultDirectories、不缓存或转发返回值，D1及无Linux部署分支按原路径执行。原观察器没有改写：来源/provider/期限门后从原loading.directories取整表，保留原index/raw logical/present与alias；A/B各自共用原固定成员/名称预算，同resolved每遍复用前核身份，比较全部路径、父链与成员，保持受保护缺失、实际空目录、首次close及sticky/drain。

只运行既有Trust相关8＋Inputs6＋DeploymentV2 13＋Launcher3共30项，全通过、0失败/错误/跳过；无新增测试、入口或native接口。覆盖真实临时目录完整成员/alias/absent/empty、两遍变化、跨目录预算和两个iterator关闭、旧首次close失败及原来源不符拒绝；Java Spotless/Checkstyle退出0。单测试阶段1450原输入与父Git blob/快照一致，唯一改动code与test/style对应，沿用9406191e…受控库不重建。

证据在工作区外`/Users/liusinan/.codex/artifacts/unified-wal/classic-launcher-directories-evidence-6f747aa7`，保留原source/class/日志/XML。正常入口唯一调用及先后顺序按实际代码静态核对，动态用例走已有data/shared observer，不能补认原批准D2来源；Linux/vendor正式阳性仍NOT_EXECUTED。这是两次路径与完整成员枚举，不证明directory stream fd、后续稳定性、实际搜索顺序、候选批准或loader winner。原childCommand/diagnostics拒绝及parent/child/Main/G1/full-bytes/full16/生产Gate OPEN，默认OFF/SYSTEM/PARK、advanced disabled及完整目标active保持。

## 正常 D2 launcher 的固定 conf 与实际 include 图

2026-11-05在父`64bc49c1918065a0c78325505699e604f026d062`上，正常D2 launcher在cache之后、dropper/childCommand/私钥之前，从同一原表按exact `/etc/ld.so.conf`、LDCONFIG、format 1选出原index，调用既有typed配置观察器。不选第一个LDCONFIG或固定下标，保留明确file -1与present空文件的区别；向量前插另一LDCONFIG仍选择原固定根。原source/provider门成功后，才进入provider-aware路径及完整include扫描。

每个实际根/子文件的SHA、完整语法验证及include指令改由`Configuration.readHashed`的同一私有image取得。原read与readHashed共用0…1MiB读取/验证，实际channel只见至多64KiB独立scratch，按实际read量复制到私有image；内部指定SHA-256，无caller digest或channel数组别名，无第二份整图。直接read保持原channel位置，Trust内容入口成功position为length，摘要finalization、结果构造和外层结果后均查原期限；空文件摘要/空Cursor有效，NUL、短读、零进展及语法拒绝合同保持。

typed LDCONFIG与包内`readLinuxDeploymentConfigurationGraph(files, Loading, ruleIndex, deadline)`共用一个私有观察主体和原递归扫描器。数据入口只返回图，不能建立source/provider/Installed/ready；没有caller bool/rootPath/预构图/授权参数。冻结成员表后核实际1024上限，复用从完整D2构造器原样提取的`requireConfigurationReferences`，在Path转换/I/O前核索引/角色、alias解算后的exact physical，以及显式absence与file/直接alias冲突，不要求无关ELF上下文。D2在私有graph/scan内部固定将真实Entry/JNI fd身份核验传到每个实际readFile，读前对checked dev/ino、读后对原held fd，再沿原first-close/path/parents/SHA。原D1及旧materials数据入口固定沿原无新增fd ABI合同；两遍完整图、原预算、顺序/重复、MODULE_RUNTIME命中拒绝及owner/sticky/drain保持。

父内容实现的两项独立反例已取红：首次实际读取后等长A→B得到A摘要/B指令；返回后改写保留的dst得到C指令。新增D2数据adapter最初共用旧无fd扫描体，正确child通过、同字节异inode child也被接受，红单独绑定此adapter阶段；修正后在child内容读取前拒绝并实际关闭/drain，不冒称原父生产D2来源阳性。只读评审还发现decoy.conf声明能被物理child.conf绕过，实际图成功的红保留；共享D2配置引用校验后以IOException及零次目标Files调用证明预检拒绝。明确缺失与实际空根、原索引移动、无效引用及正确include均覆盖。

最终Configuration7＋Graph22＋Content11＋Trust相关15＋DeploymentV2 13＋Inputs6＋Launcher3共77项全通过，0失败/错误/跳过，含新增8项；最终Java Spotless/Checkstyle退出0。精确旧75e59684…库另跑5个既有D1图、旧materials和配置用例全过，属于77中的兼容性重复。原首8及76/style绑定修正前源，不移绑最终代码；7个测试阶段各1450原输入按snapshot/父Git blob核验，最终七code与checked-final77、legacy5及final style一致。原9406191e…受控库沿用，没有native重建、ldconfig执行、cache改写或子进程启动。

证据在工作区外`/Users/liusinan/.codex/artifacts/unified-wal/classic-launcher-conf-evidence-64bc49c1`，保留原source/class/日志/XML及各红色阶段。[静态glibc 2.39 ldconfig输入实现](https://raw.githubusercontent.com/bminor/glibc/glibc-2.39/elf/ldconfig.c)原字节35213 bytes、SHA-256 `32752e37eaa6fb3253ca7bb5f1653ef575788cccf47f41780d5ecae865ee0afe`；它与[运行时cache查询](https://raw.githubusercontent.com/bminor/glibc/glibc-2.39/elf/dl-cache.c)是不同观察对象，不证明当前cache由本图生成。正常D2消费顺序为静态核对，动态证据为真实临时图及data/原owner；批准Linux/vendor阳性仍NOT_EXECUTED。加载winner、实际mmap/后续exec、完整资格与parent/child/Main/G1/full-bytes/full16/生产Gate仍OPEN，原拒绝、OFF/SYSTEM/PARK、advanced disabled及持续目标active保持。

## 正常 D2 launcher 的固定 cache 与同次字节及 fd 绑定

2026-11-05在父`a83b22dde9d409bab7977cbdfdb52cd461d24584`上，正常launcher在固定preload缺失检查之后、dropper/childCommand/私钥之前，用原`fixedLinuxCacheRule(original loading)`选择`/etc/ld.so.cache`的CACHE原index并调用既有typed配置观察器。不硬编码表位置、不要求cache一定存在，也不为NEEDED打开候选或计算能力/winner；ld.so.conf图保持独立，未接入本节点。[glibc 2.39运行时cache源码](https://raw.githubusercontent.com/bminor/glibc/glibc-2.39/elf/dl-cache.c)仅为静态依据，不提升运行资格。

CACHE内容原先先stream SHA、再同fd positional重读解析；新的`readHashed`内部创建SHA-256，与原三参`read`共用私有读取/解析，从唯一有界byte image取得完整格式结果与摘要。实际channel只接触至多64KiB scratch，每块拷入私有image，保留短读/零进展/长度/期限；Text切片及摘要共享的原数组不交给caller digest或channel，不分配第二份16MiB整图。直接Cache.read仍不移动原channel，readLinuxContent(CACHE)成功后position为length；摘要finalization、结果构造及外层结果/position完成后均检查原期限。其他内容kind的读取合同保持。

D2 present cache在原`observeLoaderConfiguration`路径上复用私有Entry及sealed JNI：打开后、内容前将真实fd dev/ino与checked physical匹配，读后再核同fd身份，随后沿原首次close、leaf/parents/path/alias/批准SHA检查。旧hashFile入口仅ELF2保留原身份检查，D1 cache没有新增fileIdentity ABI前提。typed CACHE两次独立完整读取后复用`requireLinuxCacheBinding`比较原路径、presence、实际leaf/parents和绑定解析字节的SHA，不再对重复Text执行一次深equals；PRELOAD_ABSENT/LDCONFIG原行为保持。

两个新增反例先在父实现取红：真实首次读取后同长度有效cache A→B，返回A摘要/B解析；只将首次cache open替换为同字节不同inode实际FileChannel时，父实现未拒绝。fd首轮夹具曾重复返回已关闭fd，drain断言干扰原失败；该记录保留，修正为只替换首次open后取得明确“应抛IOException但成功”的红。只读评审先移除caller digest造成的数组外露，再发现channel可保留整图ByteBuffer；扩展同一Content反例，返回后改写保留的duplicate，隔离前再次取红，scratch隔离后通过。

最终Cache16＋Content9＋Trust相关25＋DeploymentV2 12＋Inputs6＋Launcher3共71项全通过，0失败/错误/跳过，包含新增2项。最终Java Spotless/Checkstyle退出0；另用精确旧75e59684…库（337024 bytes，实际导出不含fileIdentity/uname/cgroup新符号）运行两个既有D1 cache用例全过，属于71项中的兼容性重复，不累加唯一数。原2项绿色/style、隔离前71项/旧库2项/style分别绑定各自原版本，不移绑最终代码；8个Maven测试阶段各1450原输入按snapshot/父Git blob核验，最终五code与最终两组测试及style一致。沿用9406191e…受控库，未重建native或重跑无关矩阵。

证据在工作区外`/Users/liusinan/.codex/artifacts/unified-wal/classic-launcher-cache-evidence-a83b22dd`，保留各原源码/class/日志/XML、失败及库输入；静态dl-cache.c原字节16400 bytes，SHA-256 `8c9fcaa06f745976efa7970235d07bb248dc09fc5e2c37a49f0cc35fb3a5e8b0`。正常D2调用顺序按实际代码静态确认，动态证据为真实临时文件/data入口和原owner生命周期，不能补认原批准D2来源；实际Linux/vendor启动仍NOT_EXECUTED。该结果仅为当次材料验收，不证明loader实际mmap、后续exec不变、搜索闭包或完整资格。原childCommand/diagnostics拒绝及整体parent/child/Main/G1/full-bytes/full16/生产Gate仍OPEN，默认OFF/SYSTEM/PARK、advanced disabled及持续目标active保持。

## 正常 D2 launcher 的固定系统 preload 缺失检查

2026-11-05在父`c48530e7db5db39e4d234608c85e31de034e3e7e`上，正常launcher在原D2 provider、uname/cgroup检查之后，dropper、childCommand与私钥读取之前，调用原`verifyLinuxLoaderConfiguration(index, deadline)`。index由同一原D2不可变有界configuration表中的`preloadAbsentConfigurationIndex()`选出，只匹配原字面`/etc/ld.so.preload`、PRELOAD_ABSENT、format 1、file -1；不硬编码表位置，也不接受caller Path、环境或可调根。原构造器的固定规则、唯一性与presence保证保持，未改schema或codec。[glibc 2.39的系统preload读取](https://raw.githubusercontent.com/bminor/glibc/glibc-2.39/elf/rtld.c)是静态依据，不作为运行资格。

生产trust主体未改：原source/provider/deadline门先于目标I/O，复用CONFIGURATION路径walker的两次完整观察与相等比较，保持healthy、原期限、首失败留存及实际排空。只有受保护父链上的可靠缺失才可返回；空文件、目录、末端正常或dangling symlink、权限拒绝和不安全absence父链不能成为“无preload”。不打开/解析现存preload内容，不修改真实`/etc`，没有新JNI、缓存、permit或资格字段。

首轮3项新增测试全部通过，最终Trust相关7＋DeploymentV2 12＋Inputs6＋Launcher3共28项全部通过，0失败/错误/跳过；首3已包含在28中。D2有效向量前插合法MODULE_RUNTIME规则后仍按原index选中同一固定行。Trust新增两项走原legacy表入口与D2共用的实际CONFIGURATION walker：真实空文件/目录/两种链接拒绝、注入AccessDenied不吞错且无内容open；首次实际NoSuchFile后创建空文件，第二遍拒绝，失败保留至drain。既有受保护缺失、不安全父链和typed缺source拒绝覆盖复用。Java Spotless/Checkstyle退出0，两Maven测试阶段各1450输入按原snapshot/父Git blob核验，四个改动code与test/style保持原绑定。沿用原9406191e…受控库，没有重建或重跑native/ELF矩阵。

证据在工作区外`/Users/liusinan/.codex/artifacts/unified-wal/classic-launcher-preload-evidence-c48530e7`，保留原源码/class/日志/XML及静态rtld.c原字节（88632 bytes，SHA-256 `03681f7c7cea6f542a6416e3478a8c6bfe6c3a7969d1bb081dbba4f18395ed49`）。正常D2消费点与先后顺序按实际直线代码静态确认；临时树动态测试不是原批准D2 typed阳性，实际Linux/vendor启动仍NOT_EXECUTED。检查仅表示此时系统preload路径的受保护缺失，不证明已运行父JVM的历史、helper的LD_PRELOAD/--preload、未来exec时不变或完整加载闭包。boot身份的可信procfs来源仍待设计，不能把普通路径UUID读数补成authority；原childCommand/diagnostics拒绝和整体parent/child/Main/G1/full-bytes/full16/生产Gate继续OPEN，默认OFF/SYSTEM/PARK、advanced disabled及持续目标active保持。

## 正常 D2 launcher 的 helper 可能复用检查

在 `258f473584` 的代码上，原 D2 helper 的递归取证增加逐 NEEDED ordinal 的可能复用检查。glibc 在查路径前匹配已加载对象的名称或 SONAME，也可在打开文件后按 file ID 命中并记住请求名。因此每条规则除了查 cache、自身标签、祖先 RPATH 和默认目录，还要核验可能被复用的原 context。[glibc 2.39 名称与 file ID 检查](https://github.com/bminor/glibc/blob/glibc-2.39/elf/dl-load.c)

原 owner 从已核验的可达 ELF 保留原逻辑名称、SONAME、原请求名称和与 owned fd 对照过的 dev/ino。逻辑名称、实际 SONAME 和原候选边可能引入的普通请求名称都进入可能命中范围。相同 dev/ino 共享名称关系，各 context 仍须在本 ordinal 单独获批。helper 和已验 PT_INTERP 文件同样参与，若命中解释器且未匹配其原声明 context 则拒绝。整组规则检查结束后再做末轮路径身份复核，保留首次关闭、首错和原期限。范围包含未选中候选，允许保守拒绝。

真实文件夹具使前一库的 SONAME 等于后一依赖名。旧实现验证两个实际路径后错误放行，修复后仅显式批准前一库原 context 才通过。另覆盖同 inode 的两个逻辑 context、同字节不同 inode、未选中候选和固定解释器，以及既有实际文件替换与关闭。首轮扩展测试夹具误用 Java 21 的集合 API，在 release 17 编译时退出。修正后最终 107 项定向 JUnit 全部通过，Java Spotless 与 Checkstyle 通过。沿用封存的 macOS arm64 JNI。证据位于 `/Users/liusinan/.codex/artifacts/unified-wal/classic-d2-helper-reuse-evidence-258f4735`。

本轮覆盖的是已实读文件范围内的可能复用约束。实际初始 namespace 与 vDSO、加载先后和选择、初始化行为、执行期间父进程环境及批准 Linux/vendor 实跑仍未验收。首次 diagnostics、child 和 handoff 的执行门继续拒绝。

## 首次 D2 loader diagnostics 的来源审计

以 `1199e85016c` 为代码基线。正常启动从独立的 `ControlledClassicLauncherMain` 转入 launcher worker。它先由纯 JDK preload 检查环境快照、参数和固定材料，再核验固定 JNI 的长度、摘要和路径后调用 `System.load`。随后校验完整 JDK 树、原 classpath、配置和 D2 描述，核对实际进程及 provider，再读取平台、cgroup、加载材料、unit 和 systemd 私有 socket 的 peer。此时还未创建 ZooKeeper session、普通 child 或 systemctl 查询。JNI 的 `jdkProcessLaunch` 读取 bootstrap `ProcessImpl` 的静态字段，可能触发其初始化。正常路径在该调用前后各采样一次 libc `environ`，再检查 helper 图。正常 launcher 尚未调用 `verifyLinuxLoaderDiagnostics`，该入口也会在构造 ProcessBuilder 前拒绝。

Corretto 参考 tag `21.0.10.7.1` 对应 `8fc3d1e215eb50da15e9f27a934a335081456c7d`。`ProcessImpl.init` 读取 PATH 并设置信号处置。若启用现有 Output 路径，两个固定管道线程会先启动，之后清空目标环境并调用 `ProcessBuilder.start`。Linux POSIX_SPAWN 先把父 libc `environ` 交给 `jspawnhelper` 的 exec，再经管道传递目标 env，后者的环境替换发生在已启动的 helper 内。Java `System.getenv()` 只反映缓存的环境，两个 native 空采样只约束各自观察点。两者都无法约束 JVM 线程或 native 代码在实际 spawn 期间的写入。[ProcessEnvironment](https://github.com/corretto/corretto-21/blob/8fc3d1e215eb50da15e9f27a934a335081456c7d/src/java.base/unix/classes/java/lang/ProcessEnvironment.java#L68-L99) · [ProcessImpl](https://github.com/corretto/corretto-21/blob/8fc3d1e215eb50da15e9f27a934a335081456c7d/src/java.base/unix/native/libjava/ProcessImpl_md.c) · [helper 的 exec 环境](https://github.com/corretto/corretto-21/blob/8fc3d1e215eb50da15e9f27a934a335081456c7d/src/java.base/unix/native/libjava/childproc.c#L240-L280)

该参考构建的 JLI 在特定 `LD_LIBRARY_PATH` 分支中执行 `putenv` 和 re-exec。glibc 构建且起始环境无该项时跳过这条分支。无 splash 参数的固定 class 入口排除了 splash 变量的加入，启动清理仍调用 `UnsetEnv`，缺失项在参考 glibc 中无需修改环境表。现有 JVM 参数白名单拒绝启动 agent 和 launchMechanism override，也没有显式关闭 attach 的选项。参考 HotSpot 默认允许具备相应凭据的客户端触发 attach 并加载动态 agent。即使系统服务是 root，也需要对被允许的 root 管理路径给出约束。仓库当前 Rust JNI 的环境取证使用内核复制读取指针。在此次审计涉及的 JNI 源中未见环境写入调用，但这个检查没有覆盖链接依赖、`System.load` 触发的 native 构造器、目标 JVM 实际载入的库或运行期 agent。把这些入口与准确构建及运行约束绑定是前置条件。[JLI 的 library path](https://github.com/corretto/corretto-21/blob/8fc3d1e215eb50da15e9f27a934a335081456c7d/src/java.base/unix/native/libjli/java_md.c#L223-L249) · [JLI 的启动清理](https://github.com/corretto/corretto-21/blob/8fc3d1e215eb50da15e9f27a934a335081456c7d/src/java.base/share/native/libjli/java.c#L2265-L2272) · [glibc unsetenv](https://github.com/bminor/glibc/blob/glibc-2.39/stdlib/setenv.c#L220-L255) · [HotSpot attach 与 agent 默认值](https://github.com/corretto/corretto-21/blob/8fc3d1e215eb50da15e9f27a934a335081456c7d/src/hotspot/share/runtime/globals.hpp#L1777-L1784) · [Linux attach 凭据检查](https://github.com/corretto/corretto-21/blob/8fc3d1e215eb50da15e9f27a934a335081456c7d/src/hotspot/os/linux/attachListener_linux.cpp#L361-L377)

现有格式可绑定受保护的原 `.recipe`、`.program`、`.launcher`，以及 JDK/JAR/JNI 与 D2 文件的长度和摘要。provider rule 1 约束默认文件系统实现，loader 版本文字与批准文件摘要也各有边界。审计所见 Linux program 向量使用 `21-test` 和 `test-vendor`，D2 向量的 `glibc-2.39-approved` 是合成值。既有可执行 JNI 回执绑定 macOS arm64 库，Linux 交叉检查仅验证编译。当前没有对应目标 Linux 安装的批准 JDK、glibc、JNI 和依赖的二进制到源码与构建记录，也没有绑定实际载入路径和环境写入约束的运行证据。后置文件哈希依赖原 JVM 启动信任，不能回溯证明其进入 Java 前的行为。

首版可限定普通 NEEDED 和固定绝对路径，并在得知 active hwcaps 前保守核验全部内置候选位置及相关 cache 行。仍需把真实初始 main、解释器、vDSO 及已加载对象的名称、SONAME 和 file ID 纳入逐 ordinal 的可能复用检查。例如，前一条请求装入一个具有另一 SONAME 的库，后一条请求可能在访问其已批准路径前直接复用前者。现有递归路径检查和纯数据 `SegmentClassicLinuxLoadedNames` 未闭合此行为。候选的 native 初始化及它能触达的动态加载入口也须有准确构建的行为依据。仅当这些范围和执行期环境均已闭合，才可由原 Output 执行单一已核验 loader 的 `--list-diagnostics`。诊断输出随后才能用于限定范围的选择核对。批准 Linux 实跑及反例均为 NOT_EXECUTED，当前只读审计保持现有 guard 和整体 Gate。[glibc 名称及 file ID 复用](https://github.com/bminor/glibc/blob/glibc-2.39/elf/dl-load.c) · [glibc 初始化入口](https://github.com/bminor/glibc/blob/glibc-2.39/elf/dl-init.c) · [固定 diagnostics 分支](https://github.com/bminor/glibc/blob/glibc-2.39/elf/rtld.c#L1376-L1408)

## 正常 D2 launcher 的祖先 RPATH 候选检查

在原 cached helper 可达声明图中，每个请求者使用有界反向遍历找出能到达它的 context，排除自身。共享和环按 context 索引去重。这个保守范围包含未选中分支，环也可能纳入实际加载时不会成为祖先的对象。无法核验这些对象带来的目录或候选时拒绝启动。

请求者的实际 ELF 只要存在 RUNPATH，祖先 RPATH 阶段即跳过，空 RUNPATH 也适用。祖先存在 RUNPATH 时忽略其自身 RPATH，并继续向上遍历。目录来自原 owner 实读 ELF 中的有效 RPATH，并在使用前和整组末轮复核来源身份。沿用原绝对路径、DST、CWD 和目录声明规则。 [glibc 搜索分支](https://raw.githubusercontent.com/bminor/glibc/glibc-2.39/elf/dl-load.c) [glibc 双标签规则](https://raw.githubusercontent.com/bminor/glibc/glibc-2.39/elf/get-dynamic-info.h)

每条规则使用当前请求者的原始 `DT_NEEDED` 序号及批准集合，检查祖先目录下现有的四个固定候选位置。先前 cache、自身路径标签和默认目录检查仍参与同一规则的完成判断。保留路径、fd、SHA-256、真实首次关闭与首错纪律。工作顺序仅用于取证。

真实文件反例在补齐检查前放行了未批准间接库，补齐后拒绝。最终 77 项定向 JUnit 和 Java 样式通过，覆盖空 RUNPATH、经 RUNPATH 祖先继续向上、同一路径在不同请求中的独立批准、同字节来源替换及原关闭失败。沿用已封存的宿主 JNI。实际加载者链、对象复用、token/hwcap 选择、执行环境连续性及批准 Linux/vendor 执行为 NOT_EXECUTED。证据位于 `/Users/liusinan/.codex/artifacts/unified-wal/classic-d2-helper-inherited-rpath-evidence-0f039e2c`。

## 正常 D2 launcher 的 helper 可达依赖候选检查

正常 D2 在原 JDK backend 与 native 环境采样之后，以原 owner 已绑定的 cached `jspawnhelper` 为根。复核其原 program/deployment 声明、路径与 PT_INTERP。解释器继续要求现有 self-contained ET_DYN 条件。沿每条原 `NeededRule.candidates` 的 context 索引求可达范围，包括未选中的候选。每个 context 只入队一次，同文件的不同逻辑路径及同名依赖的不同 ordinal 各自检查。队列与规则范围沿用原表界限。

按原 context 与 ordinal 顺序实际读取可达 ELF，包括零 NEEDED 叶子。每条规则用该请求者的实际 ELF 复用 cache A/B、自身 RPATH 或 RUNPATH、默认目录三段检查，并核对本规则的批准候选。保留原 NODEFLIB、路径与 fd 身份、SHA-256、权限和实际首次关闭要求。组末复核原 helper、解释器及其余已观察 context 的路径身份。独立的 ModuleRequest/dlopen 表不参与这次静态依赖遍历。

55 项定向 JUnit 和 Java 样式通过。新增临时 ELF、目录和 cache 用例覆盖未选中分支的间接未批准库、共享和环、同文件的独立逻辑 context、零 NEEDED 叶子及同字节换 inode。沿用已封存的宿主 JNI。批准 vendor/Linux 执行及实际加载选择为 NOT_EXECUTED。证据位于 `/Users/liusinan/.codex/artifacts/unified-wal/classic-d2-helper-dependencies-evidence-6abcc85c`。

检查顺序用于文件取证。祖先 RPATH、实际 namespace 和加载者链、已加载对象的名称与文件身份复用、hwcap/ISA/token 来源、实际候选选择及执行期间 parent libc environ 仍待验证。loader diagnostics、systemd 查询、child 启动和 handoff 继续受原执行条件约束。 [glibc 路径与已加载对象](https://raw.githubusercontent.com/bminor/glibc/glibc-2.39/elf/dl-load.c) [glibc 依赖装载](https://raw.githubusercontent.com/bminor/glibc/glibc-2.39/elf/dl-deps.c)

## 正常 D2 launcher 的调用线程 cgroup 成员关系

两轮原 cgroup 启动检查均在已有 source、provider、deadline、目录类型及保护链核验后，从同一个 held root 取得组合观察。原 JNI mount 查询仍独立可用。组合 JNI 只扫描一次 mountinfo，并在同一个验证过的 procfs 根下两次冷开固定 `thread-self/cgroup`，沿用初始 cgroup namespace 和 mount namespace 前后核验。

每次读至实际 EOF，只接受一行完整的 `0::/…` 加 LF。路径最多 4095 字节，组件最多 255 字节，限受控 systemd 名称字符及字面的 `\xNN`。冒号和字面反斜线保持原值。额外 hierarchy、非规范路径、截断、deleted 后缀和超限均拒绝。内核按读取线程的 cgroup namespace 写出原路径。 [Linux cgroup 源码](https://raw.githubusercontent.com/torvalds/linux/v6.17/kernel/cgroup/cgroup.c)

除借用原根的 `/` 特例外，每次均从 held root fd 以 `openat2` 相对打开成员目录，禁止越界、符号链接和跨挂载解析。成员 fd 必须报告实际目录、完整 cgroup2 magic，并通过设备、inode 和原普通 mount ID 核验。两次 proc 读取的路径须一致。首个成员 fd 仍持有时再次解析同一路径并核对目录身份，临时 fd 首次关闭成功后才返回。原 trust 留存首轮的路径、目录和挂载观察，末轮不匹配时记录失败。 [Linux openat2 UAPI](https://raw.githubusercontent.com/torvalds/linux/v6.17/include/uapi/linux/openat2.h)

定向 JUnit 35 项中 33 项通过、2 项 Linux 实读跳过。宿主 Rust 解析与值检查 10 项通过，覆盖根路径、冒号、字面转义、额外 hierarchy、截断和路径长度边界。Checked JNI、旧库缺新符号拒绝、Java 样式与 Linux x86_64 交叉检查通过。实际 Linux membership、成员目录 rename/迁移、bind/符号链接故障和完整 D2 启动均为 NOT_EXECUTED。证据位于 `/Users/liusinan/.codex/artifacts/unified-wal/classic-d2-cgroup-membership-evidence-383bd4d5`。

结果限定于执行 JNI 的线程及两轮采样。成员目录 fd 的持有区间限于一次观察。整个进程的成员关系、systemd ControlGroup、episode 长期持有与排空、retirement 和生产 Gate 仍待独立验证。 [cgroup v2 的线程模式](https://docs.kernel.org/admin-guide/cgroup-v2.html#threads)

## 原 cgroup.events 当次观察

2026-11-08，基线 `759c638705`。`NativeIO.linuxCgroupEvents` 通过 `SegmentOwnedFiles` 的原私有 Entry 和 JDK 原 fd monitor 借用一个目录 fd。核对完整 cgroup2 magic、目录类型和 dev/ino/普通 mount ID。每次以 `openat2` 打开固定 `cgroup.events`，限制 `BENEATH | NO_XDEV | NO_SYMLINKS | NO_MAGICLINKS`。读取真实 EOF 后复核持有目录和实际叶，再从同一目录重开叶名核对身份。临时 fd 的首次关闭成功后才能返回，原 owner 拒绝迟到结果。

总长上限 4096 字节，单行上限 256 字节，`populated` 必须恰好一项且为 ASCII `0` 或 `1`。支持任意行顺序，校验 `frozen` 的 0/1 及有界合法附加字段。缺项、重复、截断、坏格式、权限错误和非支持平台均明确失败。层级根缺少该文件时保留 `ENOENT`。内核的 `0` 表示当次读取所见的该 cgroup 及后代没有活进程。[Linux cgroup v2 文档](https://docs.kernel.org/admin-guide/cgroup-v2.html) · [Linux cgroup.events 实现](https://raw.githubusercontent.com/torvalds/linux/v6.17/kernel/cgroup/cgroup.c)

结果绑定原 fd 指向的对象。下一步由实际 manager `ControlGroup` 和原 episode owner 同时持有固定层级根与 episode 目录，从原根重开批准路径以复核身份。真实进程正常退出、组件排空、注册消失、持久退休和连续隔离仍需组合验证。`requestHandoff` 和 Linux `bound` 前的拒绝继续生效。

Darwin arm64 原生库构建成功，34 项宿主 Rust 测试与 274 项定向 JUnit 通过，7 项 Linux 条件用例跳过。独立 JVM 使用原 JNI 库确认新符号缺失时明确拒绝，旧目录查询继续可用。Linux x86_64 `cargo check --tests` 通过。首次 Checkstyle 发现一处测试行长，修正后新测试复验及 Spotless、Checkstyle 均通过。真实 cgroup 子树的空/占用/退出后再空、同路径重建、bind 覆盖及迟到内核样本仍为 NOT_EXECUTED。证据位于 `/Users/liusinan/.codex/artifacts/unified-wal/classic-cgroup-events-evidence-759c6387`。

## 正常 D2 launcher 绑定原 cgroup 挂载记录

首次取得固定 cgroup 目录与后续复核时，原 trust owner 先完成目录类型、保护链和 dev/ino 检查，再从同一个原 fd 取得挂载观察。独立 JNI 在原 FileDescriptor monitor 内用 `statx(AT_EMPTY_PATH)` 要求目录类型、inode 和普通 `STATX_MNT_ID`，并和同 fd 的 `fstat` 对照设备与 inode。固定 `/sys/fs/cgroup` 在观察窗口前后各做一次 NOFOLLOW、NO_AUTOMOUNT 的路径 statx，必须与原 fd 的设备、inode 和 mount ID 一致。同 inode 的另一挂载不能替换原对象。 [Linux statx](https://man7.org/linux/man-pages/man2/statx.2.html)

从同一个验证过的 procfs 根读取固定 `thread-self/ns/cgroup`、`thread-self/ns/mnt` 和 `thread-self/mountinfo`。要求初始 cgroup namespace inode `0xEFFFFFFB`，因为 cgroup 根字段由内核按调用线程的 cgroup namespace 计算。以普通 mount ID 选择唯一完整记录，核对设备号、类型 `cgroup2`、挂载点 `/sys/fs/cgroup` 和根 `/`。按字节扫描至实际 EOF，接受总量上限 1 MiB、单行上限 64 KiB，跳过未知 optional 字段。临时 fd 的首次关闭在返回前结算，末轮复核原挂载 ID、父挂载 ID 和观察到的 mount namespace inode。 [mountinfo 格式](https://man7.org/linux/man-pages/man5/proc_pid_mountinfo.5.html) [cgroup 根路径实现](https://raw.githubusercontent.com/torvalds/linux/v6.17/kernel/cgroup/cgroup.c)

定向 JUnit 35 项，33 通过、2 项 Linux 实读跳过。宿主 Rust 的 7 项解析与值检查覆盖转义和非 UTF-8 无关路径、未知标签、重复或缺失记录、非根路径、截断与边界、同 inode 不同 mount 和嵌套 namespace 拒绝。宿主 JNI、旧库缺新符号拒绝和 Java 样式通过。Linux x86_64 交叉检查只编译。真实 Linux statx、固定 mountinfo、bind/namespace 故障场景及批准环境完整 D2 启动为 NOT_EXECUTED。证据位于 `/Users/liusinan/.codex/artifacts/unified-wal/classic-d2-cgroup-mount-evidence-8008bd80`。

检查结果限定于两轮观察时的 mount view。宿主 mount namespace 资格、其他挂载别名、systemd ControlGroup、episode 成员与退出、retirement 和 `populated=0` 仍待独立验证。D2 handoff 和生产 Gate 继续等待端到端证据。

## 正常 D2 launcher 持有并复核 cgroup 目录

正常 D2 在原 provider、source 和期限校验后，沿用精确的 uname 比较，再取得固定 `/sys/fs/cgroup`。JNI 在原 bootstrap FileChannelImpl 的 FileDescriptor monitor 内，对同一 fd 调用 `fstat` 和 `fstatfs`，要求实际目录并报告 dev、ino 和完整 `f_type`。原 `fileIdentity` 仍只调用 `fstat`。旧路径级 ABI 保留，D2 从持有的 fd 取得类型。 [Linux statfs 手册](https://man7.org/linux/man-pages/man2/statfs.2.html)

路径组件要求 canonical、目录、UID 0 且 group/other 不可写。各组件比较稳定的 dev、ino、UID、GID、mode 和目录类型。根目录 `/` 沿用原 ACL 查询。`/sys` 与 `/sys/fs` 各以原 owner 登记的短期只读 NOFOLLOW fd 绑定路径身份和完整 SYSFS_MAGIC，并结算实际首次关闭。`/sys/fs/cgroup` 的原 fd 必须对应路径身份且完整类型为 CGROUP2_SUPER_MAGIC。sysfs 和 cgroup2 的 POSIX ACL 规则只用于这三个固定组件，依据批准内核基线的 kernfs 实现。 [kernfs 目录操作](https://raw.githubusercontent.com/torvalds/linux/v6.17/fs/kernfs/dir.c) [kernfs xattr handlers](https://raw.githubusercontent.com/torvalds/linux/v6.17/fs/kernfs/inode.c)

两轮观察均比较 fd 查询前后的路径链。第二轮在后续 boot、process、namespace 核验之后、systemd peer 之前，重读原 fd。正常子项变化可改变目录大小、链接数和时间戳。若固定路径被替换或保护字段改变，原 trust 记录失败，原句柄仍由该 owner 的实际 drain 结算，首次关闭失败保留。

26 项定向 JUnit 全部通过，其中 7 项 native、19 项 server。macOS 的真实目录用独立 C 程序对照 dev、ino 和 f_type。服务端覆盖同一 fd 复核、路径替换、坏路径拒绝、在途读取与实际关闭的排他、首次关闭失败。Checked JNI、旧库缺新符号拒绝和 Java 样式通过。Linux x86_64 交叉检查只编译，批准 Linux 固定 cgroup2 路径实读为 NOT_EXECUTED。证据位于 `/Users/liusinan/.codex/artifacts/unified-wal/classic-d2-cgroup-root-owner-evidence-3ecbc046`。

目录观测限于 launcher 当前 mount view。实际 mount/cgroup namespace、bind 子树、systemd ControlGroup、episode membership 和 `populated=0` 仍待独立核验。D2 handoff 和生产 Gate 继续等待批准 Linux 端到端证据。

## D2 当前可见 cgroup 标准路径的文件系统类型

以下记录原路径级 ABI 的历史验收。正常 D2 的当前调用点见上节。

2026-11-05在父`9500b3f79edcf5064a545d99305cfc2c343b7eb5`上，原NativeIO/Impl/Jni/Rust新增无参currentCgroupRootFileSystemType。Linux只对字面`/sys/fs/cgroup/`调用一次libc::statfs，保留尾slash目录语义，失败立即保存并传播errno，完整f_type转jlong；非Linux/default明确unsupported，旧JNI缺symbol保留LinkageError cause。没有任意路径参数、通用statfs DTO、fd打开/关闭、外部命令、proc/mountinfo fallback或cache，也不改原ACL/readlink及CPU拓扑的best-effort读取。

原owner核验更名为verifyLinuxKernelAndCgroupRoot并返回void：同owner原D2 source/provider/deadline→一次uname及原四字段精确比较→期限检查→一次固定路径statfs→完整long精确等于`CGROUP2_SUPER_MAGIC=0x63677270`→healthy/deadline。没有返回可转移资格、状态字段或重复uname；原launcher仅D2在provider后/dropper与私钥之前调用，旧描述符跳过。v1、tmpfs、sysfs、未知/零/负值和低32位相同但高位不同者均拒绝，不探测unified/systemd子挂载升级hybrid。[systemd v255对应路径检查](https://raw.githubusercontent.com/systemd/systemd/v255/src/basic/cgroup-util.c)及[Linux UAPI magic](https://raw.githubusercontent.com/torvalds/linux/v6.8/include/uapi/linux/magic.h)仅作静态依据，本实现不复用其缓存或完整布局分类。

新宿主库353984字节，SHA-256 `9406191eb9312ddae13f52be174cf2a24344b1da109c15ab65eda4f06f3e3fc2`。新native2及含原uname3的相关5、server kernel/cgroup4＋Inputs6/DeploymentV2 11/Provider3/Launcher3共27，合计32项当前JUnit全通过，0失败/错误/跳过；首2和checked-JNI重复2不再累计。宿主实际执行的是macOS native unsupported（errno78），不是Linux statfs或cgroup2阳性。Linux默认JAR路径会运行独立C同路径statfs参考，分别比对完整type或errno，只有实际magic命中才记录该条件阳性；本机Linux参考/实跑NOT_EXECUTED。`-Xcheck:jni`同2通过/无native warning；旧精确804093b0库的独立VM中，原uname在新symbol缺失拒绝前后均可用，cause为UnsatisfiedLinkError，旧库未改。

host build与Linux x86_64 cargo check均退出0，后者仅编译/未链接执行；Java Spotless/Checkstyle退出0，cargo fmt1仅保留与父相同的4项旧credentials格式差异。未重跑无关Rust lib test/字段unit、旧ELF/solver或完整native矩阵，没有创建/写入cgroup或改变系统服务/配置。证据`/Users/liusinan/.codex/artifacts/unified-wal/native-cgroup-root-evidence-9500b3f7`保留源/class/日志/XML、构建输入和旧库probe；三Maven阶段各1450输入按原snapshot/父Git blob核验无错配，8个改动源与各test/Java style一致，新库另绑定4项原Rust构建输入。双lsof/identity/SHA核验后仅清2个自有Cargo target的447文件路径/91410580逻辑字节（物理回收未测）和3个raw scope的6目录/17文件路径/1062404逻辑字节；源码、库、class和归档硬链接保留、raw数据块回收0，共享target/用户文件未清理。

该检查只表示固定目录路径当次解析到的文件系统类型，不能辨认cgroup2被bind的子树、cgroup namespace根、宿主其他v1 mount、controllers、父membership/写者、独立episode对象或populated=0。实际Linux/原D2 source阳性仍NOT_EXECUTED；episode对象持有/身份/权限/连续性、cgroup.procs及排空后续另验，不将Java常量或macOS拒绝当作运行证明。原childCommand/diagnostics及各整体Gate仍OPEN，完整16步active，无models/性能结论，默认OFF/SYSTEM/PARK与advanced disabled保持。

## D2 运行内核标识与受控 JNI

2026-11-05在父`1dbbf7943ab14195d0d35bfed7291512bfa61f09`上，原NativeIO/Impl/Jni/Rust新增无参currentKernelIdentity，返回sysname、machine、release、version四个不可变字符串。原受控库只调用一次libc::uname，失败立即保存errno；按各实际libc字段数组的长度找首NUL，拒绝缺失/空串/坏UTF8/未终止字段，保留完整空格、标点及Unicode，不输出nodename/domainname。default明确unsupported，Impl拒绝未知结果并在旧JNI缺symbol时保留LinkageError cause，没有JVM属性、外部命令或procfs生产fallback，也不新增持久格式。

正常launcher仅D2在verifyLinuxProvider之后、dropper/childCommand/私钥/ZK之前调用原owner.verifyLinuxKernel(deadline)。原source/provider/原期限及紧邻native调用的期限检查先于从final nativeIo取数；只按sysname=`Linux`、machine=`x86_64`及原D2.Platform的kernelRelease/kernelVersion逐字相等，再检查原healthy/deadline。没有trim、大小写/amd64转换、版本推断、caller kernel结果参数、缓存或第二套资格状态；原同步/first failure/close纪律保持。小型值比较函数只供同一入口及数据fixture使用，给定Linux DTO不补认真实来源，旧描述符跳过此检查。

最终新宿主库353840字节，SHA-256 `804093b036652cf71b7b5662f79f9073a3b0086e373086e86f8a9c6e296882ba`；本机真实uname为Darwin/arm64，与独立`/usr/bin/uname`四个单项输出逐字相等，只移除命令自身最后LF，不trim版本。新native3及含原Credentials2/FileIdentity4的相关9项全通过；server新kernel3加Inputs6/DeploymentV2 11/Provider3/Launcher3共26项全通过，均0失败/错误/跳过，共35项当前JUnit，首3和checked-JNI重复3不再累计。`-Xcheck:jni`同3通过/无native warning；独立VM加载精确旧sealed70bb，原credential调用在新symbol拒绝前后均可用，新查询因缺symbol保留UnsatisfiedLinkError cause，旧库未改。没有重跑Trust整类、旧ELF/solver/完整native矩阵。

评审前未执行的static native stubbing已移除，Linux默认打包JNI的实际对照不再因缺override而跳过；未为mock注入生产JNI。首宿主构建在旧credentials格式被formatter展开的原源码上退出0，库与快照单列保留，不移绑恢复后的源；恢复父格式后的final host build退出0，新库绑定最终Rust原源。Linux x86_64 cargo check退出0，只编译/未链接运行，真实Linux/获批D2 source阳性NOT_EXECUTED。原Cargo lib test因测试可执行链接收到动态库`-install_name`参数退出101、零用例；未改共享build.rs，逐字节抽取生产kernel_field及原test module，用独立rustc test binary完成2项字段边界测试，只算字段逻辑。Java Spotless/Checkstyle退出0；实际cargo fmt check退出1，仅保留与父相同的4项credentials既有格式差异，新函数没有新增差异。

证据`/Users/liusinan/.codex/artifacts/unified-wal/native-kernel-identity-evidence-1dbbf794`保留原构建/失败/source/class/log/XML与旧库probe；三Maven阶段各1449输入按原snapshot/父Git blob核验无错配，8个改动源与各test/Java style一致，新库另按最终4项Rust构建输入绑定。原字段测试依赖rlib与测试binary保留，Cargo失败不改写为通过。双lsof/identity/SHA核验后，仅清理本次3个Cargo target的691文件路径/168759068逻辑字节（实际物理回收未测），以及3个raw scope的6目录、18文件路径/1072487逻辑字节；原库/新库、源码、class、日志、输入及归档硬链接保留，raw regular-file数据块回收0，共享target/用户文件未清理。

该条件只证明当前调用进程的uname返回与D2原批准文字相等。[uname接口](https://man7.org/linux/man-pages/man2/uname.2.html)及[Linux v6.6 newuname](https://raw.githubusercontent.com/torvalds/linux/v6.6/kernel/sys.c)只作静态语义依据；后者复制调用者UTS视图，personality可影响release/machine，不据此证明宿主PID/user/UTS namespace、硬件、build身份、cgroup2/ext4、systemd或canary。实际Linux资格/cgroup producer、JDK/helper/父环境及完整启动仍OPEN，原childCommand/diagnostics拒绝及parent/child/Main/G1/full-bytes/full16/生产Gate不变；无models/性能结论，完整目标active，默认OFF/SYSTEM/PARK与advanced disabled保持。

## Launcher 的 D2 dropper 与第一条解释器材料关联

2026-11-05在父`a14d1025595a91e7a306a5bcdeb86cd69f405d4b`上，现有SegmentClassicLauncher.startup在原verifyLinuxProvider之后、loadPrivateKey之前调用的verifyLauncherDropper，其D2分支改接已有owned ELF2。分支同时检查owner原descriptor和caller的schema，任一为D2都必须先匹配同一原input引用，再通过originalLinuxDeployment/provider/source/原deadline及recipe；原owner绑定D2后，另一个legacy或同值重解码descriptor不能降级。公共dropper path/length/SHA继续由原Inputs构造绑定固定isolation.dropper，不建立第二份权威。旧v1及旧deployment schema一保留原Linux/Mach短头合同和流程。

固定物理DROPPER member经原表引用/role预检，使用原索引和ELF声明，沿observeLinuxElfMember同一个owned fd完成完整SHA、ELF2、前后fstat、首次真实close及原身份/父链/权限/单链接/长度检查；D2不再另开fd读32字节。原ELF64 little-endian x86-64、type/非零entry、NEEDED顺序、两tag及flags合同保持。不按dropper的alias Context挑执行路径，也不创造context=-1代表dropper。

从本次实际ELF2读取PT_INTERP。存在时须字面等于原loading.interpreter，不能仅resolved或同inode相同；不符在解释器I/O前拒绝。匹配后沿既有固定interpreter context=-1、loading.loader、aliases和owned ELF2取得第一条材料关联。缺PT_INTERP不探测loader，但仍保留并核对原NEEDED，不判断自包含。跨文件读取后复查原dropper physical的完整identity/父链，及解释器原logical/alias摘要/叶identity，再健康/期限。独立原members/Loading/index seam共用同一private主体，只返回既有member观察，不发布来源、Installed或新资格字段。[Linux v6.6 PT_INTERP→open_exec](https://raw.githubusercontent.com/torvalds/linux/v6.6/fs/binfmt_elf.c)仅作直接依赖的静态依据。

修正后新5项全部通过；最终Trust131、LauncherInputs6、DeploymentV2 11、Provider3、Launcher3共154项全部通过，0失败/错误/跳过，包含同新5；最终Spotless/Checkstyle退出0。新用例使用真实临时ELF、普通owner/未包装FileChannel及精确sealed70bb JNI，覆盖一次dropper open且只有alias Context、截断程序表及原NEEDED不符、同physical不同PT_INTERP原文、无PT_INTERP仍保留NEEDED、前置index/role/reference/deadline/source拒绝。解释器实际fd仍开且dropper fd已关时保留旧文件并复制same-byte新inode，必须取得本次跨文件末端专属拒绝。未改Elf parser/builder或重跑未改native/Process矩阵，没有执行dropper或loader。

原失败分别保留：首轮缺AtomicBoolean测试import导致testCompile失败/零测试；补import后5项中4通过、1项32-byte夹具被既有D2最小64构造约束拒绝，未到reader。负例改为保留旧可接受prefix的64-byte ELF头、缺少所声明程序表，才验证完整reader拒绝。首轮154中153通过，既有cache probe替换用例在native ACL errno35处提前拒绝、未到指定probe→owned异常；相同源码/JNI单项1及终态154通过，未改该断言/native或生产代码。native既可透传OS错误，也会在身份/ctime变化时主动EAGAIN；当前未定位具体路径/原因，不将其标为已修复或确定的环境故障。

证据`/Users/liusinan/.codex/artifacts/unified-wal/classic-linux-launcher-dropper-evidence-a14d1025`保留六测试阶段原source/class/log/XML及三个style；各1448输入按各原snapshot/父Git blob核验无错配，最终两code与reader/两次完整回归/单项复查及terminal style一致，生产源在全部阶段未变。编译失败阶段的旧test class库存不算新编译或PASS。原JNI SHA-256 `70bb629fc8afedddb757729f6b79d35fec28b596406f8b6cd4a0d99e815513ee`未重建；本机仍为Mac OS X aarch64/Amazon JDK21。静态binfmt_elf.c为58870字节、SHA-256 `31d4f4f0cf8aa5f1742b54c68808eca14dbe27c8d7509e6a8756deda1e5abc81`，不是批准kernel/vendor运行。六raw scope双lsof/identity/SHA/archive hardlink核验后清理12目录、31文件路径、2569485逻辑字节，归档保留、regular-file数据块回收0，共享target及用户文件未清理。

正常D2原provider/source阳性及其绑定owner后替换descriptor的动态验证在本机NOT_EXECUTED，路由只作实际入口静态核对；缺来源的typed拒绝实际测试，未反射预置owner或假Installed。该增量只增强已有normal consumer的材料拒绝，不证明exec/open_exec、降权、cgroup、Unix交接或传递依赖闭合。私钥/ZK/supervisor/Process顺序、childCommand的qualification拒绝和diagnostics unconditional Unsupported保持；真实JDK helper/父环境、完整启动与parent/child/Main/G1/full-bytes/full16/生产Gate继续OPEN，models/性能未执行，完整目标active，默认OFF/SYSTEM/PARK及advanced disabled不变。

2026-11-05在父`7743fa52e67d7205bb6da662670af5e7c6a65462`上，后续仅调整原launcher直线启动顺序：verifyLauncherDropper成功后、loadPrivateKey之前，调用一次既有input.childCommand(material,program)，保存其List.copyOf结果到final方法局部argv。原ProcessBuilder位置改用同一argv，supervisor/retirement、fresh scope/root和checkStarting以及Process.initialize仍在原位置；未提前创建builder/进程，未复制schema/qualification判断、缓存字段或新增permit。当前外层v2承载D1/D2的原拒绝因此先于私钥、ZooKeeper session及supervisor锁获取；无效命令的错误优先级是有意变更，合法v1数字参数和成功I/O顺序保持。局部命令数据不替代临近启动的runtime检查，也不是资格票据。

该顺序增量仅跑现有LauncherInputs6、DeploymentV2 11、Launcher3共20项，全部通过，0失败/错误/跳过，Spotless/Checkstyle退出0；未加测试专用生产seam、源码文本测试或假Installed。正常入口一次childCommand及先后关系按真实直线代码静态核对，v1精确argv/不可变性与D1/D2拒绝向量、实际未安装Main失败各只证明自身范围，不计获批D2 Main动态阳性或实际降权。证据`/Users/liusinan/.codex/artifacts/unified-wal/classic-launcher-command-order-evidence-7743fa52`保留原source/class/log/XML，1448输入按原snapshot/父Git blob核验无错配，唯一改动源与test/style一致；双lsof/identity/SHA/archive核验仅清1个raw scope的2目录、6文件路径、456905逻辑字节，归档保留/data块0。没有重跑ELF2/JNI/NEEDED或改diagnostics/childCommand拒绝，native库仅runner配置；无models/性能或整体Gate提升，真实Linux资格producer与完整16步继续OPEN/active。

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

## 受控 Linux Classic child 的 saved UID/GID 检查

在父 `dfae605310` 上，原 child 按已核验 program 的平台选择凭据读取。在 Linux 上，分别于 metadata 构建前、START 消费前和网络激活及 READY 前，通过已受控加载的 JNI 读取本次调用线程的 real、effective、saved UID/GID 与最多 64 个补充组。每次都要求六个 ID 与批准 recipe 对应相等，补充组为原精确集合。非 Linux child 与原 launcher 继续使用旧四字段 JNI ABI。

Linux 入口无路径或 PID 参数，调用 `getresuid`、`getresgid` 和有界 `getgroups`，前后复核两组 ID。syscall 失败沿实际 errno 拒绝，旧库缺符号保留 LinkageError 原因。分次读取只观察该调用线程，其他线程、组合原子性、持续不变及实际 dropper 降权需要独立证据。fsuid/fsgid、capabilities、no_new_privs、seccomp、cgroup 和旧 UID 的进程与 fd 隔离仍待核验。[getresuid(2)](https://man7.org/linux/man-pages/man2/getresuid.2.html) · [getgroups(2)](https://man7.org/linux/man-pages/man2/getgroups.2.html)

在 Darwin arm64 上构建新 JNI 库，native 六项为五通过、一项仅 Linux 的真实线程状态比对跳过；child 八项及 launcher 五项通过。新库的 `-Xcheck:jni` 复验和原 acf9afec… 库的独立缺符号 probe 均通过，后者前后可用原四字段读取。编译产物核对了三个调用位置及 Linux 分支只用新 JNI。Java Spotless、Checkstyle、新 Rust 片段格式及 Linux x86_64 交叉 `cargo check --tests` 均通过。Linux JNI 实读、合格 dropper 与系统验收为 NOT_EXECUTED。证据位于 `/Users/liusinan/.codex/artifacts/unified-wal/classic-linux-child-credentials-evidence-dfae6053`。

## 受控 Linux Classic child 的 filesystem ID 检查

在父 `47ac7bdb5f` 上，已核验的 Linux program 在三个原凭据检查点先核对原 real/effective/saved ID 及补充组，再通过独立 JNI 读取 fsuid、fsgid，并与同一 recipe 的 oldUid、primaryGid 精确比较。读失败或身份不匹配沿对应的原检查点拒绝。新 JNI 只接收无参数调用，返回两个 unsigned u32，沿用原两种凭据 JNI 的独立 ABI。

JNI 持有经类型与 inode 核验的原 `/proc` 根，经 `openat2` 的 BENEATH、NO_XDEV、NO_MAGICLINKS 和最终 NOFOLLOW 打开固定 `thread-self/status`。它检查同一 procfs 的普通叶子，读取至 EOF，上限 64 KiB 并用额外一字节检查超限。原始字节中要求唯一 `Uid:` 和 `Gid:` 行，各四个无符号十进制字段，只交付各自第四项。已开描述符的首次关闭及原身份前后复核成功后才返回。结果只反映调用线程的该次读取，与 getresuid/getresgid/getgroups 分次取得。完整线程组和持续 Unix 隔离仍需独立证据。[Linux status 字段](https://man7.org/linux/man-pages/man5/proc_pid_status.5.html) · [Linux thread-self 源码](https://raw.githubusercontent.com/torvalds/linux/v6.17/fs/proc/thread_self.c)

Darwin arm64 的 JNI 构建、Linux x86_64 交叉检查和 Java Spotless、Checkstyle 通过。Native 八项为六通过、两个 Linux 实读跳过，child 八项和 launcher 五项通过。新库的 `-Xcheck:jni` 复验与原 eca5cf65… 库的独立缺符号 probe 通过。Darwin 的 Cargo 测试进程因现有 `-install_name` 链接参数退出，单独用 rustc 编译源文件中逐字提取的解析函数及测试，四项全部通过。编译产物核对了三个检查点和 Linux 分支的调用顺序。Linux procfs JNI 实读及批准 dropper 的全链路均为 NOT_EXECUTED。证据位于 `/Users/liusinan/.codex/artifacts/unified-wal/classic-linux-child-filesystem-credentials-evidence-47ac7bdb`。

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

父`f1005222da0fea6ebe5683d14b9d67d060faf98d`上的装配WIP已在包内直接消费历史资源图：原ZK native ABSENT、现存完整根及远程/本地Cookie核验后，使用真实LedgerManager/LUM接历史Journal/Interleaved/GC，replay及初始flush后才开放fixture存储准入；Cookie不补建/修复，Fresh仍拒绝历史根。共享生命周期基类仅传递关闭请求，不共享启动许可。历史replay新增有界header/metadata长度、fence及未知特殊entry拒绝，并在使用完真实恢复ByteBuf后释放。Cookie open已开始但没有返回handle的确定反例原先错误报告组件DRAINED；修正保留原acquisition失败及未决资源，重复观察仍CLOSING、不伪造首次关闭结果。

后续实际文件回归发现原初始 flush 会吞掉 lastMark 失败。当前历史 child 在启动 Journal 写者前等待严格 flush 的实际结果，错误经原初始化失败返回。详见 [受控历史 Bookie 的启动 flush](../local-storage/README.md#受控历史-bookie-的启动-flush)。

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
