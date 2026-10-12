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
