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
