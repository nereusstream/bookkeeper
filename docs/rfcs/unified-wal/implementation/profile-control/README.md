# Experimental Profile Control/Auth — Wave 0

> **EXPERIMENTAL / NON-PROMOTABLE / NO AUTHORITY / DISCARDABLE**

本目录拥有 Unified WAL Wave 0 的 Profile control/auth interfaces、隔离 `bookie-profile` endpoint reference implementation 与机器回执。typed cold-path implementation、12项普通功能测试、机器回执与范围审计均已完成，并绑定源码提交`a8773e5c84b602fb524bf294ae707f167cc29040`。

上述 Wave 0 回执及其 scope guard 保持历史绑定。当前步骤 12 的后续实现见[真实 ZK 控制权威读取](#步骤-12真实-zk-控制权威读取)和[标准 metadata backlink 创建](#步骤-12标准-metadata-backlink-创建)；各自的隔离读取/创建证据不提升本目录的 production authority。

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

格式与静态检查：

```bash
mvn -o -pl bookkeeper-common -DskipTests spotless:check checkstyle:check
scripts/unified-wal/check-wave0-profile-control-scope.sh
```

完整机器结果见[`test-results/summary.json`](test-results/summary.json)，immutable source object、精确边界与剩余BLOCK见[`receipt.json`](receipt.json)。最终验证有意不运行会处理整个现有test-resource tree的Maven full lifecycle或module-wide RAT；一次较早的targeted lifecycle test曾由`testResources`把已有资源树复制到`target`，未执行或解析compatibility harness/test/probe/artifact，也未用作最终证据。该事件和后续direct-plugin containment已在receipt中记录。

2026-09-28新增独立的Classic前置安全修复：[master-key日志回执](classic-master-key-log-receipt.json)绑定`LedgerDescriptorImpl.checkAccess()`与真实日志事件捕获测试。错误master key仍返回原`UnauthorizedAccessException`，日志保留ledger ID而不再包含请求或缓存的master-key字节数组；1项测试先红后绿，`bookkeeper-server`的Spotless/Checkstyle通过。这不改变本目录历史Wave 0 scope guard或旧receipt的源码绑定，也不代表生产Profile control的完整secret-leak/TLS/AuthZ矩阵已通过。

同日后续隔离实现增加`TLSContextFactory.forProfileServer()`：复用现有key/trust加载，固定immediate TLS1.3和必需客户端证书；真实双端握手、握手后叶子证书canonical issuer/subject提取及Classic默认模式对照的2项定向测试通过。它不改变本目录已冻结reference回执，尚未成为Bookie listener，也未接通`ProfileTransportContext`、精确AuthZ或生产authority。

## Remaining OPEN/BLOCK

- real listener、Profile专用provider配置接线、X509身份与控制端点接线及trust rotation仍OPEN；
- concrete allowlist、完整 sidecar/backlink/fanout 与发布/CAS 仍 OPEN；隔离 fixed-key reader 和整次读取 deadline 的已实现范围见下节；
- physical protected-state owner、crash framing、group commit、at-rest protection与secure deletion仍BLOCK；
- production secret-leak/TLS/auth negative matrix和Spike A A24仍未通过；仅上述Classic mismatch日志泄漏已修复；
- stable control wire、Profile wire compatibility matrix、G1、production integration与任何authority均未解锁。

## 步骤 12：真实 ZK 控制权威读取

2026-10-05，server experimental package 的 `ZKProfileControlStore` 直接实现原 `ProfileControlStore`。调用方提供已 provision 的独立 root、ACL 和已有 ZooKeeper client，并在有界 cold-path 调用线程执行；不能在 ZK event thread 或 DATA owner 上阻塞。适配器不创建 namespace、不发布 authority、不拥有 client，也没有 watcher/cache、自动 reconnect 或自身 retry。现有 `ZKProfileRegistrationStore` 的 sync/get/deadline 原语抽取为两者复用的 package-private `ProfileZKReadClient`，其 persistent CAS 和 ephemeral 协议未改。

每次 read 由 root sync 和一次派生 leaf get 组成，共用一个正的、至多一分钟的 deadline。leaf 为 `ledgerId 的小写 hex-instance 的 32 hex-purpose ID-domain 的 canonical UTF-8 base64url（无 padding）`；ledger/instance/purpose/domain 来自原 `ProfileAuthorityKey`，不接受请求方提供任意 backend path。只有实际 get 返回 NONODE 才是 NOT_FOUND；ACL 拒绝、失连、关闭/过期会话、超时和中断均为 UNAVAILABLE，中断标记保留。格式/完整性/用途状态/key 不匹配或 ephemeral 节点为 QUARANTINED。

FOUND 携带严格解码的完整 tuple，`opaqueStoreVersion` 使用**同一次 get callback** 的 `Stat.mzxid`，不使用可在重建后复位的 dataVersion，也不从 payload 读版本。更新或删除重建之后下一次 read 重新 sync/get；已捕获的合法旧 snapshot 即使延迟到更新之后交付，也保持自己的 payload/version。它是一次读取快照，没有租约或持续 authority 保证。target/incarnation、operation ID、semantic payload、generation/range 等不在 key 内的字段，继续由原 endpoint 的 exact AuthZ、完整 scope equality 和 engine/descriptor/credential 检查拥有，reader 的 FOUND 不替代授权或 local durable effect。

该 namespace 的一个 leaf 只表示一份实验 committed-authority **view**。它不是 RFC-0001 的完整 ledger sidecar/目标集，更没有 coordinator 的 immutable backlink、E-target fanout、membership、READY、RepairIntent、delete cut、snapshot/retention 或 publication CAS。测试中直接写入 view 是 provisioner 输入，不能据此声称实际生命周期发布已经完成。

### 实验 authority view 字节

`ProfileAuthorityCodec` 为 package-private provisioner/test codec，无 credential 或 membership 副本。big-endian、magic `UWAU`、schema 1；未知版本、长度/discriminator、非 canonical UTF-8、无效 identity/范围/正 generation、trailing bytes 或 CRC 均拒绝。complete 只能为 0/1，reader 仅接受 1 和匹配 operation 的 state；required engine 为既有 wire ID 1/2/3。

| 顺序/offset | 字段 |
| --- | --- |
| 0/4/8/12 | magic/schema/总长度/complete，各 4 bytes |
| 16/20/24/28 | operation/purpose/state/required engine，各 4 bytes |
| 32/40/56/92 | ledgerId 8、instance 16、descriptor identity 36、target incarnation 16 bytes |
| 108/116/132 | authority generation 8、operation ID 16、public semantic payload identity 32 bytes |
| 164 起 | Bookie UTF-8 长度 u32 + B bytes、domain UTF-8 长度 u32 + D bytes |
| 172+B+D 起 | range flag u32，0 无 range、1 后跟 first/last 两个 i64 |
| 最后 4 bytes | CRC32C，覆盖所有此前 bytes |

总长度为 `180+B+D+R`；B 为 1～255，D 为 1～128，R 为 0 或 16，因此 182～579 bytes。range 仅适用于 RECOVERY_GRANT/CLOSE，first/last 非负且有序。固定 ID 映射如下，不依赖 enum ordinal：

| ID | operation / purpose | state |
| --- | --- | --- |
| 1 | INSTALL / PREPARING_INSTALL | PREPARING |
| 2 | ACTIVATE_INITIAL / READY_INITIAL | READY_INITIAL |
| 3 | ACTIVATE_REPLACEMENT / POST_MEMBERSHIP_REPLACEMENT | POST_MEMBERSHIP_REPLACEMENT |
| 4 | RECOVERY_GRANT / RECOVERY_GRANT | RECOVERY_OPEN |
| 5 | RECOVERY_GRANT_CLOSE / RECOVERY_GRANT_CLOSE | RECOVERY_CLOSE_AUTHORIZED |
| 6 | TOMBSTONE_OR_DELETE_APPLY / TOMBSTONE_OR_DELETE | DELETE_AUTHORIZED |
| 7 | STATUS_QUERY / STATUS_QUERY | STATUS_QUERYABLE |

此格式是可丢弃实验 namespace 表示，不是稳定 control wire 或完整 sidecar 格式。579-byte 限制约束 decoder 接受的 view；ZooKeeper SDK 已取得 bytes 后才做检查，不能把它称为 socket/SDK 分配前预算。调用方的并发准入、SDK 在超时后尚存的请求、watcher/连接持续降级和总内存预算仍需实际接线。

### 真实读取验证

Java 回执 `unified-wal-authority-read-zvp_anyu` 绑定父提交 `0231ac595a4d881488c913002dcb3002a4564484` 和 81 source inputs：新 reader 12、原 endpoint 13、本地 receipt 10、persistent CAS 8、ephemeral 13、startup facts/harness 17，共 73/73，通过且 0 failure/error/skip；Spotless/Checkstyle 通过。新测试使用实际单节点 server 和双 client，覆盖七种 purpose、全部 tuple/实际 mzxid、固定字节向量、最大 UTF-8/range、foreign key、更新/删除重建、incomplete/wrong-state/ephemeral、损坏长度/identity/CRC、真实 ACL 拒绝/关闭/过期、实际 callback 延迟导致超时、中断，以及捕获之后更新仍不混用版本。reference endpoint 使用真实 authority read，local STATUS_QUERY probe 只返回 DURABILITY_UNKNOWN；typed transport 与该 probe 不冒充真实 TLS 或物理持久化。

复现：

~~~bash
mvn -o -P '!cargo-zigbuild' -pl bookkeeper-server -am \
  -Dtest=ZKProfileControlStoreTest,IsolatedProfileControlEndpointTest,SegmentLocalReadinessStoreTest,ZKProfileRegistrationStoreTest,ZKProfileEphemeralRegistrationStoreTest,StartupReadinessFactsTest,StartupReadinessHarnessTest \
  -Dsurefire.failIfNoSpecifiedTests=false -DtestRetryCount=0 test
mvn -o -P '!cargo-zigbuild' -pl bookkeeper-server -am \
  spotless:check checkstyle:check
~~~

有限读取及四个指定反例见[authority read 模型](../models/README.md#控制权威-cold-read-投影)。真实 quorum、节点重启、实际连续失连 gate、protected-state endpoint 接线、global publication/安装激活、完整模型、掉电和性能未验证。步骤 12、Block G 与生产 Gate 保持 PARTIAL；原 Wave 0 receipt、manifest 和 scope guard 未改。

2026-10-05 后续[session 本地 DATA gate](../startup-readiness/README.md#步骤-12session-绑定的本地-data-准入)绑定实际 publisher attempt 与隔离 storage graph，失连退休不重开、原 I/O 继续排空。296 项共享 control/readiness/storage 测试及 style 通过。它不把此 reader 的 snapshot 升为持续 authority 租约，也不补齐真实 startup、生产 listener/registry、sidecar 发布或安装激活；步骤 12 仍 PARTIAL。

## 步骤 12：标准 metadata backlink 创建

2026-10-06，`ProfileMetadataBacklink` 与 `ProfileLedgerMetadataStore` 将 RFC-0001 §6 的小型 instance/sidecar reference 接到原 `LedgerManager.createLedgerMetadata`，实际验证使用既有 LongHierarchicalLedgerManager 与真实 ZooKeeper。这里是 coordinator 在 PREPARING 和全 E durable inactive install 已验证之后调用的标准 metadata 创建原语；该原语自身不验证这些前置事实、不发布 READY/activation、不返回 normal writer，也没有 sidecar root/目标集副本。Classic create/open/metadata mutation 与 SerDe 不改。

reserved custom key 为 `bookkeeper.experimental.profile.backlink`，值固定52 bytes、big-endian、schema 1，可丢弃实验表示：

| Offset | 字段 |
| --- | --- |
| 0 / 4 | `UWBL` magic / schema 1，各 u32 |
| 8 | 非负 ledgerId，i64 |
| 16 / 32 | ledger instance / opaque sidecar reference，各16-byte非零ID |
| 48 | CRC32C，覆盖前48 bytes |

reference 是 opaque identity，不是请求方可选择的 backend path，也不是授权。它不保存 descriptor、membership、receipt、secret 或历史。constructor/accessor防御性复制，decode严格检查固定长度、版本/magic、CRC、ID和ledger scope；attach保留并复制其他custom entries，已有不匹配或损坏reference不能被替换。这个值的immutable约束还不能替代后续backend mutation/CAS中的出生身份与不可变字段检查。

创建前只接受当前standard metadata格式3、OPEN、CRC32C、从entry0开始的唯一初始ensemble、无重复target及有效E/W/A，保留标准metadata原数据。无论manager创建回调成功或失败，随后都只做一次实际read：同ledger/instance/sidecar reference、相同E/W/A、ordered ensemble、digest/password、全部custom entries及显式持久ctime才形成成功；创建回复成功时为APPLIED，NODEEXISTS或回复丢失时为ALREADY_APPLIED。原ZK manager的成功回调可能仅返回提交对象，NODEEXISTS内部解析仅比较creator token，因此这些回调自身不证明完整内容。backend生成的ctime和随机creator token不被当成调用方请求内容；返回实际read snapshot的metadata/version/token。不同instance/reference或已变化的初始内容为CONFLICT，损坏reserved reference为QUARANTINED；重读失败，包括实际NONODE，保持DURABILITY_UNKNOWN，不证明原请求没有执行。无自动重建、写重试、watcher、额外线程或manager所有权。

标准ZK manager的LongVersion来自dataVersion，删除/重建会再次为0。真实回归验证旧backlink重试仍拒绝新instance且不改写它；这只是create-if-absent的结果解析。后续membership/fence/delete mutation CAS必须补齐出生身份和immutable backlink约束，不能从这里推导普通version CAS已防ABA。返回成功是对应snapshot的标准metadata事实，不是持续authority租约或初始writer availability。

新9项真实server测试覆盖固定独立字节向量及负向/防御复制、真实create与同内容重试、不变version/token、commit后丢回复解析、instance/reference/quorum/ordered ensemble/password/custom/显式ctime冲突、两个client竞争、延迟callback前输入修改、删除重建同dataVersion、legacy/坏reference、实际创建后删除且重读NONODE、关闭client和非法初始layout。新增回归在真实create完成、成功回调尚未交付时由第二client修改ack quorum：修复前 `unified-wal-local-BhHtXitj` 返回APPLIED，违背预期CONFLICT，exit1；改为实际重读后 `unified-wal-local-5znusXdb` 的9项通过，exit0。故障仅延迟/替换实际server操作后的callback，不以mock成功代替metadata持久化。单节点验证不证明ZK quorum、节点崩溃/掉电或持续权限。

复现：

~~~bash
bash scripts/unified-wal/run-local-storage-tests.sh 'ProfileLedgerMetadataStoreTest,ZKProfileControlStoreTest,ZKProfileRegistrationStoreTest,ZKProfileEphemeralRegistrationStoreTest,IsolatedProfileControlEndpointTest,TestLedgerMetadataSerDe'
mvn -o -P '!cargo-zigbuild' -pl bookkeeper-server -am -DskipTests spotless:check checkstyle:check -q
python3 scripts/unified-wal/run-storage-models.py --tool /tmp/unified-wal-tla2tools-174.jar
~~~

最终 `unified-wal-local-asvuwC2b` tool exit0，981冻结source inputs，相关6类70/70通过、0 failure/error/skip；`unified-wal-metadata-style-RCI77e1E` Spotless/Checkstyle exit0，source集一致，12份Java回执checksums已核验。模型 `unified-wal-model-7mo2zzed` tool exit0，188配置为43正向完整PASS/145指定反例，414冻结inputs/566 checksums；179共享输入及1216去重union一致，五个designInputs绑定父提交 `ffc3e8cdea081280a8f21da8c63dea3c235dfbd7`。此前不存在的isOpen接口、初次style超长行与上述实际snapshot回归失败均不计PASS；最终验证在实际read修复之后，旧运行保留原输入。验证耗时不是性能前后对照。

[有限metadata创建投影](../models/README.md#标准-metadata-backlink-创建投影)独立验证snapshot匹配和unknown规则；它不枚举全部字节、backend内部或all-E/READY组合。真实sidecar PREPARING/有界target publication、all-E durable receipts、READY与delete cut、后续birth-bound mutation CAS、安装激活coordinator、生产listener/readiness、quorum/CLOSED/完整恢复、完整A/C/D、长期、掉电及性能仍未闭合；步骤12及生产Gate保持PARTIAL。
