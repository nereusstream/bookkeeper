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


## 步骤 12：真实 PREPARING reservation 与 INSTALL 冷读

2026-10-06，`ProfileReservation`、`ProfileReservationCodec` 与 `ZKProfileReservationStore`将初始PREPARING/reservation写入真实ZK单个persistent root，再从该root投影现有`ProfileControlStore`的INSTALL authority。原provisioned单目标view reader保留其原范围；新root是coordinator准备阶段的不可覆盖创建原语，不是人工预置view、第二份membership或normal-write authority。

root key固定为`<provisioned-root>/<ledgerId hex>`，同一ledger的不同instance/sidecarId也竞争同一key。调用方拥有client、namespace与publication ACL，adapter不建父目录、不拥有client、不自动retry/cache/watch或启动新线程。冷操作必须由有界caller执行，不能运行在ZK event线程或DATA owner；一次调用共享绝对deadline，timeout/interrupt不取消已提交写入。

reservation绑定上一节的52-byte backlink，以及有序计划E集合中每个目标的完整secret-free INSTALL scope：Bookie identity/storage incarnation、descriptor identity、domain/generation、operation identity和public semantic payload identity。全部目标必须共享ledger/instance/descriptor/domain/generation/operation/payload，Bookie identity不得重复；最多32个目标，当前required engine固定SEGMENT_WAL。该集合仅表达计划安装范围，标准metadata创建后membership仍由LedgerMetadata独占。

可丢弃实验schema 1使用big-endian：

| Offset | 字段 |
| --- | --- |
| 0 / 4 / 8 / 12 | `UWPR` magic、schema 1、完整length、PREPARING kind 1，各u32 |
| 16 | mandatory feature set，u64，当前必须0 |
| 24 | 原52-byte instance/opaque sidecar backlink |
| 76 | target count，u32，1..32 |
| 80起 | 每target的u32 length与原严格`ProfileAuthorityCodec`记录；必须complete PREPARING/INSTALL/SEGMENT且无range |
| 最后4 bytes | CRC32C，覆盖此前全部bytes |

固定部分84 bytes；decoder硬上限18,740 bytes，INSTALL实际无range的最大编码18,228 bytes。列表与scope保持immutable，未知mandatory字段、坏CRC/length/count、trailing bytes、scope不一致、错误ledger和ephemeral root拒绝开放；不修复或覆盖坏authority。root没有secret、credential digest、receipt history或store version复制；版本来自同一次实际get的Stat.mzxid。

`createIfAbsent`仅提交一次persistent create，无论成功、NODEEXISTS或回复丢失，都以一次sync/get实际snapshot解析：完整backlink和全部有序scope相同才返回APPLIED/ALREADY_APPLIED及实际version；相同operation/payload identity却改变目标、incarnation、顺序、generation、instance或sidecar仍CONFLICT。损坏root为QUARANTINED，查询失败或提交后实际NONODE保持DURABILITY_UNKNOWN，不能据此认定原请求没有执行。调用方仍须保留更早UNKNOWN，不用当前未提交结果清除它。

`installAuthorityReader(localBookieId)`显式绑定本地目标，因为原AuthorityKey命名共享domain而不含target。它冷读已提交root，以exact ledger/instance/purpose/domain key和计划目标选择完整scope，生成PREPARING authority及该snapshot的mzxid。未计划目标、其他instance/domain及ACTIVATE purpose均无authority；完整请求的principal、incarnation、operation/payload、descriptor/credential和local conditional durable install仍由原endpoint/physical owner验证。该reader不授予READY、normal ACTIVE或Add，也不把一次cold read当作持续租约或跨store/local原子撤权。

新10项测试使用原真实单server/双client fixture，验证独立270-byte向量、最大目标数与输入复制、字段/范围严格拒绝、真实create及每个目标cold-interface投影、同operation冲突不覆盖、并发单physical winner、实际commit后丢reply、ack回调延迟期间其他client改变root、实际删除后的UNKNOWN、200ms提交后timeout/迟到callback、ACL/关闭session及ephemeral/wrong-ledger/corrupt拒绝。测试改变实际server操作之后的callback交付；这里没有把mock成功当作root提交或all-E local durability。

复现：

~~~bash
bash scripts/unified-wal/run-local-storage-tests.sh 'ZKProfileReservationStoreTest,ProfileLedgerMetadataStoreTest,ZKProfileControlStoreTest,ZKProfileRegistrationStoreTest,ZKProfileEphemeralRegistrationStoreTest,IsolatedProfileControlEndpointTest,TestLedgerMetadataSerDe'
mvn -o -P '!cargo-zigbuild' -pl bookkeeper-server -am -DskipTests spotless:check checkstyle:check -q
python3 scripts/unified-wal/run-storage-models.py --tool /tmp/unified-wal-tla2tools-174.jar
~~~

定向开发运行`unified-wal-local-DW0FfPZX`的10项通过；其后扩展相关覆盖，并保留原运行输入。最终 `unified-wal-local-K2IDKqqY` tool exit0，985冻结source inputs，相关7类80/80通过、0 failure/error/skip；`unified-wal-reservation-style-aHMhoBh8` Spotless/Checkstyle exit0，source集精确一致，13份Java回执checksums已核验。最终模型 `unified-wal-model-2iaijevf` tool exit0，194配置为44正向完整PASS/150指定反例，425冻结inputs/584 checksums；183共享输入及1227去重union一致，五个designInputs绑定父提交 `2877d79cc2a4c61db1f9fae272c7b171592f4abe`。官方tool1.7.4、单worker、heap512MiB/direct64MiB、seed1/fp0，无symmetry/state constraint，CHECK_DEADLOCK FALSE，不声称活性、性能或生产authority。

本次仅创建不可覆盖PREPARING root和实际INSTALL冷读投影；真实all-E local install/receipts、标准metadata前置组合、READY/目标page及delete cut CAS、birth-bound mutation、activation/coordinator、orphan GC/retention、生产listener/startup/readiness、quorum、完整A/C/D组合、长期、掉电和性能仍未闭合。步骤12和生产Gate保持PARTIAL。


## 步骤 12：PREPARING 接线真实 local inactive install

2026-10-06，`SegmentProfileInstallStore`将原`ProtectedProfileStateStore`的INSTALL transition接到`SegmentBookieControlDriver`及真实Bookie控制文件；`IsolatedSegmentStorage.profileInstallStore(timeout)`借用本graph原driver和storage identity。原typed endpoint继续执行mTLS facts/static precheck、一次actual PREPARING root冷读、exact AuthZ和descriptor/credential检查。adapter再检查完整authority scope、INSTALL purpose/required engine及实际本地Bookie/storage incarnation，错target在提交前CONFLICT；ACTIVATE及尚未接线的query/其他控制操作UNSUPPORTED。不新增日志、executor、队列、retry或客户端所有权。

实际提交沿用已有预留control owner/I/O：control task → 原log完整command验证及AES-GCM保护credential → write/force → 原owner按序apply → callback。只把对应实际durable结果映为APPLIED/ALREADY_APPLIED及local generation；CONDITION_FAILED/INCOMPATIBLE为CONFLICT，不覆盖route/credential。INSTALL保持原normal state，首次install为inactive；已active route的同内容INSTALL retry也不擅自deactivate。重复INSTALL当前仍经原durable-control路径写入重复记录后解析原结果，未提供无I/O status fast path或新的历史裁剪。

caller只在有界冷路径同步等待，append owner调用立即拒绝，避免等自己的completion。成功晚于对应force和owner apply；driver忙/容量拒绝返回TRANSIENT_RECONCILING，不能清除早先UNKNOWN。提交后timeout/interruption返回DURABILITY_UNKNOWN且不cancel future/实际write，不归还driver的busy、control ticket、copy或writer isolation；它们由实际I/O/owner终结归还。adapter不拥有graph，也不为超时调用另建补偿执行器或自动重试。

新6项验证使用实际FileChannel/force、原owner/IO和ZK server：force前response未完成且owner仍能处理control通知；200ms停止等待后原log不能关闭或由另一writer打开，实际force/apply结束后精确retry为ALREADY_APPLIED；barrier failure没有成功或normal admission；错误target/incarnation、未支持operation及owner wait在提交前拒绝；真实graph的borrowed factory可执行durable install。另将同一真实PREPARING root、原typed endpoint与3个独立Bookie控制文件组合，要求全部E进入各自force后才释放任何barrier，确认同phase并发安装、全部真实durable结果及normal-inactive。

该组合使用typed transport facts和测试principal allowlist，没有实际TLS socket、INSTALL wire packing或生产listener；全E示例是隔离3/3/2配置，尚未实现全局创建coordinator、实际receipt retention/查询及标准metadata前置门槛。它不发布READY、不执行activation、不返回normal writer，也不把一次authority cold read当作跨store/local原子delete cut。

复现：

~~~bash
bash scripts/unified-wal/run-local-storage-tests.sh 'SegmentProfileInstallStoreTest,ZKProfileReservationStoreTest,ProfileLedgerMetadataStoreTest,ZKProfileControlStoreTest,ZKProfileRegistrationStoreTest,ZKProfileEphemeralRegistrationStoreTest,IsolatedProfileControlEndpointTest,TestLedgerMetadataSerDe,SegmentExecutionGroupTest,SegmentBookieControlLogTest#installAndActivationAreSeparateDurableGatesAcrossReopen+authorityMismatchIsRejectedBeforeWriteAndBatchOrderCannotActivateAbsentRoute+bookieControlDriverKeepsOwnerResponsiveAcrossFenceWaitAndForce'
mvn -o -P '!cargo-zigbuild' -pl bookkeeper-server -am -DskipTests spotless:check checkstyle:check -q
python3 scripts/unified-wal/run-storage-models.py --tool /tmp/unified-wal-tla2tools-174.jar
~~~

首次定向`unified-wal-local-CoQA49Qf`6项通过；扩展`unified-wal-local-vqgILjtb`在ZK自带启动client将localhost解析到198.18.0.1后发生ConnectionLoss，进入all-E测试前失败，exit1不计完整PASS。新增fixture沿用已有固定127.0.0.1做法，修正后重新冻结源码；历史运行保留原绑定。修正后的`unified-wal-local-IuzY8kvk`已完成97通过/5平台跳过；首次style因未使用import exit1，移除后重新冻结验证。模型runner补齐新adapter/测试的输入，旧矩阵运行保留原输入。

最终 `unified-wal-local-xzOtn4EW` tool exit0，987冻结source inputs，相关10类102项为97通过/5 macOS Linux-affinity跳过、0 failure/error；`unified-wal-install-style-mVFZelYE` Spotless/Checkstyle exit0，source集精确一致，16份Java回执checksums已核验。最终模型 `unified-wal-model-nbfk9uf2` tool exit0，201配置为45正向完整PASS/156指定反例，435冻结inputs/605 checksums；185共享输入及1237去重union一致，五个designInputs绑定父提交 `eb8408cb5cac686e0e7e9a72cd706877c067c1ce`。官方tool1.7.4、单worker、heap512MiB/direct64MiB、seed1/fp0，无symmetry/state constraint，CHECK_DEADLOCK FALSE，不声称活性、性能或生产authority。

有限[physical INSTALL投影](../models/README.md#physical-install-分发投影)检查exact admission、barrier/owner结果、停止等待后的实际ownership及normal-state保留。真实READY/有界target pages/delete cut与birth-bound CAS、global all-E receipt/coordinator及metadata前置组合、activation、INSTALL实际TLS/wire、生产startup/readiness、quorum、完整恢复/完整A/C/D组合、长期/掉电/性能仍未闭合。步骤12和生产Gate保持PARTIAL。


## 步骤 12：all-E 后的初始 metadata 协调门槛

2026-10-06，`ProfileInitialMetadataCoordinator`复用真实PREPARING root、原typed INSTALL endpoint/physical adapter和标准metadata create adapter，接通初始创建到metadata阶段。创建开始前冻结标准metadata（含其他custom bytes和password）、canonical descriptor、全部有序计划E、credential及target binding；要求同ledger/backlink、descriptor identity、E/W/A与ensemble顺序，当前SEGMENT/SYNC_ON_ACK以及完整peer Bookie/incarnation/Engine和mandatory capabilities匹配。kind-1 credential必须与原`DigestManager.generateMasterKey(metadata.password)`相同，错误输入在任何reservation写入前拒绝。

调用方提供有界cold executor和nonblocking、request-correlated `InstallClient`，coordinator不拥有线程、client、timer或retry。每个Target绑定已验证的HELLO facts；source构造未替代真实mTLS/peer身份校验，未来wire client必须保证同一peer/request的关联。先实际create/resolve reservation，再发出全部E请求，随后统一等待结果；不会按E逐个串行等待，也不能用W或A代替E。

只有每个目标的APPLIED/ALREADY_APPLIED、durable=true、matching INSTALL/ledger/operationId/authority generation及positive install generation才形成`ProfileInstallReceipt`。receipt绑定完整immutable scope（含instance、descriptor、target/incarnation、operation/public payload/generation）、SEGMENT Engine、public capability digest及稳定local install generation；无secret/offline verifier。digest为SHA-256，domain `BookKeeper/ProfileInstallCapabilities/v1`加NUL，随后big-endian `schema:u32=1 + engine:u16 + capabilityCount:u16 + sorted capabilityId:u32/semanticVersion:u16/reserved:u16=0`；排除credential和readiness generation。空capabilities、Engine 3的独立vector为`d6ff18b72491b4622e7e0d9270c7a90b007e8af46a3d84d598e889deda594bdf`。它是当前experimental logical receipt，未冻结INSTALL response wire、stable capability inventory或生产client认证规则。

known拒绝使结果INSTALL_NOT_READY，UNKNOWN/exception/null或错绑定success为INSTALL_UNCERTAIN；均不调用standard metadata，不能据此清除更早UNKNOWN或更换instance。不会cancel已提交target任务。全部E匹配后，cold sync/get再次验证与最初同一完整PREPARING snapshot和mzxid；absent/unavailable保持RESERVATION_UNKNOWN，坏root隔离，改变/删除重建的snapshot拒绝。通过后才调用原metadata adapter，并继续以一次实际read验证完整backlink/初始内容及actual version/token。返回METADATA_CREATED/METADATA_ALREADY_CREATED只陈述metadata snapshot及有界install receipts，不是READY、local ACTIVE或normal writer。

root read与standard metadata create不是跨key原子事务；read之后root被改/删仍可能形成inert orphan。后续READY/target publication必须以实际lifecycle/delete cut CAS解决此窗口，不能从此门槛推导即时撤权、normal authority或GC许可。receipt目前随结果返回，persistent receipt/target retention及restart coordinator尚未实现。

本轮补齐两个实际依赖：原控制log保留安装时的local generation，INSTALL adapter返回该birth generation；activation/fence推进current local generation不改install generation，重放按原install顺序还原，无control/on-disk格式变更。新回归重现旧映射在activation后expected1/actual2（`unified-wal-local-JvZNZYfY` exit1），修复后真实force/retry及文件replay保持1且不deactivate既有normal状态。原metadata Result默认字符串会通过`Versioned`/LedgerMetadata展开password，真实ZK回归`unified-wal-local-jbU3g2Kj` exit1；现在两层control结果字符串只显示状态、是否有metadata及receipt数量，不展开password或custom values。

新增5项真实初始协调流程、1项install-birth replay、1项metadata结果redaction回归：3个控制文件全部force前实际ZK metadata不存在；期间修改caller custom bytes不改变已冻结输出；同内容retry返回既有metadata/version和等价receipts；A=2的成功不足、错绑定success、提交后timeout保持metadata absent；实际root version变化拒绝推进；错quorum/顺序/credential/peer在root创建前拒绝。`unified-wal-local-yymOrOq2`相关22项通过；仅known拒绝/错误response字段的负向测试使用受控client返回，成功、UNKNOWN和metadata持久事实均经实际FileChannel/ZK路径。transport仍为typed facts，测试不证明真实INSTALL socket或quorum。

复现：

~~~bash
bash scripts/unified-wal/run-local-storage-tests.sh 'SegmentProfileInstallStoreTest,ProfileLedgerMetadataStoreTest,ZKProfileReservationStoreTest,ZKProfileControlStoreTest,ZKProfileRegistrationStoreTest,ZKProfileEphemeralRegistrationStoreTest,IsolatedProfileControlEndpointTest,TestLedgerMetadataSerDe,SegmentExecutionGroupTest,SegmentBookieControlLogTest'
mvn -o -P '!cargo-zigbuild' -pl bookkeeper-server -am -DskipTests spotless:check checkstyle:check -q
python3 scripts/unified-wal/run-storage-models.py --tool /tmp/unified-wal-tla2tools-174.jar
~~~

首次完整`unified-wal-local-IhkLzTlW`的351项为346通过/5平台跳过；首次style因新增测试import顺序/unused import exit1，只有import修正后重新冻结完整验证，旧运行保持原输入。

最终 `unified-wal-local-CgLIZ88L` tool exit0，991冻结source inputs，相关10类351项为346通过/5 macOS Linux-affinity跳过、0 failure/error，含完整Bookie控制log 245项；`unified-wal-initial-style-E82C4Awl` Spotless/Checkstyle exit0，source集精确一致，16份Java回执checksums已核验。最终模型 `unified-wal-model-ufcpvr46` tool exit0，209配置为46正向完整PASS/163指定反例，448冻结inputs/629 checksums；189共享输入及1250去重union一致，五个designInputs绑定父提交 `60a0bab6e5793cff79216ea6c99de080bf60ae0f`。官方tool1.7.4、单worker、heap512MiB/direct64MiB、seed1/fp0，无symmetry/state constraint，CHECK_DEADLOCK FALSE，不声称活性、性能或生产authority。READY/有界target page与persistent receipt retention、birth-bound lifecycle/delete cut CAS、full restart coordinator、actual INSTALL TLS/wire、全E activation/normal writer、生产startup/readiness、client quorum/CLOSED/完整恢复、完整A/C/D与组合、长期/掉电/性能仍未闭合，步骤12和生产Gate保持PARTIAL。


## 步骤 12：initial READY、目标留存与 DELETE_INTENT CAS

2026-10-06，原`ZKProfileReservationStore`保持UWPR/schema1 reservation bytes，增加同事务创建的`lifecycle` head及随机birth marker；原metadata coordinator的snapshot同时绑定parent payload/mzxid、head payload/mzxid/dataVersion与birth。缺失head/marker的旧实验namespace fail closed，不自动补写未知authority，不声称迁移/长期兼容。INSTALL reader仅在PREPARING返回authority，READY/DELETE之后不授予新INSTALL。

当前initial lifecycle是单个有界head和一个固定`initial-activation`候选page：head先PREPARING(seq1, predecessor0)，READY推进semantic sequence/store version但不推进global control generation；DELETE_INTENT从PREPARING或READY推进sequence并将control generation加1，terminal且保留已发布page引用。没有per-entry progress、通用跨key事务、历史page列表或后台GC。固定candidate slot避免不同READY operation产生无界orphans；同operation/full内容冲突不覆盖。

公开实验字节使用big-endian、schema1、mandatory features0及CRC32C：`UWLH` head固定240 bytes，含backlink52、reservation SHA-256 identity32、birth UUID16、state/semantic sequence/predecessor/control generation、operation16/public payload32、page mzxid/digest32。`UWAP` page固定部分220 bytes，含backlink、lifecycle birth与独立page birth、READY operation/public payload/control generation、实际standard metadata LongVersion、creator token、canonical ensemble digest和最多32份有序install receipts；复用原strict scope codec，另存public capability digest32/local install generation。decoder最大20,156 bytes，当前无range INSTALL实际最大19,644 bytes；独立head240/page446向量验证CRC、布局及negative discriminators。

canonical ensemble digest为SHA-256，domain `BookKeeper/ProfileInitialEnsemble/v1`加NUL，再编码schema/E/W/A各u32及有序Bookie UTF-8 length:u32/name；不含password/credential，membership仍由standard metadata独占。creator token与instance/backlink同时绑定metadata出生身份；ZK metadata删除重建后的LongVersion0不能单独证明同一metadata。实际READY reader重读standard metadata，校验version/token/backlink与initial OPEN/CRC32C/ordered ensemble digest。这里提供真实单server snapshot证据，不声称metadata/root跨key原子性或多节点quorum。

先实际create-if-absent page与birth child，再CAS发布head的page引用。候选存在仅证明page persistence，直到READY CAS之前保持inert。page数据ACL移除WRITE/ADMIN；依原namespace权限保留READ/CREATE/DELETE以创建或退役birth child，marker数据只READ。Root head、page都带独立不可变birth marker，CAS检查root dataVersion、head/page marker0及head expected dataVersion；校验opaque mzxid、semantic lineage和完整内容，禁止ANY。root/page删除重建后即使dataVersion再次0，旧marker检查仍使旧CAS失败。当前没有执行child退役/数据删除的API，namespace/ACL owner及非Byzantine约束未被模拟成生产auth证明。

READY与DELETE使用同一head CAS。DELETE先赢则旧READY失败，candidate仍inert；READY先赢后DELETE保留page及全部E的Bookie/incarnation/activation授权和receipt，供delete discovery。READY response-loss后若已被DELETE吸收，可用保留的matching page/op/payload返回ALREADY_APPLIED，仍不授予新activation。page/root未知、缺失mandatory marker、坏format或referenced metadata真实NONODE隔离，不能把缺失当作EMPTY/default，也不自动修复。

`activationAuthorityReader(localBookie, manager)`从actual READY head、matching immutable page与standard metadata snapshot生成原ProfileControlStore的READY_INITIAL scope，使用独立READY operation、精确instance/descriptor/target/incarnation/control generation。PREPARING/inert page、DELETE、错误key/target或stale metadata不授权；有权principal与exact AuthZ仍由原typed endpoint验证。原physical adapter现在支持INSTALL和ACTIVATE_INITIAL，复用reserved driver/write/force/owner apply；INSTALL报告稳定birth，activation报告current local generation，replacement/query仍UNSUPPORTED。

新的真实组合为PREPARING → 3个control files/all-E install → actual standard metadata → immutable page → READY → 3个durable initial activation，证明在matching READY存在后原normal admission开放。全链仍是typed transport facts和测试allowlist，没有实际INSTALL/ACTIVATE socket/wire、client LedgerHandle或normal writer返回。DELETE_INTENT只是admission cut，不是跨store/local瞬时撤权；组合验证cut后新cold activation停止，但已active本地状态仍须后续fence/tombstone/drain，不能据此报告物理删除完成。

新增12项真实backend/vector测试覆盖page inert/ACL、实际page/READY/DELETE lost reply、同operation冲突、READY/DELETE concurrent one winner、root/page/delete ABA、metadata recreate token mismatch、mandatory page marker/metadata missing及200ms提交后READY timeout。另有1项实际coordinator/files/READY/activation/discovery组合。receipt facts在backend单测为输入，实际all-E/metadata/file链另由组合验证，未用mock成功替代真实持久化。首次page ACL只有READ使同事务birth-child create被NOAUTH拒绝，修正权限后通过；SDK实际missing使用NoSuchLedgerExistsOnMetadataServerException，已按该code隔离。旧Raw parent改写使head immutable digest不匹配，原CONFLICT回归更新为更强QUARANTINED且无authority。`unified-wal-local-rNPff5GV`33项、随后`unified-wal-local-UeqikKpr`35项通过，各保持原输入。

复现：

~~~bash
bash scripts/unified-wal/run-local-storage-tests.sh 'ZKProfileLifecycleStoreTest,ZKProfileReservationStoreTest,SegmentProfileInstallStoreTest,ProfileLedgerMetadataStoreTest,ZKProfileControlStoreTest,ZKProfileRegistrationStoreTest,ZKProfileEphemeralRegistrationStoreTest,IsolatedProfileControlEndpointTest,TestLedgerMetadataSerDe,SegmentExecutionGroupTest,SegmentBookieControlLogTest'
mvn -o -P '!cargo-zigbuild' -pl bookkeeper-server -am -DskipTests spotless:check checkstyle:check -q
python3 scripts/unified-wal/run-storage-models.py --tool /tmp/unified-wal-tla2tools-174.jar
~~~

首次完整`unified-wal-local-ZpyzxK8n`364项为359通过/5平台跳过；初次style仅因新增单行getter布局/unused import exit1，修正后重新冻结验证，旧结果保留原输入。

最终 `unified-wal-local-qlSVYzAe` tool exit0，995冻结source inputs，相关11类364项为359通过/5 macOS Linux-affinity跳过、0 failure/error，含完整Bookie控制log 245项；`unified-wal-ready-style-j5vX2UG6` Spotless/Checkstyle exit0，source集精确一致，17份Java回执checksums已核验。最终模型 `unified-wal-model-2cqx367s` tool exit0，217配置为47正向完整PASS/170指定反例，461冻结inputs/653 checksums；193共享输入及1263去重union一致，五个designInputs绑定父提交 `9a48e6346453f85c5d02615ef9e4503f30ce13c6`。官方tool1.7.4、单worker、heap512MiB/direct64MiB、seed1/fp0，无symmetry/state constraint，CHECK_DEADLOCK FALSE，不声称活性、性能或生产authority。有限[READY lifecycle投影](../models/README.md#initial-ready-lifecycle-投影)检查birth、inert/actual metadata、all-E、delete cut和retention，不能证明真实ACL/crypto/barrier/多节点quorum。physical delete、page/history GC、replacement/repair/full lifecycle、restart coordinator、actual INSTALL/ACTIVATE TLS/wire、normal writer availability、生产startup/readiness/client quorum/CLOSED恢复、完整A/C/D组合、长期/掉电/性能仍未闭合，步骤12及生产Gate保持PARTIAL。


## 步骤 12：可恢复初始协调与 all-E availability snapshot

2026-10-06，`ProfileInitialLifecycleCoordinator.initialize`复用原initial metadata plan/验证、PREPARING、candidate/READY page与physical adapter，完成初始冷路径到all-E activation及最后availability snapshot。它借用有界cold executor和nonblocking、verified HELLO/request-correlated INSTALL/ACTIVATE clients，不拥有线程、client、retry或timer。输入metadata/descriptor/credential、E集合和peer binding在开始前冻结；READY operation/public payload固定，和INSTALL operation不同，不能在retry中换identity或blind新instance。

恢复按actual durable事实区分：fresh/PREPARING且没有candidate时执行原all-E install→standard metadata gate；已有candidate或READY时读取原immutable page，并只读匹配已存在metadata，不走create-if-absent。candidate/READY证明metadata曾存在，actual NONODE隔离且保持absent，不用新metadata掩盖旧UNKNOWN。page的lifecycle birth、READY operation/payload、metadata version/creator token/backlink/ordered digest和调用方完整initial metadata都须匹配；root出生身份改变或DELETE时停止。首次metadata阶段也绑定最初root snapshot，不能把其all-E结果接到另一个root birth。

PREPARING candidate通过原READY CAS后继续，READY恢复查询保留同一page/op，再发出全部E的ACTIVATE_INITIAL，不能串行等单target或用A/W代替E。每个回复须matching operation/ledger/opId/control generation、durable及local generation晚于该target的install birth；wrong binding/null/exception/UNKNOWN保持UNCERTAIN，known拒绝为NOT_READY。全部matching completion后只读再验metadata/page binding，再cold-read matching READY reference/control birth；观察到DELETE返回DELETED。AVAILABLE仅是metadata及all-E activation完成事实的组合snapshot，不是持续lease，不创建LedgerHandle/normal writer，不证明各节点随后仍在线/未fenced，也不把跨key/cross-target读取变成原子事务。

原物理控制log保存historical operation result；仅ALREADY_DURABLE不能证明当前normal-active。新增owner上的`initialActiveGeneration`读取matching context/activation generation、active、非fenced/tombstoned/pending cut及healthy事实，typed ACTIVATE回复在该事实消失时返回STALE_OR_COMPACTED而非success。原install结果和其他control语义保留，normal Add仍无metadata读。真实回归`unified-wal-local-BSwooi19`重现fence后expected STALE/actual ALREADY，exit1；修复后`unified-wal-local-1kSuG2vh`通过，未deactivate或取消其他实际I/O。

新增9项真实flow/regression覆盖：首次full initialize；新的coordinator/ZK session与一台Bookie实际control file replay从READY恢复；持久candidate不重新INSTALL；candidate/READY引用metadata缺失时不重建；A=2 activation不足和wrong bound response；actual ACTIVATE force超时保留writer/IO，exact retry同READY operation完成；全部回复后actual DELETE cut拒绝AVAILABLE；fenced target的historical activation不被计数；READY operation/payload冲突拒绝。negative客户端只控制拒绝/错response字段，positive/UNKNOWN与metadata/READY持久事实依实际ZK/FileChannel/driver。定向`unified-wal-local-yJ8M1cQK`当时43项通过，旧运行保持原scope。

`AVAILABLE`对象只展示status/metadata presence/activation count，metadata password/custom values及credential不会在结果字符串展开。Root/SDK不可用保留uncertainty；borrowed executor拒绝不创建后台补偿，unexpected async编程错误保留exceptional future，不伪造成功。调用方停止等待不取消实际target/root/metadata操作，previous UNKNOWN仍由逻辑请求保持；本接口不提供caller deadline或新loss-window/strong-publication机制。

复现：

~~~bash
bash scripts/unified-wal/run-local-storage-tests.sh 'SegmentProfileInstallStoreTest,ZKProfileLifecycleStoreTest,ZKProfileReservationStoreTest,ProfileLedgerMetadataStoreTest,ZKProfileControlStoreTest,ZKProfileRegistrationStoreTest,ZKProfileEphemeralRegistrationStoreTest,IsolatedProfileControlEndpointTest,TestLedgerMetadataSerDe,SegmentExecutionGroupTest,SegmentBookieControlLogTest'
mvn -o -P '!cargo-zigbuild' -pl bookkeeper-server -am -DskipTests spotless:check checkstyle:check -q
python3 scripts/unified-wal/run-storage-models.py --tool /tmp/unified-wal-tla2tools-174.jar
~~~

首次扩大`unified-wal-local-xKFGBFnc`373项为368通过/5平台跳过；其后只有coordinator链式调用换行，style当前源通过，最终重新冻结完整相关范围，旧结果保留原输入。

最终 `unified-wal-local-v7KaiG3E` tool exit0，996冻结source inputs，相关11类373项为368通过/5 macOS Linux-affinity跳过、0 failure/error，含完整Bookie控制log 245项；`unified-wal-availability-style-p5JRNimT` Spotless/Checkstyle exit0，source集精确一致，17份Java回执checksums已核验。最终模型 `unified-wal-model-bf63gmga` tool exit0，225配置为48正向完整PASS/177指定反例，471冻结inputs/677 checksums；194共享输入及1273去重union一致，五个designInputs绑定父提交 `54f47dbd4ced724587fc9e8fadf91e1e054ef575`。官方tool1.7.4、单worker、heap512MiB/direct64MiB、seed1/fp0，无symmetry/state constraint，CHECK_DEADLOCK FALSE，不声称活性、性能或生产authority。[有限initial availability投影](../models/README.md#initial-availability-恢复投影)检查必需metadata不重建、all-E/current/bound/known activation与最终READY/metadata事实；不证明crypto、actual control TLS/wire、process-level complete restart、生产writer/client SDK、replacement/repair/full delete/drain/GC、service startup/readiness、client quorum/CLOSED/full recovery、full A/C/D与组合、长期/掉电/性能。步骤12及生产Gate保持PARTIAL。


## 初始 control 的真实隔离 socket

2026-10-06：显式 opt-in 实验 capability/schema1 已把 INSTALL/ACTIVATE_INITIAL 接到真实隔离 mTLS、原 endpoint 与physical driver；默认能力及通用control-body冻结状态不变。真实principal、cold authority与exact AuthZ通过后，event loop原子转账；cold timeout/UNKNOWN/断连和实际I/O终结分开，副本持续计费至最后使用。七项socket回归含实际ZK PREPARING/DELETE cut和迟到EL授权零写入。非阻塞实际client、完整READY/metadata reader与all-E coordinator经socket组合、生产listener/readiness和统一总预算仍待完成，步骤11/12与stable/G1/生产Gate保持PARTIAL/OPEN。 详见[wire布局、固定向量与所有权](../profile-wire/README.md#真实隔离初始-control-接线)。ACTIVATE socket成功用例使用committed-authority fixture；实际READY/metadata reader和3-target可恢复协调器此前仍经typed facts验证，本增量没有将两份分开的证据合并成真实all-E socket成功声明。

最终 `unified-wal-local-j9MoHs4Q` tool exit0：1002冻结输入，相关17类417项为412通过/5 macOS Linux-affinity跳过，0失败/错误；Bookie控制log完整252项包括七项新socket回归。`unified-wal-initial-socket-style-9ga4n66n` Spotless/Checkstyle exit0，142冻结Java source与test inputs一致；23份Java/style回执checksums已核验。最终模型 `unified-wal-model-0tgx4f8_` tool exit0：233配置为49正向完整PASS/184指定反例，483冻结输入/701 checksums，197共享输入及1288去重union一致；五个designInputs仍绑定运行父提交 `d7a68ffc03eb9a21bd91c1250fb6fb7b42d61f5e`。原始receipt/source/config/run不重绑定。官方tool1.7.4、单worker、heap512MiB/direct64MiB、seed1/fp0、无symmetry/state constraint、CHECK_DEADLOCK FALSE；有限PASS不代替实际TLS、fsync/掉电、内存、活性/公平性或完整A/C/D组合证据。

复现最终相关范围：

~~~bash
bash scripts/unified-wal/run-local-storage-tests.sh 'SegmentProfileInstallStoreTest,ZKProfileLifecycleStoreTest,ZKProfileReservationStoreTest,ProfileLedgerMetadataStoreTest,ZKProfileControlStoreTest,ZKProfileRegistrationStoreTest,ZKProfileEphemeralRegistrationStoreTest,IsolatedProfileControlEndpointTest,TestLedgerMetadataSerDe,SegmentExecutionGroupTest,SegmentBookieControlLogTest,ProfileInitialControlCodecTest,ProfileOperationCodecTest,ProfileReceiveDecoderTest,ProfileByteBufAllocatorTest,ProfileInboundPipelineSocketTest,ProfileTlsContextFactoryTest'
mvn -o -P '!cargo-zigbuild' -pl bookkeeper-server -am -DskipTests spotless:check checkstyle:check -q
python3 scripts/unified-wal/run-storage-models.py --tool /tmp/unified-wal-tla2tools-174.jar
~~~


## 可恢复初始协调器的真实 all-E socket 组合

2026-10-06 现有初始coordinator开始直接借用[非阻塞实际client](../profile-wire/README.md#非阻塞初始-control-client)的verified HELLO及request-correlated futures，而非构造transport facts。实际TLS principal、PREPARING/READY/metadata cold reader、原physical INSTALL/ACTIVATE、两phase全E发送及最后metadata/READY取证共同验证；没有另建coordinator、MetadataStore、driver或writer。3/3/2只是初始E=3门槛，两个local ACTIVATE完成不能代替整个E，也不是普通DATA A-quorum SDK证明。

实际response-loss后，新client和新coordinator显式解析同一READY/page birth、metadata version/creator token/backlink、scope/op，原instance/generation不变；metadata必需record消失时不重建。实验listener仍返回readiness generation0，AVAILABLE仍只是当前事实snapshot，不是continuing lease，不返回LedgerHandle或production writer。本轮没有完整server process/ZK进程重启或production startup/readiness证据；此前typed file/session replay记录保持原绑定，不能把分开的测试自动拼成完整process-level证据。详见wire实现范围、源码和回执。最终 `unified-wal-local-4qDodbai` tool exit0，1004冻结inputs，相关18类425项为420通过/5 macOS Linux-affinity跳过、0 failure/error；包含8项真实initial client/ZK/file/socket组合。`unified-wal-client-final-style-tyd9etl4` Spotless/Checkstyle exit0，200冻结Java source与去重union精确一致；24份Java/style回执checksums已核验。最终模型 `unified-wal-model-e10jjm5g` tool exit0，241配置为50正向完整PASS/191指定反例、494冻结inputs/725 checksums；199共享inputs及1299去重union一致，五个designInputs绑定父提交 `57a614c304e72985d850033c36dcff3f7232cdd5`。官方tool1.7.4，单worker、heap512MiB/direct64MiB、seed1/fp0，无symmetry/state constraint，CHECK_DEADLOCK FALSE；有限模型不替代实际crypto/fsync硬件、活性、公平性、整个内存总量、完整A/C/D、掉电、长期、性能或生产authority。历史source/config/run/receipt保持原绑定。


复现最终相关范围：

~~~bash
bash scripts/unified-wal/run-local-storage-tests.sh 'SegmentProfileInitialSocketClientTest,SegmentProfileInstallStoreTest,ZKProfileLifecycleStoreTest,ZKProfileReservationStoreTest,ProfileLedgerMetadataStoreTest,ZKProfileControlStoreTest,ZKProfileRegistrationStoreTest,ZKProfileEphemeralRegistrationStoreTest,IsolatedProfileControlEndpointTest,TestLedgerMetadataSerDe,SegmentExecutionGroupTest,SegmentBookieControlLogTest,ProfileInitialControlCodecTest,ProfileOperationCodecTest,ProfileReceiveDecoderTest,ProfileByteBufAllocatorTest,ProfileInboundPipelineSocketTest,ProfileTlsContextFactoryTest'
mvn -o -P '!cargo-zigbuild' -pl bookkeeper-server -am -DskipTests spotless:check checkstyle:check -q
python3 scripts/unified-wal/run-storage-models.py --tool /tmp/unified-wal-tla2tools-174.jar
~~~


## 初始可用事实到 ordinary transport

2026-10-06 actual all-E initial flow后可用独立DATA连接发原CRC32C/v3 entry，initial-control公共API保留；TLS/HELLO/correlation/output custody共用有界实现。原authority reader不进入ordinary热路径，三元status不压成Classic rc。[DATA与SDK边界](../profile-wire/README.md#普通-data-client-与-sdk-接线边界)明确这是sender与原AckSet组合，未接通LedgerHandle writer、连续callback、换组或fenced-close/recovery。

最终 unified-wal-local-yMsEARYx tool exit0：1007冻结inputs，相关18类431项为426通过/5 macOS Linux-affinity跳过，0 failure/error；原8项initial及新增6项ordinary DATA组合通过。unified-wal-normal-client-style-n2bhqnuh Spotless/Checkstyle exit0，203冻结Java source与union一致；24份Java/style回执checksums已核验。模型 unified-wal-model-5720ce65 tool exit0：247配置为51正向完整PASS/196指定反例，506冻结inputs/743 checksums，204共享及1309去重union一致，5个designInputs绑定父提交8f6bf6f9501741ca98d968ae2efc2ea8b8563fc5。官方tool1.7.4、单worker、heap512MiB/direct64MiB、seed1/fp0，无symmetry/state constraint，CHECK_DEADLOCK FALSE；不证明crypto/fsync硬件、活性、公平性、整个内存、完整A/C/D、长期/掉电/性能或production authority。历史source/config/run/receipt不重绑定。
