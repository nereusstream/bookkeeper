# Experimental Profile Wire — Wave 0 Block D

> **EXPERIMENTAL_TEST_MANIFEST / NOT_STABLE_WIRE / NON_PROMOTABLE / NO_AUTHORITY / DISCARDABLE**

本目录保留原 Unified WAL Wave 0 Block D 的实现锁、byte corpus、冻结的旧 decoder 反例与延期 compatibility harness；原 Block D 回执只覆盖纯 codec。后续步骤11～15在本目录记录隔离真实 TLS/socket/storage/SDK/恢复/删除接线的独立增量，当前增量包括 [OPEN 固定 LAC 前缀审计](#open-固定-lac-前缀审计)、[网络 WRITE_LAC 与确认前缀生产](#网络-write_lac-与确认前缀生产)和 [本地删除空间终结](#本地删除空间终结)。这些隔离实现不授予生产启动、注册、ACK、恢复完成或删除权限。

当前状态：**REPLACEMENT CORPUS PASS / COMPATIBILITY MATRIX DEFERRED_NOT_RUN — 原 candidate `0x0FFE4250` 的反例已冻结；replacement `0x0FF04250` 未获得 released-decoder/localhost stock-binary 兼容结论，G1保持 `BLOCKED_UNVERIFIED`**。

后续步骤11已有下文的[真实隔离普通 Add 接线](#真实隔离普通-add-接线)。它复用本目录冻结的实验字节合同；原 Block D 回执、corpus、compatibility延期和G1状态保留其历史范围。

## Scope guard

从 Block D 起点 `be3d3d55b6787b84698fe428705b398bda57a1de` 检查所有 committed、tracked 和 untracked change：

```bash
scripts/unified-wal/check-wave0-block-d-scope.sh
```

guard 只允许本模块的 pure codec、test corpus、冻结 harness 和 owner documentation；任何 `BookieImpl`、`PendingAddOp`、`LedgerStorage`、`Journal`、`EntryLog`、现有 `bookie-rpc` decoder/fallback、Cookie/registration、AutoRecovery/delete 或生产 authority 路径都会因不在 allowlist 中而 fail closed。路径处于 allowlist 不构成执行 compatibility harness 的授权；当前执行处置见下文。未跟踪的 `BtrLog Low-Latency Logging.pdf` 永久排除，不进入实现或提交。

## Pure codec milestone

`bookkeeper-common` 的内部 `org.apache.bookkeeper.common.profile.wire` 包现已实现：

- `ProfileFrame`、`ProfileFrameHeader`、`ProfileFrameCodec` 的4-byte BE outer length、固定32-byte header、magic/protocol/header/flags/reserved/length严格拒绝和allocation-before-body界限；
- pre-HELLO 4096、control 65536、absolute 5242880三个hard max；
- `ProfileHello` 的byte-exact client/server body、unsigned capability order、UTF-8/NUL/bounds；
- `ProfileStatus` 的12 status class、5 retry disposition、4 durable result；
- 60-byte `ProfileLedgerContext` 和 distinct normal/recovery/read/fence/LAC/force/list data bodies；
- `RANGE_READ`/`BATCH_RECOVERY_ADD` 只接受空request body并产生 typed `UNSUPPORTED`；
- 七个control subtype的未冻结tail明确返回 `BLOCKED_UNFROZEN_CONTROL_BODY`，没有猜测或实现schema。

## Authoritative byte corpus milestone

`bookkeeper-common/src/test/resources/profile-wire/` 冻结保留610个authoritative vectors：259 valid、86 invalid、9 adversarial old-protocol/TLS prefixes、256 fixed-seed fuzz。全部vector共享同一 `fixtures.tsv`，逐项固定phase、frame accept/reject reason、typed decode/status、close结果和byte count；`checksums.sha256` 锁定每个binary、fixture和manifest。

覆盖包括：每个subtype、client/server HELLO、全部240个status/retry/durable组合、normal/recovery distinct bodies、magic逐byte flip、1..31-byte header truncation、body truncation、outer/body mismatch、三个oversize边界、version/header/flags/reserved/subtype、malformed HELLO/status/data、normal/recovery interchange、TLS ClientHello、v3/pre-v3 adversarial prefixes、Profile后接legacy-like ADD prefix，以及seed `0x505746555a5a0001` 的256个arbitrary vectors。strict new-codec corpus test同时断言全部frame reject在body allocation前发生。

replacement `0x0FF04250` 已重建同一组610个vector及全部checksums；新增的确定性回归要求pre-v3 opcode byte `0xf0`、其XOR `0xff`结果`0x0f`及全部single-bit flips均保持在Classic opcode `1..7`之外。本阶段16个wire/corpus单测通过。仓库记录的 stock old Bookie 支持列表为 `4.14.8 / 4.15.5 / 4.16.7 / 4.17.2`，但 replacement 的 released decoder 与 localhost stock binary raw-corpus matrix 明确为 `DEFERRED_NOT_RUN`，没有 raw artifacts、machine receipt 或兼容结论。代码和new-codec corpus完成不等于 wire stable，也不等于 G1 PASS。

## Deferred compatibility execution

当前 Wave 0 不再执行 released-decoder/stock-binary 层：不解析或启动锁定的旧版 BookKeeper artifact，不启动 Bookie/ZooKeeper 进程，不打开 probe socket，也不向旧 decoder/binary 回放610个 raw vectors。`tests/profile-wire-compatibility/`、现有 corpus 与 frozen counterexample 只作为未完成工作的可追溯材料保留，不接入常规 Maven reactor、CI 或后续模块任务。

`DEFERRED_NOT_RUN` 不是 PASS、FAIL 或 waiver；它表示该层没有当前结果。G1 因此保持 `BLOCKED_UNVERIFIED`，candidate 继续 `NOT_STABLE_WIRE / NON_PROMOTABLE / NO_AUTHORITY`。不依赖 G1 的 pure codec、reference implementation 和 isolated/discardable prototype 可以继续，但不得据此接入 production listener、Classic fallback、ACK 或其他 authority。若未来单独恢复兼容认证，必须显式授权、使用新的 run identity 并从零生成证据；不得复用当前延期状态充当结果。

## Frozen counterexample

首个真实 released decoder run 在 Apache BookKeeper `4.14.8` 上发现 [`CE-20260831T105059Z`](counterexamples/ce-20260831T105059Z-4.14.8-magic-flip-1/README.md)：`0x0FFE4250` 的第二个magic byte被corpus逐byte XOR `0xff` 后成为 `0x01`，真实pre-v3 decoder把它识别为`ADDENTRY`并产生`ParsedAddRequest`。因此原候选硬 FAIL，raw bytes/artifact digest/result/repro已冻结。replacement 的G1保持 `BLOCKED_UNVERIFIED`；若未来显式恢复兼容认证，只能保留deterministic regression并用新run identity重跑全部四个真实released decoder和stock binary，不能删除vector、覆盖反例或放宽Oracle。

replacement candidate 的机器锁见 [`candidate.json`](candidate.json)。它只记录当前实验候选、延期执行状态与G1阻塞，不覆盖或重写上述counterexample。

## 真实隔离普通 Add 接线

本节保留 `66644a6021` 的 normal 切片与当时回执；后续 FENCE 接线及当前边界见下一节。

2026-10-06，后续步骤11接通真实 localhost TLS1.3/mTLS socket → HELLO → ADD_NORMAL → 隔离存储 → status 响应。入口为既有 graph 的 `installProfileConnection`，业务分发由 [SegmentProfileAddHandler](../../../../../bookkeeper-server/src/main/java/org/apache/bookkeeper/bookie/storage/segment/SegmentProfileAddHandler.java) 直接调用原存储；无新增线程、通用 service 框架、远端逐entry读取、逐entry控制记录或Classic fallback。协议使用原 subtype/header、HELLO body和8字节status，不分配新wire值；没有提升Block D或G1。

- HELLO消费接收器已验证的canonical client body，服务端只在真实mTLS握手后提取principal一次，返回实际存储的BookieId/incarnation和Segment engine。此切片advertise零个optional capability，其余操作明确unsupported；readiness generation是调用方显式输入，测试为0，不能替代真实startup/registration事实。
- normal固定100字节prefix、credential kind/20-byte长度、tail长度与本地配置的统一N同时检查。N+1声明在固定头阶段关闭，尚未预留/分配body；context、BK entry ledger/entry坐标及CRC校验后，原Bookie admission继续持有权限至实际终结。错误scope/credential、inactive/fence/tombstone及坏body不启动DATA I/O。
- entry借用只读frame view，不调用byte[] reference Add decoder来克隆大payload。原 `SegmentEntryInputs` 先取得实际source额度再同步复制；normal input/admission、对齐pool与native writer排他继续由原graph/driver拥有。相同logical payload（允许LAC封装变化）的重复成功复用原物理结果，不再写入；不同payload明确冲突。
- 只有实际 `LOCAL_DURABLE` 结果产生 `OK/NEVER/APPLIED`，该结果包括已确认的同内容重试，当前driver不另区分ALREADY_APPLIED。明确未做本次DATA I/O的资源拒绝为 `TRANSIENT_UNAVAILABLE/SAME_PROFILE_OPERATION/NONE`；force或提交不确定性为 `DURABILITY_RESULT_UNKNOWN/SAME_PROFILE_OPERATION/UNKNOWN`。STALE_ADMISSION可能晚于物理写，保留 `PROFILE_NOT_READY_OR_STALE/AFTER_CONTROL_RECONCILIATION/UNKNOWN`。detailCode保持0，不冻结新的payload-conflict detail或BKException映射。
- 原连接/Bookie明文账本另预留固定解析副本与响应buffer额度，pending数量受实际graph上限约束。已排到event loop的完成对象、下游持有的响应及frame均等实际消费/write promise终结后释放；断连/移除handler仅取消等待，不取消已提交I/O、清除坐标或提前释放pool/OS锁。已交付请求有不续期的30秒响应等待期限；这次没有单独等待30秒实测新业务timer，已有接收阶段虚拟时间验证范围保持不变。

八个真实socket场景覆盖持久化/点读/restart、同内容与冲突重试、无权限与坏body的零I/O、仅超限header、真实write后force前不得成功、force失败UNKNOWN、断连与新拒绝NONE、原I/O/drain/OS排他，以及handler移除时下游buffer和排队completion的额度生命周期。排队取消反例先用真实FOUND点读及owner barrier确认Add回调已经排队：`unified-wal-local-IVhwFRKM`在旧释放逻辑下明确失败；修正后 `unified-wal-local-gFYoEIvH` 8项全部通过（JUnit 3.054秒），未弱化断言。前期API编译/fixture状态枚举/失败session close错误的探索输出保留，不计PASS。

最终 `unified-wal-local-uA3Q6Jy1` 绑定父提交 `25343736717180ed6e05818f44cfa88f02409600` 与922个冻结输入；14类383项，378通过、5项macOS上的Linux affinity跳过，0失败/错误，Spotless/Checkstyle通过，19个回执checksum已核验。原runner补充了实际Profile/wire/TLS/certificate输入与common报告收集。相关 [有限分发模型](../models/README.md#profile-普通-add-分发投影)为69 distinct states、五个指定反例；最终矩阵39正向完整PASS/122指定反例，Java/model的116重合输入逐项匹配，1138个去重输入与五个父提交designInputs已核对，历史回执不改绑定。

复现：

```bash
bash scripts/unified-wal/run-local-storage-tests.sh 'SegmentBookieControlLogTest#profileSocket*'
bash scripts/unified-wal/run-local-storage-tests.sh 'IsolatedSegmentConfigTest,SegmentBatchFormatTest,SegmentControlFormatTest,SegmentArenaControlLogTest,SegmentBookieControlLogTest,SegmentOwnerLoopTest,SegmentExecutionGroupTest,ZKProfileEphemeralRegistrationStoreTest,ProfileFrameCodecTest,ProfileHelloStatusTest,ProfileWireCorpusTest,ProfileReceiveDecoderTest,ProfileInboundPipelineSocketTest,ProfileTlsContextFactoryTest'
mvn -o -P '!cargo-zigbuild' -pl bookkeeper-server -am -DskipTests spotless:check checkstyle:check -q
python3 scripts/unified-wal/run-storage-models.py --tool /tmp/unified-wal-tla2tools-174.jar
```

当前只证明隔离normal链路及相应明文/响应所有权。接收与原存储额度分别有界，完整TLS/socket/native与存储总账仍未闭合；没有接生产Bookie listener、真实控制operation授权/执行、完整HELLO capability/client pool、真实startup/readiness、quorum ACK、服务级未CLOSED恢复或完整模型。耗时仅为固定配置验证记录，没有TLS吞吐、CPU/entry或尾延迟的前后对照，不能宣称性能收益或步骤11/B18完成；Secret-leak完整matrix、G1兼容、掉电、长期负载及生产Gate仍OPEN。

## 真实隔离 FENCE 与控制额度

2026-10-06 在同一隔离 pipeline 接入已有 `FENCE_LEDGER`，复用 [SegmentProfileAddHandler](../../../../../bookkeeper-server/src/main/java/org/apache/bookkeeper/bookie/storage/segment/SegmentProfileAddHandler.java)、Bookie 控制 driver、保留的 CONTROL owner/I/O 份额和真实 FileChannel。wire 的100字节body、operationId及8字节status保持原合同，不新增协商、线程、服务框架或重试层。

- 真实mTLS/HELLO之后，原位解析exact context、nonzero operationId和kind=1/length=20 credential。每次FENCE重新比对本地安装的context/credential；inactive/fenced状态仍允许合法fence及same operation重试，tombstone、错实例或错凭据在效果前拒绝。FENCE消费既有data credential；它不授予INSTALL、ACTIVATE、repair grant或delete的control-plane权限，后者仍须原exact authorizer与cold authority合同。
- 首次合法FENCE先把连接名额、base、完整frame及固定解析/响应workspace在同一账本中原子转入CONTROL，再提交真实控制owner。转移失败保留旧账并关闭，不能先切断本地准入或写控制日志。外来连接的workspace不能混入转移；同次read仍持有的字节保持原lane至最后释放。
- 前序DATA尚未实际终结时，原driver关闭新的normal准入并返回WAITING_PRECUT，响应为 `TRANSIENT_UNAVAILABLE/SAME_PROFILE_OPERATION/NONE`。这里NONE只表示本次没有durable fence，不抹掉先前UNKNOWN或未终结DATA；客户端必须保留原operationId/context/credential再试。等待后由原控制write/force及owner apply完成，才返回 `OK/NEVER/APPLIED`；已持久的同scope重试为 `OK/NEVER/ALREADY_APPLIED`，不重复写入。control I/O或apply不确定性保持 `DURABILITY_RESULT_UNKNOWN/SAME_PROFILE_OPERATION/UNKNOWN`，detailCode为0。
- 接线另外保留已复制控制请求的额度至实际控制owner终结。断连、handler移除或30秒响应等待取消只停止响应；CONTROL连接名额可归还，但在途控制副本、排队completion与下游write仍各自保持frame/响应额度，不能撤销force、提前释放storage/OS锁或伪造success。新业务timer的真实30秒到期仍未单独实测。

七个真实socket场景验证fence持久化/重复/restart、四类授权/格式拒绝零控制写、包含workspace的原子转移失败零effect、真实pre-cut DATA force交错、真实fence force前无成功/断连后drain与writer排他，以及force失败UNKNOWN。同一listener/port/shared budget场景实际保留N=1024的DATA半包，第二个DATA header的body额度申请被拒后，合法FENCE仍完成；随后唯一UNCLASSIFIED名额被已完成TLS但无HELLO的连接占满，新连接被拒而已有CONTROL同操作重试仍成功，后续错凭据仍拒绝，DATA header不能借CONTROL预算。新增decoder回归验证外来workspace拒绝，既有pressure回归加入合法workspace原子转移。

socket red `unified-wal-local-TOG93LGt`在原unsupported实现下失败，接线后 `unified-wal-local-rDUeJb28`通过；七项focused `unified-wal-local-aOrwMF8I`通过，随后修正压力fixture实际payload长度，并以 `unified-wal-local-hCaLJp9J`验证真实额度拒绝。最终 `unified-wal-local-P3zaRCUQ` tool exit 0，父提交 `66644a6021bf9e4ea2e6fdcbec4d5ceadd465daf`，922冻结输入，14类391项为386通过/5 macOS Linux-affinity跳过、0失败/错误；Spotless/Checkstyle通过，19回执checksums核验。初始API编译错误和首次Checkstyle失败保留，不计PASS。

[有限FENCE投影](../models/README.md#profile-fence-分发投影)最终 `unified-wal-model-g0cie706`为40正向完整PASS/128指定反例，340冻结输入/506 checksums；与Java的116共享输入精确匹配，1146去重union和五个父提交designInputs已核验。验证耗时不构成吞吐、CPU/entry或尾延迟的前后对照。

复现：

```bash
bash scripts/unified-wal/run-local-storage-tests.sh 'SegmentBookieControlLogTest#profileSocketFence*,ProfileReceiveDecoderTest'
```

本节保留FENCE切片及当时回执；后续单条恢复写入见下一节。生产Bookie listener、TLS/socket/native与存储统一总账、完整HELLO capability/client pool、INSTALL/ACTIVATE等真正control-plane授权/执行、fresh startup/readiness、read网络操作、完整recovery、quorum ACK和完整A/C/D仍未完成。Secret-leak完整matrix、G1、长期负载、掉电及性能Gate保持OPEN，步骤11尚未完成。


## 真实隔离 ADD_RECOVERY 接线

2026-10-06，真实 localhost TLS1.3/mTLS → HELLO → ADD_RECOVERY → 原恢复 admission、源预算和 DATA driver → 8字节status 已接通。请求沿用既有144字节prefix及完整BK entry；不改变wire、持久格式、HELLO字段或optional capabilities。ADD_RECOVERY属于DATA用途，不能在CONTROL连接借用预留额度。grant由原真实控制driver持久化；测试使用已验证的typed committed fixture，尚未证明真实RepairIntent/coordinator的冷授权链。

- 固定头按本地N检查 `144 + 36 + N`，超限声明在body额度申请、分配及DATA分类之前关闭。原位读取context、非零intent ID、intent/grant generation、精确range/entry坐标和kind=1/20-byte credential；原Bookie只接纳同一OPEN grant的完整元组，inactive或fenced route可以恢复写入，pending/durable tombstone与grant close拒绝新接纳。错误格式/CRC/BK坐标为BAD_REQUEST，错route/credential及无匹配grant返回原对应status，不做DATA写入。
- server不调用reference payload decoder，也不先复制完整entry到byte[]。小授权tuple不持payload；原 `SegmentEntryInputs.acquire` 先预留实际源额度，再同步复制借用frame view，输入与恢复票据共同交给现有batcher。原reference手动入口仍保留其不可变snapshot合同；两条入口共用同一实际entry校验、batch、重复点读和终结路径，无新增队列、线程、每entry远端查询或控制记录。
- LOCAL_DURABLE返回 `OK/NEVER/APPLIED`，包括真实点读确认的同内容重试；冲突为 `PROFILE_IDENTITY_CONFLICT/NEVER/NONE`。不匹配或已关闭grant在提交前为 `RECOVERY_GRANT_INVALID/NEVER/NONE`；资源拒绝为 `TRANSIENT_UNAVAILABLE/SAME_PROFILE_OPERATION/NONE`。DATA写入后grant close/tombstone使结果过期时，返回 `RECOVERY_GRANT_INVALID/AFTER_CONTROL_RECONCILIATION/UNKNOWN`，force/I/O未知为 `DURABILITY_RESULT_UNKNOWN/SAME_PROFILE_OPERATION/UNKNOWN`，detailCode保持0。NONE不清除前次UNKNOWN；恢复成功与grant关闭均不激活normal writer。
- 取消/断连只停止RecoveryRequest等待，沿用普通Add的queued completion/write credit保护；同步复制完成后网络资源可以释放，原source/aligned pool/I/O和OS writer lock仍按真实owner终结排空。source可在复制到对齐batch后归还，不能把它描述为一直持有到force。业务30秒响应timer未单独实等验收。

七个真实socket场景覆盖准确grant写入/读取/同内容与冲突/restart、错误完整元组与损坏entry零DATA I/O、仅N+1 header、源预算耗尽后恢复、实际write/force前grant close、断连与短期限drain/native锁、force失败UNKNOWN。源压力fixture按实际4-shard分额保留三个坐标容量，以一个合法三entry batch隔离源预算；owner暂停时三份1060字节源共3180字节，第四次同逻辑请求拒绝NONE，释放后按requestId恰好核对结果，不要求网络完成顺序。既有normal queued/downstream响应所有权回归继续覆盖共享完成路径。

首个socket red `unified-wal-local-A5MkYZXS` 在原unsupported实现下失败（response error flag）；接线green `unified-wal-local-eXD9BDfI`通过。最小七项最终 `unified-wal-local-Y9QZU2Oq` 全部通过。中间压力fixture依次暴露coalesced首read额度、错误的完成顺序假设、实际每shard定位额度不足；修正夹具而未放宽生产预算或有效断言，缺失import及首次style行长失败不计PASS。

最终 `unified-wal-local-WaFfvigz` tool exit0，父提交 `6f44a97fc9c679cda0e0919663497c4dde1d4715`、922冻结输入；14类398项为393通过、5 macOS Linux-affinity跳过、零失败/错误，Spotless/Checkstyle通过，20份回执checksum核验。新增 [恢复分发有限投影](../models/README.md#profile-add_recovery-分发投影)与既有矩阵最终 `unified-wal-model-vuhtsh6x` 为41正向完整PASS/134指定反例，348冻结输入/527 checksums；116共享输入和1154去重union精确匹配，五个designInputs绑定父提交，历史回执不变。耗时只记录验证条件，不构成性能对照。

复现沿用前节14类runner和必要style/model命令；最小新行为为：

```bash
bash scripts/unified-wal/run-local-storage-tests.sh 'SegmentBookieControlLogTest#profileSocketRecovery*'
```

本轮只接通已安装本地grant的单条恢复写入，不证明READ_ENTRY成功/确定missing响应、fencing/LAC聚合、真实RepairIntent/grant control-plane、客户端pool/quorum/CLOSED、一般未CLOSED恢复、生产listener/readiness、TLS/socket/native与storage统一总账。长期/多Arena、Secret-leak完整matrix、掉电、正式性能、完整A/C/D/组合和步骤11/14/Gate仍OPEN。


## Netty backing buffer 额度接线

2026-10-06，隔离 listener 在创建 TLS handler 前安装连接专属 `ProfileByteBufAllocator`，与原 `ProfileReceiveBudget.ConnectionPermit` 共用每连接及 Bookie 三份额字节账本。UNCLASSIFIED、DATA、CONTROL 的用途、实际授权与原子名额交接合同保持；本切片不改实验 wire、存储格式或 Classic allocator。

- heap/direct 根 buffer 在 backing 分配前取得完整 capacity 的额度。使用 unpooled 根避免把缓存池的未释放 backing 当作已归还；扩缩容期间先覆盖旧 backing 加新 backing，额度不足时保留原 capacity、内容和读写 index，实际替换并释放旧 backing 后才归还旧额度。没有新增池、队列、线程、重试或配置后端；这里不声明 allocator 的性能收益。
- slice、duplicate、read-only 及 byte-order view 共用根 refcount。frame 关闭或连接断开不能提前归还仍被 retained view 持有的 backing；根实际最终释放后才扣账。连接关闭只归还名额并阻止新分配，已存在的 backing/解析/响应额度仍按各自最终所有权终结。
- fixed-header 后的 frame 直接把已预留 exact frame credit 交给 backing；输入来自同一 allocator 时不再按 readableBytes 重复计费，外来输入仍保守预留临时明文额度。响应从原 workspace 原子拆出 exact output capacity，根接管这一份，剩余解析/completion workspace 保留原终结条件。TLS wrap/unwrap、socket read、cumulation 与输出通过 channel allocator 获得的实际 ByteBuf backing 均计入同一账本；源 entry 复制仍沿用存储原预算。
- 已存在的 TLS/socket read backing 跨分类时保持原归属，后续新分配使用连接的新 lane。只有原 base、已验证完整控制 frame 与 workspace 按既有授权原子转账；不能把同次 read 残留字节全部挪到 CONTROL，也不能因 UNCLASSIFIED 满额保证新控制连接准入。

实现已核对当前 Netty 4.2.15.Final 的根构造、capacity 与 deallocate 路径；[direct 根源码](https://raw.githubusercontent.com/netty/netty/netty-4.2.15.Final/buffer/src/main/java/io/netty/buffer/UnpooledDirectByteBuf.java)的构造和实际 cleanable 释放路径不能仅靠旧 allocateDirect/freeDirect hook 计账，所以额度在根构造之前取得、在实际根 deallocate 之后归还。验证运行使用 JDK21；此结论不扩展到其他 TLS provider 的 opaque native 内存。

真实 mTLS socket 的 allocator 漏记回归 `unified-wal-local-TaioY2AG` 编译成功后预期失败（256-byte root 应使占用64→320，实际仍64），接线后 `unified-wal-local-L5WPDtHP` 转绿。六项 allocator 回归覆盖 heap/direct 最后 view、扩缩容峰值及拒绝后数据/index、共享额度拒绝与恢复、旧 read lane、workspace split、frame body 在断连后继续保留；相关四类49项 `unified-wal-local-Vw6TUk2s`通过，其中全部22个真实Profile socket场景保留成功、拒绝、UNKNOWN、cancel/drain/restart语义。新增TLS实际分配不足回归 `unified-wal-local-Qgw3A3C5` 单项通过；EmbeddedChannel断连后的close输出仍占额度，清理实际输出后才归零。首次聚合失败来自尚未调整的握手工作集与误把排队close输出当作已释放，不计PASS。

测试按实际 TLS working set 调整隔离预算；同port DATA压力用已持有的真实 backing 填充DATA份额，使第二个完整body仍在申请时被拒，已授权CONTROL仍能完成FENCE。控制转账失败测试保留256-byte CONTROL限额和零effect断言；不是增大预算来移除有效压力覆盖。

最终 `unified-wal-local-V4ePSx6Q` tool exit0，父提交 `15df420b42a69b88c1b9c03372d920d54ea49bf5`，924冻结输入；相关15类405项为400通过/5 macOS Linux-affinity跳过、0失败/错误。`unified-wal-backing-style-7jpV2V88` Spotless/Checkstyle exit0，输入与Java冻结集一致；21份Java回执checksums已核验。模型最终 `unified-wal-model-wci6ds2z` tool exit0，182配置为42正向完整PASS/140指定反例、358冻结输入/548 checksums，118共享输入精确一致，1164去重union和五个父提交designInputs已核验；历史回执保持原绑定。验证耗时不是性能前后对照。

复现：

```bash
bash scripts/unified-wal/run-local-storage-tests.sh 'IsolatedSegmentConfigTest,SegmentBatchFormatTest,SegmentControlFormatTest,SegmentArenaControlLogTest,SegmentBookieControlLogTest,SegmentOwnerLoopTest,SegmentExecutionGroupTest,ZKProfileEphemeralRegistrationStoreTest,ProfileFrameCodecTest,ProfileHelloStatusTest,ProfileWireCorpusTest,ProfileReceiveDecoderTest,ProfileByteBufAllocatorTest,ProfileInboundPipelineSocketTest,ProfileTlsContextFactoryTest'
mvn -o -P '!cargo-zigbuild' -pl bookkeeper-server -am -DskipTests spotless:check checkstyle:check -q
python3 scripts/unified-wal/run-storage-models.py --tool /tmp/unified-wal-tla2tools-174.jar
```

[有限 backing 额度投影](../models/README.md#profile-backing-额度投影)同步检查根及临时扩容 ownership，不证明 TLS 或内核总内存。JDK SSLEngine 的内部对象/缓存/内部 ByteBuffer、opaque native TLS 分配、kernel socket buffers、buffer/credit等对象开销，以及存储、索引、接收资源的统一总账仍未闭合。生产listener、fresh readiness、READ_ENTRY成功/确定missing、真实grant控制面、client quorum/CLOSED、完整A/C/D、掉电、长期负载及性能Gate保持OPEN；步骤11仍为PARTIAL。


## 接收至存储源副本交接

2026-10-06，隔离普通 Add 与 ADD_RECOVERY 的同步entry复制复用原 `SegmentEntryInputs.acquire`、batcher与driver，并取得借用frame所属连接的 `ProfileByteBufAllocator`。复制之前同时满足原source子上限与连接/共享Bookie接收账本；新的独立exact-size heap backing在分配前取得DATA credit。source计数是入口子上限，接收账本记录同一backing的实际持有，不把两份计数相加当作两份实际allocation。frame、解析workspace与源副本同时存在时各自保留实际费用，不先释放frame再复制。

- driver接管后，frame取消或断连只停止原response等待。未被owner实际用完的源root仍保持原连接DATA归属及byte credit；名额可以归还，新source分配被关闭的permit拒绝。owner复制进既有预分配对齐池或完成实际duplicate比较后，才由原source释放路径归还root费用。对齐池的全部backing/alignment padding仍在原启动保留量内，lease保留至实际I/O和owner结果终结；这里没有把root费用延长到source已经不存在的force阶段。
- allocator只对明确的credit拒绝抛 `ProfileReceiveBudget.CapacityExceededException`，source入口回滚已经取得的source份额并返回未接纳；原ordinary/recovery状态映射不变，未开始DATA的拒绝不重置此前UNKNOWN。其他runtime/JVM allocation错误仍回滚原source份额并传播。只有Profile source复用该账本；手动/其他allocator入口保持独立unpooled精确副本，不能因调用方使用pooled buffer而把未计费的大backing带入源契约。
- 无新队列、线程、池或配置后端，不改wire、持久化格式、credential/grant规则或Classic行为。原手动Recovery快照adopt路径仍按原source预算与不可变snapshot合同执行。

真实socket红色 `unified-wal-local-NSgvXgpc` 编译成功后预期失败：暂停owner时1060-byte源已排队，normal账本应为2464，实际1404。接线后 `unified-wal-local-xYus7kZX` 中normal/recovery两条真实mTLS路径均通过：frame/workspace/source共存时分别2464/2608字节，断连后仅保留1060源字节；owner复制到对齐池后source与接收费用归零，force未结束时50ms close仍失败、native DATA writer锁仍排他，force实际完成后可关闭且不回响应。第一次green中的目录重用错误与后续测试helper作用域编译错误保留，不计PASS；夹具改用独立实验目录并正确复用既有certificate helper，未放宽生产断言。

focused `unified-wal-local-p2xdB0HL`两项通过。另一个用真实已安装channel allocator的单测把连接limit设128/base64：36-byte源使两个计数为36/100，第二个36-byte复制因连接额度拒绝而source/接收计数保持36/100，释放后恢复，关闭后已有source仍占36直至最终释放；pooled调用者的副本仍使用36-byte独立unpooled backing。这是allocator/ownership验证，不是该小预算完成TLS握手的证明。完整mTLS及实际I/O由前述socket回归验证。

最终 `unified-wal-local-1PlKPtbk` tool exit0，924冻结输入，相关15类407项为402通过/5 macOS Linux-affinity跳过、0失败/错误；`unified-wal-source-style-Thj6khH7` Spotless/Checkstyle exit0，source集一致，21份Java回执checksums已核验。模型 `unified-wal-model-37pz7m69` tool exit0，182配置为42正向完整PASS/140指定反例，358冻结输入/548 checksums，118共享输入与1164去重union一致；五个designInputs仍绑定父提交 `b61887fd56b2037b886114ef924c40cbf26460ad`。初次style `unified-wal-source-style-IF6pd8GN` 的import排序错误不计PASS；最终运行发生在修正之后，先前测试/模型回执保留其原输入，不改绑定。验证耗时不作为性能结论。

复现最小新行为：

```bash
bash scripts/unified-wal/run-local-storage-tests.sh 'SegmentBookieControlLogTest#profileSocketSourceCopyRemainsChargedAfterDisconnectUntilOwnerConsumesIt+profileSourceAllocatorRefusalRollsBackQuotaAndKeepsPrivateCopyContract'
```

原[有限backing投影](../models/README.md#profile-backing-额度投影)检查两个根的分配/最终引用/关闭计费，原ordinary/recovery分发模型检查实际I/O/owner终结；这些是分开的有限切片，不声明完整pipeline/source/pool组合或总内存证明。JDK/native TLS内部对象、kernel buffers、metadata/index/object工作集、对齐池与接收资源的统一配置及总账、生产listener/readiness、READ_ENTRY成功/确定missing、真正RepairIntent/grant控制面、client quorum、完整A/C/D、掉电、长期及性能Gate仍OPEN；步骤6/11继续PARTIAL。


## 真实隔离初始 control 接线

2026-10-06：显式 `SegmentProfileControlConfig` 开启隔离 INSTALL/ACTIVATE_INITIAL；默认连接仍保持原空 capability。双方 HELLO 必须包含实验 capability `0x55494331`、semantic version 1，才接受该 body。它是可丢弃 prototype schema，通用 `ProfileOperationCodec` 对 control body 的 `BLOCKED_UNFROZEN_CONTROL_BODY` 不变，stable wire/G1、old-decoder 与 stock-binary 状态不提升。实际 server identity/incarnation 来自本地 header，principal 来自 TLS 1.3 REQUIRED mTLS 的真实叶证书；接收用途仍按首个非 HELLO 操作识别，不增加 HELLO 用途字段。

[独立初始 codec](../../../../../bookkeeper-common/src/main/java/org/apache/bookkeeper/common/profile/wire/ProfileInitialControlCodec.java)使用 big endian，request 公共布局如下。目标 Bookie/incarnation 仅从实际本地 HELLO facts 绑定，不接受网络侧任意 MetadataStore path。

| offset | 字段 | 约束 |
| --- | --- | --- |
| 0 | 原 ProfileLedgerContext，60 bytes | ledger、instance、descriptor identity |
| 60 | UWIC magic u32、schema u16、reserved u16 | schema=1、reserved=0 |
| 68 | operation ID 16 bytes、public semantic identity 32 bytes | 原 operation 身份，不重生成 |
| 116 | authority generation i64、domain length u16 | generation 正、domain UTF-8 1..128 bytes，无 NUL、非法编码或空白 |
| 126 | authority domain | ACTIVATE_INITIAL 到此精确结束，127..254 bytes |
| 126+d | INSTALL descriptor length u32、engine u16、credential kind/length 各 u16 | engine=3、kind=1、length=20 |
| 136+d | 合成/真实 protected credential 20 bytes，然后 canonical descriptor | descriptor 124..508 bytes；INSTALL 总长 281..792 bytes |

opt-in decoder 从固定 header 检查这些 body 上下界，再保留 body。codec 检查 magic/schema/reserved、严格 UTF-8、实际尾长、engine/kind/length；原 endpoint 校验 canonical descriptor、digest 及语义。60-byte reply 为原 status 8 bytes、UWIR magic u32、schema u16、显式 outcome code u16、ledger i64、operation ID 16 bytes、authority generation i64、local generation i64、reserved u32=0。outcome 1..11 依次为 APPLIED、ALREADY_APPLIED、CONFLICT、DURABILITY_UNKNOWN、NOT_FOUND、STALE_OR_COMPACTED、DENIED、TRANSIENT_RECONCILING、QUARANTINED、BAD_REQUEST、UNSUPPORTED；code 不依赖 enum ordinal。成功必须 durable 且 local generation 正，status/outcome 一致；历史 stale 和 UNKNOWN 不伪造成无效果。无法形成完整请求身份的 malformed/未协商/cold 队列拒绝仍使用原 correlated status-only 8-byte 错误回复。

三份[合成固定向量](../../../../../bookkeeper-common/src/test/resources/profile-wire/initial-control)由独立 Python struct/hashlib 编码构造，包含公开测试 credential、非生产密钥；测试精确比较整个 request/reply，并独立检查 offset。INSTALL 294 bytes SHA-256 `be79a077ab97f0bf6a6bb1e7f864cb4a0619480839e473a9596a0c4c878c75c6`，ACTIVATE 140 bytes `69c125978ee8068509c550d5df4c05c113d4634dc5852807fd9a36f06331b9e4`，APPLIED reply 60 bytes `7055579940c0c45b5f9bf5bf8c84fba2f2df2e4771ef13399bf6e5beee54e08d`。vector 不进入旧 decoder 的 stable corpus/Gate。

[原 handler](../../../../../bookkeeper-server/src/main/java/org/apache/bookkeeper/bookie/storage/segment/SegmentProfileAddHandler.java)在现有连接 pending 上限内，先保留 `8 * bodyLength + 2 * 60 + 44` 的解析/响应 workspace，再在调用方拥有的有界 cold ThreadPoolExecutor 执行原 endpoint：actual principal → static permission → authority cold read → exact AuthZ → descriptor 校验。只有此顺序通过，才在 event loop 原子把 connection/base/frame/workspace 转 CONTROL，随后提交原 physical driver。后续逐次鉴权；Executor 使用有限线程/队列及 AbortPolicy，不能把阻塞任务退回 event loop，调用方保持这些边界并排空已接纳任务后关闭 executor。没有新 executor、重试层、日志或 MetadataStore 框架。

cold task、实际 control I/O/owner、排队 response 和输出 backing 分别保留原 pending。原 facade 的 actual-terminal observer 在未提交的确定拒绝时立即调用，在已提交时仅于实际 owner callback finally 调用；cold timeout/interrupt 只返回 UNKNOWN。UNKNOWN 已回复或断连仍不能归还尚在使用的 frame/解析副本额度。event-loop admission 的 future 已超时后不再允许迟到提交；实际授权转账失败也无 control effect。

七个真实 mTLS socket 回归覆盖：INSTALL/相同 generation 重试/ACTIVATE 与后续拒绝、未协商 capability 与首请求权限拒绝、force 被阻塞时 UNKNOWN 后断连仍保留控制副本/文件锁、实际 ZK PREPARING 冷读与 DELETE cut 后零写入、cold timeout 后迟到 event-loop 授权零写入、超上限固定 header 不读 body/authority、有界 cold 队列耗尽不在 event loop 执行。ACTIVATE 成功 socket 用例使用 committed-authority fixture；完整实际 READY/metadata reader、三个目标和可恢复协调器经真实 client/socket 组合仍待接线验证。实际 ZK socket 用例只验证一个已规划 INSTALL target，不冒充全 E 创建。

复现最小新行为：

~~~bash
bash scripts/unified-wal/run-local-storage-tests.sh 'ProfileInitialControlCodecTest,SegmentBookieControlLogTest#profileSocketInitial*'
~~~

非阻塞实际 control client/请求关联及全 E socket coordinator、生产 listener/fresh readiness、完整 control-plane、JDK/native TLS 内部/kernel/metadata/object/对齐池的统一总账、步骤11/12和生产 Gate仍未完成。以上 workspace 保留只覆盖声明的已知解析副本与 response，不能作为整个进程或存储总内存上限。最终 `unified-wal-local-j9MoHs4Q` tool exit0：1002冻结输入，相关17类417项为412通过/5 macOS Linux-affinity跳过，0失败/错误；Bookie控制log完整252项包括七项新socket回归。`unified-wal-initial-socket-style-9ga4n66n` Spotless/Checkstyle exit0，142冻结Java source与test inputs一致；23份Java/style回执checksums已核验。最终模型 `unified-wal-model-0tgx4f8_` tool exit0：233配置为49正向完整PASS/184指定反例，483冻结输入/701 checksums，197共享输入及1288去重union一致；五个designInputs仍绑定运行父提交 `d7a68ffc03eb9a21bd91c1250fb6fb7b42d61f5e`。原始receipt/source/config/run不重绑定。官方tool1.7.4、单worker、heap512MiB/direct64MiB、seed1/fp0、无symmetry/state constraint、CHECK_DEADLOCK FALSE；有限PASS不代替实际TLS、fsync/掉电、内存、活性/公平性或完整A/C/D组合证据。


## 非阻塞初始 control client

2026-10-06 [ProfileInitialControlClient](../../../../../bookkeeper-server/src/main/java/org/apache/bookkeeper/discover/profile/ProfileInitialControlClient.java)直接实现现有 InstallClient/ActivationClient，只拥有一个隔离 NIO channel；SSL context、已解析远端地址与 event loops 由调用方提供。它不创建线程、executor、重试、重连、fallback 或第二套 metadata 框架。未解析 InetSocketAddress 在建 channel 前拒绝，避免当前 Netty 4.2.15.Final 默认 resolver 的同步解析进入 event loop；上层承担地址发现/解析，不为该实验 client 新建解析层。

connect 的 future 只在实际 TLS 1.3/mTLS、HTTPS hostname verification，以及 actual HELLO engine=3、预期 Bookie/incarnation、UIC1/schema1 capability 都通过后成功。TLS/HELLO deadline 与每请求 deadline 使用借用的 event loop，TCP connect 也有 timeout；close 异步关闭自己拥有的 channel并停止新请求，不关闭借用资源。首个非HELLO仍决定连接接收用途，初始client随后复用CONTROL连接，不逐操作握手；普通DATA要使用相应DATA连接。

每channel frame ID从2单调生成，耗尽前关闭而不重用；完整reply关联header operation/ID和body ledger/operation UUID/authority generation。原60-byte codec继续校验status/outcome、local generation及reserved。只有严格canonical的BAD_REQUEST/DENIED/TRANSIENT/UNSUPPORTED status-only 8-byte错误可形成当前尝试的无效果拒绝；8-byte成功、错binding、错flags、畸形/意外reply一律不能成为durable成功。断连/timeout令未完成future exceptional UNKNOWN，不把它变成NONE，也不更换逻辑operation/instance/generation；Caller显式重试仍用原scope。

maxPending为1..32，quota在排入event loop前取得，覆盖已排队及实际pending；满额返回当前尝试correlated TRANSIENT、local generation0、durable=false。请求临时编码数组在复制进outbound后清理；client持有的序列化buffer保留到真实write promise结束，再清理并释放。wire逻辑终结与write终结都完成后才归还slot；future取消只停等待，已发送请求仍保留关联及输出资源，不取消server I/O。每个outbound的声明capacity最多828 bytes，client同时保留的数量受quota约束；这不是Netty pooled backing、TLS内部/native/kernel或整个进程内存的总量证明，也不声称JVM/TLS所有秘密副本已擦除。

[真实三目标组合测试](../../../../../bookkeeper-server/src/test/java/org/apache/bookkeeper/bookie/storage/segment/SegmentProfileInitialSocketClientTest.java)使用实际FileChannel控制I/O、原storage/driver、三个真实mTLS listener、实际ZK PREPARING/READY与标准LedgerManager。两个phase分别让整个E进入force前阻塞，INSTALL未完成时不创建metadata，READY已发布但最后ACTIVATE未durable时不返回AVAILABLE；全部匹配完成后返回3/3/2初始availability snapshot。激活reply真实丢弃导致UNKNOWN，显式新client/新coordinator从同一READY/metadata/op恢复；必需metadata实际删除后QUARANTINED，不重建或写入控制日志。其余测试覆盖错HELLO/hostname/incarnation/未协商capability/未解析地址零control写入，五类reply关联/状态破坏不能成功，取消等待的quota保留，冷队列拒绝的8-byte status，以及实际reply已到而output promise仍持有时的slot/序列化副本生命周期。

输出生命周期的实际RED `unified-wal-local-czs6SjrT`仅临时取消write-terminal条件，预期TRANSIENT却得到ALREADY_APPLIED，tool exit1；脚本finally恢复原client。随后focused `unified-wal-local-vNtlhvEA`八项通过，扩大 `unified-wal-local-sieOW1ad`相关18类通过；其后增加已解析地址约束，最终另行冻结，不重绑原结果。[有限client投影](../models/README.md#初始-client-关联与-slot-投影)检查peer/binding、唯一frame ID、两种terminal、取消及UNKNOWN。

尚无LedgerHandle/生产writer、普通Add quorum SDK、replacement/recovery/full delete/catch-up、production listener/fresh startup-readiness或完整process-level重启；该组合不证明持续authority lease、跨key原子撤权、完整A/C/D、统一TLS/socket/storage总账、长期/掉电/性能或stable wire/G1。调用方需排空已接纳任务/channel输出后关闭借用资源。步骤11/12及生产Gate仍PARTIAL/OPEN。最终 `unified-wal-local-4qDodbai` tool exit0，1004冻结inputs，相关18类425项为420通过/5 macOS Linux-affinity跳过、0 failure/error；包含8项真实initial client/ZK/file/socket组合。`unified-wal-client-final-style-tyd9etl4` Spotless/Checkstyle exit0，200冻结Java source与去重union精确一致；24份Java/style回执checksums已核验。最终模型 `unified-wal-model-e10jjm5g` tool exit0，241配置为50正向完整PASS/191指定反例、494冻结inputs/725 checksums；199共享inputs及1299去重union一致，五个designInputs绑定父提交 `57a614c304e72985d850033c36dcff3f7232cdd5`。官方tool1.7.4，单worker、heap512MiB/direct64MiB、seed1/fp0，无symmetry/state constraint，CHECK_DEADLOCK FALSE；有限模型不替代实际crypto/fsync硬件、活性、公平性、整个内存总量、完整A/C/D、掉电、长期、性能或生产authority。历史source/config/run/receipt保持原绑定。

复现最小组合范围：

~~~bash
bash scripts/unified-wal/run-local-storage-tests.sh 'SegmentProfileInitialSocketClientTest'
~~~


## 普通 DATA client 与 SDK 接线边界

2026-10-06 [ProfileNormalDataClient](../../../../../bookkeeper-server/src/main/java/org/apache/bookkeeper/discover/profile/ProfileNormalDataClient.java)接入原frozen ADD_NORMAL与status-only reply；[ProfileSocketClient](../../../../../bookkeeper-server/src/main/java/org/apache/bookkeeper/discover/profile/ProfileSocketClient.java)复用CONTROL/DATA当前两种实际用途的TLS/HELLO、bounded pending、单调frame ID、reply关联、deadline及write/wire终结。原ProfileInitialControlClient公共connect/install/activate/hello/close接口不变，成为薄封装；没有复制另一套TLS生命周期或增加可插拔backend、retry/reconnect、线程/executor。DATA HELLO不要求实验UIC1，但仍验证实际TLS13、host、engine3、Bookie/incarnation；CONTROL保留原UIC1 requirement。

调用方给出installed完整entry上限36..1MiB+36；输入为原CRC32C/v3 BookKeeper entry的ByteBuf或至多两段ByteBufList。大小用long累计，在取得quota、clone/retain之前检查；满额返回当前尝试TRANSIENT/SAME_PROFILE_OPERATION/NONE。持有quota后才创建retained index snapshots，不改变caller readerIndex；caller可在方法返回后释放，future完成前不得修改共享内容。source只在event loop复制一次进定长outbound，随后归还borrow；不经reference整包克隆或先coalesce再复制。100-byte context/entry ID/flags0/kind1/credential20/length prefix不变，无新wire schema。

普通pending将ledger context/entry ID绑定唯一header ID。wrong ID/opcode/flags、非8-byte body及OK却无APPLIED/ALREADY_APPLIED不能成为成功。三元status完整交付，server force UNKNOWN保留UNKNOWN；网络断连/timeout令future exceptional UNKNOWN，当前NONE及caller取消不将旧future复活。用途连接分开，deadline关闭client channel不取消server DATA I/O；借用资源需由caller排空后关闭，副本清理不代表TLS/JVM全秘密擦除或统一总内存证明。

[六项DATA组合](../../../../../bookkeeper-server/src/test/java/org/apache/bookkeeper/bookie/storage/segment/SegmentProfileInitialSocketClientTest.java)在actual all-E initial activation后：原DigestManager/v3 ByteBufList经3个mTLS DATA连接达到actual FileChannel force，matching durable status交给原RoundRobinDistributionSchedule AckSet；第一个和重复第一个不达A=2，第二个达到A，第三个未force时仍pending。这只证明transport与原AckSet计数，不是LedgerHandle/PendingAddOp连续callback或完整SDK。authority-read计数在ordinary请求前后不变，正常路径无远端authority读取或逐entry handshake。

其余actual回归覆盖source quota/N/context/credential拒绝及cancel等待、提交后timeout同coordinate解析与文件writer isolation、actual force UNKNOWN及失败DATA session不能标记clean CLOSED、wrong ID/OK-NONE/ERROR flags不能ACK。首轮错误地要求disconnect后receive/source仍计费，但原storage已复制进对齐池，合法归还ingress；现验证force尚未结束、writer lock及same-coordinate原I/O解析，保留原交接合同。重复coordinate未解析时可以等待，不能伪称确定无效果，未跳过或弱化有效合同测试。

最终 unified-wal-local-yMsEARYx tool exit0：1007冻结inputs，相关18类431项为426通过/5 macOS Linux-affinity跳过，0 failure/error；原8项initial及新增6项ordinary DATA组合通过。unified-wal-normal-client-style-n2bhqnuh Spotless/Checkstyle exit0，203冻结Java source与union一致；24份Java/style回执checksums已核验。模型 unified-wal-model-5720ce65 tool exit0：247配置为51正向完整PASS/196指定反例，506冻结inputs/743 checksums，204共享及1309去重union一致，5个designInputs绑定父提交8f6bf6f9501741ca98d968ae2efc2ea8b8563fc5。官方tool1.7.4、单worker、heap512MiB/direct64MiB、seed1/fp0，无symmetry/state constraint，CHECK_DEADLOCK FALSE；不证明crypto/fsync硬件、活性、公平性、整个内存、完整A/C/D、长期/掉电/性能或production authority。历史source/config/run/receipt不重绑定。

复现最小范围：

~~~bash
bash scripts/unified-wal/run-local-storage-tests.sh 'SegmentProfileInitialSocketClientTest#ordinary*'
~~~

原PendingAddOp只接scalar BK rc，失败会进入Classic ensemble mutation或metadata close。不能将Profile transient/UNKNOWN压成int就宣称SDK接通；有界退避和UNKNOWN累积、Profile install→membership→activation换组、fenced-close/recovery和连续callback仍需接入原SDK。尚无LedgerHandle/生产writer，步骤11/13、生产listener/readiness、统一总账、SDK quorum/完整恢复/删除、stable/G1及生产Gate仍PARTIAL/OPEN。

### 固定初始 ensemble 的原 SDK 写入接线

2026-10-06，步骤 13 增加 ProfileLedgerHandle 与 package-private ProfileBookieClient。前者消费实际 all-E AVAILABLE snapshot 和精确目标 DATA clients，复用原 LedgerHandle/PendingAddOp 分配坐标、打包 CRC32C/v3、按不同 target 计 quorum、保持连续回调和 LAC/length。没有另建 quorum 或 pending-op 框架。初始事实再次校验 metadata/page、当前 activation 的 scope/local generation 及 actual HELLO 的 engine/Bookie/incarnation；snapshot 不成为持续权限 lease，服务器仍按当前本地权限判断 Add。

当前配置明确要求 v3、explicit LAC interval=0、delayEnsembleChange=true、关闭 fault-domain write gate。每目标保留原打包字节与 ledger/entry 坐标，用原 OrderedScheduler/OrderedExecutor 做 1～16 次尝试、1 ms～1 min deadline 及有界 backoff；原 SDK quorum timeout 仍有效。只有完整 durable APPLIED/ALREADY_APPLIED 进入原 OK callback；可重试资源 NONE 和 UNKNOWN 先分类，退避阶段不直接触发换组。无隐式重连、身份改变或坐标重用。耗尽后交回原 quorum 故障逻辑；当前 Profile 换组尚未实现，因此 quorum 不可达时停止本地写入，既不替换 membership 也不发布 Classic CLOSED。

hasUncertainWrites() 是该 handle 的历史不确定性观察：后续 NONE 或成功不会清除它，不等同于已有恢复证明或最终未决清单。网络失败、在途 deadline 及在途本地停止均可能留下远端 DATA。原 scalar SDK failure 可以表现为 WriteException，调用方须保留该观察并经后续 Profile recovery/close 解析。当前没有把逻辑失败解释为远端 NONE，也不取消服务器文件 I/O。

前台最多 1～256 个待回调 entry，payload 硬上限 1 MiB；额外 entry/oversize 在分配坐标前拒绝。每目标另有同规模逻辑投递上限，使 A 已完成后的迟到投递仍受限制。下层 DATA client 的 source/output/write/wire 配额继续独立保护实际引用。transport 借用 DATA clients/TLS/event loops；stopLocalWrites() 等待本地逻辑投递关闭和引用归还，之后调用方关闭下层 clients 并排空 channel，再关闭 SDK context。它不等待远端物理终结，不证明 ledger 已 fenced/closed。read/LAC/force transport 和 Profile recovery 未实现；closeAsync 明确失败，metadata 仍 OPEN。此隔离工厂不接入普通 BookKeeper createLedger 或生产启动入口。 现有 OPEN ledger 的 writer 重建没有实现；不得把再次取得 AVAILABLE snapshot 或再次调用 initial 工厂当作重启 writer 的资格。持久 writer 暴露与恢复后 start coordinate 的闭合仍属后续步骤13/14，不由本轮成功路径证明。

已有真实 mTLS/ZK/文件 fixture 增加五项 SDK 组合：三个首 entry 的真实 reply/write promise 被保留，后 entry 先获得 quorum 仍不能越过回调空洞；实际 DATA slots 满时原坐标有限退避，额外前台 entry 不占坐标；deadline 失败后实际 force 仍继续且 metadata 版本/state 未改变；真实 force UNKNOWN 后的已知 NONE 不会清除历史观察；scheduler 拒绝时已接纳逻辑写入和 stop 都终结。原八项 initial-control 与六项 DATA tests 保留。不是长期、掉电、全内存、完整 A/C/D 或生产可用性证据。

复现新增定向用例：scripts/unified-wal/run-local-storage-tests.sh 'SegmentProfileInitialSocketClientTest#originalSdk*+sdk*'。模型及完整实际回执见下一段；中间编译错误和未通过的 style 检查不计作通过。

最终 Java 回执 unified-wal-local-lHxRMIwp exit0，1009 冻结输入；实际执行13类418项，413通过、5项 macOS 上 Linux-affinity 跳过、0 failure/error，其中 socket 19项及原 PendingAddOp 19项均通过。原 selector 含未存在的测试类名，不能计为已执行；复现实际范围为 scripts/unified-wal/run-local-storage-tests.sh 'IsolatedSegmentConfigTest,SegmentBatchFormatTest,SegmentControlFormatTest,SegmentArenaControlLogTest,SegmentBookieControlLogTest,SegmentOwnerLoopTest,SegmentExecutionGroupTest,SegmentProfileInitialSocketClientTest,ZKProfileReservationStoreTest,ProfileLedgerMetadataStoreTest,ProfileTlsContextFactoryTest,ProfileOperationCodecTest,PendingAddOpTest'。定向 unified-wal-local-qju9ohzO 五项通过；随后在途 stop 不确定性与格式修正进入上述最终快照。首次编译 unified-wal-local-CM2qHlj9 失败及 initial style 行长错误保留为非通过结果。

Spotless/Checkstyle exit0，回执 unified-wal-sdk-style-9mkj8q1k 的135个 server Java 输入与源码 union 一致；19份 Java/style checksums 已逐项核对。模型 unified-wal-model-5b1j8vic exit0：253配置为52正向完整PASS/201指定反例，521冻结输入、761 checksums；212共享输入、1318去重 union 一致，5个 designInputsAtSourceCommit 绑定父提交6df172fb5e664fe12ba642ebd85960acee84343b。新 SDK projection 为9613 generated/2476 distinct/queue0、depth/diameter12；五个变异均违反预期性质。工具仍为官方1.7.4、单worker、heap512 MiB/direct64 MiB、seed1/fp0、无symmetry/state constraint、CHECK_DEADLOCK FALSE。未替代真实掉电/crypto/fsync硬件、全内存、活性、公平性、全部 quorum 配置、replacement/recovery/close、完整A/C/D、长期/性能或生产 authority；步骤13保持PARTIAL。

### Replacement 冷路径的标准 metadata 前置

2026-10-06，[出生绑定 membership CAS](../profile-control/README.md#标准-membership-的出生绑定-cas)已使用实际 ZK metadata parent及只读 birth marker，冻结物理出生身份/完整snapshot/mapping，保留creator token，并拒绝freeze/recovery/CLOSED/publication。它不增加 wire字段、逐entry控制或新的DATA retry机制，也不提供remote inactive INSTALL/ACTIVATE_REPLACEMENT、post-CAS history authority或SDK mapping切换。当前 ProfileLedgerHandle 仍在故障时本地停止，不能把CAS成功直接投影成替换目标可写。步骤13保持PARTIAL；下游消费必须补齐目标留存和whole pending ACK gate。

### Replacement publication 与当前 wire 边界

2026-10-06，[replacement授权与目标留存](../profile-control/README.md#replacement-授权与目标留存)已接入真实ZK、标准metadata birth/version的同一publication multi及原reserved file driver的typed ACTIVATE_REPLACEMENT。当前latest authority与cleanup discovery分别查询，DELETE保留历史且停止新的远程授权；SDK故障路径仍本地停止。此增量没有更改initial-control UIC1协商/固定schema或ADD_NORMAL wire，不advertise replacement能力，也不把已发布metadata/authority投影为SDK完成。下一步仍需冻结并接actual replacement control wire/client，再完成原pending全局gate、旧incarnation响应过滤及受影响副本resend。stable/G1和production Gate不提升。

### UIC2 replacement 控制链路

2026-10-06，[new-peer准备与actual replacement wire](../profile-control/README.md#new-peer-inactive-install-与-replacement-wire)复用ProfileInitialControlClient/shared socket与original server control dispatch。UIC1/schema1保持，opcode ACTIVATE_REPLACEMENT使用独立UIC2 id0x55494332/version2及内层schema2；wrong purpose/schema不解析成功，未协商不提交。使用原实际TLS1.3/mTLS/HTTPS peer校验、principal/static role、cold exact authority、frame correlation、CONTROL budget/copy及write/wire终结；没有新增连接用途字段或普通Add协商。四节点真实new-peer inactive INSTALL、ZK membership/page/publication、ACTV、DATA及lost ACTV response同op解析已联通。shared transport仍无retry/reconnect；SDK映射整体切换/resend尚未接通。generic控制body与stable/G1/stock gate保留原BLOCKED/DEFERRED，不能称production writer已完成。

## 原 SDK 写期换组与整体回调门

2026-10-07，实验 `ProfileLedgerHandle.enableReplacement` 在首个 append 前接纳调用方给出的 verified CONTROL/DATA target pool；pool 与变更次数分别最多32，不自动发现、注册或连接目标。失败 slot 在原 ledger executor 串行处理，固定 LAC+1、当前 metadata snapshot 和唯一 INSTALL/ACTV 身份，调用 [ProfileReplacementCoordinator](../../../../../bookkeeper-server/src/main/java/org/apache/bookkeeper/discover/profile/ProfileReplacementCoordinator.java) 完成真实 inactive INSTALL → guarded membership CAS → retained authority → 全部新目标 local activation。借用现有 bounded cold executor、连接及 SDK scheduler，不另建线程、quorum 算法或 retry 框架。

AVAILABLE 只在匹配的 durable activation、generation 及最终实际 authority/metadata 检查后返回；membership 已提交但停止接纳时返回 CONFLICT，不能把跳过 activation 的中间结果当作可用。已接纳的物理/冷任务可能继续完成，停止不撤销其效果；未确定的 Operation 留存，显式 reconnect 必须保留原 slot、Bookie/incarnation 和所有身份字段。解析成功不复活已失败的 SDK writer。

整体 completion gate 贯穿冷流程及原 CLIENT-1 pending 更新。先切换 verified client mapping，再条件更新原 metadata、撤销全部受影响 pending slot 的旧 ACK，最后只重发受影响副本；未变副本的有效 ACK 保留，恢复原连续回调。Delivery 捕获实际 peer/incarnation、原 packaged bytes 与 entry coordinate；排队期间 mapping 改变也归还旧 request count，避免查找已移除 peer。旧 delivery 的 logical terminal 不取消实际 write/wire/I/O custody。Profile 专用 PendingAddOp 入口过滤已退役 incarnation 的 ACK，同时保留 CLIENT-2 计数与回收；Classic 普通入口不增加该判断。

实际五节点组合以失败的旧 slot1/2、成功的新 slot3/4 和两个 pending entry 验证：INSTALL/ACTV force 分别被阻塞时不回调；完成后回调0、1、LAC1、length32，未变 slot0 没有重发，标准 metadata 保持 OPEN，initial 与 replacement 共5个目标留存。丢失 ACTV response 会停止 writer，显式新连接只解析原 Operation；另验证准入前停止无准备/metadata效果、membership 提交后停止不报告 AVAILABLE且可同操作解析。提交后停止的真实 RED 为 `unified-wal-local-rRh6Wdw7`（预期 CONFLICT，实际 AVAILABLE）；修复后定向 `unified-wal-local-5s6oLW8f` 四项通过。最初 `clwnI27R` 是测试缺少 import 的编译失败，不计为 RED 或 PASS。

当前范围为预连接、有界 target pool、不同 BookieId 的 3/3/2 隔离写期换组。当前 slot 替换为同 BookieId 新 incarnation 仍拒绝；rejoin/离线追赶尚未实现，过滤代码不代表这些场景已端到端验证。registration discovery、controller 进程重启、全配置/连续多轮实际换组、read/recovery/fenced-close、AutoRecovery、delete/catch-up/GC、生产 listener/readiness/完整预算、长期/掉电/性能及 stable G1 仍未验收；步骤11～16保持 PARTIAL/OPEN。

复现本增量的最小范围：

~~~bash
bash scripts/unified-wal/run-local-storage-tests.sh 'SegmentProfileInitialSocketClientTest#sdkReplaces*+sdkControlUnknown*+coordinatorCancelled*+coordinatorStopped*'
~~~

最终相关回归 `unified-wal-local-QcOf8k2C` exit0：实际8类373项全部通过，1012冻结输入；包含25项实际 socket组合、19项原 PendingAddOp 以及252项 Bookie control测试。扩大运行 `unified-wal-local-UaFKpS8N` 的7项失败是旧 HELLO 精确集合只期望UIC1，按已实现的 UIC1+UIC2 广告合同更新共享fixture后重跑通过，未跳过或弱化权限/force/timeout/资源断言。最终 style `unified-wal-sdk-replacement-final-style-6mriyfc3` exit0，213个共享 Java 输入一致；Java/style共14份checksums核验。

最终模型 `unified-wal-model-_ugmmp39` exit0：286配置为56个完整正向PASS、230个指定反例，561输入/860份checksums核验；215共享输入及1358去重union与当前源码一致，5个designInputs保持父提交 `78135bdd2efbbca2e13e54c5f5b2c6fe9e04daaa`。新gate投影31889 generated/8488 distinct、queue0、depth27。工具1.7.4、单worker、heap512MiB/direct64MiB、seed1/fp0，无symmetry或state constraint，CHECK_DEADLOCK FALSE。仅有限模型及当前隔离测试范围，不提升完整A/C/D、活性、长期/掉电/性能或生产权限；历史回执不重绑。

~~~bash
bash scripts/unified-wal/run-local-storage-tests.sh 'SegmentProfileInitialSocketClientTest,PendingAddOpTest,ZKProfileReservationStoreTest,ProfileLedgerMetadataStoreTest,ProfileInitialControlCodecTest,SegmentProfileInstallStoreTest,SegmentBookieControlLogTest,ProfileReceiveDecoderTest'
mvn -o -P '!cargo-zigbuild' -pl bookkeeper-server -am -DskipTests spotless:check checkstyle:check -q
python3 scripts/unified-wal/run-storage-models.py --tool /tmp/unified-wal-tla2tools-174.jar
~~~

## 3/3/3 与同起点连续换组验证

2026-10-07，现有真实 fixture 支持每个实例冻结 A=2 或3的 descriptor/metadata/context，默认3/3/2用例继续使用原 bytes；增加两项实际 SDK 回归。生产 writer、cold coordinator、wire及持久化格式沿用上一增量。

3/3/3 普通写在第三副本 DATA force 被阻塞时，等待前两副本实际响应、BookieClient logical request 终结并排空原 ledger executor，再确认 future 未完成、LAC=-1；第三副本 durable 后才回调 entry0，length16。该门槛不只依赖 server 发送响应的时点。

五节点连续换组先令旧slot1实际 force失败，替换为socket3；socket3也实际 force失败，随后替换为socket4。两次 ACTV force分别阻塞，第二个 entry在第一次 ACTV尚未完成时进入原SDK pending队列。两个 future在第二次 ACTV前均不完成，LAC保持-1；标准metadata始终只有起点0的fragment，最终mapping为socket0/socket4/socket2，sidecar head序号4，replacement-3/4两页均保留，initial与两次目标共5个。最终回调0、1，LAC1、length32，原UNKNOWN观察保留，metadata保持OPEN。未变socket0/2各只收到两次DATA响应；新socket4要求两份durable结果，其他响应只能是NONE，且最多6次（两条entry各最多3次attempt）。源bytes与entry coordinate沿用原 pending，不把资源拒绝后的有界重试当作新的逻辑写。

首轮 `unified-wal-local-G2HmYEjG` 普通3/3/3通过、连续换组因把新目标响应数固定为2而失败（实际3）；这是测试未区分资源重试的限制，不能计为生产bug RED。后续 `unified-wal-local-OaTw5Lyu` 两项通过，再加强logical request终结、callback记录顺序和其他结果必须为NONE的检查。未变副本精确次数、两份durable、回调/metadata/留存及上界断言均保留。最终回执见后文。

当前仅证明这些隔离场景及既有有限模型；更多连续变更/故障交错、同BookieId新incarnation、registration发现、controller进程恢复、read/recovery/fenced-close、AutoRecovery、delete/catch-up/GC、生产listener/readiness/完整预算、长期/掉电/性能与stable G1仍OPEN，步骤13保持PARTIAL。

~~~bash
bash scripts/unified-wal/run-local-storage-tests.sh 'SegmentProfileInitialSocketClientTest#sdkThreeOfThree*'
~~~

最终 `unified-wal-local-w7DpFvzI` exit0：实际2类46项全部通过（27 socket组合/19原PendingAddOp），1012冻结输入；common/server style `unified-wal-sdk-sequential-style-sf7rs8mz` exit0，213个共享Java匹配，Java/style共8份checksums核验。此次只运行相关范围，上轮373项回执保持原source绑定。首轮style因fixture一行123字符失败，断行后通过；没有修改生产实现或通过删减有效断言获得通过。

最终 `unified-wal-model-nd4nuuor` exit0：288配置为58个完整正向PASS/230个指定反例、563输入/864份checksums；215共享输入/1360去重union与当前源码一致，5个designInputs保持父提交 `903f2bd8dccf239b272b107e6c42206d8116d390`。A=3一次变更完整26639 generated/7162 distinct、queue0、depth27；A=3两次变更完整672783 generated/119418 distinct、queue0、depth44。tool1.7.4、单worker、heap512MiB/direct64MiB、seed1/fp0，无symmetry/state constraint，CHECK_DEADLOCK FALSE；这是有限检查器成本与有限性质证据，不是系统性能、活性、完整A/C/D或生产权限。历史run/config/receipt不重绑。

~~~bash
bash scripts/unified-wal/run-local-storage-tests.sh 'SegmentProfileInitialSocketClientTest,PendingAddOpTest'
mvn -o -P '!cargo-zigbuild' -pl bookkeeper-server -am -DskipTests spotless:check checkstyle:check -q
python3 scripts/unified-wal/run-storage-models.py --tool /tmp/unified-wal-tla2tools-174.jar
~~~

## READ_ENTRY 接线与验收范围

当前已接通显式URD1读配置、真实mTLS/file/ZK、DATA client与原SDK flags0普通读；后续READ_LAC源取证见[最新增量](#read_lac-源取证)。fenced完整覆盖缺失、权限拒绝、六类malformed reply及取消后实际资源寿命已有局部证据；下文保留该切片历史回执；完整READ_LAC/quorum、fencing read、独立readonly open、恢复/CLOSED与生产权限仍OPEN。

2026-10-07，当前工作区已增加点读实际终结observer：取消仅停止response等待，observer在实际I/O及owner清理后恰好一次触发；成功的LeasedResult仍归consumer，不能借observer提前归还它的pool lease。六种已有真实文件场景覆盖取消、撤权、I/O错误、I/O拒绝、consumer异常和owner失败；另检查成功结果在observer之后仍可访问且仍占用pool。

工作区新增独立experimental READ_ENTRY reply codec与三份Python struct生成的合成固定向量。候选capability为URD1（0x55524431/version1），body状态前缀沿用8字节，成功尾为UWRD magic/schema1/FOUND或MISSING tag/context60/entryId8/length4/entry，固定prefix88；非OK拒绝仍只有8字节、durableResult=NONE。FOUND要求完整BK entry为36..installed bound（绝对最大1MiB+36），MISSING只允许零entry，decode先核验scope、coordinate、精确尾长和BK entry身份再分配一次owned copy。getter仅暴露readonly view，frame关闭不改变已解码内容。这些向量只证明语法和字段，不证明真实缺失、权限或持久化。

该codec尚未由HELLO广告、server或client调用；READ_ENTRY仍未接通，服务端尚无MISSING产生路径。需要继续闭合显式网络scope权限、local candidate/visibility、完整覆盖的缺失证明、响应credit与实际读loan的交接，以及原SDK读取。现有point read找不到location仍为NOT_READY，不能映成NoSuchEntry；恢复控制面、fenced-close和完整步骤14保持OPEN。当前是未提交的接线工作，既有历史model/source/receipt不重绑，也没有新增model PASS或stable/G1权限。

已执行 `unified-wal-local-Exx4vzKf`（exit0），实际common/server共2类5项，零failure/error/skip；其中点读失败路径方法内部覆盖六种模式，另含成功loan和restart原回归。命令为 `bash scripts/unified-wal/run-local-storage-tests.sh 'ProfileReadEntryReplyCodecTest,SegmentBookieControlLogTest#livePointRead*+pointReadPins*+restartedPointRead*'`。`mvn -o -P '!cargo-zigbuild' -pl bookkeeper-server -am -DskipTests spotless:check checkstyle:check -q` exit0，`git diff --check`通过。这些局部结果不验证socket、网络权限、MISSING语义或整个恢复协议。

2026-10-07后续工作区已把URD1接到显式read配置、实际server/共享socket/DATA client及原ProfileBookieClient.readEntry：无read配置不广告；DATA client明确offer，未协商不发读请求，固定header早拒绝非72字节body。TLS principal与完整query scope经本地nonblocking policy校验，回复前再检查权限与本地read visibility；OPEN recovery range不向普通读暴露。原SDK按已有LAC范围与digest校验读取，flags0/非负point ID启用，READ_LAC、fencing read、独立readonly open和恢复控制面仍未接通。historical read peer留存上限为initial32+pool32，不因换组移除旧fragment仍需的已验证连接。

MISSING仅是fenced read cut上的本地缺失事实，不表示旧写未执行。Bookie必须durable fenced、无pending fence、当前read是唯一admitted工作且范围无uncommitted recovery grant；live lookup须fresh miss、没有occupied/pending coordinate或失败/未完成prefix。restart的缺失依据只接受与当前Arena cut匹配、完整stream prefix的全物理scan；index seed/跳过body不证明missing。没有这些依据仍NOT_READY，读refusal不映成NoSuchEntry。

实际terminal observer、Pending.readInFlight与delivery/write状态共同持有原frame/parse/response credit。native Get也算在途read；断连只停等待，真正reader owner/I/O终结后才归还。成功lease由排队EL callback持有，复制一次到精确计费output后close，output仍等write promise；client直接检验固定header/88字节prefix后做一次bounded entry copy，pre-HELLO超长reply在length阶段拒绝。完整TLS/kernel/object/storage统一预算仍未证明。

三项真实组合 `unified-wal-local-194rs4la` 已通过：SDK普通读原payload且无control-read/force增量，actual FILE fence前UNKNOWN与fence后完整coverage MISSING分离且权限撤销拒绝，300ms客户端read超时后frame额度/file pin继续保留到native Get及actual read终结。fence由真实storage driver提交（尚非网络fencing客户端），未伪造成功事实。两次超时夹具失败TH1d7wgl/NJ08tFg4因要求native Get前已有scratch lease；现按实际阶段检查file pin，未放宽原point read scratch/cancel验证。当前仍未提交，扩大回归/style和模型待闭合；完整恢复、fenced-close与步骤14保持OPEN。

本次扩大相关回归 `unified-wal-local-5aQPUGLc` exit0，实际common/server共5类75项全通过（common2/server73，socket30、原PendingAddOp19、decoder21、point read3）；冻结源码hash仍与当前工作区一致。命令为 `bash scripts/unified-wal/run-local-storage-tests.sh 'ProfileReadEntryReplyCodecTest,SegmentProfileInitialSocketClientTest,PendingAddOpTest,ProfileReceiveDecoderTest,SegmentBookieControlLogTest#livePointRead*+pointReadPins*+restartedPointRead*'`，common/server Spotless/Checkstyle exit0，`git diff --check`通过。READ新性质模型、全部必要失败路径与最终source/receipt绑定尚未完成，当前改动保持未提交；不把上述局部回归提升为完整步骤14或生产验收。

最终相关Java `unified-wal-local-cl29WO9o` exit0：实际5类77项全部通过（common2/server75，socket32、PendingAddOp19、decoder21、point read3），1018冻结输入。新增readCapabilityAndExactScopeAreRequiredBeforeReturningAnyEntry与readMalformedRepliesNeverBecomeEntrySuccessOrMissing分别覆盖未广告URD1/错instance零成功，以及实际回复的magic、instance、entryId、BK payload ledger、error flags和长度6种篡改；每种均client future异常，不是NoSuchEntry。common/server style `unified-wal-point-read-style-9fvz6x6v` exit0，216个共享Java源码匹配，Java/style共11份checksums核验。

最终Model A有限读取分发 `unified-wal-model-p92naucz` exit0：296配置为59完整正向PASS/237指定反例，575输入/888 checksums；218共享输入与1375去重union均匹配当前源码，5个designInputs保持父提交4868ff4d64对应完整SHA绑定。新投影896 generated/704 distinct、queue0、depth7，检查权限、fenced/stable complete missing、取消credit、实际terminal、queued loan、output write及reply binding；7个单独mutation命中指定性质。tool1.7.4、单worker、heap512MiB/direct64MiB、seed1/fp0，无symmetry/state constraint，CHECK_DEADLOCK FALSE；不是完整A/C/D、时钟/活性、公平性、真实crypto/byte proof、总内存、掉电/长期/性能或生产权限。MISSING是采样时本地事实，不表示早先操作没有执行，也不是持续lease。

三份合成向量长度/SHA256：found128/f029beaa86b4de6e069a4068dcad5804bdc64012131b4aa2a7a6ad4ce3010288，missing88/c278bdc5cede8009174eaa07e320324ef615810a15adbae73bd13f22c353e752，not-ready8/3335583813fb042858b1555c50ceb447dc49753a17a72971776262ef365dbc08。它们不改变既有initial-control与stable旧decoder corpus的绑定。

~~~bash
bash scripts/unified-wal/run-local-storage-tests.sh 'ProfileReadEntryReplyCodecTest,SegmentProfileInitialSocketClientTest,PendingAddOpTest,ProfileReceiveDecoderTest,SegmentBookieControlLogTest#livePointRead*+pointReadPins*+restartedPointRead*'
mvn -o -P '!cargo-zigbuild' -pl bookkeeper-server -am -DskipTests spotless:check checkstyle:check -q
python3 scripts/unified-wal/run-storage-models.py --tool /tmp/unified-wal-tla2tools-174.jar
~~~

## 真实CONTROL客户端FENCE前置

2026-10-07，当前工作区的ProfileInitialControlClient.fence复用原verified CONTROL socket、frozen100字节FENCE_LEDGER body和8字节status；按原slot、frame ID、actual write/wire终结与plaintext擦除路径运行。调用方保留operation ID，在TRANSIENT/UNKNOWN后显式重连/重试同operation；没有新wire值、body schema、线程或自动reconnect。DATA连接拒绝fence入口。

定向unified-wal-local-O31gUpe6实际3项通过：原pointRead缺失用例已改为真实网络FENCE；错误credential不成功，第一次APPLIED实际force一次、同ID重复ALREADY且不新增force；实际force阻塞导致300ms客户端UNKNOWN后Bookie仍不能close，释放并新CONTROL连接解析同ID得到ALREADY，原future不复活且只force一次。READ_LAC、fencing read与原quorum取证尚未接通，不能把单副本fence或本地missing提升为集群恢复完成。

LAC/length取证仍需保留source entryId。源entry100携带LAC99和累计length1100，并不授权CLOSED(99,1100)；后续适配须读或验证同一context的length锚点，不直接把现有DigestManager返回的源entry length当作LAC前缀length。本增量的最终回执见下文；历史source/receipt不重绑，完整步骤14仍OPEN。

最终相关Java unified-wal-local-LewZ3Uap exit0：实际3类59项全部通过（common4/server55，socket34/decoder21），1018冻结inputs；common/server style unified-wal-fence-client-style-ka3l54th exit0，216共享Java匹配，Java/style9 checksums核验。当前模型矩阵unified-wal-model-j95f2rfy exit0：296配置59完整正向PASS/237指定反例、575 inputs/888 checksums；218共享/1375去重union与当前源码一致，5个designInputs保持父提交e1e6d4e704的完整SHA绑定。复用既有fence-dispatch、socket correlation/source/output及point-read投影，不把这些投影扩展成LAC/quorum/恢复CLOSED证明。tool1.7.4、单worker、heap512/direct64MiB、seed1/fp0，无symmetry或state constraint；历史回执保持原绑定。

~~~bash
bash scripts/unified-wal/run-local-storage-tests.sh 'SegmentProfileInitialSocketClientTest,ProfileReceiveDecoderTest,ProfileInitialControlCodecTest'
mvn -o -P '!cargo-zigbuild' -pl bookkeeper-server -am -DskipTests spotless:check checkstyle:check -q
python3 scripts/unified-wal/run-storage-models.py --tool /tmp/unified-wal-tla2tools-174.jar
~~~

## READ_LAC 源取证

本节记录 URL1 的历史切片；当前 explicit LAC 能力见 [URL2 增量](#url2-explicit-lac-双证据与前缀锚点)，旧 URL1 合同及回执不重绑。

2026-10-07，READ_LAC继续使用既有opcode0x0205和60字节context请求，固定头早拒绝其他body长度。独立experimental capability URL1（0x55524c31/version1）仅在配置独立nonblocking principal+context授权时广告；URD1点读权限不会自动开启URL1。DATA客户端显式协商，两端按完整scope与请求ID关联，服务端回复前重验权限和本地visibility。任一OPEN recovery range阻止这一整ledger查询。

成功body为status8、UWLQ magic、schema1、SOURCE=1/EMPTY=2、context60、sourceEntryId8、entryLength4及完整BK entry，固定prefix88；非OK拒绝只携带status8/durable NONE。SOURCE保留原entry及其sourceEntryId、piggyback LAC、sourceLength；它是验证过的源样本，不承诺全ledger最大LAC，也不把源累计length当成LAC-prefix length。codec先校验scope、exact tail、安装上限、BK ledger/source身份和数值范围，再做一次owned copy；原SDK的CRC/quorum/length锚定适配仍待接入。

选择源复用有界occupied坐标或重启inventory，最高占用坐标未就绪则拒绝，不能绕过UNKNOWN选另一个源并声称最新。实际读取沿用point reader的fresh Get、current selector、文件pin、owner pool loan及终结observer，没有新增线程、逐Add缓存、MetadataStore查询或控制force。EMPTY只允许durable fenced、稳定唯一read admission、无OPEN recovery range、无explicit LAC且全ledger完整覆盖/无坐标的本地切点；重启index seed不证明EMPTY。explicit LAC当前不通过本源协议返回，存在时不得伪报EMPTY。

真实mTLS/ZK/files测试以entry100/LAC99/sourceLength1100验证原字节一致且无远端authority/control-force增量；空目标在fence前拒绝、实际网络fence后EMPTY，撤权拒绝。六类真实reply篡改均future异常；300ms timeout后原frame额度与file pin保留至native Get/read真正终结。原多stream重启测试新增latest源及Arena cut变化拒绝；真实explicit LAC重放后阻止整ledgerEMPTY。定向unified-wal-local-QZS96qxa exit0，实际3类11项通过。最终扩大回归、style与模型绑定见下一段。

SOURCE与EMPTY均不授权CLOSED，EMPTY不表示先前操作未执行，也不是持续absence lease。原ProfileBookieClient的LAST_ADD_CONFIRMED/fencing read仍明确拒绝，独立readonly open、最大/explicit LAC与quorum取证、正确prefix length锚点、基础恢复/CLOSED、AutoRecovery与生产Gate仍OPEN；步骤14保持PARTIAL。

最终源码回执：Java `unified-wal-local-CQpzBTsP` exit0，实际7类88项全部通过（common8/server80：socket37、PendingAddOp19、decoder21、Bookie读/explicit LAC3），1022冻结输入；common/server style `unified-wal-lac-source-style-ge08g8nc` exit0，374个Java输入与Java回执匹配，其中218与模型共享，Java/style13份checksums核验。模型 `unified-wal-model-a9lunm3r` exit0：303配置为60完整正向PASS/243指定反例、585 inputs/909 checksums；220共享/1387去重union与当前源码一致，五个designInputs保持父提交`4eca18453bfbaf1a34f69ff66f9669eb384fb0c8`绑定。新增LAC投影2048 generated/2048 distinct、queue0、depth2，六个mutation命中指定性质。tool1.7.4/单worker/heap512/direct64MiB/seed1/fp0，无symmetry或state constraint，CHECK_DEADLOCK FALSE；有限结果不证明最大LAC、CRC/crypto、完整A/C/D、时钟/活性/总内存、掉电或生产权限。

两份独立Python struct合成向量：SOURCE128/SHA256 `34f963ad35433d6d875ab7db7315f02dd9c9246c99d628ebeff408d088b6071c`，EMPTY88/SHA256 `baaf074b1b24bea032bbce1cb82429157b708fa99d25dd444e9f943808e001f0`。context为ledger44/instance16个0x01/descriptor36个0x02；SOURCE附带ledger44、entry100、LAC99、length1100、合成CRC字段0x12345678及payload0x09080706。向量的CRC只是格式字段，不是有效digest证明；真实文件测试另用原DigestManager生成数据。既有实验UIC1/UIC2与stable旧decoder向量不重绑。

~~~bash
bash scripts/unified-wal/run-local-storage-tests.sh 'ProfileInitialControlCodecTest,ProfileReadEntryReplyCodecTest,ProfileLacReplyCodecTest,SegmentProfileInitialSocketClientTest,PendingAddOpTest,ProfileReceiveDecoderTest,SegmentBookieControlLogTest#restartedPointRead*+livePointRead*+explicitLacIs*'
mvn -o -P '!cargo-zigbuild' -pl bookkeeper-server -am -DskipTests spotless:check checkstyle:check -q
python3 scripts/unified-wal/run-storage-models.py --tool /tmp/unified-wal-tla2tools-174.jar
~~~

## 原SDK的LAC覆盖与length锚定

2026-10-07，ProfileLedgerHandle.readLacEvidence增加一个显式取证入口，借用原SDK ledger executor、已验证且最多64个historical DATA peers与原DigestManager。一个handle同一时刻只有一项取证；每phase并行但单次最多32个源RPC及32个必要边界RPC，等待对应future全部终结后分析，无新线程、重试或MetadataStore调用。取消caller只取消copy future，仍保留聚合查询名额；child socket与远端I/O继续按原quota、frame/pin/loan寿命排空，RPC timeout不证明远端I/O已结束。

先冻结本handle的Versioned<LedgerMetadata>和readPeers snapshot，在最后fragment的E个已验证来源查询URL1源。只有原DigestManager同时通过ledger/source entryId与digest校验，SOURCE才以OK计入原RoundRobinDistributionSchedule.QuorumCoverageSet；只有协议的完整fenced EMPTY以NoSuchEntry计入。UNKNOWN/refusal/坏digest不投有效票，无Classic fallback；UNAUTHORIZED整体拒绝。保留每个有效源的Bookie/incarnation、entryId、piggyback LAC、sourceLength与应用payload bytes，求这些样本的maxObservedLac；这不是所有历史源或explicit LAC的最大值证明。

prefix边界取max(maxObservedLac,lastFragmentStart-1)，固定metadata历史选择该entry的原write set。已读到且属于合法write set的精确同坐标源可复用；其余按必要点读并行读取，原digest校验后只使用该entry的累计length。-1边界为0；没有合法锚点拒绝。已验证锚点之间length不一致，或已取得相邻source与prefix的应用bytes加法不匹配/溢出，均DigestMatchException，不能修统计值制造成功。结果前再检查metadata对象与peer snapshot；变化即拒绝，后续调用重新取证。返回的是同一局部handle快照证据，不能代替fresh远端metadata/birth/RecoveryContext或当前authority校验。

真实socket/ZK/files用entry99 length1000、entry100 LAC99/length1100/payload100确认返回prefix99/length1000，保留两份源100/1100且无control-force/authority-read增量；writer仍LAC=-1/length0。五种故障分别验证覆盖不足、边界缺失、坏CRC不计票、合法digest下的锚点length矛盾和相邻length矛盾。真实EMPTY验证原3/3/2需要两个有效来源、3/3/3需要一个；其余未fenced来源拒绝，metadata仍OPEN。真实response被持有时取消caller，第二次请求被TooManyRequests拒绝；释放wire后新查询成功，旧caller不复活。

定向unified-wal-local-qaShFLEX实际4项通过（方法内部五种故障、A2/A3两配置）。最终扩大回归、style与模型绑定见下文。旧source/receipt不重绑；本API不更新writer的LAC、lastAddPushed或length，不暴露成功close。原通用asyncReadLastConfirmed/BookieClient.readLac/fencing read仍未接通；explicit/最大LAC、fresh recovery metadata、逐来源fencing coverage、required-frontier推进、合法恢复写回、CLOSED、AutoRecovery与生产Gate仍OPEN。

最终Java `unified-wal-local-ciPAMd31` exit0，实际7类92项全部通过（common8/server84：socket41、PendingAddOp19、decoder21、Bookie3），1023冻结输入；common/server Spotless/Checkstyle实际exit0，记录`unified-wal-lac-evidence-style-b6jl_1qq`保留完成chunk c52c45及空输出，375个Java输入与Java回执一致、其中219与模型共享，Java/style14份checksums已核验。最终模型`unified-wal-model-n_xpb0fl` exit0：311配置为62完整正向PASS/249指定反例，595 inputs/932 checksums；221共享/1397去重union与当前源码一致，五个designInputs保持父提交`a72a48394392cb390735e6d5d4c17d96a854de54`绑定。新增A2为4328 generated/3872 distinct、A3为4808/4256，均queue0/depth6；六个mutation命中指定性质。tool1.7.4、单worker、heap512/direct64MiB、seed1/fp0，无symmetry/state constraint，CHECK_DEADLOCK FALSE。最初5qu67oxn因新模块TLA语法优先级错误INCONCLUSIVE并停止，修正及独立检查后才生成本最终回执；不把语法失败算实现RED或模型PASS，历史回执不重绑。

这只验证样本coverage与length锚点，不证明整个前缀连续性、所有副本payload一致性、fresh recovery authority、一般E>W组合、fencing/repair/close、full A+C+D、总内存、时钟/活性、长期/掉电/性能或生产权限。

~~~bash
bash scripts/unified-wal/run-local-storage-tests.sh 'ProfileInitialControlCodecTest,ProfileReadEntryReplyCodecTest,ProfileLacReplyCodecTest,SegmentProfileInitialSocketClientTest,PendingAddOpTest,ProfileReceiveDecoderTest,SegmentBookieControlLogTest#restartedPointRead*+livePointRead*+explicitLacIs*'
mvn -o -P '!cargo-zigbuild' -pl bookkeeper-server -am -DskipTests spotless:check checkstyle:check -q
python3 scripts/unified-wal/run-storage-models.py --tool /tmp/unified-wal-tla2tools-174.jar
~~~

## 恢复上下文与fencing前置

2026-10-07，原ZKProfileReservationStore和ProfileLedgerMetadataStore增加captureRecovery→beginRecovery/readRecovery。一次capture冻结OPEN metadata物理CZXID/version/creator token、完整metadata的canonical SHA256、对应已发布READY/REPLACEMENT lifecycle birth/CZXID/sequence/version/control generation、instance/descriptor及非零operation ID。UNKNOWN保留原plan；重开只采用已持久化的同一token/generation，不产生新operation，也不重建缺少的必要guard。

beginRecovery在同一标准metadata记录写IN_RECOVERY与唯一custom key `bookkeeper.experimental.profile.recovery-context`，保持ensemble历史/E/W/A、password、ctime、用户custom及creator token。原metadata birth guard与新增readonly lifecycle physical-birth guard一同进入最终multi，检查同一head/root版本及未被冻结/有pending token的标准记录。恢复开始后，原membership replacement因状态及reserved token被拒绝。删除、换组或重建竞争不能被version/UUID相同掩盖；提交后scope变化返回non-OK并保留历史效果。IN_RECOVERY本身不证明Bookie已fenced，也不授予恢复Add或close。

UWRC/schema1固定184字节：magic4/schema2/reserved2、context60、operation16、metadata CZXID8/predecessorVersion4/creatorToken8/canonical predecessor digest32、lifecycle CZXID8/UUID16/sequence8/dataVersion4/controlGeneration8、CRC32C4。generation是predecessorVersion+1；digest对去除recovery key的canonical OPEN投影计算，该投影只用于hash和published predecessor校验，永不写回OPEN。一个token及一个lifecycle guard有固定上限，无每entry metadata写入或无限retry ID。独立Python struct向量校验字段和CRC，不能作为真实authority事实。

ProfileRecoveryCoordinator借用显式ArrayBlockingQueue/AbortPolicy cold executor和最多64个已验证CONTROL peers；每次最多对最后ensemble的32个节点并行调用原100字节FENCE。只允许当前已发布目标的Bookie/incarnation；OK且APPLIED/ALREADY才计原RoundRobin coverage，UNKNOWN/NONE或peer不匹配不投票。全部RPC futures终结后重读同一恢复context；coverage满足仅返回FENCED_CONTEXT。fences列表只记录实际durable来源，未fenced来源的missing仍不能使用；后续取证须针对该来源完成现有fence。每coordinator一项在途，caller只取消copy future，无新增线程/重试/自动reconnect，child socket与实际远端I/O仍按原资源寿命排空。

真实ZK/socket/files验证实际初始化后的ledger由OPEN转IN_RECOVERY，原creator/history保持，同plan重复及新store重开均同token/version；三个实际FENCE后normal拒绝，重复不增加force。错peer映射只得到另外两份fence proof且未fenced节点仍normal；coverage不足或错误credential均non-OK，已有实际fences不回退。六个ZK/codec方法覆盖固定向量、真实CAS reply loss、delete cut竞争、相同version的metadata重建、同UUID/version的lifecycle重建、缺少必要guard不重建。生命周期ZK测试的install receipt是该后端已有输入事实，真实Bookie链由socket用例验证；未把typed fixture当作file durability。

初次ak6S0i4o/eMQuYduh为新测试编译错误（ReadStatus引用、非AutoCloseable credential误用）；tIp0g0kR的重建夹具错误地用version0删除READY head。已按实际version修正新夹具，保留生产guard与零effect断言，不算实现RED。定向unified-wal-local-SpGKe2r2 exit0，实际2类8项通过。最终扩大回归/style/模型绑定见下文。

当前只闭合恢复开始与fencing前置。fresh-context LAC/required frontier串联、合法单entry恢复grant/Add、同context CLOSED(P,length)与lost-close解析、独立readonly open、AutoRecovery/delete/rejoin、legacy mutation ACL与完整生产Gate仍OPEN。prototype允许正常Profile读取仍由显式scope policy决定；尚未实现的恢复终态可见性cut不能当作已有语义。无legacy recovered success、full A+C+D、连续lease或生产authority。

最终Java `unified-wal-local-ZluCKxGo` exit0，实际9类143项全部通过（common8/server135：socket43、lifecycle30、metadata19、PendingAddOp19、decoder21、Bookie3），1026冻结输入；common/server style `unified-wal-recovery-context-style-uudk7s_c` exit0，377个Java输入匹配，其中221与模型共享，Java/style15份checksums核验。模型`unified-wal-model-3b4kjjzo` exit0：322配置为64完整正向PASS/258指定反例，609 inputs/964 checksums；223共享/1412去重union均匹配，五个designInputs保持父提交`6661b7049db2482b063731ef2aebcc4543e3be6c`绑定。新A2/A3各12744 generated/9072 distinct、queue0/depth7，九个mutation违反指定性质。tool1.7.4/单worker/heap512/direct64MiB/seed1/fp0，无symmetry或state constraint，CHECK_DEADLOCK FALSE；不是crypto/byte producer、general E>W、frontier/grant/close、full A+C+D、clock/活性、总memory、掉电/长期/性能或生产权限。历史source/design/run回执不重绑。

合成UWRC向量184字节/SHA256 `df404b1fba7db9215bc91a7b9861f3212f3b567c408148ab0efef8a412a968b7`；Python struct/独立bitwise CRC32C生成，ledger44/instance0x01/descriptor0x02/operation0x03、metadata birth17/version0/creator29/digest0x04、lifecycle birth31/UUID(0,37)/sequence2/version1/generation1。其synthetic digest不声称对应真实metadata；真实上下文另由实际ZK计算及验证。

~~~bash
bash scripts/unified-wal/run-local-storage-tests.sh 'ProfileInitialControlCodecTest,ProfileReadEntryReplyCodecTest,ProfileLacReplyCodecTest,SegmentProfileInitialSocketClientTest,ZKProfileLifecycleStoreTest,ProfileLedgerMetadataStoreTest,PendingAddOpTest,ProfileReceiveDecoderTest,SegmentBookieControlLogTest#restartedPointRead*+livePointRead*+explicitLacIs*'
mvn -o -P '!cargo-zigbuild' -pl bookkeeper-server -am -DskipTests spotless:check checkstyle:check -q
python3 scripts/unified-wal/run-storage-models.py --tool /tmp/unified-wal-tla2tools-174.jar
~~~

## fresh-context只读取证

2026-10-07，ProfileRecoveryCoordinator.observe借用原cold executor/SDK context/verified DATA+CONTROL clients，把已有fence前置、fresh readRecovery及retained history与只读session串联。冻结同一operation/generation和metadata snapshot，按已发布current targets覆盖历史incarnation后验证source池；返回前重读同一durable context。一次coordinator permit覆盖整个fence→LAC→point window→post-check，不在fence结束时提前放开；caller只获得copy future。无新线程、重试、自动reconnect或逐entryMetadataStore请求。

ProfileRecoveryReadHandle复用原LedgerHandle的digest/distribution、既有ProfileContext和LAC/source/length实现，在IN_RECOVERY metadata下创建只读取证角色。原SDK ListenerBasedPendingReadOp/PendingReadOp以recovery flag发起合法write set的point read、digest校验及W-A+1 missing聚合。ProfileBookieClient为该角色把fencing flag适配成原CONTROL FENCE→DATA READ_ENTRY；每来源先核验DATA/CONTROL identity/incarnation并完成同instance/同operation durable fence，UNKNOWN/NONE拒绝该来源读取而非missing。每个session最多64个固定source与fence future，已有positive/pending/error future不隐式换operation重试；后续点读复用来源fence，不产生逐entry控制force。normal Add在transport及handle角色两层拒绝，close仅关闭本地session，不更新标准metadata。

LAC保留源provenance并使用正确boundary length；只读window从该boundary后的坐标按序读取最多32个candidate。候选payload与累计length相邻不符/溢出则QUARANTINED；达到window上限返回DEFERRED/missing=-1，不能把预算当作absence。原recovery读聚合definitive missing后，只返回OBSERVED_BOUNDARY样本。最大/explicit LAC、其他accepted ACK/AQ/repair required authority及完整prefix尚未收齐，因此这个样本不命名为normal-tail proof，不能发布CLOSED或legacy recovered success。candidate可能是speculative，后续仍需合法grant/恢复写回与required-frontier判定。

点读FOUND callback现在按原BookieClient借用buffer合同执行：adapter在callback后release自己的wrapper；PendingReadOp保留consumer引用，不提前释放payload。readonly probe通过原LedgerEntry.getEntry取得owned应用bytes，结果只暴露readonly view。source socket/output及远端native IO仍各自保留原quota/pin/loan到实际终结，session/caller停止不证明远端IO已取消。总TLS/kernel/native/对象/存储工作集尚未证明。

真实fixture把entry99/length1000、entry100/LAC99/length1100/payload100读为LAC boundary99/1000、candidate100/1100和missing101；window=1返回DEFERRED，并保持writer状态和metadata IN_RECOVERY。只有一个来源有CONTROL fence时不能把其他两个empty来源计missing；合法digest下entry101/length1300与应用bytes矛盾返回QUARANTINED；LAC reply被实际持有期间第二observe被拒绝，delete cut后释放则返回CONFLICT并保留历史candidate。Add拒绝、local close不写CLOSED及无逐entryforce已验证。定向unified-wal-local-XPYKddKi exit0，实际4项通过；YN9e3BwQ是新测试漏ByteBuffer引用的编译失败，风格长行修复不算实现RED。最终扩大回归/model/style见下文。

fresh-context基础读取已接通，单entry recovery grant/Add、所有required authority/frontier与最大/explicit LAC、恢复终态可见性cut、matching CLOSED(P,length,context)/lost-close、独立readonly open、AutoRecovery/delete/rejoin及生产Gate仍OPEN。

最终Java `unified-wal-local-E2Od7BZC` exit0，实际9类146项全部通过（common8/server138：socket46、lifecycle30、metadata19、PendingAddOp19、decoder21、Bookie3），1027冻结输入；common/server Spotless/Checkstyle实际exit0，记录`unified-wal-recovery-observation-style-23phd7ct`保留完成chunk177207与空输出，378个Java输入匹配/222与模型共享，Java/style16 checksums核验。最终模型`unified-wal-model-jfaltg9m` exit0：331配置66完整正向PASS/265指定反例、620 inputs/990 checksums；224共享/1423去重union匹配，五个designInputs保持父提交`62aef445dd57cd5df6f0b05531cddcd733319f1d`绑定。新增A2/A3各58752 generated/27648 distinct、queue0/depth4，七个mutation命中指定性质。tool1.7.4/单worker/heap512/direct64MiB/seed1/fp0，无symmetry/state constraint，CHECK_DEADLOCK FALSE；不是full A+C+D、完整required frontier/explicit LAC/grant/close、general E>W、crypto producer、clock/活性、总memory、掉电/长期/性能或生产权限。历史回执不重绑。

~~~bash
bash scripts/unified-wal/run-local-storage-tests.sh 'ProfileInitialControlCodecTest,ProfileReadEntryReplyCodecTest,ProfileLacReplyCodecTest,SegmentProfileInitialSocketClientTest,ZKProfileLifecycleStoreTest,ProfileLedgerMetadataStoreTest,PendingAddOpTest,ProfileReceiveDecoderTest,SegmentBookieControlLogTest#restartedPointRead*+livePointRead*+explicitLacIs*'
mvn -o -P '!cargo-zigbuild' -pl bookkeeper-server -am -DskipTests spotless:check checkstyle:check -q
python3 scripts/unified-wal/run-storage-models.py --tool /tmp/unified-wal-tla2tools-174.jar
~~~

## RepairIntent admission与实际恢复写回

2026-10-07，新增固定32槽的ProfileRepairIntent，沿用同一UWRC恢复上下文。每个immutable inert计划绑定fragment起点、被替换slot、原ensemble digest、目标Bookie/incarnation、INSTALL/grant operation、intent/grant generation和exact range；不复制标准metadata的membership权威。prepare/admit的最终ZK multi检查标准metadata与lifecycle physical birth、版本及intent birth，UNKNOWN只解析同槽同计划，冲突不覆盖或另建操作。inert计划只允许inactive INSTALL；取得matching durable INSTALL receipt后才发布readonly admission。admission与目标留存是同一持久事实，DELETE_INTENT之后仍可枚举已admit目标，但fresh grant冷读拒绝已删除或变化的context。

UWRP/schema1固定部分352字节，加bounded target/incarnation/domain与CRC32C，总上限759；其中嵌入完整UWRC184。readonly UWRA admission112字节保存实际local install generation、capability digest及intent/context SHA-256。独立struct/CRC32C向量为repair-intent.bin384字节，SHA-256 `abb56fb985f4c7f9cd927d2c8dbbe421540c90ed8d2f645700b6e837e167c6fb`；字段及hash向量不制造真实安装或authority事实。

RECOVERY_GRANT采用独立UIC3 capability `0x55494333`/schema3/opcode0x0104：原126字节control prefix、1～128字节domain，再加exact range16、intentId16及两代各8字节，body175～302字节；reply仍60字节并核对原操作、authority/local generation。UIC1/UIC2旧向量不变，旧schema不能解释grant；HELLO没有新增连接用途字段。独立recovery-grant.bin175字节向量SHA-256为`6b28e56dd3cf0e030e286ac195ba64fe1da40673fe6a5309b9b1212322d905a2`。public payload SHA沿用native `UW-RECOVERY-GRANT-V1`字节合同，无本地持久格式变更。

真实mTLS endpoint先完成principal、逐操作权限及一次exact fresh authority冷读，后转CONTROL额度并调用既有Bookie控制driver；已安装route及实际write/force/owner apply共同约束durable grant，不激活normal writer。重复grant返回同local generation的ALREADY；现有driver可能再次写/force控制记录，不能宣称零控制force。DELETE前已授权的历史效果不因后续冷读拒绝而回滚，保留目标清理义务。

ProfileNormalDataClient.addRecovery使用原DATA socket、source quota、borrow/copy/write/wire及scalar completion路径，发送已有144字节ADD_RECOVERY prefix和原DigestManager封装entry。grant/context/range绑定不允许普通Add；caller取消不提前释放实际使用中的source或I/O资源。单条重复恢复Add按既有合同返回本地durable APPLIED且不增加DATA force。此入口没有新线程、隐式retry/reconnect、逐entry metadata写入或normal activation。

真实ZK/socket/files用例依次验证unadmitted grant拒绝、显式重连后的inactive INSTALL、actual HELLO/result receipt、admission、UIC3 grant和单entry恢复Add；目标仍normal-inactive，metadata仍IN_RECOVERY。ZK测试覆盖admission实际提交后的reply loss解析、delete/同槽重建竞争、冲突不覆盖、bounded槽及删除后目标留存。typed install receipt仅作为metadata后端输入，实际安装/force链由socket用例验证。

最终Java `unified-wal-local-656ohwXB` exit0：9类151项全部通过，common9/server142（socket47、lifecycle33、metadata19、PendingAddOp19、decoder21、Bookie3），1030冻结输入。Spotless/Checkstyle `unified-wal-repair-grant-style-h6yxeece` exit0，379个Java输入匹配/223与模型共享，Java/style15 checksums核验。模型`unified-wal-model-k2fr4zox` exit0：341配置67完整正向PASS/274指定反例、632 inputs/1020 checksums；225共享/1437去重union匹配，五个designInputs保持父提交`7a1f8bb1239a55eb1df667bfa53e20d0319db154`绑定。新增ProfileRepairGrant为173 generated/109 distinct、queue0/depth4，九个mutation命中指定性质；tool1.7.4/单worker/heap512/direct64MiB/seed1/fp0，无symmetry/state constraint，CHECK_DEADLOCK FALSE。历史回执不重绑。

重现使用上一节fresh-context只读取证的同一Java/style/model三条命令。有限投影只消费inert/install/admission、观察到的delete/birth、exact auth、实际force及目标留存事实，不证明真实TLS/CRC/持久化本身或全竞态线性化。grant close、membership/replica-readable publication、required frontier/最大与explicit LAC、匹配CLOSED及lost-close、完整AutoRecovery/独立readonly open/delete/rejoin、full A+C+D、总内存、活性、掉电/长期/性能和生产权限仍OPEN，步骤14保持PARTIAL。

## UIC4 recovery grant close接线

2026-10-08，真实隔离CONTROL路径新增RECOVERY_GRANT_CLOSE，仍要求mTLS principal、静态operation权限、一次exact cold authority和RECOVERY_CLOSE_AUTHORIZED状态。inert/admitted intent、RECOVERY_OPEN grant、credential或HELLO capability均不授予close；现有ZK repair reader仍不发布close authority。membership/copy结果与close-authority的真实metadata发布器尚未实现，本节验收物理wire/driver消费独立授权输入的行为，不宣称其cluster authority已闭合。

独立UIC4 capability `0x55494334`/schema4/opcode0x0105沿用control126字节prefix与1～128字节domain，追加range16、intentId16、intent/grant generation各8和被关闭grant operationId16，body191～318字节；reply仍60字节，关联本次close operation、authority/local generation。UIC1/UIC2/UIC3旧schema和向量保持，UIC3不能解释close请求或reply。接收固定头在body额度及接收前检查上下限；合法首个close仍占UNCLASSIFIED，真实授权后才原子转CONTROL。HELLO无用途字段，无新线程/重试/自动重连机制。

独立Python struct与SHA-256向量recovery-close.bin为191字节，SHA-256 `30a5877745b3d71407656e902c0219522cdfa6d49f747c09e9a81a6a1e2b3342`；public payload沿用native `UW-RECOVERY-CLOSE-V1`，绑定context、target/incarnation、domain、grant operation、intent/generations和range，未改变本地持久格式或加入credential。typed request复制grant operationId，并拒绝与close operation相同的ID。

SegmentProfileInstallStore调用既有submitRecoveryClose：先建立grant admission cut，pre-cut ticket未终结时返回TRANSIENT_RECONCILING/NONE且不写close记录；DATA已写但cut后失去eligibility仍返回UNKNOWN。实际write/force/owner apply终结后才返回durable APPLIED/ALREADY，以同scope的本地已提交close事实核对positive local generation。重复同close返回同generation并不增加控制force。关闭恢复权限后才发布本地committed-readable，normal writer保持inactive；本地readable不替代客户端的标准membership/confirmed范围选择或完整range coverage proof。

caller timeout/断连只停止等待，actual close I/O/owner与对应request额度继续持有至实际终结，原操作ID保留用于显式重连解析。force或apply失败返回UNKNOWN，不伪造durable success，不重新开放恢复准入；原native restart回归检查matching close重放。没有STATUS_QUERY授权、metadata CLOSED或新的retry operation。

四个真实mTLS/files测试消费明确声明的独立close-authority输入，验证真实ZK repair reader缺少close authority、RECOVERY_OPEN状态拒绝、实际单entry写回及close、重复无额外force、close后的实际READ_ENTRY、pre-cut DATA force持有、close force持有与timeout后同ID解析，以及force失败UNKNOWN。metadata仍IN_RECOVERY且ensemble未变，admitted目标仍在history中；这些测试没有制造membership/copy publication事实。固定头边界测试验证190/319字节在body接收前拒绝，191/318半包在授权前仍计UNCLASSIFIED；codec验证独立向量、ID所有权、跨schema/operation拒绝及reply关联。

~~~bash
bash scripts/unified-wal/run-local-storage-tests.sh 'ProfileInitialControlCodecTest,ProfileReadEntryReplyCodecTest,ProfileLacReplyCodecTest,SegmentProfileInitialSocketClientTest,ZKProfileLifecycleStoreTest,ProfileLedgerMetadataStoreTest,PendingAddOpTest,ProfileReceiveDecoderTest,SegmentBookieControlLogTest#restartedPointRead*+livePointRead*+explicitLacIs*+recoveryCloseWaitsForPrecutTicket*+uncertainRecoveryCloseForce*+profileSocketRecoveryGrantClose*+recoveredRecoveryCandidateLosesEligibility*'
mvn -o -P '!cargo-zigbuild' -pl bookkeeper-server -am -DskipTests spotless:check checkstyle:check -q
python3 scripts/unified-wal/run-storage-models.py --tool /tmp/unified-wal-tla2tools-174.jar
~~~

最终Java `unified-wal-local-cs02h4mj` exit0：9类161项全部通过，common10/server151（socket51、lifecycle33、metadata19、PendingAddOp19、decoder22、Bookie7），1031冻结输入。Spotless/Checkstyle实际session1107完成chunk d22dc9/exit0/空输出，归档`unified-wal-recovery-close-style-9xx5ba13`的379 Java输入匹配/223与模型共享，Java/style15 checksums核验。最终模型`unified-wal-model-1_ferwu3` exit0：351配置68完整正向PASS/283指定反例、643 inputs/1050 checksums；225共享/1449去重union匹配，五个designInputs保持父提交`d6d78ded151c21987908f02d3f7164fe4eafd41b`绑定。新增close投影179 generated/138 distinct、queue0/depth6，九个mutation命中指定性质；tool1.7.4/单worker/heap512/direct64MiB/seed1/fp0，无symmetry/state constraint，CHECK_DEADLOCK FALSE。

定向`unified-wal-local-yK1Xo7mB`为common6/socket3共9项通过，新增force失败与接收头边界的`unified-wal-local-3trdrvbh`为2项通过。首轮4d820fXb是测试漏ByteBuffer引用的编译失败，两处长行修正不作为实现RED。首次完整回归/model的输入随后因补充两项风险验证变化，最终回执已重新冻结，旧source/design/result未重绑。有限模型消费独立close authority，不证明其metadata发布器或实际crypto/bytes/force本身。

实际membership/copy result与ZK close-authority publication、完整required frontier/最大和explicit LAC、matching CLOSED及lost-close、完整AutoRecovery/独立readonly open/delete/rejoin、full A+C+D、总预算、活性、掉电/长期/性能和production authority仍OPEN，步骤14保持PARTIAL。

## 历史fragment repair membership与close授权

2026-10-08，commitRepair是完成copy后的cold发布原语：caller保留同一intent、copied-at标准metadata snapshot与close ID。只允许range完整覆盖有下一fragment起点的历史fragment；最后开放fragment没有terminal/frontier proof时返回CONFLICT。copy完成是调用前提，原语本身不生成copy/required-frontier/strong reset证据，也没有接入完整AutoRecovery worker。

最终ZK multi同时检查metadata physical birth/version、原lifecycle/delete cut、intent birth与admission physical guard，再写唯一标准ensemble mapping及immutable committed receipt。response loss读取同slot/operation的实际结果，不重新选择target；冲突不覆盖。admission现在同时持久化24字节physical guard，路径包含实际intent节点的出生版本。commit head也保存该版本，所以同UUID/version0重建不能通过旧cut；缺失guard不隐式重建。旧实验admission缺少guard时不再提供authority，尚无稳定格式/迁移承诺。

UWRC恢复context及其operation不变；标准metadata增加单个bounded commit-head key，最多32条slot/intent UUID/physical version/content digest引用。每次提交保存before version、before/after canonical metadata SHA、旧member和固定close ID/generation。fresh readRecovery按引用逆演最多32次mapping变化，核对每步digest、birth、admission及版本，再校验原published predecessor；逆演仅用于验证，永不写回OPEN。缺失/损坏必需proof、修改其他metadata内容或错误mapping均fail closed。membership权威仍只有标准metadata，receipt不复制第二份ensemble map。

UWRC/schema1字节不变。新repair commit receipt magic为`0x5557524d`（UWRM，区别于恢复context的UWRC），schema1/reserved0，106字节固定部分加1～255字节旧Bookie，总107～361字节，CRC32C。UWRH head为16+60*n字节，n=1～32，总上限1936；每项slot4/intent UUID16/physical version8/receipt SHA32。独立Python struct/CRC32C向量repair-commit.bin113字节，SHA-256 `b11289617d0600b18713ad16966dc52a77e93488c62c1754855b18f13b530d31`；repair-commit-head.bin76字节，SHA-256 `20ffa767b856426e3bfbba6d1dcd0a781978d92e2dab51e80fe2f7a412ad010f`。这些向量只验证格式，不制造commit或copy事实。

exact close authority由实际current metadata、完整head/receipt proof、intent admission及最后context recheck导出RECOVERY_CLOSE_AUTHORIZED；已提交intent不再授予fresh RECOVERY_GRANT。old结果仍在history中，后续mapping变化不把旧target重新授权。UIC4 close沿用native public SHA、pre-cut DATA排空、write/force/owner apply和本地committed-readable，不激活normal writer。DELETE后的admitted目标与旧/新member清理历史继续留存，不删除intent/receipt或执行GC。

实际mTLS/ZK/files场景先全E确认entry0，再通过既有replacement协调器建立从entry1开始的新fragment；最新E实际FENCE后读survivor entry0，inactive安装/admit新target、grant及actual recovery Add，随后真实membership/close-authority发布和UIC4 close，最后READ_ENTRY。测试没有注入close authority；metadata保持IN_RECOVERY、新target仍normal-inactive，完整恢复/CLOSED没有被伪造。ZK场景覆盖两次连续commit/new store采用同token、实际CAS reply loss、delete抢先、同UUID/version0 intent重建、缺失必需proof和unbounded last fragment拒绝；codec独立向量覆盖字段所有权、CRC/长度/schema/重复slot/上限。

最终Java `unified-wal-local-BEO0Fee6` exit0：9类167项全部通过，common10/server157（socket52、lifecycle38、metadata19、PendingAddOp19、decoder22、Bookie7），1034冻结输入。Spotless/Checkstyle实际session54772完成chunk d7c59d/exit0/空输出，归档`unified-wal-repair-membership-style-tbaqb0r0`的380 Java输入匹配/224与模型共享，Java/style15 checksums核验。最终模型`unified-wal-model-lfhel3f9` exit0：362配置69完整正向PASS/293指定反例、656 inputs/1083 checksums；226共享/1464去重union匹配，五个designInputs保持父提交`9ec110346f3bb9c210b3819db4968b9ea4503064`绑定。新增投影121 generated/94 distinct、queue0/depth8，十个mutation命中指定性质；tool1.7.4/单worker/heap512/direct64MiB/seed1/fp0，无symmetry/state constraint，CHECK_DEADLOCK FALSE。历史回执不重绑。

定向`unified-wal-local-j1KsEcBj`为9项通过（ZK7/socket2），同UUID重建`unified-wal-local-w4PrZv71`的1项测试通过，之后的长行style失败已修正；早期delete方法名和private path引用属于测试编译错误，不作为实现RED。为避免与UWRC context混淆，新receipt改为独立UWRM，最终Java/model已按最终格式重新冻结。

重现使用上一节UIC4的Java/style/model三条命令。最后fragment/frontier与matching CLOSED、完整copy coordinator/AutoRecovery worker、CLOSED后的独立readonly open、distributed delete/rejoin/GC、full A+C+D、总预算、活性、掉电/长期/性能及生产authority仍OPEN，步骤14保持PARTIAL。

## 历史fragment完整copy协调与同操作恢复

2026-10-08，ProfileRecoveryCoordinator.repairHistorical复用现有cold executor、同一attempt permit、verified DATA/CONTROL池与固定RepairIntent。冻结descriptor、credential和metadata custom/password输入，busy请求在深复制前拒绝；只接受有下一fragment边界的完整历史range。流程为fresh context/快照检查、最新E的durable fence、actual inactive INSTALL、admission、grant、copy、同一copied-at snapshot的membership publication与UIC4 close。没有新线程、队列、自动retry/reconnect或per-entry metadata更新。

copy复用原SDK recovery probe、write-set选择、digest验证及W-A+1 missing分类；Probe owner token禁止用其他reader的样本封装。读取handle只做原DigestManager恢复entry序列化，normal Add和metadata close仍拒绝。协调器每次只持有一个source entry/target RPC，按实际durable APPLIED/ALREADY推进计数；UNKNOWN/NONE、absence或读取失败不推进membership。通过迭代callback和原SDK ordered executor读取下一entry，不保留按range长度增长的future链或payload列表。reader和packed entry在对应实际使用终结后释放；server实际IO/owner与client write/wire custody继续由原组件持有，RPC timeout不证明它们已取消。

caller只取得copy future，取消仅设置stop：当前entry RPC继续终结并保留attempt permit，之后停止新entry及尚未开始的publication；已经提交的metadata结果仍解析并close，不回滚目标历史。异常future保留原cause，不把实现异常伪装为成功或无effect。copy失败保留admitted target/grant与同一操作供显式恢复。若fresh head已引用该intent的同birth/version，则解析原copied-at snapshot/close ID并直接close，不再读取/写回payload；新coordinator与显式reconnect也不换target或operation。

回归定位并修复换组后的fence来源选择：原recoveryCurrentTargets只返回最新replacement新增节点，导致未变成员无法计fencing coverage。现在校验activation历史并按BookieId采用最近的activation incarnation，完整保留未变当前成员；repair-only admission不覆盖普通activation身份。readonly recovery handle可接受backend已验证的generation+bounded commit-head-count metadata版本，仍要求同physical metadata birth和原UWRC context。

四个实际mTLS/ZK/files场景覆盖3条cumulative-length entry完整copy、发布后重复/新coordinator及lost-close-reply解析无recopy、持有DATA force时caller取消/busy/排空后沿用同操作，以及DATA force失败时不发布mapping/close/normal。所有场景使用原read/digest及actual DATA；metadata保持IN_RECOVERY，history保留，新target normal-inactive。该入口是历史fragment协调能力，尚未接underreplication worker/lock/scheduling，也不完成最后开放fragment、required frontier或matching CLOSED。独立readonly-open、完整AutoRecovery/delete/rejoin、full A+C+D、总memory、活性、掉电/长期/性能与production authority仍OPEN。

最终Java `unified-wal-local-ZDl8kEhW` exit0：9类171项全部通过，common10/server161（socket56、lifecycle38、metadata19、PendingAddOp19、decoder22、Bookie7），1034冻结输入。Spotless/Checkstyle实际session88830完成chunk c3b000/exit0/空输出，归档`unified-wal-repair-copy-style-83zwqmp9`的380 Java输入匹配/224与模型共享，Java/style15 checksums核验。最终模型`unified-wal-model-0_bda69p` exit0：372配置70完整正向PASS/302指定反例、667 inputs/1113 checksums；226共享/1475去重union匹配，五个designInputs保持父提交`21081423bcf778c450452089a63e86cc17e53a8b`绑定。新增投影67 generated/51 distinct、queue0/depth10，九个mutation命中指定性质；tool1.7.4/单worker/heap512/direct64MiB/seed1/fp0，无symmetry/state constraint，CHECK_DEADLOCK FALSE。历史回执不重绑。

定向实际RED `unified-wal-local-dkQjCsWD`不能完成换组后的fence/copy；修复未变成员身份选择后`unified-wal-local-6TQ1I7UZ`的2项通过，四场景`unified-wal-local-ycHlB6RT`全部通过。model首轮zn7yy2kd是TLA运算符括号解析错误，无性质通过声明；修正后subset `unified-wal-model-0vf4skai`为1正向/9指定反例，再运行上述完整矩阵。

重现沿用UIC4小节的Java/style/model三条命令；模型消费source/fence/grant及RPC实际终结事实，不生产crypto/TLS/force或完整跨组件预算证据。

## URL2 explicit LAC 双证据与前缀锚点

2026-10-08，继承上轮未提交改动并完成验证。URL1/schema1 字节、拒绝和 EMPTY 合同保持原样；已有独立 LAC 授权的隔离 listener 另外广告 opt-in URL2（`0x55524c32`/schema2）。DATA client 同时 offer URL1/URL2，按实际 server capability 选择 schema2 或旧 schema1；URL2 自身可协商 READ_LAC，不要求另外 offer URL1。capability 不授予读权限，仍执行原 mTLS principal/context 授权及回复前 visibility 检查，没有新 opcode、请求 body 或普通 Add 热路径开销。

URL2 成功 body 的固定 prefix 为100字节：沿用 status8/UWLQ magic/schema/tag/context60/sourceEntryId8/entryLength4，再加 explicitLac8/explicitLength4；尾部先完整原 BK entry，再独立20字节 CRC32C ledger/LAC digest。tag1/2/3/4分别为 SOURCE/EMPTY/EXPLICIT/SOURCE+EXPLICIT。各组件缺失有唯一形状，explicit 缺失为(-1,0)，存在时长度必须20，允许实际 LAC=-1。严格拒绝错 scope/ledger/value/tag、截断、额外尾部、unsigned overflow、超安装上限及错 schema；非OK仍仅 status8。源码只启用既有 CRC32C Profile 子集，不新增其他 digest 模式。

实际 owner read 继续使用 fresh Get/current selector/file pin/池 loan，终结时连同本地 durable control 中的 explicit LAC 取得同一 admission 下的回复。无 DATA 时，只有完整稳定 fenced absence proof 才能返回 EXPLICIT 或 EMPTY；有 explicit 的 EXPLICIT 不等于整 ledger EMPTY。旧 URL1 不忽略 explicit 后伪报 EMPTY。超时/取消后的 frame、loan、response credit 和实际 I/O 终结合同沿用既有路径；回复增加的固定字节纳入原 response/workspace 额度。

SDK 分别使用原 DigestManager 校验 entry 与 explicit digest，整份双证据合法后才计原 coverage 并保留来源 identity/incarnation。任一组件损坏排除该 endpoint 的两份贡献，继续其他副本；剩余覆盖不足才失败，不因 valid-looking source 而忽略同 endpoint 的坏 explicit。最大有效 piggyback/explicit 提示与最后 fragment 起点共同决定待锚定 P；explicit 本身没有 length，仍读/复用 exact P 的原 entry 累计应用 length，缺锚点或长度矛盾 non-OK，不更新 writer LAC/length或发布 CLOSED。此 API 是已取回来源的证据，尚不宣称完整 required-frontier authority。

三个独立固定向量已经通过 Python struct 逐字段校验，Java writer/parser 逐字节对照与全截断边界拒绝验证：

| 向量 | 字节 | SHA-256 |
| --- | --- | --- |
| explicit-source.bin | 160 | `d67e2054466c1be5f71eadb0f3d9f839affbd8e1b81849c51753aeb5b743f223` |
| explicit-only.bin | 120 | `f3ebb34ecd304e1ae374260a0ddb1efe69127aa8df7dbee6f5bcc4141ef1e90a` |
| explicit-empty.bin | 100 | `f18e7731cb7c95070977caf07a7fc839276a1df237600ea88aeb0fd7497929b8` |

真实 mTLS/ZK/files/原 SDK 场景验证 source100/piggyback98/length1100 + explicit99 → P99/length1000；仅 explicit99 而缺 entry99 时不给成功前缀；一个 explicit digest 损坏且余下两副本有效可取 P100/length1100，两坏副本则覆盖不足。writer LAC 始终未被取证修改。既有读/恢复/换组/UNKNOWN/资源寿命回归保留。

最终 Java `unified-wal-local-4ZBc2KWT` exit0：9类176项全部通过，common12/server164，零失败/错误/跳过；1037冻结输入 SHA-256 与最终源码一致。common/server Spotless/Checkstyle `unified-wal-explicit-lac-style-2crF7Oih` exit0。相关模型 `unified-wal-model-xr861fp7` exit0：34配置为6个完整正向 PASS/28个指定反例，669源码输入与503结果 checksums 全部核对；其中 A2/A3 explicit frontier 投影各25756/22800及28996/25392 generated/distinct，queue0/depth6，source投影4096/4096，queue0/depth2。工具1.7.4、单worker、heap512/direct64MiB、seed1/fp0，无symmetry/state constraint。只运行变化及关联读取/readonly recovery 切片，未重跑整个374配置目录；此前完整矩阵及所有 historical source/design/run 保持原绑定。runner 的可选 config-prefix 只选择已声明配置，未知 prefix 拒绝，默认仍执行完整矩阵。

```bash
bash scripts/unified-wal/run-local-storage-tests.sh 'ProfileInitialControlCodecTest,ProfileReadEntryReplyCodecTest,ProfileLacReplyCodecTest,SegmentProfileInitialSocketClientTest,ZKProfileLifecycleStoreTest,ProfileLedgerMetadataStoreTest,PendingAddOpTest,ProfileReceiveDecoderTest,SegmentBookieControlLogTest#restartedPointRead*+livePointRead*+explicitLacIs*+recoveryCloseWaitsForPrecutTicket*+uncertainRecoveryCloseForce*+profileSocketRecoveryGrantClose*+recoveredRecoveryCandidateLosesEligibility*'
mvn -o -P '!cargo-zigbuild' -pl bookkeeper-server -am -DskipTests spotless:check checkstyle:check -q
python3 scripts/unified-wal/run-storage-models.py --tool /path/to/tla2tools.jar --config-prefix a-profile-lac- --config-prefix ce-profile-lac- --config-prefix a-profile-point-read --config-prefix ce-profile-point-read --config-prefix a-profile-recovery-observation --config-prefix ce-profile-recovery-observation
```

额外 RAT 检查：common 模块0个未知许可证；server 模块失败于16个既有 unified-wal-segment `.hex`向量。根目录首次失败445项，补齐本次两个新cfg的完整ASF许可证头后仍失败443项，全部对应HEAD已有文件；未改RAT设置或扩大本次修复范围。最终模型回执在许可证头变更后独立重跑同一34配置并重新冻结；早期 aur5sicf 回执不改绑定。根目录/服务器RAT未通过，不能据此声称完整仓库检查通过；相关可复现命令为 `mvn -o -P '!cargo-zigbuild' -pl bookkeeper-common,bookkeeper-server -DskipTests apache-rat:check -q` 与 `mvn -o -N -P '!cargo-zigbuild' -DskipTests apache-rat:check -q`。

本增量不新增网络 WRITE_LAC；该写路径仍明确 unsupported，测试通过原 reserved durable control driver 写入 explicit 事实。完整最大提示/required authority、最后开放 fragment 的 normal-tail oracle、恢复 write-back/matching CLOSED/lost-close、AutoRecovery、独立 readonly-open、delete/rejoin、完整 A+C+D、总预算、掉电/长期/性能及生产权限仍 OPEN。步骤14仍 PARTIAL，不把模型或进程可读当成 fsync/掉电、TLS 总内存或可保留数据生产验收。


## Required prefix 与 normal-tail 只读取证

2026-10-08，新增原 `ProfileRecoveryReadHandle.probeEvidence` 和 `ProfileRecoveryCoordinator.verifyPrefix`。普通 `probe` 保留原 Classic recovery read 行为；required-hole 检查则不能把达到 `W-A+1` 的聚合 missing 当成全部合法来源耗尽。新入口复用原 distribution write-set、AckSet、fencing DATA transport 和 DigestManager，对 exact write set 的每个来源等本次 callback 终结，分别保存已验证 Bookie/incarnation 及 FOUND/MISSING/CORRUPT/UNAVAILABLE/UNAUTHORIZED。超时、失败和不匹配来源不成为 missing；只保留一份已校验应用 payload，逐回复比较 payload 与累计 length，允许原封装 LAC/digest 的合法差异。

verifyPrefix 要求实际 URL2 证据能力、同一已接受 IN_RECOVERY token 和有限正数坐标/时间预算，先沿原算法取得当前 write-set fencing coverage，再 cold-read 固定 metadata/来源历史与 LAC/explicit/边界 length。当前 required producer 为覆盖验证的提示与冻结最后 fragment 起点；新入口从 entry0 检查连续性，按 exact source set 推进至 required frontier 和后续 tail。每条的累计 length 取原 entry，并检查相邻应用 bytes；没有从零累加一个替代 length、逐entry metadata、持久进度或新线程。一个 payload/RPC source set 在途，future graph 不随 ledger 长度增长；坐标预算可到正 int 上限，空 ledger 的最大预算测试不会分配对应长度的数组。

| 当前坐标事实 | 结果 |
| --- | --- |
| 合法来源存在 payload，且无有效内容/长度矛盾 | 继续连续前缀取证；单份候选仍不证明 AQ |
| required 坐标无候选，全部来源 definitive missing 或 digest-invalid | DATA_LOSS / 有限来源 evidence exhausted，保存 unresolved coordinate |
| required 坐标仍有 unknown/unavailable | DEFERRED，即使已有 `W-A+1` 个 missing |
| frontier 之后无候选，exact write-set 有 definitive absence coverage | VERIFIED_NORMAL_TAIL，P=x-1；空 ledger 为(-1,0) |
| 无 required authority 的早期 hole，后面存在 speculative payload | 在 hole 处终止并排除该后缀，不制造 DATA_LOSS |
| 两份有效 payload 或 length 矛盾 | QUARANTINED；普通损坏 endpoint 排除后继续其他来源 |

返回的 bounded rich result 包含 context、实际 metadata snapshot、required frontier、P/L、first unresolved coordinate、来源分类和 reason。读后再次核对实际 metadata birth/version/完整内容与原 lifecycle/context；变化、暂不可用和 required authority 不可恢复分开报告。已有同 BookieId 的不同 retained incarnation 在 rejoin 合同接通前明确拒绝，不能使用最新 incarnation 的 missing 否定历史副本。LAC 边界的有效 length 矛盾使用附加 typed exception 区分，legacy 仍投影为既有 DigestMatch code，无新 wire error code。

caller cancel/deadline 只停止未来扫描或等待。deadline 使用借用 SDK scheduler；在途 cold/socket/source work 保留单次 attempt permit，直到实际本地 child 终结后归还；迟到结果不复活 caller。unexpected implementation exception 保留原 cause。历史 fragment copy 现也使用完整来源取证，保留 CONFLICT/QUARANTINED/DATA_LOSS，而不在 publication 前全部压成 DEFERRED；失败或取消仍不发布 membership/close/normal authority。

四个新真实 mTLS/ZK/files/SDK 测试方法内部覆盖 A2/A3 三entry连续前缀、count budget、required hole 与两missing+unknown、最后来源上的 required payload、更晚未提交 suffix、有效 payload/相邻长度/锚点长度冲突、单损坏源继续、空 ledger、cancel/busy/drain、整attempt deadline、DELETE cut 和无 URL2 的零新增 control force 拒绝。metadata 保持 IN_RECOVERY。新 independent point oracle 按 required/finite-source 条件计算结果，不借用生产 status classifier。

广度回归 `unified-wal-local-V5tIMQrS` exit0：9类180项全部通过，common12/server168（socket63），零失败/错误/跳过。随后去掉无合同依据的一百万坐标硬上限，仅保留 caller 的正 int/Duration budget，并用 `Integer.MAX_VALUE` 空 ledger 验证固定工作集；最终定向 `unified-wal-local-BFOEqVzV` 的4方法通过，1037个源码 hash 与最终工作区一致。未把广度回执重绑定为修改后的源码，也未因两行参数/test-budget变动无目的重复全量测试。最终 Spotless/Checkstyle `unified-wal-prefix-final-checks-B88zVIs6` style exit0；同目录 root RAT仍 exit1/443个HEAD已有未批准路径，本次新 model/cfg 均有完整ASF许可证头，无新许可证失败，RAT设置未改。

最终模型 `unified-wal-model-p48elju2` exit0：相关57配置9完整正向 PASS/48指定反例，683源码hash/560结果checksums均核对；sourceCommit及五个designInputs保持父提交 `f4d7c893fa52472d5710717d5a02ea3faddf1220`。新 `ProfileRecoveryPrefix` A2/A3 各340000 generated/144000 distinct、queue0/depth7，11个 mutation分别覆盖提前完成、unknown计absence/exhaustion、忽略required/conflict/prior/length、取消提前放permit、context/source gate及错误CLOSED。checker1.7.4、单worker、heap512/direct64MiB、seed1/fp0，无symmetry/state constraint；一次检查约1.89s/RSS312049664 bytes只是检查器成本。只执行变化及关联LAC/point/readonly/copy切片，未宣称整个387配置目录本轮通过；历史回执保留原绑定。

```bash
bash scripts/unified-wal/run-local-storage-tests.sh 'ProfileInitialControlCodecTest,ProfileReadEntryReplyCodecTest,ProfileLacReplyCodecTest,SegmentProfileInitialSocketClientTest,ZKProfileLifecycleStoreTest,ProfileLedgerMetadataStoreTest,PendingAddOpTest,ProfileReceiveDecoderTest,SegmentBookieControlLogTest#restartedPointRead*+livePointRead*+explicitLacIs*+recoveryCloseWaitsForPrecutTicket*+uncertainRecoveryCloseForce*+profileSocketRecoveryGrantClose*+recoveredRecoveryCandidateLosesEligibility*'
bash scripts/unified-wal/run-local-storage-tests.sh 'SegmentProfileInitialSocketClientTest#prefixVerifier*'
mvn -o -P '!cargo-zigbuild' -pl bookkeeper-server -am -DskipTests spotless:check checkstyle:check -q
python3 scripts/unified-wal/run-storage-models.py --tool /path/to/tla2tools.jar --config-prefix a-profile-recovery-prefix --config-prefix ce-profile-recovery-prefix --config-prefix a-profile-lac- --config-prefix ce-profile-lac- --config-prefix a-profile-point-read --config-prefix ce-profile-point-read --config-prefix a-profile-recovery-observation --config-prefix ce-profile-recovery-observation --config-prefix a-profile-repair-copy --config-prefix ce-profile-repair-copy
```

VERIFIED_NORMAL_TAIL 只证明这次读取判据。后续恢复 Add 的 quorum/write-back、最后开放 fragment 的终态权限/visibility cut、同上下文 matching durable CLOSED 与响应丢失/restart解析仍须接通；读到候选或 P/L 不能替代这些事实。完整额外 ACK/AQ/closed authority producer、长期重验成本/大ledger策略、网络 WRITE_LAC、AutoRecovery/marker lifecycle、独立readonly-open、delete/rejoin、完整 A+C+D、总预算、掉电/可持续性能及 production Gate 继续 OPEN。步骤14仍 PARTIAL。


## 固定最后 prefix 与真实 last-fragment 写回

2026-10-08，原只读取证之后增加单份 immutable prefix selection；标准 metadata 的出生绑定引用固定该节点，最后开放 fragment 的 RepairIntent 才能取得有限终点。`selectPrefix`是cold publication primitive，消费调用方已完成的 VERIFIED_NORMAL_TAIL predicate；真实socket测试使用原verifyPrefix生产该输入，ZK后端测试的predicate是显式输入，不冒充读/crypto/fsync证明。选择不授予AQ、CLOSED、normal或strong-reset权限。

新实验UWRF/schema1固定256字节：header8、原UWRC context184、beforeVersion4、对应canonical metadata SHA32、required frontier8、P8、应用length8、CRC32C4。UWRS/schema1引用固定56字节：header8、publicationBeforeVersion4、实际node birthZxid8、完整UWRF SHA32、CRC32C4。decode严格拒绝错shape/magic/schema/reserved/CRC、负边界、P<required、empty长度非0及溢出；数组owned-copy。原UWRC184、UWRP、UWRM/UWRH和UIC1～4字节不变。

首次选择先创建fixed node，再建立不可写birth guard，最后以同一metadata physical-birth/version与lifecycle/delete cut CAS发布标准引用。节点data使用原page ACL保持不可写并允许birth child；guard不可写。publication检查准确guard路径，precheck后删除重建version0不能通过旧cut；已引用节点丢失/重建/digest不匹配时隔离，不重建authority。不同P/length不覆盖旧选择。回复丢失读取实际引用解析同一选择；inert node尚未引用时仍不授权last-fragment，history并发先赢可在新read predicate下验证原immutable选择的完整origin后发布，保留原P/length。

标准metadata多一次selection版本递增。既有repair head仍独立累计，每份UWRM保留实际beforeVersion；恢复按reference的publication cut分别验证其前后的版本，并逆演到选择原metadata origin和原UWRC predecessor。SELF write-back只在已selected的last fragment允许：标准membership保持原成员，仍原子追加bounded copy receipt及其close authority；普通历史SELF不放开。没有新的entry级metadata、线程、Maven模块或控制框架。

新repairLastFragment复用原inactive INSTALL/receipt/admission、UIC3 grant、原SDK完整来源/digest、单entry实际ADD_RECOVERY、同snapshot publication和UIC4 close。current成员复用matching install semantic identity；fenced后recovery Add可以幂等确认原durable DATA，LAC封装变化不会改应用数据或新增不必要DATA force。新target真实复制完整selected range。cancel保留actual child/attempt permit，停止未来copy；已published intent在显式reconnect/新coordinator后按原close ID解析，不recopy。REPAIRED_LAST_FRAGMENT仍不是RECOVERED_AND_CLOSED，metadata保持IN_RECOVERY。

实际回归覆盖三entry/length48的new-peer copy、同起点标准mapping publication、lost-close回复/new coordinator零recopy、DATA force持有时caller取消/busy/同操作继续，以及3/3/3全部current成员SELF recovery Add/close、membership不变和零额外DATA force。真实ZK另覆盖selection回复丢失、不同P拒绝、相同bytes/version0重建、physical guard阻止pre-CAS重建、history穿插selection和随后last-fragment commit的版本/逆演、DELETE/外部metadata变更及未selected最后fragment仍拒绝。

两个独立Python struct/CRC32C向量：recovery-prefix.bin256字节，SHA256 `72b6248f70c0e2d77e99d5acbfd7393d605c3be015711175060605ce9ac34653`；recovery-prefix-reference.bin56字节，SHA256 `e0ca486ff55a30630532644c5989660b010590b7bcfc489f04ab6e5570b547cf`。Java逐字节对照、所有截断、alias/非法边界与ownership验证通过，向量不制造selection事实。

广度`unified-wal-local-xQFIniuq` exit0：9类189项全通过（common12/server177：socket66、lifecycle44、metadata19、PendingAddOp19、decoder22、Bookie7）。随后只修两处测试长行格式，最终`unified-wal-local-pvUAsJ6V`的10项相关测试通过；1040当前source hashes核对，旧广度回执不重绑。最终Spotless/Checkstyle `unified-wal-selection-final-style-admRVXDG` exit0。root RAT `unified-wal-last-fragment-checks-8KEzrSnF`仍exit1/443个HEAD已有未批准文件，无本次新增失败；不改规则绕过。最初pRf6vSSM的真实ACL反例已保留：用完全read-only节点创建birth child会被拒绝，改复用既有page ACL后通过；不通过弱化权限或断言解决。早期import遗漏/fixture把未admit peer放进source pool属于接线错误，不算协议RED。

最终模型`unified-wal-model-n9mm5ud3` exit0：79配置11完整正向PASS/68指定反例，696源码hash/614 checksums核对。selection投影70 generated/50 distinct，queue0；membership扩展857/662，queue0。新9个selection与1个last-fragment authority反例均命中指定性质。tool1.7.4、单worker、heap512/direct64MiB、seed1/fp0，无symmetry/state constraint；只执行变化与关联切片，未宣称全398配置本轮通过。首轮sezfgn31把历史read adoption错误当成持续有效lease而失败，修正当前adoption在marker restore后失效，再实际重跑；不是实现PASS。所有historical source/design/run保持父绑定，新回执sourceCommit为`1c7379ba08398cee9fdf23a5db31a685991c2066`。

```bash
bash scripts/unified-wal/run-local-storage-tests.sh 'ProfileInitialControlCodecTest,ProfileReadEntryReplyCodecTest,ProfileLacReplyCodecTest,SegmentProfileInitialSocketClientTest,ZKProfileLifecycleStoreTest,ProfileLedgerMetadataStoreTest,PendingAddOpTest,ProfileReceiveDecoderTest,SegmentBookieControlLogTest#restartedPointRead*+livePointRead*+explicitLacIs*+recoveryCloseWaitsForPrecutTicket*+uncertainRecoveryCloseForce*+profileSocketRecoveryGrantClose*+recoveredRecoveryCandidateLosesEligibility*'
bash scripts/unified-wal/run-local-storage-tests.sh 'ZKProfileLifecycleStoreTest#prefixSelection*+selectedPrefix*+repairOfUnbounded*,SegmentProfileInitialSocketClientTest#selectedLastFragment*'
mvn -o -P '!cargo-zigbuild' -pl bookkeeper-server -am -DskipTests spotless:check checkstyle:check -q
python3 scripts/unified-wal/run-storage-models.py --tool /path/to/tla2tools.jar --config-prefix a-profile-recovery-selection --config-prefix ce-profile-recovery-selection --config-prefix a-profile-repair-membership --config-prefix ce-profile-repair-membership --config-prefix a-profile-recovery-prefix --config-prefix ce-profile-recovery-prefix --config-prefix a-profile-lac- --config-prefix ce-profile-lac- --config-prefix a-profile-point-read --config-prefix ce-profile-point-read --config-prefix a-profile-recovery-observation --config-prefix ce-profile-recovery-observation --config-prefix a-profile-repair-copy --config-prefix ce-profile-repair-copy
```

当前显式入口完成每个target的range写回/close，没有整体quorum orchestrator、标准ledger CLOSED(P,length,context) publication/resolution或正常DATA的terminal visibility cut。extra required authority producer、empty-last-fragment的整体close流程、AutoRecovery/marker lifecycle、readonly-open、delete/rejoin、全A+C+D、总预算、掉电/长期性能与production Gate仍OPEN；步骤14仍PARTIAL。


## 非空恢复 terminal cut 与 matching CLOSED

2026-10-08，`finishRecovery(context, peers)`在逐target恢复写回之后聚合当前最后fragment的完整copy结果。先读取matching标准CLOSED；仍IN_RECOVERY时构造cold closingPlan，经实际UIC4 close取得不同原ensemble slot的durable结果，再用原RoundRobinDistributionSchedule ACK集合核验A。当前只支持E=W=3、A=2或3和非空selected最后range；不足、重复、UNKNOWN、错Bookie/incarnation/operation/context及缺语义能力的结果不授权close。当前结果`CLOSED_METADATA_DURABLE`只表示matching CLOSED元数据事实，不宣称整个AutoRecovery或生产readiness完成。

UIC4请求/响应字节不变。新增HELLO实验语义能力UIC5（id `0x55494335`、version1）表示接收端实现terminal cut；真实CONTROL客户端提出该能力，服务端在控制接线存在时广告。旧实验UIC4-only节点即使返回普通grant close的APPLIED，也不能计入terminal quorum。native Bookie控制新增type11 `RECOVERY_TERMINAL_CLOSE`，body沿用type9的exact scope/grant/intent绑定；cold authority仅对已selected的最后fragment、已发布完整copy receipt授予terminal状态，historical fragment仍用type9。

terminal prepare停新normal/recovery准入并等待所有pre-cut normal和recovery DATA实际终结，再走原控制I/O force与owner apply；不增加普通Add的远端metadata/日志/handshake。pending和durable P限制旧点读结果完成及READ_LAC最新来源选择；durable P还限制restart/coverage inventory。已排除的suffix参与compaction时跳过，必要prefix继续搬迁和读取；whole-allocation FREE仅消费匹配context的durable P排除证据并保持原reclamation/cut复核，pending或未知context不授权FREE；shared预创建DATA文件不截短。同close operation的已durable type9可升级type11，checkpoint必须保留成功升级的最新记录，rotation/replay恢复同一个cut；后到的旧recovery grant不能改变terminal读资格。

标准metadata CAS保留原CLOSED以外的context、CToken、membership及custom/history，把P与同一选择的应用length一起发布。multi同时检查metadata physical birth/version、原lifecycle/delete cut及selection节点/birth guard；matching resolver重验UWRC起点、selection origin和完整retained repair head，按单次close版本偏移逆演。CLOSED不进入readRecovery、不重新OPEN；提交回复丢失或new coordinator优先解析实际matching结果，无recopy或额外control force。紧随后续DELETE_INTENT的matching历史close可以被解析；它保留历史完成事实，不授予新的访问权限。

独立Python struct/CRC32C的152字节`bookie-terminal-framing.bin`，SHA256 `2df309b9733b02bce56266b7809356dacd11ac17fa8e7be29512619d6cdeee0b`，仅检验native type11 framing。fixture body是4字节示例，独立parser与writer逐字节相符不构成合法grant、copy或terminal authority。

真实回归包括3/3/2与3/3/3完整SELF写回、未被required的洞后physical suffix、重复slot/旧能力/不足A拒绝、实际ZK multi提交但CONNECTIONLOSS的matching解析、新coordinator零force、CLOSED拒绝重新recovery、节点restart与预创建容量保持，以及紧随delete的历史完成解析。native测试另覆盖旧type9同ID升级、pre-cut normal请求等待、旧读票据终结重验、双checkpoint/rotation/replay及必要prefix/suffix混合compaction。checkpoint丢失升级的真实RED `unified-wal-local-ktFmWhwK`已保留；修复成功type11替换checkpointState中的旧type9，而其他first-result规则不变。

当前仅已处理type11的接收节点具有持久P上界；离线节点尚需catch-up/rejoin。空ledger(-1,0)、有历史数据但最后fragment为空的整体close、额外accepted required authority producer、独立readonly-open、完整RepairIntent/underreplication worker/AutoRecovery、全A+C+D、总预算、掉电/长期性能与生产Gate仍OPEN；步骤14保持PARTIAL。历史source/design/receipt不重绑。


关联模型`unified-wal-model-5z2ychc3` exit0：104配置15完整正向PASS/89指定反例，718当前源码hash及682 artifact checksums全部核对。completion的A2/A3各55,856 generated/32,040 distinct、queue0；terminal-cut为18/11、queue0；WholeAllocationFree为217/68、queue0。固定tool1.7.4（恢复官方jar并校验既有SHA1/SHA256）、单worker、heap512/direct64MiB、seed1/fp0，无symmetry/state constraint；catalog418未全量重跑。早期缺工具/terminal模型语法错误不算PASS；历史97配置run在后续grant/FREE修复前完成，最终104配置独立冻结，sourceCommit仍为本批父提交`36fb485823979fb8b939d96857fa96a25a055bf3`，不重绑旧run。

```bash
bash scripts/unified-wal/run-local-storage-tests.sh 'ProfileInitialControlCodecTest,ProfileReadEntryReplyCodecTest,ProfileLacReplyCodecTest,SegmentProfileInitialSocketClientTest,ZKProfileLifecycleStoreTest,ProfileLedgerMetadataStoreTest,ZKProfileControlStoreTest,PendingAddOpTest,ProfileReceiveDecoderTest,SegmentBookieControlLogTest#restartedPointRead*+livePointRead*+explicitLacIs*+recoveryCloseWaitsForPrecutTicket*+uncertainRecoveryCloseForce*+profileSocketRecoveryGrantClose*+recoveredRecoveryCandidateLosesEligibility*+terminalClose*+terminalControlFraming*+graphCompactionSkipsTerminalSuffixAndPreservesRequiredPrefix+graphCompactionTerminalGroupNeedsNoCopyAllocation+graphCompactionRequiresGroupNetGainPreservesSharedLiveDataAndReusesFreedSource'
mvn -o -P '!cargo-zigbuild' -pl bookkeeper-server -am -DskipTests spotless:check checkstyle:check -q
python3 scripts/unified-wal/run-storage-models.py --tool /path/to/tla2tools.jar --config-prefix a-profile-recovery-completion --config-prefix ce-profile-recovery-completion --config-prefix c-profile-terminal-cut --config-prefix ce-profile-terminal-cut --config-prefix a-profile-recovery-selection --config-prefix ce-profile-recovery-selection --config-prefix a-profile-repair-membership --config-prefix ce-profile-repair-membership --config-prefix a-profile-recovery-prefix --config-prefix ce-profile-recovery-prefix --config-prefix a-profile-lac- --config-prefix ce-profile-lac- --config-prefix a-profile-point-read --config-prefix ce-profile-point-read --config-prefix a-profile-recovery-observation --config-prefix ce-profile-recovery-observation --config-prefix a-profile-repair-copy --config-prefix ce-profile-repair-copy --config-prefix c-whole-free --config-prefix ce-whole-free
```


最终Java `unified-wal-local-HgWxojIU` exit0：10类206项全部通过（common12/server194：socket66、lifecycle44、metadata19、control store12、PendingAddOp19、decoder22、Bookie12），1041当前source hashes核对。修复前`unified-wal-local-PdEapG9C`确实把晚到grant判APPLIED、`unified-wal-local-bGqdfrdF`的FREE确实拒绝已排除suffix；后者其余205项通过但该run整体失败。两项修复后的`unified-wal-local-vxCGt2ms`6项定向与最终206项通过，RED保留。早期测试API/import/路径遗漏是接线错误；`heIdWeQA`与已停止的`YZ5i3xlR`曾并发共享Maven target，类缺失使该轮无效，不算实现RED或PASS，最终验证已串行完成。

最终Spotless/Checkstyle `unified-wal-terminal-checks-zEv642N7` exit0；root RAT exit1仍为443个既有未批准文件，无新增失败，不改license规则绕过。文档相对链接、manifest JSON、历史字段保留及git diff检查通过；本批不push、不接生产路径。


## 空最后 fragment 的独立终态关闭

2026-10-08，`finishRecovery`继续覆盖空ledger `(-1,0)`以及历史prefix已验证、最后fragment为空的关闭。UIC4仍需要真实非空exact grant；空最后range改用同一实验CONTROL协议内的`RECOVERY_TERMINAL_CLOSE`（opcode `0x0108`、UIC6 capability `0x55494336`/schema5）。它复用现有冷读authorizer、control quota、write/force/owner apply和原distinct ACK集合，不创建DATA grant、RepairIntent、新的控制框架或线程。当前E=W=3、A2/A3；非空selected最后range继续走原UIC4/type11。

新请求沿用COMMON126和1～128字节domain，再附48字节TerminalBinding：P8、应用length8、完整selection digest32，body175～302字节，reply仍60字节/schema5。P必须>=-1且非Long.MAX_VALUE、length>=0、(-1,length)只能为(-1,0)。typed request先验证domain-separated SHA256绑定scope实例/descriptor、target incarnation、authority generation、P/L及selection digest；decode重新计算绑定，改变P但保留旧public identity会被拒绝。UIC1～4旧布局与固定向量不变；generic operation codec对新control body仍BLOCKED，只有明确实验opt-in和UIC6 agreement才执行。旧UIC5语义能力不替代UIC6。

cold closingPlan先重验UWRC、standard metadata、完整历史与已发布selection；空最后range仅允许P=lastFragmentStart-1，不能借空关闭跳过冻结的历史prefix。target从已有READY/replacement activation receipts按当前member slot取出；确定性close ID绑定原context、selection digest、slot、Bookie/incarnation，payload identity固定P/L。没有新增repair head或membership。cold authority复用repair/close reader，关闭CAS再重验同一现有target列表、physical birth/version、lifecycle与selection guard；matching CLOSED仍保留全部历史，不重新OPEN。

native Bookie新增实验type12 `RECOVERY_TERMINAL_BOUNDARY`，scope discriminator7及固定48字节binding，没有grant字段。prepare先切新normal/recovery准入并等待全部pre-cut两类I/O；原close driver共享一次control barrier，owner apply保存同一P。generic control batch显式拒绝该命令，不能绕过pre-cut路径。pending cut不是FREE authority；force失败本进程不发布durable结果、generation或排除证据。成功cut沿用旧读结果重验、READ_LAC/restart inventory上界及suffix回收规则；checkpoint/rotation/replay保留type12。ProfileAuthorityCodec同时修复state8的旧上界7拒绝问题，并为新purpose分配8；真实persistent-node全purpose回归包含它，原id不重排。

独立Python struct/SHA256 request向量`terminal-boundary-close.bin`188字节，SHA256 `362df960afa8191364c7277eb5e5cc83008482990cfe39f860e8463702414e27`；独立struct/CRC32C native向量`bookie-terminal-boundary.bin`344字节，SHA256 `1b8df2541ef089074570d49dde54d581004087510a1c813e02be864a55cc1d97`。前者覆盖所有截断、alias/shape、schema隔离、P/length溢出与body identity；后者对照独立framing parser与writer。向量只生产字节，不授予metadata、DATA或terminal authority。

真实TLS/socket/ZK/files验证四场景：A2/A3的(-1,0)，以及真实replacement到fragment3后历史CLOSED(2,48)。原SDK point predicate选择边界；洞后的physical suffix不成为required prefix。重复slot、仅旧UIC5、不足A均不发布CLOSED；actual ZK multi已提交但CONNECTIONLOSS后解析matching结果，新coordinator零额外control force。所有DATA force计数保持，没有fake grant/copy或新增repair head；尾部节点restart继续拒绝suffix且预创建文件容量保持。另有held terminal reply时caller cancel/busy/同ID续接，native两类pre-cut排空、generic batch绕过拒绝、force失败以及双checkpoint/rotation/replay验证。单进程故障/重新打开不等同硬件掉电证据。

当前结果仍仅为matching `CLOSED_METADATA_DURABLE`与已接收节点的本地cut；额外accepted required-frontier authority producer、独立readonly-open、AutoRecovery/underreplication和完整RepairIntent lifecycle、离线catch-up/rejoin、删除/GC、总预算、全A+C+D、掉电/长期/性能与production Gate仍OPEN，步骤14保持PARTIAL。历史模块manifest/source/design/receipt不重绑。


最终广度`unified-wal-local-ApNkUH8O` exit0：13类236项全部通过，common37（initial codec7、operation4、frame7、reference control13、read2、LAC4）/server199（socket68、lifecycle44、metadata19、control store12、PendingAddOp19、decoder22、Bookie15）。最后仅一处测试长行换行，最终`unified-wal-local-LYo9qDB1`13项通过、1043当前source hashes核对；旧广度source hash不重绑。早期编译枚举/方法抽取、缺import、测试未连接第四peer及native向量制作时的字段偏移错误均为接线/fixture错误，不计协议RED。force失败测试的本进程拒绝与重新打开控制日志解析分别报告，不声称硬件掉电PASS。

最终模型`unified-wal-model-1r60su1w` exit0：112配置19完整正向PASS/93指定反例，726当前source hashes、702 artifact checksums核对。completion空ledger A2/A3各55,856 generated/32,040 distinct，历史empty-last A2/A3各111,712/64,080，正向queue0；新增wrong empty length/prefix、虚构grant、跳过历史floor四反例。terminal-cut18/11、WholeAllocationFree217/68，queue0，继续消费实际I/O/cold authority为输入，不生产crypto/TLS/fsync。初轮`_1613j8q`的虚构grant在Init失败导致无探索统计，INCONCLUSIVE未算PASS；改为发生在有效Init之后的错误admission action，再用原性质取得指定反例。固定tool1.7.4/worker1/heap512/direct64MiB/seed1/fp0，无symmetry/state constraint；全426 catalog及完整A+C+D未重跑。新run父绑定380a02bbc0f9ae14f3dcbe14003dca167f4069be，历史回执保持原输入。

```bash
bash scripts/unified-wal/run-local-storage-tests.sh 'ProfileInitialControlCodecTest,ProfileOperationCodecTest,ProfileFrameCodecTest,IsolatedProfileControlEndpointTest,ProfileReadEntryReplyCodecTest,ProfileLacReplyCodecTest,SegmentProfileInitialSocketClientTest,ZKProfileLifecycleStoreTest,ProfileLedgerMetadataStoreTest,ZKProfileControlStoreTest,PendingAddOpTest,ProfileReceiveDecoderTest,SegmentBookieControlLogTest#restartedPointRead*+livePointRead*+explicitLacIs*+recoveryCloseWaitsForPrecutTicket*+uncertainRecoveryCloseForce*+profileSocketRecoveryGrantClose*+recoveredRecoveryCandidateLosesEligibility*+terminalClose*+terminalControlFraming*+standaloneTerminalBoundary*+graphCompactionSkipsTerminalSuffixAndPreservesRequiredPrefix+graphCompactionTerminalGroupNeedsNoCopyAllocation+graphCompactionRequiresGroupNetGainPreservesSharedLiveDataAndReusesFreedSource'
bash scripts/unified-wal/run-local-storage-tests.sh 'ProfileInitialControlCodecTest,SegmentBookieControlLogTest#standaloneTerminalBoundary*,SegmentProfileInitialSocketClientTest#emptyLastFragmentClosesMatchingBoundaryWithoutGrantOrCopy+cancelledEmptyBoundaryCloseKeepsActualAttemptUntilTerminalReply+selectedLastFragmentWritesBackToTheExistingMembersWithoutChangingMembership'
mvn -o -P '!cargo-zigbuild' -pl bookkeeper-server -am -DskipTests spotless:check checkstyle:check -q
python3 scripts/unified-wal/run-storage-models.py --tool /path/to/tla2tools.jar --config-prefix a-profile-recovery-completion --config-prefix ce-profile-recovery-completion --config-prefix c-profile-terminal-cut --config-prefix ce-profile-terminal-cut --config-prefix a-profile-recovery-selection --config-prefix ce-profile-recovery-selection --config-prefix a-profile-repair-membership --config-prefix ce-profile-repair-membership --config-prefix a-profile-recovery-prefix --config-prefix ce-profile-recovery-prefix --config-prefix a-profile-lac- --config-prefix ce-profile-lac- --config-prefix a-profile-point-read --config-prefix ce-profile-point-read --config-prefix a-profile-recovery-observation --config-prefix ce-profile-recovery-observation --config-prefix a-profile-repair-copy --config-prefix ce-profile-repair-copy --config-prefix c-whole-free --config-prefix ce-whole-free
```

最终Spotless/Checkstyle `unified-wal-empty-final-checks-biXBRrhZ` exit0；root RAT exit1仍443个全部在父HEAD已有的未批准文件，无新增失败。历史manifest字段、相对链接及git diff检查通过；未push、不接生产listener/目录。


## 独立 CLOSED readonly-open 与原 SDK 读取

2026-10-09，新增显式`ProfileRecoveryCoordinator.openReadOnly(ledgerId, sdk, password, verifiedDataPool, maxReadEntries)`，从实际标准CLOSED独立导出UWRC/selection并打开原SDK `ReadOnlyLedgerHandle`。它不依赖调用方保留原恢复context，不执行recovery/grant/fence/Add，也不注册未验证的metadata listener。当前匹配已经实现的E=W=3、A2/A3 recovery-CLOSED；普通writer close、无UWRC的外来CLOSED及默认`BookKeeper.openLedger`自动Profile routing未接入，不据此宣称通用open或完整recovery完成。

cold `readClosedForOpen`重验matching CLOSED的P/L/context、physical metadata birth/version、完整selection/repair head与activation/admission历史。成员来源覆盖当前metadata的全部historical ensembles，按Bookie/incarnation核对verified HELLO；同BookieId出现多个retained incarnation时拒绝歧义，不推测rejoin。DELETE_INTENT不沿用historical-close查询的迟到成功作为新访问权限。handle构造后再读一次完整cold事实，root/metadata/context/sources改变、失去权限或SDK关闭时关闭未交付handle并返回non-OK；caller cancel保留actual cold attempt的permit，实际终结后清理未交付handle。

现有ProfileLedgerHandle内的小型ClosedReader复用原ReadOnlyLedgerHandle、ProfileContext和ProfileBookieClient。P/L固定于matching metadata，忽略来源LAC提示及未经新cold open验证的metadata更新；confirmed/unconfirmed、legacy/future都在请求分配前检查范围和P上界，transport再次拒绝e>P。maxReadEntries为1～256，aggregate读额度不足返回TooManyRequests，不建立新队列；future取消不会撤销actual SDK read，预算随实际read operation终结归还，未交付LedgerEntries关闭。legacy完成/同步抛错用同一释放门保证一次归还。wire batch read APIs仍明确IllegalOp，基础多entry读取使用原point read调度、digest与historical membership映射。

password在DATA前按原metadata核验；ordinary read及本地close均无额外MetadataStore写入、FENCE或control force。close停止本地transport，借用的SDK、DATA clients和底层在途请求仍由原owner管理；这不是全网络I/O排空或强访问撤销。caller继续负责正常交付的LedgerEntries生命周期，完整TLS/socket/SDK总预算仍待闭合。

真实验证覆盖新SDK打开A2/A3的(-1,0)、selected非空CLOSED(2,48)、历史prefix后empty-last CLOSED(2,48)，完整payload读取和legacy unconfirmed读取、local LAC/length、suffix拒绝、batch/Add拒绝、wrong password以及metadata版本/control force保持。held READ_ENTRY reply下caller cancel不提前释放额度，原operation结束后可继续读取；未交付entries关闭。actual ZK第四次metadata读取持有最终open检查，DELETE先赢后拒绝返回已构造handle；同一切点caller cancel/busy/实际结束后重开通过。标准metadata删除再按相同bytes/version0重建仍拒绝physical birth/guard丢失，不重建authority。实验测试只操作独立fixture namespace/目录。

当前为显式预连接池的readonly增量；自动发现/连接、普通writer CLOSED的fenced-close adoption、额外required authority producer、完整RepairIntent/AutoRecovery、离线catch-up/rejoin/delete/GC、全A+C+D、完整预算/掉电/长期性能及生产Gate仍OPEN，步骤14保持PARTIAL。历史源码/合同/run/manifest字段不重绑。

最终相关模型`unified-wal-model-7iag1mq5` exit0：122配置20完整正向PASS/102指定反例；737当前source hashes及732 artifact checksums核对。readonly投影192 generated/112 distinct、queue0，九个指定反例分别命中live delete、metadata birth、peer incarnation、最后gate、cancelled open返回、actual read permit、固定P/L、suffix和mutation性质。worker1、heap512/direct64MiB、tool1.7.4、seed1/fp0，无symmetry/state constraint；catalog436未全量运行。metadata/crypto/TLS/read completion是输入，模型不替代真实验证；父绑定d605fa384d33a668c3cd77941285cfb9b96a4282，historical source/design/run不重绑。


最终Java`unified-wal-local-HfnDRrIB` exit0：13类237项全通过（common37/server200；socket69、lifecycle44、metadata19、control store12、PendingAddOp19、decoder22、Bookie15），1043当前source hashes核对；源码/测试在最终广度后未变。早期checked-exception/import遗漏与model runner注册脚本定位错误属于接线错误，不计协议RED。最终Spotless/Checkstyle`unified-wal-readonly-checks-UpXB9CV8` exit0；root RAT exit1仍443个父HEAD已有路径，无新增失败，不改规则绕过。relative links、manifest JSON/historical values及git diff检查通过；不push、不接生产路径。

```bash
bash scripts/unified-wal/run-local-storage-tests.sh 'ProfileInitialControlCodecTest,ProfileOperationCodecTest,ProfileFrameCodecTest,IsolatedProfileControlEndpointTest,ProfileReadEntryReplyCodecTest,ProfileLacReplyCodecTest,SegmentProfileInitialSocketClientTest,ZKProfileLifecycleStoreTest,ProfileLedgerMetadataStoreTest,ZKProfileControlStoreTest,PendingAddOpTest,ProfileReceiveDecoderTest,SegmentBookieControlLogTest#restartedPointRead*+livePointRead*+explicitLacIs*+recoveryCloseWaitsForPrecutTicket*+uncertainRecoveryCloseForce*+profileSocketRecoveryGrantClose*+recoveredRecoveryCandidateLosesEligibility*+terminalClose*+terminalControlFraming*+standaloneTerminalBoundary*+graphCompactionSkipsTerminalSuffixAndPreservesRequiredPrefix+graphCompactionTerminalGroupNeedsNoCopyAllocation+graphCompactionRequiresGroupNetGainPreservesSharedLiveDataAndReusesFreedSource'
mvn -o -P '!cargo-zigbuild' -pl bookkeeper-server -am -DskipTests spotless:check checkstyle:check -q
python3 scripts/unified-wal/run-storage-models.py --tool /path/to/tla2tools.jar --config-prefix a-profile-readonly --config-prefix ce-profile-readonly --config-prefix a-profile-recovery-completion --config-prefix ce-profile-recovery-completion --config-prefix c-profile-terminal-cut --config-prefix ce-profile-terminal-cut --config-prefix a-profile-recovery-selection --config-prefix ce-profile-recovery-selection --config-prefix a-profile-repair-membership --config-prefix ce-profile-repair-membership --config-prefix a-profile-recovery-prefix --config-prefix ce-profile-recovery-prefix --config-prefix a-profile-lac- --config-prefix ce-profile-lac- --config-prefix a-profile-point-read --config-prefix ce-profile-point-read --config-prefix a-profile-recovery-observation --config-prefix ce-profile-recovery-observation --config-prefix a-profile-repair-copy --config-prefix ce-profile-repair-copy --config-prefix c-whole-free --config-prefix ce-whole-free
```


## 整体 fenced point recovery 入口与持久身份续接

2026-10-09，`ProfileRecoveryCoordinator.recover`把已有真实组件串成显式整体入口：fresh metadata → OPEN下条件化beginRecovery或采用已有IN_RECOVERY → 原fence coverage/SDK point predicate → immutable prefix selection → 当前最后fragment成员的SELF write-back → distinct terminal ACK quorum → matching标准CLOSED。已有matching live recovery-CLOSED只重读完整事实，返回原context/P/L，不改metadata、不再force；DELETE不被当成新恢复成功。只有matching close返回`RECOVERED_AND_CLOSED`，各阶段的DEFERRED/CONFLICT/QUARANTINED/DATA_LOSS及bounded reason独立保留。

当前支持原E=W=3、A2/A3，CRC32C/已启用SDK与descriptor scope。key/descriptor/metadata tuple先校验，wrong credential不冻结OPEN metadata。DATA/CONTROL池上限各64、descriptor固定原canonical bound、point coordinate上限及整体duration(<=1分钟)由调用方明确给出。深复制与借用资源随现有single-attempt gate一起持有，无新线程、队列、重复基础quorum算法或普通Add metadata成本。独立fence/verify/repair/finish API保留原行为；实际phase代码提取为内部调用供整体attempt复用。

cancel/deadline只结束caller等待并停后续phase；已提交metadata/control/DATA不回滚，actual child未终结不释放gate。deadline返回已知context/P/L，UNKNOWN不变成absence或close成功。scheduler拒绝也保留已经运行的actual attempt，不能在异常分支提前归还gate。已有selection直接重验并续接其固定P/L；不在已处理terminal cut的节点上重新探测P+1并重新选择。count limit/required hole不创建CLOSED，真实DATA_LOSS只来自required有限来源耗尽的原point predicate。

SELF准备复用原32槽RepairIntent和admission/commit/head体系：先找当前member/selected range的已有committed或pending intent，复用其出生身份、字段及close ID；无记录时才选空slot，按原context/selection/member/incarnation和domain-separated kind派生固定intent/install/grant/close IDs。inert历史不覆盖，满额停止。SELF install使用`recovery-self` authority domain与原initial install key分开，native仍验证已安装context/descriptor/credential/public identity，且没有normal activation。当前member不可用时不制造intent/ACK；可用成员达A才允许标准close，离线成员仍有catch-up/修复义务。

已published intent续接通过fresh readRecovery的完整head digest/出生身份/前后版本逆演及matching committed节点取得close scope。它不拿当前metadata假装原copied-at，也不弱化原commitRepair的严格条件；没有recopy或重写publication。历史fragment冻结的required prefix仍由原SDK oracle按exact historical source set读取；最后fragment为空直接走UIC6/type12，无fake grant/copy。这里不清underreplication marker、不授予strong completion/loss reset，不把A份close当成全部E份离线修复完成。

真实场景覆盖A2/A3的OPEN空/非空、真实replacement后的非空last fragment与historical empty-last，以及A2缺一当前接收端的quorum close。A3同条件先DEFERRED/IN_RECOVERY，完整池回来后以原context/selection/published IDs继续。新coordinator/不同caller operation ID重读matching结果零额外control force；wrong key保持OPEN；required hole与count limit保持non-OK。held第一份UIC4 close回复下cancel及整体deadline不提前释放gate，原publication保留，续接后原DATA force计数不增加。

首轮CD9NwecP的真实续接反例已保留：将current metadata传给原commitRepair会与其原copied-at校验冲突。修复为cold验证既有committed head并只解析原close，原commitRepair条件不变。初轮SELF install沿用initial authority domain导致exact scope冲突，改domain separation；不是放松authorizer/native检查。早期checked exception、SDK internal config访问及测试把drop-reply误作hold-reply属于接线/fixture错误，不计协议RED。

当前仍为显式预连接池的整体基础恢复增量；extra accepted required-frontier producer、普通writer CLOSED fenced-close adoption、默认SDK routing、AutoRecovery worker/marker lifecycle、完整RepairIntent cleanup、离线catch-up/rejoin/delete/GC、总预算、全A+C+D、掉电/长期性能与production Gate仍OPEN，步骤14保持PARTIAL。原source/design/run/manifest字段保持历史绑定。


最终Java`unified-wal-local-gUDDoOhU` exit0：13类240项全部通过（common37/server203；socket72、lifecycle44、metadata19、control store12、PendingAddOp19、decoder22、Bookie15），1043当前source hashes核对。private phase提取先经D7iuobqt三项原API回归；续接copied-at反例CD9NwecP、其后yx7xwfXK完整基础场景及m6pIX7El cancel/deadline/limit/loss均有独立记录；最终广度含全部新行为，历史结果不重绑。

最终模型`unified-wal-model-9iavbptf` exit0：133配置22完整正向PASS/111指定反例，749当前source hashes/764 artifact checksums核对。whole recovery A2为990 generated/716 distinct、A3为1029/715，正向queue0；verified select、fixed P/context/intent、stop、gate、matching close、UNKNOWN和history九反例命中。固定tool1.7.4/worker1/heap512/direct64MiB/seed1/fp0、无symmetry/state constraint；catalog447未全量或完整A+C+D接受，模型不生成实际I/O/crypto/metadata/AutoRecovery事实。父绑定451bd07c51da7a6f69fafc464231fd914f5eb473，旧source/design/run保持原绑定。

```bash
bash scripts/unified-wal/run-local-storage-tests.sh 'ProfileInitialControlCodecTest,ProfileOperationCodecTest,ProfileFrameCodecTest,IsolatedProfileControlEndpointTest,ProfileReadEntryReplyCodecTest,ProfileLacReplyCodecTest,SegmentProfileInitialSocketClientTest,ZKProfileLifecycleStoreTest,ProfileLedgerMetadataStoreTest,ZKProfileControlStoreTest,PendingAddOpTest,ProfileReceiveDecoderTest,SegmentBookieControlLogTest#restartedPointRead*+livePointRead*+explicitLacIs*+recoveryCloseWaitsForPrecutTicket*+uncertainRecoveryCloseForce*+profileSocketRecoveryGrantClose*+recoveredRecoveryCandidateLosesEligibility*+terminalClose*+terminalControlFraming*+standaloneTerminalBoundary*+graphCompactionSkipsTerminalSuffixAndPreservesRequiredPrefix+graphCompactionTerminalGroupNeedsNoCopyAllocation+graphCompactionRequiresGroupNetGainPreservesSharedLiveDataAndReusesFreedSource'
mvn -o -P '!cargo-zigbuild' -pl bookkeeper-server -am -DskipTests spotless:check checkstyle:check -q
python3 scripts/unified-wal/run-storage-models.py --tool /path/to/tla2tools.jar --config-prefix a-profile-whole-recovery --config-prefix ce-profile-whole-recovery --config-prefix a-profile-readonly --config-prefix ce-profile-readonly --config-prefix a-profile-recovery-completion --config-prefix ce-profile-recovery-completion --config-prefix c-profile-terminal-cut --config-prefix ce-profile-terminal-cut --config-prefix a-profile-recovery-selection --config-prefix ce-profile-recovery-selection --config-prefix a-profile-repair-membership --config-prefix ce-profile-repair-membership --config-prefix a-profile-recovery-prefix --config-prefix ce-profile-recovery-prefix --config-prefix a-profile-lac- --config-prefix ce-profile-lac- --config-prefix a-profile-point-read --config-prefix ce-profile-point-read --config-prefix a-profile-recovery-observation --config-prefix ce-profile-recovery-observation --config-prefix a-profile-repair-copy --config-prefix ce-profile-repair-copy --config-prefix c-whole-free --config-prefix ce-whole-free
```

广度后仅一处constructor长行换行；最终`unified-wal-local-5QbrOF4R`3项整体恢复定向全通过、1043当前hash核对。最终`unified-wal-model-wroy_j4v`11配置2正向/9指定反例、749当前hash/540 checksums核对；广度133配置run的唯一source差异是该换行，models/configs/runner不变，历史广度回执不重绑。最终Spotless/Checkstyle`unified-wal-whole-final-style-O9PC3jXu` exit0。root RAT `Kxvonn3R` exit1仍443个父HEAD已有未批准路径、无新增失败，不改规则绕过。relative links/manifest历史字段/git diff检查通过，未push。


## CLOSED fragment repair 与固定 close history cut

2026-10-09，接 AutoRecovery 前补齐必要的closed复制前置：`repairClosedFragment`复用原RepairIntent/admission、UIC3、原SDK point/digest、单entry实际DATA、membership+immutable receipt同事务和UIC4。整个metadata保持CLOSED；不重新OPEN或IN_RECOVERY，不重选P/L/context，不清underreplication marker或重置loss budget。原public readRecovery仍拒绝CLOSED，新的repair-only cold view要求matching live closed事实及close reference。

标准CLOSED publication在同一次birth/version/lifecycle/selection条件化CAS中增加UWCF/schema1固定80字节reference：header8、close前metadata version4、当时repair head count4、metadata birth8、P8、应用length8、UWRC完整SHA256 32、reserved4、CRC32C4。它标识原close在同一standard metadata history中的位置，只校验原CLOSED事实，不另建membership/close authority。后续closed repair追加head/version时保留原reference/P/L/CToken；inverse按selection与close各一次offset校验每个receipt，再恢复原UWRC/predecessor。OPEN digest投影只排除该reserved key，不发布OPEN投影。

旧可丢弃experimental CLOSED缺UWCF仍可按原末尾close规则解析；closed repair明确拒绝缺reference，未实现升级/migration、不提升stable兼容。新reference不改变UWRC/UWRF/UWRS/UWRP/UWRM/UWRH、UIC1～6或native DATA/control旧字节布局。独立Python struct/SHA256/CRC32C `recovery-close-cut.bin`80字节，SHA256 `f5614cd99868b41de35c924f5c397de98960913d891d13bb40f2173958a04c6a`；codec逐字节、全部截断、字段ownership/reserved/边界验证。向量只生产字节，不生产quorum或持久化事实。

readRepairable先独立验证matching CLOSED、metadata physical birth/version、原selection/完整head/activation与live delete gate。prepare/admit/publish及cold grant/close reader共用它，legacy recovery API不获得重新开写权限。closed membership CAS与原CopiedAt完整snapshot比较，保留原close bytes/P/L；copy后DELETE或metadata变化阻断publication。actual DATA copy完成但UIC4未durable时新grant range仍不可读；close late/UNKNOWN按原操作解析。

copy reader从同一已验证closed snapshot沿用原exact source set/FENCE、digest/应用length验证，上界固定P，unavailable不计absence。新目标仍inactive，first durable payload前有admitted RepairIntent；不会normal activation。resume已published目标通过新增cold resolveRepairClose重验head digest/intent birth/version/current membership及原close ID，直接解析close，不拿current metadata假装CopiedAt、不重写receipt或recopy。原commitRepair的条件保留。

真实A2/A3场景从整体恢复得到CLOSED(2,48)，向两个新Bookie连续复制/换组；每次state/P/L/context/CToken及原UWCF保持，fresh closed reader重验完整inverse，新target实际点读可见且normal禁用。第一UIC4 actual force后丢回复/断连，新coordinator与fresh snapshot按原close ID零recopy、零新增DATA force；第二target重启保留e2可读/e3拒绝和预创建容量。所有路径复用原pool/owner/I/O，未建新Maven模块或worker框架。

这补上worker复制CLOSED fragment的必要存储/控制面能力；真实ReplicationWorker Profile route、typed健康复核/marker清理、同BookieId rejoin、离线catch-up、完整delete/cleanup、额外required authority、normal writer CLOSED adoption、默认SDK routing、全预算/组合/掉电/长期性能与production Gate仍OPEN。步骤14保持PARTIAL，历史source/design/run字段不重绑。


广度`unified-wal-local-NKyQyO4H` exit0：13类242项全部通过（common37/server205：socket73、lifecycle45、metadata19、control store12、PendingAddOp19、decoder22、Bookie15）。最后仅一处callback长行换行；最终`unified-wal-local-FLahjd4o`3项相关测试通过，1044当前source hashes核对，旧广度不重绑。早期访问package-private digest的编译错误不计协议RED。

模型广度`unified-wal-model-ajwhad2c` exit0：143配置23完整正向PASS/120指定反例，760源码hash/794 artifact checksums；closed repair正向29 generated/20 distinct、queue0，九个指定反例分别命中birth、delete、reopen、P/L、原close cut、close version offset、copy barrier、readable和recopy。最终format-only`unified-wal-model-_8bwlaae`10配置1正向9反例，760当前hash/550 checksums核对；models/configs/runner不变，广度唯一source差异是callback换行。固定tool1.7.4/worker1/heap512/direct64MiB/seed1/fp0，无symmetry/state constraint；全457或完整A+C+D未运行，模型不产生crypto/TLS/实际I/O/worker清理证明，父绑定7bd5f86c59179492991ed1750e8f3fd76ae7cc63。

```bash
bash scripts/unified-wal/run-local-storage-tests.sh 'ProfileInitialControlCodecTest,ProfileOperationCodecTest,ProfileFrameCodecTest,IsolatedProfileControlEndpointTest,ProfileReadEntryReplyCodecTest,ProfileLacReplyCodecTest,SegmentProfileInitialSocketClientTest,ZKProfileLifecycleStoreTest,ProfileLedgerMetadataStoreTest,ZKProfileControlStoreTest,PendingAddOpTest,ProfileReceiveDecoderTest,SegmentBookieControlLogTest#restartedPointRead*+livePointRead*+explicitLacIs*+recoveryCloseWaitsForPrecutTicket*+uncertainRecoveryCloseForce*+profileSocketRecoveryGrantClose*+recoveredRecoveryCandidateLosesEligibility*+terminalClose*+terminalControlFraming*+standaloneTerminalBoundary*+graphCompactionSkipsTerminalSuffixAndPreservesRequiredPrefix+graphCompactionTerminalGroupNeedsNoCopyAllocation+graphCompactionRequiresGroupNetGainPreservesSharedLiveDataAndReusesFreedSource'
mvn -o -P '!cargo-zigbuild' -pl bookkeeper-server -am -DskipTests spotless:check checkstyle:check -q
python3 scripts/unified-wal/run-storage-models.py --tool /path/to/tla2tools.jar --config-prefix a-profile-closed-repair --config-prefix ce-profile-closed-repair --config-prefix a-profile-whole-recovery --config-prefix ce-profile-whole-recovery --config-prefix a-profile-readonly --config-prefix ce-profile-readonly --config-prefix a-profile-recovery-completion --config-prefix ce-profile-recovery-completion --config-prefix c-profile-terminal-cut --config-prefix ce-profile-terminal-cut --config-prefix a-profile-recovery-selection --config-prefix ce-profile-recovery-selection --config-prefix a-profile-repair-membership --config-prefix ce-profile-repair-membership --config-prefix a-profile-recovery-prefix --config-prefix ce-profile-recovery-prefix --config-prefix a-profile-lac- --config-prefix ce-profile-lac- --config-prefix a-profile-point-read --config-prefix ce-profile-point-read --config-prefix a-profile-recovery-observation --config-prefix ce-profile-recovery-observation --config-prefix a-profile-repair-copy --config-prefix ce-profile-repair-copy --config-prefix c-whole-free --config-prefix ce-whole-free
```

最终Spotless/Checkstyle `unified-wal-closed-final-style-Rk5caHaK` exit0；root RAT `2myvSAgJ` exit1仍443个父HEAD已有未批准路径，无新增失败、不改规则绕过。relative links、manifest JSON/historical values及git diff检查通过；未push、不接生产目录。

## CLOSED 全副本健康复核

2026-10-09，第14步 AutoRecovery 的清理前置新增 `ProfileRecoveryCoordinator.verifyClosedReplicas`。它独立读取实际 matching CLOSED，沿用原 context/P/L、birth、selection、完整 repair head、activation/repair 来源与 DELETE gate；不接受调用者给出的 healthy 布尔值。范围固定 CRC32C、E=W=3、A2/A3，预连接 DATA/control pool 各最多64、最多一个实际 attempt、显式 coordinate 上限与至多一分钟 deadline。旧可丢弃 CLOSED 缺 UWCF 不获得此 proof，也不做升级。

当前末段所有目标须有匹配的 DATA/control incarnation 及 durable FENCE，空 ledger/empty-last 也执行这一检查。closed fence 来源现从完整 fresh CLOSED sources 取得，包含 inactive INSTALL 后已发布的 repair 目标；原 activation-only 选择会漏掉这些目标。首次复核此前没有参加 A2 recovery 的合法副本时，可以新增一次真实 FENCE force；重复 fence 不另造写入。没有 normal activation、普通 Add 逐 entry cold read 或新线程/队列。

随后逐坐标检查 `0..P`：复用原 SDK exact write-set 的 fencing `probeEvidence`、digest、payload 比较与累计应用 length。全部 W 个来源均为 FOUND、无 payload/length 冲突且最终 length 等于 CLOSED L，才算完整；A 份 FOUND 不足。MISSING/CORRUPT 返回 UNDERREPLICATED 和首个未满足坐标的完整 source outcomes；UNKNOWN/不可达返回 DEFERRED，权限失败返回 CONFLICT，合法副本 payload/length 冲突进入 QUARANTINED。只持有一份 payload/一个 source set，一个坐标实际终结后才进入下一个，不读 P+1、不使用 Range，不保留与 ledger 长度成比例的 future 或 entry 图。

实际 scan 完成后再次独立 cold read，要求同 context、metadata physical birth/version/全部内容和 exact source receipts，并重新消费 DELETE gate。只有这一末次检查通过，私有构造器才生成 `ReplicaHealthProof`，绑定最后的完整 CLOSED snapshot。检查期间取消/deadline 可先终结 caller，但 actual child 和 cold recheck 终结前仍持有原 attempt permit；取消不生成 proof，不提前释放正在使用的借用连接/buffer，结束时关闭本地 reader，原 SDK/peer pools 仍由调用者持有。

该 proof 是一次有限观察。它尚未包含 worker 自身 lock、underreplication marker 版本/physical birth、同事务清理条件，因此不能直接授权 `markLedgerReplicated`；检查之后发生的媒体故障也不被这份观察预测。当前未修改原 `ReplicationWorker` 或 marker 清理路径，默认服务路由保持 OFF，步骤14及生产 Gate 仍 PARTIAL/OPEN。下一实际接线是 worker 的 Profile 分支、持久 intent 续接，以及同一清理 cut 下的 fresh proof/lock/marker 条件消费；同BookieId rejoin、离线 catch-up、完整 delete 和稳定兼容仍未交付。

真实验证覆盖 A2/A3 空/非空 ledger、历史末段/empty-last、两次 CLOSED 目标替换，以及 A2 仍可恢复但历史第三副本缺 `e0`：复核明确 UNDERREPLICATED，完成原 CLOSED historical copy 后才得到 proof；缺失 peer 为 DEFERRED。实际 TLS 回复 digest 破坏不能成为 FOUND，错误 credential、读取拒绝、coordinate 限额不生成 proof。三个 actual READ_ENTRY 回复被持有时，cancel/deadline 后第二 attempt 仍被拒绝；释放实际回复后可重新完成。持有回复期间真实 DELETE 或相同 bytes 的 metadata 删除重建均阻断最终 proof，且 metadata 没有被重新开放。

有限 `ProfileReplicaHealth.tla` 检查两坐标、三副本及空 ledger：完整副本、UNKNOWN、length、empty targets、birth/version/delete freshness、caller stop 与 actual read permit，1正向/9指定反例。reply/digest/FENCE outcome 是输入，不生产 fsync/TLS/crypto/worker lock/marker 事实，不证明检查后的物理媒体稳定性、完整 A/C/D 组合或 liveness。

重现使用既有 runner：

```bash
bash scripts/unified-wal/run-local-storage-tests.sh 'SegmentProfileInitialSocketClientTest#closedReplicaHealthFindsHistoricalHoleAndRequiresAllCopiesAfterRepair+closedReplicaHealthCancellationAndAuthorityChangesNeverDeliverProof+wholePointRecoveryClosesEmptyAndNonemptyFromOpenWithOriginalSdkAndQuorum+closedRepairCopiesToNewTargetsPreservingOriginalCloseAcrossRepeatedMembershipChanges'
mvn -o -P '!cargo-zigbuild' -pl bookkeeper-server -am -DskipTests spotless:check checkstyle:check -q
python3 scripts/unified-wal/run-storage-models.py --tool "$UW_TLC_JAR" --config-prefix a-profile-replica-health --config-prefix ce-profile-replica-health
```

最终Java `unified-wal-local-mD0mQuck` exit0：13类244项全部通过，common37/server207（socket75、lifecycle45、metadata19、control store12、PendingAddOp19、decoder22、Bookie15），1044当前输入，16 artifact checksums。最终style `unified-wal-replica-health-checks-rsXuD6Qm` exit0；root RAT exit1，443个unapproved路径全部在父HEAD，新增model/cfg不增加失败，原RAT规则不绕过。checks目录6个artifact checksums。

模型广度 `unified-wal-model-e1zy0v_3` exit0：153配置24完整正向PASS/129指定反例，771 source inputs/824 artifact checksums；之后仅测试调用长行换行。最终 `unified-wal-model-o60l_nvd`10配置1正向/9指定反例，771当前输入/561 checksums；Java与最终模型227重合/1588去重当前输入及父HEAD五个designInputs全部核对，历史广度不重绑。新正向38116 generated/19380 distinct、queue0、depth/diameter12、1.044秒/peak RSS186744832 bytes；这是有限模型检查消耗，非存储性能。tool1.7.4，单worker、heap512/direct64MiB、seed1/fp0，无symmetry/state constraint，CHECK_DEADLOCK FALSE；完整467配置未运行。

初次 Java `unified-wal-local-AtyTfrTH`暴露closed新target未纳入fence的实际DEFERRED，修复完整source选择后`unified-wal-local-toiVa5rq`该场景及两项新故障场景通过；其唯一失败是A3此前已经全部fenced却仍期待额外force的测试计数，按真实A2缺receiver条件修正后运行上述最终广度。初次模型 `unified-wal-model-8yc1i5m1` 缺FiniteSets导入，INCONCLUSIVE不计PASS；补齐后定向 `unified-wal-model-k0v55ha_`10配置符合预期，再执行上述广度/最终检查。style首轮一处122字符行失败，换行后最终检查通过。

## Profile marker 条件化清理

2026-10-09，第14步的显式清理入口 `ProfileRecoveryCoordinator.completeReplication` 接通上节真实全副本健康 proof 与 `ZkLedgerUnderreplicationManager`。proof 绑定原签发 coordinator、原 health attempt deadline、exact CLOSED snapshot，以及开始任何 health read 前捕获的私有 ProfileTask（原 manager/lock 对象及 nonce），且只消费一次；新 coordinator/过期/已使用 proof、busy gate 或 caller 在 cold submission 前取消都不能清理。观察用的无 worker overload 不携带 task，明确拒绝清理；同 manager release/reacquire 后的新 lock 也不能被旧 proof 消费。失败或 UNKNOWN 后需要实际重新复核，不复用旧 healthy 位。所有能力仍是隔离 opt-in，未接原 ReplicationWorker 自动循环。

消费前再次独立验证 matching CLOSED 的 context/P/L、physical metadata birth/version、selection、完整 head/sources 与 DELETE。清理同时检查标准 metadata birth child/version、原 lifecycle physical birth/UUID/root/head version；它们与 marker 和 worker witness 的删除放在同一次 ZK multi。仅原 coordinator 与 underreplication manager 使用同一个借用 ZK mutation client 才支持这一路径；其他后端/不同 client 显式拒绝。ROOT/metadata/DELETE 在最终事务前改变，不因历史健康观察放行。

manager 的 `enableProfileCleanupGuards` 必须在取得 task 前显式启用；默认 OFF 的 Classic 路径保持原行为。opt-in poll/get 以同一个 multi 创建原路径的 ephemeral lock 和随机、session 专属的不可改写 ephemeral witness，捕获原 marker version/czxid、原 session 及 lock physical birth；回复丢失也保留本地 pending obligation；actual pair创建与release在同一captured lock monitor下串行，不能提前忘记pending创建。手工 acquire 没有捕获 marker 身份，不能据它清 marker。原 lock data/UnderreplicatedLedgerFormat/布局版本不改变，新 witness 是实验 namespace 扩展；取得 task 前捕获的私有 receipt 固定这一个 lock 对象，清理不能改取同 ledger 的新 held lock。

清理只接受非空、最多256个 exact-version `Op.check`。marker 使用只读 persistent `profile-birth-<czxid>` 子节点，8字节BE physical birth；创建后重读 marker/child 验证，防止 version0 ABA。若父 marker 在 witness 创建窗口重建，拒绝清理并去掉本次误写入的新父节点中的旧 birth child，使新任务能再取得。最终一个 multi 检查 cold authority、原 session witness、lock version、marker birth witness/version，并删除 witness、marker、lock；失败保持 marker/lock。CLEARED 仅表示此 guarded removal 实际返回成功。

正常 opt-in release/close 先验原 session、实际 lock birth/owner/version 和唯一 witness，再在同一 multi 去掉 pair；不存在原 witness 时不删除新 holder 的 lock。失败保留本地义务供重试；实际 create 或 cleanup 事务完成后丢回复不伪造成功，后续 release 不删除其他 manager 的新 lock。session 终止/更新撤销旧 witness。此协议覆盖受控 worker release/close/session 终止；管理者在检查后强制删除并重建 lock 却保留旧 witness 的强 reset/revoke 并发协议仍 disabled，不能拿 pre-read+version 检查宣称已支持它。

proof 领取/清理持有同一个实际 coordinator gate，caller cancel 不提前释放已经开始的 cold command；已经提交的 guarded mutation 可以在 caller 取消后终结，不回滚持久事实。ZK 不可用/中断返回 DEFERRED，marker/authority 身份变化返回 CONFLICT，DELETE/invalid authority 分别为 DELETED/QUARANTINED。空 hierarchy 与 marker/page retention GC 尚未作为该入口的完成事实；候选选择、持久 RepairIntent 续接、worker 分支与实际 attempt 排空、同BookieId rejoin/离线 catch-up、集群 delete 及完整预算/组合/兼容/性能验收仍 OPEN，步骤14保持 PARTIAL。

真实验证分别生产不同事实：原 TLS/文件/SDK 全副本复核产生 proof，真实 ZK manager 检验清理与 marker/lock 行为。前者覆盖成功、marker更新、DELETE、错误签发者、不同 ZK client、同 bytes metadata 重建、proof 过期、原 task release/reacquire及无task observation；后者覆盖默认OFF、错误/空checks、手工lock、marker update/相同version重建、最终multi前竞争、witness创建时重建、create/cleanup实际提交后丢回复及session终止后的新holder保留。测试 fixture 显式使用127.0.0.1，避免当前localhost被外部DNS代理解析到198.18.0.1；未修改主机DNS或生产配置。

有限 `ProfileMarkerCleanup.tla` 以已经完成的 health/guard/reply 为输入，记录清理时的 cut，检查原 marker birth/version、metadata/DELETE、原 lock witness、原acquired task、issuer/deadline/once、cold submission前stop、actual gate及UNKNOWN后的foreign lock保留。1正向/12指定反例；不生产 DATA/digest/fsync/TLS、完整 worker 调度或强 reset/liveness。晚于清理的重建/故障不会倒写历史 CLEARED。

重现使用既有 runner：

```bash
bash scripts/unified-wal/run-local-storage-tests.sh 'TestLedgerUnderreplicationManager,SegmentProfileInitialSocketClientTest#closedHealthProofClearsOnlyMatchingMarkerThroughItsIssuerAndOriginalLock+closedReplicaHealthFindsHistoricalHoleAndRequiresAllCopiesAfterRepair+closedReplicaHealthCancellationAndAuthorityChangesNeverDeliverProof'
mvn -o -P '!cargo-zigbuild' -pl bookkeeper-server -am -DskipTests spotless:check checkstyle:check -q
python3 scripts/unified-wal/run-storage-models.py --tool "$UW_TLC_JAR" --config-prefix a-profile-marker-cleanup --config-prefix ce-profile-marker-cleanup
```

最终Java `unified-wal-local-RXp8VfLD` exit0：14类269项全部通过，common37/server232（socket76、URmanager24、lifecycle45、metadata19、control store12、PendingAddOp19、decoder22、Bookie15），1046当前冻结输入/17 artifact checksums。最终style `unified-wal-marker-final-checks-dSfgcejv` exit0；root RAT exit1/443 unapproved路径，全部在父HEAD，新增model/cfg未增加失败；checks目录6个checksums，不改RAT规则、不计CI全绿。

最终模型 `unified-wal-model-lh5o9ekm` exit0：166配置25完整正向PASS/141指定反例，787当前source inputs/863 artifact checksums；Java与模型229重合/1604去重输入及父HEAD五个designInputs全部核对，历史回执不重绑。新marker正向12908 generated/2944 distinct、queue0、depth/diameter13、0.923秒/peak RSS155844608 bytes；此为有限模型消耗，非存储性能。tool1.7.4，单worker、heap512/direct64MiB、seed1/fp0，无symmetry/state constraint，CHECK_DEADLOCK FALSE；完整480配置未运行。

前期`unified-wal-local-n4zJtscr`是SessionExpiredException构造签名的编译错误，不计协议RED；`unified-wal-local-X8Lawqnp`四项因localhost外部DNS解析而在fixture初始化失败，未执行协议检查。改用literal loopback后`unified-wal-local-l030ANhc`5项通过；增加pending acquire/release串行与其测试后`unified-wal-local-uJrqepQq`8项通过。原较早广度`unified-wal-local-xp3muzcq`268项/model `unified-wal-model-1ujvitem`165配置在补原task绑定前通过，仅作原切片历史记录；发现旧proof可能消费同manager新task后，改为起点绑定私有receipt并扩大到上述最终269项/166配置，不以旧结果代替修复后验证。初次模型`unified-wal-model-0w41kl25`因TLA运算符括号解析失败为INCONCLUSIVE；修正后的`unified-wal-model-ud9f5ly1`12配置及较早`unified-wal-model-q91ev34v`12配置不包含最终task反例，均不替代当前13配置。

## 原 ReplicationWorker 的 Profile 自动循环

2026-10-09，第14步增加原 `ReplicationWorker` 的显式 Profile constructor，使用原 SDK/factory/worker thread/backoff/stats，借用已有 coordinator、一个 canonical descriptor、已验证 DATA/control pools。默认 production constructor 不启用 Profile；实际 metadata 只要含 backlink key（包括损坏值），便不进入 Classic open/copy。显式 Profile cohort 中 metadata 丢失/不可用也保留 marker，不能用 NoSuchLedger 当删除事实；未启用 cohort 时原 Classic NoSuch 语义保留。

`WorkerOptions` 固定 pool各最多64、coordinate上限、每attempt至多32个新复制及至多一分钟stop deadline；descriptor按现有validator解码、每ledger按实际metadata password派生protected credential，不逐entry访问远端MetadataStore或handshake。新constructor要求同借用ZK mutation context与兼容SDK scope，并在poll前启用原manager的task guards。默认服务启动/bootstrap、registry发现pool及动态重连/多descriptor配置仍未接线，当前仅显式隔离cohort。

一次 `WorkerAttempt` 从原poll task捕获私有receipt，在同一个coordinator实际permit下串起原private恢复核心：OPEN进入现有fenced point recovery，IN_RECOVERY按已存context/selection/SELF/head续接，matching CLOSED直接验证；再处理worker已持久化的COPY intent、实际inactive INSTALL/admission/grant/原SDK单entry copy、同snapshot membership publication与原close ID。A3缺当前接收者时，在已固定selection下复制/更换last fragment再重入原恢复，不能把A2结果当A3 close。已发布目标的close续接不recopy、不消耗新复制预算；当该目标随后丢失，可保留历史并修复新的当前副本。

worker的inert计划仍使用原固定32槽UWRP及admission/commit receipt，不另建工作项日志。`worker-repair` authority domain隔离原normal/SELF operation；install/intent/grant IDs由原context、当前metadata digest、fragment/member/last与实际target incarnation确定，close ID在计划创建时固定，published时读取原receipt。重启先查原目标与ID再选择新target；不会因reply丢失换ID或覆写旧意图。候选来自提供的verified pools，按BookieId稳定顺序选择，排除全部当前及留存activation/repair目标，不复用旧BookieId/incarnation，不做rack/AZ或strong loss reset。

CLOSED从0..P沿原exact write-set复核完整W份合法DATA和累积length，末次重读metadata/DELETE后，以原task/lock/marker条件化清理。最后fragment的FENCE失败仅给UNAVAILABLE诊断用于有界复制选择，不当DATA absence/FOUND；有冲突/权限/required loss时保留相应错误与marker。成功只有REPLICATED且guarded removal实际返回成功；numLedgersReplicated及原rereplicate stats按这一结果统计，失败使用原exception counter。

deadline/stop只停止未来阶段，不提前终结实际attempt。worker被interrupt/shutdown时设置stop，等待已发RPC、buffer custody及cold command实际终结，才release原task receipt；不走Classic的延迟release timer，不关闭借用SDK/peer pools。已经提交的持久化或CAS不回滚，UNKNOWN仍按原intent/close identity续接，不释放DATA坐标。实际I/O/网络始终不终结时不能声称drain已完成；无liveness或瞬时强撤权保证。

真实worker在原线程循环中验证A2/A3：健康CLOSED、OPEN缺一个成员后COPY/close、CLOSED缺成员、OPEN空ledger全副本健康，以及历史fragment的第三副本缺DATA后COPY/全副本复核。另验证默认OFF、损坏backlink、缺metadata、没有unused候选的空CLOSED缺成员都保留marker；持有三条实际READ回复时shutdown保持lock与permit，排空后借用SDK/pools可用，新worker接原marker。UIC4实际force后丢回复/断连，新coordinator沿published intent零recopy，再在maxRepairs=1下替换损坏目标及之后丢失的修复目标，保留P/L、原UWCF与完整head。Classic实际bookie的replication stats/empty open两项回归也执行；ZooKeeperUtil保留服务器numeric loopback，不反解到外部DNS，不更改主机DNS或生产网络设置。

该worker里程碑当时未实现空ledger/empty-last缺成员的grantless membership repair；最新实现见下节。candidate耗尽/32槽满额保留任务，不自行强reset；worker仅自动续接本路径的worker-repair intent，其他人工intent/更复杂并发协调仍需对应合同与验证。原AutoRecoveryMain服务bootstrap、动态pool、sameBookieId rejoin/离线catch-up、hierarchy/page GC、distributed delete、完整A/C/D组合/总预算/兼容/掉电/长期性能仍OPEN，步骤14保持PARTIAL，生产Gate不提升。

有限 `ProfileReplicationWorker.tla` 投影已启用worker的route/recovery/plan/copy/publication/close/health/marker/stop ownership组合。2正向（A2/A3）/14指定反例覆盖已识别Profile不得Classic fallback、全部W才能清marker、durable copy前置、normal禁用、intent固定、published不recopy、child留lock、原task/DELETE、descriptor、UNKNOWN、EMPTY不得使用DATA copy、work bound和P固定。该模块投影DATA-copy路径，最新grantless EMPTY协议由下节独立模型拥有。point selection、force/guard/reply是输入，非fsync/TLS/crypto生产者；无Profile证据且未启用cohort的Classic路径不在该Profile保证内。

重现：

```bash
bash scripts/unified-wal/run-local-storage-tests.sh 'SegmentProfileInitialSocketClientTest#replicationWorker*,TestReplicationWorker#testReplicationStats+testReplicateEmptyOpenStateLedger'
mvn -o -P '!cargo-zigbuild' -pl bookkeeper-server -am -DskipTests spotless:check checkstyle:check -q
python3 scripts/unified-wal/run-storage-models.py --tool "$UW_TLC_JAR" --config-prefix a-profile-replication-worker --config-prefix ce-profile-replication-worker
```

Java广度 `unified-wal-local-LBkcGJJv` exit0：15类275项（common37/server238，socket80、URmanager24、Classic worker2、lifecycle45、metadata19、control store12、PendingAddOp19、decoder22、Bookie15），1049冻结输入/18 artifact checksums；它在最后预算修复及historical场景扩展前运行，历史绑定不改。预算修复/扩展后 `unified-wal-local-XZ89GbCr`8项通过（含两项Classic），1049 inputs/5 checksums；最后仅WorkerRepairPlan record长行换行，当前 `unified-wal-local-bNb7w4QU`2项worker主线通过，1049当前输入/4 checksums。最终style `unified-wal-worker-final-checks-eUiTDc42` exit0；root RAT exit1/443 unapproved全部在父HEAD，新增model/cfg未增加失败，checks目录6项。无CI全绿或性能改善声明。

模型广度 `unified-wal-model-kwjihg5e` exit0：182配置27完整正向PASS/155指定反例，806输入/910 artifact checksums；之后仅两处生产代码的新复制预算/对应测试和最终record换行，TLA/配置未改变。最后预算后的定向 `unified-wal-model-6t30rm5u`16配置符合预期；最终 `unified-wal-model-kq_twh8v`16配置2正向/14指定反例、806当前输入/603 checksums。最终Java与模型232重合/1623去重当前inputs及父HEAD五个designInputs逐项核验；历史run不重绑。A2正向2445 generated/1350 distinct、queue0、depth/diameter12、0.919秒/peak RSS132579328 bytes；A3为2508/1402/0、13/13、0.877秒/129728512 bytes，是有限模型消耗，非存储性能。官方tool1.7.4、单worker、heap512/direct64MiB、seed1/fp0、无symmetry/state constraint、CHECK_DEADLOCK FALSE。完整496配置未运行。

早期 `unified-wal-local-Wcz1OWTH`/`GxO6EURv`/`iQSkfu35` 分别是metadataDigest的IOException声明、FutureUtils包装interrupt后的无效catch、配置setter返回void的编译错误，不计协议RED。`unified-wal-local-TSSTFkXD`5项及`QIKDmWUt`6项先验证worker/Classic切片，再进入广度；发现published close续接错误消耗新copy预算后补上述maxRepairs=1覆盖。初次模型 `unified-wal-model-ncyqftoa`为括号解析失败INCONCLUSIVE，不计PASS，修正后才执行定向与广度。

## 空副本的 grantless membership repair

2026-10-09，第14步在原 worker-repair 固定32槽、admission、membership CAS及guarded marker completion中补齐空ledger／历史空末片段缺成员。只允许冻结selection的P=lastStart-1，保留原P/L/context、metadata出生与UWCF close-history；历史DATA仍须通过原point恢复与全部W健康复核。新目标复用已验证pool与原INSTALL driver，保持inactive，不获取DATA坐标、RECOVERY_GRANT或normal activation。

顺序为 inert EMPTY intent → 实际inactive INSTALL/receipt → 原admission → 原UIC6 terminalClose实际force/apply成功 → 同一ZK multi发布标准membership、UWRM/head及hash绑定的immutable empty-boundary receipt。CLOSED adoption和terminal冷authority reader都验证receipt；缺失、损坏、scope/hash/P/L/selection不匹配进入QUARANTINED。receipt只记录已完成边界，不成为第二份membership authority。私有构造的EmptyRepairProof仅由实际terminalClose客户端完成产生；公开DATA copied-at入口拒绝EMPTY，调用者不能用任意snapshot或伪造copy数量发布空修复。

UWRP现有DATA schema1/kind0和UWRM DATA schema1/kind0字节保持不变；新增EMPTY使用schema2/kind1。EMPTY UWRP编码first=fragmentStart、last=fragmentStart-1、zero grant ID，在offset320插入独立terminal operation ID16，逻辑range为null，DATA grant/close API明确拒绝。EMPTY UWRM在offset100追加empty-boundary SHA-25632；原UWRH仍schema1。UWEB/schema1固定124字节：header8、intent SHA-25632、terminal ID16、authority generation8、P8、L8、selection digest32、实际local generation8、CRC32C4。当前MAX UWRP775、UWRM393；旧DATA长度上界与固定向量仍有效。这些是实验存储codec扩展，不声明stock binary/old decoder或stable兼容。

三个独立Python struct/SHA-256/CRC32C生成的固定向量由Java strict parser验证并逐字节往返：repair-empty-intent.bin400（64397a5b9a05df146695678855e0a20706d8d4697f52de800c16602d93f1d73d）、repair-empty-boundary.bin124（ece2f51a18a4d880baaf1bb062d38d88bac493c9a347773b49d2aa4ebc63b12a）、repair-empty-commit.bin145（704207f5209be53f55ef7e58b0b01eb1ba51ff2c5a7a5787fe2d581d14ca6357）。测试覆盖全部截断、重算CRC后的schema/kind/sentinel/grant/operation错配及数组ownership；没有重生成旧DATA向量。

UNKNOWN或stop后沿原intent birth/terminal ID续接；已admitted不重做INSTALL，避免改变原admission local generation。目标terminal持久成功但reply未交付时，保留原task/permit，排空后新coordinator按原操作幂等解析，再发布；DELETE或metadata身份变化时不能发布。published结果冷解析原receipt，无DATA recopy。后续恢复close quorum和完整W健康复核继续复用原路径，只有原task条件化清理实际成功才清marker。

真实TLS/ZK/native files测试覆盖A2/A3各四种OPEN或CLOSED空ledger／有历史prefix的空末片段；marker清除、原P/L保持、零DATA force、inactive及目标文件重启后type12 suffix exclusion都验证。另测实际boundary reply持有期间shutdown保留原lock、同intent续接、DELETE阻止publication、receipt删除/损坏导致QUARANTINED，以及terminal force失败保留旧membership/marker和原task。进程退出后文件重读不作为掉电证据。

有限ProfileEmptyRepair检查两个冻结输入P=-1/L=0与P=2/L=48，1正向/11指定反例验证admission、durable边界先于publication、零DATA/零grant/无normal、原cut/birth/receipt/操作ID、DELETE与实际child permit。verified historical prefix、force/apply、cold metadata guard是输入；模型不生产TLS/crypto/fsync或liveness事实。DATA worker投影的EMPTY mutation仍表示禁止fake DATA repair，不替代新grantless路径。

复现：

```bash
bash scripts/unified-wal/run-local-storage-tests.sh 'ZKProfileLifecycleStoreTest#emptyRepairVectorsExplicitlyForbidDataAndBindTheirBoundaryReceipt+repairIntent*+repairCommit*,SegmentProfileInitialSocketClientTest#replicationWorkerRepairsEmptyLedgerAndEmptyLastWithoutDataOrGrantForA2AndA3+emptyRepairForceFailureRetainsMembershipAndOriginalTask+emptyRepairLostBoundaryReplyResumesOriginalOperationAndDeleteNeverPublishes'
python3 scripts/unified-wal/run-storage-models.py --tool "$UW_TLC_JAR" --config-prefix a-profile-empty-repair --config-prefix ce-profile-empty-repair
```

当前仍是显式隔离cohort、默认OFF、可丢弃目录。没有unused候选或固定槽耗尽继续DEFERRED；service bootstrap/动态pool、外部intent协调、same-ID rejoin/离线catch-up、delete/GC、总预算/完整A+C+D、stable兼容、Linux NUMA/掉电/长期性能和生产Gate仍OPEN，步骤14保持PARTIAL。

模型广度 `unified-wal-model-mdoft16o` exit0：194配置28完整正向PASS/166指定反例，819源码inputs/946 artifact checksums；之后仅两处实现注释、accessor换行和测试长行换行，TLA/配置不变。最后定向 `unified-wal-model-ybw2p3z1` exit0：12配置1完整正向/11指定反例、819当前inputs/609 checksums。EMPTY正向106 generated/64 distinct、queue0、depth/diameter9、0.792秒/peak RSS106070016 bytes（广度run），是有限模型消耗，非存储性能。tool1.7.4、单worker、heap512/direct64MiB、seed1/fp0，无symmetry/state constraint，CHECK_DEADLOCK FALSE；完整508配置和完整组合未运行，历史run不重绑。

Java广度 `unified-wal-local-7JWtFZ0f` exit0：15类279项全部通过，common37/server242（socket83、lifecycle46、URmanager24、Classic worker2、metadata19、control store12、PendingAddOp19、decoder22、Bookie15），1052冻结inputs/18 artifact checksums；之后仅上述注释/格式调整。最终 `unified-wal-local-ckN58IQn` exit0：7项定向全部通过，1052当前inputs/5 checksums。最终style `unified-wal-empty-final-style-XKFTaAnK` exit0/2 checksums；root RAT `unified-wal-empty-checks-VkHnCexR` exit1/443 unapproved路径，与上一里程碑完全相同且全在父HEAD，5 checksums包含初轮三个accessor LeftCurly格式失败记录；已修正并通过最终style，不改RAT规则、不计CI全绿。最终Java与模型232共享/1639去重inputs、五个父HEAD designInputs及artifact checksums逐项核验；run sourceCommit/head保持父提交和冻结working-source hash的真实身份，不重绑。

2026-10-09 AutoRecovery动态连接前置已补[registration消费者发现与再次复核](../startup-readiness/README.md#消费者发现与再次复核)，仍需实际mTLS/HELLO/再次复核与pool ownership接线；发现观察结果不授予ledger mutation authority，原WorkerOptions仍为显式preconnected cohort，默认服务bootstrap与生产Gate保持OPEN。

## 注册观察驱动的连接与实际本地排空

2026-10-11，第12/14步在既有ProfileInitialControlClient/ProfileNormalDataClient新增显式connectRegistered，返回一个拥有该次连接的RegisteredConnection。调用者提供既有ZK RegisteredPeer观察、client TLS、EventLoopGroup和有界ArrayBlockingQueue/AbortPolicy cold executor；group/executor保持借用，无新线程、后台watch、自动重试或普通Add的逐entry MetadataStore调用。

连接顺序为原观察再次cold复核 → cold解析endpoint/DNS → 既有TLS1.3+mTLS与HTTPS hostname校验 → 既有HELLO的engine/BookieId/incarnation/实际能力检查 → HELLO readiness generation等于观察中的READY generation → 再次cold复核 → 交付CONTROL或DATA client。当前实际endpoint只接受tls://host:port，显式端口1..65535，拒绝userinfo/path/query/fragment、不支持scheme或不可解析地址；不改UWEP存储格式，旧hint文本仍可被消费者观察，但不能被这个连接入口当作可连接地址。READY的protocol major/minor必须属于当前实验版本；hints不授予transport、ledger或ACK authority，实际RPC/coordinator仍检查所需capability和权限。

连接预算以nanoTime差值检查，CONNECT、TLS和HELLO消费连接阶段剩余预算；成功后每次RPC仍使用原配置timeout，不把耗时的cold检查扣到普通RPC超时上。caller等待超时、取消或close停止未来阶段；已排队/运行的cold任务和DNS/read仍必须实际终结，不承诺即时中断。公开completion是副本，不能复活内部结果。失败、取消或最后身份变化关闭该次socket，返回固定错误文本，不公开endpoint或敏感输入的异常cause。

原close仍为非阻塞触发；新增closeAsync等待实际本地channel关闭、HELLO输出promise以及所有已取得的RPC/source/plaintext输出slot终结。close永久收走unused semaphore permits，slot在write和wire均terminal后才归还并收走；在closed检查前已开始的caller仍不能在drain后取得被回收的permit。channel closeFuture之后还经过event-loop收尾，避免把closeFuture早于outbound失败/inactive回调当作排空完成。RegisteredConnection的closeAsync额外等待实际cold child；借用group/executor必须活到该future完成，不得先shutdownNow丢弃尚未执行的任务。

caller completion回调不应持有lifecycle lock或推迟实际socket close：先记录stop并启动关闭，完成drain检查，再在锁外完成caller future。对应真实回归让completion callback等待drain，同时cold终检仍在队列中，验证关闭不阻止cold收尾。任一底层实际工作不终结时不能声称drain完成；调用者也不能在自己的Netty event loop中阻塞等待closeAsync。

这个future仅证明本地资源终结，不是远端force/abort或确定NONE的证据。已提交后断连/timeout仍保留UNKNOWN和原操作/坐标/intent义务，不回滚、不截短共享文件，不因连接关闭授予FREE。旧未注册connect入口和普通RPC结果三元组继续使用既有行为。

真实TLS/ZK/native files测试包括CONTROL与DATA成功连接后实际FENCE、普通Add和点读；不支持endpoint、错误TLS trust、incarnation/generation错配；持有实际HELLO回复时删除注册；取消后queued cold终检排空；回复已到但输出promise未终结时close继续等待并在terminal后清零plaintext；queued DATA借用继续计费、恰好归还且原输入不变；连接cold排队消耗2.5秒后，实际FENCE回复持有2秒仍按原4秒RPC配置等待。单节点durable不冒充quorum ACK或掉电证据。

有限ProfileRegisteredConnection投影验证pre/HELLO/post三种事实、cold/output/HELLO-write/transport实际terminal前不归还容量，以及关闭收走permit阻止旧caller晚取得；1正向/8指定反例。事实与terminal均为输入，模型不生产TLS/ZK/fsync、不覆盖remote abort、clock/liveness或完整预算组合。实际callback锁等待与RPC计时由Java回归检验。

复现：

```bash
bash scripts/unified-wal/run-local-storage-tests.sh 'SegmentProfileInitialSocketClientTest#registered*+closeWaits*+validatedReplyDoesNotReleaseSerializedSlotBeforeActualWritePromise+cancelledWaitingRetainsBoundedPendingUntilOriginalWireTerminal'
mvn -o -P '!cargo-zigbuild' -pl bookkeeper-server -am -DskipTests spotless:check checkstyle:check -q
python3 scripts/unified-wal/run-storage-models.py --tool "$UW_TLC_JAR" --config-prefix a-profile-registered-connection --config-prefix ce-profile-registered-connection --config-prefix a-profile-peer-discovery --config-prefix ce-profile-peer-discovery --config-prefix a-profile-initial-client --config-prefix ce-profile-initial-client --config-prefix a-profile-normal-client --config-prefix ce-profile-normal-client
```

当前为显式单连接入口。AutoRecoveryMain bootstrap、动态pool/cohort发现及新旧/排空连接总预算、service shutdown与原task/session所有权仍待接线；same-ID rejoin、离线catch-up/delete/GC、完整A+C+D、stable兼容、Linux NUMA/掉电/正式长期性能和生产Gate保持OPEN，步骤12/14保持PARTIAL。历史临时回执在2026-10-11恢复时已不可访问，因此当前源码重新运行独立验证；历史名称、sourceCommit/designInputs和结果绑定不改写，本轮回执另存持久artifact目录。

相关模型 `unified-wal-model-ezwn7ipx` exit0：44配置7完整正向PASS/37指定反例，837源码inputs/685 artifact checksums；在最后caller回调锁外完成的收尾前。最终 `unified-wal-model-nx8kvzgw` exit0：9配置1正向/8指定反例、837当前inputs/621 checksums。正向225 generated/163 distinct、queue0、depth/diameter11、1.027秒/peak RSS109920256 bytes（最终run），是有限模型消耗而非存储性能。tool1.7.4、单worker、heap512/direct64MiB、seed1/fp0、无symmetry/state constraint、CHECK_DEADLOCK FALSE；完整524配置未运行。本轮Linux NUMA、掉电与正式可持续性能为NOT_EXECUTED。

关闭顺序回归的实际RED `unified-wal-local-37Yuqit5` exit1：临时恢复旧的锁内caller completion先于socket close顺序后，registeredCloseCallbackCanAwaitDrainWithoutHoldingLifecycleLock在实际cold排队场景产生Timeout；修复源码经SHA guard恢复，未覆盖未知改动。此前修复版`unified-wal-local-aIrRNINt`两项PASS，最后源码的完整相关回归另行记录。受控mutation及fixed/red源码保存在`/Users/liusinan/.codex/artifacts/unified-wal/connection-callback-red-aywd9rqr`，不将测试编译错误或fixture误用INSTALL当成协议反例。

最终Java `unified-wal-local-UOAB6lBP` exit0：17类317项全部通过、零skip，common37/server280（socket91、readiness13、ephemeral17、lifecycle46、UR24、Classic worker2、metadata19、control store12、PendingAddOp19、decoder22、Bookie15），1052当前冻结inputs/20 artifact checksums。最终style/root RAT `connection-checks-0kIfSr76`为exit0/exit1，443 unapproved路径全在父HEAD、没有新增失败路径，5 checksums；不改许可规则，不计CI全绿。Java/model共享232/去重1657 inputs与五个父HEAD designInputs全部核验，run的parent head与working-source hashes不重绑。

本轮Java、模型、RED及check回执保存在`/Users/liusinan/.codex/artifacts/unified-wal/registered-connection-evidence-842lirim`及其archive.json列出的checks/mutation目录；原manifest/config/command内容保持原run位置和身份，archive为逐字节副本。TLC1.7.4从官方release重新取得，SHA-1 bee4a54f3ee3d4afc347c3240ec2d9e93b075104、SHA-256 936a262061c914694dfd669a543be24573c45d5aa0ff20a8b96b23d01e050e88，与冻结工具锁一致。历史tmp目录缺失不借用为当前验证。

## Worker 与服务关闭的中断及并发边界

2026-10-11，第14步在动态pool/service接线前修复原ReplicationWorker和AutoRecoveryMain的shutdown边界。原worker在join被中断后会继续关闭underreplication manager／自有SDK；同时workerRunning=false会让第二个shutdown caller提前返回。实际Profile READ child仍持有原task和buffer时，这两条路径都不能作为关闭完成。

ReplicationWorker用既有实例monitor记录唯一shutdown owner和cleanup结束。owner停止新循环、取消原timer、interrupt原线程；所有外部caller均等该线程实际终结，收到interrupt只记账并继续join，非owner还等owner cleanup结束。完成后恢复各caller原有或等待期间的interrupt。worker在run的终态异常路径自停时不join自己，external owner已经负责关闭时worker也不等待该owner，避免互相等待；这条自停路径只能在当前attempt返回后到达。start与shutdown互斥，启动前close也取消timer/关闭资源，并显式拒绝之后重启，不复活已结束实例。原isRunning仍表示循环状态，不能替代shutdown实际排空证明。

AutoRecoveryMain同样只有一个shutdown owner，其余caller等待该procedure收尾；启动与关闭互斥。入口及auditor/worker等待期间的interrupt先记录，auditor被中断后继续等待；worker返回时恢复的signal先取出，保持SDK.close的等待不带继承interrupt，最后再恢复caller signal。依赖关闭在实际worker drain之后发起。关闭过程中新的依赖异常仍沿原日志/失败语义处理，本段不宣称所有SDK/manager close都无条件成功，也不证明任意永不终结child的liveness。

两条真实RED分别为：持有三条actual READ回复时预先interrupt的worker shutdown竟提前返回；原AutoRecoveryMain在带interrupt关闭后原始ZooKeeper client仍为CONNECTED。修复后的回归保留实际task/marker直到READ终结，覆盖加入等待中的interrupt和并发第二caller、self-stop、借用SDK/pool继续可用与原marker续接；真实Classic AutoRecovery验证metadata client达到CLOSED、signal保留、启动前shutdown与不能重启，并保留startup/shutdown/session loss及原Classic worker验证。测试持有原始ZK引用，避免把driver清空引用误认为client未关闭。

复现：

```bash
bash scripts/unified-wal/run-local-storage-tests.sh 'AutoRecoveryMainTest,SegmentProfileInitialSocketClientTest#replicationWorker*,TestReplicationWorker#testReplicationStats+testReplicateEmptyOpenStateLedger+testRWZKConnectionLostOnNonRecoverableZkError,TestLedgerUnderreplicationManager'
mvn -o -P '!cargo-zigbuild' -pl bookkeeper-server -am -DskipTests spotless:check checkstyle:check -q
python3 scripts/unified-wal/run-storage-models.py --tool "$UW_TLC_JAR" --config-prefix a-profile-service-shutdown --config-prefix ce-profile-service-shutdown --config-prefix a-profile-replication-worker --config-prefix ce-profile-replication-worker --config-prefix a-profile-registered-connection --config-prefix ce-profile-registered-connection
```

有限ProfileServiceShutdown把actual child/thread/SDK close结果作为输入，2正向（外部/self）/8指定反例检查join、并发caller、manager/SDK开始关闭、signal恢复、继承interrupt导致metadata未关闭、自join与shutdown后重启；不生产真实SDK/ZK/网络，不证明clock/liveness/remote abort。当前仅修复服务基础关闭，不新增Profile bootstrap、发现、pool或强reset；步骤14和生产Gate仍PARTIAL/OPEN。普通Add无新增远端metadata、handshake或持久化操作；Linux NUMA、掉电与正式性能本轮NOT_EXECUTED。

实际RED：`unified-wal-local-cgrZmhTm` exit1，预interrupt caller提前返回，原task/READ尚未drain；`unified-wal-local-vjgSGz5V` exit1，原始ZK client expected CLOSED/actual CONNECTED，保留additional-source-lock.json锁定当时AutoRecoveryMain及测试（早期runner尚未包含这两个路径）。随后runner补齐这两个既有文件的source lock。中间qmVwCqi2为测试在driver已清空引用后访问null的错误，不作为实现通过或协议反例。

最终Java `unified-wal-local-cvMZ8z32` exit0：4类39项全部通过、零skip（AutoRecoveryMain5、Classic worker3、URmanager24、actual Profile worker7），1054当前inputs/7 artifact checksums。style/RAT `shutdown-checks-kyrqrOuu` exit0/exit1，443 unapproved路径全在父HEAD，无新增失败路径，5 checksums；不改许可规则、不计CI全绿。模型相关pkrb6t4a 35配置5完整正向/30指定反例、850 inputs/680 checksums，在最后日志换行前；最终akh085ut 10配置2正向/8反例、850当前inputs/633 checksums。外部正向39 generated/24 distinct，self78/38，queue0、depth11，分别0.852/0.833秒、peak RSS108003328/107429888 bytes，是有限模型消耗而非性能。tool1.7.4、单worker、heap512/direct64MiB、seed1/fp0、无symmetry/state constraint、CHECK_DEADLOCK FALSE；完整534及完整A+C+D未全跑。

最终Java/model共享234/去重1670 inputs及五个父HEAD designInputs全部核验；历史绑定不改。本轮RED、Java/model和checks在`/Users/liusinan/.codex/artifacts/unified-wal/service-shutdown-evidence-2p9jrbu9`及archive.json列出的checks目录有持久副本，保留原run身份。

## 注册 cohort peer pool 与实际 worker lease

2026-10-11，第14步新增ProfileRecoveryPeerPool，直接复用consumer观察和connectRegistered两种真实client。配置给定唯一BookieId cohort（1..64），不枚举无界registry children；每次实际task重新读取READY/ephemeral事实并建立一份固定CONTROL/DATA连接快照。没有额外线程、watch、自动重试、第二工作项日志或普通Add的逐entry metadata/handshake；group及有界cold owner仍借用。

pool支持一个正在建立／借用／排空的snapshot。Limits显式约束总connection reservation（最多128）、总RPC slot reservation（最多4096）、每connection slot（最多32）、entry bound及连接/RPC timeout。开始cold发现前为整个cohort保守预留2*N connections和2*N*perConnectionSlots；不可用peer的两个连接都实际close/drain后才继续，最后所有cold和connection实际terminal前不释放batch。usage报告reserved capacity，不把“future已失败”“map为空”或“开始close”当可复用容量。新acquisition在旧snapshot存活时明确拒绝；这一串行策略让旧、connecting、borrowed、draining工作共同处于一个预算，不能超额叠加。

这些是pool connection/RPC-slot上界，未宣称完成TLS/kernel/allocator/SDK全链路字节预算或可持续性能验收。cohort固定，注册状态、地址与同incarnation的READY/publisher episode动态刷新；已成功验证的BookieId/incarnation在本pool生命周期内绑定，变化时明确拒绝，不自动same-ID rejoin。coordinator的持久install/activation/repair历史仍是ledger权限与目标身份依据，重新创建pool不绕过该合同。

Acquisition持有实际cold/connection孩子，caller cancellation只停止未来工作，closeAsync还等实际收尾；Lease只由pool生成，冻结data/control maps，并由实际attempt的owner释放。pool.closeAsync停止新snapshot，对已交付lease继续等待owner释放，不强制切换其peer、撤掉正在执行的I/O或提前归还容量。Lease释放后getter拒绝使用；group/executor必须保持活到pool实际close完成。

原ReplicationWorker增加显式pool constructor，要求WorkerOptions不同时传入平行static maps。先在原UR task/lock下获取lease，实际acquisition失败或shutdown时仍等其cold/connection drain，再release原task；获取期间收到interrupt后不启动新的恢复阶段。成功后复用原beginWorker/recovery/COPY/固定intent与全W健康；coordinator在完整health proof之后、guarded marker transaction之前先close lease并等待本地实际output/source/cold终结，再fresh metadata cut和原task条件化清理。失败路径也由worker实际收尾pool后release原receipt。原static constructor、Classic routing和默认服务OFF保留，worker不拥有借用pool/SDK。

真实TLS/ZK/native files验证：pool关闭期间lease仍可普通Add/点读，第二acquisition被拒绝且capacity保持；worker从registry动态取得三个可连接节点与一个实际listener不可连接节点，完成真实新target COPY/成员修复和marker清理；实际READ reply已到但client write promise被持有时，6条connection reservation、原task和marker都保留，终结后才清理；cold发现排队期间shutdown保持task/capacity，实际任务终结后归还；同incarnation的新READY episode重新连接、不同incarnation拒绝；不满足总slot预算的配置在发起工作前拒绝。

有限ProfileRecoveryPeerPool投影1正向/8指定反例覆盖reservation上界、cold/transport terminal、原task、borrowed lease、验证事实、incarnation与actual child；registry/HELLO/terminal是输入，不生产TLS/ZK/fsync、byte budget、clock/liveness或remote abort。初次语法错误为INCONCLUSIVE；随后缺少禁止重复交付的模型guard导致正向反例，修正为与实际一次性acquisition一致，不改变Java合同来迁就模型。

复现：

```bash
bash scripts/unified-wal/run-local-storage-tests.sh 'SegmentProfileInitialSocketClientTest#registeredPeerPool*+registeredPool*+replicationWorkerConsumesRegisteredPoolAndRepairsAnActuallyUnavailablePeer'
python3 scripts/unified-wal/run-storage-models.py --tool "$UW_TLC_JAR" --config-prefix a-profile-recovery-peer-pool --config-prefix ce-profile-recovery-peer-pool --config-prefix a-profile-service-shutdown --config-prefix ce-profile-service-shutdown --config-prefix a-profile-registered-connection --config-prefix ce-profile-registered-connection --config-prefix a-profile-replication-worker --config-prefix ce-profile-replication-worker --config-prefix a-profile-peer-discovery --config-prefix ce-profile-peer-discovery
```

这是显式、配置cohort的真实registered pool/worker消费路径，未新增AutoRecoveryMain的Profile bootstrap或默认listener。全registry发现、全内存字节预算、same-ID rejoin/离线catch-up/delete/GC、完整组合/兼容/长期性能与生产Gate仍OPEN；步骤12/14保持PARTIAL。Linux NUMA/掉电/正式性能本轮NOT_EXECUTED，模型PASS不替代实际内存/网络/存储证据。

Java广度 `unified-wal-local-6IExbUWK` exit0：10类256项零skip（socket98、lifecycle46、metadata19、control12、readiness13、ephemeral17、UR24、Classic worker3、AutoRecovery5、PendingAddOp19），1055冻结inputs/13 artifact checksums；之后只强化held-output用例为实际等待channel closeFuture，确保已到health/lease drain barrier。最终 `unified-wal-local-RaoO9oaA` exit0：5项真实pool/worker定向PASS，1055当前inputs/4 checksums。style/RAT peer-pool-checks-FPIBoGFU exit0/exit1，443 unapproved全在父HEAD无新增，5 checksums；最终强化测试后的peer-pool-final-style-MGXcg1sN exit0/2 checksums。不改许可规则、不称CI全绿。

相关模型 `unified-wal-model-o_634iuy`51配置7完整正向/44指定反例、861 inputs/720 checksums；最终强化测试后的`unified-wal-model-6n5ycbyg`9配置1完整正向/8反例、861当前inputs/642 checksums。pool正向64 generated/50 distinct、queue0、depth11、0.800秒/peak RSS107315200 bytes（广度run），非性能结论。tool1.7.4、单worker、heap512/direct64MiB、seed1/fp0、无symmetry/state constraint、CHECK_DEADLOCK FALSE；完整543与完整组合未接受。Java/model共享235/去重1681当前inputs与五个父HEAD设计锁全部核验，历史run不重绑。

本轮回执及模型调试记录有持久副本`/Users/liusinan/.codex/artifacts/unified-wal/peer-pool-evidence-5nkh13yg`，archive.json列出checks目录，保留原run metadata/command/source身份。初轮srheoptl因TLA precedence语法错误为INCONCLUSIVE，cgxvj4no缺一次性deliver guard导致模型正向失败；两者都保留，不计为Java协议RED或最终PASS。

## 显式 AutoRecovery service bootstrap 与 owned 资源

2026-10-11，第14步在原AutoRecoveryMain/AutoRecoveryService增加programmatic ProfileSettings入口，默认构造器继续OFF。settings给定原reservation/readiness namespace及ACL、client TLS、固定cohort、peer limits与空static-map WorkerOptions，复用现有descriptor validator/cold authority/pool/recovery；不新增配置文件格式、RFC、模块或平行控制框架。mutable ACL在输入/输出深拷贝，descriptor仍由既有WorkerOptions拥有。

显式分支创建一个owned NIO group和1线程/64槽ArrayBlockingQueue+AbortPolicy cold owner。SDK通过原BookKeeper builder创建SYSTEM role，明确v3 wire/explicitLacInterval=0，借用该NIO group。使用该SDK的ZK metadata driver和factory新建native AbstractZkLedgerManager，再创建原ProfileLedgerMetadataStore、reservation/registration adapters、coordinator及registered pool；SDK自己的CleanupLedgerManager仍由SDK拥有，额外native manager由ProfileResources拥有，避免testing-only unwrap或混入外部ZK session。AuditorElector与ReplicationWorker保留原服务线程/factory/stat/marker路径，worker显式借用该coordinator/pool。Session-expiry listener在全部字段初始化后安装。

当前不实现Classic payload/replica/placement audit对Profile的读取合同，所以显式配置必须把这三种periodic interval设为0，非0在分配资源前拒绝，不暗中修改caller配置。原metadata bookie-membership auditor/election仍保留；它使用Classic available registry，完整Profile registration-aware availability与periodic audit producer尚未接通，不能把bootstrap或worker成功当作这部分验收。native Profile操作仍由既有mTLS/HELLO/授权检查，默认SDK Profile open和legacy mutation/ACL验证仍按既有未闭合范围处理。

成功关闭按原auditor/worker实际终结 → pool实际drain → owned native metadata manager close → SDK close → cold shutdown/实际termination → group shutdown/实际termination顺序进行，继承或等待期间的interrupt记账后在procedure结束恢复；不先shutdownNow丢掉queued child。原AutoRecoveryService的stop仍采用原no-op语义，实际销毁由close发起。本入口不支持关停后重启。初始化失败按已创建资源收尾，SDK close错误附加到原异常；本轮验证SDK/pool绑定后auditor identity失败会回收新建transport owner。关闭异常不作为资源无条件成功终结的证据。runtime有固定profile-autorecovery/profile-autorecovery-cold线程名用于运维区分，不含credentials或endpoint。

真实TLS/ZK/native files测试使用服务自身不同SDK/ZK session和owned group/cold：实际不可连接的成员通过registered pool新target COPY/membership修复并清marker；close后owned group/cold terminated、外部SDK继续可用；持有三条真实READ回复期间预interrupt shutdown caller，原task/owned SDK/group/cold保持到实际lease结束，随后原始owned ZK client CLOSED、source容量归零、signal保留、marker继续待处理；故意在SDK/pool绑定后auditor identity失败时新建named owner线程全部终结。unsupported audit组合在resource allocation前拒绝。

有限ProfileServiceBootstrap投影使用SDK/ZK context、supported audit scope、ready和actual worker/pool/SDK终结作为输入，1正向/7指定反例检查default OFF、绑定context、audit限制、实际worker工作开始时peer ready输入、pool前不关SDK、SDK前不关owners和失败cleanup。它不生产SDK/TLS/ZK/fsync、allocator/kernel byte bounds、clock/liveness或remote abort。

复现：

```bash
bash scripts/unified-wal/run-local-storage-tests.sh 'AutoRecoveryMainTest,SegmentProfileInitialSocketClientTest#profileAutoRecoveryService*+profileServiceShutdown*+profileBootstrapInitialization*+registeredPeerPool*+registeredPool*+replicationWorker*,TestReplicationWorker#testReplicationStats+testReplicateEmptyOpenStateLedger+testRWZKConnectionLostOnNonRecoverableZkError,TestLedgerUnderreplicationManager'
python3 scripts/unified-wal/run-storage-models.py --tool "$UW_TLC_JAR" --config-prefix a-profile-service-bootstrap --config-prefix ce-profile-service-bootstrap --config-prefix a-profile-service-shutdown --config-prefix ce-profile-service-shutdown --config-prefix a-profile-recovery-peer-pool --config-prefix ce-profile-recovery-peer-pool --config-prefix a-profile-registered-connection --config-prefix ce-profile-registered-connection --config-prefix a-profile-replication-worker --config-prefix ce-profile-replication-worker
```

最终Java oNtOEDZW exit0：4类47项零skip（AutoRecovery5、URmanager24、Classic worker3、actual socket/service/pool/worker15），1056当前inputs/7 checksums。style/RAT bootstrap-final-checks-fHuELJNn exit0/exit1，443 unapproved路径全部在父HEAD、没有新增，5 checksums，不计CI全绿。相关模型54v7k04l 52配置7完整正向/45指定反例、871 inputs/731 checksums，最后import/长行格式前；最终jensczj9 8配置1正向/7反例、871当前inputs/649 checksums。正向269 generated/227 distinct、queue0、depth10、0.849秒/peak RSS109248512 bytes（最终run），非性能结论。tool1.7.4、单worker、heap512/direct64MiB、seed1/fp0、无symmetry/state constraint、CHECK_DEADLOCK FALSE。Java/model共享236/去重1691 inputs及五个父HEAD设计锁核验，完整551与完整组合未接受；历史run身份不改。

回执持久副本在`/Users/liusinan/.codex/artifacts/unified-wal/service-bootstrap-evidence-3lge8fhj`及archive.json checks目录。前期xWRb0KuS为unused KeeperException catch编译错误；PiwVUdy1为SDK CleanupLedgerManager不属native manager的拒绝，改用factory创建并拥有native manager；iAXpkSt0为fixture禁止loopback的配置失败，测试显式允许loopback、生产默认不改；bootstrap-checks-J1iaTeSP为import/长行checkstyle失败，已修正。这些不作为协议RED。

当前已接显式配置cohort的owned服务入口，仍无默认CLI启用或生产Bookie listener/startup接管。registration-aware Profile auditor、全registry与全内存字节预算、same-ID rejoin/离线catch-up/delete/GC、完整兼容/组合/正式持续性能/运维及生产Gate继续OPEN，步骤12/14保持PARTIAL；Linux NUMA/掉电/正式性能本轮NOT_EXECUTED。实际canary/生产启用仍须环境及相应授权。

## SDK 关闭过程中的新中断

2026-10-11，规划检查指出cf9459b838入口的一个实际窗口：BookKeeper.close先设置closed准入位，之后才在scheduler/mainWorkerPool等处等待并最终close metadata driver；新interrupt发生在等待中会跳出，Main原catch继续回收cold/group并结束procedure。再次SDK close因closed=true直接return，不能修复滞留session。前一轮“预中断+held READ”的PASS只覆盖进SDK等待前取出signal，不覆盖这个窗口；历史回执不重绑。

本轮在原Main内增加一次性的autorecovery-sdk-close线程，既用于runtime shutdown，也用于初始化失败收尾。它只执行原SDK close，不引入新SDK API或普通Add开销。原唯一shutdown owner等待该线程实际终结，任何caller interrupt只记账、继续join，最后恢复；SDK异常传回caller，不把准入closed位当作完成，也不重试不可续接的close。每个Main的这一关闭阶段至多一个closer，不创建常驻调度器或重试队列；cold/group仍在SDK procedure返回之后收尾。异常返回不证明所有资源无条件关闭成功，原SDK的其他timeout/error语义未被升级成新保证。

新增真实回归把服务自身SDK scheduler任务持有，再等closed位已设置/真实owned ZK仍CONNECTED时中断shutdown caller。修复前Gxr9qFjq exit1，1项实际FAIL，shutdown在scheduler未终结时返回；修复后j4fuCHGa exit0，1PASS，caller保持等待且cold/group未shutdown，释放任务后owned ZK CLOSED、cold/group terminated、signal保留、external SDK继续可用。RED finally显式清理滞留driver，不让测试遗留session；最终状态断言在这段清理前执行。

ProfileServiceBootstrap原模型补入sdkClosing、caller signal、actual SDK return输入及signal restore，区分准入关闭与procedure结束；新增两个指定反例SDKCompletionNeedsActualReturn/ReturnPreservesSignal。仍是有限投影，不生产SDK/TLS/ZK/fsync事实、内存字节证据或被永久阻塞child的liveness。

复现：

```bash
bash scripts/unified-wal/run-local-storage-tests.sh 'AutoRecoveryMainTest,SegmentProfileInitialSocketClientTest#profileAutoRecoveryService*+profileServiceShutdown*+profileBootstrapInitialization*+profileShutdownInterruptDuringSdkClose*+registeredPeerPool*+registeredPool*+replicationWorker*,TestReplicationWorker#testReplicationStats+testReplicateEmptyOpenStateLedger+testRWZKConnectionLostOnNonRecoverableZkError,TestLedgerUnderreplicationManager'
python3 scripts/unified-wal/run-storage-models.py --tool "$UW_TLC_JAR" --config-prefix a-profile-service-bootstrap --config-prefix ce-profile-service-bootstrap --config-prefix a-profile-service-shutdown --config-prefix ce-profile-service-shutdown --config-prefix a-profile-recovery-peer-pool --config-prefix ce-profile-recovery-peer-pool --config-prefix a-profile-registered-connection --config-prefix ce-profile-registered-connection --config-prefix a-profile-replication-worker --config-prefix ce-profile-replication-worker
mvn -o -P '!cargo-zigbuild' -pl bookkeeper-server -am -DskipTests spotless:check checkstyle:check -q
```

最终Java bCRQ9XaW exit0：4类48项零skip（AutoRecovery5、URmanager24、Classic worker3、actual service/socket/pool/worker16），1056当前inputs/7 artifact checksums。sdk-close-checks-oc10vnwf的style exit0、RAT exit1，443 unapproved路径全在父HEAD，无新增，5 checksums；不计CI全绿。模型wk9v00r_ 10配置1完整正向/9指定反例、873inputs/655 checksums；相关hw_gy2l6 54配置7完整正向/47指定反例、873inputs/737 checksums。bootstrap正向499 generated/407 distinct、queue0、depth12、0.852秒/peak RSS110755840 bytes（相关run），是有限模型消耗而非性能结论；tool1.7.4/单worker/heap512/direct64MiB/seed1/fp0/无symmetry/state constraint/CHECK_DEADLOCK FALSE。Java/model共享236/去重1693当前inputs与五个父HEAD设计锁核验。完整catalog553/完整A+C+D组合未运行；Linux NUMA/掉电/正式性能NOT_EXECUTED，步骤12/14仍PARTIAL，生产Gate不提升。

持久回执在`/Users/liusinan/.codex/artifacts/unified-wal/sdk-close-evidence-juy8ki3e`，archive.json指向checks目录，原source/config/run/command身份不改。原auditor的初始集合、变化监听与membership周期扫描仍使用Classic registry；Profile/cohort/descriptor/instance/incarnation感知的故障发现、全字节预算、离线追赶/rejoin/delete/GC及综合验收继续OPEN。registration observation不替代DATA missing证明或repair授权。

## Auditor 排空与 owned SDK 关闭

2026-10-11，接入Profile故障发现前补齐另一个实际service生命周期窗口：原Auditor.shutdown在executor.awaitTermination被interrupt后直接catch/return，elector随后丢掉auditor引用，Main进入worker/SDK阶段；实际audit任务和它的metadata依赖可能尚未终结。SDK one-shot closer的上一轮证据不覆盖这个更早的audit阶段。

Auditor现在先取出caller signal，等待实际executor终结；等待中的新interrupt只记账并继续。原30秒后尝试shutdownNow的策略保留，仍须等actual termination才close各task/admin/manager并恢复signal；不把executor.isShutdown当成结束。Auditor及AuditorElector的ownBkc分支复用Main已有的独立one-shot SDK close方法（package内部共享），无需另一套close框架；Profile服务的auditor/elector仍借用Main的SDK。admin/manager等close错误仍按原错误语义报告，procedure结束不等于资源无条件成功。

真实回归在服务已经选出的auditor自身executor中持有一个实际任务，等shutdown线程已进入Auditor.shutdown的等待后注入interrupt。IUtkcEcf exit1、1实际FAIL：held任务未结束时shutdown已return，后续SDK关闭使滞留auditor工作不可用；测试finally显式回收RED的遗留auditor，不把该清理当验收。E9QccJOb exit0、1PASS：held任务未结束时SDK/owned ZK保持可用，释放后executor terminated、owned ZK CLOSED、signal保留、external SDK继续可用。该场景证明executor与SDK生命周期边界，不是Profile DATA missing或自动故障发现的证据。NkHzqV1O为遗漏admin.close checked InterruptedException的编译失败，无测试执行；修正其原错误处理后继续验证，不计协议RED。

两份runner补入Auditor/AuditorElector/AuditorTask/AuditorBookieCheckTask/BookieLedgerIndexer及本轮选中的AuditorBookieTest源码锁。原ProfileServiceShutdown投影增加actual auditor terminal输入与一个指定反例AuditorNeedsActualDrain，native manager/SDK之前须同时结束实际auditor和worker；不生产这些actual输入、ZK/TLS/fsync或clock/liveness保证。

复现：

```bash
bash scripts/unified-wal/run-local-storage-tests.sh 'AutoRecoveryMainTest,AuditorBookieTest#testShutdown+testSuccessiveAuditorCrashes,SegmentProfileInitialSocketClientTest#profileAutoRecoveryService*+profileServiceShutdown*+profileBootstrapInitialization*+profileShutdownInterruptDuring*+registeredPeerPool*+registeredPool*+replicationWorker*,TestReplicationWorker#testReplicationStats+testReplicateEmptyOpenStateLedger+testRWZKConnectionLostOnNonRecoverableZkError,TestLedgerUnderreplicationManager'
python3 scripts/unified-wal/run-storage-models.py --tool "$UW_TLC_JAR" --config-prefix a-profile-service-bootstrap --config-prefix ce-profile-service-bootstrap --config-prefix a-profile-service-shutdown --config-prefix ce-profile-service-shutdown --config-prefix a-profile-recovery-peer-pool --config-prefix ce-profile-recovery-peer-pool --config-prefix a-profile-registered-connection --config-prefix ce-profile-registered-connection --config-prefix a-profile-replication-worker --config-prefix ce-profile-replication-worker
mvn -o -P '!cargo-zigbuild' -pl bookkeeper-server -am -DskipTests spotless:check checkstyle:check -q
```

最终Java 9g4pj27S exit0：5类51项零skip（URmanager24、Classic auditor shutdown/successive election2、AutoRecovery5、Classic worker3、actual service/socket/pool/worker17），1062当前inputs/8 artifact checksums。auditor-drain-checks-ht2mj80b的style exit0、RAT exit1，443 unapproved路径全在父HEAD、无新增，5 checksums，不计CI全绿。narrow wzcf66vo 11配置2完整正向/9指定反例、879inputs/657 checksums，在最后补入AuditorBookieTest源码锁前；相关750_1r7x 55配置7正向/48反例、880当前inputs/740 checksums。外部shutdown正向52 generated/30 distinct，self91/44，queue0、depth12，分别0.793/0.832秒、peak RSS107593728/106725376 bytes（相关run），非性能结论；tool1.7.4/单worker/heap512/direct64MiB/seed1/fp0/无symmetry/state constraint/CHECK_DEADLOCK FALSE。Java/model共享242/去重1700当前inputs与五个父HEAD design锁核验，历史run不重绑。

持久回执在`/Users/liusinan/.codex/artifacts/unified-wal/auditor-drain-evidence-edhuj_ar`及archive.json checks目录。catalog554/完整A+C+D组合未运行，Linux NUMA/掉电/正式性能NOT_EXECUTED；Profile注册感知的初始集合/变化通知/周期扫描、全字节预算、离线追赶/rejoin/delete/GC及综合验收仍OPEN，步骤12/14仍PARTIAL，生产Gate不提升。

## Profile 成员故障发现与原 auditor 接线

2026-10-11，第14步接通原AuditorElector/Auditor/AuditorBookieCheckTask的显式Profile成员发现。ProfileAuditor直接借用同一服务SDK/ZK context创建的原registration/reservation/metadata adapters，固定cohort（最多64）和WorkerOptions已验证的descriptor。普通构造器仍OFF。显式Profile auditor只检查带backlink且匹配本descriptor/cohort的ledger；Classic auditor使用限定的getClassicBookieToLedgerIndex过滤实验Profile backlink，避免把Classic registry的同名Bookie可用性错误套到Profile ledger；共享公开getBookieToLedgerIndex仍保留所有引用（下节修复）。Classic ledger仍走原index；不另建membership、选举、任务或权限日志。

Profile分支复用原lostBookieRecoveryDelay、多成员故障处理、delay变更和periodicBookieCheck调度，启动known集合来自配置cohort，最终成员权威仍是LedgerMetadata ensembles。初始扫描即使periodic interval=0也照原规则执行一次。新observer顺序消费原LedgerRangeIterator，每次只保留一个metadata/probe与至多64个peer observations，不再构建另一份全ledger索引；原backend range、SDK/ZK/TLS/kernel/allocator的总字节上限仍未完成，此处不声明全内存有界。

watchPeers只在配置Bookie的readiness path安装至多64个persistent recursive watch，不枚举整个registry或动态扩大cohort；事件仅请求原auditor executor上的cold检查，不在ZK事件线程读peer/授予权限。Profile通知在executor中合并为至多一项额外排队任务，实际scan仍串行；同BookieId的READY/incarnation/session变化也会按原delay安排检查。实际auditor排空后retire本observer，再best-effort移除仅属于它的watch；底层session仍由SDK拥有并实际关闭，observer retirement不冒充远端abort/注册删除或磁盘回收。需要支持persistent recursive watch的ZK；安装失败按原启动错误处理，不暗中改用另一发现框架。

逐ledger核对metadata版本/内容/physical birth、backlink/instance、lifecycle/creatorToken/quorum和retained activation/repair目标身份；要求所有历史ensemble成员在cohort内，并把peer incarnation与已保留receipt匹配。对FOUND再cold revalidate，NOT_FOUND再读，最后再检查metadata/root cut。NOT_FOUND或稳定但不匹配的incarnation只生成原underreplication检查任务；UNAVAILABLE/CORRUPT、变化中的cut、缺失/不一致历史及scope不匹配都defer，不折算为空集合或DATA absence。DELETED不产生新任务。observer不写FENCE/INSTALL/grant/CLOSED、membership或clear marker，原worker/coordinator继续拥有实际授权、COPY、完整W健康证明及guarded completion。

真实闭环：启动前删除Profile writable，保留Classic同名available节点且不手工mark；服务自动产生任务、由真实mTLS/native COPY替换到新目标并在全W健康/lease drain后清marker。关闭过程前的健康副本在observer-only启动中不产生任务；interval=0时删除writable也由真实watch产生原task，再启动原worker完成修复。interval=1的真实periodic检查遇到损坏service-info会记录defer、保持ledger OPEN且不产生marker；恢复注册继续扫描，同名incarnation改变会留下检查任务，但metadata仍OPEN且未授予写权。早期Classic43/Profile44同地址验证曾要求公开index只含Classic，后续共享调用方回归揭示该要求范围过宽；当前公开index包含43/44，只有Classic auditor生成43的任务，显式Profile observer不处理Classic ledger。

初轮ZDf95ord exit1是真实RED：Classic同名registry掩盖Profile缺失，目标未替换；sc9PadVn为使用package-private helpers/PageResult字段错误的编译失败（无测试），改为复用discover/profile内原helpers与公开descriptor codec；R5EC4HNQ已自动完成membership但旧等待只看membership、尚未到marker实际排空，保持有效assert并改为同时等待membership和marker。Bv9qYbVj启动GREEN、7N7SfO6i三场景及kusm5Jry（含incarnation）三场景PASS。历史run保持原source/config/command身份，不重绑到后续源码。

复现：

```bash
bash scripts/unified-wal/run-local-storage-tests.sh 'AutoRecoveryMainTest,AuditorBookieTest#testShutdown+testSuccessiveAuditorCrashes,AuditorPeriodicBookieCheckTest,AuditorBookieCheckTaskTest,SegmentProfileInitialSocketClientTest#profileAuditor*+profileAutoRecoveryService*+profileServiceShutdown*+profileBootstrapInitialization*+profileShutdownInterruptDuring*+registeredPeerPool*+registeredPool*+replicationWorker*,TestReplicationWorker#testReplicationStats+testReplicateEmptyOpenStateLedger+testRWZKConnectionLostOnNonRecoverableZkError,TestLedgerUnderreplicationManager'
python3 scripts/unified-wal/run-storage-models.py --tool "$UW_TLC_JAR" --config-prefix a-profile-membership-audit --config-prefix ce-profile-membership-audit --config-prefix a-profile-service-bootstrap --config-prefix ce-profile-service-bootstrap --config-prefix a-profile-service-shutdown --config-prefix ce-profile-service-shutdown --config-prefix a-profile-recovery-peer-pool --config-prefix ce-profile-recovery-peer-pool --config-prefix a-profile-registered-connection --config-prefix ce-profile-registered-connection --config-prefix a-profile-replication-worker --config-prefix ce-profile-replication-worker --config-prefix a-profile-peer-discovery --config-prefix ce-profile-peer-discovery
mvn -o -P '!cargo-zigbuild' -pl bookkeeper-server -am -DskipTests spotless:check checkstyle:check -q
```

ProfileMembershipAudit有限投影1完整正向/9指定反例覆盖Classic遮蔽与错误路由、scope/cut/freshness、UNKNOWN/同名incarnation、marker只是调度、通知合并。输入facts不由模型生产，模型不证明实际ZK watch/decoder/mTLS/fsync、内存全预算、clock/liveness或完整组合。

当前已经是显式、固定descriptor/cohort的Profile成员故障发现与原worker闭环。周期DATA/replica/placement审计仍明确拒绝非0配置；全registry/总字节预算、普通writer closed adoption/required authority producer、离线追赶/rejoin/delete/GC、完整兼容/组合/长期正式性能及运维/生产验收仍OPEN，步骤12/14仍PARTIAL。Linux NUMA/掉电/正式性能本轮NOT_EXECUTED。

广度Java cvxA27kV exit0：7类57项零skip（Classic periodic1/task2/UR24/auditor2/Main5/worker3、native service/pool/worker20），1066 inputs/10 artifacts；之后只修正checkstyle指出测试行5683的121字符换行，未弱化断言或改变代码语义。最终JRKGj99g exit0：3项真实成员发现/修复/UNKNOWN/incarnation全部PASS，1066当前inputs/4 artifacts。相关模型_b04wuab 72配置9完整正向/63指定反例，895inputs/783checksums，在最后测试换行前；最终_x63yc0m 10配置1正向/9反例，895当前inputs/667checksums。正向1792 generated/768 distinct、queue0、depth3、0.870秒/peak RSS112410624 bytes（最终run），非性能结论；tool1.7.4、单worker、heap512/direct64MiB、seed1/fp0、无symmetry/state constraint、CHECK_DEADLOCK FALSE。Java/model共享246/去重1715当前inputs与五个父HEAD设计锁核验，catalog564及完整A+C+D未全跑。

本轮历史及最终回执持久副本在`/Users/liusinan/.codex/artifacts/unified-wal/membership-audit-evidence-r9nyiz9e`；archive.json列出初次风格失败及最终checks目录，source/config/run/command身份不改。

最终membership-audit-final-checks-ppc__8jf：style exit0/RAT exit1，443 unapproved路径全部在父HEAD、无新增，5 artifact checksums；初次n1lqyxuv只失败于测试行长121，2 checksums保留。未改许可规则、不称CI全绿。

## 共享引用索引与 legacy 退役拒绝

2026-10-11，规划检查发现35a86ed03f的共享调用方回归：BookKeeperAdmin.decommissionBookie也消费公开getBookieToLedgerIndex。全局跳过Profile ledger后，目标仍在Profile ensemble内且无UR marker（例如注册损坏被正确defer）时，legacy退役可能把缺失索引当迁移完成，正常返回。x8MKPFtt是真实RED：原auditor已选出、实际Profile metadata44仍引用socket2、无marker，legacy方法等待原30秒后成功返回，assertThrows失败。

恢复公开索引的完整引用语义，只由Classic AuditorBookieCheckTask调用package内部的getClassicBookieToLedgerIndex。两条路径复用同一个原metadata walker，不建第二索引框架；key presence决定Profile路由，损坏backlink不降级成Classic。BookKeeperAdmin对known ensemble引用在triggerAudit前作fresh metadata读取，遇到Profile tag明确IllegalOpException；原30秒后的fresh index及UR marker引用再次执行同一检查。metadata read失败沿原错误路径退出，不当不存在；实际已删除ledger可按原NoSuch语义忽略。未创建repair权限、迁移或删除数据，不能把这项拒绝当作Profile decommission实现。

真实验证：共享公开index保留Classic43/Profile44；真正Classic auditor在其cookie目录初始化后只为43产生UR task，44仍无marker。legacy拒绝Profile引用后metadata ensemble/marker及外部SDK保留。原Profile startup/watch/periodic UNKNOWN/incarnation行为保留。JQVwW3nA定向4PASS；2VvQio9R的Classic auditor因借用legacy ZK的cookie目录/ledgers/cookies未建而提前退出，不作为协议RED；补齐该真实目录后ZFUmtrVm的3类10项全部PASS，无mock成功代替真实索引/审计。

本修复只覆盖标准ensemble和UR marker中可见的Profile引用。retained activation、未发布/已admit RepairIntent、离线数据与删除/GC obligations的完整退役协议尚未实现；legacy API不是Profile物理安全退役入口。Profile bookie实际退役/擦盘仍不得启用，需步骤15/16完整合同和实际环境授权。Classic cold admin命令新增preflight读取，不宣称性能收益；普通Add无此查询。

ProfileMembershipAudit补入共享admin index与legacy退役输入/2指定反例，要求公开查询保留引用、legacy不以已知Profile引用结束成功；模型不生产实际metadata、retained-history cleanup或磁盘回收证明。

复现：

```bash
bash scripts/unified-wal/run-local-storage-tests.sh 'SegmentProfileInitialSocketClientTest#sharedIndexRetainsAllReferencesWhileClassicAuditorOnlyMarksClassicLedgers+legacyDecommissionRejectsStillReferencedProfileWithoutMarker+profileAuditor*,AuditorBookieCheckTaskTest,BookieLedgerIndexTest,BookieDecommissionTest#testDecommissionBookie+testDecommissionForEmptyLedgers'
python3 scripts/unified-wal/run-storage-models.py --tool "$UW_TLC_JAR" --config-prefix a-profile-membership-audit --config-prefix ce-profile-membership-audit
```

两份runner补入共享BookKeeperAdmin及实际BookieLedgerIndexTest/BookieDecommissionTest输入；历史source/design/receipt绑定不改。完整Profile删除/rejoin/catch-up/GC、总字节预算、周期DATA审计、完整兼容/组合/正式性能与生产Gate仍OPEN，步骤12/14/15保持原未完成范围。Linux NUMA/掉电/正式性能本轮NOT_EXECUTED。

广度JblH1wJd exit0：9类64项零skip（Classic periodic1/task2/index3/UR24/auditor2/Main5/worker3/decommission普通+empty2、native22），1067 inputs/12 artifact checksums。之后仅把admin行1717和测试行5591长行换行，语义和有效断言不变；最终mxzr0tOQ exit0，3类10项全部PASS、1067当前inputs/6 checksums。相关模型r8hp_jno 74配置9完整正向/65指定反例、900inputs/789 checksums，在最后两处换行前；最终_6x_m2x_ 12配置1正向/11反例、900当前inputs/673 checksums。正向5440 generated/1920 distinct、queue0、depth5、1.040秒/peak RSS121552896 bytes（最终run），是有限模型消耗而非性能；tool1.7.4/单worker/heap512/direct64MiB/seed1/fp0/无symmetry/state constraint/CHECK_DEADLOCK FALSE。Java/model共享249/去重1718当前inputs与五个父HEAD设计锁核验，catalog566/完整组合未全跑。

持久回执在`/Users/liusinan/.codex/artifacts/unified-wal/decommission-boundary-evidence-ydhv573w`，archive.json指向checks，原source/config/run/command身份冻结不改。

最终decommission-boundary-checks-0i0hf8nq的style exit0、RAT exit1，443 unapproved路径全在父HEAD，无新增，5 artifact checksums。保留既有许可规则，不计CI全绿。

## Profile CLOSED DATA 周期审计

2026-10-11，第14步把原AuditorReplicasCheckTask的periodic调度接到既有真实全W replica-health验证器。原Classic replica-check只分析CLOSED ledger；本分支沿用该范围，且包括匹配的空CLOSED/empty-last事实，不为了审计主动fence OPEN writer。默认构造器继续OFF；显式Profile设置允许非负auditorPeriodicReplicasCheckInterval，payload check/placement check非0继续拒绝。没有新的调度器、元数据日志、普通Add查询或另一套quorum算法。

ProfileAuditor复用原streamed metadata scan，先核验descriptor/cohort/backlink/lifecycle/retained incarnation与fresh registration，只有匹配CLOSED继续DATA检查。直接借用同一Main SDK/context/coordinator与原registered peer pool，使用原WorkerOptions的coordinate/duration界限。pool为一个正在建立/借用/排空的snapshot，audit与worker争用同一connection/RPC-slot预算；busy或失败defer，不创建第二pool或扩大预算。大于配置coordinate界限、未匹配CLOSED authority或错误/变化中的状态仍defer，不计完整审计或data loss；全registry/range/SDK/TLS/kernel/allocator总字节证明未完成。

原verifyClosedReplicas保留caller deadline/cancellation回复；新增同一coordinator内的ReplicaHealthAttempt.completion只在actual health child终结、permit归还后完成，stop只停止未来阶段。审计等actual completion，再close acquisition并等待实际cold/client/output/lease drain，最后再检查metadata physical birth/version/content与root cut，之后才允许生成原UR检查任务。caller timeout/interrupt不是排空，服务SDK/cold/group保持到原auditor实际结束。没有remote abort/NONE/FREE或坐标回滚。

实际READ证据继续由原fenced matching CLOSED checker逐坐标检查所有write-set副本的digest/payload/累计length；MISSING/CORRUPT且final cut稳定才生成需检查的replica名字。UNAVAILABLE、权限拒绝、冲突、quarantine、预算终止或metadata/root改变只报告defer。FULLY_REPLICATED只计健康观察，不clear已有marker、不发布repair/normal权限或loss reset；原worker仍执行durable intent/grant/COPY/membership/terminal/完整W复核及guarded marker completion。replicasCheckCTime只记录本轮执行时间，不代表所有ledger已健康。

真实闭环：五节点fixture保留历史fragment在socket2的实际DATA hole，所有publisher仍READY；periodic检查无手工mark生成socket2检查任务，原worker复制到新target4、发布历史membership、完整W复核和actual lease drain后清marker，原CLOSED P=3/L=64保持。健康CLOSED扫描及随后权限拒绝均保留原marker；故意持有三条真实READ回复超过200ms caller deadline后，audit仍保留6条connection reservation，pre-interrupted service shutdown等待actual health/lease，owned SDK/ZK持续可用，释放后才ZK CLOSED/容量归零。验证的是本地真实mTLS/ZK/files及生命周期，不是掉电/持续性能证据。

uuFQWjdD为旧服务明确拒绝replica interval非0的能力缺口（无协议成功）；biqUR5uJ闭环GREEN1。dbZEo2tC另两场景未启用fixture recovery-read capability，真实checker按合同拒绝，补齐该既有能力后arkobyXa三场景全部PASS，未弱化生产校验。两份runner补入实际AuditorReplicasCheckTask及其Classic原测试源码，历史source/config/run/command不重绑。

复现：

```bash
bash scripts/unified-wal/run-local-storage-tests.sh 'AutoRecoveryMainTest,AuditorBookieTest#testShutdown+testSuccessiveAuditorCrashes,AuditorPeriodicBookieCheckTest,AuditorBookieCheckTaskTest,AuditorReplicasCheckTaskTest,BookieLedgerIndexTest,SegmentProfileInitialSocketClientTest#profileReplicaAudit*+profilePeriodicReplicaAudit*+closedReplicaHealth*+sharedIndexRetainsAllReferencesWhileClassicAuditorOnlyMarksClassicLedgers+legacyDecommissionRejectsStillReferencedProfileWithoutMarker+profileAuditor*+profileAutoRecoveryService*+profileServiceShutdown*+profileBootstrapInitialization*+profileShutdownInterruptDuring*+registeredPeerPool*+registeredPool*+replicationWorker*,TestReplicationWorker#testReplicationStats+testReplicateEmptyOpenStateLedger+testRWZKConnectionLostOnNonRecoverableZkError,TestLedgerUnderreplicationManager'
python3 scripts/unified-wal/run-storage-models.py --tool "$UW_TLC_JAR" --config-prefix a-profile-replica-audit --config-prefix ce-profile-replica-audit --config-prefix a-profile-membership-audit --config-prefix ce-profile-membership-audit --config-prefix a-profile-replica-health --config-prefix ce-profile-replica-health --config-prefix a-profile-service-bootstrap --config-prefix ce-profile-service-bootstrap --config-prefix a-profile-service-shutdown --config-prefix ce-profile-service-shutdown --config-prefix a-profile-recovery-peer-pool --config-prefix ce-profile-recovery-peer-pool --config-prefix a-profile-registered-connection --config-prefix ce-profile-registered-connection --config-prefix a-profile-replication-worker --config-prefix ce-profile-replication-worker --config-prefix a-profile-peer-discovery --config-prefix ce-profile-peer-discovery
```

ProfileReplicaAudit有限投影1正向/9指定反例要求CLOSED与scope、rich UNKNOWN不计hole、健康观察不clear、actual health/lease先于容量和SDK关闭、shared reservation不复制与最终fresh cut；facts仍为输入，不由模型生产TLS/ZK/fsync、完整状态/时钟/liveness或总字节界限。

当前CLOSED DATA审计已接原coordinator/worker闭环，OPEN audit/正常writer close与adoption/额外required authority producer、payload/placement audit、总预算、离线追赶/rejoin/delete/retained-history GC/完整退役、完整兼容与组合及正式性能/运维/生产Gate仍OPEN；步骤12/14/15保持未完成范围。Linux NUMA/掉电/正式性能NOT_EXECUTED。

最终kCot2Spa exit0：9类68项零skip（Classic periodic1/task2/index3/UR24/replica-task1/auditor2/Main5/worker3，native27含原health cancellation/authority与新增周期审计），1069当前inputs/12 artifact checksums；本轮最后长行已在该run前修正。narrow abiye6_8 10配置1完整正向/9指定反例、912inputs/680 checksums，在最后test input锁/换行前；相关o_pfyzrv 94配置11正向/83反例、913当前inputs/838 checksums。新正向396 generated/336 distinct、queue0、depth7、0.859秒/peak RSS110542848 bytes（相关run），非性能结论；tool1.7.4/单worker/heap512/direct64MiB/seed1/fp0/无symmetry/state constraint/CHECK_DEADLOCK FALSE。Java/model共享251/去重1731当前inputs与五个父HEAD设计锁核验，catalog576/完整A+C+D组合未全跑。

持久回执在`/Users/liusinan/.codex/artifacts/unified-wal/replica-audit-evidence-si79s4ka`及archive.json列出的checks目录，旧source/design/config/run/command身份冻结不改。

最终replica-audit-checks-exixx7_6：style exit0、RAT exit1，443 unapproved路径全在父HEAD，无新增，5 artifact checksums。不改许可规则、不称CI全绿。


## SDK writer stop 与实际 DATA 排空

2026-10-11，续接普通 writer close 前先修复必要的本地排空边界。原 ProfileBookieClient.closeCompletion 只等待 SDK delivery callback，A 个成功、逻辑 deadline 或停止 delivery 后，socket 仍可能持有排队源副本或已编码输出/write promise；不能由该旧 completion 推导 writer DATA 已排空。

ProfileNormalDataClient.beginNormalAdd 返回独立的 result 与 actual completion；沿用 ProfileSocketClient 原 writeDone/wireDone/released 判定，在源借用已归还、输出已清零/release、wire 已终结及原 RPC slot 归还后才完成 actual completion。普通 addNormal/addRecovery 的 result/status、wire 与存储格式不变。SDK 仅跟踪自己发出的普通 Add attempts，逻辑 delivery 完成不移除 actual custody；stop 冻结新发送，先结束所有 delivery，再等待已借用 attempts 的实际终态。发送登记与 stop 之间的局部锁也覆盖原 executor 拒绝后的同步 close fallback。caller cancellation 只取消等待副本，不污染共享 completion。

保留借用 peer/socket，未增加线程、远端控制查询、逐 entry fsync 或 handshake。完成仅证明本 handle 的本地普通 DATA source/output/RPC custody 已终结；其他使用者的请求不属于这个 barrier。断连/timeout 的 actual wire terminal 仍不证明服务端 I/O 已取消、NONE、FREE 或 durable CLOSED，UNKNOWN 观察不清除。读取、LAC、replacement 控制操作及远端持久化另由各自合同约束；普通 writer 冻结 P/length 与 durable terminal/metadata close 尚未接通。

真实回归 writerStopWaitsForActualDataWritePromiseAfterSdkQuorum 使用真实 mTLS/ZK/files/原 SDK，达到 A 后保留实际 DATA output promise，stop 必须持续等待；取消第一次 caller future 后，第二次 stop 仍待 actual，释放 promise 后输出才清零、借用连接仍可同坐标重试，metadata 保持 OPEN。writerStopWaitsForQueuedDataBorrowAndFailsPendingAppend 在原单 event loop 上暂停未编码 DATA，pending append 失败并调整已确认 length 后，stop 仍等待排队借用完成。原 sdkDeadlineKeepsUnknownRemoteIoAndNeverPublishesClassicClosedMetadata 补充 logical timeout 后 stop 不提前完成，释放真实 server force 后才排空；UNKNOWN 与 OPEN 语义不变。

FqfR0HHI 为第一条回归 RED：旧 stop 提前返回；EbCtp1J5 最小 GREEN 1。GmyjXCEO 的 25 项中 24 通过、新 queued 用例错误地期待共用单 event loop 暂停期间仍得到 quorum，修正为 pending append 场景；X2L5QTV2 为该新测试的 Callable 缺少返回值编译失败，已修正。最终 0dINjiIr 的 25 项全部 PASS、零 skip；6NQqG64s 的整体恢复/末段 SELF 写回及原 worker 10 项全部 PASS、零 skip，共 35 个不重叠真实用例。历史 receipt 与本轮失败事实保留，不把未执行范围计 PASS。

复现：

```bash
bash scripts/unified-wal/run-local-storage-tests.sh 'SegmentProfileInitialSocketClientTest#writerStop*+originalSdk*+sdk*+ordinary*+closeWaitsForQueuedDataBorrowAndDoesNotChangeTheOriginalInput+closeWaitsForActualWritePromiseEvenAfterReplyAndTransportClose+validatedReplyDoesNotReleaseSerializedSlotBeforeActualWritePromise+cancelledWaitingRetainsBoundedPendingUntilOriginalWireTerminal'
bash scripts/unified-wal/run-local-storage-tests.sh 'SegmentProfileInitialSocketClientTest#wholePointRecoveryClosesEmptyAndNonemptyFromOpenWithOriginalSdkAndQuorum+selectedLastFragmentWritesBackToTheExistingMembersWithoutChangingMembership+replicationWorker*'
python3 scripts/unified-wal/run-storage-models.py --tool "$UW_TLC_JAR" --config-prefix a-profile-normal-client-source --config-prefix ce-profile-normal-client-source --config-prefix a-profile-registered-connection --config-prefix ce-profile-registered-connection
mvn -o -P '!cargo-zigbuild' -pl bookkeeper-server -am spotless:check checkstyle:check
mvn -o -P '!cargo-zigbuild' apache-rat:check
```

既有 ProfileNormalClientSource 有限投影增加 SDK stop/caller cancellation，只消费原 source/output/quota 实际终态；early-stop 和 cancel-as-completion 两种错误规则得到指定 SdkStopWaitsActualCustody 反例。最终 3b8imp78 的 17 配置为 2 完整正向/15 指定反例，正向 source 模型 403 generated/262 distinct、queue 0、depth 10、1.125 秒/peak RSS 111853568 bytes；tool 1.7.4、单 worker、heap 512/direct 64 MiB、seed 1/fp 0、无 symmetry/state constraint、CHECK_DEADLOCK FALSE。不是性能结论、实际 TLS/fsync producer、完整时钟/公平性/活性或 A+C+D 全组合证明。

本轮持久证据在 /Users/liusinan/.codex/artifacts/unified-wal/writer-stop-evidence-kr2up7fw，archive.json 保留原 run/source/config/command 身份。style exit 0；RAT 初次 445 含两个新配置的短许可头，补齐标准 Apache 头后仍 exit 1、443 unapproved 路径全存在于父 HEAD，无新增；不改许可规则，不称 CI 全绿。步骤 12/14/15 与生产 Gate 仍 PARTIAL/OPEN；普通 durable writer close/adoption、额外 required authority producer、OPEN/payload/placement audit、full-bytes、离线/rejoin/delete/history GC/完整退役、兼容/组合/正式性能/运维仍未闭合。Linux NUMA/真实掉电/正式性能本轮 NOT_EXECUTED。


## 普通 writer close 与固定 cut 续接

2026-10-11，第14步在显式提供既有 cold coordinator、canonical descriptor/validator 和 verified CONTROL peers 后，把原 ProfileLedgerHandle.close/closeAsync/asyncClose 接到同一个关闭 procedure。enableWriterClose 与 replacement 依赖配置一样在首次 append 前完成；未提供这些依赖的旧 factory 仍显式拒绝 durable close，不创建第二个 SDK/线程/连接池或猜测控制面。普通 Add 路径不新增 metadata/control/handshake。当前是有界、隔离、可丢弃实验接线，不是默认 BookKeeper.open/create 路由或生产 authority。

原 ledger executor 停收、drain pending adds 并调整 length，在同一 handle lock 内冻结 lastAddConfirmed=P、length=L、OPEN metadata snapshot 与单次 operation ID。之后先结束 pending callbacks，并等待上节自身 DATA source/output/RPC 的 actual completion；caller 取消 close future 不取消共享 procedure，重复 close 消费同一结果。只有实际排空后，才用原 SDK base context 和保留 DATA peers 调用既有 coordinator。旧 metadata/creator/backlink、descriptor、credential、root 或 pending replacement 对不上时返回 non-OK；不重新 OPEN，不分配新的 entry 坐标。

关闭 cut 以标准 metadata 的 bookkeeper.experimental.profile.writer-close key 保存，和 OPEN→IN_RECOVERY/context 在同一次 birth/version/root-checked ZK multi 发布。实验 UWNC/schema1 固定32字节：magic4、schema/reserved各2、P/L各8、reserved4、CRC32C4；(-1,0) 和 Long.MAX_VALUE/负值规则严格检查。两份独立原始向量为 55574e4300010000ffffffffffffffff000000000000000000000000f8ee81ee 与 55574e4300010000000000000000000000000000000000100000000089792ce9。它记录必须保持的普通 writer cut，不证明任何 DATA 或 CLOSED，也不另存 membership。

该 marker 进入 context 的 canonical OPEN projection digest，不能在 repair/history inverse 时被删除；核对原 READY/replacement publication 时只投影掉这个由本次 CAS 新增的 marker，仍保留原 membership/版本/creator/birth证明。原 OPEN 中冲突 reserved 状态的拒绝保留。Writer context 使用同为184字节的 UWRC/schema2，强制 marker 存在；普通 recovery 的 schema1/原 corpus 字节与12参数构造入口不变。Schema2 不能伪装成 schema1：锁定父源码18853770fe的旧 decode 严格要求schema1，已静态核对其拒绝条件；当前向量测试独立改 header/CRC、核验旧向量不变与未知schema3拒绝。历史 Profile-aware 二进制实际 recovery/混跑和完整 stock-binary/old-decoder 矩阵未执行，不计 G1 或 stable PASS。

通用 recovery/worker 看到这个 matching mandatory writer context 时，也只读验证0..P及累计 L，产生 VERIFIED_WRITER_PREFIX，既不查询 normal tail 是否存在，也不吸收P之后的物理记录；required hole、length/payload冲突、权限或预算不足都不发布选择/关闭。随后复用原 UWRF/UWRS、SELF bounded last-fragment COPY/grant-close、空末段 UIC6/type12、distinct terminal ACK 和唯一标准 CLOSED(P,L,context)/UWCF close-history CAS。该路径沿用原 fencing、UNKNOWN、真实I/O和固定操作续接，不制造普通 activation 或 loss reset。提供的 CONTROL principal 仍需原 exact AuthZ，DATA credential 不代替控制权限。

真实验证包括：A2/A3 空、非空 SDK writer 的实际副本均留有更长未确认 tail，close、独立 readonly-open 和健康文件 close/restart 保持原 P/L 与读上界；held DATA output promise 期间 metadata 仍为原 OPEN，取消第一次 caller future、重复 close、新 append 拒绝和最后输出清零均被覆盖；已提交 IN_RECOVERY/cut但reply未到时，caller deadline 后另一实际 ZK client/新 coordinator 沿用原 context、P/L 关闭，改动或删除 cut 则 non-OK；真实 DATA force 失败触发已完成 replacement 后，历史fragment与当前membership保持，CLOSED CAS reply CONNECTIONLOSS按重读同一结果解析，两段 SDK payload 都可读。后两类是实际 reply-window/fresh-session 验证，不称真实进程死亡；当前文件 restart 是健康关闭/replay，未称掉电证明。

复现：

```bash
bash scripts/unified-wal/run-local-storage-tests.sh 'SegmentProfileInitialSocketClientTest#ordinaryWriterClose*+writerStop*+originalSdk*+sdk*+wholePointRecovery*+wholeRecovery*+selectedLastFragment*+closedRepair*+closedReplicaHealth*+replicationWorker*,ZKProfileLifecycleStoreTest,ProfileLedgerMetadataStoreTest'
bash scripts/unified-wal/run-local-storage-tests.sh 'SegmentProfileInitialSocketClientTest#ordinaryWriterClose*+writerStop*+wholePointRecoveryClosesEmptyAndNonemptyFromOpenWithOriginalSdkAndQuorum,ZKProfileLifecycleStoreTest,ProfileLedgerMetadataStoreTest'
bash scripts/unified-wal/run-local-storage-tests.sh 'SegmentProfileInitialSocketClientTest#ordinaryWriterClose*+wholePointRecoveryClosesEmptyAndNonemptyFromOpenWithOriginalSdkAndQuorum,ZKProfileLifecycleStoreTest#writerCloseContextHasItsOwnSchemaAndPreservesTheLegacyVector+ordinaryWriterCutHasIndependentFixedBytesAndStrictBounds+recoveryContextVectorOwnsItsFieldsAndRejectsMalformedIdentity'
python3 scripts/unified-wal/run-storage-models.py --tool "$UW_TLC_JAR" --config-prefix a-profile-writer-close --config-prefix ce-profile-writer-close --config-prefix a-profile-normal-client-source --config-prefix ce-profile-normal-client-source --config-prefix a-profile-whole-recovery --config-prefix ce-profile-whole-recovery
mvn -o -P '!cargo-zigbuild' -pl bookkeeper-server -am spotless:check checkstyle:check
mvn -o -P '!cargo-zigbuild' apache-rat:check
```

o0XW6MW5原整体入口1通过；JEUF4LcB新测试误用不存在的 storage helper，testCompile失败，修正为已有实际 restart入口后KVSX6gV7首项通过；vjXoj95p3项、fCOPZraU历史/丢CLOSED回复1项通过。广度Er5XHRyQ 3类101、csnnW1mt 3类102均零fail/skip，分别在最后测试换行/新增历史场景与显式保留 reserved guard 前；TamSz4Dr3类73在schema2前，schema2后的3M6fyKpH3类74零fail/skip，在最后3处仅长行换行前。最终Hc3KETQ9两类8项零fail/skip，1069当前inputs逐项hash匹配；这些范围分开保留，不相加为独立用例数、不移绑旧PASS。最终 style exit0；RAT exit1，443 unapproved路径全部存在父HEAD，无新增，不改许可规则或称CI全绿。

最终模型dcq_0y4g为27配置：4完整正向/23指定反例，924当前inputs逐项hash匹配。新增 ProfileWriterClose 只投影 confirmed P/L、DATA terminal、同CAS cut、digest/支持kind、固定adoption、length与caller cancellation；新正向142 generated/96 distinct、queue0、depth7、0.990秒/peak RSS108806144 bytes，非性能结论；tool1.7.4、单worker、heap512/direct64MiB、seed1/fp0、无symmetry/state constraint、CHECK_DEADLOCK FALSE。TLS/fsync、完整SDK/metadata历史、所有timer、公平性/活性与A+C+D完整组合仍非该有限模型产物。

持久证据在 /Users/liusinan/.codex/artifacts/unified-wal/writer-close-evidence-a6gtqzmd，archive.json保留所有原source/config/run/command、失败与阶段身份。当前 prefix验证/SELF末段操作仍有显式 coordinate/duration界限，超限defer；完整大规模close成本、任意故障/进程kill/掉电、混跑兼容、默认SDK路由、其他required-authority producer、OPEN/payload/placement audit、full-bytes、离线追赶/rejoin/delete/history GC/退役和综合运维/性能未闭合。步骤12/14/15与生产Gate仍PARTIAL/OPEN，Linux NUMA/真实掉电/正式性能本轮NOT_EXECUTED。


## Fenced-CLOSED 集群逻辑删除与目标义务

2026-10-11，第15步新增显式 ProfileDeleteCoordinator，借用原 SDK scheduler、cold executor、ZK reservation/metadata stores。deleteLedgerAsync/beginDelete 只接受已通过原完整历史/selection/close验证的 matching fenced-CLOSED；OPEN 或未满足该证明不代为关闭、不按 online-delete 返回成功。默认 BookKeeper.deleteLedger 路由未接本入口，普通 Add 无新 metadata/control 查询、线程、连接池或 fsync。

流程为 fresh CLOSED/root proof → 同一 root cut CAS消费标准 metadata birth/version → DELETE_INTENT关闭新 admission → 在标准CLOSED记录CAS发布既有 membership-frozen key → 从该sealed版本与原initial/replacement/admitted-repair历史枚举全部bookie/incarnation → 持久有界immutable delete-targets及其birth guard → 再次标准metadata CAS发布 bookkeeper.experimental.profile.deleted 的出生绑定manifest引用。P/L、creator、原ensemble map与recovery close事实不改变；先seal再枚举，所有受支持membership/grant/activation publication仍消费原root cut，后续repair metadata写对freeze明确拒绝。仅DELETE_INTENT、seal、inert target candidate或UNKNOWN都不是logical completion。

使用实验UWDS/schema1固定88-byte seal（request/epoch/pre-seal version/metadata及lifecycle物理birth/完整CLOSED digest）、UWDM/schema1 target manifest和UWDT/schema1固定88-byte logical reference。manifest使用既有60-byte ProfileLedgerContext、88-byte seal、count与严格UTF-8 bookie/16-byte incarnation，固定prefix含CRC为164 bytes；目标按UTF-8 bytes/incarnation排序且去重，沿原32 initial/32 replacement pages/32 repair slots给出最多1088目标，512KiB前置上限。reference绑定manifest czxid、digest及seal digest，immutable birth witness和最后fresh root/metadata检查阻止同字节/version0重建冒充旧义务。不是新的membership/ACK权威或持久任务系统。

LOGICALLY_DELETED只证明authoritative cluster tombstone及完整可恢复target义务已提交，不等待目标online/ACK、local access barrier、runtime drain、compaction或FREE。原standard metadata与所有历史输入暂保留，供发现/续接；没有伪造target delete-applied、delivery sequence/cursor或physical receipt。历史 matching CLOSED查询在核对seal/manifest后投影原版本，仍能报告此前真实关闭；新readonly-open和repair admission被原DELETE cut拒绝。旧reader在目标尚未本地tombstone时仍可能读，符合普通删除的边界，强访问撤销继续disabled。

同一request/instance/payload继续三段CAS，回复丢失按实际persistent state解析；另request冲突。Caller timeout/cancel停止等待和未来阶段，beginDelete.completion仅在实际cold procedure返回后完成，不是remote ZK request abort/NONE。Procedure仍运行时保留同一permit和借用资源；冷阶段尚未终结不能以caller future作为owner排空证明。持久admission/history/manifest不依赖易失发送队列，下一阶段delivery/catch-up将消费这些义务。

真实验证：OPEN precondition不改root；generic recovery(schema1)与ordinary writer close(schema2)两类CLOSED都在断开一个actual listener/CONTROL/DATA连接后完成logical delete并保留该incarnation；新open拒绝，既有SDK readonly reader仍可读，P/L/ensemble不变。实际force失败→同起点replacement覆盖socket2后，最终ensemble没有socket2，manifest仍包括四个获授权目标；root cut/seal/logical三段真实ZK multi的commit后CONNECTIONLOSS都重读同request完成。Held cut reply期间cancel不release permit，实际cold completion后由另一ZK客户端/新coordinator续接同请求；已经引用的manifest删除或同字节重建均quarantine且不复活root。

独立raw UWDS向量与CRC、UWDM布局/count/排序/重复/truncation/超大count前置拒绝和不可变incarnation也有覆盖。初始8ZiFt841/r6EbHx2l发现manifest把既有60-byte ledger context误计为64-byte的实现错误（RED/诊断），改为引用ENCODED_LENGTH后41IEMqGI GREEN1，临时异常诊断已撤销。TUGpq4OC GREEN2；IilDQfCo4项是actual completion API前且测试使用错误的8-thread executor barrier假设，不作为排空证明；换成真实Attempt.completion后ePXr6PJy4项通过。最终456zVqhN三类76项（metadata19/lifecycle49/native8）、OjTIJZUW原整体recovery/repair/health/worker14项均零fail/skip，两run共1071当前inputs分别核对无变化；重复覆盖不合计为独立用例。

复现：

```bash
bash scripts/unified-wal/run-local-storage-tests.sh 'SegmentProfileInitialSocketClientTest#logicalDelete*+ordinaryWriterClose*,ZKProfileLifecycleStoreTest,ProfileLedgerMetadataStoreTest'
bash scripts/unified-wal/run-local-storage-tests.sh 'SegmentProfileInitialSocketClientTest#wholePointRecovery*+wholeRecovery*+closedRepair*+closedReplicaHealth*+replicationWorker*'
python3 scripts/unified-wal/run-storage-models.py --tool "$UW_TLC_JAR" --config-prefix a-profile-logical-delete --config-prefix ce-profile-logical-delete --config-prefix a-profile-writer-close --config-prefix ce-profile-writer-close --config-prefix a-profile-whole-recovery --config-prefix ce-profile-whole-recovery
mvn -o -P '!cargo-zigbuild' -pl bookkeeper-server -am spotless:check checkstyle:check
mvn -o -P '!cargo-zigbuild' apache-rat:check
```

最终模型6tg4udtw共27配置，4完整正向/23指定反例，935当前inputs匹配；新logical投影只消费fenced-close、ZK durability/birth/version、retained history与cold terminal输入，覆盖precondition、先seal、完整义务、logical条件、出生绑定、cancel/permit及offline不阻塞logical。新正向109 generated/72 distinct、queue0、depth8、0.868秒/peak RSS107724800 bytes，非性能结果；TLC1.7.4/单worker/heap512/direct64MiB/seed1/fp0、无symmetry/state constraint/CHECK_DEADLOCK FALSE，未生产TLS/files、delivery/cursor/rejoin、全metadata拓扑、时钟/公平性/活性或A+C+D完整组合。旧yvc3rna6在cold行动/终态guard补齐前，不移绑最终源码。

持久证据 /Users/liusinan/.codex/artifacts/unified-wal/logical-delete-evidence-hqfilyta 保留原run/source/config/command、失败和阶段绑定。compile/style exit0；RAT exit1，443 unapproved全在父HEAD，无新增、不称CI全绿。当前只有有界cluster logical阶段；实际TOMBSTONE wire/本地delete-applied、跨ledger ordered delivery/no-hole cursor、snapshot/catch-up、真实startup rejoin、physical query/GC/decommission、默认SDK routing与完整兼容/模型/性能/运维仍未闭合，步骤12/14/15及生产Gate仍PARTIAL/OPEN。Linux NUMA/掉电/正式性能本轮NOT_EXECUTED。


## 初始共享 delete delivery journal

2026-10-12，第15步在现有cluster logical tombstone/完整target manifest上新增实际ZK delivery journal。每个Bookie stable identity/storage incarnation共用一个stream，不按ledger分配；initial streamGeneration=1/assignmentGeneration=1，immutable assignment、head与birth witness同一multi创建。assignment只固定本实验投递绑定，不授予writable registration或handshake/ACK权限，generation handoff/wipe未实现仍拒绝。

appendDeleteDelivery先fresh验证原root、sealed metadata、出生绑定cluster tombstone和目标在完整manifest内，再在同一ZK multi中create immutable event、create per-ledger/target同请求续接index、CAS head N→N+1。event与index物理birth相同；head数据版本等于committed-through，head/witness/assignment物理birth一致。初始有限上限每stream4096 events；满额返回LIMIT保留义务，不盲建第二stream、丢事件或假装可持续无限增长。没有snapshot/compaction前不扩张此界限。

Experimental UWDH/schema1 head为70+UTF8 bookie长度 bytes；UWDE/schema1 event为242+长度 bytes，使用既有60-byte ledger context、88-byte logical tombstone reference、stream/generation/assignment/sequence、request/epoch和exact target/incarnation，CRC32C严格，单record上限512 bytes。跨ledger head增长不改变assignment generation。未确认multi结果从同一index+head+event重读解析；并发loser可能UNAVAILABLE，重试仍沿同ledger/target/request，不能换target或多消耗序号。

readDeleteDelivery仅返回已committed且仍匹配cluster tombstone、完整ledger context/target、request/epoch与index birth的effect；缺失或同字节重建、assignment/head身份不符、sequence<=head却无event均quarantine。sequence>当前head是UNAVAILABLE，不提供可推进cursor的non-applicability证明。读取不写cursor、不消费physical receipt，未接本地TOMBSTONE/应用/注册gate。旧incarnation不能由新incarnation继承本stream；既有assignment/index存在而head缺失时拒绝重建新stream。

真实两-ledger验证在同一个SDK/ZK和三个实际Bookie文件/socket上重新创建ledger45、执行真实all-E INSTALL/ACTIVATE与empty writer close，再和ledger44分别logical delete；同target的两个event严格seq1/2且stream一致，只有一个head+一个assignment。重复append沿原坐标；wrong incarnation拒绝。event1丢失和同字节重建拒绝；head缺失/重建而old assignment保留拒绝。另一个真实ZK客户端注入event/index/head commit后的CONNECTIONLOSS，重读得到seq1；不同实际客户端并发同请求只有一个event/seq赢家。该测试是实际网络/存储创建关闭与ZK投递authority，不是目标已经收到或applied。

复现：

```bash
bash scripts/unified-wal/run-local-storage-tests.sh 'SegmentProfileInitialSocketClientTest#committedDeleteDeliver*+logicalDelete*+ordinaryWriterClose*,ZKProfileLifecycleStoreTest,ProfileLedgerMetadataStoreTest'
bash scripts/unified-wal/run-local-storage-tests.sh 'SegmentProfileInitialSocketClientTest#committedDeleteDeliver*+logicalDelete*,ZKProfileLifecycleStoreTest#deleteDeliveryCodecsHaveFixedBoundsAndIndependentCrc'
python3 scripts/unified-wal/run-storage-models.py --tool "$UW_TLC_JAR" --config-prefix a-profile-delete-delivery --config-prefix ce-profile-delete-delivery --config-prefix a-profile-logical-delete --config-prefix ce-profile-logical-delete
mvn -o -P '!cargo-zigbuild' -pl bookkeeper-server -am spotless:check checkstyle:check
mvn -o -P '!cargo-zigbuild' apache-rat:check
```

首次compile失败来自record target accessor遮蔽outer parser及未捕获IOException，已修正；FlqS2GS2两ledger首项GREEN1。YJGfDEhE两项GREEN在丢回复与竞争尚未分离前，其race不保证注入发生，不计loss证明；随后先单独loss并assert实际dropped，再另target竞态。UBhycuOa三类79零fail/skip（metadata19/lifecycle50/native10）在末次仅unit长行换行及完整reader context复核前；rE0eNXVV两类7在reader复核前。最终kkggENGg两类7零fail/skip，1072当前inputs逐项匹配；不移绑原79。最终style exit0，RAT exit1，443 unapproved全部父HEAD既有、无新增，不称CI全绿。

新ProfileDeleteDelivery有限投影以logical tombstone、ZK atomicity/birth/assignment作为输入，覆盖head/event原子提交、同请求唯一seq、精确binding、missing不可补成功和非per-ledger stream六个指定反例；initial biom2ze0缺Sequences模块导致SANY INCONCLUSIVE，修复后最终j2innjna的15配置为2完整正向/13指定反例，944当前inputs匹配。新正向308 generated/110 distinct、queue0、depth8、0.945秒/peak RSS108871680 bytes，非性能结论；TLC1.7.4/单worker/heap512/direct64MiB/seed1/fp0、无symmetry/state constraint/CHECK_DEADLOCK FALSE。没有生产local cursor、TLS/fsync、注册线性化、handoff、snapshot/GC、完整时间/活性/公平性或A+C+D。

持久证据 /Users/liusinan/.codex/artifacts/unified-wal/delete-delivery-evidence-rv38_1o0 保留原source/config/run/command、compile/SANY/style失败与阶段结果。当前只完成committed delivery source及其只读消费验证；实际TOMBSTONE wire、delete-applied/cursor、snapshot/catch-up、registration required-through cut/rejoin、物理查询/GC/decommission和全16步其他范围仍未闭合，步骤12/14/15及生产Gate保持PARTIAL/OPEN。首批隔离可丢弃，Linux NUMA/掉电/正式性能本轮NOT_EXECUTED。


## UIC7 本地 delete-applied 与连续 cursor

2026-10-12，在 `7d241ce0e500390ff0e1d18206eb452964069f97` 后继续接实际 committed delivery → cold exact authority → mTLS CONTROL → 原 Bookie write/force/owner apply。首批仍显式隔离、OFF/SYSTEM/PARK、可丢弃，不开放生产或普通 SDK 自动 delete routing。

`ProfileDeleteDelivery.Event.deleteApply(domain)` 从完整 event 产生确定 operation ID 与 96-byte `DeleteBinding`：stream UUID/generation、assignment generation、sequence、原 request ID、delete epoch、cluster tombstone reference digest；scope 另绑定 ledger/instance/descriptor、Bookie/storage incarnation、domain、epoch/op/payload。只有 initial generation1/assignment1 与 sequence1～4096。`deleteAuthorityReader` 从 endpoint 派生的固定 ledger key、本地 Bookie/incarnation receipt 重新读取实际 journal event，再由原 `readDeleteDelivery` 校验 head/assignment/birth、event/index 同事务、logical tombstone/seal/完整 manifest；domain 必须来自原 reservation。仅 DELETE_INTENT、伪造 caller event、缺失/重建 authority 均不能授权。原 static principal/exact scope/AuthZ、bounded cold executor、CONTROL receive/output quota与实际 driver ownership继续使用。

experimental UIC7 id `0x55494337`、semantic/schema version6，opcode复用 `TOMBSTONE_OR_DELETE_APPLY=0x0106`；UWIC/UWIR内层schema6，request223～350 bytes、reply60 bytes。CONTROL HELLO显式offer/advertise；delete操作单独检查peer capability/negotiation。Plain legacy local tombstone请求不接受为该wire body；其他UIC1～6原bytes保留，generic `ProfileOperationCodec`仍拒绝未冻结control body。只有source/golden corpus校验，released decoder/stock binary矩阵仍DEFERRED_NOT_RUN，G1保持BLOCKED_UNVERIFIED。

Bookie新增type13 `DELETE_DELIVERY_APPLY`，原scope后保存96-byte binding，同一write/force/owner apply建立instance-specific terminal TOMBSTONE与continuous applied cursor。single control lane一次应用一个event；seq>N+1在任何写盘/准入cut前返回TRANSIENT_RECONCILING，不memoize成永久冲突；其他stream/generation、同坐标不同语义或local instance冲突拒绝。同请求重试ALREADY_APPLIED，重试早期seq不回退cursor。成功reply的localGeneration是该event sequence，包括本地没有ledger slot时；它是该experimental effect坐标，不是writable registration或生产receipt。force/timeout失败保留UNKNOWN而不发布成功cursor。新normal/recovery/read准入及迟到结果资格沿原TOMBSTONE cut拒绝；旧DATA/读ticket和文件pin保留至实际终结。cursor不要求runtime drain结束，也不证明ACCESS_BARRIER_COMPLETE、physical-reclaimed或FREE。

type13原record进入已有retained operation checkpoint，按原sequence重放重建cursor；rotation/restart沿原storage header/incarnation、完整control校验及同步，不从新incarnation继承cursor。记录缺口、错误binding或其他initial assignment拒绝打开/应用，不猜测non-applicability、不盲开新stream。当前仅一个固定initial stream；snapshot/历史压缩扩容及assignment handoff未实现。

真实两-ledger/socket用例增加非空ledger44/空ledger45：实际SDK append、writer close、旧readonly读取、logical delete后未本地tombstone前读取；伪造stream拒绝，seq2先到可重试，seq1/2按序durable，held真实DATA文件read pin期间cursor可推进；对应目标后续READ_ENTRY返回既有REFUSED/PROFILE_NOT_READY_OR_STALE、NONE和空body。重试旧seq不回退；实际storage restart保留两terminal context和cursor2，event同字节重建后endpoint隔离，即便本地已有ALREADY结果也必须重新授权。真实ZK提交回复丢失/并发winner原覆盖保留；目标TOMBSTONE回复在真实网络丢失后fresh CONTROL重试同request为ALREADY_APPLIED；另target真实force失败为DURABILITY_UNKNOWN/localGeneration0且cursor未发布。本地真实FileChannel覆盖absent ledger、gap无force/状态改变、旧driver-owned DATA/read ticket不释放/不允许rotation/reclamation cut、checkpoint rotation后cursor回放及继续seq3。

复现：

```bash
bash scripts/unified-wal/run-local-storage-tests.sh 'ProfileInitialControlCodecTest,ProfileReceiveDecoderTest,SegmentBookieControlLogTest#continuousDelete*+uncertainDelete*+*Tombstone*+tombstone*+*Checkpoint*+*Rotation*,SegmentProfileInitialSocketClientTest#committedDeleteDeliver*+logicalDelete*+ordinaryWriterClose*,ZKProfileLifecycleStoreTest,ProfileLedgerMetadataStoreTest'
python3 scripts/unified-wal/run-storage-models.py --tool "$UW_TLC_JAR" --config-prefix a-profile-delete-applied --config-prefix ce-profile-delete-applied --config-prefix a-profile-delete-delivery --config-prefix ce-profile-delete-delivery --config-prefix a-profile-logical-delete --config-prefix ce-profile-logical-delete
mvn -o -P '!cargo-zigbuild' -pl bookkeeper-server -am spotless:check checkstyle:check
mvn -o -P '!cargo-zigbuild' apache-rat:check
```

持久证据 `/Users/liusinan/.codex/artifacts/unified-wal/delete-applied-evidence-q3gjc362` 保留原run/source/config/command及阶段compile/断言错误，不重绑历史PASS。首个gz0OixDk的9项GREEN早于非空payload/丢目标回复/force失败覆盖；B21NsMYG和sAEz0Y3H分别为testCompile错误（ticket方法、测试插入锚点）；mfDv5KZL的RED来自未claim driver ticket与误认既有read refusal status，修正为真实driver custody和既有REFUSED/空body合同。7E0TueYE共12项零fail/error/skip。

最终K5aQf9D2六类158项（common codec8、receive decoder22、Bookie control49、metadata19/lifecycle50、真实socket10）零fail/error/skip，1072当前source inputs逐项匹配；不把重复12项与158相加。最终style exit0；RAT exit1，443 unapproved路径均存在于父HEAD，无新增，不称CI全绿。

新 `ProfileDeleteApplied` 有限投影消费exact cold authority、force及checkpoint字节作为输入，检查continuous effect、durable前不成功、cursor/checkpoint重建、旧custody不能提前release、initial assignment和actual incarnation，7个mutations均为指定反例。最终7ii07jzb共23配置为3完整正向/20指定反例，953当前inputs匹配；新正向287 generated/120 distinct、queue0、depth12、0.895秒/peak RSS111116288 bytes，非性能结论。TLC1.7.4/单worker/heap512/direct64MiB/seed1/fp0，无symmetry/state constraint、CHECK_DEADLOCK FALSE。模型不生产TLS/fsync/ZK/registration事实，不含snapshot/handoff/GC、完整时钟/活性/公平性或A+C+D组合。Linux NUMA/真实掉电/正式性能本轮NOT_EXECUTED。

当前新增wire/apply/continuous cursor切片；registration required-through提交cut、startup/offline catch-up/rejoin、snapshot/handoff、physical query/history GC/decommission、默认SDK routing及全16步其他验收仍未闭合。步骤12/14/15与生产Gate保持PARTIAL/OPEN，持续目标active。


## 初始 required-through cut 与真实 rejoin 追赶

2026-10-12，在 `aac954a0f5873322a66dad92616a273791a51511` 后继续接固定initial assignment的实际delete catch-up与writable publication cut。此前UIC7/native type13、原DLR/readiness/service-info/session-admission schema和历史receipt不重写；当前仍显式隔离、OFF/SYSTEM/PARK、可丢弃。此增量不关闭完整startup/registration/rejoin或生产Gate。

`readDeleteHead(target)`直接sync/read/验证original shared head、immutable assignment和birth，并给出固定gen1/assignment1的required-through。缺失不会解释成EMPTY/default或触发恢复重建。`createInitialDeleteAssignment(target)`仅供显式首次commissioning创建empty head0，head/birth/immutable assignment同multi；old assignment仍在而head缺失时拒绝重新初始化。它不是wipe/decommission或旧incarnation reset授权，不在startup reader中自动调用；新incarnation独立target key，旧scope的obligation仍保留。完整verified bootstrap、历史压缩/snapshot及commissioning/terminal proof生产授权仍OPEN。

`ZKProfileEphemeralRegistrationStore.bindDeleteDelivery`在原startup attempt任何cold operation前绑定本地storage incarnation、原reservation store及其durable cursor supplier，必须是publisher同一个实际ZK client。fresh writable create的原multi增加assignment0、原stream birth及head dataVersion检查；fixed assignment必须匹配readiness有效generation，本地cursor须匹配stream/gen/assignment并到达该head的N（空N0允许无effect cursor）。delete event先提交则旧head check失败，保持无writable并继续追赶；registration先提交则后续event仍在该incarnation共享stream中，ordinary head增长不改assignment generation，也不单凭新N撤销已经成立的registration。owned ALREADY/reply-loss解析保留原session/nonce；不新增metadata node/schema、Add-time lease、watch时序authority或轮询重试层。未绑定的历史reference publisher仍只覆盖其原输入事实，不代表具备此delete barrier。

`IsolatedSegmentStorage.catchUpProfileDeletes`是显式bounded cold入口：要求原graph已实际恢复，读取自己的唯一initial assignment/head，按cursor+1重读完整committed event及原cold exact DELETE authority，逐effect复用原`SegmentProfileInstallStore`/控制I/O/owner force/apply，再fresh head；未解释的缺口、wrong binding、missing authority或changed incarnation不成功。最多maxEvents、deadline停止继续本地提交，相关borrowed store保持原cold read deadline；没有新增线程、队列、snapshot backend、自动bootstrap、全日志内存副本或physical reclaim等待。LIMIT需要caller另一次有界调用；UNKNOWN只解析已提交记录。`profileDeleteProgress`由这个仍opened/healthy的原control log提供，失败/关闭graph不再提供可注册的progress；无从caller supplied READY facts反推cursor。

catch-up Attempt的completion以原store的实际local control I/O/owner apply observer为界，独立于caller等待及UNKNOWN result，访问器返回future副本。它不取消底层I/O，不证明ZK callback已取消、remote abort、全graph资源排空、access barrier、FREE或physical完成。原graph关闭/回收继续等待自己的driver/ticket/pin保护条件。

真实组合复用三个实际Bookie/mTLS/SDK与ZK创建、关闭并logical-delete的非空ledger44和空ledger45。一个尚未本地delete的incarnation关闭原graph并从实际目录restart，预先绑定原session Admission，初始gate关闭/cursor缺失；即使输入persistent readiness声称READY，fresh writable在cursor未到head1时拒绝。cold追赶seq1后，原DLR在真实FileChannel/目录force并读回；另实际ZK client在writable multi前提交ledger45的seq2，原注册因head版本竞争失败且gate未打开。held实际DATA文件pin期间继续force/apply seq2，writable multi提交后丢回复，经同session/nonce重读与ALREADY解析后才openAfterReconciliation；目标后续读取不能发布已删除entry，demote保持不可重开。

另一个实际incarnation从同目录restart，head2/maxEvents1只应用seq1并返回LIMIT；第二effect的真实控制force被hold，timeout返回UNKNOWN/cursor仍1/completion pending，caller手动complete自己的future副本不影响actual completion。release后原owner apply/cursor2与actual completion终结，新cold调用fresh解析为CAUGHT_UP且无额外force。两项真实ZK单测另覆盖empty commissioning、missing head/保留assignment不重建，以及wrong publisher client/incarnation/effective generation；其cursor/compatibility输入不冒充物理恢复，该链由上述actual graph组合提供。

复现：

```bash
bash scripts/unified-wal/run-local-storage-tests.sh 'ZKProfileEphemeralRegistrationStoreTest,ZKProfileRegistrationStoreTest,ZKProfileLifecycleStoreTest,SegmentProfileInitialSocketClientTest#committedDeleteDeliver*+logicalDelete*+*Registration*+*Registered*+registered*+registration*+*Rejoin*'
python3 scripts/unified-wal/run-storage-models.py --tool "$UW_TLC_JAR" --config-prefix a-profile-delete-registration --config-prefix ce-profile-delete-registration --config-prefix a-profile-delete-applied --config-prefix ce-profile-delete-applied --config-prefix a-registration-admission --config-prefix ce-registration-admission
mvn -o -P '!cargo-zigbuild' -pl bookkeeper-server -am spotless:check checkstyle:check
mvn -o -P '!cargo-zigbuild' apache-rat:check
```

原U87S3UjE的RED只因真实第二incarnation加入后旧namespace count仍期待2；更新为每incarnation一head/assignment并按exact target选head，5xmqxKl3的3项GREEN早于LIMIT/held force覆盖。最终fiQW91IE四类101项（ephemeral19/persistent13/lifecycle50/socket19）零fail/error/skip，1072当前source inputs逐项匹配；不叠加重复kopOhE3K三项。style exit0；RAT exit1，443 unapproved全是父HEAD已有路径、无新增，不称CI全绿。

新`ProfileDeleteRegistration`有限投影只消费durable local cursor、committed head/initial assignment与ZK原子性输入，覆盖cursor不足、跳过head cut、missing default、wrong generation/incarnation及普通head增长不demote六反例。首次_e85up19遗漏Integers的负数定义，为SANY INCONCLUSIVE；修正后ks4f684z共20配置为3完整正向/17指定反例，961当前inputs匹配。新正向190 generated/78 distinct、queue0、depth8、0.805秒/peak RSS107970560 bytes，非性能结果；TLC1.7.4/单worker/heap512/direct64MiB/seed1/fp0，无symmetry/state constraint、CHECK_DEADLOCK FALSE。未生产TLS/fsync/硬件/ZK quorum、其他startup facts、在线delivery loop、snapshot/handoff/GC、完整时间/公平性/活性或A+C+D组合。

持久证据 `/Users/liusinan/.codex/artifacts/unified-wal/delete-rejoin-evidence-rinuvevb` 保留各原source/config/run/command、失败及阶段身份。当前为explicit fixed initial catch-up/registration cut；持续在线投递/默认bootstrap与其他required authority/device/compatibility facts、snapshot/handoff/history GC/terminal decommission、full-bytes、完整运行/兼容/性能/运维验收仍未闭合。步骤12/14/15和生产Gate保持PARTIAL/OPEN；Linux NUMA/真实掉电/正式性能本轮NOT_EXECUTED，持续目标active。


## 全目标 publication 与在线节点消费

2026-10-12，在 `65a28a35c4e3947438b2e36b9f85a2e031c79c34` 后补齐logical success与target journal之间的窗口，并接显式原注册实例的持续消费。删除前置仍是matching fenced-CLOSED；OPEN ledger在线删除、强访问撤权/reset、高级Range、跨Arena迁移继续disabled。当前在线消费指已注册Bookie消费CLOSED删除effects，不是开放在线删除前置或生产路由。

原logical实现先发布cluster tombstone，后续才手动append target event，writable cut只能看到已经增长的head。现在完整retained manifest落盘后，按其固定排序逐target原子记录event/index/head与`delivery-prepared`前缀K→K+1；全部目标recorded后，原logical tombstone CAS检查同birth/manifest与完整publication前缀。任何registration若在该target event前赢，则后续event保留在其online stream；若在event后尝试，则必须等待active effect并到达相应N。logical completion之前所有target的required-through都已经可见，不依赖watch先后或caller随后补append，不等待target ACK/offline节点/physical reclaim。每个事务仅一个target；既有最大1088目标保留，当前未做1088-target真实耗时验证。

新增`PreparedDeliveries`是该manifest的有序提交前缀，明确区别于Bookie applied cursor/派生清理队列：固定reference、publication UUID、targetCount/through、CRC，mutable node dataVersion严格等于through，同multi birth child防止ABA。每次前缀推进只能与新event/index/head同事务，或重读并检查已存在的exact immutable event/index/head后推进；回复丢失从原前缀续接，不重复序号。caller cancel/期限在后续阶段检查，全部event已recorded后取消仍不提交logical；沿同request/reference恢复即可激活原events。源丢失、出生绑定失配或缺口不补成功。

为了区别新数据的missing publication与旧schema1未包含该引用，UWDT reference新增schema2/104 bytes，保留原schema1/88 bytes；schema2追加16-byte publication UUID，绑定required prefix birth。UWDE event随reference新增schema2，为258+UTF8 target长度、最大513 bytes；原schema1为242+长度。PreparedDeliveries为schema2/140 bytes，legacy reference为schema1/124 bytes。原head、seal、manifest、UIC7的96-byte binding、native type13与历史vectors字节保持；旧严格reference/event reader会拒绝schema2，只计源码/新decoder验证，stock/released binary兼容矩阵仍DEFERRED_NOT_RUN、G1 BLOCKED_UNVERIFIED。历史schema1 logical reference可在同request下验证完整manifest并补齐原target records；新schema2 referenced prefix缺失时quarantine且不盲建新前缀。完整legacy/bootstrap/mixed-binary验收仍OPEN。

recorded event在cluster tombstone激活前为`PREPARED`；`readDeleteDelivery`检查真实seal/manifest/event/index/head/birth与prefix覆盖，返回此状态但不提供DELETE_AUTHORIZED。cold authority reader将其defer，catch-up不能推进cursor或写local TOMBSTONE。只有parent metadata已匹配该reference、publication完整时才COMMITTED并授权。新schema2 prefix缺失/重建必须拒绝，不能借caller READY或已经得到的旧结果跳过fresh authority。

`SegmentProfileDeleteFollower`由原opened/registered graph显式启动，必须属于同delete source、storage incarnation、原Admission/session；借用现有bounded cold executor及scheduler，不创建线程、队列、target连接或新持久任务系统。一个scheduled trigger、一个active probe，按有限maxEvents/timeout调用原catch-up；LIMIT继续小批，PREPARED/transient等待下一次，UNKNOWN等original actual local control completion后再fresh解析。原writable的ordinary head增长不demote；源明确缺失/损坏或原session退休则关闭此consumer和local admission，不自动重开。failure可由owner查询，未吞成成功。

stop立即退休准入并取消下一次tick，仍等待已接受cold工作及它已提交的actual local I/O/owner apply；future访问器只给副本，caller不能伪造actual drain。graph `close(timeout)`在原owner仍运行时先等consumer，再关原execution/files/index/control；超时返回false并保留原locks供下一次drain。没有把stop当作remote abort、NONE、FREE或强访问/physical完成。borrowed executor/client/scheduler仍由caller按原职责排空和关闭。

真实组合新增：generic CLOSED的ledger44，实际目录重启并原session registration先赢head0后启动consumer；真实ZK pause在全部target event/prefix已提交、logical CAS尚未发送时，读到PREPARED，exact authority UNAVAILABLE且没有native force/cursor。发布logical后，三个target的COMMITTED event均已存在；consumer自动进入actual native force，held force timeout为UNKNOWN，ordinary head增长不改变原registration。stop/future副本/100ms graph close都不释放在途资源，独立DATA writer锁仍被排他占有；release后原owner apply/cursor1、consumer drain与graph close完成。原两-ledger rejoin race改为真实ledger45 logical publication在注册前提交，验证当前自动所有target source，而非手动追加造成的假race。

另真实commit-reply hold在最后publication前缀事务之后取消：completion保持pending，metadata无logical key；实际cold return后仍无logical，新coordinator同request使用相同event/sequence/publication ID完成。删除schema2 required prefix后，effect read、append查询和同操作delete均quarantine且不重建。新数据的missing index也quarantine，query-only append不补新event/sequence，head保持原N；返回前验证exact ledger context/target/request/epoch/reference，不能从别的ledger receipt取得结果。metadata cut/seal/logical的三种loss继续按语义path注入，额外journal loss在automatic event/index/head/prefix commit后发生并重读同坐标。离线target无listener仍得到自己的journal obligation。原格式单测新增schema2/CRC/truncation/owned publication ID、1088计数边界与255-byte target的实际513-byte event；它不是1088-node或最坏fanout性能证明。

复现：

```bash
bash scripts/unified-wal/run-local-storage-tests.sh 'ZKProfileLifecycleStoreTest,ZKProfileEphemeralRegistrationStoreTest,ZKProfileRegistrationStoreTest,ProfileLedgerMetadataStoreTest,SegmentProfileInitialSocketClientTest#automaticDeletePublication*+preparedDeleteCancellation*+committedDeleteDeliver*+logicalDelete*+ordinaryWriterClose*+*Registration*+*Registered*+registered*+registration*+*Rejoin*,SegmentBookieControlLogTest#continuousDelete*+uncertainDelete*+*Tombstone*+tombstone*+*Checkpoint*+*Rotation*'
bash scripts/unified-wal/run-local-storage-tests.sh 'ZKProfileLifecycleStoreTest#deleteDeliveryCodecs*+versionedDeleteReference*,SegmentProfileInitialSocketClientTest#automaticDeletePublication*+preparedDeleteCancellation*+committedDeleteDeliver*+logicalDelete*'
python3 scripts/unified-wal/run-storage-models.py --tool "$UW_TLC_JAR" --config-prefix a-profile-delete-publication --config-prefix ce-profile-delete-publication --config-prefix a-profile-delete-registration --config-prefix ce-profile-delete-registration --config-prefix a-profile-delete-applied --config-prefix ce-profile-delete-applied --config-prefix a-profile-logical-delete --config-prefix ce-profile-logical-delete
mvn -o -P '!cargo-zigbuild' -pl bookkeeper-server -am spotless:check checkstyle:check
mvn -o -P '!cargo-zigbuild' apache-rat:check
```

ZqosUvct六类176项（control49/persistent13/lifecycle51/ephemeral19/metadata19/socket25）零fail/error/skip，在末次test换行及schema1已有logical但publication未完时的UNAVAILABLE/CANCELLED分类前；保留阶段绑定、不移绑。OrcDYFwr两类10项在新schema2 query-only index保护前；GFA4k7oK十项在最后exact ledger/instance/reference检查前，保持阶段绑定。最终iqAKCBuq两类10项零fail/error/skip，1073当前source inputs匹配。初始0iI8QYyG是schema1 publication阶段、92RYZb4d六项在consumer前，vYvlwGnj七项在cancel/prefix-loss案例前，EvDYMDoK十项在末次上述调整前；不叠加重复覆盖。最终style exit0；RAT exit1、443 unapproved全在父HEAD已有，无新增，不称CI全绿。

新`ProfileDeletePublication`有限投影以fenced-CLOSED、ZK原子性、immutable source和actual I/O/owner终结为输入，覆盖前缀早于index、logical早于全部目标、inert消费、prefix birth、stop后新工作、提前drain和probe重叠七个指定反例。最终p2tsybra共31配置为4完整正向/27指定反例，970当前inputs匹配；新正向327 generated/148 distinct、queue0、depth14、模型最终指标见归档coverage-summary.json，非性能测量，非性能结论。TLC1.7.4/单worker/heap512/direct64MiB/seed1/fp0，无symmetry/state constraint、CHECK_DEADLOCK FALSE；未生产TLS/fsync/硬件、真实1088-target边界、snapshot/GC/handoff、完整时钟/公平性/活性或A+C+D组合。

持久证据 `/Users/liusinan/.codex/artifacts/unified-wal/delete-publication-evidence-q384f2jy` 保留原source/config/run/command、阶段与style失败。当前automatic full-target source与explicit online consumer已接；默认bootstrap/SDK routing、其余required compatibility/device/authority producer、snapshot/handoff/history GC/decommission、full-bytes及综合兼容/性能/运维仍未闭合。步骤12/14/15及生产Gate保持PARTIAL/OPEN，Linux NUMA/真实掉电/正式性能本轮NOT_EXECUTED，完整目标继续active。


2026-10-12 [实际恢复 graph 与原 startup 发布桥接](../startup-readiness/README.md#实际恢复-graph-与原-startup-发布桥接)已把原header/control/DATA/index/owner恢复与committed delete追赶产生的device/allocator/route/delete facts接回原Harness、DLR/persistent/info/writable和session admission，再启动原consumer；错scope/client或scheduler拒绝不会成功开放。广度77项与最终1项真实组合、20配置有限模型分别保留stage输入，style通过、RAT443父HEAD历史问题无新增。compatibility/new-scope/credential仍由外部policy提供，默认bootstrap/SDK routing、snapshot/handoff/GC/full-bytes与综合Gate未闭合，步骤12/14/15保持PARTIAL/OPEN。


2026-10-12 [持久startup requirements与冻结发布条件](../startup-readiness/README.md#持久-startup-requirements-与冻结发布条件)已在独立ZKnamespace持久声明完整compatibility/单Arena expected set，原publisher绑定私有source观察并在writable multi同时检查原birth/version；重建或preflight后变化不被新hint吸收。显式pre-construction检查及actual reader/writer1阻止caller自报未来runtime，原graph恢复/consumer复用。阶段85/最终26项Java、相关18/最终4配置模型分别绑定source，style通过，RAT443历史问题无新增。它不是stock fence或OS credential证明；default bootstrap/SDK routing、snapshot/handoff/GC/full-bytes与综合Gate仍OPEN。


2026-10-12 [actual Unix access→原startup组合](../startup-readiness/README.md#实际-unix-credential-的-new-scope-access-快照)已把native UID、实际canonical roots/NONE/READ_WRITE送入原reference fallback，再复用actual mTLS/ZK/CLOSED/delete/restart/source/harness/consumer。0711目录已知文件实际可读的假成功真实RED/GREEN，最终31项零fail/error/skip、style通过、RAT443父HEAD历史问题无新增。ACCESS_SNAPSHOT_ONLY不证明old-service disposition、stock binary或生产启动，default bootstrap/SDK路由/全fallback与综合Gate仍OPEN。


2026-10-12 [owned真实Classic关闭→原startup组合](../startup-readiness/README.md#实际旧-classic-episode-的有序关闭与-new-scope-组合)已从实际old UID JVM/原EmbeddedServer、SDK E1成功写读、原stack/driver关闭与process terminal产生受控old DRAINED观察，closed-live和halt37/ephemeral expiry负向均拒绝；与实际access/source/replay/harness/consumer正常及scheduler拒绝组合通过。最终34项零fail/error/skip，style通过、RAT443历史问题无新增。外部old-service authority、stock binary、permanent decommission及default bootstrap/SDK routing和综合Gate仍OPEN。


## 显式 Profile listener 与实际网络 credit 排空

2026-10-12，`SegmentProfileListener` 从已恢复的original `IsolatedSegmentStorage` 复用 `installProfileConnection`，真正绑定Profile-only NIO listener，持有parent及全部accepted channels。TLS/HELLO/operation parser、static/exact authorizer、原CONTROL冷读、storage drivers、deadline/status与charged backing均沿原pipeline；不另建handler/协议/线程/冷队列。graph/TLS/cold/acceptor/worker仍由caller持有；传入receive budget由该listener episode管理，不能与另一个独立listener共享并分别关闭。

`bound()`与`closeAsync()`只返回private actual future的副本，caller complete/cancel不宣告真实终态。bind失败先清理native channel及本组件budget，再向bound caller传播失败；已恢复graph和借用event loops保持可用。这个入口是显式isolated组件，不自动创建storage目录、不产生compatibility/new-scope authority，不接入default EmbeddedServer或生产配置；HELLO readiness generation仍是完成上游startup后应传入的值，不能由bind成功授予WRITABLE。

关闭先退休新连接admission，stay-closed child group关闭现有及迟到member，再关闭parent native listener。parent关闭后在每个原worker queue中插入barrier，等待已accept而尚未register/init的socket完成交接，再复核/关闭child group并等原receive ledger实际归零。零connection count不能证明尚未入账的accepted fd已关闭；worker barrier不回收外部event loops，也不作为storage、远端abort或FREE。

原 `ProfileReceiveBudget.closeAdmissionAndDrain` 拒绝新connection与新unowned reserve，不调整任何已持有credit/slot。已接纳、尚未close的connection继续按原上限申请charged TLS close/output backing，不能靠退休绕过分配记账或拒绝必要的closing buffer；closed connection仍不能新reserve。frame/source/response/backing/derived view独立等最后实际归还，所有lane bytes与connection slots为零才实际完成。完成callback在内部ledger monitor之外，不引入轮询或额外调度队列；future副本避免caller伪造actual完成。

本终态覆盖owned native listener/child channels及原ledger可见的network/cold/response custody；存储pool/DATA I/O/owner仍须由caller随后执行original graph drain/close，cold pool和borrowed loops在其所属全部工作终结后再关闭。它不补齐JDK/native TLS内部对象、kernel buffers、全索引/storage bytes或完整总账，也不保证远端未写入。关闭/barrier失败传播而不宣告成功排空；borrowed worker不得在本drain前主动停掉。

真实mTLS测试让initial INSTALL的actual authority read在原bounded cold pool中挂起，然后close listener：socket已关闭、caller等待100ms超时、future副本complete均不能回收仍持有的frame/workspace。cold实际返回后network credit drain完成，原worker/cold未被关闭；旧port拒绝连接。bind原已用port的失败不损坏borrowed graph，其原endpoint仍能完成真实all-E initialize。另在两个actual worker上暂停，使parent已accept的plain TCP socket尚未入账；connection slots全零但close仍pending，release后queued socket实际关闭，peer读到EOF。allocator回归同时持有direct parent/view与charged closing heap buffer，断连后最后view才完成drain；原decoder DATA frame在channel finish后仍保留credit直到release。

复现：

```bash
bash scripts/unified-wal/run-local-storage-tests.sh 'ProfileByteBufAllocatorTest,ProfileReceiveDecoderTest,SegmentProfileInitialSocketClientTest#ownedProfileListener*+ordinaryDataClient*+ordinarySourceQuota*+ordinaryDataRejection*+ordinaryTimeout*+ordinaryServerForce*+ordinaryReply*+writerStop*+ordinaryWriterClose*+readinessBridge*+automaticDeletePublication*'
python3 scripts/unified-wal/run-storage-models.py --tool "$UW_TLC_JAR" --config-prefix a-profile-listener-drain --config-prefix ce-profile-listener-drain --config-prefix a-profile-backing-budget --config-prefix ce-profile-backing-budget
mvn -o -P '!cargo-zigbuild' -pl bookkeeper-server -am spotless:check checkstyle:check
mvn -o -P '!cargo-zigbuild' apache-rat:check
```

父源码 `23b40fdaf89c0a50a911afa23a8534c9c6b2179b`；最初budget及listener尝试是test import缺失的compile失败，不计PASS/生产bug红。最小31项green在accept race新增前，UELuQrk8三listener/32项包含该竞争；最终LuMqtKxW三类46项（allocator7/decoder22/socket17）零failure/error/skip，1095当前inputs匹配，normal DATA/UNKNOWN、writer stop/close及原readiness/delete consumer共享回归也执行。最终style exit0；RAT仍exit1/443父HEAD历史路径，无新增，不改许可规则、不称CI全绿。

新有限 `ProfileListenerDrain` 投影将native parent/child close、worker queue交接与actual cold/output/backing事件作为Java输入；retire后重新admit、disconnect早还credit、caller完成actual、早drain和漏worker barrier五条错误规则均取得指定反例。首次hwqwy6f2的新六配置为TLA precedence SANY失败，保留为INCONCLUSIVE、不计PASS；补齐括号后最终07__nh2q十三配置为2正向完整探索/11指定反例，986当前inputs匹配。新正向374 generated/127 distinct、queue0、depth12、0.768秒/peak RSS107724800 bytes属于checker消耗，非服务性能。TLC1.7.4、单worker、heap512/direct64MiB、seed1/fp0、无symmetry/state constraint、CHECK_DEADLOCK FALSE；catalog634/完整A+C+D未全跑，模型不生产TLS/fsync/native fd或完整活性事实。

archive `/Users/liusinan/.codex/artifacts/unified-wal/profile-listener-evidence-2xq840r1` 保留stage source/config/command/checker/compile/style失败及receipt/checksum。当前完成显式listener owner与实际network credit drain，后续仍需把original required-authority/physical facts、pre-storage-open、listener端口与registration及graph一起接入完整startup owner；default bootstrap/SDK routing/full-bytes/stock matrix与综合Gate继续OPEN。步骤11/12/14/15和生产Gate保持PARTIAL/OPEN，Linux NUMA/真实掉电/正式性能本轮NOT_EXECUTED，完整目标继续active。

2026-10-12 后续 [显式 Profile 服务 owner](../startup-readiness/README.md#显式-profile-服务启动与-owner-排空)已接 frozen requirements/pre-open root binding、实际 bind endpoint、原 Harness/registration/consumer、注册发现 SDK 及 listener→storage→DLR drain。实际 force 未终结或 startup publication 晚到时保留 owner/locks，不重新开放 retired admission；58项相关 Java 与 style 通过，RAT443父 HEAD 路径无新增。listener 仍只拥有网络侧 custody，服务 owner 续接原 storage drain；default EmbeddedServer 和完整生产 authority/full-bytes/综合验收继续 OPEN。

2026-10-12 [EmbeddedServer 显式 Profile lifecycle](../startup-readiness/README.md#embeddedserver-显式-profile-分支与-lifecycle)已用原 stack 驱动实际 owner startup/bind/registration/SDK/restart，并在 Classic 构造前选择该分支。held native force 或 publication 未终结时，关闭异常阻止 stack 越过原 owner；handler 返回不能把失败启动标 STARTED。普通 DATA/UNKNOWN/close/readiness/delete 与原 Classic 回归共28项通过，配置 bootstrap/full authority/full-bytes/综合 Gate仍 OPEN。

2026-10-12 [配置与显式 bootstrap](../startup-readiness/README.md#配置转换与显式-bootstrap)现在实际 properties 经原配置对象冻结 graph 参数，原公开 authority/transport 接入 same owner/listener/SDK；多值/未知/非法配置及 ISOLATED 无 owner 在 Classic/data open 前拒绝。30项相关回归通过，不增加普通 Add 的 MetadataStore、handshake 或 control fsync；Main/owned环境/full authority/full-bytes/综合 Gate仍 OPEN。

2026-10-12 [owned ZK/metadata/cold/loops](../startup-readiness/README.md#同一服务-episode-的-owned-资源初始化与终止)已在同一服务episode复用原authority readers，原listener/graph实际drain后才确认元数据与外部执行资源终结。三节点实际SDK/原REGISTER/restart、held metadata task及partial初始化失败与原DATA/close回归共33项通过；TLS与完整Main/authority/full-bytes/综合Gate仍OPEN。


## OPEN 固定 LAC 前缀审计

2026-10-12，第14步在原 ProfileAuditor / AuditorReplicasCheckTask 的周期任务中增加 OPEN 固定前缀观察。原 CLOSED 分支、matching CLOSED 全副本 proof 和 worker 条件化清 marker 保持原合同；OPEN 分支使用同一 SDK、coordinator、registered peer pool、coordinate/duration 界限和实际 completion，不增加调度器或普通 Add 查询。

ProfileRecoveryCoordinator.beginVerifyOpenReplicas 从实际冷读冻结 OPEN metadata birth/version/完整内容、READY root、published page 和 retained source incarnation。ProfileRecoveryReadHandle.fromOpenAudit 复用原 ProfileBookieClient，只发送 flags=0 普通 READ 和原 READ_LAC；拒绝 recovery probe/packing，不发 FENCE、recovery Add、metadata close 或恢复 grant。原 SDK LAC quorum 与 matching point-read length 产生本轮固定的下界 P/L，ReplicaScan 只检查 0..P 的全部 write-set 副本、digest/payload 和连续累计 length；writer 后续增长不扩大这次扫描。

健康结果为 VERIFIED_OPEN_PREFIX/openCut，无 CLOSED ReplicaHealthProof，独立计数 profile_open_prefix_audit_healthy，不证明整个 OPEN ledger 或未确认 tail 健康。UNDERREPLICATED 只使用实际 scoped MISSING/CORRUPT。原 Bookie 缺失证明仍要求 durable fenced 与稳定读准入：活动、未 fenced 源缺少 DATA 仍返回 UNKNOWN/UNAVAILABLE，审计不主动 fence 它；已有 durable fence 的稳定缺失才可计 MISSING。权限拒绝、未知结果、预算终止或 authority 变化不生成 loss/健康证明。健康 OPEN 观察不清已有 marker，不产生新的持久 required-frontier authority。

扫描后重新冷读完整 metadata/root/retained sources；周期任务再等待 actual health 和 lease/output/cold drain，最后复核 metadata/root，稳定且有实际缺失/损坏才写原 underreplication 检查任务。caller timeout/cancel 和线程中断不提前释放共享 reservation，也不提前关闭 owned SDK/ZK。

五个新真实 mTLS/ZK/file/SDK 用例覆盖：持有 anchor READ 时 writer 继续 Add、固定 P/L 与无 CLOSED proof；实际 wire corruption、权限拒绝和 metadata CAS 变化；周期任务自动标记损坏且 writer 仍 OPEN；未 fenced UNKNOWN 与已有 durable fence 下 MISSING 的区别；超过 deadline 后 shutdown 仍等待三条实际 anchor 回复和 lease 排空。原 CLOSED 周期审计、普通 writer close、whole recovery、prefix verifier 和 SDK LAC 共享路径同时回归。

复现：

```bash
bash scripts/unified-wal/run-local-storage-tests.sh 'SegmentProfileInitialSocketClientTest#openReplicaAudit*+profileReplicaAudit*+profilePeriodicReplicaAudit*+requiredPrefix*+normalTail*+originalSdkRead*+explicitLac*+wholeRecovery*+ordinaryWriterClose*'
bash scripts/unified-wal/run-local-storage-tests.sh 'SegmentProfileInitialSocketClientTest#prefixVerifier*+closedReplicaHealth*+sdkLacEvidence*+explicitOnlyLac*+corruptExplicitLac*+lacSource*'
python3 scripts/unified-wal/run-storage-models.py --tool "$UW_TLC_JAR" --config-prefix a-open-replica-audit --config-prefix ce-open-replica-audit --config-prefix a-profile-replica-audit --config-prefix ce-profile-replica-audit
mvn -o -P '!cargo-zigbuild' -pl bookkeeper-server -am -DskipTests spotless:check checkstyle:check
mvn -o -N apache-rat:check
```

父源码 a71a23ed730be7f066eeb3128cbc8bda32276957。最终 Java 2xzvT988 的15项与 aK4pB6vU 的13项均零 failure/error/skip，各1103 source inputs 与当前文件匹配；实际执行的用例名在 XML，不把未匹配的 wildcard 当覆盖。早期 GKixWslU 的失败来自 OPEN READ 错用 HIGH_PRIORITY-only 被原 transport 拒绝及旧指标断言；修正为原 flags=0 和独立 OPEN 指标，未放宽生产协议校验。AE9QKkFq 三项、uycWr0Ej 广度结果保留阶段输入，不移绑最终源码。

最终模型 l2xb3ba7：20配置为2完整正向/18指定反例，997当前 source inputs；OPEN 正向592 generated/416 distinct、queue0、depth8、0.861秒/peak RSS109592576 bytes，CLOSED正向396/336、queue0、depth7。ProfileOpenReplicaAudit 要求 scoped known cut、固定前缀、不 fence、实际 source/final fresh、不清 marker、actual health/lease 排空；真实 LAC/ZK/TLS/fsync 等作为输入，不生产这些事实，不检查完整时钟/fairness/liveness。TLC1.7.4、单worker、heap512/direct64MiB、seed1/fp0、无 symmetry/state constraint、CHECK_DEADLOCK FALSE。checker成本不是服务性能，catalog644/完整 A+C+D 未全跑，原 CLOSED 模型与历史回执不重绑。

持久证据 /Users/liusinan/.codex/artifacts/unified-wal/open-audit-evidence-ah8rwjp3 保留原 run/source/config/command、失败与阶段身份；最终 Java/model 去重1841输入匹配。Spotless/Checkstyle exit0；RAT exit1、443 unapproved路径全部存在父HEAD，无新增，不称CI全绿。额外 required authority producer、任意大 ledger 审计、payload/placement、全链路字节预算、完整 Main/外部 authority/stock矩阵、snapshot/handoff/history GC/decommission、兼容/正式性能/运维和综合验收继续 OPEN。步骤11/12/14/15与生产Gate保持PARTIAL/OPEN；Linux NUMA/真实掉电/正式性能本轮NOT_EXECUTED，默认OFF/SYSTEM/PARK及disabled能力保持。

## 网络 WRITE_LAC 与确认前缀生产

2026-10-12，第11/14步接通已有 WRITE_LAC opcode/codec、本地 EXPLICIT_LAC control record 与真实 SDK。新增独立实验 HELLO capability UWL1（0x55574c31/version1），按原递增顺序编码；只在显式 control 配置下 advertise。原 decoder 在分配 body 前要求该 CRC32C 首版请求恰为116字节，正文仍是既有 context60/LAC8/credential24/bodyLength4/原 BK digest20；opcode/body codec/控制持久化格式及 URL1/URL2 不变。未协商能力不发送或执行写入，返回 unsupported/NONE，不降级 Classic。

SegmentProfileAddHandler 验证原 ledger/LAC CRC32C digest、非负LAC、外层与digest同值，再验证 actual writable admission、installed instance/credential/active状态。它复用原 SegmentBookieControlDriver 的 reserved control I/O、force 和 owner apply；只有实际 APPLIED_DURABLE/ALREADY_DURABLE 返回成功。force失败或结果不确定保留 UNKNOWN，无前进、回滚或伪造 NONE。相同字节重复不再次force；旧值/冲突拒绝，fence后只能解析既有同值，不接受更高LAC，TOMBSTONE/身份变化仍拒绝。连接仍使用原 DATA receive/RPC额度，控制文件I/O使用既有预留，不增设队列/线程或普通Add往返。

ProfileLedgerHandle.flushConfirmedLac 在原 ordered SDK executor 冻结自己的 lastAddConfirmed 和当前ensemble，复用原 DigestManager、PendingWriteLacOp 和distinct ACK quorum；空/停止writer拒绝。固定cut和digest随同一次操作发送，API不接受caller指定未来frontier。ProfileBookieClient复用原 bounded delivery/retry/status分类和实际drain集合，ProfileSocketClient只借用20字节digest且在真实output/write/wire终态归还slot。A个成功或caller取消不提前结束其他目标的actual custody，writer stop/close仍等这些借用终结；transport结束不证明远端I/O已取消或durability NONE。explicitLacInterval仍要求0，本增量为显式调用，不新增周期flush策略。

该真实producer把已经SDK确认、尚未piggyback传播的尾entry作为durable explicit LAC交给既有 URL2/quorum/point-length/required-prefix消费路径。explicit LAC不携带length，不免除连续prefix验证；后续speculative payload的较低提示不能抹去已接受LAC，不能跨hole关闭。它不创建strong token、repair-completion/reset authority，不接管默认SDK路由。

六个新真实socket/SDK测试覆盖A2/A3确认prefix、required frontier与speculative suffix、实际文件reopen后保留explicit/fence、CRC/credential/instance/同值/fence边界、force前distinct AQ门槛、force失败UNKNOWN、丢回复fresh socket零重复force、未支持capability、caller取消时held force与已durable但held reply的actual drain。原required-loss/UNKNOWN/无authority suffix oracle改为使用网络producer；其他corrupt-read用例仍由既有底层故障输入产生坏explicit，未放宽网络校验。

共享回归纠正两处旧测试断言：旧 socket helper 只期待最初两个HELLO capability，更新为当前完整8项精确列表；FREE拒绝文本已随既有terminal exclusion/applied successor合同扩展，改为核对live record原因。活数据拒绝、grant保留、完整terminal条件下FREE及full/covered restart行为断言保留，生产回收代码未改。

复现：

```bash
bash scripts/unified-wal/run-local-storage-tests.sh 'ProfileOperationCodecTest,ProfileFrameCodecTest,ProfileHelloStatusTest,ProfileLacReplyCodecTest,ProfileReceiveDecoderTest,SegmentBookieControlLogTest,SegmentProfileInitialSocketClientTest#writeLac*+prefixVerifier*+sdkLacEvidence*+explicit*Lac*+corruptExplicitLac*+lacSource*+openReplicaAudit*+profileReplicaAudit*+profilePeriodicReplicaAudit*+closedReplicaHealth*+ordinaryWriterClose*+wholeRecovery*+originalSdk*+ordinaryDataClient*'
python3 scripts/unified-wal/run-storage-models.py --tool "$UW_TLC_JAR" --config-prefix a-profile-write-lac --config-prefix ce-profile-write-lac --config-prefix a-profile-lac-evidence --config-prefix ce-profile-lac-evidence --config-prefix a-profile-normal-client-source --config-prefix ce-profile-normal-client-source
mvn -o -P '!cargo-zigbuild' -pl bookkeeper-server -am -DskipTests spotless:check checkstyle:check
mvn -o -N apache-rat:check
```

父源码9ebcdbdb5146df61243b2a4e0a05259ba63b8028。最终 oKgwCo3S exit0，7类337项零failure/error/skip（common codec/HELLO/LAC18、decoder23、Bookie control260、socket定向36），1103当前inputs匹配；JDK为本次实际Corretto21.0.10，不重绑旧Java17结果。lzDjgecH为proto enum引用compile失败；80wvFV2o的5项socket错误为开发期HELLO排序拒绝、decoder1通过；7QK3wOwJ定向6项通过，早于最后两个新socket用例；jfVMagDE广度337中8项旧断言失败，其余结果按阶段保留，不冒充最终全绿。

最终模型 huj366ry：29配置为5完整正向/24指定反例，1010当前inputs，Java/model去重1853；runner补锁实际复用的PendingWriteLacOp。新A2模型260405 generated/41896 distinct，A3为248663/39520，均queue0/depth21，分别1.791/1.678秒、peak RSS271597568/272793600 bytes。ProfileWriteLac有限投影检查scope/digest/confirmed cut、force前无apply/ACK、distinct AQ、UNKNOWN不计票、actual source/output/wire排空、fence后不增长与replay authority保留；真实TLS/digest/fsync/ZK作为输入，不由模型生产，无fairness/liveness/完整A+C+D或性能结论。TLC1.7.4/单worker/heap512/direct64MiB/seed1/fp0、无symmetry/state constraint、CHECK_DEADLOCK FALSE；catalog655/完整组合未全跑。3udquej5、phk5kbrp、594kpcen原stage输入保留，不移绑。

持久证据 /Users/liusinan/.codex/artifacts/unified-wal/write-lac-evidence-oucr0lk4 保留原run/source/config/command及失败阶段。最终Spotless/Checkstyle exit0；RAT exit1、443 unapproved路径全部存在父HEAD，无新增，不称CI全绿。其余accepted ACK/AQ/repair-completion producer、自动explicit策略/默认SDK routing、完整OPEN/payload/placement审计、总字节预算、Main/外部authority/stock矩阵、snapshot/handoff/history GC/decommission、正式性能/运维与综合验收仍OPEN。步骤11/12/14/15与生产Gate保持PARTIAL/OPEN，默认OFF/SYSTEM/PARK及disabled能力保持；Linux NUMA/真实掉电/正式性能本轮NOT_EXECUTED。

## 本地删除空间终结

2026-10-12，第15步新增显式本地图入口 IsolatedSegmentStorage.reclaimDeletedLedger / SegmentProfilePhysicalDelete。它只接受匹配的实际本地 terminal TOMBSTONE，借用既有 bounded cold executor、原 index checkpoint/COPY/MOVE/FREE 与 owner/I/O服务；无新增graph线程/队列、普通Add远端查询或持久格式。caller排队deadline使用JDK共享timeout服务，每graph最多一个活跃attempt；未把该timer纳入完整进程字节证明。REUSABLE仅表示该instance的旧allocated空间已不再保留、可由Arena内部复用，不表示filesystem shrink、secure erase、其他目标完成、强访问撤销或history GC。

先完成原终态native row清理、原owner coordinate归还与durable coverage，再在原Bookie/files reclamation protection及borrowed scratch下检查当前allocation原始字节。检查包括terminal DATA和已失去selector的旧RELOCATION_COPY，不能由index miss、current selector或cursor推断物理终结。首次cut在原DATA/read I/O实际quiescent后冻结，按控制顺序的16-allocation窗口遍历；既有validateAllocationIdentity在持久化前强制file=ownerShard%dataFiles，原startup/compaction单当前stream-generation合同继续适用。未知/损坏、不可用文件、未排空义务或超出总扫描/compaction/duration界限均非成功。

含该instance的窗口交给原compactor完整捕获/recapture、COPY force、条件MOVE、fresh FREE proof及control force/apply；随后重新扫描至无该instance的allocated copy。混合块为了真正退役旧块，可在该显式physical请求中接受零净容量增益；普通容量维护仍要求正净增益。其他ledger活数据必须由原COPY/MOVE保留，destination与source退休bytes分开返回。统计只累计本轮完成的RECLAIMED组，sourceBytesRetired包含共享旧allocation的空间，不等于该ledger的payload bytes；重复/新attempt从实际durable FREE/generation重读，不依赖旧内存计数。

caller result与actual completion独立；JDK completeOnTimeout覆盖cold排队时间，PENDING/取消停止未来阶段；deadline/cancel只停止未来阶段，不撤销已接受的index/COPY/force/FREE。原SegmentOwnerLoop新增无future分配的可选terminal观察点，在实际completion slot归还后调用；physical index检查等待全部原worker终态、compaction等待原ticket终态，scratch/guard及singleton仍保持到child终结。UNKNOWN保留实际exception原因；没有把target超时解释为NONE/decommission，也没有在此API清metadata、target obligation或tombstone。

真实mTLS/ZK/SDK普通close→logical delete→UIC7 force/apply之后，本地入口覆盖非空/空ledger、wrong instance/尚未apply的其他target拒绝、旧file容量不变与healthy reopen；native混合allocation验证普通NO_NET_GAIN及physical零净增益COPY/FREE、其他ledger继续可读写和restart；corrupt原始DATA且index已清理仍UNKNOWN，总扫描budget不足仍LIMIT并可从durable状态重试。持有FREE force时deadline/cancel仍保留文件保护/actual completion，重复请求不抢singleton；释放后新attempt观察已经完成的退休。独立真实owner/IO用例验证终态观察点之前slot不可借用，之后原slot可用；真实cold队列阻塞时caller按时PENDING，singleton/completion保持到排队工作退出，超时任务不新增I/O。

复现：

```bash
bash scripts/unified-wal/run-local-storage-tests.sh 'SegmentBookieControlLogTest,SegmentExecutionGroupTest#terminalObserver*+allOwnerPools*+controlIoRuns*+crossThreadReturn*+partialPool*+fullDataAdmission*+entryInputsShare*+bufferBudget*+unavailableAffinity*,SegmentProfileInitialSocketClientTest#physicalDelete*+committedDelete*+automaticDelete*+logicalDelete*+preparedDelete*'
python3 scripts/unified-wal/run-storage-models.py --tool "$UW_TLC_JAR" --config-prefix a-profile-physical-delete --config-prefix ce-profile-physical-delete --config-prefix c-compaction-group --config-prefix c-terminal-index --config-prefix ce-terminal-index --config-prefix c-whole-free --config-prefix ce-whole-free
mvn -o -P '!cargo-zigbuild' -pl bookkeeper-server -am -DskipTests spotless:check checkstyle:check
mvn -o -N apache-rat:check
```

父源码c78c32f4bad940d217685f781742ccdcf3529f99。最终IcyjZt0h exit0，3类282项零failure/error/skip（Bookie control262、owner9、socket11），1104当前inputs匹配；本机Corretto21.0.10，不移绑旧Java17结果。H0ei3tBu仅compile成功、selector未匹配测试，不计行为PASS。开发期YaZ5CMdK/Iw87Qk6F/aWWB6U00保留原失败：背压/未等actual结束、空cut假设、错误辅助方法/未释放read lease、owner配置与扫描budget口径；mKTVVOfP是引用不存在helper的testCompile失败。9ntIcil6五项通过早于后续范围核对；BVcDXkYN广度282仅一项off-route用例被既有allocation身份校验拒绝，该不可达假设已删除，没有放宽持久化校验。pvEXp1aO的281项早于排队deadline调整，x_0tvuaq保留该阶段模型绑定；其余旧scope、结果和源码绑定冻结。

最终模型34tuje3n，20配置为4完整正向/16指定反例，1020当前inputs；新ProfilePhysicalDelete检查scope/known bytes、不能index/selector-only跳过allocated copies、COPY保留其他ledger、FREE force、old I/O与actual owner credit终态。正向110 generated/78 distinct、queue0、depth12、0.832秒/peak RSS106938368 bytes；结合原compaction-group/terminal-index/whole-FREE，Java/model去重1864输入核验。TLC1.7.4、Corretto21、单worker、heap512/direct64MiB、seed1/fp0、无symmetry/state constraint、CHECK_DEADLOCK FALSE；真实TLS/ZK/CRC/fsync为输入，无full lifecycle/fairness/liveness/性能结论，catalog664/完整A+C+D未全跑。jvuqmgpu/c47vx6lw/x_0tvuaq的旧projection/stage不重绑。

持久证据 /Users/liusinan/.codex/artifacts/unified-wal/physical-delete-evidence-ejqsebvi 保留原run/source/config/command、失败与阶段身份。最终Spotless/Checkstyle exit0；RAT exit1、443 unapproved路径全部在父HEAD，无新增，不称CI全绿。网络physical query/receipt、全historical target终结、accepted decommission/unrecoverable、retention policy与history/journal snapshot/handoff/GC仍OPEN；旧义务全部保留。本地保守quiescence与显式limit不证明任意持续负载下完成。Main/默认routing/外部authority/stock矩阵/full-bytes/正式性能/运维/综合验收继续OPEN，步骤11/12/14/15与生产Gate保持PARTIAL/OPEN；默认OFF/SYSTEM/PARK和disabled能力保持，Linux NUMA/真实掉电/正式性能本轮NOT_EXECUTED。

## 网络删除空间查询与全目标观察

2026-10-12，父源码04bfae5221之后新增独立UIC8 capability 0x55494338/version1，在冻结OPERATION_STATUS控制opcode上只开放显式有界physical-delete请求。ProfilePhysicalDeleteCodec以24字节前缀封装原UIC7的完整删除事件；最多4096次allocation扫描、256组compaction、60000毫秒。固定96字节reply包含原status、REUSABLE/NOT_DELETED/PENDING/LIMIT/UNKNOWN、计数/字节、Bookie/Arena cuts、完整canonical请求SHA-256和CRC32C。HELLO仍固定bookie/incarnation，客户端另检验requestId/op/flags/status及request摘要；UIC8未协商时不发physical请求。没有改变UIC1–7、持久化control格式或普通Add路径。

服务端复用原IsolatedProfileControlEndpoint的mTLS principal、static permission、直接提交权威、exact scope与engine检查；只读protected adapter要求SegmentBookieControlLog中同一operation/scope/DeleteBinding已持久应用，且同一ledger instance有terminal TOMBSTONE。当前deleteApplied cursor不能替代该事件，也不在此请求中补应用/跳过UIC7。原event-loop transfer先取得CONTROL credit再启动SegmentProfilePhysicalDelete；原index/COPY/MOVE/FREE与fresh raw allocation inspection拥有本地物理事实。报告REUSABLE前再次直接读同一删除权威和本地事件；权威消失或变化，即使FREE已完成，也只报告UNKNOWN。PENDING/LIMIT/UNKNOWN不伪造NONE；未授权、未协商、body非法或未持久应用的pre-effect refusal不启动回收。

ProfileInitialControlClient.PhysicalAttempt把waiting result与实际本地RPC/source/output终态分开。取消/回复丢失不撤销已接受的server I/O；server的cold/control/receive custody保留到实际child与输出promise终态。caller RPC completion不是remote FREE完成证明；本地REUSABLE仍仅表示Arena可复用，不是文件缩小、secure erase、强reader barrier或生产删除完成。

ZKProfileReservationStore新增readLogicalDeletion及readLedgerDeleteDelivery，只读核验原metadata birth/version/CLOSED digest、DELETE_INTENT、logical reference、manifest/publication及实际target-index/stream event。ProfileDeleteCoordinator.beginObservePhysicalDeletion冻结完整历史manifest，逐个按bookie/incarnation匹配已有verified CONTROL peers，复用bounded cold与SDK scheduler。缺peer保留PENDING，拒绝/UNKNOWN不成为成功；只有全部历史targets返回同一事件绑定的REUSABLE、且最终logical manifest仍相同时，才报告REUSABLE_EVERY_TARGET。源数据损坏报告QUARANTINED，未取得事实保留PENDING；没有AQ替代全目标、删offline obligation、补投递或产生GC权限。该结果是ephemeral observation；durable per-target terminal receipt、accepted decommission/unrecoverable和retention/snapshot/handoff/history GC仍OPEN。

真实ZK/mTLS/SDK/file用例覆盖同事件FREE/重复零FREE、未apply其他target拒绝、部分peer与全目标观察、错误incarnation cohort拒绝、held FREE下PENDING/UNKNOWN及server file/receive custody、回复丢失重连、取消等待实际wire/output、CRC正确但request摘要错误拒绝、独立cap缺失无authority read/无retirement，以及FREE期间实际committed event消失后的UNKNOWN与协调器QUARANTINED。UIC1–7/原control admission共享回归保持原断言。

验证使用Corretto21.0.10；bpv6hst8阶段9类349项Java全部通过（0 failure/error/skip），保留其原source绑定。5个文件仅作换行/导入修正后，vfC4BlUq最终8类70项通过，1106源码输入逐SHA核验；未重新执行的262项Bookie全类结果仍属于原阶段，不移绑。最终utiasjpr的1030输入、24模型配置（3完整正向/21指定反例）与Java去重1874输入核验一致；新正向4765 generated/3684 distinct、queue0、depth8，0.901秒/peak RSS131235840 bytes。catalog671/完整A+C+D未全跑；TLC1.7.4、单worker/heap512/direct64MiB/seed1/fp0、CHECK_DEADLOCK FALSE，不提升fairness/liveness或性能Gate。

重跑入口仍为原脚本，具体命令/各阶段inputs与原XML、模型配置/反例在证据目录保留：

```bash
bash scripts/unified-wal/run-local-storage-tests.sh 'ProfilePhysicalDeleteCodecTest,ProfileInitialControlCodecTest,ProfileFrameCodecTest,ProfileHelloStatusTest,ProfileOperationCodecTest,IsolatedProfileControlEndpointTest,ProfileReceiveDecoderTest,SegmentProfileInitialSocketClientTest#physicalDeleteControl*+newPeerInactive*+lostReplacementSocketResponse*+verifiedHello*+ownedProfileListenerWaits*'
python3 scripts/unified-wal/run-storage-models.py --tool "$UW_TLC_JAR" --config-prefix a-profile-physical-wire --config-prefix ce-profile-physical-wire --config-prefix a-profile-physical-delete --config-prefix ce-profile-physical-delete --config-prefix a-profile-delete-publication --config-prefix ce-profile-delete-publication
```

证据目录 /Users/liusinan/.codex/artifacts/unified-wal/physical-wire-evidence-oi3enfjt 保留最终source、原阶段5文件、命令/输入/XML、模型与诊断失败；旧PASS不移绑。最终Spotless/Checkstyle exit0；编译/测试完全终结后RAT独立exit1、443 unapproved路径全部存在父HEAD，无新增，不称CI全绿。完整16步与步骤11/12/14/15、生产Gate仍PARTIAL/OPEN；Linux NUMA、真实掉电、正式性能/canary本轮NOT_EXECUTED。

## 删除物理终结记录与持久接续

2026-10-12，父源码2bee47a83d之后新增ProfileDeletePhysicalReceipt（UWPT/schema1）：固定132字节外壳，加有界原Event和完整UIC8 request，保留固定96字节REUSABLE reply、nonce和独立CRC32C。event包含ledger context、target/incarnation、delete request/epoch、stream/assignment/sequence与logical tombstone reference的birth/digest；request保留原UIC7 scope/DeleteBinding及有界参数，reply仍由实际socket的request/op/flags/摘要校验拥有。package-private collector仅接受同一verified HELLO/capability/engine及同事件REUSABLE；PENDING/LIMIT/UNKNOWN、另一incarnation或另一event不产生记录。stored decoder只检验语法和绑定，不生产TLS/authority事实。

原ZKProfileReservationStore在既有delete-targets下按完整target/incarnation key持久保存一条有界physical row及同multi birth child。byte body不可改写，child绑定nonce、body SHA-256及同一creation zxid；发布复用recoveryRootChecks、原metadata membership birth/version checks、frozen manifest/reference birth、实际delivery index、stream assignment/birth和event。conditional multi之前再次检查live；之后从实际metadata重读，未知提交结果只查询同一记录。已存在的matching row保留第一次request/cuts/计数，重复查询、不同本地query limits或coordinator重建不覆写它。每次读仍要求同一logical manifest、metadata birth/CLOSED seal、原提交event/index和receipt birth guard；缺失/recreate/corrupt不计为终结。row只属于原Profile namespace owner的实验接受边界，不是外部wipe/decommission attestation。

ProfileDeleteCoordinator.beginAwaitPhysicalDeletion复用同一bounded cold、SDK timer、active custody和原CONTROL peers；beginObservePhysicalDeletion保持只读ephemeral语义。新入口先查询每个完整历史target的匹配持久记录，缺记录且缺peer继续PENDING，有peer时只从实际UIC8 REUSABLE写记录；最终再次逐target重读全部row和logical manifest后，才能返回DURABLY_REUSABLE_EVERY_TARGET。实际Bookie已离线时，只有此前有匹配记录的同一incarnation可接续；timeout、cursor、最终ensemble map和AQ不能替代它。该结果表示本实验scope的Arena复用终态，未保存或宣称secure erase、强访问屏障、accepted decommission/unrecoverable或history-GC权限。row中的bytes/counters来自所保存的具体attempt，不是ledger累计payload/文件系统返还量。

waiting future取消不会释放仍在实际cold procedure/本地RPC中持有的资源；已接受的ZK multi或server I/O可晚于取消提交，后续从原native状态解析。completion是实际collector return与本地RPC/source/output drain，不能用它证明远程ZK请求撤销或remote FREE终止。native held FREE下PENDING不会写记录；释放后新attempt从持久FREE/generation重读再收集REUSABLE。native ZK multi确已提交但回调CONNECTIONLOSS、或回调held期间caller cancel，不丢弃或重复发布第一次记录。

真实mTLS/ZK/SDK/file组合覆盖：部分target持久进度→新store/coordinator重建→原target实际关连接/listener/storage→其余target回收→所有target实际离线后无peer接续；缺peer无记录保持PENDING、完整target义务/tombstone不删除；错误incarnation、receipt guard缺失/重建、相同metadata bytes/version/guard内容但physical birth重建均拒绝；实际multi回复丢失与caller cancel/held cold completion解析同native row；held FREE/PENDING零row后再恢复。独立codec测试仅拥有canonical/bounds/CRC/绑定/non-REUSABLE拒绝，不能代替实际producer。

G69bT181阶段在Corretto21.0.10执行6类105项Java，0 failure/error/skip；保留原1108输入绑定。随后加强metadata ABA fixture，原bytes、version和guard内容全部还原，仅physical birth改变；最终hEWWqxvs的2类2项通过，1108输入逐SHA核验，未变更生产代码。最终6l4xfie0执行31模型配置（4完整正向/27指定反例），1040 inputs与Java去重1884一致。新ProfilePhysicalReceipt正向6787 generated/2360 distinct、queue0、depth19，0.941秒/peak RSS131678208 bytes。TLC1.7.4、单worker/heap512/direct64MiB/seed1/fp0，无symmetry/state constraint，CHECK_DEADLOCK FALSE；catalog678/完整A+C+D未全跑，不形成fairness/liveness或性能结论。此前wDdB9uvF中metadata测试未先移除真实birth children而触发NotEmpty，修正fixture后保留原拒绝断言；失败不计PASS，不移绑旧阶段。

```bash
bash scripts/unified-wal/run-local-storage-tests.sh 'ProfileDeletePhysicalReceiptTest,ProfilePhysicalDeleteCodecTest,ProfileInitialControlCodecTest,ZKProfileLifecycleStoreTest,ProfileLedgerMetadataStoreTest,SegmentProfileInitialSocketClientTest#physicalDelet*+committedDelete*+automaticDelete*+logicalDelete*+preparedDelete*+ordinaryWriterClose*'
python3 scripts/unified-wal/run-storage-models.py --tool "$UW_TLC_JAR" --config-prefix a-profile-physical-receipt --config-prefix ce-profile-physical-receipt --config-prefix a-profile-physical-wire --config-prefix ce-profile-physical-wire --config-prefix a-profile-physical-delete --config-prefix ce-profile-physical-delete --config-prefix a-profile-delete-publication --config-prefix ce-profile-delete-publication
```

证据 /Users/liusinan/.codex/artifacts/unified-wal/physical-receipt-evidence-aurhprzt 保留最终源码、105项阶段的原test文件、原命令/inputs/XML、模型与诊断阶段，旧PASS不移绑。最终Spotless/Checkstyle exit0；编译/测试终结后的独立RAT exit1，443 unapproved路径全部存在父HEAD，无新增，不称CI全绿。retention/offline/backup-restore policy、accepted terminal decommission/unrecoverable、stream/journal snapshot/handoff及history/page GC仍OPEN；本实现不清旧义务、删除stream event或压缩tombstone。Main/默认routing/外部authority/stock矩阵/full-bytes/正式性能/运维/全16步验收继续OPEN，步骤11/12/14/15与生产Gate不提升。默认OFF/SYSTEM/PARK和disabled能力不变；Linux NUMA、真实掉电、正式性能/canary本轮NOT_EXECUTED。

## 删除流完整 snapshot 与真实启动追赶

2026-10-12，父源码6bf92b4a3f之后新增ProfileDeleteSnapshot（UWDX root/UWDY page，schema1）：64 effects/page、最多64 pages，覆盖原stream最多4096个连续event；最多64个snapshot generations。root绑定完整target/incarnation、原stream/assignment generation、covered-through、snapshot generation/nonce及全部page SHA-256。每页保留原完整Event和original event/index creation zxid，顺序/数量/目标/stream/digest/CRC必须匹配；不能用digest-only root、缺page或cursor替代effect。该snapshot保存完整prefix，不在此阶段去重/裁剪历史或改变generation1的assignment。

原ZKProfileReservationStore的显式cold publish先从真实committed delivery与per-ledger index收集完整prefix，再按固定generation创建不可变candidate/root birth child和有界pages，最后以原snapshot-current的版本CAS发布，并同multi创建published witness。selection version必须等于snapshot generation-1，published witness绑定candidate physical birth/body SHA，且witness creation zxid必须等于selection修改zxid；所有page都须在root创建后、publication之前创建。Root/page/selection或原index重建，即使bytes/version/CRC相同，也不直接成为新authority。部分准备保持inert；取消停止未来stage，丢发布回复从native状态解析。并发claim同一generation时保留一个健康candidate，竞争方可重读/接续，不把合法赢家当作corruption。发布不删除journal或旧snapshot，head普通增长继续保留完整suffix。

readDeleteSnapshot完整核验全部pages、root birth和publication witness，二次确认同一selection，才构造private native DeleteSnapshotView。原readDeleteDelivery/authority reader在journal event缺失时，仅可使用matching已发布snapshot中同一sequence的完整Event和original birth，并继续核验原per-ledger index、metadata physical birth/CLOSED seal、logical tombstone及exact scope。已有journal event若corrupt/recreated仍拒绝，不能由snapshot静默覆盖。prefix查询不能越过cut，suffix仍从原journal无洞读取。

IsolatedSegmentStorage.catchUpProfileDeletes在本次cold attempt中只加载一次完整view，复用原authority reader与原Bookie TOMBSTONE/delete-applied type13 write/force/owner apply；没有新local cursor格式或持久派生队列。逐条effect达到durable apply和实际owner terminal后才推进cursor，LIMIT/UNKNOWN保留未完成项；snapshot本身不能跳过TOMBSTONE，仍不等待physical compaction/FREE。捕获view只属于本次attempt，不新增全局cache；selection变化导致重读/未就绪，不能扩大scope。ProfileDeleteCoordinator.beginSnapshot复用原bounded AbortPolicy cold、SDK timer、active custody，waiting cancel/deadline与实际cold return分开，不把completion称为远程ZK撤销证明。

真实ZK/mTLS/SDK/file验证覆盖非空ledger44＋非空ledger45：snapshot prefix1发布后head增长到2，在可丢弃fixture中移除event1，healthy重开从snapshot逐条应用第一份TOMBSTONE，maxEvents=1只到LIMIT/cursor1，随后从真实suffix达到cursor2；snapshot generation2仍不改变stream/assignment generation1。实际65个已初始化、普通close和logical delete的ledgers形成两个native pages，maxEvents64拒绝部分发布，第二page缺失在任何bootstrap effect前拒绝。缺page、同bytes/version page或selection重建、原index重建均不bootstrap；native publication回复丢失、page回调held期间caller cancel/actual drain、原candidate接续及两个实际publisher并发也保留原journal和local cursor义务。Fixture默认READ只授权44，新用例仅显式加入实际READY安装结果的第二context；不是注入successful source/fence/receipt。此前错误读取尚未生成的recovery context造成fixture失败，修正后原数据/TOMBSTONE断言保留，失败不计PASS。

最终SWrojBUj在Corretto21.0.10执行7类111项Java，0 failure/error/skip，1110输入逐SHA核验；r7gvztdd执行23有限模型配置（3完整正向/20指定反例）、1051 inputs与Java去重1895一致。新ProfileDeleteSnapshot正向358 generated/164 distinct、queue0、depth13，0.952秒/peak RSS110624768 bytes；TLC1.7.4、单worker/heap512/direct64MiB/seed1/fp0，无symmetry/state constraint，CHECK_DEADLOCK FALSE。catalog686/完整A+C+D未全跑，不作完整fairness/liveness、长期容量或性能结论。

```bash
bash scripts/unified-wal/run-local-storage-tests.sh 'ProfileDeleteSnapshotTest,ProfileDeletePhysicalReceiptTest,ProfilePhysicalDeleteCodecTest,ProfileInitialControlCodecTest,ZKProfileLifecycleStoreTest,ProfileLedgerMetadataStoreTest,SegmentProfileInitialSocketClientTest#deleteSnapshot*+physicalDelet*+committedDelete*+automaticDelete*+logicalDelete*+preparedDelete*+ordinaryWriterClose*'
python3 scripts/unified-wal/run-storage-models.py --tool "$UW_TLC_JAR" --config-prefix a-profile-delete-snapshot --config-prefix ce-profile-delete-snapshot --config-prefix a-profile-physical-receipt --config-prefix ce-profile-physical-receipt --config-prefix a-profile-delete-publication --config-prefix ce-profile-delete-publication
```

证据 /Users/liusinan/.codex/artifacts/unified-wal/delete-snapshot-evidence-8dhv920k 保留最终source/命令/inputs/XML、模型与诊断阶段，旧PASS不移绑。assignment G+1 PREPARED/effective handoff、retention/offline/backup-restore policy、accepted decommission/unrecoverable、journal prefix/history/page/tombstone GC仍OPEN；本API没有删旧event或放弃任何target义务。Main/默认routing/外部authority/stock矩阵/full-bytes/正式性能/运维和全16步综合验收继续OPEN。默认OFF/SYSTEM/PARK、disabled能力及生产Gate不提升；Linux NUMA、真实掉电、正式性能/canary本轮NOT_EXECUTED。
最终Spotless/Checkstyle exit0；编译/测试完全终结后的独立RAT exit1、443 unapproved路径全部存在父HEAD，无新增，不称CI全绿。

## Assignment handoff 的 PREPARED 与投递 cut

2026-10-12，父源码a6dae28dda之后新增ProfileDeleteHandoff的有界UWHP/schema1计划与UWHA/schema1 admission。计划绑定stable target/incarnation、原Head/stream/assignment、head physical birth、next generation、操作ID和已完整发布snapshot的generation/birth/SHA；空stream以实际Head through0表示无prefix，没有虚构snapshot或local cursor。此节只实现G+1 PREPARED的必要前置协议；本地持久adoption由下节接续，native catch-up ACK、EFFECTIVE CAS和old-route retirement仍未实现，不能把计划或freeze当成切换完成。

原ZKProfileReservationStore为每个existing shared stream初始化assignment-admission，active G1初态与stream/target digest绑定。handoff先核验完整native snapshot及每个effect的authoritative LOGICALLY_DELETED/COMMITTED状态，再在同一个ZK multi中check原head version/birth与snapshot selection/root，更新admission version并创建不可变handoff-prepared。plan creation zxid必须等于admission修改zxid，plan/body digest和原head/cut必须仍一致；重建bytes/version不能冒充原准备事务。当前G1 Head和immutable assignment没有改变，旧event/index/metadata/target义务继续保留。

原prepareDeleteDeliveries主路径和legacy append路径在创建新event时共同读取admission，并把同一版本Op.check加入原event/index/head multi：旧publisher先赢则head version使准备冲突，准备先赢则旧admission version使迟到publisher的multi失败，不产生越过固定cut的新旧路由event。已记录event仍可读取和幂等解析；准备不阻止现有G1普通DATA或原prefix的实际TOMBSTONE应用。新的logical delete若无法完成该target的delivery准备，保持未完成，不发布普通logical成功。已有PREPARED但尚未logical activation的event是暂时的snapshot前置条件，不误标为corruption。

ProfileDeleteCoordinator.beginPrepareHandoff复用原bounded AbortPolicy cold、SDK scheduler与active custody；cancel/deadline只结束等待并停止未来准备stage，已接受ZK I/O按native状态解析，actual completion不提前随caller cancel完成。丢准备事务回复、取消期间native回调held、coordinator重建都只解析同一operation/cut；另一operation冲突。active admission缺失但已有plan、或相同bytes/version的admission新physical transaction均拒绝。

实际mTLS/ZK/SDK/file用例覆盖：ledger44已logical delete且target尚无local cursor时，G+1计划不会产生local G2 cursor，原G1 DATA writer45仍可追加且没有额外control force；对45的新logical delete不能越过cut；原44的TOMBSTONE/cursor可继续实际应用为G1。另一个真实ZK publisher读到旧admission后被暂停，准备先commit再释放它，旧multi不能生成event2。native准备回复CONNECTIONLOSS、caller cancel/held actual completion、相同计划接续、不同operation拒绝、admission ABA和空stream准备均有独立实际验证。

RLOeS31I原阶段7类114项Java通过，保留原source绑定；两文件仅换行修正后，taTxfenk最终3类6项通过，1112当前inputs逐SHA核验。最终ipces_48执行23有限模型配置（3完整正向/20指定反例），1061 inputs与Java去重1905一致。新ProfileDeleteHandoff正向106 generated/62 distinct、queue0、depth6、0.795秒/peak RSS105988096 bytes。TLC1.7.4/Corretto21/单worker/heap512/direct64MiB/seed1/fp0，无symmetry/state constraint，CHECK_DEADLOCK FALSE；catalog693/完整A+C+D未全跑，不证明完整handoff/fairness/liveness或性能。

```bash
bash scripts/unified-wal/run-local-storage-tests.sh 'ProfileDeleteHandoffTest,ProfileDeleteSnapshotTest,ProfileDeletePhysicalReceiptTest,ProfileInitialControlCodecTest,ZKProfileLifecycleStoreTest,ProfileLedgerMetadataStoreTest,SegmentProfileInitialSocketClientTest#deleteHandoff*+deleteSnapshot*+physicalDelet*+committedDelete*+automaticDelete*+logicalDelete*+preparedDelete*+ordinaryWriterClose*'
python3 scripts/unified-wal/run-storage-models.py --tool "$UW_TLC_JAR" --config-prefix a-profile-delete-handoff --config-prefix ce-profile-delete-handoff --config-prefix a-profile-delete-snapshot --config-prefix ce-profile-delete-snapshot --config-prefix a-profile-delete-publication --config-prefix ce-profile-delete-publication
```

证据 /Users/liusinan/.codex/artifacts/unified-wal/handoff-prepare-evidence-13l0dgu2 保留最终source、114项原阶段两文件、原命令/inputs/XML、模型与诊断stage，旧PASS不移绑。准备中的stream不会自行恢复new delete allocation；本地adoption已由下节接续，native ACK及EFFECTIVE协议仍明确保持OPEN，未提供人为取消freeze或伪造ACK的绕过。retention/accepted terminal decommission/unrecoverable、old route/journal/history/page GC及Main/默认routing/外部authority/stock矩阵/full-bytes/正式性能/运维/全16步验收继续OPEN。默认OFF/SYSTEM/PARK和disabled能力不变，生产Gate不提升；Linux NUMA、真实掉电、正式性能/canary本轮NOT_EXECUTED。
最终Spotless/Checkstyle exit0；编译/测试终结后的独立RAT exit1，443 unapproved路径全部存在父HEAD，无新增，不称CI全绿。


## Assignment handoff 的本地持久 adoption

2026-10-12，在PREPARED/cut之后接续本地必要阶段。`IsolatedSegmentStorage.adoptPreparedProfileDeletes`是有界cold入口：按原Bookie/incarnation读取native PREPARED完整Plan，先通过原`catchUpProfileDeletes`应用所有G1 prefix，等待真实owner终态，再重读原Plan。原`SegmentBookieControlDriver`占用既有control writer和reserved I/O；Bookie control type14 `DELETE_HANDOFF_ADOPT`以原Plan operation ID、next generation、expected predecessor generation及完整canonical Plan作为header/body。写入与force成功后才由owner顺序apply；随后再次native核验Plan，才返回`ADOPTED_DURABLE`。该状态只证明这一opened graph的本地持久准备，不是native ACK或EFFECTIVE事实，未增加wire opcode。

`SegmentBookieControlLog`只接受匹配实际storage identity且本地no-hole cursor恰好等于predecessor cut的Plan；空cut要求没有本地effect，不构造sequence0 DeleteBinding。adoption单独保存，现有cursor继续是G1，原event UUID/scope、TOMBSTONE与物理终结记录均保留。已有同一Plan按完整字节解析为ALREADY，无再次force；不同Plan或与原控制operation冲突拒绝。adoption后的新旧流event被本地拒绝，已应用event继续幂等；普通G1 DATA不受此准备阶段影响。

记录接入原journal首遍扫描、checkpoint retained operations、旋转及restart replay。checkpoint history清理不会把adoption当作旧checkpoint ID删除。重放核验原cut和完整header/body绑定，恢复的是已force/apply的Plan与原G1 effects；不把prepared next generation写成当前cursor或READY generation。空cut adoption也是拒绝同scope旧binary rollback的持久authority存在事实。

force/I/O失败不公布本地adoption并使原writer fail-closed。timeout/interruption只结束等待，`DeleteAdoptionAttempt.completion`独立等到原owner completion credit实际归还；held force解除后才可解析实际落地结果。fresh native source被替换或不可确认时不返回durable adoption成功，即使本地同Plan已保存；不会覆盖本地记录或猜测有效assignment。

验证：最终`EyoTHVLz`四类27项Java，失败/错误/跳过均0。新增五项实际文件/原ZK/SDK/网络验证包括完整prefix TOMBSTONE→独立adoption force、相同Plan免force、G1正常Add、checkpoint旋转重启、空cut重启、错误target/未完整cut、force失败、held force超时后实际终态与native admission同bytes/version ABA。原`K6RgekWM`五项阶段PASS保留原SocketTest源码，未移绑最终ABA断言。`fnkts5vj`20项有限配置为3正向完整PASS/17指定性质反例；新增adoption正向53 generated/34 distinct、queue0、depth/diameter9、1.012s、RSS106708992B，四个反例分别检验完整cut、force、checkpoint保留和真实owner释放边界。模型的native authority与物理I/O来自Java事实，不证明native ACK/EFFECTIVE、真实掉电或完整A+C+D。Java1112与模型1067输入的1911个唯一source hash在最终源码逐项核验一致；当前声明catalog698项，本次只运行20项。

```sh
scripts/unified-wal/run-local-storage-tests.sh 'SegmentBookieControlLogTest#*DeleteHandoff*+deleteHandoffAdoptionRequiresCompleteCutAndSurvivesCheckpointRotation+continuousDeleteCursor*+uncertainDeleteDelivery*+originalJournalRestoresLatestCompactedCheckpointAndCorruptionFallback+legacyCheckpoint*+bookieControlDriverKeepsOwnerResponsiveAcrossFenceWaitAndForce+rejectedControlIo*+explicitLacUsesReservedControlIoAndAppliesOnTheOwner,SegmentProfileInitialSocketClientTest#deleteHandoff*+deleteSnapshot*+committedDeleteDeliveriesShareOneIncarnationStreamAcrossTwoActualLedgers,SegmentControlFormatTest,ProfileDeleteHandoffTest'
python3 scripts/unified-wal/run-storage-models.py --tool "$UW_TLC_JAR" --config-prefix a-profile-delete-adoption --config-prefix ce-profile-delete-adoption --config-prefix a-profile-delete-handoff --config-prefix ce-profile-delete-handoff --config-prefix a-profile-delete-snapshot --config-prefix ce-profile-delete-snapshot
```

证据`/Users/liusinan/.codex/artifacts/unified-wal/handoff-adoption-evidence-pe_6hpi5`保存最终source、原stage override、命令/inputs/XML、模型完整trace及diagnostics。Spotless/Checkstyle exit0；Java与模型终止后的独立RAT exit1，443 unapproved均存在父HEAD、无新增。native ACK由下节接续；conditional EFFECTIVE、old-route retirement及连续G2 journal/registration接线仍OPEN；retention/accepted terminal decommission/unrecoverable/history GC，以及Main/默认routing/外部authority/stock矩阵/full-bytes/正式性能/运维/全16步验收继续推进。默认OFF/SYSTEM/PARK及disabled能力不变，生产Gate OPEN；Linux NUMA、真实掉电、正式性能/canary本轮NOT_EXECUTED。


## Assignment handoff 的 native ACK

2026-10-12，在type14本地持久adoption之后接续native acknowledgement。`SegmentProfileDeleteAdoption`由原storage graph在实际force/apply、owner completion credit归还及fresh native Plan复核后生成，构造器私有、factory仅原segment包可用；凭据绑定实际执行请求的opened Bookie control log、完整Plan与原type14 control sequence。原owner关闭、失败或该记录不再匹配时无法重新确认。catch-up UNKNOWN不产生该凭据；关闭与接续竞争保留原执行owner，避免从变化的graph字段取得另一owner。

`ZKProfileReservationStore.acknowledgeDeleteHandoff`的写入入口只接受该凭据。先核验当前native PREPARED、head physical birth、admission/plan事务身份、固定cut与完整snapshot，再将原head/assignment/birth、admission version、immutable Plan与snapshot publication检查加入同一native multi。该multi创建`handoff-ack`和immutable birth child；UWHK/schema1 body保存完整canonical Plan、真实local control sequence及随机birth nonce，guard绑定body SHA并要求与row同creation zxid。写入后按native状态重读，只有完整一致的首条记录返回`ACKNOWLEDGED`。

read-only `readDeleteHandoffAcknowledgement`独立复核完整PREPARED、row/body/guard birth及fresh source。row recreated、missing/changed witness、admission ABA、内容损坏或native来源无法确认时拒绝成功。匹配Plan的重复接续保留首条immutable row；coordinator重建、原节点关闭后的read-only读取和原健康文件restart后的再次确认均复用原记录。ACK记录表示已持久catch-up的原Plan事实，G1 cursor、effective assignment、原event/metadata/TOMBSTONE/target义务均保留。

`ProfileDeleteCoordinator.beginAcknowledgeHandoff`借用原SDK scheduler、bounded AbortPolicy cold executor和单active custody。取消/超时只结束等待、阻止未来stage；已接受native multi按原procedure与native重读解析。actual completion表示冷过程实际返回，不能用cancel释放其active占用。丢提交回复会查询同一native ACK；取消期间held callback解除前同coordinator拒绝下一active ACK。原ZK client负责未结束的native请求，不新增线程、队列、通用receipt框架或wire opcode。

验证：`ZSxRB1e3`最终六类32项Java，失败/错误/跳过均0；覆盖实际非空TOMBSTONE→type14→native ACK、空cut、held local force无凭据/无ACK、停止future stage、lost native reply、held native callback取消与active占用、offline read、健康文件restart接续、首条body保留及row/birth同bytes/version ABA。`0u9jO5LM`五项原阶段PASS保留两份当时source；阶段模型回执保留其原source/input范围。最终`vv22l03u`26有限配置为4正向完整PASS/22指定反例，新增ACK正向200 generated/92 distinct、queue0、depth/diameter10、0.793s、RSS107560960B；五个反例检验actual local proof、PREPARED physical identity、row birth、matching retry与actual cold release。模型把native authority与本地force能力作为Java提供的事实，不证明EFFECTIVE、真实掉电或全A+C+D。原显式模型输入清单已补入新增凭据类；Java1113与模型1075输入的1919唯一source hash在最终源码逐项核验一致。当前声明catalog704项，本次运行26项。

```sh
scripts/unified-wal/run-local-storage-tests.sh 'ProfileDeleteHandoffTest,ProfileDeleteSnapshotTest,ProfileDeletePhysicalReceiptTest,SegmentBookieControlLogTest#*DeleteHandoff*+deleteHandoffAdoptionRequiresCompleteCutAndSurvivesCheckpointRotation+continuousDeleteCursor*+uncertainDeleteDelivery*+originalJournalRestoresLatestCompactedCheckpointAndCorruptionFallback+legacyCheckpoint*+bookieControlDriverKeepsOwnerResponsiveAcrossFenceWaitAndForce+rejectedControlIo*+explicitLacUsesReservedControlIoAndAppliesOnTheOwner,SegmentProfileInitialSocketClientTest#deleteHandoff*+deleteSnapshot*+committedDeleteDeliveriesShareOneIncarnationStreamAcrossTwoActualLedgers,SegmentControlFormatTest'
python3 scripts/unified-wal/run-storage-models.py --tool "$UW_TLC_JAR" --config-prefix a-profile-handoff-ack --config-prefix ce-profile-handoff-ack --config-prefix a-profile-delete-adoption --config-prefix ce-profile-delete-adoption --config-prefix a-profile-delete-handoff --config-prefix ce-profile-delete-handoff --config-prefix a-profile-delete-snapshot --config-prefix ce-profile-delete-snapshot
```

证据`/Users/liusinan/.codex/artifacts/unified-wal/handoff-ack-evidence-e0npfd3k`保存最终source、原stage overrides、命令/inputs/XML、完整模型trace与diagnostics。Spotless/Checkstyle exit0；最终checker与Java均终止后的独立RAT exit1，443 unapproved路径均存在父HEAD、无新增。下一阶段必须接续conditional EFFECTIVE、真实G2 journal/progress/registration和old-route retirement，ACK不能单独完成assignment切换；retention/offline/backup-restore policy、accepted terminal decommission/unrecoverable/history GC、Main/默认routing/其他required authority/stock矩阵/full-bytes/正式性能/运维/全16步验收继续OPEN。默认OFF/SYSTEM/PARK和disabled能力不变，生产Gate OPEN；Linux NUMA、真实掉电、正式性能/canary本轮NOT_EXECUTED。


## Assignment handoff 的 EFFECTIVE 与 G2/G3 接续

2026-10-12接续原PREPARED/native ACK协议。`ZKProfileEphemeralRegistrationStore.makeDeleteHandoffEffective`只消费原opened storage owner的opaque adoption与该publisher绑定的实际gate/client/source。先退休原本地gate，再在原ZK multi中join readiness birth/version、原Plan/ACK/stream/admission/snapshot检查，更新head与active assignment、创建immutable UWHE/schema1 effect及同事务birth guard，并删除该session/nonce的service-info、writable与marker。旧READY投影同时改为NON_WRITABLE；这项负向投影保留原local readiness generation，不伪造新的LOCAL_READY。另一publisher的节点不能作为owned节点删除；版本竞争和未知回复按native状态解析。

shared stream与初始birth保持，head版本显式包含已完成assignment切换次数。每代Plan/ACK/effect保留：G2沿用原path，后续使用generation suffix。native head必须有完整、有界、单调cut的effect lineage；原Plan/ACK/effect body、physical birth与创建顺序均核验。旧event仍按其对应cut归属，新增event使用当前assignment。mixed snapshot的结构检查与native lineage核验分开，旧prefix不能仅因当前generation前进而失效，也不能被重新标成新event。当前仍保留所有journal、Plan/ACK/effect、snapshot pages和instance-specific义务，无GC授权。

`SegmentBookieControlLog`的type15 `DELETE_HANDOFF_EFFECTIVE`保存完整native effect，使用effect birth作为独立operation ID，并核验本地原type14 adoption和完整predecessor cut。原writer write/force成功后由owner顺序apply，checkpoint及journal首遍扫描/rotation/replay保留全部handoff记录。`ProfileDeleteDelivery.Progress`报告effective assignment与no-hole through；原last-event DeleteBinding保留其真实generation/request/UUID。空cut的进展来自原type14/type15，始终没有虚构sequence0删除event。新generation请求须匹配已应用type15，旧已应用event仍可幂等解析。

原`catchUpProfileDeletes`先应用每个handoff cut前的原event，再按native immutable witness续接type14/type15，最后消费当前suffix；UNKNOWN仅结束等待，原owner completion独立排空。`bindNextProfileRegistration`要求原gate已退休、实际effective assignment已前进，并等待旧follower实际终态后才能绑定新nonce；pending gate不能被当成退休gate替换。同assignment的原attempt不会因reconnect重新开放。

`ZKProfileStartupRequirementsStore.bindDeleteAssignments`为原immutable compatibility/device source绑定实际native assignment producer。observation的assignment generation来自同client、同Bookie/incarnation的完整native head，低于commissioned requirement或缺失/腐坏均拒绝；READY multi再次join该assignment版本。原compatibility/device envelope、birth与其他要求保持，普通head增长不改变assignment fact。新的READY仍经过原真实graph recovery、durable local receipt、persistent readiness和writable publication，不以cluster effect替代local type15/cursor。

实际证据：`LpxLOfFL`十类79项Java（server 71＋common codec 8）（失败/错误/跳过0）覆盖原组件回归、三目标G1→G2→G3、真实SDK新ledger Add/普通close、G2事件、混合G1/G2 pages、原prefix读取、control rotation及三节点真实文件restart。`LvPJJ3Ze`验证empty cut下native CONNECTIONLOSS/held callback、NON_WRITABLE窗口、type15 held force的UNKNOWN与actual completion、零effect progress和row/birth ABA；无mock force成功。历史阶段`dbRlEJFZ`、`8OjbNvZy`、`RLapJvPm`、`LO11v0Px`按原source保存，不移绑新增断言。`KtmhVCaB`最终两类21项补验覆盖明确retired状态与拒绝替换pending gate；79项阶段保留原三份source，最终新增断言不移绑。

35有限配置为5正向完整PASS/30指定反例；最终`yg5meujg`新增两次effective切换投影106 generated/61 distinct、queue0、depth/diameter18、5.947s、RSS88899584B。模型的native事务、完整bytes、physical birth与force事实由Java验证提供，不证明全A+C+D、真实掉电或生产authority。713个声明配置中本轮只运行相关35项。G1 canonical bytes保持；G2/G3的旧严格reader拒绝属于源码检查，released-decoder/stock binary兼容矩阵仍未执行。

证据`/Users/liusinan/.codex/artifacts/unified-wal/handoff-effective-evidence-_fbj01xu`保存各stage source/input/命令/XML、模型和失败诊断。最终Java1113与模型1085输入的1929个唯一source SHA逐项核验一致；所有历史stage按其source/input冻结。最终Spotless/Checkstyle exit0；最终Java/checker终止后的独立RAT exit1，443 unapproved路径全部存在父HEAD，无新增。默认OFF/SYSTEM/PARK与disabled能力不变；生产Gate OPEN。后续继续retention/offline/backup-restore、accepted decommission/unrecoverable、history/page/tombstone/lineage GC，以及Main/default SDK/其他required authority/full-bytes/兼容/正式性能/运维/全16步综合验收；Linux NUMA、真实掉电、正式性能/canary本轮NOT_EXECUTED。


最终复现命令：

```sh
scripts/unified-wal/run-local-storage-tests.sh 'ProfileInitialControlCodecTest,ProfileDeleteHandoffTest,ProfileDeleteSnapshotTest,ProfileDeletePhysicalReceiptTest,ZKProfileStartupRequirementsStoreTest,ZKProfileEphemeralRegistrationStoreTest,ZKProfileRegistrationStoreTest,SegmentBookieControlLogTest#*DeleteHandoff*+deleteHandoffAdoptionRequiresCompleteCutAndSurvivesCheckpointRotation+continuousDeleteCursor*+uncertainDeleteDelivery*+originalJournalRestoresLatestCompactedCheckpointAndCorruptionFallback+legacyCheckpoint*,SegmentProfileInitialSocketClientTest#deleteHandoff*+deleteSnapshot*+committedDeleteDeliveriesShareOneIncarnationStreamAcrossTwoActualLedgers,SegmentControlFormatTest'
scripts/unified-wal/run-local-storage-tests.sh 'ZKProfileEphemeralRegistrationStoreTest,SegmentProfileInitialSocketClientTest#deleteHandoffEffective*'
python3 scripts/unified-wal/run-storage-models.py --tool "$UW_TLC_JAR" --config-prefix a-profile-delete-effective --config-prefix ce-profile-delete-effective --config-prefix a-profile-handoff-ack --config-prefix ce-profile-handoff-ack --config-prefix a-profile-delete-adoption --config-prefix ce-profile-delete-adoption --config-prefix a-profile-delete-handoff --config-prefix ce-profile-delete-handoff --config-prefix a-profile-delete-snapshot --config-prefix ce-profile-delete-snapshot
```

原46/79分量没有扩大成整个系统验收；最终21项的目的为pending gate收紧及该组件回归。当前G2/G3 end-to-end与有限schema bounds不声称全部64代、实际掉电、stock binary或正式性能已验证。清理仅在source/raw receipt/diagnostic/checksums归档并确认原进程终止后执行精确本任务临时路径；共享targets、用户PDF/改动及其他任务产物保留。
