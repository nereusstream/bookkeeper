# Experimental Profile Wire — Wave 0 Block D

> **EXPERIMENTAL_TEST_MANIFEST / NOT_STABLE_WIRE / NON_PROMOTABLE / NO_AUTHORITY / DISCARDABLE**

本目录保留原 Unified WAL Wave 0 Block D 的实现锁、byte corpus、冻结的旧 decoder 反例与延期 compatibility harness；原 Block D 回执只覆盖纯 codec。后续步骤11～14在本目录记录隔离真实 TLS/socket/storage/SDK/恢复接线的独立增量，最新为 [固定prefix与last-fragment写回](#固定最后-prefix-与真实-last-fragment-写回)。这些隔离实现不授予生产启动、注册、ACK、恢复完成或删除权限。

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
