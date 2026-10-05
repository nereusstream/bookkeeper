# Experimental Profile Wire — Wave 0 Block D

> **EXPERIMENTAL_TEST_MANIFEST / NOT_STABLE_WIRE / NON_PROMOTABLE / NO_AUTHORITY / DISCARDABLE**

本目录拥有 Unified WAL Wave 0 Block D 的实现锁、byte corpus、冻结的旧 decoder 反例与保留但不再执行的 compatibility harness。Block D 只实现纯字节 codec；不实现 listener、endpoint、TLS 服务，不接入生产启动、Bookie Add/ACK、storage、registration、recovery 或 delete authority。

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
