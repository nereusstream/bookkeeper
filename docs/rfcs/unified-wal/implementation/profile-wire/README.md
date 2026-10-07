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
