目标：按已经确认并写入现有文档的 Unified WAL 16 步施工顺序，在本会话持续实施、验证并同步文档，逐步达到首个可保留数据生产候选的可审查交付。先实施步骤1～4，紧接步骤5～7形成真实写入—点读—重启恢复闭环，再按依赖推进8～10索引/回收/长期运行、11～15网络/MetadataStore/客户端/恢复/删除/rejoin，以及16综合验收与运维交付。不要只给建议或再次整理计划，也不要在完成一个小模块后自行停止整个目标。

SDK 入口决策：用户2026-10-25已选择显式 Profile create/open，先接 CLOSED 只读 open 与实际排空；不再为同一入口选择重复提问。normal writer create 必须有本次原子 fresh origin，不能从已有 READY/OPEN 重建 P=-1 writer。

Main 首版部署决策：用户2026-10-27已选择先限已有旧服务的受控部署；要求已有 commissioned requirements、真实旧服务 episode 及明确外部信任输入。现有 properties 中的 DRAINED/FORMAT_READY 或 mTLS 成功不自行构成 old-service/Unix access/key/policy authority；具体 issuer 与受保护输入装载仍须实现并真实验证，不伪造从未存在的旧 scope。该选择冻结首版范围，不代表 Main 已交付。

运行方式：按用户2026-10-25最新授权，本会话作为唯一实施主会话，可以使用多 agent 处理职责边界明确的子任务，由主会话统一变更集、验证和本地提交。模型与推理档按用户2026-10-25最新指定，主会话及后续agent统一使用 `gpt-6.1-sol` / `xhigh`（极高）；新agent显式指定该配置，不恢复旧模型agent继续执行。下文历史迁移记录中的旧型号及禁止子agent安排保留为历史。子agent应明确文件或只读职责边界，保护彼此改动；不通过另一CLI启动第二个编码agent，不另开独立实施会话。可以运行普通构建、测试和必要的并发测试进程。原规划会话 01a0c84a-11d9-7951-80a4-8f2a6933c139 继续负责总体规划与只读检查。保持一个持续目标，按真实证据推进，不能把里程碑完成或上下文压缩当作总目标完成。

SDK接入方式：按用户2026-10-25答复，首版采用显式 Profile create/open，先接CLOSED只读open与实际排空。默认Classic行为和缺少可信Profile环境时的拒绝保持；这不授予same-ledger writer takeover。继续实现public create时，必须持有自身取得的原子fresh reservation/lifecycle birth，不把control的matching READY续接当作新writer发行。

源码与文档交接：
- 项目是 nereusstream/bookkeeper，保存的项目名 turbo-bk，源工作区 /Users/liusinan/apps/ideaproject/nereusstream/bookkeeper。不要误用 /Users/liusinan/apps/ideaproject/GITHUB/bookkeeper。
- 用户确认的源码基线是 e8b162076fd8cfa84a11351a82caac8c5ac1f939，源分支 turbo-bk。原工作区现有三份未提交规划文档变更是本次明确的输入，不能丢弃，也不能把 origin 上旧版顺序当成最新版。
- 交接快照目录：/var/folders/vk/l_r0z80j1dj93fsrjx3zqv4r0000gn/T/unified-wal-implementation-handoff-8k5_iuhq。其中 handoff.json 记录原始源码commit、三份文档的 SHA-256；planning-docs.patch 是仅针对三份文档的补丁，目录中还保存三份完整文档。
- 2026-09-22 用户更新执行位置：直接在原仓库 /Users/liusinan/apps/ideaproject/nereusstream/bookkeeper 的 turbo-bk 分支实施，不使用worktree。规划文档已提交为 e75c67f3514d1627ea790d052e2c9226e593aa3c，基于原源码基线 e8b162076fd8cfa84a11351a82caac8c5ac1f939；无需重复导入。已有变化先辨明归属，不能 reset --hard、clean、stash 或覆盖不明变更。
- 后续源码和docs直接在原仓库 turbo-bk 同步，供规划会话检查。保留未跟踪的 BtrLog Low-Latency Logging.pdf，不复制、纳入提交或删除它。旧worktree的六个输入文件在应用迁移后全部通过快照SHA-256核验；旧安排仅为历史，不再约束执行位置。
- 当前用户授权实现和隔离验证，没有授权push、创建/更新PR或对外发消息；不要做这些发布动作。不要加 Assisted-by: OpenAI Codex trailer；用户已授权按可审查的小里程碑本地提交，只暂存本模块明确路径，不用 git add .，保持正常可审查的小变更。

实施依据与范围：
1. 以导入后的 docs/rfcs/unified-wal/implementation/README.md 为施工清单，docs/turbo-bk.md §10/§11 为导航和Gate，docs/rfcs/unified-wal/implementation/manifest.json 的 plannedWork 为规划信息；详细合同由 RFC-0001、RFC-0003、RFC-0004、RFC-0005、Spike A/B/C及Round 7拥有。步骤号只表示顺序，沿用CLIENT-1/2、UW-1～UW-7，不新增RFC、Maven模块或平行工作项体系。
2. 复用已有 ProfileDescriptor、Profile control/auth、startup/readiness reference，以及experimental wire codec/corpus。不要重做另一套同名内存框架，不把reference测试当成真实持久化/网络/MetadataStore证明。
3. 步骤1固定独立实验目录、单Arena、shared block、固定逻辑shard和一种真实I/O后端，默认 OFF / SYSTEM / PARK。实现、确定性故障测试和测量跟随每个模块交付，不到最后再补；复用现有测试及benchmark，不建设通用平台。暂未实现能力明确拒绝，不能空方法返回成功。
4. 步骤2把CLIENT-1/2作为两个独立Classic维护补丁。先得到失败反例，再修复ACK/故障域状态及全部受影响pending/slot的整体completion gate，保留未变有效ACK；覆盖4/3/2先处理write-set外slot。CLIENT-2覆盖最后旧响应、当前响应及callback未完成的回收组合，验证恰好回收一次和buffer生命周期。保留正常Add分配/CPU及timeout统计含义。它们不阻塞本地文件原型，但必须在步骤13前完成。
5. 步骤3的writer、独立parser、实验字节格式及固定向量配套交付；ENTRY_DATA与RELOCATION_COPY有可恢复区分。处理短写、sync失败、截断、generation、统一N和溢出；只选一个I/O backend，固定预创建容量满额停止，不提前开发迁移平台。
6. 步骤4按固定owner与PARK、安全显式binding、owner初始化/首次触碰的NUMA池顺序实施。复用cpu-affinity模块但不走旧acquireCore自动抢核/下线sibling策略。验证实际mask、继承、部分启动清理、非连续cpuset、socket/SMT、降级及原池归还；completion和控制有预留容量。NUMA不复制全局预算，CPU/worker数不改变持久身份。BOUNDED_SPIN最后独立测，不阻塞写入模块。
7. 步骤5～7保持Bookie/Arena两种控制owner，合批durable后按序判断并apply，再接真实DATA合批、前缀、热定位、逐entry local result、幂等、点读及restart。单进程崩溃后可读不能冒充掉电安全；提交后timeout/unknown不释放坐标，不截短多stream共享文件，恢复需验证当前必要DATA并完成必要同步。第一里程碑必须真实文件I/O闭环，不只搭接口。
8. 步骤8～10随后实现applied checkpoint/rotation、真实RocksDB派生索引及persisted coverage、全索引丢失重建、tombstone/MOVE/FREE/reuse与净回收、空间耗尽及多Arena。局部性能、真实TLS/quorum性能和带索引/回收的可持续性能分别报告，CPU pin、NUMA池、spin按A/B/C/D分别测。
9. 步骤11/12可按稳定接口开发，但真实集群成功路径联调等待对应本地存储验证。普通Add无逐entry远端MetadataStore、控制日志或handshake。步骤13～15保留身份、UNKNOWN、整体ACK gate、初始/写期activation历史、RepairIntent、fenced-close及离线清理义务，复用Classic基础quorum/recovery算法；基础恢复不能随高级Range延期。
10. DirectJournal是独立可选对照支线，不是Segment前置。dedicated extent、高级Range、rack/AZ、强撤权/strong reset、在线删除和跨Arena迁移保持disabled，除非依已有证据和实际需求另行推进；不为完成目标凭空扩大首版。
11. 当前首批仍为隔离、可丢弃实验，不能接管真实生产ACK或动生产数据目录。实施授权不等于实际生产部署授权；需要真实环境、凭据或canary部署时，先完成可本地完成的实现、验证与具体可审查交付，再明确实际缺口。缺少Linux NUMA硬件、掉电环境或正式性能环境时如实记录NOT_EXECUTED，不伪造PASS，也不能因为单项环境缺失停止其他可独立推进的工作。
12. 实际启用A/C、必要D及组合模型与实现同步验证，有限模型PASS不代替fsync/TLS/内存证据。raw old-decoder/stock-binary历史延期状态与冻结反例保留原绑定；2026-10-13已按持续目标恢复fresh隔离验证，当前PASS_SCOPED、完整G1仍BLOCKED_UNVERIFIED，发布对应stable面前必须补齐内部effect等合同范围；不以局部通过宣称完整兼容。所有historical sourceCommit、designInputsAtSourceCommit、receipt和旧结果目录保持原绑定，新验证有独立source/config/run身份。

执行记录：完整启动正文曾因目标工具的4000字符限制被拒绝（正文4111字符），因此目标工具使用引用正文的精简目标；应用迁移后工具返回无目标；随后依用户明确指令在迁移后的会话01a0c86a-7130-73b2-b68f-45d726cac59b恢复持续active目标（无token预算），没有将旧目标标记完成。原始未修改正文保留在交接备份 unified-wal-before-local-handoff-g_j26t8t，SHA-256为4e51eb6d300aec56aabf938d85f4c43bb4b26478e59d1f4ce99df4fed1a3de0d。本文件同步已被用户覆盖的工作区、提交与执行配置约束；此前单agent及固定模型安排按历史记录保留。

持续交付与检查：
- 每个模块留下实际源码、必要测试、可复现命令、测量记录、实际完成状态及未覆盖范围。按最小闭环顺序集成，避免只堆线程/控制/索引框架。
- 持续更新既有README、总体文档、owner RFC和必要manifest的当前实现状态，区分已实现、验证通过、仅静态推理及未运行；不把计划写成完成，不提升尚未闭合Gate。
- 使用中文简洁报告进展；遇到正常实现决策自行处理，不逐模块索要重复确认。目标只能在其实际完成条件满足后标记complete，不能因时间或上下文结束提前标记。
请注意，这期间不要忘了同步更新 docs中的文档，要保证文档中的内容是实现的最新版。

2026-10-08续接：原实施会话01a0c86a-7130-73b2-b68f-45d726cac59b因上下文错误中断，用户指定当前会话01a11942-d65c-7ce2-9c9c-b1bafae62da2继续同一目标和共享工作区。原未提交URL2改动已读交接并继续验证；总目标保持active，不把会话迁移当完成。用户同时明确继续与规划检查入口01a0c84a-11d9-7951-80a4-8f2a6933c139交互，里程碑发送源码SHA/实际验证/剩余范围；这不授权第二编码agent、并行修改或发布动作。

2026-10-11续接：旧实施会话01a11942-d65c-7ce2-9c9c-b1bafae62da2因context window耗尽failed并idle，用户指定新唯一实施会话01a12af4-a451-7c71-8f62-6ee31980e9ff接续完整16步合同。新会话已实际调用create_goal创建无token预算的active目标，继续gpt-6.1-sol/xhigh、单agent及原turbo-bk目录；旧会话不恢复编码，规划会话仍仅只读检查。起始HEAD核实为419cca10595f11a2b70d72f36148c6b3654788e5，本地origin/turbo-bk领先196提交，仅未跟踪PDF；历史输入锁和receipt不回退或移绑。先核对普通writer close的冻结P/length和实际DATA请求/输出promise/buffer排空，再复用既有durable terminal/metadata close，不能用扩展未确认tail的recovery代替普通close。步骤12/14/15与生产Gate保持PARTIAL/OPEN，会话接续不是总目标完成。


2026-10-11本会话实施进展：18853770fe先交付SDK writer stop的实际DATA排空；随后在原共享工作区继续普通writer close，加入同记录固定P/L cut、writer-only同长UWRC/schema2、原SELF/terminal/标准CLOSED与固定cut续接。实际测试、source/config/receipt及剩余边界由profile-wire/README.md的“普通 writer close 与固定 cut 续接”拥有。持续目标保持active；显式有界实验切片、一次提交和schema2源码拒绝检查都不是16步、生产或完整兼容验收完成，继续独立推进其他required authority、OPEN audit、full-bytes、删除/离线/rejoin/GC及综合验收。


2026-10-11继续第15步：883a42abd0之后已接显式fenced-CLOSED cluster logical delete，保留完整historical targets/离线义务、metadata seal与出生绑定tombstone、actual cold cancellation和原关闭查询；相关实现/当前证据由profile-wire/README.md“Fenced-CLOSED 集群逻辑删除与目标义务”拥有。当前尚未接本地TOMBSTONE wire/delete-applied、ordered delivery/cursor/catch-up/rejoin及physical/GC，继续这些真实剩余项与其他全16步未闭合范围；目标仍active，logical完成不代表整个目标或生产Gate完成。


2026-10-12继续第15步：01b0df6fff后已接initial共享Bookie/incarnation delete delivery journal，同一ZKmulti提交无洞event/index/head，真实跨两ledger验证及exact read消费由profile-wire/README.md“初始共享 delete delivery journal”拥有。当前尚不产生本地delete-applied/cursor或writable authority，继续接实际TOMBSTONE wire/apply、catch-up/rejoin与全16步其余未闭合项；持续目标active，投递journal里程碑不等于整个目标完成。


2026-10-12继续第15步：7d241ce0e5之后已接UIC7实际committed delivery/cold exact authority与Bookie type13 TOMBSTONE/continuous delete-applied cursor，同record持久并沿原checkpoint/rotation/restart重建；gap写盘前可重试、旧seq不回退、UNKNOWN不发布成功，实际旧I/O/read pin独立排空。源码/测试/模型及当前receipt由profile-wire/README.md“UIC7 本地 delete-applied 与连续 cursor”拥有。继续registration required-through提交cut、startup/offline catch-up/rejoin、snapshot/handoff、physical/GC/decommission及全16步其他未闭合范围；持续目标active，步骤12/14/15和生产Gate保持PARTIAL/OPEN。


2026-10-12继续第15步：aac954a0f5之后已接fixed initial shared assignment/head的实际catch-up和原writable multi required-through cut，从healthy recovered control cursor校验；实际磁盘restart、DLR/seq2注册竞争、LIMIT/UNKNOWN/actual local completion与lost registration reply同session解析由profile-wire/README.md“初始 required-through cut 与真实 rejoin 追赶”拥有。持续目标active，步骤12/14/15及生产Gate仍PARTIAL/OPEN。继续持续在线delivery与默认bootstrap/其余required authority/device/compatibility producer、snapshot/handoff/history GC/decommission、full-bytes、兼容/正式性能/运维及综合验收，不以本切片或提交标记整个16步完成。


2026-10-12继续第15步：65a28a35c4之后已把全部target event/index/head与有界publication prefix原子记录，完整prefix后才logical activation；schema2 reference固定required prefix birth，PREPARED不授权local delete。原registered graph已显式接borrowed cold/scheduler持续消费，UNKNOWN/stop/close保留actual local I/O及locks直到终结；实际网络/ZK/文件/注册竞争与当前证据由profile-wire/README.md“全目标 publication 与在线节点消费”拥有。默认bootstrap/SDK路由、其余required facts、snapshot/handoff/history GC/decommission、full-bytes/兼容/正式性能/运维与全部综合验收继续推进。持续目标active，步骤12/14/15及生产Gate保持PARTIAL/OPEN，不把本提交当16步完成。


2026-10-12继续第12/15步：3e82a21ed9后已把original recovered graph的device/allocator/route/delete事实接回原StartupReadinessHarness、真实DLR/ZK发布/session admission和既有consumer。manifest/client不匹配与scheduler拒绝路径已有真实组合证据，当前源码/阶段source及未覆盖范围由startup-readiness/README.md“实际恢复 graph 与原 startup 发布桥接”拥有。compatibility fence/new-scope独立credential真实issuer与默认EmbeddedServer/bootstrap/SDK routing继续OPEN，snapshot/handoff/history GC/decommission、full-bytes/兼容/正式性能/运维及全16步综合验收继续推进。持续目标active，步骤12/14/15和生产Gate保持PARTIAL/OPEN，本切片不等于整个目标完成。


2026-10-12继续第12步：a048b29cae后已持久初始单Arena compatibility/expected manifest source、显式构造前要求检查和原writable multi frozen birth条件；实际reader/writer version1不接受caller自报未来runtime，原恢复graph/consumer沿用。源码/实际stage与当前验证、物理proof未覆盖边界由startup-readiness/README.md“持久 startup requirements 与冻结发布条件”拥有。继续old-binary fence或独立credential/new-scope真实证明、default EmbeddedServer/bootstrap/SDK routing、snapshot/handoff/history GC/decommission、full-bytes/兼容/正式性能/运维及全16步综合验收。持续目标active，步骤12/14/15与生产Gate保持PARTIAL/OPEN，持久声明不能替代实际proof，本里程碑不等于目标完成。


2026-10-12继续第12步：7f46962589后已在本机实际运行native双UID，对隔离new-scope完整有界现有文件/可读marker取得access快照并导入原actual restart/source/harness组合；directory-only在0711目录/0644已知文件上的假成功已真实RED/GREEN。最终31项/当前source与边界由startup-readiness/README.md“实际 Unix credential 的 new-scope access 快照”拥有。old-service disposition/OS-service全范围issuer、stock binary/pre-storage-open矩阵/default EmbeddedServer/SDK routing仍OPEN，继续它们及snapshot/handoff/GC/decommission/full-bytes/兼容/正式性能/运维与全16步验收。持续目标active，ACCESS_SNAPSHOT_ONLY不升级整体fallback/B14/B15或步骤12/14/15/生产Gate。


2026-10-12继续第12步：25b7913260后已在old UID独立JVM运行当前真实Classic EmbeddedServer，实际SDK成功写读、原stack/journal/storage/driver关闭、original registration与actual process终态，owned DRAINED观察联接实际Unix access与原new target replay/source/harness。closed-live或abrupt/expiry absence不伪造排空；当前source/34项与未覆盖边界由startup-readiness/README.md“实际旧 Classic episode 的有序关闭与 new-scope 组合”拥有。继续外部old-service authority、released stock binary/pre-storage-open矩阵、default EmbeddedServer/SDK routing、snapshot/handoff/GC/decommission/full-bytes及全16步兼容/性能/运维/综合验收。持续目标active，单个owned episode不是整个fallback/B14/B15或生产Gate完成。


2026-10-12继续第11/12步：23b40fdaf8后已抽出实际Profile listener owner，复用原opened graph/pipeline，管理native parent/child/worker交接与原receive ledger退休/actual drain，held cold/backing与未入账accepted fd不提前完成，caller future不污染actual终态。源码/46项Java/13模型配置与未覆盖范围由profile-wire/README.md“显式 Profile listener 与实际网络 credit 排空”拥有。继续将required/physical proofs、pre-storage-open、端口/registration/graph接入full startup owner/default EmbeddedServer及SDK routing，以及snapshot/handoff/GC/decommission/full-bytes/stock matrix/性能/运维和全16步综合验收；完整目标active，生产Gate不提升。

2026-10-12继续第11/12步：08a15da7b7后已接显式 SegmentProfileService owner，先 frozen requirements/version/new-scope 与实际 Arena/Index 根绑定，再构造/open 原 graph、实际 bind endpoint、原 Harness/registration/admission/consumer 和 SDK 注册发现；关闭按 listener→storage→DLR 排空，publication stop/held DATA force 保留原 owner/locks。actual old Classic drain/双 UID access 已接 existing restart，最终58项 Java/style通过，RAT443父 HEAD 路径无新增；stage源码与边界由 startup-readiness/README.md“显式 Profile 服务启动与 owner 排空”拥有。继续 default EmbeddedServer/配置入口与完整外部 authority、released stock/pre-storage-open矩阵、snapshot/handoff/GC/decommission/full-bytes/兼容/正式性能/运维及全16步综合验收；默认 OFF/SYSTEM/PARK与disabled能力不变，完整目标 active、步骤11/12/14/15及生产Gate保持PARTIAL/OPEN，不以服务切片或提交结束目标。

2026-10-12继续第11/12步：9796c5c77d后已把原 EmbeddedServer builder 的显式 Profile 分支置于全部 Classic 构造前，原 lifecycle stack 驱动同一 owner startup/SDK/restart；actual close 未终结时不标终态、不吞错或释放更早资源，handler 返回也不伪造 STARTED。最终28项/1100源码输入与style通过，构建结束后的独立RAT443父 HEAD 路径无新增；当前状态由 startup-readiness/README.md“EmbeddedServer 显式 Profile 分支与 lifecycle”拥有。继续配置/bootstrap factory与完整外部 authority、released stock/pre-storage-open矩阵、snapshot/handoff/GC/decommission/full-bytes/兼容/正式性能/运维及全16步综合验收。Classic 默认、OFF/SYSTEM/PARK和disabled能力不变；完整目标 active，步骤11/12/14/15及生产 Gate保持PARTIAL/OPEN。

2026-10-12继续第11/12步：6a043e7e45后已接原配置→bounded graph 参数快照→公开 typed 环境→同一 owner/EmbeddedServer 的显式 bootstrap；actual properties 加载/SDK/restart与 pre-open invalid拒绝有最终30项/1100输入证据，prefix iterator漏 flat keys 的两条 red 已修正、原断言保留，style通过、独立RAT443父 HEAD问题无新增。现状由 startup-readiness/README.md“配置转换与显式 bootstrap”拥有。Main裸配置仍不能生成 external authority/environment，ISOLATED 无 owner 在 Classic 构造前拒绝；继续完整 Main/owned资源/外部 authority、stock矩阵、snapshot/handoff/GC/decommission/full-bytes/兼容/性能/运维与全16步综合验收。默认 OFF/SYSTEM/PARK和disabled能力不变；完整目标 active，生产 Gate不提升。

2026-10-12继续第11/12步：1e7c0c37a9后已接同handle deferred owned ZK/原metadata driver与LM/cold/scheduler/event-loops初始化，原服务drain后确认各实际终态再关闭；SDK/restart、held metadata任务、partial初始化失败共最终33项/1103输入验证、style通过、RAT443父HEAD无新增，状态由 startup-readiness/README.md“同一服务 episode 的 owned 资源初始化与终止”拥有。TLS/physical issuer仍借用，继续完整Main/外部authority/TLS native ownership、stock矩阵、snapshot/handoff/GC/decommission/full-bytes/兼容/性能/运维及全16步综合验收。默认OFF/SYSTEM/PARK与disabled不变，完整目标active、生产Gate不提升。

2026-10-12继续第14步：a71a23ed73之后已接原周期任务的OPEN固定SDK LAC下界前缀审计，复用flags=0读、全W扫描、同一coordinator/peer pool与actual health/lease drain；writer仍可继续Add，无CLOSED proof或marker清理，未fenced缺失保持UNKNOWN。当前28项Java、20有限模型配置与source/receipt由profile-wire/README.md“OPEN 固定 LAC 前缀审计”拥有。继续其他required authority producer、完整审计/总预算、Main/外部authority/stock矩阵、snapshot/handoff/history GC/decommission/full-bytes/兼容/正式性能/运维与全16步综合验收；持续目标active，步骤11/12/14/15和生产Gate保持PARTIAL/OPEN，不以本切片或提交结束目标。

2026-10-12继续第11/14步：9ebcdbdb51之后已接原网络WRITE_LAC、Bookie EXPLICIT_LAC force/apply与SDK冻结确认prefix的distinct AQ；实际URL2/required-prefix消费、fence/idempotence/UNKNOWN和actual output/wire drain由profile-wire/README.md“网络 WRITE_LAC 与确认前缀生产”拥有。当前337项Java与29有限模型配置通过，历史stage source/receipt不移绑。继续其他accepted ACK/AQ/repair-completion authority、自动策略/默认routing、完整审计/总预算/Main/外部authority/stock矩阵、snapshot/handoff/history GC/decommission/full-bytes/兼容/正式性能/运维与全16步综合验收；持续目标active、生产Gate保持OPEN，不以本切片或提交结束目标。

2026-10-12继续第15步：c78c32f4ba之后已显式接本地terminal TOMBSTONE→原index/coordinate/coverage→实际allocated DATA/旧COPY检查→COPY/MOVE/FREE→fresh REUSABLE，原owner completion-credit归还后才确认child terminal；混合块零净容量增益、真实UIC7/SDK/reopen、corrupt/limit与cancel/held FREE均有当前证据，范围由profile-wire/README.md“本地删除空间终结”拥有。继续网络physical query/receipt、全historical targets、accepted terminal decommission/unrecoverable、retention/snapshot/handoff/history GC，以及Main/默认routing/full-bytes/兼容/正式性能/运维和全16步综合验收；完整目标active、生产Gate保持OPEN，不以本地REUSABLE或提交结束目标。

2026-10-12继续第15步：04bfae5221之后已接独立UIC8真实mTLS物理回收请求，绑定同一已持久应用的UIC7事件与fresh提交权威；ProfileDeleteCoordinator保留完整历史target/incarnation并执行全目标ephemeral观察，缺peer/UNKNOWN不伪造完成，未生成durable terminal receipt或GC权限。当前范围与验证由profile-wire/README.md“网络删除空间查询与全目标观察”拥有。继续durable per-target terminal receipt、离线/rejoin/accepted decommission/unrecoverable、retention/snapshot/handoff/history GC，以及Main/默认routing/外部authority/stock矩阵/full-bytes/兼容/正式性能/运维和全16步综合验收。持续目标active、生产Gate OPEN；不以全目标本次REUSABLE或本提交结束目标。

2026-10-12继续第15步：2bee47a83d之后已将实际UIC8 REUSABLE接到原ZK条件化不可变per-target physical row/birth guard，新await按完整历史manifest逐target重读，支持coordinator重建和有matching记录的真实离线target；未知/未完成不产生记录，未执行GC或删除旧义务。当前105项Java/31有限模型、1884源码输入与证据边界由profile-wire/README.md“删除物理终结记录与持久接续”拥有。继续retention/offline/backup-restore policy、accepted decommission/unrecoverable、stream/journal snapshot/handoff/history/page GC，以及Main/默认routing/外部authority/stock矩阵/full-bytes/兼容/正式性能/运维和全16步综合验收。持续目标active、生产Gate OPEN，不以本里程碑或提交结束目标。
metadata ABA最终补验还原原bytes/version/guard内容，仅birth改变仍拒绝；105项阶段不移绑，hEWWqxvs最终2项与6l4xfie0模型/1884当前输入核验一致，style通过、RAT443父HEAD无新增。

2026-10-12继续第12/15步：6bf92b4a3f之后已将generation1 shared delete stream完整prefix接到bounded immutable snapshot pages/root、同ZK multi publication witness及原local TOMBSTONE/cursor/actual owner启动追赶；native65-ledger跨page、缺prefix＋真实suffix、missing/recreate、cancel/丢回复/并发得到111项Java与23有限模型证据，1895当前source核验一致。原journal、metadata及target义务均保留，snapshot不提供GC权限。当前source/receipt由profile-wire/README.md“删除流完整 snapshot 与真实启动追赶”拥有。继续assignment G+1 PREPARED/effective handoff、retention/offline/backup-restore policy、accepted decommission/unrecoverable、journal/history/page/tombstone GC，以及Main/默认routing/外部authority/stock矩阵/full-bytes/兼容/正式性能/运维和全16步综合验收；完整目标active、生产Gate OPEN，不以本里程碑或提交结束目标。

2026-10-12继续第12/15步：a6dae28dda之后已接真实assignment G+1 PREPARED和old journal allocation cut，完整snapshot/COMMITTED prefix与admission/head同multi排序，迟到实际publisher不能越cut，旧G1 DATA与已记录TOMBSTONE仍可实际推进；114项阶段/最终6项Java、23模型与1905当前source核验一致。当前没有local G+1 force/adoption、catch-up ACK、EFFECTIVE CAS或old-route retirement，准备不是完成；下次必须继续这些必要阶段，不能以freeze或本提交终止完整目标。scope/receipt由profile-wire/README.md“Assignment handoff 的 PREPARED 与投递 cut”拥有。继续handoff全路径与retention/terminal退役/history GC，以及Main/默认routing/外部authority/stock矩阵/full-bytes/兼容/正式性能/运维/全16步综合验收；完整目标active、生产Gate OPEN。


2026-10-12继续第12/15步：fe0b898586之后已将实际PREPARED Plan接入原Bookie type14持久adoption、完整G1 effect cut、owner实际终态及checkpoint/rotation/restart replay；G1 cursor与普通DATA保留，空cut不伪造effect，native admission ABA拒绝重新确认。最终4类27项Java、20有限模型配置与1911当前source核验一致；source/receipt由profile-wire/README.md“Assignment handoff 的本地持久 adoption”拥有。下一项必须继续native catch-up ACK、conditional EFFECTIVE、old-route retirement和连续G2 journal/registration接线；本地adoption不是切换完成，不以本里程碑结束完整目标。继续retention/offline/backup-restore policy、accepted decommission/unrecoverable、journal/history/page/tombstone GC，以及Main/默认routing/外部authority/stock矩阵/full-bytes/兼容/正式性能/运维/全16步综合验收。完整目标active，生产Gate OPEN；Linux NUMA、真实掉电、正式性能/canary本轮NOT_EXECUTED。


2026-10-12继续第12/15步：5343f852c9之后已将原graph实际type14 force/apply/owner终态产生的不可公开构造凭据，接到原ZK conditional native ACK row与同事务birth guard。完整Plan/cut/physical identity、首条immutable记录、offline read与健康文件restart接续得到真实验证；ACK不改变G1 cursor或effective assignment，不删除原义务。最终6类32项Java、26有限模型配置、1919当前source及style通过，source/receipt由profile-wire/README.md“Assignment handoff 的 native ACK”拥有。下次必须继续conditional EFFECTIVE、连续G2 journal/progress与registration接线、old-route retirement；不得以local/native ACK或本提交结束完整目标。继续retention/offline/backup-restore policy、accepted decommission/unrecoverable、journal/history/page/tombstone GC，以及Main/默认routing/其他required authority/stock矩阵/full-bytes/兼容/正式性能/运维/全16步综合验收。完整目标active，生产Gate OPEN；Linux NUMA、真实掉电、正式性能/canary本轮NOT_EXECUTED。


2026-10-12继续第12/15步：21ef9c5ee7之后已接原publisher conditional EFFECTIVE/owned registration退役/NON_WRITABLE同multi，原type15与独立effective progress、cut lineage、G2/G3 journal与实际startup assignment producer。三目标真实SDK Add/close、混合pages、旧event身份、旋转/健康文件restart、回复窗口/held force UNKNOWN/ABA有定向证据；79项原阶段Java（server 71/common codec 8）与35有限模型完成。pending gate收紧的最终21项补验、35有限模型、1929最终source与style完成；独立RAT443父HEAD无新增。将该里程碑作本地提交并继续完整目标，不把当前分量当作完整16步完成。scope/receipt由profile-wire/README.md“Assignment handoff 的 EFFECTIVE 与 G2/G3 接续”拥有；继续retention/offline/backup-restore、accepted terminal decommission/unrecoverable、history/page/tombstone/lineage GC，以及Main/default SDK/其他required authority/full-bytes/兼容/正式性能/运维与全16步综合验收。完整目标active、生产Gate OPEN；Linux NUMA、真实掉电、正式性能/canary仍NOT_EXECUTED。每阶段归档后复核精确可清理产物，保护源码/收据/原失败诊断和活动任务，记录释放空间。


2026-10-12继续第15步：acbe51b599之后已接显式cluster retention policy native CAS、immutable revisions和原logical tombstone同事务receipt；实际策略竞争/回复丢失/旧窗口/无策略历史/guard ABA有证据。6类38项Java、29有限模型配置、1938当前source与style通过；独立RAT443父HEAD无新增。scope/receipt由profile-wire/README.md“删除 retention policy 与原子 receipt”拥有。仍须继续真实backup/restore接受、rejoin与history/page/tombstone/lineage GC、accepted terminal decommission/unrecoverable，以及Main/default SDK/其他required authority/full-bytes/兼容/正式性能/运维/全16步综合验收。BOUND不是GC权限，不删除历史义务。完整目标active，生产Gate OPEN；默认OFF/SYSTEM/PARK和disabled能力不变。每阶段归档后复核精确自有可清理产物，保留source/receipt、原失败诊断和活动任务。


2026-10-12继续第12/15步：9d581d67db之后已接snapshot/同incarnation EFFECTIVE cut后的真实有界journal prefix compaction与删后physical receipt native条件，保持全部effect/index/target/tombstone/retention/lineage和物理义务。真实G2/G3、原SDK DATA/close/delete、source竞争、UNKNOWN/cancel custody、rotation/restart和missing chunk有6类34项原阶段及最终1项补验、31有限模型配置、1946当前source和style证据；scope/receipt由profile-wire/README.md“删除 journal prefix 的实际 compaction”拥有。继续backup/restore接受、retention时间屏障、snapshot/page/lineage/target Manifest/tombstone GC、accepted permanent decommission/unrecoverable及Main/default SDK/其他required authority/full-bytes/兼容/正式性能/运维/全16步综合验收。当前接口只删除可由完整snapshot替代的duplicate journal rows，不缩窄整体目标。目标active、生产Gate OPEN；默认OFF/SYSTEM/PARK和disabled能力保持，每阶段继续归档并复核精确自有可清理产物。


2026-10-13继续full-bytes必要路径：6da8d38a47之后已将原ProfileSocketClient/registered cohort TLS/帧backing接入原额度allocator，普通/恢复Add及WRITE_LAC使用已计费exact私有snapshot，actual close等待最后output view而非只等slot/promise。最终4类69项Java、24原有限模型配置、1946当前source与style有证据，scope/receipt由profile-wire/README.md“客户端 transport backing 与实际关闭”拥有。这里不闭合full-bytes：codec/caller数组、JDK/opaque native TLS/kernel/object overhead及storage/index/control/cold统一总账继续实施，性能收益未测量。继续backup/restore、retention时间屏障/剩余history GC、永久退役/required authority/default SDK/兼容/正式性能/运维和全16步验收。目标active，生产Gate OPEN，默认OFF/SYSTEM/PARK与disabled能力不变；按阶段归档后复核精确自有临时路径，不释放活动资源。


2026-10-13继续full-bytes必要路径：8774f689a8之后已复用原connection/lane/Credit接codec Workspace，共享quota而分开计量，正常/异常/owner转交后归还；fallback reply只复制有界body。正确read fixture的实际编码/解码拒绝、最后backing drain与原wire/SDK有8类93项Java、24原有限模型配置、1946当前source/style证据，scope/receipt由profile-wire/README.md“客户端 codec workspace 与有界 reply 解析”拥有。继续caller reply/长期对象预算转交、server codec/control/cold/JDK/opaque TLS/kernel/object/storage/index统一总账及正式测量，不能据该上界reservation将full-bytes或16步目标判定完成。backup/restore、retention时间屏障/剩余history GC、永久退役/required authority/default routing/兼容/性能/运维继续推进；目标active、生产Gate OPEN，默认OFF/SYSTEM/PARK与disabled不变，按阶段归档/复核精确自有可清理路径。

2026-10-13继续full-bytes必要路径：724f267bc9之后已复用原receive ledger接服务端workspace类别与响应backing原子转交，保持原frame/control授权及actual cold/force/output owner排空。scope/分阶段Java与原有限backing模型证据由profile-wire/README.md“接收端 workspace 分类与响应 backing 转交”拥有。该增量只完成既有预留的类别和转交；MetadataStore内部/长期对象/caller reply/JDK/opaque TLS/kernel/storage/index统一总账与正式测量继续实施。backup/restore、retention时间屏障/剩余history GC、永久退役/required authority/default routing/兼容/性能/运维和全16步验收继续推进；目标active、生产Gate OPEN、默认OFF/SYSTEM/PARK与disabled不变，阶段归档后复核精确自有临时输出再清理。

2026-10-13继续UW-7/full-bytes必要路径：6c99ec81c6之后已将实际ProfileService固定对齐pool的root容量在storage分配前计入同一receive DATA账本，拒绝超额open；原network实际drain与storage实际pool drain分开按生命周期推进，source沿原allocator而不重复预留input cap。最终Java/原有限模型/source/checker与scope由startup-readiness/README.md“固定存储池与网络共享 DATA 预算”拥有。继续索引/控制/长期对象/MetadataStore内部/JDK/opaque native TLS/kernel/caller reply/cohort统一总账、正式测量，以及backup/restore、retention时间屏障/剩余history GC、永久退役/required authority/default routing/兼容/性能/运维和完整16步验收；目标active、生产Gate OPEN，默认OFF/SYSTEM/PARK及disabled不变。Linux专项5项依原Assume条件跳过，不称已验收；每阶段归档后复核精确自有临时路径再清理。

2026-10-13继续UW-7/full-bytes必要路径：868469df47后已将原派生index 1 MiB block cache改为strict并接到ProfileService原DATA working-set预留，actual storage open前必须同时取得pool+cache容量。真实旧service quota红与当前拒绝绿、native SST cache usage及原SDK/异步索引/进程退出恢复等最终2类34项证据由local-storage/README.md“派生 index block cache 与共享 DATA 预算”拥有。实际RocksDB9.9.3 WBM charging失败可被忽略，不能将其soft memtable threshold或该cache额度提升为全部native内存完成；继续memtable/index/控制/长期对象/caller/MetadataStore/JDK/opaque TLS/kernel/cohort总账与正式测量，以及backup/restore、retention时间屏障/剩余history GC、永久退役/required authority/default routing/兼容/性能/运维和完整16步验收。目标active、生产Gate OPEN、默认OFF/SYSTEM/PARK及disabled不变；阶段归档后核对精确自有临时输出再清理。

2026-10-13继续第15步：b067903330之后已将全部historical targets原REUSABLE native receipts及冻结policy/receipt/metadata/root绑定到不可变retention barrier；opaque coordinator观察只从该完整cut后取本地System.nanoTime，重启重新等待完整policy窗口、检查时重验native出生/字节/all-target digest。实际窗口/重启、缺目标/no-policy、guard ABA/native CAS、丢回复/held cancel与原G2/G3相关Java和有限模型证据由profile-wire/README.md“全目标终结屏障与单调 retention 窗口”拥有。WINDOW_ELAPSED仅完成该局部时间条件，不赋予GC或decommission权限；继续backup/restore接受、accepted永久退役/不可恢复、剩余history/page/tombstone/lineage GC以及required authority/default SDK/full-bytes/兼容/性能/运维和完整16步验收。目标active、生产Gate OPEN、默认OFF/SYSTEM/PARK及disabled不变；Linux NUMA/真实掉电/正式性能/canary仍NOT_EXECUTED，阶段归档后核验精确自有临时输出再清理。

2026-10-13继续CLIENT/default SDK兼容必要路径：0c062ac89f之后已在原LedgerOpenOp加入mandatory Profile backlink的明确拒绝，保留原password/digest优先级且不创建Classic handle/listener、执行Classic recovery metadata mutation或切换协议。真实旧CLOSED返回Classic success的红、新OPEN metadata不变及writer继续、合法CRC未来backlink/损坏marker与三Bookie Classic普通custom/恢复读取的最终2类17项证据由profile-wire/README.md“默认 SDK open 的 Profile 拒绝边界”拥有。这里只完成当前SDK的unsupported边界；继续完整Main/外部authority issuer/默认Profile路由、released stock兼容以及backup/restore接受、accepted退役/history GC、full-bytes/正式性能/运维和全部16步验收。目标active、生产Gate OPEN，默认OFF/SYSTEM/PARK与disabled不变，按阶段归档/核验精确自有临时产物再清理。

2026-10-13 [恢复执行的 released compatibility 矩阵](profile-wire/README.md#恢复执行的-released-compatibility-矩阵)：父源码ea98f8b20bebcf639c3ddf07effdeae2aebfd7e3上四个released decoder和真实私有loopback stock Main各跑610向量，原probe Oracle均通过；Classic sentinel成功、stock文件/native namespace快照不变、原旧magic控制在四个decoder均预期FAIL。636个输入/实际JAR/JDK/class-load与312 raw文件归档核验，所有自有进程终结/端口释放。当前PASS_SCOPED，内部handle/master-key/allocation无直接观测使完整G1仍BLOCKED_UNVERIFIED；步骤11/12/14/15、生产Gate与16步验收仍OPEN，默认OFF/SYSTEM/PARK及disabled不变。继续其余兼容effect、Main/default routing/required authority、full-bytes、backup/restore/accepted退役/GC、正式性能与运维；不重绑历史回执。

2026-10-13 [released stock内部effect与corpus补齐](profile-wire/README.md#released-stock-内部-effect-与-corpus-补齐)：父源码55388d6da0上新增外部JDI方法/channel观测，四版本最终611向量的Classic effect为零、611连接实际终结，前后Classic控制触发实际路径。当前生成器要求RECOVERY_TERMINAL_CLOSE而committed corpus缺失的三项RED已用现有生成器补资源，原610 raw bytes和14补充fixtures不变；当前Profile五类37项通过，observer定向Checkstyle通过。639 stock/corpus及1116 Java输入/三阶段raw/checker归档见profile-wire记录，旧610结果不移绑。PASS_SCOPED完成固定reference corpus的内部effect观察；完整current control-body/mixed-version/auth/semantic/startup与16步其余验收仍OPEN，G1 BLOCKED_UNVERIFIED、生产Gate OPEN，默认OFF/SYSTEM/PARK及disabled不变。继续其余兼容、Main/default routing/required authority、full-bytes、backup/restore/accepted退役/GC、正式性能与运维；目标active。

2026-10-13 [当前control请求与released resolver](profile-wire/README.md#当前-control-请求与-released-resolver)：父365dcb802e上将actual client两个control codec的9完整请求及45 magic/truncation变体接入原corpus，665-vector原611 bytes不变；七类47项Profile测试PASS，四版decoder/真实stock各665连接与actual channel drain/内部zero effect/native快照均通过，old CE仍各expected FAIL。四个released resolver的四种endpoint选择/拒绝输入也通过pure logic，registration I/O/publication不因此获证。815 stock与1170 Java输入、独立resolver source、1185 unique SHA/raw/RED/GREEN/checker证据由profile-wire记录拥有；common及resolver style通过，独立RAT仍父443无新增。支持的实验control schema样本已覆盖，mixed rolling versions/auth/semantic/startup及general stable control-body、Main/default route/其他required authority/full-bytes/backup-restore/accepted退役/GC/性能/运维与16步综合验收继续OPEN，G1 BLOCKED_UNVERIFIED、生产Gate OPEN、目标active。默认OFF/SYSTEM/PARK及disabled不变，原610/611结果不移绑。

2026-10-13 [owned lifecycle实际self-drain拒绝](startup-readiness/README.md#owned-lifecycle-的实际-self-drain-拒绝)：父54800228d8上将同一owned服务cold/delete/metadata/ZK event contexts及原Netty loops按实际Thread identity拦在退休/demotion/shutdown前；原metadata明确await API也先拒绝self，legacy非阻塞close及合法外部timeout/actual drain不变。实际旧cold自等待RED、四种managed contexts/直接metadata self-await及同名外部caller正常关闭GREEN，最终3类44 registered/42 PASS/2既有hierarchical条件skip，1170 inputs/style通过、独立RAT443父已有无新增。旧source/receipt保留原绑定，本次native caller identity未移绑TLC；没有新线程/队列/格式或authority issuer。继续Main/default SDK/其他required producer/full-bytes/backup-restore/accepted退役/GC/mixed兼容/性能/运维及16步验收；目标active，步骤11/12/14/15和生产Gate OPEN，默认OFF/SYSTEM/PARK及disabled不变。

2026-10-13 [CLOSED readonly open独立actual-drain](profile-wire/README.md#closed-readonly-open-的独立-actual-drain-信号)：父9cbeb5e2e7上原coordinator新增beginOpenReadOnly/ReadOnlyOperation分开caller completion/cancel与实际cold/未交付handle终结；原openReadOnly兼容委托，peers继续borrowed，合法交接先claim再释放cold quota，callback可继续open。真实held最后metadata、cancel仍占quota、delete/birth拒绝、success callback/nested open及其他调用方最终8项通过，相关11有限模型1正向/10指定反例、730仅catalog；2005 unique source输入冻结、style通过、独立RAT父443无新增。默认SDK公开Profile入口选择仍待明确，本增量不等同已完成默认路由。继续完整Main/required producer/full-bytes/backup-restore/accepted退役/GC/mixed/性能/运维和16步验收；目标active、生产Gate OPEN、默认OFF/SYSTEM/PARK及disabled不变。


2026-10-13继续full-bytes：父eda1d7d760上新增相同严格decoder的borrowed view与owned READ_ENTRY response，原SDK读取保留原网络backing credit到最后caller view释放。取消丢弃/失败/ordered拒绝释放未交付response，actual close不因caller callback终结而提前完成。真实RED与最终45项GREEN/style、原7配置模型的阶段source/receipt由profile-wire/README.md“SDK 读响应的 backing 所有权交接”拥有；总预算、其他reply/caller数组、默认SDK公开入口/Main/required producer、backup-restore/accepted退役/GC/mixed/性能/运维及综合验收继续OPEN。目标active，默认OFF/SYSTEM/PARK、disabled及生产Gate不提升。


2026-10-13继续full-bytes：父73b67bc6e9后已接 actual SDK LAC fan-in 与 matching anchor 的原 frame credit，URL1/URL2旧heap decoder保持兼容；source/explicit retained view、取消回复丢弃及collector拒绝释放均有真实验证。最终三类24项/style、17个有限模型配置及阶段source/receipt由profile-wire/README.md“SDK LAC evidence 的 backing 所有权”拥有；没有把有限root/evidence投影当作组合模型。legacy reply/caller数组、对象/TLS/kernel及存储/index/control/cold总账、默认SDK公开runtime选择、Main/required producer、backup-restore/accepted退役/GC/mixed/性能/运维和全16步验收继续OPEN；目标active、生产Gate不提升，默认OFF/SYSTEM/PARK及disabled不变。


2026-10-13继续full-bytes：父2719623ab6后新增owned point-evidence candidate，原worker/probeLast、required prefix/health/repair都延续原source backing credit并关闭，标准packed payload独立retain到实际发送链归还；取消与失败交付释放已验证。最终21项/style与40有限模型及阶段source/receipt由profile-wire/README.md“Recovery candidate 的 backing 所有权”拥有，有限模型未枚举新Probe/packed引用或组合。legacy probe/reply、observation列表、SDK header/caller数组和全部统一总预算，默认SDK公开runtime/Main/required producer、backup-restore/accepted退役/GC/mixed/性能/运维与全16步验收继续OPEN；目标active，生产Gate不提升，默认OFF/SYSTEM/PARK及disabled不变。


2026-10-13继续full-bytes：父58b942ee53上原标准V3 digest提供explicit header allocator，内部repair header已计入目标DATA原quota，payload保持source credit。额度不足DEFERRED且不写DATA/发布membership，checksum失败与最后header view的实际释放已验证；默认旧入口兼容。最终17项/style、17有限模型及2016 unique输入/receipt由profile-wire/README.md“Repair packing header 的 DATA 预算”拥有，未冒充跨source-target byte组合模型。其余SDK/header/legacy/observation/caller及统一总预算，默认SDK公开runtime/Main/required producer、backup-restore/accepted退役/GC/mixed/性能/运维与全16步验收继续OPEN；目标active、生产Gate不提升，默认OFF/SYSTEM/PARK及disabled不变。


2026-10-13继续full-bytes：父af791ffb69后已接原normal quorum probe的owned candidate primitive，Listener entry、caller取消/未交付与最后backing释放已真实验证；保持原首个payload/缺失阈值，required-hole all-source入口独立。最终10项/style与16有限模型、2005 unique输入由profile-wire/README.md“Normal quorum probe 的 owned candidate”拥有。observe()实际列表仍heap、需明确其结果释放公共契约（AGENTS §1），原默认SDK runtime选择也待答复；不把新primitive冒充实际observation预算闭合。其余统一预算、Main/required producer、backup-restore/accepted退役/GC/mixed/性能/运维和全16步继续OPEN，目标active、生产Gate不提升，默认OFF/SYSTEM/PARK及disabled不变。


2026-10-13继续full-bytes/index：父69699cd886后原derived RocksDB Get/key/value改为fixed-output，完整长度不符仍拒绝、不假missing，格式/恢复和cleanup/MOVE约束沿用。父源码真实大行分配RED与当前18项/storage/style、1170 stage输入/format-only检查及同版本依赖源码由local-storage/README.md“派生 index 的有界 Java 读取”拥有。未把有界Java复制当作native/对象/统一总账或性能证明，原模型不覆盖此JNI分配、不重跑或移绑。默认SDK/observe公共约定、Main/required producer、backup-restore/accepted退役/GC/mixed/正式性能/运维和全16步继续OPEN，目标active、生产Gate不提升，默认OFF/SYSTEM/PARK及disabled不变。


2026-10-13父fd3b1be724上的声明目录完整验证：730配置全部终结，118正向完整搜索PASS/612指定反例；现有Unified WAL Java43类整轮866项原exit1（2个遗留契约断言失败、0错误、5个Linux条件跳过），已修正两处测试并独立补验失败方法2项通过。四类补验首次被会话中断、回执为INCONCLUSIVE；新阶段253项全部通过，style通过；终态后独立RAT exit1/443父HEAD路径无新增，不称CI全绿，原失败/模型回执和修正stage分别保留，不改称整轮全绿。完整A+C+D组合、全字节、默认SDK/observe公共约定、Main/required producer、backup-restore/accepted退役/GC/mixed/正式性能/运维和16步验收继续OPEN，目标active、生产Gate不提升，默认OFF/SYSTEM/PARK及disabled不变。 当前证据由profile-wire/README.md“声明模型目录与现有 Java 测试整轮验证”和models/README.md“当前声明目录的完整验证”拥有；继续完整合同，不因验证里程碑或本地提交结束目标。


2026-10-13父36163d9908后在原WholeAllocationFree新增CDSpec，直接组合两个原ProfileLogicalDelete实例与共享allocation的tombstone force/apply、旧I/O/reader drain、FREE force/apply；新四个指定反例覆盖早投递、无force、丢邻居和部分effect伪FREE。最终10配置2完整正向/8指定反例与实际两类6项Java通过，2010 unique source匹配；原Spec五配置保持原计数，catalog735尚未全跑、原730回执不移绑。style通过，实际终态后的独立RAT exit1/443父HEAD路径无新增，不称CI全绿。该单target/两ledger组合不冒充完整A+C+D，继续全16步及其余OPEN范围，目标active、生产Gate不提升，默认OFF/SYSTEM/PARK及disabled不变。 当前scope/source/receipt由models/README.md“C+D 的 logical 到共享 allocation FREE 组合”和profile-wire/README.md“C+D local FREE 组合与实际边界验证”拥有；下次继续A+C关键交界及完整合同，不因本地组合里程碑停止目标。


2026-10-22父30377492f4后在原ProfileSdkBridge新增ACSpec，直接由两个独立SegmentAllocator实例的local publication供给原A2 SDK ACK，slot2保留验证过的remote响应输入；已发请求可在caller超时后进入owner，取消不取消accepted I/O，恢复同步不mint原live ACK。最终17配置4完整正向/13指定反例、旧11配置计数不变，两阶段8+2原Java与最终新增1项真实队列/独立force测试通过，2016 unique路径按stage冻结，1项测试强化差异分别保留；catalog741未全跑，历史receipt不移绑。Spotless/Checkstyle通过；Java/model/style实际终结后独立RAT exit1，443父HEAD路径无新增，不称CI全绿。该单entry/每episode一个batch/pair内一次crash组合仍未闭合同批fence/冲突重试/跨batch UNKNOWN/多Arena/连续两次故障与完整A+C+D；继续全16步，目标active、生产Gate不提升，默认OFF/SYSTEM/PARK及disabled不变。 当前scope/source/receipt由models/README.md“A+C 的 local publication 到 SDK ACK 组合”和profile-wire/README.md“A+C local publication 到 SDK ACK 的实际验证”拥有；继续A+C剩余关键边界及完整合同，不以有限组合或本地提交结束目标。


2026-10-22父098838a574后在原C中分开file durability与逐batch actual I/O return，只有完整actual-return prefix才可local publication，未返回任务继续保留inflight；原A+C实例直接消费该事实。新增suffix提前发布反例精确命中“两批字节durable、仅第二批返回、第一批仍在途”。最终18配置4完整正向/14指定反例及现有4项真实排空/fence/mixed测试通过，2017 unique路径与重叠SHA核验一致；C/A+C状态计数细化，旧SDK6配置计数未变，catalog742未全跑、历史结果不移绑。Spotless/Checkstyle通过；Java/model/style实际终结后的独立RAT exit1，443父HEAD路径无新增，不称CI全绿。Java/wire/持久格式未改，分布式双batch/multi-Arena/冲突/完整A+C+D及其他16步验收仍OPEN，目标active、生产Gate不提升，默认OFF/SYSTEM/PARK及disabled不变。 当前scope/source/receipt由models/README.md“Physical prefix 的逐 batch I/O 终态”和profile-wire/README.md“Physical prefix 的实际 I/O 排空验证”拥有；继续A+C跨batch实际ACK及完整合同，不因本地模型/提交结束目标。


2026-10-22父85716ef7f3后在原A+C新增suffix pending操作的分布式ACK窗口，完整DATA prefix durable仍必须等待predecessor实际返回；错误suffix DURABLE一直传播到SDK success的反例已命中。最终20配置5完整正向/15指定反例、旧18配置计数不变；两项新增真实A2/A3双batch与两项默认夹具回归通过，2019 unique路径/重叠SHA一致。两轮fixture未进入目标窗口的失败保留，当前hold在原Obligation终态交接，生产代码/wire/持久格式与旧fixture默认不变。catalog744未全跑，历史receipt不移绑。Spotless/Checkstyle通过；Java/model/style实际终结后独立RAT exit1，443父HEAD路径无新增，不称CI全绿。完整多peer/多SDK操作/multi-Arena/fence/冲突/恢复/两次故障/A+C+D及其余16步仍OPEN，目标active、生产Gate不提升，默认OFF/SYSTEM/PARK及disabled不变。 当前scope/source/receipt由models/README.md“A+C suffix 的分布式 ACK”和profile-wire/README.md“跨 batch suffix 的真实 quorum”拥有；继续完整合同，不以有限窗口或本地提交结束目标。


[超时后同坐标重试的身份验证](profile-wire/README.md#超时后同坐标重试的身份验证)：2026-10-22父c60271c0f7上加强已有真实socket测试：原force在途且caller已超时，payload-only/length-only改变都冲突，LAC-only变化续接原操作、只发生一次force；该项及assembling/pending/index三项现有回归全部通过。原A+C增加同坐标cut与四个指定反例，最终25配置6完整正向/19指定反例、原20计数不变，2024 unique source/重叠SHA一致；owner为坐标保护oracle，不宣称第二个Y物理写、hot/index或restart冲突恢复已建模。Spotless/Checkstyle通过；终态后独立RAT exit1/443父HEAD路径无新增。catalog749未全跑，历史receipt保持绑定；生产/wire/格式未改，完整冲突恢复/multi-Arena/fence/两次故障/A+C+D和其余16步仍OPEN，目标active、生产Gate不提升，默认OFF/SYSTEM/PARK及disabled不变。


[Checkpoint record 的流式摘要与写入](local-storage/README.md#checkpoint-record-的流式摘要与写入)：2026-10-22父830918cbb2上去掉原双owner image摘要/写入阶段的完整record临时复制，改为一个16-byte prefix和原record只读view，字节/SHA/force与publication合同不变。相同约8MiB workload旧digest/write分配8,407,640/8,421,296 bytes触发真实RED，新为12,992/29,704 bytes；最终两类20项全部通过，含向量、partial body失败、force/损坏回退/子进程退出及DATA图restart。1170输入按stage冻结，style通过；终态后独立RAT exit1/443父HEAD路径无新增。额外分段write的I/O成本未测，不宣称性能/full-bytes闭合；模型无变化不重跑。其他required范围与全16步仍OPEN，目标active、生产Gate不提升，默认OFF/SYSTEM/PARK及disabled不变。


[Recovery reader 的本地 RPC 实际排空](profile-wire/README.md#recovery-reader-的本地-rpc-实际排空)：2026-10-22父f115cd7d22上修复reader已回逻辑结果却未等自身READ/FENCE/LAC/anchor output终态即关闭的真实缺口；复用原request drain锁/集合，parent coordinator六处等待close完成才归还名额，borrowed peers保持可用。真实旧源码RED、最终五窗口及相关23项零failure/error/skip，15原有限模型配置符合预期；模型没有新reader组合，2024路径按stage冻结、import-only排序单列。style通过；终态后独立RAT exit1/443父HEAD路径无新增。observe结果公共约定/默认SDK/Main/required producer/full-bytes/backup-restore/accepted退役/GC/mixed/完整模型/正式性能/运维与全16步仍OPEN，目标active、生产Gate不提升，默认OFF/SYSTEM/PARK及disabled不变。


[LAC collector 排队期间的关闭排空](profile-wire/README.md#lac-collector-排队期间的关闭排空)：2026-10-22父36eb57665d后补原RPC终态之外的collector排队窗口；LAC内部result在原request drain登记，caller取消不提前释放，executor关闭兜底也等待owned reply清理。真实旧源码RED、新窗口通过；24项相关回归原23通过/1旧hold清理失败，已按actual-drain合同修正该方法并定向补验两项通过，原失败保留、不改称整轮全绿。1170输入按四stage冻结，style通过，终态后独立RAT443父HEAD路径无新增。模型未改不重跑，其他16步验收/生产Gate仍OPEN，目标active，默认OFF/SYSTEM/PARK及disabled不变。


[Checkpoint 流式写入的本地 JMH 对照](local-storage/README.md#checkpoint-流式写入的本地-jmh-对照)：2026-10-22父bc2f189186上新增原microbenchmarks入口，以相同有效1/64条ALLOC image、digest/open/truncate/write/file force/目录同步对照830918旧codec与当前流式codec。旧→新→新→旧的8个fork均核验所加载class及完整image字节一致。64条Java分配约50–52KB/op→25.8–25.9KB/op，write66→130/op，worker CPU上升；短运行延迟波动不支持稳定延迟/吞吐提升结论。141相关输入、冻结jar/旧class、原始JMH/环境与复现脚本归档，package/style通过，独立终态RAT443父HEAD路径无新增。无生产改动，不提升B18/正式性能或其他Gate；全16步目标active，生产Gate OPEN，Linux NUMA/掉电/正式性能/canary仍NOT_EXECUTED，默认OFF/SYSTEM/PARK及disabled不变。


[Backup/restore 首版接受语义](profile-wire/README.md#backuprestore-的首版接受语义)已按用户指定规划会话的2026-10-22裁决冻结：只支持新 BookieId/incarnation/目录的 verified export/rebuild，由当前未回滚集群授予 repair/membership 与新目标 bootstrap；旧控制/cursor/grant 不恢复为权限，旧 incarnation 删除及物理义务原样保留。原地 restore/同 BookieId 切换/MetadataStore 回滚继续 disabled；恢复接受下界未实现前不据窗口到期 GC。owner RFC 已同步，导出/导入及恢复专属验收仍 OPEN；后续据此实施，非语义完成即实现完成，完整目标 active、生产 Gate OPEN。


[SDK ACK→CLOSED删除→共享FREE组合](models/README.md#sdk-ack-到-closed-删除及共享-free-的生命周期组合)：2026-10-22父b0710fd09c上直接组合原SDK/两个storage episode与两logical-delete/原allocation。最终五配置为1完整正向（529798/99442、queue0）、3指定mutation反例及1完整成功路径witness；只合并closed且drained后冻结SDK历史的显式TLC VIEW，原动作/性质保留，无state constraint或symmetry。1131输入/全部早期求值失败、未完成搜索及最终源码各自冻结；Python/TLC通过，独立RAT443父HEAD既有无新增。无生产Java改动，不重复Java测试，不将原receipt升级本组合native验收；catalog754未全跑，完整A+C+D及16步/生产Gate仍OPEN、目标active、默认OFF/SYSTEM/PARK与disabled不变。


[离线 backup image 与不可服务输入](local-storage/README.md#离线-backup-image-与不可服务输入)：2026-10-22父4b1ba0889d上复用原DATA/双control锁和完整恢复扫描，建立有界manifest/外部SHA reference、全native文件复制/force/重读及借用scratch的instance点读；marked input在服务replay前拒绝，旧权限不作新授权。新启动拒绝RED，最终两类六项0 failure/error/skip、style通过；1171输入各stage冻结，独立RAT443父HEAD既有无新增。仅完成离线输入层，repair来源/新目标导入/membership/bootstrap及恢复专属组合仍OPEN，未称restore完成；旧目标义务保留，全16步目标active、生产Gate OPEN，默认OFF/SYSTEM/PARK和disabled不变。


[离线镜像接入当前 CLOSED repair-copy](profile-wire/README.md#离线镜像接入当前-closed-repair-copy)：2026-10-23父b7c9e32bc6上，镜像仅作附加source，经current CLOSED/context/birth/retained source校验，复用inactive install/admit/grant/copy/membership/close；缺项取当前副本，冲突隔离，不降frontier或授normal。原copy同时等待reply与actual output终结，caller cancel保持实际资源；新增七场景及既有路径共12方法全部通过，reply-only敏感性RED保留。repair模型12配置2正向/10指定反例，catalog756未全跑；1171 Java/1134模型输入去重2033，各stage保持原绑定。style通过；RAT补两新cfg许可证后回到443父HEAD既有路径，无新增。新目标assignment/snapshot/no-hole suffix/required-through/readiness/registration、restore reuse/crash/lost reply及完整接受仍OPEN，旧目标义务不解除。完整16步目标active、生产Gate OPEN、默认OFF/SYSTEM/PARK与disabled不变，Linux NUMA/真实掉电/正式性能/canary未执行。


[恢复目标的首次 service bootstrap 组合验证](profile-wire/README.md#恢复目标的首次-service-bootstrap-组合验证)：2026-10-23父389b49fada上，仅补原真实restore fixture的组合覆盖。新目标copy完成后可经当前verified-zero/head重启注册并mTLS读出原数据；首次注册前删除则用该目标自己的完整snapshot追赶，publication实际提交前已有cursor1/tombstone且尚无注册，旧Manifest/目标义务不变。最终三方法0 failure/error/skip（扩展restore方法九场景＋既有service drain/restart及snapshot/suffix），1171输入和早期零cursor测试假设失败各自冻结，无生产Java/TLA改动、不重跑TLC。完整restore的reuse/crash/lost reply/接受下界及其余16步验收仍OPEN，目标active、生产Gate OPEN、默认OFF/SYSTEM/PARK和disabled不变。


[旧恢复输入与当前 metadata 身份隔离](profile-wire/README.md#旧恢复输入与当前-metadata-身份隔离)：2026-10-23父7021519bb3上，仅补真实restore当前metadata身份覆盖：相同body/version重建但native birth不同、同ledgerId非法替换instance/backlink，两者均QUARANTINED且零DATA/零copy/metadata不变/不授normal；既有镜像context隔离和wrong-instance tombstone原native/replay验证复用。最终两类三方法通过，restore方法11场景；两轮fixture/API与分类假设失败分别保留，1171输入冻结，无生产Java/TLA变更、不重跑TLC。该结果不授予合法reuse/原地restore；恢复crash/lost reply/接受下界/运维及完整16步继续OPEN，目标active、生产Gate OPEN、默认OFF/SYSTEM/PARK和disabled不变。


[镜像导入丢回复后的同 intent 续接](profile-wire/README.md#镜像导入丢回复后的同-intent-续接)：2026-10-23父796582579f上，真实首条recovery DATA force后丢回复/断DATA，目标经正常停止和service重启、新coordinator同intent续接仅为缺项新增一次force；membership后grant-close丢回复/断CONTROL，新coordinator续接copy0/零新增DATA force。13场景restore方法与原historical lost-close两方法通过，原CLOSED及权限保持；1171输入/两轮fixture假设失败分别冻结，无生产Java/TLA改动、不重跑TLC。仅验证正常restart及上述回复丢失，非进程kill/掉电；恢复crash/cursor/readiness/registration丢回复/接受下界/运维与完整16步仍OPEN，目标active、生产Gate OPEN、默认OFF/SYSTEM/PARK和disabled不变。


[Owned service 的 repair INSTALL 与崩溃目标隔离](profile-wire/README.md#owned-service-的-repair-install-与崩溃目标隔离)：2026-10-23父3e233f324b上，修复owned PREPARING_INSTALL漏查repair authority：仅initial NOT_FOUND时查原repair reader。真实owned/ZK/mTLS INSTALL NOT_FOUND RED已复现；修复后子JVM首条recovery DATA OK/APPLIED前halt23、无Java cleanup，原target因unclosed DATA session拒绝重开/注册；另新身份可完成copy/member/close，随后删除仍保留崩溃及新target义务。最终两方法通过（restore14场景及原owned SDK/drain），1171输入/五阶段保持原绑定，TLA未改不移绑旧receipt。**同target/同intent的unclean-session crash resume及新target4注册闭环尚未验收，不能以新身份重建替代该完整目标**；cursor/readiness/registration恢复、接受下界/运维及全16步仍OPEN、目标active、生产Gate OPEN，默认OFF/SYSTEM/PARK与disabled不变，真实掉电/Linux NUMA/正式性能/canary未执行。


[崩溃后新身份目标的注册与在线删除闭环](profile-wire/README.md#崩溃后新身份目标的注册与在线删除闭环)：2026-10-23父807f78579a上，只扩展原restore测试。真实child halt后原target3保持隔离，新身份target4完成当前owned startup/registration/fresh peer+mTLS点读，再由原follower投递实际删除到cursor1/tombstone并拒绝后续读；Manifest完整字节与旧、新target义务保持。原14场景方法通过，1171 native输入及最终format-only换行分开保存；生产Java/TLA未改，不重跑TLC。补齐上一阶段新target4注册缺口，**不替代原target/原intent的unclean-session恢复**；相关新增数据语义已提请用户选择，未答复不放行。完整16步仍OPEN、目标active、生产Gate OPEN、默认OFF/SYSTEM/PARK与disabled不变，Linux NUMA/真实掉电/正式性能/canary未执行。


[派生 index 的 write-buffer 配置容量预留](local-storage/README.md#派生-index-的-write-buffer-配置容量预留)：2026-10-23父a08d52884a上，原shared DataWorkingSet启动前预留扩展为固定pool＋1 MiB cache＋两份1 MiB write-buffer配置容量，实际RocksDB选项值不变。真实quota缺口RED已复现；最终三方法通过，含原owned SDK/drain及恢复14场景，1171输入/red-green/同版依赖源码分别冻结。测试fixture显式4 MiB total，生产quota不自动增大；reservation保持至实际storage/index关闭。只是配置容量预留，不宣称memtable/native/RSS硬上限或full-bytes闭合。TLA转移未改；统一预算、同身份unclean-session恢复待决语义及其余16步继续OPEN，目标active、生产Gate OPEN、默认OFF/SYSTEM/PARK和disabled不变，Linux NUMA/真实掉电/正式性能/canary未执行。

[只读磁盘状态诊断](local-storage/README.md#只读磁盘状态诊断)：2026-10-23父4907120bff上，实验 main 复用原 Arena replay，以既有三类排他锁和READ-only内容channel报告观察到的身份/布局、session/quarantine与分配状态，不执行恢复sync或授予服务/restore权限；原正常恢复与unclosed gate保持。真实文件/独立JVM/锁、fallback与坏suffix验证由该节拥有。这里只交付离线诊断，完整运维/同身份crash接受语义/16步验收仍OPEN，目标active、生产Gate OPEN，默认OFF/SYSTEM/PARK及disabled不变。

[恢复 bootstrap 的 snapshot suffix 与发布丢回复](profile-wire/README.md#恢复-bootstrap-的-snapshot-suffix-与发布丢回复)：2026-10-23父d4a3e38114上，补实际恢复目标snapshot cut1＋真实第二ledger删除suffix/head2；完整时cursor2/两tombstone后注册，缺event2时保持cursor1且无writable。persistent readiness与writable两次实际ZK multi后分别回CONNECTIONLOSS，同session精确确认成功，Manifest/旧目标义务保持。仅测试fixture与既有文档变化，原14场景保持；不替代同incarnation unclosed crash恢复或完整接受，16步目标active、生产Gate OPEN、默认OFF/SYSTEM/PARK及disabled不变。

[显式 SDK writer 的 generated header 字节额度](profile-wire/README.md#显式-sdk-writer-的-generated-header-字节额度)：2026-10-23父9a9aa9cd79上，普通V3 header在entryId前取得原DATA额度，WRITE_LAC header同样计费；额度不足不分配entry、不发送DATA，取消/回收与source→socket实际交接保持。仅绕过计费的敏感性RED已复现，最终三类37项通过，Classic三digest实际读写及原SDK/close/替换/恢复排空保持。这里是显式writer生成buffer的单handle额度，不关闭全字节/性能/默认SDK及其他16步范围，目标active、生产Gate OPEN，默认OFF/SYSTEM/PARK及disabled不变。

[三副本 SDK 到本地删除 FREE 的组合](models/README.md#三副本-sdk-到本地删除-free-的组合)：2026-10-23父f5fc64ace4上，原SDK/两个storage与delete/FREE保持，第三slot新增自己的原allocator publication/actual I/O return；close输入还须SDK停止及三请求/source/I/O排空。六配置为1完整正向（41770502/6170077、queue0）、4指定反例和1三副本成功路径witness，1142输入相同、显式3600秒预算；762目录未全跑。仍只有一个本地allocation的D/FREE，metadata/fenced-CLOSED为输入，旧slot迟到回复的全部close路径与完整A+C+D/16步验收继续OPEN；无Java变更，目标active、生产Gate OPEN、默认OFF/SYSTEM/PARK及disabled不变。

[SDK 停止后丢弃回复的实际请求结算](models/README.md#sdk-停止后丢弃回复的实际请求结算)：2026-10-23父1fef8057fc上，组合模型补原slot0/1在SDK停止且实际I/O返回后的sent结算，不生成ACK。九配置为1完整正向（44131126/6740125、queue0）、6指定反例、2个49步A=2成功后停止/丢弃回复/local FREE witness；1146模型输入相同，native1172未变、不重跑Java。具体witness的I/O在stop前返回；仍非全目标物理完成、全部timeout/故障或完整A+C+D。766目录未全跑，16步目标active、生产Gate OPEN，默认OFF/SYSTEM/PARK及disabled不变。

[SDK 迟到 DATA 到删除复用及健康重启的原生组合](profile-wire/README.md#sdk-迟到-data-到删除复用及健康重启的原生组合)：2026-10-23父bf9c813280上，新增一方法串联实际3/3/2 SDK ACK、第三副本force跨越stop、固定P/L普通close、三目标delete/本地FREE、ledger45复用generation2及逐节点健康restart/mTLS点读。最终1 PASS、1172输入匹配、生产Java/TLA未改、style通过；旧Manifest/三target义务保留。仅单Arena顺序复用的有限组合，非crash、多Arena、持久physical receipt/GC或完整16步验收；目标active、生产Gate OPEN，默认OFF/SYSTEM/PARK及disabled不变。

[Profile 启动恢复扫描的 DATA 字节额度](local-storage/README.md#profile-启动恢复扫描的-data-字节额度)：2026-10-23父66e860ea32上，将原 service DATA quota 传给 restart scan，在分配 header/page/batch/covered MOVE scratch 前计费，实际 scope 结束归还；额度不足不启动新 DATA session。真实大 batch 拒绝、caller cancel/实际 force、CRC 失败回收及原 covered MOVE/owned SDK restart 共4方法通过，1172输入匹配、最终style通过；绕过 budget 转发的敏感性RED独立保留。只闭合启动DATA buffer计费，全字节、同身份crash/多Arena待决语义和完整16步仍OPEN，目标active、生产Gate OPEN，默认OFF/SYSTEM/PARK及disabled不变。

[共享 allocation 存活邻居的搬迁与复用](profile-wire/README.md#共享-allocation-存活邻居的搬迁与复用)：2026-10-23父c8483d3549上，原三副本SDK生命周期加入同块未删除ledger45；第三COPY force未返回时无MOVE/FREE，实际搬迁后邻居可读，ledger46的256条普通Add经合法尾部耗尽后复用旧块generation2。三节点健康restart后邻居/新首末entry仍可读、44拒绝、历史义务保持。三方法通过，1172输入/三轮fixture假设失败/最终纯换行分别保留，style通过；生产Java/TLA未改，不重跑模型。仅闭合该有限组合，完整16步目标active、生产Gate OPEN，默认OFF/SYSTEM/PARK及disabled不变。

[控制 checkpoint 的流式读回校验](local-storage/README.md#控制-checkpoint-的流式读回校验)：2026-10-23父b03297b919上，Bookie/Arena 写后验证复用原 parser，用最多64 KiB record scratch逐条比较冻结状态；恢复仍返回独立record，格式/force/commit/publication不变。8 MiB有限image的旧比较额外Java分配8,535,328 bytes，新版本最终185,200 bytes；最终两类48方法通过、1172输入匹配、style通过，旧算法RED独立保留。不是RSS/吞吐或统一CONTROL总账证明；完整16步目标active、生产Gate OPEN，默认OFF/SYSTEM/PARK及disabled不变。

[Profile checkpoint 校验的 CONTROL 容量预留](local-storage/README.md#profile-checkpoint-校验的-control-容量预留)：2026-10-23父a533d249d2上，Service在storage open前从原CONTROL额度保留69776 bytes，保持至实际storage关闭；未接线启动缺口及同步drain回调重入重复归还两次RED分别保留，最终先移走token引用再归还。最终两类19方法通过、1172输入匹配、style通过，覆盖真实held checkpoint force、停止准入后verification、原SDK/Embedded/owned restart与bind cleanup。测试只增加固定容量，原DATA/网络空间保持；统一CONTROL/full-bytes和其余16步仍OPEN，目标active、生产Gate OPEN，默认OFF/SYSTEM/PARK及disabled不变。

[Arena checkpoint 的借用记录恢复](local-storage/README.md#arena-checkpoint-的借用记录恢复)：2026-10-23父8579d1e211上，原parser以一份record scratch同步回调Arena解码，完整digest成功前只形成私有候选，失败清空后fallback；Bookie retained read与写后verify保持各自ownership。4 MiB有限image额外Java分配4,269,832→最终135,536 bytes，晚digest错误不遗留ghost allocation/id。最终三类56方法通过、1172输入匹配、style通过；启动streaming复用既有CONTROL预留，未改格式/配置/TLA。typed/frozen/Bookie状态及完整full-bytes/16步仍OPEN，目标active、生产Gate OPEN，默认OFF/SYSTEM/PARK及disabled不变。


[Arena checkpoint 的按需编码与持有态](local-storage/README.md#arena-checkpoint-的按需编码与持有态)：2026-10-23父e1fb23a0fa上，Arena只冻结原不可变命令/状态/序号，摘要、写出及精确读回比较时按需编码单条record；Bookie retained原始字节保持。实际两个JVM在同16,000控制结果/首次image force前的full-GC histogram中，只读buffer增量16,000→0、byte[]增量3,393,904→66,064，完整image字节相同且各自重开；非RSS/峰值/性能结论，重复编码成本未测。Service原CONTROL预留扩为200848 bytes并保持至实际storage关闭，生产quota不自动增加；不足启动的敏感性RED和原始失败分层保留；最终四类72项通过、1172输入一致、style通过，TLA未改不重跑TLC。typed/Bookie/其他full-bytes及完整16步仍OPEN，目标active、生产Gate OPEN，默认OFF/SYSTEM/PARK及disabled不变。


[Bookie control 原始记录的容量预留与关闭释放](local-storage/README.md#bookie-control-原始记录的容量预留与关闭释放)：2026-10-23父8b245c0ce5上，原Service CONTROL working-set在storage open前增加3倍journal的Bookie canonical backing容量，合计200848＋3×controlLogBytes，实际storage终结后归还；原raw map在close路径清空，durable字节/cut/closed拒绝保持。父版2RED、同选择修复2PASS；同4096实际INSTALL文件的两JVM在保留closed对象时，父版仍4096个只读buffer，新版降至0，磁盘字节相同且重开成功；最终三类37项通过、1172输入一致、style通过，TLA未改不重跑TLC。仅raw backing预留/生命周期与有限full-GC持有态，typed/JDK/native等完整full-bytes未闭合；全部16步目标active、生产Gate OPEN，默认OFF/SYSTEM/PARK及disabled不变。


[控制批次的逐条编码与完整预检](local-storage/README.md#控制批次的逐条编码与完整预检)：2026-10-23父ab44b475a5上，双owner在任何I/O前逐条完整预检/累计整批长度，预留原坐标后逐条编码writeFully、整批一次force及原逐序apply；去掉整批serialized列表，写者排他/UNKNOWN/partial前缀恢复保持。新两项后段拒绝/第二write失败与既有定向共6项通过；相同observer的四JVM在首次write前只读buffer增量均4096→1，Arena journal字节相同，Bookie逐项重开核对context/credential。codec预留331920＋既有3倍journal的raw backing，实际inline值核对；最终四类82项通过、1172输入一致、style通过，TLA未改不重跑TLC。CPU/累计分配未测，typed/JDK/native等完整full-bytes未闭合。全部16步目标active、生产Gate OPEN，默认OFF/SYSTEM/PARK及disabled不变。


[控制批次流式编码的本地 JMH 对照](local-storage/README.md#控制批次流式编码的本地-jmh-对照)：2026-10-23父ed036afd83上，原microbench新增固定256批append/force/apply入口；每批1/64条，Arena同块竞争/Bookie新INSTALL。两版相同release17编译、16有效fork按父→新→新→父完成真实force及每轮重开/class校验；初轮major65/61混用的16 fork保留且不作性能对照。累计分配上升已实测（64条Arena59.5→78.2–96.6KB，Bookie约700→803KB），CPU/wall波动不支持稳定收益。生产Java未改，后续复用原codec校验/尺寸以消除预检frame额外构造；完整full-bytes/正式性能与16步目标仍active、生产Gate OPEN，默认OFF/SYSTEM/PARK及disabled不变。


[控制批次预检的校验与尺寸复用](local-storage/README.md#控制批次预检的校验与尺寸复用)：2026-10-24父3c99a1f92c上，双owner整批预检改为原codec共享字段校验/尺寸，移除预检frame/CRC构造；实际完整编码、整批坐标预留、write/一次force/顺序apply及UNKNOWN不变。两项codec边界与既有五类最终87项、style通过，独立RAT443父路径无新增。同一benchmark/依赖、同release17编译67 class并核对实际加载/Maven指令流；16有效fork实测64条分配Arena96.6→63.3 KB、Bookie803.7–808.3→747.4–750.0 KB，CPU/wall波动仍不支持稳定收益。未改格式/模型或缩减CONTROL预留，完整full-bytes/正式性能与16步继续OPEN，目标active、生产Gate OPEN，默认OFF/SYSTEM/PARK及disabled不变。


[当前 Java 整轮与注册池排空观察](profile-wire/README.md#当前-java-整轮与注册池排空观察)：2026-10-24父5a16759da1上的43类整轮实际897项，891通过/1 error/5个Linux条件skip；唯一旧pool测试在hold输出promise时提前等channel close，独立单方法亦复现。按原reader actual-drain先于lease关闭合同，仅修正该方法的观察顺序并加强task拒绝提前释放；最终四项回归通过，其他方法字节及生产Java/TLA不变。原整轮exit1和全部输入/回执保留，不把补验改称整轮全绿；style通过、独立RAT443父路径无新增。其余16步/完整验收继续OPEN，目标active、生产Gate OPEN，默认OFF/SYSTEM/PARK及disabled不变。


[跨 batch quorum 的 durable 回复观察](profile-wire/README.md#跨-batch-quorum-的-durable-回复观察)：2026-10-25父85d158c432上，原43类整轮暴露回复总数观察错误；真实input额度拒绝产生NONE后，旧总数断言的两个受控反例失败。最终仅统计durable结果，先完成第三副本entry0再对suffix拒绝，保留前两副本physicalTerminal与A2/A3屏障，三个节点使用原子force调用计数并结合实际终态。原尝试数3/退避5ms/deadline10s保持。最终同一43类整轮897项为892通过/0 failure/error/5个Linux条件skip，用例身份及1172输入一致；style通过、独立RAT443父路径无新增。所有先前失败分阶段保留；仅macOS Java PASS_SCOPED，生产/TLA未改，完整16步目标active、生产Gate OPEN，默认OFF/SYSTEM/PARK及disabled不变。

后续full-bytes已通过只读追踪定位到Bookie/Arena control owner真实关闭后的typed collections及容器backing保留。实施时必须保留当前delete、rollback诊断、session/quarantine和标量cut等关闭后可观察事实，不能复用会清除这些事实的recovery reset；当前尚未实施或取得heap证据。


[控制 owner 关闭后的历史状态释放](local-storage/README.md#控制-owner-关闭后的历史状态释放)：2026-10-25父8d2b448a0b上，Bookie/Arena在原close guard之后替换9/10个历史容器并释放pending，保留rollback/delete/session/quarantine/cut诊断；Arena关闭后的历史查询明确拒绝，原真实排空与protected settlement不变。扩展6个既有方法，9项定向及最终4类114项全部通过、输入/用例身份匹配、style通过；独立RAT仍443父路径无新增。相同冻结native输入与同JDK的两JVM持续持有closed owner：Bookie LedgerState4096→0、Arena AppliedOperation16001→0、被观察HashMap table57392→0槽位，重开及磁盘SHA不变；不是峰值/RSS或完整full-bytes。未改格式/模型或减少CONTROL预留，默认SDK/observe、同身份unclean-session恢复、多Arena及其余16步仍OPEN，目标active、生产Gate OPEN，默认OFF/SYSTEM/PARK及disabled不变。


[按 allocation 索引当前与退休区间](local-storage/README.md#按-allocation-索引当前与退休区间)：2026-10-25父f51c412490上，Arena current/retired位置索引从逐block展开改为每allocation单项区间，保留exact FREE/reuse、跨grant拒绝、generation和实际保护终结；有ID的grant查询继续O(1)，容量/Inspection仍按block计数。新增1方法及扩展真实2-block FREE/reuse方法；fixture仅补header占用的4096B后7项定向通过，原容量失败独立保留。最终5类590项零failure/error/skip，用例身份和1172输入匹配，style通过；独立RAT仍443父路径无新增。四JVM真实control对照中65536-block索引/Slot从65536降为1，control字节及replay一致；仅control metadata稀疏DATA夹具，不是完整预分配或服务证明。全部生成DATA已按终态/句柄检查清理，证据保留。完整full-bytes/B11/其余16步仍OPEN，目标active、生产Gate OPEN，默认OFF/SYSTEM/PARK及disabled不变。


[Bookie checkpoint 的 typed 按需编码与 100k 驻留](local-storage/README.md#bookie-checkpoint-的-typed-按需编码与-100k-驻留)：2026-10-25父e227679ff2上，Bookie common checkpoint记录改为冻结Command/sequence/status的typed按需编码，保留原sealed、首次结果及terminal成功覆盖，special raw和CONTROL预留不变；checkpointId仅解析commit。新journal/image精确bytes及最终五类591项全部通过、source/用例身份匹配，style通过、独立RAT443父路径无新增。真实100k native-control驻留中raw backing降至0；metadata保守下界append146.1→69.3MB、replay152.5→75.7MB，整进程live增量约下降87MiB。下界不代替上界，完整B11/full-bytes仍OPEN；fixture不含MetadataStore/SDK/service/RocksDB。原始测量、失败准备和cwd误复制清理独立保留，未修改原PDF。完整16步目标active、生产Gate OPEN，默认OFF/SYSTEM/PARK及disabled不变。


[Bookie common 结果与 checkpoint 的单一索引](local-storage/README.md#bookie-common-结果与-checkpoint-的单一索引)：2026-10-25父bd3beb346f上，common AppliedOperation同时拥有原结果与checkpoint tuple，保留失败terminal upgrade的少量历史override及原bytes/status/sequence。首次592项TLS关闭清理error及原绑定保留；修正helper的actual closeFuture/最终drain观察后，最终同一五类592项全部通过、source/用例身份匹配、style通过，独立RAT443父路径无新增。100k同seed重放减少20万Map entries/UUID，整进程live增量减少19,845,032 B；补计的九根对象图171,946,208 B是去重reachable shallow，不是独占retained/B11 PASS。下一项继续私有scope驻留压缩，保留String等值、外部target/incarnation、原sealed、正常路径免解码及实际终态；尚未实现不称完成。full-bytes/其他16步仍OPEN，目标active、生产Gate OPEN，默认OFF/SYSTEM/PARK及disabled保持。


[Bookie 私有 scope 的紧凑驻留](local-storage/README.md#bookie-私有-scope-的紧凑驻留)：2026-10-25父a0f6d8217a上，私有StoredScope已共享准确context/固定target，保留String等值、外部target/incarnation、原sealed/canonical bytes与DATA免解码路径。10项定向及最终五类594项全部通过，source/用例身份匹配、style通过，独立RAT443父路径无新增。两JVM同100k seed的九根对象图171,946,208→122,346,384 B（163.98→116.68 MiB），减少约140万对象；reachable shallow不提升独占retained/完整Bookie B11。继续运行期/暂态typed预算上界、完整B11与其余full-bytes，以及默认SDK/observe、同incarnation unclosed-session接受、多Arena及其他16步。目标active、生产Gate OPEN，默认OFF/SYSTEM/PARK及disabled保持；不以本切片结束完整目标。


[Managed storage 的 100k 账本与 Arena 短记录预留缺口](local-storage/README.md#managed-storage-的-100k-账本与-arena-短记录预留缺口)：2026-10-25源码8a81b08fb4上，新增正常startNew→原CONTROL driver填充100k→真实healthy close→fresh restart的完整isolated storage计量；200000 write/1563 force、全部route与drain通过，重放included图122,370,392 B（116.70 MiB）、整进程live134,196,304 B，FD/线程实际终结。另以原requestPool生成10000短记录、rotation和force冻结，Arena十根可达图3,812,536 B，高于整个storage CONTROL公式3,477,648 B，栈上snapshot另列；不是独占retained超额或portable bound证明。当前预留未单列Arena/快照成本，下一项按record/rows/high-water和真实并发重算，不以任意倍数替代。未改Java/TLA，原594/style PASS保持原绑定；B11/full-bytes及16步目标active、生产Gate OPEN，OFF/SYSTEM/PARK与disabled保持。


[CONTROL typed 状态与工作集的启动预留](local-storage/README.md#control-typed-状态与工作集的启动预留)：2026-10-25父0e63840b99上，以支持布局的typed/history/index、快照/recovery及预检前pending模型替代旧3L金额，仍在原service open前单次预留、实际close后释放，生产quota不自动扩大。支持64位HotSpot17/21与8..256对齐，其他布局拒绝启用实验服务；默认OFF/Classic不变。旧额度可启动的真实RED保留；8项定向和六类597项全部通过，source/用例匹配、style通过。四JVM实测所选类型shallow≤模型；1MiB日志/P16/S4保守预留96,518,568B，128MiB对应11,534,644,136B，不等于实际驻留。模型不闭合任意Throwable/JCA/native或完整状态计数证明；B11/full-bytes及16步目标active、生产Gate OPEN，OFF/SYSTEM/PARK与disabled保持。


[显式离线 backup/restore 运维入口](local-storage/README.md#显式离线-backuprestore-运维入口)：2026-10-25父86c9259dd7上，公开 offlineBackup factory 复用原 ISOLATED 配置冻结历史身份/bounds，capture 交付独立 CREATE_NEW/force 的56 B外部reference；import 复用当前 CLOSED/RepairIntent 授权与原复制/bootstrap，私有 image 只在实际 completion 后关闭。17个既有恢复场景改走公开入口，覆盖源目录丢失、坏reference、同步失败清理、取消及外部完成future不能提前释放native锁；定向5方法及扩大三类215项全部通过，输入/用例身份匹配。最终三处换行有非空白相同证明，style通过、独立RAT仍443父路径无新增；原失败与源码绑定保留，详细证据由上述 canonical 节拥有。仍为显式Java运维API，默认SDK/Main、完整恢复接受下界、同incarnation unclosed-session接受、B11/full-bytes及其余16步继续OPEN，目标active、生产Gate OPEN，默认OFF/SYSTEM/PARK和disabled保持。


[显式 SDK CLOSED 只读入口与依赖关闭顺序](profile-wire/README.md#显式-sdk-closed-只读入口与依赖关闭顺序)：2026-10-25父cc7b8cdd82上，BookKeeper显式enable/open复用原registered pool与CLOSED coordinator，单个open/handle持有lease到实际read/output终结；SDK close先排空Profile再关闭原metadata/executors，中断可继续，self-drain提前拒绝。三项真实服务定向与相关两类23项全部通过、输入匹配，style通过；新增有限模型1正向/3指定反例符合预期，旧Spec/11cfg保持原绑定；独立RAT仍443父路径无新增，原证据由上述canonical节拥有。默认builder拒绝保持，writer create/默认routing/Main可信issuer/full-bytes及其余16步继续OPEN，目标active、生产Gate OPEN，OFF/SYSTEM/PARK与disabled保持。


[Writer close 的实际终态与排队关闭](profile-wire/README.md#writer-close-的实际终态与排队关闭)：2026-10-25父4910d16933上，beginCloseWriter分开bounded result与actualCompleted；包内writer owner将已接受但尚未freeze的close纳入实际排空，local-stop先赢不发布CLOSED，成功callback等待lease、失败/deadline保持原回复。三项真实定向与相关14项全部通过、style通过，新增有限模型1正向/3指定反例符合预期，旧Spec/8cfg保持原绑定；独立RAT仍443既有路径无新增，原证据由上述canonical节拥有。用户已选择显式create/open；public create下一项需要原子fresh reservation归属，不能以matching READY重发writer。该入口仍待接入，完整目标active、生产Gate OPEN，OFF/SYSTEM/PARK与disabled保持。


[显式 SDK fresh create 与 writer 归属](profile-wire/README.md#显式-sdk-fresh-create-与-writer-归属)：2026-10-25父9ef30669e4上，接公开create与已选readonly入口共用单owner，原生fresh reservation/held birth复核约束writer发行；未交付原请求可在同SDK续接，handoff后不重发，SDK全局stop只本地排空并保留OPEN。定向11项及扩大四类68项全部通过、输入/用例身份匹配，style通过，新增有限模型1正向/4指定反例符合预期、旧Spec/8cfg保留；独立RAT仍443既有路径无新增。完整验收仍按canonical节实际结果判断。后续默认routing/Main可信authority输入、full-bytes/B11、其余恢复/历史GC/兼容/正式性能/综合验收继续OPEN；目标active、生产Gate OPEN、OFF/SYSTEM/PARK与disabled保持。


[共享 cohort 的多 ledger SDK 与子视图排空](profile-wire/README.md#共享-cohort-的多-ledger-sdk-与子视图排空)：2026-10-26父c5ec73163f上，显式SDK增加有界maxOpenLedgers、共享单Epoch/原6连接与独立CONTROL/DATA子视图；READ/LAC derived views按回复分离，最后子操作才关闭父lease/pool。首CONTROL复用已有pending等待实际授权，取消ready观察前不归还名额；原NONE背压与fresh归属保持。六项定向与五类275项全部通过、用例/输入匹配；最后一行纯换行有6个owner class指令一致证明且六项补验通过，style通过。新增有限模型1正向/5指定反例符合预期、原Spec/15cfg保持；独立RAT仍443既有路径无新增。各阶段源码/回执由canonical节拥有。首版默认builder继续Classic；完整目标active、生产Gate OPEN、OFF/SYSTEM/PARK与disabled保持。


[显式 SDK 写期换组与实际排空](profile-wire/README.md#显式-sdk-写期换组与实际排空)：2026-10-26父d232f2df64上，显式enableProfile可提供原replacement coordinator，请求冻结0..32的maxEnsembleChanges、默认0；同cohort standby在fresh claim前验证，初始DATA仍只含E，close保留未来目标CONTROL。已接受换组从ordered排队到实际cold/完成callback及连续变更均持有episode，SDK stop等待实际终结及子视图输出，停止后不采纳/重发且不制造CLOSED。三项定向及五类278项全部通过、输入/用例匹配、style通过；新增有限模型1正向/3指定反例符合预期、旧Spec/10cfg保持。首轮两项权限/存在oracle错误及原源码回执保留，修正仅查实际hot/index空坐标。独立RAT仍443既有路径无新增，证据由canonical节拥有。完整目标active、生产Gate OPEN、OFF/SYSTEM/PARK与disabled保持；Main可信输入/full-bytes/B11/恢复/GC/兼容/正式性能及其余16步继续OPEN。


[已发布 repair intent 的跨进程续接](profile-wire/README.md#已发布-repair-intent-的跨进程续接)：2026-10-26父4ba371d277上，prepareRepair在当前完整recovery context校验后优先取回全字节相同的持久intent，只有新intent才要求predecessor ensemble匹配；后续admit/publication/close权限校验保持。原生产在第二新JVM重载时CONFLICT的真实RED已复现；修复后两个协调器JVM从保存输入独立重读，首进程在durable close丢回复后actual completion并halt23，第二进程同birth/version/closeId续接、copy0且无新增DATA force，目标持续运行。定向1项及五类279项全部通过、源码/用例/child绑定匹配，style通过；独立RAT仍443父路径且字节全同、无新增。TLA未改，不移绑旧PASS；源码回执由canonical节拥有。原unclosed-session隔离、恢复接受下界/GC/Main/full-bytes/B11/兼容/正式性能及其余16步仍OPEN，目标active、生产Gate OPEN、OFF/SYSTEM/PARK与disabled保持。


[永久坐标根的 DATA 预留与关闭退休](local-storage/README.md#永久坐标根的-data-预留与关闭退休)：2026-10-27父277f4d6514上，原DATA启动单次预留加入按logicalShards/indexedEntries计算的永久occupied HashSet/Key/context容量，复用既有HotSpot布局模型，quota不足在storage/index/目录open前拒绝。实际owner/I/O/pin及资源close成功后退休hot五容器与共享startup inventory，再退额度；false/异常close保持原状态。两个目标RED已复现，修正后测试源码相同；两项定向及六类618项全部通过、输入/用例匹配，四JVM碰撞Key图不超模型、style通过，独立RAT443父路径且字节全同、无新增。原夹具第二写拒绝单独保留，不称产品RED；证据由canonical节拥有。金额只覆盖该永久根，full-bytes/B11及其余16步仍OPEN；Main首版按用户选择限已有旧服务受控部署，issuer/装载/进程验收未完成。目标active、生产Gate OPEN、OFF/SYSTEM/PARK与disabled保持。


[无客户端证书的真实 TLS 拒绝与预算归还](profile-wire/README.md#无客户端证书的真实-tls-拒绝与预算归还)：2026-10-27父47ba97ef85上，新增真实TLS1.3无客户端证书拒绝测试；以exact accepted server握手失败及实际channel终止为准，验证零明文/响应、AuthZ/authority read与存储effect，预算归零后同listener带证书UIC1完成durable INSTALL/ACTIVATE及normal admission。生产源码未改；定向1项及三类285项通过、冻结输入与用例匹配。实际格式/RAT回执由canonical节拥有。仅补认证边界证据，完整G1、Main issuer/装载、TLS总内存/full-bytes/B11及其余16步仍OPEN；目标active、生产Gate OPEN、OFF/SYSTEM/PARK与disabled保持。


[Owned Profile TLS 的 native 引用与服务终态](startup-readiness/README.md#owned-profile-tls-的-native-引用与服务终态)：2026-10-27父3414108da6上，明确无factory重载在原episode内创建TLS，实际listener/storage/credit/metadata/loops终结后释放；旧带factory重载仍borrowed。Profile handler选择/构造配置、refresh与close共用monitor，成功refresh释放旧owner、失败保留旧context，未交付engine异常归还；成功reinit拒绝，关闭幂等且拒绝新入口。相同probe字节在旧native context残留上RED、修复后2→1→0 GREEN；17项定向通过，五类540项中532通过/8为原Classic格式前置条件跳过、0失败错误。初次编译接线错误及原输入独立保留，源码/验证由canonical节拥有。仅补TLS实际ownership，不闭合Main issuer/装载、TLS总字节/full-bytes/B11、完整G1或其余16步；目标active、生产Gate OPEN、OFF/SYSTEM/PARK与disabled保持。


[Hot 与 restart inventory 的驻留元数据预算](local-storage/README.md#hot-与-restart-inventory-的驻留元数据预算)：2026-10-27父d4837e0ea7上，原DATA单次启动预留加入hot/pending元数据与restart inventory，复用Layout的identity/immutable容器及direct view上界；两map高水位、各shard失败admission、旧index返回窗口计入，正常Key/context沿occupied。Inventory寿命跨terminal退休/新ledger复用，另计旧C个context，不重复Key；quota不足在storage/目录前拒绝，原actual关闭/五容器与inventory退休后退费。预算RED已复现，阶段6/600项保留原绑定；最终费用修正仅inventory函数/注释，三类260项零失败/错误/跳过，style/RAT以canonical实际回执为准。四最终JVM独立2C context图不超模型，但不证明状态数/RSS/native/完整B11。raw-unpooled backing、解析/执行器/pool其余元数据、Main/完整G1与其余16步仍OPEN；目标active、生产Gate OPEN、OFF/SYSTEM/PARK与disabled保持。


[Pooled 解析工作区与派生 pending context](local-storage/README.md#pooled-解析工作区与派生-pending-context)：2026-10-27父5d4cd9ccda上，原DATA一次预留加入按pool几何K、owner/维护并发及P票据返回窗口计算的pooled解析工作区和PendingNormal独立context A；4KiB K20，hot重解析B不共享A。PoolSubmission在invoke前消费callback，切断旧CONTROL task到future DATA leader的回指；异常汇总/后续投递/actual drain保持。冷扫描复用page/content credit计本批元数据，按已验证n限制重读记录数，超额整批拒绝。预算与两callback实际RED已复现，6项定向通过，四JVM真实解析/Hot/Pending/A+B图不超模型；最终六类627项零失败/错误/跳过，style/RAT以canonical实际回执为准。完整read/driver/跨批扫描/compaction/pool/raw图及B11仍OPEN，不计性能/native/RSS或状态数证明；目标active、生产Gate OPEN、OFF/SYSTEM/PARK与disabled保持。


[恢复扫描的跨批工作区](local-storage/README.md#恢复扫描的跨批工作区)：2026-10-27父9a195f5b7f上，冷路径新增一个跨批credit，按C/S与Arena实际M/R/A计entries、foundMoves、stream ranges、headers/postcoordinates及独立旧seed；indexed入口先付费再建seed，保持到实际扫描/postcheck终结，不给normal Add新增credit或扩大生产quota。目标RED复现且同方法字节GREEN，四项定向与四JVM有限图通过；初次627项整集5失败/2错误及fork超时保留；同码7项及完整SDK235项复验通过，另五类392项保持原通过，全部627用例身份匹配；style通过，RAT443原路径/原字节无新增，具体初次根因未证明，实际回执由canonical拥有。该证据不闭合真实峰值/状态数/native/RSS或B11；其余full-bytes/Main issuer/完整G1与16步仍OPEN，目标active、生产Gate OPEN、OFF/SYSTEM/PARK与disabled保持。


[点读元数据与 loan 资源退休](local-storage/README.md#点读元数据与-loan-资源退休)：2026-10-28父c51f3ba9b5上，将 pooled reader/command/result/fresh Get 的已知元数据加入原service预留，无buffer LAC loan由原wire ResponseCredit另付；loan关闭清空三项资源，command在物理/owner双终态后退休payload。真实RED、定向/整类回执、初次cold-restart欠额与仅成功夹具同额补费、四JVM有限图及最终源码/class等价边界由canonical拥有。没有normal Add新增credit/atomic或生产quota扩容；此切片不闭合全部point-read调用者图、driver/执行器/pool、native/RSS或B11。Main具体issuer/受保护装载、完整G1及其余16步继续OPEN，目标active、生产Gate OPEN、OFF/SYSTEM/PARK和disabled能力保持。


[失败 DATA 前缀的 payload 引用退休](local-storage/README.md#失败-data-前缀的-payload-引用退休)：2026-10-28父a08d1043d1上，actual completion 后同时摘除prefix与Hot的已终结batch/reservation根，保留未完成I/O身份、UNKNOWN坐标、原through与真实双门归还；owner失败但force成功仍UNKNOWN且无虚构quarantine。三条真实RED与同文件GREEN、相关Bookie/SDK/重启回执由canonical拥有。execution/pool已知图与终止退根、Input的U/36边界、opaque/native、Main issuer、完整G1和B11仍OPEN；完整目标active、生产Gate OPEN、OFF/SYSTEM/PARK及disabled保持。


[执行与池的固定 Java 容量和关闭退根](local-storage/README.md#执行与池的固定-java-容量和关闭退根)：2026-10-28父c474431233上，第八项service DATA预留支付已知execution/owner/I/O/pool固定Java图及PREFER旧空容器重叠；成功native close/actual termination/input0后清多条内部执行别名，timeout/本次失败保持。旧路径两条RED、同文件定向/整类、四JVM边界与RefCnt补列的实际回执由canonical拥有。Input动态N=U/36、Thread/registry/Worker/AQS waiter、CPU/cleaner/native及完整B11仍OPEN；Main具体issuer/受保护装载、完整G1及其余16步继续推进，目标active、生产Gate OPEN、OFF/SYSTEM/PARK与disabled保持。


[Input 核心元数据与源引用退休](local-storage/README.md#input-核心元数据与源引用退休)：2026-10-28父cab51e1c8e上，第九项service DATA预留按N=floor(U/36)支付已知Input/context/heap-root/RefCnt/NIO与claim工作区；成功release后清根并退款，Entry/bulk同步退休旧源引用。四条真实RED及同字节测试的13项GREEN由canonical记录；Driver描述符/解析图、native、Main issuer、完整G1和B11仍OPEN，持续目标active、生产Gate OPEN，OFF/SYSTEM/PARK与disabled保持。


[Driver 批次回调与重复请求引用退休](local-storage/README.md#driver-批次回调与重复请求引用退休)：2026-10-28父2aadc71bb1上，首次deliver消费completion，duplicates/waiters在成功交接后清根；UNKNOWN与actual I/O双门、错误汇总保持。真实RED/GREEN和当前验证由canonical拥有；Driver完整容量、Main issuer、G1与B11仍OPEN，完整目标active、生产Gate OPEN，OFF/SYSTEM/PARK与disabled保持。


[Driver normal 与 recovery 的已知 Java 容量](local-storage/README.md#driver-normal-与-recovery-的已知-java-容量)：2026-10-28父16933ee747上，第十项DATA预留支付源/preparation、独立Bookie/owner集合及整串返回窗口的已知Java图；waiter成功post后换空容器，旧九项金额保持。公式、实际验证及有限测量边界由canonical拥有；raw/compaction/JDK/native、Main issuer、G1和B11仍OPEN，目标active、生产Gate OPEN，OFF/SYSTEM/PARK与disabled保持。


[Released SDK 与当前 Classic Bookie 的互操作](profile-wire/README.md#released-sdk-与当前-classic-bookie-的互操作)：四版released SDK对固定28e4c48138当前Classic完成真实create/write/close、独立重读及健康同roots重启后重读，12个SDK进程/两次server/JDI effect与实际排空由canonical记录。原665-vector不重跑、不移绑；此分支PASS_SCOPED，完整G1仍BLOCKED_UNVERIFIED，Main可信输入与B11仍OPEN，完整目标active、生产Gate OPEN，OFF/SYSTEM/PARK与disabled保持。
