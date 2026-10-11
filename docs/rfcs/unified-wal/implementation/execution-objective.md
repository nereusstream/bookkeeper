目标：按已经确认并写入现有文档的 Unified WAL 16 步施工顺序，在本会话持续实施、验证并同步文档，逐步达到首个可保留数据生产候选的可审查交付。先实施步骤1～4，紧接步骤5～7形成真实写入—点读—重启恢复闭环，再按依赖推进8～10索引/回收/长期运行、11～15网络/MetadataStore/客户端/恢复/删除/rejoin，以及16综合验收与运维交付。不要只给建议或再次整理计划，也不要在完成一个小模块后自行停止整个目标。

运行方式：按用户当前约束，本会话固定使用 gpt-6.1-sol，reasoning effort 为 xhigh（覆盖早期 gpt-6-astra/high 设置）。全程只有一个 agent，禁止 spawn_agent、子agent、explorer/worker、内部并行agent、委派给其他会话或通过另一CLI启动第二个编码agent。可以运行普通构建、测试和必要的并发测试进程。原规划会话 01a0c84a-11d9-7951-80a4-8f2a6933c139 保留为所有步骤的规划检查入口；本会话承担实施。保持一个持续目标，按真实证据推进，不能把里程碑完成或上下文压缩当作总目标完成。

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
12. 实际启用A/C、必要D及组合模型与实现同步验证，有限模型PASS不代替fsync/TLS/内存证据。raw old-decoder/stock-binary当前延期状态与冻结反例保留，在发布对应stable面前按合同恢复验证；不永久DEFERRED却宣称兼容。所有historical sourceCommit、designInputsAtSourceCommit、receipt和旧结果目录保持原绑定，新验证有独立source/config/run身份。

执行记录：完整启动正文曾因目标工具的4000字符限制被拒绝（正文4111字符），因此目标工具使用引用正文的精简目标；应用迁移后工具返回无目标；随后依用户明确指令在迁移后的会话01a0c86a-7130-73b2-b68f-45d726cac59b恢复持续active目标（无token预算），没有将旧目标标记完成。原始未修改正文保留在交接备份 unified-wal-before-local-handoff-g_j26t8t，SHA-256为4e51eb6d300aec56aabf938d85f4c43bb4b26478e59d1f4ce99df4fed1a3de0d。本文件同步已被用户覆盖的工作区、提交与单agent模型约束。

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
