# 实验文件格式 v1

状态：EXPERIMENTAL / DISCARDABLE；只用于隔离目录，不承诺 stable on-disk compatibility。实现位于 `bookie.storage.segment`，writer 与独立 parser 同时交付；已有ALLOC/ALLOC_POOL及单调DEVICE_STATE文件隔离的Arena局部状态机，Bookie INSTALL/ACTIVATE/FENCE、recovery grant/close、explicit LAC/tombstone局部控制日志和受控DATA grant/逐entry结果。现有Bookie/Arena applied checkpoint与A/B发布切片见下文；真实RocksDB冷重建/覆盖页见下文；全死allocation FREE与退役前缀已接通，见下文；一般MOVE/复用selector覆盖恢复、在线Bookie/Arena维护、服务级readiness与未正常关闭restart与生产entry success authority仍未完成。

当前增量仅将控制 checkpoint image metadata 升为 v2，原 v1 格式记录与向量保留；现行内部历史保留和恢复规则见[image v2](#内部-checkpoint-历史压缩与-image-v2)。

所有数值为 big-endian，offset 从相应结构开头按字节计算。保留字段和 padding 必须为零；CRC 为 CRC32C，覆盖范围采用 `[start,end)`。长度先经固定 header 校验和上限检查，再分配/读取 body。未知版本、类型、generation 或 mandatory feature 均拒绝。

离线备份的 v1 manifest 以原 Bookie control cut、文件长度和 SHA-256 封存现有控制历史。恢复入口验封后由该历史读取 effective progress 和最后实际 applied cursor。镜像保留原格式，恢复来源 witness 由集群在原准入事务中保存。详见 [恢复准入](../profile-wire/README.md#恢复准入绑定已封存镜像与原删除流)。

## 目录和文件身份

`IsolatedSegmentFiles.create` 只创建不存在的目录，使用 `CREATE_NEW` 和独占 `isolated.lock`，不接管已有目录。默认两个 DATA 文件 `data-0`、`data-1`，各16 MiB；全容量写零后写入身份块，再 `force(true)`。`superblock-a`、`superblock-b`、`bookie-control`、`arena-control` 初始各4096字节，仅含身份块。随后同步 lock、目录和父目录。失败关闭本次打开的资源，保留未完成目录用于诊断，重试不自动采用该目录。

身份块4096字节：

| Offset | 字段 |
| --- | --- |
| 0 / 8 / 12 | `UWSTOR01` u64 / version=1 u32 / fileId i32 |
| 16 / 32 | storage incarnation 16 bytes / Arena UUID 16 bytes |
| 48 / 56 / 64 / 72 | format / device-manifest / Bookie-control / Arena-control generation，均 u64=1 |
| 80 | checkpoint generation u64=0 |
| 88 / 92 / 96 | DATA file count u32 / logical shards u32 / bytes per DATA file u64 |
| 104 / 108 / 112 | alignment u32=4096 / readable payload maximum u32=1048576 / shared-block feature u64=1 |
| 120 / 124 | BookieId UTF-8 length u32（1～255）/ bytes，不能含 NUL |
| BookieId 之后～4092 | zero reserved |
| 4092 | CRC32C `[0,4092)` |

DATA fileId 从0递增；superblock A/B 为 -1/-2，Bookie/Arena control 为 -3/-4；Arena checkpoint A/B为-5/-6，Bookie checkpoint A/B为-7/-8。身份页的generation仍为初始值，动态checkpoint引用位于独立发布页，不auto-restamp身份页，也不把完整身份块解释为startup ready。

## 多 Arena 的显式 v2 身份基础

2026-10-30按指定规划会话裁决新增`SegmentArenaManifest`和v2身份页编码/校验；原三参数`SegmentFileHeader`仍产生上述逐字节不变的v1。后续本地接线已使独立Bookie control及两个Arena的真实文件/恢复/index可共享此scope；实际storage/service运行与发布图仍拒绝v2，完整required设备启动、预算和维护仍OPEN，不能据局部owner或部分Arena发布readiness。

manifest采用big-endian的`UWAMAN01`、schema u32=1、incarnation[16]、BookieId、Arena数量、独立Bookie control root、全局shard数量、按UUID字符串升序的全部Arena及逐shard的Arena索引。文本为u32长度加严格UTF-8；BookieId最多255 bytes，root最多4096 bytes，Arena和shard各1～64，单个输入最多1 MiB。每Arena记录UUID、绝对规范root、DATA文件数及每文件容量，root之间及与control root不得嵌套/重叠，全部容量用checked arithmetic。映射明确保留每个全局shard，不依赖worker或当前可读设备。manifest identity为`SHA-256(ASCII("BookKeeper/SegmentArenaManifest/v1\0") || 完整编码)`，不含可调worker/CPU/NUMA/准入额度或运行期checkpoint，也不含反向引用该digest的身份页。

共同Classic/Profile route域使用明确的manifest schema=2；schema1的五参数构造、原字节与hash域保持。schema2沿用上述字段，在shard映射后追加Classic root count u32（1～192），然后按绝对root字符串升序保存每根的u32长度+UTF-8及roles u32（JOURNAL=1、LEDGER=2、INDEX=4）。同一物理根仅出现一次并合并角色；完整清单必须包含全部三类角色，不接受未知位、空集合、重复、乱序或成员之间/与Segment根的嵌套重叠。仍受每root 4096 bytes和整体1 MiB上界约束，schema2摘要域为`BookKeeper/SegmentArenaManifest/v2\0`。身份页仍为下述显式v2，仅其完整manifest digest随新schema改变；没有原地升级已有绑定。当前native文件层不创建或验证Classic引擎、marker或commissioning sidecar，实际根集preflight由后续启动接线完成，不能据清单自认启用成功。

`writeNew`在已存在的canonical control root内以CREATE_NEW/NOFOLLOW、0600创建`arena-manifest`，处理完整短写、file force及目录同步；失败保留候选，不覆盖/截断采用。`read`先核对路径无symlink、文件private权限和有界长度，再与外部commissioned identity核对digest，最后严格解析及重编码比对、核对存放位置。此reader不发行commissioning、UID/旧服务/根访问或durability证明，不代替Main loader；完整初建还须持久创建control root、绑定同一操作进度、创建并验证全部required成员。正常restart不得把缺失成员补成空Arena。

v2仍为4096 bytes，offset 8显式version=2，offset112的mandatory feature为3（shared-block及共同拓扑）。原Bookie/incarnation/fileId/generation/alignment字段沿用；Arena文件保留自身UUID和持久DATA几何。Bookie scope使用全零Arena UUID、DATA文件数/容量为0，由manifest明确的control root绑定整体，不借用某个Arena表示Bookie。superblock A/B可分别属于Bookie或Arena；fileId -3/-7/-8只属于Bookie，DATA及-4/-5/-6只属于Arena。BookieId后到384仍为零，新增区如下：

| Offset | v2字段 |
| --- | --- |
| 384 / 392 | `UWTOP001` u64 / manifest generation u64=1 |
| 400 | manifest identity[32] |
| 432 / 436 / 440 | Arena数量u32 / role u32（1 Bookie、2 Arena）/ Arena index i32（Bookie为-1） |
| 444～4092 / 4092 | zero reserved / CRC32C `[0,4092)` |

v2 Bookie控制根由createManifestControl消费外部commissioned identity，在不存在的独立目录先保存完整manifest，再创建superblock-a/b和bookie-control并force/sync；per-Arena目录只创建其DATA、superblock-a/b和arena-control。Bookie的打开/恢复只验证自身文件及共同manifest，整Bookie的required设备恢复/readiness另由完整服务图证明。native member开/建要求canonical root或现有parent，allocator/driver/恢复/index均检查固定shard成员。

v2 INSTALL verifier仍使用原49-byte AES-GCM sealed字段，原storage binding（incarnation、零Arena UUID、BookieId及ledger context）后附manifest identity[32]作为AAD；v1 binding与bytes保持。Bookie/Arena control framing和checkpoint/rotation封套不改为另一套格式，其native身份页必须分别是相应scope的v2；派生index/coverage的scope digest在v2使用Arena身份页fileId=-4，v1仍用-3。复制control并重写身份页不能绕过verifier的manifest绑定，互换Arena index也不能通过scope校验。

v2校验同时检查完整manifest、所属root/config、文件角色及成员身份；修改mapping、布局、root或成员会改变共同身份，重算页CRC不使其匹配旧manifest。v1/v2互拒，v2不混入v1成员；不提供same-incarnation转换或header重盖章入口。需要搬运数据仍走新scope verified export/rebuild。此格式基础不提供stock Classic binary fence，现有目录/credential隔离合同保持。

## DATA batch

受控物理路径固定 `logicalShard = ledgerId % logicalShards`、`fileIndex = logicalShard % dataFiles`；逻辑 streamId 等于 shard。受控normal准入和物理热候选在DATA预留前核对每条record的路由；点读与重启扫描也拒绝错shard记录。底层framing writer和显式原始文件写入仍可构造格式正确但路由错误的实验字节，它们不授予服务权限。每批128字节 header、1～256个 record、zero padding、32字节 footer，总长4096对齐，最多2 MiB，不能跨文件容量。默认新写 payload 上限 N=1 MiB；它是配置、普通请求、恢复请求、读取预算和 segment record 上限的最小值。普通/恢复 wire payload 上限分别为5,242,712/5,242,668，历史44字节差集全部拒绝。读取预算预留128字节 envelope；步骤11必须证明真实 adapter 的 envelope 能放入预算后才能启用。收紧新写配置不收紧已有 v1 文件的1 MiB读取能力。

Batch header：

| Offset | 字段 |
| --- | --- |
| 0 / 8 / 12 | `UWDATA01` u64 / version=1 u32 / headerLength=128 u32 |
| 16 / 20 | fileIndex u32 / logical shard u32 |
| 24 / 32 | allocation u64 / allocation generation u64 |
| 40 / 48 / 56 / 64 | streamId u64 / stream generation u64 / batch sequence u64 / absolute file offset u64 |
| 72 / 76 / 80 | totalLength u32 / recordCount u32 / recordRegionBytes u32 |
| 84～124 / 124 | reserved / CRC32C `[0,124)` |

每个 record 的160字节 header：

| Offset | 字段 |
| --- | --- |
| 0 / 4 / 6 / 7 | `UWR1` u32 / version=1 u16 / origin u8 / reserved u8 |
| 8 / 12 | recordLength u32 / BK entryLength u32 |
| 16 / 24 / 32 | allocation u64 / generation u64 / absolute record offset u64 |
| 40 | 既有 `ProfileLedgerContext` 60 bytes |
| 100 / 116 | MOVE UUID 16 bytes / MOVE generation u64 |
| 124 / 128 / 132 / 140 | fileIndex u32 / shard u32 / stream generation u64 / batch sequence u64 |
| 148～156 / 156 | reserved / CRC32C `[0,156)` |
| 160 | 既有 BK entry：32字节 metadata、4字节 CRC32C、payload |

BK CRC32C 覆盖 metadata 和 payload，跳过原 CRC 字段；ledgerId 必须匹配 context，entryId≥0、LAC≥-1、累计 length≥0。origin=1 为 `ENTRY_DATA`，MOVE身份全零；origin=2 为 `RELOCATION_COPY`，MOVE UUID 非零且 generation>0。copy 可读不表示 MOVE 已 apply，不允许据此发布普通定位。record 自带坐标和独立 checksum，可只读 header 与该 record，无需为小 entry 读取整批；最终 selector、allocation pin 和读语义在步骤7接入。

Footer 从 totalLength-32 开始：`UWEND001` u64、sequence u64、totalLength u32、CRC32C `[128,footer)` u32、重复 header CRC u32、CRC32C `[footer,footer+28)` u32。所有 padding 必须为零。

`FrozenBatch` 私有持有完整字节，只暴露只读视图，提交后不再改写来源或提交位。写入坐标在调用 I/O 前保留；短写循环直到完成，再执行 `FileChannel.force(false)`。写入、同步或连续16次零进展失败后，受影响文件停止接收写入，坐标不归还，不通过截短共享文件或换 stream 绕过。这里只提供物理 batch 写入，无每 entry 的业务成功结果。

## 控制记录

128字节 header、opaque body、zero padding、16字节 footer，8字节对齐，总长144～65536。Bookie owner=1，Arena owner=2；type 编码限定在对应 owner 内。

| Offset | 字段 |
| --- | --- |
| 0 / 8 / 12 / 16 / 20 | `UWCTL001` u64 / version=1 u32 / owner u32 / type u32 / totalLength u32 |
| 24 / 32 / 48 / 56 | sequence u64 / operation UUID 16 bytes / operation generation u64 / expected generation u64 |
| 64 / 68 / 72 | bodyLength u32 / headerLength=128 u32 / log generation u64 |
| 80～124 / 124 | reserved / CRC32C `[0,124)` |
| 128 | opaque body，然后 zero padding |
| totalLength-16 | `CTLE` u32 / CRC32C `[128,footer)` u32 / sequence u64 |

Bookie type 1～9：ROUTE、CREDENTIAL、INSTALL、ACTIVATE、FENCE、RECOVERY_GRANT、EXPLICIT_LAC、TOMBSTONE、RECOVERY_GRANT_CLOSE。Arena type 1～7：ALLOC、ALLOC_POOL、MOVE、FREE、CHECKPOINT_BEGIN、CHECKPOINT_COMMIT、DEVICE_STATE。此处是 framing 类型目录；隔离Arena日志解释ALLOC/ALLOC_POOL及下述DEVICE_STATE子集，Bookie日志已解释下述ROUTE、INSTALL/ACTIVATE/FENCE、recovery grant/close、explicit LAC及tombstone局部合同；CREDENTIAL仍无独立语义apply。parser 验证期望 owner、log generation、连续 sequence、非零 operation identity 及 checksum，不将 decode 等同于命令成功。步骤8负责 checkpoint/rotation。

Bookie ROUTE（type=1）的body严格为64 bytes：schema u32=1、action u32（CLAIM_CLASSIC=1、BOOTSTRAP_COMPLETE=2）、value i64、enable binding UUID[16]及完整common manifest SHA-256[32]；header的operation generation固定1、expected generation固定0，record总长208 bytes。CLASSIC value为非负ledgerId，BOOTSTRAP_COMPLETE value为封口记录前序的applied cut，空导入明确为-1，与ledger0无关。native owner只在显式传入与schema2 manifest相符的RouteBinding时接受这些记录；缺绑定/错误digest在资源创建前拒绝，错误enable UUID在重放时拒绝。此输入本身不发行外部commissioning或根隔离authority。

ROUTE与Profile命令复用同一operation-id索引及durable后顺序apply，混合batch按实际记录顺序竞争。CLASSIC仅ABSENT→CLASSIC，PROFILE/TOMBSTONED冲突；后续Profile命令不能覆盖CLASSIC。未封口时不接受Profile语义状态变化，首次封口必须覆盖该记录之前的精确cut；匹配已封口结果可幂等重读，不能改cut。原operationId不同body冲突；所有仍有效的claims、封口和原结果进入同一流式checkpoint，rotation/restart不释放route。容量不足明确拒绝，不以Classic数据删除触发route GC。查询在PREPARED、未知I/O或未完成claim时不返回ABSENT。真实BookieImpl异步首次claim、全部legacy gate、root markers和native启用binding仍须接线；本段只描述native控制owner，不代表共同服务启用。

Bookie局部INSTALL把route、instance、descriptor、加密verifier与初始inactive状态原子绑定在一条记录中；ACTIVATE是后续独立记录。两种body均先按顺序放入operation u32（INSTALL=1、ACTIVATE_INITIAL=2、ACTIVATE_REPLACEMENT=3）、ledgerId u64、instance[16]、descriptor identity[36]、目标storage incarnation[16]、authority generation u64、public semantic payload identity[32]，再放目标Bookie UTF-8长度u16+bytes、authority domain UTF-8长度u16+bytes。INSTALL另附canonical descriptor长度u16+bytes及49字节sealed verifier：version=1 u8、AES-GCM nonce[12]、ciphertext[20]、tag[16]。控制header的operation UUID绑定重试，operation generation等于authority generation；expected generation当前固定0，具体条件在顺序apply时判断。sealed字段AAD绑定storage incarnation、Arena UUID、BookieId和60字节ledger context；AES密钥由调用方从目录外提供，丢失/错误密钥拒绝重开。字段CRC只检测意外损坏，不能替代控制文件的访问保护或外部committed authority。

Bookie日志写完整batch并一次`force(true)`后才按sequence应用；未apply时INSTALL不开放normal写，已apply INSTALL仍inactive，只有匹配ACTIVATE后局部准入查询才为真。重开先验证当前必需日志的完整framing与连续序号，再恢复同步并重放；显式rotation后的根与后缀规则见下文。错误密钥、损坏/不完整尾部拒绝可写打开。不支持尾部猜测截断、真实MetadataStore direct read、生产listener或请求级local result；局部准入查询也不是逐entry local success。

Bookie局部FENCE记录的body固定为60字节ledger context；header使用wire操作UUID、operation generation=1、expected generation=0。先由匹配context和20字节verifier的受控请求在Bookie状态锁下关闭新normal准入；已取得的有限DATA准入票据必须保持到其物理I/O终结，未终结时返回`WAITING_PRECUT`且不写FENCE。此前已durable的ACTIVATE仍须先按原序apply，切断准入后的ACTIVATE不得再追加。票据全部终结后才对单条FENCE写入并`force(true)`，再应用monotonic fenced状态并回复durable；慢force期间不重开准入，force不确定则当前log fail-closed。完整FENCE记录重开时同步后重放，重复操作从持久状态解析。隔离DATA物理驱动可接收与每条普通record一一匹配的预准入票据，直到真实FileChannel I/O和原owner回调均终结才释放；较早批次失败时，后批即使已收到失败回调，仍须等自身I/O终结。Bookie控制owner驱动让FENCE的write/force在预留控制I/O执行，durable后回原owner apply；控制I/O拒收且尚未写入时退回物理后缀预留，保留本进程的准入切点供同操作重试。原物理实验入口不提供Bookie authority；实际Add local result、response loss与底层I/O ownership的联合验收仍待步骤6。

当前ALLOC/ALLOC_POOL body为48字节：file index u32、owner shard u32、owner shard generation u64、allocation ID u64、绝对文件offset u64、whole block数u32、保留零u32、stream首个物理sequence u64。allocation ID为正数，同一Arena内不能重复授给另一范围；同一shard/generation后续分配必须声明相同首序号，否则按条件失败且不授予空间。当前仅接受expected generation=0，成功分配的allocation generation为1；ALLOC要求1块，ALLOC_POOL可覆盖连续多块，每块1 MiB且从DATA身份块后的4096开始。控制header的operation UUID/generation绑定命令重试，文件身份块绑定Arena与storage incarnation。局部日志按batch完整写入并同步，然后按sequence逐条判断/apply；失败no-op仍消耗sequence。只有已同步且成功apply的grant及stream起点可查询；每文件已授权block的最高结束位置可由重放状态计算，并随只读DATA恢复候选报告。零填充的已授权block仍包含在内；这个水位不证明pool中的DATA位置已用或未用，也不授权普通Add或可写重开。完整记录在重开时验证并同步后重放，未知/损坏尾部拒绝可写恢复；未执行下述显式Arena rotation时容量满额停止；尚不支持FREE与复用。此前40字节语义记录不被新解释器接纳；此文件格式仍为可丢弃实验，不承诺跨修订版本重开。

Arena `DEVICE_STATE` v1 有四种 16 字节 body。文件隔离依次编码 version u32=1、action u32=1、DATA file index u32、reserved u32=0，expected generation 固定为 0。会话记录依次编码 version u32=1、action u32、epoch u64。action 2 为 OPEN，action 3 为健康 CLOSE，action 4 为 `RESOLVE_NEVER_ALLOCATED_SESSION`。正 epoch 必须等于外层 operation generation。OPEN 的 expected generation 为 epoch-1，CLOSE 与 action 4 均为 epoch。未知版本、action、长度或身份绑定拒绝重放。旧 reader 遇 action 4 拒绝打开该历史。

批次 force 后按序 apply。同一 UUID 和命令沿用原幂等结果，改 body 发生冲突。OPEN 要求前一个 epoch 已结束、predecessor 匹配且无持久文件隔离，首个 epoch 还拒绝接管 legacy allocation。健康 CLOSE 由持同一 DATA 锁的实际 writer 提交。它先关闭新 DATA 准入、排空 I/O 和预留并关闭描述符，确认 Arena 控制和故障通知均已终结、没有文件故障，再持锁完成 CLOSE 的 I/O 和 apply。完整但 force 未知的 CLOSE 经新 owner 验证并恢复同步后可解析为已关闭，原调用仍失败。OPEN 前逻辑预留至少 160 字节 CLOSE 容量。

action 4 仅由既有 graph restart 在持原作用域 DATA 锁、完整 Arena 重放后提交。前驱须为同一仍 OPEN 的 epoch，无运行时 writer、控制未决或文件隔离，并且完整业务历史从未成功应用 ALLOC 或 ALLOC_POOL。已发 grant 后全部 FREE 仍不满足条件。apply 和 replay 都核对该条件。记录结束旧 epoch，保留 epoch 数值与幂等身份，checkpoint 和 rotation 保留该业务记录。原重开验证通过后以普通 OPEN 开启下一 epoch。该记录表达从未授权 DATA 写入的证明。已有授权历史的 OPEN 会话仍拒绝物理重开。

文件隔离后续 ALLOC 条件失败，原 grant 和 stream 起点仍保留。实际故障可先设置易失准入 gate，只有日志中的 DEVICE_STATE 改变重放状态。持久隔离和无法解析的 OPEN 在控制重放后阻止物理重开。缺少隔离记录或后续 DATA sync 成功均无法解除该限制。实际 driver 和生命周期验证见[本地状态](README.md)。

实际绑定的DATA合批路径在I/O前为该记录共享预留每文件160字节逻辑控制日志容量（128-byte header、16-byte body、16-byte footer）及原owner通知完成槽。普通控制追加不能占用这份容量，未知结果不释放实际I/O资源；成功DATA不写故障记录，只在I/O/owner终结后归还预留。控制忙时每文件保留一条通知并按原序执行，停收仍使用原ticket排空；预留不足先背压，Arena关闭/物理重开拒绝未终结通知。这不预分配实际文件系统空间，也不证明记录落盘前退出、实际控制I/O或owner失败已可跨重启解析。

`openForLocalResults` 要求已结束的正 session epoch。graph 已验证并应用 action 4 后也进入该路径。同一排他锁覆盖 DATA 验证与恢复同步、Bookie route 选择和稳定 applied cut、每 shard 唯一未退役 stream 及原 owner 的有界前缀和定位 bootstrap。下一批 DATA 接续原 stream generation 和 sequence，writer session epoch 独立递增。legacy、仍 OPEN 的会话、多个未退役 stream 或任一验证、同步、cut 失败均拒绝。普通物理扫描和恢复候选仍只提供物理观察结果。

隔离文件的`writeAuthorized`在真实DATA写入前核对同一目录和storage/Arena身份、文件可用状态、冻结batch的完整物理范围、allocation ID/generation、shard及其stream generation与已apply的Arena grant；当前固定shard原型把stream generation作为owner shard generation。跨两个1 MiB块的batch必须拥有同一pool grant。原始`write`仍只供底层framing/I/O实验，不提供授权检查。该路径没有DATA used/unused恢复；Bookie票据与逐entry local result由上层隔离driver提供，不证明生产ACK。

`SegmentDataRecovery.inspect`只读扫描已停写的DATA文件：零页计数不推导unused；非零页必须形成CRC和身份完整、整段有当前grant的batch。完整但无grant的原始格式写入也拒绝。`validateAndSync`在同一目录锁与验证路径下，对完整检查的DATA文件执行一次`force(false)`后才返回；注入同步失败或无grant/脏页时不返回成功结果。`recoverPhysicalInventory`沿同一路径、在明确entry容量内，待全目录验证及同步均完成才返回普通`ENTRY_DATA`的物理位置；`RELOCATION_COPY`不进入普通清单，容量不足也不泄露部分清单。清单不发布到热目录，不提供连续物理前缀、当前selector、可复用判定或Bookie级恢复权限。

运行时DATA提交先预留固定物理范围，再由I/O线程对同一冻结batch完整写入并force；同文件较高offset可以先返回，但`SegmentPhysicalPrefix`先核对已apply的Arena stream首序号，再仅按连续sequence和各reservation的真实force释放物理候选。恢复扫描也拒绝与重放的Arena首序号不一致的调用参数。这个起点绑定不等于恢复了末端durable-through；volatile tracker不是持久成功游标，不能由它单独恢复ACK、定位或可写重启。

`SegmentHotDirectory`为冻结batch中的`ENTRY_DATA`先占有界热定位容量，再绑定实际DATA reservation；只有该reservation从上述连续物理前缀释放后才把解析位置转为普通物理候选。`RELOCATION_COPY`虽共享batch持久路径但不进入普通候选。第二份同坐标物理DATA仍被拒绝；受控normal重复检查可在原owner等待同坐标在途批次，或对已发布候选执行点读比较。权限化selector和RocksDB接管仍缺失，候选不是entry成功凭据。

同坐标比较的隔离慢路径可从上述候选点读已存entry，先按固定CRC32C布局校验新输入，再比较应用payload bytes及累计ledger length；piggyback LAC和随之变化的CRC不参与逻辑相等判断。不同context/entry坐标、坏CRC或不能点读均拒绝，不能推导已存DATA不存在。已发布候选的点读比较由有界I/O线程执行；在途同坐标normal重试由原owner比较冻结内容并等待原物理结果。两条受控路径均需Bookie准入票据，尚无完整timeout/取消、跨入口bytes预算或可写重启幂等。

隔离`SegmentPointRead`从已解析batch生成record位置，读取时先验证DATA文件身份、Arena grant和batch header，再按4096字节边界读取目标record header与内容；长度在分配前受格式上限和batch record区域约束，最终由独立parser核验record/BK CRC与locator身份。该原语不授予locator发布、read pin或DATA durability，调用方仍须先证明这些上层前提。当前Arena没有FREE/复用。

隔离资源 owner `IsolatedSegmentStorage` 在持原 DATA 锁完成 Arena 重放后处理 action 4 的窄范围，再按原顺序启动 Bookie 控制 owner、派生索引、DATA 恢复与 shard bootstrap。全部 bootstrap 完成才开放本地请求。关闭先停收并等待实际 I/O、owner 和消费方 buffer lease，随后在目录锁内关闭 DATA、记录健康 CLOSE，再关闭控制日志。超时保留在途资源和排他锁，启动失败走同一清理路径。已授权 DATA 的异常退出恢复、完整 Bookie readiness 和生产 ACK 仍待验收。

## Arena checkpoint与发布页

原v1 image持久化Arena已执行状态表（现行v2增量见[内部checkpoint历史压缩](#内部-checkpoint-历史压缩与-image-v2)）：每个operation的首次命令、原sequence与条件结果，按sequence排序。重复请求不会重复存储，条件失败的no-op仍保留；由它重建当前allocation/stream/session/quarantine及幂等状态。through取连续applied sequence，空日志为-1，同时绑定该cut的真实log end offset。它不含DATA payload、ACK日志、pin/future/buffer历史，也没有FREE/MOVE selector。

`arena-checkpoint-a/b`先写4096字节身份页，再写128字节metadata和payload。metadata包络及superblock第二页的4096字节发布包络如下：

| Offset | 字段 |
| --- | --- |
| 0 / 8 / 12 | metadata为`UWACP001`、publication为`UWASB001`，各8 bytes / version u32=1 / referenceLength u32=64 |
| 16 / 24 / 28 / 36 / 44 | checkpoint generation u64 / slot u32（0=A、1=B）/ applied through i64 / control log end offset u64 / payload bytes u32 |
| 48～80 | payload SHA-256 32 bytes |
| metadata 80 / 84～124 / 124 | unique operation count u32 / zero reserved / CRC32C `[0,124)` |
| publication 80～4092 / 4092 | zero reserved / CRC32C `[0,4092)` |

payload的每项为`firstSequence i64 + status u32 + recordBytes u32 + 原control framing record`。status=1/2/3/4依次为APPLIED_DURABLE/ALREADY_DURABLE/CONDITION_FAILED/INCOMPATIBLE。记录sequence严格递增，首项为0，cut为-1时必须为空；record长度先按144～65536及剩余文件界限核对，再读取并独立解析。重建还检查operation不重复及条件结果一致。整个payload绑定SHA-256，metadata/image文件长度必须精确；单次编码/读入一个有界record，payload总量不超过现有controlLogBytes两倍，另加4224字节身份/metadata。

`CHECKPOINT_COMMIT`使用既有Arena type=6，body是上表16～80的64字节reference；operation generation为checkpoint generation，expected generation为其前一代。image完整写入、force、目录同步并重读验证后，commit才进入正常Arena顺序，durable且apply成功后才能写inactive发布页及force、验证。Arena A/B发布页位于原superblock offset4096，自己的页完整时文件至少8192字节；存在Bookie页时可到12288字节，原身份页仍保持不变；不完整第二页不能成为publication。旧隔离原型遇到新增mandatory CHECKPOINT_COMMIT/第二页会拒绝打开，未建立稳定格式兼容承诺。

恢复先验证和同步仍完整保留的旧控制日志，匹配commit、publication及image，选择最高完整可验证的发布代；从状态表重建后重放through之后的完整后缀。image或发布页损坏时可选择另一份，都不可用则从完整日志恢复。身份页损坏、必需旧日志缺失/损坏仍拒绝，不将坏尾静默截断。完整可读publication在暴露恢复结果前force。未显式调用下述Arena rotation的目录继续要求全部旧日志；Bookie rotation、长期历史压缩及在线维护接线仍未实现。

独立Python struct/逐位CRC32C生成的`arena-empty-checkpoint-v1.hex`（4224 bytes）、`arena-checkpoint-publication-v1.hex`（4096 bytes）、`arena-checkpoint-commit-v1.hex`（208 bytes）固定空cut=-1、generation=1、slot=A，commit使用UUID(1,2)。真实Java writer逐字节比较，独立parser读取同一向量；它们不改写既有DATA/ALLOC向量。

## Arena日志代与双恢复根

`SegmentArenaControlLog.rotate()`只在控制I/O/pending均终结、没有DATA writer owner或未终结隔离通知时执行。它独占控制提交，查询不持有慢I/O锁。`arena-control.lock`是固定sidecar排他scope，复用进程内开channel前拒绝重入及跨进程FileLock；日志及另一owner身份检查不打开该锁文件；这遵守[JDK FileLock平台限制](https://docs.oracle.com/en/java/javase/21/docs/api/java.base/java/nio/channels/FileLock.html)建议的单channel锁scope。DATA仍使用原`isolated.lock`。旧格式binary的协同写者互斥未验证，不允许混用版本；这是可丢弃实验格式。

先连续完成两个正常checkpoint：第一份普通applied cut为S，第二份cut为S+1，中间只有第一条内部CHECKPOINT_COMMIT；两条commit分别位于S+1/S+2，reference的offset相差208，checkpoint代相差1，slot相反。两份image因此保存相同普通authority，第二份另外包含第一条commit。新日志头保留两份完整reference及两个commit UUID，恢复第一份image时补演两条commit，恢复第二份时补演第二条。不得凭空重新生成UUID或重置逻辑序号。

新物理代使用固定交替文件`arena-control-generation-a/b`（偶数代=A，奇数代=B）。身份页仍为fileId=-4，第二页4096-byte header magic=`UWARG001`、version=1、reference长度64、16～80为第二份reference；80～256为下表的rotation extension，256～4092全零、4092为CRC32C[0,4092)。新日志从8192起，framing的physical log generation使用新代，control sequence从S+3继续；image中的记录始终规范化为log generation=1。

| offset | Arena发布页及日志头的rotation extension |
|---|---|
| 80 / 88 | magic `UWARP001` / physical log generation u64（≥2） |
| 96～160 / 160～176 | 第一份64-byte reference / 原commit UUID |
| 176～240 / 240～256 | 第二份64-byte reference / 原commit UUID |
| 256～4092 | 全零 |

Arena发布页仍使用`UWASB001`及原16～80 selected reference；首次rotation和后续普通checkpoint均保留同一physical generation extension，Bookie第三页不变。先写完整新文件、force、同步目录并验证header，才分别写/force/验证两个Arena发布页。两份都完整后，向原`arena-control`第二页写强制识别marker、截为8192并force；它不再是可重放日志。随后切换句柄并删除前一物理代、同步目录，才返回成功与恢复控制准入。旧段删除前，两份fresh image、commit及新日志根均已验证；中途失败不返回成功，当前实例停止控制写。

启动选择最高完整publication的physical generation，核对对应文件身份/header，完整检查并同步当前必要suffix，再从最高合法image恢复。旧段已回收时不能回退到空原日志；两份image均不可用、选中日志缺失/损坏或suffix截断均拒绝。单份image或publication损坏可通过另一份恢复同一普通authority及当前后缀；后续普通checkpoint可替换两份初始image。未发布的候选和旧代只在两份fresh当前根完整后，凭持久化的两条commit身份证明退休，固定交替路径不会无限生成文件。

容量≥8800字节时，普通控制及DATA failure预留保留额外416字节供两条rotation commit；更小旧配置仍按原有限日志运行并拒绝rotation。两份image各自保留最多2×control容量的操作状态，全部首次条件结果及内部checkpoint历史仍保留；达到状态预算明确停止。这证明物理prefix回收，不证明长期幂等历史压缩、自动维护、MOVE/FREE selector或长期存储有界运行。

独立Python struct/逐位CRC32C的`arena-rotation-header-v1.hex`和`arena-rotation-publication-v1.hex`均4096 bytes，固定物理代2、空普通cut、第一commit UUID(1,2)、第二commit UUID(3,4)；Java writer逐字节对照。JVM halt覆盖image、新文件、双根及marker的force前边界，实际掉电/文件系统barrier可靠性仍未验证。

## Bookie checkpoint与双owner发布页

`SegmentControlCheckpoint`为两owner复用同一record/envelope编解码；Bookie条件状态仍由`SegmentBookieControlLog`执行。Bookie image `bookie-checkpoint-a/b`使用fileId=-7/-8、metadata magic=`UWBCP001`；包络字段/大小、SHA、status及CRC范围与上述Arena表相同。payload保存首次控制命令/结果、FENCE、checkpoint命令和每ledger最新EXPLICIT_LAC；按原sequence重建，不重复保存同operation重试或旧LAC。INSTALL继续携带原sealed storage value，以原身份绑定解封，不新增明文credential或DATA。

Bookie `BOOKIE_CHECKPOINT_COMMIT`使用owner=BOOKIE/code=10，body为64-byte reference，operation generation/expected generation分别是当前/前一checkpoint代。image force/目录同步/重读后，commit进入既有控制排他、write/force/apply顺序；durable且apply后才发布。自己的4096-byte `UWBSB001`页在superblock offset8192，完整时文件12288字节；Arena页仍在4096，两owner不截断或覆写另一owner的页。首次只发布Bookie时，中间Arena页保持零且不是有效Arena publication。只读取自己的完整、CRC正确页；Bookie页半写时，完整Arena页仍可使用。

未执行rotation时，恢复完整验证、解封与同步旧Bookie日志，匹配自己的commit/page/image，重建已执行条件结果及最新LAC，随后重放cut后完整后缀；runtime资源和pending gates不从checkpoint重建为已提交事实。较新image不可用可以回退另一份；两份均不可用时完整日志恢复。缺失/损坏mandatory旧prefix/suffix、错误密钥或identity仍拒绝打开。原格式原型不识别新增Bookie mandatory type/第三页，会拒绝；仍未提供stable兼容承诺。显式停写rotation按下节回收旧prefix；没有在线维护调度入口。

独立Python struct/逐位CRC32C生成`bookie-empty-checkpoint-v1.hex`（4224 bytes）、`bookie-checkpoint-publication-v1.hex`（4096 bytes）和`bookie-checkpoint-commit-v1.hex`（208 bytes）；空cut=-1、generation=1、slot=A、commit UUID(1,2)。Java writer逐字节对照，独立parser校验同一字节；旧Arena/DATA固定向量保持原内容。

## Bookie日志代与双恢复根

`SegmentBookieControlLog.rotate()`复用上述相邻applied cut、两条原commit UUID、新文件/目录同步、两个根发布后才回收的顺序。`SegmentControlRotation`只共享两种owner的页编解码和文件名规则，权限条件仍由Bookie执行。Bookie在无在途normal/read/recovery admission、控制I/O或未apply batch时显式执行；期间拒绝新票据、其他控制提交及关闭，权限查询不等待慢I/O。固定`bookie-control.lock`覆盖整个切换，与Arena锁及DATA目录锁分别管理。

| 结构 | Bookie字节/路径 |
|---|---|
| 两个交替物理文件 | `bookie-control-generation-a/b`，偶数代=A、奇数代=B |
| 身份页 | fileId=-3，原storage/Arena/Bookie身份不变 |
| 第二页header | offset4096，`UWBRG001`，其余字段与Arena header相同 |
| rotation extension | offset80的magic=`UWBRP001`，原两份reference及commit UUID位置不变 |
| superblock发布页 | offset8192、magic=`UWBSB001`，两owner只更新自己的4096-byte页 |
| 新日志首记录 | offset8192，physical log generation使用新代，全局sequence接续 |

快照及内存中保留规范log generation=1的首次命令/结果、FENCE/内部commit与最新LAC。只在物理写入/读取边界转换framing generation及其header CRC，sealed INSTALL body不改写、不重新加密；恢复仍用原storage binding与外部密钥解封。重放检查条件结果、checkpoint predecessor和完整当前suffix，不重新授予已fenced/tombstone权限或已关闭grant，也不把runtime pending gate持久化为成功事实。

两根force/验证后，原`bookie-control`写mandatory marker并截为8192；它不再提供全日志fallback。最高有效publication选中的物理文件与完整suffix必须存在且通过验证/恢复同步；单份初始image可通过另一份恢复相同普通authority，后续checkpoint保留物理代绑定。两份image都无效时拒绝，不能回退空原日志。旧代或候选仅在fresh当前根及原commit identity证明其退休后删除，固定两条路径不无限增长。

control容量≥10656字节（8192 header、416双commit预留、2048普通控制槽）时，普通写入保留416字节供rotation；更小旧配置保持有限日志并拒绝rotation。image仍各受2×control容量payload上限约束，原v1首次条件结果及内部checkpoint历史不prune，状态满额停止；现行v2只压缩内部checkpoint历史，详见[后续增量](#内部-checkpoint-历史压缩与-image-v2)。没有在线调度或长期历史压缩，不据物理日志回收宣称可长期运行。

独立Python struct/逐位CRC32C生成`bookie-rotation-header-v1.hex`及`bookie-rotation-publication-v1.hex`，均4096 bytes，物理代2、相邻checkpoint代1/2、原commit UUID(1,2)/(3,4)。Java逐字节对照。真实文件测试覆盖sealed credential、fence/LAC/closed grant、三轮容量耗尽、7个JVM退出阶段、短写/force失败、跨JVM锁和双owner DATA正常restart；进程退出不等于真实掉电，也未验证旧binary混用写者。

## RocksDB派生定位与覆盖页

隔离冷启动使用现有依赖`rocksdbjni=9.9.3`，不复用Classic `KeyValueStorageRocksDB.Batch.flush()`的命名来推断持久性：后者是write调用，本路径显式执行`RocksDB.flush(FlushOptions.setWaitForFlush(true))`。单column family、`disableWAL=true/sync=false/useFsync=true`，每次WriteBatch最多256个定位；write buffer=1 MiB×最多2、block cache=1 MiB、最多32 open files和2 background jobs。这些是本组件配置，不是进程RSS/全部Bookie资源或磁盘峰值证明。WAL关闭时Put可能在进程退出后丢失，依据[官方说明](https://github.com/facebook/rocksdb/wiki/Basic-Operations)；[锁定版本flush API](https://github.com/facebook/rocksdb/blob/v9.9.3/java/src/main/java/org/rocksdb/FlushOptions.java)等待flush完成。

`derived-index`目录由RocksDB自己的排他锁保护。binding key为单byte 0；64-byte value依次为`UWIDB001`、version u32=1、locatorBytes u32=160、index UUID[16]、SHA-256[32]。scope SHA取本配置与identity的Bookie身份页（fileId=-3）完整4096 bytes；重建前通过同一CF WriteBatch更新随机index UUID并deleteRange定位namespace，再flush。破坏旧定位前已退休旧覆盖；关闭WAL的重新载入不改变Bookie/Arena权威日志。

定位key共77 bytes：namespace u8=16、magic=`UWIDXK01`、原60-byte ledger context、entryId u64；它绑定ledger instance/descriptor。160-byte value：

| Offset | 字段 |
|---|---|
| 0 / 8～24 | `UWIDXL01` / index UUID |
| 24 / 28 / 32 | DATA file index u32 / shard u32 / allocation u64 |
| 40 / 48 / 56 / 64 | allocation generation / stream generation / batch sequence / batch offset，均u64 |
| 72 / 76 / 84 | batch length u32 / record offset u64 / record length u32 |
| 88～148 / 148 | ledger context[60] / entryId u64 |
| 156 | CRC32C[0,156) |

fresh Get校验generation、key/context一致、CRC和记录范围，仅返回物理定位；调用者仍须执行Bookie权限、DATA record及当前selector/pin检查。普通/恢复ENTRY_DATA已在热发布后，通过已有owner DATA票据/共享DATA I/O异步调用不flush的WriteBatch；同坐标不同定位拒绝覆盖，不在append owner同步访问DB。每shard最多一个在途索引批次，最多256个定位引用；实际query-visible后原owner才移除匹配热记录，失败继续保留。无积压时pump不扫描热表。点读及普通/恢复重复写先hot，miss后在原DATA I/O执行fresh Get，再复用同一票据进行DATA读取和独立校验；Get不使用旧snapshot或negative-cache。文件pin从Get前保持到实际I/O/owner终结，捕获的不可变定位不因hot淘汰而失效，仍检查当前单stream前缀、Arena grant、Bookie权限及ENTRY_DATA来源。有限坐标占用集合没有定位/payload，保留已接受/unknown坐标；derived missing不能授权同坐标重写。热条目和索引/完整恢复条目采用独立容量，按固定shard拆分；无MOVE/FREE时坐标容量到限明确背压。后台失败关闭新DATA准入，实际I/O与原owner完成槽位终结后才归还工作集；关闭不提前释放native锁。一般MOVE/delete排序及完整native/disk预算尚未闭合，持久字节格式不变。

固定`index-coverage-a/b`为目录中的独立4096-byte页；单CF真实flush后才写完整inactive页、force、同步目录并重读验证。page位置：

| Offset | 字段 |
|---|---|
| 0 / 8 / 12 / 16 | `UWICOV01` / version u32=1 / stream count u32 / publication generation u64 |
| 24～40 / 40～72 | index UUID / scope SHA-256 |
| 72 / 80 / 88 | Arena applied cut i64 / Bookie applied cut i64 / locator count u64 |
| 96～128 | 按key无符号排序的完整key+value字节SHA-256 |
| 128之后 | 每stream 32 bytes：shard u32、reserved u32=0、stream generation u64、first/through u64 |
| 最后一个stream后～4092 / 4092 | zero reserved / CRC32C[0,4092) |

stream至多logicalShards个、按shard递增，当前无MOVE/FREE范围内每shard只接受一个已授权连续stream。冷重建范围来自同次完整DATA校验/同步及重放的Arena起点；运行期范围来自所有原owner已追平的连续物理前缀/坐标cut，并校验实际native行及Arena grant，不由最大entryId或Put完成推导。恢复只选scope/UUID匹配、CRC正确且全部locator数量/内容摘要相符的最高页，再force页及同步目录；marker缺失、坏页、数据库重建代不匹配或内容缺失不提供覆盖。删除`derived-index`后新UUID使外部旧页失效，控制与DATA仍是重建来源。publication或force失败不返回成功；完整未force页仅在恢复同步后可解析，不改变原调用失败。

`IsolatedSegmentStorage.restart()`在新writer session/owner开放前，全量校验同步并按Bookie本地route过滤DATA，重建DB和覆盖；新写由既有热表先提供定位，再异步入库。runtime Put本身不更新coverage；独立有界检查点每原worker占一DATA票据冻结已追平cut，暂停所有索引pump后真实flush并发布页。新DATA可留hot，不能扩大本次cut；全部原worker解冻后终结。publication序号取两份scope/CRC/UUID有效页最大值加一，不因其旧内容已不匹配重置；新UUID仍从1开始。运行期页写入/force失败返回UNKNOWN且不使既有query-visible行失效，native flush/内容失败仍终止DB使用。旧页只有全部实际内容仍匹配时才可解析。字段字节布局与固定向量不变。正常关闭restart现可在真实DB contents与scope/UUID/control/stream/grant匹配时，只校验覆盖内128-byte批头与连续性，未覆盖授权tail仍完整校验并恢复force；覆盖内必要头缺失或物理缺口仍拒绝。当前Bookie后缀决定过滤，覆盖本身不授予权限。有效覆盖保留index UUID，仅补写tail及移除不可读行，flush后推进publication；无效覆盖仍全量扫描/新代重建。不删除或缩减DATA/控制authority，点读继续独立record校验。索引损坏无法打开时报告错误，完整删除索引后可重建；没有自动清理未知损坏目录或在线重建。

独立Python struct/逐位CRC32C向量：`index-key-v1.hex`77 bytes、`index-location-v1.hex`160 bytes、`index-coverage-v1.hex`4096 bytes。固定index UUID(1,2)、scope SHA为32个0x04；仅为编码oracle，不是该scope的权限或实际文件authority。真实Get/flush、tombstone/copy排除、旧代退休、坏页/缺定位、native lock、短写/force失败、3个JVM halt阶段及删库后的storage图正常restart由局部回执拥有。

## 固定向量与验证边界

`src/test/resources/unified-wal-segment` 中 `entry-v1.hex`、`copy-v1.hex` 各4096字节，`arena-alloc-v1.hex` 为192字节，包含上述48字节语义body和首序号11。向量由独立 Python struct/逐位 CRC32C 生成，Java writer 不参与生成；Java writer 输出逐字节对照，独立 parser 读取同一向量。既有DATA向量使用ledger7/shard0，仅是framing oracle，不是通过固定shard准入的服务记录；其历史字节和回执不因新增路由校验而改写。

复现命令和源码绑定见[本地存储状态](README.md)。真实 FileChannel 测试包含短写、同步失败、零进展、部分初始化、固定容量满额停止、授权点读和共享文件不同 stream 损坏边界。它们不证明完整restart、完整allocator、quorum、Linux native I/O、掉电持久性或性能提升。


正常恢复使用的coverage证据携带源配置/身份，仅为内存proof，不在上述page加入机器路径。`Scan`统计实际成功扫描的读取bytes、完整校验bytes、covered batch数及跳过body bytes；不把固定头检查或恢复force省略，也不据这些计数声称设备I/O/性能结论。当前仍限正常关闭、单Arena/no-MOVE/FREE；字段编码和固定向量不变。


分配控制顺序与低地址追加（2026-10-03）：字节格式与固定向量不变。写入使用当前已apply的allocation operation原sequence作为grant顺序，stream cursor跨grant前进而不依赖文件offset递增；恢复每allocation只保留一个连续batch-sequence区间，再按原控制顺序拼接验证stream前缀。原sequence已由现有checkpoint operation状态保存，rotation不改绑定；低地址并不等于旧grant尾部可返回，仍不支持FREE或旧generation残留复用。实现与回执见[本地状态](README.md#分配控制顺序与低地址追加)。


## 整allocation FREE body v1

实验 Arena `type=4` 使用原控制 framing，总长272字节、body 128字节。FREE body 布局保持不变；后续健康会话下的整块复用使用下述 ALLOC generation 与 CLOSED v2 末端规则。header 的 expected generation 是当前旧 allocation generation，operation generation/result generation 为其 +1；首批实际转换为1→2。writer 必须持有实际 DATA proof 与新准入/I/O/pin 排空保护；独立 decoder 的 durable facts 不能直接作为新的 runtime proof 提交。

| Body offset | 字段 |
|---|---|
| 0 / 4 | body version u32=1 / reserved u32=0 |
| 8 / 12 / 16 | file u32 / owner shard u32 / stream generation u64 |
| 24 / 32 / 40 / 44 | allocation id u64 / allocation start u64 / blocks u32 / reserved u32=0 |
| 48 / 56 | stream first sequence u64 / original successful allocation control sequence u64 |
| 64 / 72 / 80 | first retired batch sequence i64 / through i64 / parsed record count u64 |
| 88 / 96 | required Bookie applied cut i64 / SHA-256 of the entire verified allocation, 32 bytes |

无 DATA 的初始全零范围使用 record count=0、first/through=-1；它不增加 stream 前缀。空 FREE 仍保留其必要 Bookie cut；恢复不能因没有物理序号范围省略该检查。非空区间有界、连续，所有 record 都具有相同匹配 allocation 身份及准确终态 tombstone，且不含 RELOCATION_COPY。外层仍验证 header/body CRC32C、sequence、owner/type、operation id 与 generation。成功 apply 后原 grant 失效，保留 retirement 和 generation 2；同一 operation 的重试不重复 bump，generation 0 ALLOC 不得复占退役 slot。

重放只按前序 applied grant、原 operation sequence、完整范围及 expected generation 判断，保留必要 Bookie cut；不重读已退役历史 DATA，也不通过重启时 pin=0 补认 proof。原 allocation 与 retirement facts 随既有 applied checkpoint/rotation 保留。固定独立向量 `arena-whole-free-v1.hex` 为272字节：file=0/shard=1、allocation=1、offset=4096、1 block、stream generation/start=1、原控制序号0、退役区间1..1/1 record、Bookie cut=0、digest=00..1f、FREE sequence=1、UUID=0:2。此向量验证编码/解码，不能单独授权 runtime 回收。


## 整块复用 ALLOC 与 CLOSED v2

ALLOC/ALLOC_POOL 仍使用上述48-byte body。初始 expected generation=0 创建 allocation generation 1；复用时 expected generation 必须等于完整 retired extent 的 FREE result generation（至少2），新 grant 使用该 generation。不存在通用 expected+1 的复用规则。id 唯一、同文件/整范围、准确 blocks 与当前 applied FREE 全部匹配才成功；任一 slot 已被 grant 占用则条件失败。旧 FREE 重试在 range 已复用后返回 CONDITION_FAILED/generation 0，历史事实仍保留以验证 stream。

Arena DEVICE_STATE type=7 的健康 CLOSED v2 body 为 `24 + 24*N` bytes，外层 CRC/framing 不变。只有当前 reused allocations 存在时使用 v2；无此类 grant 时保留原 v1 16-byte OPEN/CLOSED。header operation/expected generation 均为当前 DATA session epoch。body 大端：

| Offset | 字段 |
|---|---|
| 0 / 4 / 8 | version u32=2 / action u32=3 / session epoch u64 |
| 16 / 20 | tail count u32=N / reserved u32=0 |
| 24+24*i / 32+24*i / 40+24*i | allocation id u64 / current generation u64 / exclusive used end u64 |

列表按 allocation id 递增，精确覆盖所有当前 reused grants，不能缺失、重复、增加或更换 generation。end 在 allocation start..end 内且4096对齐；无当前 DATA 时 end=start。运行期证据由实际健康关闭的 writer 在 I/O/reservation/read lease 全部终结后提供，外部 decoded command 不能直接提交该见证。控制预算为当前及本批潜在新增 reused grants 预留完整关闭记录，N 最大2723；容量不足则拒绝扩大授权。有 reused grant 历史的 OPEN 会话继续拒绝 restart。

恢复仅解析末端内当前必要 DATA，禁止零页、错误 grant/generation、跨末端批次及 CRC 损坏；跳过末端后的旧残留。再次 FREE 可在健康排空 gate 下使用运行期实际 reservation end/已恢复 CLOSED end，完整 extent hash 仍包含被逻辑排除的旧 suffix 原始字节；SHA 不是独立删除或定位 authority。

独立 Python struct/逐位 CRC32C 向量 `arena-reused-allocation-v1.hex` 与 `arena-reused-close-v2.hex` 各192 bytes。前者 ALLOC_POOL sequence=2/UUID=0:3/operation generation=1/expected=2，file0/shard1/stream generation1/allocation2/offset4096/1 block/first sequence1；后者 CLOSED sequence=3/UUID=0:4/session epoch1，唯一 tail allocation2/generation2/end8192。Java独立读取并逐字节对照，向量本身不授予 runtime grant 或健康关闭权威。


运行期终态派生清理（2026-10-04）：native location key/value、index UUID、A/B coverage字节与固定向量不变。明确捕获的Bookie准确终态集合只用于当前有界维护：暂停全部原index owner与entry准入，实际native删除成功后原owner释放同一集合的坐标，再由现有runtime checkpoint验证更新计数/stream并flush/force发布。native query-visible删除不表示持久coverage，coverage失败不改写TOMBSTONE、FREE或DATA authority。随后出现的新tombstone在下一次处理，restart仍按Bookie后缀过滤；[实际实现与回执](README.md#运行期终态索引与坐标回收)不改变实验格式或发布兼容承诺。


`submitRelocation` 的内部持久化结果复用既有 RELOCATION_COPY envelope 和 DATA 地址，不新增持久/wire 字段。私有构造结果绑定现有 move identity/generation、目标定位及完整 entry SHA-256；copy durable completion 不发布普通 hot/derived locator，不授予 ACK 或 MOVE authority。格式向量及来源区别不变，执行证据见[内部完成切片](README.md#relocation-副本内部持久化完成)。


## 条件 MOVE v1

隔离 Arena type=MOVE（3），control framing不变；header expectedGeneration=0，operation UUID/generation绑定实际copy envelope。body包含version u32=1、rowCount u32（1..256）与required Bookie applied cut i64（>=-1），随后每行252 bytes：context60、entryId i64、源geometry64、目标geometry64、expected predecessor selector i64（初始-1）、source allocation control sequence i64、target allocation control sequence i64、完整entry SHA-256原始32 bytes。geometry为file/shard各u32，allocation/generation/streamGeneration/sequence/offset各i64，batchLength u32、recordOffset i64、recordLength u32。所有control引用必须早于本MOVE sequence；geometry满足固定shard、alignment、文件/batch/record范围。

最大body为64528 bytes，完整control record为64672 bytes，低于65536上限。最多256个唯一context/entry坐标；多行条件整批成立后才应用。源与目标完整entry bytes一致，operation generation为copy身份；selector为赢家的绝对Arena控制sequence。它不编码另一份结果日志或Bookie权限，重放按既有durable→顺序apply。

独立Python struct/逐位CRC32C生成的 `arena-move-v1.hex` 为416 bytes，sequence=3、UUID=0:601、operation generation7、Bookie cut1；唯一ledger44/entry1，源/目标allocation5/generation1/stream1、DATA sequence1→2、offset4096→8192、recordOffset4224→8320、recordLength204、前驱-1、allocation order0、digest00..1f。Java逐字节对照，另验证256行、257行拒绝、未知version、未来引用、错误record span与header generation；格式事实不授予新的runtime proof。执行范围见[当前MOVE切片](README.md#受保护的条件-move-与当前位置)。


## MOVE-aware FREE v2

Arena type=FREE/control header不变，body version u32=2、reserved u32=0，后续沿用v1 allocation identity48、allocation order/first sequence/through/record count/Bookie cut各i64，在原digest32之前增加Arena applied cut i64。body136 bytes、完整record280 bytes。Arena cut>=0且早于本control sequence；首次apply要求cut等于本sequence-1，操作精确重试仍先查询原结果及当前retired slot。纯准确终态普通DATA仍编码v1 body128 bytes，原oracle不变。

v2证明依赖当前selector：迁走的普通源须匹配成功后继entry digest，并实际验证当前目标；当前活COPY禁止FREE，未选COPY为orphan，准确终态COPY可退役其selector。grant/generation与原ALLOC序号、held runtime gate仍必须成立；decoded v2与摘要不授予新的运行期proof。metadata-only重放不再读取已退役源，现有CLOSED尾部与退役stream范围校验继续适用。

独立Python struct/逐位CRC32C生成 `arena-move-free-v2.hex`（280 bytes），沿用v1 sample sequence1、FREE operation generation2/expected1、allocation order0，在body offset96加入Arena cut0；digest字节仍00..1f。Java逐字节对照并拒绝cut=-1、当前/未来cut、decoded新提交，同时保留v1/128-byte断言。范围与回执见[当前回收切片](README.md#搬迁源与-orphan-的整块回收)。


2026-10-04 [MOVE覆盖恢复增量](README.md#native-move-索引维护与覆盖恢复)复用原160字节native locator和独立A/B coverage页，不改变MOVE/FREE、DATA或coverage字节格式。当前selector行经真实target校验、原owner排空及native flush后可发布覆盖；恢复匹配覆盖仍验证当前必要副本，旧hint或缺失/坏target不产生权限。此前格式向量不重绑。


2026-10-04 [合批compaction](README.md#同-arena-有界合批-compaction)沿用原ALLOC、COPY、MOVE及FREE v1/v2字节。FREE合批首次apply时，各v2 proof的Arena cut为起始applied cut加前序成员数；每条命令仍消费自身sequence，不能给全部FREE写相同cut。共享runtime保护和phase UUID派生不增加持久字段；既有独立向量保留。


2026-10-04 [多批compaction](README.md#同-arena-多批-compaction)不改持久/wire字段。一个新destination allocation内各COPY使用连续物理sequence和递增offset；首批root UUID，后批按 `COPY/<index>` 派生不同UUID，各自对应一次条件MOVE。整组FREE仍逐命令绑定自身前序selector cut；不增加另一份任务日志或成功位，独立向量继续适用。


## 内部 checkpoint 历史压缩与 image v2

2026-10-04，Bookie/Arena 新生成的 checkpoint image metadata 在offset8写 `version=2`；magic、64-byte reference、operation framing/status、payload SHA-256、metadata CRC和全部publication/control/rotation字节布局保持原值。旧v1 image继续读取，其首项sequence=0及完整checkpoint predecessor重演规则不变。版本2是明确的实验image扩展，不新增wire操作或长期兼容承诺；旧binary/完整旧decoder验证仍按既有Gate延期，不能混用写者。

v2保留所有现有业务状态表记录与原sequence/首次条件结果，包括ALLOC/stream/session/quarantine、MOVE/FREE/anti-ABA及Bookie sealed INSTALL、FENCE/T、grant/close和最新LAC。只筛选成功的内部CHECKPOINT_COMMIT：保留当前物理恢复根的两个commit UUID/reference与最新两个成功predecessor，最多四项；失败的内部条件结果也不丢弃。新根完整发布、原日志marker/prefix处理及旧代回收/目录同步全部成功后，才清理同类已替代内存记录；常规checkpoint与在途suffix仍占原控制日志预算。

因此v2 image的首项可以不为0，内部checkpoint generation可有间隔。只在版本2的已保存成功内部commit上，恢复核对原framing/reference、严格递增generation、generation小于正在恢复的reference及原结果，再恢复该commit的expected predecessor并正常apply；最后仍要求精确等于image generation减一。业务条件照原顺序重新检验，不据当前磁盘或pin补认成功；真实journal完整suffix仍禁止sequence gap。v1 image不允许该代际跳跃；未知version、缺失必需predecessor/commit、digest/身份/长度/结果错误仍回退或拒绝，不靠较大generation放行。

新增独立Python struct/逐位CRC32C向量 `arena-empty-checkpoint-v2.hex` 与 `bookie-empty-checkpoint-v2.hex`（均4224 bytes）：只把对应v1空image的metadata version改为2并重算CRC。既有v1向量保持原文件/哈希，实际Java writer同时逐字节核对v2和显式legacy v1 codec；读者区分两者，原commit/publication向量保持匹配。恢复、固定状态96轮、空状态/legacy误分类和损坏回退证据见[本地增量](README.md#内部-checkpoint-历史的有界保留)。

2026-10-05，原 journal 与物理轮转 journal 均复用各 owner 的同一 snapshot 恢复入口。原 journal 先核对全部完整控制记录、commit 与发布引用，并完成日志同步；随后合法 v2 image 可按内部 generation 间隔恢复，仍须准确保留目标代的 predecessor。正常条件结果、完整必要 suffix、发布页同步和坏 image 回退规则不变；不能因使用原 journal 而把完整 v2 快照误判为坏 image，或跳过 predecessor 校验。本增量不改任何持久字节/版本。

该扩展不压缩业务历史、T身份或DATA session/generation链；持续create/delete、反复MOVE/FREE/reuse、完整metadata内存/磁盘总账仍可能达到声明上限并拒绝。没有放宽2×control-state image预算或DATA/控制成功条件，不代表B17、长期负载、多Arena、掉电、旧binary或生产Gate通过。


2026-10-05，[Arena 当前状态索引](README.md#arena-当前授权与历史身份的分离)在成功控制 apply 时保留 grant 的首次序号、已用 ID 闭区间/高水位、增量退役范围和含空 FREE 的必要 Bookie cut。上述字段是由原完整命令 replay 重建的派生业务状态；原 ALLOC/FREE、MOVE、session、控制 framing 与 checkpoint image v1/v2 字节不变。当前 image 仍保存完整业务命令/首次结果，没有新增请求代或紧凑状态编码，也没有删除可证明幂等所需的历史。


2026-10-05，[当前 COPY 与 selector 状态](README.md#当前-copy-证明与-selector-状态)由原成功 MOVE/FREE replay 重建。原 MOVE 完整命令和首次结果仍进入 image v1/v2；当前索引不是新的持久编码，不改变 COPY 记录的 MOVE UUID/generation、准确几何或 entry SHA-256 校验。

## 受控 Classic 交接材料 v1

本节只冻结离线bootstrap的输入字节，不改变既有Segment文件/ROUTE/seal/native binding格式。codec为包内SegmentClassicHandoff，固定用途OFFLINE_CLASSIC_BOOTSTRAP=1，不提供私钥或通用sign-facts入口。验签返回数据；受控launcher的实际关闭/正常退出/UID隔离、固定retirement记录、材料文件保护和当前native状态仍由完整issuer/loader核验，当前接线及验收OPEN。

所有整数为big-endian；u32 Unix UID/GID按无符号读取。UUID为原16字节；摘要固定32字节。文本为u32字节长度＋严格UTF-8，不含NUL，不作Unicode或路径重写。未知schema/flags/purpose/role、重复/缺失必需组、超界长度、非规范字段和尾随字节拒绝。

| 材料 | 固定编码 |
| --- | --- |
| proof header | `UWCHOF01[8]`、u16 schema=1、u16 flags=0、u32 payloadLength、signerKeyId[32]；共48字节。 |
| proof signature | 末尾64字节Ed25519；总长≤65536，验签前先核对总长/payloadLength及外部已信任signer，再对原始header/payload验签后解析。 |
| signature input | ASCII `BookKeeper/CommonClassicHandoff/v1\0`（末尾实际NUL）＋完整48字节header＋payload。仅JCA `Ed25519`，无算法协商/压缩/证书URL。 |
| exact policy | `UWCBPOL1[8]`、u16 schema=1、u16 flags=0、u32 purpose=1、bindingId[16]、BookieId文本（1..255字节）、目标incarnation[16]、完整manifest identity[32]、目标UID:u32；总长≤512。只作完整字段相等匹配，无通配解释。 |
| AES key identity | 独立原始key必须32字节；`SHA-256(ASCII("BookKeeper/CommonClassicAesKey/v1\0") || rawKey)`，证明不包含key。 |
| retirement intent | `UWCRETI1[8]`、u16 schema=1、u16 flags=0、下述operation、u16物理成员数、物理成员数组；总长≤65536。policy与intent摘要各为其完整规范字节SHA-256。固定cluster＋Bookie文件路径/guard及create-only持久发布由launcher/loader承担，codec不写文件。 |

operation依次包含：u32 purpose=1；部署身份文本（1..1024字节）；程序/classpath/config清单摘要[32]；signerKeyId[32]（与proof header一致）；u32原native binding字节数（1..8192）及原BookieRouteBinding规范字节；native原birth UUID[16]、creation/modification zxid:i64、version:u32。只接受原PREPARED/version0。目标cluster/Bookie/Cookie出生/version/hash、bindingId、incarnation、manifest/control根复用该native记录，不复制第二份路径清单，也不补造Classic历史incarnation。

其后为requirements原来源：节点路径文本（1..4096字节）、envelope birth UUID[16]、creation/modification zxid:i64、version:u32=0、原metadata字节SHA-256[32]；原commissioned device/format/migration/delete-assignment generation各i64、minimum reader/writer各u32。均绑定原不可变记录，不能把当前native delete-assignment overlay重编码为旧材料。原始byte identity还覆盖其余requirements字段。

随后为实际episode：控制通道UUID[16]、PID:i64、进程start epochSecond:i64与nano:u32、registration birth zxid:i64与ephemeralOwner:i64；旧UID:u32、目标UID:u32、AES key identity[32]、精确policy摘要[32]。旧UID不得为0且与目标UID不同；其专用/无提权及子进程/其他fd使用者终结由实际issuer证明。PID和时间只辅助关联，codec不据此产生租约或接管任意进程。

物理成员为kind:u16、manifest索引:u16、device:i64、inode:i64（非零）；kind=1 Classic根（0..191）、2独立control根（仅0）、3 Arena根（0..63）。三组都必须存在，按kind/索引连续规范排序，禁止重复物理device/inode；总数≤257。proof对每成员另带当前uid:u32、gid:u32、mode:u32（仅07777位）。采用时还须与完整受保护manifest的准确成员数及实际物理/Unix状态匹配，不能从可读子集重算映射。

proof payload依次为operation、u16成员数及带权限的成员数组、u32实际drain结果=1、u32正常exitCode=0、终结控制sequence:i64、退出epochSecond:i64与nano:u32、实际关闭回执摘要[32]、u32完整Unix隔离结果=1、独立Unix验证回执摘要[32]、原retirement intent摘要[32]。时间只作审计，nano必须0..999999999。intent绑定同operation及同物理成员identity，关闭/权限回执均须由固定实际适配器产生；uid/gid/mode或结果常量本身不替代真实证据。

三个独立向量位于`bookkeeper-server/src/test/resources/unified-wal-segment/classic-handoff-{proof,policy,retirement}-v1.hex`。固定测试key来自[RFC 8032 §7.1 test 1](https://www.rfc-editor.org/rfc/rfc8032.html#section-7.1)，仅为公开测试材料；独立Python struct/hashlib按本节顺序编码，OpenSSL Ed25519签名及验签后，Java与JCA重现相同字节/签名。JCA算法名依据[Java 21 Signature标准名称](https://docs.oracle.com/en/java/javase/21/docs/specs/security/standard-names.html#signature-algorithms)。固定测试数据不证明真实旧服务episode、UID隔离或可采用session。


材料读取子owner采用的已有supervisor文件名为`scope-<hex>.lock`，对应intent为同stem加`.retirement`。hex是以下字节的SHA-256小写十六进制：ASCII `BookKeeper/CommonClassicSupervisor/v1\0`（实际末尾NUL）、clusterId的u32 UTF-8字节长度及原字节、原clusterBirthZxid:i64、BookieId的u32 UTF-8字节长度及原字节。所有整数big-endian；同cluster出生与Bookie的binding/episode/PID变化不改变名称。外部部署必须固定同一受保护supervisor目录，不能为重试换目录。loader子owner不创建这些文件；launcher的create-only intent、完整写入/force/真实close/不可覆盖发布与目录同步尚待实现。属性检查及fd/guard终态的当前边界由[交接材料 I/O owner 与 supervisor 终态](../startup-readiness/README.md#交接材料-io-owner-与-supervisor-终态)拥有。


原子发布沿用既有临时字节与final文件，不新增proof/native编码。只使用native-io窄renameNoReplace：Linux renameat2(RENAME_NOREPLACE)，macOS renamex_np(RENAME_EXCL)；temp与final同一受保护目录并持原supervisor guard，不用hardlink或可能覆盖的move。写入、force、首次native close成功后才改名，再目录同步及真实资源终结。successor仅核验同操作原final；临时文件无论是否完整/已签名都不自动promote或补签。rename后dirsync失败保留原失败和final，不回滚/重签；重启后final确实存在且所有原scope/物理/签名/retirement/当前metadata/Unix条件通过时，可完成恢复同步后复用，不追认原调用成功。final缺失/损坏/冲突拒绝。当前原语及未完成的publisher接合边界见[固定交接的原子不覆盖改名](../startup-readiness/README.md#固定交接的原子不覆盖改名)。


当前owned文件publisher使用同目录唯一`.handoff-candidate-<UUID>`作为本owner候选名；此名不产生新的supervisor scope/op/episode，也不是successor输入，恢复不枚举或promote它。候选与final沿既有retirement/proof规范字节；guard仍按cluster birth＋Bookie固定。原owner将候选完整写入、仅为该新文件设置已冻结目标UID、force并确认首次native close后，调用renameNoReplace，再复核原final inode/bytes及完成目录同步/close。只可安全重做目录同步，不覆盖/删除/回滚原final；已有final续用必须保持原packet/signature。实际实现与资源边界见[retirement 与 proof 的 owned 文件发布](../startup-readiness/README.md#retirement-与-proof-的-owned-文件发布)。
