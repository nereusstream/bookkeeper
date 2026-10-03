# 实验文件格式 v1

状态：EXPERIMENTAL / DISCARDABLE；只用于隔离目录，不承诺 stable on-disk compatibility。实现位于 `bookie.storage.segment`，writer 与独立 parser 同时交付；已有ALLOC/ALLOC_POOL及单调DEVICE_STATE文件隔离的Arena局部状态机，Bookie INSTALL/ACTIVATE/FENCE、recovery grant/close、explicit LAC/tombstone局部控制日志和受控DATA grant/逐entry结果。现有Bookie/Arena applied checkpoint与A/B发布切片见下文；完整selector、rotation、服务级可写restart与生产entry success authority仍未完成。

所有数值为 big-endian，offset 从相应结构开头按字节计算。保留字段和 padding 必须为零；CRC 为 CRC32C，覆盖范围采用 `[start,end)`。长度先经固定 header 校验和上限检查，再分配/读取 body。未知版本、类型、generation 或 mandatory feature 均拒绝。

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

Bookie type 1～9：ROUTE、CREDENTIAL、INSTALL、ACTIVATE、FENCE、RECOVERY_GRANT、EXPLICIT_LAC、TOMBSTONE、RECOVERY_GRANT_CLOSE。Arena type 1～7：ALLOC、ALLOC_POOL、MOVE、FREE、CHECKPOINT_BEGIN、CHECKPOINT_COMMIT、DEVICE_STATE。此处是 framing 类型目录；隔离Arena日志解释ALLOC/ALLOC_POOL及下述DEVICE_STATE子集，Bookie日志已解释INSTALL/ACTIVATE/FENCE、recovery grant/close、explicit LAC及tombstone局部合同；其余类型仍无语义apply。parser 验证期望 owner、log generation、连续 sequence、非零 operation identity 及 checksum，不将 decode 等同于命令成功。步骤8负责 checkpoint/rotation。

Bookie局部INSTALL把route、instance、descriptor、加密verifier与初始inactive状态原子绑定在一条记录中；ACTIVATE是后续独立记录。两种body均先按顺序放入operation u32（INSTALL=1、ACTIVATE_INITIAL=2、ACTIVATE_REPLACEMENT=3）、ledgerId u64、instance[16]、descriptor identity[36]、目标storage incarnation[16]、authority generation u64、public semantic payload identity[32]，再放目标Bookie UTF-8长度u16+bytes、authority domain UTF-8长度u16+bytes。INSTALL另附canonical descriptor长度u16+bytes及49字节sealed verifier：version=1 u8、AES-GCM nonce[12]、ciphertext[20]、tag[16]。控制header的operation UUID绑定重试，operation generation等于authority generation；expected generation当前固定0，具体条件在顺序apply时判断。sealed字段AAD绑定storage incarnation、Arena UUID、BookieId和60字节ledger context；AES密钥由调用方从目录外提供，丢失/错误密钥拒绝重开。字段CRC只检测意外损坏，不能替代控制文件的访问保护或外部committed authority。

Bookie日志写完整batch并一次`force(true)`后才按sequence应用；未apply时INSTALL不开放normal写，已apply INSTALL仍inactive，只有匹配ACTIVATE后局部准入查询才为真。重开先验证完整framing与连续序号，再恢复同步并完整重放；错误密钥、损坏/不完整尾部拒绝可写打开。不支持尾部猜测截断、checkpoint/rotation、真实MetadataStore direct read、生产listener或请求级local result；局部准入查询也不是逐entry local success。

Bookie局部FENCE记录的body固定为60字节ledger context；header使用wire操作UUID、operation generation=1、expected generation=0。先由匹配context和20字节verifier的受控请求在Bookie状态锁下关闭新normal准入；已取得的有限DATA准入票据必须保持到其物理I/O终结，未终结时返回`WAITING_PRECUT`且不写FENCE。此前已durable的ACTIVATE仍须先按原序apply，切断准入后的ACTIVATE不得再追加。票据全部终结后才对单条FENCE写入并`force(true)`，再应用monotonic fenced状态并回复durable；慢force期间不重开准入，force不确定则当前log fail-closed。完整FENCE记录重开时同步后重放，重复操作从持久状态解析。隔离DATA物理驱动可接收与每条普通record一一匹配的预准入票据，直到真实FileChannel I/O和原owner回调均终结才释放；较早批次失败时，后批即使已收到失败回调，仍须等自身I/O终结。Bookie控制owner驱动让FENCE的write/force在预留控制I/O执行，durable后回原owner apply；控制I/O拒收且尚未写入时退回物理后缀预留，保留本进程的准入切点供同操作重试。原物理实验入口不提供Bookie authority；实际Add local result、response loss与底层I/O ownership的联合验收仍待步骤6。

当前ALLOC/ALLOC_POOL body为48字节：file index u32、owner shard u32、owner shard generation u64、allocation ID u64、绝对文件offset u64、whole block数u32、保留零u32、stream首个物理sequence u64。allocation ID为正数，同一Arena内不能重复授给另一范围；同一shard/generation后续分配必须声明相同首序号，否则按条件失败且不授予空间。当前仅接受expected generation=0，成功分配的allocation generation为1；ALLOC要求1块，ALLOC_POOL可覆盖连续多块，每块1 MiB且从DATA身份块后的4096开始。控制header的operation UUID/generation绑定命令重试，文件身份块绑定Arena与storage incarnation。局部日志按batch完整写入并同步，然后按sequence逐条判断/apply；失败no-op仍消耗sequence。只有已同步且成功apply的grant及stream起点可查询；每文件已授权block的最高结束位置可由重放状态计算，并随只读DATA恢复候选报告。零填充的已授权block仍包含在内；这个水位不证明pool中的DATA位置已用或未用，也不授权普通Add或可写重开。完整记录在重开时验证并同步后重放，未知/损坏尾部拒绝可写恢复；容量满额停止，尚不支持FREE、复用和rotation。此前40字节语义记录不被新解释器接纳；此文件格式仍为可丢弃实验，不承诺跨修订版本重开。

Arena `DEVICE_STATE`当前接受16-byte body的三种action。文件隔离使用version u32=1、action u32=1、DATA file index u32、reserved u32=0；expected generation固定0，operation UUID/generation绑定重试。writer会话使用version u32=1、action u32=2（OPEN）或3（CLOSE）、epoch u64；epoch必须为正且等于header的operation generation。OPEN的expected generation为epoch-1，CLOSE为epoch。未知版本/action、越界文件、非零reserved、epoch/header不符或其他长度拒绝重放，没有文件隔离clear/reset操作。

批次force后按序apply；重复相同操作返回ALREADY_DURABLE，同operation身份改body返回INCOMPATIBLE。OPEN要求前一个epoch已关闭、predecessor吻合且没有持久文件隔离；首个epoch另要求没有已有allocation，不为legacy补认健康历史。CLOSE要求同一epoch仍OPEN且无持久隔离；它不删除文件隔离或使unknown范围可复用。未匹配CLOSE的会话重放在恢复DATA sync前拒绝物理重开。仅持有同一writer目录锁的生命周期入口才授予运行时会话所有权；OPEN成功后文件writer才可接收DATA。关闭入口先关DATA准入、排空实际I/O/预留并关闭descriptor，保留目录锁至CLOSE I/O终结及结果处理，且要求Arena控制/故障通知已排空、无运行时文件错误。完整但force未知的OPEN重放仍保持不可写；完整但force未知的CLOSE只在验证控制记录并恢复同步后可解析为已关闭，不将原关闭调用的失败改成成功。OPEN前逻辑预留160字节CLOSE容量；不预分配实际文件系统空间，无逐entry会话记录。

文件隔离后续ALLOC条件失败，原grant/stream起点保留为所有权事实，隔离不是FREE。实际故障可先设置易失准入gate；该gate不改变较早持久ALLOC的结果，只有日志中的DEVICE_STATE改变重放状态。完整隔离或未关闭会话在控制恢复同步后仍阻止DATA物理重开；后次DATA sync成功不解除。无隔离记录不证明旧I/O错误不存在；未启用会话的legacy手动原型仍不承担该检测保证。实际driver与生命周期的验证边界见[本地状态](README.md)。

实际绑定的DATA合批路径在I/O前为该记录共享预留每文件160字节逻辑控制日志容量（128-byte header、16-byte body、16-byte footer）及原owner通知完成槽。普通控制追加不能占用这份容量，未知结果不释放实际I/O资源；成功DATA不写故障记录，只在I/O/owner终结后归还预留。控制忙时每文件保留一条通知并按原序执行，停收仍使用原ticket排空；预留不足先背压，Arena关闭/物理重开拒绝未终结通知。这不预分配实际文件系统空间，也不证明记录落盘前退出、实际控制I/O或owner失败已可跨重启解析。

正常关闭会话的`openForLocalResults`不增加新字节类型或逐entry成功日志。CLOSE及正session epoch只提供正常关闭依据；local result还要求同一排他锁内的完整DATA验证/恢复sync、Bookie route选择和稳定applied cut、每shard唯一未退役stream及原owner的有界前缀/定位bootstrap。连续through来自同次完整扫描，下一DATA batch接续原stream generation/sequence；writer session epoch独立递增，不是新stream或allocation generation。旧授权范围仍全部跳过，由既有ALLOC_POOL提供fresh范围。普通物理扫描/恢复候选保持原资格；legacy、未关闭会话、多未退役stream或任何验证/sync/cut失败不据此返回本地持久结果。范围限无MOVE/FREE且正常关闭的管理会话，不证明一般selector、未正常关闭恢复或生产ACK。

隔离文件的`writeAuthorized`在真实DATA写入前核对同一目录和storage/Arena身份、文件可用状态、冻结batch的完整物理范围、allocation ID/generation、shard及其stream generation与已apply的Arena grant；当前固定shard原型把stream generation作为owner shard generation。跨两个1 MiB块的batch必须拥有同一pool grant。原始`write`仍只供底层framing/I/O实验，不提供授权检查。该路径没有DATA used/unused恢复；Bookie票据与逐entry local result由上层隔离driver提供，不证明生产ACK。

`SegmentDataRecovery.inspect`只读扫描已停写的DATA文件：零页计数不推导unused；非零页必须形成CRC和身份完整、整段有当前grant的batch。完整但无grant的原始格式写入也拒绝。`validateAndSync`在同一目录锁与验证路径下，对完整检查的DATA文件执行一次`force(false)`后才返回；注入同步失败或无grant/脏页时不返回成功结果。`recoverPhysicalInventory`沿同一路径、在明确entry容量内，待全目录验证及同步均完成才返回普通`ENTRY_DATA`的物理位置；`RELOCATION_COPY`不进入普通清单，容量不足也不泄露部分清单。清单不发布到热目录，不提供连续物理前缀、当前selector、可复用判定或Bookie级恢复权限。

运行时DATA提交先预留固定物理范围，再由I/O线程对同一冻结batch完整写入并force；同文件较高offset可以先返回，但`SegmentPhysicalPrefix`先核对已apply的Arena stream首序号，再仅按连续sequence和各reservation的真实force释放物理候选。恢复扫描也拒绝与重放的Arena首序号不一致的调用参数。这个起点绑定不等于恢复了末端durable-through；volatile tracker不是持久成功游标，不能由它单独恢复ACK、定位或可写重启。

`SegmentHotDirectory`为冻结batch中的`ENTRY_DATA`先占有界热定位容量，再绑定实际DATA reservation；只有该reservation从上述连续物理前缀释放后才把解析位置转为普通物理候选。`RELOCATION_COPY`虽共享batch持久路径但不进入普通候选。第二份同坐标物理DATA仍被拒绝；受控normal重复检查可在原owner等待同坐标在途批次，或对已发布候选执行点读比较。权限化selector和RocksDB接管仍缺失，候选不是entry成功凭据。

同坐标比较的隔离慢路径可从上述候选点读已存entry，先按固定CRC32C布局校验新输入，再比较应用payload bytes及累计ledger length；piggyback LAC和随之变化的CRC不参与逻辑相等判断。不同context/entry坐标、坏CRC或不能点读均拒绝，不能推导已存DATA不存在。已发布候选的点读比较由有界I/O线程执行；在途同坐标normal重试由原owner比较冻结内容并等待原物理结果。两条受控路径均需Bookie准入票据，尚无完整timeout/取消、跨入口bytes预算或可写重启幂等。

隔离`SegmentPointRead`从已解析batch生成record位置，读取时先验证DATA文件身份、Arena grant和batch header，再按4096字节边界读取目标record header与内容；长度在分配前受格式上限和batch record区域约束，最终由独立parser核验record/BK CRC与locator身份。该原语不授予locator发布、read pin或DATA durability，调用方仍须先证明这些上层前提。当前Arena没有FREE/复用。

隔离资源owner `IsolatedSegmentStorage`启动和关闭复用以上OPEN/CLOSE、恢复扫描及原owner bootstrap顺序，不增加新的持久字节类型。全部shard bootstrap完成后才开放本地请求；关闭先停收并等待实际I/O、owner及消费方buffer lease，随后在目录锁内关闭DATA/记录健康CLOSE，再关闭控制日志。超时不生成CLOSE或提前释放锁，启动失败按相同顺序清理；完整Bookie readiness、未正常关闭恢复和生产ACK仍未接通。

## Arena checkpoint与发布页

当前只持久化Arena已执行状态表：每个operation的首次命令、原sequence与条件结果，按sequence排序。重复请求不会重复存储，条件失败的no-op仍保留；由它重建当前allocation/stream/session/quarantine及幂等状态。through取连续applied sequence，空日志为-1，同时绑定该cut的真实log end offset。它不含DATA payload、ACK日志、pin/future/buffer历史，也没有FREE/MOVE selector。

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

恢复先验证和同步仍完整保留的旧控制日志，匹配commit、publication及image，选择最高完整可验证的发布代；从状态表重建后重放through之后的完整后缀。image或发布页损坏时可选择另一份，都不可用则从完整日志恢复。身份页损坏、必需旧日志缺失/损坏仍拒绝，不将坏尾静默截断。完整可读publication在暴露恢复结果前force。当前未回收任何旧控制prefix，不声称compact长期历史或完成rotation/retention；完整rotation/retention及在线维护接线仍未实现。

独立Python struct/逐位CRC32C生成的`arena-empty-checkpoint-v1.hex`（4224 bytes）、`arena-checkpoint-publication-v1.hex`（4096 bytes）、`arena-checkpoint-commit-v1.hex`（208 bytes）固定空cut=-1、generation=1、slot=A，commit使用UUID(1,2)。真实Java writer逐字节比较，独立parser读取同一向量；它们不改写既有DATA/ALLOC向量。

## Bookie checkpoint与双owner发布页

`SegmentControlCheckpoint`为两owner复用同一record/envelope编解码；Bookie条件状态仍由`SegmentBookieControlLog`执行。Bookie image `bookie-checkpoint-a/b`使用fileId=-7/-8、metadata magic=`UWBCP001`；包络字段/大小、SHA、status及CRC范围与上述Arena表相同。payload保存首次控制命令/结果、FENCE、checkpoint命令和每ledger最新EXPLICIT_LAC；按原sequence重建，不重复保存同operation重试或旧LAC。INSTALL继续携带原sealed storage value，以原身份绑定解封，不新增明文credential或DATA。

Bookie `BOOKIE_CHECKPOINT_COMMIT`使用owner=BOOKIE/code=10，body为64-byte reference，operation generation/expected generation分别是当前/前一checkpoint代。image force/目录同步/重读后，commit进入既有控制排他、write/force/apply顺序；durable且apply后才发布。自己的4096-byte `UWBSB001`页在superblock offset8192，完整时文件12288字节；Arena页仍在4096，两owner不截断或覆写另一owner的页。首次只发布Bookie时，中间Arena页保持零且不是有效Arena publication。只读取自己的完整、CRC正确页；Bookie页半写时，完整Arena页仍可使用。

恢复完整验证、解封与同步旧Bookie日志，匹配自己的commit/page/image，重建已执行条件结果及最新LAC，随后重放cut后完整后缀；runtime资源和pending gates不从checkpoint重建为已提交事实。较新image不可用可以回退另一份；两份均不可用时完整日志恢复。缺失/损坏mandatory旧prefix/suffix、错误密钥或identity仍拒绝打开。原格式原型不识别新增Bookie mandatory type/第三页，会拒绝；仍未提供stable兼容承诺。当前不回收旧日志，也没有在线维护控制入口。

独立Python struct/逐位CRC32C生成`bookie-empty-checkpoint-v1.hex`（4224 bytes）、`bookie-checkpoint-publication-v1.hex`（4096 bytes）和`bookie-checkpoint-commit-v1.hex`（208 bytes）；空cut=-1、generation=1、slot=A、commit UUID(1,2)。Java writer逐字节对照，独立parser校验同一字节；旧Arena/DATA固定向量保持原内容。

## 固定向量与验证边界

`src/test/resources/unified-wal-segment` 中 `entry-v1.hex`、`copy-v1.hex` 各4096字节，`arena-alloc-v1.hex` 为192字节，包含上述48字节语义body和首序号11。向量由独立 Python struct/逐位 CRC32C 生成，Java writer 不参与生成；Java writer 输出逐字节对照，独立 parser 读取同一向量。既有DATA向量使用ledger7/shard0，仅是framing oracle，不是通过固定shard准入的服务记录；其历史字节和回执不因新增路由校验而改写。

复现命令和源码绑定见[本地存储状态](README.md)。真实 FileChannel 测试包含短写、同步失败、零进展、部分初始化、固定容量满额停止、授权点读和共享文件不同 stream 损坏边界。它们不证明完整restart、完整allocator、quorum、Linux native I/O、掉电持久性或性能提升。
