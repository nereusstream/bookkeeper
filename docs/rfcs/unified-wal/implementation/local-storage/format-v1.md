# 实验文件格式 v1

状态：EXPERIMENTAL / DISCARDABLE；只用于隔离目录，不承诺 stable on-disk compatibility。实现位于 `bookie.storage.segment`，writer 与独立 parser 同时交付；已有ALLOC/ALLOC_POOL及单调DEVICE_STATE文件隔离的Arena局部状态机，Bookie INSTALL/ACTIVATE/FENCE、recovery grant/close、explicit LAC/tombstone局部控制日志和受控DATA grant/逐entry结果。现有Bookie/Arena applied checkpoint与A/B发布切片见下文；真实RocksDB冷重建/覆盖页见下文；一般MOVE/FREE selector覆盖恢复、在线Bookie/Arena维护、服务级readiness与未正常关闭restart与生产entry success authority仍未完成。

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

Bookie日志写完整batch并一次`force(true)`后才按sequence应用；未apply时INSTALL不开放normal写，已apply INSTALL仍inactive，只有匹配ACTIVATE后局部准入查询才为真。重开先验证当前必需日志的完整framing与连续序号，再恢复同步并重放；显式rotation后的根与后缀规则见下文。错误密钥、损坏/不完整尾部拒绝可写打开。不支持尾部猜测截断、真实MetadataStore direct read、生产listener或请求级local result；局部准入查询也不是逐entry local success。

Bookie局部FENCE记录的body固定为60字节ledger context；header使用wire操作UUID、operation generation=1、expected generation=0。先由匹配context和20字节verifier的受控请求在Bookie状态锁下关闭新normal准入；已取得的有限DATA准入票据必须保持到其物理I/O终结，未终结时返回`WAITING_PRECUT`且不写FENCE。此前已durable的ACTIVATE仍须先按原序apply，切断准入后的ACTIVATE不得再追加。票据全部终结后才对单条FENCE写入并`force(true)`，再应用monotonic fenced状态并回复durable；慢force期间不重开准入，force不确定则当前log fail-closed。完整FENCE记录重开时同步后重放，重复操作从持久状态解析。隔离DATA物理驱动可接收与每条普通record一一匹配的预准入票据，直到真实FileChannel I/O和原owner回调均终结才释放；较早批次失败时，后批即使已收到失败回调，仍须等自身I/O终结。Bookie控制owner驱动让FENCE的write/force在预留控制I/O执行，durable后回原owner apply；控制I/O拒收且尚未写入时退回物理后缀预留，保留本进程的准入切点供同操作重试。原物理实验入口不提供Bookie authority；实际Add local result、response loss与底层I/O ownership的联合验收仍待步骤6。

当前ALLOC/ALLOC_POOL body为48字节：file index u32、owner shard u32、owner shard generation u64、allocation ID u64、绝对文件offset u64、whole block数u32、保留零u32、stream首个物理sequence u64。allocation ID为正数，同一Arena内不能重复授给另一范围；同一shard/generation后续分配必须声明相同首序号，否则按条件失败且不授予空间。当前仅接受expected generation=0，成功分配的allocation generation为1；ALLOC要求1块，ALLOC_POOL可覆盖连续多块，每块1 MiB且从DATA身份块后的4096开始。控制header的operation UUID/generation绑定命令重试，文件身份块绑定Arena与storage incarnation。局部日志按batch完整写入并同步，然后按sequence逐条判断/apply；失败no-op仍消耗sequence。只有已同步且成功apply的grant及stream起点可查询；每文件已授权block的最高结束位置可由重放状态计算，并随只读DATA恢复候选报告。零填充的已授权block仍包含在内；这个水位不证明pool中的DATA位置已用或未用，也不授权普通Add或可写重开。完整记录在重开时验证并同步后重放，未知/损坏尾部拒绝可写恢复；未执行下述显式Arena rotation时容量满额停止；尚不支持FREE与复用。此前40字节语义记录不被新解释器接纳；此文件格式仍为可丢弃实验，不承诺跨修订版本重开。

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

control容量≥10656字节（8192 header、416双commit预留、2048普通控制槽）时，普通写入保留416字节供rotation；更小旧配置保持有限日志并拒绝rotation。image仍各受2×control容量payload上限约束，首次条件结果及内部checkpoint历史不prune，状态满额停止。没有在线调度或长期历史压缩，不据物理日志回收宣称可长期运行。

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
