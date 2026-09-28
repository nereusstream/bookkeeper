# 实验文件格式 v1

状态：EXPERIMENTAL / DISCARDABLE；只用于隔离目录，不承诺 stable on-disk compatibility。实现位于 `bookie.storage.segment`，writer 与独立 parser 同时交付；已有ALLOC/ALLOC_POOL的隔离Arena局部状态机、Bookie INSTALL/ACTIVATE/FENCE局部控制日志及显式DATA grant检查，尚无Bookie recovery grant/tombstone、selector、checkpoint发布或entry success authority。

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

DATA fileId 从0递增；superblock A/B 为 -1/-2，Bookie/Arena control 为 -3/-4。该版本只识别初始 generation，尚不选择 checkpoint，也不将完整身份块解释为 startup ready。

## DATA batch

固定 `fileIndex = logicalShard % dataFiles`；逻辑 streamId 等于 shard。每批128字节 header、1～256个 record、zero padding、32字节 footer，总长4096对齐，最多2 MiB，不能跨文件容量。默认新写 payload 上限 N=1 MiB；它是配置、普通请求、恢复请求、读取预算和 segment record 上限的最小值。普通/恢复 wire payload 上限分别为5,242,712/5,242,668，历史44字节差集全部拒绝。读取预算预留128字节 envelope；步骤11必须证明真实 adapter 的 envelope 能放入预算后才能启用。收紧新写配置不收紧已有 v1 文件的1 MiB读取能力。

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

Bookie type 1～8：ROUTE、CREDENTIAL、INSTALL、ACTIVATE、FENCE、RECOVERY_GRANT、EXPLICIT_LAC、TOMBSTONE。Arena type 1～7：ALLOC、ALLOC_POOL、MOVE、FREE、CHECKPOINT_BEGIN、CHECKPOINT_COMMIT、DEVICE_STATE。此处是 framing 类型目录；隔离Arena日志只解释ALLOC/ALLOC_POOL，Bookie日志只解释INSTALL/ACTIVATE/FENCE，其余type仍无语义apply。parser 验证期望 owner、log generation、连续 sequence、非零 operation identity 及 checksum，不将 decode 等同于命令成功。步骤8负责 checkpoint/rotation。

Bookie局部INSTALL把route、instance、descriptor、加密verifier与初始inactive状态原子绑定在一条记录中；ACTIVATE是后续独立记录。两种body均先按顺序放入operation u32（INSTALL=1、ACTIVATE_INITIAL=2、ACTIVATE_REPLACEMENT=3）、ledgerId u64、instance[16]、descriptor identity[36]、目标storage incarnation[16]、authority generation u64、public semantic payload identity[32]，再放目标Bookie UTF-8长度u16+bytes、authority domain UTF-8长度u16+bytes。INSTALL另附canonical descriptor长度u16+bytes及49字节sealed verifier：version=1 u8、AES-GCM nonce[12]、ciphertext[20]、tag[16]。控制header的operation UUID绑定重试，operation generation等于authority generation；expected generation当前固定0，具体条件在顺序apply时判断。sealed字段AAD绑定storage incarnation、Arena UUID、BookieId和60字节ledger context；AES密钥由调用方从目录外提供，丢失/错误密钥拒绝重开。字段CRC只检测意外损坏，不能替代控制文件的访问保护或外部committed authority。

Bookie日志写完整batch并一次`force(true)`后才按sequence应用；未apply时INSTALL不开放normal写，已apply INSTALL仍inactive，只有匹配ACTIVATE后局部准入查询才为真。重开先验证完整framing与连续序号，再恢复同步并完整重放；错误密钥、损坏/不完整尾部拒绝可写打开。不支持尾部猜测截断、checkpoint/rotation、真实MetadataStore direct read、生产listener或请求级local result；局部准入查询也不是逐entry local success。

Bookie局部FENCE记录的body固定为60字节ledger context；header使用wire操作UUID、operation generation=1、expected generation=0。先由匹配context和20字节verifier的受控请求在Bookie状态锁下关闭新normal准入；已取得的有限DATA准入票据必须保持到其物理I/O终结，未终结时返回`WAITING_PRECUT`且不写FENCE。此前已durable的ACTIVATE仍须先按原序apply，切断准入后的ACTIVATE不得再追加。票据全部终结后才对单条FENCE写入并`force(true)`，再应用monotonic fenced状态并回复durable；慢force期间不重开准入，force不确定则当前log fail-closed。完整FENCE记录重开时同步后重放，重复操作从持久状态解析。隔离DATA物理驱动可接收与每条普通record一一匹配的预准入票据，直到真实FileChannel I/O和原owner回调均终结才释放；较早批次失败时，后批即使已收到失败回调，仍须等自身I/O终结。Bookie控制owner驱动让FENCE的write/force在预留控制I/O执行，durable后回原owner apply；控制I/O拒收且尚未写入时退回物理后缀预留，保留本进程的准入切点供同操作重试。原物理实验入口不提供Bookie authority；实际Add local result、response loss与底层I/O ownership的联合验收仍待步骤6。

当前ALLOC/ALLOC_POOL body为48字节：file index u32、owner shard u32、owner shard generation u64、allocation ID u64、绝对文件offset u64、whole block数u32、保留零u32、stream首个物理sequence u64。allocation ID为正数，同一Arena内不能重复授给另一范围；同一shard/generation后续分配必须声明相同首序号，否则按条件失败且不授予空间。当前仅接受expected generation=0，成功分配的allocation generation为1；ALLOC要求1块，ALLOC_POOL可覆盖连续多块，每块1 MiB且从DATA身份块后的4096开始。控制header的operation UUID/generation绑定命令重试，文件身份块绑定Arena与storage incarnation。局部日志按batch完整写入并同步，然后按sequence逐条判断/apply；失败no-op仍消耗sequence。只有已同步且成功apply的grant及stream起点可查询；每文件已授权block的最高结束位置可由重放状态计算，并随只读DATA恢复候选报告。零填充的已授权block仍包含在内；这个水位不证明pool中的DATA位置已用或未用，也不授权普通Add或可写重开。完整记录在重开时验证并同步后重放，未知/损坏尾部拒绝可写恢复；容量满额停止，尚不支持FREE、复用和rotation。此前40字节语义记录不被新解释器接纳；此文件格式仍为可丢弃实验，不承诺跨修订版本重开。

隔离文件的`writeAuthorized`在真实DATA写入前核对同一目录和storage/Arena身份、冻结batch的完整物理范围、allocation ID/generation、shard及其stream generation与已apply的Arena grant；当前固定shard原型把stream generation作为owner shard generation。跨两个1 MiB块的batch必须拥有同一pool grant。原始`write`仍只供底层framing/I/O实验，不提供授权检查。该路径没有Bookie route/fence、DATA used/unused恢复或逐entry成功判定。

`SegmentDataRecovery.inspect`只读扫描已停写的DATA文件：零页计数不推导unused；非零页必须形成CRC和身份完整、整段有当前grant的batch。完整但无grant的原始格式写入也拒绝。`validateAndSync`在同一目录锁与验证路径下，对完整检查的DATA文件执行一次`force(false)`后才返回；注入同步失败或无grant/脏页时不返回成功结果。`recoverPhysicalInventory`沿同一路径、在明确entry容量内，待全目录验证及同步均完成才返回普通`ENTRY_DATA`的物理位置；`RELOCATION_COPY`不进入普通清单，容量不足也不泄露部分清单。清单不发布到热目录，不提供连续物理前缀、当前selector、可复用判定或Bookie级恢复权限。

运行时DATA提交先预留固定物理范围，再由I/O线程对同一冻结batch完整写入并force；同文件较高offset可以先返回，但`SegmentPhysicalPrefix`先核对已apply的Arena stream首序号，再仅按连续sequence和各reservation的真实force释放物理候选。恢复扫描也拒绝与重放的Arena首序号不一致的调用参数。这个起点绑定不等于恢复了末端durable-through；volatile tracker不是持久成功游标，不能由它单独恢复ACK、定位或可写重启。

`SegmentHotDirectory`为冻结batch中的`ENTRY_DATA`先占有界热定位容量，再绑定实际DATA reservation；只有该reservation从上述连续物理前缀释放后才把解析位置转为普通物理候选。`RELOCATION_COPY`虽共享batch持久路径但不进入普通候选。第二份同坐标物理DATA仍被拒绝；受控normal重复检查可在原owner等待同坐标在途批次，或对已发布候选执行点读比较。权限化selector和RocksDB接管仍缺失，候选不是entry成功凭据。

同坐标比较的隔离慢路径可从上述候选点读已存entry，先按固定CRC32C布局校验新输入，再比较应用payload bytes及累计ledger length；piggyback LAC和随之变化的CRC不参与逻辑相等判断。不同context/entry坐标、坏CRC或不能点读均拒绝，不能推导已存DATA不存在。已发布候选的点读比较由有界I/O线程执行；在途同坐标normal重试由原owner比较冻结内容并等待原物理结果。两条受控路径均需Bookie准入票据，尚无完整timeout/取消、跨入口bytes预算或可写重启幂等。

隔离`SegmentPointRead`从已解析batch生成record位置，读取时先验证DATA文件身份、Arena grant和batch header，再按4096字节边界读取目标record header与内容；长度在分配前受格式上限和batch record区域约束，最终由独立parser核验record/BK CRC与locator身份。该原语不授予locator发布、read pin或DATA durability，调用方仍须先证明这些上层前提。当前Arena没有FREE/复用。

## 固定向量与验证边界

`src/test/resources/unified-wal-segment` 中 `entry-v1.hex`、`copy-v1.hex` 各4096字节，`arena-alloc-v1.hex` 为192字节，包含上述48字节语义body和首序号11。向量由独立 Python struct/逐位 CRC32C 生成，Java writer 不参与生成；Java writer 输出逐字节对照，独立 parser 读取同一向量。

复现命令和源码绑定见[本地存储状态](README.md)。真实 FileChannel 测试包含短写、同步失败、零进展、部分初始化、固定容量满额停止、授权点读和共享文件不同 stream 损坏边界。它们不证明完整restart、完整allocator、quorum、Linux native I/O、掉电持久性或性能提升。
