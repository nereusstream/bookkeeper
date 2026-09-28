# 实验文件格式 v1

状态：EXPERIMENTAL / DISCARDABLE；只用于隔离目录，不承诺 stable on-disk compatibility。实现位于 `bookie.storage.segment`，writer 与独立 parser 同时交付；仅有ALLOC/ALLOC_POOL的隔离Arena局部状态机，尚无Bookie控制、DATA接入、selector、checkpoint发布或entry success authority。

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

Bookie type 1～8：ROUTE、CREDENTIAL、INSTALL、ACTIVATE、FENCE、RECOVERY_GRANT、EXPLICIT_LAC、TOMBSTONE。Arena type 1～7：ALLOC、ALLOC_POOL、MOVE、FREE、CHECKPOINT_BEGIN、CHECKPOINT_COMMIT、DEVICE_STATE。此处是 framing 类型目录；目前仅隔离Arena控制日志解释ALLOC/ALLOC_POOL，其他type及Bookie控制仍无语义apply。parser 验证期望 owner、log generation、连续 sequence、非零 operation identity 及 checksum，不将 decode 等同于命令成功。步骤8负责 checkpoint/rotation。

当前ALLOC/ALLOC_POOL body为32字节：file index u32、owner shard u32、owner shard generation u64、绝对文件offset u64、whole block数u32、保留零u32。当前仅接受expected generation=0，成功分配的allocation generation为1；ALLOC要求1块，ALLOC_POOL可覆盖连续多块，每块1 MiB且从DATA身份块后的4096开始。控制header的operation UUID/generation绑定命令重试，文件身份块绑定Arena与storage incarnation。局部日志按batch完整写入并同步，然后按sequence逐条判断/apply；失败no-op仍消耗sequence。只有已同步且成功apply的grant可查询；查询不证明pool中的DATA位置已用或未用，也不授权普通Add。完整记录在重开时验证并同步后重放，未知/损坏尾部拒绝可写恢复；容量满额停止，尚不支持FREE、复用和rotation。

## 固定向量与验证边界

`src/test/resources/unified-wal-segment` 中 `entry-v1.hex`、`copy-v1.hex` 各4096字节，`arena-alloc-v1.hex` 为160字节。向量由独立 Python struct/逐位 CRC32C 生成，Java writer 不参与生成；Java writer 输出逐字节对照，独立 parser 读取同一向量。旧ALLOC向量body仅为两个u64(5,1)，只验证framing；它不符合上述32字节语义body，不能经Arena控制日志执行。

复现命令和源码绑定见[本地存储状态](README.md)。真实 FileChannel 测试包含短写、同步失败、零进展、部分初始化、固定容量满额停止、只读单 record 和共享文件不同 stream 损坏边界。它们不证明 restart、allocator、quorum、Linux native I/O、掉电持久性或性能提升。
