# Spike A：Profile 显式安装协议否证规范

> 状态：**Planned / Not Executed**<br>
> 对应 RFC：[RFC-0001](../RFC-0001-profile-capability-install.md)<br>
> 性质：否证型；PASS 只允许 RFC 继续评审，不代表实现或生产就绪

## 1. 要回答的问题

本 Spike 只回答：两阶段 `INSTALL_LEDGER_PROFILE` 是否能在创建、重试、崩溃、mixed-version 和 ensemble replacement 下保持 fail closed。

主要风险：

- 未安装 ledger 被新 Profile Add 接受；
- 相同 instance、不同 descriptor 被静默合并；
- 安装信息重启后丢失；
- 一般 `E > W` 的轮转 write set 触达未安装 Bookie；
- replacement 先进入 metadata，随后才尝试 install；
- response loss 产生第二个 instance 或不一致 OPEN；
- 创建失败留下可被误用的 orphan install。

## 2. 非目标

- 不测 Segment payload 性能；
- 不决定最终 proto field number 或 on-disk layout；
- 不实现 Sequenced takeover；
- 不证明 general E/W/A recovery；
- 不把 happy-path demo 当作 RFC acceptance。

## 3. 运行前必须锁定

以下 manifest 任一字段为 `TBD` 时不得开始正式 Spike：

```text
sourceCommit
RFC0001Revision
prototypeCommit
metadataDriverAndVersion
BookieCount
clientCount
E/W/A configurations
Bookie engine cohorts
protocol version matrix
JDK and JVM flags
fault injector version
test seed policy
artifact output directory
```

最低 quorum 配置：

```text
3/3/2
3/3/3
4/3/2 or another E > W rotation case
```

正式结果必须使用全新 immutable output 目录；不得覆盖先前运行。

## 4. 最小原型范围

原型必须包含：

- metadata 状态 `PREPARING/INSTALLING/INSTALLED/OPEN`；
- canonical descriptor 与 hash；
- `INSTALL_LEDGER_PROFILE` request/receipt；
- Bookie durable `LEDGER_PROFILE_INSTALL` record；
- Bookie restart replay；
- Add 的 `ledgerInstanceId + profileDescriptorHash` 校验；
- capability/engine placement filter；
- replacement install-before-CAS；
- 可观测的 orphan install 状态；
- 精确错误码和 audit events。

允许使用简化 payload engine，但不得用 in-memory install 代替 durable record。

## 5. 观测与 Oracle

每个事件必须记录：

```text
testRunId
scenarioId
logicalStep
metadataVersion
ledgerId
ledgerInstanceId
descriptorHash
installRequestId
bookieId
localInstallGeneration
requestType
responseCode
faultId
monotonicTimestamp
```

Oracle 从三类证据独立计算：

1. MetadataStore versioned history；
2. 每个 Bookie durable control-record dump；
3. client request/response 与 fault timeline。

只看最终内存状态或最终 API success 不足以判定。

## 6. 场景矩阵

### A1：Install 前 Add

步骤：创建 PREPARING/INSTALLING ledger，在 0、部分和全部 Bookie install 前分别发送新 Profile Add。

Oracle：所有未 durable install 的目标 Bookie 返回 `EPROFILE_NOT_INSTALLED`；不能创建 Classic handle，不能写 payload。

### A2：部分安装成功

步骤：E 个节点中只允许子集完成 install，阻断其余节点，尝试 OPEN 和 write-set rotation。

Oracle：metadata 不得进入 OPEN；任何轮转 entry 都不能绕过未安装节点形成被客户端视为合法的创建完成。

### A3：Install response loss

步骤：Bookie durable install 后丢失 response，使用相同 request 重试，并分别让协调器 crash/restart。

Oracle：返回同一 instance/hash 的幂等 receipt；不产生第二个 local generation owner，不产生冲突 metadata。

### A4：Metadata OPEN CAS response loss/failure

步骤：全部 install 后，在 OPEN CAS 提交前、提交后 response loss、CAS conflict 三处注入。

Oracle：重新读取 authority 后只能得到一个 OPEN instance，或维持非 OPEN；不得盲建第二个 ledger instance。

### A5：相同 instance、不同 Profile

步骤：对已安装 instance 修改任一 mandatory descriptor 字段并重算 hash，发送 install/Add。

Oracle：确定返回 `EPROFILE_MISMATCH`；不得更新既有 routing，不得降级。

### A6：相同 ledgerId、不同 live instance

步骤：并发创建两个 instance，或在旧 instance 未终结时安装新 instance。

Oracle：至多一个 instance 可进入 OPEN；Bookie routing 不得把两者合并。

### A7：Engine/capability mismatch

步骤：将 Segment descriptor 发送到 Classic cohort；分别缺失每个 mandatory capability。

Oracle：placement 排除或 install 明确失败；无 payload 写入，无 lazy Classic handle。

### A8：`E > W` write-set rotation

步骤：全部 E install 后逐 entry 轮转；另做只安装 W 个节点的负向控制。

Oracle：全 E 安装时所有 write set 可用；只安装 W 的负向控制必须无法 OPEN。

### A9：Ensemble replacement happy path

步骤：选择满足 capability 的 replacement，install，恢复数据，CAS metadata，再发送命中新节点的 Add。

Oracle：durable replacement receipt 的时间/序列严格早于 active ensemble metadata。

### A10：Replacement install 失败

步骤：在 validation、control write、fsync、receipt 各处失败或丢包。

Oracle：失败节点不进入 active ensemble；不能先 CAS 后补装；允许选择新节点但不降低 Profile。

### A11：Bookie restart

步骤：在 install durable 前后、receipt 前后、首次 Add 前后重启。

Oracle：接受集合只由 durable record 决定；未 durable install 不复活，已 durable install 不丢失。

### A12：Old client / new Bookie

步骤：发送现有 Classic 请求。

Oracle：保持 Classic 兼容；不能误要求新 Profile identity，也不能把 Classic handle 标记为已安装新 Profile。

### A13：New client / old Bookie

步骤：分别使用 client-only opaque Profile 和需要 install 的 Profile。

Oracle：client-only 按 Classic 兼容合同工作；installed/Segment Profile 在 placement 或 install 阶段明确失败。

### A14：Orphan install

步骤：全部或部分 install 后永久放弃创建；触发候选 GC，并重放极晚 install response/retry。

Oracle：orphan 从未接受 Add；GC 只在权威 metadata 证明后执行；GC 后旧请求不能重新激活。

### A15：Watch/cache stale

步骤：暂停 metadata watch、返回旧 cache，直接发送 instance/hash 不匹配 Add。

Oracle：Bookie 本地 durable install 校验仍 fail closed；watch 不是正确性依赖。

## 7. 故障注入点

至少覆盖：

- metadata create write 前后；
- PREPARING -> INSTALLING CAS 前后；
- Bookie control record append、fsync、routing publish 前后；
- install receipt serialization/send 前后；
- INSTALLED/OPEN CAS 前后；
- replacement install、data recovery、ensemble CAS 各边界；
- Bookie restart/replay 中；
- coordinator restart 和 leader change；
- duplicate、delay、reorder、drop response。

fault injector 必须输出实际命中计数。计划注入但未命中的 case 不算执行。

## 8. 并发与随机测试

除确定性场景外，执行有 seed 的状态机 fuzz：

```text
create
install
retry
cancel
add
restart Bookie
restart coordinator
replace Bookie
stale metadata read
```

每个 seed 的操作序列、fault sequence 和最终 oracle dump 必须保留。失败 seed 必须可单独重放。

## 9. 硬 Gate

PASS 必须同时满足：

```text
uninstalled new-profile Add accepted                = 0
profile mismatch silently downgraded                = 0
instance mismatch silently merged                   = 0
engine/capability mismatch payload writes           = 0
ensemble metadata active before replacement install = 0
OPEN without all-E durable receipts                 = 0
restart lost durable install                        = 0
restart resurrected non-durable install             = 0
duplicate OPEN instances                            = 0
unreplayable executed fault scenarios               = 0
```

所有指定 deterministic scenarios 必须 100% 执行并命中 fault；所有断言为硬失败，不接受“低概率”。

## 10. 立即停止条件

发现以下任一项立即停止扩展原型，保留现场并回到 RFC：

- 未安装 ledger 的新 Profile Add 被接受；
- mismatch 进入 Classic 或其他静默降级；
- replacement metadata 先于 durable install 生效；
- crash/retry 产生两个 OPEN instance；
- Bookie restart 后接受未由 durable record 授权的请求；
- orphan GC 能让旧请求重新激活。

停止后不得通过修改 oracle、忽略 seed 或缩小协议结构继续宣称 PASS。

## 11. 必交 artifacts

```text
manifest.json
results.json
scenario-matrix.csv
event-log/
metadata-history/
bookie-control-dumps/
fault-injection-log/
failed-seed-reproducers/
checksums.txt
README.md
```

`README.md` 必须写明 source/prototype/RFC revision、环境、开始结束时间、执行者、Gate 结果和已知未覆盖项。

## 12. 结果解释

- PASS：只证明该原型和锁定矩阵未否证 RFC-0001，可以进入 RFC 接受评审。
- FAIL：RFC-0001 保持 P0 Blocked，先处理 counterexample。
- INCONCLUSIVE：场景未执行、fault 未命中、证据缺失或 oracle 不可复现；不得按 PASS 处理。

即使 PASS，也不授权 Segment 正式实现、authority 切换或生产部署。
