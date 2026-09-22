# Classic客户端维护

本目录记录施工步骤2的CLIENT-1/CLIENT-2独立补丁，不属于Segment ACK接入，不提升Unified WAL的Gate或旧binary兼容结论。

## CLIENT-2 迟到响应回收

`PendingAddOp.writeComplete()`的旧地址响应分支在减少pending计数后补调用已有`maybeRecycle()`并立即返回，不在回收后访问字段。同步边界和回收条件沿用原实现，正常Add和当前地址响应路径未改变。

`PendingAddOpTest`新增四种确定性顺序：最后响应来自旧/当前Bookie，callback在这些响应之前/之后。测试运行真实`PendingAddOp`生命周期和Netty引用计数，用受控BookieClient及计数Recycler handle精确观察：callback一次、对象回收一次、每个transport引用终结前不释放其buffer、callback未运行时不归还对象。

基线`d105ec7a11`加测试时，9项中`testLastOldResponseRecyclesAfterCallback`失败：最后旧响应后`lh`仍非null；其他8项通过。仓库自己的`testRetryCount=2`导致该失败重复验证三次，均失败。测试脚本随后补上正确的`-DtestRetryCount=0`，不依赖重跑掩盖问题。修复后9项全通过。此反例证明对象池归还缺口，不声称已证明payload永久泄漏。

复现：

```bash
scripts/unified-wal/run-local-storage-tests.sh PendingAddOpTest
mvn -o -pl bookkeeper-server -DskipTests spotless:check checkstyle:check
```

source/run与输出校验见[CLIENT-2回执](client2-receipt.json)。测试不是实际网络或生产负载验证；此一行补丁不增加正常Add路径的分配和计算，性能数字尚未测量。

## CLIENT-1

`LedgerHandle`保持原`changingEnsemble` gate覆盖远端phase结束及整个本地切换：先对全部pending完成映射、ACK撤销和资格重算，再重发受影响的write-set副本，最后按连续前缀恢复callback。切换中出现新失败继续换组，不提前开gate。未替换ACK保留，write-set外slot不重发；完全未受影响的已完成entry也会显式恢复callback。同ensemble重试同样受本地gate保护。

`PendingAddOp`同步撤销ACK位与旧Bookie成功集合，即使数量仍达A也重新检查故障域；关闭策略时不新建/维护该集合。timeout计数仍只在启用故障域检查、当前有效数量已达A而未完成时增加。callback后的回收同步保护保持。对创建早于metadata切换、入队晚于遍历的尚未发送Add，`initiate()`重新绑定当前已发布ensemble。

先在`30215501bb`加测试得到5个确定性失败（14项中的5项）；核心修复后26项通过。补充入队交错又得到1个失败反例，修复后最终19项`PendingAddOpTest`、12项`HandleFailuresTest`、54项`TestRackawareEnsemblePlacementPolicy`共85项通过，无重跑、失败或跳过。Spotless/Checkstyle通过。每个运行的源码hash和失败信息见[CLIENT-1回执](client1-receipt.json)。受控响应替代网络，真实的LedgerHandle、PendingAddOp、摘要封装、ACK调度和ByteBuf生命周期参与测试；不能将这些测试当作Profile分布式联调。

```bash
scripts/unified-wal/run-local-storage-tests.sh PendingAddOpTest,HandleFailuresTest,TestRackawareEnsemblePlacementPolicy
mvn -o -P '!cargo-zigbuild' -pl microbenchmarks -am -DskipTests package
scripts/unified-wal/compare-client-bookkeeping.py
```

JMH沿用现有microbenchmarks模块，对比`30215501bb`的两份客户端类与当前实现；旧源码独立编译到临时目录，通过classpath覆盖，不改变checkout。测量3/3/2下创建、三个当前地址ACK、callback和recycle，1KiB可复用payload；不计摘要、发送、入队调度及真实I/O。每模式2fork、3×1秒warmup、5×1秒measurement；同时记录thread CPU和GC分配。CPU ns/op由同批`cpuNanos/operations`计算。该检查不替代正常全Add/TLS/quorum或可持续性能Gate。

本机短测量结果（macOS arm64，非独占性能环境；不作为总体性能Gate）：

| 故障域检查 | 版本 | ns/op | thread CPU ns/op | B/op |
| --- | --- | ---: | ---: | ---: |
| false | baseline | 163.18 | 162.53 | 504.00 |
| true | baseline | 172.51 | 171.35 | 504.00 |
| false | current | 142.17 | 141.70 | 264.00 |
| true | current | 165.09 | 164.49 | 504.00 |

固定配置下，默认关闭策略的记账路径减少240 B/op；启用策略时分配不变。这次局部测量未观察到CPU增长，但不据此推断完整Add或集群提升。新增initiate映射刷新由正确性测试覆盖，不在此记账微基准计时段内。[输入锁](benchmark/inputs.json)、[原始baseline](benchmark/baseline.json)、[原始current](benchmark/current.json)及[结果/限制](benchmark/summary.json)分别保留。
