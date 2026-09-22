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

ACK/故障域集合与多slot整体completion gate尚未修复，继续按RFC-0001 §9.5先构造反例；不能以CLIENT-2通过宣布步骤2完成。
