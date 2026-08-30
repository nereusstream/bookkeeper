# ProfileDescriptor reference implementation

> 状态：**Wave 0 reference evidence complete / NON-PROMOTABLE / NO AUTHORITY**

该模块在 `bookkeeper-common` 中提供 JDK-only、内部、unstable 的冷路径 reference implementation。它不依赖网络、磁盘或 MetadataStore，也没有 endpoint、registration、payload write、ACK、recovery 或 delete authority。

## 实现范围

Production source 位于：

```text
bookkeeper-common/src/main/java/org/apache/bookkeeper/common/profile/
```

包含：

- immutable `ProfileDescriptor`、`ProfileCapability` 与冻结枚举；
- `ProfileDescriptorCodec` 唯一 canonical writer 与 strict decoder；
- `ProfileDescriptorValidator` fail-closed 语义/registry 校验；
- immutable `ProfileDescriptorIdentity`，SHA-256 suite 1、36-byte identity；
- capability/failure-domain registry contracts，但没有 production implementation 或 production ID。

测试路径中的 `TestProfileRegistries` 是唯一 registry 实现，其 ID 明确属于 fixture。`IndependentProfileDescriptorVerifier` 位于 test source，独立迭代 raw fields、独立构造 hash preimage、独立计算 SHA-256 和 field dump，不复用 production decoder/hash helper。

## 冻结 byte contract

实现精确执行：

```text
magic                         BKPD
header                        16 bytes
field header                  8 bytes
mandatory fields              10
capability order              strict unsigned ascending
legal length                  124 + 6*N, N=0..64, 124..508
absolute input cap            1024 before capability-body allocation
identity                      suite:u16 + schema:u16 + SHA-256[32]
```

输入原字节必须 canonical。duplicate、out-of-order、missing、unknown/default alias、wrong length、flags、padding、trailing、truncation、509/1024 noncanonical、1025 oversize 和 declared identity mismatch 均 fail closed；不存在 parse-normalize-reencode 接受路径。

## 可复核 artifacts

Authoritative corpus 位于：

```text
bookkeeper-common/src/test/resources/profile-descriptor/
├── valid/                         # 6 byte-exact .bin vectors
├── invalid/                       # 40 fail-closed .bin vectors + manifest
├── expected/identities.json       # independent digest + 36-byte identity
├── expected/field-dumps.json      # independent field dumps
├── expected/fuzz-results.json     # fixed-seed result receipt
├── fixtures.json                  # typed fixture semantics
└── checksums.txt                  # SHA-256 for every artifact above
```

Corpus 默认只读核对。只有显式指定 `-DprofileDescriptor.updateCorpus=true` 才会重建 source resources。

## 复现命令

```bash
mvn -pl bookkeeper-common \
  -Dtest='org.apache.bookkeeper.common.profile.*Test' \
  -Dsurefire.rerunFailingTestsCount=0 test

mvn -pl bookkeeper-common -DskipTests spotless:check checkstyle:check
mvn -pl bookkeeper-common -DskipTests apache-rat:check
```

## 未解除的 BLOCK

- failure-domain policy production registry；
- capability production registry 和 legal-combination table；
- owner RFC acceptance 与 Spike A/B/C；
- stable Profile wire、独立 endpoint、live shadow 和任何 Segment ACK authority；
- production Cookie/registration、AutoRecovery/delete；
- same BookieId/same storage scope promotion。

因此本模块只能证明冻结 descriptor reference contract 的本地可执行一致性，不能 mint production descriptor，也不能宣称 Gate G1/G2/G3、Segment authority 或 production readiness。
