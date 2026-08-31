# Counterexample CE-20260831T105059Z

> **FROZEN / REAL RELEASED DECODER / G1 FAIL FOR CANDIDATE `0x0FFE4250`**

The exact Apache BookKeeper `4.14.8` Maven release artifact (`org.apache.bookkeeper:bookkeeper-server:4.14.8`, release commit `965c3627328787e2750f41c2a4eda6c1d709d7c6`) decoded corpus vector `invalid/magic-flip-1` as `org.apache.bookkeeper.proto.BookieProtocol$ParsedAddRequest` after its real hybrid decoder permanently changed from v3 to pre-v3.

Frozen input SHA-256 is `0bfe0f405e6aded84d58d736cea01875500af39b2fb14f595c454d7a7d0236a5`:

```text
00 00 00 2c 0f 01 42 50 00 01 00 00 00 20 00 01
00 00 00 00 00 00 00 00 00 00 00 01 00 00 00 0c
00 00 00 00 00 01 00 01 00 00 00 00 00 00 00 00
```

The original candidate magic is `0f fe 42 50`. The authoritative byte-flip corpus XORs each magic byte with `ff`; flipping the second byte produces `0f 01 42 50`. In pre-v3 framing, byte two is the opcode and `0x01` is `ADDENTRY`, so this mutation is a genuine Classic route claim rather than a mock or harness proxy.

Artifact digests:

- JAR SHA-256: `e88024bf7d2ae37e91c846e02a4954b2a256e4a8300f03a71f271d9823259567`
- JAR SHA-512: `433614ca2cdc1026f2c56cdb558f6401f90cab0795aaff126904c621cc06443463026df4dab3b3e1b07a17e58eefea60bda5109ecfdef3c101f7d5ba43ecd060`
- `decoder-result.jsonl` SHA-256: `83faea23117195b4751305aa3c7b567a5a928ce127166ca4fd39e3ce4600d3e2`
- `decoder-summary.json` SHA-256: `000b5aad9ab5e658dfee61777153ad0dd7c859dcf036ee57b3fe8c9dbd7ec11b`

Exact reproduction from repository root after resolving the Maven artifact:

```bash
mkdir -p target/profile-wire-repro/classes
mvn -q -f tests/profile-wire-compatibility/pom.xml \
  -Dbookkeeper.version=4.14.8 dependency:build-classpath \
  -Dmdep.outputFile="$PWD/target/profile-wire-repro/classpath.txt" -DincludeScope=runtime
/Library/Java/JavaVirtualMachines/zulu-17.jdk/Contents/Home/bin/javac \
  -cp "$(cat target/profile-wire-repro/classpath.txt)" \
  -d target/profile-wire-repro/classes \
  tests/profile-wire-compatibility/src/StockDecoderProbe.java
/Library/Java/JavaVirtualMachines/zulu-17.jdk/Contents/Home/bin/java \
  -cp "target/profile-wire-repro/classes:$(cat target/profile-wire-repro/classpath.txt)" \
  StockDecoderProbe 4.14.8 \
  bookkeeper-common/src/test/resources/profile-wire \
  bookkeeper-common/src/test/resources/profile-wire/fixtures.tsv \
  target/profile-wire-repro/decoder.jsonl \
  target/profile-wire-repro/decoder-summary.json
```

This counterexample forbids G1 PASS and forbids keeping `0x0FFE4250`. It does not authorize weakening Classic, removing the vector, changing the zero-effect Oracle, adding fallback, or dual-writing. The next candidate must update the owner RFC and corpus, retain this frozen artifact, add deterministic regression coverage, and rerun the entire matrix under a new immutable run identity.
