#!/usr/bin/env bash
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements. See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to you under the Apache License, Version 2.0.
set -euo pipefail
cd "$(git rev-parse --show-toplevel)"
run_dir="$(mktemp -d "${TMPDIR:-/tmp}/unified-wal-local-XXXXXXXX")"
tests="${1:-IsolatedSegmentConfigTest}"
network_flags=(-o)
if [[ "${UW_FETCH_DEPENDENCIES:-false}" == "true" ]]; then network_flags=(); fi
command=(mvn "${network_flags[@]}" -P '!cargo-zigbuild' -pl bookkeeper-server -am
  "-Dtest=$tests" -Dsurefire.failIfNoSpecifiedTests=false
  -DtestRetryCount=0 -Dsurefire.rerunFailingTestsCount=0 test)
python3 - "$run_dir" "${command[@]}" <<'PY'
import hashlib, json, platform, subprocess, sys
from pathlib import Path
paths = set(Path('bookkeeper-server/src/main/java/org/apache/bookkeeper/bookie/storage/segment').glob('*.java'))
paths.update(Path('bookkeeper-server/src/test/java/org/apache/bookkeeper/bookie/storage/segment').glob('*.java'))
paths.update(Path('bookkeeper-server/src/test/resources/unified-wal-segment').glob('*'))
paths.update(Path('bookkeeper-server/src/main/java/org/apache/bookkeeper/client').glob('*.java'))
paths.update(Path('bookkeeper-server/src/test/java/org/apache/bookkeeper/client').glob('*.java'))
paths.update(Path('bookkeeper-server/src/main/java/org/apache/bookkeeper/proto/profile').glob('*.java'))
paths.update(Path('bookkeeper-server/src/test/java/org/apache/bookkeeper/proto/profile').glob('*.java'))
paths.update(Path('bookkeeper-server/src/main/java/org/apache/bookkeeper/discover/profile').glob('*.java'))
paths.update(Path('bookkeeper-server/src/test/java/org/apache/bookkeeper/discover/profile').glob('*.java'))
paths.update(Path('bookkeeper-server/src/main/java/org/apache/bookkeeper/meta').glob('*.java'))
paths.update(Path('bookkeeper-server/src/main/java/org/apache/bookkeeper/versioning').glob('*.java'))
paths.add(Path('bookkeeper-server/src/test/java/org/apache/bookkeeper/meta/TestLedgerMetadataSerDe.java'))
paths.update(Path('bookkeeper-server/src/main/java/org/apache/bookkeeper') / name for name in
             ('client/api/LedgerMetadata.java', 'client/api/DigestType.java',
              'conf/AbstractConfiguration.java', 'conf/ClientConfiguration.java', 'meta/zk/ZKMetadataDriverBase.java',
              'proto/checksum/DigestManager.java', 'proto/checksum/MacDigestManager.java'))
paths.update(Path('bookkeeper-common/src/main/java/org/apache/bookkeeper/common/profile').rglob('*.java'))
paths.update(Path('bookkeeper-common/src/test/java/org/apache/bookkeeper/common/profile').rglob('*.java'))
paths.update(Path('bookkeeper-common/src/test/resources/profile-wire').rglob('*.bin'))
paths.update([Path('bookkeeper-server/src/main/java/org/apache/bookkeeper/tls/TLSContextFactory.java'),
              Path('bookkeeper-server/src/test/java/org/apache/bookkeeper/tls/ProfileTlsContextFactoryTest.java')])
paths.update(Path('bookkeeper-server/src/test/resources') / name for name in
             ('server-key.pem', 'server-san-cert.pem', 'client-cert.pem', 'client-key.pem'))
paths.update([Path('pom.xml'), Path('bookkeeper-server/pom.xml'), Path('scripts/unified-wal/run-local-storage-tests.sh')])
manifest = {'head': subprocess.check_output(['git', 'rev-parse', 'HEAD'], text=True).strip(),
            'branch': subprocess.check_output(['git', 'branch', '--show-current'], text=True).strip(),
            'platform': platform.platform(), 'command': sys.argv[2:],
            'sourceSha256': {str(p): hashlib.sha256(p.read_bytes()).hexdigest() for p in sorted(paths)},
            'scope': 'Isolated Java tests; native Rust cross-build disabled; no production authority'}
Path(sys.argv[1], 'inputs.json').write_text(json.dumps(manifest, indent=2)+'\n')
PY
printf 'Run directory: %s\n' "$run_dir"
set +e
"${command[@]}" > "$run_dir/maven.log" 2>&1
result=$?
set -e
printf '%s\n' "$result" > "$run_dir/exit-code"
# Maven/test failure is preserved, never converted to PASS by the receipt collection.
mkdir "$run_dir/reports"
find bookkeeper-common/target/surefire-reports bookkeeper-server/target/surefire-reports \
  -name 'TEST-*.xml' -newer "$run_dir/inputs.json" \
  -exec cp {} "$run_dir/reports/" \; 2>/dev/null || true
tail -35 "$run_dir/maven.log"
exit "$result"
