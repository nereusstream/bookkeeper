#!/usr/bin/env bash
#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements. See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to you under the Apache License, Version 2.0.
#

set -euo pipefail

readonly STARTUP_READINESS_BASE="${STARTUP_READINESS_BASE:-df122c524ddc1739ef58e1f1ede3d8140ee0de86}"
readonly REPOSITORY_ROOT="$(git rev-parse --show-toplevel)"

cd "$REPOSITORY_ROOT"

if ! git cat-file -e "${STARTUP_READINESS_BASE}^{commit}" 2>/dev/null; then
    echo "BLOCK: missing startup/readiness base commit ${STARTUP_READINESS_BASE}" >&2
    exit 2
fi

changed_paths="$({
    git diff --name-only --diff-filter=ACMR "${STARTUP_READINESS_BASE}...HEAD"
    git diff --name-only --diff-filter=ACMR
    git ls-files --others --exclude-standard
} | sort -u)"

blocked=0
while IFS= read -r changed_path; do
    [[ -z "$changed_path" ]] && continue
    [[ "$changed_path" == "BtrLog Low-Latency Logging.pdf" ]] && continue
    case "$changed_path" in
        bookkeeper-common/src/main/java/org/apache/bookkeeper/common/profile/startup/* \
        |bookkeeper-common/src/test/java/org/apache/bookkeeper/common/profile/startup/* \
        |docs/turbo-bk.md \
        |docs/rfcs/unified-wal/RFC-0005-segment-bookie-state.md \
        |docs/rfcs/unified-wal/spikes/SPIKE-B-allocator-block.md \
        |docs/rfcs/unified-wal/implementation/README.md \
        |docs/rfcs/unified-wal/implementation/manifest.json \
        |docs/rfcs/unified-wal/implementation/startup-readiness/* \
        |scripts/unified-wal/check-wave0-startup-readiness-scope.sh)
            ;;
        *)
            echo "BLOCK: startup/readiness path is outside the experimental allowlist: $changed_path" >&2
            blocked=1
            ;;
    esac
done <<< "$changed_paths"

if [[ "$blocked" -ne 0 ]]; then
    exit 1
fi

if git grep -n -E \
    'org\.apache\.bookkeeper\.common\.profile\.startup|StartupReadinessHarness|ProfileRegistrationStore' \
    HEAD -- \
    '*.java' \
    ':(exclude)bookkeeper-common/src/main/java/org/apache/bookkeeper/common/profile/startup/**' \
    ':(exclude)bookkeeper-common/src/test/java/org/apache/bookkeeper/common/profile/startup/**'; then
    echo "BLOCK: startup/readiness reference types are integrated into a production Java path" >&2
    exit 1
fi

if git grep -n -E \
    '^import org\.apache\.bookkeeper\.(bookie|client|meta|proto|replication|server)\.' \
    HEAD -- \
    'bookkeeper-common/src/main/java/org/apache/bookkeeper/common/profile/startup/*.java'; then
    echo "BLOCK: startup/readiness package imports a production authority package" >&2
    exit 1
fi

echo "PASS: startup/readiness changes stay isolated from production Add, storage, registration, delete, and deferred compatibility paths"
