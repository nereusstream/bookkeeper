#!/usr/bin/env bash
#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements. See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to you under the Apache License, Version 2.0.
#

set -euo pipefail

readonly PROFILE_CONTROL_BASE="${PROFILE_CONTROL_BASE:-761f65a9b93626afaf16484a8d270df06989f5b3}"
readonly REPOSITORY_ROOT="$(git rev-parse --show-toplevel)"

cd "$REPOSITORY_ROOT"

if ! git cat-file -e "${PROFILE_CONTROL_BASE}^{commit}" 2>/dev/null; then
    echo "BLOCK: missing Profile control base commit ${PROFILE_CONTROL_BASE}" >&2
    exit 2
fi

changed_paths="$({
    git diff --name-only --diff-filter=ACMR "${PROFILE_CONTROL_BASE}...HEAD"
    git diff --name-only --diff-filter=ACMR
    git ls-files --others --exclude-standard
} | sort -u)"

blocked=0
while IFS= read -r changed_path; do
    [[ -z "$changed_path" ]] && continue
    [[ "$changed_path" == "BtrLog Low-Latency Logging.pdf" ]] && continue
    case "$changed_path" in
        bookkeeper-common/src/main/java/org/apache/bookkeeper/common/profile/control/* \
        |bookkeeper-common/src/test/java/org/apache/bookkeeper/common/profile/control/* \
        |docs/turbo-bk.md \
        |docs/rfcs/unified-wal/RFC-0001-profile-capability-install.md \
        |docs/rfcs/unified-wal/spikes/SPIKE-A-profile-install.md \
        |docs/rfcs/unified-wal/implementation/README.md \
        |docs/rfcs/unified-wal/implementation/manifest.json \
        |docs/rfcs/unified-wal/implementation/profile-control/* \
        |scripts/unified-wal/check-wave0-profile-control-scope.sh)
            ;;
        *)
            echo "BLOCK: Profile control path is outside the experimental allowlist: $changed_path" >&2
            blocked=1
            ;;
    esac
done <<< "$changed_paths"

if [[ "$blocked" -ne 0 ]]; then
    exit 1
fi

if git grep -n -E \
    'org\.apache\.bookkeeper\.common\.profile\.control|IsolatedProfileControlEndpoint|ProfileControlAuthorizer' \
    HEAD -- \
    '*.java' \
    ':(exclude)bookkeeper-common/src/main/java/org/apache/bookkeeper/common/profile/control/**' \
    ':(exclude)bookkeeper-common/src/test/java/org/apache/bookkeeper/common/profile/control/**'; then
    echo "BLOCK: Profile control reference types are integrated into a production Java path" >&2
    exit 1
fi

if git grep -n -E \
    '^import org\.apache\.bookkeeper\.(bookie|meta|proto|replication|server)\.' \
    HEAD -- \
    'bookkeeper-common/src/main/java/org/apache/bookkeeper/common/profile/control/*.java'; then
    echo "BLOCK: Profile control reference package imports a production authority package" >&2
    exit 1
fi

echo "PASS: Profile control changes stay outside production authority and deferred compatibility paths; production Java integration is zero"
