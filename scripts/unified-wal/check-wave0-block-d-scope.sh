#!/usr/bin/env bash
#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements. See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to you under the Apache License, Version 2.0.
#

set -euo pipefail

readonly BLOCK_D_BASE="${BLOCK_D_BASE:-be3d3d55b6787b84698fe428705b398bda57a1de}"
readonly REPOSITORY_ROOT="$(git rev-parse --show-toplevel)"

cd "$REPOSITORY_ROOT"

if ! git cat-file -e "${BLOCK_D_BASE}^{commit}" 2>/dev/null; then
    echo "BLOCK: missing Block D base commit ${BLOCK_D_BASE}" >&2
    exit 2
fi

changed_paths="$({
    git diff --name-only --diff-filter=ACMR "${BLOCK_D_BASE}...HEAD"
    git diff --name-only --diff-filter=ACMR
    git ls-files --others --exclude-standard
} | sort -u)"

blocked=0
while IFS= read -r path; do
    [[ -z "$path" ]] && continue
    [[ "$path" == "BtrLog Low-Latency Logging.pdf" ]] && continue
    case "$path" in
        bookkeeper-common/src/main/java/org/apache/bookkeeper/common/profile/wire/* \
        |bookkeeper-common/src/test/java/org/apache/bookkeeper/common/profile/wire/* \
        |bookkeeper-common/src/test/resources/profile-wire/* \
        |bookkeeper-common/pom.xml \
        |docs/turbo-bk.md \
        |docs/rfcs/unified-wal/RFC-0001-profile-capability-install.md \
        |docs/rfcs/unified-wal/RFC-0004-range-recovery-delete.md \
        |docs/rfcs/unified-wal/grill/ROUND-07-exact-implementation-manifest.md \
        |docs/rfcs/unified-wal/spikes/SPIKE-A-profile-install.md \
        |docs/rfcs/unified-wal/implementation/README.md \
        |docs/rfcs/unified-wal/implementation/manifest.json \
        |docs/rfcs/unified-wal/implementation/profile-wire/* \
        |scripts/unified-wal/check-wave0-block-d-scope.sh \
        |tests/profile-wire-compatibility/*)
            ;;
        *)
            echo "BLOCK: Wave 0 Block D path is outside the experimental allowlist: $path" >&2
            blocked=1
            ;;
    esac
done <<< "$changed_paths"

if [[ "$blocked" -ne 0 ]]; then
    exit 1
fi

echo "PASS: Wave 0 Block D changed paths stay outside production authority and compatibility surfaces"
