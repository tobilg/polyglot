#!/usr/bin/env bash
# Usage: check-crate-version.sh <crate-name> <version>
# Exit 0 if version exists on crates.io, 1 if not
set -euo pipefail

crate="$1"
version="$2"

url="https://crates.io/api/v1/crates/${crate}"
response=$(curl -sf --max-time 20 "$url" 2>/dev/null) || exit 1

echo "$response" | python3 -c "
import json, sys
data = json.load(sys.stdin)
versions = [v['num'] for v in data.get('versions', [])]
sys.exit(0 if '${version}' in versions else 1)
"
