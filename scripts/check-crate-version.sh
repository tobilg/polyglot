#!/usr/bin/env bash
# Usage: check-crate-version.sh <crate-name> <version>
# Exit 0 if version exists on crates.io, 1 if not
set -euo pipefail

if [[ $# -ne 2 ]]; then
  echo "Usage: $0 <crate-name> <version>" >&2
  exit 2
fi

crate="$1"
version="$2"
user_agent="${CHECK_CRATE_USER_AGENT:-polyglot-ci/check-crate-version}"
connect_timeout="${CHECK_CRATE_CONNECT_TIMEOUT:-10}"
max_time="${CHECK_CRATE_MAX_TIME:-30}"
curl_retry="${CHECK_CRATE_CURL_RETRY:-2}"
max_attempts="${CHECK_CRATE_MAX_ATTEMPTS:-6}"
base_delay="${CHECK_CRATE_BASE_DELAY_SECONDS:-2}"

if [[ -z "$crate" || -z "$version" ]]; then
  echo "crate and version must be non-empty" >&2
  exit 2
fi

curl_json_status() {
  local url="$1"
  local body_file="$2"
  local status

  status="$(curl \
    --silent \
    --show-error \
    --location \
    --connect-timeout "$connect_timeout" \
    --max-time "$max_time" \
    --retry "$curl_retry" \
    --retry-delay 1 \
    --retry-all-errors \
    --header "Accept: application/json" \
    --header "User-Agent: ${user_agent}" \
    --output "$body_file" \
    --write-out "%{http_code}" \
    "$url" || true)"

  if [[ -z "$status" ]]; then
    status="000"
  fi

  printf '%s' "$status"
}

version_url="https://crates.io/api/v1/crates/${crate}/${version}"
delay="$base_delay"

# Fast path: exact version endpoint.
for attempt in $(seq 1 "$max_attempts"); do
  body_file="$(mktemp)"
  status="$(curl_json_status "$version_url" "$body_file")"

  case "$status" in
    200)
      rm -f "$body_file"
      exit 0
      ;;
    404)
      rm -f "$body_file"
      exit 1
      ;;
    429|5??|000)
      echo "Transient crates.io response for ${crate} ${version}: HTTP ${status} (attempt ${attempt}/${max_attempts})" >&2
      rm -f "$body_file"
      if [[ "$attempt" -lt "$max_attempts" ]]; then
        sleep "$delay"
        delay=$((delay * 2))
      fi
      ;;
    *)
      echo "Unexpected crates.io response for ${crate} ${version}: HTTP ${status}" >&2
      head -c 300 "$body_file" >&2 || true
      echo >&2
      rm -f "$body_file"
      if [[ "$attempt" -lt "$max_attempts" ]]; then
        sleep "$delay"
        delay=$((delay * 2))
      fi
      ;;
  esac
done

# Fallback: versions list endpoint, in case exact endpoint behavior changes.
list_body_file="$(mktemp)"
list_status="$(curl_json_status "https://crates.io/api/v1/crates/${crate}" "$list_body_file")"

if [[ "$list_status" == "200" ]]; then
  python3 -c "
import json, sys
data = json.load(sys.stdin)
versions = [v['num'] for v in data.get('versions', [])]
sys.exit(0 if '${version}' in versions else 1)
" < "$list_body_file"
fi

echo "Failed to determine crates.io version availability for ${crate} ${version} (HTTP ${list_status} on fallback endpoint)" >&2
rm -f "$list_body_file"
exit 1
