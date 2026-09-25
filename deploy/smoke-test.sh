#!/usr/bin/env bash
# End-to-end smoke test against a running StorageMesh (docker compose up).
# Exercises: create bucket, upload, download+verify, dedup, pre-signed URL, delete.
#
# Usage: ./deploy/smoke-test.sh [GATEWAY_URL]
set -euo pipefail

GW="${1:-http://localhost:8080}"
KEY="dev-access-key"
H() { printf '\n=== %s ===\n' "$1"; }
auth=(-H "X-Access-Key: ${KEY}")

H "health"
curl -fsS "${GW}/healthz"; echo

H "create bucket 'demo' (409 if it already exists is fine)"
code=$(curl -sS "${auth[@]}" -X PUT "${GW}/demo" -o /dev/null -w '%{http_code}')
echo "status=${code}"
[[ "${code}" == "200" || "${code}" == "409" ]] || { echo "unexpected status"; exit 1; }

H "upload object"
printf 'hello storagemesh %s\n' "$(date)" > /tmp/sm_obj.txt
# Make it big enough to span multiple chunks would need >8MiB; small is fine for smoke.
curl -fsS "${auth[@]}" -X PUT --data-binary @/tmp/sm_obj.txt "${GW}/demo/notes/hello.txt"; echo

H "download + verify"
curl -fsS "${auth[@]}" "${GW}/demo/notes/hello.txt" -o /tmp/sm_got.txt
diff /tmp/sm_obj.txt /tmp/sm_got.txt && echo "bytes match ✓"

H "dedup: upload identical content under a second key"
curl -fsS "${auth[@]}" -X PUT --data-binary @/tmp/sm_obj.txt "${GW}/demo/notes/copy.txt"; echo
echo "(check gateway logs / chunks table: ref_count should be 2 for shared chunks)"

H "pre-signed GET url (no credentials needed to use it)"
PRES=$(curl -fsS "${auth[@]}" "${GW}/demo/notes/hello.txt?presign=GET&expiry=300")
echo "${PRES}"
# JSON encodes '&' as &; decode it so the query string parses.
URL_PATH=$(printf '%s' "${PRES}" | sed -n 's/.*"url":"\([^"]*\)".*/\1/p' | sed 's/\\u0026/\&/g')
curl -fsS "${GW}${URL_PATH}" && echo "  <- fetched via presigned URL ✓"

H "list objects"
curl -fsS "${auth[@]}" "${GW}/demo?prefix=notes/"; echo

H "delete object"
curl -fsS "${auth[@]}" -X DELETE "${GW}/demo/notes/hello.txt" -o /dev/null -w "status=%{http_code}\n"

echo -e "\nSmoke test passed ✓"
