#!/usr/bin/env bash
# Joint multi-station upload demo (happy path + validation + merge interaction).
#
# Runs entirely over HTTP against a locally started server (docker compose or
# uvicorn). Override the base URL with RAIN_BASE_URL.
#
# It shows:
#   1. validation rejects a CSV that does not cover 2-10 stations;
#   2. one joint upload stores one Parquet batch per station and returns all
#      batch ids in a single response;
#   3. before/after publication visibility through the normal query API;
#   4. a merge started independently never swallows the joint batches;
#   5. the legacy single-station endpoint keeps working.
set -euo pipefail

BASE_URL="${RAIN_BASE_URL:-http://127.0.0.1:8000}"
DAY="2026-11-12"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SAMPLES="${SCRIPT_DIR}/samples"
WORK="$(mktemp -d)"
trap 'rm -rf "${WORK}"' EXIT

pp() { python3 -c "
import sys, json
raw = sys.stdin.read()
try:
    print(json.dumps(json.loads(raw), ensure_ascii=False, indent=2))
except json.JSONDecodeError:
    print(raw)
"; }

echo "### 0. health"
curl -fsS "${BASE_URL}/health" | pp

echo; echo "### 1. validation: a one-station CSV is rejected for the joint endpoint"
curl -s -o "${WORK}/err.json" -w "http=%{http_code}\n" -X POST \
    -F "file=@${SAMPLES}/st002_2026-11-12_batch1.csv;filename=single.csv" \
    "${BASE_URL}/days/${DAY}/joint-batches"
cat "${WORK}/err.json" | pp

echo; echo "### 2. joint upload of one CSV covering ST001/ST002/ST003"
JOINT="$(curl -fsS -X POST \
    -F "file=@${SAMPLES}/joint_st001_st002_st003_2026-11-12.csv;filename=joint.csv" \
    "${BASE_URL}/days/${DAY}/joint-batches")"
echo "${JOINT}" | pp
GROUP_ID="$(echo "${JOINT}" | python3 -c 'import sys,json; print(json.load(sys.stdin)["group_id"])')"
echo "group_id=${GROUP_ID}"

echo; echo "### 3. after publication every station is queryable via the normal API"
for st in ST001 ST002 ST003; do
  curl -fsS "${BASE_URL}/stations/${st}/days/${DAY}/records" \
    | python3 -c "
import sys, json
d = json.load(sys.stdin)
print(f\"  {d['station_id']}: rows={d['row_count']} files={d['file_count']} batches={d['batch_ids']}\")"
done

echo; echo "### 4. merge on ST001 compacts only that station; joint batches survive"
curl -fsS -X POST "${BASE_URL}/stations/ST001/days/${DAY}/merge" \
  | python3 -c "
import sys, json
d = json.load(sys.stdin)
print(f\"  merged_batch={d['merged_batch_id']} sources={d['source_batch_ids']}\")"
for st in ST001 ST002 ST003; do
  curl -fsS "${BASE_URL}/stations/${st}/days/${DAY}/records" \
    | python3 -c "
import sys, json
d = json.load(sys.stdin)
print(f\"  {d['station_id']}: rows={d['row_count']} files={d['file_count']}\")"
done

echo; echo "### 5. legacy single-station endpoint still works"
curl -fsS -X POST \
    -F "file=@${SAMPLES}/st002_2026-11-12_batch1.csv;filename=legacy.csv" \
    "${BASE_URL}/stations/ST002/days/${DAY}/batches" \
  | python3 -c "import sys,json; d=json.load(sys.stdin); print(f\"  legacy batch={d['batch_id']} rows={d['row_count']}\")"
curl -fsS "${BASE_URL}/stations/ST002/days/${DAY}/records" \
  | python3 -c "import sys,json; d=json.load(sys.stdin); print(f\"  ST002 now rows={d['row_count']} files={d['file_count']}\")"

echo; echo "joint upload demo finished OK"
