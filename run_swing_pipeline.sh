#!/usr/bin/env bash
set -euo pipefail

# -----------------------------
# Usage check
# -----------------------------
if [[ $# -lt 2 ]]; then
  echo "Usage: $0 START_DATE END_DATE [ADR_CUTOFF]"
  echo "Example: $0 2025-10-01 2025-12-12 3.5"
  exit 1
fi

START_DATE="$1"
END_DATE="$2"
ADR_CUTOFF="${3:-3.5}"   # default = 3.5

echo "========================================"
echo "Running Swing Trading Pipeline"
echo "START_DATE=${START_DATE}"
echo "END_DATE=${END_DATE}"
echo "ADR_CUTOFF=${ADR_CUTOFF}"
echo "========================================"

# -----------------------------
# Ensure we are at project root
# -----------------------------
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "${SCRIPT_DIR}"

# -----------------------------
# Step 1: Fetch NSE historical data
# -----------------------------
echo "[1/2] Fetching NSE historical data..."

python3 -m src.jobs.fetch_nse_hist \
  --start_date "${START_DATE}" \
  --end_date "${END_DATE}"

echo "[1/2] NSE data fetch completed"

# -----------------------------
# Step 2: Run swing scan
# -----------------------------
echo "[2/2] Running swing scan..."

python3 -m src.jobs.run_swing_scan \
  --start_date "${START_DATE}" \
  --end_date "${END_DATE}" \
  --adr_cutoff "${ADR_CUTOFF}"

echo "[2/2] Swing scan completed"

echo "========================================"
echo "Pipeline finished successfully"
echo "========================================"
