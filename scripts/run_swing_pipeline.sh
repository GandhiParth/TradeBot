#!/usr/bin/env bash
set -euo pipefail

# -----------------------------
# Defaults
# -----------------------------
RUN_FETCH=false
ADR_CUTOFF_DEFAULT=3.5

# -----------------------------
# Parse flags
# -----------------------------
while [[ $# -gt 0 ]]; do
  case "$1" in
    --fetch)
      RUN_FETCH=true
      shift
      ;;
    -*)
      echo "Unknown option: $1"
      exit 1
      ;;
    *)
      break
      ;;
  esac
done

# -----------------------------
# Usage check
# -----------------------------
if [[ $# -lt 1 ]]; then
  echo "Usage: $0 [--fetch] END_DATE [ADR_CUTOFF]"
  echo "Example: $0 --fetch 2025-12-12 3.5"
  exit 1
fi

END_DATE="$1"
ADR_CUTOFF="${2:-$ADR_CUTOFF_DEFAULT}"

START_DATE="$(date -d "${END_DATE} -3 months" +%Y-%m-%d)"

# -----------------------------
# Ensure we are at project root
# -----------------------------
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "${SCRIPT_DIR}")"
cd "${PROJECT_ROOT}"

#----------------------------
# LOGS
#----------------------------
LOG_DIR="${PROJECT_ROOT}/logs"
mkdir -p "${LOG_DIR}"
LOG_FILE="${LOG_DIR}/run_swing_pipeline_$(date +%Y-%m-%d).log"

## ENV
source .venv/bin/activate

# Overwrite log file on each run, still print to console
exec > >(tee "${LOG_FILE}") 2>&1


echo "========================================"
echo "Running Swing Trading Pipeline"
echo "START_DATE=${START_DATE}"
echo "END_DATE=${END_DATE}"
echo "ADR_CUTOFF=${ADR_CUTOFF}"
echo "RUN_FETCH=${RUN_FETCH}"
echo "========================================"



# -----------------------------
# Step 1: Fetch NSE historical data (optional)
# -----------------------------
if [[ "${RUN_FETCH}" == true ]]; then
  echo "[1/3] Fetching NSE historical data..."

  python3 -m src.jobs.make_dir --fetch

  python3 -m src.jobs.fetch_nse_hist \
    --start_date "${START_DATE}" \
    --end_date "${END_DATE}"

  echo "[1/3] NSE data fetch completed"
else
  echo "[1/3] Skipping data fetch (use --fetch to enable)"
  python3 -m src.jobs.make_dir
fi

# -----------------------------
# Step 2: Run swing scan
# -----------------------------
echo "[2/3] Running swing scan..."

python3 -m src.jobs.run_swing_scan \
  --start_date "${START_DATE}" \
  --end_date "${END_DATE}" \
  --adr_cutoff "${ADR_CUTOFF}"

echo "[2/3] Swing scan completed"

# -----------------------------
# Step 3: Run filter scan
# -----------------------------
echo "[3/3] Running filter scan..."

python3 -m src.jobs.run_filter_scan \
  --end_date "${END_DATE}" \
  --adr_cutoff "${ADR_CUTOFF}"

echo "[3/3] Filter scan completed"

echo "========================================"
echo "Pipeline finished successfully"
echo "========================================"
