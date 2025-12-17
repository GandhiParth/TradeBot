#!/usr/bin/env bash
set -euo pipefail

# -----------------------------
# Defaults
# -----------------------------
RUN_FETCH=false

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
  echo "Usage: $0 [--fetch] END_DATE"
  echo "Example: $0 --fetch 2025-12-12"
  exit 1
fi

END_DATE="$1"

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
LOG_FILE="${LOG_DIR}/run_nse_classification_$(date +%Y-%m-%d).log"

## ENV
source .venv/bin/activate

# Overwrite log file on each run, still print to console
exec > >(tee "${LOG_FILE}") 2>&1


echo "========================================"
echo "Running NSE Classification Pipeline"
echo "END_DATE=${END_DATE}"
echo "RUN_FETCH=${RUN_FETCH}"
echo "========================================"



# -----------------------------
# Step 1: Fetch NSE historical data (optional)
# -----------------------------
if [[ "${RUN_FETCH}" == true ]]; then
  echo "[1/2] Fetching NSE Classification..."

  python3 -m src.jobs.make_dir --fetch

else
  echo "[1/2] Skipping make dir for runs (use --fetch to enable)"
  python3 -m src.jobs.make_dir
fi

# -----------------------------
# Step 2: Run NSE Classification
# -----------------------------
echo "[2/2] Running NSE classification..."

python3 -m src.jobs.fetch_nse_classification \
  --end_date "${END_DATE}" \


echo "[2/2] NSE classification completed"

echo "========================================"
echo "Pipeline finished successfully"
echo "========================================"
