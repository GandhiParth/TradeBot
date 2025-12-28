#!/usr/bin/env bash
set -euo pipefail

# -----------------------------
# Defaults (mirror Python)
# -----------------------------
RUN_FETCH=false
ADR_CUTOFF_DEFAULT=3.5
FREQ_DEFAULT="day"

# -----------------------------
# Parse flags
# -----------------------------
while [[ $# -gt 0 ]]; do
  case "$1" in
    --fetch)
      RUN_FETCH=true
      shift
      ;;
    --run_mode)
      RUN_MODE="$2"
      shift 2
      ;;
    --end_date)
      END_DATE="$2"
      shift 2
      ;;
    --adr_cutoff)
      ADR_CUTOFF="$2"
      shift 2
      ;;
    --freq)
      FREQ="$2"
      shift 2
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
# Required args
# -----------------------------
: "${END_DATE:?--end_date is required (YYYY-MM-DD)}"
: "${RUN_MODE:?--run_mode is required}"

# -----------------------------
# Apply defaults if unset
# -----------------------------
ADR_CUTOFF="${ADR_CUTOFF:-$ADR_CUTOFF_DEFAULT}"
FREQ="${FREQ:-$FREQ_DEFAULT}"

# -----------------------------
# Ensure project root
# -----------------------------
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "${SCRIPT_DIR}")"
cd "${PROJECT_ROOT}"

# -----------------------------
# Logs
# -----------------------------
LOG_DIR="${PROJECT_ROOT}/logs"
mkdir -p "${LOG_DIR}"
LOG_FILE="${LOG_DIR}/run_swing_pipeline_${END_DATE}.log"

# -----------------------------
# ENV
# -----------------------------
source .venv/bin/activate
exec > >(tee "${LOG_FILE}") 2>&1

echo "========================================"
echo "Running Swing Trading Pipeline"
echo "RUN_MODE=${RUN_MODE}"
echo "END_DATE=${END_DATE}"
echo "ADR_CUTOFF=${ADR_CUTOFF}"
echo "FREQ=${FREQ}"
echo "RUN_FETCH=${RUN_FETCH}"
echo "========================================"

# -----------------------------
# Build command once
# -----------------------------
CMD=(
  time python3 -m src.jobs.scanner
  --run_mode "${RUN_MODE}"
  --end_date "${END_DATE}"
  --adr_cutoff "${ADR_CUTOFF}"
  --freq "${FREQ}"
)

if [[ "${RUN_FETCH}" == true ]]; then
  CMD+=(--fetch)
fi

# -----------------------------
# Execute
# -----------------------------
"${CMD[@]}"
echo "SCAN COMPLETED"

# -----------------------------
# Post-run step for run_mode=1
# -----------------------------
if [[ "${RUN_MODE}" == "1" ]]; then
  echo "RUN_MODE=1 detected → running NSE analysis script"

  time python3 -m src.jobs.nse_analysis \
    --end_date "${END_DATE}" 

  rclone sync /home/parthgandhi/TradeBot/storage/data/IND/ChartsMaze gdrive:Backup/SwingTrade/ChartsMaze/RS --progress
  
  echo "NSE analysis completed"
fi

