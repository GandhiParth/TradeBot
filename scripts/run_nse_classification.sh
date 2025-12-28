#!/usr/bin/env bash
set -euo pipefail

# -----------------------------
# Parse args
# -----------------------------
while [[ $# -gt 0 ]]; do
  case "$1" in
    --end_date)
      END_DATE="$2"
      shift 2
      ;;
    *)
      echo "Unknown option: $1"
      exit 1
      ;;
  esac
done

: "${END_DATE:?--end_date is required (YYYY-MM-DD)}"

# -----------------------------
# Setup
# -----------------------------
PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${PROJECT_ROOT}"

source .venv/bin/activate

LOG_FILE="logs/run_nse_classification_${END_DATE}.log"
mkdir -p logs
exec > >(tee "${LOG_FILE}") 2>&1

# -----------------------------
# Run
# -----------------------------
echo "Running NSE Classification | END_DATE=${END_DATE}"

time python3 -m src.jobs.nse_classification --end_date "${END_DATE}"

echo "SCAN COMPLETED"
