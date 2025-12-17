from pathlib import Path

#### Paths ####
storage_path = Path("./storage").resolve()
runs_path = storage_path / "runs"
core_path = storage_path / "core"
scans_save_path = runs_path / "scans"
filter_save_path = runs_path / "filters"
####################################


#### DB CONNECTIONS ###
_runs_db_path = runs_path / "data.db"
runs_conn = f"sqlite:///{_runs_db_path}"

_core_db_path = core_path / "nse.db"
core_conn = f"sqlite:///{_core_db_path}"
#####################################

#### KITE CONF ####
_HISTORICAL_TABLE_ID = "historical"
_HISTORICAL__FAILED_TABLE_ID = "historical_failed"

kite_conf = {
    "kite_cred_path": "/home/parthgandhi/TradeBot_archive/credentials/kite.ini",
    "kite_conf_path": Path("./src/brokers/kite/conf.ini").resolve(),
    "hist_table_id": _HISTORICAL_TABLE_ID,
    "failed_hist_table_id": _HISTORICAL__FAILED_TABLE_ID,
}
#####################################


#### NSE CONF ####
_NSE_WEBSITE = "https://www.nseindia.com/"
_FAILED_TABLE_ID = "failed_industry_classification"
_CLASSIFICATION_TABLE_ID = "equity_industry_classification"

nse_conf = {
    "nse_url": _NSE_WEBSITE,
    "classification_table_id": _CLASSIFICATION_TABLE_ID,
    "failed_classification_table_id": _FAILED_TABLE_ID,
}

##################################


#### SCANS CONF ####
_VOLUME_THRESHOLD = 10_00_00

_LOOKBACK_DAYS_TO_MIN_RETURN_PCT = {
    1: 4.99,  # close today ≥ close 1 day ago * 1.05
    22: 10,  # close today ≥ close 22 days ago * 1.10
    67: 20,  # close today ≥ close 67 days ago * 1.22
    125: 60,  # close today ≥ close 125 days ago * 1.60
}

_MID_DOWN_COUNT_THRESHOLD = 2
_PULLBACK_NEAR_PCT = 1  # in %

scans_conf = {
    "volume_threshold": _VOLUME_THRESHOLD,
    "pullback_near_pct": _PULLBACK_NEAR_PCT,
    "lookback_min_return_pct": _LOOKBACK_DAYS_TO_MIN_RETURN_PCT,
}

###############################
