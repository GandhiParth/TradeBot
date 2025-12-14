from pathlib import Path

download_path = Path("./downloads").resolve()
scans_save_path = download_path / "scans"
filter_save_path = download_path / "filters"

_db_path = Path("downloads/data.db").resolve()
db_conn = f"sqlite:///{_db_path}"


kite = {
    "kite_cred_path": "/home/parthgandhi/TradeBot_archive/credentials/kite.ini",
    "kite_conf_path": Path("./src/brokers/kite/conf.ini").resolve(),
    "hist_table_name": "historical",
    "failed_hist_table_name": "historical_failed",
}
