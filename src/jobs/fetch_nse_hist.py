import argparse
import logging
from datetime import datetime, timedelta
from pathlib import Path

import polars as pl

from src.brokers.kite.login import KiteHistorical, KiteLogin, fetch_kite_instruments
from src.conf import kite_conf, runs_conn, runs_path, scans_conf
from src.utils import setup_logger, timeit

setup_logger()

logger = logging.getLogger(__name__)


def adjust_date_with_lookback(
    date_str: str, lookback_days: int, max_lookback_return_pct: int
) -> str:
    """ """

    date_obj = datetime.strptime(date_str, "%Y-%m-%d")

    adjusted = (
        date_obj
        - timedelta(days=lookback_days)
        - timedelta(days=max_lookback_return_pct * 6)
    )

    return adjusted.strftime("%Y-%m-%d 00:00:00")


@timeit
def fetch_nse_historical_data(
    start_date: str,
    end_date: str,
    conf: dict,
    download_path: Path,
    conn: str,
    freq: str = "day",
):
    """ """

    kite = KiteLogin(credentials_path=conf["kite_cred_path"])()

    fetch_kite_instruments(kite=kite, download_path=download_path, exchanges=["NSE"])

    df = (
        pl.scan_parquet(f"{download_path}/NSE.parquet")
        .with_columns(
            pl.col("symbol")
            .str.split(by="-")
            .list.get(index=1, null_on_oob=True)
            .fill_null("EQ")
            .alias("suffix")
        )
        .filter(pl.col("suffix").is_in(["RR", "IV", "EQ"]))
        .collect()
    )

    logger.info(f"Data will be fecthed for {df.shape[0]} symbols")

    save_path = f"{download_path}/fetch_symbol.parquet"
    df.write_parquet(save_path)

    kite_hist = KiteHistorical(
        kite=kite, file_location=save_path, config_location=conf["kite_conf_path"]
    )

    kite_hist.get_historical_data(
        start_date=start_date,
        end_date=end_date,
        frequency=freq,
        oi_flag=False,
        continuous_flag=False,
        db_conn=conn,
        failed_table_name=conf["failed_hist_table_id"],
        insert_table_name=conf["hist_table_id"],
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Fetch NSE Historical Data")
    parser.add_argument("--end_date", required=True, help="End date YYYY-MM-DD")
    parser.add_argument("--start_date", required=True, help="Start date YYYY-MM-DD")
    args = parser.parse_args()

    end_date = datetime.strptime(args.end_date, "%Y-%m-%d").strftime(
        "%Y-%m-%d 00:00:00"
    )
    start_date = datetime.strptime(args.start_date, "%Y-%m-%d").strftime(
        "%Y-%m-%d 00:00:00"
    )
    lookback = (
        datetime.strptime(args.end_date, "%Y-%m-%d")
        - datetime.strptime(args.start_date, "%Y-%m-%d")
    ).days

    start_date = adjust_date_with_lookback(
        args.end_date,
        lookback_days=lookback,
        max_lookback_return_pct=max(scans_conf["lookback_min_return_pct"]),
    )

    logger.info(f"START DATE: {start_date} | END DATE: {end_date}")

    fetch_nse_historical_data(
        start_date=start_date,
        end_date=end_date,
        conf=kite_conf,
        download_path=runs_path,
        conn=runs_conn,
    )
