import logging
from datetime import datetime, timedelta
from pathlib import Path

import polars as pl

from conf import download_path, db_conn, kite as kite_conf
from src.brokers.kite.kite import KiteHistorical, KiteLogin, fetch_kite_instruments
from src.utils import timeit
from src.scans.conf import filters_dict

logger = logging.getLogger(__name__)


def adjust_date_with_lookback(date_str: str, lookback_days: int) -> str:
    date_obj = datetime.strptime(date_str, "%Y-%m-%d")

    adjusted = (
        date_obj - timedelta(days=lookback_days) - timedelta(days=max(filters_dict) * 3)
    )

    return adjusted.strftime("%Y-%m-%d 00:00:00")


@timeit
def fetch_nse_historical_data(
    start_date: str,
    end_date: str,
    freq: str = "day",
    conf: dict = kite_conf,
    download_path: str = download_path,
    db_conn: str = db_conn,
):
    """
    Fetches historical data of NSE stocks

    :param start_date: Description
    :type start_date: str
    :param end_date: Description
    :type end_date: str
    :param freq: Description
    :type freq: str
    :param conf: Description
    :type conf: dict
    :param download_path: Description
    :type download_path: str
    """

    Path(download_path).mkdir(parents=True, exist_ok=True)

    kite = KiteLogin(credentials_path=kite_conf["kite_cred_path"])()

    fetch_kite_instruments(kite=kite, download_path=download_path, exchanges=["NSE"])

    save_path = f"{download_path}/fetch_symbol.parquet"

    df = (
        pl.scan_parquet(f"{download_path}/NSE.parquet")
        .remove(
            (pl.col("segment") == "NSE")
            & (pl.col("symbol").str.contains("-", literal=True))
        )
        .collect()
    )

    logger.info(f"Data will be fecthed for {df.shape[0]} symbols")
    df.write_parquet(save_path)

    kite_hist = KiteHistorical(
        kite=kite, file_location=save_path, config_location=kite_conf["kite_conf_path"]
    )
    kite_hist.get_historical_data(
        start_date=start_date,
        end_date=end_date,
        frequency=freq,
        oi_flag=False,
        continuous_flag=False,
        db_conn=db_conn,
        failed_table_name=conf["failed_hist_table_name"],
        insert_table_name=conf["hist_table_name"],
    )


if __name__ == "__main__":
    import argparse
    from src.utils import setup_logger

    setup_logger()

    parser = argparse.ArgumentParser(description="Fetch NSE Historical Data")
    parser.add_argument("--end_date", required=True, help="End date YYYY-MM-DD")
    parser.add_argument("--start_date", required=True, help="Start date YYYY-MM-DD")
    # parser.add_argument("--lookback", required=True, help="Lookback days")
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

    start_date = adjust_date_with_lookback(args.end_date, lookback_days=lookback)

    logger.info(f"START DATE: {start_date} | END DATE: {end_date}")

    fetch_nse_historical_data(start_date=start_date, end_date=end_date)
