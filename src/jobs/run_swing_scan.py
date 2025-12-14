from conf import db_conn, kite as kite_conf, download_path, scans_save_path
from datetime import datetime
from src.utils import setup_logger
import argparse
from src.scans.swing_scan import (
    prep_scan_data,
    basic_scan,
    high_adr_scan,
    find_stocks,
)

import logging
import polars as pl


setup_logger()
logger = logging.getLogger(__name__)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run Swing Scans")
    parser.add_argument("--end_date", required=True, help="End date YYYY-MM-DD")
    parser.add_argument("--start_date", required=True, help="Start date YYYY-MM-DD")
    parser.add_argument("--adr_cutoff", required=True, help="ADR Cutoff")
    args = parser.parse_args()

    end_date = datetime.strptime(args.end_date, "%Y-%m-%d")
    start_date = datetime.strptime(args.start_date, "%Y-%m-%d")
    adr_cutoff = float(args.adr_cutoff)

    master_df = prep_scan_data(
        ins_file_path=download_path / "NSE.parquet",
        db_conn=db_conn,
        kite_conf=kite_conf,
    )
    basic_scan_df = basic_scan(data=master_df)
    adr_scan_df = high_adr_scan(data=basic_scan_df, cut_off=adr_cutoff)

    basic_stocks_df = find_stocks(
        data=basic_scan_df, start_date=start_date, end_date=end_date
    )

    logger.info(
        f"MIN DATE for stocks scan: {basic_stocks_df.select(pl.col("timestamp").min().cast(pl.String())).item(0,0)}"
    )
    logger.info(
        f"MAX DATE for stocks scan: {basic_stocks_df.select(pl.col("timestamp").max().cast(pl.String())).item(0,0)}"
    )
    logger.info(
        f"# Stocks in BASIC SCAN: {basic_stocks_df.select(pl.col("symbol").n_unique()).item(0,0)}"
    )
    adr_stocks_df = find_stocks(
        data=adr_scan_df, start_date=start_date, end_date=end_date
    )
    logger.info(
        f"# Stocks in ADR SCAN: {basic_stocks_df.select(pl.col("symbol").n_unique()).item(0,0)}"
    )

    basic_scan_df.collect().write_parquet(scans_save_path / "basic_scan.parquet")
    adr_scan_df.collect().write_parquet(scans_save_path / "adr_scan.parquet")
    adr_stocks_df.write_parquet(scans_save_path / "adr_stocks.parquet")
    basic_stocks_df.write_parquet(scans_save_path / "basic_stocks.parquet")
