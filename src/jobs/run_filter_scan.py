from conf import (
    download_path,
    db_conn,
    kite as kite_conf,
    filter_save_path,
    scans_save_path,
)
from src.utils import setup_logger
import argparse
from src.scans.conf import PULLBACK_NEAR_PCT
from datetime import datetime
import polars as pl
import logging
from src.scans.filter_scan import basic_filter, pullback_filter


setup_logger()
logger = logging.getLogger(__name__)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run Filter Scans")
    parser.add_argument("--end_date", required=True, help="End date YYYY-MM-DD")
    parser.add_argument("--adr_cutoff", required=True, help="ADR Cutoff")
    args = parser.parse_args()

    end_date = datetime.strptime(args.end_date, "%Y-%m-%d")
    adr_cutoff = float(args.adr_cutoff)

    scan_symbol_list = (
        pl.scan_parquet(scans_save_path / "adr_stocks.parquet")
        .collect()
        .get_column("symbol")
        .to_list()
    )

    logger.info(f"Stocks in the Scan List {len(scan_symbol_list)}")

    query = f"""
    select * 
    from {kite_conf["hist_table_name"]}
    where symbol in {tuple(scan_symbol_list)}
    """
    data = pl.read_database_uri(query=query, uri=db_conn)

    basic_stock_list = basic_filter(
        data=data, symbol_list=scan_symbol_list, scan_date=end_date
    )
    data = data.filter(pl.col("symbol").is_in(basic_stock_list))

    pullback_df = pullback_filter(
        data=data, end_date=end_date, near_pct=PULLBACK_NEAR_PCT, adr_cutoff=adr_cutoff
    )
    pullback_df.write_csv(filter_save_path / "pullback.csv")
    logger.info(f"# Stocks in PullBack: {pullback_df.shape[0]}")
