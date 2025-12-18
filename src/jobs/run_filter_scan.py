import argparse
import logging
from datetime import datetime

import polars as pl

from src.conf import filter_path, kite_conf, runs_conn, scans_conf, scans_path
from src.scans.filter_scan import adr_filter, basic_filter, pullback_filter, vcp_filter
from src.utils import setup_logger

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
        pl.scan_csv(scans_path / "adr_stocks.csv")
        .collect()
        .get_column("symbol")
        .to_list()
    )

    logger.info(f"Stocks in the Scan List {len(scan_symbol_list)}")

    query = f"""
    select * 
    from {kite_conf["hist_table_id"]}
    where symbol in {tuple(scan_symbol_list)}
    """
    data = pl.read_database_uri(query=query, uri=runs_conn)

    basic_stock_list = basic_filter(
        data=data, symbol_list=scan_symbol_list, scan_date=end_date, conf=scans_conf
    )
    data = data.filter(pl.col("symbol").is_in(basic_stock_list))

    # Basic filter
    basic_filter_df = data.filter(pl.col("timestamp") == end_date)
    basic_filter_df.write_csv(filter_path / "basic_filter.csv")
    logger.info(f"# Stocks in Basic Filter: {basic_filter_df.shape[0]}")

    # ADR filter
    adr_filter_df = adr_filter(data=data, adr_cutoff=adr_cutoff, end_date=end_date)
    adr_filter_df.write_csv(filter_path / "adr_filter.csv")
    logger.info(f"# Stocks in ADR Filter: {adr_filter_df.shape[0]}")

    # Pullback filter
    pullback_df = pullback_filter(
        data=data, end_date=end_date, adr_cutoff=adr_cutoff, conf=scans_conf
    )
    pullback_df.write_csv(filter_path / "pullback.csv")
    logger.info(f"# Stocks in PullBack: {pullback_df.shape[0]}")

    # VCP filter
    vcp_df = vcp_filter(
        data=data, end_date=end_date, conf=scans_conf["vcp_filter_conf"]
    )
    vcp_df.write_csv(filter_path / "vcp.csv")
    logger.info(f"# Stocks in VCP: {vcp_df.shape[0]}")
