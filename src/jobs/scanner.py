import argparse
import logging
import shutil
from datetime import datetime, timedelta
from pathlib import Path

import polars as pl

from src.config.exchange_tables import EXCHG_TABLES
from src.config.run_modes import RUN_MODES
from src.config.scans import filter_conf, scans_conf
from src.config.storage_layout import StorageLayout
from src.scans.filter_scan import (
    adr_filter,
    basic_filter,
    pullback_filter,
    pullback_reversal_filter,
    sma_200_filter,
    vcp_filter,
)
from src.scans.swing_scan import basic_scan, find_stocks, high_adr_scan, prep_scan_data
from src.utils import setup_logger

logger = logging.getLogger(__name__)
setup_logger()


def _get_start_lookback_date(end_date: str, mode_conf: dict) -> tuple[str, str]:
    _date_obj = datetime.strptime(end_date, "%Y-%m-%d")
    start_date = _date_obj - timedelta(
        days=mode_conf["scans_conf"]["months_lookback"] * 30
    )
    lookback_date = start_date - timedelta(
        days=mode_conf["scans_conf"]["data_lookback_days"]
    )

    start_date = start_date.strftime("%Y-%m-%d")
    lookback_date = lookback_date.strftime("%Y-%m-%d")

    logger.info(
        f"LOOKBACK DATE: {lookback_date} | START_DATE: {start_date} | END_DATE: {end_date}"
    )

    return start_date, lookback_date


def _make_dir(end_date: str, market: str, exchange: str, fetch_flag: bool):
    data_path = StorageLayout.data_dir(market=market, exchange=exchange)
    db_path = StorageLayout.db_path(market=market, exchange=exchange)

    if fetch_flag:
        if data_path.exists() and data_path.is_dir():
            logger.info(f"Deleting Directory: {data_path}")
            shutil.rmtree(data_path)
        data_path.mkdir(exist_ok=True, parents=True)
        logger.info(f"Creating Directory: {data_path}")

    runs_path = StorageLayout.runs_dir(
        run_date=end_date, market=market, exchange=exchange
    )
    if runs_path.exists() and runs_path.is_dir():
        logger.info(f"Deleting Directory: {runs_path}")
        shutil.rmtree(runs_path)

    runs_path.mkdir(parents=True, exist_ok=True)
    logger.info(f"Creating Directory: {runs_path}")

    scans_path = StorageLayout.scans_dir(
        run_date=end_date, market=market, exchange=exchange
    )
    scans_path.mkdir(parents=True)
    logger.info(f"Creating Directory: {scans_path}")

    filters_path = StorageLayout.filters_dir(
        run_date=end_date, market=market, exchange=exchange
    )
    filters_path.mkdir(parents=True)
    logger.info(f"Creating Directory: {filters_path}")

    return data_path, db_path, runs_path, scans_path, filters_path


def _run_swing_scan(
    db_path: Path,
    scans_path: Path,
    table_id: str,
    scans_conf: dict,
    start_date: str,
    end_date: str,
    adr_cutoff: float,
):
    start_date = datetime.strptime(start_date, "%Y-%m-%d")
    end_date = datetime.strptime(end_date, "%Y-%m-%d")

    master_df = prep_scan_data(
        conn=f"sqlite:///{db_path}",
        table_id=table_id,
        lookback_min_gains_dict=scans_conf["lookback_min_return_pct"],
    )

    ## BASIC SCAN
    basic_scan_df = basic_scan(data=master_df, conf=scans_conf)
    logger.info(
        f"MIN DATE for stocks scan: {basic_scan_df.select(pl.col('timestamp').min().cast(pl.String())).collect().item(0, 0)}"
    )
    logger.info(
        f"MAX DATE for stocks scan: {basic_scan_df.select(pl.col('timestamp').max().cast(pl.String())).collect().item(0, 0)}"
    )

    ## Basic Stocks in Search Dates Range
    basic_stocks_df = find_stocks(
        data=basic_scan_df, start_date=start_date, end_date=end_date
    )
    basic_scan_df.collect().write_csv(scans_path / "basic_scan.csv")
    basic_stocks_df.write_csv(scans_path / "basic_stocks.csv")
    logger.info(f"SCan path: {scans_path}")
    logger.info(
        f"# Stocks in BASIC SCAN: {basic_stocks_df.select(pl.col('symbol').n_unique()).item(0, 0)}"
    )

    ## ADR SCAN
    adr_scan_df = high_adr_scan(data=master_df, adr_cutoff=adr_cutoff, conf=scans_conf)
    logger.info(
        f"MIN DATE for stocks scan: {adr_scan_df.select(pl.col('timestamp').min().cast(pl.String())).collect().item(0, 0)}"
    )
    logger.info(
        f"MAX DATE for stocks scan: {adr_scan_df.select(pl.col('timestamp').max().cast(pl.String())).collect().item(0, 0)}"
    )

    ## ADR Stocks in Search Dates Range
    adr_stocks_df = find_stocks(
        data=adr_scan_df, start_date=start_date, end_date=end_date
    )
    adr_scan_df.collect().write_csv(scans_path / "adr_scan.csv")
    adr_stocks_df.write_csv(scans_path / "adr_stocks.csv")
    logger.info(
        f"# Stocks in ADR SCAN: {adr_stocks_df.select(pl.col('symbol').n_unique()).item(0, 0)}"
    )


def _run_filter_scan(
    db_path: Path,
    scans_path: Path,
    filters_path: Path,
    table_id: str,
    scans_conf: dict,
    filters_conf: dict,
    end_date: str,
    adr_cutoff: float,
):
    end_date = datetime.strptime(args.end_date, "%Y-%m-%d")

    scan_symbol_list = (
        pl.scan_csv(scans_path / "basic_stocks.csv")
        .collect()
        .get_column("symbol")
        .to_list()
    )
    logger.info(f"Stocks in the Scan List {len(scan_symbol_list)}")

    query = f"""
    select * 
    from {table_id}
    where symbol in {tuple(scan_symbol_list)}
    """

    data = pl.read_database_uri(query=query, uri=f"sqlite:///{db_path}")
    basic_stock_list = basic_filter(
        data=data, symbol_list=scan_symbol_list, scan_date=end_date, conf=scans_conf
    )
    data = data.filter(pl.col("symbol").is_in(basic_stock_list))

    # Basic filter
    basic_filter_df = data.filter(pl.col("timestamp") == end_date)
    basic_filter_df.write_csv(filters_path / "basic_filter.csv")
    logger.info(f"# Stocks in Basic Filter: {basic_filter_df.shape[0]}")

    # 200 SMA filter
    basic_filter_stocks = basic_filter_df.get_column("symbol").to_list()
    data = data.filter(pl.col("symbol").is_in(basic_filter_stocks))
    sma_200_filter_df = sma_200_filter(data=data, end_date=end_date)
    sma_200_filter_df.write_csv(filters_path / "sma_200_filter.csv")
    logger.info(f"# Stocks in SMA 200 Filter: {sma_200_filter_df.shape[0]}")

    # ADR filter
    sma_200_filter_stocks = sma_200_filter_df.get_column("symbol").to_list()
    data = data.filter(pl.col("symbol").is_in(sma_200_filter_stocks))
    adr_filter_df = adr_filter(data=data, adr_cutoff=adr_cutoff, end_date=end_date)
    adr_filter_df.write_csv(filters_path / "adr_filter.csv")
    logger.info(f"# Stocks in ADR Filter: {adr_filter_df.shape[0]}")

    # Final ADR Filter
    adr_filter_stocks = adr_filter_df.get_column("symbol").to_list()
    data = data.filter(pl.col("symbol").is_in(adr_filter_stocks))

    # Pullback filter
    pullback_df = pullback_filter(
        data=data, end_date=end_date, conf=filters_conf["pullback"]
    )
    pullback_df.select(pl.exclude("flag_dates")).write_csv(
        filters_path / "pullback.csv"
    )
    pullback_df.write_parquet(filters_path / "pullback.parquet")
    logger.info(f"# Stocks in PullBack: {pullback_df.shape[0]}")

    # VCP filter
    vcp_df = vcp_filter(data=data, end_date=end_date, conf=filters_conf["vcp"])
    vcp_df.write_csv(filters_path / "vcp.csv")
    logger.info(f"# Stocks in VCP: {vcp_df.shape[0]}")

    ## Pullback Reversal
    reversal_df = pullback_reversal_filter(
        data=data, end_date=end_date, conf=filters_conf["pullback"]
    )
    reversal_df.write_csv(filters_path / "reversal.csv")
    logger.info(f"# Stocks in Pullback reversal: {reversal_df.shape[0]}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run Swing Scans")
    parser.add_argument("--fetch", action="store_true", help="Fetch Data")
    parser.add_argument("--run_mode", required=True, help="Run Mode")
    parser.add_argument("--end_date", required=True, help="End date YYYY-MM-DD")
    parser.add_argument("--adr_cutoff", default=3.5, help="ADR Cutoff")
    parser.add_argument("--freq", default="day", help="Frequency of data to be fetched")

    args = parser.parse_args()
    fetch_flag = args.fetch
    mode_conf = RUN_MODES[args.run_mode]
    end_date = args.end_date
    adr_cutoff = float(args.adr_cutoff)
    frequency = args.freq

    ## Fetch Dates
    start_date, lookback_date = _get_start_lookback_date(
        end_date=end_date, mode_conf=mode_conf
    )

    ## Make Dir
    data_path, db_path, runs_path, scans_path, filters_path = _make_dir(
        end_date=end_date,
        market=mode_conf["market"],
        exchange=mode_conf["exchange"],
        fetch_flag=fetch_flag,
    )

    ## Fetch Data
    if fetch_flag:
        logger.info("Fetching Data....")
        broker = mode_conf["broker"]
        broker = broker(
            market=mode_conf["market"],
            exchange=mode_conf["exchange"],
            start_date=lookback_date,
            end_date=end_date,
            frequency=frequency,
            config=mode_conf["config"],
            tables=EXCHG_TABLES,
        )()

    ## Run Swing Scan
    logger.info("######### Running Swing Scan #########")
    data_table_id = EXCHG_TABLES[mode_conf["exchange"]]["ohlcv_daily"]
    _run_swing_scan(
        db_path=db_path,
        scans_path=scans_path,
        table_id=data_table_id,
        scans_conf=scans_conf[mode_conf["market"]],
        start_date=start_date,
        end_date=end_date,
        adr_cutoff=adr_cutoff,
    )

    ## Run Filter Scan
    logger.info("######### Running Filter Scan #########")
    _run_filter_scan(
        db_path=db_path,
        scans_path=scans_path,
        filters_path=filters_path,
        table_id=data_table_id,
        scans_conf=scans_conf[mode_conf["market"]],
        filters_conf=filter_conf[mode_conf["market"]],
        end_date=end_date,
        adr_cutoff=adr_cutoff,
    )
