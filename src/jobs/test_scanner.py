import logging
from src.utils import setup_logger
from src.config.run_modes import RUN_MODES
import argparse
from datetime import datetime, timedelta
from src.config.exchange_tables import EXCHG_TABLES
from src.config.storage_layout import StorageLayout
import shutil
from src.scans.swing_scan import basic_scan, find_stocks, high_adr_scan, prep_scan_data
from pathlib import Path
from src.config.scans import scans_conf
import polars as pl

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
    data_path = StorageLayout.data_dir(market=market, exchange=market)
    db_path = data_path = StorageLayout.db_path(market=market, exchange=market)

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


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run Swing Scans")
    parser.add_argument("--fetch", action="store_true", help="Fetch Data")
    parser.add_argument("--run_mode", required=True, help="Run Mode")
    parser.add_argument("--end_date", required=True, help="End date YYYY-MM-DD")
    parser.add_argument("--adr_cutoff", default=3.5, help="ADR Cutoff")
    parser.add_argument("--freq", default="day", help="ADR Cutoff")

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

    data_table_id = EXCHG_TABLES[mode_conf["market"]]["ohlcv_daily"]
    _run_swing_scan(
        db_path=db_path,
        scans_path=scans_path,
        table_id=data_table_id,
        scans_conf=scans_conf[mode_conf["market"]],
        start_date=start_date,
        end_date=end_date,
    )
