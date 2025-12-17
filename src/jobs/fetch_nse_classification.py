import logging
from src.utils import setup_logger
from src.brokers.nse.industry import (
    create_classification_table,
    fetch_nse_industry_classification,
    prepare_symbol_list,
)
import argparse
from src.conf import core_conn, core_path, nse_conf, runs_path

setup_logger()
logger = logging.getLogger(__name__)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run NSE Industry Classification")
    parser.add_argument("--end_date", required=True, help="End date YYYY-MM-DD")
    args = parser.parse_args()

    fetch_date = args.end_date

    create_classification_table(conn=core_conn, conf=nse_conf)

    ins_path = runs_path / "NSE.parquet"
    symbol_list = prepare_symbol_list(
        ins_path=ins_path, fetch_date=fetch_date, conf=nse_conf, conn=core_conn
    )

    fetch_nse_industry_classification(
        symbol_list=symbol_list, fetch_date=fetch_date, conf=nse_conf, conn=core_conn
    )
