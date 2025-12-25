import argparse
import logging


from src.config.storage_layout import StorageLayout
from src.utils import setup_logger
from src.config.market import Market
from src.config.exchange import Exchange

from src.config.run_modes import RUN_MODES
import shutil

setup_logger()
logger = logging.getLogger(__name__)


def __make_dir(market: str, exchange: str, fetch_flag: bool):
    data_path = StorageLayout.data_dir(market=market, exchange=exchange)
    db_path = StorageLayout.db_path(market=market, exchange=exchange)

    if fetch_flag:
        if data_path.exists() and data_path.is_dir():
            logger.info(f"Deleting Directory: {data_path}")
            shutil.rmtree(data_path)

    data_path.mkdir(exist_ok=True, parents=True)
    logger.info(f"Creating Directory: {data_path}")

    return data_path, db_path


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run NSE Industry Classification")
    parser.add_argument(
        "--fetch", action="store_true", help="Delete Eveerything & Fecth"
    )
    parser.add_argument("--end_date", required=True, help="End date YYYY-MM-DD")
    parser.add_argument("--run_mode", help="Run Mode", default="2")
    args = parser.parse_args()

    fetch_date = args.end_date
    mode_conf = RUN_MODES[args.run_mode]

    data_path, db_path = __make_dir(
        market=mode_conf["market"],
        exchange=mode_conf["exchange"],
        fetch_flag=args.fetch,
    )

    logger.info("######### Starting NSE Industry Fetching #########")

    instruments_path = StorageLayout.data_dir(
        market=Market.INDIA_EQUITIES, exchange=Exchange.NSE
    )

    instruments_path = mode_conf["config"].INSTRUMENTS_FILE_PATH

    logger.info(f"INSTRUMENTS PATH: {instruments_path}")

    broker = mode_conf["broker"]
    broker = broker(
        market=mode_conf["market"],
        exchange=mode_conf["exchange"],
        end_date=args.end_date,
        config=mode_conf["config"],
    )(instruments_path=instruments_path)

    logger.info("######### Completed NSE Industry Fetching #########")
