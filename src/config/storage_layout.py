import logging
from pathlib import Path

logger = logging.getLogger(__name__)


class StorageLayout:
    ROOT = Path("./storage").resolve()

    RUNS = ROOT / "runs"
    DATA = ROOT / "data"

    @staticmethod
    def runs_dir(run_date: str, market: str, exchange: str) -> Path:
        out = StorageLayout.RUNS / run_date / market / exchange
        logger.debug(f"Returning path: {out}")
        return out

    @staticmethod
    def data_dir(market: str, exchange: str) -> Path:
        out = StorageLayout.DATA / market / exchange
        logger.debug(f"Returning path: {out}")
        return out

    @staticmethod
    def scans_dir(run_date: str, market: str, exchange: str) -> Path:
        out = (
            StorageLayout.runs_dir(run_date=run_date, market=market, exchange=exchange)
            / "scans"
        )
        logger.debug(f"Returning path: {out}")
        return out

    @staticmethod
    def filters_dir(run_date: str, market: str, exchange: str) -> Path:
        out = (
            StorageLayout.runs_dir(run_date=run_date, market=market, exchange=exchange)
            / "filters"
        )
        logger.debug(f"Returning path: {out}")
        return out

    @staticmethod
    def db_path(market: str, exchange: str) -> Path:
        out = StorageLayout.data_dir(market=market, exchange=exchange) / "data.db"
        logger.debug(f"Returning path: {out}")
        return out
