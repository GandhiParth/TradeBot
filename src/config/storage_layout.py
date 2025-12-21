from pathlib import Path
import logging

logger = logging.getLogger(__name__)


class StorageLayout:
    ROOT = Path("./storage").resolve()

    RUNS = ROOT / "runs"
    DATA = ROOT / "data"

    @staticmethod
    def runs_dir(run_date: str) -> Path:
        out = StorageLayout.RUNS / run_date
        logger.debug(f"Returning path: {out}")
        return out

    @staticmethod
    def data_dir(market: str, exchange: str) -> Path:
        out = StorageLayout.DATA / market / exchange
        logger.debug(f"Returning path: {out}")
        return out

    @staticmethod
    def scans_dir(market: str, run_date: str) -> Path:
        out = StorageLayout.runs_dir(run_date=run_date) / market / "scans"
        logger.debug(f"Returning path: {out}")
        return out

    @staticmethod
    def filters_dir(market: str, run_date: str) -> Path:
        out = StorageLayout.runs_dir(run_date=run_date) / market / "filters"
        logger.debug(f"Returning path: {out}")
        return out

    @staticmethod
    def db_path(market: str, exchange: str) -> Path:
        out = StorageLayout.data_dir(market=market, exchange=exchange) / "data.db"
        logger.debug(f"Returning path: {out}")
        return out
