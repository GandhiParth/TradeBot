from src.config.storage_layout import StorageLayout

from src.config.market import Market
from src.config.exchange import Exchange


class NSEConfig:
    NAME = "NSE"

    CLASSIFICATION_TABLE_ID = "industry_classification"

    FAILED_CLASSIFICATION_TABLE_ID = "industry_classification_failed"

    URL = "https://www.nseindia.com/"

    INSTRUMENTS_FILE_PATH = (
        StorageLayout.data_dir(market=Market.INDIA_EQUITIES, exchange=Exchange.NSE)
        / "instruments.parquet"
    )
