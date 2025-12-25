import logging

from src.brokers.nse.industry import (create_classification_table,
                                      fetch_nse_industry_classification,
                                      prepare_symbol_list)
from src.config.brokers.nse import NSEConfig
from src.config.exchange import Exchange
from src.config.market import Market
from src.config.storage_layout import StorageLayout


class NSE:
    def __init__(
        self, market: Market, exchange: Exchange, end_date: str, config: NSEConfig
    ):
        self._market = market
        self._exchange = exchange
        self._end_date = end_date
        self._config = config

        self.logger = logging.getLogger(__name__)

        self._db_path = StorageLayout.db_path(
            market=self._market, exchange=self._exchange
        )
        self._conn = f"sqlite:///{self._db_path}"

        self.logger.info(
            f"Broker: {self._config.NAME} | Market: {self._market} | EXCHG: {self._exchange}"
        )

    def __call__(self, instruments_path: str):
        create_classification_table(conn=self._conn, conf=self._config)

        symbol_list = prepare_symbol_list(
            ins_path=instruments_path,
            fetch_date=self._end_date,
            conf=self._config,
            conn=self._conn,
        )
        fetch_nse_industry_classification(
            symbol_list=symbol_list,
            fetch_date=self._end_date,
            conf=self._config,
            conn=self._conn,
        )
