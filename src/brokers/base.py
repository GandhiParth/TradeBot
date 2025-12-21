import logging
from abc import ABC, abstractmethod

from src.config.exchange import Exchange
from src.config.market import Market
from src.config.storage_layout import StorageLayout
from src.utils import timeit


class BaseBroker(ABC):
    """
    Contract that all brokers must implement.
    Brokers fetch data, they do NOT own storage.
    """

    def __init__(
        self,
        market: Market,
        exchange: Exchange,
        start_date: str,
        end_date: str,
        frequency: str,
        config: object,
        tables: dict,
    ):
        self._market = market.value
        self._exchange = exchange.value
        self._start_date = start_date
        self._end_date = end_date
        self._frequency = frequency
        self._config = config
        self._tables_name = tables[exchange]
        self.logger = logging.getLogger(self.__class__.__name__)

        self.logger.info(
            f"Broker: {self._config.NAME} | Market: {self._market} | EXCHG: {self._exchange}"
        )

        self._download_path = StorageLayout.data_dir(
            market=self._market, exchange=self._exchange
        )
        self._download_path.mkdir(parents=True, exist_ok=True)

        self._db_path = StorageLayout.db_path(
            market=self._market, exchange=self._exchange
        )

    @timeit
    @abstractmethod
    def login(self) -> object:
        """
        Authenticate / initialize session.
        Must be idempotent.
        """
        pass

    @timeit
    @abstractmethod
    def fetch_instruments(self) -> None:
        """
        Fetch tradable instruments for (market, exchange).

        Returns:
            iterable of normalized instrument dicts
        """
        pass

    @timeit
    @abstractmethod
    def fetch_ohlcv(
        self,
    ) -> None:
        """
        Fetch OHLCV bars.

        Returns:
            iterable of normalized OHLCV dicts
        """
        pass

    @abstractmethod
    def __call__(
        self,
    ) -> None:
        """
        Run all steps
        """
        pass
