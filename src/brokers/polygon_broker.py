from datetime import datetime

import polars as pl

from src.brokers.base import BaseBroker
from src.brokers.polygon.login import polygon_login
from src.brokers.polygon.instruments import fetch_instruments
from src.brokers.polygon.historical import polygon_historical
import time


class Polygon(BaseBroker):
    def login(self) -> None:
        self._client = polygon_login(credentials_path=self._config.CREDENTIALS_PATH)

    def fetch_instruments(self):
        ins_df = fetch_instruments(
            client=self._client, type="CS", market="stocks", exchange=self._exchange
        )

        self.logger.info(f"Symbols in Instruments List {ins_df.shape[0]}")
        ins_df.write_parquet(self._download_path / "instruments.parquet")

    def fetch_ohlcv(self):
        df = (
            pl.scan_parquet(self._download_path / "instruments.parquet")
            .select("symbol")
            .collect()
        )

        self.logger.info(f"Data will be fecthed for {df.shape[0]} symbols")

        save_path = self._download_path / "symbols_fecthed.parquet"
        df.write_parquet(save_path)

        self.logger.info("Starting Data Fetching...")

        polygon_historical(
            client=self._client,
            file_location=save_path,
            start_date=self._start_date,
            end_date=self._end_date,
            db_conn=f"sqlite:///{self._db_path}",
            insert_table_name=self._tables_name["ohlcv_daily"],
        )

    def __call__(self):
        self.login()
        self.fetch_instruments()
        time.sleep(60)
        self.fetch_ohlcv()
