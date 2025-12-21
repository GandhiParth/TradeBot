from datetime import datetime

import polars as pl

from src.brokers.base import BaseBroker
from src.brokers.kite.historical import KiteHistorical
from src.brokers.kite.instruments import fetch_instruments
from src.brokers.kite.login import KiteLogin


class Kite(BaseBroker):
    def login(self) -> None:
        self._client = KiteLogin(credentials_path=self._config.CREDENTIALS_PATH)()

    def fetch_instruments(self):
        ins_df = fetch_instruments(kite=self._client, exchange=self._exchange)

        self.logger.info(f"Symbols in Instruments List {ins_df.shape[0]}")
        ins_df.write_parquet(self._download_path / "instruments.parquet")

    def fetch_ohlcv(self):
        df = (
            pl.scan_parquet(self._download_path / "instruments.parquet")
            .remove(pl.col("segment") == "INDICES")
            .with_columns(
                pl.col("symbol")
                .str.split(by="-")
                .list.get(index=1, null_on_oob=True)
                .fill_null("EQ")
                .alias("suffix")
            )
            .filter(pl.col("suffix").is_in(["RR", "IV", "EQ"]))
            .collect()
        )

        self.logger.info(f"Data will be fecthed for {df.shape[0]} symbols")

        save_path = self._download_path / "symbols_fecthed.parquet"
        df.write_parquet(save_path)

        kite_hist = KiteHistorical(
            kite=self._client,
            file_location=save_path,
            config=self._config,
        )

        end_date = datetime.strptime(self._end_date, "%Y-%m-%d").strftime(
            "%Y-%m-%d 00:00:00"
        )
        start_date = datetime.strptime(self._start_date, "%Y-%m-%d").strftime(
            "%Y-%m-%d 00:00:00"
        )

        self.logger.info("Starting Data Fetching...")

        kite_hist.get_historical_data(
            start_date=start_date,
            end_date=end_date,
            frequency=self._frequency,
            oi_flag=False,
            continuous_flag=False,
            db_conn=f"sqlite:///{self._db_path}",
            failed_table_name=self._tables_name["ohlcv_failed"],
            insert_table_name=self._tables_name["ohlcv_daily"],
        )

        self.logger.info(self._db_path)

    def __call__(self):
        self.login()
        self.fetch_instruments()
        self.fetch_ohlcv()
