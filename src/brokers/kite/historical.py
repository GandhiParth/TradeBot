import logging
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
from typing import Literal, Union

import polars as pl
from kiteconnect import KiteConnect
from ratelimit import limits, sleep_and_retry


class KiteHistorical:
    """
    Gets Historical Data from Kite
    """

    def __init__(self, kite: KiteConnect, file_location: str, config: object) -> None:
        """
        Initializes the KiteHistorical class.

        Parameters:
        kite (KiteConnect): An instance of the KiteConnect object used for making API requests.
        file_location (str): Path to the instrument list parquet file.
        config_location (str): Path to the Kite INI configuration file.
        """
        self._kite = kite
        self._file_location = file_location
        self.logger = logging.getLogger(self.__class__.__name__)

        self._config = config

        self._historical_rate_limit = self._config.HISTORICAL_DATA_LIMIT_DAYS
        self._historical_api_limit = self._config.API_RATE_LIMIT_SECONDS["historical"]

    def _get_date_ranges(
        self,
        start_date: datetime,
        end_date: datetime,
        interval: Literal[
            "minute",
            "3minute",
            "5minute",
            "10minute",
            "15minute",
            "30minute",
            "60minute",
            "day",
        ],
    ) -> list[tuple[datetime, datetime]]:
        """
        Divides the time period into date ranges that comply with the rate limit for the specified interval.

        Parameters:
        from_date (datetime): Start date for fetching historical data.
        to_date (datetime): End date for fetching historical data.
        interval (Literal["minute", "3minute", "5minute", "10minute", "15minute", "30minute", "60minute"]): Frequency of the historical data.

        Returns:
        List[Tuple[datetime, datetime]]: A list of tuples representing the start and end dates for each range.
        """
        if interval not in self._historical_rate_limit:
            raise ValueError(
                "Invalid interval. Must be one of: "
                + ", ".join(self._historical_rate_limit.keys())
            )

        max_days = int(self._historical_rate_limit[interval])

        current_start = start_date
        date_ranges = []

        while current_start < end_date:
            current_end = min(current_start + timedelta(days=max_days), end_date)
            date_ranges.append((current_start, current_end))
            current_start = current_end + timedelta(seconds=1)

        return date_ranges

    @sleep_and_retry
    @limits(calls=3, period=1)
    def _get_historical_data(
        self,
        instrument_token: str,
        from_date: datetime,
        to_date: datetime,
        interval: str,
        continuous: bool,
        oi: bool,
    ) -> dict[str, Union[str, dict[str, list[list[Union[str, float, int]]]]]]:
        """
        Fetches historical data from the Kite API.

        Parameters:
        instrument_token (str): The token of the instrument for which data is to be fetched.
        from_date (datetime): Start date for the data.
        to_date (datetime): End date for the data.
        interval (str): Frequency of the data (e.g., "minute", "day").
        continuous (bool): If True, fetch continuous contract data for futures.
        oi (bool): If True, fetch open interest data.

        Returns:
        A dictionary with the following structure:
            {
                "status": "success" | "failure",
                "data": {
                    "candles": [
                        [
                            timestamp (str in ISO 8601 format, e.g. "2017-12-15T09:15:00+0530"),
                            open (float),
                            high (float),
                            low (float),
                            close (float),
                            volume (int)
                        ],
                        ...
                    ]
                }
        }
        """

        data = self._kite.historical_data(
            instrument_token=instrument_token,
            from_date=from_date,
            to_date=to_date,
            interval=interval,
            continuous=continuous,
            oi=oi,
        )
        return data

    def _get_data(
        self,
        date_ranges: list[tuple[datetime, datetime]],
        instrument_token: str,
        symbol: str,
        interval: str,
        oi_flag: bool,
        continuous_flag: bool,
    ) -> tuple[list, dict]:
        """
        Concurrently fetches historical data for a specific instrument over multiple date ranges.

        Parameters:
        date_ranges (List[Tuple[datetime, datetime]]): List of start and end date ranges.
        instrument_token (str): The token of the instrument for which data is to be fetched.
        interval (str): Frequency of the data (e.g., "minute", "day").
        oi_flag (bool): Boolean FLag to get Open Interest.
        continuous_flag (bool): Boolean FLag to get Continuous Data.

        Returns:
        Tuple[List, Dict]: A tuple containing the fetched data and a dictionary of failed date ranges.
        """

        param_map = {}

        with ThreadPoolExecutor(max_workers=self._historical_api_limit) as executor:
            for start_date, end_date in date_ranges:
                future = executor.submit(
                    self._get_historical_data,
                    instrument_token,
                    start_date,
                    end_date,
                    interval,
                    continuous_flag,
                    oi_flag,
                )
                param_map[future] = {
                    "symbol": symbol,
                    "start_date": start_date.strftime("%Y-%m-%d %H:%M:%S"),
                    "end_date": end_date.strftime("%Y-%m-%d %H:%M:%S"),
                    "interval": interval,
                    "ins_token": instrument_token,
                }

                try:
                    df = future.result()
                    if df != []:
                        df = self._generate_dataframe(candles=df, symbol=symbol)
                        df.write_database(
                            table_name=self._table_name,
                            connection=self._conn,
                            if_table_exists="append",
                        )

                        self.logger.info(
                            f"""Data Inserted Successfully for {param_map[future]}"""
                        )
                    else:
                        self.logger.info(f"""No Data Found for {param_map[future]}""")
                    del param_map[future]
                except Exception as e:
                    self.logger.error(e)
                    self.logger.error(f"""Failed for {param_map[future]}""")

        return param_map

    def _generate_dataframe(
        self,
        candles,
        symbol: str,
    ) -> pl.DataFrame:
        """
        Generates a Polars DataFrame from the given OHLCV candle data.

        Parameters:
        candles (List[List[str, float, float, float, float, float, Optional[float]]]): List of OHLCV candle data.
        symbol (str): The trading symbol for the instrument.

        Returns:
        pl.DataFrame: A Polars DataFrame containing the historical data.
        """
        return (
            pl.DataFrame(
                candles,
                schema_overrides={
                    "date": pl.Datetime(time_unit="us", time_zone="UTC"),
                    "open": pl.Float64,
                    "high": pl.Float64,
                    "low": pl.Float64,
                    "close": pl.Float64,
                    "volume": pl.Int64,
                },
            )
            .lazy()
            .with_columns(
                pl.col("date")
                .dt.convert_time_zone(time_zone="Asia/Calcutta")
                .alias("timestamp"),
                pl.lit(symbol).alias("symbol"),
            )
            .select("symbol", "timestamp", "open", "high", "low", "close", "volume")
            .collect()
        )

    def get_historical_data(
        self,
        start_date: str,
        end_date: str,
        frequency: Literal[
            "minute",
            "3minute",
            "5minute",
            "10minute",
            "15minute",
            "30minute",
            "60minute",
            "day",
        ],
        oi_flag: bool,
        continuous_flag: bool,
        db_conn: str,
        insert_table_name: str,
        failed_table_name: str,
    ) -> None:
        """
        Fetches historical data for all instruments listed in the provided CSV file and writes the data to a database table.

        Parameters:
        file_location (str): Path to the CSV file containing the symbols to fetch historical data for.
        start_date (str): Start date for fetching historical data. YYYY-MM-DD HH:MM:SS
        end_date (str): End date for fetching historical data. YYYY-MM-DD HH:MM:SS
        frequency (Literal["minute", "3minute", "5minute", "10minute", "15minute", "30minute", "60minute", "day"]): Frequency of the historical data.
        table_name (str): Name of the table where the data will be stored.
        """

        self._table_name = insert_table_name
        self._conn = db_conn

        symbol_tokens = (
            pl.scan_parquet(source=self._file_location)
            .select("symbol", "instrument_token")
            .sort("symbol")
            .collect()
            .rows()
        )

        self.logger.info(f"""Number of Tokens are {len(symbol_tokens)}""")

        start_date = datetime.strptime(start_date, "%Y-%m-%d %H:%M:%S")
        end_date = datetime.strptime(end_date, "%Y-%m-%d %H:%M:%S")

        date_ranges = self._get_date_ranges(
            start_date=start_date, end_date=end_date, interval=frequency
        )

        self.logger.info(f"""Total Number of Date Ranges are {len(date_ranges)}""")

        for symbol, token in symbol_tokens:
            param_map = self._get_data(
                date_ranges=date_ranges,
                instrument_token=token,
                symbol=symbol,
                interval=frequency,
                oi_flag=oi_flag,
                continuous_flag=continuous_flag,
            )

            self.logger.info(
                f"""Failed to get for {len(param_map)} date ranges for {symbol}"""
            )

            if len(param_map) > 0:
                failed_df = pl.DataFrame(list(param_map.values()))
                failed_df.write_database(
                    table_name=failed_table_name,
                    connection=db_conn,
                    if_table_exists="append",
                )
                self.logger.info(f"""Failed List added for {symbol}""")
