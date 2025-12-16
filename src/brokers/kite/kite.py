import logging
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
from typing import Literal, Union

import pandas as pd
import polars as pl
from kiteconnect import KiteConnect
from pyotp import TOTP
from ratelimit import limits, sleep_and_retry
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.firefox.options import Options as FirefoxOptions

from src.brokers.kite.exceptions import KiteError
from src.utils import get_today, read_ini_file

logger = logging.getLogger(__name__)


class KiteLogin:
    """
    Handles Connection to Kite Broker
    """

    def __init__(self, credentials_path: str) -> None:
        """
        Initializes KiteLogin with credentials loaded from an ini file.

        The file must contain a "Kite" section with the following keys:
        - "user_id"
        - "password"
        - "api_key"
        - "api_secret_key"
        - "totp_key"

        Parameters:
        credentials_path (str): The path to the credentials file (must be an ini file)

        Returns:
        None
        """

        self._credentials = read_ini_file(file_location=credentials_path)
        self.logger = logging.getLogger(self.__class__.__name__)

        if self._credentials is None:
            raise KiteError(
                f"""No Credentials in file at location {credentials_path}"""
            )

        self._check_file()

    def _check_file(self) -> None:
        """
        Validate the credentials file.

        This method checks that the ini file has the required "KITE" section
        and that all necessary keys are present within that section.

        Raises:
        KiteLoginError: If any error occurs in the login process
        """
        if "KITE" not in self._credentials.sections():
            raise KiteError("""The "KITE" section is missing from the ini file""")

        required_keys = ["user_id", "password", "api_key", "api_secret_key", "totp_key"]

        for key in required_keys:
            if key not in self._credentials["KITE"]:
                raise KiteError(
                    f"""The required key "{key}" is missing in the 'KITE' section."""
                )

    def _generate_request_token(self) -> str:
        """
        Generate a request token for KiteConnect.

        Returns:
        str: The generated request token.
        """
        kite = KiteConnect(api_key=self._credentials["KITE"]["api_key"])
        options = FirefoxOptions()
        options.add_argument("--headless")
        driver = webdriver.Firefox(options=options)
        driver.get(kite.login_url())
        driver.implicitly_wait(10)
        username = driver.find_element(By.ID, "userid")
        password = driver.find_element(By.ID, "password")
        username.send_keys(self._credentials["KITE"]["user_id"])
        password.send_keys(self._credentials["KITE"]["password"])
        driver.find_element(
            By.XPATH, "//button[@class='button-orange wide' and @type='submit']"
        ).send_keys(Keys.ENTER)
        pin = driver.find_element(By.XPATH, '//*[@type="number"]')
        token = TOTP(self._credentials["KITE"]["totp_key"]).now()
        pin.send_keys(token)
        time.sleep(10)

        self.logger.debug(driver.current_url.split("request_token="))

        request_token = driver.current_url.split("request_token=")[1][:32]
        driver.quit()

        self.logger.info("""Request Token is Generated Successfully""")
        return request_token

    def _generate_access_token(self, request_token: str) -> str:
        """
        Generate an access token for the given request_token.

        Parameters:
        request_token (str): The request token generated during login.

        Returns:
        str: The generated access token.

        Raises:
        KiteLoginError: If access token generation is not successful.
        """
        kite = KiteConnect(api_key=self._credentials["KITE"]["api_key"])
        response = kite.generate_session(
            request_token=request_token,
            api_secret=self._credentials["KITE"]["api_secret_key"],
        )

        self.logger.debug(response)

        access_token = response["access_token"]
        self.logger.info("""Access Token Generated is Successfully""")

        return access_token

    def _auto_login(self) -> KiteConnect:
        """
        Automatically logs in to Kite and returns a KiteConnect object.

        Returns:
        KiteConnect: The KiteConnect object after successful login.
        """

        self.logger.info("Starting Kite Loign")

        request_token = self._generate_request_token()
        access_token = self._generate_access_token(request_token=request_token)

        try:
            kite = KiteConnect(
                api_key=self._credentials["KITE"]["api_key"], access_token=access_token
            )

            self.logger.info("Kite Connection object created successfully")

            return kite
        except Exception as e:
            self.logger.error(e)
            raise KiteError(e)

    def auto_login(self) -> KiteConnect:
        """
        Automatically logs in to Kite and returns a KiteConnect object.

        Parameters:
        save_path (str): Path to save the kite access token for future use.

        Returns:
        KiteConnect: The KiteConnect object after loading credentials or performing login.
        """
        kite = self._auto_login()

        return kite

    def __call__(self):
        return self.auto_login()


def fetch_kite_instruments(
    kite: KiteConnect,
    download_path: str,
    exchanges: list[str] = [
        "BFO",
        "BSE",
        "CDS",
        "GLOBAL",
        "MCX",
        "NCO",
        "NFO",
        "NSE",
        "NSEIX",
    ],
) -> None:
    """
    Downloads the instrument list for different exchanges as CSV file
    at the download path given

    Parameters:
    kite (KiteConnect): KiteConnect object ot fetch the instrument list
    download_path (str): The path to download the instruments list
    exchanges List[str]: List of exchnages to download for.
    """

    ins_schema = {
        "instrument_token": pl.String,
        "exchange_token": pl.String,
        "tradingsymbol": pl.String,
        "name": pl.String,
        "last_price": pl.Float64,
        "expiry": pl.String,
        "strike": pl.Float64,
        "tick_size": pl.Float64,
        "lot_size": pl.Int64,
        "instrument_type": pl.String,
        "segment": pl.String,
        "exchange": pl.String,
    }

    today = get_today()

    for excg in exchanges:
        try:
            ins = kite.instruments(exchange=excg)
            df = pd.DataFrame(ins)
            df = df.astype(
                {col: "string" for col in df.select_dtypes(include="object").columns}
            )

            df = pl.from_pandas(df, schema_overrides=ins_schema).rename(
                {"tradingsymbol": "symbol"}
            )
            df_path = download_path / f"{excg}.parquet"
            df.write_parquet(df_path)

            logger.info(f"""Successfully Fetched Instruments for {excg} at {df_path}""")

        except Exception as e:
            logger.warning(
                f"""Failed to fetch instrument list for {excg} for date {today}."""
            )
            logger.error(e)


class KiteHistorical:
    """
    Gets Historical Data from Kite
    """

    def __init__(
        self,
        kite: KiteConnect,
        file_location: str,
        config_location: str,
    ) -> None:
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

        self._config_location = config_location
        self._config = read_ini_file(self._config_location)

        self._historical_rate_limit = self._config["historical_data_limit_days"]
        self._historical_api_limit = int(
            self._config["api_rate_limit_seconds"]["historical"]
        )

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


@sleep_and_retry
@limits(calls=10, period=1)
def _delete_gtt_order(kite: KiteConnect, id: int):
    kite.delete_gtt(trigger_id=id)


@sleep_and_retry
@limits(calls=10, period=1)
def _place_gtt_order(
    kite: KiteConnect,
    trigger_type: str,
    tradingsymbol: str,
    exchange: str,
    trigger_values: list[float],
    last_price: float,
    order_single: dict,
) -> dict:
    single_order = kite.place_gtt(
        trigger_type=trigger_type,
        tradingsymbol=tradingsymbol,
        exchange=exchange,
        trigger_values=trigger_values,
        last_price=last_price,
        orders=order_single,
    )
    return single_order


@sleep_and_retry
@limits(calls=10, period=1)
def _place_order(
    kite: KiteConnect,
    variety: str,
    exchange: str,
    tradingsymbol: str,
    transaction_type: str,
    quantity: int,
    order_type: str,
    product: str,
    price: float,
    trigger_price: float,
) -> str:
    return kite.place_order(
        variety=variety,
        exchange=exchange,
        tradingsymbol=tradingsymbol,
        transaction_type=transaction_type,
        quantity=quantity,
        order_type=order_type,
        product=product,
        price=price,
        trigger_price=trigger_price,
    )


class TransactionType(Enum):
    BUY = "BUY"
    SELL = "SELL"


def delete_gtt(kite: KiteConnect, transaction_type: TransactionType) -> None:
    """
    Deletes all the exisiting GTT orders based on Transaction Type
    """

    gtt_orders = pl.DataFrame(kite.get_gtts())

    if gtt_orders.shape[0] == 0:
        logger.warning("No Active GTT orders present")
        return None

    gtt_orders = (
        gtt_orders.lazy()
        .filter(pl.col("status") == "active")
        .explode("orders")
        .with_columns(pl.col("orders").struct.unnest())
        .filter(pl.col("transaction_type") == transaction_type)
        .select("id", "tradingsymbol", "exchange")
        .collect()
    )

    if gtt_orders.shape[0] == 0:
        logger.warning(f"No Active {transaction_type.value} GTT orders present")
        return None

    logger.info(f"{transaction_type.value} GTT ordes fetched successfully !!")

    gtt_orders = gtt_orders.rows()

    for id, symbol, exchg in gtt_orders:
        try:
            _delete_gtt_order(kite=kite, id=id)
            logger.info(
                f"Deleted {transaction_type.value} GTT for {symbol} at exchange {exchg} successfully !!"
            )
        except Exception as e:
            logger.error(
                f"Error deleting {transaction_type.value} GTT for {symbol} at exchange {exchg} successfully !!"
            )
            logger.error(f"{e}")

    return None


def place_gtt(
    orders: pl.DataFrame,
    kite: KiteConnect,
    trans_type: Literal["BUY", "SELL"],
    order_type: str = KiteConnect.ORDER_TYPE_LIMIT,
    product: str = KiteConnect.PRODUCT_CNC,
    exchange: str = KiteConnect.EXCHANGE_NSE,
    trigger_type: str = KiteConnect.GTT_TYPE_SINGLE,
):
    """
    Place Buy/ Sell Single Leg GTT orders
    """
    order_count = orders.shape[0]
    logger.info(f"""Total Orders to be placed are: {order_count}""")
    for order in orders.to_dicts():
        symbol = order["symbol"]
        trigger_price = order["trigger_price"]
        ltp = order["ltp"]
        quantity = order["quantity"]
        limit_price = order["limit_price"]

        order_single = [
            {
                "exchange": exchange,
                "tradingsymbol": symbol,
                "transaction_type": trans_type,
                "quantity": quantity,
                "order_type": order_type,
                "product": product,
                "price": limit_price,
            }
        ]

        try:
            single_order = _place_gtt_order(
                kite=kite,
                trigger_type=trigger_type,
                tradingsymbol=symbol,
                exchange=exchange,
                trigger_values=[trigger_price],
                last_price=ltp,
                order_single=order_single,
            )

            logger.info(
                f"""GTT order for {symbol} at trigger price {trigger_price} for {quantity} shares at limit of {limit_price} placed successfully with id {single_order["trigger_id"]}"""
            )
            order_count -= 1
        except Exception as e:
            logger.error(
                f"FAILED GTT order for {symbol} at trigger price {trigger_price} for {quantity} shares at limit of {limit_price}"
            )
            logger.error(f"{e}")

    if order_count == 0:
        logger.info("All orders placed successfully !!")

    else:
        logger.warning(f"Failed to place {order_count} orders")
