import logging
import time

from kiteconnect import KiteConnect
from pyotp import TOTP
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.firefox.options import Options as FirefoxOptions

from src.brokers.exceptions import KiteError
from src.utils import read_ini_file, get_today

import polars as pl
from pathlib import Path
import pandas as pd

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
    save_path = f"""{download_path}/{today}"""

    Path(save_path).mkdir(parents=True, exist_ok=True)

    for excg in exchanges:
        try:
            ins = kite.instruments(exchange=excg)
            df = pd.DataFrame(ins)
            df = df.astype(
                {col: "string" for col in df.select_dtypes(include="object").columns}
            )
            df = pl.from_pandas(df, schema_overrides=ins_schema)
            df_path = save_path + f"/{excg}.parquet"
            df.write_parquet(df_path)
            logger.info(f"""Successfully Fetched Instruments for {excg} at {df_path}""")

        except Exception as e:
            logger.warning(
                f"""Failed to fetch instrument list for {excg} for date {today}."""
            )
            logger.error(e)
