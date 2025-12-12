import logging
import time

from kiteconnect import KiteConnect
from pyotp import TOTP
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.firefox.options import Options as FirefoxOptions

from src.brokers.kite.exceptions import KiteError
from src.utils import read_ini_file


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
