import polars as pl
import logging
from selenium import webdriver
from selenium.webdriver.firefox.options import Options as FirefoxOptions
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
import time
from selenium.webdriver.common.keys import Keys


from src.utils import setup_logger
from src.brokers.nse.conf import (
    FAILED_TABLE_NAME,
    CLASSIFICATION_TABLE_NAME,
    nse_conn,
    NSE_WEBSITE,
)

setup_logger()

logger = logging.getLogger(__name__)


def prepare_symbol_list(ins_path: str, fetch_date: str, db_conn=nse_conn):

    query = f"""
        select symbol
        from {CLASSIFICATION_TABLE_NAME}
        where timestamp = '{fetch_date}'
    """
    success_df = pl.read_database_uri(query=query, uri=db_conn)
    success_symbols = success_df.get_column("symbol").to_list()

    logger.info(f"# of Symbols data already fecthed for: {len(success_symbols)}")

    ins_df = (
        pl.scan_parquet(ins_path)
        .remove(
            (pl.col("segment") == "INDICES")
            | (pl.col("name").str.contains(r"^SDL.*%"))
            | (pl.col("name") == "")
            | (pl.col("name").str.starts_with("GOI TBILL"))
            | (pl.col("name").str.contains("GOLDBONDS", literal=True))
            | (pl.col("name").str.contains("RE.", literal=True))
            | (pl.col("symbol").str.ends_with("-GS"))
        )
        .with_columns(
            pl.col("symbol").str.split(by="-").list.first().alias("search_symbol")
        )
        .select("search_symbol")
        .collect()
    )

    logger.info(f"# of Symbols to fetch data: {ins_df.shape[0]}")

    fetch_symbols = (
        ins_df.remove(pl.col("search_symbol").is_in(success_symbols))
        .sort("search_symbol")
        .get_column("search_symbol")
        .to_list()
    )

    logger.info(f"# of Symbols data will be fecthed for: {len(fetch_symbols)}")

    return fetch_symbols


def fetch_nse_industry_classification(
    symbol_list: list[str],
    fetch_date: str,
    nse_website: str = NSE_WEBSITE,
    db_conn=nse_conn,
):

    count = 0

    for symbol in symbol_list:
        if count % 100 == 0:
            logger.info(
                f"Fetched data Successfully for {count}/{len(symbol_list)} symbols"
            )

        try:
            options = FirefoxOptions()
            driver = webdriver.Firefox(options=options)
            driver.get(nse_website)
            driver.implicitly_wait(15)
            driver.maximize_window()

            search_box = driver.find_element(
                By.XPATH,
                "//input[@role='combobox' and contains(@class,'rbt-input-main')]",
            )

            search_box.click()
            search_box.send_keys(Keys.CONTROL, "a")
            search_box.send_keys(Keys.BACKSPACE)

            for ch in symbol:
                time.sleep(0.15)
                search_box.send_keys(ch)

            time.sleep(0.7)
            search_box.send_keys(Keys.ARROW_DOWN)
            search_box.send_keys(Keys.ENTER)

            time.sleep(2.5)

            info_btn = driver.find_element(
                By.XPATH,
                "//span[@role='button' and contains(@aria-label,'Industry Classification')]",
            )
            driver.execute_script("arguments[0].click();", info_btn)

            time.sleep(0.5)

            macro_sector = driver.find_element(
                By.XPATH,
                "//td[normalize-space()='Macro-Economic Sector']/following-sibling::td",
            ).text

            sector = driver.find_element(
                By.XPATH, "//td[normalize-space()='Sector']/following-sibling::td"
            ).text

            industry = driver.find_element(
                By.XPATH, "//td[normalize-space()='Industry']/following-sibling::td"
            ).text

            basic_industry = driver.find_element(
                By.XPATH,
                "//td[normalize-space()='Basic Industry']/following-sibling::td",
            ).text

            time.sleep(1)

            driver.execute_script(
                "arguments[0].click();",
                driver.find_element(By.XPATH, "//button[@aria-label='Close']"),
            )

            time.sleep(0.6)  # allow modal overlay to disappear

            market_cap = driver.find_element(
                By.XPATH,
                "//div[normalize-space()='Total Market Cap (₹ Cr.)']/following-sibling::div[1]",
            ).text

            data = {
                "timestamp": fetch_date,
                "symbol": symbol,
                "macro_economic_sector": macro_sector,
                "sector": sector,
                "industry": industry,
                "basic_industry": basic_industry,
                "market_cap_cr": market_cap,  # float
            }

            df = pl.DataFrame([data])

            df.write_database(
                table_name=CLASSIFICATION_TABLE_NAME,
                connection=db_conn,
                if_table_exists="append",
            )

            count += 1
            time.sleep(5)

        except Exception as e:
            logger.error(f"Failed for {symbol}: {e}")

            data = {
                "timestamp": fetch_date,
                "symbol": symbol,
            }

            df = pl.DataFrame([data])

            df.write_database(
                table_name=FAILED_TABLE_NAME,
                connection=db_conn,
                if_table_exists="append",
            )
        finally:
            driver.quit()

    logger.info(f"Fetched data Successfully for {count}/{len(symbol_list)} symbols")
