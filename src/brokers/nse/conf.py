from pathlib import Path


_nse_path = Path("nse.db").resolve()
nse_conn = f"sqlite:///{_nse_path}"


NSE_WEBSITE = "https://www.nseindia.com/"
FAILED_TABLE_NAME = "failed_industry_classification"
CLASSIFICATION_TABLE_NAME = "equity_industry_classification"
