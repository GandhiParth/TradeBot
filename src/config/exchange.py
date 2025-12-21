from enum import Enum


class Exchange(str, Enum):
    # India
    NSE = "NSE"

    # US
    NYSE = "XNYS"
    NASDAQ = "XNAS"
