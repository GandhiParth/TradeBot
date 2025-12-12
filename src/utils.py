import configparser
import logging
from datetime import datetime
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)


def setup_logger():

    root_logger = logging.getLogger()
    if root_logger.handlers:
        for handler in root_logger.handlers:
            root_logger.removeHandler(handler)

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(funcName)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        handlers=[logging.StreamHandler()],
    )


def read_ini_file(file_location: str) -> Optional[configparser.ConfigParser]:
    """
    Reads an ini file and returns a ConfigParser object.

    Parameters:
    file_location (str): The path to the ini file.

    Returns:
    Optional[configparser.ConfigParser]: The ConfigParser object if the file exists, None otherwise.
    """

    if not Path(file_location).exists():
        logger.warning(f"File: {file_location} does not exist")
        return None

    config = configparser.ConfigParser()
    config.read(file_location)

    return config


def get_today(format: str = "%Y-%m-%d") -> str:
    """
    Return today's date in the given format.

    Parameters:
    format (str): The format for the date string. Default is "%Y-%m-%d".

    Returns:
    str: Today's date formatted as specified.
    """
    today = datetime.now()
    formatted_date = today.strftime(format)
    return formatted_date
