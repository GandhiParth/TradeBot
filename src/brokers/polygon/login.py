import logging

from massive import RESTClient

from src.utils import read_ini_file

logger = logging.getLogger(__name__)


def polygon_login(credentials_path: str) -> RESTClient:
    conf = read_ini_file(credentials_path)["POLYGON"]
    client = RESTClient(api_key=conf["api_key"])
    logger.info("Polyon Login Successfully")

    return client
