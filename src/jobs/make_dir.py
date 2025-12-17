import argparse
import logging
import shutil

from src.conf import (core_path, filter_path, runs_path, scans_path,
                      storage_path)
from src.utils import setup_logger

setup_logger()

logger = logging.getLogger(__name__)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Creating Required Directories")
    parser.add_argument(
        "--fetch", action="store_true", help="Whether to run fetch-related dirs"
    )
    args = parser.parse_args()

    storage_path.mkdir(parents=True, exist_ok=True)
    core_path.mkdir(parents=True, exist_ok=True)

    if args.fetch:
        logger.info("Creating runs directories")
        if runs_path.exists() and runs_path.is_dir():
            logger.info(f"Deleting Directory {runs_path}")
            shutil.rmtree(runs_path)

        runs_path.mkdir(parents=True, exist_ok=True)
        scans_path.mkdir(exist_ok=True)
        filter_path.mkdir(exist_ok=True)
