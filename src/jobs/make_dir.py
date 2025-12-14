if __name__ == "__main__":
    from pathlib import Path
    from conf import download_path, scans_save_path, filter_save_path
    import shutil
    import logging
    from src.utils import setup_logger

    setup_logger()

    logger = logging.getLogger(__name__)

    path = Path(download_path)

    if path.exists() and path.is_dir():
        logger.info(f"Deleting Directory {path}")
        shutil.rmtree(path)

    path.mkdir(parents=True, exist_ok=True)
    scans_save_path.mkdir(exist_ok=True)
    filter_save_path.mkdir(exist_ok=True)
