import logging
from typing import Optional

def setup_logging(quiet: bool = False) -> logging.Logger:
    """Configure logging for the application"""
    logger = logging.getLogger('bson_tools')
    logger.setLevel(logging.WARNING if quiet else logging.INFO)

    if not logger.handlers:
        handler = logging.StreamHandler()
        formatter = logging.Formatter('%(levelname)s: %(message)s')
        handler.setFormatter(formatter)
        logger.addHandler(handler)

    return logger
