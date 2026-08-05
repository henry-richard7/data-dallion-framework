import logging
import sys

def get_logger(name: str) -> logging.Logger:
    """Returns a standardized logger instance with preconfigured formatter and handlers."""
    logger = logging.getLogger(name)
    if not logger.handlers:
        formatter = logging.Formatter(
            fmt="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S"
        )
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)
    return logger
