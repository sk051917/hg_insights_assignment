import logging
import sys
from datetime import datetime


def get_logger(name: str, log_level: int = logging.INFO) -> logging.Logger:

    logger = logging.getLogger(name)
    
    if not logger.handlers:
        logger.setLevel(log_level)
        
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(log_level)
        
        formatter = logging.Formatter(
            fmt='%(asctime)s | %(levelname)-8s | %(name)s | %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        console_handler.setFormatter(formatter)
        
        logger.addHandler(console_handler)
        logger.propagate = False
    
    return logger