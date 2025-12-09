"""
Logging configuration module.

This module sets up logging for the pipeline. Since this file is named logging.py,
we need to import the standard library logging module explicitly to avoid
importing ourselves.
"""

import sys
import os
import logging

from pipeline_dimensional_data import config

log_dir = os.path.dirname(config.LOG_FILE)
if log_dir and not os.path.exists(log_dir):
    os.makedirs(log_dir, exist_ok=True)

LOG_FILE = config.LOG_FILE

LOG_FORMAT = "%(asctime)s | %(levelname)s | %(name)s | %(message)s"

logging.basicConfig(
    level=logging.INFO,
    format=LOG_FORMAT,
    handlers=[
        logging.FileHandler(LOG_FILE, encoding='utf-8'),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger()

getLogger = logging.getLogger
INFO = logging.INFO
DEBUG = logging.DEBUG
WARNING = logging.WARNING
ERROR = logging.ERROR
CRITICAL = logging.CRITICAL
