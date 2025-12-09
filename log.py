"""
Logging configuration module.

This module sets up logging for the pipeline. Since this file is named logging.py,
we need to import the standard library logging module explicitly to avoid
importing ourselves.
"""

import sys
import os

# Import standard library logging module explicitly to avoid conflict with this file's name
# We use importlib to ensure we get the standard library module, not this file
if 'logging' not in sys.modules:
    import importlib.util
    spec = importlib.util.find_spec('logging')
    _std_logging = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(_std_logging)
    sys.modules['logging'] = _std_logging
else:
    # If logging was already imported (should be standard library), use it
    _std_logging = sys.modules['logging']

from pipeline_dimensional_data import config

# Ensure logs dir exists
log_dir = os.path.dirname(config.LOG_FILE)
if log_dir and not os.path.exists(log_dir):
    os.makedirs(log_dir, exist_ok=True)

LOG_FILE = config.LOG_FILE

LOG_FORMAT = "%(asctime)s | %(levelname)s | %(name)s | %(message)s"

_std_logging.basicConfig(
    level=_std_logging.INFO,
    format=LOG_FORMAT,
    handlers=[
        _std_logging.FileHandler(LOG_FILE, encoding='utf-8'),
        _std_logging.StreamHandler()
    ]
)

# A convenience root logger for import elsewhere
logger = _std_logging.getLogger()

# Re-export standard library functions so other modules can use: from logging import getLogger
getLogger = _std_logging.getLogger
INFO = _std_logging.INFO
DEBUG = _std_logging.DEBUG
WARNING = _std_logging.WARNING
ERROR = _std_logging.ERROR
CRITICAL = _std_logging.CRITICAL
