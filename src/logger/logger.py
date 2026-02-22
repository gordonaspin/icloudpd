"""Custom logging setup function"""

import sys
import logging

def setup_logger():
    """Set up logger and add stdout handler"""
    logger = logging.getLogger("icloudpd")
    pyicloud_logger = logging.getLogger('pyicloud')

    formatter = logging.Formatter(
        fmt="%(asctime)s %(levelname)-8s %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S")

    handler = logging.StreamHandler(stream=sys.stdout)
    handler.setFormatter(formatter)
    handler.name = "stdoutLogger"
    logger.addHandler(handler)
    pyicloud_logger.addHandler(handler)

    pyicloud_logger.disabled = logger.disabled
    pyicloud_logger.setLevel(logging.ERROR)

    return logger
