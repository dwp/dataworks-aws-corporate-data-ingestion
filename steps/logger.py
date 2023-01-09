import logging
import sys


def setup_logging(log_level, log_path):
    root = logging.getLogger()
    root.setLevel(log_level)
    for old_handler in root.handlers:
        root.removeHandler(old_handler)

    if log_path is None:
        handler = logging.StreamHandler(sys.stdout)
    else:
        handler = logging.FileHandler(log_path)

    formatter = logging.Formatter("{asctime} {name} {levelname:8s} {message}", style="{")
    handler.setFormatter(formatter)
    root.addHandler(handler)

