import logging
import os
import sys
import datetime as dt


class CustomLogFormatter(logging.Formatter):
    converter=dt.datetime.fromtimestamp

    def formatTime(self, record, datefmt=None):
        ct = self.converter(record.created)
        if datefmt:
            s = ct.strftime(datefmt)
        else:
            t = ct.strftime("%Y-%m-%d %H:%M:%S")
            s = "%s.%03d" % (t, record.msecs)
        return s


def setup_logging(log_level, log_path):
    the_logger = logging.getLogger()
    for old_handler in the_logger.handlers:
        the_logger.removeHandler(old_handler)

    if log_path is None:
        handler = logging.StreamHandler(sys.stdout)
    else:
        handler = logging.FileHandler(log_path)

    json_format = '{ "timestamp": "%(asctime)s", "log_level": "%(levelname)s", "message": "%(message)s" }'
    handler.setFormatter(CustomLogFormatter(json_format))
    the_logger.addHandler(handler)
    new_level = logging.getLevelName(log_level.upper())
    the_logger.setLevel(new_level)

    return the_logger
