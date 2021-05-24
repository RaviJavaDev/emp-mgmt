import os
import logging
from pathlib import Path


class FileLogger:
    def __init__(self):
        os.makedirs(os.path.join(os.path.dirname(Path(os.path.realpath('__file__')).parent), 'log'), exist_ok=True)
        logging.basicConfig(
            filename=os.path.join(os.path.dirname(Path(os.path.realpath('__file__')).parent), 'log', 'db_log.txt'),
            level=logging.DEBUG, format='%(asctime)s %(levelname)s %(message)s')

    def debug(self, message):
        logging.debug(message)

    def info(self, message):
        logging.info(message)

    def error(self, message):
        logging.error(message)

    def warning(self, message):
        logging.warning(message)

    def critical(self, message):
        logging.critical(message)

    def disable(self):
        logging.disable()
