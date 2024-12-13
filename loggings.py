from logging import getLogger, INFO, DEBUG, ERROR, FileHandler, Formatter, StreamHandler
from colorama import Fore, Style, init
import os

init(autoreset=True)

logger = getLogger("DimensionalDataFlow")
logger.setLevel(DEBUG)

log_file = "logs/logs_dimensional_data_pipeline.txt"
os.makedirs(os.path.dirname(log_file), exist_ok=True)

if not logger.handlers:
    file_handler = FileHandler(log_file)
    file_handler.setLevel(DEBUG)

    stream_handler = StreamHandler()
    stream_handler.setLevel(DEBUG)

    file_formatter = Formatter(
        '%(asctime)s - %(levelname)s - %(message)s'
    )
    file_handler.setFormatter(file_formatter)

    class ColoredFormatter(Formatter):
        def format(self, record):
            if record.levelname == "INFO":
                record.msg = f"{Fore.GREEN}{record.msg}{Style.RESET_ALL}"
            elif record.levelname == "ERROR":
                record.msg = f"{Fore.RED}{record.msg}{Style.RESET_ALL}"
            elif record.levelname == "DEBUG":
                record.msg = f"{Fore.YELLOW}{record.msg}{Style.RESET_ALL}"
            return super().format(record)

    console_formatter = ColoredFormatter("%(levelname)s: %(message)s")
    stream_handler.setFormatter(console_formatter)

    logger.addHandler(file_handler)
    logger.addHandler(stream_handler)
