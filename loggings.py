from logging import getLogger, INFO, DEBUG, ERROR, FileHandler, Formatter, StreamHandler
from colorama import Fore, Style, init
import os

# Initialize colorama for colored console output
init(autoreset=True)

# Configure and initialize the logger globally
logger = getLogger("DimensionalDataFlow")
logger.setLevel(DEBUG)

# Ensure the logs directory exists
log_file = "logs/logs_dimensional_data_pipeline.txt"
os.makedirs(os.path.dirname(log_file), exist_ok=True)

# Avoid duplicate handlers
if not logger.handlers:
    # Create a file handler
    file_handler = FileHandler(log_file)
    file_handler.setLevel(DEBUG)

    # Create a stream handler for console output
    stream_handler = StreamHandler()
    stream_handler.setLevel(DEBUG)

    # Create a formatter for file output
    file_formatter = Formatter(
        '%(asctime)s - %(levelname)s - %(message)s'
    )
    file_handler.setFormatter(file_formatter)

    # Add colored formatter for console output
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

    # Add the handlers to the logger
    logger.addHandler(file_handler)
    logger.addHandler(stream_handler)
