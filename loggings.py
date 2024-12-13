from logging import getLogger, INFO, FileHandler, Formatter, StreamHandler
from colorama import Fore, Style, init
import os

# Initialize colorama for colored console output
init(autoreset=True)

def get_logger(execution_id: str, log_file: str = "logs/logs_dimensional_data_pipeline.txt"):
    """
    Configures and returns a logger for the dimensional data flow.

    Args:
        execution_id (str): Unique execution identifier (UUID).
        log_file (str): Path to the log file.

    Returns:
        logging.Logger: Configured logger instance.
    """
    # Ensure the logs directory exists
    os.makedirs(os.path.dirname(log_file), exist_ok=True)

    # Create a logger
    logger = getLogger(f"DimensionalDataFlow-{execution_id}")
    logger.setLevel(INFO)

    # Avoid duplicate handlers
    if not logger.handlers:
        # Create a file handler
        file_handler = FileHandler(log_file)
        file_handler.setLevel(INFO)

        # Create a stream handler for console output
        stream_handler = StreamHandler()
        stream_handler.setLevel(INFO)

        # Create a formatter for file output
        file_formatter = Formatter(
            f'%(asctime)s - ExecutionID: {execution_id} - %(levelname)s - %(message)s'
        )
        file_handler.setFormatter(file_formatter)

        # Add colored formatter for console output
        class ColoredFormatter(Formatter):
            def format(self, record):
                if record.levelname == "INFO":
                    record.msg = f"{Fore.GREEN}{record.msg}{Style.RESET_ALL}"
                elif record.levelname == "ERROR":
                    record.msg = f"{Fore.RED}{record.msg}{Style.RESET_ALL}"
                return super().format(record)

        console_formatter = ColoredFormatter("%(levelname)s: %(message)s")
        stream_handler.setFormatter(console_formatter)

        # Add the handlers to the logger
        logger.addHandler(file_handler)
        logger.addHandler(stream_handler)

    return logger
