# logging.py

from logging import getLogger, INFO, FileHandler, Formatter
import os

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

        # Create a formatter
        formatter = Formatter(
            f'%(asctime)s - ExecutionID: {execution_id} - %(levelname)s - %(message)s'
        )
        file_handler.setFormatter(formatter)

        # Add the handler to the logger
        logger.addHandler(file_handler)

    return logger
