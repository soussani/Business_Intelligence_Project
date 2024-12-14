import sys
import os

from pipeline_dimensional_data.tasks import (
    create_tables_task,
    ingest_fact_table_task,
    ingest_fact_error_task,
)

from utils import generate_uuid, update_dimensional_tables
from loggings import logger


class DimensionalDataFlow:
    """
    Class to orchestrate the execution of dimensional data tasks.

    Attributes:
        execution_id (str): Unique identifier for each execution.
    """

    def __init__(self):
        """
        Initialize the data flow with a unique execution ID.
        """
        self.execution_id = generate_uuid()
        self.tasks_status = {}

    def exec(self, start_date: str, end_date: str):
        """
        Executes ETL tasks sequentially.

        Args:
            start_date (str): Start date for data processing.
            end_date (str): End date for data processing.

        Returns:
            dict: The final status of the entire pipeline.
        """
        logger.info(f"Starting Dimensional Data Flow Execution ID: {self.execution_id}")

        # Define SQL file paths
        create_tables_file = "infrastructure_initiation/dimensional_db_table_creation.sql"
        update_fact_file = "pipeline_dimensional_data/queries/update_fact.sql"
        update_fact_error_file = "pipeline_dimensional_data/queries/update_fact_error.sql"

        try:
            # Task 1: Create Tables
            self.tasks_status['create_tables'] = create_tables_task(create_tables_file)
            if not self.tasks_status['create_tables']['success']:
                raise Exception("Table creation failed.")
            logger.info("Table creation completed successfully.")

            # Task 2: Ingest Fact Table
            self.tasks_status['ingest_fact'] = ingest_fact_table_task(update_fact_file, start_date, end_date)
            if not self.tasks_status['ingest_fact']['success']:
                raise Exception("Fact table ingestion failed.")
            logger.info("Fact table ingestion completed successfully.")

            # Task 3: Ingest FactError Table
            self.tasks_status['ingest_fact_error'] = ingest_fact_error_task(update_fact_error_file, start_date, end_date)
            if not self.tasks_status['ingest_fact_error']['success']:
                raise Exception("FactError table ingestion failed.")
            logger.info("FactError table ingestion completed successfully.")

            logger.info(f"Execution {self.execution_id} completed successfully!")
            return self.tasks_status

        except Exception as e:
            logger.error(f"Execution {self.execution_id} failed: {str(e)}", exc_info=True)
            self.tasks_status['error'] = str(e)
            return self.tasks_status

