from pipeline_dimensional_data.tasks import (
    create_tables_task,
    load_raw_data_task,
    update_dimensional_tables_task,
    ingest_fact_table_task,
    ingest_fact_error_task,
    populate_dim_sor_task,
)
from utils import generate_uuid
from loggings import logger


class DimensionalDataFlow:
    """
    Orchestrates the ETL pipeline by defining tasks and their execution order.
    """

    def __init__(self):
        """
        Initializes the flow with a unique execution ID and task status tracker.
        """
        self.execution_id = generate_uuid()
        self.tasks_status = {}

    def exec(self, start_date: str, end_date: str):
        logger.info(f"Starting Dimensional Data Flow Execution ID: {self.execution_id}")

        try:
            # Task 1: Create Tables
            self.tasks_status['create_tables'] = create_tables_task()
            logger.info("Tables created successfully.")

            # Task 2: Load Raw Data
            self.tasks_status['load_raw_data'] = load_raw_data_task("raw_data_source.xlsx")
            logger.info("Raw data loaded successfully.")

            # Task 3: Update Dimensional Tables
            # Update Dimensional Tables
            dim_tables_directory = "pipeline_dimensional_data/queries"
            self.tasks_status['update_dim_tables'] = update_dimensional_tables_task(dim_tables_directory)
            logger.info("Dimensional tables updated successfully.")

            # Task 4: Ingest Fact Table
            fact_file = "pipeline_dimensional_data/queries/update_fact.sql"
            self.tasks_status['ingest_fact'] = ingest_fact_table_task(fact_file, start_date, end_date)
            logger.info("Fact table ingestion completed successfully.")

            # Task 5: Ingest FactError Table
            fact_error_file = "pipeline_dimensional_data/queries/update_fact_error.sql"
            self.tasks_status['ingest_fact_error'] = ingest_fact_error_task(fact_error_file, start_date, end_date)
            logger.info("FactError table ingestion completed successfully.")

            # Task 6: Populate Dim_SOR
            dim_sor_file = "pipeline_dimensional_data/queries/update_dim_sor.sql"
            self.tasks_status['populate_dim_sor'] = populate_dim_sor_task(dim_sor_file)
            logger.info("Dim_SOR table populated successfully.")

            logger.info(f"Execution {self.execution_id} completed successfully!")
            return self.tasks_status

        except Exception as e:
            logger.error(f"Execution {self.execution_id} failed: {str(e)}", exc_info=True)
            self.tasks_status['error'] = str(e)
            return self.tasks_status
