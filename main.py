import sys
import argparse
from pipeline_dimensional_data.flow import DimensionalDataFlow
from loggings import logger
from pipeline_dimensional_data.tasks import reset_db
from utils import execute_sql_script_from_file, load_raw_data_to_staging, update_dimensional_tables, update_fact_orders, update_fact_error_table


def get_args():
    """
    Parses command-line arguments for start and end dates.
    """
    parser = argparse.ArgumentParser(description="Run the dimensional data pipeline.")
    parser.add_argument("--start_date", required=True, help="The start date (YYYY-MM-DD).")
    parser.add_argument("--end_date", required=True, help="The end date (YYYY-MM-DD).")
    return parser.parse_args()


def main():
    """
    Main function to execute the dimensional data pipeline.
    """
    try:
        # Reset the database
        logger.info("Resetting the database...")
        reset_db()

        #Create db
        logger.info("Creating database...")
        execute_sql_script_from_file("infrastructure_initiation/dimensional_db_creation.sql")
        # Run table creation scripts
        logger.info("Creating tables...")
        execute_sql_script_from_file("infrastructure_initiation/staging_raw_table_creation.sql")

        # Load raw data
        logger.info("Loading raw data into staging tables...")
        load_raw_data_to_staging("raw_data_source.xlsx")

        # Parse arguments
        args = get_args()
        start_date = args.start_date
        end_date = args.end_date

        logger.info(f"Starting pipeline execution from {start_date} to {end_date}.")

        # Create an instance of DimensionalDataFlow
        data_flow = DimensionalDataFlow()

        # Execute the ETL pipeline
        pipeline_status = data_flow.exec(start_date, end_date)
        if not isinstance(pipeline_status, dict):
            raise TypeError(f"Expected a dictionary, got {type(pipeline_status)}")

        logger.info("Execution completed!")

        update_dimensional_tables()
        update_fact_orders()
        update_fact_error_table()
        logger.info("Dim Tables update completed!")

        logger.info(f"Final Pipeline Status: {pipeline_status}")

    except Exception as e:
        logger.error(f"Pipeline execution failed: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
