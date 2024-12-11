# Required Libraries
import os
from datetime import datetime
from utils import get_db_config, execute_sql_script_from_file
import pyodbc


# Database connection handler
def get_db_connection():
    """Establishes a database connection using config."""
    db_config = get_db_config()
    connection_string = (
        f"Driver={db_config['driver']};"
        f"Server={db_config['server']};"
        f"Database={db_config['database']};"
        f"UID={db_config['user']};"
        f"PWD={db_config['password']};"
        f"TrustServerCertificate=yes;"
        )
    return pyodbc.connect(connection_string)


# Task 1: Create Tables
def create_tables_task(sql_file_path: str):
    """
    Creates database tables by executing a given SQL script.
    """
    try:
        execute_sql_script_from_file(sql_file_path)
        print(f"Tables created successfully from: {sql_file_path}")
        return {'success': True}
    except Exception as e:
        print(f"Table creation failed: {e}")
        return {'success': False, 'error': str(e)}


# Task 2: Ingest Data into Fact Table
def ingest_fact_table_task(sql_file_path: str, start_date: str, end_date: str):
    """
    Ingests data into the fact table using a parametrized SQL file.
    """
    try:
        conn = get_db_connection()
        with open(sql_file_path, 'r', encoding='utf-8') as sql_file:
            sql_script = sql_file.read()

        # Execute SQL with parameters
        with conn.cursor() as cursor:
            cursor.execute(sql_script, start_date, end_date)
            conn.commit()
            print(f"Data successfully ingested from: {sql_file_path}")
        return {'success': True}
    except Exception as e:
        print(f"Data ingestion failed: {e}")
        return {'success': False, 'error': str(e)}


# Task 3: Ingest Faulty Rows into FactError Table
def ingest_fact_error_task(sql_file_path: str, start_date: str, end_date: str):
    """
    Ingests faulty rows into the FactError table.
    """
    try:
        conn = get_db_connection()
        with open(sql_file_path, 'r', encoding='utf-8') as sql_file:
            sql_script = sql_file.read()

        # Execute SQL with parameters
        with conn.cursor() as cursor:
            cursor.execute(sql_script, start_date, end_date)
            conn.commit()
            print(f"Faulty rows ingested from: {sql_file_path}")
        return {'success': True}
    except Exception as e:
        print(f"FactError ingestion failed: {e}")
        return {'success': False, 'error': str(e)}


# Main Pipeline Execution
def run_pipeline():
    """
    Main function to run the entire ETL pipeline in sequence.
    """

    # Define SQL file paths
    create_tables_file = "pipeline_dimensional_data/queries/create_tables.sql"
    update_fact_file = "pipeline_dimensional_data/queries/update_fact.sql"
    update_fact_error_file = "pipeline_dimensional_data/queries/update_fact_error.sql"

    # Define date parameters
    start_date = "2023-01-01"
    end_date = "2024-12-31"

    # Run pipeline tasks in sequence
    tasks_status = {}

    print("Starting ETL pipeline...")

    # Task 1: Create Tables
    tasks_status['create_tables'] = create_tables_task(create_tables_file)
    if not tasks_status['create_tables']['success']:
        print("Pipeline terminated: Table creation failed.")
        return tasks_status

    # Task 2: Ingest Fact Table
    tasks_status['ingest_fact'] = ingest_fact_table_task(update_fact_file, start_date, end_date)
    if not tasks_status['ingest_fact']['success']:
        print("Pipeline terminated: Fact table ingestion failed.")
        return tasks_status

    # Task 3: Ingest FactError Table
    tasks_status['ingest_fact_error'] = ingest_fact_error_task(update_fact_error_file, start_date, end_date)
    if not tasks_status['ingest_fact_error']['success']:
        print("Pipeline terminated: FactError ingestion failed.")
        return tasks_status

    print("Pipeline completed successfully!")
    return tasks_status


# Execute pipeline when running the script directly
if __name__ == "__main__":
    pipeline_status = run_pipeline()
    print("Final pipeline status:", pipeline_status)
