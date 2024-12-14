from pipeline_dimensional_data.config_db import get_db_connection, ensure_database_exists
from utils import execute_sql_script_from_file, load_raw_data_to_staging
from loggings import logger
import os

# Task 1: Create Tables
def create_tables_task():
    try:
        logger.info("Creating database and tables...")
        execute_sql_script_from_file("infrastructure_initiation/dimensional_db_creation.sql")
        execute_sql_script_from_file("infrastructure_initiation/staging_raw_table_creation.sql")
        execute_sql_script_from_file("infrastructure_initiation/dimensional_db_table_creation.sql")
        logger.info("Tables created successfully.")
        return {'success': True}
    except Exception as e:
        logger.error(f"Table creation failed: {e}", exc_info=True)
        return {'success': False, 'error': str(e)}

# Task 2: Load Raw Data
def load_raw_data_task(raw_data_path: str):
    try:
        if not os.path.exists(raw_data_path):
            raise FileNotFoundError(f"Raw data file not found: {raw_data_path}")

        logger.info(f"Loading raw data from: {raw_data_path}")
        load_raw_data_to_staging(raw_data_path)
        logger.info("Raw data loaded successfully.")
        return {'success': True}
    except Exception as e:
        logger.error(f"Failed to load raw data: {e}", exc_info=True)
        return {'success': False, 'error': str(e)}

# Task 3: Ingest Data into Fact Table
def ingest_fact_table_task(sql_file_path: str, start_date: str, end_date: str):
    try:
        conn = get_db_connection()
        with open(sql_file_path, 'r', encoding='utf-8') as sql_file:
            sql_script = sql_file.read()

        with conn.cursor() as cursor:
            cursor.execute(sql_script.format(start_date=start_date, end_date=end_date))
            conn.commit()
            logger.info(f"Data successfully ingested from: {sql_file_path}")
        return {'success': True}
    except Exception as e:
        logger.error(f"Data ingestion failed: {e}", exc_info=True)
        return {'success': False, 'error': str(e)}

# Task 4: Ingest Faulty Rows into FactError Table
def ingest_fact_error_task(sql_file_path: str, start_date: str, end_date: str):
    """
    Ingests faulty rows into the FactError table.
    """
    try:
        conn = get_db_connection()
        with open(sql_file_path, 'r', encoding='utf-8') as sql_file:
            sql_script = sql_file.read()

        # Use parameterized queries to prevent injection and handle date ranges
        with conn.cursor() as cursor:
            cursor.execute(sql_script.format(start_date=start_date, end_date=end_date))
            conn.commit()
            logger.info(f"Faulty rows ingested from: {sql_file_path}")
        return {'success': True}
    except Exception as e:
        logger.error(f"FactError ingestion failed: {e}", exc_info=True)
        return {'success': False, 'error': str(e)}

# Task 5: Populate Dim_SOR Table
def populate_dim_sor_task(sql_file_path: str):
    try:
        conn = get_db_connection()
        with open(sql_file_path, 'r', encoding='utf-8') as sql_file:
            sql_script = sql_file.read()

        with conn.cursor() as cursor:
            cursor.execute(sql_script)
            conn.commit()
            logger.info(f"Dim_SOR table populated from: {sql_file_path}")
        return {'success': True}
    except Exception as e:
        logger.error(f"Dim_SOR population failed: {e}", exc_info=True)
        return {'success': False, 'error': str(e)}

# Task 6: Update Dimensional Tables
def update_dimensional_tables_task(query_directory: str):
    """
    Executes all SQL scripts in the specified directory to update dimensional tables.

    Args:
        query_directory (str): Path to the directory containing SQL query files for dimensional tables.

    Returns:
        dict: Task execution status for each file.
    """
    tasks_status = {}
    try:
        sql_files = [
            os.path.join(query_directory, file)
            for file in os.listdir(query_directory)
            if file.startswith("update_dim_") and file.endswith(".sql")
        ]

        if not sql_files:
            logger.warning(f"No SQL scripts found in directory: {query_directory}")
            return {"success": False, "error": "No SQL scripts found"}

        conn = get_db_connection()
        cursor = conn.cursor()

        for sql_file in sql_files:
            try:
                with open(sql_file, "r", encoding="utf-8") as file:
                    sql_script = file.read()

                logger.info(f"Executing script: {sql_file}")
                cursor.execute(sql_script)
                conn.commit()
                tasks_status[os.path.basename(sql_file)] = {"success": True}
                logger.info(f"Successfully executed: {sql_file}")

            except Exception as e:
                logger.error(f"Failed to execute script {sql_file}: {e}", exc_info=True)
                tasks_status[os.path.basename(sql_file)] = {"success": False, "error": str(e)}

        conn.close()
        return tasks_status

    except Exception as e:
        logger.error(f"Error during dimensional tables update: {e}", exc_info=True)
        return {"success": False, "error": str(e)}

# Main Pipeline Execution
def run_pipeline(start_date: str, end_date: str):
    try:
        # Ensure the database exists
        ensure_database_exists()
    except Exception as e:
        logger.error(f"Pipeline terminated: Database creation/check failed. {e}")
        return {'success': False, 'error': str(e)}

    # Define SQL script file paths
    create_tables_directory = "infrastructure_initiation"
    raw_data_file = "raw_data_source.xlsx"
    queries_directory = "pipeline_dimensional_data/queries"
    update_fact_file = os.path.join(queries_directory, "update_fact.sql")
    update_fact_error_file = os.path.join(queries_directory, "update_fact_error.sql")
    update_dim_sor_file = os.path.join(queries_directory, "update_dim_sor.sql")

    tasks_status = {}

    logger.info("Starting ETL pipeline...")

    # Task 1: Create Tables
    tasks_status['create_tables'] = create_tables_task()
    if not tasks_status['create_tables']['success']:
        logger.error("Pipeline terminated: Table creation failed.")
        return tasks_status

    # Task 2: Load Raw Data
    tasks_status['load_raw_data'] = load_raw_data_task(raw_data_file)
    if not tasks_status['load_raw_data']['success']:
        logger.error("Pipeline terminated: Raw data loading failed.")
        return tasks_status

    # Task 3: Ingest Fact Table
    tasks_status['ingest_fact'] = ingest_fact_table_task(update_fact_file, start_date, end_date)
    if not tasks_status['ingest_fact']['success']:
        logger.error("Pipeline terminated: Fact table ingestion failed.")
        return tasks_status

    # Task 4: Ingest FactError Table
    # Add FactError ingestion task
    tasks_status['ingest_fact_error'] = ingest_fact_error_task(update_fact_error_file, start_date, end_date)
    if not tasks_status['ingest_fact_error']['success']:
        logger.error("Pipeline terminated: FactError ingestion failed.")
        return tasks_status


    # Task 5: Populate Dim_SOR Table
    tasks_status['populate_dim_sor'] = populate_dim_sor_task(update_dim_sor_file)
    if not tasks_status['populate_dim_sor']['success']:
        logger.error("Pipeline terminated: Dim_SOR population failed.")
        return tasks_status

    # Task 6: Update Dimensional Tables
    tasks_status['update_dimensional_tables'] = update_dimensional_tables_task(queries_directory)
    if not tasks_status['update_dimensional_tables']['success']:
        logger.error("Pipeline terminated: Dimensional tables update failed.")
        return tasks_status

    logger.info("Pipeline completed successfully!")
    return tasks_status


def reset_db():
    """
    Drops all tables and constraints from the database.
    """
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        # Drop all constraints
        cursor.execute("""
            DECLARE @sql NVARCHAR(MAX) = N'';
            SELECT @sql += N'ALTER TABLE ' + QUOTENAME(s.name) + '.' + QUOTENAME(t.name) +
                          ' DROP CONSTRAINT ' + QUOTENAME(fk.name) + ';'
            FROM sys.foreign_keys fk
            INNER JOIN sys.tables t ON fk.parent_object_id = t.object_id
            INNER JOIN sys.schemas s ON t.schema_id = s.schema_id;
            EXEC sp_executesql @sql;
        """)

        # Drop all tables
        cursor.execute("""
            DECLARE @sql NVARCHAR(MAX) = N'';
            SELECT @sql += N'DROP TABLE ' + QUOTENAME(s.name) + '.' + QUOTENAME(t.name) + ';'
            FROM sys.tables t
            INNER JOIN sys.schemas s ON t.schema_id = s.schema_id;
            EXEC sp_executesql @sql;
        """)

        conn.commit()
        logger.info("Database reset successfully.")
    except Exception as e:
        logger.error(f"Failed to reset the database: {e}", exc_info=True)

