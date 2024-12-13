from utils import get_db_config, execute_sql_script_from_file
import pyodbc
from loggings import logger


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
        logger.info(f"Tables created successfully from: {sql_file_path}")
        return {'success': True}
    except Exception as e:
        logger.error(f"Table creation failed: {e}", exc_info=True)
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
            logger.info(f"Data successfully ingested from: {sql_file_path}")
        return {'success': True}
    except Exception as e:
        logger.error(f"Data ingestion failed: {e}", exc_info=True)
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
            logger.info(f"Faulty rows ingested from: {sql_file_path}")
        return {'success': True}
    except Exception as e:
        logger.error(f"FactError ingestion failed: {e}", exc_info=True)
        return {'success': False, 'error': str(e)}


# Main Pipeline Execution
def run_pipeline(start_date: str, end_date: str):

    # Define SQL file paths
    create_tables_file = "infrastructure_initiation/dimensional_db_table_creation.sql"
    update_fact_file = "pipeline_dimensional_data/queries/update_fact.sql"
    update_fact_error_file = "pipeline_dimensional_data/queries/update_fact_error.sql"

    tasks_status = {}

    logger.info("Starting ETL pipeline...")

    tasks_status['create_tables'] = create_tables_task(create_tables_file)
    if not tasks_status['create_tables']['success']:
        logger.error("Pipeline terminated: Table creation failed.")
        return tasks_status

    tasks_status['ingest_fact'] = ingest_fact_table_task(update_fact_file, start_date, end_date)
    if not tasks_status['ingest_fact']['success']:
        logger.error("Pipeline terminated: Fact table ingestion failed.")
        return tasks_status

    tasks_status['ingest_fact_error'] = ingest_fact_error_task(update_fact_error_file, start_date, end_date)
    if not tasks_status['ingest_fact_error']['success']:
        logger.error("Pipeline terminated: FactError ingestion failed.")
        return tasks_status

    logger.info("Pipeline completed successfully!")
    return tasks_status


def reset_db():
    """
    Drops all tables in the database, resetting it completely.
    """
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        cursor.execute("""
                DECLARE @sql NVARCHAR(MAX) = N'';
                SELECT @sql += N'ALTER TABLE ' + QUOTENAME(s.name) + '.' + QUOTENAME(t.name) + 
                              ' DROP CONSTRAINT ' + QUOTENAME(fk.name) + ';'
                FROM sys.foreign_keys fk
                INNER JOIN sys.tables t ON fk.parent_object_id = t.object_id
                INNER JOIN sys.schemas s ON t.schema_id = s.schema_id;
                EXEC sp_executesql @sql;
                """)
        cursor.execute("""
                DECLARE @sql NVARCHAR(MAX) = N'';
                SELECT @sql += N'DROP TABLE ' + QUOTENAME(s.name) + '.' + QUOTENAME(t.name) + ';'
                FROM sys.tables t
                INNER JOIN sys.schemas s ON t.schema_id = s.schema_id;
                EXEC sp_executesql @sql;
                """)
        conn.commit()
        print("All tables and constraints dropped successfully.")
        return {'success': True}
    except Exception as e:
        print(f"Failed to drop database tables: {e}")
        return {'success': False, 'error': str(e)}