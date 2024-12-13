import pyodbc
import uuid
import pandas as pd
from loggings import logger
from pipeline_dimensional_data.config_db import get_db_config, get_db_connection, ensure_database_exists

# Generate unique UUID for task execution tracking
def generate_uuid() -> str:
    """
    Generates a unique UUID string.
    Returns:
        str: A unique UUID string.
    """
    return str(uuid.uuid4())

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

# SQL execution utility
def execute_sql_script_from_file(file_path: str, config_file='sql_server_config.cfg'):
    """
    Reads an SQL script from a file and executes it using a database connection.

    Args:
        file_path (str): Path to the .sql file.
        config_file (str): Path to the configuration file.

    Returns:
        None: Executes the SQL script without returning a result.
    """
    ensure_database_exists()

    db_config = get_db_config(config_file)

    connection_string = (
        f"Driver={db_config['driver']};"
        f"Server={db_config['server']};"
        f"Database={db_config['database']};"
        f"UID={db_config['user']};"
        f"PWD={db_config['password']};"
        f"TrustServerCertificate=yes;"
    )

    try:
        logger.info("Connecting to the database...")
        with pyodbc.connect(connection_string, autocommit=True) as conn:
            logger.info("Connection established successfully.")

            with open(file_path, 'r', encoding='utf-8') as sql_file:
                sql_script = sql_file.read()
                logger.info(f"Loaded SQL script from: {file_path}")

            sql_statements = sql_script.split(';')
            logger.info(f"SQL script contains {len(sql_statements)} statements.")

            with conn.cursor() as cursor:
                for i, statement in enumerate(sql_statements):
                    statement = statement.strip()
                    if statement:  # Skip empty statements
                        logger.debug(f"Executing statement {i + 1}: {statement[:50]}...")
                        try:
                            cursor.execute(statement)
                            logger.info(f"Statement {i + 1} executed successfully.")
                        except pyodbc.Error as e:
                            logger.error(f"Error executing statement {i + 1}: {str(e)}", exc_info=True)

            logger.info(f"All statements executed successfully from: {file_path}")

    except FileNotFoundError:
        logger.error(f"File not found: {file_path}", exc_info=True)
    except pyodbc.Error as e:
        logger.error(f"Database connection or execution failed: {str(e)}", exc_info=True)
    except Exception as e:
        logger.error(f"An unexpected error occurred: {str(e)}", exc_info=True)

# Data cleaning utility
def clean_customers_data(df):
    """
    Cleans the Customers data before insertion into the database.
    Args:
        df (pd.DataFrame): DataFrame containing Customers data.

    Returns:
        pd.DataFrame: Cleaned DataFrame.
    """
    if 'Region' in df.columns:
        df['Region'] = df['Region'].fillna("Unknown")
    
    if 'Phone' in df.columns:
        df['Phone'] = df['Phone'].astype(str)
    if 'Fax' in df.columns:
        df['Fax'] = df['Fax'].astype(str)
    
    return df

# Data loading utility
def load_raw_data_to_staging(raw_data_path: str):
    """
    Loads raw data from an Excel file into staging tables in the database.
    """
    try:
        connection_string = (
            f"Driver={{ODBC Driver 18 for SQL Server}};"
            f"Server=localhost,1433;"
            f"Database=ORDER_DDS;"
            f"UID=sa;"
            f"PWD=InaKarapetyan123;"
            f"TrustServerCertificate=yes;"
        )

        conn = pyodbc.connect(connection_string)
        cursor = conn.cursor()
        logger.info("Connected to the database.")

        excel_data = pd.ExcelFile(raw_data_path)
        sheet_names = excel_data.sheet_names

        table_mappings = {
            "Categories": "StagingCategories",
            "Customers": "StagingCustomers",
            "Employees": "StagingEmployees",
            "Products": "StagingProducts",
            "Region": "StagingRegion",
            "Shippers": "StagingShippers",
            "Suppliers": "StagingSuppliers",
            "Territories": "StagingTerritories",
            "Orders": "StagingOrders",
            "Order Details": "StagingOrderDetails",
        }

        for sheet_name, table_name in table_mappings.items():
            if sheet_name in sheet_names:
                logger.info(f"Loading data from sheet: {sheet_name} into table: {table_name}")
                df = excel_data.parse(sheet_name)

                df.columns = [col.strip().replace(" ", "_") for col in df.columns]

                if table_name == "StagingCustomers":
                    df = clean_customers_data(df)

                for _, row in df.iterrows():
                    placeholders = ", ".join(["?" for _ in row])
                    insert_query = f"INSERT INTO {table_name} ({', '.join(df.columns)}) VALUES ({placeholders})"
                    cursor.execute(insert_query, tuple(row))

                conn.commit()
                logger.info(f"Data successfully loaded into {table_name}.")
                logger.info(f"{table_name} has {len(df)} rows after insertion.")
            else:
                logger.warning(f"Sheet {sheet_name} not found in the Excel file.")

    except Exception as e:
        logger.error(f"Error loading raw data to staging: {e}", exc_info=True)
    finally:
        if conn:
            conn.close()
            logger.info("Database connection closed.")
