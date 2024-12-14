import pyodbc
import uuid
import pandas as pd
from loggings import logger
from pipeline_dimensional_data.config_db import get_db_config, get_db_connection, ensure_database_exists
import numpy as np
from decimal import Decimal

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
    ensure_database_exists()
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


def execute_sql_inserts(df, table_name, conn):
    cursor = conn.cursor()
    for index, row in df.iterrows():
        try:
            cleaned_row = []
            for col, x, dtype in zip(df.columns, row, df.dtypes):
                if table_name == "Staging_Customers" and col == "CustomerID":
                    cleaned_row.append(str(x).strip() if pd.notnull(x) else None)
                elif table_name == "Staging_Employees" and col in ["EmployeeID", "ReportsTo"]:
                    cleaned_row.append(int(x) if pd.notnull(x) else None)
                elif table_name == "Staging_Employees" and col in ["BirthDate", "HireDate"]:
                    cleaned_row.append(pd.to_datetime(x, errors='coerce') if pd.notnull(x) else None)
                elif table_name == "Staging_OrderDetails" and col in ["OrderID", "ProductID", "Quantity"]:
                    cleaned_row.append(int(x) if pd.notnull(x) else None)
                elif table_name == "Staging_Orders" and col in ["OrderDate", "ShippedDate", "RequiredDate"]:
                    cleaned_row.append(pd.to_datetime(x, errors='coerce') if pd.notnull(x) else None)
                elif dtype.kind == 'i':
                    cleaned_row.append(int(x) if pd.notnull(x) else None)
                elif dtype.kind == 'f':
                    cleaned_row.append(Decimal(str(x)).quantize(Decimal("0.00")) if pd.notnull(x) else None)
                elif dtype.kind == 'O':
                    cleaned_row.append(str(x).strip() if pd.notnull(x) else None)
                elif table_name == "Staging_Categories" and col == "CategoryID":
                    cleaned_row.append(int(x) if pd.notnull(x) else None)
                elif table_name == "Staging_Categories" and col in ["CategoryName", "Description"]:
                    cleaned_row.append(str(x).strip() if pd.notnull(x) else None)

                else:
                    cleaned_row.append(None)
            placeholders = ", ".join(["?" for _ in cleaned_row])
            insert_query = f"INSERT INTO {table_name} ({', '.join(df.columns)}) VALUES ({placeholders})"
            cursor.execute(insert_query, tuple(cleaned_row))
        except Exception as e:
            logger.error(f"Failed to insert row {index} into {table_name}: {e}", exc_info=True)
    conn.commit()
    logger.info(f"Inserted {len(df)} rows into {table_name}.")

#Loading raw data in db
def load_raw_data_to_staging(raw_data_path: str):
    """
    Loads raw data from an Excel file into staging tables in the database.
    """
    conn = get_db_connection()
    try:
        # Load data from Excel sheets
        df_products = pd.read_excel(raw_data_path, sheet_name='Products')
        df_region = pd.read_excel(raw_data_path, sheet_name='Region')
        df_shippers = pd.read_excel(raw_data_path, sheet_name='Shippers')
        df_suppliers = pd.read_excel(raw_data_path, sheet_name='Suppliers')
        df_suppliers.sort_values(by='Phone', ascending=True, na_position='first', inplace=True)
        df_suppliers['Phone'] = df_suppliers['Phone'].astype(str)
        df_suppliers.replace({np.nan: None, np.inf: None, -np.inf: None}, inplace=True)
        df_territories = pd.read_excel(raw_data_path, sheet_name='Territories')
        df_orders = pd.read_excel(raw_data_path, sheet_name='Orders')
        df_customers = pd.read_excel(raw_data_path, sheet_name='Customers')
        df_employees = pd.read_excel(raw_data_path, sheet_name='Employees')
        df_order_details = pd.read_excel(raw_data_path, sheet_name='OrderDetails')
        df_categories = pd.read_excel(raw_data_path, sheet_name='Categories')

        # Insert data into respective tables
        execute_sql_inserts(df_products, 'Staging_Products', conn)
        execute_sql_inserts(df_region, 'Staging_Region', conn)
        execute_sql_inserts(df_shippers, 'Staging_Shippers', conn)
        execute_sql_inserts(df_suppliers, 'Staging_Suppliers', conn)
        execute_sql_inserts(df_territories, 'Staging_Territories', conn)
        execute_sql_inserts(df_orders, 'Staging_Orders', conn)
        execute_sql_inserts(df_customers, 'Staging_Customers', conn)
        execute_sql_inserts(df_employees, 'Staging_Employees', conn)
        execute_sql_inserts(df_order_details, 'Staging_OrderDetails', conn)
        execute_sql_inserts(df_categories, 'Staging_Categories', conn)

    except Exception as e:
        logger.error(f"Error loading raw data to staging: {e}", exc_info=True)
    finally:
        if conn:
            conn.close()
            logger.info("Database connection closed.")