import configparser
import os
import pyodbc
from loggings import logger


# Database configuration loader
def get_db_config(config_file='./sql_server_config.cfg'):
    """
    Reads database configuration from a config file.

    Args:
        config_file (str): Path to the configuration file.

    Returns:
        dict: Database connection parameters.
    """
    config = configparser.ConfigParser()
    config.read(config_file)
    if 'SQL_SERVER' not in config:
        raise KeyError("'SQL_SERVER' section not found in the configuration file.")
    db_config = {
        'driver': config['SQL_SERVER']['driver'],
        'server': config['SQL_SERVER']['server'],
        'database': config['SQL_SERVER']['database'],
        'user': config['SQL_SERVER']['user'],
        'password': config['SQL_SERVER']['password']
    }
    return db_config


# Database connection handler
def get_db_connection(default_db=None):
    """
    Establishes a database connection using config.
    Args:
        default_db (str): Optional default database to connect to. If not specified, connects to master.
    """
    db_config = get_db_config()
    database = default_db or db_config['database']
    connection_string = (
        f"Driver={db_config['driver']};"
        f"Server={db_config['server']};"
        f"Database={database};"
        f"UID={db_config['user']};"
        f"PWD={db_config['password']};"
        f"TrustServerCertificate=yes;"
    )
    return pyodbc.connect(connection_string)


# Database existence checker / creator
def ensure_database_exists(db_creation_sql_path='infrastructure_initiation/dimensional_db_creation.sql'):
    """
    Ensures the database exists by executing a SQL script.
    """
    db_config = get_db_config()

    connection_string = (
        f"Driver={db_config['driver']};"
        f"Server={db_config['server']};"
        f"Database=master;"
        f"UID={db_config['user']};"
        f"PWD={db_config['password']};"
        f"TrustServerCertificate=yes;"
    )

    if not os.path.exists(db_creation_sql_path):
        raise FileNotFoundError(f"Database creation SQL file not found: {db_creation_sql_path}")

    try:
        with pyodbc.connect(connection_string, autocommit=True) as conn:
            cursor = conn.cursor()

            with open(db_creation_sql_path, 'r', encoding='utf-8') as sql_file:
                sql_script = sql_file.read()

            cursor.execute(sql_script)

    except pyodbc.Error as e:
        logger.error(f"Error during database creation: {e}")
        raise