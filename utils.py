# If configparser is not installed, run: pip install configparser
import configparser
import pyodbc
import uuid


# Generate unique UUID for task execution tracking
def generate_uuid() -> str:
    """
    Generates a unique UUID string.
    Returns:
        str: A unique UUID string.
    """
    return str(uuid.uuid4())


# Database configuration loader
def get_db_config(config_file='./sql_server_config.cfg'):
    """
    Reads database configuration from a config file.

    Args:
        config_file (str): Path to the configuration file.

    Returns:
        dict: Database connection parameters.
    """
# def get_db_config(config_file='./sql_server_config.cfg'):
    config = configparser.ConfigParser()
    config.read(config_file)
    print('Config Sections:', config.sections())  # Debug to check available sections
    if 'SQL_SERVER' not in config:
        raise KeyError("'SQL_SERVER' section not found in the configuration file.")
    db_config = {
        'driver': config['SQL_SERVER']['driver'],
        'server': config['SQL_SERVER']['server'],
        'database': config['SQL_SERVER']['database'],
        'user': config['SQL_SERVER']['user'],
        'password': config['SQL_SERVER']['password']
    }
    print('Parsed Config:', db_config)  # Debug to check parsed values
    return db_config


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
    # Load database configuration
    db_config = get_db_config(config_file)

    # Create connection string
    connection_string = (
        f"Driver={db_config['driver']};"
        f"Server={db_config['server']};"
        f"Database={db_config['database']};"
        f"UID={db_config['user']};"
        f"PWD={db_config['password']};"
        f"TrustServerCertificate=yes;"
    )

    try:
        print("Connecting to the database...")
        with pyodbc.connect(connection_string, autocommit=True) as conn:
            print("Connection established successfully.")

            # Read the SQL file
            with open(file_path, 'r', encoding='utf-8') as sql_file:
                sql_script = sql_file.read()
                print(f"Loaded SQL script from: {file_path}")

            # Split SQL script into individual statements
            sql_statements = sql_script.split(';')
            print(f"SQL script contains {len(sql_statements)} statements.")

            # Execute each statement
            with conn.cursor() as cursor:
                for i, statement in enumerate(sql_statements):
                    statement = statement.strip()
                    if statement:  # Skip empty statements
                        print(f"\nExecuting statement {i + 1}: {statement[:50]}...")
                        try:
                            cursor.execute(statement)
                            print(f"Statement {i + 1} executed successfully.")
                        except pyodbc.Error as e:
                            print(f"Error executing statement {i + 1}: {str(e)}")
            print(f"All statements executed successfully from: {file_path}")

    except FileNotFoundError:
        print(f"File not found: {file_path}")
    except pyodbc.Error as e:
        print(f"Database connection or execution failed: {str(e)}")
    except Exception as e:
        print(f"An unexpected error occurred: {str(e)}")

# Example usage (for testing only)
if __name__ == "__main__":
    # Example SQL file path
    sql_file_path = "pipeline_dimensional_data/queries/update_fact.sql"

    # Execute the script using config file
    execute_sql_script_from_file(sql_file_path)
