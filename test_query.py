import pyodbc
from loggings import logger

def test_db_query(connection_string, query):
    """
    Executes a SELECT query and logs the results.

    Args:
        connection_string (str): The database connection string.
        query (str): The SQL query to execute.

    Returns:
        None
    """
    try:
        with pyodbc.connect(connection_string) as conn:
            with conn.cursor() as cursor:
                cursor.execute(query)
                rows = cursor.fetchall()
                # Log column names
                columns = [column[0] for column in cursor.description]
                logger.info(f"Columns: {' | '.join(columns)}")
                logger.info("-" * 50)
                # Log each row
                for row in rows:
                    logger.debug(f"Row: {' | '.join(map(str, row))}")
    except pyodbc.Error as e:
        logger.error(f"Database query failed: {str(e)}", exc_info=True)
    except Exception as e:
        logger.error(f"An unexpected error occurred: {str(e)}", exc_info=True)

