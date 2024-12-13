import pyodbc

def test_db_query(connection_string, query):
    """
    Executes a SELECT query and prints the results.

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
                # Print column names
                columns = [column[0] for column in cursor.description]
                print(" | ".join(columns))
                print("-" * 50)
                # Print each row
                for row in rows:
                    print(" | ".join(map(str, row)))
    except pyodbc.Error as e:
        print(f"Database query failed: {str(e)}")
    except Exception as e:
        print(f"An unexpected error occurred: {str(e)}")

if __name__ == "__main__":
    # Define your database connection string
    connection_string = (
        "Driver={ODBC Driver 18 for SQL Server};"
        "Server=localhost,1433;"
        "Database=ORDER_DDS;"
        "UID=sa;"
        "PWD=HragS123;"
        "TrustServerCertificate=yes;"
    )

    # Define a test query
    test_query = "SELECT * FROM dbo.FactOrders;"

    # Execute the test function
    test_db_query(connection_string, test_query)
