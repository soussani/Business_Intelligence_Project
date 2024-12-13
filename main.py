import sys
import os

# SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
# sys.path.append(os.path.dirname(SCRIPT_DIR))
from pipeline_dimensional_data.flow import DimensionalDataFlow

def main():
    """
    Main function to execute the dimensional data pipeline.
    """
    # Define start and end dates for the data flow execution
    start_date = "2023-01-01"
    end_date = "2024-12-31"

    # Create an instance of DimensionalDataFlow
    data_flow = DimensionalDataFlow()

    # Execute the ETL pipeline
    pipeline_status = data_flow.exec(start_date, end_date)

    # Print the final execution status
    print("\nExecution completed!")
    print("Final Pipeline Status:", pipeline_status)


if __name__ == "__main__":
    main()
