# Business Intelligence Project


## Project OverviewDimensional Data Pipeline Project Documentation


## Overview

This project implements an end-to-end ETL (Extract, Transform, Load) pipeline to process raw data from staging tables into dimensional tables. The pipeline is designed for SQL Server and includes functionality to reset databases, load raw data, execute table creation scripts, and run ETL workflows.

---

## Features

- Automatic table creation
- Fact table and error table ingestion
- Table reset on every pipeline execution
- Logging with `loguru`

---

## Execution Workflow

Reset the Database: Clears all existing data and structures.
Create Tables: Executes SQL scripts to create staging and dimensional tables.
Load Raw Data: Processes and loads data from Excel sheets into staging tables.
Run ETL Workflow: Transforms data from staging tables to dimensional tables and populates fact tables.

--- 

## Setup Instructions

### Clone the Repository

```bash
git clone https://github.com/yourusername/Business_Intelligence_Project.git
cd Business_Intelligence_Project
```

## Create a Virtual Environment

```bash
python -m venv venv
source venv/bin/activate  # On Linux/Mac
venv\Scripts\activate   # On Windows
```

## Install Dependencies

```bash
pip install -r requirements.txt

```

## Configure Database Credentials

### *1.  Copy the template:*
   ```bash
   cp sql_server_config_template.cfg sql_server_config.cfg
```

### *2.   Open sql_server_config.cfg and fill in your credentials:*
```bash
[SQL_SERVER]
driver={ODBC Driver 18 for SQL Server}
server=your_server_address
database=your_database_name
user=your_username
password=your_password
```

## Run the application

```bash
python main.py --start_date="YYYY-MM-DD" --end_date="YYYY-MM-DD"
```

## Logging

Logs are generated using the loguru library. Relevant logs include:

* INFO: Successful steps.
* DEBUG: SQL execution details.
* ERROR: Issues encountered during execution.

## Troubleshooting

1. Database Connection Issues:
2. Ensure the SQL Server is running and accessible.
3. Verify credentials in sql_server_config.cfg.
4. Missing Packages:
* Run pip install -r requirements.txt to install dependencies.
5. Permission Errors:
* Ensure your database user has the required permissions.

## Advanced Features

- Parameterized Queries
SQL queries are designed with parameterization to enhance security and flexibility.

- 
Modular Design
The project is modular, separating concerns into distinct modules:

flow.py: Manages ETL workflows.
tasks.py: Handles database reset logic.
utils.py: Provides utility functions for SQL execution and data processing.

## Contributors 🎉

Meet the awesome team behind this project:

- Ina Karapetyan 
- Hrag Sousaani
- Elen Galoyan
- Gor Yeghiazaryan
- Lilit Ivanyan