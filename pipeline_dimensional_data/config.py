import configparser

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