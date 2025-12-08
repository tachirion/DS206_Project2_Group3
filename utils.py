import configparser
from typing import Dict, Any
import os


def get_sql_config(filename: str, database: str) -> Dict[str, Any]:
    """
    Reads SQL configuration details from a `.cfg` file and returns the database connection parameters as a dictionary.

    Args:
        filename (str): The path to the `.cfg` configuration file.
        database (str): The name of the database section in the configuration file.

    Returns:
        Dict[str, Any]: A dictionary containing the database connection parameters.

    Raises:
        ValueError: If the driver is unsupported or missing in the configuration file.

    Example `.cfg` file:
        [Database1]
        Driver={ODBC Driver 17 for SQL Server}
        Server=DESKTOP-I2N8O9P
        Database=Orders_ER
        Trusted_Connection=yes

        [Database2]
        Driver={ODBC Driver 18 for SQL Server}
        Server=DESKTOP-I2N8O9P
        Database=Orders_ER
        Trusted_Connection=yes
        Encrypt=no

        [Database3]
        Driver={ODBC Driver 18 for SQL Server}
        Server=DESKTOP-I2N8O9P
        Database=Orders_ER
        Trusted_Connection=yes
        Encrypt=yes
        TrustServerCertificate=yes
    """
    cf = configparser.ConfigParser()
    cf.read(filename)

    config = {
        "Driver": cf.get(database, "Driver"),
        "Server": cf.get(database, "Server"),
        "Database": cf.get(database, "Database"),
        "Trusted_Connection": cf.get(database, "Trusted_Connection"),
    }

    if config["Driver"] == "{ODBC Driver 18 for SQL Server}":
        config["Encrypt"] = cf.get(database, "Encrypt")
        if cf.has_option(database, "TrustServerCertificate"):
            config["TrustServerCertificate"] = cf.get(database, "TrustServerCertificate")
    elif config["Driver"] != "{ODBC Driver 17 for SQL Server}":
        raise ValueError(f"Unsupported driver: {config['Driver']}")

    return config


def create_connection_string(config: Dict[str, str]) -> str:
    """
    Generates a SQL Server connection string from the provided configuration dictionary.

    Args:
        config (Dict[str, str]): A dictionary containing SQL Server connection parameters.
            Expected keys: 'Driver', 'Server', 'Database', 'Trusted_Connection'.
            Optional keys: 'Encrypt', 'TrustServerCertificate'.

    Returns:
        str: The formatted SQL Server connection string.
    """
    conn_str = (
        f"Driver={config['Driver']};"
        f"Server={config['Server']};"
        f"Database={config['Database']};"
        f"Trusted_Connection={config['Trusted_Connection']};"
    )

    if "Encrypt" in config:
        conn_str += f"Encrypt={config['Encrypt']};"
    if "TrustServerCertificate" in config:
        conn_str += f"TrustServerCertificate={config['TrustServerCertificate']};"

    return conn_str


def extract_tables_db(cursor, *args):
    results = []
    for x in cursor.execute('exec sp_tables'):
        if x[1] not in args:
            results.append(x[2])
    return results


def extract_table_cols(cursor, table_name):
    result = []
    for row in cursor.columns(table=table_name):
        result.append(row.column_name)
    return result


def find_primary_key(cursor, table_name, schema):
    table_primary_key = cursor.primaryKeys(table_name, schema=schema)
    columns = [column[0] for column in cursor.description]
    results = []
    for row in cursor.fetchall():
        results.append(dict(zip(columns, row)))
    try:
        return results[0]
    except:
        pass
    return results


def load_query(query_name, input_dir):
    for script in os.listdir(input_dir):
        if query_name in script:
            with open(input_dir + '\\' + script, 'r') as script_file:
                sql_script = script_file.read()
            break
    return sql_script
    