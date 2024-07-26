import sqlite3
import logging
import argparse
from clickhouse_driver import Client
from datetime import datetime
from typing import List, Dict, Any, Optional

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# ClickHouse settings
CLICKHOUSE_SETTINGS = {
    "input_format_null_as_default": True,
    "max_insert_block_size": 1000000,
    "max_threads": 8,
}

def parse_datetime(value: str) -> Optional[datetime]:
    """Parse a string to a datetime object, handling fractional seconds."""
    if not value or not value.strip():
        return None
    value = value.split(".")[0]  # Remove fractional seconds
    try:
        return datetime.strptime(value.strip(), "%Y-%m-%d %H:%M:%S")
    except ValueError:
        logging.warning(f"Failed to parse DateTime: {value}")
        return None

def parse_date(value: str) -> Optional[datetime.date]:
    """Parse a string to a date object."""
    if not value or not value.strip():
        return None
    try:
        return datetime.strptime(value.strip(), "%Y-%m-%d").date()
    except ValueError:
        logging.warning(f"Failed to parse Date: {value}")
        return None

def infer_clickhouse_type(value: Any) -> str:
    """Infer the ClickHouse type based on the value."""
    if isinstance(value, bool):
        return "Boolean"
    if isinstance(value, int):
        if value < 0:
            return "Int32"
        elif value <= 4294967295:  # Max for UInt32
            return "UInt32"
        else:
            return "UInt64"
    if isinstance(value, float):
        return "Float64"
    if isinstance(value, str):
        return "String"
    return "String"  # Default to String for unknown types

def create_clickhouse_table(clickhouse_client: Client, table_name: str, clickhouse_columns: List[str], primary_key: str, clickhouse_database: str) -> None:
    """Create a ClickHouse table based on the provided schema."""
    try:
        clickhouse_client.execute(f"DESCRIBE TABLE {clickhouse_database}.{table_name}")
        logging.info("Table %s already exists in ClickHouse.", table_name)
        return
    except Exception:
        logging.info("Table %s does not exist. Creating a new table.", table_name)

    engine = "ReplacingMergeTree"
    order_by = f"ORDER BY {primary_key if primary_key else 'tuple()'}"
    create_table_query = (
        f"CREATE TABLE IF NOT EXISTS {clickhouse_database}.{table_name} "
        f"({', '.join(clickhouse_columns)}) "
        f"ENGINE = {engine} {order_by}"
    )
    logging.debug(f"Creating table: {create_table_query}")
    clickhouse_client.execute(create_table_query)

def print_clickhouse_schema(clickhouse_client: Client, table_name: str, clickhouse_database: str) -> None:
    """Print the schema of the specified ClickHouse table."""
    schema_query = f"DESCRIBE TABLE {clickhouse_database}.{table_name}"
    schema = clickhouse_client.execute(schema_query)
    logging.info("ClickHouse schema %s: %s", table_name, ", ".join([f"{col[0]} -> {col[1]}" for col in schema]))

def prepare_row(row: tuple, clickhouse_column_names: List[str], column_types: Dict[str, str]) -> List[Any]:
    """Prepare a single row for insertion into ClickHouse."""
    prepared_row = []
    for name, value in zip(clickhouse_column_names, row):
        expected_type = column_types[name]
        if expected_type == "Int64":
            prepared_row.append(int(value or 0))
        elif expected_type == "UInt32":
            prepared_row.append(int(value or 0) & 0xFFFFFFFF)  # Ensure it's within UInt32 range
        elif expected_type == "UInt64":
            prepared_row.append(int(value or 0) & 0xFFFFFFFFFFFFFFFF)  # Ensure it's within UInt64 range
        elif expected_type == "Float64":
            prepared_row.append(float(value or 0))
        elif expected_type == "String":
            prepared_row.append(str(value))
        elif expected_type == "DateTime":
            prepared_row.append(parse_datetime(value))
        elif expected_type == "Date":
            prepared_row.append(parse_date(value))
        elif expected_type == "Boolean":
            prepared_row.append(bool(value))
        else:
            prepared_row.append(value)  # Default case
    return prepared_row

def fetch_and_prepare_rows(sqlite_cursor: sqlite3.Cursor, clickhouse_column_names: List[str], column_types: Dict[str, str], chunk_size: int) -> List[List[Any]]:
    """Fetch rows from SQLite and prepare them for insertion into ClickHouse."""
    prepared_rows = []
    while True:
        rows = sqlite_cursor.fetchmany(chunk_size)
        if not rows:
            break
        for row in rows:
            prepared_rows.append(prepare_row(row, clickhouse_column_names, column_types))
        yield prepared_rows
        prepared_rows = []

def infer_column_types(sqlite_cursor: sqlite3.Cursor, table_name: str) -> Dict[str, str]:
    """Infer column types from the SQLite table schema."""
    sqlite_cursor.execute(f"PRAGMA table_info({table_name})")
    columns = sqlite_cursor.fetchall()
    column_types = {}
    for col in columns:
        name, type_, _, _, is_primary_key = col[1], col[2], col[3], col[4], col[5]
        ch_type = {
            "INTEGER": "Int64",
            "INT": "Int64",
            "REAL": "Float64",
            "FLOAT": "Float64",
            "VARCHAR": "String",
            "TEXT": "String",
            "DATETIME": "DateTime",
            "DATE": "Date",
        }.get(type_.upper(), "String")  # Default to String for unknown types
        column_types[name] = ch_type
    return column_types

def sqlite_to_clickhouse(sqlite_db_path: str, clickhouse_host: str, clickhouse_port: int, clickhouse_user: str, clickhouse_password: str, clickhouse_database: str, chunk_size: int) -> None:
    """Transfer data from SQLite to ClickHouse."""
    with sqlite3.connect(sqlite_db_path) as sqlite_conn:
        logging.info("Connected to SQLite database: %s", sqlite_db_path)
        sqlite_cursor = sqlite_conn.cursor()

        sqlite_cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = sqlite_cursor.fetchall()

        clickhouse_client = Client(
            host=clickhouse_host,
            port=clickhouse_port,
            user=clickhouse_user,
            password=clickhouse_password,
            database=clickhouse_database,
            settings=CLICKHOUSE_SETTINGS,
        )
        logging.info("Connected to ClickHouse at %s:%s", clickhouse_host, clickhouse_port)

        for table in tables:
            table_name = table[0]
            logging.info("Processing table: %s", table_name)

            column_types = infer_column_types(sqlite_cursor, table_name)
            clickhouse_columns = [f"{name} {ch_type}" for name, ch_type in column_types.items()]

            create_clickhouse_table(clickhouse_client, table_name, clickhouse_columns, None, clickhouse_database)
            print_clickhouse_schema(clickhouse_client, table_name, clickhouse_database)

            sqlite_cursor.execute(f"SELECT * FROM {table_name}")
            column_names = list(column_types.keys())
            insert_query = f"INSERT INTO {clickhouse_database}.{table_name} ({', '.join(column_names)}) VALUES"

            total_rows = 0
            for prepared_rows in fetch_and_prepare_rows(sqlite_cursor, column_names, column_types, chunk_size):
                clickhouse_client.execute(insert_query, prepared_rows)
                total_rows += len(prepared_rows)
                logging.info("Inserted %d rows into %s", len(prepared_rows), table_name)

            logging.info("Total %d rows inserted into %s", total_rows, table_name)
            clickhouse_client.execute(f"OPTIMIZE TABLE {clickhouse_database}.{table_name}")
            logging.info("Optimized table %s in ClickHouse.", table_name)

        clickhouse_client.disconnect()
        logging.info("Conversion completed successfully!")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Transfer data from SQLite to ClickHouse.")
    parser.add_argument("--sqlite", required=True, help="Path to the SQLite database.")
    parser.add_argument("--clickhouse-host", required=True, help="ClickHouse host.")
    parser.add_argument("--clickhouse-port", type=int, default=9000, help="ClickHouse port.")
    parser.add_argument("--clickhouse-user", required=True, help="ClickHouse user.")
    parser.add_argument("--clickhouse-password", required=True, help="ClickHouse password.")
    parser.add_argument("--clickhouse-database", required=True, help="ClickHouse database name.")
    parser.add_argument("--chunk-size", type=int, default=10000, help="Chunk size for INSERT operations.")

    args = parser.parse_args()

    sqlite_to_clickhouse(
        sqlite_db_path=args.sqlite,
        clickhouse_host=args.clickhouse_host,
        clickhouse_port=args.clickhouse_port,
        clickhouse_user=args.clickhouse_user,
        clickhouse_password=args.clickhouse_password,
        clickhouse_database=args.clickhouse_database,
        chunk_size=args.chunk_size,
    )
