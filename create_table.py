# create_table.py
import os
from dotenv import load_dotenv
from clickhouse_driver import Client
import logging

from sync_orchestrator import ENV_CH_HOST, ENV_CH_PASSWORD, ENV_LOG_LEVEL

# Configure basic logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Load environment variables
load_dotenv()

# Get table names from environment variables
DATA_TABLE_NAME = os.getenv("DATA_TABLE_NAME")
CONTROL_TABLE_NAME = os.getenv("CONTROL_TABLE_NAME")
CLICKHOUSE_HOST = os.getenv(ENV_CH_HOST)
CLICKHOUSE_PASSWORD = os.getenv(ENV_CH_PASSWORD)


def table_exists(client: Client, table_name: str) -> bool:
    """Checks if a table exists in ClickHouse."""
    try:
        client.execute(f"SELECT 1 FROM system.tables WHERE database = currentDatabase() AND name = '{table_name}'")
        return True
    except Exception as e:  # Catch connection errors, table not found, etc.
        logging.error(f"Error checking if table '{table_name}' exists: {e}")
        return False  # Assume it doesn't exist to avoid issues.



def create_tables(client: Client):
    """Creates the data and control tables if they don't exist."""

    DATA_TABLE_DDL = f"""
    CREATE TABLE IF NOT EXISTS default.{DATA_TABLE_NAME}
    (
        `Timestamp` DateTime CODEC(DoubleDelta, ZSTD(10)),
        `Source` LowCardinality(String) CODEC(ZSTD(1)),
        `Ticker` LowCardinality(String) CODEC(ZSTD(1)),
        `H` Float64 CODEC(Delta(8), ZSTD(10)),
        `L` Float64 CODEC(Delta(8), ZSTD(10))
    )
    ENGINE = ReplacingMergeTree
    PARTITION BY toYYYYMM(Timestamp)
    ORDER BY (Source, Ticker, Timestamp)
    SETTINGS index_granularity = 8192
    """

    CONTROL_TABLE_DDL = f"""
    CREATE TABLE IF NOT EXISTS default.{CONTROL_TABLE_NAME}
    (
        `Date` Date,
        `Source` LowCardinality(String) CODEC(ZSTD(1)),
        `Ticker` LowCardinality(String) CODEC(ZSTD(1)),
        `Ticks` Int32 DEFAULT 0 CODEC(Delta(4), ZSTD(10)),
        `V` Float32 DEFAULT 0.0 CODEC(Delta(4), ZSTD(10))
    )
    ENGINE = ReplacingMergeTree
    PARTITION BY toYYYYMM(Date)
    ORDER BY (Date, Source, Ticker)
    SETTINGS index_granularity = 8192
    """


    try:
        # Check and create the data table
        if DATA_TABLE_NAME and not table_exists(client, DATA_TABLE_NAME):
            logging.info(f"Creating data table: {DATA_TABLE_NAME}")
            client.execute(DATA_TABLE_DDL)
            logging.info(f"Data table '{DATA_TABLE_NAME}' created successfully.")
        elif DATA_TABLE_NAME:
            logging.info(f"Data table '{DATA_TABLE_NAME}' already exists.")
        else:
            logging.warning("DATA_TABLE_NAME is not set. Skipping data table creation.")

        # Check and create the control table
        if CONTROL_TABLE_NAME and not table_exists(client, CONTROL_TABLE_NAME):
            logging.info(f"Creating control table: {CONTROL_TABLE_NAME}")
            client.execute(CONTROL_TABLE_DDL)
            logging.info(f"Control table '{CONTROL_TABLE_NAME}' created successfully.")
        elif CONTROL_TABLE_NAME:
            logging.info(f"Control table '{CONTROL_TABLE_NAME}' already exists.")
        else:
            logging.warning("CONTROL_TABLE_NAME is not set. Skipping control table creation.")


    except Exception as e:
        logging.error(f"Error creating tables: {e}")


if __name__ == "__main__":
    if not CLICKHOUSE_HOST or not CLICKHOUSE_PASSWORD:
        logging.error("CLICKHOUSE_HOST and CLICKHOUSE_PASSWORD must be set in the environment.")
    else:
        try:
            client = Client(host=CLICKHOUSE_HOST, password=CLICKHOUSE_PASSWORD)
            create_tables(client)
        except Exception as e:
            logging.error(f"Could not connect to ClickHouse: {e}")
        finally:
            if 'client' in locals():  # Check if client was created before disconnecting
                client.disconnect()