import logging
import datetime
from typing import List, Tuple, Any, Dict
import sys

import pandas as pd
from clickhouse_driver import Client

from utils.types import SourceType, Frequency
from utils.downloader import download_and_process_ticks_to_df
from utils.discovery import get_ticker_files
from utils.sync_orchestrator_cleaner import cleanup_period_data

import os
# --- CONSTANTS FOR ENVIRONMENT VARIABLES ---
# These variables define where to get the connection configuration from
ENV_CH_HOST = "CLICKHOUSE_HOST"
ENV_CH_PASSWORD = "CLICKHOUSE_PASSWORD"
ENV_LOG_LEVEL = "LOG_LEVEL"
DEFAULT_ENV_CH_HOST = "172.16.0.9"

# --- Definition of columns and their target types ---
DATA_TABLE_TIMESTAMP_COLUMN: str = 'Timestamp'
DATA_TABLE_COLUMNS: List[str] = [DATA_TABLE_TIMESTAMP_COLUMN, 'Source', 'Ticker', 'H', 'L']
CONTROL_TABLE_COLUMNS: List[str] = ['Date', 'Source', 'Ticker', 'Ticks', 'V']

# Dictionary-schemas for forced type setting before insertion
DATA_TABLE_DTYPES = {
    'Timestamp': 'datetime64[s]',
    'Source': 'object',
    'Ticker': 'object',
    'H': 'float64',
    'L': 'float64'
}

CONTROL_TABLE_DTYPES = {
    'Date': 'datetime64[s]', # Wanted to use 'datetime64[D]' but could not (driver's problem)
    'Source': 'object',
    'Ticker': 'object',
    'Ticks': 'int32',
    'V': 'float32'
}
# -----------------------------------------------------------


def create_summary_record(
    trade_date: datetime.date,
    source: SourceType,
    ticker: str,
    ticks: int,
    volume: float
) -> Dict[str, Any]:
    """
    Creates and validates a dictionary for insertion into the control table.
    The function signature is a form of validation.
    """
    record = {
        'Date': trade_date,
        'Source': source,
        'Ticker': ticker,
        'Ticks': ticks,
        'V': volume
    }
    
    # quick check just in case,
    # if CONTROL_TABLE_COLUMNS changes, but this function does not.
    if set(record.keys()) != set(CONTROL_TABLE_COLUMNS):
        raise ValueError(f"Keys in create_summary_record do not match CONTROL_TABLE_COLUMNS. "
                         f"Expected: {set(CONTROL_TABLE_COLUMNS)}, Received: {set(record.keys())}")

    return record


def _monthly_bulk_load(source: SourceType, ticker: str, data_table:str, control_table:str, client: Client, dry_run:bool = False):
    """Performs the initial "cold" load with cleanup on failure."""
    prefix = f"[{ticker}][{source}]"
    logging.info(f"{prefix} Initial loading: searching for monthly archives...")
    monthly_files = get_ticker_files(source=source, ticker=ticker, frequency="monthly")
    
    if not monthly_files:
        logging.warning(f"{prefix} Monthly archives not found.")
        return

    min_month = min(monthly_files.keys())
    max_month = max(monthly_files.keys())
    
    logging.info(
        f"{prefix} Found {len(monthly_files)} monthly archives "
        f"in the range from {min_month.strftime('%Y-%m')} to {max_month.strftime('%Y-%m')}. "
        f"Starting processing..."
    )
    
    for month_date, size in sorted(monthly_files.items()):
        logging.info(f"{prefix} Processing month {month_date.strftime('%Y-%m')} (size: {size / 1e6:.2f} MB)...")
        try:
            full_df = download_and_process_ticks_to_df(trade_date=month_date, ticker=ticker, source=source, frequency="monthly")
            if full_df.empty:
                logging.warning(f"{prefix} DataFrame for month {month_date.strftime('%Y-%m')} is empty.")
                continue

            # 1. Prepare and insert data into the main table
            df_for_data_table = full_df[DATA_TABLE_COLUMNS].astype(DATA_TABLE_DTYPES)
            if not df_for_data_table.empty:
                min_ts = df_for_data_table[DATA_TABLE_TIMESTAMP_COLUMN].min()
                max_ts = df_for_data_table[DATA_TABLE_TIMESTAMP_COLUMN].max()
                logging.info(f"{prefix} Time range for insertion: from {min_ts.strftime('%Y-%m-%d %H:%M:%S')} to {max_ts.strftime('%Y-%m-%d %H:%M:%S')}")
        
            if not dry_run:
                logging.info(f"{prefix} Inserting {len(df_for_data_table)} bars into '{data_table}'...")
                client.insert_dataframe(f'INSERT INTO {data_table} ({", ".join(DATA_TABLE_COLUMNS)}) VALUES', df_for_data_table)
            else:
                logging.info(f"{prefix} [DRY RUN] Skipping insertion of {len(df_for_data_table)} bars into '{data_table}'.")
            
            # 2. Calculation and insertion of daily aggregates
            adjusted_timestamp = full_df['Timestamp'] - pd.Timedelta(seconds=1)
            daily_summary = full_df.groupby(adjusted_timestamp.dt.date).agg(
                Ticks=('Ticks', 'sum'),
                V=('V', lambda x: x.sum() / 1_000_000)
            ).reset_index()
            daily_summary.rename(columns={'Timestamp': 'Date'}, inplace=True)
            daily_summary['Source'] = source
            daily_summary['Ticker'] = ticker
            
            df_for_control_table = daily_summary[CONTROL_TABLE_COLUMNS].astype(CONTROL_TABLE_DTYPES)
            
            if not dry_run:
                logging.info(f"{prefix} Inserting {len(df_for_control_table)} records into '{control_table}'...")
                client.insert_dataframe(f'INSERT INTO {control_table} ({", ".join(CONTROL_TABLE_COLUMNS)}) VALUES', df_for_control_table)
            else:
                logging.info(f"{prefix} [DRY RUN] Skipping insertion of {len(df_for_control_table)} records into '{control_table}'.")
            
            logging.info(f"{prefix} Month {month_date.strftime('%Y-%m')} successfully loaded.")

        except Exception as e:
            logging.error(f"{prefix} Error processing month {month_date.strftime('%Y-%m')}: {e}")
            if not dry_run:
                cleanup_period_data(client, source, ticker, month_date, 'monthly', data_table, control_table)
            else:
                logging.warning(f"{prefix} [DRY RUN] Skipping data cleanup for {month_date.strftime('%Y-%m')}.")
            raise e

def _daily_incremental_sync(source: SourceType, ticker: str, data_table:str, control_table:str, client: Client, dry_run:bool = False):
    """Performs incremental synchronization with cleanup on failure."""
    prefix = f"[{ticker}][{source}]"
    logging.info(f"{prefix} Incremental update: looking for available daily files...")
    available_files = get_ticker_files(source=source, ticker=ticker, frequency="daily")
    available_dates = set(available_files.keys())
    
    query_downloaded = f"SELECT DISTINCT Date FROM {control_table} FINAL WHERE Source = %(source)s AND Ticker = %(ticker)s"
    downloaded_dates = {pd.Timestamp(row[0]).date() for row in client.execute(query_downloaded, {'source': source, 'ticker': ticker})}
    
    dates_to_download = sorted(list(available_dates - downloaded_dates))
    
    if not dates_to_download:
        logging.info(f"{prefix} All available daily files are already loaded.")
        return
        
    first_day = dates_to_download[0]
    last_day = dates_to_download[-1]
    
    logging.info(
        f"{prefix} Need to download {len(dates_to_download)} new daily files "
        f"in the range from {first_day.strftime('%Y-%m-%d')} to {last_day.strftime('%Y-%m-%d')}..."
    )

    for trade_date in dates_to_download:
        logging.info(f"{prefix} Processing day {trade_date}...")
        try:
            full_df = download_and_process_ticks_to_df(trade_date=trade_date, ticker=ticker, source=source, frequency="daily")
            
            if full_df.empty:
                logging.warning(f"{prefix} DataFrame for {trade_date} is empty. Writing 'empty' record.")
                summary_record = create_summary_record(
                    trade_date=trade_date,
                    source=source,
                    ticker=ticker,
                    ticks=0,
                    volume=0.0
                    )
            else:
                df_for_data_table = full_df[DATA_TABLE_COLUMNS].astype(DATA_TABLE_DTYPES)
                if not df_for_data_table.empty:
                    min_ts = df_for_data_table[DATA_TABLE_TIMESTAMP_COLUMN].min()
                    max_ts = df_for_data_table[DATA_TABLE_TIMESTAMP_COLUMN].max()
                    logging.info(f"{prefix} Time range for insertion: from {min_ts.strftime('%Y-%m-%d %H:%M:%S')} to {max_ts.strftime('%Y-%m-%d %H:%M:%S')}")

                if not dry_run:
                    logging.info(f"{prefix} Inserting {len(df_for_data_table)} bars into '{data_table}'...")
                    client.insert_dataframe(f'INSERT INTO {data_table} ({", ".join(DATA_TABLE_COLUMNS)}) VALUES', df_for_data_table)
                else:
                    logging.info(f"{prefix} [DRY RUN] Skipping insertion of {len(df_for_data_table)} bars into '{data_table}'.")

                end_of_day = pd.to_datetime(trade_date) + pd.Timedelta(days=1)
                df_for_summary = full_df[full_df['Timestamp'] < end_of_day]
                summary_record = create_summary_record(
                    trade_date=trade_date,
                    source=source,
                    ticker=ticker,
                    ticks=df_for_summary['Ticks'].sum(),
                    volume=df_for_summary['V'].sum() / 1_000_000
                )
            
            summary_df = pd.DataFrame([summary_record])
            summary_df_for_insertion = summary_df[CONTROL_TABLE_COLUMNS].astype(CONTROL_TABLE_DTYPES)
            
            if not dry_run:
                logging.info(f"{prefix} Inserting record into '{control_table}': {summary_record}")
                client.insert_dataframe(f'INSERT INTO {control_table} ({", ".join(CONTROL_TABLE_COLUMNS)}) VALUES', summary_df_for_insertion)
            else:
                logging.info(f"{prefix} [DRY RUN] Skipping insertion of record into '{control_table}' for {summary_record['Date']}.")
            
            logging.info(f"{prefix} Day {trade_date} successfully processed.")

        except Exception as e:
            logging.error(f"{prefix} Error processing day {trade_date}: {e}")
            cleanup_period_data(client, source, ticker, trade_date, 'daily', data_table, control_table)
            continue

def sync_ticker_data(source: SourceType, ticker: str, data_table: str, control_table: str, client: Client, dry_run:bool = False):
    """Main orchestrator function."""
    prefix = f"[{ticker}][{source}]"
    logging.info(f"{prefix} --- Starting synchronization for {ticker} ({source}) ---")
    try:
        query_exists = f"SELECT count() FROM {control_table} WHERE Source = %(source)s AND Ticker = %(ticker)s"
        records_count = client.execute(query_exists, {'source': source, 'ticker': ticker})[0][0]

        if records_count == 0:
            logging.info(f"{prefix} Records not found. Starting initial bulk load (monthly)...")
            _monthly_bulk_load(source, ticker, data_table, control_table, client, dry_run = dry_run)
            logging.info(f"{prefix} Bulk load completed. Starting loading with daily files (daily)...")
            _daily_incremental_sync(source, ticker, data_table, control_table, client, dry_run = dry_run)
        else:
            logging.info(f"{prefix} Existing records found. Starting incremental update (daily)...")
            _daily_incremental_sync(source, ticker, data_table, control_table, client, dry_run = dry_run)
            
        logging.info(f"{prefix} --- Synchronization for {ticker} ({source}) successfully completed ---")
    except Exception as e:
        logging.critical(f"{prefix} Critical error during synchronization {ticker}: {e}", exc_info=True)
        

def run_sync_in_process(task_params: Dict[str, Any]):
    """
    Wrapper for running in a separate process.
    Accepts task parameters as a dictionary.
    Reads the connection configuration from environment variables.
    """
    # 1. Read task parameters from the dictionary
    source = task_params['source']
    ticker = task_params['ticker']
    data_table = task_params['data_table']
    control_table = task_params['control_table']
    dry_run = task_params.get('dry_run', True)
    prefix = f"[{ticker}][{source}]"
    
    # 2. Read the connection configuration from environment variables
    ch_host = os.getenv(ENV_CH_HOST, DEFAULT_ENV_CH_HOST)
    ch_password = os.getenv(ENV_CH_PASSWORD)
    log_level_str = os.getenv(ENV_LOG_LEVEL, 'INFO').upper()
    
    # Configure logging for this specific process
    if '_worker_logging_configured' not in globals():
        logging.basicConfig(
            level=log_level_str,
            format='%(asctime)s - %(levelname)s - %(processName)s - %(message)s',
            stream=sys.stdout
        )
        # Set the flag after the first configuration
        globals()['_worker_logging_configured'] = True
    
    client = None
    try:
        # Create a client using the configuration from the environment
        client = Client(host=ch_host, password=ch_password, settings={'use_numpy': True})
        
        # Call the main function
        sync_ticker_data(source, ticker, data_table, control_table, client, dry_run = dry_run)
        
        return { "ticker": ticker, "status": "OK" }
    except Exception as e:
        logging.critical(f"{prefix} CRITICAL ERROR: {e}", exc_info=False)
        return { "ticker": ticker, "status": "FAILED", "error": str(e) }
    finally:
        if client:
            client.disconnect()