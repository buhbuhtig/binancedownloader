# utils/sync_orchestrator_cleaner.py

import logging
import datetime
from clickhouse_driver import Client

from .types import SourceType, Frequency

def cleanup_period_data(
    client: Client,
    source: SourceType,
    ticker: str,
    process_date: datetime.date,
    frequency: Frequency,
    data_table_name: str,
    control_table_name: str
):
    """
    Deletes all data for the specified period (day or month) from both tables
    to ensure idempotency in case of failure.
    """
    period_str = process_date.strftime('%Y-%m' if frequency == 'monthly' else '%Y-%m-%d')
    logging.warning(f"[{ticker}][{source}] Operation failed. Starting data cleanup for {period_str}...")
    
    # Parameters remain the same
    params = {'source': source, 'ticker': ticker, 'date': process_date}
    
    try:
        # --- CHANGES IN QUERIES ---
        
        # Cleaning the main data table
        if frequency == 'monthly':
            # Explicitly convert the %(date)s parameter to Date
            cleanup_query_data = f"ALTER TABLE {data_table_name} DELETE WHERE Source = %(source)s AND Ticker = %(ticker)s AND toStartOfMonth(Timestamp) = toDate(%(date)s)"
        else: # daily
            cleanup_query_data = f"ALTER TABLE {data_table_name} DELETE WHERE Source = %(source)s AND Ticker = %(ticker)s AND toDate(Timestamp) = toDate(%(date)s)"
        
        logging.info(f"[{ticker}][{source}] Cleaning '{data_table_name}'...")
        client.execute(cleanup_query_data, params)

        # Cleaning the control table
        if frequency == 'monthly':
            # Explicitly convert the %(date)s parameter to Date
            cleanup_query_control = f"ALTER TABLE {control_table_name} DELETE WHERE Source = %(source)s AND Ticker = %(ticker)s AND toStartOfMonth(Date) = toDate(%(date)s)"
        else: # daily
            cleanup_query_control = f"ALTER TABLE {control_table_name} DELETE WHERE Source = %(source)s AND Ticker = %(ticker)s AND Date = toDate(%(date)s)"
        
        logging.info(f"[{ticker}][{source}] Cleaning '{control_table_name}'...")
        client.execute(cleanup_query_control, params)
        
        # ---------------------------
        
        logging.warning(f"[{ticker}][{source}] Data cleanup for {period_str} successfully completed.")
        
    except Exception as cleanup_e:
        logging.error(f"[{ticker}][{source}] CRITICAL ERROR: Failed to perform data cleanup for {period_str}! The database may be in an inconsistent state. Error: {cleanup_e}")
        raise cleanup_e