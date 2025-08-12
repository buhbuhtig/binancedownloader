#downloader.py
import datetime
import io
import zipfile
import hashlib
import logging
from typing import Dict, Any, Optional, Callable, List

import pandas as pd
import requests
from pandas.io.parsers.readers import TextFileReader

from .decorators import retry_on_io_error, ChecksumError
from .types import SourceType, Frequency

# --- Configuration (unchanged) ---
BINANCE_URL_TEMPLATES: Dict[SourceType, Dict[Frequency, str]] = {
    "BINANCE-SPOT": {
        "daily": "https://data.binance.vision/data/spot/daily/trades/{ticker}/{ticker}-trades-{date_str}.zip",
        "monthly": "https://data.binance.vision/data/spot/monthly/trades/{ticker}/{ticker}-trades-{date_str}.zip",
    },
    "BINANCE-FUT": {
        "daily": "https://data.binance.vision/data/futures/um/daily/trades/{ticker}/{ticker}-trades-{date_str}.zip",
        "monthly": "https://data.binance.vision/data/futures/um/monthly/trades/{ticker}/{ticker}-trades-{date_str}.zip",
    }
}
SOURCE_CONFIG: Dict[SourceType, Dict[str, Any]] = {
    "BINANCE-SPOT": {"column_names": ["trade_id", "price", "qty", "quote_qty", "timestamp_ms", "is_buyer_maker", "is_best_match"]},
    "BINANCE-FUT": {"column_names": ["trade_id", "price", "qty", "quote_qty", "timestamp_ms", "is_buyer_maker"]}
}
# --------------------

# --- NEW HELPER FUNCTION ---
def _process_chunk_iterator(
    chunk_iterator: TextFileReader,
    timestamp_unit: str,
    progress_callback: Optional[Callable[[int], None]]
) -> List[pd.DataFrame]:
    """
    Iterates through chunks, aggregates them, and reports progress.
    This helper function encapsulates the duplicated logic.
    """
    processed_chunks = []
    total_rows_processed = 0
    for chunk_df in chunk_iterator:
        total_rows_processed += len(chunk_df)
        if progress_callback:
            progress_callback(total_rows_processed)

        chunk_df['Timestamp'] = pd.to_datetime(chunk_df['timestamp_ms'], unit=timestamp_unit)
        chunk_df['V'] = chunk_df['price'] * chunk_df['qty']
        
        agg_chunk = chunk_df.resample('5s', on='Timestamp', label='right', closed='right').agg(
            H=('price', 'max'),
            L=('price', 'min'),
            V=('V', 'sum'),
            Ticks=('price', 'count')
        ).dropna()
        
        processed_chunks.append(agg_chunk)
    return processed_chunks
# ----------------------------------------

@retry_on_io_error(retries=3, delay=5)
def download_and_process_ticks_to_df(
    trade_date: datetime.date,
    ticker: str,
    source: SourceType,
    frequency: Frequency = "daily",
    chunksize: int = 5_000_000,
    progress_callback: Optional[Callable[[int], None]] = None
) -> pd.DataFrame:
    """
    Downloads, verifies, and processes tick data from Binance Vision with improved logging.
    """
    FINAL_COLS = {
        'Timestamp': 'datetime64[s]', 'H': 'float64', 'L': 'float64',
        'V': 'int32', 'Ticks': 'int32', 'Source': 'object', 'Ticker': 'object'
    }
    
    ticker = ticker.upper()
    date_str = trade_date.strftime("%Y-%m-%d") if frequency == "daily" else trade_date.strftime("%Y-%m")
    
    url_template = BINANCE_URL_TEMPLATES[source][frequency]
    url = url_template.format(ticker=ticker, date_str=date_str)
    checksum_url = f"{url}.CHECKSUM"
    
    # ... (логика скачивания и проверки чексуммы без изменений) ...
    logging.info(f"[{ticker}][{source}] Downloading the main file for {date_str}...")
    response_zip = requests.get(url, timeout=600)
    response_zip.raise_for_status()
    zip_content = response_zip.content
    logging.info(f"[{ticker}][{source}] The main file for {date_str} was successfully downloaded ({len(zip_content) / 1e6:.2f} MB).")
    logging.info(f"[{ticker}][{source}] Downloading the checksum file for {date_str}...")
    response_checksum = requests.get(checksum_url, timeout=10)
    response_checksum.raise_for_status()
    logging.info(f"[{ticker}][{source}] The checksum file was successfully downloaded.")
    expected_checksum = response_checksum.text.split()[0]
    logging.info(f"[{ticker}][{source}] Starting SHA-256 verification for the file for {date_str}...")
    local_checksum = hashlib.sha256(zip_content).hexdigest()
    if local_checksum != expected_checksum:
        raise ChecksumError(f"[{ticker}][{source}] Checksum verification error for {url}. Expected: {expected_checksum}, received: {local_checksum}")
    logging.info(f"[{ticker}][{source}] Checksum matched. Data is intact.")
    
    logging.info(f"[{ticker}][{source}] Starting data processing from the file for {date_str}...")
    zip_file = zipfile.ZipFile(io.BytesIO(zip_content))
    csv_filename = zip_file.namelist()[0]
    full_col_names = SOURCE_CONFIG[source]["column_names"]
    COLS_TO_USE = ["price", "qty", "timestamp_ms"]
    DTYPES = {"price": "float64", "qty": "float64", "timestamp_ms": "int64"}

    processed_chunks: List[pd.DataFrame]
    timestamp_unit = 'ms'  # Default to milliseconds
    if source == "BINANCE-SPOT" and trade_date >= datetime.date(2025, 1, 1):
        timestamp_unit = 'us'  # Switch to microseconds
    logging.info(f"[{ticker}][{source}] Using microseconds for timestamp conversion. {trade_date}")
    
    with zip_file.open(csv_filename) as csv_stream:
        try:
            chunk_iterator = pd.read_csv(
                csv_stream, header=None, names=full_col_names,
                usecols=COLS_TO_USE, dtype=DTYPES, chunksize=chunksize
            )
            processed_chunks = _process_chunk_iterator(chunk_iterator, timestamp_unit, progress_callback)

        except ValueError as e:
            if "could not convert string to float" in str(e):
                csv_stream.seek(0)
                chunk_iterator_with_header = pd.read_csv(
                    csv_stream, header=0, names=full_col_names,
                    usecols=COLS_TO_USE, dtype=DTYPES, chunksize=chunksize
                )
                processed_chunks = _process_chunk_iterator(chunk_iterator_with_header, timestamp_unit, progress_callback)
            else:
                raise e

    if not processed_chunks:
        logging.info(f"[{ticker}][{source}] Processing completed. The source file for {date_str} is empty.")
        return pd.DataFrame({col: pd.Series(dtype=dtype) for col, dtype in FINAL_COLS.items()})

    result_df = pd.concat(processed_chunks)
    
    if not result_df.index.is_unique:
        result_df = result_df.groupby(result_df.index).agg({
            'H': 'max', 'L': 'min', 'V': 'sum', 'Ticks': 'sum'
        })

    result_df = result_df.astype({'H': 'float64', 'L': 'float64', 'V': 'int32', 'Ticks': 'int32'})
    
    result_df.reset_index(inplace=True)
    result_df.rename(columns={'index': 'Timestamp'}, inplace=True)
    result_df['Timestamp'] = result_df['Timestamp'].dt.floor('s').astype('datetime64[s]')
    result_df['Source'] = source
    result_df['Ticker'] = ticker
    
    logging.info(f"[{ticker}][{source}] Data from the file for {date_str} was successfully processed.")
    return result_df[list(FINAL_COLS.keys())]