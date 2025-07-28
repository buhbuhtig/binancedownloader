# Binance Historical Data Ingestion Pipeline

This project provides a robust and efficient pipeline for downloading, processing, and storing historical trade data from Binance (Spot and Futures) into a ClickHouse database.

The data is sourced from the **Binance Vision** archive, a repository of historical data files, *not* from the Binance API. This allows for the ingestion of large volumes of historical data without being subject to API rate limits.

## Key Features

*   **Data Sources:** Supports both Binance Spot (**BINANCE-SPOT**) and Futures (**BINANCE-FUT**)  markets. Configurable via environment variables.
*   **Data Frequency:** Handles both monthly and daily data archives from Binance Vision. The system automatically downloads monthly data first to bootstrap the dataset, then switches to daily data for incremental updates.
*   **Data Aggregation:** Downloads raw trade data and aggregates it into 5-second High-Low (HL5s) bars, aligned to the right edge of the interval [00:00:00 - 00:00:04.999] -> 00:00:05.
*   **ClickHouse Storage:** Stores the aggregated data in a ClickHouse database, optimized for time-series analysis.
*   **Error Handling & Idempotency:** Implements robust error handling with retries and data cleanup mechanisms to ensure data integrity. The control table prevents data duplication in case of failures. If a failure occurs during processing, data for the failed period is removed from the database, ensuring a clean state for the next run.
*   **Liquidity Statistics:** Maintains a separate control table that stores daily aggregates (tick count and trading volume) for each ticker, enabling liquidity analysis and historical volume tracking.
*   **Ticker Filtering:** Allows flexible ticker filtering using a lambda function, enabling the selective download of specific trading pairs (e.g., only USDT or USDC pairs).
*   **Multi-threaded Processing:** Leverages multi-threading for parallel processing of multiple tickers, significantly reducing the overall synchronization time.

## Architecture

The pipeline consists of the following main components:

*   **`create_table.py`:** Creates the necessary tables in ClickHouse if they don't already exist.
*   **`utils/downloader.py`:** Downloads, verifies, and processes tick data from Binance Vision, aggregating it into 5-second bars.
*   **`utils/discovery.py`:** Discovers available tickers and data files on the Binance Vision servers.
*   **`utils/sync_orchestrator_cleaner.py`:**  Handles data cleanup in case of errors, ensuring data consistency.
*   **`sync_orchestrator.py`:** Orchestrates the entire synchronization process for a single ticker, deciding between initial bulk load and incremental updates.
*   **`run.py` (MULTI-THREAD):** The main entry point for running the pipeline. It manages the parallel processing of multiple tickers.

## Prerequisites

*   **Python 3.10+**
*   **ClickHouse Database:** A running ClickHouse instance.
*   **Required Python Packages:**
    ```bash
    pip install python-dotenv clickhouse-driver pandas requests tqdm
    ```

## Installation and Configuration

1.  **Clone the Repository:**
    ```bash
    git clone <your_repository_url>
    cd <repository_directory>
    ```

2.  **Create a `.env` File:**
    Create a `.env` file in the project root directory to store your configuration settings.  Example:

    ```
    SOURCE_TO_SYNC=BINANCE-FUT
    DATA_TABLE_NAME=hl5s
    CONTROL_TABLE_NAME=hl5s_daily_downloaded
    NUM_PROCESSES=3
    DRY_RUN_MODE=False
    CLICKHOUSE_HOST=XXX.XXX.XXX.XXX
    CLICKHOUSE_PASSWORD=your_clickhouse_password
    ```

    **Important:**
    *   Replace `XXX.XXX.XXX.XXX` and `your_clickhouse_password` with your actual ClickHouse credentials.
    *   `DRY_RUN_MODE=True` will simulate the data loading process without actually writing to ClickHouse. Useful for testing.
    *   `NUM_PROCESSES` should be set to a reasonable number based on your system's CPU cores & RAM.
    

3.  **Environment Variables:** The following environment variables are used to configure the pipeline. Set them in your `.env` file:

    *   `SOURCE_TO_SYNC`:  `BINANCE-SPOT` or `BINANCE-FUT` (defaults to `BINANCE-FUT`).
    *   `DATA_TABLE_NAME`: The name of the ClickHouse table to store the 5-second bar data (defaults to `hl5s`).
    *   `CONTROL_TABLE_NAME`: The name of the ClickHouse table to store the daily summary data (defaults to `hl5s_daily_downloaded`).
    *   `NUM_PROCESSES`: The number of parallel processes to use (defaults to `3`).
    *   `DRY_RUN_MODE`:  `True` or `False`. If `True`, the script will perform a "dry run" without actually writing data to ClickHouse (defaults to `False`).
    *   `CLICKHOUSE_HOST`: The hostname or IP address of your ClickHouse server.
    *   `CLICKHOUSE_PASSWORD`: The password for your ClickHouse user.

## Usage

1.  **Create ClickHouse Tables:**
    Run the `create_table.py` script to create the necessary tables in your ClickHouse database.  Ensure that the `CLICKHOUSE_HOST` and `CLICKHOUSE_PASSWORD` environment variables are correctly set.
    ```bash
    python create_table.py
    ```
    This script will check if the tables already exist and create them if they don't.

2.  **Run the Synchronization Pipeline:**
    Execute the `run.py` script to start the data synchronization process.
    ```bash
    python run.py
    ```

    The script will:

    *   Read the configuration from the `.env` file (see & rename `.env.demo` ).
    *   Discover available tickers on Binance Vision (based on the `SOURCE_TO_SYNC` setting).
    *   Filter the tickers based on the defined filter (currently, tickers ending in 'USDT' or 'USDC').
    *   Launch a multi-threaded process to download, process, and load the data for each ticker.
    *   Display progress information and summary results.

## Customization

*   **Ticker Filtering:**  Modify the `ticker_filter` lambda function in `run.py` to customize the ticker selection criteria.
*   **Data Aggregation Interval:**  Adjust the resampling interval (currently 5 seconds) in `utils/downloader.py`.
*   **ClickHouse Table Definitions:**  Modify the `CREATE TABLE` statements in `create_table.py` to customize the table structure (if needed).

## Logging

The pipeline uses Python's built-in `logging` module. Log messages are written to the console.  You can configure the logging level (e.g., `INFO`, `DEBUG`, `WARNING`, `ERROR`) using the `LOG_LEVEL` environment variable.

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.