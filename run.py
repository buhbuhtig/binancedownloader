#MULTI-THREAD
import os
from dotenv import load_dotenv
from multiprocessing import Pool
from tqdm import tqdm


from utils.discovery import get_available_tickers
from sync_orchestrator import run_sync_in_process, ENV_CH_HOST, ENV_CH_PASSWORD, ENV_LOG_LEVEL

SOURCE_TO_SYNC='BINANCE-FUT'
DATA_TABLE_NAME = 'hl5s_test'
CONTROL_TABLE_NAME = 'hl5s_daily_downloaded'
NUM_PROCESSES = 3
#===============
DRY_RUN_MODE = False
#===============

LOG_LEVEL = 'INFO'
CLICKHOUSE_HOST = '172.16.0.9'
CLICKHOUSE_PASSWORD = '123'

os.environ[ENV_CH_HOST] = CLICKHOUSE_HOST
os.environ[ENV_LOG_LEVEL] = LOG_LEVEL
os.environ[ENV_CH_PASSWORD] = CLICKHOUSE_PASSWORD

tickers = get_available_tickers(
    source=SOURCE_TO_SYNC,
        frequency="daily",
        ticker_filter=lambda ticker: ticker.endswith( ('USDT','USDC') ),
        progress_callback=None
    )
       
tasks = [
    {
        "source": SOURCE_TO_SYNC,
        "ticker": ticker,
        "data_table": DATA_TABLE_NAME,
        "control_table": CONTROL_TABLE_NAME,
        "dry_run": DRY_RUN_MODE
    }
    for ticker in tickers
]
print(f"Found {len(tasks)} tickers. Starting parallel processing on {NUM_PROCESSES} processes...")
print(f"'Dry Run' mode: {DRY_RUN_MODE}")

with Pool(processes=NUM_PROCESSES) as pool:
    results = list(
        tqdm(pool.imap_unordered(run_sync_in_process, tasks), total=len(tasks))
    )

print("\n--- Processing completed. Results: ---")
success_count = sum(1 for r in results if r['status'] == "OK")
fail_count = len(results) - success_count
print(f"Successful: {success_count}, With errors: {fail_count}")

if fail_count > 0:
    print("\nTickers with errors:")
    for res in results:
        if res['status'] == "FAILED":
            print(f" - {res['ticker']}: {res['error']}")