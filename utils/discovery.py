import re
import datetime
from typing import Literal, Dict, Set, Callable, Optional, Any
import requests
from xml.etree import ElementTree
from .decorators import retry_on_io_error

from .types import SourceType, Frequency

# Configuration now includes paths for both daily and monthly data
BINANCE_LISTING_URLS: Dict[SourceType, Dict[Frequency, str]] = {
    "BINANCE-SPOT": {
        "daily": "https://s3-ap-northeast-1.amazonaws.com/data.binance.vision/?delimiter=/&prefix=data/spot/daily/trades/",
        "monthly": "https://s3-ap-northeast-1.amazonaws.com/data.binance.vision/?delimiter=/&prefix=data/spot/monthly/trades/"
    },
    "BINANCE-FUT": {
        "daily": "https://s3-ap-northeast-1.amazonaws.com/data.binance.vision/?delimiter=/&prefix=data/futures/um/daily/trades/",
        "monthly": "https://s3-ap-northeast-1.amazonaws.com/data.binance.vision/?delimiter=/&prefix=data/futures/um/monthly/trades/"
    }
}

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
}

DEFAULT_TICKER_FILTER: Callable[[str], bool] = lambda ticker: ticker.endswith(("USDT", "USDC"))


# --- Helper Parsing Functions ---

def _parse_s3_v1_xml_tickers(xml_content: str) -> Dict[str, Any]:
    """
    Helper function to parse a list of tickers (common prefixes)
    from a V1 S3 ListBucketResult XML response.
    """
    root = ElementTree.fromstring(xml_content)
    namespace = {'s3': 'http://s3.amazonaws.com/doc/2006-03-01/'}
    
    prefixes = [p.text for p in root.findall('s3:CommonPrefixes/s3:Prefix', namespace)]
    tickers = {p.rstrip('/').split('/')[-1] for p in prefixes if p}
    
    is_truncated = root.findtext('s3:IsTruncated', 'false', namespace).lower() == 'true'
    next_marker = root.findtext('s3:NextMarker', None, namespace) if is_truncated else None
    
    return {'tickers': tickers, 'next_marker': next_marker}

def _parse_s3_v1_xml_files(xml_content: str, frequency: Frequency) -> Dict[str, Any]:
    """
    Helper function to parse a list of daily or monthly files (contents)
    from a V1 S3 ListBucketResult XML response.
    """
    root = ElementTree.fromstring(xml_content)
    namespace = {'s3': 'http://s3.amazonaws.com/doc/2006-03-01/'}
    
    files_data = {}
    if frequency == "daily":
        date_pattern = re.compile(r"(\d{4}-\d{2}-\d{2})\.zip$")
    else:
        date_pattern = re.compile(r"(\d{4}-\d{2})\.zip$")
    
    for content in root.findall('s3:Contents', namespace):
        key = content.findtext('s3:Key', '', namespace)
        match = date_pattern.search(key)
        
        if match:
            date_str = match.group(1)
            if frequency == "daily":
                file_date = datetime.date.fromisoformat(date_str)
            else:
                year, month = map(int, date_str.split('-'))
                file_date = datetime.date(year, month, 1)
            
            file_size = int(content.findtext('s3:Size', '0', namespace))
            files_data[file_date] = file_size
            
    is_truncated = root.findtext('s3:IsTruncated', 'false', namespace).lower() == 'true'
    next_marker = root.findtext('s3:NextMarker', None, namespace) if is_truncated else None

    return {'files': files_data, 'next_marker': next_marker}


# --- Public API Functions ---
@retry_on_io_error(retries=3, delay=5)
def get_available_tickers(
    source: SourceType,
    frequency: Frequency = "daily", # <-- НОВЫЙ ПАРАМЕТР
    ticker_filter: Optional[Callable[[str], bool]] = DEFAULT_TICKER_FILTER,
    progress_callback: Optional[Callable[[int], None]] = None
) -> Set[str]:
    """
    Discovers all available tickers for a given source and frequency.

    Args:
        source: The data source ("BINANCE-SPOT" or "BINANCE-FUT").
        frequency: The data frequency, "daily" or "monthly".
        ticker_filter: An optional function to filter tickers.
        progress_callback: An optional callback for progress reporting.

    Returns:
        A set of all available ticker strings that match the filter.
    """
    # --- ИЗМЕНЕНИЕ: Используем frequency для выбора правильного URL ---
    base_url = BINANCE_LISTING_URLS[source][frequency]
    # -----------------------------------------------------------------
    
    all_tickers: Set[str] = set()
    next_marker: Optional[str] = None
    
    while True:
        url = f"{base_url}&marker={next_marker}" if next_marker else base_url
        
        response = requests.get(url, timeout=30, headers=HEADERS)
        response.raise_for_status()
        
        xml_content = response.content.decode('utf-8-sig')
        
        page_data = _parse_s3_v1_xml_tickers(xml_content)
        all_tickers.update(page_data['tickers'])
        
        if progress_callback:
            progress_callback(len(all_tickers))
        
        if page_data['next_marker']:
            next_marker = page_data['next_marker']
        else:
            break
            
    if ticker_filter:
        return {ticker for ticker in all_tickers if ticker_filter(ticker)}
    
    return all_tickers

@retry_on_io_error(retries=3, delay=5)
def get_ticker_files(
    source: SourceType,
    ticker: str,
    frequency: Frequency = "daily",
    progress_callback: Optional[Callable[[int], None]] = None
) -> Dict[datetime.date, int]:
    """
    Discovers all available daily or monthly data files for a specific ticker.
    (Docstring is unchanged)
    """
    base_url = BINANCE_LISTING_URLS[source][frequency] + ticker.upper() + '/'
    all_files: Dict[datetime.date, int] = {}
    next_marker: Optional[str] = None
    
    while True:
        url = f"{base_url}&marker={next_marker}" if next_marker else base_url
        
        response = requests.get(url, timeout=30, headers=HEADERS)
        response.raise_for_status()
        
        xml_content = response.content.decode('utf-8-sig')
        page_data = _parse_s3_v1_xml_files(xml_content, frequency=frequency)
        
        all_files.update(page_data['files'])
        
        if progress_callback:
            progress_callback(len(all_files))
            
        if page_data['next_marker']:
            next_marker = page_data['next_marker']
        else:
            break
            
    return all_files