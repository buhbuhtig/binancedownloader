# utils/decorators.py
import time
from functools import wraps
from typing import Callable, Any
import requests
import logging

class ChecksumError(IOError):
    """
    Custom exception raised for checksum verification failures.
    Inherits from IOError to be caught by IO-related error handlers.
    """
    pass

# Tuple of exceptions that are considered IO errors
IO_EXCEPTIONS = (requests.exceptions.RequestException, IOError, ChecksumError)

def retry_on_io_error(retries: int = 3, delay: int = 5) -> Callable:
    """
    A decorator to retry a function if it raises an IO-related exception.
    Logs warnings on retries and an error on final failure.

    Args:
        retries (int): The total number of attempts to make.
        delay (int): The number of seconds to wait between retries.

    Returns:
        The result of the wrapped function if it succeeds.

    Raises:
        The last caught exception if all retries fail.
    """
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs) -> Any:
            last_exception = None
            for attempt in range(retries):
                try:
                    return func(*args, **kwargs)
                except IO_EXCEPTIONS as e:
                    last_exception = e
                    # If this is not the last attempt, log a warning and wait
                    if attempt < retries - 1:
                        log_message = (
                            f"Function '{func.__name__}' failed with {type(e).__name__}: {e}. "
                            f"Retrying in {delay}s... (Attempt {attempt + 1}/{retries})"
                        )
                        logging.warning(log_message)
                        time.sleep(delay)
                    else:
                        # If this is the last attempt, just exit the loop to log the final error
                        break
            
            # If the loop completes (all attempts failed), log an error and raise the exception
            logging.error(f"All {retries} retries for '{func.__name__}' failed. Raising last exception.")
            raise last_exception
        return wrapper
    return decorator