# utils/types.py

"""
Centralized module for common data type definitions of the project.
"""

from typing import Literal

# Data source type (exchange and section)
SourceType = Literal["BINANCE-SPOT", "BINANCE-FUT"]

# Data frequency type (daily or monthly archives)
Frequency = Literal["daily", "monthly"]