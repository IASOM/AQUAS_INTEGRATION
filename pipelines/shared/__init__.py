"""Shared utilities for pipelines."""
from .db import get_connection
from .utils import (
    ensure_daily_range,
    load_output_matrix,
    save_output_matrix,
    load_state,
    save_state,
    load_last_date_from_output,
    get_min_max_date,
    get_year_ranges,
    get_data_for_year,
)
from .logging_config import setup_logging
from .parquet_storage import (
    ParquetIncrementalManager,
    ParquetFinalStore,
    load_and_merge_final_outputs,
)
from .final_joiner import FinalDataJoiner, IncrementalFinalJoiner

__all__ = [
    "get_connection",
    "ensure_daily_range",
    "load_output_matrix",
    "save_output_matrix",
    "load_state",
    "save_state",
    "load_last_date_from_output",
    "get_min_max_date",
    "get_year_ranges",
    "get_data_for_year",
    "setup_logging",
    "ParquetIncrementalManager",
    "ParquetFinalStore",
    "load_and_merge_final_outputs",
    "FinalDataJoiner",
    "IncrementalFinalJoiner",
]
