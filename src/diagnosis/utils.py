import pandas as pd
from typing import Optional
from pathlib import Path 
import pyodbc
import json
from datetime import datetime

def ensure_daily_range(
    df: pd.DataFrame,
    start: Optional[str] = None,
    end: Optional[str] = None,
    fill_value: int | float = 0,
) -> pd.DataFrame:
    out = df.copy()
    out.index = pd.to_datetime(out.index, errors = "coerce")
    out = out[~out.index.isna()].sort_index()

    if out.empty:
        return out

    idx_start = pd.to_datetime(start) if start is not None else out.index.min()
    idx_end = pd.to_datetime(end) if end is not None else out.index.max()

    full_idx = pd.date_range(idx_start, idx_end, freq="D")
    out = out.reindex(full_idx).fillna(fill_value)
    return out



def load_output_matrix(path: str | Path) -> pd.DataFrame:
    path = Path(path)
    df = pd.read_parquet(path, index_col="Timestamp")
    df.index = pd.to_datetime(df.index)
    return df.sort_index()

def save_output_matrix(df: pd.DataFrame, path:str | Path) -> None:
    path = Path(path)
    path.parent.mkdir(parents=True,  exist_ok=True)

    df = df.reset_index().rename(columns={'index':'Timestamp'})

    df.to_parquet(path, index=True)

def load_state(state_file: str | Path) -> Optional[pd.Timestamp]:
    state_file = Path(state_file)

    if not state_file.exists():
        return None

    try: 
        with open(state_file, "r", encoding="utf-8") as f:
            state = json.load(f)
    except Exception:
        return None

    value = state.get("last_loaded_date")
    if value is None:
        return None

    return pd.to_datetime(value, errors = "coerce")

def save_state(state_file: str | Path, last_loaded_date: pd.Timestamp) -> None:
    state_file = Path(state_file)
    state_file.parent.mkdir(parents=True, exist_ok=True)
    
    with open(state_file, "w", encoding="utf-8") as f:
        json.dump(
            {
                "last_loaded_date":pd.to_datetime(last_loaded_date).isoformat(),
                "updated_at": datetime.now().isoformat()
            },
            f,
            ensure_ascii=False,
            indent=2
        )


def load_last_date_from_output(final_file: str | Path) -> Optional[pd.Timestamp]:
    final_file = Path(final_file)

    if not final_file.exists():
        return None

    try: 
        df = pd.read_parquet(final_file, index_col="Timestamp")
        df.index = pd.to_datetime(df.index)
    except Exception:
        return None

    if df.empty:
        return None

    idx = pd.to_datetime(df.index, errors = "coerce")
    idx = idx[~idx.isna()]

    if len(idx) == 0:
        return None

    return idx.max()

def get_min_max_date(
    conn: pyodbc.Connection,
    schema: str,
    table_name: str,
    date_column: str,
    min_valid_date: str = "2008-01-01",
) -> tuple[Optional[pd.Timestamp], Optional[pd.Timestamp]]:    
    query = f"""
    SELECT
        MIN([{date_column}]) AS min_date,
        MAX([{date_column}]) AS max_date
    FROM [{schema}].[{table_name}]
    where [{date_column}] IS NOT NULL
        AND [{date_column}] >= ?
    """
    df = pd.read_sql_query(query, conn, params=[min_valid_date])

    if df.empty or pd.isna(df.loc[0, "min_date"]) or pd.isna(df.loc[0, "max_date"]):
        return None, None

    return pd.to_datetime(df.loc[0, "min_date"]), pd.to_datetime(df.loc[0, "max_date"])

def get_year_ranges(start_date: pd.Timestamp, end_date: pd.Timestamp) -> list[tuple[int, pd.Timestamp, pd.Timestamp]]:
    ranges = []
    for year in range(start_date.year, end_date.year + 1):
        year_start = pd.Timestamp(f"{year}-01-01 00:00:00")
        year_end = pd.Timestamp(f"{year + 1}-01-01 00:00:00")
        ranges.append((year, year_start, year_end))
    return ranges

def get_data_for_year(
    conn: pyodbc.Connection,
    schema: str,
    table_name: str,
    date_column: str,
    year_start: pd.Timestamp, 
    year_end: pd.Timestamp, 
    last_loaded_date: Optional[pd.Timestamp] = None,
    selected_cols: Optional[list[str]] = None,
) -> pd.DataFrame:

    cols_sql = ", ".join(f"[{c}]" for c in selected_cols)
    
    if last_loaded_date is None:
        query = f"""
        SELECT {cols_sql}
        FROM [{schema}].[{table_name}]
        WHERE [{date_column}] >= ?
            AND [{date_column}] < ?
        ORDER BY [{date_column}] ASC
        """
        params = [year_start, year_end]
    else:
        query = f"""
        SELECT {cols_sql}
        FROM [{schema}].[{table_name}]
        WHERE [{date_column}] >= ?
            AND [{date_column}] < ?
            AND [{date_column}] > ?
        ORDER BY [{date_column}] ASC
        """
        params = [year_start, year_end, last_loaded_date]

    return pd.read_sql_query(query, conn, params=params)


def load_last_date_from_finals(final_dir: str | Path) -> Optional[pd.Timestamp]:
    final_dir = Path(final_dir)

    files = [
        final_dir / "selected_cat.csv",
        final_dir / "selected_rs.csv",
        final_dir / "selected_up.csv",
        final_dir / "selected_cat.parquet",
        final_dir / "selected_rs.parquet",
        final_dir / "selected_up.parquet"
    ]

    last_dates = []

    for file in files: 
        if not file.exists() or file.stat().st_size == 0:
            continue

        try:
            if file.suffix == ".parquet":
                df = pd.read_parquet(file)
            else:
                df = pd.read_csv(file)

            if "Timestamp" in df.columns:
                dates = pd.to_datetime(df["Timestamp"], format = "mixed", errors = "coerce")
            elif isinstance(df.index, pd.DatetimeIndex):
                dates = pd.Series(df.index)
            else:
                idx_dates = pd.to_datetime(df.index, format="mixed", errors="coerce")
                dates = pd.Series(idx_dates)
            
            dates = dates.dropna()
            dates = dates[
                (dates >= pd.Timestamp("2008-01-01")) & 
                (dates <= pd.Timestamp.today() + pd.Timedelta(days=1))
            ]

            if not dates.empty:
                last_dates.append(dates.max())

        except Exception as e:
            print(f"[WARN] Could not read {file}: {e}")
        
    if not last_dates:
        return None

    return max(last_dates)


def clean_incremental_dir(incremental_dir: str | Path, keep_files: list[str]):
    incremental_dir = Path(incremental_dir)

    for f in incremental_dir.iterdir():
        if f.name not in keep_files:
            try:
                f.unlink()
            except Exception:
                pass