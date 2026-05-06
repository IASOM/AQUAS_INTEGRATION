# ========================
# AGREGACIÓ A MATRIUS DIARIES
# ========================
import pandas as pd 
from typing import Optional, Iterable
from pathlib import Path 
from .utils import *
import logging

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

def save_output_matrix(
    df: pd.DataFrame, 
    path:str | Path
) -> None:
    path = Path(path)
    path.parent.mkdir(parents=True,  exist_ok=True)
    df.to_parquet(path.with_suffix(".parquet"), index="True")
    df.to_csv(path.with_suffix(".csv"), index="True")

def incremental_add_long_counts(
    df_new: pd.DataFrame,
    output_file: str | Path,
    key_cols: list[str],
    value_col: str = "n"
) -> pd.DataFrame:
    
    output_file = Path(output_file)
    output_file.parent.mkdir(parents=True, exist_ok=True)

    df_new = (
        df_new
        .groupby(key_cols, as_index=False, observed=True)[value_col]
        .sum()
    )

    batch_file = output_file.parent / f"{output_file.stem}_{pd.Timestamp.now():%Y%m%d_%H%M%S}.parquet"

    df_new.to_parquet(batch_file, index=False)

    return df_new


def build_daily_total_general(
    reduced_df: pd.DataFrame,
    date_column: str,
) -> pd.DataFrame:
    
    return reduced_df.groupby(date_column, as_index=False)["n"].sum().rename(columns={"n": "DIAG_TOTAL"}).set_index(date_column).sort_index()


def build_daily_total_by_group(
    reduced_df: pd.DataFrame,
    group_col: str,
    date_column: str,
) -> pd.DataFrame:
    
    out = reduced_df.groupby(
        [date_column, group_col],
        as_index = False
    )["n"].sum().pivot(index=date_column, columns=group_col, values="n").fillna(0).sort_index()
    
    out.columns = [f"DIAG_TOTAL__{group_col}__{c}" for c in out.columns]

    return out


def build_grouped_long(
    reduced_df: pd.DataFrame,
    code_col: str,
    date_column: str,
    group_col: Optional[str]=None,
    value_col: str = "n",
) -> pd.DataFrame:

    keep = [date_column, code_col, value_col] if group_col is None else [date_column, group_col, code_col, value_col]    
    out = reduced_df[keep].copy()

    group_cols = [date_column, code_col] if group_col is None else [date_column, group_col, code_col]

    out = out.groupby(
        group_cols,
        as_index=False
    )[value_col].sum()
    
    out["LEVEL"] = code_col

    return out


def build_selected_daily_matrix(
    reduced_df: pd.DataFrame,
    selected_codes: Iterable[str],
    group_col: Optional[str]=None,
    date_column: str = "DATA_VISITA",
    value_col: str = "n",
) -> pd.DataFrame:
    selected_codes = sorted(set(str(x).strip().upper() for x in selected_codes if pd.notna(x)))
    out = reduced_df[reduced_df["ICD10_3"].isin(selected_codes)].copy()

    if group_col is None:
        grouped = out.groupby(
            [date_column, "ICD10_3"], 
            as_index = False
        )[value_col].sum()

        wide = grouped.pivot(
            index=date_column,
            columns="ICD10_3",
            values= value_col
        ).fillna(0).sort_index()

        wide.columns = [f"ICD10_3__{c}" for c in wide.columns]
        return wide

    grouped = out.groupby(
        [date_column, "ICD10_3", group_col],
        as_index=False
    )[value_col].sum()

    grouped["feature"] = grouped["ICD10_3"] + "__" + group_col + "__" + grouped[group_col].astype(str)
    
    wide = grouped.pivot(index=date_column, columns="feature", values=value_col).fillna(0).sort_index()
    wide.columns = [f"ICD10_3__{c}" for c in wide.columns]

    return wide


def _wide_from_level(
    reduced_df: pd.DataFrame,
    level_col: str,
    prefix: str,
    date_column: str = "DATA_VISITA",
    group_col: Optional[str] = None,
    value_col: str = "n",
) -> pd.DataFrame:

    if group_col is None:
        grouped = (
            reduced_df
            .groupby([date_column, level_col], as_index = False)[value_col]
            .sum()
        )

        wide = (
            grouped
            .pivot(index=date_column, columns=level_col, values=value_col)
            .fillna(0)
            .sort_index()
        )

        wide.columns = [f"{prefix}_{c}" for c in wide.columns]
        return wide
    
    grouped = (
        reduced_df
        .groupby([date_column, group_col, level_col], as_index=False)[value_col]
        .sum()
    )

    grouped["feature"] = (
        prefix 
        + "__"
        + grouped[level_col].astype(str)
        + "__"
        + group_col
        + "__"
        + grouped[group_col].astype(str)
    )    

    wide = (
        grouped
        .pivot(index=date_column, columns="feature", values=value_col)
        .fillna(0)
        .sort_index()
    )

    return wide


def build_selected_wide_all_levels(
    reduced_df: pd.DataFrame,
    selected_codes: Iterable[str],
    date_column: str = "DATA_VISITA",
    group_col: Optional[str] = None,
    value_col: str = "n",
) -> pd.DataFrame:

    selected_codes = sorted(
        set(str(x).strip().upper() for x in selected_codes if pd.notna(x))
    )

    pieces = []

    if group_col is None:
        total = (
            reduced_df
            .groupby(date_column, as_index=True)[value_col]
            .sum()
            .to_frame("TOTAL")
        )
    else:
        tmp = (
            reduced_df
            .groupby([date_column, group_col], as_index=False)[value_col]
            .sum()
        )
        tmp["feature"] = "TOTAL__" + group_col + "__" + tmp[group_col].astype(str)

        total = (
            tmp.pivot(index=date_column, columns="feature", values=value_col)
            .fillna(0)
            .sort_index()
        )

    pieces.append(total)

    pieces.append(
        _wide_from_level(
            reduced_df,
            level_col = "ICD10_CHAPTER",
            prefix = "CHAPTER",
            date_column = date_column,
            group_col = group_col,
            value_col = value_col,
        )
    )

    pieces.append(
        _wide_from_level(
            reduced_df,
            level_col = "ICD10_SUBCHAPTER",
            prefix = "SUBCHAPTER",
            date_column = date_column,
            group_col = group_col,
            value_col = value_col,
        )
    )

    selected_df = reduced_df[reduced_df["ICD10_3"].isin(selected_codes)].copy()

    pieces.append(
        _wide_from_level(
            selected_df,
            level_col="ICD10_3",
            prefix="ICD10_3",
            date_column = date_column,
            group_col = group_col, 
            value_col = value_col,
        )
    )

    out = pd.concat(pieces, axis=1).fillna(0).sort_index()
    out.index = pd.to_datetime(out.index)
    return out






def build_final_outputs(
    reduced_file: str | Path,
    selected_codes_file: str | Path,
    final_output_dir: str | Path,
    incremental_output_dir: str | Path,
    date_column: str = "DATA_VISITA",
    start_date: Optional[str] = None,
    end_date: Optional[str]=None
) -> dict[str, str]:


    final_output_dir = Path(final_output_dir)
    incremental_output_dir = Path(incremental_output_dir)
    final_output_dir.mkdir(parents=True, exist_ok=True)
    incremental_output_dir.mkdir(parents=True, exist_ok=True)

    outputs = {}

    reduced_file = Path(reduced_file)
    if reduced_file.suffix == ".parquet":
        reduced_df = pd.read_parquet(reduced_file)
    else:
        reduced_df = pd.read_csv(reduced_file)

    reduced_df["DATA_VISITA"] = pd.to_datetime(reduced_df["DATA_VISITA"], format = "mixed", errors="coerce")
    reduced_df = reduced_df.dropna(subset=["DATA_VISITA"])

    selected_df = pd.read_csv(selected_codes_file)
    selected_codes = selected_df[selected_df.columns[0]].astype(str).str.upper().str.strip().tolist()


    for name, code_col in[
        ("chapters_cat", "ICD10_CHAPTER"),
        ("subchapters_cat", "ICD10_SUBCHAPTER"),
        ("icd10_3_cat", "ICD10_3"),
    ]:
        path = incremental_output_dir / f"{name}.parquet"
        build_grouped_long(
            reduced_df,
            code_col = code_col,
            date_column = "DATA_VISITA"
        ).to_parquet(path, index=False)
        outputs[name] = str(path)

    for prefix, group_col in [("rs", "RS"), ("up", "UP")]:
        for level_name, code_col in [
            ("chapters", "ICD10_CHAPTER"),
            ("subchapters", "ICD10_SUBCHAPTER"),
            ("icd10_3", "ICD10_3"),
        ]:
            path = incremental_output_dir / f"{level_name}_{prefix}.parquet"
            build_grouped_long(
                reduced_df,
                code_col = code_col,
                date_column = "DATA_VISITA",
                group_col = group_col,
            ).to_parquet(path, index=False)
            outputs[f"{level_name}_{prefix}"] = str(path)

    selected_cat = build_selected_wide_all_levels(
        reduced_df = reduced_df,
        selected_codes = selected_codes,
        date_column = "DATA_VISITA",
        group_col = None,
    )

    selected_rs = build_selected_wide_all_levels(
        reduced_df = reduced_df,
        selected_codes = selected_codes,
        date_column = "DATA_VISITA",
        group_col = "RS",
    )

    selected_up = build_selected_wide_all_levels(
        reduced_df = reduced_df,
        selected_codes = selected_codes,
        date_column = "DATA_VISITA",
        group_col = "UP",
    )

    selected_cat = ensure_daily_range(selected_cat, start = start_date, end = end_date, fill_value = 0)
    selected_rs = ensure_daily_range(selected_rs, start = start_date, end = end_date, fill_value = 0)
    selected_up = ensure_daily_range(selected_up, start = start_date, end = end_date, fill_value = 0)

    final_files = {
        "selected_cat": selected_cat,
        "selected_rs": selected_rs,
        "selected_up": selected_up,
    }

    
    for key, df in final_files.items():
        path_parquet = final_output_dir / f"{key}.parquet"
        path_csv = final_output_dir / f"{key}.csv"
        save_output_matrix(df, path_parquet)
        save_output_matrix(df, path_csv)
        outputs[key] = str(path_parquet)

    return outputs