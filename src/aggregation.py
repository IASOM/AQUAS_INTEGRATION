# ========================
# AGREGACIÓ A MATRIUS DIARIES
# ========================
import pandas as pd 
from typing import Optional
from pathlib import Path 
from utils import *
import logging

def build_daily_features_by_group(
    df: pd.DataFrame,
    group_col: str,
    date_column: str = "DATA_VISITA",
    value_col: str = "counts",
    vars_cols: Optional[list[str]] = None,
    prefix: str = "demanda",
) -> pd.DataFrame:
    if vars_cols is None:
        vars_cols = [
            "VISI_LLOC_VISITA",
            "VISI_SITUACIO_VISITA",
            "SERVEI_CODI",
            "TIPUS_CLASS",
            "TIPUS_VISITA_AGRUPAT",
        ]
        
    d = df.copy()
    d[date_column] = pd.to_datetime(d[date_column], errors = "coerce").dt.floor("D")
    d = d.dropna(subset=[date_column])

    pieces = []

    # 1) variables per grup
    for v in vars_cols:
        tmp = d[[date_column, group_col, v, value_col]].copy()

        tmp[group_col] = (
            tmp[group_col]
            .fillna("NA")
            .astype(str)
            .str.strip()
            .replace("", "NA")
        )
        tmp[v] = (
            tmp[v]
            .fillna("NA")
            .astype(str)
            .str.strip()
            .replace("", "NA")
        )

        tmp["feature"] = (
            tmp[v].astype(str)
            .str.cat(tmp[group_col].astype(str), sep="_")
            .radd(prefix + "_" + v + "_")
        )
        tmp = tmp.groupby([date_column, "feature"], as_index=False)[value_col].sum()
        
        pieces.append(tmp)


    #2) un unic total per grup
    tmp_total = d[[date_column, group_col, value_col]].copy()
    tmp_total[group_col] = tmp_total[group_col].astype(str).str.strip()

    tmp_total["feature"] = prefix + "__TOTAL_" + group_col + "_" + tmp_total[group_col]
    tmp_total = tmp_total.groupby([date_column, "feature"], as_index=False)[value_col].sum()

    pieces.append(tmp_total)
    
    long_df = pd.concat(pieces, ignore_index=True)

    wide = (
        long_df
        .pivot_table(index=date_column, columns="feature", values=value_col, aggfunc="sum",fill_value=0, )
        .fillna(0)
        .sort_index()
    )

    wide = wide.groupby(level=0).sum()
    wide.index = pd.to_datetime(wide.index)
    
    return wide


def build_daily_total_cat(
    df: pd.DataFrame,
    date_column: str = "DATA_VISITA",
    value_col: str = "counts",
    vars_cols: Optional[list[str]] = None,
    prefix:str = "demanda",
) -> pd.DataFrame:

    if vars_cols is None:
         vars_cols = [
            "VISI_LLOC_VISITA",
            "VISI_SITUACIO_VISITA",
            "SERVEI_CODI",
            "TIPUS_CLASS",
            "TIPUS_VISITA_AGRUPAT",
        ]

    out = df.copy()
    
    out[date_column] = pd.to_datetime(out[date_column], errors = "coerce").dt.floor("D")
    out = out.dropna(subset=[date_column])

    pieces = []

    # 1) Total CAT
    tmp_total = out[[date_column, value_col]].copy()
    tmp_total["feature"] = prefix + "__TOTAL_CAT"
    tmp_total = tmp_total.groupby([date_column, "feature"], as_index=False)[value_col].sum()
    pieces.append(tmp_total)
    
    # 2) variables per grup
    for v in vars_cols:
        tmp = out[[date_column, v, value_col]].copy()
        tmp[v] = (
            tmp[v]
            .fillna("NA")
            .astype(str)
            .str.strip()
            .replace("", "NA")
        )

        tmp["feature"] = prefix + "__" + v + "__" + tmp[v]
        tmp = tmp.groupby([date_column, "feature"], as_index=False)[value_col].sum()
        
        pieces.append(tmp)

    long_df = pd.concat(pieces, ignore_index=True)
    
    wide = (
        long_df
        .pivot(index=date_column, columns="feature", values=value_col)
        .fillna(0)
        .sort_index()
    )

    wide = wide.groupby(level=0).sum()
    wide.index = pd.to_datetime(wide.index)

    return wide

def build_final_outputs(
    output_cat_file: str | Path,
    output_rs_file: str | Path,
    output_up_file: str | Path,
    final_cat_file: str | Path,
    final_rs_file: str | Path,
    final_up_file: str | Path,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
) -> None:
    df_cat = load_output_matrix(output_cat_file)
    df_rs = load_output_matrix(output_rs_file)
    df_up = load_output_matrix(output_up_file)
    
    df_cat = ensure_daily_range(df_cat, start=start_date, end=end_date, fill_value=0)
    df_rs = ensure_daily_range(df_rs, start=start_date, end=end_date, fill_value=0)
    df_up = ensure_daily_range(df_up, start=start_date, end=end_date, fill_value=0)

    save_output_matrix(df_cat, final_cat_file)
    save_output_matrix(df_rs, final_rs_file)
    save_output_matrix(df_up, final_up_file)

    logging.info(f"Saved final CAT dataset -> {final_cat_file} | shape={df_cat.shape}")
    logging.info(f"Saved final RS dataset -> {final_rs_file} | shape={df_rs.shape}")
    logging.info(f"Saved final UP dataset -> {final_up_file} | shape={df_up.shape}")
