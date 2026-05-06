# ========================
# PIPELINE PRINCIPAL INCREMENTAL
# ========================
import pandas as pd 
from pathlib import Path 
from .db import get_connection
from .utils import *
import logging
from .transformations import prepare_visits_chunk
from .aggregation import *

def run_incremental_pipeline(
    db_server: str,
    db_database: str,
    schema: str,
    table_name: str,
    date_column: str,
    up_rs: pd.DataFrame, 
    state_file: str | Path,
    output_cat_file: str | Path,
    output_rs_file: str | Path,
    output_up_file: str | Path,
    final_cat_file: str | Path,
    auth_mode: str = "ActiveDirectoryIntegrated",
    min_valid_date: str = "2000-01-01",
) -> None:
    conn = get_connection(db_server, db_database, auth_mode = auth_mode)

    try:
        #last_loaded_date = load_state(state_file)
        #logging.info(f"Last loaded {date_column}: {last_loaded_date}")
        last_loaded_date = load_last_date_from_output(final_cat_file)
        logging.info(f"Last loaded DATA_VISITA from output: {last_loaded_date}")

        min_date, max_date = get_min_max_date(
            conn=conn,
            schema=schema,
            table_name=table_name,
            date_column=date_column,
            min_valid_date=min_valid_date,
        )    

        if min_date is None or max_date is None:
            logging.info("No valid data found in source table")
            return 

        today_date = pd.Timestamp.today().normalize()
        if max_date > today_date:
            max_date = today_date
        
        start_date = min_date if last_loaded_date is None else last_loaded_date
        logging.info(f"Range to process: {start_date} -> {max_date}")

        year_ranges = get_year_ranges(start_date, max_date)
        global_max_loaded = last_loaded_date

        for year, year_start, year_end in year_ranges:
            logging.info(f"Processing year {year}")

            df_chunk = get_data_for_year(
                conn = conn,
                schema = schema,
                table_name = table_name,
                date_column = date_column,
                year_start = year_start,
                year_end = year_end,
                last_loaded_date = last_loaded_date,
                selected_cols = ["DATA_VISITA","UP","VISI_LLOC_VISITA","VISI_SITUACIO_VISITA","SERVEI_CODI","TIPUS_CLASS","VISI_TIPUS_VISITA"]
            )

            if df_chunk.empty:
                logging.info(f"No rows found for year {year}")
                continue
    
            logging.info(f"Rows found for year {year}: {len(df_chunk)}")

            # Transformació immediate del chunk
            df_chunk = prepare_visits_chunk(df_chunk, up_rs = up_rs)

            # Matrius reduides
            cat_chunk = build_daily_total_cat(df_chunk)
            rs_chunk = build_daily_features_by_group(df_chunk, group_col = "RS")
            up_chunk = build_daily_features_by_group(df_chunk, group_col = "UP")
            
            # Merge incremental
            incremental_add_daily_matrix(cat_chunk, output_cat_file)
            incremental_add_daily_matrix(rs_chunk, output_rs_file)
            incremental_add_daily_matrix(up_chunk, output_up_file)

            chunk_max = df_chunk["DATA_VISITA"].max()
            if pd.notna(chunk_max):
                if global_max_loaded is None or chunk_max > global_max_loaded:
                    global_max_loaded = chunk_max

            del df_chunk
            del cat_chunk
            del rs_chunk
            del up_chunk

        if global_max_loaded is not None:
            save_state(state_file, global_max_loaded)
            logging.info(f"Updated last loaded date: {global_max_loaded}")

        logging.info("Incremental pipeline completed successfully.")

    finally:
        conn.close()

# ========================
# MERGE INCREMENTAL DE MATRIUS
# ========================
def incremental_add_daily_matrix(df_new: pd.DataFrame, output_file:str | Path) -> pd.DataFrame:
    output_file = Path(output_file)
    output_file.parent.mkdir(parents=True, exist_ok=True)

    df_new = df_new.copy()
    df_new.index = pd.to_datetime(df_new.index)
    df_new = df_new.apply(pd.to_numeric,errors="coerce").astype("float32")

    if output_file.exists():
        df_old = pd.read_csv(output_file, index_col=0)
        df_old.index = pd.to_datetime(df_old.index)
        df_old = df_old.apply(pd.to_numeric,errors="coerce").astype("float32")

        all_cols = sorted(set(df_old.columns).union(df_new.columns))
        df_old = df_old.reindex(columns=all_cols, fill_value=0)
        df_new = df_new.reindex(columns=all_cols, fill_value=0)

        all_idx = df_old.index.union(df_new.index)
        df_old = df_old.reindex(all_idx, fill_value=0)
        df_new = df_new.reindex(all_idx, fill_value=0)

        df_final = df_old.add(df_new, fill_value=0)
    else:
        df_final = df_new.copy()

    df_final = df_final.sort_index()
    df_final.to_csv(output_file, encoding = "utf-8-sig", index_label = "Timestamp")
    return df_final