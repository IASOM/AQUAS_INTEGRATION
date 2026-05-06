# ========================
# PIPELINE PRINCIPAL INCREMENTAL
# ========================
import pandas as pd 
from pathlib import Path 

from .utils import *
import logging
from typing import Optional
from .transformations import prepare_diagnosis_chunk
from .aggregation import incremental_add_long_counts
from .db import get_connection


def get_diagnosis_data_for_year(
    conn,
    schema: str,
    table_name: str,
    date_column: str,
    up_column: str, 
    diag_code_column: str,
    year_start: pd.Timestamp,
    year_end: pd.Timestamp,
    last_loaded_date: Optional[pd.Timestamp] = None,

) -> pd.DataFrame:

    selected_cols = [date_column, up_column, diag_code_column]
    cols_sql= ", ".join(f"[{c}]" for c in selected_cols)


    if last_loaded_date is None:
            query = f"""
            SELECT {cols_sql}
            FROM [{schema}].[{table_name}]
            WHERE [{date_column}] >= ?
                AND [{date_column}] < ?
                AND[{diag_code_column}] IS NOT NULL
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
            AND [{diag_code_column}] IS NOT NULL
        ORDER BY [{date_column}] ASC
        """
        params = [year_start, year_end, last_loaded_date]
    
    return pd.read_sql_query(query, conn, params = params)

def run_incremental_diagnosis_pipeline(
    db_server: str,
    db_database: str,
    schema: str,
    table_name: str,
    date_column: str,
    up_column: str,
    diag_code_column: str,
    up_rs: pd.DataFrame, 
    state_file: str | Path,
    reduced_output_file: str | Path,
    final_output_file: str | Path,
    auth_mode: str = "ActiveDirectoryIntegrated",
    min_valid_date: str = "2008-01-01",
) -> None:
    conn = get_connection(db_server, db_database, auth_mode = auth_mode)

    try:
        last_loaded_date = load_last_date_from_finals(final_output_file)
        logging.info(f"Last loaded date from finals: {last_loaded_date}")

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
            logging.info(f"Processing diagnosis year {year}")

            df_chunk = get_diagnosis_data_for_year(
                conn = conn,
                schema = schema,
                table_name = table_name,
                date_column = date_column,
                up_column = up_column,
                diag_code_column = diag_code_column,
                year_start = year_start,
                year_end = year_end,
                last_loaded_date = last_loaded_date,
            )

            if df_chunk.empty:
                logging.info(f"No diagnosis rows found for year {year}")
                continue
    
            logging.info(f"Diagnosis rows found for year {year}: {len(df_chunk)}")

            # Transformació immediate del chunk
            df_chunk = prepare_diagnosis_chunk(
                df_chunk, 
                up_rs = up_rs,
                date_column=date_column,
                up_column=up_column,
                diag_code_column=diag_code_column,
            )

            incremental_add_long_counts(
                df_new=df_chunk,
                output_file=reduced_output_file,
                key_cols=["DATA_VISITA", "UP", "ICD10_3", "RS", "ICD10_CHAPTER", "ICD10_SUBCHAPTER"], value_col="n"
            )

            chunk_max = df_chunk["DATA_VISITA"].max()

            if pd.notna(chunk_max) and (global_max_loaded is None or chunk_max > global_max_loaded ):
                    global_max_loaded = chunk_max

        if global_max_loaded is not None:
            save_state(state_file, global_max_loaded)
            logging.info(f"Updated diagnosis last loaded date: {global_max_loaded}")

        logging.info("Diagnosis incremental pipeline completed successfully.")

    finally:
        conn.close()