# ========================
# CONFIG
# ========================

import pandas as pd 
import logging
import warnings
import gc

from .utils import *
from .config import *
from .incremental import run_incremental_diagnosis_pipeline
from .aggregation import build_final_outputs

warnings.filterwarnings(
    "ignore",
    message = "pandas only supports SQLAlchemy"
)

logging.basicConfig(
    level = logging.INFO,
    format= "%(asctime)s - %(levelname)s - %(message)s"
)

def main():

    gc.collect()

    up_rs = pd.read_excel(UP_RS_FILE, sheet_name = UP_RS_SHEET)

    run_incremental_diagnosis_pipeline(
        db_server = DB_SERVER,
        db_database = DB_DATABASE,
        schema = SCHEMA,
        table_name = TABLE_NAME,
        date_column = DATE_COLUMN,
        up_column=UP_COLUMN,
        diag_code_column=DIAG_CODE_COLUMN,
        up_rs = up_rs,
        state_file = STATE_FILE,
        reduced_output_file=REDUCED_OUTPUT_FILE,
        final_output_file=FINAL_OUTPUT_DIR,
        auth_mode = AUTH_MODE,
        min_valid_date=MIN_VALID_DATE,
    )

    outputs = build_final_outputs(
        reduced_file=REDUCED_OUTPUT_FILE,
        selected_codes_file= SELECTED_CODES_FILE,
        final_output_dir= FINAL_OUTPUT_DIR,
        incremental_output_dir = INCREMENTAL_OUTPUT_DIR,
        start_date = FINAL_START_DATE,
        end_date = FINAL_END_DATE,
        date_column = DATE_COLUMN,
    )

    clean_incremental_dir(
        INCREMENTAL_OUTPUT_DIR,
        keep_files = ["daily_up_icd10_3_counts.parquet"]
    )

    print("Diagnosis incremental pipeline finished successfully.")
    
    for name, path in outputs.items():
        print(f"{name}: {path}")
    
    gc.collect()

if __name__ == "__main__":
    main()