# ========================
# CONFIG
# ========================

from .config import *
from .db import get_connection
from .incremental import run_incremental_pipeline
from .aggregation import build_final_outputs
from .transformations import prepare_visits_chunk
import pandas as pd 
import logging
import warnings
from .utils import *

warnings.filterwarnings(
    "ignore",
    message = "pandas only supports SQLAlchemy"
)

logging.basicConfig(
    level = logging.INFO,
    format= "%(asctime)s - %(levelname)s - %(message)s"
)

def main():
    up_rs = pd.read_excel(UP_RS_FILE, sheet_name = UP_RS_SHEET)

    run_incremental_pipeline(
        db_server = DB_SERVER,
        db_database = DB_DATABASE,
        schema = SCHEMA,
        table_name = TABLE_NAME,
        date_column = DATE_COLUMN,
        up_rs = up_rs,
        state_file = STATE_FILE,
        output_cat_file = OUTPUT_CAT_FILE,
        output_rs_file = OUTPUT_RS_FILE,
        output_up_file = OUTPUT_UP_FILE,
        final_cat_file = FINAL_CAT_FILE,
        auth_mode = AUTH_MODE,
        min_valid_date=MIN_VALID_DATE,
    )

    build_final_outputs(
        output_cat_file = OUTPUT_CAT_FILE,
        output_rs_file = OUTPUT_RS_FILE,
        output_up_file = OUTPUT_UP_FILE,
        final_cat_file = FINAL_CAT_FILE,
        final_rs_file = FINAL_RS_FILE,
        final_up_file = FINAL_UP_FILE,
        start_date = FINAL_START_DATE,
        end_date = FINAL_END_DATE,
    )

    save_state(STATE_FILE, pd.to_datetime(FINAL_END_DATE))

    print("Incremental demand pipeline finished successfully.")

if __name__ == "__main__":
    main()