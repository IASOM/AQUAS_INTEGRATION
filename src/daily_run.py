from __future__ import annotations

import argparse
import logging
import time 
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd

from demanda.config import BASE_DIR as BASE_DIR_DEMAND
from diagnosis.config import BASE_DIR as BASE_DIR_DIAGNOSIS
from demanda.main import main as run_pipeline_demanda
from diagnosis.diagnosis_main import main as run_pipeline_diagnosis

FINAL_CAT_FILE_DEMAND = BASE_DIR_DEMAND / "finals" / "demanda_CAT.csv"
FINAL_RS_FILE_DEMAND = BASE_DIR_DEMAND / "finals" / "demanda_RS.csv"
FINAL_UP_FILE_DEMAND = BASE_DIR_DEMAND / "finals" / "demanda_UP.csv"

FINAL_CAT_FILE_DIAGNOSIS = BASE_DIR_DIAGNOSIS / "finals" / "selected_cat.csv"
FINAL_RS_FILE_DIAGNOSIS = BASE_DIR_DIAGNOSIS / "finals" / "selected_rs.csv"
FINAL_UP_FILE_DIAGNOSIS = BASE_DIR_DIAGNOSIS / "finals" / "selected_up.csv"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

OUTPUT_FINAL_PARQUET_FILE =  Path("C:/Users/ghernandezgu/Desktop/PREDAP/FINAL/HISTORICAL_data.parquet")

def concat_final_files_to_parquet(output_parquet_file: Path | str) -> None:
    output_parquet_file = Path(output_parquet_file)
    output_parquet_file.parent.mkdir(parents=True, exist_ok=True)

    data_files = [Path(FINAL_CAT_FILE_DEMAND), Path(FINAL_RS_FILE_DEMAND), Path(FINAL_UP_FILE_DEMAND),Path(FINAL_CAT_FILE_DIAGNOSIS), Path(FINAL_RS_FILE_DIAGNOSIS), Path(FINAL_UP_FILE_DIAGNOSIS)]
    missing = [str(path) for path in data_files if not path.exists()]
    if missing:
        raise FileNotFoundError(
            f"Cannot concatenate parquet. Missing final files: {', '.join(missing)}"

        )

    df_cat_demanda = pd.read_csv(data_files[0], index_col="Timestamp")
    df_rs_demanda = pd.read_csv(data_files[1], index_col="Timestamp")
    df_up_demanda = pd.read_csv(data_files[2], index_col="Timestamp")

    df_cat_diag = pd.read_csv(data_files[3], index_col="Timestamp")
    df_rs_diag = pd.read_csv(data_files[4], index_col="Timestamp")
    df_up_diag = pd.read_csv(data_files[5], index_col="Timestamp")

    df_cat_demanda.index = pd.to_datetime(df_cat_demanda.index)
    df_rs_demanda.index = pd.to_datetime(df_rs_demanda.index)
    df_up_demanda.index = pd.to_datetime(df_up_demanda.index)

    df_cat_diag.index = pd.to_datetime(df_cat_diag.index)
    df_rs_diag.index = pd.to_datetime(df_rs_diag.index)
    df_up_diag.index = pd.to_datetime(df_up_diag.index)

    df_combined = pd.concat([df_cat_demanda, df_rs_demanda, df_up_demanda, df_cat_diag, df_rs_diag, df_up_diag], axis=1, join="outer")
    df_combined = df.combined.fillna(0).sort_index()
    df_combined.index.name= "Timestamp"

    try:
        import pyarrow
        engine= "pyarrow"
    except ImportError:
        engine = None
    
    if engine:
        df_combined.to_parquet(output_parquet_file, engine=engine)
    else:
        df_combined.to_parquet(output_parquet_file)

    logging.info(
        "Saved combined parquet output -> %s | shape=%s",
        output_parquet_file,
        df_copmbined.shape,
    )

def run_daily_job(output_parquet_file: Path | str) -> None:
    logging.info("Running daily pipeline job")
    run_pipeline_demanda()
    run_pipeline_diagnosis()
    concat_final_files_to_parquet(output_parquet_file)
    logging.info("Daily pipeline job complete")

def get_seconds_until_next_run(hour: int, minute:int) -> int:
    now = datetime.now()
    next_run = now.replace(hour=hour, minute=minute, second=0, microsecond=0)
    if next_run <= now:
        next_run += timedelta(days=1)
    return int((next_run - now).total_seconds())

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Run the main pipeline daily at a fixed hour and write a combined parquet file."
    )
    parser.add_argument(
        "--hour",
        type=int,
        default=2,
        help="Hour of day to run the pipeline (0-23). Default: 2",
    )
    parser.add_argument(
        "--minute",
        type=int,
        default=0,
        help="Minute of hour to run the pipeline. Default: 0",
    )
    parser.add_argument(
        "--once",
        action="store_true",
        help="Run the pipeline once immediately (or as test) and then exit.",
    )
    args = parser.parse_args()

    if args.once or args.run_now:
        run_daily_job(OUTPUT_FINAL_PARQUET_FILE)
        return
        
    while True:
        seconds = get_seconds_until_next_run(args.hour, args.minute)
        logging.info(
            "Next run scheduled ar %02d:%02d (in %d seconds)",
            args.hour,
            args.minute,
            seconds,
        )
        time.sleep(seconds)
        try:
            run_daily_job(OUTPUT_FINAL_PARQUET_FILE)
        except Exception as exc:
            logging.exception("Daily pipeline job failed: %s", exc)
            logging.info("Will retry at next scheduled time.")

if __name__ == "__main__":
    main()