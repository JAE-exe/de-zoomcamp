#!/usr/bin/env python

import pandas as pd
from sqlalchemy import create_engine
from tqdm.auto import tqdm
from io import StringIO
import argparse
import logging
import time


# --------------------------------------------------
# LOGGING
# --------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

logger = logging.getLogger(__name__)


# --------------------------------------------------
# DATA TYPES
# --------------------------------------------------

DTYPE = {
    "VendorID": "Int64",
    "passenger_count": "Int64",
    "trip_distance": "float64",
    "RatecodeID": "Int64",
    "store_and_fwd_flag": "string",
    "PULocationID": "Int64",
    "DOLocationID": "Int64",
    "payment_type": "Int64",
    "fare_amount": "float64",
    "extra": "float64",
    "mta_tax": "float64",
    "tip_amount": "float64",
    "tolls_amount": "float64",
    "improvement_surcharge": "float64",
    "total_amount": "float64",
    "congestion_surcharge": "float64"
}

PARSE_DATES = [
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime"
]


# --------------------------------------------------
# COPY FUNCTION
# --------------------------------------------------

def copy_to_postgres(df, table_name, conn):

    buffer = StringIO()

    df.to_csv(buffer, index=False, header=False)
    buffer.seek(0)

    cursor = conn.cursor()

    cursor.copy_expert(
        sql=f"COPY {table_name} FROM STDIN WITH CSV",
        file=buffer
    )

    cursor.close()


# --------------------------------------------------
# INGESTION FUNCTION
# --------------------------------------------------

def ingest_data(params):

    prefix = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/"

    file_url = f"{prefix}yellow_tripdata_{params.year}-{params.month:02d}.csv.gz"

    logger.info(f"Downloading from: {file_url}")

    engine = create_engine(
        f"postgresql://{params.pg_user}:{params.pg_pass}"
        f"@{params.pg_host}:{params.pg_port}/{params.pg_db}"
    )

    # ---------------------------
    # Create Schema
    # ---------------------------

    logger.info("Creating schema...")

    df_schema = pd.read_csv(file_url, nrows=0)

    df_schema.to_sql(
        name=params.target_table,
        con=engine,
        if_exists="replace",
        index=False
    )

    logger.info("Schema created")

    # ---------------------------
    # Iterator
    # ---------------------------

    df_iter = pd.read_csv(
        file_url,
        dtype=DTYPE,
        parse_dates=PARSE_DATES,
        chunksize=100_000
    )

    conn = engine.raw_connection()

    total_rows = 0
    start_time = time.time()

    logger.info("Starting ingestion...")

    try:

        for i, df_chunk in enumerate(tqdm(df_iter, desc="Ingesting")):

            chunk_start = time.time()

            copy_to_postgres(df_chunk, params.target_table, conn)

            conn.commit()

            rows = len(df_chunk)
            total_rows += rows

            elapsed = time.time() - chunk_start

            logger.info(
                f"Chunk {i+1} → {rows:,} rows "
                f"| {rows/elapsed:,.0f} rows/sec"
            )

    except Exception as e:
        logger.error(f"Ingestion failed: {e}")
        conn.rollback()
        raise

    finally:
        conn.close()

    logger.info(f"Total rows inserted: {total_rows:,}")
    logger.info(f"Total time: {time.time() - start_time:.2f} sec")


# --------------------------------------------------
# CLI PARAMETERS
# --------------------------------------------------

def get_args():

    parser = argparse.ArgumentParser()

    parser.add_argument("--year", type=int, required=True)
    parser.add_argument("--month", type=int, required=True)

    parser.add_argument("--pg-user", required=True)
    parser.add_argument("--pg-pass", required=True)
    parser.add_argument("--pg-host", required=True)
    parser.add_argument("--pg-port", type=int, required=True)
    parser.add_argument("--pg-db", required=True)

    parser.add_argument("--target-table", required=True)

    return parser.parse_args()


# --------------------------------------------------
# MAIN
# --------------------------------------------------

if __name__ == "__main__":

    args = get_args()

    ingest_data(args)
