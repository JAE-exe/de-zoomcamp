import click
import pandas as pd
from sqlalchemy import create_engine
from tqdm.auto import tqdm

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
    "congestion_surcharge": "float64",
}

PARSE_DATES = [
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime"
]


@click.command()
@click.option("--pg-user", default="root", help="PostgreSQL user")
@click.option("--pg-pass", default="root", help="PostgreSQL password")
@click.option("--pg-host", default="localhost", help="PostgreSQL host")
@click.option("--pg-port", default=5432, type=int, help="PostgreSQL port")
@click.option("--pg-db", default="ny_taxi", help="PostgreSQL database name")
@click.option("--year", required=True, type=int, help="Year of the data")
@click.option("--month", required=True, type=int, help="Month of the data")
@click.option("--target-table", default="yellow_taxi_data", help="Target table name")
@click.option("--chunksize", default=100_000, type=int, help="CSV chunk size")
def run(
    pg_user,
    pg_pass,
    pg_host,
    pg_port,
    pg_db,
    year,
    month,
    target_table,
    chunksize,
):
    """Ingest NYC Yellow Taxi data into PostgreSQL."""

    base_url = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow"
    csv_url = f"{base_url}/yellow_tripdata_{year}-{month:02d}.csv.gz"

    engine = create_engine(
        f"postgresql+psycopg://{pg_user}:{pg_pass}@{pg_host}:{pg_port}/{pg_db}"
    )

    print(f"Downloading data from: {csv_url}")
    print(f"Inserting into table: {target_table}")

    df_iter = pd.read_csv(
        csv_url,
        dtype=DTYPE,
        parse_dates=PARSE_DATES,
        iterator=True,
        chunksize=chunksize,
    )

    first = True

    for df_chunk in tqdm(df_iter, desc="Ingesting chunks"):
        if first:
            df_chunk.head(0).to_sql(
                name=target_table,
                con=engine,
                if_exists="replace",
            )
            first = False

        df_chunk.to_sql(
            name=target_table,
            con=engine,
            if_exists="append",
        )

    print("Ingestion completed successfully")


if __name__ == "__main__":
    run()
