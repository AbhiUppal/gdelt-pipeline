import argparse
import os
import io
from datetime import date
from dotenv import load_dotenv
import logging
import polars as pl
import psycopg
from psycopg import sql
from download_events_v2 import get_dates_to_process
from process_events import EVENTS_SCHEMA

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    filename="logs/process_events.log"
)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

# Database connection parameters
DB_CONFIG = {
    "dbname": os.getenv("PG_DB"),
    "user": os.getenv("PG_USER"),
    "password": os.getenv("PG_PASS"),
    "host": os.getenv("PG_HOST"),
    "port": os.getenv("PG_PORT"),
}


# PostgreSQL type mapping from Polars types
POLARS_TO_POSTGRES = {
    pl.Int64: "BIGINT",
    pl.Int32: "INTEGER",
    pl.Float64: "DOUBLE PRECISION",
    pl.Utf8: "TEXT",
}


def get_postgres_type(polars_type) -> str:
    """Map Polars data type to PostgreSQL data type."""
    return POLARS_TO_POSTGRES.get(polars_type, "TEXT")


def create_events_table(conn):
    """
    Create the events table if it doesn't exist, with proper indexes.

    Args:
        conn: psycopg connection object
    """
    logger.info("Creating/Verifying Events Table")

    # Build column definitions from schema
    columns = []
    for col_name, polars_type in EVENTS_SCHEMA.items():
        pg_type = get_postgres_type(polars_type)
        if col_name == "GlobalEventID":
            columns.append(f'"{col_name}" {pg_type} PRIMARY KEY')
        else:
            columns.append(f'"{col_name}" {pg_type}')

    columns_sql = ",\n    ".join(columns)

    create_table_sql = f"""
    CREATE TABLE IF NOT EXISTS events (
        {columns_sql}
    );
    """

    with conn.cursor() as cur:
        cur.execute(create_table_sql)
        conn.commit()
        logger.info("Events table created/verified")

        # Create indexes
        indexes = [
            ('idx_events_day', 'Day'),
            ('idx_events_monthyear', 'MonthYear'),
            ('idx_events_actor1country', 'Actor1CountryCode'),
            ('idx_events_actor2country', 'Actor2CountryCode'),
            ('idx_events_eventcode', 'EventCode'),
        ]

        logger.info("Creating Indexes")
        for idx_name, col_name in indexes:
            try:
                cur.execute(
                    sql.SQL("CREATE INDEX IF NOT EXISTS {} ON events ({})").format(
                        sql.Identifier(idx_name),
                        sql.Identifier(col_name)
                    )
                )
                logger.debug(f"Index {idx_name} on {col_name}")
            except Exception as e:
                logger.error(f"Failed to create index {idx_name}: {e}")

        conn.commit()
        logger.info("All indexes created/verified")


def delete_events_for_date(conn, process_date: date) -> int:
    """
    Delete all events that were added on a specific date (DATEADDED field).

    Note: Events are deleted by DATEADDED, not by Day. The Day field represents
    when the event actually occurred, while DATEADDED represents when it was
    added to GDELT. A single file (e.g., 20240101.parquet) contains events from
    many different dates that were all added to GDELT on 2024-01-01.

    Args:
        conn: psycopg connection object
        process_date: Date to delete events for (matches DATEADDED prefix)

    Returns:
        Number of rows deleted
    """
    # DATEADDED format is YYYYMMDDHHMMSS
    # We want to delete all events where DATEADDED starts with YYYYMMDD
    dateadded_start = int(process_date.strftime("%Y%m%d") + "000000")
    dateadded_end = int(process_date.strftime("%Y%m%d") + "235959")

    with conn.cursor() as cur:
        cur.execute(
            'DELETE FROM events WHERE "DATEADDED" >= %s AND "DATEADDED" <= %s',
            (dateadded_start, dateadded_end)
        )
        deleted_count = cur.rowcount
        conn.commit()

    return deleted_count


def load_parquet_to_db(conn, parquet_path: str, process_date: date) -> int:
    """
    Load a parquet file into the events table using COPY.

    Args:
        conn: psycopg connection object
        parquet_path: Path to parquet file
        process_date: Date being processed

    Returns:
        Number of rows inserted
    """
    # Read parquet file
    df = pl.read_parquet(parquet_path)
    total_rows = len(df)

    logger.info(f"Loaded {total_rows} rows from parquet")

    # Process in batches of 10,000
    batch_size = 10_000
    total_inserted = 0

    for batch_start in range(0, total_rows, batch_size):
        batch_end = min(batch_start + batch_size, total_rows)
        batch_df = df.slice(batch_start, batch_end - batch_start)

        # Convert to CSV format in memory for COPY
        # Replace None/null with \N (PostgreSQL NULL indicator)
        csv_buffer = io.StringIO()
        batch_df.write_csv(csv_buffer, separator="\t", include_header=False, null_value="\\N")
        csv_buffer.seek(0)

        # Use COPY to load data
        with conn.cursor() as cur:
            # Get column names in order
            columns = list(EVENTS_SCHEMA.keys())
            columns_sql = sql.SQL(", ").join(map(sql.Identifier, columns))

            with cur.copy(
                sql.SQL("COPY events ({}) FROM STDIN").format(columns_sql)
            ) as copy:
                while True:
                    data = csv_buffer.read(8192)
                    if not data:
                        break
                    copy.write(data)

        batch_inserted = batch_end - batch_start
        total_inserted += batch_inserted

        if total_rows > batch_size:
            logger.debug(f"Inserted batch: {batch_start + 1}-{batch_end} ({batch_inserted} rows)")

    conn.commit()
    return total_inserted


def process_date_load(conn, process_date: date, force: bool = False) -> bool:
    """
    Load events for a specific date into the database.

    Args:
        conn: psycopg connection object
        process_date: Date to process
        force: If True, delete existing data for this date first

    Returns:
        True if successful, False otherwise
    """
    # Build parquet file path
    year_dir = os.path.join("data", "events2", str(process_date.year))
    month_dir = os.path.join(year_dir, f"{process_date.month:02d}")
    day_dir = os.path.join(month_dir, f"{process_date.day:02d}")
    parquet_filename = f"{process_date.strftime('%Y%m%d')}.parquet"
    parquet_path = os.path.join(day_dir, parquet_filename)

    # Check if parquet file exists
    if not os.path.exists(parquet_path):
        logger.error(f"Parquet file not found: {parquet_path}")
        return False

    logger.info(f"Found parquet file: {parquet_path}")

    try:
        # Delete existing data if force mode
        if force:
            logger.info("Force mode: deleting existing data...")
            deleted_count = delete_events_for_date(conn, process_date)
            logger.info(f"Deleted {deleted_count:,} existing row(s)")
        else:
            # Check if data already exists (by DATEADDED, not Day)
            dateadded_start = int(process_date.strftime("%Y%m%d") + "000000")
            dateadded_end = int(process_date.strftime("%Y%m%d") + "235959")
            with conn.cursor() as cur:
                cur.execute(
                    'SELECT COUNT(*) FROM events WHERE "DATEADDED" >= %s AND "DATEADDED" <= %s',
                    (dateadded_start, dateadded_end)
                )
                existing_count = cur.fetchone()[0]
                if existing_count > 0:
                    logger.warning(f"Data already exists for {process_date} ({existing_count:,} rows)")
                    logger.warning(f"Use --force to replace existing data")
                    return False

        # Load data
        logger.info("Loading data into database...")
        inserted_count = load_parquet_to_db(conn, parquet_path, process_date)
        logger.info(f"Inserted {inserted_count:,} row(s)")

        return True

    except Exception as e:
        logger.error(f"Error loading data: {e}", exc_info=True)
        conn.rollback()
        return False


def main():
    parser = argparse.ArgumentParser(
        description="Load GDELT events from parquet files into PostgreSQL database"
    )
    parser.add_argument(
        "-d",
        "--dates",
        nargs="*",
        help="Dates to load in YYYY, YYYY-MM, or YYYY-MM-DD format. Defaults to yesterday if not specified.",
    )
    parser.add_argument(
        "-f",
        "--force",
        action="store_true",
        help="Force reload: delete existing data for specified dates before loading",
    )

    args = parser.parse_args()

    logger.info("=" * 60)
    logger.info("GDELT Events Database Loader")
    logger.info("=" * 60)

    dates = get_dates_to_process(args.dates)

    if args.force:
        logger.warning("FORCE MODE: Will replace existing data")

    logger.info(f"Processing {len(dates)} date(s):")
    for d in dates:
        logger.info(f"  • {d}")

    # Connect to database
    logger.info("Connecting to Database")
    try:
        conn = psycopg.connect(**DB_CONFIG)
        logger.info(f"Connected to {DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['dbname']}")
    except Exception as e:
        logger.error(f"Failed to connect to database: {e}", exc_info=True)
        return

    try:
        # Create table and indexes
        create_events_table(conn)

        # Process each date
        logger.info("=" * 60)
        logger.info("LOADING DATA")
        logger.info("=" * 60)

        success_count = 0
        fail_count = 0

        for process_date in dates:
            logger.info(f"--- {process_date} ---")
            if process_date_load(conn, process_date, force=args.force):
                success_count += 1
            else:
                fail_count += 1

        # Summary
        logger.info("=" * 60)
        logger.info("SUMMARY")
        logger.info("=" * 60)
        logger.info(f"Successfully loaded: {success_count}")
        logger.info(f"Failed or skipped:   {fail_count}")
        logger.info("=" * 60)

    finally:
        conn.close()
        logger.info("Database connection closed")


if __name__ == "__main__":
    main()
