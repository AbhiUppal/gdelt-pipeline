import argparse
import logging
import os
import polars as pl
from datetime import date
from download_events_v2 import get_dates_to_process

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    filename="logs/process_events.log"
)
logger = logging.getLogger(__name__)


# GDELT 2.0 Events schema based on the official codebook
EVENTS_SCHEMA = {
    # Event ID and Date Attributes
    "GlobalEventID": pl.Int64,
    "Day": pl.Int32,
    "MonthYear": pl.Int32,
    "Year": pl.Int32,
    "FractionDate": pl.Float64,
    # Actor1 Attributes
    "Actor1Code": pl.Utf8,
    "Actor1Name": pl.Utf8,
    "Actor1CountryCode": pl.Utf8,
    "Actor1KnownGroupCode": pl.Utf8,
    "Actor1EthnicCode": pl.Utf8,
    "Actor1Religion1Code": pl.Utf8,
    "Actor1Religion2Code": pl.Utf8,
    "Actor1Type1Code": pl.Utf8,
    "Actor1Type2Code": pl.Utf8,
    "Actor1Type3Code": pl.Utf8,
    # Actor2 Attributes
    "Actor2Code": pl.Utf8,
    "Actor2Name": pl.Utf8,
    "Actor2CountryCode": pl.Utf8,
    "Actor2KnownGroupCode": pl.Utf8,
    "Actor2EthnicCode": pl.Utf8,
    "Actor2Religion1Code": pl.Utf8,
    "Actor2Religion2Code": pl.Utf8,
    "Actor2Type1Code": pl.Utf8,
    "Actor2Type2Code": pl.Utf8,
    "Actor2Type3Code": pl.Utf8,
    # Event Action Attributes
    "IsRootEvent": pl.Int32,
    "EventCode": pl.Utf8,
    "EventBaseCode": pl.Utf8,
    "EventRootCode": pl.Utf8,
    "QuadClass": pl.Int32,
    "GoldsteinScale": pl.Float64,
    "NumMentions": pl.Int32,
    "NumSources": pl.Int32,
    "NumArticles": pl.Int32,
    "AvgTone": pl.Float64,
    # Actor1 Geography
    "Actor1Geo_Type": pl.Int32,
    "Actor1Geo_Fullname": pl.Utf8,
    "Actor1Geo_CountryCode": pl.Utf8,
    "Actor1Geo_ADM1Code": pl.Utf8,
    "Actor1Geo_ADM2Code": pl.Utf8,
    "Actor1Geo_Lat": pl.Float64,
    "Actor1Geo_Long": pl.Float64,
    "Actor1Geo_FeatureID": pl.Utf8,
    # Actor2 Geography
    "Actor2Geo_Type": pl.Int32,
    "Actor2Geo_Fullname": pl.Utf8,
    "Actor2Geo_CountryCode": pl.Utf8,
    "Actor2Geo_ADM1Code": pl.Utf8,
    "Actor2Geo_ADM2Code": pl.Utf8,
    "Actor2Geo_Lat": pl.Float64,
    "Actor2Geo_Long": pl.Float64,
    "Actor2Geo_FeatureID": pl.Utf8,
    # Action Geography
    "ActionGeo_Type": pl.Int32,
    "ActionGeo_Fullname": pl.Utf8,
    "ActionGeo_CountryCode": pl.Utf8,
    "ActionGeo_ADM1Code": pl.Utf8,
    "ActionGeo_ADM2Code": pl.Utf8,
    "ActionGeo_Lat": pl.Float64,
    "ActionGeo_Long": pl.Float64,
    "ActionGeo_FeatureID": pl.Utf8,
    # Data Management
    "DATEADDED": pl.Int64,
    "SOURCEURL": pl.Utf8,
}


def process_day(process_date: date, force: bool = False) -> bool:
    """
    Process all CSV files for a single day into a single parquet file.

    Args:
        process_date: Date to process
        force: If True, reprocess even if parquet file already exists

    Returns:
        True if processing was successful, False otherwise
    """
    # Create directory path
    year_dir = os.path.join("data", "events2", str(process_date.year))
    month_dir = os.path.join(year_dir, f"{process_date.month:02d}")
    day_dir = os.path.join(month_dir, f"{process_date.day:02d}")

    # Check if directory exists
    if not os.path.exists(day_dir):
        logger.warning(f"Directory does not exist: {day_dir}")
        return False

    # Define output parquet file path
    parquet_filename = f"{process_date.strftime('%Y%m%d')}.parquet"
    parquet_path = os.path.join(day_dir, parquet_filename)

    # Check if parquet file already exists
    if os.path.exists(parquet_path) and not force:
        logger.info(f"Parquet file already exists: {parquet_path} (use --force to reprocess)")
        return False

    # Find all CSV files in the directory
    csv_files = [
        os.path.join(day_dir, f)
        for f in os.listdir(day_dir)
        if f.endswith(".export.CSV")
    ]

    if not csv_files:
        logger.warning(f"No CSV files found in {day_dir}")
        return False

    logger.info(f"Processing {len(csv_files)} CSV file(s) for {process_date}...")

    try:
        # Read all CSV files and concatenate them
        # GDELT files are tab-delimited with no header
        dataframes = []
        for csv_file in sorted(csv_files):
            logger.debug(f"Reading {os.path.basename(csv_file)}")
            df = pl.read_csv(
                csv_file,
                separator="\t",
                has_header=False,
                new_columns=list(EVENTS_SCHEMA.keys()),
                schema_overrides=EVENTS_SCHEMA,
                truncate_ragged_lines=True,
            )
            dataframes.append(df)

        # Concatenate all dataframes
        combined_df = pl.concat(dataframes)

        logger.info(f"Combined {len(combined_df)} total events")

        # Write to parquet
        combined_df.write_parquet(parquet_path, compression="snappy")
        logger.info(f"Wrote parquet file: {parquet_path}")

        # Delete CSV files after successful write
        for csv_file in csv_files:
            os.remove(csv_file)
        logger.info(f"Deleted {len(csv_files)} CSV file(s)")

        return True

    except Exception as e:
        logger.error(f"Error processing {process_date}: {e}", exc_info=True)
        return False


def main():
    parser = argparse.ArgumentParser(
        description="Process GDELT events CSV files into daily parquet files"
    )
    parser.add_argument(
        "-d",
        "--dates",
        nargs="*",
        help="Dates to process in YYYY, YYYY-MM, or YYYY-MM-DD format. Defaults to yesterday if not specified.",
    )
    parser.add_argument(
        "-f",
        "--force",
        action="store_true",
        help="Force reprocessing even if parquet files already exist",
    )

    args = parser.parse_args()

    logger.info("Starting GDELT events processing")
    dates = get_dates_to_process(args.dates)

    if args.force:
        logger.warning("Force mode: reprocessing all dates")
    else:
        logger.info(f"Processing {len(dates)} date(s):")
        for d in dates:
            logger.info(f"  {d}")

    # Process each date
    success_count = 0
    fail_count = 0

    for process_date in dates:
        logger.info(f"--- Processing {process_date} ---")
        if process_day(process_date, force=args.force):
            success_count += 1
        else:
            fail_count += 1

    # Summary
    logger.info("=== Summary ===")
    logger.info(f"Successfully processed: {success_count}")
    logger.info(f"Failed or skipped: {fail_count}")


if __name__ == "__main__":
    main()
