import argparse
import asyncio
import calendar
from datetime import date, timedelta
import httpx
import polars as pl
import os
import re
import zipfile


def parse_date_argument(date_str: str) -> list[date]:
    """
    Parse a date argument and return a list of date objects.

    Args:
        date_str: Date string in format "YYYY", "YYYY-MM", or "YYYY-MM-DD"

    Returns:
        List of date objects

    Raises:
        ValueError: If date_str is in an invalid format
    """
    # Regex patterns for each date format
    year_pattern = re.compile(r'^\d{4}$')
    month_pattern = re.compile(r'^\d{4}-\d{2}$')
    day_pattern = re.compile(r'^\d{4}-\d{2}-\d{2}$')

    if year_pattern.match(date_str):
        # Year format: YYYY
        year = int(date_str)
        start_date = date(year, 1, 1)
        end_date = date(year, 12, 31)

        dates = []
        current = start_date
        while current <= end_date:
            dates.append(current)
            current += timedelta(days=1)
        return dates

    elif month_pattern.match(date_str):
        # Month format: YYYY-MM
        year, month = map(int, date_str.split('-'))

        # Get the last day of the month
        _, last_day = calendar.monthrange(year, month)

        start_date = date(year, month, 1)
        end_date = date(year, month, last_day)

        dates = []
        current = start_date
        while current <= end_date:
            dates.append(current)
            current += timedelta(days=1)
        return dates

    elif day_pattern.match(date_str):
        # Day format: YYYY-MM-DD
        year, month, day = map(int, date_str.split('-'))
        return [date(year, month, day)]

    else:
        raise ValueError(f"Invalid date format: {date_str}. Expected YYYY, YYYY-MM, or YYYY-MM-DD")


def get_dates_to_process(date_args: list[str] | None = None) -> list[date]:
    """
    Process date arguments and return a sorted list of unique dates.

    Args:
        date_args: List of date strings, or None to default to yesterday

    Returns:
        Sorted list of date objects (earliest first)
    """
    if not date_args:
        # Default to yesterday
        yesterday = date.today() - timedelta(days=1)
        return [yesterday]

    # Use a set to collect unique dates
    dates_set = set()

    for date_arg in date_args:
        parsed_dates = parse_date_argument(date_arg)
        dates_set.update(parsed_dates)

    # Convert to sorted list (earliest first)
    return sorted(dates_set)

async def _download_event_file(url: str, file_date: date, max_retries: int = 3):
    """Download a file with retry logic."""
    filename = url.split("/")[-1]

    # Create directory structure: data/events/YYYY/MM/DD/
    year_dir = os.path.join("data", "events", str(file_date.year))
    month_dir = os.path.join(year_dir, f"{file_date.month:02d}")
    day_dir = os.path.join(month_dir, f"{file_date.day:02d}")
    os.makedirs(day_dir, exist_ok=True)

    zip_path = os.path.join(day_dir, filename)

    for attempt in range(max_retries):
        try:
            async with httpx.AsyncClient(timeout=30.0) as client:
                response = await client.get(url)

                if response.status_code == 200:
                    # Write the zip file
                    with open(zip_path, "wb") as f:
                        f.write(response.content)

                    # Extract the zip file
                    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                        zip_ref.extractall(day_dir)
                    os.remove(zip_path)
                    return
                else:
                    error_msg = f"Failed to download {url}: HTTP {response.status_code}"
                    if attempt < max_retries - 1:
                        print(f"{error_msg}, retrying in 1s... (attempt {attempt + 1}/{max_retries})")
                        await asyncio.sleep(1)
                    else:
                        raise ValueError(error_msg)
        except Exception as e:
            if attempt < max_retries - 1:
                print(f"Error downloading {url}: {e}, retrying in 1s... (attempt {attempt + 1}/{max_retries})")
                await asyncio.sleep(1)
            else:
                raise


async def download_events(dates: list[date], master_list: pl.DataFrame, force: bool = False):
    """
    Download GDELT event files for the specified dates.

    Args:
        dates: List of dates to download
        master_list: Master file list dataframe
        force: If True, ignore date and processed filters
    """
    # Filter master list to only export files
    files_to_download = master_list.filter(pl.col("url").str.contains("export.CSV.zip"))

    if not force:
        # Filter by dates
        files_to_download = files_to_download.filter(pl.col("date").is_in(dates))

        # Filter to only unprocessed files
        files_to_download = files_to_download.filter(pl.col("processed") == False)

    if len(files_to_download) == 0:
        print("No files to download")
        return

    print(f"Downloading {len(files_to_download)} file(s)...")

    # Create download tasks for all files
    tasks = []
    for i, row in enumerate(files_to_download.iter_rows(named=True), 1):
        url = row["url"]
        file_date = row["date"]
        filename = url.split("/")[-1]

        print(f"[{i}/{len(files_to_download)}] Queuing {filename}...")
        tasks.append(_download_event_file(url, file_date))

    # Download all files concurrently
    await asyncio.gather(*tasks)

    print("Download complete!")


def main():
    parser = argparse.ArgumentParser(
        description="Download GDELT events data for specified dates"
    )
    parser.add_argument(
        '-d', '--dates',
        nargs='*',
        help='Dates to process in YYYY, YYYY-MM, or YYYY-MM-DD format. Defaults to yesterday if not specified.'
    )
    parser.add_argument(
        '-f', '--force',
        action='store_true',
        help='Force download all files, ignoring date and processed filters'
    )

    args = parser.parse_args()
    dates = get_dates_to_process(args.dates)

    master_list = pl.read_parquet("data/masterfilelist.parquet")

    if not args.force:
        print(f"Processing {len(dates)} date(s):")
        for d in dates:
            print(f"  {d}")
    else:
        print("Force mode: downloading all available export files")

    asyncio.run(download_events(dates, master_list, force=args.force))


if __name__ == "__main__":
    main()
