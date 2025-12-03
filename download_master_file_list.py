import httpx
import os
import polars as pl

MASTER_FILE_LIST_URL = "http://data.gdeltproject.org/gdeltv2/masterfilelist.txt"

def main():
    # Download the latest master file list
    response = httpx.get(MASTER_FILE_LIST_URL)
    if response.status_code == 200:
        with open("data/masterfilelist.txt", "wb") as f:
            f.write(response.content)
    else:
        print(f"Failed to download master file list: {response.status_code}")
        return

    # Parse the downloaded file
    new_df = pl.read_csv(
        "data/masterfilelist.txt",
        separator=" ",
        has_header=False,
        schema_overrides={
            "column_1": pl.String,
            "column_2": pl.String,
            "column_3": pl.String
        }
    )
    new_df = new_df.select([
        pl.col("column_1").alias("size"),
        pl.col("column_2").alias("md5"),
        pl.col("column_3").alias("url")
    ])
    new_df = new_df.filter(~pl.col("url").str.contains("gkg"))

    # Extract date from URL (format: YYYYMMDDHHMISS in filename)
    new_df = new_df.with_columns([
        pl.col("url")
        .str.extract(r"(\d{8})\d{6}\.", 1)
        .str.to_date("%Y%m%d")
        .alias("date")
    ])
    new_df = new_df.with_columns(pl.lit(False).alias("processed"))

    # Check if existing parquet file exists
    parquet_path = "data/masterfilelist.parquet"
    if os.path.exists(parquet_path):
        # Read existing data
        existing_df = pl.read_parquet(parquet_path)

        # Find new rows by anti-joining on URL (rows in new_df that aren't in existing_df)
        rows_to_add = new_df.join(
            existing_df.select("url"),
            on="url",
            how="anti"
        )

        if len(rows_to_add) > 0:
            # Append new rows to existing data
            combined_df = pl.concat([existing_df, rows_to_add])
            combined_df.write_parquet(parquet_path)
            print(f"Added {len(rows_to_add)} new rows to master file list")
            print(f"Total rows: {len(combined_df)}")
        else:
            print("No new rows to add")
    else:
        # First time - write the entire dataframe
        new_df.write_parquet(parquet_path)
        print(f"Created master file list with {len(new_df)} rows")
        print(new_df.head(10))

    # Clean up downloaded file
    os.remove("data/masterfilelist.txt")

if __name__ == "__main__":
    main()