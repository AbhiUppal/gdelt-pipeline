import httpx
import os
import polars as pl

MASTER_FILE_LIST_URL = "http://data.gdeltproject.org/gdeltv2/masterfilelist.txt"

def main():
    if not os.path.exists("data/masterfilelist.txt"):
        response = httpx.get(MASTER_FILE_LIST_URL)
        if response.status_code == 200:
            with open("data/masterfilelist.txt", "wb") as f:
                f.write(response.content)
        else:
            print(f"Failed to download master file list: {response.status_code}")

    df = pl.read_csv(
        "data/masterfilelist.txt",
        separator=" ",
        has_header=False,
        schema_overrides={
            "column_1": pl.String,
            "column_2": pl.String,
            "column_3": pl.String
        }
    )
    df = df.select([
        pl.col("column_1").alias("size"),
        pl.col("column_2").alias("md5"),
        pl.col("column_3").alias("url")
    ])
    df = df.filter(~pl.col("url").str.contains("gkg"))
    df.write_parquet("data/masterfilelist_processed.parquet")

    os.remove("data/masterfilelist.txt")

if __name__ == "__main__":
    main()