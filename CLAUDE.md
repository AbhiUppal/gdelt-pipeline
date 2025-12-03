# Project Overview

This project is a data pipeline for ingesting GDELT events and mentions data into a Postgres database. It includes scripts for downloading, processing, and loading the data. It is implemented in Python using the uv package manager. It uses psycopg as the database driver and sqlalchemy as the ORM. It will be containerized with Docker.

Links to all GDELT files are stored in a file called the `data/masterfilelist.parquet`. The pipeline downloads files from GDELT, processes them into Parquet format, and loads them into a Postgres database.

Every day, a new master file list is pulled in, and we update this file with any new files that have not yet been downloaded. The pipeline then downloads any new files, processes them, and loads them into the database.

## Download Process

Data are downloaded one day at a time (note that a file is uploaded every 15 minutes). The `data/` directory is organized as follows:

`data` --> `dataset` --> `year` --> `month` --> `day` --> `files`.

When files are downloaded, they are stored in the appropriate year/month/day directory. They are then processed into Parquet format, and the original CSV files are deleted to save space. Once these parquet files are loaded into the database, they are also deleted from local storage and replaced with empty text files to indicate that they have been processed.