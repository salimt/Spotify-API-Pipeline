import configparser
import pathlib
import sys
from datetime import datetime, timedelta

import pandas as pd
import snowflake.connector
from airflow import settings
from airflow.hooks.S3_hook import S3Hook
from airflow.models import Connection
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator

import validation as va

# Read Configuration File
config = configparser.ConfigParser()
script_path = pathlib.Path(__file__).parent.resolve()
config_file = "config.conf"
config_file_path = script_path / config_file
if config_file_path.exists():
    config.read(f"{script_path}/{config_file}")
else:
    print(f"Config file '{config_file_path}' does not exist.")

# Read the configuration file
config.read('config.conf')

# Get the values from the 's3 config' section
userVal = config.get('snowflake config', 'user')
passwordVal = config.get('snowflake config', 'password')
accountVal = config.get('snowflake config', 'account')
warehouseVal = config.get('snowflake config', 'warehouse')
databaseVal = config.get('snowflake config', 'database')
schemaVal = config.get('snowflake config', 'schema')
roleVal = config.get('snowflake config', 'role')
bucketName = config.get('s3 config', 'bucket_name')
SNOWFLAKE_CONN_ID = config.get('snowflake config', 'conn_id')

output_name = sys.argv[1]


def main():
    try:
        va.validate_input(output_name)
        copy_to_snowflake()
    except Exception as e:
        print(f"Error {e}")
        sys.exit(1)


def copy_to_snowflake():
    # Construct S3 file URL
    file_path = f"/tmp/{output_name}.csv"
    object_name = f"{output_name}.csv"
    print("S3 file URL:", file_path)

    # Load the first few rows of the CSV file to infer the column names and types
    try:
        df = pd.read_csv(file_path, nrows=5)
        print("DF.READ CSV", df.head(5))
        # Construct column definitions for CREATE TABLE statement
        columns = ", ".join([f"{column} VARCHAR" for column in df.columns])
        print("COLUMNS", columns)
        # Connect to Snowflake
        conn = snowflake.connector.connect(
            user=userVal,
            password=passwordVal,
            account=accountVal,
            warehouse=warehouseVal,
            database=databaseVal,
            schema=schemaVal,
            role=roleVal
        )

        cursor = conn.cursor()
        print("Connection to Snowflake")

        # Create the table dynamically
        create_table_sql = f"CREATE OR REPLACE TABLE sample_table ({columns})"
        cursor.execute(create_table_sql)
        print("Table created successfully.")

        # Execute COPY INTO statement
        conn.cursor().execute(f"""
            CREATE OR REPLACE STAGE sample_table
            FILE_FORMAT = (TYPE = 'CSV', FIELD_DELIMITER = ',', SKIP_HEADER = 1)
            COPY_OPTIONS = (PURGE = TRUE);
        """)
        # Putting Data
        conn.cursor().execute(f"PUT file://{file_path} @sample_table")

        # Copy data from stage to table
        conn.cursor().execute(f"""
            COPY INTO spotify_test.sample_table
                (track, artist, danceability, energy, key, loudness, mode, speechiness, acousticness, instrumentalness, liveness, valence, tempo, type, id, uri, track_href, analysis_url, duration_ms, time_signature, genres)
            FROM (
                SELECT
                    REGEXP_REPLACE($1, '[^A-Za-z0-9 ]', '') AS track,
                    REGEXP_REPLACE($2, '[^A-Za-z0-9 ]', '') AS artist,
                    $3 AS danceability,
                    $4 AS energy,
                    $5 AS key,
                    $6 AS loudness,
                    $7 AS mode,
                    $8 AS speechiness,
                    $9 AS acousticness,
                    $10 AS instrumentalness,
                    $11 AS liveness,
                    $12 AS valence,
                    $13 AS tempo,
                    $14 AS type,
                    $15 AS id,
                    REGEXP_REPLACE($16, '[^A-Za-z0-9 ]', '') AS uri,
                    REGEXP_REPLACE($17, '[^A-Za-z0-9 ]', '') AS track_href,
                    REGEXP_REPLACE($18, '[^A-Za-z0-9 ]', '') AS analysis_url,
                    REGEXP_REPLACE($19, '[^A-Za-z0-9 ]', '') AS duration_ms,
                    REGEXP_REPLACE($20, '[^A-Za-z0-9 ]', '') AS time_signature,
                    REGEXP_REPLACE($21, '[^A-Za-z0-9 ]', '') AS genres
                FROM @spotify_test.sample_table
            )
            FILE_FORMAT = (
                TYPE = 'csv',
                FIELD_DELIMITER = ',',
                SKIP_HEADER = 1
            );
        """)

        print("Data copied to Snowflake successfully.")

        # Close cursor and connection
        cursor.close()
        conn.close()

    except Exception as e:
        print(f"An error occurred: {e}")


if __name__ == "__main__":
    main()
