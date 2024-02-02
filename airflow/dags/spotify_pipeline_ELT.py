from airflow.models import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta

"""
DAG to extract Spotify data, load into AWS S3, and copy to AWS Redshift
"""

# Output name of extracted file. This will be passed to each
# DAG task so they know which file to process
output_name = datetime.now().strftime("%Y%m%d")

# Run our DAG daily and ensure DAG run will kick off
# once Airflow is started, as it will try to "catch up"

schedule_interval = "@daily"
start_date = days_ago(1)

default_args = {"owner": "airflow", "depends_on_past": False, "retries": 1}

with DAG(
    dag_id="spotify_pipeline_ELT",
    description="Spotify ELT",
    schedule_interval=schedule_interval,
    default_args=default_args,
    start_date=start_date,
    catchup=True,
    max_active_runs=1,
    tags=["SpotifyELT"],
) as dag:

    extract_spotify_data = BashOperator(
        task_id="extract_spotify_data",
        bash_command=f"python /opt/airflow/extraction/spotify_data_extraction.py {output_name}",
        dag=dag,
    )
    extract_spotify_data.doc_md = "Extract Spotify data and store as CSV"

    s3_connect_create_load = BashOperator(
        task_id="upload_to_s3",
        bash_command=f"python /opt/airflow/extraction/s3_connect_create_load.py {output_name}",
        dag=dag,
    )
    s3_connect_create_load.doc_md = "Upload Spotify CSV data to S3 bucket"
    
    s3_to_snowflake = BashOperator(
        task_id="s3_to_snowflake",
        bash_command=f"python /opt/airflow/extraction/s3_to_snowflake.py {output_name}",
        dag=dag,
    )
    s3_to_snowflake.doc_md = "Copy S3 CSV file to Redshift table"

extract_spotify_data >> s3_connect_create_load >> s3_to_snowflake
