# Spotify ELT Pipeline

A data pipeline to extract Spotify data from a playlist that is created by a students.

Output is a Google Data Studio report, providing insight into the track features and preferences.

## Motivation

It provided a good opportunity to develop skills and experience in a range of tools. As such, project is more complex than required, utilising dbt, airflow, docker and cloud based storage, and usage of localstack for testing.

## Architecture

<img src="https://i.imgur.com/rcDiMqj.jpeg" width=85% height=90%>

1. Extract data using [Spotify API](https://developer.spotify.com/)
1. Simulate AWS S3 locally for testing with [localstack](https://www.localstack.cloud/)
1. Load into [AWS S3](https://aws.amazon.com/s3/)
1. Copy into [Snowflake](https://www.snowflake.com/en/)
1. Transform using [dbt](https://www.getdbt.com)
1. Create [Google Looker Studio](https://lookerstudio.google.com/u/0/m) Dashboard 
1. Orchestrate with [Airflow](https://airflow.apache.org) in [Docker](https://www.docker.com)

## Output

[<img src="https://i.imgur.com/O89cvKU.jpeg" width=70% height=70%>](https://datastudio.google.com/reporting/e927fef6-b605-421c-ae29-89a66e11ea18)

* Final output from Google Data Studio. Link [here](https://lookerstudio.google.com/reporting/a6785e52-ddbe-4e81-92c2-acd72112a38a/page/jAGpD). Note that Dashboard is reading from a static CSV output from Snowflake. 

## Pull Repo

> **NOTE**: This was developed using Windows 10. If you're on Mac or Linux, you may need to amend certain components if issues are encountered.

  ```bash
  git clone https://github.com/salimt/Spotify-API-Pipeline.git
  cd Spotify-API-Pipeline
  ```
