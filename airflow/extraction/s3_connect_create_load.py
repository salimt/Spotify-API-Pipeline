import configparser
import pathlib
import sys
from datetime import datetime

import boto3
from airflow.hooks.S3_hook import S3Hook

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
endpointUrl = config.get('s3 config', 'endpoint_url')
aws_access_keyId = config.get('s3 config', 'aws_access_key_id')
aws_secret_accessKey = config.get('s3 config', 'aws_secret_access_key')
regionName = config.get('s3 config', 'region_name')
bucketName = config.get('s3 config', 'bucket_name')


def main():
    output_name = sys.argv[1]
    try:
        va.validate_input(output_name)
        conn = connect_s3()
        create_bucket(conn)
        upload_file(conn, output_name)
    except Exception as e:
        print(f"Error with file input. Error {e}")
        sys.exit(1)


def connect_s3():
    """Connect to S3 Instance"""
    try:
        s3_client = boto3.client('s3', endpoint_url=endpointUrl,
                                 aws_access_key_id=aws_access_keyId,
                                 aws_secret_access_key=aws_secret_accessKey,
                                 region_name=regionName)
        return s3_client
    except Exception as e:
        print(f"Can't connect to S3. Error: {e}")
        sys.exit(1)


def create_bucket(s3_client):
    """Create to S3 Bucket"""
    try:
        s3_client.create_bucket(Bucket=bucketName)
        return s3_client
    except Exception as e:
        print(f"Can't create S3 Bucket. Error: {e}")
        sys.exit(1)


def upload_file(s3_client, file_name):
    """Upload file to S3 Bucket"""
    file_path = f"/tmp/{file_name}.csv"
    object_name = f"{file_name}.csv"

    try:
        s3_client.upload_file(file_path, bucketName, object_name)
        print(f"File uploaded successfully to S3 bucket '{bucketName}' with key '{object_name}'")
    except Exception as e:
        print(f"Failed to upload file to S3: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
