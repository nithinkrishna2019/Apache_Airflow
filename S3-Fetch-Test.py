import boto3
import pandas as pd
from io import StringIO

# Initialize S3 client
s3 = boto3.client('s3')

# Define bucket and file path
bucket_name = "aws-glue-s3-bucket"
file_key = "Apache_Airflow_PJ/tweets.csv"

# Download file
s3.download_file(bucket_name, file_key, "tweets.csv")

print("File downloaded successfully!")

