from botocore.exceptions import ClientError
import boto3
import logging
import pandas as pd
from io import BytesIO

def process_yellow_taxi_files(bucket_name, months, aws_access_key, aws_secret_key, output_bucket):
    """
    Process yellow taxi files by filtering specific columns and saving them to another S3 bucket.

    Args:
        bucket_name (str): Input S3 bucket name.
        months (list): List of months to process.
        aws_access_key (str): AWS access key.
        aws_secret_key (str): AWS secret key.
        output_bucket (str): Output S3 bucket name.
    """
    # Columns to keep
    columns_to_keep = [
        "VendorID",
        "passenger_count",
        "total_amount",
        "tpep_pickup_datetime",
        "tpep_dropoff_datetime"
    ]

    # Initialize S3 client
    s3 = boto3.client(
        's3',
        aws_access_key_id=aws_access_key,
        aws_secret_access_key=aws_secret_key
    )

    for month in months:
        filename = f"yellow_tripdata_{month}.parquet"
        file_key = f"{month}/{filename}"

        try:
            # Fetch the file from S3
            obj_data = s3.get_object(Bucket=bucket_name, Key=file_key)
            file_content = obj_data['Body'].read()

            # Read the Parquet file into a DataFrame
            df = pd.read_parquet(BytesIO(file_content))

            # Filter the DataFrame to keep only the specified columns
            df_filtered = df[columns_to_keep]

            # Save the filtered DataFrame to a Parquet file in memory
            output_buffer = BytesIO()
            df_filtered.to_parquet(output_buffer, index=False)
            output_buffer.seek(0)

            # Define the output key and upload the filtered file to S3
            output_key = f"{month}/{filename}"
            s3.put_object(Bucket=output_bucket, Key=output_key, Body=output_buffer.getvalue())
            logging.info(f"Saved filtered file to S3: s3://{output_bucket}/{output_key}")
        except ClientError as e:
            if e.response['Error']['Code'] == "AccessDenied":
                logging.error(f"Access denied for file: s3://{bucket_name}/{file_key}")
            elif e.response['Error']['Code'] == "NoSuchKey":
                logging.warning(f"File not found: s3://{bucket_name}/{file_key}")
            else:
                logging.error(f"Unexpected error: {e}")
        except Exception as e:
            logging.error(f"Error processing file {file_key}: {e}")