import yaml
import logging
from dq import validate_data_quality, save_validation_results_to_s3
import os
import boto3
import pandas as pd
from io import BytesIO
from dotenv import load_dotenv

load_dotenv()

def load_config(config_path):
    """
    Load configuration from a YAML file.

    Args:
        config_path (str): Path to the configuration file.

    Returns:
        dict: Configuration dictionary.
    """
    with open(config_path, 'r') as file:
        config = yaml.safe_load(file)
    return config

def load_data_from_s3(bucket_name, s3_key, aws_access_key, aws_secret_key):
    """
    Load a Parquet file from S3 into a Pandas DataFrame.

    Args:
        bucket_name (str): Name of the S3 bucket.
        s3_key (str): Key of the file in the S3 bucket.
        aws_access_key (str): AWS access key.
        aws_secret_key (str): AWS secret key.

    Returns:
        pd.DataFrame: The loaded DataFrame.
    """
    s3 = boto3.client(
        's3',
        aws_access_key_id=aws_access_key,
        aws_secret_access_key=aws_secret_key
    )
    obj = s3.get_object(Bucket=bucket_name, Key=s3_key)
    return pd.read_parquet(BytesIO(obj['Body'].read()))

def main():
    # Configure logging
    logging.basicConfig(level=logging.INFO)

    # Load configuration
    config_path = "../config/config.yaml"
    config = load_config(config_path)

    # Extract configuration values
    aws_access_key = os.getenv('AWS_ACCESS_KEY')
    aws_secret_key = os.getenv('AWS_SECRET_KEY')
    bucket_name = config['s3']['input_bucket']
    output_bucket = config['s3']['output_bucket']
    prefix = config['s3']['prefix']
    months = config['pipeline']['months']

    for month in months:
        s3_key = f"{prefix}{month}.parquet"
        logging.info(f"Processing file: s3://{bucket_name}/{s3_key}")

        # Load data from S3
        df = load_data_from_s3(bucket_name, s3_key, aws_access_key, aws_secret_key)

        # Define validation rules
        rules = {
            "schema": {
                "VendorID": "int64",
                "passenger_count": "int64",
                "total_amount": "float64",
                "tpep_pickup_datetime": "datetime64[ns]",
                "tpep_dropoff_datetime": "datetime64[ns]"
            },
            "not_null": ["VendorID", "passenger_count", "total_amount"],
            "unique_columns": ["VendorID", "tpep_pickup_datetime"],
            "min_rows": 100,  # Minimum number of rows required
            "business_rules": {
                "valid_total_amount": lambda df: (df["total_amount"] >= 0).all()
            }
        }

        # Validate data quality
        validation_results = validate_data_quality(df, rules)
        logging.info(f"Validation results for {s3_key}: {validation_results}")

        # Save validation results to S3
        save_validation_results_to_s3([validation_results], output_bucket, "validation_results", aws_access_key, aws_secret_key)

if __name__ == "__main__":
    main()