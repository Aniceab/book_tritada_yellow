import yaml
import logging
from transform.process_data import process_yellow_taxi_files
import os
import boto3
from dotenv import load_dotenv

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
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

def main():
    # Configure logging
    logging.basicConfig(level=logging.INFO)

    # Load configuration
    config_path = os.path.join(os.path.dirname(__file__), '../config/config.yaml')
    config = load_config(config_path)

    # Extract configuration values
    aws_secret_key = os.getenv('AWS_SECRET_KEY')
    aws_access_key = os.getenv('AWS_ACCESS_KEY')
    bucket_name = config['s3']['input_bucket']
    output_bucket = config['s3']['output_bucket']
    prefix = config['s3']['prefix']
    months = config['pipeline']['months']

    # Process yellow taxi files
    process_yellow_taxi_files(bucket_name, months, aws_access_key, aws_secret_key, output_bucket)

if __name__ == "__main__":
    main()



    