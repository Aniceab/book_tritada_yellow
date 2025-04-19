import boto3
from botocore.exceptions import NoCredentialsError, ClientError
import os

def get_s3_client(aws_access_key, aws_secret_key):
    return boto3.client(
        's3',
        aws_access_key_id=aws_access_key,
        aws_secret_access_key=aws_secret_key
    )

def download_file_from_s3(bucket_name, s3_key, local_path, aws_access_key, aws_secret_key):
    s3 = get_s3_client(aws_access_key, aws_secret_key)
    try:
        s3.download_file(bucket_name, s3_key, local_path)
        print(f"Downloaded {s3_key} from bucket {bucket_name} to {local_path}")
    except FileNotFoundError:
        print("The specified local path does not exist.")
    except NoCredentialsError:
        print("Credentials not available.")
    except ClientError as e:
        print(f"Failed to download file: {e}")

def upload_file_to_s3(local_path, bucket_name, s3_key, aws_access_key, aws_secret_key):
    s3 = get_s3_client(aws_access_key, aws_secret_key)
    try:
        s3.upload_file(local_path, bucket_name, s3_key)
        print(f"Uploaded {local_path} to bucket {bucket_name} as {s3_key}")
    except FileNotFoundError:
        print("The specified local file does not exist.")
    except NoCredentialsError:
        print("Credentials not available.")
    except ClientError as e:
        print(f"Failed to upload file: {e}")

def list_files_in_s3(bucket_name, prefix, aws_access_key, aws_secret_key):
    s3 = get_s3_client(aws_access_key, aws_secret_key)
    try:
        response = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
        if 'Contents' in response:
            return [obj['Key'] for obj in response['Contents']]
        else:
            return []
    except NoCredentialsError:
        print("Credentials not available.")
    except ClientError as e:
        print(f"Failed to list files: {e}")
        return []