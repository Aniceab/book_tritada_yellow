def save_validation_results_to_s3(results, bucket_name, prefix, aws_access_key, aws_secret_key):
    """
    Save validation results to S3 in a Hive-compatible structure.

    Args:
        results (list): A list of validation results (as dictionaries).
        bucket_name (str): The name of the S3 bucket.
        prefix (str): The prefix (folder path) for saving the results.
        aws_access_key (str): AWS access key.
        aws_secret_key (str): AWS secret key.
    """
    # Initialize S3 client
    s3 = boto3.client(
        's3',
        aws_access_key_id=aws_access_key,
        aws_secret_access_key=aws_secret_key
    )

    # Convert results to a DataFrame
    df = pd.DataFrame(results)

    # Add a partition column (e.g., date)
    df["validation_date"] = datetime.now().strftime("%Y-%m-%d")

    # Save DataFrame to Parquet in memory
    buffer = BytesIO()
    df.to_parquet(buffer, index=False)
    buffer.seek(0)

    # Define S3 key with Hive-compatible structure
    s3_key = f"{prefix}/validation_date={df['validation_date'].iloc[0]}/validation_results.parquet"

    # Upload to S3
    s3.put_object(Bucket=bucket_name, Key=s3_key, Body=buffer.getvalue())
    logging.info(f"Validation results saved to S3: s3://{bucket_name}/{s3_key}")