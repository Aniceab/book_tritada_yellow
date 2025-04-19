import pandas as pd
import boto3
import logging
from datetime import datetime
from io import BytesIO

def validate_data_quality(df, rules):
    """
    Validate the quality of the data based on predefined rules.

    Args:
        df (pd.DataFrame): The DataFrame to validate.
        rules (dict): A dictionary of validation rules.

    Returns:
        dict: A dictionary with validation results.
    """
    results = {
        "schema_validation": True,
        "null_validation": True,
        "duplicate_validation": True,
        "business_rules_validation": True,
        "volumetric_validation": True,
        "errors": []
    }

    # Validate schema
    if "schema" in rules:
        expected_schema = rules["schema"]
        actual_schema = dict(zip(df.columns, df.dtypes.astype(str)))
        if expected_schema != actual_schema:
            results["schema_validation"] = False
            results["errors"].append(f"Schema mismatch! Expected: {expected_schema}, Found: {actual_schema}")

    # Validate null values
    if "not_null" in rules:
        not_null_columns = rules["not_null"]
        null_check = df[not_null_columns].isnull().sum()
        null_issues = null_check[null_check > 0].to_dict()
        if null_issues:
            results["null_validation"] = False
            results["errors"].append(f"Null values found in columns: {null_issues}")

    # Validate duplicates
    if "unique_columns" in rules:
        unique_columns = rules["unique_columns"]
        duplicates = df.duplicated(subset=unique_columns).sum()
        if duplicates > 0:
            results["duplicate_validation"] = False
            results["errors"].append(f"Found {duplicates} duplicate rows based on columns: {unique_columns}")

    # Validate volumetric rules
    if "min_rows" in rules:
        min_rows = rules["min_rows"]
        if len(df) < min_rows:
            results["volumetric_validation"] = False
            results["errors"].append(f"Row count {len(df)} is less than the minimum required {min_rows}")

    # Validate business rules
    if "business_rules" in rules:
        business_rules = rules["business_rules"]
        for rule_name, rule_func in business_rules.items():
            if not rule_func(df):
                results["business_rules_validation"] = False
                results["errors"].append(f"Business rule '{rule_name}' failed!")

    return results


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