CREATE EXTERNAL TABLE IF NOT EXISTS validation_results (
    schema_validation BOOLEAN COMMENT 'Indicates if the schema validation passed',
    null_validation BOOLEAN COMMENT 'Indicates if the null validation passed',
    duplicate_validation BOOLEAN COMMENT 'Indicates if the duplicate validation passed',
    business_rules_validation BOOLEAN COMMENT 'Indicates if the business rules validation passed',
    volumetric_validation BOOLEAN COMMENT 'Indicates if the volumetric validation passed',
    errors ARRAY<STRING> COMMENT 'List of errors found during validation'
)
PARTITIONED BY (
    validation_date STRING COMMENT 'Partition column for the validation date (YYYY-MM-DD)'
)
STORED AS PARQUET
LOCATION 's3://prd-yellow-taxi-table-1/validation_results/'
TBLPROPERTIES (
    'parquet.compression'='SNAPPY'
);