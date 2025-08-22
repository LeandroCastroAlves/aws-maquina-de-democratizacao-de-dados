project = "democratizacao"
environment = "prd"
s3_bucket = "raw-democratizacao"

# ------------------------------
# Variables Glue Job
# ------------------------------
max_concurrent_runs = 3  # Concurrency for temporary Glue jobs
s3_bucket_glue_script  = "s3://aws-glue-assets-314146324926-us-east-1/scripts"
glue_script_path = "democratizacao.py"
number_of_workers = 2  # Default number of workers for Glue Job
worker_type = "G.1X"  # Tipo de worker para Glue Job


# ------------------------------
# Variables Lambda Function
# ------------------------------
lambda_zip_path = "lambda/src/lambda_function.zip"  # Path to the Lambda zip
lambda_script_path = "lambda/src/lambda_function.py"  # Path to the Lambda script