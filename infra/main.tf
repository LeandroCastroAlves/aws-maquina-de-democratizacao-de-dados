# ------------------------------
# IAM Roles e Policy
# ------------------------------
resource "aws_iam_role" "lambda_role" {
  name = "${var.project}-lambda-role-${var.environment}"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action = "sts:AssumeRole"
      Effect = "Allow"
      Principal = { Service = "lambda.amazonaws.com" }
    }]
  })
}

resource "aws_iam_role" "glue_role" {
  name = "${var.project}-glue-role-${var.environment}"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action = "sts:AssumeRole"
      Effect = "Allow"
      Principal = { Service = "glue.amazonaws.com" }
    }]
  })
}

resource "aws_iam_policy" "glue_lambda_policy" {
  name        = "GlueLambdaPolicy"
  path        = "/iamsr/policy/"
  description = "Policy para Lambda e Glue"
  policy      = file("iamsr/policy/glue_lambda_policy.json") 
}

resource "aws_iam_role_policy_attachment" "attach_lambda" {
  role       = aws_iam_role.lambda_role.name
  policy_arn = aws_iam_policy.glue_lambda_policy.arn
}

resource "aws_iam_role_policy_attachment" "attach_glue" {
  role       = aws_iam_role.glue_role.name
  policy_arn = aws_iam_policy.glue_lambda_policy.arn
}

# ------------------------------
# Lambda Function
# ------------------------------
resource "aws_lambda_function" "lambda_democratizacao" {
  filename      = "${var.lambda_zip_path}"
  function_name = "${var.project}-lambda-${var.environment}"
  handler       = "lambda_function.lambda_handler"
  runtime       = "python3.9"
  role          = aws_iam_role.lambda_role.arn
}

# ------------------------------
# Permissão para S3 invocar o Lambda
# ------------------------------
resource "aws_lambda_permission" "allow_s3_invoke" {
  statement_id  = "AllowS3Invoke"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.lambda_democratizacao.function_name
  principal     = "s3.amazonaws.com"
  source_arn    = "arn:aws:s3:::${var.s3_bucket}"
}

# ------------------------------
# S3 Bucket Notification (dispara Lambda)
# ------------------------------
resource "aws_s3_bucket_notification" "bucket_trigger" {
  bucket = var.s3_bucket

  lambda_function {
    lambda_function_arn = aws_lambda_function.lambda_democratizacao.arn
    events              = ["s3:ObjectCreated:*"]
  }

  depends_on = [aws_lambda_permission.allow_s3_invoke]
}

# ------------------------------
# Glue Job
# ------------------------------
resource "aws_glue_job" "democratizacao_v3" {
  name     = "${var.project}-job-glue-${var.environment}"
  role_arn = aws_iam_role.glue_role.arn

  command {
    name            = "glueetl"
    script_location = "${var.s3_bucket_glue_script}/${var.glue_script_path}"
  }

  default_arguments = {
    "--job-language" = "python"
    "--TempDir"      = "${var.s3_bucket_glue_script}/temporary/"
  }
  
  execution_property {
    max_concurrent_runs = var.max_concurrent_runs 
  }

  number_of_workers = var.number_of_workers
  worker_type       = "G.1X"  # Tipo de worker para Glue Job
  timeout      = 60  # Timeout in minutes
  max_retries  = 1
}
