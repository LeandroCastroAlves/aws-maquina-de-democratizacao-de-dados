variable "project" {
  default = ""
}

variable "environment" {
  default = ""
}

variable "s3_bucket" {
  default = ""
}

variable "lambda_zip_path" {
    default = "lambda/src/lambda_function.zip"
}   # Caminho do zip do Lambda

variable "glue_script_path" {
    default = ""
}  # Caminho do script Glue dentro do bucket

variable "s3_bucket_glue_script" {
  default = ""
}

variable "number_of_workers" {
  type = number
  default = 2  # Default number of workers for Glue Job
}

variable "worker_type" {
  type = string
  default = "G.1X"  # Tipo de worker para Glue Job
}

variable "max_concurrent_runs" {
  type = number
  default = 3  # Concurrency for temporary Glue jobs
}  # Concurrency for temporary Glue jobs

variable "lambda_script_path" {
  default = ""
}  # Caminho do zip do Lambda