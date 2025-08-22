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

variable "max_capacity" {
  default = 10
}  # Concurrency for temporary Glue jobs

variable "lambda_script_path" {
  default = ""
}  # Caminho do zip do Lambda