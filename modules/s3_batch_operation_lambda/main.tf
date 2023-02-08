# to be used with s3 batch job to backup files from one S3 bucket to another
resource "aws_lambda_function" "s3_batch_copy" {
  filename         = data.archive_file.s3_batch_copy.output_path
  function_name    = "s3_batch_copy"
  role             = aws_iam_role.s3_batch_copy_lambda.arn
  handler          = "lambda_handler.lambda_handler"
  runtime          = "python3.8"
  source_code_hash = data.archive_file.s3_batch_copy.output_base64sha256
  timeout          = 900

  environment {
    variables = {
      DESTINATION_BUCKET_ARN  = var.destination_s3_bucket_arn,
      MAX_POOL_CONNECTIONS    = var.max_pool_connections,
      MAX_CONCURRENCY         = var.max_concurrency,
      MULTIPART_CHUNKSIZE     = var.multipart_chunksize,
      MAX_ATTEMPTS            = var.max_attempts,
      COPY_METADATA           = var.copy_metadata,
      COPY_TAGGING            = var.copy_tagging,
      COPY_STORAGE_CLASS      = var.copy_storage_class,
      DESTINATION_KMS_KEY_ARN = var.destination_bucket_kms_arn
    }
  }
}

data "archive_file" "s3_batch_copy" {
  type        = "zip"
  source_dir  = "${path.module}/src"
  output_path = "${path.module}/s3_batch_copy.zip"
}
