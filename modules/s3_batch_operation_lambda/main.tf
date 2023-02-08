# to be used with s3 batch job to backup files from one S3 bucket to another
resource "aws_lambda_function" "s3_batch_copy" {
  filename = data.archive_file.s3_batch_copy.output_path
  function_name = "s3_batch_copy"
  role = aws_iam_role.s3_batch_copy_lambda.arn
  handler = "lambda_handler.handler"
  runtime = "python3.8"
  source_code_hash = data.archive_file.s3_batch_copy.output_base64sha256
  timeout = 900
}

data "archive_file" "s3_batch_copy" {
  type        = "zip"
  source_dir  = "${path.module}/src"
  output_path = "${path.module}/s3_batch_copy.zip"
}
