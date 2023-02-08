# to be used with s3 batch job to backup files from one S3 bucket to another
resource "aws_lambda_function" "s3_batch_copy" {
  filename = data.archive_file.s3_batch_copy.output_path
  function_name = "s3_batch_copy"
  role = aws_iam_role.s3_batch_copy_lambda.arn
  handler = "lambda_handler.handler"
  runtime = "python3.8"
  source_code_hash = data.archive_file.start_corporate_data_ingestion.output_base64sha256
  timeout = 900
}

data "archive_file" "s3_batch_copy" {
  type        = "zip"
  source_dir  = "${path.module}/lambda/s3_batch_copy/src"
  output_path = "${path.module}/lambda/s3_batch_copy.zip"
}

data "aws_iam_policy_document" "assume_role_s3_batch_copy_lambda" {
  statement {
    sid     = "APLambdaAuthAssumeRole"
    actions = ["sts:AssumeRole"]

    principals {
      identifiers = ["lambda.amazonaws.com"]
      type        = "Service"
    }
  }
}

# LAMBDA: s3_batch_copy
resource "aws_iam_role" "s3_batch_copy_lambda" {
  name               = "s3_batch_copy"
  assume_role_policy = data.aws_iam_policy_document.assume_role_s3_batch_copy_lambda.json
  tags = {
    "Name" = "s3_batch_copy"
  }
}

resource "aws_iam_role_policy" "s3_batch_copy_lambda" {
  role   = aws_iam_role.s3_batch_copy_lambda.name
  policy = data.aws_iam_policy_document.s3_batch_copy_lambda.json
}

data "aws_iam_policy_document" "s3_batch_copy_lambda" {
  statement {
    sid = "LambdaBasicExecution"
    actions = [
      "logs:CreateLogGroup",
      "logs:CreateLogStream",
      "logs:PutLogEvents"
    ]
    resources = ["*"]
  }
}