# Call manually from Concourse to ingest the audit logs from a specific day or range of days
resource "aws_lambda_function" "start_corporate_data_ingestion" {
  filename         = data.archive_file.start_corporate_data_ingestion.output_path
  function_name    = "start_corporate_data_ingestion"
  role             = aws_iam_role.start_corporate_data_ingestion.arn
  handler          = "lambda_handler.lambda_handler"
  runtime          = "python3.8"
  source_code_hash = data.archive_file.start_corporate_data_ingestion.output_base64sha256
  timeout          = 900

  environment {
    variables = {
      AWS_ACCOUNT           = local.account[local.environment],
      START_DATE            = local.start_corporate_data_ingestion.start_date,
      END_DATE              = local.start_corporate_data_ingestion.end_date,
      SOURCE_S3_PREFIX      = local.start_corporate_data_ingestion.source_s3_prefix,
      DESTINATION_S3_PREFIX = local.start_corporate_data_ingestion.destination_s3_prefix,
      COLLECTION_NAME       = local.start_corporate_data_ingestion.collection_name
    }
  }
}

data "archive_file" "start_corporate_data_ingestion" {
  type        = "zip"
  source_dir  = "${path.module}/lambda/start_corporate_data_ingestion/src"
  output_path = "${path.module}/lambda/start_corporate_data_ingestion.zip"
}

data "aws_iam_policy_document" "assume_role_lambda" {
  statement {
    sid     = "APLambdaAuthAssumeRole"
    actions = ["sts:AssumeRole"]

    principals {
      identifiers = ["lambda.amazonaws.com"]
      type        = "Service"
    }
  }
}

# LAMBDA: start_corporate_data_ingestion
resource "aws_iam_role" "start_corporate_data_ingestion" {
  name               = "start_corporate_data_ingestion"
  assume_role_policy = data.aws_iam_policy_document.assume_role_lambda.json
}

resource "aws_iam_role_policy" "start_corporate_data_ingestion" {
  role   = aws_iam_role.start_corporate_data_ingestion.name
  policy = data.aws_iam_policy_document.start_corporate_data_ingestion.json
}

data "aws_iam_policy_document" "start_corporate_data_ingestion" {
  statement {
    sid = "AllowEMROperation"
    actions = [
      "elasticmapreduce:DescribeCluster",
      "elasticmapreduce:DescribeStep",
      "elasticmapreduce:TerminateJobFlows",
      "elasticmapreduce:AddJobFlowSteps",
    ]
    resources = ["*"]
  }

  statement {
    sid = "AllowInvokeEMRLauncher"
    actions = [
      "lambda:InvokeFunction",
    ]
    resources = ["arn:aws:lambda:${local.region}:${local.account[local.environment]}:function:${aws_lambda_function.dataworks_aws_corporate_data_ingestion_emr_launcher.function_name}"]
  }

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
