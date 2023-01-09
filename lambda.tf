# Call manually from Concourse to ingest the audit logs from a specific day or range of days
resource "aws_lambda_function" "start_corporate_data_ingestion_manually" {
  filename         = data.archive_file.start_corporate_data_ingestion_manually.output_path
  function_name    = "start_corporate_data_ingestion_manually"
  role             = aws_iam_role.start_corporate_data_ingestion_manually.arn
  handler          = "lambda_handler.lambda_handler"
  runtime          = "python3.8"
  source_code_hash = data.archive_file.start_corporate_data_ingestion_manually.output_base64sha256
  timeout          = 900

  environment {
    variables = {
      AWS_ACCOUNT = local.account[local.environment],
      EXPORT_DATE_OR_RANGE = local.start_corporate_data_ingestion_manually["export_date_or_range"],
      SOURCE_S3_PREFIX = local.start_corporate_data_ingestion_manually["source_s3_prefix"],
      DESTINATION_S3_PREFIX = local.start_corporate_data_ingestion_manually["destination_s3_prefix"],
    }
  }
}

data "archive_file" "start_corporate_data_ingestion_manually" {
  type        = "zip"
  source_dir  = "${path.module}/lambda/start_corporate_data_ingestion_manually/src"
  output_path = "${path.module}/lambda/start_corporate_data_ingestion_manually.zip"
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

# LAMBDA: start_corporate_data_ingestion_manually
resource "aws_iam_role" "start_corporate_data_ingestion_manually" {
  name               = "start_corporate_data_ingestion_manually"
  assume_role_policy = data.aws_iam_policy_document.assume_role_lambda.json
}

resource "aws_iam_role_policy" "start_corporate_data_ingestion_manually" {
  role   = aws_iam_role.start_corporate_data_ingestion_manually.name
  policy = data.aws_iam_policy_document.start_corporate_data_ingestion_manually.json
}

data "aws_iam_policy_document" "start_corporate_data_ingestion_manually" {
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
