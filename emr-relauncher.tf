variable "dataworks_emr_relauncher_zip" {
  type = map(string)

  default = {
    base_path = ""
    version   = ""
  }
}

resource "aws_lambda_function" "aws_emr_template_repository_emr_relauncher" {
  filename      = "${var.dataworks_emr_relauncher_zip["base_path"]}/dataworks-emr-relauncher-${var.dataworks_emr_relauncher_zip["version"]}.zip"
  function_name = "aws_emr_template_repository_emr_relauncher"
  role          = aws_iam_role.aws_emr_template_repository_emr_relauncher_lambda_role.arn
  handler       = "event_handler.handler"
  runtime       = "python3.8"
  source_code_hash = filebase64sha256(
    format(
      "%s/dataworks-emr-relauncher-%s.zip",
      var.dataworks_emr_relauncher_zip["base_path"],
      var.dataworks_emr_relauncher_zip["version"]
    )
  )
  publish = false
  timeout = 60

  environment {
    variables = {
      SNS_TOPIC       = aws_sns_topic.aws_emr_template_repository_cw_trigger_sns.arn
      TABLE_NAME      = local.data_pipeline_metadata
      MAX_RETRY_COUNT = local.aws_emr_template_repository_max_retry_count[local.environment]
      LOG_LEVEL       = "debug"
    }
  }

  tags = {
    Name = "aws_emr_template_repository_emr_relauncher"
  }

  depends_on = [aws_cloudwatch_log_group.aws_emr_template_repository_emr_relauncher_log_group]
}

resource "aws_cloudwatch_event_target" "aws_emr_template_repository_emr_relauncher_target" {
  rule      = aws_cloudwatch_event_rule.aws_emr_template_repository_failed.name
  target_id = "aws_emr_template_repository_emr_relauncher_target"
  arn       = aws_lambda_function.aws_emr_template_repository_emr_relauncher.arn
}

resource "aws_iam_role" "aws_emr_template_repository_emr_relauncher_lambda_role" {
  name               = "aws_emr_template_repository_emr_relauncher_lambda_role"
  assume_role_policy = data.aws_iam_policy_document.aws_emr_template_repository_emr_relauncher_assume_policy.json
  tags = {
    Name = "aws_emr_template_repository_emr_relauncher_lambda_role"
  }
}

resource "aws_lambda_permission" "aws_emr_template_repository_emr_relauncher_invoke_permission" {
  statement_id  = "AllowExecutionFromCloudWatch"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.aws_emr_template_repository_emr_relauncher.function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.aws_emr_template_repository_failed.arn
}

data "aws_iam_policy_document" "aws_emr_template_repository_emr_relauncher_assume_policy" {
  statement {
    sid     = "aws_emr_template_repositoryEMRLauncherLambdaAssumeRolePolicy"
    effect  = "Allow"
    actions = ["sts:AssumeRole"]

    principals {
      identifiers = ["lambda.amazonaws.com"]
      type        = "Service"
    }
  }
}

data "aws_iam_policy_document" "aws_emr_template_repository_emr_relauncher_scan_dynamo_policy" {
  statement {
    effect = "Allow"

    actions = [
      "dynamodb:DescribeTable",
      "dynamodb:Scan"
    ]

    resources = [
      "arn:aws:dynamodb:${var.region}:${local.account[local.environment]}:table/${local.data_pipeline_metadata}"
    ]
  }
}

data "aws_iam_policy_document" "aws_emr_template_repository_emr_relauncher_sns_policy" {
  statement {
    sid    = "AllowAccessToSNSLauncherTopic"
    effect = "Allow"

    actions = [
      "sns:Publish",
    ]

    resources = [
      aws_sns_topic.aws_emr_template_repository_cw_trigger_sns.arn
    ]
  }
}

data "aws_iam_policy_document" "aws_emr_template_repository_emr_relauncher_pass_role_document" {
  statement {
    effect = "Allow"
    actions = [
      "iam:PassRole"
    ]
    resources = [
      "arn:aws:iam::*:role/*"
    ]
  }
}

resource "aws_iam_policy" "aws_emr_template_repository_emr_relauncher_scan_dynamo_policy" {
  name        = "aws_emr_template_repositoryEmrRelauncherScanDynamoDb"
  description = "Allow Emr relauncher to scan pipeline metadata table"
  policy      = data.aws_iam_policy_document.aws_emr_template_repository_emr_relauncher_scan_dynamo_policy.json
  tags = {
    Name = "aws_emr_template_repository_emr_relauncher_scan_dynamo_policy"
  }
}

resource "aws_iam_policy" "aws_emr_template_repository_emr_relauncher_sns_policy" {
  name        = "aws_emr_template_repositoryEmrRelauncherSnsPublish"
  description = "Allow aws_emr_template_repository to run job flow"
  policy      = data.aws_iam_policy_document.aws_emr_template_repository_emr_relauncher_sns_policy.json
  tags = {
    Name = "aws_emr_template_repository_emr_relauncher_sns_policy"
  }
}

resource "aws_iam_policy" "aws_emr_template_repository_emr_relauncher_pass_role_policy" {
  name        = "aws_emr_template_repositoryEmrRelauncherPassRole"
  description = "Allow Emr relauncher to publish messages to launcher topic"
  policy      = data.aws_iam_policy_document.aws_emr_template_repository_emr_relauncher_pass_role_document.json
  tags = {
    Name = "aws_emr_template_repository_emr_relauncher_pass_role_policy"
  }
}

resource "aws_iam_role_policy_attachment" "aws_emr_template_repository_emr_relauncher_pass_role_attachment" {
  role       = aws_iam_role.aws_emr_template_repository_emr_relauncher_lambda_role.name
  policy_arn = aws_iam_policy.aws_emr_template_repository_emr_relauncher_pass_role_policy.arn
}

resource "aws_iam_role_policy_attachment" "aws_emr_template_repository_emr_relauncher_policy_execution" {
  role       = aws_iam_role.aws_emr_template_repository_emr_relauncher_lambda_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaVPCAccessExecutionRole"
}

resource "aws_iam_role_policy_attachment" "aws_emr_template_repository_emr_relauncher_sns_attachment" {
  role       = aws_iam_role.aws_emr_template_repository_emr_relauncher_lambda_role.name
  policy_arn = aws_iam_policy.aws_emr_template_repository_emr_relauncher_sns_policy.arn
}

resource "aws_iam_role_policy_attachment" "aws_emr_template_repository_emr_relauncher_scan_dynamo_attachment" {
  role       = aws_iam_role.aws_emr_template_repository_emr_relauncher_lambda_role.name
  policy_arn = aws_iam_policy.aws_emr_template_repository_emr_relauncher_scan_dynamo_policy.arn
}

resource "aws_cloudwatch_log_group" "aws_emr_template_repository_emr_relauncher_log_group" {
  name = "/aws/lambda/aws_emr_template_repository_emr_relauncher"
  retention_in_days = 180
}
