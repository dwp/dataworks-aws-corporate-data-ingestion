variable "emr_launcher_zip" {
  type = map(string)

  default = {
    base_path = ""
    version   = ""
  }
}

resource "aws_lambda_function" "dataworks_aws_corporate_data_ingestion_emr_launcher" {
  filename      = "${var.emr_launcher_zip["base_path"]}/emr-launcher-${var.emr_launcher_zip["version"]}.zip"
  function_name = replace("${local.emr_cluster_name}_emr_launcher", "-", "_")
  role          = aws_iam_role.dataworks_aws_corporate_data_ingestion_emr_launcher_lambda_role.arn
  handler       = "emr_launcher.handler.handler"
  runtime       = "python3.7"
  source_code_hash = filebase64sha256(
    format(
      "%s/emr-launcher-%s.zip",
      var.emr_launcher_zip["base_path"],
      var.emr_launcher_zip["version"]
    )
  )
  publish = false
  timeout = 60

  environment {
    variables = {
      EMR_LAUNCHER_CONFIG_S3_BUCKET = data.terraform_remote_state.common.outputs.config_bucket.id
      EMR_LAUNCHER_CONFIG_S3_FOLDER = "emr/${local.emr_cluster_name}"
      EMR_LAUNCHER_LOG_LEVEL        = "debug"
    }
  }

  tags = {
    Name = "${local.emr_cluster_name}_emr_launcher"
  }

  depends_on = [aws_cloudwatch_log_group.dataworks_aws_corporate_data_ingestion_emr_launcher_log_group]
}

resource "aws_iam_role" "dataworks_aws_corporate_data_ingestion_emr_launcher_lambda_role" {
  name               = "${local.emr_cluster_name}_emr_launcher_lambda_role"
  assume_role_policy = data.aws_iam_policy_document.dataworks_aws_corporate_data_ingestion_emr_launcher_assume_policy.json
  tags = {
    Name = "${local.emr_cluster_name}_emr_launcher_lambda_role"
  }
}

data "aws_iam_policy_document" "dataworks_aws_corporate_data_ingestion_emr_launcher_assume_policy" {
  statement {
    sid     = "EMRLauncherLambdaAssumeRolePolicy"
    effect  = "Allow"
    actions = ["sts:AssumeRole"]

    principals {
      identifiers = ["lambda.amazonaws.com"]
      type        = "Service"
    }
  }
}

data "aws_iam_policy_document" "dataworks_aws_corporate_data_ingestion_emr_launcher_read_s3_policy" {
  statement {
    effect = "Allow"
    actions = [
      "s3:GetObject",
    ]
    resources = [
      format("arn:aws:s3:::%s/emr/${local.emr_cluster_name}/*", data.terraform_remote_state.common.outputs.config_bucket.id)
    ]
  }
  statement {
    effect = "Allow"
    actions = [
      "kms:Decrypt",
    ]
    resources = [
      data.terraform_remote_state.common.outputs.config_bucket_cmk.arn
    ]
  }
}

data "aws_iam_policy_document" "dataworks_aws_corporate_data_ingestion_emr_launcher_runjobflow_policy" {
  statement {
    effect = "Allow"
    actions = [
      "elasticmapreduce:RunJobFlow",
      "elasticmapreduce:AddTags",
    ]
    resources = [
      "*"
    ]
  }
}

data "aws_iam_policy_document" "dataworks_aws_corporate_data_ingestion_emr_launcher_pass_role_document" {
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

resource "aws_iam_policy" "dataworks_aws_corporate_data_ingestion_emr_launcher_read_s3_policy" {
  name        = "${local.emr_cluster_name}ReadS3"
  description = "Allow dataworks_aws_corporate_data_ingestion to read from S3 bucket"
  policy      = data.aws_iam_policy_document.dataworks_aws_corporate_data_ingestion_emr_launcher_read_s3_policy.json
  tags = {
    Name = "${local.emr_cluster_name}_emr_launcher_read_s3_policy"
  }
}

resource "aws_iam_policy" "dataworks_aws_corporate_data_ingestion_emr_launcher_runjobflow_policy" {
  name        = "${local.emr_cluster_name}RunJobFlow"
  description = "Allow dataworks_aws_corporate_data_ingestion to run job flow"
  policy      = data.aws_iam_policy_document.dataworks_aws_corporate_data_ingestion_emr_launcher_runjobflow_policy.json
  tags = {
    Name = "dataworks_aws_corporate_data_ingestion_emr_launcher_runjobflow_policy"
  }
}

resource "aws_iam_policy" "dataworks_aws_corporate_data_ingestion_emr_launcher_pass_role_policy" {
  name        = "${local.emr_cluster_name}PassRole"
  description = "Allow dataworks_aws_corporate_data_ingestion to pass role"
  policy      = data.aws_iam_policy_document.dataworks_aws_corporate_data_ingestion_emr_launcher_pass_role_document.json
  tags = {
    Name = "dataworks_aws_corporate_data_ingestion_emr_launcher_pass_role_policy"
  }
}

resource "aws_iam_role_policy_attachment" "dataworks_aws_corporate_data_ingestion_emr_launcher_read_s3_attachment" {
  role       = aws_iam_role.dataworks_aws_corporate_data_ingestion_emr_launcher_lambda_role.name
  policy_arn = aws_iam_policy.dataworks_aws_corporate_data_ingestion_emr_launcher_read_s3_policy.arn
}

resource "aws_iam_role_policy_attachment" "dataworks_aws_corporate_data_ingestion_emr_launcher_runjobflow_attachment" {
  role       = aws_iam_role.dataworks_aws_corporate_data_ingestion_emr_launcher_lambda_role.name
  policy_arn = aws_iam_policy.dataworks_aws_corporate_data_ingestion_emr_launcher_runjobflow_policy.arn
}

resource "aws_iam_role_policy_attachment" "dataworks_aws_corporate_data_ingestion_emr_launcher_pass_role_attachment" {
  role       = aws_iam_role.dataworks_aws_corporate_data_ingestion_emr_launcher_lambda_role.name
  policy_arn = aws_iam_policy.dataworks_aws_corporate_data_ingestion_emr_launcher_pass_role_policy.arn
}

resource "aws_iam_role_policy_attachment" "dataworks_aws_corporate_data_ingestion_emr_launcher_policy_execution" {
  role       = aws_iam_role.dataworks_aws_corporate_data_ingestion_emr_launcher_lambda_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaVPCAccessExecutionRole"
}

resource "aws_iam_policy" "dataworks_aws_corporate_data_ingestion_emr_launcher_getsecrets" {
  name        = "${local.emr_cluster_name}GetSecrets"
  description = "Allow dataworks_aws_corporate_data_ingestion function to get secrets"
  policy      = data.aws_iam_policy_document.dataworks_aws_corporate_data_ingestion_emr_launcher_getsecrets.json
}

data "aws_iam_policy_document" "dataworks_aws_corporate_data_ingestion_emr_launcher_getsecrets" {
  statement {
    effect = "Allow"

    actions = [
      "secretsmanager:GetSecretValue",
    ]

    resources = [
      data.terraform_remote_state.internal_compute.outputs.metadata_store_users.corporate_data_ingestion_writer.secret_arn,
    ]
  }
}

resource "aws_iam_role_policy_attachment" "dataworks_aws_corporate_data_ingestion_emr_launcher_getsecrets" {
  role       = aws_iam_role.dataworks_aws_corporate_data_ingestion_emr_launcher_lambda_role.name
  policy_arn = aws_iam_policy.dataworks_aws_corporate_data_ingestion_emr_launcher_getsecrets.arn
}

resource "aws_cloudwatch_log_group" "dataworks_aws_corporate_data_ingestion_emr_launcher_log_group" {
  name              = "/aws/lambda/dataworks_aws_corporate_data_ingestion_emr_launcher"
  retention_in_days = 180
}
