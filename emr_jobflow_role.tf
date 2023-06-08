data "aws_iam_policy_document" "ec2_assume_role" {
  statement {
    effect = "Allow"

    principals {
      type        = "Service"
      identifiers = ["ec2.amazonaws.com"]
    }

    actions = ["sts:AssumeRole"]
  }
}

resource "aws_iam_role" "dataworks_aws_corporate_data_ingestion" {
  name               = local.emr_cluster_name
  assume_role_policy = data.aws_iam_policy_document.ec2_assume_role.json
  tags = {
    Name = local.emr_cluster_name
  }
}

resource "aws_iam_instance_profile" "dataworks_aws_corporate_data_ingestion" {
  name = local.emr_cluster_name
  role = aws_iam_role.dataworks_aws_corporate_data_ingestion.id
  tags = {
    Name = local.emr_cluster_name
  }
}

resource "aws_iam_role_policy_attachment" "ec2_for_ssm_attachment" {
  role       = aws_iam_role.dataworks_aws_corporate_data_ingestion.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonSSMManagedInstanceCore"
}

resource "aws_iam_role_policy_attachment" "dataworks_aws_corporate_data_ingestion_ebs_cmk" {
  role       = aws_iam_role.dataworks_aws_corporate_data_ingestion.name
  policy_arn = aws_iam_policy.dataworks_aws_corporate_data_ingestion_ebs_cmk_encrypt.arn
}

resource "aws_iam_role_policy_attachment" "dataworks_aws_corporate_data_ingestion_acm" {
  role       = aws_iam_role.dataworks_aws_corporate_data_ingestion.name
  policy_arn = aws_iam_policy.dataworks_aws_corporate_data_ingestion_acm.arn
}

data "aws_iam_policy_document" "dataworks_aws_corporate_data_ingestion_extra_ssm_properties" {
  statement {
    effect = "Allow"
    actions = [
      "cloudwatch:PutMetricData",
    ]

    resources = [
      "*",
    ]
  }
  statement {
    effect = "Allow"
    actions = [
      "ec2:DescribeInstanceStatus",
    ]

    resources = [
      "*",
    ]
  }
  statement {
    effect = "Allow"
    actions = [
      "ds:CreateComputer",
      "ds:DescribeDirectories",
    ]
    resources = [
      "*",
    ]
  }
  statement {
    effect = "Allow"
    actions = [
      "logs:CreateLogGroup",
      "logs:CreateLogStream",
      "logs:DescribeLogGroups",
      "logs:DescribeLogStreams",
      "logs:PutLogEvents",
    ]
    resources = [
      "*",
    ]
  }

  statement {
    effect = "Allow"

    actions = [
      "s3:GetBucketLocation",
      "s3:ListBucket",
    ]

    resources = [
      "arn:aws:s3:::eu-west-2.elasticmapreduce",
    ]
  }

  statement {
    effect = "Allow"

    actions = [
      "s3:Get*",
      "s3:List*",
    ]

    resources = [
      "arn:aws:s3:::eu-west-2.elasticmapreduce/libs/script-runner/*",
    ]
  }
}

resource "aws_iam_policy" "dataworks_aws_corporate_data_ingestion_extra_ssm_properties" {
  name        = "${local.emr_cluster_name}AwsEmrTemplateRepositoryExtraSSM"
  description = "Additional properties to allow for SSM and writing logs"
  policy      = data.aws_iam_policy_document.dataworks_aws_corporate_data_ingestion_extra_ssm_properties.json
  tags = {
    Name = "dataworks_aws_corporate_data_ingestion_extra_ssm_properties"
  }
}

resource "aws_iam_role_policy_attachment" "dataworks_aws_corporate_data_ingestion_extra_ssm_properties" {
  role       = aws_iam_role.dataworks_aws_corporate_data_ingestion.name
  policy_arn = aws_iam_policy.dataworks_aws_corporate_data_ingestion_extra_ssm_properties.arn
}

resource "aws_iam_role_policy_attachment" "dataworks_aws_corporate_data_ingestion_certificates" {
  role       = aws_iam_role.dataworks_aws_corporate_data_ingestion.name
  policy_arn = aws_iam_policy.dataworks_aws_corporate_data_ingestion_certificates.arn
}

data "aws_iam_policy_document" "dataworks_aws_corporate_data_ingestion_write_logs" {
  statement {
    effect = "Allow"

    actions = [
      "s3:GetBucketLocation",
      "s3:ListBucket",
      "s3:GetEncryptionConfiguration",
      "s3:AbortMultipartUpload",
      "s3:ListMultipartUploadParts",
      "s3:ListBucketMultipartUploads"
    ]

    resources = [
      data.terraform_remote_state.security-tools.outputs.logstore_bucket.arn,
    ]
  }

  statement {
    effect = "Allow"

    actions = [
      "s3:GetObject*",
      "s3:PutObject*",

    ]

    resources = [
      "${data.terraform_remote_state.security-tools.outputs.logstore_bucket.arn}/${local.s3_log_prefix}_$folder$",
      "${data.terraform_remote_state.security-tools.outputs.logstore_bucket.arn}/${local.s3_log_prefix}/*",
    ]
  }
}

resource "aws_iam_policy" "dataworks_aws_corporate_data_ingestion_write_logs" {
  name        = "${local.emr_cluster_name}WriteLogs"
  description = "Allow writing of dataworks_aws_corporate_data_ingestion logs"
  policy      = data.aws_iam_policy_document.dataworks_aws_corporate_data_ingestion_write_logs.json
  tags = {
    Name = "dataworks_aws_corporate_data_ingestion_write_logs"
  }
}

resource "aws_iam_role_policy_attachment" "dataworks_aws_corporate_data_ingestion_write_logs" {
  role       = aws_iam_role.dataworks_aws_corporate_data_ingestion.name
  policy_arn = aws_iam_policy.dataworks_aws_corporate_data_ingestion_write_logs.arn
}

data "aws_iam_policy_document" "dataworks_aws_corporate_data_ingestion_read_config" {
  statement {
    effect = "Allow"

    actions = [
      "s3:GetBucketLocation",
      "s3:ListBucket",
    ]

    resources = [
      data.terraform_remote_state.common.outputs.config_bucket.arn,
    ]
  }

  statement {
    effect = "Allow"

    actions = [
      "s3:GetObject*",
    ]

    resources = [
      "${data.terraform_remote_state.common.outputs.config_bucket.arn}/*",
    ]
  }

  statement {
    effect = "Allow"

    actions = [
      "kms:Decrypt",
      "kms:DescribeKey",
    ]

    resources = [
      data.terraform_remote_state.common.outputs.config_bucket_cmk.arn,
    ]
  }
}

resource "aws_iam_policy" "dataworks_aws_corporate_data_ingestion_read_config" {
  name        = "${local.emr_cluster_name}ReadConfig"
  description = "Allow reading of dataworks_aws_corporate_data_ingestion config files"
  policy      = data.aws_iam_policy_document.dataworks_aws_corporate_data_ingestion_read_config.json
  tags = {
    Name = "dataworks_aws_corporate_data_ingestion_read_config"
  }
}

resource "aws_iam_role_policy_attachment" "dataworks_aws_corporate_data_ingestion_read_config" {
  role       = aws_iam_role.dataworks_aws_corporate_data_ingestion.name
  policy_arn = aws_iam_policy.dataworks_aws_corporate_data_ingestion_read_config.arn
}

data "aws_iam_policy_document" "dataworks_aws_corporate_data_ingestion_read_artefacts" {
  statement {
    effect = "Allow"

    actions = [
      "s3:GetBucketLocation",
      "s3:ListBucket",
    ]

    resources = [
      data.terraform_remote_state.management_artefact.outputs.artefact_bucket.arn,
    ]
  }

  statement {
    effect = "Allow"

    actions = [
      "s3:GetObject*",
    ]

    resources = [
      "${data.terraform_remote_state.management_artefact.outputs.artefact_bucket.arn}/*",
    ]
  }

  statement {
    effect = "Allow"

    actions = [
      "kms:Decrypt",
      "kms:DescribeKey",
    ]

    resources = [
      data.terraform_remote_state.management_artefact.outputs.artefact_bucket.cmk_arn,
    ]
  }
}

resource "aws_iam_policy" "dataworks_aws_corporate_data_ingestion_read_artefacts" {
  name        = "${local.emr_cluster_name}ReadArtefacts"
  description = "Allow reading of dataworks_aws_corporate_data_ingestion software artefacts"
  policy      = data.aws_iam_policy_document.dataworks_aws_corporate_data_ingestion_read_artefacts.json
  tags = {
    Name = "dataworks_aws_corporate_data_ingestion_read_artefacts"
  }
}

resource "aws_iam_role_policy_attachment" "dataworks_aws_corporate_data_ingestion_read_artefacts" {
  role       = aws_iam_role.dataworks_aws_corporate_data_ingestion.name
  policy_arn = aws_iam_policy.dataworks_aws_corporate_data_ingestion_read_artefacts.arn
}

data "aws_iam_policy_document" "dataworks_aws_corporate_data_ingestion_write_dynamodb" {
  statement {
    effect = "Allow"

    actions = [
      "dynamodb:DescribeTable",
      "dynamodb:GetItem",
      "dynamodb:UpdateItem",
      "dynamodb:PutItem",
      "dynamodb:Scan",
      "dynamodb:GetRecords",
      "dynamodb:Query",
    ]

    resources = [
      data.terraform_remote_state.internal_compute.outputs.uc_export_crown_dynamodb_table.arn,
      data.terraform_remote_state.internal_compute.outputs.data_pipeline_metadata_dynamo.arn
    ]
  }
}

resource "aws_iam_policy" "dataworks_aws_corporate_data_ingestion_write_dynamodb" {
  name        = "${local.emr_cluster_name}AwsEmrTemplateRepositoryDynamoDB"
  description = "Allows read and write access todataworks_aws_corporate_data_ingestion's EMRFS DynamoDB table"
  policy      = data.aws_iam_policy_document.dataworks_aws_corporate_data_ingestion_write_dynamodb.json
  tags = {
    Name = "dataworks_aws_corporate_data_ingestion_write_dynamodb"
  }
}

resource "aws_iam_role_policy_attachment" "dataworks_aws_corporate_data_ingestion_dynamodb" {
  role       = aws_iam_role.dataworks_aws_corporate_data_ingestion.name
  policy_arn = aws_iam_policy.dataworks_aws_corporate_data_ingestion_write_dynamodb.arn
}

data "aws_iam_policy_document" "dataworks_aws_corporate_data_ingestion_metadata_change" {
  statement {
    effect = "Allow"

    actions = [
      "ec2:ModifyInstanceMetadataOptions",
      "ec2:*Tags",
    ]

    resources = [
      "arn:aws:ec2:${var.region}:${local.account[local.environment]}:instance/*",
    ]
  }
}


resource "aws_iam_role_policy_attachment" "dataworks_aws_corporate_data_ingestion_read_write_published_bucket" {
  role       = aws_iam_role.dataworks_aws_corporate_data_ingestion.name
  policy_arn = aws_iam_policy.dataworks_aws_corporate_data_ingestion_read_write_published_bucket.arn
}

resource "aws_iam_role_policy_attachment" "dataworks_aws_corporate_data_ingestion_read_write_corporate_bucket" {
  role       = aws_iam_role.dataworks_aws_corporate_data_ingestion.name
  policy_arn = aws_iam_policy.dataworks_aws_corporate_data_ingestion_read_write_corporate_bucket.arn
}

resource "aws_iam_role_policy_attachment" "dataworks_aws_corporate_data_ingestion_read_write_processed_bucket" {
  role       = aws_iam_role.dataworks_aws_corporate_data_ingestion.name
  policy_arn = aws_iam_policy.dataworks_aws_corporate_data_ingestion_read_write_processed_bucket.arn
}

resource "aws_iam_policy" "dataworks_aws_corporate_data_ingestion_metadata_change" {
  name        = "${local.emr_cluster_name}MetadataOptions"
  description = "Allow editing of Metadata Options"
  policy      = data.aws_iam_policy_document.dataworks_aws_corporate_data_ingestion_metadata_change.json
  tags = {
    Name = "dataworks_aws_corporate_data_ingestion_metadata_change"
  }
}

resource "aws_iam_role_policy_attachment" "dataworks_aws_corporate_data_ingestion_metadata_change" {
  role       = aws_iam_role.dataworks_aws_corporate_data_ingestion.name
  policy_arn = aws_iam_policy.dataworks_aws_corporate_data_ingestion_metadata_change.arn
}
