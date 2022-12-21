data "aws_iam_user" "breakglass" {
  user_name = "breakglass"
}

data "aws_iam_role" "ci" {
  name = "ci"
}

data "aws_iam_role" "administrator" {
  name = "administrator"
}

data "aws_iam_role" "aws_config" {
  name = "aws_config"
}

data "aws_iam_policy_document" "dataworks_aws_corporate_data_ingestion_ebs_cmk" {
  statement {
    sid    = "EnableIAMPermissionsBreakglass"
    effect = "Allow"

    principals {
      type        = "AWS"
      identifiers = [data.aws_iam_user.breakglass.arn]
    }

    actions   = ["kms:*"]
    resources = ["*"]
  }

  statement {
    sid    = "EnableIAMPermissionsAccount"
    effect = "Allow"

    principals {
      type        = "AWS"
      identifiers = [local.account[local.environment]]
    }

    actions   = ["kms:*"]
    resources = ["*"]
  }

  statement {
    sid    = "EnableIAMPermissionsCI"
    effect = "Allow"

    principals {
      identifiers = [data.aws_iam_role.ci.arn]
      type        = "AWS"
    }

    actions   = ["kms:*"]
    resources = ["*"]
  }

  statement {
    sid    = "DenyCIEncryptDecrypt"
    effect = "Deny"

    principals {
      type        = "AWS"
      identifiers = [data.aws_iam_role.ci.arn]
    }

    actions = [
      "kms:Encrypt",
      "kms:Decrypt",
      "kms:ImportKeyMaterial",
      "kms:ReEncryptFrom",
    ]
    resources = ["*"]
  }

  statement {
    sid    = "EnableIAMPermissionsAdministrator"
    effect = "Allow"

    principals {
      type        = "AWS"
      identifiers = [data.aws_iam_role.administrator.arn]
    }

    actions = [
      "kms:Describe*",
      "kms:List*",
      "kms:Get*"
    ]
    resources = ["*"]
  }

  statement {
    sid    = "EnableAWSConfigManagerScanForSecurityHub"
    effect = "Allow"

    principals {
      type        = "AWS"
      identifiers = [data.aws_iam_role.aws_config.arn]
    }

    actions = [
      "kms:Describe*",
      "kms:Get*",
      "kms:List*"
    ]

    resources = ["*"]
  }

  statement {
    sid    = "EnableIAMPermissionsAnalyticDatasetGen"
    effect = "Allow"

    principals {
      type        = "AWS"
      identifiers = [aws_iam_role.dataworks_aws_corporate_data_ingestion.arn, aws_iam_role.dataworks_aws_corporate_data_ingestion_emr_service.arn]
    }

    actions = [
      "kms:Encrypt",
      "kms:Decrypt",
      "kms:ReEncrypt*",
      "kms:GenerateDataKey*",
      "kms:DescribeKey"
    ]

    resources = ["*"]

  }

  statement {
    sid    = "Allowdataworks_aws_corporate_data_ingestionServiceGrant"
    effect = "Allow"

    principals {
      type        = "AWS"
      identifiers = [aws_iam_role.dataworks_aws_corporate_data_ingestion.arn, aws_iam_role.dataworks_aws_corporate_data_ingestion_emr_service.arn]
    }

    actions = ["kms:CreateGrant"]

    resources = ["*"]

    condition {
      test     = "Bool"
      variable = "kms:GrantIsForAWSResource"
      values   = ["true"]
    }
  }
}

resource "aws_kms_key" "dataworks_aws_corporate_data_ingestion_ebs_cmk" {
  description             = "Encrypts dataworks_aws_corporate_data_ingestion EBS volumes"
  deletion_window_in_days = 7
  is_enabled              = true
  enable_key_rotation     = true
  policy                  = data.aws_iam_policy_document.dataworks_aws_corporate_data_ingestion_ebs_cmk.json

  # ProtectsSensitiveData = "True" - the dataworks_aws_corporate_data_ingestion cluster decrypts sensitive data
  # that it reads from HBase. It can potentially spill this to disk if it can't
  # hold it all in memory, which is likely given the size of the dataset.
  tags = {
    Name                  = "${local.emr_cluster_name}_ebs_cmk"
    ProtectsSensitiveData = "True"
  }
}

resource "aws_kms_alias" "dataworks_aws_corporate_data_ingestion_ebs_cmk" {
  name          = "alias/${local.emr_cluster_name}_ebs_cmk"
  target_key_id = aws_kms_key.dataworks_aws_corporate_data_ingestion_ebs_cmk.key_id
}

data "aws_iam_policy_document" "dataworks_aws_corporate_data_ingestion_ebs_cmk_encrypt" {
  statement {
    effect = "Allow"

    actions = [
      "kms:Encrypt",
      "kms:Decrypt",
      "kms:ReEncrypt*",
      "kms:GenerateDataKey*",
      "kms:DescribeKey",
    ]

    resources = [aws_kms_key.dataworks_aws_corporate_data_ingestion_ebs_cmk.arn]
  }

  statement {
    effect = "Allow"

    actions = ["kms:CreateGrant"]

    resources = [aws_kms_key.dataworks_aws_corporate_data_ingestion_ebs_cmk.arn]
    condition {
      test     = "Bool"
      variable = "kms:GrantIsForAWSResource"
      values   = ["true"]
    }
  }
}

resource "aws_iam_policy" "dataworks_aws_corporate_data_ingestion_ebs_cmk_encrypt" {
  name        = "dataworks-aws-corporate-data-ingestion-EbsCmkEncrypt"
  description = "Allow encryption and decryption using the dataworks_aws_corporate_data_ingestion EBS CMK"
  policy      = data.aws_iam_policy_document.dataworks_aws_corporate_data_ingestion_ebs_cmk_encrypt.json
  tags = {
    Name = "dataworks_aws_corporate_data_ingestion_ebs_cmk_encrypt"
  }
}
