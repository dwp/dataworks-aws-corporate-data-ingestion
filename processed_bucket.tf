data "aws_iam_policy_document" "dataworks_aws_corporate_data_ingestion_write" {
  statement {
    effect = "Allow"

    actions = [
      "s3:GetBucketLocation",
      "s3:ListBucket",
    ]

    resources = [
      data.terraform_remote_state.common.outputs.processed_bucket.arn,
    ]
  }

  statement {
    effect = "Allow"

    actions = [
      "s3:GetObject*",
      "s3:DeleteObject*",
      "s3:PutObject*",
    ]

    resources = [
      "${data.terraform_remote_state.common.outputs.processed_bucket.arn}/*"
    ]
  }

  statement {
    effect = "Allow"

    actions = [
      "kms:Encrypt",
      "kms:Decrypt",
      "kms:ReEncrypt*",
      "kms:GenerateDataKey*",
      "kms:DescribeKey",
    ]

    resources = [
      data.terraform_remote_state.common.outputs.processed_bucket_cmk.arn,
    ]
  }
}

resource "aws_iam_policy" "dataworks_aws_corporate_data_ingestion_read_write_processed_bucket" {
  name        = "dataworks-aws-corporate-data-ingestion-ReadWriteAccessToProcessedBucket"
  description = "Allow read and write access to the processed bucket"
  policy      = data.aws_iam_policy_document.dataworks_aws_corporate_data_ingestion_write.json
  tags = {
    Name = "dataworks_aws_corporate_data_ingestion_read_write_processed_bucket"
  }
}

