data "aws_iam_policy_document" "dataworks_aws_corporate_data_ingestion_read_write_corporate_bucket" {
  statement {
    effect = "Allow"

    actions = [
      "s3:GetBucketLocation",
      "s3:ListBucket",
    ]

    resources = [
      data.terraform_remote_state.aws_ingestion.outputs.corporate_storage_bucket.arn,
    ]
  }

  statement {
    effect = "Allow"

    actions = [
      "s3:GetObject*",
    ]

    resources = [
      "${data.terraform_remote_state.aws_ingestion.outputs.corporate_storage_bucket.arn}/corporate_storage/*",
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
      data.terraform_remote_state.aws_ingestion.outputs.input_bucket_cmk.arn,
    ]
  }
}

resource "aws_iam_policy" "dataworks_aws_corporate_data_ingestion_read_write_corporate_bucket" {
  name        = "CorporateDataIngestionReadParquetFromCorporateBucket"
  description = "Allow writing of Corporate Data Ingestion files to corporate bucket"
  policy      = data.aws_iam_policy_document.dataworks_aws_corporate_data_ingestion_read_write_corporate_bucket.json
}
