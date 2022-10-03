data "aws_iam_policy_document" "aws_emr_template_repository_write" {
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

resource "aws_iam_policy" "aws_emr_template_repository_read_write_processed_bucket" {
  name        = "aws-emr-template-repository-ReadWriteAccessToProcessedBucket"
  description = "Allow read and write access to the processed bucket"
  policy      = data.aws_iam_policy_document.aws_emr_template_repository_write.json
  tags = {
    Name = "aws_emr_template_repository_read_write_processed_bucket"
  }
}

