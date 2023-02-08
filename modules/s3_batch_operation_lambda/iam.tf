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

  statement {
    sid    = "AllowReadWriteOnDestinationBucket"
    effect = "Allow"
    actions = [
      "s3:ListBucket",
      "s3:PutObject",
      "s3:PutObjectVersionAcl",
      "s3:PutObjectAcl",
      "s3:PutObjectVersionTagging",
      "s3:PutObjectTagging",
      "s3:GetObject",
      "s3:GetObjectVersion",
      "s3:GetObjectAcl",
      "s3:GetObjectTagging",
      "s3:GetObjectVersionAcl",
      "s3:GetObjectVersionTagging"
    ]
    resources = [
      "${var.destination_s3_bucket_arn}/*",
      var.destination_s3_bucket_arn,
    ]
  }

  statement {
    sid    = "KMSOnDestinationBucket"
    effect = "Allow"
    actions = [
      "kms:Decrypt",
      "kms:GenerateDataKey",
      "kms:Encrypt"
    ]

    resources = [
      var.destination_bucket_kms_arn
    ]
  }

  statement {
    sid    = "AllowReadOnSourceBucket"
    effect = "Allow"
    actions = [
      "s3:GetObject",
      "s3:GetObjectVersion",
      "s3:GetObjectAcl",
      "s3:GetObjectTagging",
      "s3:GetObjectVersionAcl",
      "s3:GetObjectVersionTagging"
    ]
    resources = [for s in var.source_s3_bucket_prefix : join("/", [var.source_s3_bucket_arn, s, "*"])]

  }

  statement {
    sid    = "AllowListOnSourceBucket"
    effect = "Allow"
    actions = [
      "s3:ListBucket",
    ]
    resources = [
      var.source_s3_bucket_arn
    ]

  }

  statement {
    sid    = "KMSOnSourceBucket"
    effect = "Allow"
    actions = [
      "kms:Decrypt"
    ]

    resources = [
      var.source_bucket_kms_arn
    ]
  }
}
