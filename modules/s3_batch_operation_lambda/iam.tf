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
}