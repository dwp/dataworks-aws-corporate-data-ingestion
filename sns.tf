resource "aws_sns_topic" "corporate_data_ingestion" {
  name = "corporate_data_ingestion_cw_trigger_sns"
  tags = {
    Name = "corporate_data_ingestion_cw_trigger_sns"
  }
}

resource "aws_sns_topic_policy" "corporate_data_ingestion" {
  arn    = aws_sns_topic.corporate_data_ingestion.arn
  policy = data.aws_iam_policy_document.corporate_data_ingestion_sns_topic_policy_document.json
}

data "aws_iam_policy_document" "corporate_data_ingestion_sns_topic_policy_document" {
  policy_id = "CorporateDataIngestionSNSTopicPolicyDocument"

  statement {
    sid = "DefaultPolicy"

    actions = [
      "SNS:GetTopicAttributes",
      "SNS:SetTopicAttributes",
      "SNS:AddPermission",
      "SNS:RemovePermission",
      "SNS:DeleteTopic",
      "SNS:Subscribe",
      "SNS:ListSubscriptionsByTopic",
      "SNS:Publish",
      "SNS:Receive",
    ]

    condition {
      test     = "StringEquals"
      variable = "AWS:SourceOwner"

      values = [
        local.account[local.environment],
      ]
    }

    effect = "Allow"

    principals {
      type        = "AWS"
      identifiers = ["*"]
    }

    resources = [
      aws_sns_topic.corporate_data_ingestion.arn,
    ]
  }

  statement {
    sid = "AllowCloudwatchEventsToPublishToTopic"

    actions = [
      "SNS:Publish",
    ]

    effect = "Allow"

    principals {
      type        = "Service"
      identifiers = ["events.amazonaws.com"]
    }

    resources = [
      aws_sns_topic.corporate_data_ingestion.arn,
    ]
  }
}

resource "aws_sns_topic_subscription" "corporate_data_ingestion" {
  topic_arn = aws_sns_topic.corporate_data_ingestion.arn
  protocol  = "lambda"
  endpoint  = aws_lambda_function.dataworks_aws_corporate_data_ingestion_emr_launcher.arn
}

resource "aws_lambda_permission" "corporate_data_ingestion_sns_lambda_permission" {
  statement_id  = "CorporateDataIngestionAllowExecutionFromSNS"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.dataworks_aws_corporate_data_ingestion_emr_launcher.function_name
  principal     = "sns.amazonaws.com"
  source_arn    = aws_sns_topic.corporate_data_ingestion.arn
}

output "uc_export_to_crown_controller_messages_sns_topic" {
  value = {
    arn  = aws_sns_topic.corporate_data_ingestion.arn
    name = aws_sns_topic.corporate_data_ingestion.name
  }
}
