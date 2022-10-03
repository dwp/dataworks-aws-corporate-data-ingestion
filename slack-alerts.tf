resource "aws_cloudwatch_event_rule" "dataworks_aws_corporate_data_ingestion_failed" {
  name          = "${local.emr_cluster_name}_failed"
  description   = "Sends failed message to slack when dataworks_aws_corporate_data_ingestion cluster terminates with errors"
  event_pattern = <<EOF
{
  "source": [
    "aws.emr"
  ],
  "detail-type": [
    "EMR Cluster State Change"
  ],
  "detail": {
    "state": [
      "TERMINATED_WITH_ERRORS"
    ],
    "name": [
      "${local.emr_cluster_name}"
    ]
  }
}
EOF

  tags = {
    Name = "${local.emr_cluster_name}_failed"
  }
}

resource "aws_cloudwatch_event_rule" "dataworks_aws_corporate_data_ingestion_terminated" {
  name          = "${local.emr_cluster_name}_terminated"
  description   = "Sends failed message to slack when dataworks_aws_corporate_data_ingestion cluster terminates by user request"
  event_pattern = <<EOF
{
  "source": [
    "aws.emr"
  ],
  "detail-type": [
    "EMR Cluster State Change"
  ],
  "detail": {
    "state": [
      "TERMINATED"
    ],
    "name": [
      "${local.emr_cluster_name}"
    ],
    "stateChangeReason": [
      "{\"code\":\"USER_REQUEST\",\"message\":\"User request\"}"
    ]
  }
}
EOF

  tags = {
    Name = "${local.emr_cluster_name}_terminated"
  }
}

resource "aws_cloudwatch_event_rule" "dataworks_aws_corporate_data_ingestion_success" {
  name          = "${local.emr_cluster_name}_success"
  description   = "checks that all steps complete"
  event_pattern = <<EOF
{
  "source": [
    "aws.emr"
  ],
  "detail-type": [
    "EMR Cluster State Change"
  ],
  "detail": {
    "state": [
      "TERMINATED"
    ],
    "name": [
      "${local.emr_cluster_name}"
    ],
    "stateChangeReason": [
      "{\"code\":\"ALL_STEPS_COMPLETED\",\"message\":\"Steps completed\"}"
    ]
  }
}
EOF

  tags = {
    Name = "${local.emr_cluster_name}_success"
  }
}

resource "aws_cloudwatch_event_rule" "dataworks_aws_corporate_data_ingestion_success_with_errors" {
  name          = "${local.emr_cluster_name}_success_with_errors"
  description   = "checks that all mandatory steps complete but with failures on non mandatory steps"
  event_pattern = <<EOF
{
  "source": [
    "aws.emr"
  ],
  "detail-type": [
    "EMR Cluster State Change"
  ],
  "detail": {
    "state": [
      "TERMINATED"
    ],
    "name": [
      "${local.emr_cluster_name}"
    ],
    "stateChangeReason": [
      "{\"code\":\"STEP_FAILURE\",\"message\":\"Steps completed with errors\"}"
    ]
  }
}
EOF

  tags = {
    Name = "${local.emr_cluster_name}_success_with_errors"
  }
}

resource "aws_cloudwatch_event_rule" "dataworks_aws_corporate_data_ingestion_running" {
  name          = "${local.emr_cluster_name}_running"
  description   = "checks that dataworks_aws_corporate_data_ingestion is running"
  event_pattern = <<EOF
{
  "source": [
    "aws.emr"
  ],
  "detail-type": [
    "EMR Cluster State Change"
  ],
  "detail": {
    "state": [
      "RUNNING"
    ],
    "name": [
      "${local.emr_cluster_name}"
    ]
  }
}
EOF

  tags = {
    Name = "${local.emr_cluster_name}_running"
  }
}

resource "aws_cloudwatch_metric_alarm" "dataworks_aws_corporate_data_ingestion_failed" {
  count                     = local.dataworks_aws_corporate_data_ingestion_alerts[local.environment] == true ? 1 : 0
  alarm_name                = "${local.emr_cluster_name}_failed"
  comparison_operator       = "GreaterThanOrEqualToThreshold"
  evaluation_periods        = "1"
  metric_name               = "TriggeredRules"
  namespace                 = "AWS/Events"
  period                    = "60"
  statistic                 = "Sum"
  threshold                 = "1"
  alarm_description         = "This metric monitors cluster failed with errors"
  insufficient_data_actions = []
  alarm_actions             = [data.terraform_remote_state.security-tools.outputs.sns_topic_london_monitoring.arn]
  dimensions = {
    RuleName = aws_cloudwatch_event_rule.dataworks_aws_corporate_data_ingestion_failed.name
  }
  tags = {
    Name              = "${local.emr_cluster_name}_failed",
    notification_type = "Error",
    severity          = "Critical"
  }
}

resource "aws_cloudwatch_metric_alarm" "dataworks_aws_corporate_data_ingestion_terminated" {
  count                     = local.dataworks_aws_corporate_data_ingestion_alerts[local.environment] == true ? 1 : 0
  alarm_name                = "${local.emr_cluster_name}_terminated"
  comparison_operator       = "GreaterThanOrEqualToThreshold"
  evaluation_periods        = "1"
  metric_name               = "TriggeredRules"
  namespace                 = "AWS/Events"
  period                    = "60"
  statistic                 = "Sum"
  threshold                 = "1"
  alarm_description         = "This metric monitors cluster terminated by user request"
  insufficient_data_actions = []
  alarm_actions             = [data.terraform_remote_state.security-tools.outputs.sns_topic_london_monitoring.arn]
  dimensions = {
    RuleName = aws_cloudwatch_event_rule.dataworks_aws_corporate_data_ingestion_terminated.name
  }
  tags = {
    Name              = "${local.emr_cluster_name}_terminated",
    notification_type = "Information",
    severity          = "High"
  }
}

resource "aws_cloudwatch_metric_alarm" "dataworks_aws_corporate_data_ingestion_success" {
  count                     = local.dataworks_aws_corporate_data_ingestion_alerts[local.environment] == true ? 1 : 0
  alarm_name                = "${local.emr_cluster_name}_success"
  comparison_operator       = "GreaterThanOrEqualToThreshold"
  evaluation_periods        = "1"
  metric_name               = "TriggeredRules"
  namespace                 = "AWS/Events"
  period                    = "60"
  statistic                 = "Sum"
  threshold                 = "1"
  alarm_description         = "Monitoring dataworks_aws_corporate_data_ingestion completion"
  insufficient_data_actions = []
  alarm_actions             = [data.terraform_remote_state.security-tools.outputs.sns_topic_london_monitoring.arn]
  dimensions = {
    RuleName = aws_cloudwatch_event_rule.dataworks_aws_corporate_data_ingestion_success.name
  }
  tags = {
    Name              = "${local.emr_cluster_name}_success",
    notification_type = "Information",
    severity          = "Critical"
  }
}

resource "aws_cloudwatch_metric_alarm" "dataworks_aws_corporate_data_ingestion_success_with_errors" {
  count                     = local.dataworks_aws_corporate_data_ingestion_alerts[local.environment] == true ? 1 : 0
  alarm_name                = "${local.emr_cluster_name}_success_with_errors"
  comparison_operator       = "GreaterThanOrEqualToThreshold"
  evaluation_periods        = "1"
  metric_name               = "TriggeredRules"
  namespace                 = "AWS/Events"
  period                    = "60"
  statistic                 = "Sum"
  threshold                 = "1"
  alarm_description         = "Monitoring dataworks_aws_corporate_data_ingestion completion"
  insufficient_data_actions = []
  alarm_actions             = [data.terraform_remote_state.security-tools.outputs.sns_topic_london_monitoring.arn]
  dimensions = {
    RuleName = aws_cloudwatch_event_rule.dataworks_aws_corporate_data_ingestion_success_with_errors.name
  }
  tags = {
    Name              = "${local.emr_cluster_name}_success_with_errors",
    notification_type = "Warning",
    severity          = "High"
  }
}

resource "aws_cloudwatch_metric_alarm" "dataworks_aws_corporate_data_ingestion_running" {
  count                     = local.dataworks_aws_corporate_data_ingestion_alerts[local.environment] == true ? 1 : 0
  alarm_name                = "${local.emr_cluster_name}_running"
  comparison_operator       = "GreaterThanOrEqualToThreshold"
  evaluation_periods        = "1"
  metric_name               = "TriggeredRules"
  namespace                 = "AWS/Events"
  period                    = "60"
  statistic                 = "Sum"
  threshold                 = "1"
  alarm_description         = "Monitoring dataworks_aws_corporate_data_ingestion running"
  insufficient_data_actions = []
  alarm_actions             = [data.terraform_remote_state.security-tools.outputs.sns_topic_london_monitoring.arn]
  dimensions = {
    RuleName = aws_cloudwatch_event_rule.dataworks_aws_corporate_data_ingestion_running.name
  }
  tags = {
    Name              = "${local.emr_cluster_name}_running",
    notification_type = "Information",
    severity          = "Critical"
  }
}
