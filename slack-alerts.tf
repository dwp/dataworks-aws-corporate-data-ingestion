# Event rules for the cluster
resource "aws_cloudwatch_event_rule" "dataworks_aws_corporate_data_ingestion_cluster_alerts" {
  for_each      = { for alert_rule in local.alert_rules_cluster_mapping : "${alert_rule.alert_type}-${alert_rule.alert_name}" => alert_rule }
  name          = "${local.emr_cluster_name}_${each.key}"
  description   = "Sends ${each.value.alert_name} message to slack when ${local.emr_cluster_name} ${each.value.alert_type} ${each.value.alert_state}"
  event_pattern = jsonencode(each.value.alert_rule)

  tags = {
    Name = "${local.emr_cluster_name}_${each.key}"
  }
}

#Event rules for each step in a defined collection 
resource "aws_cloudwatch_event_rule" "dataworks_aws_corporate_data_ingestion_step_alerts" {
  for_each      = { for alert_rule in local.alert_rules_step_mapping : "${alert_rule.alert_type}-${alert_rule.alert_collection}-${alert_rule.alert_name}" => alert_rule }
  name          = "${local.emr_cluster_name}_${each.key}"
  description   = "Sends ${each.value.alert_name} message to slack when ${local.emr_cluster_name} ${each.value.alert_type} ${each.value.alert_state}"
  event_pattern = jsonencode(each.value.alert_rule)

  tags = {
    Name = "${local.emr_cluster_name}_${each.key}"
  }
}

# Alarms for the cluster
resource "aws_cloudwatch_metric_alarm" "dataworks_aws_corporate_data_ingestion_cluster" {
  for_each                  = local.dataworks_aws_corporate_data_ingestion_alerts[local.environment] == true ? { for alert_rule in local.alert_rules_cluster_mapping : "${alert_rule.alert_type}-${alert_rule.alert_name}" => alert_rule } : {}
  alarm_name                = "${local.emr_cluster_name}-${each.key}"
  comparison_operator       = "GreaterThanOrEqualToThreshold"
  evaluation_periods        = "1"
  metric_name               = "TriggeredRules"
  namespace                 = "AWS/Events"
  period                    = "60"
  statistic                 = "Sum"
  threshold                 = "1"
  alarm_description         = "This metric monitors ${local.emr_cluster_name} ${each.key}"
  insufficient_data_actions = []
  alarm_actions             = [data.terraform_remote_state.security-tools.outputs.sns_topic_london_monitoring.arn]
  dimensions = {
    RuleName = aws_cloudwatch_event_rule.dataworks_aws_corporate_data_ingestion_cluster_alerts[each.key].name
  }
  tags = {
    Name              = "${local.emr_cluster_name}_${each.key}",
    notification_type = "${each.value.notification_type}",
    severity          = "${each.value.severity}"
  }
}

# Alarms for the steps
resource "aws_cloudwatch_metric_alarm" "dataworks_aws_corporate_data_ingestion_step" {
  for_each                  = local.dataworks_aws_corporate_data_ingestion_alerts[local.environment] == true ? { for alert_rule in local.alert_rules_step_mapping : "${alert_rule.alert_type}-${alert_rule.alert_collection}-${alert_rule.alert_name}" => alert_rule } : {}
  alarm_name                = "${local.emr_cluster_name}-${each.key}"
  comparison_operator       = "GreaterThanOrEqualToThreshold"
  evaluation_periods        = "1"
  metric_name               = "TriggeredRules"
  namespace                 = "AWS/Events"
  period                    = "60"
  statistic                 = "Sum"
  threshold                 = "1"
  alarm_description         = "This metric monitors ${local.emr_cluster_name} ${each.key}"
  insufficient_data_actions = []
  alarm_actions             = [data.terraform_remote_state.security-tools.outputs.sns_topic_london_monitoring.arn]
  dimensions = {
    RuleName = aws_cloudwatch_event_rule.dataworks_aws_corporate_data_ingestion_step_alerts[each.key].name
  }
  tags = {
    Name              = "${local.emr_cluster_name}_${each.key}",
    notification_type = "${each.value.notification_type}",
    severity          = "${each.value.severity}"
  }
}