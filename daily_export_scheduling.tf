resource "aws_cloudwatch_event_rule" "utc_06_00_daily" {
  count               = local.run_daily_export_on_schedule[local.environment] == true ? 1 : 0
  name                = "corporate_data_ingestion_utc_06_00_daily"
  description         = "6AM every day"
  schedule_expression = "cron(00 06 ? * * *)"
}

resource "aws_cloudwatch_event_target" "run_daily_export" {
  count     = local.run_daily_export_on_schedule[local.environment] == true ? 1 : 0
  rule      = aws_cloudwatch_event_rule.utc_06_00_daily[0].name
  target_id = aws_sns_topic.corporate_data_ingestion.name
  arn       = aws_sns_topic.corporate_data_ingestion.arn
  input     = <<JSON
  {"launch_type": "scheduled"}
  JSON
}
