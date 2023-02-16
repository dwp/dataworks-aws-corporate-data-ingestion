resource "aws_cloudwatch_event_rule" "utc_06_00_daily" {
  count               = local.run_daily_export_on_schedule[local.environment] == true ? 1 : 0
  name                = "corporate_data_ingestion_utc_06_00_daily"
  description         = "6AM every day"
  schedule_expression = "cron(00 06 ? * * *)"
}

resource "aws_cloudwatch_event_target" "run_daily_export" {
  for_each  = local.collections_configuration
  rule      = aws_cloudwatch_event_rule.utc_06_00_daily[0].name
  target_id = aws_sns_topic.corporate_data_ingestion.name
  arn       = aws_sns_topic.corporate_data_ingestion.arn
  input     = <<JSON
  {
    "s3_overrides": null,
    "overrides": {
      "Steps": [{
        "Name": "corporate-data-ingestion::${each.key}::daily-run",
        "ActionOnFailure": "CONTINUE",
        "HadoopJarStep": {
          "Jar": "command-runner.jar",
          "Args": [
            "spark-submit",
            "/opt/emr/steps/corporate_data_ingestion.py",
            "--source_s3_prefix",
            "${each.value["source_s3_prefix"]}",
            "--destination_s3_prefix",
            "${each.value["destination_s3_prefix"]}",
            "--collection_names",
            "${each.value["collection_names"]}",
            "--concurrency",
            "${each.value["concurrency"]}"
          ]
        }
      }]
    },
    "extend": null,
    "additional_step_args": null
  }
  JSON
}
