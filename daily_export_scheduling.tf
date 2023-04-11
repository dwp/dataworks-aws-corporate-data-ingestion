resource "aws_cloudwatch_event_rule" "utc_09_30_daily_except_sunday" {
  count               = local.run_daily_export_on_schedule[local.environment] == true ? 1 : 0
  name                = "corporate_data_ingestion_utc_09_30_daily_except_sunday"
  description         = "corporate_data_ingestion_utc_09_30_daily_except_sunday"
  schedule_expression = "cron(30 09 ? * 2-7 *)"
}

resource "aws_cloudwatch_event_rule" "utc_10_00_sunday" {
  count               = local.run_daily_export_on_schedule[local.environment] == true ? 1 : 0
  name                = "corporate_data_ingestion_utc_10_00_sunday"
  description         = "corporate_data_ingestion_utc_10_00_sunday"
  schedule_expression = "cron(00 10 ? * 1 *)"
}

resource "aws_cloudwatch_event_target" "run_daily_export_monday_to_saturday" {
  for_each  = local.run_daily_export_on_schedule[local.environment] == true ? local.collections_configuration : tomap({})
  rule      = aws_cloudwatch_event_rule.utc_09_30_daily_except_sunday[0].name
  target_id = aws_sns_topic.corporate_data_ingestion.name
  arn       = aws_sns_topic.corporate_data_ingestion.arn
  input     = <<JSON
  {
    "s3_overrides": null,
    "overrides": {
      "Steps": [{
        "Name": "corporate-data-ingestion::${each.key}::daily-run",
        "ActionOnFailure": "TERMINATE_CLUSTER",
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

resource "aws_cloudwatch_event_target" "run_daily_export_sunday" {
  for_each  = local.run_daily_export_on_schedule[local.environment] == true ? local.collections_configuration : tomap({})
  rule      = aws_cloudwatch_event_rule.utc_10_00_sunday[0].name
  target_id = aws_sns_topic.corporate_data_ingestion.name
  arn       = aws_sns_topic.corporate_data_ingestion.arn
  input     = <<JSON
  {
    "s3_overrides": null,
    "overrides": {
      "Steps": [{
        "Name": "corporate-data-ingestion::${each.key}::daily-run",
        "ActionOnFailure": "TERMINATE_CLUSTER",
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
