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

resource "aws_cloudwatch_event_rule" "utc_09_30_daily_except_friday" {
  count               = local.run_daily_export_on_schedule[local.environment] == true ? 1 : 0
  name                = "corporate_data_ingestion_utc_09_30_daily_except_friday"
  description         = "corporate_data_ingestion_utc_09_30_daily_except_friday"
  schedule_expression = "cron(30 09 ? * 1-5,7 *)"
}

resource "aws_cloudwatch_event_rule" "utc_09_30_friday" {
  count               = local.run_daily_export_on_schedule[local.environment] == true ? 1 : 0
  name                = "corporate_data_ingestion_utc_09_30_friday"
  description         = "corporate_data_ingestion_utc_09_30_friday"
  schedule_expression = "cron(30 09 ? * 6 *)"
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
            "--db",
            "${each.value["db"]}",
            "--collection",
            "${each.value["collection"]}"
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
            "--db",
            "${each.value["db"]}",
            "--collection",
            "${each.value["collection"]}"
          ]
        }
      }]
    },
    "extend": null,
    "additional_step_args": null
  }
  JSON
}

resource "aws_cloudwatch_event_target" "run_daily_export_except_friday" {
  for_each  = local.run_daily_export_on_schedule[local.environment] == true ? local.collections_configuration : tomap({})
  rule      = aws_cloudwatch_event_rule.utc_09_30_daily_except_friday[0].name
  target_id = aws_sns_topic.corporate_data_ingestion.name
  arn       = aws_sns_topic.corporate_data_ingestion.arn
  input     = <<JSON
  {
    "s3_overrides": null,
    "overrides": {
      "Steps": [{
        "Name": "corporate-data-ingestion::calculationParts::daily-run",
        "ActionOnFailure": "TERMINATE_CLUSTER",
        "HadoopJarStep": {
          "Jar": "command-runner.jar",
          "Args": [
            "spark-submit",
            "/opt/emr/steps/corporate_data_ingestion.py",
            "--db",
            "calculator",
            "--collection",
            "calculationParts"
          ]
        }
      }]
    },
    "extend": null,
    "additional_step_args": null
  }
  JSON
}

resource "aws_cloudwatch_event_target" "run_update_weekly_on_friday" {
  for_each  = local.run_daily_export_on_schedule[local.environment] == true ? local.collections_configuration : tomap({})
  rule      = aws_cloudwatch_event_rule.utc_09_30_friday[0].name
  target_id = aws_sns_topic.corporate_data_ingestion.name
  arn       = aws_sns_topic.corporate_data_ingestion.arn
  input     = <<JSON
  {
    "s3_overrides": null,
    "overrides": {
      "Steps": [{
        "Name": "corporate-data-ingestion::calculationParts::weekly-update",
        "ActionOnFailure": "TERMINATE_CLUSTER",
        "HadoopJarStep": {
          "Jar": "command-runner.jar",
          "Args": [
            "spark-submit",
            "/opt/emr/steps/corporate_data_ingestion.py",
            "--db",
            "calculator",
            "--collection",
            "calculationParts",
            "--force_collection_update"
          ]
        }
      }]
    },
    "extend": null,
    "additional_step_args": null
  }
  JSON
}