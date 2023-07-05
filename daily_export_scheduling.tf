
resource "aws_cloudwatch_event_rule" "emr_cluster" {
  for_each            = { for collection in local.collection_schedules_mapping : "${collection.collection_name}-${collection.schedule_name}-${collection.cron_name}" => collection }
  name                = "${local.emr_cluster_acronym}_${each.value.collection_name}_${each.value.cron_name}"
  description         = "${replace(local.emr_cluster_name, "-", "_")}_${each.value.collection_name}_${each.value.cron_name}"
  schedule_expression = local.cron_schedules[each.value.cron_name]
}

resource "aws_cloudwatch_event_target" "triggered_event" {
  for_each  = { for collection in local.collection_schedules_mapping : "${collection.collection_name}-${collection.schedule_name}-${collection.cron_name}" => collection }
  rule      = aws_cloudwatch_event_rule.emr_cluster[each.key].name
  target_id = aws_sns_topic.corporate_data_ingestion.name
  arn       = aws_sns_topic.corporate_data_ingestion.arn
  input     = <<JSON
  {
    "s3_overrides": null,
    "overrides": {
      "Steps": [{
        "Name": "${local.emr_cluster_name}::${each.value.collection_name}::${each.value.schedule_name}",
        "ActionOnFailure": "TERMINATE_CLUSTER",
        "HadoopJarStep": {
          "Jar": "command-runner.jar",
          "Args": [
            "spark-submit",
            "/opt/emr/steps/corporate_data_ingestion.py",
            "--db",
            "${local.collections_configuration[each.value.collection_name]["db"]}",
            "--collection",
            "${each.value.collection_name}",
            "${try(local.collection_overrides[local.environment][each.value.collection_name][each.value.schedule_name].extra_args, local.collection_overrides["default"][each.value.collection_name][each.value.schedule_name].extra_args, "")}"
          ]
        }
      }]
    },
    "extend": null,
    "additional_step_args": null
  }
  JSON
}
