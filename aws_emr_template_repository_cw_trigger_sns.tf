resource "aws_sns_topic" "dataworks_aws_corporate_data_ingestion_cw_trigger_sns" {
  name = "${local.emr_cluster_name}_cw_trigger_sns"

  tags = {
    "Name" = "${local.emr_cluster_name}_cw_trigger_sns"
  }
}

output "dataworks_aws_corporate_data_ingestion_cw_trigger_sns_topic" {
  value = aws_sns_topic.dataworks_aws_corporate_data_ingestion_cw_trigger_sns
}
