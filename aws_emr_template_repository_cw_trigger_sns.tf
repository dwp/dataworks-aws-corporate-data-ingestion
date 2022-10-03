resource "aws_sns_topic" "aws_emr_template_repository_cw_trigger_sns" {
  name = "${local.emr_cluster_name}_cw_trigger_sns"

  tags = {
    "Name" = "${local.emr_cluster_name}_cw_trigger_sns"
  }
}

output "aws_emr_template_repository_cw_trigger_sns_topic" {
  value = aws_sns_topic.aws_emr_template_repository_cw_trigger_sns
}
