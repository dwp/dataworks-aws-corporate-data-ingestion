resource "aws_security_group_rule" "ingress_uc_feature_metastore_v2" {
  description              = "Allow mysql traffic to Aurora RDS from aws_emr_template_repository"
  from_port                = 3306
  protocol                 = "tcp"
  security_group_id        = data.terraform_remote_state.internal_compute.outputs.hive_metastore_v2.security_group.id
  to_port                  = 3306
  type                     = "ingress"
  source_security_group_id = aws_security_group.aws_emr_template_repository_common.id
}

resource "aws_security_group_rule" "egress_uc_feature_metastore_v2" {
  description              = "Allow mysql traffic to Aurora RDS from aws_emr_template_repository"
  from_port                = 3306
  protocol                 = "tcp"
  security_group_id        = aws_security_group.aws_emr_template_repository_common.id
  to_port                  = 3306
  type                     = "egress"
  source_security_group_id = data.terraform_remote_state.internal_compute.outputs.hive_metastore_v2.security_group.id
}
