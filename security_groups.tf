resource "aws_security_group" "dataworks_aws_corporate_data_ingestion_master" {
  name                   = "dataworks_aws_corporate_data_ingestion Master"
  description            = "Contains rules for dataworks_aws_corporate_data_ingestion master nodes; most rules are injected by EMR, not managed by TF"
  revoke_rules_on_delete = true
  vpc_id                 = data.terraform_remote_state.internal_compute.outputs.vpc.vpc.vpc.id
  tags = {
    Name = "${local.emr_cluster_name}_master"
  }
}

resource "aws_security_group" "dataworks_aws_corporate_data_ingestion_slave" {
  name                   = "${local.emr_cluster_name} Slave"
  description            = "Contains rules for dataworks_aws_corporate_data_ingestion slave nodes; most rules are injected by EMR, not managed by TF"
  revoke_rules_on_delete = true
  vpc_id                 = data.terraform_remote_state.internal_compute.outputs.vpc.vpc.vpc.id
  tags = {
    Name = "${local.emr_cluster_name}_slave"
  }
}

resource "aws_security_group" "dataworks_aws_corporate_data_ingestion_common" {
  name                   = "${local.emr_cluster_name} Common"
  description            = "Contains rules for both dataworks_aws_corporate_data_ingestion master and dataworks_aws_corporate_data_ingestion slave nodes"
  revoke_rules_on_delete = true
  vpc_id                 = data.terraform_remote_state.internal_compute.outputs.vpc.vpc.vpc.id
  tags = {
    Name = "${local.emr_cluster_name}_common"
  }
}

resource "aws_security_group" "dataworks_aws_corporate_data_ingestion_emr_service" {
  name                   = "dataworks_aws_corporate_data_ingestion EMR Service"
  description            = "Contains rules for EMR service when managing the dataworks_aws_corporate_data_ingestion cluster; rules are injected by EMR, not managed by TF"
  revoke_rules_on_delete = true
  vpc_id                 = data.terraform_remote_state.internal_compute.outputs.vpc.vpc.vpc.id
  tags = {
    Name = "dataworks_aws_corporate_data_ingestion_emr_service"
  }
}

resource "aws_security_group_rule" "egress_https_to_vpc_endpoints" {
  description              = "Allow HTTPS traffic to VPC endpoints"
  from_port                = 443
  protocol                 = "tcp"
  security_group_id        = aws_security_group.dataworks_aws_corporate_data_ingestion_common.id
  to_port                  = 443
  type                     = "egress"
  source_security_group_id = data.terraform_remote_state.internal_compute.outputs.vpc.vpc.interface_vpce_sg_id
}

resource "aws_security_group_rule" "ingress_https_vpc_endpoints_from_emr" {
  description              = "Allow HTTPS traffic from dataworks_aws_corporate_data_ingestion"
  from_port                = 443
  protocol                 = "tcp"
  security_group_id        = data.terraform_remote_state.internal_compute.outputs.vpc.vpc.interface_vpce_sg_id
  to_port                  = 443
  type                     = "ingress"
  source_security_group_id = aws_security_group.dataworks_aws_corporate_data_ingestion_common.id
}

resource "aws_security_group_rule" "egress_https_s3_endpoint" {
  description       = "Allow HTTPS access to S3 via its endpoint"
  type              = "egress"
  from_port         = 443
  to_port           = 443
  protocol          = "tcp"
  prefix_list_ids   = [data.terraform_remote_state.internal_compute.outputs.vpc.vpc.prefix_list_ids.s3]
  security_group_id = aws_security_group.dataworks_aws_corporate_data_ingestion_common.id
}

resource "aws_security_group_rule" "egress_http_s3_endpoint" {
  description       = "Allow HTTP access to S3 via its endpoint (YUM)"
  type              = "egress"
  from_port         = 80
  to_port           = 80
  protocol          = "tcp"
  prefix_list_ids   = [data.terraform_remote_state.internal_compute.outputs.vpc.vpc.prefix_list_ids.s3]
  security_group_id = aws_security_group.dataworks_aws_corporate_data_ingestion_common.id
}


resource "aws_security_group_rule" "egress_internet_proxy" {
  description              = "Allow Internet access via the proxy (for ACM-PCA)"
  type                     = "egress"
  from_port                = 3128
  to_port                  = 3128
  protocol                 = "tcp"
  source_security_group_id = data.terraform_remote_state.internal_compute.outputs.internet_proxy.sg
  security_group_id        = aws_security_group.dataworks_aws_corporate_data_ingestion_common.id
}

resource "aws_security_group_rule" "ingress_internet_proxy" {
  description              = "Allow proxy access from dataworks_aws_corporate_data_ingestion"
  type                     = "ingress"
  from_port                = 3128
  to_port                  = 3128
  protocol                 = "tcp"
  source_security_group_id = aws_security_group.dataworks_aws_corporate_data_ingestion_common.id
  security_group_id        = data.terraform_remote_state.internal_compute.outputs.internet_proxy.sg
}

resource "aws_security_group_rule" "cdi_host_outbound_tanium_1" {
  description              = "CDI host outbound port 1 to Tanium"
  type                     = "egress"
  from_port                = var.tanium_port_1
  to_port                  = var.tanium_port_1
  protocol                 = "tcp"
  security_group_id        = aws_security_group.dataworks_aws_corporate_data_ingestion_common.id
  source_security_group_id = data.terraform_remote_state.internal_compute.outputs.tanium_service_endpoint.sg
}

resource "aws_security_group_rule" "cdi_host_outbound_tanium_2" {
  description              = "CDI host outbound port 2 to Tanium"
  type                     = "egress"
  from_port                = var.tanium_port_2
  to_port                  = var.tanium_port_2
  protocol                 = "tcp"
  security_group_id        = aws_security_group.dataworks_aws_corporate_data_ingestion_common.id
  source_security_group_id = data.terraform_remote_state.internal_compute.outputs.tanium_service_endpoint.sg
}

resource "aws_security_group_rule" "cdi_host_inbound_tanium_1" {
  description              = "CDI host inbound port 1 from Tanium"
  type                     = "ingress"
  from_port                = var.tanium_port_1
  to_port                  = var.tanium_port_1
  protocol                 = "tcp"
  security_group_id        = data.terraform_remote_state.internal_compute.outputs.tanium_service_endpoint.sg
  source_security_group_id = aws_security_group.dataworks_aws_corporate_data_ingestion_common.id
}

resource "aws_security_group_rule" "cdi_host_inbound_tanium_2" {
  description              = "CDI host inbound port 2 from Tanium"
  type                     = "ingress"
  from_port                = var.tanium_port_2
  to_port                  = var.tanium_port_2
  protocol                 = "tcp"
  security_group_id        = data.terraform_remote_state.internal_compute.outputs.tanium_service_endpoint.sg
  source_security_group_id = aws_security_group.dataworks_aws_corporate_data_ingestion_common.id
}

resource "aws_security_group_rule" "egress_dataworks_aws_corporate_data_ingestion_to_dks" {
  description       = "Allow requests to the DKS"
  type              = "egress"
  from_port         = 8443
  to_port           = 8443
  protocol          = "tcp"
  cidr_blocks       = data.terraform_remote_state.crypto.outputs.dks_subnet.cidr_blocks
  security_group_id = aws_security_group.dataworks_aws_corporate_data_ingestion_common.id
}

resource "aws_security_group_rule" "ingress_to_dks" {
  provider    = aws.crypto
  description = "Allow inbound requests to DKS from dataworks_aws_corporate_data_ingestion"
  type        = "ingress"
  protocol    = "tcp"
  from_port   = 8443
  to_port     = 8443

  cidr_blocks = data.terraform_remote_state.internal_compute.outputs.corporate_data_processing_subnet.cidr_blocks

  security_group_id = data.terraform_remote_state.crypto.outputs.dks_sg_id[local.environment]
}

# https://docs.aws.amazon.com/emr/latest/ManagementGuide/emr-man-sec-groups.html#emr-sg-elasticmapreduce-sa-private
resource "aws_security_group_rule" "emr_service_ingress_master" {
  description              = "Allow EMR master nodes to reach the EMR service"
  type                     = "ingress"
  from_port                = 9443
  to_port                  = 9443
  protocol                 = "tcp"
  source_security_group_id = aws_security_group.dataworks_aws_corporate_data_ingestion_master.id
  security_group_id        = aws_security_group.dataworks_aws_corporate_data_ingestion_emr_service.id
}

# The EMR service will automatically add the ingress equivalent of this rule,
# but doesn't inject this egress counterpart
resource "aws_security_group_rule" "emr_master_to_core_egress_tcp" {
  description              = "Allow master nodes to send TCP traffic to core nodes"
  type                     = "egress"
  from_port                = 0
  to_port                  = 65535
  protocol                 = "tcp"
  source_security_group_id = aws_security_group.dataworks_aws_corporate_data_ingestion_slave.id
  security_group_id        = aws_security_group.dataworks_aws_corporate_data_ingestion_master.id
}

# The EMR service will automatically add the ingress equivalent of this rule,
# but doesn't inject this egress counterpart
resource "aws_security_group_rule" "emr_core_to_master_egress_tcp" {
  description              = "Allow core nodes to send TCP traffic to master nodes"
  type                     = "egress"
  from_port                = 0
  to_port                  = 65535
  protocol                 = "tcp"
  source_security_group_id = aws_security_group.dataworks_aws_corporate_data_ingestion_master.id
  security_group_id        = aws_security_group.dataworks_aws_corporate_data_ingestion_slave.id
}

# The EMR service will automatically add the ingress equivalent of this rule,
# but doesn't inject this egress counterpart
resource "aws_security_group_rule" "emr_core_to_core_egress_tcp" {
  description       = "Allow core nodes to send TCP traffic to other core nodes"
  type              = "egress"
  from_port         = 0
  to_port           = 65535
  protocol          = "tcp"
  self              = true
  security_group_id = aws_security_group.dataworks_aws_corporate_data_ingestion_slave.id
}

# The EMR service will automatically add the ingress equivalent of this rule,
# but doesn't inject this egress counterpart
resource "aws_security_group_rule" "emr_master_to_core_egress_udp" {
  description              = "Allow master nodes to send UDP traffic to core nodes"
  type                     = "egress"
  from_port                = 0
  to_port                  = 65535
  protocol                 = "udp"
  source_security_group_id = aws_security_group.dataworks_aws_corporate_data_ingestion_slave.id
  security_group_id        = aws_security_group.dataworks_aws_corporate_data_ingestion_master.id
}

# The EMR service will automatically add the ingress equivalent of this rule,
# but doesn't inject this egress counterpart
resource "aws_security_group_rule" "emr_core_to_master_egress_udp" {
  description              = "Allow core nodes to send UDP traffic to master nodes"
  type                     = "egress"
  from_port                = 0
  to_port                  = 65535
  protocol                 = "udp"
  source_security_group_id = aws_security_group.dataworks_aws_corporate_data_ingestion_master.id
  security_group_id        = aws_security_group.dataworks_aws_corporate_data_ingestion_slave.id
}

# The EMR service will automatically add the ingress equivalent of this rule,
# but doesn't inject this egress counterpart
resource "aws_security_group_rule" "emr_core_to_core_egress_udp" {
  description       = "Allow core nodes to send UDP traffic to other core nodes"
  type              = "egress"
  from_port         = 0
  to_port           = 65535
  protocol          = "udp"
  self              = true
  security_group_id = aws_security_group.dataworks_aws_corporate_data_ingestion_slave.id
}

resource "aws_security_group_rule" "egress_https_dynamodb_endpoint" {
  description       = "Allow HTTPS access to DynamoDB via its endpoint (EMRFS)"
  type              = "egress"
  from_port         = 443
  to_port           = 443
  protocol          = "tcp"
  prefix_list_ids   = [data.terraform_remote_state.internal_compute.outputs.vpc.vpc.prefix_list_ids.dynamodb]
  security_group_id = aws_security_group.dataworks_aws_corporate_data_ingestion_common.id
}
