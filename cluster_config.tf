resource "aws_emr_security_configuration" "ebs_emrfs_em" {
  name          = "dataworks_aws_corporate_data_ingestion_ebs_emrfs"
  configuration = jsonencode(local.ebs_emrfs_em)
}

resource "aws_s3_object" "cluster" {
  bucket = data.terraform_remote_state.common.outputs.config_bucket.id
  key    = "emr/${local.emr_cluster_name}/cluster.yaml"
  content = templatefile("${path.module}/cluster_config/cluster.yaml.tpl",
    {
      s3_log_bucket              = data.terraform_remote_state.security-tools.outputs.logstore_bucket.id
      s3_log_prefix              = local.s3_log_prefix
      ami_id                     = var.emr_ami_id
      service_role               = aws_iam_role.dataworks_aws_corporate_data_ingestion_emr_service.arn
      instance_profile           = aws_iam_instance_profile.dataworks_aws_corporate_data_ingestion.arn
      security_configuration     = aws_emr_security_configuration.ebs_emrfs_em.id
      emr_release                = var.emr_release[local.environment]
      cluster_name               = local.emr_cluster_name
      dwx_environment_tag_value  = local.common_repo_tags.Environment
      application_tag_value      = data.aws_default_tags.provider_tags.tags.Application
      function_tag_value         = data.aws_default_tags.provider_tags.tags.Function
      business_project_tag_value = data.aws_default_tags.provider_tags.tags.Business-Project
      environment_tag_value      = data.aws_default_tags.provider_tags.tags.Environment
    }
  )
  tags = {
    Name = "cluster"
  }
}

resource "aws_s3_object" "instances" {
  bucket = data.terraform_remote_state.common.outputs.config_bucket.id
  key    = "emr/${local.emr_cluster_name}/instances.yaml"
  content = templatefile("${path.module}/cluster_config/instances.yaml.tpl",
    {
      keep_cluster_alive = local.keep_cluster_alive[local.environment]
      add_master_sg      = aws_security_group.dataworks_aws_corporate_data_ingestion_common.id
      add_slave_sg       = aws_security_group.dataworks_aws_corporate_data_ingestion_common.id
      subnet_ids         = data.terraform_remote_state.internal_compute.outputs.corporate_data_processing_subnet.subnets.*.id

      master_sg                           = aws_security_group.dataworks_aws_corporate_data_ingestion_master.id
      slave_sg                            = aws_security_group.dataworks_aws_corporate_data_ingestion_slave.id
      service_access_sg                   = aws_security_group.dataworks_aws_corporate_data_ingestion_emr_service.id
      instance_type_core_one              = var.emr_instance_type_core_one[local.environment]
      instance_type_master                = var.emr_instance_type_master[local.environment]
      core_instance_count                 = var.emr_core_instance_count[local.environment]
      instance_core_ebs_size_in_gb        = var.instance_core_ebs_size_in_gb[local.environment]
      instance_master_ebs_size_in_gb      = var.instance_master_ebs_size_in_gb[local.environment]
      capacity_reservation_preference     = local.emr_capacity_reservation_preference
      capacity_reservation_usage_strategy = local.emr_capacity_reservation_usage_strategy
    }
  )
  tags = {
    Name = "instances"
  }
}

resource "aws_s3_object" "steps" {
  bucket = data.terraform_remote_state.common.outputs.config_bucket.id
  key    = "emr/${local.emr_cluster_name}/steps.yaml"
  content = templatefile("${path.module}/cluster_config/steps.yaml.tpl",
    {
      s3_config_bucket    = data.terraform_remote_state.common.outputs.config_bucket.id
      action_on_failure   = local.step_fail_action[local.environment]
      s3_published_bucket = data.terraform_remote_state.common.outputs.published_bucket.id
      environment         = local.hcs_environment[local.environment]
      proxy_http_host     = data.terraform_remote_state.internal_compute.outputs.internet_proxy.host
      proxy_http_port     = data.terraform_remote_state.internal_compute.outputs.internet_proxy.port
      install_tenable     = local.tenable_install[local.environment]
      install_trend       = local.trend_install[local.environment]
      install_tanium      = local.tanium_install[local.environment]
      tanium_server_1     = data.terraform_remote_state.internal_compute.outputs.tanium_service_endpoint.dns
      tanium_server_2     = local.tanium2
      tanium_env          = local.tanium_env[local.environment]
      tanium_port         = var.tanium_port_1
      tanium_log_level    = local.tanium_log_level[local.environment]
      tenant              = local.tenant
      tenantid            = local.tenantid
      token               = local.token
      policyid            = local.policy_id[local.environment]
    }
  )
  tags = {
    Name = "steps"
  }
}


resource "aws_s3_object" "configurations" {
  bucket = data.terraform_remote_state.common.outputs.config_bucket.id
  key    = "emr/${local.emr_cluster_name}/configurations.yaml"
  content = templatefile("${path.module}/cluster_config/configurations.yaml.tpl",
    {
      s3_log_bucket                       = data.terraform_remote_state.security-tools.outputs.logstore_bucket.id
      s3_log_prefix                       = local.s3_log_prefix
      s3_published_bucket                 = data.terraform_remote_state.common.outputs.published_bucket.id
      s3_corporate_bucket                 = data.terraform_remote_state.aws_ingestion.outputs.corporate_storage_bucket.id
      proxy_no_proxy                      = replace(replace(local.no_proxy, ",", "|"), ".s3", "*.s3")
      proxy_http_host                     = data.terraform_remote_state.internal_compute.outputs.internet_proxy.host
      proxy_http_port                     = data.terraform_remote_state.internal_compute.outputs.internet_proxy.port
      proxy_https_host                    = data.terraform_remote_state.internal_compute.outputs.internet_proxy.host
      proxy_https_port                    = data.terraform_remote_state.internal_compute.outputs.internet_proxy.port
      environment                         = local.environment
      s3_published_bucket                 = data.terraform_remote_state.common.outputs.published_bucket.id
      spark_kyro_buffer                   = local.spark_kyro_buffer[local.environment]
      spark_executor_cores                = local.spark_executor_cores[local.environment]
      spark_executor_memory               = local.spark_executor_memory[local.environment]
      spark_driver_memory                 = local.spark_driver_memory[local.environment]
      spark_driver_cores                  = local.spark_driver_cores[local.environment]
      spark_task_cpus                     = local.spark_task_cpus[local.environment]
      spark_default_parallelism           = local.spark_default_parallelism[local.environment]
      spark_sql_shuffle_partitions        = local.spark_sql_shuffle_partitions[local.environment]
      spark_yarn_executor_memory_overhead = local.spark_yarn_executor_memory_overhead[local.environment]
      spark_executor_instances            = local.spark_executor_instances[local.environment]
      hive_metastore_endpoint             = data.terraform_remote_state.internal_compute.outputs.hive_metastore_v2.endpoint
      hive_metastore_database_name        = data.terraform_remote_state.internal_compute.outputs.hive_metastore_v2.database_name
      hive_metastore_username             = data.terraform_remote_state.internal_compute.outputs.metadata_store_users.corporate_data_ingestion_writer.username
      hive_metastore_pwd                  = data.terraform_remote_state.internal_compute.outputs.metadata_store_users.corporate_data_ingestion_writer.secret_name

    }
  )
  tags = {
    Name = "configurations"
  }
}

