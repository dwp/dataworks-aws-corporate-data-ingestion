locals {
  region = "eu-west-2"

  persistence_tag_value = {
    development = "Ignore"
    qa          = "Ignore"
    integration = "Ignore"
    preprod     = "Ignore"
    production  = "Ignore"
  }

  auto_shutdown_tag_value = {
    development = "True"
    qa          = "False"
    integration = "True"
    preprod     = "False"
    production  = "False"
  }

  overridden_tags = {
    Role         = "corporate-data-ingestion"
    Owner        = "dataworks-aws-corporate-data-ingestion"
    Persistence  = local.persistence_tag_value[local.environment]
    AutoShutdown = local.auto_shutdown_tag_value[local.environment]
  }

  common_repo_tags = merge(module.dataworks_common.common_tags, local.overridden_tags)
  common_emr_tags = {
    for-use-with-amazon-emr-managed-policies = "true"
  }

  #Note that if you change this, you MUST first remove the use of it from all log groups because CI can't (and shouldn't) delete them
  emr_cluster_name = "corporate-data-ingestion"

  env_certificate_bucket = "dw-${local.environment}-public-certificates"
  mgt_certificate_bucket = "dw-${local.management_account[local.environment]}-public-certificates"
  dks_endpoint           = data.terraform_remote_state.crypto.outputs.dks_endpoint[local.environment]

  crypto_workspace = {
    management-dev = "management-dev"
    management     = "management"
  }

  management_workspace = {
    management-dev = "default"
    management     = "management"
  }

  management_account = {
    development = "management-dev"
    qa          = "management-dev"
    integration = "management-dev"
    preprod     = "management"
    production  = "management"
  }

  root_dns_name = {
    development = "dev.dataworks.dwp.gov.uk"
    qa          = "qa.dataworks.dwp.gov.uk"
    integration = "int.dataworks.dwp.gov.uk"
    preprod     = "pre.dataworks.dwp.gov.uk"
    production  = "dataworks.dwp.gov.uk"
  }

  dataworks_aws_corporate_data_ingestion_log_level = {
    development = "DEBUG"
    qa          = "DEBUG"
    integration = "DEBUG"
    preprod     = "INFO"
    production  = "INFO"
  }

  dataworks_aws_corporate_data_ingestion_version = {
    development = "0.0.1"
    qa          = "0.0.1"
    integration = "0.0.1"
    preprod     = "0.0.1"
    production  = "0.0.1"
  }

  dataworks_aws_corporate_data_ingestion_alerts = {
    development = false
    qa          = false
    integration = false
    preprod     = false
    production  = true
  }

  data_pipeline_metadata = data.terraform_remote_state.internal_compute.outputs.data_pipeline_metadata_dynamo.name

  amazon_region_domain = "${data.aws_region.current.name}.amazonaws.com"
  endpoint_services = [
    "dynamodb", "ec2", "ec2messages", "glue", "kms", "logs", "monitoring", ".s3", "s3", "secretsmanager", "ssm",
    "ssmmessages"
  ]
  no_proxy = "169.254.169.254,${join(",", formatlist("%s.%s", local.endpoint_services, local.amazon_region_domain))},${local.dataworks_aws_corporate_data_ingestion_pushgateway_hostname}"
  ebs_emrfs_em = {
    EncryptionConfiguration = {
      EnableInTransitEncryption = false
      EnableAtRestEncryption    = true
      AtRestEncryptionConfiguration = {

        S3EncryptionConfiguration = {
          EncryptionMode             = "CSE-Custom"
          S3Object                   = "s3://${data.terraform_remote_state.management_artefact.outputs.artefact_bucket.id}/emr-encryption-materials-provider/encryption-materials-provider-all.jar"
          EncryptionKeyProviderClass = "uk.gov.dwp.dataworks.dks.encryptionmaterialsprovider.DKSEncryptionMaterialsProvider"
        }
        LocalDiskEncryptionConfiguration = {
          EnableEbsEncryption       = true
          EncryptionKeyProviderType = "AwsKms"
          AwsKmsKey                 = aws_kms_key.dataworks_aws_corporate_data_ingestion_ebs_cmk.arn
        }
      }
    }
  }

  keep_cluster_alive = {
    development = false
    qa          = false
    integration = false
    preprod     = false
    production  = false
  }

  step_fail_action = {
    development = "TERMINATE_CLUSTER"
    qa          = "TERMINATE_CLUSTER"
    integration = "TERMINATE_CLUSTER"
    preprod     = "TERMINATE_CLUSTER"
    production  = "TERMINATE_CLUSTER"
  }

  dks_max_retries = {
    development = 2
    qa          = 2
    integration = 2
    preprod     = 10
    production  = 10
  }

  extra_python_files = toset(["data.py", "dks.py", "utils.py", "hive.py", "ingesters.py", "logger.py", "dynamodb.py"])

  cw_agent_namespace                   = "/app/${local.emr_cluster_name}"
  cw_agent_log_group_name              = "/app/${local.emr_cluster_name}"
  cw_agent_bootstrap_loggrp_name       = "/app/${local.emr_cluster_name}/bootstrap_actions"
  cw_agent_steps_loggrp_name           = "/app/${local.emr_cluster_name}/step_logs"
  cw_agent_metrics_collection_interval = 60

  s3_log_prefix = "emr/${local.emr_cluster_name}"

  dynamodb_final_step = {
    development = "temp"
    qa          = "temp"
    integration = "temp"
    preprod     = "temp"
    production  = "temp"
  }

  skip_sns_notification_on_corporate_data_ingestion_completion = {
    development = "true"
    qa          = "true"
    integration = "true"
    preprod     = "false"
    production  = "false"
  }

  # These should be `false` unless we have agreed this data product is to use the capacity reservations so as not to interfere with existing data products running
  use_capacity_reservation = {
    development = false
    qa          = false
    integration = false
    preprod     = false
    production  = false
  }

  emr_capacity_reservation_preference = local.use_capacity_reservation[local.environment] == true ? "open" : "none"

  emr_capacity_reservation_usage_strategy = local.use_capacity_reservation[local.environment] == true ? "use-capacity-reservations-first" : ""

  emr_subnet_non_capacity_reserved_environments = data.terraform_remote_state.common.outputs.aws_ec2_non_capacity_reservation_region

  dataworks_aws_corporate_data_ingestion_pushgateway_hostname = "${aws_service_discovery_service.dataworks_aws_corporate_data_ingestion_services.name}.${aws_service_discovery_private_dns_namespace.dataworks_aws_corporate_data_ingestion_services.name}"

  dataworks_aws_corporate_data_ingestion_max_retry_count = {
    development = "0"
    qa          = "0"
    integration = "0"
    preprod     = "0"
    production  = "0"
  }

  # 5 cores per executor is "usually sensible"
  spark_executor_cores = {
    development = 5
    qa          = 5
    integration = 5
    preprod     = 5
    production  = 5
  }

  # Memory per executor = available memory / num executors
  spark_executor_memory = {
    development = 24
    qa          = 24
    integration = 24
    preprod     = 37
    production  = 37
  }

  # >~ 10% of executor memory
  spark_yarn_executor_memory_overhead = {
    development = 5
    qa          = 5
    integration = 5
    preprod     = 5
    production  = 5
  }

  spark_driver_memory = {
    development = 10
    qa          = 10
    integration = 10
    preprod     = local.spark_executor_memory.preprod
    production  = local.spark_executor_memory.production
  }

  spark_driver_cores = {
    development = 5
    qa          = 5
    integration = 5
    preprod     = local.spark_executor_cores.preprod
    production  = local.spark_executor_cores.production
  }

  spark_kyro_buffer = {
    development = "256m"
    qa          = "256m"
    integration = "256m"
    preprod     = "2047m"
    production  = "2047m" # Max amount allowed
  }

  spark_executor_instances = {
    development = 150
    qa          = 150
    integration = 150
    preprod     = 170
    production  = 170
  }

  spark_default_parallelism = {
    development = 100
    qa          = 100
    integration = 100
    preprod     = local.spark_executor_instances.preprod * local.spark_executor_cores.preprod * 2
    production  = local.spark_executor_instances.production * local.spark_executor_cores.production * 2
  }

  spark_sql_shuffle_partitions = {
    development = 100
    qa          = 100
    integration = 100
    preprod     = local.spark_default_parallelism.preprod
    production  = local.spark_default_parallelism.production
  }


  hive_metastore_location = "data/dataworks-aws-corporate-data-ingestion"


  run_daily_export_on_schedule = {
    development = false
    qa          = false
    integration = false
    preprod     = true
    production  = true
  }

  collections_configuration = {
    businessAudit = {
      source_s3_prefix      = "corporate_storage/ucfs_audit"
      destination_s3_prefix = "corporate_data_ingestion/json/daily"
      collection_names      = "data:businessAudit"
      concurrency           = "1"
    }
  }
}
