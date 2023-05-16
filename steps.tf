resource "aws_s3_object" "corporate_data_ingestion_script" {
  bucket = data.terraform_remote_state.common.outputs.config_bucket.id
  key    = "component/${local.emr_cluster_name}/corporate_data_ingestion.py"
  content = templatefile("${path.module}/steps/corporate_data_ingestion.py",
    {
      url                = format("%s/datakey/actions/decrypt", data.terraform_remote_state.crypto.outputs.dks_endpoint[local.environment])
      aws_default_region = "eu-west-2"
      log_level          = "INFO"
      log_path           = "/var/log/dataworks-aws-corporate-data-ingestion/corporate-data-ingestion.log"
    }
  )
}

resource "aws_s3_object" "python_configuration_file" {
  bucket = data.terraform_remote_state.common.outputs.config_bucket.id
  key    = "component/${local.emr_cluster_name}/configuration.json"
  content = templatefile("${path.module}/steps/configuration.json",
    {
      s3_corporate_bucket  = data.terraform_remote_state.aws_ingestion.outputs.corporate_storage_bucket.id
      s3_published_bucket  = data.terraform_remote_state.common.outputs.published_bucket.id
      dks_decrypt_endpoint = "${data.terraform_remote_state.crypto.outputs.dks_endpoint[local.environment]}/datakey/actions/decrypt"
      dks_max_retries      = local.dks_max_retries[local.environment]
      extra_python_files   = jsonencode(local.extra_python_files)
    }
  )
}

resource "aws_s3_object" "python_utils" {
  for_each = local.extra_python_files
  bucket   = data.terraform_remote_state.common.outputs.config_bucket.id
  key      = "component/${local.emr_cluster_name}/${each.key}"
  content  = file("${path.module}/steps/${each.key}")
}

resource "aws_s3_object" "snapshot_updater_file" {
  bucket = data.terraform_remote_state.common.outputs.config_bucket.id
  key    = "component/${local.emr_cluster_name}/snapshot_updater/snapshot_updater.sql"
  content = templatefile("${path.module}/steps/snapshot_updater/snapshot_updater.sql",
    {
      s3_published_bucket = data.terraform_remote_state.common.outputs.published_bucket.id

    }
  )
}

data "aws_s3_object" "calculation_parts_ddl" {
  for_each = {
    "src_calculator_parts" : "component/uc_repos/aws-uc-lab/child_calculation/build/src_calculator_parts_ddl",
    "src_childcare_entitlement" : "component/uc_repos/aws-uc-lab/child_calculation/build/src_childcare_entitlement_ddl",
    "src_calculator_calculationparts_housing_calculation" : "component/uc_repos/aws-uc-lab/housing_calculator/build/src_calculator_calculationparts_housing_calculation_ddl",
  }
  bucket = data.terraform_remote_state.common.outputs.config_bucket.id
  key    = each.value

  tags = {
    Name : each.key
  }
}

