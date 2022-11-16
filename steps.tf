resource "aws_s3_bucket_object" "corporate_data_ingestion_script" {
  bucket = data.terraform_remote_state.common.outputs.config_bucket.id
  key    = "component/${local.emr_cluster_name}/corporate-data-ingestion.py"
  content = templatefile("${path.module}/steps/corporate-data-ingestion.py",
    {
      url                = format("%s/datakey/actions/decrypt", data.terraform_remote_state.crypto.outputs.dks_endpoint[local.environment])
      aws_default_region = "eu-west-2"
      log_level          = "INFO"
      log_path           = "/var/log/dataworks-aws-corporate-data-ingestion/corporate-data-ingestion.log"
    }
  )
}

resource "aws_s3_bucket_object" "logger" {
  bucket  = data.terraform_remote_state.common.outputs.config_bucket.id
  key     = "component/${local.emr_cluster_name}/logger.py"
  content = file("${path.module}/steps/logger.py")
}

resource "aws_s3_bucket_object" "python_configuration_file" {
  bucket = data.terraform_remote_state.common.outputs.config_bucket.id
  key    = "component/${local.emr_cluster_name}/configuration.json"
  content = templatefile("${path.module}/steps/configuration.json",
    {
      s3_corporate_bucket  = data.terraform_remote_state.aws_ingestion.outputs.corporate_storage_bucket.id
      s3_published_bucket  = data.terraform_remote_state.common.outputs.published_bucket.id
      dks_decrypt_endpoint = "${data.terraform_remote_state.crypto.outputs.dks_endpoint[local.environment]}/datakey/actions/decrypt"
      dks_max_retries      = local.dks_max_retries[local.environment]
    }
  )
}

resource "aws_s3_bucket_object" "python_utils" {
  for_each = toset(["data.py", "dks.py"])
  bucket   = data.terraform_remote_state.common.outputs.config_bucket.id
  key      = "component/${local.emr_cluster_name}/${each.key}"
  content  = file("${path.module}/steps/${each.key}")
}
