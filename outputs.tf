output "dataworks_aws_corporate_data_ingestion_common_sg" {
  value = {
    id = aws_security_group.dataworks_aws_corporate_data_ingestion_common
  }
}

output "dataworks_aws_corporate_data_ingestion_emr_launcher_lambda" {
  value = aws_lambda_function.dataworks_aws_corporate_data_ingestion_emr_launcher
}

output "private_dns" {
  value = {
    dataworks_aws_corporate_data_ingestion_discovery_dns = aws_service_discovery_private_dns_namespace.dataworks_aws_corporate_data_ingestion_services
    dataworks_aws_corporate_data_ingestion_discovery     = aws_service_discovery_service.dataworks_aws_corporate_data_ingestion_services
  }

  sensitive = true
}

output "start_backup_arn" {
  value = module.s3_batch_copy_lambda.start_backup_arn
}
