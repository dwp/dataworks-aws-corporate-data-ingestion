output "dataworks_aws_corporate_data_ingestion_common_sg" {
  value = {
    id = aws_security_group.dataworks_aws_corporate_data_ingestion_common
  }
}

output "dataworks_aws_corporate_data_ingestion_emr_launcher_lambda" {
  value = aws_lambda_function.dataworks_aws_corporate_data_ingestion_emr_launcher
}

output "start_corporate_data_ingestion_manually_arn" {
  value = aws_lambda_function.start_corporate_data_ingestion_manually.arn
}

output "private_dns" {
  value = {
    dataworks_aws_corporate_data_ingestion_discovery_dns = aws_service_discovery_private_dns_namespace.dataworks_aws_corporate_data_ingestion_services
    dataworks_aws_corporate_data_ingestion_discovery     = aws_service_discovery_service.dataworks_aws_corporate_data_ingestion_services
  }

  sensitive = true
}
