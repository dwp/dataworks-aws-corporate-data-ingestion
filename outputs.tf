output "aws_emr_template_repository_common_sg" {
  value = {
    id = aws_security_group.aws_emr_template_repository_common
  }
}

output "aws_emr_template_repository_emr_launcher_lambda" {
  value = aws_lambda_function.aws_emr_template_repository_emr_launcher
}

output "private_dns" {
  value = {
    aws_emr_template_repository_discovery_dns = aws_service_discovery_private_dns_namespace.aws_emr_template_repository_services
    aws_emr_template_repository_discovery     = aws_service_discovery_service.aws_emr_template_repository_services
  }
}
