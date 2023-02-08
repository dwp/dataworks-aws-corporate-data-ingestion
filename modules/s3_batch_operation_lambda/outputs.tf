output "start_backup_arn" {
  value = aws_lambda_function.s3_batch_copy.arn
}

output "start_backup_function_name" {
  value = aws_lambda_function.s3_batch_copy.function_name
}
