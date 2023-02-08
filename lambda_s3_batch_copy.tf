module "s3_batch_copy_lambda" {
  source = "./modules/s3_batch_operation_lambda"

  source_s3_bucket_arn       = data.terraform_remote_state.common.outputs.published_bucket["arn"]
  source_bucket_kms_arn      = data.terraform_remote_state.common.outputs.published_bucket_cmk["arn"]
  destination_s3_bucket_arn  = aws_s3_bucket.backup_bucket.arn
  destination_bucket_kms_arn = aws_kms_key.backup_bucket_cmk.arn
}