variable "destination_s3_bucket_arn" {
  type        = string
  description = "Destination s3 bucket arn"
}

variable "source_s3_bucket_arn" {
  type        = string
  description = "Source s3 bucket arn"
}

variable "source_s3_bucket_prefix" {
  type        = list(any)
  description = "optionally restrict read permissions to source s3 prefix: 's3://<bucket>/<this_var>"
}

variable "destination_bucket_kms_arn" {
  type        = string
  description = "kms key arn for destination bucket"
}

variable "source_bucket_kms_arn" {
  type        = string
  description = "kms key arn for source bucket"
}

variable "max_pool_connections" {
  type        = number
  description = "specifies the maximum number of connections to allow in a pool. By default, it is set to 10"
}

variable "max_concurrency" {
  type        = number
  description = "determines the number of concurrency or threads used to perform the operation"
}

variable "multipart_chunksize" {
  type        = number
  description = "determines the chunk size that the CLI uses for multipart transfers of individual files"
}

variable "max_attempts" {
  type        = number
  description = "provides Boto3’s retry handler with a value of maximum retry attempts"
}

variable "copy_metadata" {
  type        = bool
  description = "set to true to copy file metadata"
}

variable "copy_tagging" {
  type        = bool
  description = "set to true to copy file tags"
}

variable "copy_storage_class" {
  type        = string
  description = "set destination S3 Strorage Class"
}
