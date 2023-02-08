variable "destination_s3_bucket_arn" {
  type = string
  description = "Destination s3 bucket arn"
}

variable "destination_s3_prefix" {
  type = string
  default = "*"
  description = "optionally restrict read/write permissions to s3 destination prefix: 's3://<bucket>/<this_var>"
}

variable "source_s3_bucket_arn" {
  type = string
  description = "Source s3 bucket arn"
}

variable "source_s3_bucket_prefix" {
  type = string
  default = "*"
  description = "optionally restrict read permissions to source s3 prefix: 's3://<bucket>/<this_var>"
}

variable "destination_bucket_kms_arn" {
  type = string
  description = "kms key arn for destination bucket"
}

variable "source_bucket_kms_arn" {
  type = string
  description = "kms key arn for source bucket"
}
