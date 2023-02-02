resource "aws_kms_key" "backup_bucket_cmk" {
  description             = "UCFS Processed Bucket Master Key"
  deletion_window_in_days = 7
  is_enabled              = true
  enable_key_rotation     = true

  tags = {
    Name                  = "backup_bucket_cmk"
    ProtectsSensitiveData = "false"
  }
}

resource "aws_kms_alias" "backup_bucket_cmk" {
  name          = "alias/backup_bucket_cmk"
  target_key_id = aws_kms_key.backup_bucket_cmk.key_id
}

output "backup_bucket_cmk" {
  value = aws_kms_key.backup_bucket_cmk
}

resource "random_id" "backup_bucket" {
  byte_length = 16
}

resource "aws_s3_bucket" "backup_bucket" {
  tags = {
    Name = "backup-bucket"
  }
  bucket = random_id.backup_bucket.hex
  acl    = "private"

  versioning {
    enabled = false
  }

  logging {
    target_bucket = data.terraform_remote_state.security-tools.outputs.logstore_bucket.id
    target_prefix = "S3Logs/${random_id.backup_bucket.hex}/ServerLogs"
  }

  lifecycle_rule {
    id      = ""
    prefix  = ""
    enabled = true

    noncurrent_version_expiration {
      days = 30
    }
  }

  server_side_encryption_configuration {
    rule {
      apply_server_side_encryption_by_default {
        kms_master_key_id = aws_kms_key.backup_bucket_cmk.arn
        sse_algorithm     = "aws:kms"
      }
    }
  }
}

data "aws_iam_policy_document" "backup_bucket" {
  statement {
    sid     = "InventoryAndAnalyticsPolicy"
    effect  = "Allow"
    actions = ["s3:PutObject"]

    resources = [
      "${aws_s3_bucket.backup_bucket.arn}/*",
    ]

    principals {
      type        = "Service"
      identifiers = ["s3.amazonaws.com"]
    }

    condition {
      test     = "ArnLike"
      values   = [data.terraform_remote_state.common.outputs.published_bucket.arn]
      variable = "aws:SourceArn"
    }

    condition {
      test     = "StringEquals"
      values   = [local.account[local.environment]]
      variable = "aws:SourceAccount"
    }

    condition {
      test     = "StringEquals"
      values   = ["bucket-owner-full-control"]
      variable = "s3:x-amz-acl"
    }

  }

}

resource "aws_s3_bucket_policy" "backup_bucket" {
  bucket = aws_s3_bucket.backup_bucket.id
  policy = data.aws_iam_policy_document.backup_bucket.json
}

resource "aws_s3_bucket_analytics_configuration" "backup_bucket_analytics_entire_bucket" {
  bucket = aws_s3_bucket.backup_bucket.bucket
  name   = "backup_bucket_entire_bucket"
}

resource "aws_s3_bucket_public_access_block" "backup_bucket" {
  bucket = aws_s3_bucket.backup_bucket.id

  block_public_acls       = true
  block_public_policy     = true
  restrict_public_buckets = true
  ignore_public_acls      = true
}

output "backup_bucket" {
  value = aws_s3_bucket.backup_bucket
}

resource "aws_s3_bucket_inventory" "backup_intermediate_table_managed" {
  bucket = data.terraform_remote_state.common.outputs.published_bucket.id
  name   = "InventoryIntermediateTableManaged"

  included_object_versions = "Current"

  schedule {
    frequency = "Daily"
  }

  filter {
    prefix = "analytical-dataset/hive/external/uc_dw_auditlog.db/auditlog_managed/"
  }

  destination {
    bucket {
      format     = "CSV"
      bucket_arn = aws_s3_bucket.backup_bucket.arn
      prefix     = "inventory_intermediate_table_managed"
    }
  }
}

resource "aws_s3_bucket_inventory" "backup_intermediate_table_raw" {
  bucket = data.terraform_remote_state.common.outputs.published_bucket.id
  name   = "InventoryIntermediateTableRaw"

  included_object_versions = "Current"

  schedule {
    frequency = "Daily"
  }

  filter {
    prefix = "analytical-dataset/hive/external/uc_dw_auditlog.db/auditlog_raw/"
  }

  destination {
    bucket {
      format     = "CSV"
      bucket_arn = aws_s3_bucket.backup_bucket.arn
      prefix     = "inventory_intermediate_table_raw"
    }
  }
}

resource "aws_s3_bucket_inventory" "backup_user_table_redacted" {
  bucket = data.terraform_remote_state.common.outputs.published_bucket.id
  name   = "InventoryUserTableRedacted"

  included_object_versions = "Current"

  schedule {
    frequency = "Daily"
  }

  filter {
    prefix = "data/uc/auditlog_red_v/"
  }

  destination {
    bucket {
      format     = "CSV"
      bucket_arn = aws_s3_bucket.backup_bucket.arn
      prefix     = "inventory_user_table_redacted"
    }
  }
}

resource "aws_s3_bucket_inventory" "backup_user_table_secure" {
  bucket = data.terraform_remote_state.common.outputs.published_bucket.id
  name   = "InventoryUserTableSecure"

  included_object_versions = "Current"

  schedule {
    frequency = "Daily"
  }

  filter {
    prefix = "data/uc/auditlog_sec_v/"
  }

  destination {
    bucket {
      format     = "CSV"
      bucket_arn = aws_s3_bucket.backup_bucket.arn
      prefix     = "inventory_user_table_secure"
    }
  }
}
