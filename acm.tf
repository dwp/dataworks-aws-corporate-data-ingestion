resource "aws_acm_certificate" "dataworks_aws_corporate_data_ingestion" {
  certificate_authority_arn = data.terraform_remote_state.aws_certificate_authority.outputs.root_ca.arn
  domain_name               = "dataworks-aws-corporate-data-ingestion.${local.env_prefix[local.environment]}${local.dataworks_domain_name}"

  options {
    certificate_transparency_logging_preference = "ENABLED"
  }
  tags = {
    Name = local.emr_cluster_name
  }
}

data "aws_iam_policy_document" "dataworks_aws_corporate_data_ingestion_acm" {
  statement {
    effect = "Allow"

    actions = [
      "acm:ExportCertificate",
    ]

    resources = [
      aws_acm_certificate.dataworks_aws_corporate_data_ingestion.arn
    ]
  }
}

resource "aws_iam_policy" "dataworks_aws_corporate_data_ingestion_acm" {
  name        = "ACMExport-dataworks-aws-corporate-data-ingestion-Cert"
  description = "Allow export of dataworks-aws-corporate-data-ingestion certificate"
  policy      = data.aws_iam_policy_document.dataworks_aws_corporate_data_ingestion_acm.json
  tags = {
    Name = "dataworks_aws_corporate_data_ingestion_acm"
  }
}

data "aws_iam_policy_document" "dataworks_aws_corporate_data_ingestion_certificates" {
  statement {
    effect = "Allow"

    actions = [
      "s3:Get*",
      "s3:List*",
    ]

    resources = [
      "arn:aws:s3:::${local.mgt_certificate_bucket}*",
      "arn:aws:s3:::${local.env_certificate_bucket}/*",
    ]
  }
}

resource "aws_iam_policy" "dataworks_aws_corporate_data_ingestion_certificates" {
  name        = "dataworks_aws_corporate_data_ingestionGetCertificates"
  description = "Allow read access to the Crown-specific subset of the dataworks_aws_corporate_data_ingestion"
  policy      = data.aws_iam_policy_document.dataworks_aws_corporate_data_ingestion_certificates.json
  tags = {
    Name = "dataworks_aws_corporate_data_ingestion_certificates"
  }
}


