resource "aws_acm_certificate" "aws_emr_template_repository" {
  certificate_authority_arn = data.terraform_remote_state.aws_certificate_authority.outputs.root_ca.arn
  domain_name               = "aws-emr-template-repository.${local.env_prefix[local.environment]}${local.dataworks_domain_name}"

  options {
    certificate_transparency_logging_preference = "ENABLED"
  }
  tags = {
    Name = local.emr_cluster_name
  }
}

data "aws_iam_policy_document" "aws_emr_template_repository_acm" {
  statement {
    effect = "Allow"

    actions = [
      "acm:ExportCertificate",
    ]

    resources = [
      aws_acm_certificate.aws_emr_template_repository.arn
    ]
  }
}

resource "aws_iam_policy" "aws_emr_template_repository_acm" {
  name        = "ACMExport-aws-emr-template-repository-Cert"
  description = "Allow export of aws-emr-template-repository certificate"
  policy      = data.aws_iam_policy_document.aws_emr_template_repository_acm.json
  tags = {
    Name = "aws_emr_template_repository_acm"
  }
}

data "aws_iam_policy_document" "aws_emr_template_repository_certificates" {
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

resource "aws_iam_policy" "aws_emr_template_repository_certificates" {
  name        = "aws_emr_template_repositoryGetCertificates"
  description = "Allow read access to the Crown-specific subset of the aws_emr_template_repository"
  policy      = data.aws_iam_policy_document.aws_emr_template_repository_certificates.json
  tags = {
    Name = "aws_emr_template_repository_certificates"
  }
}


