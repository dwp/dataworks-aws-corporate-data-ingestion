data "aws_iam_policy_document" "emr_assume_role" {
  statement {
    effect = "Allow"

    principals {
      type        = "Service"
      identifiers = ["elasticmapreduce.amazonaws.com"]
    }

    actions = ["sts:AssumeRole"]
  }
}

data "aws_iam_policy_document" "emr_capacity_reservations" {
  statement {
    effect = "Allow"

    actions = [
      "ec2:CreateLaunchTemplateVersion"
    ]

    resources = ["*"]
  }

  statement {
    effect = "Allow"

    actions = [
      "ec2:DescribeCapacityReservations"
    ]

    resources = ["*"]
  }

  statement {
    effect = "Allow"

    actions = [
      "resource-groups:ListGroupResources"
    ]

    resources = ["*"]
  }
}

resource "aws_iam_role" "aws_emr_template_repository_emr_service" {
  name               = "${local.emr_cluster_name}_emr_service"
  assume_role_policy = data.aws_iam_policy_document.emr_assume_role.json
  tags = {
    Name = "${local.emr_cluster_name}_emr_service"
  }
}

# This is new and should replace the deprecated one but doesn't work correctly
# resource "aws_iam_role_policy_attachment" "emr_attachment_service" {
#   role       = aws_iam_role.aws_emr_template_repository_emr_service.name
#   policy_arn = "arn:aws:iam::aws:policy/service-role/AmazonEMRServicePolicy_v2"
# }

# This is deprecated and needs a ticket to remove it
resource "aws_iam_role_policy_attachment" "emr_attachment_old" {
  role       = aws_iam_role.aws_emr_template_repository_emr_service.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AmazonElasticMapReduceRole"
}

resource "aws_iam_policy" "emr_capacity_reservations" {
  name        = "${local.emr_cluster_name}CapacityReservations"
  description = "Allow usage of capacity reservations"
  policy      = data.aws_iam_policy_document.emr_capacity_reservations.json
  tags = {
    Name = "emr_capacity_reservations"
  }
}

resource "aws_iam_role_policy_attachment" "emr_capacity_reservations" {
  role       = aws_iam_role.aws_emr_template_repository_emr_service.name
  policy_arn = aws_iam_policy.emr_capacity_reservations.arn
}

resource "aws_iam_role_policy_attachment" "aws_emr_template_repository_emr_service_ebs_cmk" {
  role       = aws_iam_role.aws_emr_template_repository_emr_service.name
  policy_arn = aws_iam_policy.aws_emr_template_repository_ebs_cmk_encrypt.arn
}
