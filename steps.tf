#uploading of step files to s3 go here

resource "aws_s3_bucket_object" "example_step_name_sh" {
  bucket     = data.terraform_remote_state.common.outputs.config_bucket.id
  kms_key_id = data.terraform_remote_state.common.outputs.config_bucket_cmk.arn
  key        = "component/aws-emr-template-repository/example-create-database.sh"
  content = templatefile("${path.module}/steps/example-create-database.sh",
    {
      published_bucket = format("s3://%s", data.terraform_remote_state.common.outputs.published_bucket.id)
      hive_metastore_location = local.hive_metastore_location
    }
  )
  tags = {
    Name = "example_step_name_sh"
  }
}
