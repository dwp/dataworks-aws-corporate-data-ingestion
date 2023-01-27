variable "emr_release" {
  default = {
    development = "6.2.0"
    qa          = "6.2.0"
    integration = "6.2.0"
    preprod     = "6.2.0"
    production  = "6.2.0"
  }
}

variable "truststore_aliases" {
  description = "comma seperated truststore aliases"
  type        = list(string)
  default     = ["dataworks_root_ca", "dataworks_mgt_root_ca"]
}

variable "emr_instance_type_master" {
  default = {
    development = "m5.2xlarge"
    qa          = "m5.4xlarge"
    integration = "m5.4xlarge"
    preprod     = "m5.2xlarge"
    production  = "m5.16xlarge"
  }
}

variable "emr_instance_type_core_one" {
  default = {
    development = "m5.4xlarge"
    qa          = "m5.xlarge"
    integration = "m5.xlarge"
    preprod     = "m5.4xlarge"
    production  = "m5.16xlarge"
  }
}

# Count of instances
variable "emr_core_instance_count" {
  default = {
    development = "10"
    qa          = "2"
    integration = "2"
    preprod     = "10"
    production  = "39"
  }
}

variable "emr_ami_id" {
  description = "AMI ID to use for the EMR nodes"
}

variable "region" {
  description = "AWS Region name"
  default     = "eu-west-2"
}

variable "corporate_storage_s3_prefix" {
  default = {
    development = "corporate_storage/"
    qa          = "corporate_storage/"
    integration = "corporate_storage/"
    preprod     = "corporate_storage/"
    production  = "corporate_storage/"
  }
}
