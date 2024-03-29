variable "emr_release" {
  default = {
    development = "6.4.0"
    qa          = "6.4.0"
    integration = "6.4.0"
    preprod     = "6.4.0"
    production  = "6.4.0"
  }
}

variable "truststore_aliases" {
  description = "comma seperated truststore aliases"
  type        = list(string)
  default     = ["dataworks_root_ca", "dataworks_mgt_root_ca"]
}

variable "emr_instance_type_master" {
  default = {
    development = "m5.4xlarge"
    qa          = "m5.4xlarge"
    integration = "m5.4xlarge"
    preprod     = "r5.12xlarge"
    production  = "r5.12xlarge"
  }
}

variable "emr_instance_type_core_one" {
  default = {
    development = "m5.4xlarge"
    qa          = "m5.4xlarge"
    integration = "m5.4xlarge"
    preprod     = "r5.12xlarge"
    production  = "r5.12xlarge"
  }
}

# Count of instances
variable "emr_core_instance_count" {
  default = {
    development = "3"
    qa          = "3"
    integration = "3"
    preprod     = "19"
    production  = "30"
  }
}

variable "instance_core_ebs_size_in_gb" {
  default = {
    development = "250"
    qa          = "250"
    integration = "250"
    preprod     = "250"
    production  = "2000"
  }
}

variable "instance_master_ebs_size_in_gb" {
  default = {
    development = "250"
    qa          = "250"
    integration = "250"
    preprod     = "250"
    production  = "500"
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

variable "tanium_port_1" {
  description = "tanium port 1"
  type        = string
  default     = "16563"
}

variable "tanium_port_2" {
  description = "tanium port 2"
  type        = string
  default     = "16555"
}