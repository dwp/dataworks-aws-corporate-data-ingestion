#!/usr/bin/env python3

import boto3
import botocore
import jinja2
import os
import sys
import yaml
import json


def main():
    if 'AWS_PROFILE' in os.environ:
        boto3.setup_default_session(profile_name=os.environ['AWS_PROFILE'])
    if 'AWS_REGION' in os.environ:
        secrets_manager = boto3.client(
            'secretsmanager', region_name=os.environ['AWS_REGION'])
    else:
        secrets_manager = boto3.client('secretsmanager')

    try:
        response = secrets_manager.get_secret_value(
            SecretId="/concourse/dataworks/terraform")
    except botocore.exceptions.ClientError as e:
        error_message = e.response["Error"]["Message"]
        if "The security token included in the request is invalid" in error_message:
            print("ERROR: Invalid security token used when calling AWS Secrets Manager. Have you run `aws-sts` recently?")
        else:
            print("ERROR: Problem calling AWS Secrets Manager: {}".format(
                error_message))
        sys.exit(1)

    config_data = yaml.load(
        response['SecretBinary'], Loader=yaml.FullLoader)
    config_data['terraform'] = json.loads(
        response['SecretBinary'])["terraform"]

    with open('terraform.tf.j2') as in_template:
        template = jinja2.Template(in_template.read())
    with open('terraform.tf', 'w+') as terraform_tf:
        terraform_tf.write(template.render(config_data))
    with open('terraform.tfvars.j2') as in_template:
        template = jinja2.Template(in_template.read())
    with open('terraform.tfvars', 'w+') as terraform_tfvars:
        terraform_tfvars.write(template.render(config_data))
    print("Terraform config successfully created")


if __name__ == "__main__":
    main()
