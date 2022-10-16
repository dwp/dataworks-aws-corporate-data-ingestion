import os
import boto3
import botocore


def get_s3_client():
    client_config = botocore.config.Config(
        max_pool_connections=100, retries={"max_attempts": 10, "mode": "standard"}
    )
    client = boto3.client("s3",
                          config=client_config,
                          **({"endpoint_url": "http://aws-s3:4566"} if "LOCALSTACK" in os.environ else {}))
    return client

