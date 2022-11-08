import argparse
import json
import uuid
import zlib
from datetime import datetime
from dataclasses import dataclass
from py4j.protocol import Py4JJavaError

import boto3
import botocore

from pyspark.sql import SparkSession
from logger import setup_logging


DEFAULT_AWS_REGION = "eu-west-2"

# Interpolated by terraform
logger = setup_logging(
    log_level="${log_level}",
    log_path="${log_path}",
)


@dataclass
class ConfigurationFile:
    """Class for keeping configuration read from terraform-interpolated configuration file."""
    s3_corporate_bucket: str
    s3_published_bucket: str


@dataclass
class Configuration:
    """Class for keeping application configuration."""
    correlation_id: str
    run_timestamp: str  # format: "%Y-%m-%d_%H-%M-%S"
    collection_name: str
    source_s3_prefix: str
    destination_s3_prefix: str
    configuration_file: ConfigurationFile


class Utils(object):
    @staticmethod
    def get_s3_client():
        client_config = botocore.config.Config(
            max_pool_connections=100, retries={"max_attempts": 10, "mode": "standard"}
        )
        client = boto3.client("s3", config=client_config)
        return client

    @staticmethod
    def get_spark_session(configuration):
        spark = (
            SparkSession.builder.master("yarn")
                .config("spark.executor.heartbeatInterval", "300000")
                .config("spark.storage.blockManagerSlaveTimeoutMs", "500000")
                .config("spark.network.timeout", "500000")
                .config("spark.hadoop.fs.s3.maxRetries", "20")
                .config("spark.rpc.numRetries", "10")
                .config("spark.task.maxFailures", "10")
                .config("spark.scheduler.mode", "FAIR")
                .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2")
                .appName("spike")
                .enableHiveSupport()
                .getOrCreate()
        )
        return spark

    @staticmethod
    def get_list_keys_for_s3_prefix(s3_client, s3_bucket, s3_prefix):
        logger.info(
            "Looking for files to process in bucket : %s with prefix : %s",
            s3_bucket,
            s3_prefix,
        )
        keys = []
        paginator = s3_client.get_paginator("list_objects_v2")
        pages = paginator.paginate(Bucket=s3_bucket, Prefix=s3_prefix)
        for page in pages:
            if "Contents" in page:
                for obj in page["Contents"]:
                    keys.append(obj["Key"])
        if s3_prefix in keys:
            keys.remove(s3_prefix)
        return keys

    @staticmethod
    def decompress(compressed_text):
        return zlib.decompress(compressed_text, 16 + zlib.MAX_WBITS)


class CorporateDataIngester:
    def __init__(self, configuration):
        self.configuration = configuration
        self.s3_client = Utils.get_s3_client()
        self.spark = Utils.get_spark_session(configuration)

    def read_binary(self, file_path):
        return self.spark.sparkContext.binaryFiles(file_path)

    # Processes and publishes data
    def execute(self):
        s3_source_url = f"s3://{self.configuration.configuration_file.s3_corporate_bucket}/" \
                        f"{self.configuration.source_s3_prefix.lstrip('/')}"

        s3_destination_url = f"s3://{self.configuration.configuration_file.s3_published_bucket}/" \
                             f"{self.configuration.destination_s3_prefix.lstrip('/')}"
        try:
            self.empty_s3_destination_prefix()
            rdd = self.read_binary(s3_source_url)
            self.create_result_file_in_s3_destination_prefix(rdd.count())
            decompressed = rdd.mapValues(Utils.decompress)
            decompressed.saveAsTextFile(s3_destination_url, compressionCodecClass="com.hadoop.compression.lzo.LzopCodec")
        except Py4JJavaError as err:
            logger.error(f"""Spark error occurred processing collection named {configuration.collection_name} """
                         f""" for correlation id: {configuration.correlation_id} {str(err)}" """)
            raise
        except Exception as err:
            logger.error(f"""Unexpected error occurred processing collection named {configuration.collection_name} """
                         f""" for correlation id: {configuration.correlation_id} {str(err)}" """)
            raise

    # Empty S3 destination prefix before publishing
    def empty_s3_destination_prefix(self):
        s3_resource = boto3.resource('s3')
        bucket = s3_resource.Bucket(self.configuration.configuration_file.s3_published_bucket)
        bucket.objects.filter(Prefix=self.configuration.destination_s3_prefix).delete()

    # Creates result file in S3 in S3 destination prefix
    def create_result_file_in_s3_destination_prefix(self, record_ingested_count):
        try:
            result_json = json.dumps({"correlation_id": self.configuration.correlation_id,
                                      "record_ingested_count": record_ingested_count})

            s3_client = Utils.get_s3_client()
            response = s3_client.put_object(
                Body=result_json,
                Bucket=self.configuration.configuration_file.s3_published_bucket,
                Key=f"corporate_data_ingestion/audit_logs_transition/results/{self.configuration.correlation_id}/result.json")
        except Exception as ex:
            logger.error(
                f"""Unable to create result file for correlation id: {configuration.correlation_id} {repr(ex)}""")
            raise


def get_parameters():
    """Define and parse command line args."""
    parser = argparse.ArgumentParser(
        description="Receive args provided to spark submit job"
    )
    # Parse command line inputs and set defaults
    parser.add_argument("--correlation_id", default=f"corporate_data_ingestion_{str(uuid.uuid4())}")
    parser.add_argument("--source_s3_prefix", required=True)
    parser.add_argument("--destination_s3_prefix", required=True)
    args, unrecognized_args = parser.parse_known_args()

    if len(unrecognized_args) > 0:
        logger.warning(
            "Unrecognized args %s found for the correlation id %s",
            unrecognized_args,
            args.correlation_id,
        )

    return args


def main(configuration):
    ingester = CorporateDataIngester(configuration)
    ingester.execute()


if __name__ == "__main__":
    args = get_parameters()
    with open("/opt/emr/steps/configuration.json", "r") as fd:
        data = json.load(fd)

    configuration_file = ConfigurationFile(**data)

    configuration = Configuration(correlation_id=args.correlation_id,
                                  run_timestamp=datetime.now().strftime("%Y-%m-%d_%H-%M-%S"),
                                  collection_name="data.businessAudit",
                                  source_s3_prefix=args.source_s3_prefix,
                                  destination_s3_prefix=args.destination_s3_prefix,
                                  configuration_file=configuration_file)
    logger.info(f"Processing spark job for correlation_id: {args.correlation_id}")

    main(configuration)
