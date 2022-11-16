import argparse
import json
import uuid
import zlib
from datetime import datetime
from typing import Optional, List

import boto3
import pyspark
from botocore import config as boto_config
from botocore.client import BaseClient
from py4j.protocol import Py4JJavaError
from pyspark.sql import SparkSession

from logger import setup_logging
from data import UCMessage, ConfigurationFile, Configuration
from dks import MessageCryptoHelper, DKSService

DEFAULT_AWS_REGION = "eu-west-2"

# Interpolated by terraform
logger = setup_logging(
    log_level="${log_level}",
    log_path="${log_path}",
)


class Utils(object):
    @staticmethod
    def get_list_keys_for_s3_prefix(
        s3_client: BaseClient, s3_bucket: str, s3_prefix: str
    ) -> List[str]:
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
    def decompress(compressed_text: bytes, accumulator: pyspark.Accumulator) -> bytes:
        accumulator += 1
        return zlib.decompress(compressed_text, 16 + zlib.MAX_WBITS)

    @staticmethod
    def to_records(multi_record_bytes: bytes) -> List[str]:
        return multi_record_bytes.decode().rstrip("\n").split("\n")

    @staticmethod
    def get_decryption_helper(
        decrypt_endpoint: str,
        dks_call_accumulator: Optional[pyspark.Accumulator] = None,
    ) -> MessageCryptoHelper:
        certificates = (
            "/etc/pki/tls/certs/private_key.crt",
            "/etc/pki/tls/private/private_key.key",
        )
        verify = "/etc/pki/ca-trust/source/anchors/analytical_ca.pem"

        return MessageCryptoHelper(
            DKSService(
                dks_decrypt_endpoint=decrypt_endpoint,
                dks_datakey_endpoint="not_configured",
                certificates=certificates,
                verify=verify,
                dks_call_accumulator=dks_call_accumulator,
            )
        )


class CorporateDataIngester:
    def __init__(self, configuration):
        self.configuration = configuration
        self.s3_client = self.get_s3_client()
        self.spark = self.get_spark_session()

    @staticmethod
    def get_spark_session():
        spark = (
            SparkSession.builder.master("yarn")
            .config("spark.executor.heartbeatInterval", "300000")
            .config("spark.storage.blockManagerSlaveTimeoutMs", "500000")
            .config("spark.network.timeout", "500000")
            .config("spark.executor.instances", "1")
            .config("spark.executor.cores", "1")
            .config("spark.hadoop.fs.s3.maxRetries", "20")
            .config("spark.rpc.numRetries", "10")
            .config("spark.task.maxFailures", "10")
            .config("spark.scheduler.mode", "FAIR")
            .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2")
            .appName("corporate-data-ingestion-spike")
            .enableHiveSupport()
            .getOrCreate()
        )
        return spark

    @staticmethod
    def get_s3_client() -> BaseClient:
        client_config = boto_config.Config(
            max_pool_connections=100, retries={"max_attempts": 10, "mode": "standard"}
        )
        client = boto3.client("s3", config=client_config)
        return client

    def read_binary(self, file_path):
        return self.spark.sparkContext.binaryFiles(file_path)

    # Processes and publishes data
    def execute(self):
        s3_source_url = "s3://{bucket}/{prefix}".format(
            bucket=self.configuration.configuration_file.s3_corporate_bucket,
            prefix=self.configuration.source_s3_prefix.lstrip("/"),
        )
        s3_destination_url = "s3://{bucket}/{prefix}".format(
            bucket=self.configuration.configuration_file.s3_published_bucket,
            prefix=self.configuration.destination_s3_prefix.lstrip("/"),
        )

        try:
            # Set up accumulators, instantiate providers for decryption
            file_accumulator = self.spark.sparkContext.accumulator(0)
            record_accumulator = self.spark.sparkContext.accumulator(0)
            dks_call_accumulator = self.spark.sparkContext.accumulator(0)
            logger.info(f"Instantiating decryption helper")
            decryption_helper = Utils.get_decryption_helper(
                decrypt_endpoint=self.configuration.configuration_file.dks_decrypt_endpoint,
                dks_call_accumulator=dks_call_accumulator,

            )

            logger.info(f"Emptying destination prefix")
            self.empty_s3_destination_prefix()

            file_rdd = self.read_binary(s3_source_url).mapValues(
                lambda x: Utils.decompress(x, file_accumulator)
            )
            record_rdd = (
                file_rdd.flatMapValues(Utils.to_records)
                .coalesce(1)
                .map(lambda x: UCMessage(x[1]))
                .map(
                    lambda x: decryption_helper.decrypt_dbobject(x, record_accumulator)
                )
                .saveAsTextFile(
                    s3_destination_url,
                    compressionCodecClass="com.hadoop.compression.lzo.LzopCodec",
                )
            )

            file_count = file_accumulator.value
            record_count = record_accumulator.value
            dks_call_count = dks_call_accumulator.value

            self.create_result_file_in_s3_destination_prefix(record_count)
            logger.info(f"Number files in RDD: {file_count}")
            logger.info(f"Number of records in RDD: {record_count}")
            logger.info(f"Number of call to DKS: {dks_call_count}")

        except Py4JJavaError as err:
            logger.error(
                f"""Spark error occurred processing collection named {self.configuration.collection_name} """
                f""" for correlation id: {self.configuration.correlation_id} {str(err)}" """
            )
            raise
        except Exception as err:
            logger.error(
                f"""Unexpected error occurred processing collection named {self.configuration.collection_name} """
                f""" for correlation id: {self.configuration.correlation_id} "{str(err)}" """
            )
            raise

    # Empty S3 destination prefix before publishing
    def empty_s3_destination_prefix(self) -> None:
        s3_resource = boto3.resource("s3")
        bucket = s3_resource.Bucket(
            self.configuration.configuration_file.s3_published_bucket
        )
        bucket.objects.filter(Prefix=self.configuration.destination_s3_prefix).delete()

    # Creates result file in S3 in S3 destination prefix
    def create_result_file_in_s3_destination_prefix(
        self, record_ingested_count: int
    ) -> None:
        try:
            result_json = json.dumps(
                {
                    "correlation_id": self.configuration.correlation_id,
                    "record_ingested_count": record_ingested_count,
                }
            )

            s3_client = self.get_s3_client()
            response = s3_client.put_object(
                Body=result_json,
                Bucket=self.configuration.configuration_file.s3_published_bucket,
                Key=f"corporate_data_ingestion/audit_logs_transition/results/{self.configuration.correlation_id}/result.json",
            )
        except Exception as ex:
            logger.error(
                f"""Unable to create result file for correlation id: {self.configuration.correlation_id} {repr(ex)}"""
            )
            raise


def get_parameters() -> argparse.Namespace:
    """Define and parse command line args."""
    parser = argparse.ArgumentParser(
        description="Receive args provided to spark submit job"
    )
    # Parse command line inputs and set defaults
    parser.add_argument("--correlation_id", default=str(uuid.uuid4()))
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


def main():
    args = get_parameters()
    logger.info(f"args: ", extra={arg: val for arg, val in vars(args).items()})
    with open("/opt/emr/steps/configuration.json", "r") as fd:
        data = json.load(fd)
    configuration_file = ConfigurationFile(**data)
    configuration = Configuration(
        correlation_id=args.correlation_id,
        run_timestamp=datetime.now().strftime("%Y-%m-%d_%H-%M-%S"),
        collection_name="data.businessAudit",
        source_s3_prefix=args.source_s3_prefix,
        destination_s3_prefix=args.destination_s3_prefix,
        configuration_file=configuration_file,
    )
    ingester = CorporateDataIngester(configuration)
    logger.info(f"Processing spark job for correlation_id: {args.correlation_id}")
    ingester.execute()


if __name__ == "__main__":
    main()
