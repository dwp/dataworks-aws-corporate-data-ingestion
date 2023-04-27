import argparse
import datetime as dt
import itertools
import json
import os
import sys
import uuid
from concurrent.futures import ThreadPoolExecutor
from logging import getLogger
from os import path

import boto3
from pyspark.sql import SparkSession

from data import ConfigurationFile, Configuration
from dynamodb import DynamoDBHelper
from hive import HiveService
from ingesters import BaseIngester, BusinessAuditIngester, CalculationPartsIngester, CalcPartBenchmark, \
    CalculationPartsDeduplicate, CalculationPartsAppend
from logger import setup_logging

DEFAULT_AWS_REGION = "eu-west-2"

# Interpolated by terraform
setup_logging(
    log_level="${log_level}", log_path="${log_path}",
)

logger = getLogger("corporate-data-ingestion")


def get_spark_session() -> SparkSession:
    spark = (
        SparkSession.builder.master("yarn")
        .config("spark.storage.blockManagerSlaveTimeoutMs", "500000")
        .config("spark.hadoop.fs.s3.maxRetries", "20")
        .config("spark.rpc.numRetries", "10")
        .config("spark.task.maxFailures", "10")
        .config("spark.scheduler.mode", "FAIR")
        .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2")
        .config("spark.hadoop.hive.exec.dynamic.partition", "true")
        .config("spark.hadoop.hive.exec.dynamic.partition.mode", "nonstrict")
        .appName("corporate-data-ingestion-spike")
        .enableHiveSupport()
        .getOrCreate()
    )
    return spark


def get_parameters() -> argparse.Namespace:
    """Define and parse command line args."""
    parser = argparse.ArgumentParser(
        description="Receive args provided to spark submit job"
    )
    # Parse command line inputs and set defaults
    parser.add_argument("--correlation_id", default=str(uuid.uuid4()))
    parser.add_argument("--source_s3_prefix", required=True)
    parser.add_argument("--destination_s3_prefix", required=True)
    parser.add_argument("--start_date", required=False, help="format %Y-%m-%d, uses previous day if not provided")
    parser.add_argument("--end_date", required=False, help="format %Y-%m-%d, uses previous day if not provided")
    parser.add_argument("--collection_names", required=True, help="name of the collections to process")
    parser.add_argument("--override_ingestion_class", required=False, help="Optionally use specific ingestion class")
    parser.add_argument("--concurrency", required=False, default="5",
                        help="Concurrent collections processed, default=5")
    args, unrecognized_args = parser.parse_known_args()

    if len(unrecognized_args) > 0:
        logger.warning(
            f"Unrecognized args {unrecognized_args} found for the correlation id {args.correlation_id}"
        )

    return args


def process_collection(collection_name, override_ingestion_class, ingesters, configuration: Configuration,
                       spark_session, hive_session, dynamodb_client):
    dynamodb_helper = DynamoDBHelper(
        client=dynamodb_client,
        correlation_id=configuration.correlation_id,
        collection_name=collection_name,
        cluster_id=configuration.cluster_id,
    )

    start = dt.datetime.strptime(configuration.start_date, "%Y-%m-%d")
    end = dt.datetime.strptime(configuration.end_date, "%Y-%m-%d")

    if override_ingestion_class:
        ingestion_class = ingesters.get(override_ingestion_class)
        if not ingestion_class:
            raise ValueError(f"Override ingestion class not found: {override_ingestion_class}")
    else:
        ingestion_class = ingesters.get(collection_name, BaseIngester)

    export_date_range = [
        (start + dt.timedelta(days=x)).strftime("%Y-%m-%d")
        for x in range(0, (end - start).days + 1)
    ]

    for export_date in export_date_range:
        configuration.export_date = export_date
        dynamodb_helper.update_status(dynamodb_helper.IN_PROGRESS, export_date)
        logger.info(f"Initialising ingester for collection: {collection_name} - [{export_date}]")
        ingester = ingestion_class(configuration, collection_name, spark_session, hive_session)
        logger.info(f"{ingester.__class__.__name__}::{collection_name}::{export_date}:: ingester initialised")
        logger.info(f"{ingester.__class__.__name__}::{collection_name}::{export_date}:: ingester running")
        try:
            ingester.run()
            dynamodb_helper.update_status(dynamodb_helper.COMPLETED, export_date)
        except Exception as e:
            dynamodb_helper.update_status(dynamodb_helper.FAILED, export_date)
            raise e
        logger.info(f"{ingester.__class__.__name__}::{collection_name}::{export_date}:: ingester completed")


def main():
    try:
        job_start_time = dt.datetime.now()
        logger.info("getting args")
        args = get_parameters()
        logger.info(f"args: {str(args)}")
        today_str = dt.datetime.now().date().strftime("%Y-%m-%d")

        logger.info("parsing configuration file")
        with open("/opt/emr/steps/configuration.json", "r") as fd:
            data = json.load(fd)
        configuration_file = ConfigurationFile(**data)
        configuration = Configuration(
            correlation_id=args.correlation_id,
            run_timestamp=job_start_time.strftime("%Y-%m-%d_%H-%M-%S"),
            start_date=args.start_date if args.start_date else today_str,
            end_date=args.end_date if args.end_date else today_str,
            collection_names=args.collection_names.split(","),
            override_ingestion_class=args.override_ingestion_class,
            source_s3_prefix=args.source_s3_prefix,
            destination_s3_prefix=args.destination_s3_prefix,
            concurrency=int(args.concurrency),
            cluster_id=os.environ.get("EMR_CLUSTER_ID", "NOT_SET"),
            configuration_file=configuration_file,
        )

        logger.info("Spark session: initialising")
        spark_session = get_spark_session()
        logger.info(str(configuration_file.extra_python_files))
        for filename in configuration_file.extra_python_files:
            spark_session.sparkContext.addPyFile(path.join("/opt/emr/steps", filename))
        logger.info("Spark session: initialised")

        # Instantiate Hive service
        logger.info("Hive session: initialising")
        hive_session = HiveService(
            correlation_id=configuration.correlation_id,
            spark_session=spark_session,
        )
        logger.info("Hive session: initialised")

        dynamo_db_client = boto3.client("dynamodb")

        ingesters = {
            "data:businessAudit": BusinessAuditIngester,
            "calculator:calculationParts": CalculationPartsIngester,
            # "calculator:calculationPartsBenchmark": CalcPartBenchmark,
            "calculator:calculationPartsDeduplicate": CalculationPartsDeduplicate,
            "calculator:calculationPartsAppend": CalculationPartsAppend,
        }

        with ThreadPoolExecutor(max_workers=configuration.concurrency) as executor:
            _results = list(executor.map(
                process_collection,
                configuration.collection_names,
                itertools.repeat(configuration.override_ingestion_class),
                itertools.repeat(ingesters),
                itertools.repeat(configuration),
                itertools.repeat(spark_session),
                itertools.repeat(hive_session),
                itertools.repeat(dynamo_db_client),
            ))

    except Exception as err:
        logger.error(repr(err))
        sys.exit(-1)


if __name__ == "__main__":
    main()
