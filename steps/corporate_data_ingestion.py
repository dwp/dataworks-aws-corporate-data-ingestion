import argparse
import datetime as dt
import json
import os
import sys
import uuid
from logging import getLogger
from os import path

import boto3
from pyspark.sql import SparkSession

from data import ConfigurationFile, Configuration
from dynamodb import DynamoDBHelper
from hive import HiveService
from ingesters import BaseIngester, BusinessAuditIngester, CalculationPartsIngester
from logger import setup_logging

DEFAULT_AWS_REGION = "eu-west-2"

# Interpolated by terraform
setup_logging(
    log_level="${log_level}",
    log_path="${log_path}",
)

logger = getLogger("corporate-data-ingestion")


def get_spark_session() -> SparkSession:
    """gets spark_session with configuration"""
    spark = (
        SparkSession.builder.master("yarn")
        .config("spark.storage.blockManagerSlaveTimeoutMs", "500000")
        .config("spark.hadoop.fs.s3.maxRetries", "20")
        .config("spark.rpc.numRetries", "10")
        .config("spark.task.maxFailures", "10")
        .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2")
        .config("spark.hadoop.hive.exec.dynamic.partition", "true")
        .config("spark.hadoop.hive.exec.dynamic.partition.mode", "nonstrict")
        .config("spark.sql.orc.impl", "hive")
        .appName("corporate-data-ingestion-spike")
        .enableHiveSupport()
        .getOrCreate()
    )
    return spark


def get_arguments() -> argparse.Namespace:
    """Define and parse command line args."""
    parser = argparse.ArgumentParser(description="Receive args provided to spark submit job")
    # Parse command line inputs and set defaults
    parser.add_argument("--correlation_id", default=str(uuid.uuid4()))
    parser.add_argument("--source_s3_prefix", required=False)
    parser.add_argument("--destination_s3_prefix", required=False)
    parser.add_argument("--start_date", required=False, help="format %Y-%m-%d, uses previous day if not provided")
    parser.add_argument("--end_date", required=False, help="format %Y-%m-%d, uses previous day if not provided")
    parser.add_argument("--collection", required=False, help="name of the collection to process")
    parser.add_argument("--db", required=False, help="name of the collection database")
    parser.add_argument("--table_name", required=False, help="name of the collection in mongodb")
    parser.add_argument(
        "--concurrency", required=False, default="5", help="Concurrent collections processed, default=5"
    )
    parser.add_argument(
        "--force_collection_update", required=False, default=False,
        help="force the update of the collection given as argument",
        action="store_true")
    parser.add_argument(
        "--force_export_to_hive", required=False, default=False,
        help="force the export of the collection to hive",
        action="store_true")
    args, unrecognized_args = parser.parse_known_args()

    if len(unrecognized_args) > 0:
        logger.warning(f"Unrecognized args {unrecognized_args} found for the correlation id {args.correlation_id}")

    return args


def process_collection(configuration: Configuration, spark_session, hive_session, dynamodb_client):
    """Initialise collection-specific ingesters, initialise helpers, run ingestion code """
    dynamodb_helper = DynamoDBHelper(
        client=dynamodb_client,
        correlation_id=configuration.correlation_id,
        collection_name=configuration.collection_name,
        cluster_id=configuration.cluster_id,
    )

    collection_name = configuration.collection_name

    # ingestion_class = getattr(ingesters, ingestion_class)
    start = dt.datetime.strptime(configuration.start_date, "%Y-%m-%d")
    end = dt.datetime.strptime(configuration.end_date, "%Y-%m-%d")

    ingesters_map = {
        "data:businessAudit": BusinessAuditIngester,
        "calculator:calculationParts": CalculationPartsIngester,
    }

    export_date_range = [(start + dt.timedelta(days=x)).strftime("%Y-%m-%d") for x in range(0, (end - start).days + 1)]

    for export_date in export_date_range:
        configuration.export_date = export_date
        dynamodb_helper.update_status(dynamodb_helper.IN_PROGRESS, export_date)
        logger.info(f"Initialising ingester for collection: {collection_name} - [{export_date}]")
        ingester = ingesters_map.get(configuration.collection_name, BaseIngester)(
            configuration, spark_session, hive_session, dynamodb_helper,
        )
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
    """Get args, set configuration and launch collection processing"""
    try:
        job_start_time = dt.datetime.now()
        today_str = job_start_time.date().strftime("%Y-%m-%d")

        logger.info("parsing script arguments")
        args = get_arguments()
        logger.info(f"args: {str(args)}")

        logger.info("parsing configuration file")
        with open("/opt/emr/steps/configuration.json", "r") as fd:
            data = json.load(fd)
        configuration_file = ConfigurationFile(**data)

        logger.info("Spark session: initialising")
        spark_session = get_spark_session()
        for filename in configuration_file.extra_python_files:
            spark_session.sparkContext.addPyFile(path.join("/opt/emr/steps", filename))
        logger.info("Spark session: initialised")

        dynamo_db_client = boto3.client("dynamodb")

        # Instantiate Hive service
        logger.info("Hive session: initialising")
        hive_session = HiveService(
            correlation_id=args.correlation_id,
            spark_session=spark_session,
        )
        logger.info("Hive session: initialised")

        if args.collection and args.db:
            collections = {f"{args.db}:{args.collection}": {"db": args.db, "table": args.collection}}
        else:
            collection_file = """
            {
               "collections_all":
               {
                    "db.calculator.calculationParts": {"db" : "calculator", "table" : "calculationParts"}
                }
            }
            """
            collections = json.loads(collection_file)["collections_all"]

        for key, value in collections.items():
            configuration = Configuration(
                correlation_id=args.correlation_id,
                run_timestamp=job_start_time.strftime("%Y-%m-%d_%H-%M-%S"),
                start_date=args.start_date if args.start_date else today_str,
                end_date=args.end_date if args.end_date else today_str,
                collection_name=f"{value['db']}:{value['table']}",
                db_name=value["db"],
                table_name=value["table"],
                source_s3_prefix=args.source_s3_prefix,
                destination_s3_prefix=args.destination_s3_prefix,
                concurrency=int(args.concurrency),
                cluster_id=os.environ.get("EMR_CLUSTER_ID", "NOT_SET"),
                configuration_file=configuration_file,
                force_collection_update=args.force_collection_update,
                force_export_to_hive=args.force_export_to_hive,
            )
            process_collection(configuration, spark_session, hive_session, dynamo_db_client)

    except Exception as err:
        logger.error(repr(err))
        sys.exit(-1)


if __name__ == "__main__":
    main()
