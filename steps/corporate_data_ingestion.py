import argparse
import json
import sys
import uuid
from datetime import datetime
from os import path


from pyspark.sql import SparkSession

from data import ConfigurationFile, Configuration

from hive import HiveService
from logger import setup_logging
from ingesters import BaseIngester, BusinessAuditIngester
from logging import getLogger

DEFAULT_AWS_REGION = "eu-west-2"

# Interpolated by terraform
setup_logging(
    log_level="${log_level}", log_path="${log_path}",
)

logger = getLogger("corporate-data-ingestion")


def get_spark_session() -> SparkSession:
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


def get_parameters() -> argparse.Namespace:
    """Define and parse command line args."""
    parser = argparse.ArgumentParser(
        description="Receive args provided to spark submit job"
    )
    # Parse command line inputs and set defaults
    parser.add_argument("--correlation_id", default=str(uuid.uuid4()))
    parser.add_argument("--source_s3_prefix", required=True)
    parser.add_argument("--destination_s3_prefix", required=True)
    parser.add_argument(
        "--export_date",
        required=False,
        help="format %Y-%m-%d, uses today if not provided",
    )
    parser.add_argument(
        "--transition_db_name",
        required=True,
        help="name of the transition Hive database",
    )
    parser.add_argument(
        "--db_name",
        required=True,
        help="name of the Hive database exposed to the end-users",
    )
    parser.add_argument(
        "--collection_name", required=True, help="name of the collection to process"
    )
    args, unrecognized_args = parser.parse_known_args()

    if len(unrecognized_args) > 0:
        logger.warning(
            f"Unrecognized args {unrecognized_args} found for the correlation id {args.correlation_id}"
        )

    return args


def main():
    try:
        job_start_time = datetime.now()
        logger.info("getting args")
        args = get_parameters()
        logger.info(f"args: {str(args)}")

        logger.info("parsing configuration file")
        with open("/opt/emr/steps/configuration.json", "r") as fd:
            data = json.load(fd)
        configuration_file = ConfigurationFile(**data)
        configuration = Configuration(
            correlation_id=args.correlation_id,
            run_timestamp=job_start_time.strftime("%Y-%m-%d_%H-%M-%S"),
            export_date=args.export_date
            if args.export_date
            else job_start_time.strftime("%Y-%m-%d"),
            collection_name=args.collection_name,
            source_s3_prefix=args.source_s3_prefix,
            destination_s3_prefix=args.destination_s3_prefix,
            transition_db_name=args.transition_db_name,
            db_name=args.db_name,
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
            transition_db_name=configuration.transition_db_name,
            db_name=configuration.db_name,
            correlation_id=configuration.correlation_id,
            spark_session=spark_session,
        )
        logger.info("Hive session: initialised")

        logger.info(
            f"Initialising ingester for collection: {configuration.collection_name}"
        )
        ingester = {
            "data.businessAudit": BusinessAuditIngester,
            "foo": BaseIngester,
            "bar": BaseIngester,
        }.get(configuration.collection_name)(configuration, spark_session, hive_session)
        logger.info(f"{ingester.__class__.__name__} ingester initialised")

        logger.info(f"Processing spark job for correlation_id: {args.correlation_id}")
        ingester.run()
    except Exception as err:
        logger.error(repr(err))
        sys.exit(-1)


if __name__ == "__main__":
    main()
