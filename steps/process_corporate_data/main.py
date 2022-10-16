import os
import argparse
import datetime
import sys

from logger import setup_logging
from resume_step import should_skip_step
from pyspark.sql import SparkSession
from utility import get_s3_client
from process import process_collection

logger = setup_logging(
    log_level=os.environ["CORPORATE_DATA_INGESTION_LOG_LEVEL"].upper()
    if "CORPORATE_DATA_INGESTION_LOG_LEVEL" in os.environ else "INFO",
    log_path="${log_path}" if "LOCALSTACK" not in os.environ else None,
)


def main(spark, args, s3_client, run_timestamp, s3_corporate_bucket, s3_published_bucket):
    try:
        process_collection(
            spark,
            args,
            s3_client,
            run_timestamp,
            s3_corporate_bucket,
            s3_published_bucket
        )
    except Exception as ex:
        logger.error(
            "Some error occurred for correlation id : %s %s ",
            args.correlation_id,
            repr(ex),
        )
        # raising exception is not working with YARN so need to send an exit code(-1) for it to fail the job
        sys.exit(-1)


def get_spark_session(args):
    spark = (
        SparkSession.builder.master("spark://spark-master:7077")
            # .config("spark.metrics.conf", "/opt/emr/metrics/metrics.properties")
            # .config("spark.metrics.namespace", f"cdi_{args.snapshot_type.lower()}")
            # .config("spark.executor.heartbeatInterval", "300000")
            # .config("spark.storage.blockManagerSlaveTimeoutMs", "500000")
            # .config("spark.network.timeout", "500000")
            # .config("spark.driver.extraClassPath", "/usr/lib/spark/*")
            # .config("spark.executor.extraClassPath", "/usr/lib/spark/*")
            # .config("spark.hadoop.fs.s3.maxRetries", "20")
            # .config("spark.rpc.numRetries", "10")
            # .config("spark.task.maxFailures", "10")
            # .config("spark.scheduler.mode", "FAIR")
            # .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2")
            .appName("spike")
            # .enableHiveSupport()
            .getOrCreate()
    )
    #
    # if os.environ.get("LOCALSTACK", False):
    local_conf = spark.sparkContext._jsc.hadoopConfiguration()
    local_conf.set("fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    local_conf.set("fs.s3a.access.key", "test")
    local_conf.set("fs.s3a.secret.key", "test")
    local_conf.set("fs.s3a.path.style.access", "true")
    local_conf.set("fs.s3a.endpoint", "http://aws-s3:4566")
    return spark


def get_parameters():
    """Define and parse command line args."""
    parser = argparse.ArgumentParser(
        description="Receive args provided to spark submit job"
    )
    # Parse command line inputs and set defaults
    parser.add_argument("--correlation_id", required=True)
    parser.add_argument("--s3_prefix", required=True)
    parser.add_argument("--monitoring_topic_arn", required=True)
    parser.add_argument("--snapshot_type", required=True)
    parser.add_argument("--export_date", default=datetime.datetime.now().strftime("%Y-%m-%d"))
    args, unrecognized_args = parser.parse_known_args()

    if len(unrecognized_args) > 0:
        logger.warning(
            "Unrecognized args %s found for the correlation id %s",
            unrecognized_args,
            args.correlation_id,
        )

    return args


if __name__ == "__main__":
    args = get_parameters()
    logger.info(f"Processing spark job for correlation id : {args.correlation_id}, export date : {args.export_date}, "
                f"snapshot_type : {args.snapshot_type} and s3_prefix : {args.s3_prefix}")

    if should_skip_step(current_step_name="spark-submit"):
        logger.info("Step needs to be skipped so will exit without error")
        exit(0)

    spark = get_spark_session(args)
    s3_client = get_s3_client()

    s3_corporate_bucket = os.getenv("S3_CORPORATE_BUCKET")
    s3_publish_bucket = os.getenv("S3_PUBLISHED_BUCKET")

    run_timestamp = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")

    main(spark, args, s3_client, run_timestamp, s3_corporate_bucket, s3_publish_bucket)
