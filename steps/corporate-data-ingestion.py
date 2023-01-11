import argparse
import json
import sys
import uuid
from datetime import datetime
from os import path

import boto3
from botocore import config as boto_config
from botocore.client import BaseClient
from py4j.protocol import Py4JJavaError
from pyspark.sql import SparkSession

from data import UCMessage, ConfigurationFile, Configuration
from utils import Utils
from hive import HiveService
from logger import setup_logging
from logging import getLogger

DEFAULT_AWS_REGION = "eu-west-2"

# Interpolated by terraform
setup_logging(
    log_level="${log_level}",
    log_path="${log_path}",
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


class CorporateDataIngester:
    def __init__(self, configuration, spark_session):
        self._configuration = configuration
        self._spark_session = spark_session
        logger.info("S3 client: initialising")
        self._s3_client = self.get_s3_client()
        logger.info("S3 client: initialised")

    @staticmethod
    def get_s3_client() -> BaseClient:
        client_config = boto_config.Config(max_pool_connections=100, retries={"max_attempts": 10, "mode": "standard"})
        client = boto3.client("s3", config=client_config)
        return client

    def read_binary(self, file_path):
        return self._spark_session.sparkContext.binaryFiles(file_path)

    # Processes and publishes data
    def execute(self):
        logger.info("Reading configuration")
        correlation_id = self._configuration.correlation_id
        export_date = self._configuration.export_date

        corporate_bucket = self._configuration.configuration_file.s3_corporate_bucket
        source_prefix = self._configuration.source_s3_prefix

        published_bucket = self._configuration.configuration_file.s3_published_bucket
        destination_prefix = self._configuration.destination_s3_prefix

        # define source and destination s3 URIs
        s3_source_url = "s3://{bucket}/{prefix}".format(
            bucket=corporate_bucket,
            prefix=source_prefix.lstrip("/"),
        )
        s3_destination_url = "s3://{bucket}/{prefix}".format(
            bucket=published_bucket,
            prefix=path.join(destination_prefix.lstrip("/"), export_date)
        )

        # begin processing
        try:
            file_accumulator = self._spark_session.sparkContext.accumulator(0)
            record_accumulator = self._spark_session.sparkContext.accumulator(0)
            dks_call_accumulator = self._spark_session.sparkContext.accumulator(0)

            logger.info(f"Instantiating decryption helper")
            decryption_helper = Utils.get_decryption_helper(
                decrypt_endpoint=self._configuration.configuration_file.dks_decrypt_endpoint,
                dks_call_accumulator=dks_call_accumulator,
            )

            logger.info(f"Emptying destination prefix")
            self.empty_s3_prefix(published_bucket=published_bucket, prefix=destination_prefix)

            # Persist records to JSONL in S3
            logger.info("starting pyspark processing")
            (
                self.read_binary(s3_source_url)
                    .mapValues(lambda x: Utils.decompress(x, file_accumulator))
                    .flatMapValues(Utils.to_records)
                    .map(lambda x: UCMessage(x[1]))
                    .map(lambda x: decryption_helper.decrypt_message_dbObject(x, correlation_id, record_accumulator))
                    .map(lambda x: x.dbobject)
                    .saveAsTextFile(s3_destination_url, compressionCodecClass="com.hadoop.compression.lzo.LzopCodec")
            )

            # stats for logging
            file_count = file_accumulator.value
            record_count = record_accumulator.value
            dks_call_count = dks_call_accumulator.value

            self.create_result_file_in_s3_destination_prefix(record_count)
            logger.info(f"Number files in RDD: {file_count}")
            logger.info(f"Number of records in RDD: {record_count}")
            logger.info(f"Number of call to DKS: {dks_call_count}")

        except Py4JJavaError as err:
            logger.error(
                f"""Spark error occurred processing collection named {correlation_id} """
                f""" for correlation id: {correlation_id} {str(err)}" """
            )
            raise
        except Exception as err:
            logger.error(
                f"""Unexpected error occurred processing collection named {self._configuration.collection_name} """
                f""" for correlation id: {correlation_id} "{str(err)}" """
            )
            raise

    # Empty S3 destination prefix before publishing
    @staticmethod
    def empty_s3_prefix(published_bucket, prefix) -> None:
        s3_resource = boto3.resource("s3")
        bucket = s3_resource.Bucket(published_bucket)
        bucket.objects.filter(Prefix=prefix).delete()

    # Creates result file in S3 in S3 destination prefix
    def create_result_file_in_s3_destination_prefix(self, record_ingested_count: int) -> None:
        try:
            result_json = json.dumps(
                {
                    "correlation_id": self._configuration.correlation_id,
                    "record_ingested_count": record_ingested_count,
                }
            )

            _ = self._s3_client.put_object(
                Body=result_json,
                Bucket=self._configuration.configuration_file.s3_published_bucket,
                Key=f"corporate_data_ingestion/audit_logs_transition/results/{self._configuration.correlation_id}/result.json",
            )
        except Exception as ex:
            logger.error(
                f"""Unable to create result file for correlation id: {self._configuration.correlation_id} {repr(ex)}"""
            )
            raise


def get_parameters() -> argparse.Namespace:
    """Define and parse command line args."""
    parser = argparse.ArgumentParser(description="Receive args provided to spark submit job")
    # Parse command line inputs and set defaults
    parser.add_argument("--correlation_id", default=str(uuid.uuid4()))
    parser.add_argument("--source_s3_prefix", required=True)
    parser.add_argument("--destination_s3_prefix", required=True)
    parser.add_argument("--export_date", required=False, help="format %Y-%m-%d, uses today if not provided")
    parser.add_argument("--transition_db_name", required=True, help="name of the transition Hive database")
    parser.add_argument("--db_name", required=True, help="name of the Hive database exposed to the end-users")
    args, unrecognized_args = parser.parse_known_args()

    if len(unrecognized_args) > 0:
        logger.warning(f"Unrecognized args {unrecognized_args} found for the correlation id {args.correlation_id}")

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
            export_date=args.export_date if args.export_date else job_start_time.strftime("%Y-%m-%d"),
            collection_name="data.businessAudit",
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

        logger.info("CorporateDataIngester initialising")
        ingester = CorporateDataIngester(configuration, spark_session)
        logger.info("CorporateDataIngester initialised")
        logger.info(f"Processing spark job for correlation_id: {args.correlation_id}")
        ingester.execute()

        s3_destination_url = "s3://{bucket}/{prefix}".format(
            bucket=configuration.configuration_file.s3_published_bucket,
            prefix=path.join(configuration.destination_s3_prefix.lstrip("/"), configuration.export_date)
        )

        # Instantiate Hive service
        hive_service = HiveService(transition_db_name=configuration.transition_db_name,
                                   db_name=configuration.db_name,
                                   correlation_id=configuration.correlation_id,
                                   spark_session=spark_session)
        hive_service.create_database_if_not_exist(configuration.transition_db_name)
        hive_service.create_database_if_not_exist(configuration.db_name)

        # Declare parameters for audit logs processing
        sql_file_base_location = "/opt/emr/audit_sql/"
        db_name = configuration.transition_db_name
        table_name = "auditlog"
        export_date = configuration.export_date

        # Create raw managed table (two columns)
        sql_statement = f"""
            CREATE TABLE IF NOT EXISTS {db_name}.auditlog_raw (val STRING)
            PARTITIONED BY (date_str STRING) STORED
            AS orc TBLPROPERTIES ('orc.compress'='ZLIB')
        """
        hive_service.execute_sql_statement_with_interpolation(sql_statement=sql_statement)

        # Create expanded managed table (multi-columns)
        interpolation_dict = {"#{hivevar:auditlog_database}": configuration.transition_db_name}
        hive_service.execute_sql_statement_with_interpolation(file=path.join(sql_file_base_location,
                                                                             "auditlog_managed_table.sql"),
                                                              interpolation_dict=interpolation_dict)

        # Create raw external table (two columns) and populate raw managed table
        external_table_name = f"auditlog_raw_{configuration.export_date.replace('-', '_')}"
        sql_statement = f"""
            CREATE EXTERNAL TABLE {db_name}.{external_table_name} (val STRING) PARTITIONED BY (date_str STRING) STORED AS TEXTFILE LOCATION '{s3_destination_url}';
            ALTER TABLE {db_name}.{external_table_name} ADD IF NOT EXISTS PARTITION(date_str='{export_date}') LOCATION '{s3_destination_url}';
            INSERT OVERWRITE TABLE {db_name}.{table_name}_raw SELECT * FROM {db_name}.{external_table_name};
            DROP TABLE IF EXISTS {db_name}.{external_table_name}
        """
        hive_service.execute_sql_statement_with_interpolation(sql_statement=sql_statement)

        # Create raw expended table (multi-columns) and populate expended managed table
        interpolation_dict = {"#{hivevar:auditlog_database}": configuration.transition_db_name,
                              "#{hivevar:date_underscore}": export_date.replace("-", "_"),
                              "#{hivevar:date_hyphen}": export_date,
                              "#{hivevar:serde}": "org.openx.data.jsonserde.JsonSerDe",
                              "#{hivevar:data_location}": s3_destination_url,
                              }
        hive_service.execute_sql_statement_with_interpolation(file=path.join(sql_file_base_location,
                                                                             "auditlog_external_table.sql"),
                                                              interpolation_dict=interpolation_dict)

        # Create secured view-like table
        sec_v_location = f"s3://{configuration.configuration_file.s3_published_bucket}/data/uc/auditlog_sec_v/"
        interpolation_dict = {"#{hivevar:uc_database}": configuration.db_name,
                              "#{hivevar:location_str}": sec_v_location}
        hive_service.execute_sql_statement_with_interpolation(
            file=path.join(sql_file_base_location, "create_auditlog_sec_v.sql"),
            interpolation_dict=interpolation_dict)

        # Alter secured view-like table
        with open(path.join(sql_file_base_location, "auditlog_sec_v_columns.txt"), "r") as fd:
            sec_v_columns = fd.read().strip('\n')
            interpolation_dict = {"#{hivevar:uc_database}": configuration.db_name,
                                  "#{hivevar:date_hyphen}": export_date,
                                  "#{hivevar:uc_dw_auditlog_database}": configuration.transition_db_name,
                                  "#{hivevar:auditlog_sec_v_columns}": sec_v_columns,
                                  "#{hivevar:location_str}": sec_v_location,
                                  }
            hive_service.execute_sql_statement_with_interpolation(
                file=path.join(sql_file_base_location, "alter_add_part_auditlog_sec_v.sql"),
                interpolation_dict=interpolation_dict)

        # Create redacted view-like table
        red_v_location = f"s3://{configuration.configuration_file.s3_published_bucket}/data/uc/auditlog_red_v/"
        interpolation_dict = {"#{hivevar:uc_database}": configuration.db_name,
                              "#{hivevar:location_str}": red_v_location}
        hive_service.execute_sql_statement_with_interpolation(
            file=path.join(sql_file_base_location, "create_auditlog_red_v.sql"),
            interpolation_dict=interpolation_dict)

        # Alter redacted view-like table
        with open(path.join(sql_file_base_location, "auditlog_red_v_columns.txt"), "r") as fd:
            red_v_columns = fd.read().strip('\n')
            interpolation_dict = {"#{hivevar:uc_database}": configuration.db_name,
                                  "#{hivevar:date_hyphen}": export_date,
                                  "#{hivevar:uc_dw_auditlog_database}": configuration.transition_db_name,
                                  "#{hivevar:auditlog_red_v_columns}": red_v_columns,
                                  "#{hivevar:location_str}": red_v_location,
                                  }
            hive_service.execute_sql_statement_with_interpolation(
                file=path.join(sql_file_base_location, "alter_add_part_auditlog_red_v.sql"),
                interpolation_dict=interpolation_dict)

    except Exception as err:
        logger.error(repr(err))
        sys.exit(-1)


if __name__ == "__main__":
    main()
