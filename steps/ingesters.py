import datetime as dt
import json
import logging
from os import path

import boto3
from boto3.dynamodb.conditions import Attr
from pyspark.sql.functions import row_number, col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.window import Window

from data import UCMessage, Configuration
from dynamodb import DynamoDBHelper
from utils import Utils

logger = logging.getLogger("ingesters")


class BaseIngester:
    def __init__(self, configuration: Configuration, spark_session, hive_session, dynamodb_helper: DynamoDBHelper):
        self._configuration = configuration
        self._collection_name = configuration.collection_name
        self._spark_session = spark_session
        self._hive_session = hive_session
        self.dynamodb_helper = dynamodb_helper
        self.daily_output_prefix = None

    def read_dir(self, file_path):
        return self._spark_session.sparkContext.textFile(file_path)

    def run(self):
        raise NotImplementedError

    def update(self):
        raise NotImplementedError

    def execute_hive_statements(self):
        raise NotImplementedError

    # Empty S3 destination prefix before publishing
    @staticmethod
    def empty_s3_prefix(bucket, prefix) -> None:
        s3_resource = boto3.resource("s3")
        bucket = s3_resource.Bucket(bucket)
        bucket.objects.filter(Prefix=prefix).delete()


class BusinessAuditIngester(BaseIngester):
    def __init__(self, configuration, spark_session, hive_session, dynamodb_helper):
        super().__init__(configuration, spark_session, hive_session, dynamodb_helper)
        self.intermediate_db_name = "uc_dw_auditlog"
        self.user_db_name = "uc"

    # Processes and publishes data
    def decrypt_and_process(self):
        correlation_id = self._configuration.correlation_id
        prefix_date = (dt.datetime.strptime(self._configuration.export_date, "%Y-%m-%d") - dt.timedelta(days=1)).strftime("%Y-%m-%d")
        collection_name = self._collection_name

        corporate_bucket = self._configuration.configuration_file.s3_corporate_bucket
        source_prefix = path.join(
            self._configuration.source_s3_prefix,
            *prefix_date.split("-"),
            *collection_name.split(":"),
        )

        published_bucket = self._configuration.configuration_file.s3_published_bucket
        destination_prefix = path.join(
            self._configuration.destination_s3_prefix.lstrip("/"),
            self._configuration.export_date,
            *collection_name.split(":"),
        )
        self.destination_prefix = destination_prefix

        # define source and destination s3 URIs
        s3_source_url = "s3://{bucket}/{prefix}".format(bucket=corporate_bucket, prefix=source_prefix.lstrip("/"))
        s3_destination_url = "s3://{bucket}/{prefix}".format(bucket=published_bucket, prefix=destination_prefix)

        # begin processing
        try:
            dks_hit_accumulator = self._spark_session.sparkContext.accumulator(0)
            dks_miss_accumulator = self._spark_session.sparkContext.accumulator(0)

            logger.info(f"Instantiating decryption helper")
            decryption_helper = Utils.get_decryption_helper(
                decrypt_endpoint=self._configuration.configuration_file.dks_decrypt_endpoint,
                correlation_id=correlation_id,
                dks_hit_acc=dks_hit_accumulator,
                dks_miss_acc=dks_miss_accumulator,
            )

            logger.info(f"Emptying destination prefix: '{destination_prefix}'")
            self.empty_s3_prefix(bucket=published_bucket, prefix=destination_prefix)

            # empty dict sent to each container for caching
            dks_key_cache = {}

            # Persist records to JSONL in S3
            logger.info("starting pyspark processing")
            (
                self.read_dir(s3_source_url)
                    .map(lambda x: UCMessage(x, collection_name))
                    .map(lambda uc_message: decryption_helper.decrypt_dbObject(uc_message, dks_key_cache))
                    .map(UCMessage.transform)
                    .map(UCMessage.validate)
                    .map(UCMessage.sanitise)
                    .map(lambda x: x.utf8_decrypted_record)
                    .saveAsTextFile(
                    s3_destination_url,
                    compressionCodecClass="com.hadoop.compression.lzo.LzopCodec",
                )
            )
            logger.info("Initial pyspark ingestion completed")

            # stats for logging
            dks_hits = dks_hit_accumulator.value
            dks_misses = dks_miss_accumulator.value

            logger.info(f"DKS Hits: {dks_hits}")
            logger.info(f"DKS Misses: {dks_misses}")

        except Exception as err:
            logger.error(
                f"""Unexpected error occurred processing collection named {self._collection_name} """
                f""" for correlation id: {correlation_id} "{str(err)}" """
            )
            raise

    def run(self):
        self.decrypt_and_process()
        self.execute_hive_statements()

    def execute_hive_statements(self):
        published_bucket = self._configuration.configuration_file.s3_published_bucket
        destination_prefix = self.destination_prefix

        # define source and destination s3 URIs
        s3_destination_url = "s3://{bucket}/{prefix}".format(bucket=published_bucket, prefix=destination_prefix)

        logger.info("Starting post-processing for businessAudit")
        configuration = self._configuration
        hive_session = self._hive_session

        hive_session.create_database_if_not_exist(self.intermediate_db_name)
        hive_session.create_database_if_not_exist(self.user_db_name)

        # Declare parameters for audit logs processing
        sql_file_base_location = "/opt/emr/audit_sql/"
        db_name = self.intermediate_db_name
        table_name = "auditlog"
        export_date = configuration.export_date

        # Create raw managed table (two columns)
        sql_statement = f"""
                CREATE TABLE IF NOT EXISTS {db_name}.auditlog_raw (val STRING)
                PARTITIONED BY (date_str STRING) STORED
                AS orc TBLPROPERTIES ('orc.compress'='ZLIB')
            """
        hive_session.execute_sql_statement_with_interpolation(
            sql_statement=sql_statement
        )

        # Create expanded managed table (multi-columns)
        interpolation_dict = {
            "#{hivevar:auditlog_database}": self.intermediate_db_name
        }
        hive_session.execute_sql_statement_with_interpolation(
            file=path.join(sql_file_base_location, "auditlog_managed_table.sql"),
            interpolation_dict=interpolation_dict,
        )

        # Create raw external table (two columns) and populate raw managed table
        external_table_name = (
            f"auditlog_raw_{configuration.export_date.replace('-', '_')}"
        )
        sql_statement = f"""
                DROP TABLE IF EXISTS {db_name}.{external_table_name};
                CREATE EXTERNAL TABLE {db_name}.{external_table_name} (val STRING) PARTITIONED BY (date_str STRING) STORED AS TEXTFILE LOCATION '{s3_destination_url}';
                ALTER TABLE {db_name}.{external_table_name} ADD IF NOT EXISTS PARTITION(date_str='{export_date}') LOCATION '{s3_destination_url}';
                INSERT OVERWRITE TABLE {db_name}.{table_name}_raw SELECT * FROM {db_name}.{external_table_name};
                DROP TABLE IF EXISTS {db_name}.{external_table_name}
            """
        hive_session.execute_sql_statement_with_interpolation(
            sql_statement=sql_statement
        )

        # Create raw expended table (multi-columns) and populate expended managed table
        interpolation_dict = {
            "#{hivevar:auditlog_database}": self.intermediate_db_name,
            "#{hivevar:date_underscore}": export_date.replace("-", "_"),
            "#{hivevar:date_hyphen}": export_date,
            "#{hivevar:serde}": "org.openx.data.jsonserde.JsonSerDe",
            "#{hivevar:data_location}": s3_destination_url,
        }
        hive_session.execute_sql_statement_with_interpolation(
            file=path.join(sql_file_base_location, "auditlog_external_table.sql"),
            interpolation_dict=interpolation_dict,
        )

        # Create secured view-like table
        sec_v_location = f"s3://{configuration.configuration_file.s3_published_bucket}/data/uc/auditlog_sec_v/"
        interpolation_dict = {
            "#{hivevar:uc_database}": self.user_db_name,
            "#{hivevar:location_str}": sec_v_location,
        }
        hive_session.execute_sql_statement_with_interpolation(
            file=path.join(sql_file_base_location, "create_auditlog_sec_v.sql"),
            interpolation_dict=interpolation_dict,
        )

        # Alter secured view-like table
        with open(
            path.join(sql_file_base_location, "auditlog_sec_v_columns.txt"), "r"
        ) as fd:
            sec_v_columns = fd.read().strip("\n")
            interpolation_dict = {
                "#{hivevar:uc_database}": self.user_db_name,
                "#{hivevar:date_hyphen}": export_date,
                "#{hivevar:uc_dw_auditlog_database}": self.intermediate_db_name,
                "#{hivevar:auditlog_sec_v_columns}": sec_v_columns,
                "#{hivevar:location_str}": sec_v_location,
            }
            hive_session.execute_sql_statement_with_interpolation(
                file=path.join(
                    sql_file_base_location, "alter_add_part_auditlog_sec_v.sql"
                ),
                interpolation_dict=interpolation_dict,
            )

        # Create redacted view-like table
        red_v_location = f"s3://{configuration.configuration_file.s3_published_bucket}/data/uc/auditlog_red_v/"
        interpolation_dict = {
            "#{hivevar:uc_database}": self.user_db_name,
            "#{hivevar:location_str}": red_v_location,
        }
        hive_session.execute_sql_statement_with_interpolation(
            file=path.join(sql_file_base_location, "create_auditlog_red_v.sql"),
            interpolation_dict=interpolation_dict,
        )

        # Alter redacted view-like table
        with open(
            path.join(sql_file_base_location, "auditlog_red_v_columns.txt"), "r"
        ) as fd:
            red_v_columns = fd.read().strip("\n")
            interpolation_dict = {
                "#{hivevar:uc_database}": self.user_db_name,
                "#{hivevar:date_hyphen}": export_date,
                "#{hivevar:uc_dw_auditlog_database}": self.intermediate_db_name,
                "#{hivevar:auditlog_red_v_columns}": red_v_columns,
                "#{hivevar:location_str}": red_v_location,
            }
            hive_session.execute_sql_statement_with_interpolation(
                file=path.join(
                    sql_file_base_location, "alter_add_part_auditlog_red_v.sql"
                ),
                interpolation_dict=interpolation_dict,
            )


class CalculationPartsIngester(BaseIngester):
    def run(self):
        self.decrypt_and_process()
        if self._configuration.force_collection_update:
            self.update()

    def update(self):
        # Retrieves latest  CDI export entry from dynamodb
        latest_cdi_export_dynamodb_entry = None
        dynamodb = boto3.resource("dynamodb")
        table = dynamodb.Table("data_pipeline_metadata")
        response = table.scan(
            FilterExpression=(
                Attr("DataProduct").contains("CDI")
                & Attr("S3_Prefix_CDI_Export").exists()
                & Attr("Status").eq("COMPLETED")
            )
        )

        latest_date, latest_index, buffer = None, None, None
        for index, item in enumerate(response["Items"]):
            try:
                buffer = dt.datetime.strptime(item["Date"], "%Y-%m-%d")
            except ValueError as e:
                print(f"error: {str(e)}")
            if latest_date is None or buffer > latest_date:
                latest_date, latest_index = buffer, index

        if response["Items"]:
            # DynamoDB to provide date and path for latest CDI export
            latest_cdi_export_dynamodb_entry = response["Items"][latest_index]
            latest_cdi_export_s3_prefix = latest_cdi_export_dynamodb_entry["S3_Prefix_CDI_Export"]
            latest_cdi_export_date = dt.datetime.strptime(latest_cdi_export_dynamodb_entry["Date"], "%Y-%m-%d")
        else:
            # raise ValueError("Could not find a CDI export to update")
            latest_cdi_export_s3_prefix = "corporate_data_ingestion/exports/calculator/calculationParts/2023-05-17/"
            latest_cdi_export_date = dt.datetime.strptime("2023-05-17", "%Y-%m-%d")

        published_bucket = self._configuration.configuration_file.s3_published_bucket

        daily_output_prefix = path.join(
            self._configuration.destination_s3_prefix.lstrip("/"),
            self._configuration.db_name,
            self._configuration.table_name,
        )
        daily_output_url = "s3://{bucket}/{prefix}".format(bucket=published_bucket, prefix=daily_output_prefix)

        latest_cdi_export_s3_url = path.join("s3://", published_bucket, latest_cdi_export_s3_prefix)

        export_output_prefix = path.join(
            "corporate_data_ingestion/exports/calculator/calculationParts/", f"{self._configuration.export_date}/"
        )
        export_output_url = path.join(f"s3://{published_bucket}", export_output_prefix)

        self.dynamodb_helper.update_status(
            status=self.dynamodb_helper.IN_PROGRESS,
            export_date=self._configuration.export_date,
            extra={"S3_Prefix_CDI_Export": {"S": export_output_prefix}},
        )

        schema_dailies = StructType(
            [
                StructField("id", StringType(), nullable=False),
                StructField("id_part", StringType(), nullable=False),
                StructField("export_year", IntegerType(), nullable=False),
                StructField("export_month", IntegerType(), nullable=False),
                StructField("export_day", IntegerType(), nullable=False),
                StructField("db_type", StringType(), nullable=False),
                StructField("val", StringType(), nullable=False),
            ]
        )

        schema_cdi_output = StructType(
            [
                StructField("id", StringType(), nullable=False),
                StructField("id_part", StringType(), nullable=False),
                StructField("db_type", StringType(), nullable=False),
                StructField("val", StringType(), nullable=False),
            ]
        )

        # Read daily data since last export
        df_dailies = (
            self._spark_session.read.schema(schema_dailies)
            .orc(daily_output_url)
            .filter(
                (col("export_year") > latest_cdi_export_date.year)
                & (col("export_month") > latest_cdi_export_date.month)
                & (col("export_day") > latest_cdi_export_date.day)
            )
            .select(col("id"), col("id_part"), col("db_type"), col("val"))
        )

        # read most recent export
        df_cdi_output = self._spark_session.read.schema(schema_cdi_output).orc(latest_cdi_export_s3_url)

        # Union and find latest record for each ID
        window_spec = Window.partitionBy("id_part", "id_key").orderBy("db_type")
        (
            df_cdi_output.union(df_dailies)
            .repartitionByRange(
                4096, "id_part", "id_key"
            )  # todo: remove number of partitions and influence via spark config
            .withColumn("row_number", row_number().over(window_spec))
            .filter(df_cdi_output.row_number == 1)
            .write.partitionBy("id_part")
            .orc(export_output_url, mode="overwrite", compression="zlib")
        )

    def decrypt_and_process(self):
        correlation_id = self._configuration.correlation_id
        export_date = self._configuration.export_date
        prefix_date = (dt.datetime.strptime(export_date, "%Y-%m-%d") - dt.timedelta(days=1)).strftime("%Y-%m-%d")
        collection_name = self._collection_name

        corporate_bucket = self._configuration.configuration_file.s3_corporate_bucket
        source_prefix = path.join(
            self._configuration.source_s3_prefix,
            *prefix_date.split("-"),
            self._configuration.db_name,
            self._configuration.table_name,
        )

        daily_output_prefix = path.join(
            # Overridden until BaseIngester is updated
            "corporate_data_ingestion/orc/daily/",
            self._configuration.db_name,
            self._configuration.table_name,
        )
        self.daily_output_prefix = daily_output_prefix
        published_bucket = self._configuration.configuration_file.s3_published_bucket

        s3_source_url = "s3://{bucket}/{prefix}".format(bucket=corporate_bucket, prefix=source_prefix.lstrip("/"))
        daily_output_url = "s3://{bucket}/{prefix}".format(bucket=published_bucket, prefix=daily_output_prefix)

        # begin processing
        try:
            dks_hit_accumulator = self._spark_session.sparkContext.accumulator(0)
            dks_miss_accumulator = self._spark_session.sparkContext.accumulator(0)

            logger.info(f"Instantiating decryption helper")
            decryption_helper = Utils.get_decryption_helper(
                decrypt_endpoint=self._configuration.configuration_file.dks_decrypt_endpoint,
                correlation_id=correlation_id,
                dks_hit_acc=dks_hit_accumulator,
                dks_miss_acc=dks_miss_accumulator,
            )

            # empty dict sent to each container for caching
            dks_key_cache = {}

            def to_row(x: UCMessage):
                id_str = x.id
                id_json = json.loads(id_str)
                id_part = id_json.get("id")[:2]
                export_date_list = export_date.split("-")
                export_year = export_date_list[0]
                export_month = export_date_list[1]
                export_day = export_date_list[2]

                return (
                    id_str,
                    id_part,
                    int(export_year),
                    int(export_month),
                    int(export_day),
                    "INSERT" if not x.is_delete else "DELETE",
                    x.utf8_decrypted_record,
                )

            # Persist records to JSONL in S3
            self._spark_session.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
            logger.info("starting pyspark processing")
            pyspark_df = (
                self.read_dir(s3_source_url)
                .map(lambda x: UCMessage(x, collection_name))
                .map(lambda uc_message: decryption_helper.decrypt_dbObject(uc_message, dks_key_cache))
                .map(UCMessage.validate)
                .map(UCMessage.sanitise)
                .map(to_row)
                .toDF(["id", "id_part", "export_year", "export_month", "export_day", "db_type", "val"])
                .repartitionByRange("id_part", "id")
                .sortWithinPartitions("id")
                .write.partitionBy("export_year", "export_month", "export_day", "id_part")
                .orc(daily_output_url, mode="overwrite", compression="zlib")
            )
            logger.info("Initial pyspark ingestion completed")

            # stats for logging
            dks_hits = dks_hit_accumulator.value
            dks_misses = dks_miss_accumulator.value

            logger.info(f"DKS Hits: {dks_hits}")
            logger.info(f"DKS Misses: {dks_misses}")

            return pyspark_df

        except Exception as err:
            logger.error(
                f"""Unexpected error occurred processing collection named {self._collection_name} """
                f""" for correlation id: {correlation_id} "{str(err)}" """
            )
            raise
