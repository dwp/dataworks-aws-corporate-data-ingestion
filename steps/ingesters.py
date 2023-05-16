import logging
from os import path

import boto3
import json
import datetime as dt

from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, from_json

from data import UCMessage, Configuration
from utils import Utils

logger = logging.getLogger("ingesters")


class BaseIngester:
    def __init__(self, configuration, collection_name, spark_session, hive_session):
        self._configuration = configuration
        self._collection_name = collection_name
        self._spark_session = spark_session
        self._hive_session = hive_session
        self.destination_prefix = None

    def read_dir(self, file_path):
        return self._spark_session.sparkContext.textFile(file_path)

    def run(self):
        self.decrypt_and_process()

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
            self.empty_s3_prefix(published_bucket=published_bucket, prefix=destination_prefix)

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

    def execute_hive_statements(self):
        raise NotImplementedError

    # Empty S3 destination prefix before publishing
    @staticmethod
    def empty_s3_prefix(published_bucket, prefix) -> None:
        s3_resource = boto3.resource("s3")
        bucket = s3_resource.Bucket(published_bucket)
        bucket.objects.filter(Prefix=prefix).delete()


class BusinessAuditIngester(BaseIngester):
    def __init__(self, configuration, collection_name, spark_session, hive_session):
        super().__init__(configuration, collection_name, spark_session, hive_session)
        self.intermediate_db_name = "uc_dw_auditlog"
        self.user_db_name = "uc"

    def run(self):
        super(BusinessAuditIngester, self).run()
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
            self.empty_s3_prefix(published_bucket=published_bucket, prefix=destination_prefix)

            # empty dict sent to each container for caching
            dks_key_cache = {}

            def to_row(x: UCMessage):
                id_str = x.id
                id_json = json.loads(id_str)
                id_m = f"{id_json.get('id')}_{id_json.get('type')}"
                id_part = id_json.get('id')[:2]

                return (
                    id_str,
                    id_m,
                    id_part,
                    "INSERT" if not x.is_delete else "DELETE",
                    x.utf8_decrypted_record,
                )

            # Persist records to JSONL in S3
            logger.info("starting pyspark processing")
            (
                self.read_dir(s3_source_url)
                .map(lambda x: UCMessage(x, collection_name))
                .map(lambda uc_message: decryption_helper.decrypt_dbObject(uc_message, dks_key_cache))
                .map(UCMessage.validate)
                .map(UCMessage.sanitise)
                .map(to_row)
                .toDF(['id', 'id_m', 'id_part', 'dbtype', 'val'])
                .repartition("dbtype", "id_part")
                .sort("id")
                .write.partitionBy("dbtype", "id_part").orc(s3_destination_url, mode="overwrite", compression="zlib")
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


class CalcPartBenchmark:
    # Processes and publishes data
    def run(self):
        raise NotImplementedError

    def __init__(self, configuration: Configuration, collection_name, spark_session, hive_session):
        self._configuration = configuration
        self._collection_name = collection_name
        self._spark_session = spark_session
        self._hive_session = hive_session
        self.destination_prefix = None

    @staticmethod
    def empty_s3_prefix(published_bucket, prefix) -> None:
        s3_resource = boto3.resource("s3")
        bucket = s3_resource.Bucket(published_bucket)
        bucket.objects.filter(Prefix=prefix).delete()

    def read_dir(self, file_path):
        return self._spark_session.sparkContext.textFile(file_path)

    def merge_snapshot_dedupe(self):
        configuration = self._configuration
        self.empty_s3_prefix(
            published_bucket=configuration.configuration_file.s3_published_bucket,
            prefix="corporate_data_ingestion/calculation_parts/full_merge_2/"
        )

        snapshot_location = "s3://{bucket}/{prefix}".format(
            bucket=configuration.configuration_file.s3_published_bucket,
            prefix="corporate_data_ingestion/calculation_parts/snapshot/"
        )
        daily_deduped_prefix = "s3://{bucket}/{prefix}".format(
            bucket=configuration.configuration_file.s3_published_bucket,
            prefix="corporate_data_ingestion/calculation_parts/combined_daily_data_dedup/"
        )
        output_prefix = "s3://{bucket}/{prefix}".format(
            bucket=configuration.configuration_file.s3_published_bucket,
            prefix="corporate_data_ingestion/calculation_parts/full_merge_2/"
        )

        snapshot_schema = StructType([
            StructField("id_key", StringType(), nullable=False),
            StructField("dbType", StringType(), nullable=False),
            StructField("json", StringType(), nullable=False),
            StructField("id_part", StringType(), nullable=False),
        ])

        daily_schema = StructType([
            StructField("id_key", StringType(), nullable=False),
            StructField("dbType", StringType(), nullable=False),
            StructField("json", StringType(), nullable=False),
            StructField("row_number", IntegerType(), nullable=False),
            StructField("id_part", StringType(), nullable=False),
        ])

        snapshot_df = (
            self._spark_session.read.schema(snapshot_schema).orc(snapshot_location)
            .select("id_key", "id_part", "dbType", "json")
        )

        deduped_daily_df = (
            self._spark_session.read.schema(daily_schema).orc(daily_deduped_prefix)
            .select("id_key", "id_part", "dbType", "json")
        )

        window_spec = Window.partitionBy("id_part", "id_key").orderBy("dbType")
        combined_df = (
            snapshot_df.union(deduped_daily_df)
            .repartitionByRange(4096, "id_part", "id_key")
            .withColumn("row_number", row_number().over(window_spec))
        )

        (
            combined_df
            .filter(combined_df.row_number == 1)
            .write.partitionBy("id_part")
            .orc(output_prefix, mode="overwrite", compression="zlib")
        )

    def dedup_monthly(self):
        configuration = self._configuration
        # export_date = configuration.export_date  # format "2022-10-01"
        source_prefix = "corporate_data_ingestion/calculation_parts/combined_daily_data_october_to_march/"
        dest_prefix = "corporate_data_ingestion/calculation_parts/combined_daily_data_dedup/"

        s3_source_url = "s3://{bucket}/{prefix}".format(
            bucket=configuration.configuration_file.s3_published_bucket,
            prefix=source_prefix,
        )
        s3_destination_url = "s3://{bucket}/{prefix}".format(
            bucket=configuration.configuration_file.s3_published_bucket,
            prefix=dest_prefix,
        )

        logger.warning(f"Emptying prefix: {dest_prefix}")
        self.empty_s3_prefix(configuration.configuration_file.s3_published_bucket, dest_prefix)

        logger.info("starting pyspark processing")

        schema = StructType([
            StructField("id_key", StringType(), nullable=False),
            StructField("id_part", StringType(), nullable=False),
            StructField("dbType", StringType(), nullable=False),
            StructField("json", StringType(), nullable=False),
        ])

        window_spec = Window.partitionBy("id_part", "id_key").orderBy("dbType")

        df = self._spark_session.read.schema(schema).orc(s3_source_url) \
            .repartition("id_part") \
            .withColumn("row_number", row_number().over(window_spec))

        df.filter(df.row_number == 1).write.partitionBy("id_part").orc(
            s3_destination_url,
            mode="overwrite",
            compression="zlib"
        )

    def append_daily(self):
        configuration = self._configuration
        export_date = configuration.export_date  # format "2022-10-01"
        dest_prefix = "corporate_data_ingestion/calculation_parts/combined_daily_data_october_to_march/"

        s3_source_url = "s3://{bucket}/{prefix}/{export_date}/{collection}".format(
            bucket=configuration.configuration_file.s3_published_bucket,
            prefix="corporate_data_ingestion/json/daily",
            export_date=export_date,
            collection="calculator/calculationParts"
        )
        s3_destination_url = "s3://{bucket}/{prefix}".format(
            bucket=configuration.configuration_file.s3_published_bucket,
            prefix=dest_prefix,
        )

        if export_date == "2022-10-01":
            logger.warning(f"Emptying prefix: {dest_prefix}")
            self.empty_s3_prefix(configuration.configuration_file.s3_published_bucket, dest_prefix)

        logger.info("starting pyspark processing")

        schema = StructType([
            StructField("id", StringType(), nullable=False),
            StructField("id_m", StringType(), nullable=False),
            StructField("val", StringType(), nullable=False),
            StructField("dbtype", StringType(), nullable=False),
            StructField("id_part", StringType(), nullable=False),
        ])

        df = self._spark_session.read.schema(schema).orc(s3_source_url) \
            .withColumnRenamed("id_m", "id_key") \
            .withColumnRenamed("dbtype", "dbType") \
            .withColumnRenamed("val", "json") \
            .select("id_key", "id_part", "dbType", "json") \
            .repartition("id_part").sortWithinPartitions("id_key")

        df.write.partitionBy("id_part").orc(s3_destination_url, mode="append", compression="zlib")

    def ingest_snapshot(self):
        configuration = self._configuration

        logger.info("starting pyspark processing")
        s3_source_url = "s3://{bucket}/{prefix}".format(bucket=configuration.configuration_file.s3_published_bucket,
                                                        prefix="analytical-dataset/archive/11_2022_backup/calculationParts/")

        s3_destination_url = "s3://{bucket}/{prefix}".format(bucket=configuration.configuration_file.s3_published_bucket,
                                                             prefix="corporate_data_ingestion/calculation_parts/snapshot/")

        df = self.read_dir(s3_source_url)
        (
            df.map(json.loads)
            .map(lambda x: (f'{x.get("_id").get("id")}_{x.get("_id").get("type")}',
                            f'{x.get("_id").get("id")}'[0:2],
                            "INSERT" if x.get("_removedDateTime") is None else "DELETE",
                            json.dumps(x, ensure_ascii=False, separators=(',', ':'))
                            ))
            .toDF(["id_key", "id_part", "dbType", "json"])
            .repartition("id_part")
            .sortWithinPartitions("id_key")
            .write.partitionBy("id_part").orc(s3_destination_url, mode="overwrite", compression="zlib")
        )

    def publish_calculation_parts_textfile(self):
        configuration = self._configuration

        self.empty_s3_prefix(
            published_bucket=configuration.configuration_file.s3_published_bucket,
            prefix="corporate_data_ingestion/calculation_parts/230417/"
        )

        logger.info("starting pyspark processing")
        s3_source_url = "s3://{bucket}/{prefix}".format(
            bucket=configuration.configuration_file.s3_published_bucket,
            prefix="corporate_data_ingestion/calculation_parts/full_merge_2/"
        )

        s3_destination_url = "s3://{bucket}/{prefix}".format(
            bucket=configuration.configuration_file.s3_published_bucket,
            prefix="corporate_data_ingestion/calculation_parts/230417/attempt_1/"
        )
        schema = StructType([
            StructField("id_key", StringType(), nullable=False),
            StructField("dbType", StringType(), nullable=False),
            StructField("json", StringType(), nullable=False),
            StructField("row_number", IntegerType(), nullable=False),
            StructField("id_part", StringType(), nullable=False),
        ])

        df = self._spark_session.read.schema(schema).orc(s3_source_url)
        df.rdd.map(lambda x: x["json"]).repartition(8192).saveAsTextFile(
            s3_destination_url,
            compressionCodecClass="com.hadoop.compression.lzo.LzopCodec"
        )

    def publish_calculation_parts_to_table(self, table: str, ddl: str):
        snapshot_location = "s3://{bucket}/{prefix}".format(
            bucket=self._configuration.configuration_file.s3_published_bucket,
            prefix="corporate_data_ingestion/calculation_parts/full_merge_2/",
        )

        logger.info("starting pyspark processing")

        schema = StructType([
            StructField("id_key", StringType(), nullable=False),
            StructField("dbType", StringType(), nullable=False),
            StructField("json", StringType(), nullable=False),
            StructField("row_number", IntegerType(), nullable=False),
            StructField("id_part", StringType(), nullable=False),
        ])

        with open(f"/opt/emr/calculation_parts_ddl/{ddl}", "r") as f:
            json_schema = f.read()

        df = self._spark_session.read.schema(schema).orc(snapshot_location)
        (
            df
            .select(from_json("json", json_schema).alias("json"), "id_part", "id_key")
            .repartitionByRange(1024, "id_part", "id_key").select("json.*")
            .write.format("orc").mode("overwrite").saveAsTable(f"dwx_audit_transition.{table}")
        )


class CalculationPartsDeduplicate(CalcPartBenchmark):
    def run(self):
        self.dedup_monthly()


class CalculationPartsAppend(CalcPartBenchmark):
    def run(self):
        self.append_daily()


class CalculationPartsMergeSnapshot(CalcPartBenchmark):
    def run(self):
        self.merge_snapshot_dedupe()


class CalculationPartsPublishCalculatorParts(CalcPartBenchmark):
    def run(self):
        self.publish_calculation_parts_to_table(
            table="src_calculator_parts",
            ddl="src_calculator_parts_ddl",
        )


class CalculationPartsPublishHousingCalculator(CalcPartBenchmark):
    def run(self):
        self.publish_calculation_parts_to_table(
            table="src_calculator_calculationparts_housing_calculation",
            ddl="src_calculator_calculationparts_housing_calculation_ddl",
        )


class CalculationPartsPublishChildcareEntitlement(CalcPartBenchmark):
    def run(self):
        self.publish_calculation_parts_to_table(
            table="src_childcare_entitlement",
            ddl="src_childcare_entitlement_ddl"
        )
