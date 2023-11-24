import datetime as dt
import json
import logging
from os import path

import boto3
from boto3.dynamodb.conditions import Attr
from pyspark.sql import Row
from pyspark.sql.functions import count, row_number, col, from_json
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.window import Window
from pyspark.storagelevel import StorageLevel

from data import UCMessage, Configuration
from dynamodb import DynamoDBHelper
from utils import Utils

logger = logging.getLogger("ingesters")


class BaseIngester:
    DEFAULT_K2HB_S3_ROOT_PREFIX = "corporate_storage/ucfs_main"

    def __init__(self, configuration: Configuration, spark_session, hive_session, dynamodb_helper: DynamoDBHelper):
        self._configuration = configuration
        self._collection_name = configuration.collection_name
        self._spark_session = spark_session
        self._hive_session = hive_session
        self.dynamodb_helper = dynamodb_helper

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
    DEFAULT_K2HB_S3_ROOT_PREFIX = "corporate_storage/ucfs_audit/"
    DEFAULT_CDI_DAILY_PREFIX = "corporate_data_ingestion/json/daily/"

    def __init__(self, configuration, spark_session, hive_session, dynamodb_helper):
        super().__init__(configuration, spark_session, hive_session, dynamodb_helper)
        self.intermediate_db_name = "uc_dw_auditlog"
        self.user_db_name = "uc"
        self.destination_prefix = None

    # Processes and publishes data for audit log
    def decrypt_and_process(self):
        correlation_id = self._configuration.correlation_id
        prefix_date = (dt.datetime.strptime(self._configuration.export_date, "%Y-%m-%d") - dt.timedelta(days=1)).strftime("%Y-%m-%d")
        collection_name = self._collection_name

        corporate_bucket = self._configuration.configuration_file.s3_corporate_bucket
        # Use source prefix from configuration if set, otherwise use default
        source_prefix = self._configuration.source_s3_prefix or path.join(
            self.DEFAULT_K2HB_S3_ROOT_PREFIX,
            *prefix_date.split("-"),
            *collection_name.split(":"),
        )

        published_bucket = self._configuration.configuration_file.s3_published_bucket
        # Use destination prefix from configuration if set, otherwise use default
        destination_prefix = self._configuration.destination_s3_prefix or path.join(
            self.DEFAULT_CDI_DAILY_PREFIX,
            self._configuration.export_date,
            *collection_name.split(":"),
        )
        self.destination_prefix = destination_prefix

        # define source and destination s3 URIs
        s3_source_url = "s3://{bucket}/{prefix}".format(bucket=corporate_bucket, prefix=source_prefix.lstrip("/"))
        s3_destination_url = "s3://{bucket}/{prefix}".format(bucket=published_bucket, prefix=destination_prefix.lstrip("/"))

        # begin processing for audit log
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

    # audit log
    def run(self):
        self.decrypt_and_process()
        self.execute_hive_statements()

    # audit log
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
    DEFAULT_CDI_DAILY_PREFIX = "corporate_data_ingestion/orc/daily/"
    DEFAULT_CDI_EXPORT_PREFIX = "corporate_data_ingestion/exports/"

    def run(self):
        self.decrypt_and_process()

        if self._configuration.force_collection_update:
            self.update()
            self.export_to_hive_table()

        if self._configuration.force_export_to_hive:
            self.export_to_hive_table()

    # calc parts - write to uc labs tables
    # https://github.com/dwp/dataworks-aws-corporate-data-ingestion/blob/master/docs/data-engineering-summary.md#export-to-hive-table
    def export_to_hive_table(self):
        tables_to_publish = [
            {
                "table_name": "src_calculator_parts",
                "ddl": "src_calculator_parts_ddl"
            },
            {
                "table_name": "src_childcare_entitlement",
                "ddl": "src_childcare_entitlement_ddl"}
            ,
            {
                "table_name": "src_calculator_calculationparts_housing_calculation",
                "ddl": "src_calculator_calculationparts_housing_calculation_ddl"
            },
        ]

        # ensure tabled are dropped as there can be issues overwriting them with spark
        self._hive_session.execute_queries([f"DROP TABLE IF EXISTS uc_lab_staging.{item['table_name']}" for item in tables_to_publish])

        published_bucket = self._configuration.configuration_file.s3_published_bucket
        export_output_prefix = path.join(
            self.DEFAULT_CDI_EXPORT_PREFIX,
            "calculator/calculationParts/",
            self._configuration.export_date,
        )

        # full snapshot export location
        # eg s3://61dxx/corporate_data_ingestion/exports/calculator/calculationParts/
        export_output_url = path.join(f"s3://{published_bucket}", export_output_prefix)

        # schema of full snapshot
        schema_cdi_output = StructType(
            [
                StructField("id", StringType(), nullable=False),
                StructField("id_part", StringType(), nullable=False),
                StructField("db_type", StringType(), nullable=False),
                StructField("val", StringType(), nullable=False),
            ]
        )

        logger.info("Publishing calculationParts tables")
        logger.info(f"Export path: {export_output_url}")

        # read full snapshot into dataframe and persist it
        source_df = (
            self._spark_session
            .read
            .schema(schema_cdi_output)
            .orc(export_output_url)
            .persist(storageLevel=StorageLevel.DISK_ONLY)
        )

        #TODO handle hardcoded partition range 1024

        # gets the json schema for each table and write only that subset of json fields into orc format
        for table_dict in tables_to_publish:
            logger.info(f"Publishing table: {table_dict['table_name']}")
            with open(f"/opt/emr/calculation_parts_ddl/{table_dict['ddl']}", "r") as f:
                json_schema = f.read()

            (
                source_df
                .select(from_json("val", json_schema).alias("val"), "id_part", "id")
                .repartitionByRange(1024, "id_part", "id").select("val.*")
                .write.format("orc").mode("overwrite").saveAsTable(f"uc_lab_staging.{table_dict['table_name']}")
            )
            logger.info(f"Published table: {table_dict['table_name']}")

    # updates calc parts full snapshot using a range of days
    # https://github.com/dwp/dataworks-aws-corporate-data-ingestion/blob/master/docs/data-engineering-summary.md#update
    def update(self):

        #TODO decouple from dynamoDB

        # Retrieves latest  CDI export entry from dynamodb
        dynamodb = boto3.resource("dynamodb")
        table = dynamodb.Table("data_pipeline_metadata")
        response = table.scan(
            FilterExpression=(
                Attr("DataProduct").eq("CDI-calculator:calculationParts")
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
            #TODO raise ValueError("Could not find a CDI export to update")
            latest_cdi_export_s3_prefix = "corporate_data_ingestion/exports/calculator/calculationParts/2023-05-17/"
            latest_cdi_export_date = dt.datetime.strptime("2023-05-17", "%Y-%m-%d")

        # Use destination prefix from configuration if set, otherwise use default
        # << CalcParts uses date-partitioned storage, and therefore doesn't specify the export-date in the path
        daily_output_prefix = self._configuration.destination_s3_prefix or path.join(
            self.DEFAULT_CDI_DAILY_PREFIX,
            self._configuration.db_name,
            self._configuration.table_name,
        )
        published_bucket = self._configuration.configuration_file.s3_published_bucket

        # source of daily records
        # eg s3://61dxx/corporate_data_ingestion/orc/daily/calculator/calculationParts
        daily_output_url = "s3://{bucket}/{prefix}".format(bucket=published_bucket, prefix=daily_output_prefix.lstrip("/"))


        # Latest CDI export
        # snapshot_s3_prefix allows the source snapshot to be set in args
        latest_cdi_export_s3_url = path.join("s3://", published_bucket, latest_cdi_export_s3_prefix)
        export_output_prefix = self._configuration.snapshot_s3_prefix or path.join(
            self.DEFAULT_CDI_EXPORT_PREFIX,
            "calculator/calculationParts/",
            self._configuration.export_date,
        )

        # location of full snapshot exports
        # eg s3://61dxx/corporate_data_ingestion/exports/calculator/calculationParts/2023-06-30/
        export_output_url = path.join(f"s3://{published_bucket}", export_output_prefix)

        self.dynamodb_helper.update_status(
            status=self.dynamodb_helper.IN_PROGRESS,
            export_date=self._configuration.export_date,
            extra={"S3_Prefix_CDI_Export": {"Value": {"S": export_output_prefix}}},
        )

        # schema for uc decrypted dailies
        schema_dailies = StructType(
            [
                StructField("id", StringType(), nullable=False),
                StructField("db_type", StringType(), nullable=False),
                StructField("val", StringType(), nullable=False),
                StructField("export_year", IntegerType(), nullable=False),
                StructField("export_month", IntegerType(), nullable=False),
                StructField("export_day", IntegerType(), nullable=False),
                StructField("id_part", StringType(), nullable=False),
            ]
        )

        # schema for the full snapshot
        schema_cdi_output = StructType(
            [
                StructField("id", StringType(), nullable=False),
                StructField("db_type", StringType(), nullable=False),
                StructField("val", StringType(), nullable=False),
                StructField("id_part", StringType(), nullable=False),
            ]
        )

        logger.info("Starting Merge")
        logger.info(f"Most recent export dated: {latest_cdi_export_date}")
        logger.info(f"Most recent export prefix: {latest_cdi_export_s3_prefix}")
        logger.info(f"Partitioned daily data in prefix: {daily_output_prefix}")
        logger.info(f"Partitioned daily data filtered for: 'export date > {latest_cdi_export_date}'")
        logger.info(f"New export output prefix: {export_output_prefix}")
        cdi_year = latest_cdi_export_date.year
        cdi_month = latest_cdi_export_date.month
        cdi_day = latest_cdi_export_date.day

        # Read all daily folders in list
        # the default is all days since the last export date entry which is stored in audit database table
        df_dailies = (
            self._spark_session.read.schema(schema_dailies)
            .orc(daily_output_url)
            .filter((
                (col("export_year") > cdi_year)
                | ((col("export_year") == cdi_year) & (col("export_month") > cdi_month))
                | ((col("export_year") == cdi_year) & (col("export_month") == cdi_month) & (col("export_day") > cdi_day))
            ))
            .select(col("id"), col("db_type"), col("val"), col("id_part"))
        )

        # read most recent full snapshot
        df_cdi_output = (
            self._spark_session.read.schema(schema_cdi_output)
            .orc(latest_cdi_export_s3_url)
            .select(col("id"), col("db_type"), col("val"), col("id_part"))
         )

        # TODO remove number of partitions and influence via spark config for merge

        # union all dailies with the full snapshot then get latest record for each id
        # write in compressed orc format and partition by id_part (first 2 chars of id field)
        window_spec = Window.partitionBy("id_part", "id").orderBy("db_type")
        (
            df_cdi_output.union(df_dailies)
            .repartitionByRange(
                4096, "id_part", "id"
            )
            .withColumn("row_number", row_number().over(window_spec))
            .filter(col("row_number") == 1)
            .write.partitionBy("id_part")
            .orc(export_output_url, mode="overwrite", compression="zlib")
        )

        logger.info("Completed Merge")


        # validate that records exist for all days up to and including the latest_cdi_export_date
        # there should be 1000s for each day in each sample but there may be a bit of bleed near midnight where records
        # appear in next day folder so we set a minimum so these get ignored

        # how many days to check - can check back a few years just as easily as last month
        # and give better coverage
        day_sample_count = 30

        # minimum records per day sample
        minimum_records_per_day = 30

        # optimisation - the % of records for spark to sample
        percent_sample = 0.1

        # eg 2023-06-29
        latest_export_date_string = latest_cdi_export_date.strftime('%Y-%m-%d')

        # source folder for latest snapshot
        source_df = (
            spark.read.schema(schema_cdi_output).orc(export_output_url)
        )

        # minimal structure we need to apply to the json to get the fields we need
        json_schema = StructType(
            [
                StructField("_id", StructType([
                    StructField("id", StringType())
                    ,StructField("type", StringType())
                ]))
                    ,StructField("_lastModifiedDateTime", StructType([
                    StructField("d_date", StringType())
                ]))
            ]
        )

        # get the last modified date and id from the json field called val
        # optimisation - just use one partition for id_part
        json_df=source_df.filter(source_df.id_part == '00') \
            .select(from_json("val", json_schema).alias("val")).select("val.*")

        fractional_df = json_df.sample(fraction=percent_sample)

        # count records grouping by last modified date in reverse date order
        # agg with filter are the spark equivalent of the sql 'having count(*) > minimum_records_per_day'
        date_list = fractional_df.rdd.map(lambda x: Row(last_modified=x._lastModifiedDateTime.d_date[:10], _id=x._id)).toDF() \
            .groupBy('last_modified') \
            .agg(count('*').alias('count')) \
            .filter(col('count') >= minimum_records_per_day) \
            .orderBy(col("last_modified").desc()) \
            .take(day_sample_count)

        # in reverse order so latest is index 0
        # eg 2023-06-29
        latest_date_string = date_list[0].last_modified
        eariest_date_string = date_list[-1].last_modified

        date_difference = dt.datetime.strptime(latest_date_string, '%Y-%m-%d') - dt.datetime.strptime(eariest_date_string, '%Y-%m-%d')

        if day_sample_count != date_difference.days + 1:
            raise Exception(f"Days are missing - last {day_sample_count} days found go further back than expected to {eariest_date_string} from {latest_date_string}")

        if latest_date_string != latest_export_date_string:
            raise Exception(f"The latest date {latest_date_string} does not match {latest_export_date_string}")

        logger.info(f"The latest date found is {latest_export_date_string} which is correct and the last {day_sample_count} days are all present")


    # calc parts
    # https://github.com/dwp/dataworks-aws-corporate-data-ingestion/blob/master/docs/data-engineering-summary.md#decrypt-and-process-1
    def decrypt_and_process(self):
        correlation_id = self._configuration.correlation_id
        export_date = self._configuration.export_date
        prefix_date = (dt.datetime.strptime(export_date, "%Y-%m-%d") - dt.timedelta(days=1)).strftime("%Y-%m-%d")
        collection_name = self._collection_name

        corporate_bucket = self._configuration.configuration_file.s3_corporate_bucket
        # Use source prefix from configuration if set, otherwise use default
        source_prefix = self._configuration.source_s3_prefix or path.join(
            self.DEFAULT_K2HB_S3_ROOT_PREFIX,
            *prefix_date.split("-"),
            self._configuration.db_name,
            self._configuration.table_name,
        )

        # Use destination prefix from configuration if set, otherwise use default
        # << CalcParts uses date-partitioned storage, and therefore doesn't specify the export-date in the path
        daily_output_prefix = self._configuration.destination_s3_prefix or path.join(
            self.DEFAULT_CDI_DAILY_PREFIX,
            self._configuration.db_name,
            self._configuration.table_name,
        )
        published_bucket = self._configuration.configuration_file.s3_published_bucket

        # source of kafka uc encrypted data written by K2HB
        # eg s3://485xxx/corporate_storage/ucfs_main/2023/07/02/calculator/calculationParts/
        s3_source_url = "s3://{bucket}/{prefix}".format(bucket=corporate_bucket, prefix=source_prefix.lstrip("/"))

        # location for uc decrypted daily data in orc format partitioned by year/month/day/id_part
        # eg s3://61dxx/corporate_data_ingestion/orc/daily/calculator/calculationParts
        daily_output_url = "s3://{bucket}/{prefix}".format(bucket=published_bucket, prefix=daily_output_prefix.lstrip("/"))

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

            # write decrypted daily records in compressed ORC format
            # daily_output_url with partition prefixes looks like this
            # eg s3://61dxx/corporate_data_ingestion/orc/daily/calculator/calculationParts/export_year=2023/export_month=7/export_day=4/id_part=00
            # id - the guid from the calc parts collection _id.id eg f7420bec-1697-4589-b125-882a5976ca1e
            # id_part - first 2 characters of the id guid so range is 00-ff - used for partitioning, has equal spread over values
            # export_year/export_month/export_day - date to export dropping leading zeros
            # db_type either DELETE or INSERT for calc parts - usually come as a pair of records - DELETE is final state
            # val - the full json content
            # there are several optimisations here - see docs/data-engineering-summary.md#decrypt-and-process-1
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
                .write.option("partitionOverwriteMode", "dynamic")
                .partitionBy("export_year", "export_month", "export_day", "id_part")
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
