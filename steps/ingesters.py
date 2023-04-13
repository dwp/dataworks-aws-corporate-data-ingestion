import logging
from os import path

import boto3
import json
import datetime as dt

from data import UCMessage
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

    # Processes and publishes data
    def run(self):
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


class CalcPartBenchmark:
    def __init__(self, configuration, collection_name, spark_session, hive_session):
        self._configuration = configuration
        self._collection_name = collection_name
        self._spark_session = spark_session
        self._hive_session = hive_session
        self.destination_prefix = None

    def read_dir(self, file_path):
        return self._spark_session.sparkContext.textFile(file_path)

    def create_baseline_with_insert_only(self):
        """Processes the most recent ADG-based CalculationParts snapshot into an 'enriched' table with INSERT records only
        """
        hive_session = self._hive_session

        sql_statement = f"""
                    DROP TABLE IF EXISTS dwx_audit_transition.calc_parts_snapshot_insert_only_minified;
                    CREATE TABLE dwx_audit_transition.calc_parts_snapshot_insert_only_minified
                        (id_key STRING)
                        STORED AS orc TBLPROPERTIES ('orc.compress'='ZLIB');
                    INSERT INTO dwx_audit_transition.calc_parts_snapshot_insert_only_minified
                        SELECT id_key FROM dwx_audit_transition.calc_parts_snapshot_enriched_unpartitioned
                        WHERE dbType = 'INSERT';
                """
        hive_session.execute_sql_statement_with_interpolation(
            sql_statement=sql_statement
        )

    def create_baseline_with_delete_only(self):
        """Processes the most recent ADG-based CalculationParts snapshot into an 'enriched' table with DELETE records only
        """
        hive_session = self._hive_session

        sql_statement = f"""
                    DROP TABLE IF EXISTS dwx_audit_transition.calc_parts_snapshot_delete_only_minified;
                    CREATE TABLE dwx_audit_transition.calc_parts_snapshot_delete_only_minified
                        (id_key STRING)
                        STORED AS orc TBLPROPERTIES ('orc.compress'='ZLIB');
                    INSERT INTO dwx_audit_transition.calc_parts_snapshot_delete_only_minified
                        SELECT id_key FROM dwx_audit_transition.calc_parts_snapshot_enriched_unpartitioned
                        WHERE dbType = 'DELETE';
                """
        hive_session.execute_sql_statement_with_interpolation(
            sql_statement=sql_statement
        )

    def create_baseline(self):
        """Processes the most recent ADG-based CalculationParts snapshot into an 'enriched' table
        """
        configuration = self._configuration
        hive_session = self._hive_session

        sql_statement = f"""
                    DROP TABLE IF EXISTS dwx_audit_transition.calc_parts_snapshot_enriched_unpartitioned;
                    CREATE TABLE dwx_audit_transition.calc_parts_snapshot_enriched_unpartitioned (id_key STRING, dbType STRING, json STRING)
                    STORED AS orc TBLPROPERTIES ('orc.compress'='ZLIB');
                """
        hive_session.execute_sql_statement_with_interpolation(
            sql_statement=sql_statement
        )

        logger.info("starting pyspark processing")
        s3_source_url = "s3://{bucket}/{prefix}".format(bucket=configuration.configuration_file.s3_published_bucket,
                                                        prefix="corporate_data_ingestion/hive/external/dwx_audit_transition.db/calc_parts_snapshot_enriched_unpartitioned/")

        (
            self.read_dir(s3_source_url)
            .map(json.loads)
            .map(lambda x: (f'{x.get("_id").get("id")}_{x.get("_id").get("type")}',
                            "INSERT" if x.get("_removedDateTime") is None else "DELETE",
                            json.dumps(x, ensure_ascii=False, separators=(',', ':')))
                 )
            .toDF(["id_key", "dbType", "json"])
            .write.insertInto("dwx_audit_transition.calc_parts_snapshot_enriched_unpartitioned", overwrite=True)
        )

    def create_new_baseline(self):
        """Processes the most recent ADG-based CalculationParts snapshot into an 'enriched' table
        """
        configuration = self._configuration
        hive_session = self._hive_session

        sql_statement = f"""
                    DROP TABLE IF EXISTS dwx_audit_transition.calc_parts_snapshot;
                    CREATE TABLE dwx_audit_transition.calc_parts_snapshot (id_key STRING, json STRING) PARTITIONED BY (dbType STRING, id_part STRING)
                    STORED AS orc TBLPROPERTIES ('orc.compress'='ZLIB');
                """
        hive_session.execute_sql_statement_with_interpolation(
            sql_statement=sql_statement
        )

        logger.info("starting pyspark processing")
        s3_source_url = "s3://{bucket}/{prefix}".format(bucket=configuration.configuration_file.s3_published_bucket,
                                                        prefix="corporate_data_ingestion/hive/external/dwx_audit_transition.db/calc_parts_snapshot_enriched_unpartitioned/")

        df = self._spark_session.read.orc(s3_source_url)
        (
            df.coalesce(1000)
            .withColumn("id_part", df.id_key[0:2])
            .select("id_key", "json", "dbType", "id_part")
            .write.insertInto("dwx_audit_transition.calc_parts_snapshot", overwrite=True)
        )

    def benchmark_reconciliation(self):
        hive_session = self._hive_session

        # Create intermediate table
        create_tables = f"""
                    DROP TABLE IF EXISTS dwx_audit_transition.int_calc_parts_latest_unmatched;
                    CREATE TABLE dwx_audit_transition.int_calc_parts_latest_unmatched ( id_key string, dbtype STRING, json STRING);
                """
        hive_session.execute_sql_statement_with_interpolation(
            sql_statement=create_tables
        )

        # Get orphan INSERT records from full snapshot by checking against DELETE records from a batch of daily records
        statement = f"""
                TRUNCATE TABLE dwx_audit_transition.int_calc_parts_latest_unmatched;
                INSERT INTO dwx_audit_transition.int_calc_parts_latest_unmatched
                SELECT distinct i.id_key, i.dbtype, i.json
                FROM dwx_audit_transition.calc_parts_snapshot_enriched_insert_only i
                LEFT JOIN dwx_audit_transition.int_calc_parts_range_final d
                ON i.id_key = d.id_key
                AND d.id_key IS null;
        """
        hive_session.execute_sql_statement_with_interpolation(
            sql_statement=statement
        )

        # Empty and repopulate calc_parts_snapshot_enriched_insert_only with orphans INSERT exclusively
        statement = f"""
                TRUNCATE TABLE dwx_audit_transition.calc_parts_snapshot_enriched_insert_only;
                INSERT INTO dwx_audit_transition.calc_parts_snapshot_enriched_insert_only
                SELECT id_key, dbtype, json
                FROM dwx_audit_transition.int_calc_parts_latest_unmatched;
        """
        hive_session.execute_sql_statement_with_interpolation(
            sql_statement=statement
        )

        # Add daily INSERT records to dwx_audit_transition.calc_parts_snapshot_enriched_insert_only
        statement = f"""
                INSERT INTO dwx_audit_transition.calc_parts_snapshot_enriched_insert_only
                SELECT id_key, db_type, json
                FROM dwx_audit_transition.int_calc_parts_range_insert;
        """
        hive_session.execute_sql_statement_with_interpolation(
            sql_statement=statement
        )

        # Append daily DELETE records to snapshot DELETE records
        statement = f"""
                INSERT INTO dwx_audit_transition.calc_parts_snapshot_enriched_delete_only
                SELECT id_key, db_type, json
                FROM dwx_audit_transition.int_calc_parts_range_final;
        """
        hive_session.execute_sql_statement_with_interpolation(
            sql_statement=statement
        )

    # Processes and publishes data
    def run(self):
        # self.create_new_baseline()
        # self.create_baseline_with_insert_only()
        # self.create_baseline_with_delete_only()
        # self.benchmark_reconciliation()
        self.merge_daily_import_into_monthly_tables()

    def daily_test(self):
        configuration = self._configuration
        hive_session = self._hive_session
        daily_location = "s3://{published_bucket}/corporate_data_ingestion/json/daily/{daily_date}/calculator/calculationParts/".format(
            published_bucket=configuration.configuration_file.s3_published_bucket,
            daily_date="2022-10-01"
        )

        # create new-snapshot table
        drop_table = "DROP TABLE IF EXISTS dwx_audit_transition.calc_parts_snapshot_updated"
        create_table = """CREATE TABLE dwx_audit_transition.calc_parts_snapshot_updated (id_key STRING, dbType STRING, json STRING)  STORED AS orc TBLPROPERTIES ('orc.compress'='ZLIB')"""
        hive_session.execute_sql_statement_with_interpolation(sql_statement=drop_table)
        hive_session.execute_sql_statement_with_interpolation(sql_statement=create_table)

        # create daily table
        drop_daily_table = "DROP TABLE IF EXISTS dwx_audit_transition.calc_parts_daily_2022_10_01"
        create_daily_table = """CREATE TABLE dwx_audit_transition.calc_parts_daily_2022_10_01 (id_key STRING, dbType STRING, json STRING)  STORED AS orc TBLPROPERTIES ('orc.compress'='ZLIB')"""
        hive_session.execute_sql_statement_with_interpolation(sql_statement=drop_daily_table)
        hive_session.execute_sql_statement_with_interpolation(sql_statement=create_daily_table)

        # Read daily data into pyspark and process the same way as full snapshot
        (
            self.read_dir(daily_location)
            .map(json.loads)
            .map(lambda x: (f'{x.get("_id").get("id")}_{x.get("_id").get("type")}',
                            "INSERT" if x.get("_removedDateTime") is None else "DELETE",
                            json.dumps(x, ensure_ascii=False, separators=(',', ':'))))
            .toDF(["id_key", "dbType", "json"])
            .write.insertInto("dwx_audit_transition.calc_parts_daily_2022_10_01", overwrite=True)
        )

    def record_daily_statistics(self, table_name, statistics_table_name, db_name):
        """ Generate and record daily merge statistics for the table name given as parameter """

        export_date = (dt.datetime.strptime(self._configuration.export_date, "%Y-%m-%d") - dt.timedelta(days=1)).strftime("%Y_%m")

        record_statistics = f"""
            INSERT INTO {db_name}.{statistics_table_name}
            SELECT
                FROM_UNIXTIME(UNIX_TIMESTAMP()) AS datetime,
                '{export_date}' AS date_processed,
                '{table_name}' AS table_name,
                db_type,
                MIN(last_date) AS min_date,
                MAX(last_date) AS max_date,
                COUNT(id_key) AS record_count
            FROM {db_name}.{table_name} GROUP BY db_type
        """
        self._hive_session.execute_sql_statement_with_interpolation(sql_statement=record_statistics)

    def merge_daily_import_into_monthly_tables(self):
        """ Merge daily import into monthly tables. Both day and month values are derived from 'export_date' parameter
        """
        logger.info("Starting merge daily Calculation Parts ingest into monthly tables")
        hive_session = self._hive_session
        db_name = "dwx_audit_transition"

        prefix_date = (
                dt.datetime.strptime(self._configuration.export_date, "%Y-%m-%d") - dt.timedelta(days=1)).strftime(
            "%Y_%m_%d")
        collection_name = "calculator:calculationParts"  # Collection name hardcoded here because a different collection_name is used to select this ingester during testing

        published_bucket = self._configuration.configuration_file.s3_published_bucket

        source_prefix = path.join(
            self._configuration.destination_s3_prefix.lstrip("/"),
            self._configuration.export_date,
            *collection_name.split(":"),
        )

        s3_source_url = "s3://{bucket}/{prefix}".format(bucket=published_bucket, prefix=source_prefix.lstrip("/"))

        # Create external table over daily location in S3
        external_table_name = f"external_calculation_parts_daily_{prefix_date}"
        create_external_table = f"""CREATE EXTERNAL TABLE {db_name}.{external_table_name} (val STRING)
                                    STORED AS TEXTFILE LOCATION '{s3_source_url}'"""

        hive_session.execute_sql_statement_with_interpolation(sql_statement=create_external_table)

        # Create monthly permanent tables
        monthly_transaction_complete_table_name = f"calculation_parts_{prefix_date[:-3]}_transaction_complete"
        monthly_transaction_start_table_name = f"calculation_parts_{prefix_date[:-3]}_transaction_start"
        daily_statistics_table_name = f"calculation_parts_{prefix_date[:-3]}_statistics"
        create_permanent_tables = f"""
        CREATE TABLE IF NOT EXISTS {db_name}.{monthly_transaction_complete_table_name} (id_key STRING, id_prefix STRING, db_type STRING, last_date STRING, json STRING);
        CREATE TABLE IF NOT EXISTS {db_name}.{monthly_transaction_start_table_name} (id_key STRING, id_prefix STRING, db_type STRING, last_date STRING, json STRING);
        CREATE TABLE IF NOT EXISTS {db_name}.{daily_statistics_table_name} (datetime TIMESTAMP, date_processed STRING, table_name STRING, db_type STRING, min_date STRING, max_date STRING, record_count STRING);
        """
        hive_session.execute_sql_statement_with_interpolation(sql_statement=create_permanent_tables)

        # Create temporary tables
        transaction_complete_table_name = "calculation_parts_transaction_complete"
        transaction_start_table_name = "calculation_parts_transaction_start"
        transaction_end_table_name = "calculation_parts_transaction_end"
        transaction_unmatched_table_name = "calculation_parts_transaction_unmatched"
        control_table_name = "calculation_parts_control"

        create_temporary_tables = f"""
        DROP TABLE IF EXISTS {db_name}.{transaction_complete_table_name};
        CREATE TABLE {db_name}.{transaction_complete_table_name} (id_key STRING, id_prefix STRING, db_type STRING, last_date STRING, json STRING);

        DROP TABLE IF EXISTS {db_name}.{transaction_start_table_name};
        CREATE TABLE {db_name}.{transaction_start_table_name} (id_key STRING, id_prefix STRING, db_type STRING, last_date STRING, json STRING);

        DROP TABLE IF EXISTS {db_name}.{transaction_end_table_name};
        CREATE TABLE {db_name}.{transaction_end_table_name} (id_key STRING, id_prefix STRING, db_type STRING, last_date STRING, json STRING);

        DROP TABLE IF EXISTS {db_name}.{transaction_unmatched_table_name};
        CREATE TABLE {db_name}.{transaction_unmatched_table_name} (id_key STRING, id_prefix STRING, db_type STRING, last_date STRING, json STRING);

        DROP TABLE IF EXISTS {db_name}.{control_table_name};
        CREATE TABLE {db_name}.{control_table_name} (id_key STRING, delete_count STRING, insert_count STRING, record_count STRING, last_date STRING, db_type STRING);
        """
        hive_session.execute_sql_statement_with_interpolation(sql_statement=create_temporary_tables)

        # Create daily table
        daily_table_name = f"calculation_parts_{prefix_date}"
        create_daily_table = f"""CREATE TABLE {db_name}.{daily_table_name} (id_key STRING, id_prefix STRING, db_type STRING, last_date STRING, json STRING)"""
        hive_session.execute_sql_statement_with_interpolation(sql_statement=create_daily_table)

        # Populate daily table
        populate_daily_table = f"""
            INSERT INTO {db_name}.{daily_table_name}
            SELECT
                CONCAT(GET_JSON_OBJECT(val, '$._id.id'), '_', GET_JSON_OBJECT(val, '$._id.type')) AS id_key
                ,SUBSTR(GET_JSON_OBJECT(val, '$._id.id'), 0, 2) AS id_prefix
                ,CASE WHEN GET_JSON_OBJECT(val, '$._removedDateTime') IS null THEN 'INSERT' ELSE 'DELETE' END AS db_type
                ,SUBSTR(COALESCE(GET_JSON_OBJECT(val, '$._removedDateTime.d_date'), GET_JSON_OBJECT(val, '$.createdDateTime.d_date')), 0, 10) AS last_date
                ,val AS json
            FROM {db_name}.{external_table_name}
        """
        hive_session.execute_sql_statement_with_interpolation(sql_statement=populate_daily_table)

        # Populate 'control' table
        populate_control_table = f"""
                INSERT INTO {db_name}.{control_table_name}
                SELECT
                    id_key
                    ,SUM(CASE WHEN db_type='DELETE' THEN 1 ELSE 0 END) AS delete_count
                    ,SUM(CASE WHEN db_type='INSERT' THEN 1 ELSE 0 END) AS insert_count
                    ,COUNT(*) AS record_count
                    ,'' AS last_date
                    ,'' AS db_type
                FROM {db_name}.{daily_table_name}
                GROUP BY id_key
        """
        hive_session.execute_sql_statement_with_interpolation(sql_statement=populate_control_table)
        self.record_daily_statistics(control_table_name, daily_statistics_table_name, db_name)

        # Gather completed transactions (pairs of INSERT and DELETE records)
        populate_transaction_complete_table = f"""
            INSERT INTO {db_name}.{transaction_complete_table_name}
            SELECT DISTINCT s.* FROM {db_name}.{daily_table_name} s
            INNER JOIN {db_name}.{control_table_name} c
            ON c.id_key = s.id_key
            WHERE s.db_type='DELETE'
            AND c.delete_count>=1
            AND c.insert_count>=1
        """
        hive_session.execute_sql_statement_with_interpolation(sql_statement=populate_transaction_complete_table)
        self.record_daily_statistics(transaction_complete_table_name, daily_statistics_table_name, db_name)

        # Gather orphan DELETE records (confirmation that a previously opened transaction is now closed)
        populate_transaction_end_table = f"""
            INSERT INTO {db_name}.{transaction_end_table_name}
            SELECT DISTINCT s.* FROM {db_name}.{daily_table_name} s
            INNER JOIN {db_name}.{control_table_name} c
            ON c.id_key = s.id_key
            WHERE s.db_type='DELETE'
            AND c.delete_count>=1
            AND c.insert_count==0
        """
        hive_session.execute_sql_statement_with_interpolation(sql_statement=populate_transaction_end_table)
        self.record_daily_statistics(transaction_end_table_name, daily_statistics_table_name, db_name)

        # Gather orphan INSERT records (confirmation that a record has been opened but not completed yet)
        populate_transaction_start_table = f"""
            INSERT INTO {db_name}.{transaction_start_table_name}
            SELECT DISTINCT s.* FROM {db_name}.{daily_table_name} s
            LEFT JOIN {db_name}.{control_table_name} c
            ON c.id_key = s.id_key
            WHERE s.db_type='INSERT'
            AND c.delete_count==0
            AND c.insert_count>=1
        """
        hive_session.execute_sql_statement_with_interpolation(sql_statement=populate_transaction_start_table)
        self.record_daily_statistics(transaction_start_table_name, daily_statistics_table_name, db_name)

        # Append to monthly completed transaction
        append_completed_transaction = f"""
            INSERT INTO {db_name}.{monthly_transaction_complete_table_name}
            SELECT * FROM {db_name}.{transaction_complete_table_name};

            INSERT INTO {db_name}.{monthly_transaction_complete_table_name}
            SELECT * FROM {transaction_end_table_name};
        """
        hive_session.execute_sql_statement_with_interpolation(sql_statement=append_completed_transaction)
        self.record_daily_statistics(monthly_transaction_complete_table_name, daily_statistics_table_name, db_name)

        # Rebuild transaction_start_table filtering out the INSERT records
        # without matching DELETE after processing of current day
        rebuild_transaction_start_table = f"""
            INSERT INTO {db_name}.{transaction_unmatched_table_name}
            SELECT i.* FROM {db_name}.{monthly_transaction_start_table_name} i
            LEFT JOIN {db_name}.{transaction_end_table_name} d
            ON i.id_key = d.id_key
            AND d.id_key IS null;

            TRUNCATE TABLE {db_name}.{monthly_transaction_start_table_name};

            INSERT INTO {db_name}.{monthly_transaction_start_table_name}
            SELECT * FROM {db_name}.{transaction_unmatched_table_name};"
        """
        hive_session.execute_sql_statement_with_interpolation(sql_statement=rebuild_transaction_start_table)
        self.record_daily_statistics(monthly_transaction_start_table_name, daily_statistics_table_name, db_name)

        # Append new INSERT records to monthly_transaction_start table
        append_to_monthly_transaction_start = f"""
            INSERT INTO {db_name}.{monthly_transaction_start_table_name}
            SELECT * FROM {db_name}.{transaction_start_table_name};"
        """
        hive_session.execute_sql_statement_with_interpolation(sql_statement=append_to_monthly_transaction_start)
        self.record_daily_statistics(monthly_transaction_start_table_name, daily_statistics_table_name, db_name)
