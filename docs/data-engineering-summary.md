# Data Engineering Logic Flow

Table of Contents:
- [Purpose](#purpose)
- [Summary](#summary)
  - [Processes Daily Data](#processes-daily-data)
  - [Completes post-processing](#completes-post-processing)
- [Logic Flow](#logic-flow)
  - [data.businessAudit](#databusinessaudit)
  - [calculator.calculationParts](#calculatorcalculationparts)


### Purpose

This documentation was written to outline the steps that the cluster takes to process data.
It is not intended to walk through the helpers, utils, data classes line-by-line.

The code snippets are up-to-date as of 28/06/23

### Summary
The Corporate Data Ingester does the following:

#### Processes daily data:
  - Reads streamed 'change data' from S3 (corporate storage bucket)
  - Decrypts the payload
  - Completes validation/transformation/sanitisation (per HTME)
  - Stores the change data in S3 (published bucket)

#### Completes post-processing:
  - data.businessAudit:
    - appends daily data to a basic DDL=(val string) managed table
    - appends daily data to 2 audit tables with schemas applied
  - calculator.calculationParts:
    - combines previous snapshot with daily data
    - ranks & deduplicates on ID (by record type - either INSERT or DELETE)
    - produces updated snapshot
  - < future generic pipeline >
    - combine previous snapshot with daily data
    - rank & deduplicate on ID (by timestamp desc)
    - produces updated snapshot


### Logic Flow
The easiest way to read the code is to look for the `.run()` method of the appropriate ingester
in `steps/ingesters.py`.  This method is called for each 'export date' passed to the application, 
if you launch the step with `start_date=2022-01-01`, `end_date=2022-01-07`, the `.run()` method would be
called once for each date between 01/01/22 - 07/01/22

### data.businessAudit
Audit data is a true 'append-only' collection.  Each ID is only seen once, and will never be updated/deleted.
For this reason, the processing is simplified.

#### Decrypt and Process
`.run()` calls `.decrypt_and_process()`.  The most significant snippet of the method is this:
```
## as of 28/06/23
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
```
The above represents a list of instructions to spark for processing the data.
1. read the data from s3
2. turn each 'message' (received over kafka) into a UCMessage object
3. decrypt the payload of each message
4. run `transform`, `validate`, and `sanitise` methods on each UCMessage object
5. pull out the (decrypted, transformed, validated, sanitised) payload in a standardised way
6. save to s3 as **TextFile** - no complex processing is required and so ORC doesn't
offer a significant benefit here. It's also easier to impose schema on the JSON text data

The data is stored in the `corporate_data_ingestion/json/daily/` prefix

The UCMessage class/methods can be inspected in `steps/data.py`

#### Execute Hive Statements
After `.decrypt_and_process()`, `.execute_hive_statements()` is run.  This method completes the post-processing
previously done by ADG for data.businessAudit.

The SQL files are downloaded to the cluster during the bootstrap phase (`bootstrap_actions/download_scripts.sh`)
and executed in pyspark.

The following is repeated for each (`table`, `schema`) 
1. external table created over daily JSON data with `schema`
2. partition manually added to external table
3. partition copied from external table to managed table


### calculator.calculationParts
CalculationParts is very large (~10TB), but in terms of data received is simpler than most collections.  For each ID
an INSERT record is followed by a DELETE record.  This helps shortcut the logic to find the latest record, by
prioritising DELETE > INSERT.  For this reason, calculationParts logic code is simpler than that needed for
a generic pipeline.

####
### Decrypt and Process
`.run()` calls `.decrypt_and_process()`.  The most significant snippet of the method is this:
```
## as of 28/06/23
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
```
The above represents a list of instructions to spark for processing the data.
1. read the data from s3
2. turn each 'message' received over kafka into a UCMessage object
3. decrypt the payload of each message
4. run `validate` and `sanitise` methods on each UCMessage object (`transform` is data.businessAudit-only)
5. Extract the data required from the UCMessage object into a "Row"
6. Turn the RDD of `Row` objects into a dataframe
7. Repartition the data by "id_part" (first 2 chars of the ID) and "id" - ensuring 'like' IDs are stored together
8. Sort each partition by "id"
9. Write out the data to S3 (to an existing ORC dataset), partitioned by export_date

The data is stored in the `corporate_data_ingestion/orc/daily/` prefix

#### Update
`.run()` will conditionally call `.update()`.  This creates a new full dataset by finding and reading the previous
full dataset, and applying new data since that date.

We start by defining the two dataframes to be joined together; the previous full dataset, and the daily data ingested 
then.

previous full dataset:
```
## as of 28/06/23
df_cdi_output = (
    self._spark_session.read.schema(schema_cdi_output)
    .orc(latest_cdi_export_s3_url)
    .select(col("id"), col("db_type"), col("val"), col("id_part"))
 )
```
1. apply schema
2. read data
3. select columns required (in the same order as the next dataset)

daily data since previous full dataset:
```
## as of 28/06/23
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
```
1. apply schema
2. read data
3. filter for data since the previous full dataset - this is a little more complex than
intended because the dataset is partitioned by `year, month, day` rather than `date`
4. select the columns required (in the same order as the previous dataset)

The dataframes are then union-ed and deduplicated:
```
## as of 28/06/23
window_spec = Window.partitionBy("id_part", "id").orderBy("db_type")
(
    df_cdi_output.union(df_dailies)
    .repartitionByRange(
        4096, "id_part", "id"
    )  # todo: remove number of partitions and influence via spark config
    .withColumn("row_number", row_number().over(window_spec))
    .filter(col("row_number") == 1)
    .write.partitionBy("id_part")
    .orc(export_output_url, mode="overwrite", compression="zlib")
)
```
1. with the full dataset, union the daily dataset
2. repartition the data to ensure like-ids are stored together
3. use the window_spec to add 'row_number'; row_number resets on each new id,
and ranks the records by a characteristic (DELETE > INSERT)
4. filter for 'top' records
5. write data to s3 as **ORC** - important that we maintain partitioning, sorting, 
and metadata so that we can efficiently use this dataset to perform a future merge

The data is stored in the `corporate_data_ingestion/exports/` prefix

#### Export to Hive Table
`.run()` will conditionally call `.export_to_hive_table()`.  This applies a schema
to the JSON values in the ORC data, and copies into managed tables in uc_lab_staging database.

Typically, the uc_lab_external_batch cluster would apply the schema to JSON text files, however
we found it was not capable of doing so due to the volume of calculationParts data.  It's possible
in future that the uc_lab cluster could be tuned to handle this.  In the meantime the CDI
process has assumed some responsibility for publishing the uc_lab_staging data that 
comes from calculationParts.

to start:
```
## as of 28/06/23
    source_df = (
        self._spark_session
        .read
        .schema(schema_cdi_output)
        .orc(export_output_url)
        .persist(storageLevel=StorageLevel.DISK_ONLY)
    )
```
A dataframe is created to contain the source data (entire snapshot of calculationParts).
The instruction given is to persist to disk - otherwise each time we use this dataframe, it
would read the data from S3 again.

for each (`table`, `table_ddl`), we apply a schema to the JSON values in the dataframe
and publish the data to the table.
```
## as of 28/06/23
(
    source_df
    .select(from_json("val", json_schema).alias("val"), "id_part", "id")
    .repartitionByRange(1024, "id_part", "id").select("val.*")
    .write.format("orc").mode("overwrite").saveAsTable(f"uc_lab_staging.{table_dict['table_name']}")
)
```
1. select the following from the dataframe
   1. 'val' with table-specific schema imposed
   2. 'id_part'
   3. 'id'
2. reduce the number of partitions; applying the schema reduces data volume
significantly (same number of rows, lots of data disregarded)
3. select only the schema-driven data & write the data to the appropriate table

