# Data storage

<!-- TOC -->
* [Data storage](#data-storage)
  * [Summary](#summary)
  * [Source Data](#source-data)
    * [File storage](#file-storage)
    * [Encryption](#encryption)
  * [Intermediate & Destination Storage](#intermediate--destination-storage)
    * [data.businessAudit](#databusinessaudit)
    * [calculator.calculationParts](#calculatorcalculationparts)
<!-- TOC -->

## Summary
Data is stored in different shapes and formats throughout the platform and in
the CDI.

Source - Data in corporate storage bucket, put by K2HB
Intermediate (incremental) Storage - Daily incremental data
Destination - Full export storage


## Source Data
In the corporate storage bucket, there are separate prefixes for each:
- data.businessAudit (ucfs_audit)
- equalities (ucfs_equalities)
- all other collections (ucfs_main)

These were aligned to a specific K2HB ASG:
ucfs_main: `main` consumer and `main-dedicated` consumer for larger collections
ucfs_equalities: `equalities` consumer for specific collections
ucfs_audit: `audit` consumer for data.businessAudit

Now there is an additional consumer ASG `s3_only` which processes 
calculationParts and is intended to be the consumer for any collections
previously in the `main` group but no longer ingested to HBase

### File storage

Files are stored in `.json.gz` - batched by K2HB initially, and then coalesced
into fewer, larger files by the
[corporate-storage-coalescence](https://github.com/dwp/dataworks-corporate-storage-coalescence) 
process. Each line in the file is a json record, see
[K2HB repo](https://github.com/dwp/kafka-to-hbase/tree/master/docs) for samples.

### Encryption

The bucket objects are encrypted with SSE-KMS

Within the files, records contain an encrypted `dbObject` field.  This can be
decrypted using the encryptionMaterials, which themselves must be decrypted by 
DKS.


## Intermediate & Destination Storage
The corporate data ingestion process decrypts and transforms the messages
stored in the corporate storage bucket.  Audit is an append-only collection 
and so a 'full export' can be created simply by appending this incremental
data to an existing table.

### data.businessAudit

**File Storage**

Incremental data is maintained as JSONL textfiles (LZO compressed).  The data
is split by EMR into `part-XXXXX.lzo` files - each file is the result of a 
single task on the cluster, running on a specific executor, on a specific node.

The textfiles contain a single json record on each line, which can be read in 
hive by imposing a schema over the data with an external table.

The destination for Audit is hive-managed tables.

**Encryption**

The encryption here follows the standard pattern for EMR clusters on Dataworks

When writing using an EMR cluster (as opposed to K2HB in EC2), we can use a 
custom encryption materials provider.  This plugin ensures that data written out 
by the cluster to S3 is encrypted on the cluster using keys provided by DKS.  
Some metadata is stored with the objects in S3 to allow EMR clusters to decrypt 
the data using DKS.

Plugin configured [here](https://github.com/dwp/dataworks-aws-corporate-data-ingestion/blob/54b0339e86eeb828b2b0bd57436359498a376660/cluster_config.tf#L1-L4).
The objects also appear to be encrypted by SSE-KMS

### calculator.calculationParts

**File Storage**

Output data for calculationParts (and likely future collections) will need
to be maintained in a columnar file format (currently ORC).  This significantly 
improves both performance and compression ratios. The ORC files also include
additional columns such as `id`, `id_part` (2 hex-digit prefix of the id), and
`db_type` which is used in deduplication.

ORC maintains metadata, which is particularly helpful when data is partitioned 
and sorted.  It allows spark to make intelligent decisions when planning joins,
repartitioning, etc. and when reading files.

The S3-stored data is partitioned by `id_part` into 256 partitions, and sorted by 
`id_part, id`.  Within spark, the number of spark-partitions is currently set
to a specific number to reduce the number of files produced.

N.B. downstream processes typically rely on receiving JSONL data and use
JsonSerDe, which is not compatible with ORC.  It's likely that to maintain
compatibility with downstream processes for future collections, JSONL data will
also be published.

**Encryption**

As above for data.businessAudit (Uses EMRFS custom encryption materials 
provider, follows pattern for EMR on dataworks)
