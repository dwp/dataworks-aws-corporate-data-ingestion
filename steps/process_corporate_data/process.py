import logging

logger = logging.getLogger(__name__)


def read_binary(spark, file_path):
    return spark.sparkContext.textFile(file_path)


def get_list_keys_for_prefix(s3_client, s3_bucket, s3_prefix):
    logger.info(
        "Looking for files to process in bucket : %s with prefix : %s",
        s3_bucket,
        s3_prefix,
    )
    keys = []
    paginator = s3_client.get_paginator("list_objects_v2")
    pages = paginator.paginate(Bucket=s3_bucket)
    for page in pages:
        if "Contents" in page:
            for obj in page["Contents"]:
                keys.append(obj["Key"])
    if s3_prefix in keys:
        keys.remove(s3_prefix)
    return keys


def process_collection(
        spark,
        args,
        s3_client,
        run_timestamp,
        s3_corporate_bucket,
        s3_published_bucket
):
    s3_keys_from_bucket = get_list_keys_for_prefix(s3_client, s3_corporate_bucket, f"{args.export_date}/")
    print(f"s3a://{s3_published_bucket}")
    for key in s3_keys_from_bucket:
        print(f"S3 key is = '{key}'")
        plain = read_binary(spark, f"s3a://{s3_corporate_bucket}/{key}")
        print(plain.collect())
        # plain.saveAsTextFile(f"s3a://{s3_published_bucket}/{key}")



