#!/bin/sh

CURRENT_DATE=$(date +'%Y/%m/%d')

aws --endpoint-url=http://aws-s3:4566 s3api create-bucket \
    --bucket $S3_CORPORATE_BUCKET \
    --region $AWS_DEFAULT_REGION

aws --endpoint-url=http://aws-s3:4566 s3api put-object --bucket $S3_CORPORATE_BUCKET --key $CURRENT_DATE/testfile.txt --body /app/testfile.txt

aws --endpoint-url=http://aws-s3:4566 s3api create-bucket \
    --bucket $S3_PUBLISHED_BUCKET \
    --region $AWS_DEFAULT_REGION

aws --endpoint-url=http://aws-s3:4566 s3api list-objects-v2 --bucket $S3_PUBLISHED_BUCKET
