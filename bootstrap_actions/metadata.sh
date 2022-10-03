#!/bin/bash
RESPONSE=$(curl -s -o /dev/null -w "%{http_code}" http://169.254.169.254/latest/meta-data)

if [[ "$RESPONSE" == "200" ]]; then
    echo "IMDSv1 Enabled"
    INSTANCE_ID=$(curl -s http://169.254.169.254/latest/meta-data/instance-id)
    REGION=$(curl -s http://169.254.169.254/latest/dynamic/instance-identity/document|grep region|awk -F\" '{print $4}')
    echo "$REGION"
    echo "$INSTANCE_ID"
    aws ec2 modify-instance-metadata-options \
    --region "$REGION" \
    --instance-id "$INSTANCE_ID" \
    --http-tokens optional \
    --http-endpoint enabled
fi
