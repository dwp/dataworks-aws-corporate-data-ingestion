#!/bin/bash

source /opt/shared/common_logging.sh

log_dataworks_aws_corporate_data_ingestion_message() {
    set +u

    message="${1}"
    component="${2}"
    process_id="${3}"

    application="dataworks_aws_corporate_data_ingestion"

    app_version="NOT_SET"
    if [ -f "/opt/emr/version" ]; then
        app_version=$(cat /opt/emr/version)
    fi

    log_level="NOT_SET"
    if [ -f "/opt/emr/log_level" ]; then
        log_level=$(cat /opt/emr/log_level)
    fi

    environment="NOT_SET"
    if [ -f "/opt/emr/environment" ]; then
        environment=$(cat /opt/emr/environment)
    fi

    log_message "${message}" "${log_level}" "${app_version}" "${process_id}" "${application}" "${component}" "${environment}" "${@:4}"
}
