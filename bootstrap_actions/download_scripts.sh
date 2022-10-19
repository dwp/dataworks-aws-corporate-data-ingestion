#!/bin/bash

sudo mkdir -p /var/log/dataworks-aws-corporate-data-ingestion
sudo mkdir -p /opt/emr/steps
sudo mkdir -p /opt/shared
sudo mkdir -p /var/ci
sudo chown hadoop:hadoop /var/log/dataworks-aws-corporate-data-ingestion
sudo chown hadoop:hadoop /opt/emr
sudo chown hadoop:hadoop /opt/emr/steps
sudo chown hadoop:hadoop /opt/shared
sudo chown hadoop:hadoop /var/ci
export LOG_LEVEL="${dataworks_aws_corporate_data_ingestion_log_level}"
export LOG_PATH="${dataworks_aws_corporate_data_ingestion_log_path}"

echo "${VERSION}" > /opt/emr/version
echo "${ENVIRONMENT_NAME}" > /opt/emr/environment
echo "${dataworks_aws_corporate_data_ingestion_log_level}" > /opt/emr/log_level

# Download the logging scripts
$(which aws) s3 cp "${S3_COMMON_LOGGING_SHELL}"  /opt/shared/common_logging.sh
$(which aws) s3 cp "${S3_LOGGING_SHELL}"         /opt/emr/logging.sh

# Set permissions
chmod u+x /opt/shared/common_logging.sh
chmod u+x /opt/emr/logging.sh

(
    # Import the logging functions
    source /opt/emr/logging.sh

    function log_wrapper_message() {
        log_dataworks_aws_corporate_data_ingestion_message "$${1}" "download_scripts.sh" "$${PID}" "$${@:2}" "Running as: ,$USER"
    }

    log_wrapper_message "Downloading & install latest bootstrap and steps scripts"
    $(which aws) s3 cp --recursive "${scripts_location}/" /var/ci/ --include "*.sh"

    log_wrapper_message "Apply recursive execute permissions to the folder"
    sudo chmod --recursive a+rx /var/ci

    log_wrapper_message "Moving python steps files to steps folder"
    aws s3 cp "${corporate_data_ingestion_script}" /opt/emr/steps/.
    aws s3 cp "${python_logger_script}" /opt/emr/steps/.
    aws s3 cp "${python_configuration_file}" /opt/emr/steps/.
    sudo chmod --recursive a+rx /opt/emr/steps/.

    log_wrapper_message "Script downloads completed"


)  >> /var/log/dataworks-aws-corporate-data-ingestion/download_scripts.log 2>&1
