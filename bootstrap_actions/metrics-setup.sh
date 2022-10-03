#!/bin/bash
set -euo pipefail
(
    # Import the logging functions
    source /opt/emr/logging.sh

    function log_wrapper_message() {
        log_aws_emr_template_repository_message "$1" "metrics-setup.sh" "$$" "Running as: $USER"
    }

    log_wrapper_message "Pulling files from S3"

    METRICS_FILEPATH="/opt/emr/metrics"

    mkdir -p /opt/emr/metrics

    aws s3 cp "${metrics_pom}" $METRICS_FILEPATH/pom.xml
    aws s3 cp "${prometheus_config}" $METRICS_FILEPATH/prometheus_config.yml

    log_wrapper_message "Fetching and unzipping maven"

    MAVEN="apache-maven"
    VERSION="3.6.3"

    export http_proxy="${proxy_url}"
    export https_proxy="${proxy_url}"

    curl -o "/tmp/$MAVEN-$VERSION.tar.gz" "https://archive.apache.org/dist/maven/maven-3/$VERSION/binaries/$MAVEN-$VERSION-bin.tar.gz"
    tar -C /tmp -xvf "/tmp/$MAVEN-$VERSION.tar.gz"

    log_wrapper_message "Moving maven and cleaning up"

    mv "/tmp/$MAVEN-$VERSION" "$METRICS_FILEPATH/$MAVEN"
    rm "/tmp/$MAVEN-$VERSION.tar.gz"

    log_wrapper_message "Resolving dependencies for metrics"

    #shellcheck disable=SC2001
    PROXY_HOST=$(echo "${proxy_url}" | sed 's|.*://\(.*\):.*|\1|') # SED is fine to use here
    #shellcheck disable=SC2001
    PROXY_PORT=$(echo "${proxy_url}" | sed 's|.*:||') # SED is fine to use here

    export MAVEN_OPTS="-DproxyHost=$PROXY_HOST -DproxyPort=$PROXY_PORT"
    $METRICS_FILEPATH/$MAVEN/bin/mvn -f $METRICS_FILEPATH/pom.xml dependency:copy-dependencies -DoutputDirectory="$METRICS_FILEPATH/dependencies"

) >> /var/log/aws-emr-template-repository/metrics_setup.log 2>&1
