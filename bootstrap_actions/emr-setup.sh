#!/usr/bin/env bash
echo "Installing scripts"
$(which aws) s3 cp "${S3_CLOUDWATCH_SHELL}"            /opt/emr/cloudwatch.sh

echo "Changing the Permissions"
chmod u+x /opt/emr/cloudwatch.sh

(
    # Import the logging functions
    source /opt/emr/logging.sh
    
    function log_wrapper_message() {
        log_aws_emr_template_repository_message "$${1}" "emr-setup.sh" "$${PID}" "$${@:2}" "Running as: ,$USER"
    }
    log_wrapper_message "Setting up the Proxy"

    echo -n "Running as: "
    whoami
    
    export AWS_DEFAULT_REGION="${aws_default_region}"

    FULL_PROXY="${full_proxy}"
    FULL_NO_PROXY="${full_no_proxy}"
    export http_proxy="$FULL_PROXY"
    export HTTP_PROXY="$FULL_PROXY"
    export https_proxy="$FULL_PROXY"
    export HTTPS_PROXY="$FULL_PROXY"
    export no_proxy="$FULL_NO_PROXY"
    export NO_PROXY="$FULL_NO_PROXY"
    export AWS_EMR_TEMPLATE_REPOSITORY_LOG_LEVEL="${AWS_EMR_TEMPLATE_REPOSITORY_LOG_LEVEL}"

    echo "Setup cloudwatch logs"
    sudo /opt/emr/cloudwatch.sh \
    "${cwa_metrics_collection_interval}" "${cwa_namespace}"  "${cwa_log_group_name}" \
    "${aws_default_region}" "${cwa_bootstrap_loggrp_name}" "${cwa_steps_loggrp_name}"

    log_wrapper_message "Getting the DKS Certificate Details "
    log_wrapper_message "Getting the DKS Certificate Details "
    
    ## get dks cert
    trust_store_pass=$(uuidgen -r)
    key_store_pass=$(uuidgen -r)
    key_pass=$(uuidgen -r)
    acm_pass=$(uuidgen -r)

    export TRUSTSTORE_PASSWORD="$trust_store_pass"
    export KEYSTORE_PASSWORD="$key_store_pass"
    export PRIVATE_KEY_PASSWORD="$key_pass"
    export ACM_KEY_PASSWORD="$acm_pass"
    
    #sudo mkdir -p /opt/emr
    #sudo chown hadoop:hadoop /opt/emr
    touch /opt/emr/dks.properties
cat >> /opt/emr/dks.properties <<EOF
identity.store.alias=${private_key_alias}
identity.key.password=$PRIVATE_KEY_PASSWORD
spark.ssl.fs.enabled=true
spark.ssl.keyPassword=$KEYSTORE_PASSWORD
identity.keystore=/opt/emr/keystore.jks
identity.store.password=$KEYSTORE_PASSWORD
trust.keystore=/opt/emr/truststore.jks
trust.store.password=$TRUSTSTORE_PASSWORD
data.key.service.url=${dks_endpoint}
EOF
    
    log_wrapper_message "Retrieving the ACM Certificate details"
    
    acm-cert-retriever \
    --acm-cert-arn "${acm_cert_arn}" \
    --acm-key-passphrase "$ACM_KEY_PASSWORD" \
    --keystore-path "/opt/emr/keystore.jks" \
    --keystore-password "$KEYSTORE_PASSWORD" \
    --private-key-alias "${private_key_alias}" \
    --private-key-password "$PRIVATE_KEY_PASSWORD" \
    --truststore-path "/opt/emr/truststore.jks" \
    --truststore-password "$TRUSTSTORE_PASSWORD" \
    --truststore-aliases "${truststore_aliases}" \
    --truststore-certs "${truststore_certs}" \
    --jks-only true >> /var/log/aws-emr-template-repository/acm-cert-retriever.log 2>&1
    
    #shellcheck disable=SC2024
    sudo -E acm-cert-retriever \
    --acm-cert-arn "${acm_cert_arn}" \
    --acm-key-passphrase "$ACM_KEY_PASSWORD" \
    --private-key-alias "${private_key_alias}" \
    --truststore-aliases "${truststore_aliases}" \
    --truststore-certs "${truststore_certs}"  >> /var/log/aws-emr-template-repository/acm-cert-retriever.log 2>&1 # No sudo needed to write to file, so redirect is fine
    
    cd /etc/pki/ca-trust/source/anchors/ || exit

    sudo touch analytical_ca.pem
    sudo chown hadoop:hadoop /etc/pki/tls/private/"${private_key_alias}".key /etc/pki/tls/certs/"${private_key_alias}".crt /etc/pki/ca-trust/source/anchors/analytical_ca.pem
    TRUSTSTORE_ALIASES="${truststore_aliases}"

    #shellcheck disable=SC2001
    for F in $(echo "$TRUSTSTORE_ALIASES" | sed "s/,/ /g"); do #Shellcheck wants to not use sed for POSIX compliance but is ok here as it works
        (sudo cat "$F.crt"; echo) >> analytical_ca.pem;
    done
    
    UUID=$(dbus-uuidgen | cut -c 1-8)
    TOKEN=$(curl -X PUT -H "X-aws-ec2-metadata-token-ttl-seconds: 21600" "http://169.254.169.254/latest/api/token")

    instance=$(curl -H "X-aws-ec2-metadata-token:$TOKEN" -s http://169.254.169.254/latest/meta-data/instance-id)
    role=$(jq .instanceRole /mnt/var/lib/info/extraInstanceData.json)
    
    export INSTANCE_ID="$instance"
    export INSTANCE_ROLE="$role"

    host="${name}-$${INSTANCE_ROLE//\"}-$UUID"
    export HOSTNAME="$host"

    hostnamectl set-hostname "$HOSTNAME"
    aws ec2 create-tags --resources "$INSTANCE_ID" --tags Key=Name,Value="$HOSTNAME"

    chmod u+x /var/ci/update_dynamo.sh

    /var/ci/update_dynamo.sh &

    # /var/ci/status_metrics.sh &
    
    log_wrapper_message "Completed the emr-setup.sh step of the EMR Cluster"

) >> /var/log/aws-emr-template-repository/emr-setup.log 2>&1
