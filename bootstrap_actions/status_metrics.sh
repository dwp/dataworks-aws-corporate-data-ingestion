#!/bin/bash
(
    INSTANCE_ROLE=$(jq .instanceRole /mnt/var/lib/info/extraInstanceData.json)

    #only log on master to avoid duplication
    if [[ "$INSTANCE_ROLE" != '"MASTER"' ]]; then
        exit 0
    fi

    source /opt/emr/logging.sh

    function log_wrapper_message() {
        log_aws_emr_template_repository_message "$${1}" "status_metrics.sh" "$${PID}" "$${@:2}" "Running as: ,$USER"
    }

  log_wrapper_message "Start running status_metrics.sh Shell"

  STEP_DETAILS_DIR=/mnt/var/lib/info/steps
  CORRELATION_ID_FILE=/opt/emr/correlation_id.txt
  SNAPSHOT_TYPE_FILE=/opt/emr/snapshot_type.txt
  EXPORT_DATE_FILE=/opt/emr/export_date.txt

  DATE=$(date '+%Y-%m-%d')
  CLUSTER_ID=$(jq '.jobFlowId' < /mnt/var/lib/info/job-flow.json)
  CLUSTER_ID="$${CLUSTER_ID//\"}"

  FAILED_STATUS="FAILED"
  COMPLETED_STATUS="COMPLETED"
  CANCELLED_STATUS="CANCELLED"

  FINAL_STEP_NAME="${dynamodb_final_step}"

  while [[ ! -f "$SNAPSHOT_TYPE_FILE" ]] && [[ ! -f "$EXPORT_DATE_FILE" ]]
  do
    sleep 5
  done

  CORRELATION_ID=$(cat $CORRELATION_ID_FILE)
  SNAPSHOT_TYPE=$(cat $SNAPSHOT_TYPE_FILE)
  EXPORT_DATE=$(cat $EXPORT_DATE_FILE)

  if [[ -z "$EXPORT_DATE" ]]; then
    log_wrapper_message "Export date from file was empty, so defaulting to today's date"
    EXPORT_DATE="$DATE"
  fi

  processed_files=()

  push_metric() {
    log_wrapper_message "Sending to push gateway with value $1"

    cat << EOF | curl --data-binary @- "http://${aws_emr_template_repository_pushgateway_hostname}:9091/metrics/job/${emr_cluster_name}"
                aws_emr_template_repository_status{snapshot_type="$SNAPSHOT_TYPE", export_date="$EXPORT_DATE", cluster_id="$CLUSTER_ID", component="AWS_EMR_TEMPLATE_REPOSITORY", correlation_id="$CORRELATION_ID"} $1
EOF
  }

  check_step_dir() {
    cd "$STEP_DETAILS_DIR" || exit
    #shellcheck disable=SC2231
    for i in $STEP_DETAILS_DIR/*.json; do # We want wordsplitting here
      #shellcheck disable=SC2076
      if [[ "$${processed_files[@]}" =~ "$${i}" ]]; then # We do not want a REGEX check here so it is ok
        continue
      fi
      state=$(jq -r '.state' "$i")
      while [[ "$state" != "$COMPLETED_STATUS" ]]; do
        step_script_name=$(jq -r '.args[0]' "$i")
        if [[ "$step_script_name" == "python3" ]]; then
            step_script_name=$(jq -r '.args[1]' "$i")
        fi
        CURRENT_STEP=$(echo "$step_script_name" | sed 's:.*/::' | cut -f 1 -d '.')
        state=$(jq -r '.state' "$i")
        if [[ "$state" == "$FAILED_STATUS" ]] ; then
          log_wrapper_message "Failed step. Step Name: $CURRENT_STEP, Step status: $state"
          push_metric 3
          exit 0
        fi
        if [[ "$state" == "$CANCELLED_STATUS" ]]; then
          log_wrapper_message "Cancelled step. Step Name: $CURRENT_STEP, Step status: $state"
          push_metric 4
          exit 0
        fi
        if [[ "$CURRENT_STEP" == "$FINAL_STEP_NAME" ]] && [[ "$state" == "$COMPLETED_STATUS" ]]; then
          push_metric 2
          log_wrapper_message "All steps completed. Final step Name: $CURRENT_STEP, Step status: $state"
          exit 0
        fi
        if [[ "$PREVIOUS_STATE" != "$state" ]] && [[ "$PREVIOUS_STEP" != "$CURRENT_STEP" ]]; then
          log_wrapper_message "Successful step. Last step name: $PREVIOUS_STEP, Last step status: $PREVIOUS_STATE, Current step name: $CURRENT_STEP, Current step status: $state"
          processed_files+=( "$i" )
        else
          sleep 5
        fi
        PREVIOUS_STATE="$state"
        PREVIOUS_STEP="$CURRENT_STEP"
      done
    done
    check_step_dir
  }

  push_metric 1
  #kick off loop to process all step files
  check_step_dir

) >> /var/log/aws-emr-template-repository/status_metrics.log 2>&1
