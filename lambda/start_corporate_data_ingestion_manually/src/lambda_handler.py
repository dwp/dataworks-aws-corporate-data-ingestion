import datetime
import json
import time
import logging
import sys
import uuid
import os
from dataclasses import dataclass
from typing import Dict
from botocore.exceptions import ClientError

import boto3
from boto3 import Session

# Initialise logging
logger = logging.getLogger(__name__)
log_level = os.environ.get("LOG_LEVEL", "INFO")
logger.setLevel(logging.getLevelName(log_level))
logging.basicConfig(
    stream=sys.stdout,
    format="%(asctime)s %(levelname)s %(module)s "
    "%(process)s[%(thread)s] %(message)s",
)
logger.info(f"Logging at {log_level} level")


@dataclass
class EMRConfig:
    aws_session: Session
    emr_launcher_name: str
    emr_launcher_payload: str


class EMRService:
    EMR_TERMINATING = (
        "TERMINATING",
        "TERMINATED",
        "TERMINATED_WITH_ERRORS",
    )
    EMR_WAITING = ("WAITING",)

    def __init__(self, configuration: EMRConfig):
        self._session = configuration.aws_session
        self._emr_client = self._session.client("emr")
        self._emr_launcher_name = configuration.emr_launcher_name
        self._emr_launcher_payload = configuration.emr_launcher_payload
        self._cluster_id = None
        self._steps = 0

    def poll_cluster(
        self,
        cluster_id,
        timeout=600,
        expected_statuses=EMR_WAITING,
        unexpected_statuses=EMR_TERMINATING,
    ):
        emr_client = self._emr_client
        end_time = time.time() + timeout

        while time.time() <= end_time:
            waited = int(round(time.time() - (end_time - timeout)))
            state = exponential_backoff(
                method_reference=emr_client.describe_cluster,
                method_arguments={"ClusterId": cluster_id},
            )["Cluster"]["Status"]["State"]

            if state in expected_statuses:
                logger.info(f"Cluster reached expected state: {state}")
                return state
            elif state in unexpected_statuses:
                raise ValueError(f"Cluster reached unexpected state: {state}")
            else:
                logger.info(f"State: {state} - waited {waited}/{timeout}s")
                time.sleep(10)
                continue

        raise TimeoutError(
            f"EMR Cluster did not reach 'WAITING' state within timeout: {timeout}s"
        )

    def poll_step(
        self,
        step_id,
        cluster_id,
        timeout=600,
        expected_statuses=("COMPLETED",),
        unexpected_statuses=("CANCELLED", "FAILED", "INTERRUPTED"),
    ):
        emr_client = self._emr_client
        start_time = time.time()
        end_time = start_time + timeout

        while time.time() <= end_time or timeout is None:
            waited = int(time.time() - start_time)
            step_state = exponential_backoff(
                method_reference=emr_client.describe_step,
                method_arguments={"ClusterId": cluster_id, "StepId": step_id},
            )["Step"]["Status"]["State"]

            if step_state in expected_statuses:
                logger.info(f"Step reached expected state: {step_state}")
                return
            elif step_state in unexpected_statuses:
                raise ValueError(f"Step reached unexpected state: {step_state}")
            else:
                logger.info(f"Step state: {step_state} - waited {waited}/{timeout}s")
                time.sleep(10)
                continue

        raise TimeoutError(
            f"Step did not reach 'WAITING' state within timeout: {timeout}s"
        )

    def launch_cluster(self, wait=True, timeout=900):
        logger.info("Invoking emr-launcher")
        lambda_client = self._session.client("lambda")
        response = lambda_client.invoke(
            FunctionName=self._emr_launcher_name, Payload=self._emr_launcher_payload,
        )

        payload = response["Payload"].read()
        cluster_id = json.loads(payload.decode()).get("JobFlowId")

        logger.info(f"Launching Cluster: {cluster_id}")
        if wait:
            self.poll_cluster(cluster_id)

        self._cluster_id = cluster_id
        return self._cluster_id

    def terminate_cluster(self, cluster_id=None, wait=True, timeout=300):
        if not cluster_id:
            cluster_id = self._cluster_id
        logger.info(f"Terminating cluster: {cluster_id}")
        self._emr_client.terminate_job_flows(JobFlowIds=[cluster_id])
        if wait:
            self.poll_cluster(
                cluster_id,
                expected_statuses=["TERMINATED", "TERMINATED_WITH_ERRORS"],
                unexpected_statuses=[],
            )

    def _submit_single_step(self, step, wait=False, timeout=500):
        logger.info(f"Submitting step: {step['Name']}")

        response = self._emr_client.add_job_flow_steps(
            JobFlowId=self._cluster_id, Steps=[step]
        )
        step_id = response["StepIds"][0]

        if wait:
            self.poll_step(step_id=step_id, cluster_id=self._cluster_id)

        return step_id

    def process_date_or_range_of_dates(self, export_date_or_range, source_s3_prefix, destination_s3_prefix):
        if ";" in export_date_or_range:
            logger.info(f"Processing range of dates: {export_date_or_range}, "
                        f"source: {source_s3_prefix}, destination: {destination_s3_prefix}")
            export_date_or_range = export_date_or_range.split(";")
            start = datetime.datetime.strptime(export_date_or_range[0], "%Y-%m-%d")
            end = datetime.datetime.strptime(export_date_or_range[1], "%Y-%m-%d")
            export_date_or_range = [
                (start + datetime.timedelta(days=x)).strftime("%Y-%m-%d")
                for x in range(0, (end - start).days + 1)
            ]
        else:
            logger.info(f"Processing single date: {export_date_or_range}")
            export_date_or_range = [export_date_or_range]

        for date in export_date_or_range:
            step = generate_step(
                source_s3_prefix=source_s3_prefix,
                destination_s3_prefix=destination_s3_prefix,
                export_date=date,
            )
            self._submit_single_step(step)


def generate_step(source_s3_prefix, destination_s3_prefix, export_date) -> Dict:
    """ Generates the "step" dictionary to be submitted to an EMR cluster.  Specific to the data.businessAudit and the
     corporate-data-ingestion job. Export date of 05/12/22 will process all data from the previous day -
     within the prefix "{s3_prefix}/22/12/04" and place it in the hive partition "22-12-05"
     Export date == ingest_date + 1

    :param source_s3_prefix: containing multiple date-organised folders
    :param destination_s3_prefix: will contain the lzo-compressed output file. Existing content will be purged from the prefix
    :param export_date: the "export date" used by original pipeline
    :return: step_dict: dictionary describing a step to be submitted to EMR cluster
    """
    export_dt = datetime.datetime.strptime(export_date, "%Y-%m-%d")
    ingest_dt = export_dt - datetime.timedelta(days=1)
    source_s3_prefix = (
        f"{source_s3_prefix}/{ingest_dt.strftime('%Y/%m/%d')}/data/businessAudit"
    )
    return {
        "Name": f"corporate-data-ingest::export-date::{export_date}",
        "ActionOnFailure": "TERMINATE_CLUSTER",
        "HadoopJarStep": {
            "Jar": "command-runner.jar",
            "Args": [
                "spark-submit",
                "/opt/emr/steps/corporate-data-ingestion.py",
                "--correlation_id",
                f"{uuid.uuid4()}_export_{export_date}",
                "--export_date",
                export_date,
                "--source_s3_prefix",
                source_s3_prefix,
                "--destination_s3_prefix",
                destination_s3_prefix,
                "--transition_db_name",
                "dwx_audit_transition",
                "--db_name",
                "dwx_audit_transition",
            ],
        },
    }


def exponential_backoff(
    method_reference,
    method_arguments,
    time_delay_in_seconds=10,
    max_consecutive_adverse_events=10,
    adverse_events_counter=0,
):
    try:
        return method_reference(**method_arguments)
    except ClientError as err:
        if err.response["Error"]["Code"] == "ThrottlingException":
            if adverse_events_counter >= max_consecutive_adverse_events:
                raise TimeoutError(
                    f"AWS EMR API rate exceeded in spite of exponential backoff: {err}"
                )
            else:
                logger.info(
                    f"AWS EMR API rate exceeded. Exponential backoff {time_delay_in_seconds}s"
                    f" ({adverse_events_counter}/{max_consecutive_adverse_events})"
                )
                time.sleep(time_delay_in_seconds * adverse_events_counter)
                exponential_backoff(
                    method_reference, method_arguments, adverse_events_counter + 1
                )


def lambda_handler(event, context):
    export_date_or_range = os.environ.get("EXPORT_DATE_OR_RANGE")
    source_s3_prefix = os.environ.get("SOURCE_S3_PREFIX")
    destination_s3_prefix = os.environ.get("DESTINATION_S3_PREFIX")

    config = EMRConfig(
        aws_session=boto3.Session(),
        emr_launcher_name="corporate_data_ingestion_emr_launcher",
        emr_launcher_payload=json.dumps(
            {
                "s3_overrides": None,
                "overrides": {
                    "Instances": {"KeepJobFlowAliveWhenNoSteps": False},
                    "Steps": [],
                },
                "extend": None,
                "additional_step_args": None,
            }
        ),
    )
    service = EMRService(configuration=config)
    service.launch_cluster()
    service.process_date_or_range_of_dates(export_date_or_range, source_s3_prefix, destination_s3_prefix)


if __name__ == "__main__":
    logger.info("Lambda start_corporate_data_ingestion_manually started")
    json_content = json.loads(open("event.json", "r").read())
    lambda_handler(json_content, None)
