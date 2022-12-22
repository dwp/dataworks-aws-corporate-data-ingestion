import datetime
import json
import time
import uuid
from dataclasses import dataclass
from typing import Dict

import boto3
from boto3 import Session


@dataclass
class EMRConfig:
    aws_session: Session
    emr_launcher_name: str
    emr_launcher_payload: str


class EMRService:
    EMR_TERMINATING = ("TERMINATING", "TERMINATED", "TERMINATED_WITH_ERRORS")
    EMR_WAITING = ("WAITING",)

    def __init__(self, configuration: EMRConfig):
        self._session = configuration.aws_session
        self._emr_client = self._session.client("emr")
        self._emr_launcher_name = configuration.emr_launcher_name
        self._emr_launcher_payload = configuration.emr_launcher_payload
        self._cluster_id = None
        self._steps = 0

    def poll_cluster(self, cluster_id, timeout=600, expected_statuses=EMR_WAITING, unexpected_statuses=EMR_TERMINATING):
        emr_client = self._emr_client
        end_time = time.time() + timeout

        while time.time() <= end_time:
            waited = int(round(time.time() - (end_time - timeout)))
            state = emr_client.describe_cluster(ClusterId=cluster_id)["Cluster"]["Status"]["State"]

            if state in expected_statuses:
                print(f"Cluster reached expected state: {state}")
                return state
            elif state in unexpected_statuses:
                raise ValueError(f"Cluster reached unexpected state: {state}")
            else:
                print(f"State: {state} - waited {waited}/{timeout}s")
                time.sleep(10)
                continue

        raise TimeoutError(f"EMR Cluster did not reach 'WAITING' state within timeout: {timeout}s")

    def poll_step(
            self,
            step_id,
            cluster_id,
            timeout=600,
            expected_statuses=("COMPLETED",),
            unexpected_statuses=("CANCELLED", "FAILED", "INTERRUPTED")
    ):
        emr_client = self._emr_client
        start_time = time.time()
        end_time = start_time + timeout

        while time.time() <= end_time or timeout is None:
            waited = int(time.time() - start_time)
            step_state = emr_client.describe_step(ClusterId=cluster_id, StepId=step_id)["Step"]["Status"]["State"]

            if step_state in expected_statuses:
                print(f"Step reached expected state: {step_state}")
                return
            elif step_state in unexpected_statuses:
                raise ValueError(f"Step reached unexpected state: {step_state}")
            else:
                print(f"Step state: {step_state} - waited {waited}/{timeout}s")
                time.sleep(10)
                continue

        raise TimeoutError(f"Step did not reach 'WAITING' state within timeout: {timeout}s")

    def _launch_cluster(self, wait=True, timeout=900):
        print("Invoking emr-launcher")
        lambda_client = self._session.client("lambda")
        response = lambda_client.invoke(
            FunctionName=self._emr_launcher_name,
            Payload=self._emr_launcher_payload,
        )

        payload = response["Payload"].read()
        cluster_id = json.loads(payload.decode()).get("JobFlowId")

        print(f"Launching Cluster: {cluster_id}")
        if wait:
            self.poll_cluster(cluster_id)

        self._cluster_id = cluster_id
        return self._cluster_id

    def _terminate_cluster(self, cluster_id=None, wait=True, timeout=300):
        if not cluster_id:
            cluster_id = self._cluster_id
        print(f"Terminating cluster: {cluster_id}")
        self._emr_client.terminate_job_flows(JobFlowIds=[cluster_id])
        if wait:
            self.poll_cluster(
                cluster_id,
                expected_statuses=["TERMINATED", "TERMINATED_WITH_ERRORS"],
                unexpected_statuses=[],
            )

    def _submit_single_step(self, step, wait=False, timeout=500):
        print("Submitting step")

        response = self._emr_client.add_job_flow_steps(
            JobFlowId=self._cluster_id,
            Steps=[step]
        )
        step_id = response["StepIds"][0]

        if wait:
            self.poll_step(step_id=step_id, cluster_id=self._cluster_id)

        return step_id

def generate_step(source_s3_prefix, destination_s3_prefix, export_date) -> Dict:
    """ Generates the "step" dictionary to be submitted to an EMR cluster.  Specific to the data.businessAudit and the
     corporate-data-ingestion job. Export date of 05/12/22 will process all data from the previous day -
     within the prefix "{s3_prefix}/22/12/04" and place it in the hive partition "22-12-05"
     Export date == ingest_date + 1

    :param source_s3_prefix: containing multiple date-organised folders
    :param export_date: the "export date" used by original pipeline
    :return: step_dict: dictionary describing a step to be submitted to EMR cluster
    """
    export_dt = datetime.datetime.strptime(export_date, "%Y-%m-%d")
    ingest_dt = export_dt - datetime.timedelta(days=1)
    source_s3_prefix = "{prefix}/{year}/{month}/{day}/data/businessAudit".format(
        prefix=source_s3_prefix,
        year=ingest_dt.year,
        month=ingest_dt.month,
        day=ingest_dt.day,
    )
    # todo: validate parameters below, (databases used, prefixes)
    return {
        "Name": f"corporate-data-ingest::export-date::{export_date}",
        "ActionOnFailure": "TERMINATE_CLUSTER",
        "HadoopJarStep": {
            "Jar": "command-runner.jar",
            "Args": [
                "spark-submit",
                "/opt/emr/steps/corporate_data_ingestion.py",
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
            ]
        }
    }


if __name__ == "__main__":
    config = EMRConfig(
        aws_session=boto3.Session(profile_name="dataworks-development"),
        emr_launcher_name="corporate_data_ingestion_emr_launcher",
        emr_launcher_payload=json.dumps({
            "s3_overrides": None,
            "overrides": {"Instances": {"KeepJobFlowAliveWhenNoSteps": True}, "Steps": []},
            "extend": None,
            "additional_step_args": None,
        })
    )
