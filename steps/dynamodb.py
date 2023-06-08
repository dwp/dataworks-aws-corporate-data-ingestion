import datetime as dt
from typing import Dict, Optional

from botocore.client import BaseClient


class DynamoDBHelper:
    DYNAMO_TABLE = "data_pipeline_metadata"
    IN_PROGRESS = "IN_PROGRESS"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"

    def __init__(
        self,
        client: BaseClient,
        correlation_id,
        collection_name,
        cluster_id,
        run_id=0,
    ):
        self.client = client
        self.correlation_id = correlation_id
        self.collection_name = collection_name
        self.data_product = f"CDI-{collection_name}"
        self.cluster_id = cluster_id
        self.run_id = run_id
        self.KEY = {
            "Correlation_Id": {"S": self.correlation_id},
            "DataProduct": {"S": self.data_product},
        }

    def get_status(self):
        """Returns status of current data product/run, or None if no status found"""
        response = self.client.get_item(TableName=self.DYNAMO_TABLE, Key=self.KEY)

        status = response.get("Item", {}).get("Status", {}).get("S")
        if status and status in (self.FAILED, self.COMPLETED, self.IN_PROGRESS):
            return status

        return

    def update_status(self, status, export_date, extra: Optional[Dict] = None):
        """update the DYNAMO_TABLE with the latest status for the data_product"""
        if not self.get_status():
            """Create the row with required fields"""
            data = {
                **self.KEY,
                "Run_Id": {"S": "0"},
                "Status": {"S": status},
                "Cluster_Id": {"S": self.cluster_id},
                "Date": {"S": export_date},
                "TimeToExist": {"N": str((dt.datetime.now() + dt.timedelta(weeks=52 * 2)).timestamp())},
            }
            data.update(extra if extra else {})
            self.client.put_item(TableName=self.DYNAMO_TABLE, Item=data)
        else:
            """Update existing row with status_field"""
            data = {"Status": {"Value": {"S": status}}}
            data.update(extra if extra else {})
            self.client.update_item(
                TableName=self.DYNAMO_TABLE, Key=self.KEY, AttributeUpdates=data
            )
