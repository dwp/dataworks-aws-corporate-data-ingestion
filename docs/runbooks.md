# Run books

As of 27/03/23 Corporate Data Ingestion is live for data.businessAudit only

The schedule is [here](https://github.com/dwp/dataworks-aws-corporate-data-ingestion/blob/10d17a9df00dd3b53bc9621dbc20bc7f08abe2d9/daily_export_scheduling.tf#L5)

## Known types of failure

1) [Cluster launched and started processing data but encountered an error](#processing_failed)
2) [Cluster launched but failed before starting to process the data](#launch_failed)
3) [Cluster did not launch](#no_launch)

If the cluster launched and started processing data, check the cloudwatch log group `/app/corporate-data-ingestion/step_logs`

### 1.<a id="processing_failed"> </a> Cluster launched and started processing data but encountered an error
> com.amazon.ws.emr.hadoop.fs.shaded.com.amazonaws.services.s3.model.AmazonS3Exception: Not Found (Service: Amazon S3; Status Code: 404; Error Code: 404 Not Found

This is most commonly because the cluster clashed with the [corporate-storage-coalescence](https://github.com/dwp/dataworks-corporate-storage-coalescence) job

The coalescer job runs before CDI to merge small files output in S3 by Kafka-to-hbase (K2HB). 
It creates new, larger files and deletes the old ones.  If it runs at the same time as CDI,
pyspark will scan the files in S3 at the beginning of the job, and then get a 404 when 
accessing them later because they have been removed. See [DTWRKS-888](https://dwpdigital.atlassian.net/browse/DTWRKS-888)

Consider investigating why the coalescer is still running.  It could be one of the following:
- More data is coming through K2HB than usual, resulting in more files
- K2HB is not batching up the data sufficiently & outputting many very small files.  See [DTWRKS-67](https://dwpdigital.atlassian.net/browse/DTWRKS-67)

#### Fix: Use a utility job to relaunch the cluster.
1. Update the CI parameters in `../ci/`
   1. Ensure that the collection name is correct (`data:businessAudit`)
   2. Ensure that the dates are correct in format YYYY-MM-DD; if today's cluster failed, use today's date for start and end
2. Aviator the changes
3. Re-run corporate data ingestion using the Utility Job: `Concourse > dataworks-aws-corporate-data-ingestion > Utility > start-corporate-data-ingestion-production`


### 2) <a id="launch_failed"> </a> Cluster launched but failed before starting to process the data

Barring misconfiguration of the cluster, this could be due to resource contention.  Consider
re-running, and if still a problem then try a different instance type.  The main factor is the amount of memory available which determines how many executors can run.

### 3) <a id="no_launch"> </a> Cluster did not launch

The cluster is currently set to run every morning at 9:30AM UTC.  There is no adjustment to the schedule for BST,
and it will appear to run 1 hour late during BST.

The cluster is launched by a cloudwatch_event_rule resource which sends an SNS message, triggering the 
CDI emr-launcher: 
- Check the cloudwatch event rule in the AWS console to see if it triggered
- Check the CDI emr-launcher log group to see if any errors were encountered