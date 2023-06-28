# Troubleshooting Failures

### Summary
There are several places to look for logs and indications of why the cluster failed.
- The pyspark application (in `steps/`) produces logs in cloudwatch
- The spark history server surfaces statuses/statistics for spark applications, stages and tasks
- Spark executor logs are available via Spark history server
- Hadoop application logs (incl. spark executor logs) are available in S3


### Proposed troubleshooting approach

#### 1. Check Cloudwatch
When a step on the cluster has failed, the first place to look is cloudwatch 
(log group `/app/corporate-data-ingestion/step_logs`)

This will surface some information from spark to indicate why the failure has 
occurred, such as:
```
2023-06-28 14:51:53,768 corporate-data-ingestion ERROR 
AnalysisException('Path does not exist: s3://<bucketredacted>/corporate_data_ingestion/exports/calculator/calculationParts/2023-06-27', 
    'org.apache.spark.sql.AnalysisException: Path does not exist: s3://<bucketredacted>/corporate_data_ingestion/exports/calculator/calculationParts/2023-06-27
        at org.apache.spark.sql.execution.datasources.DataSource$.$anonfun$checkAndGlobPathIfNecessary$4(DataSource.scala:806)
        at org.apache.spark.sql.execution.datasources.DataSource$.$anonfun$checkAndGlobPathIfNecessary$4$adapted(DataSource.scala:803)
        at org.apache.spark.util.ThreadUtils$.$anonfun$parmap$2(ThreadUtils.scala:372)
        at scala.concurrent.Future$.$anonfun$apply$1(Future.scala:659)
        at scala.util.Success.$anonfun$map$1(Try.scala:255)
        at scala.util.Success.map(Try.scala:213)
        at scala.concurrent.Future.$anonfun$map$1(Future.scala:292)
        at scala.concurrent.impl.Promise.liftedTree1$1(Promise.scala:33)
        at scala.concurrent.impl.Promise.$anonfun$transform$1(Promise.scala:33)
        at scala.concurrent.impl.CallbackRunnable.run(Promise.scala:64)
        at java.util.concurrent.ForkJoinTask$RunnableExecuteAction.exec(ForkJoinTask.java:1402)
        at java.util.concurrent.ForkJoinTask.doExec(ForkJoinTask.java:289)
        at java.util.concurrent.ForkJoinPool$WorkQueue.runTask(ForkJoinPool.java:1056)
        at java.util.concurrent.ForkJoinPool.runWorker(ForkJoinPool.java:1692)
        at java.util.concurrent.ForkJoinWorkerThread.run(ForkJoinWorkerThread.java:175)
', None)
```

#### 2. Check the step logs
In the EMR console, under the 'steps' tab, check the `controller, stderr, stdout` logs
occasionally these will contain useful information at the end of the file

#### 3. Check the spark history server
If steps 1/2 don't provide clear information about the failure, you can check
the spark history server.

The spark history server shows information in a hierarchy:
- application > job > stage > task

Each step on the cluster will likely have its own application
Each instruction to pyspark (i.e. action on a dataframe/RDD such as 
`.write`, `.saveAsTextFile`) will have its own job

Each job is divided into stages and tasks by spark.  Tasks are assigned to executors, 
and a single executor's logfile will contain the logs for several tasks

Diving into the executor view, you can see if tasks are failing.  Identify an executor
with failed tasks, and check the logs for that executor.


#### 3. Check other hadoop application logs
If there are no failed tasks, or the previous sets of logs don't explain the behaviour,
EMR collects logs from a number of applications on the cluster and publishes on S3.
Check the EMR console to find the "Log destination in Amazon S3" for the cluster, 
or navigate to the following:
> s3://`<log_bucket>`/emr/corporate-data-ingestion/`<cluster_id>`/node/`<master_ec2_instance_id>`/applications/

The most interesting/useful logs are in the following subdirectories:
- hadoop-yarn > resource allocation, indication of containers dying
- hive > errors here when hive-managed tables failed to drop previously

You can also find the application logs for all other instances:
> s3://<log_bucket>/emr/corporate-data-ingestion/<cluster_id>/node/<ec2_instance_id>