---
Configurations:
- Classification: "spark"
  Properties:
    "maximizeResourceAllocation": "false"

- Classification: "yarn-site"
  Properties:
    "yarn.log-aggregation-enable": "true"
    "yarn.nodemanager.remote-app-log-dir": "s3://${s3_log_bucket}/${s3_log_prefix}/yarn"
    "yarn.nodemanager.vmem-check-enabled": "false"
    "yarn.nodemanager.pmem-check-enabled": "false"

- Classification: "spark-defaults"
  Properties:
    "spark.yarn.jars": "/usr/lib/spark/jars/*,/opt/emr/metrics/dependencies/*"
    "spark.sql.catalogImplementation": "hive"
    "spark.yarn.dist.files": "/etc/spark/conf/hive-site.xml,/etc/pki/tls/private/private_key.key,/etc/pki/tls/certs/private_key.crt,/etc/pki/ca-trust/source/anchors/analytical_ca.pem"
    "spark.executor.extraClassPath": "/usr/lib/hadoop-lzo/lib/*:/usr/lib/hadoop/hadoop-aws.jar:/usr/share/aws/aws-java-sdk/*:/usr/share/aws/emr/emrfs/conf:/usr/share/aws/emr/emrfs/lib/*:/usr/share/aws/emr/emrfs/auxlib/*:/usr/share/aws/emr/goodies/lib/emr-spark-goodies.jar:/usr/share/aws/emr/security/conf:/usr/share/aws/emr/security/lib/*:/usr/share/aws/hmclient/lib/aws-glue-datacatalog-spark-client.jar:/usr/share/java/Hive-JSON-Serde/hive-openx-serde.jar:/usr/share/aws/sagemaker-spark-sdk/lib/sagemaker-spark-sdk.jar:/usr/share/aws/emr/s3select/lib/emr-s3-select-spark-connector.jar:/opt/emr/metrics/dependencies/*"
    "spark.driver.extraClassPath": "/usr/lib/hadoop-lzo/lib/*:/usr/lib/hadoop/hadoop-aws.jar:/usr/share/aws/aws-java-sdk/*:/usr/share/aws/emr/emrfs/conf:/usr/share/aws/emr/emrfs/lib/*:/usr/share/aws/emr/emrfs/auxlib/*:/usr/share/aws/emr/goodies/lib/emr-spark-goodies.jar:/usr/share/aws/emr/security/conf:/usr/share/aws/emr/security/lib/*:/usr/share/aws/hmclient/lib/aws-glue-datacatalog-spark-client.jar:/usr/share/java/Hive-JSON-Serde/hive-openx-serde.jar:/usr/share/aws/sagemaker-spark-sdk/lib/sagemaker-spark-sdk.jar:/usr/share/aws/emr/s3select/lib/emr-s3-select-spark-connector.jar:/opt/emr/metrics/dependencies/*"
    "spark.executor.defaultJavaOptions": "-XX:+UseG1GC -XX:+UnlockDiagnosticVMOptions -XX:+G1SummarizeConcMark -XX:InitiatingHeapOccupancyPercent=35 -verbose:gc -XX:+PrintGCDetails -XX:+PrintGCDateStamps -XX:OnOutOfMemoryError='kill -9 %p' -Dhttp.proxyHost='${proxy_http_host}' -Dhttp.proxyPort='${proxy_http_port}' -Dhttp.nonProxyHosts='${proxy_no_proxy}' -Dhttps.proxyHost='${proxy_https_host}' -Dhttps.proxyPort='${proxy_https_port}'"
    "spark.driver.defaultJavaOptions": "-XX:+UseG1GC -XX:+UnlockDiagnosticVMOptions -XX:+G1SummarizeConcMark -XX:InitiatingHeapOccupancyPercent=35 -verbose:gc -XX:+PrintGCDetails -XX:+PrintGCDateStamps -XX:OnOutOfMemoryError='kill -9 %p' -Dhttp.proxyHost='${proxy_http_host}' -Dhttp.proxyPort='${proxy_http_port}' -Dhttp.nonProxyHosts='${proxy_no_proxy}' -Dhttps.proxyHost='${proxy_https_host}' -Dhttps.proxyPort='${proxy_https_port}'"
    "spark.serializer": "org.apache.spark.serializer.KryoSerializer"
    "spark.kryoserializer.buffer.max": "${spark_kyro_buffer}"
    "spark.driver.maxResultSize": "1g"
    "spark.sql.warehouse.dir": "s3://${s3_published_bucket}/corporate_data_ingestion/hive/external"
    "spark.dynamicAllocation.enabled": "false"
    "spark.executor.cores": "${spark_executor_cores}"
    "spark.executor.memory": "${spark_executor_memory}g"
    "spark.executor.memoryOverhead": "${spark_yarn_executor_memory_overhead}g"
    "spark.driver.memory": "${spark_driver_memory}g"
    "spark.driver.cores": "${spark_driver_cores}"
    "spark.executor.instances": "${spark_executor_instances}"
    "spark.default.parallelism": "${spark_default_parallelism}"
    "spark.sql.shuffle.partitions": "${spark_sql_shuffle_partitions}"
    "spark.rdd.compress": "true"
    "spark.shuffle.compress": "true"
    "spark.shuffle.spill.compress": "true"
    "spark.memory.fraction": "0.80"
    "spark.memory.storageFraction": "0.30"
    "spark.network.timeout": "800s"
    "spark.executor.heartbeatInterval": "60s"
    "spark.sql.autoBroadcastJoinThreshold": "-1"

- Classification: "spark-hive-site"
  Properties:
    "hive.txn.manager": "org.apache.hadoop.hive.ql.lockmgr.DbTxnManager"
    "hive.enforce.bucketing": "true"
    "hive.exec.dynamic.partition.mode": "nonstrict"
    "hive.compactor.initiator.on": "false"
    "hive.compactor.worker.threads": "1"
    "hive.support.concurrency": "true"
    "javax.jdo.option.ConnectionURL": "jdbc:mysql://${hive_metastore_endpoint}:3306/${hive_metastore_database_name}"
    "javax.jdo.option.ConnectionDriverName": "org.mariadb.jdbc.Driver"
    "javax.jdo.option.ConnectionUserName": "${hive_metastore_username}"
    "javax.jdo.option.ConnectionPassword": "${hive_metastore_pwd}"
    "hive.metastore.client.socket.timeout": "7200"
    "hive.stats.autogather": "false"

- Classification: "hive-site"
  Properties:
    "hive.metastore.warehouse.dir": "s3://${s3_published_bucket}/corporate_data_ingestion/hive/external"
    "hive.txn.manager": "org.apache.hadoop.hive.ql.lockmgr.DbTxnManager"
    "hive.enforce.bucketing": "true"
    "hive.exec.dynamic.partition.mode": "nonstrict"
    "hive.compactor.initiator.on": "false"
    "hive.compactor.worker.threads": "1"
    "hive.support.concurrency": "true"
    "javax.jdo.option.ConnectionURL": "jdbc:mysql://${hive_metastore_endpoint}:3306/${hive_metastore_database_name}?createDatabaseIfNotExist=true"
    "javax.jdo.option.ConnectionDriverName": "org.mariadb.jdbc.Driver"
    "javax.jdo.option.ConnectionUserName": "${hive_metastore_username}"
    "javax.jdo.option.ConnectionPassword": "${hive_metastore_pwd}"
    "hive.metastore.client.socket.timeout": "7200"
    "hive.mapred.mode": "nonstrict"
    "hive.strict.checks.cartesian.product": "false"
    "hive.exec.parallel": "true"
    "hive.exec.parallel.thread.number": "32"
    "hive.exec.failure.hooks": "org.apache.hadoop.hive.ql.hooks.ATSHook"
    "hive.exec.post.hooks": "org.apache.hadoop.hive.ql.hooks.ATSHook"
    "hive.exec.pre.hooks": "org.apache.hadoop.hive.ql.hooks.ATSHook"
    "hive.vectorized.execution.enabled": "false"
    "hive.vectorized.execution.reduce.enabled": "false"
    "hive.vectorized.complex.types.enabled": "false"
    "hive.vectorized.use.row.serde.deserialize": "false"
    "hive.vectorized.execution.ptf.enabled": "false"
    "hive.vectorized.row.serde.inputformat.excludes": ""
    "hive_timeline_logging_enabled": "true"
    "hive.blobstore.optimizations.enabled": "false"
    "hive.prewarm.enabled": "true"
    "hive.auto.convert.join": "true"
    "hive.auto.convert.join.noconditionaltask.size": "4915"
    "hive.merge.tezfiles": "true"
    "hive.merge.smallfiles.avgsize": "128000000"
    "hive.merge.size.per.task": "128000000"

- Classification: "emrfs-site"
  Properties:
    "fs.s3.maxConnections": "10000"
    "fs.s3.maxRetries": "20"

- Classification: "spark-env"
  Configurations:
  - Classification: "export"
    Properties:
      "PYSPARK_PYTHON": "/usr/bin/python3"
      "S3_PUBLISHED_BUCKET": "${s3_published_bucket}"
      "S3_CORPORATE_BUCKET": "${s3_corporate_bucket}"

- Classification: "hadoop-env"
  Configurations:
  - Classification: "export"
    Properties:
      "HADOOP_NAMENODE_OPTS": "\"-javaagent:/opt/emr/metrics/dependencies/jmx_prometheus_javaagent-0.14.0.jar=7101:/opt/emr/metrics/prometheus_config.yml\""
      "HADOOP_DATANODE_OPTS": "\"-javaagent:/opt/emr/metrics/dependencies/jmx_prometheus_javaagent-0.14.0.jar=7103:/opt/emr/metrics/prometheus_config.yml\""

- Classification: "yarn-env"
  Configurations:
  - Classification: "export"
    Properties:
      "YARN_RESOURCEMANAGER_OPTS": "\"-javaagent:/opt/emr/metrics/dependencies/jmx_prometheus_javaagent-0.14.0.jar=7105:/opt/emr/metrics/prometheus_config.yml\""
      "YARN_NODEMANAGER_OPTS": "\"-javaagent:/opt/emr/metrics/dependencies/jmx_prometheus_javaagent-0.14.0.jar=7107:/opt/emr/metrics/prometheus_config.yml\""
