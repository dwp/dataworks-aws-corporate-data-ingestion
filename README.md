# dataworks-aws-corporate-data-ingestion

The corporate data ingestion pipeline is built to regularly ingest data from the corporate storage bucket.
The *.json.gz files are read, decompressed, decrypted and stored in the published bucket.  The cluster performs
similar steps to the HTME and ADG infrastructure.

![corporate-data-ingestion.png](docs%2Fcorporate-data-ingestion.png)

<!-- TOC -->
* [dataworks-aws-corporate-data-ingestion](#dataworks-aws-corporate-data-ingestion)
  * [Concourse pipeline](#concourse-pipeline)
    * [Admin jobs](#admin-jobs)
      * [Start cluster](#start-cluster)
      * [Stop clusters](#stop-clusters)
  * [AMI tests](#ami-tests)
  * [Code structure](#code-structure)
  * [Cluster & PySpark application configuration](#cluster--pyspark-application-configuration)
  * [Execution](#execution)
  * [Ingesters](#ingesters)
    * [How do we achieve abstraction and reusability?](#how-do-we-achieve-abstraction-and-reusability)
    * [RDD or DataFrame?](#rdd-or-dataframe)
    * [Further reading](#further-reading)
<!-- TOC -->


## Concourse pipeline

There is a concourse pipeline for dataworks-aws-corporate-data-ingestion named `dataworks-aws-corporate-data-ingestion`. The code for this pipeline is in the `ci` folder. The main part of the pipeline (the `master` group) deploys the infrastructure and runs the e2e tests. There are a number of groups for rotating passwords and there are also admin groups for each environment.

### Admin jobs

Any jobs that require the use of aviator, e.g. starting and stopping clusters need to be added to the [dataworks-admin-utils repository](https://github.com/dwp/dataworks-admin-utils). An example of existing admin jobs for the `aws-clive` data product can be [seen here](https://ci.dataworks.dwp.gov.uk/teams/utility/pipelines/aws-clive)

#### Start cluster

This job will start a dataworks-aws-corporate-data-ingestion cluster.

#### Stop clusters

For stopping clusters, you can run the `stop-cluster` job.  If no cluster ID is specified in the yaml, it will terminate ALL current `dataworks-aws-corporate-data-ingestion` clusters in the environment.

## AMI tests

There is an automated AMI upgrade pipeline embedded into the pipeline of this repo (ci/jobs/ami-test). This is in a serial_group with the QA deployment pipeline to ensure that the AMI-tests do not interfere with the repo's deployment pipeline.

Please let the tests run and the deployment pipeline will continue automatically.

Additionally, creation of new EMR infrastructure means that there is a new consumer of the al2-emr-ami. The required templates for running the tests are already built into ci/jobs/ami-test. However, any new consumers of this AMI will also have to make a change to the [ami-builder-configs](https://github.com/dwp/ami-builder-configs) repository.

Example of resource needed for AMI-tests from `aws-clive`:


    - name: aws-clive
      .: (( inject meta.resources.ami-builder-configs ))
        source:
          paths:
            - results/aws-clive.test

And this resource will need to be used as an input for the test as [seen here](https://github.com/dwp/ami-builder-configs/blob/74c0ab96fb968f47d17f367810f75bb49d84395b/ci/infra/jobs/build_amis/dw-al2-emr-ami.yml#L34-L66)

Any resources needed for enabling the AMI-test require to have already been vetted by the normal deployment pipeline. Therefore, each resource requires a `passed` attribute:

    - get: ANY-RESOURCE
      trigger: false
      passed:
        - dataworks-aws-corporate-data-ingestion-qa

## Code structure

```
dataworks-aws-corporate-data-ingestion/
   boostrap_actions/
   ci/ -> Concourse files
       meta.yml -> Defines the behaviour for each Concourse jobs
       groups.yml -> Organises jobs into groups in the Concourse UI
       jobs/ -> Definition of the CI pipelines for each environment
          utility/ -> Definition of utility pipeline for each environment
              start-backup.yml -> Used as a one-off to perform a full backup of a few large Hive tables
              start-corporate-data-ingestion.yml -> Regularly used to manually trigger the ingestion/processing of a collection

   cluster_config/ -> The EMR launcher lambda defined in `emr-launcher.tf` uses those file to configure the clusters. You can dynamically change the cluster configuration by supplying `s3_overrides`, `overrides`, `extend` or `additional_step_args` in the event body sent to the lambda. (More details here: https://github.com/dwp/emr-launcher#what-does-it-do).
   modules/ -> Contains everything needed to run a one-off backup of a S3 bucket against an S3 inventory. We used this code once, as a precaution, before productionising the pipeline for the first time.
   steps/ -> Contains all the file composing the PySpark application we run on the cluster
      corporate_data_ingestion.py -> application entrypoint. Contains main function, argument parser and logic for ingesters selection, iterations and parallel scheduling of steps
      hive.py -> utily object and function to use Spark through Hive SQL
      ingesters.py -> code for BaseIngester class and derived classes
      logger.py -> common logger configuration
      dks.py -> handle interaction with dks including retrieval of decryption keys and decryption of dbObject

   emr-luncher.tf -> definition of resources needed to create the lambda responsible for creating and starting corporate-data-ingestion EMR clusters and related tasks
   daily_export_scheduling.tf -> definition of daily run schedule
   cluster_config.tf -> save cluster configuration files to S3 with interpolated values from locals and variables
   published_bucket.tf -> IAM policy describing cluster role permission to the pubished_bucket
   corporate_bucket.tf -> IAM policy describing cluster role permission to the corporate_bucket
   emr_jobflow_role.tf -> describe cluster permission
   outputs.tf -> outputs from the module. can be reference by module higher in the repository hierarchy (UCDS Github -> aws-common-infrastructure -> wiki -> Terraform-Repository-Hierarchy)
   locals.tf -> local configuration, this is where most of the behaviour of the cluster is parameterised. Some configuration variables can also be found in variables.tf
```

## Cluster & PySpark application configuration
The cluster is configured by the files in the folder `cluster_config`. 
The PySpark application running on the cluster is contained in the folder `steps`. There are two ways to configure the
behaviour of the application.

1. Using steps/configuration.json
   This file is copied to S3 and its values interpolated by `steps.tf`. It is then copied to the cluster by the bootstrap
   action `bootstrap_actions/download_scripts.sh`

2. Using CLI parameter. The PySpark application `steps/corporate_data_ingestion.py` parses its CLI arguments and adds them to a `Configuration` object

In both cases, the values are stored in the `Configuration` object defined in `steps/data.py`.
This object gets created in the main function (steps/corporate_data_ingestion.py) and is then passed to the subsequent calls including the Ingesters' `run()` methods.
It allows a great centralisation of configuration.

## Execution
The function `process_collection` parses the parameter `export_date`, instantiates an Ingester for the collection it received as
parameter with a Configuration object that includes the `export_date` given as parameter. It then executes the `run()` method
on the instantiated ingester, starting the processing of the collection. (Note: when the `export_date` contains a range of
dates, the `process_collection` function iterates through each date and applies the above for each one of them.)

## Ingesters

### How do we achieve abstraction and reusability?
steps/ingesters.py contains our ingestion classes. We use oriented object programming (OOP) to reuse logic between collection thus reducing code repetition and cost of refactoring.
Specifically, the class `BaseIngester` has a method `run()` which handles moving files from one location in S3 to another and performing the following actions ("on-the-fly"):
- decode from base64 to utf8
- query DKS using the encryption metadata stored in each record and use the returned decryption key to decrypt the `dbOjbect` key (use caching to avoid querying DKS when the encryption metadata are reused in multiple records)
- identify and mark corrupted record (missing fields, badly formatted date etc)
- reformat date fields into a Hive-supported format
- perform various sanitisations and small transformations

To reiterate, the `BaseIngester` class performs the actions above because we consider them common to all the collections. The dictionary `ingesters` defined in `steps/corporate_data_ingestion.py`
maps collection names with their associated ingester.

Example:

    ingesters = {
      "data:businessAudit": BusinessAuditIngester,
      "calculator:calculationParts": BaseIngester,
    }

To sum up, when the default ingestion behaviour defined in the `run()` method of the BaseIngester class is not suited
for a particular collection, we create a derived class from `BaseIngester` an overwrite the `run()` method.
In the overwritten version of `run()` we have the choice to call the parent implementation of `run()` like this
`super(MyNewIngester, self).run()` followed by additional logic or omit to call the parent implementation altogether
and write a completely new implementation. The ingester `BusinessAuditIngester` is a code example as it inherits from
`BaseIngester` call its parent `run()` implementation then call a method to execute a few Hive statements.
Here what it looks like:

    def run(self):
      super(BusinessAuditIngester, self).run()
      self.execute_hive_statements()

### RDD or DataFrame?
Before ingestion, every collection is made of a large number of JSONL files (file where each line is a valid JSON object) in S3 (corporate bucket).
We have called ingestion usually consists in reading and loading those files into a Spark Resilient Distributed Dataset (RDD).
We then apply various transformation, validation, sanitisation and decryption tasks to each row in the RDD and saves the output
in another location in S3 (published bucket). The resulting files are .ORC files compressed with LZO.

When we need to perform more complex transformation/analysis on a collection such as running Window function to selectively remove
duplicates or running joins, we can extend the `run()` method and use PySpark DataFrame instead. Our processing of the collection
`CalculationParts` in a good example of the utilisation of PySpark DataFrame.

### Further reading
There is additional documentation in the `docs/` directory of this repository