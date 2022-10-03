# aws-emr-template-repository

## A template repository for building EMR cluster in AWS

This repo contains Makefile and base terraform folders and jinja2 files to fit the standard pattern.
This repo is a base to create new Terraform repos, renaming the template files and adding the githooks submodule, making the repo ready for use.

Running aviator will create the pipeline required on the AWS-Concourse instance, in order pass a mandatory CI ran status check.  this will likely require you to login to Concourse, if you haven't already.

After cloning this repo, please generate `terraform.tf` and `terraform.tfvars` files:  
`make bootstrap`

In addition, you may want to do the following: 

1. Create non-default Terraform workspaces as and if required:  
    `make terraform-workspace-new workspace=<workspace_name>` e.g.  
    ```make terraform-workspace-new workspace=qa```

1. Configure Concourse CI pipeline:
    1. Add/remove jobs in `./ci/jobs` as required 
    1. Create CI pipeline:  
`aviator`

## Networking

Before you are able to deploy your EMR cluster, the new service will need the networking for it configured.   

[An example](https://git.ucd.gpn.gov.uk/dip/aws-internal-compute/blob/master/aws-emr-template-repository_network.tf) of this can be seen in the `internal-compute` VPC where a lot of our EMR clusters are deployed. 

If you are creating the subnets in a different repository, remember to output the address as seen [here](https://git.ucd.gpn.gov.uk/dip/aws-internal-compute/blob/master/outputs.tf#L47-L53)

## Concourse pipeline

There is a concourse pipeline for aws-emr-template-repository named `aws-emr-template-repository`. The code for this pipeline is in the `ci` folder. The main part of the pipeline (the `master` group) deploys the infrastructure and runs the e2e tests. There are a number of groups for rotating passwords and there are also admin groups for each environment.

### Admin jobs

Any jobs that require the use of aviator, e.g. starting and stopping clusters need to be added to the [dataworks-admin-utils repository](https://github.com/dwp/dataworks-admin-utils). An example of existing admin jobs for the `aws-clive` data product can be [seen here](https://ci.dataworks.dwp.gov.uk/teams/utility/pipelines/aws-clive)

#### Start cluster

This job will start an aws-emr-template-repository cluster. In order to make the cluster do what you want it to do, you can alter the following environment variables in the pipeline config and then run `aviator` to update the pipeline before kicking it off:

The parameters below come from an existing product called `aws-clive` and serve only as an example. They will need to be tailored to the needs of your data product.

1. S3_PREFIX (required) -> the S3 output location for the HTME data to process, i.e. `analytical-dataset/2020-08-13_22-16-58/`
1. EXPORT_DATE (required) -> the date the data was exported, i.e `2021-04-01`
1. CORRELATION_ID (required) -> the correlation id for this run, i.e. `<some_unique_correlation_id>`
1. SNAPSHOT_TYPE (required) -> `full`


#### Stop clusters

For stopping clusters, you can run the `stop-cluster` job to terminate ALL current `aws-emr-template-repository` clusters in the environment.

### Clear dynamo row (i.e. for a cluster restart)   

Sometimes the aws-emr-template-repository cluster is required to restart from the beginning instead of restarting from the failure point.
To be able to do a full cluster restart, delete the associated DynamoDB row if it exists. The keys to the row are `Correlation_Id` and `DataProduct` in the DynamoDB table storing cluster state information (see [Retries](#retries)).   
The `clear-dynamodb-row` job is responsible for carrying out the row deletion.

To do a full cluster restart

* Manually enter CORRELATION_ID and DATA_PRODUCT of the row to delete to the `clear-dynamodb-row` job and run aviator.


    jobs:
      - name: dev-clear-dynamodb-row
        plan:
          - .: (( inject meta.plan.clear-dynamodb-row ))
            config:
              params:
                AWS_ROLE_ARN: arn:aws:iam::((aws_account.development)):role/ci
                AWS_ACC: ((aws_account.development))
                CORRELATION_ID: <Correlation_Id of the row to delete>
                DATA_PRODUCT: <DataProduct of the row to delete>

* Run the admin job to `<env>-clear-dynamodb-row`

* You can then run `start-cluster` job with the same `Correlation_Id` from fresh.

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
        - aws-emr-template-repository-qa
 

## Optional Features

`data.tf.OPTIONAL` can be used if your product requires writing data to the published S3 bucket.
