# dataworks-aws-corporate-data-ingestion

The corporate data ingestion pipeline is built to regularly ingest data from the corporate storage bucket.
The *.json.gz files are read, decompressed, decrypted and stored in the published bucket.  The cluster performs
similar steps to the HTME and ADG infrastructure.


## Concourse pipeline

There is a concourse pipeline for dataworks-aws-corporate-data-ingestion named `dataworks-aws-corporate-data-ingestion`. The code for this pipeline is in the `ci` folder. The main part of the pipeline (the `master` group) deploys the infrastructure and runs the e2e tests. There are a number of groups for rotating passwords and there are also admin groups for each environment.

### Admin jobs

Any jobs that require the use of aviator, e.g. starting and stopping clusters need to be added to the [dataworks-admin-utils repository](https://github.com/dwp/dataworks-admin-utils). An example of existing admin jobs for the `aws-clive` data product can be [seen here](https://ci.dataworks.dwp.gov.uk/teams/utility/pipelines/aws-clive)

#### Start cluster

This job will start an dataworks-aws-corporate-data-ingestion cluster.

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
