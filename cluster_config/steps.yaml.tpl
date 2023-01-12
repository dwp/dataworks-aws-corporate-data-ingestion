---
BootstrapActions:
- Name: "download_scripts"
  ScriptBootstrapAction:
    Path: "s3://${s3_config_bucket}/component/corporate-data-ingestion/download_scripts.sh"
- Name: "start_ssm"
  ScriptBootstrapAction:
    Path: "file:/var/ci/start_ssm.sh"
- Name: "metadata"
  ScriptBootstrapAction:
    Path: "file:/var/ci/metadata.sh"
- Name: "emr-setup"
  ScriptBootstrapAction:
    Path: "file:/var/ci/emr-setup.sh"
- Name: "installer"
  ScriptBootstrapAction:
    Path: "file:/var/ci/installer.sh"
- Name: "metrics-setup"
  ScriptBootstrapAction:
    Path: "file:/var/ci/metrics-setup.sh"
Steps:
- Name: "submit-job"
  HadoopJarStep:
    Args:
    - "spark-submit"
    - "--master"
    - "yarn"
    - "--conf"
    - "spark.yarn.submit.waitAppCompletion=true"
    - "/opt/emr/steps/corporate_data_ingestion.py"
    Jar: "command-runner.jar"
  ActionOnFailure: "${action_on_failure}"
- Name: "send_notification"
  HadoopJarStep:
    Args:
    - "python3"
    - "/opt/emr/steps/send_notification.py"
    Jar: "command-runner.jar"
  ActionOnFailure: "${action_on_failure}"

