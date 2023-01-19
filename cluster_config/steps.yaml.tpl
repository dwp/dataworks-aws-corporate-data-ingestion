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
