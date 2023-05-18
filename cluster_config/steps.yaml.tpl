---
BootstrapActions:
- Name: "run-log4j-patch"
  ScriptBootstrapAction:
    Path: "s3://${s3_config_bucket}/component/corporate-data-ingestion/patch-log4j-emr-6.4.0-v2.sh"
- Name: "download_scripts"
  ScriptBootstrapAction:
    Path: "s3://${s3_config_bucket}/component/corporate-data-ingestion/download_scripts.sh"
- Name: "start_ssm"
  ScriptBootstrapAction:
    Path: "file:/var/ci/start_ssm.sh"
- Name: "metadata"
  ScriptBootstrapAction:
    Path: "file:/var/ci/metadata.sh"
- Name: "config_hcs"
  ScriptBootstrapAction:
    Path: "file:/var/ci/config_hcs.sh"
    Args: [
      "${environment}", 
      "${proxy_http_host}",
      "${proxy_http_port}"
    ]
- Name: "emr-setup"
  ScriptBootstrapAction:
    Path: "file:/var/ci/emr-setup.sh"
- Name: "installer"
  ScriptBootstrapAction:
    Path: "file:/var/ci/installer.sh"
- Name: "metrics-setup"
  ScriptBootstrapAction:
    Path: "file:/var/ci/metrics-setup.sh"
