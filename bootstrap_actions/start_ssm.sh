#!/bin/bash

# During provisioning, EMR >= 5.30.0 stops the SSM agent but fails to start it
# again for unknown reasons.

os_version=$(cat /etc/system-release)

if [ "${os_version}" == "Amazon Linux release 2 (Karoo)" ]; then
  sudo systemctl start amazon-ssm-agent
fi
