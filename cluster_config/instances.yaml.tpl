---
Instances:
  KeepJobFlowAliveWhenNoSteps: ${keep_cluster_alive}
  AdditionalMasterSecurityGroups:
  - "${add_master_sg}"
  AdditionalSlaveSecurityGroups:
  - "${add_slave_sg}"
  Ec2SubnetIds:
  %{for subnet_id in subnet_ids ~}
  - ${subnet_id}
  %{endfor ~}

  EmrManagedMasterSecurityGroup: "${master_sg}"
  EmrManagedSlaveSecurityGroup: "${slave_sg}"
  ServiceAccessSecurityGroup: "${service_access_sg}"
  InstanceFleets:
  - InstanceFleetType: "MASTER"
    Name: MASTER
    TargetOnDemandCapacity: 1
    %{~ if capacity_reservation_preference == "open" ~}
    LaunchSpecifications:
      OnDemandSpecification:
        AllocationStrategy: "lowest-price"
        CapacityReservationOptions:
          CapacityReservationPreference: "${capacity_reservation_preference}"
          UsageStrategy: "${capacity_reservation_usage_strategy}"
    %{~ endif ~}
    InstanceTypeConfigs:
    - EbsConfiguration:
        EbsBlockDeviceConfigs:
        - VolumeSpecification:
            SizeInGB: 250
            VolumeType: "gp3"
          VolumesPerInstance: 1
      InstanceType: "${instance_type_master}"
  - InstanceFleetType: "CORE"
    Name: CORE
    TargetOnDemandCapacity: ${core_instance_count}
    %{~ if capacity_reservation_preference == "open" ~}
    LaunchSpecifications:
      OnDemandSpecification:
        AllocationStrategy: "lowest-price"
        CapacityReservationOptions:
          CapacityReservationPreference: "${capacity_reservation_preference}"
          UsageStrategy: "${capacity_reservation_usage_strategy}"
    %{~ endif ~}
    InstanceTypeConfigs:
    - EbsConfiguration:
        EbsBlockDeviceConfigs:
        - VolumeSpecification:
            SizeInGB: 250
            VolumeType: "gp3"
          VolumesPerInstance: 1
      InstanceType: "${instance_type_core_one}"
