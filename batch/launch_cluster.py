import os
import time

import boto3
from dotenv import load_dotenv

load_dotenv()

REGION = os.getenv("AWS_REGION", "us-east-1")
S3_BATCH = os.getenv("S3_BATCH", "earthquake-pipeline-batch")
NAME = os.getenv("EMR_CLUSTER_NAME", "earthquake-pipeline")
CORE_NODES = int(os.getenv("EMR_CORE_NODES", "2"))
INSTANCE_TYPE = os.getenv("EMR_INSTANCE_TYPE", "m5.xlarge")
SCALE_MIN = int(os.getenv("EMR_SCALE_MIN", "2"))
SCALE_MAX = int(os.getenv("EMR_SCALE_MAX", "8"))

emr = boto3.client("emr", region_name=REGION)
ec2 = boto3.client("ec2", region_name=REGION)


def default_subnet():
    vpcs = ec2.describe_vpcs(Filters=[{"Name": "isDefault", "Values": ["true"]}])["Vpcs"]
    if not vpcs:
        raise SystemExit("no default vpc")
    subs = ec2.describe_subnets(
        Filters=[{"Name": "vpc-id", "Values": [vpcs[0]["VpcId"]]}]
    )["Subnets"]
    if not subs:
        raise SystemExit("no subnet in default vpc")
    subs.sort(key=lambda s: -s.get("AvailableIpAddressCount", 0))
    return subs[0]["SubnetId"]


def launch():
    subnet = default_subnet()
    resp = emr.run_job_flow(
        Name=NAME,
        ReleaseLabel=os.getenv("EMR_RELEASE", "emr-6.15.0"),
        Applications=[{"Name": "Spark"}],
        LogUri=f"s3://{S3_BATCH}/emr-logs/",
        JobFlowRole="EMR_EC2_DefaultRole",
        ServiceRole="EMR_DefaultRole",
        VisibleToAllUsers=True,
        Instances={
            "InstanceGroups": [
                {
                    "Name": "Primary",
                    "InstanceRole": "MASTER",
                    "InstanceType": INSTANCE_TYPE,
                    "InstanceCount": 1,
                },
                {
                    "Name": "Core",
                    "InstanceRole": "CORE",
                    "InstanceType": INSTANCE_TYPE,
                    "InstanceCount": CORE_NODES,
                },
            ],
            "Ec2SubnetId": subnet,
            "KeepJobFlowAliveWhenNoSteps": True,
            "TerminationProtected": False,
        },
    )
    cid = resp["JobFlowId"]
    print(f"cluster={cid} subnet={subnet}")

    emr.put_managed_scaling_policy(
        ClusterId=cid,
        ManagedScalingPolicy={
            "ComputeLimits": {
                "UnitType": "Instances",
                "MinimumCapacityUnits": SCALE_MIN,
                "MaximumCapacityUnits": SCALE_MAX,
                "MaximumCoreCapacityUnits": SCALE_MAX,
            }
        },
    )
    print(f"managed scaling min={SCALE_MIN} max={SCALE_MAX}")
    return cid


def wait_ready(cid):
    while True:
        st = emr.describe_cluster(ClusterId=cid)["Cluster"]["Status"]["State"]
        print(f"state={st}", flush=True)
        if st in ("WAITING", "RUNNING"):
            return st
        if st in ("TERMINATED", "TERMINATED_WITH_ERRORS", "TERMINATING"):
            raise SystemExit(f"cluster failed: {st}")
        time.sleep(30)


if __name__ == "__main__":
    cid = launch()
    here = os.path.join(os.path.dirname(__file__), "..", "..", "config", "emr_cluster_id.txt")
    with open(os.path.abspath(here), "w") as f:
        f.write(cid)
    wait_ready(cid)
    print(f"ready={cid}")
