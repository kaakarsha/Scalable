import os
import sys
import time

import boto3
from dotenv import load_dotenv

load_dotenv()

REGION = os.getenv("AWS_REGION", "us-east-1")
ssm = boto3.client("ssm", region_name=REGION)
ec2 = boto3.client("ec2", region_name=REGION)
ASG = os.getenv("ASG_NAME", "earthquake-pipeline-asg")


def asg_instances():
    r = ec2.describe_instances(
        Filters=[
            {"Name": "tag:aws:autoscaling:groupName", "Values": [ASG]},
            {"Name": "instance-state-name", "Values": ["running"]},
        ]
    )
    return [i["InstanceId"] for res in r["Reservations"] for i in res["Instances"]]


def run(instance_id, command, timeout=180):
    cid = ssm.send_command(
        InstanceIds=[instance_id],
        DocumentName="AWS-RunShellScript",
        Parameters={"commands": [command]},
    )["Command"]["CommandId"]
    waited = 0
    while waited < timeout:
        time.sleep(3)
        waited += 3
        try:
            inv = ssm.get_command_invocation(CommandId=cid, InstanceId=instance_id)
        except ssm.exceptions.InvocationDoesNotExist:
            continue
        if inv["Status"] in ("Success", "Failed", "Cancelled", "TimedOut"):
            return inv["Status"], inv.get("StandardOutputContent", ""), inv.get("StandardErrorContent", "")
    return "TIMEOUT", "", ""


if __name__ == "__main__":
    cmd = " ".join(sys.argv[1:]) or "echo no command"
    for iid in asg_instances():
        st, out, err = run(iid, cmd)
        print(f"===== {iid} [{st}] =====")
        if out:
            print(out.rstrip())
        if err:
            print("--- stderr ---")
            print(err.rstrip())
