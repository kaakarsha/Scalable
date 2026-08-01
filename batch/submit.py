import os
import time
import boto3
from dotenv import load_dotenv

load_dotenv()

REGION = os.getenv("AWS_REGION", "us-east-1")
S3_BATCH = os.getenv("S3_BATCH", "earthquake-pipeline-batch")
S3_RAW_PATH = os.getenv("S3_RAW_PATH", "s3://earthquake-pipeline-raw/data/")
S3_BATCH_PATH = os.getenv("S3_BATCH_PATH", "s3://earthquake-pipeline-batch/output/")
SCRIPT_KEY = os.getenv("BATCH_SCRIPT_KEY", "scripts/batch_job.py")

CLUSTER = os.getenv("EMR_CLUSTER_ID", "")
if not CLUSTER:
    _here = os.path.dirname(os.path.abspath(__file__))
    for _rel in ("../../config", "../config"):
        _p = os.path.join(_here, _rel, "emr_cluster_id.txt")
        if os.path.exists(_p):
            CLUSTER = open(_p).read().strip()
            break

emr = boto3.client("emr", region_name=REGION)
s3 = boto3.client("s3", region_name=REGION)


def upload_script(local_path):
    s3.upload_file(local_path, S3_BATCH, SCRIPT_KEY)
    return f"s3://{S3_BATCH}/{SCRIPT_KEY}"


def submit(script_s3):
    if not CLUSTER:
        raise SystemExit("set EMR_CLUSTER_ID")
    step = {
        "Name": "earthquake-batch",
        "ActionOnFailure": "CONTINUE",
        "HadoopJarStep": {
            "Jar": "command-runner.jar",
            "Args": [
                "spark-submit",
                "--deploy-mode",
                "cluster",
                script_s3,
                S3_RAW_PATH,
                S3_BATCH_PATH,
            ],
        },
    }
    resp = emr.add_job_flow_steps(JobFlowId=CLUSTER, Steps=[step])
    sid = resp["StepIds"][0]
    print(f"step={sid}")
    return sid


def wait_step(step_id):
    while True:
        desc = emr.describe_step(ClusterId=CLUSTER, StepId=step_id)
        state = desc["Step"]["Status"]["State"]
        print(f"state={state}")
        if state in ("COMPLETED", "FAILED", "CANCELLED", "INTERRUPTED"):
            return state
        time.sleep(30)


if __name__ == "__main__":
    here = os.path.join(os.path.dirname(__file__), "batch_job.py")
    uri = upload_script(here)
    sid = submit(uri)
    print(wait_step(sid))
