import os
import sys
from concurrent.futures import ThreadPoolExecutor

import boto3
from botocore.config import Config
from dotenv import load_dotenv

load_dotenv()

REGION = os.getenv("AWS_REGION", "us-east-1")
BUCKET = os.getenv("S3_RAW", "earthquake-pipeline-raw")
SRC_PREFIX = "data/"
DST_PREFIX = "bench-input/"

s3 = boto3.client("s3", region_name=REGION, config=Config(max_pool_connections=64))


def source_keys():
    keys = []
    tok = None
    while True:
        kw = {"Bucket": BUCKET, "Prefix": SRC_PREFIX}
        if tok:
            kw["ContinuationToken"] = tok
        r = s3.list_objects_v2(**kw)
        for o in r.get("Contents", []):
            if o["Key"].endswith(".ndjson"):
                keys.append((o["Key"], o["Size"]))
        tok = r.get("NextContinuationToken")
        if not tok:
            break
    return keys


def copy_one(job):
    src, dst = job
    s3.copy_object(Bucket=BUCKET, CopySource={"Bucket": BUCKET, "Key": src}, Key=dst)


def main(replicas):
    keys = source_keys()
    if not keys:
        raise SystemExit(f"no .ndjson under s3://{BUCKET}/{SRC_PREFIX}")
    base_mb = sum(s for _, s in keys) / 1e6
    jobs = []
    for r in range(replicas):
        for k, _ in keys:
            name = k.split("/")[-1].replace(".ndjson", "")
            jobs.append((k, f"{DST_PREFIX}r{r:03d}_{name}.ndjson"))
    print(f"source={len(keys)} objects {base_mb:.1f} MB -> {len(jobs)} objects "
          f"{base_mb*replicas/1000:.2f} GB")
    with ThreadPoolExecutor(max_workers=48) as ex:
        list(ex.map(copy_one, jobs))
    print(f"done s3://{BUCKET}/{DST_PREFIX}")


if __name__ == "__main__":
    main(int(sys.argv[1]) if len(sys.argv) > 1 else 60)
