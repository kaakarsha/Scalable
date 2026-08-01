import os
import time

import boto3
from dotenv import load_dotenv

load_dotenv()

REGION = os.getenv("AWS_REGION", "us-east-1")
DB = os.getenv("ATHENA_DB", "earthquake_pipeline")
TABLE = os.getenv("ATHENA_TABLE", "batch_view")
S3_BATCH_PATH = os.getenv("S3_BATCH_PATH", "s3://earthquake-pipeline-batch/output/")
S3_OUT = os.getenv("S3_ATHENA_OUT", "s3://earthquake-pipeline-batch/athena-results/")

athena = boto3.client("athena", region_name=REGION)

DDL = [
    f"CREATE DATABASE IF NOT EXISTS {DB}",
    f"DROP TABLE IF EXISTS {DB}.{TABLE}",
    f"""CREATE EXTERNAL TABLE {DB}.{TABLE} (
          region string,
          events bigint,
          avg_mag double,
          max_mag double
        )
        STORED AS PARQUET
        LOCATION '{S3_BATCH_PATH}region_stats/'""",
    f"DROP TABLE IF EXISTS {DB}.mag_bands",
    f"""CREATE EXTERNAL TABLE {DB}.mag_bands (
          mag_band string,
          events bigint
        )
        STORED AS PARQUET
        LOCATION '{S3_BATCH_PATH}mag_bands/'""",
]


def run(sql):
    q = athena.start_query_execution(
        QueryString=sql,
        QueryExecutionContext={"Database": "default"},
        ResultConfiguration={"OutputLocation": S3_OUT},
    )["QueryExecutionId"]
    while True:
        st = athena.get_query_execution(QueryExecutionId=q)["QueryExecution"]["Status"]
        s = st["State"]
        if s in ("SUCCEEDED", "FAILED", "CANCELLED"):
            if s != "SUCCEEDED":
                print(f"  ! {s}: {st.get('StateChangeReason','')}")
            return s
        time.sleep(1.5)


if __name__ == "__main__":
    for sql in DDL:
        head = " ".join(sql.split())[:70]
        print(f"{run(sql):<10} {head}")
    print("\nverify:")
    q = athena.start_query_execution(
        QueryString=f"SELECT region, events, round(avg_mag,2) avg_mag "
                    f"FROM {DB}.{TABLE} ORDER BY events DESC LIMIT 10",
        QueryExecutionContext={"Database": "default"},
        ResultConfiguration={"OutputLocation": S3_OUT},
    )["QueryExecutionId"]
    while True:
        s = athena.get_query_execution(QueryExecutionId=q)["QueryExecution"]["Status"]["State"]
        if s in ("SUCCEEDED", "FAILED", "CANCELLED"):
            break
        time.sleep(1.5)
    if s == "SUCCEEDED":
        rs = athena.get_query_results(QueryExecutionId=q)["ResultSet"]["Rows"]
        for r in rs:
            print("  " + "  ".join(c.get("VarCharValue", "") for c in r["Data"]))
