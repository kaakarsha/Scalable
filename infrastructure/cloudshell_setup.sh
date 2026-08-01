#!/bin/bash
# Bootstrap the pipeline in AWS CloudShell.
# CloudShell already carries the Learner Lab credentials, so no AWS keys are written.
set -e

REPO="https://github.com/srivenkatborab/Scalable.git"
APP="$HOME/Scalable"
REGION="us-east-1"
ACCT=$(aws sts get-caller-identity --query Account --output text)
SUFFIX="${ACCT: -6}"
S3_RAW="earthquake-pipeline-raw-$SUFFIX"
S3_BATCH="earthquake-pipeline-batch-$SUFFIX"

if [ -d "$APP/.git" ]; then
  git -C "$APP" fetch origin main -q && git -C "$APP" reset --hard origin/main -q
else
  git clone -q "$REPO" "$APP"
fi
cd "$APP"
echo "code: $(git rev-parse --short HEAD)"

python3 -m venv .venv 2>/dev/null || true
./.venv/bin/pip install -q --upgrade pip
./.venv/bin/pip install -q boto3 requests python-dotenv pyathena matplotlib

cat > "$APP/.env" <<EOF
AWS_REGION=$REGION
KINESIS_STREAM=earthquake-stream
KINESIS_SHARDS=4
S3_RAW=$S3_RAW
S3_BATCH=$S3_BATCH
DYNAMO_TABLE=earthquake-speed-view
ATHENA_DB=earthquake_pipeline
ATHENA_TABLE=batch_view
S3_ATHENA_OUT=s3://$S3_BATCH/athena-results/
S3_RAW_PATH=s3://$S3_RAW/data/
S3_BATCH_PATH=s3://$S3_BATCH/output/
WINDOW_SECONDS=300
TOP_N=10
USGS_FEED=https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_day.geojson
ASG_NAME=earthquake-pipeline-asg
EOF

echo "account=$ACCT  raw=$S3_RAW  batch=$S3_BATCH"
echo
echo "ready. from $APP run:"
echo "  ./.venv/bin/python batch/launch_cluster.py          # EMR (~8 min, writes cluster id)"
echo "  ./.venv/bin/python batch/submit.py                  # Spark batch over full history"
echo "  ./.venv/bin/python serving/refresh_athena.py        # point Athena at new Parquet"
echo "  ./.venv/bin/python serving/query.py                 # batch + speed merge"
echo "  ./.venv/bin/python ingestion/loadgen.py --events 100000 --workers 24"
echo "  ./.venv/bin/python infrastructure/ssm_run.py 'systemctl is-active earthquake-speed'"
