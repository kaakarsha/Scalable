#!/bin/bash
set -x
exec > /var/log/earthquake-bootstrap.log 2>&1

REGION="us-east-1"
S3_RAW="earthquake-pipeline-raw-427706"
S3_BATCH="earthquake-pipeline-batch-427706"
REPO="https://github.com/srivenkatborab/Scalable.git"
DEMO_EIP="50.19.98.60"
APP=/opt/earthquake
PY=$APP/venv/bin/python
PIP=$APP/venv/bin/pip

dnf install -y python3 python3-pip python3-devel gcc git || \
  yum install -y python3 python3-pip python3-devel gcc git

rm -rf $APP
git clone --depth 1 "$REPO" $APP

python3 -m venv $APP/venv
$PIP install --upgrade pip
$PIP install boto3 requests python-dotenv streamlit pandas pyathena

cat > $APP/.env <<EOF
AWS_REGION=$REGION
KINESIS_STREAM=earthquake-stream
KINESIS_SHARDS=2
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

cat > /etc/systemd/system/earthquake-speed.service <<EOF
[Unit]
Description=Earthquake speed layer (Kinesis 5-min sliding window -> DynamoDB)
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
WorkingDirectory=$APP
ExecStart=$PY -u $APP/speed/consumer.py
Restart=always
RestartSec=10

[Install]
WantedBy=multi-user.target
EOF

cat > /etc/systemd/system/earthquake-dashboard.service <<EOF
[Unit]
Description=Earthquake Streamlit dashboard
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
WorkingDirectory=$APP
ExecStart=$APP/venv/bin/streamlit run $APP/dashboard/app.py --server.address 0.0.0.0 --server.port 8501 --server.headless true
Restart=always
RestartSec=10

[Install]
WantedBy=multi-user.target
EOF

cat > /etc/systemd/system/earthquake-load.service <<EOF
[Unit]
Description=Earthquake load generator (on-demand, drives ASG CPU scale-out)
After=network-online.target

[Service]
Type=simple
WorkingDirectory=$APP
ExecStart=$PY -u $APP/ingestion/loadgen.py --events 200000 --workers 16 --no-s3
Restart=no
EOF

systemctl daemon-reload
systemctl enable --now earthquake-speed.service
systemctl enable --now earthquake-dashboard.service

# Reclaim the stable demo URL: the ASG may replace this instance at any time, so
# re-associate the Elastic IP on every boot rather than by hand.
TOKEN=$(curl -sX PUT "http://169.254.169.254/latest/api/token" \
  -H "X-aws-ec2-metadata-token-ttl-seconds: 300" 2>/dev/null)
IID=$(curl -s -H "X-aws-ec2-metadata-token: $TOKEN" \
  http://169.254.169.254/latest/meta-data/instance-id 2>/dev/null)
ALLOC=$(aws ec2 describe-addresses --region $REGION \
  --query "Addresses[?PublicIp=='$DEMO_EIP'].AllocationId" --output text 2>/dev/null)
if [ -n "$IID" ] && [ -n "$ALLOC" ] && [ "$ALLOC" != "None" ]; then
  aws ec2 associate-address --instance-id "$IID" --allocation-id "$ALLOC" \
    --allow-reassociation --region $REGION && echo "EIP $DEMO_EIP -> $IID"
fi

echo "earthquake-pipeline ready $(date -u)" > $APP/READY
echo "dashboard: http://$DEMO_EIP:8501" >> $APP/READY
