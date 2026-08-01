#!/bin/bash
set -x
exec > /var/log/earthquake-bootstrap.log 2>&1

REGION="us-east-1"
S3_RAW="earthquake-pipeline-raw-427706"
S3_BATCH="earthquake-pipeline-batch-427706"
APP=/opt/earthquake

dnf install -y python3 python3-pip git || yum install -y python3 python3-pip git
python3 -m pip install --upgrade pip
python3 -m pip install boto3 requests python-dotenv streamlit pandas

mkdir -p $APP
aws s3 cp s3://$S3_BATCH/app/ $APP/ --recursive --region $REGION || true

cat > $APP/.env <<EOF
AWS_REGION=$REGION
KINESIS_STREAM=earthquake-stream
S3_RAW=$S3_RAW
S3_BATCH=$S3_BATCH
DYNAMO_TABLE=earthquake-speed-view
ATHENA_DB=earthquake_pipeline
ATHENA_TABLE=batch_view
S3_ATHENA_OUT=s3://$S3_BATCH/athena-results/
S3_RAW_PATH=s3://$S3_RAW/data/
WINDOW_SECONDS=300
TOP_N=10
USGS_FEED=https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_day.geojson
EOF

cat > /etc/systemd/system/earthquake-speed.service <<'EOF'
[Unit]
Description=Earthquake speed layer (Kinesis sliding window -> DynamoDB)
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
WorkingDirectory=/opt/earthquake
ExecStart=/usr/bin/python3 /opt/earthquake/speed/consumer.py
Restart=always
RestartSec=10
StandardOutput=journal
StandardError=journal

[Install]
WantedBy=multi-user.target
EOF

cat > /etc/systemd/system/earthquake-dashboard.service <<'EOF'
[Unit]
Description=Earthquake Streamlit dashboard
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
WorkingDirectory=/opt/earthquake
ExecStart=/usr/local/bin/streamlit run /opt/earthquake/dashboard/app.py --server.address 0.0.0.0 --server.port 8501 --server.headless true
Restart=always
RestartSec=10

[Install]
WantedBy=multi-user.target
EOF

cat > /etc/systemd/system/earthquake-load.service <<'EOF'
[Unit]
Description=Earthquake load generator (on-demand, drives ASG scale-out)
After=network-online.target

[Service]
Type=simple
WorkingDirectory=/opt/earthquake
ExecStart=/usr/bin/python3 /opt/earthquake/ingestion/loadgen.py --events 400000 --workers 16 --no-s3
Restart=no
EOF

systemctl daemon-reload
systemctl enable --now earthquake-speed.service
[ -f $APP/dashboard/app.py ] && systemctl enable --now earthquake-dashboard.service

echo "earthquake-pipeline-demo ready $(date -u)" > $APP/READY
