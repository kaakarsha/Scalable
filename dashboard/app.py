import os
import boto3
import pandas as pd
import streamlit as st

REGION = os.getenv("AWS_REGION", "us-east-1")
DYNAMO = os.getenv("DYNAMO_TABLE", "earthquake-speed-view")
STREAM = os.getenv("KINESIS_STREAM", "earthquake-stream")
S3_RAW = os.getenv("S3_RAW", "earthquake-pipeline-raw-427706")
S3_BATCH = os.getenv("S3_BATCH", "earthquake-pipeline-batch-427706")
TOP_N = int(os.getenv("TOP_N", "10"))

st.set_page_config(page_title="Earthquake Lambda Analytics", layout="wide")
st.title("Earthquake Lambda Analytics Dashboard")
st.caption("USGS → Kinesis → Batch ∥ Speed (5-min window) → Serving merge | us-east-1")

def items(prefix):
    table = boto3.resource("dynamodb", region_name=REGION).Table(DYNAMO)
    rows = []
    for rank in range(1, TOP_N + 1):
        item = table.get_item(Key={"pk": f"{prefix}#{rank}"}).get("Item")
        if item:
            rows.append({
                "rank": rank,
                "region": item.get("region"),
                "count": int(float(item.get("count", 0))),
                "avg_mag": float(item.get("avg_mag", 0) or 0),
                "source": item.get("source", prefix),
            })
    return rows

def kinesis_status():
    k = boto3.client("kinesis", region_name=REGION)
    d = k.describe_stream_summary(StreamName=STREAM)["StreamDescriptionSummary"]
    return d.get("StreamStatus"), d.get("OpenShardCount")

c1,c2,c3,c4 = st.columns(4)
stt, sh = kinesis_status()
c1.metric("Kinesis", STREAM)
c2.metric("Status", stt)
c3.metric("Shards", sh)
c4.metric("DynamoDB", DYNAMO)

speed = items("speed")
batch = items("batch")
serving = items("serving")

col1, col2 = st.columns(2)
with col1:
    st.subheader("Speed layer (last 5 min window)")
    if speed:
        df = pd.DataFrame(speed)
        st.dataframe(df, use_container_width=True)
        st.bar_chart(df.set_index("region")["count"])
    else:
        st.warning("No speed data")
with col2:
    st.subheader("Batch layer (full history)")
    if batch:
        df = pd.DataFrame(batch)
        st.dataframe(df, use_container_width=True)
        st.bar_chart(df.set_index("region")["count"])
    else:
        st.warning("No batch data")

st.subheader("Serving merge (batch + speed)")
if serving:
    df = pd.DataFrame(serving)
    st.dataframe(df, use_container_width=True)
    st.bar_chart(df.set_index("region")["count"])
else:
    st.warning("No serving data")

st.subheader("Fixed demo names")
st.code(f"""region=us-east-1
kinesis={STREAM}
s3_raw={S3_RAW}
s3_batch={S3_BATCH}
dynamo={DYNAMO}
asg=earthquake-pipeline-asg min1/max3
scale_out=CPU>70 cooldown120s
scale_in=CPU<30 cooldown300s
eip=50.19.98.60
athena=earthquake_pipeline.batch_view
""")
if st.button("Refresh"):
    st.rerun()
