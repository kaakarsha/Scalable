import os
from datetime import datetime, timedelta, timezone

import boto3
import pandas as pd
import streamlit as st
from dotenv import load_dotenv

load_dotenv()

REGION = os.getenv("AWS_REGION", "us-east-1")
DYNAMO = os.getenv("DYNAMO_TABLE", "earthquake-speed-view")
STREAM = os.getenv("KINESIS_STREAM", "earthquake-stream")
S3_RAW = os.getenv("S3_RAW", "earthquake-pipeline-raw-427706")
S3_BATCH = os.getenv("S3_BATCH", "earthquake-pipeline-batch-427706")
ATHENA_DB = os.getenv("ATHENA_DB", "earthquake_pipeline")
ATHENA_TBL = os.getenv("ATHENA_TABLE", "batch_view")
S3_ATHENA_OUT = os.getenv("S3_ATHENA_OUT", f"s3://{S3_BATCH}/athena-results/")
ASG_NAME = os.getenv("ASG_NAME", "earthquake-pipeline-asg")
TOP_N = int(os.getenv("TOP_N", "10"))
WINDOW_S = int(os.getenv("WINDOW_SECONDS", "300"))

st.set_page_config(page_title="Earthquake Lambda Analytics", layout="wide",
                   initial_sidebar_state="collapsed")

st.markdown("""
<style>
  .block-container {padding-top: 2rem; padding-bottom: 1rem;}
  [data-testid="stMetricValue"] {font-size: 1.5rem;}
  h1 {font-size: 2rem !important;}
  .tag {display:inline-block;padding:2px 10px;border-radius:12px;
        font-size:0.75rem;font-weight:600;margin-right:6px;}
  .speed {background:#fde7e9;color:#c0392b;}
  .batch {background:#e3f0fb;color:#1f6fb2;}
  .serve {background:#e8f6ee;color:#1e8449;}
</style>
""", unsafe_allow_html=True)


@st.cache_resource
def clients():
    return {
        "dynamodb": boto3.resource("dynamodb", region_name=REGION),
        "kinesis": boto3.client("kinesis", region_name=REGION),
        "cloudwatch": boto3.client("cloudwatch", region_name=REGION),
        "autoscaling": boto3.client("autoscaling", region_name=REGION),
        "emr": boto3.client("emr", region_name=REGION),
        "athena": boto3.client("athena", region_name=REGION),
    }


C = clients()


def speed_view():
    t = C["dynamodb"].Table(DYNAMO)
    rows = []
    for rank in range(1, TOP_N + 1):
        it = t.get_item(Key={"pk": f"speed#{rank}"}).get("Item")
        if it:
            rows.append({
                "region": it.get("region"),
                "events": int(float(it.get("count", 0))),
                "avg_mag": round(float(it.get("avg_mag", 0) or 0), 2),
                "hotspot": it.get("hotspot") == "1",
                "ts": int(it.get("ts", 0) or 0),
            })
    return pd.DataFrame(rows)


@st.cache_data(ttl=60)
def batch_view():
    import time as _t
    a = C["athena"]
    q = a.start_query_execution(
        QueryString=f"SELECT region, events, avg_mag FROM {ATHENA_DB}.{ATHENA_TBL} "
                    f"ORDER BY events DESC LIMIT {TOP_N}",
        QueryExecutionContext={"Database": "default"},
        ResultConfiguration={"OutputLocation": S3_ATHENA_OUT},
    )["QueryExecutionId"]
    s = "RUNNING"
    for _ in range(40):
        s = a.get_query_execution(QueryExecutionId=q)["QueryExecution"]["Status"]["State"]
        if s in ("SUCCEEDED", "FAILED", "CANCELLED"):
            break
        _t.sleep(1)
    if s != "SUCCEEDED":
        return pd.DataFrame()
    rows = a.get_query_results(QueryExecutionId=q)["ResultSet"]["Rows"][1:]
    out = []
    for r in rows:
        d = [c.get("VarCharValue") for c in r["Data"]]
        out.append({"region": d[0], "events": int(d[1]), "avg_mag": round(float(d[2] or 0), 2)})
    return pd.DataFrame(out)


@st.cache_data(ttl=30)
def metric_series(namespace, metric, dims, stat, label, minutes=30):
    end = datetime.now(timezone.utc)
    r = C["cloudwatch"].get_metric_statistics(
        Namespace=namespace, MetricName=metric, Dimensions=dims,
        StartTime=end - timedelta(minutes=minutes), EndTime=end,
        Period=60, Statistics=[stat],
    )["Datapoints"]
    if not r:
        return pd.DataFrame()
    df = pd.DataFrame([{"time": d["Timestamp"], label: round(d[stat], 1)} for d in r])
    return df.sort_values("time").set_index("time")


def asg_state():
    g = C["autoscaling"].describe_auto_scaling_groups(
        AutoScalingGroupNames=[ASG_NAME])["AutoScalingGroups"]
    if not g:
        return None, []
    acts = C["autoscaling"].describe_scaling_activities(
        AutoScalingGroupName=ASG_NAME, MaxRecords=6)["Activities"]
    return g[0], acts


def emr_state():
    return C["emr"].list_clusters(
        ClusterStates=["STARTING", "BOOTSTRAPPING", "RUNNING", "WAITING"]).get("Clusters", [])


# ---------------- header ----------------
st.title("Earthquake Lambda Analytics")
st.caption(f"USGS → Kinesis → (EMR Spark batch ∥ {WINDOW_S//60}-min sliding-window speed) "
           f"→ Athena + DynamoDB serving merge · {REGION}")

try:
    ks = C["kinesis"].describe_stream_summary(
        StreamName=STREAM)["StreamDescriptionSummary"]
    k_status, k_shards = ks["StreamStatus"], ks["OpenShardCount"]
except Exception:
    k_status, k_shards = "UNKNOWN", 0

kdf = metric_series("AWS/Kinesis", "IncomingRecords",
                    [{"Name": "StreamName", "Value": STREAM}], "Sum", "records/min")
recent = int(kdf["records/min"].tail(5).sum()) if not kdf.empty else 0
peak = int(kdf["records/min"].max()) if not kdf.empty else 0

g, acts = asg_state()
clusters = emr_state()

m = st.columns(6)
m[0].metric("Kinesis", k_status, f"{k_shards} shards")
m[1].metric("Records (5 min)", f"{recent:,}")
m[2].metric("Peak / min", f"{peak:,}")
m[3].metric("ASG capacity", f"{g['DesiredCapacity']}/{g['MaxSize']}" if g else "n/a",
            f"min {g['MinSize']}" if g else "")
m[4].metric("EMR clusters", len(clusters),
            clusters[0]["Status"]["State"] if clusters else "none active")
m[5].metric("Speed window", f"{WINDOW_S//60} min", f"top-{TOP_N}")

st.divider()

# ---------------- ingestion ----------------
st.subheader("Ingestion — records flowing into Kinesis")
c1, c2 = st.columns([3, 2])
with c1:
    if kdf.empty:
        st.info("No Kinesis datapoints in the last 30 min — start the load generator.")
    else:
        st.area_chart(kdf, height=240, color="#1f6fb2")
        st.caption(f"peak {peak:,} records/min · {k_shards} shards "
                   f"(~{k_shards*1000:,} records/s ceiling)")
with c2:
    cdf = metric_series("AWS/EC2", "CPUUtilization",
                        [{"Name": "AutoScalingGroupName", "Value": ASG_NAME}],
                        "Average", "CPU %")
    if cdf.empty:
        st.info("No EC2 CPU datapoints yet.")
    else:
        st.line_chart(cdf, height=240, color="#c0392b")
        st.caption("ASG scale-out: CPU > 70% (2×60s), cooldown 120s · scale-in CPU < 30%")

st.divider()

# ---------------- speed vs batch ----------------
st.subheader("Lambda architecture — speed layer vs batch layer")
sc, bc = st.columns(2)

sdf = speed_view()
with sc:
    st.markdown(f'<span class="tag speed">SPEED LAYER</span> DynamoDB · last '
                f'{WINDOW_S//60} min', unsafe_allow_html=True)
    if sdf.empty:
        st.info("Speed view empty — no recent stream traffic.")
    else:
        age = int(datetime.now(timezone.utc).timestamp()) - int(sdf["ts"].max())
        st.caption(f"updated {age}s ago · {len(sdf)} regions in window · "
                   f"{sdf['events'].sum():,} events")
        st.bar_chart(sdf.set_index("region")["events"], height=280, color="#c0392b")
        hot = sdf[sdf["hotspot"]]["region"].tolist()
        if hot:
            st.warning(f"Hotspots flagged: {', '.join(hot)}")

bdf = batch_view()
with bc:
    st.markdown('<span class="tag batch">BATCH LAYER</span> Athena over EMR Spark '
                'Parquet · full history', unsafe_allow_html=True)
    if bdf.empty:
        st.info("Batch view unavailable — run the EMR batch job.")
    else:
        st.caption(f"{bdf['events'].sum():,} events across top {len(bdf)} regions "
                   f"· served from S3, no cluster required")
        st.bar_chart(bdf.set_index("region")["events"], height=280, color="#1f6fb2")

st.divider()

# ---------------- serving merge ----------------
st.subheader("Serving layer — the Lambda merge")
st.markdown('<span class="tag serve">SERVING</span> batch (accurate, full history) '
            '+ speed (fresh, recent window)', unsafe_allow_html=True)

if bdf.empty:
    st.info("Run the batch job to populate the merge.")
else:
    bmap = dict(zip(bdf["region"], bdf["events"]))
    smap = dict(zip(sdf["region"], sdf["events"])) if not sdf.empty else {}
    regions = set(bmap) | set(smap)
    mdf = pd.DataFrame([{
        "region": r,
        "batch": int(bmap.get(r, 0)),
        "speed": int(smap.get(r, 0)),
        "total": int(bmap.get(r, 0)) + int(smap.get(r, 0)),
    } for r in regions]).sort_values("total", ascending=False).head(TOP_N)

    left, right = st.columns([2, 3])
    with left:
        st.dataframe(mdf, hide_index=True, use_container_width=True, height=330)
    with right:
        st.bar_chart(mdf.set_index("region")[["batch", "speed"]], height=330,
                     color=["#1f6fb2", "#c0392b"], stack=True)

st.divider()

# ---------------- elasticity ----------------
st.subheader("Auto-scaling infrastructure")
e1, e2 = st.columns(2)
with e1:
    st.markdown("**EC2 Auto Scaling Group**")
    if g:
        st.write(f"`{ASG_NAME}` — min {g['MinSize']} / desired "
                 f"{g['DesiredCapacity']} / max {g['MaxSize']}")
        if acts:
            adf = pd.DataFrame([{
                "time": a["StartTime"].strftime("%H:%M:%S"),
                "status": a["StatusCode"],
                "description": a["Description"][:60],
            } for a in acts])
            st.dataframe(adf, hide_index=True, use_container_width=True, height=200)
with e2:
    st.markdown("**EMR managed scaling**")
    if clusters:
        for c in clusters:
            st.write(f"`{c['Id']}` {c['Name']} — {c['Status']['State']}")
        st.caption("Managed scaling: min 2 / max 8 instances")
    else:
        st.info("No active EMR cluster — the batch view is still served from S3 via "
                "Athena. The cluster is only needed to regenerate it.")

st.divider()
if st.button("Refresh now", type="primary"):
    st.cache_data.clear()
    st.rerun()
st.caption(f"Rendered {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')} UTC · "
           f"raw `{S3_RAW}` · batch `{S3_BATCH}`")
