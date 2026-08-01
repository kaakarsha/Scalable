import argparse
import csv
import json
import os
import statistics
import threading
import time
from datetime import datetime, timezone
from pathlib import Path

import boto3
from dotenv import load_dotenv

load_dotenv()

REGION = os.getenv("AWS_REGION", "us-east-1")
STREAM = os.getenv("KINESIS_STREAM", "earthquake-stream")
S3_BATCH = os.getenv("S3_BATCH", "earthquake-pipeline-batch")
S3_RAW_PATH = os.getenv("S3_RAW_PATH", "s3://earthquake-pipeline-raw/data/")
CLUSTER = os.getenv("EMR_CLUSTER_ID", "")
RESULTS = Path(__file__).resolve().parent / "results"
RESULTS.mkdir(parents=True, exist_ok=True)

emr = boto3.client("emr", region_name=REGION)
s3 = boto3.client("s3", region_name=REGION)
kinesis = boto3.client("kinesis", region_name=REGION)


def _cluster_id():
    cid = CLUSTER
    if not cid:
        p = Path(__file__).resolve().parent.parent.parent / "config" / "emr_cluster_id.txt"
        if p.exists():
            cid = p.read_text().strip()
    if not cid:
        raise SystemExit("no EMR cluster id (set EMR_CLUSTER_ID or config/emr_cluster_id.txt)")
    return cid


def upload_job():
    local = Path(__file__).resolve().parent.parent / "batch" / "batch_job.py"
    key = "scripts/batch_job.py"
    s3.upload_file(str(local), S3_BATCH, key)
    return f"s3://{S3_BATCH}/{key}"


def run_step(cid, script, executors, cores, label):
    out = f"s3://{S3_BATCH}/bench/{label}/"
    step = {
        "Name": f"bench-{label}",
        "ActionOnFailure": "CONTINUE",
        "HadoopJarStep": {
            "Jar": "command-runner.jar",
            "Args": [
                "spark-submit",
                "--deploy-mode", "cluster",
                "--conf", "spark.dynamicAllocation.enabled=false",
                "--num-executors", str(executors),
                "--executor-cores", str(cores),
                "--executor-memory", "2g",
                script,
                S3_RAW_PATH,
                out,
            ],
        },
    }
    sid = emr.add_job_flow_steps(JobFlowId=cid, Steps=[step])["StepIds"][0]
    print(f"  step={sid} executors={executors} cores={cores}", flush=True)
    while True:
        d = emr.describe_step(ClusterId=cid, StepId=sid)["Step"]
        st = d["Status"]["State"]
        if st in ("COMPLETED", "FAILED", "CANCELLED", "INTERRUPTED"):
            tl = d["Status"]["Timeline"]
            start = tl.get("StartDateTime")
            end = tl.get("EndDateTime")
            secs = (end - start).total_seconds() if start and end else None
            return st, secs, sid
        time.sleep(15)


def mode_speedup(configs):
    cid = _cluster_id()
    script = upload_job()
    print(f"cluster={cid} script={script}")
    rows = []
    t1 = None
    for ex, co in configs:
        slots = ex * co
        label = f"e{ex}c{co}"
        print(f"[speedup] {slots} parallel task slots", flush=True)
        st, secs, sid = run_step(cid, script, ex, co, label)
        if st != "COMPLETED":
            print(f"  !! {st} — skipping {label}")
            continue
        if t1 is None:
            t1 = secs
        rows.append({
            "executors": ex, "cores_per_executor": co, "task_slots": slots,
            "seconds": round(secs, 2), "speedup": round(t1 / secs, 3) if secs else 0,
            "efficiency": round((t1 / secs) / slots, 3) if secs else 0,
            "step_id": sid,
        })
        print(f"  {label}: {secs:.1f}s speedup={t1/secs:.2f}x", flush=True)

    if rows:
        with (RESULTS / "speedup_measured.csv").open("w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
            w.writeheader()
            w.writerows(rows)
        _plot_speedup(rows)
    return rows


def _plot_speedup(rows):
    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt

        xs = [r["task_slots"] for r in rows]
        ys = [r["speedup"] for r in rows]
        fig, ax = plt.subplots(figsize=(6, 4))
        ax.plot(xs, ys, marker="o", linewidth=2, label="measured")
        ax.plot(xs, xs, linestyle="--", color="grey", label="ideal (linear)")
        ax.set_xlabel("Spark task slots (executors x cores)")
        ax.set_ylabel("speedup vs 1 slot")
        ax.set_title("Batch layer speedup - EMR PySpark (measured)")
        ax.grid(alpha=0.3)
        ax.legend()
        fig.tight_layout()
        fig.savefig(RESULTS / "speedup_measured.png", dpi=150)
        plt.close(fig)

        fig, ax = plt.subplots(figsize=(6, 4))
        ax.plot(xs, [r["seconds"] for r in rows], marker="s", color="#c0392b", linewidth=2)
        ax.set_xlabel("Spark task slots")
        ax.set_ylabel("job wall-clock (s)")
        ax.set_title("Batch job runtime vs parallelism (measured)")
        ax.grid(alpha=0.3)
        fig.tight_layout()
        fig.savefig(RESULTS / "batch_runtime_measured.png", dpi=150)
        plt.close(fig)
    except Exception as e:
        print(f"plot skip: {e}")


def _reader_proc(stream, region, stop_evt, q):
    import boto3 as _b

    k = _b.client("kinesis", region_name=region)
    shards = k.describe_stream(StreamName=stream)["StreamDescription"]["Shards"]
    its = [
        k.get_shard_iterator(
            StreamName=stream, ShardId=s["ShardId"], ShardIteratorType="LATEST"
        )["ShardIterator"]
        for s in shards
    ]
    samples = []
    while not stop_evt.is_set():
        nxt = []
        for it in its:
            if not it:
                continue
            try:
                resp = k.get_records(ShardIterator=it, Limit=1000)
            except Exception:
                time.sleep(0.5)
                nxt.append(it)
                continue
            now_ms = time.time() * 1000
            for rec in resp.get("Records", []):
                try:
                    body = json.loads(rec["Data"])
                    sent = body.get("ingested_ms")
                    if sent:
                        samples.append((now_ms - sent) / 1000.0)
                except Exception:
                    pass
            nxt.append(resp.get("NextShardIterator"))
        its = nxt
        time.sleep(0.2)
    q.put(samples)


def mode_latency(rates, events_per_rate):
    from importlib import util as _util

    lg_path = Path(__file__).resolve().parent.parent / "ingestion" / "loadgen.py"
    spec = _util.spec_from_file_location("loadgen", lg_path)
    loadgen = _util.module_from_spec(spec)
    spec.loader.exec_module(loadgen)

    import multiprocessing as mp

    rows = []
    for rate in rates:
        print(f"\n[latency] target rate={rate}/s events={events_per_rate}", flush=True)
        ctx = mp.get_context("spawn")
        stop_evt = ctx.Event()
        q = ctx.Queue()
        reader = ctx.Process(target=_reader_proc, args=(STREAM, REGION, stop_evt, q))
        reader.start()
        time.sleep(6)

        loadgen._stats.update({"sent": 0, "failed": 0, "throttled": 0, "bytes": 0, "latencies": []})
        r = loadgen.run(events_per_rate, rate, 12, 500, 5000, False)

        time.sleep(10)
        stop_evt.set()
        try:
            samples = q.get(timeout=30)
        except Exception:
            samples = []
        reader.join(timeout=20)
        if reader.is_alive():
            reader.terminate()

        s = [x for x in samples if 0 < x < 120]
        if not s:
            print("  no latency samples captured")
            continue
        s.sort()
        row = {
            "target_rate": rate,
            "achieved_eps": round(r["throughput"], 1),
            "samples": len(s),
            "mean_s": round(statistics.fmean(s), 3),
            "p50_s": round(s[len(s) // 2], 3),
            "p95_s": round(s[int(len(s) * 0.95)], 3),
            "max_s": round(s[-1], 3),
        }
        rows.append(row)
        print(f"  achieved={row['achieved_eps']}/s p50={row['p50_s']}s p95={row['p95_s']}s n={len(s)}")

    if rows:
        with (RESULTS / "latency_measured.csv").open("w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
            w.writeheader()
            w.writerows(rows)
        _plot_latency(rows)
    return rows


def _plot_latency(rows):
    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt

        xs = [r["achieved_eps"] for r in rows]
        fig, ax = plt.subplots(figsize=(6, 4))
        ax.plot(xs, [r["p50_s"] for r in rows], marker="o", linewidth=2, label="p50")
        ax.plot(xs, [r["p95_s"] for r in rows], marker="^", linewidth=2, label="p95")
        ax.set_xlabel("achieved ingestion rate (events/s)")
        ax.set_ylabel("end-to-end speed-layer latency (s)")
        ax.set_title("Speed layer latency vs ingestion rate (measured)")
        ax.grid(alpha=0.3)
        ax.legend()
        fig.tight_layout()
        fig.savefig(RESULTS / "latency_measured.png", dpi=150)
        plt.close(fig)

        fig, ax = plt.subplots(figsize=(6, 4))
        ax.plot([r["target_rate"] for r in rows], xs, marker="o", linewidth=2, label="achieved")
        ax.plot([r["target_rate"] for r in rows], [r["target_rate"] for r in rows],
                linestyle="--", color="grey", label="offered")
        ax.set_xlabel("offered rate (events/s)")
        ax.set_ylabel("achieved throughput (events/s)")
        ax.set_title("Ingestion throughput: offered vs achieved (measured)")
        ax.grid(alpha=0.3)
        ax.legend()
        fig.tight_layout()
        fig.savefig(RESULTS / "throughput_measured.png", dpi=150)
        plt.close(fig)
    except Exception as e:
        print(f"plot skip: {e}")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--mode", choices=["speedup", "latency", "all"], default="all")
    ap.add_argument("--events", type=int, default=20000)
    a = ap.parse_args()

    stamp = datetime.now(timezone.utc).isoformat()
    print(f"benchmark run {stamp}\n")

    if a.mode in ("latency", "all"):
        mode_latency([200, 500, 1000, 2000], a.events)
    if a.mode in ("speedup", "all"):
        mode_speedup([(1, 1), (2, 1), (4, 1), (8, 1)])
    print("\nresults in", RESULTS)
