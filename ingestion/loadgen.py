import argparse
import csv
import json
import os
import random
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from pathlib import Path

import boto3
import requests
from botocore.config import Config
from dotenv import load_dotenv

load_dotenv()

REGION = os.getenv("AWS_REGION", "us-east-1")
STREAM = os.getenv("KINESIS_STREAM", "earthquake-stream")
S3_RAW = os.getenv("S3_RAW", "earthquake-pipeline-raw")
FEED = os.getenv(
    "USGS_FEED",
    "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_day.geojson",
)
RESULTS = Path(__file__).resolve().parent.parent / "benchmarks" / "results"

_cfg = Config(retries={"max_attempts": 10, "mode": "adaptive"}, max_pool_connections=64)
kinesis = boto3.client("kinesis", region_name=REGION, config=_cfg)
s3 = boto3.client("s3", region_name=REGION, config=_cfg)

_lock = threading.Lock()
_stats = {"sent": 0, "failed": 0, "throttled": 0, "bytes": 0, "latencies": []}


def region_key(place):
    if not place:
        return "unknown"
    parts = [p.strip() for p in place.split(",")]
    return parts[-1].lower() if parts else "unknown"


def parse_feature(f):
    p = f.get("properties") or {}
    g = f.get("geometry") or {}
    c = g.get("coordinates") or [None, None, None]
    return {
        "id": f.get("id"),
        "mag": p.get("mag"),
        "place": p.get("place"),
        "time": p.get("time"),
        "type": p.get("type"),
        "lon": c[0],
        "lat": c[1],
        "depth": c[2],
        "region": region_key(p.get("place")),
    }


def fetch_seed():
    r = requests.get(FEED, timeout=30)
    r.raise_for_status()
    feats = r.json().get("features") or []
    seed = [parse_feature(f) for f in feats]
    seed = [s for s in seed if s.get("region")]
    if not seed:
        raise SystemExit("empty USGS feed")
    return seed


def amplify(seed, i):
    base = seed[i % len(seed)]
    ev = dict(base)
    ev["id"] = f"{base['id']}-r{i}"
    ev["source_id"] = base["id"]
    ev["synthetic"] = i >= len(seed)
    mag = base.get("mag")
    ev["mag"] = round(mag + random.uniform(-0.3, 0.3), 2) if mag is not None else None
    ev["time"] = int(time.time() * 1000)
    ev["ingested_at"] = datetime.now(timezone.utc).isoformat()
    ev["ingested_ms"] = time.time() * 1000
    return ev


def put_batch(records):
    now_ms = time.time() * 1000
    for e in records:
        e["ingested_ms"] = now_ms
    entries = [
        {"Data": json.dumps(e).encode("utf-8"), "PartitionKey": str(e["region"])[:256]}
        for e in records
    ]
    nbytes = sum(len(x["Data"]) for x in entries)
    attempt = 0
    while entries and attempt < 8:
        t0 = time.time()
        resp = kinesis.put_records(StreamName=STREAM, Records=entries)
        dt = time.time() - t0
        failed = resp.get("FailedRecordCount", 0)
        ok = len(entries) - failed
        with _lock:
            _stats["sent"] += ok
            _stats["bytes"] += nbytes if attempt == 0 else 0
            _stats["latencies"].append(dt)
            if failed:
                _stats["throttled"] += failed
        if not failed:
            return
        retry = []
        for rec, res in zip(entries, resp["Records"]):
            if res.get("ErrorCode"):
                retry.append(rec)
        entries = retry
        attempt += 1
        time.sleep(min(0.1 * (2**attempt), 2.0))
    if entries:
        with _lock:
            _stats["failed"] += len(entries)


def put_s3_chunk(records, part):
    body = "\n".join(json.dumps(e) for e in records).encode("utf-8")
    s3.put_object(
        Bucket=S3_RAW,
        Key=f"data/load_{part:05d}.ndjson",
        Body=body,
        ContentType="application/x-ndjson",
    )
    return len(body)


def run(total, rate, workers, batch_size, s3_chunk, to_s3):
    seed = fetch_seed()
    print(f"seed_events={len(seed)} target={total} rate={rate}/s workers={workers}")

    batches = []
    for start in range(0, total, batch_size):
        n = min(batch_size, total - start)
        batches.append([amplify(seed, start + k) for k in range(n)])

    s3_parts = []
    if to_s3:
        flat = [e for b in batches for e in b]
        for i in range(0, len(flat), s3_chunk):
            s3_parts.append(flat[i : i + s3_chunk])

    t0 = time.time()
    per_batch_delay = (batch_size / rate) if rate > 0 else 0

    def worker(idx_batch):
        idx, batch = idx_batch
        if per_batch_delay:
            target = t0 + (idx * per_batch_delay) / max(workers, 1) * workers
            slack = target - time.time()
            if slack > 0:
                time.sleep(slack)
        put_batch(batch)

    with ThreadPoolExecutor(max_workers=workers) as ex:
        list(ex.map(worker, enumerate(batches)))
    kin_elapsed = time.time() - t0

    s3_bytes = 0
    s3_elapsed = 0.0
    if s3_parts:
        ts0 = time.time()
        with ThreadPoolExecutor(max_workers=min(workers, 16)) as ex:
            s3_bytes = sum(ex.map(lambda p: put_s3_chunk(p[1], p[0]), enumerate(s3_parts)))
        s3_elapsed = time.time() - ts0

    lats = sorted(_stats["latencies"])
    p50 = lats[len(lats) // 2] if lats else 0
    p95 = lats[int(len(lats) * 0.95)] if lats else 0
    thr = _stats["sent"] / kin_elapsed if kin_elapsed else 0

    print("\n--- kinesis ingest ---")
    print(f"sent={_stats['sent']} failed={_stats['failed']} throttled_retries={_stats['throttled']}")
    print(f"elapsed={kin_elapsed:.2f}s throughput={thr:.1f} events/s")
    print(f"payload={_stats['bytes']/1e6:.2f} MB  ({_stats['bytes']/1e6/kin_elapsed:.2f} MB/s)")
    print(f"put_records latency p50={p50*1000:.1f}ms p95={p95*1000:.1f}ms")
    if s3_parts:
        print("\n--- s3 raw ---")
        print(f"objects={len(s3_parts)} bytes={s3_bytes/1e6:.2f} MB elapsed={s3_elapsed:.2f}s")

    RESULTS.mkdir(parents=True, exist_ok=True)
    with (RESULTS / "loadgen_runs.csv").open("a", newline="") as f:
        w = csv.writer(f)
        if f.tell() == 0:
            w.writerow(
                ["ts", "target", "rate", "workers", "sent", "failed", "elapsed_s",
                 "throughput_eps", "mb", "p50_ms", "p95_ms"]
            )
        w.writerow([
            datetime.now(timezone.utc).isoformat(), total, rate, workers,
            _stats["sent"], _stats["failed"], round(kin_elapsed, 3),
            round(thr, 2), round(_stats["bytes"] / 1e6, 3),
            round(p50 * 1000, 2), round(p95 * 1000, 2),
        ])
    return {"sent": _stats["sent"], "elapsed": kin_elapsed, "throughput": thr}


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--events", type=int, default=50000)
    ap.add_argument("--rate", type=float, default=0)
    ap.add_argument("--workers", type=int, default=8)
    ap.add_argument("--batch-size", type=int, default=500)
    ap.add_argument("--s3-chunk", type=int, default=5000)
    ap.add_argument("--no-s3", action="store_true")
    a = ap.parse_args()
    run(a.events, a.rate, a.workers, min(a.batch_size, 500), a.s3_chunk, not a.no_s3)
