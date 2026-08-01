import json
import os
import sys
import time
from collections import defaultdict

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "ingestion"))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "speed"))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "serving"))

from producer import fetch_usgs, parse_feature
from consumer import SlidingWindow, hotspot_flags
from query import merge

DURATION_S = int(os.getenv("RT_DURATION", "45"))
POLL_S = float(os.getenv("RT_POLL", "8"))
WINDOW_S = int(os.getenv("WINDOW_SECONDS", "300"))
TOP_N = int(os.getenv("TOP_N", "10"))


def batch_from_history(events):
    counts = defaultdict(int)
    mag_sum = defaultdict(float)
    mag_n = defaultdict(int)
    for ev in events:
        r = ev.get("region") or "unknown"
        counts[r] += 1
        m = ev.get("mag")
        if m is not None:
            mag_sum[r] += float(m)
            mag_n[r] += 1
    ranked = sorted(counts.items(), key=lambda x: -x[1])
    return ranked, {
        r: (mag_sum[r] / mag_n[r] if mag_n[r] else 0.0) for r in counts
    }


def main():
    print("=== LOCAL REALTIME PIPELINE (live USGS, no AWS) ===")
    print(f"feed=USGS all_hour  duration={DURATION_S}s  poll={POLL_S}s  window={WINDOW_S}s")
    window = SlidingWindow(window_s=WINDOW_S, bucket_s=10)
    history = []
    seen = set()
    t0 = time.time()
    rounds = 0
    total_new = 0

    while time.time() - t0 < DURATION_S:
        rounds += 1
        try:
            features = fetch_usgs()
        except Exception as e:
            print(f"[round {rounds}] fetch error: {e}")
            time.sleep(POLL_S)
            continue

        new_events = []
        for f in features:
            ev = parse_feature(f)
            eid = ev.get("id")
            if not eid or eid in seen:
                continue
            seen.add(eid)
            new_events.append(ev)
            history.append(ev)
            window.add(ev.get("region") or "unknown", ev.get("mag"))

        total_new += len(new_events)
        speed = window.top(TOP_N)
        flags = hotspot_flags(speed)
        batch_ranked, _ = batch_from_history(history)
        batch_view = batch_ranked[:TOP_N]
        speed_view = [(r, c) for r, c, _ in speed]
        merged = merge(batch_view, speed_view, top_n=TOP_N)

        elapsed = time.time() - t0
        print(f"\n--- t={elapsed:.0f}s round={rounds} new={len(new_events)} "
              f"total_unique={len(seen)} history={len(history)} ---")
        print("SPEED top-N (last window):")
        for i, (r, c, avg) in enumerate(speed[:5], 1):
            hot = " HOT" if flags.get(r) else ""
            print(f"  {i}. {r:<20} count={c} avg_mag={avg:.2f}{hot}")
        print("BATCH top-N (session full history):")
        for i, (r, c) in enumerate(batch_view[:5], 1):
            print(f"  {i}. {r:<20} events={c}")
        print("SERVING merge (batch + speed):")
        for i, (r, c) in enumerate(merged[:5], 1):
            print(f"  {i}. {r:<20} score={c}")

        if time.time() - t0 >= DURATION_S:
            break
        time.sleep(POLL_S)

    print("\n=== REALTIME RUN SUMMARY ===")
    print(f"rounds={rounds} unique_events={len(seen)} total_new_ingested={total_new}")
    print(f"elapsed={time.time()-t0:.1f}s")
    if not seen:
        print("WARNING: no events ingested from USGS")
        return 1
    print("OK: live ingest + speed window + batch + serving merge all ran on real data")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
