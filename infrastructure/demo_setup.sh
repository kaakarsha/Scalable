#!/bin/bash
# One-command demo setup. Brings up EC2, EMR and the producer, then reports readiness.
# Wrapped in main() so bash parses the whole file before the self-update runs.

main() {
  set -uo pipefail

  APP="$HOME/Scalable"
  REGION="us-east-1"
  ASG="earthquake-pipeline-asg"
  EIP="50.19.98.60"
  PY="$APP/.venv/bin/python"

  ok()   { printf "  [ok]   %s\n" "$1"; }
  info() { printf "  ...    %s\n" "$1"; }
  fail() { printf "  [FAIL] %s\n" "$1"; }
  step() { printf "\n== %s ==\n" "$1"; }

  step "0/4  Refreshing code"
  cd "$APP" || { fail "no $APP"; return 1; }
  git fetch origin main -q && git reset --hard origin/main -q
  ok "code at $(git rev-parse --short HEAD)"
  if [ ! -x "$PY" ]; then
    info "building virtual environment (one off, about 2 minutes)"
    python3 -m venv "$APP/.venv" >/dev/null 2>&1
    "$APP/.venv/bin/pip" install -q --upgrade pip
    "$APP/.venv/bin/pip" install -q boto3 requests python-dotenv pyathena matplotlib
  fi
  bash "$APP/infrastructure/cloudshell_setup.sh" >/dev/null 2>&1
  ok "environment ready"

  step "1/4  EC2 and dashboard"
  code=$(curl -s -o /dev/null -w "%{http_code}" --max-time 10 "http://$EIP:8501" 2>/dev/null)
  if [ "$code" = "200" ]; then
    ok "dashboard live at http://$EIP:8501"
  else
    info "dashboard not answering, requesting an instance"
    aws autoscaling set-desired-capacity --auto-scaling-group-name "$ASG" \
      --desired-capacity 1 --region "$REGION" >/dev/null 2>&1
    for i in $(seq 1 24); do
      sleep 15
      code=$(curl -s -o /dev/null -w "%{http_code}" --max-time 10 "http://$EIP:8501" 2>/dev/null)
      [ "$code" = "200" ] && break
      info "waiting for bootstrap ($((i*15))s)"
    done
    [ "$code" = "200" ] && ok "dashboard live at http://$EIP:8501" \
                        || fail "dashboard still not answering, check the ASG in the console"
  fi

  step "2/4  EMR cluster"
  cid=$(aws emr list-clusters --active --region "$REGION" \
        --query "Clusters[?Name=='earthquake-pipeline']|[0].Id" --output text 2>/dev/null)
  if [ -n "$cid" ] && [ "$cid" != "None" ]; then
    ok "reusing existing cluster $cid"
    echo "$cid" > "$APP/../config/emr_cluster_id.txt" 2>/dev/null || true
  else
    info "creating cluster, this takes about 8 minutes"
    "$PY" "$APP/batch/launch_cluster.py" 2>&1 | grep -E "cluster=|state=|ready=" | tail -3
    cid=$(aws emr list-clusters --active --region "$REGION" \
          --query "Clusters[?Name=='earthquake-pipeline']|[0].Id" --output text 2>/dev/null)
    [ -n "$cid" ] && [ "$cid" != "None" ] && ok "cluster $cid ready" || { fail "cluster did not start"; return 1; }
  fi

  step "3/4  Batch view"
  done_steps=$(aws emr list-steps --cluster-id "$cid" --region "$REGION" \
               --query "length(Steps[?Status.State=='COMPLETED'])" --output text 2>/dev/null)
  if [ "${done_steps:-0}" -gt 0 ] 2>/dev/null; then
    ok "$done_steps completed step(s) already on the cluster"
  else
    info "running the Spark job, about 2 minutes"
    "$PY" "$APP/batch/submit.py" 2>&1 | tail -1
  fi
  "$PY" "$APP/serving/refresh_athena.py" >/dev/null 2>&1 && ok "Athena table refreshed" \
    || info "Athena refresh skipped"

  step "4/4  Producer"
  if pgrep -f loadgen.py >/dev/null 2>&1; then
    ok "producer already running"
  else
    nohup "$PY" "$APP/ingestion/loadgen.py" --events 400000 --workers 24 --no-s3 \
      > /tmp/producer.log 2>&1 &
    sleep 8
    pgrep -f loadgen.py >/dev/null 2>&1 && ok "producer started, writing to /tmp/producer.log" \
                                        || fail "producer did not start, see /tmp/producer.log"
  fi

  printf "\n== Ready to record ==\n"
  printf "  dashboard    http://%s:8501\n" "$EIP"
  printf "  EMR cluster  %s\n" "$cid"
  printf "  producer     %s\n" "$(pgrep -fc loadgen.py 2>/dev/null || echo 0) process(es)"
  printf "\n  Open the seven tabs, then start recording.\n\n"
}

main "$@"
