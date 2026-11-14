#!/usr/bin/env bash
# Start the whole cluster on ONE machine with Docker (development / backup demo).
#   ./scripts/start.sh
set -euo pipefail
cd "$(dirname "$0")/.."
export PROJECT=upi
sed -i.bak "s/^PROJECT=.*/PROJECT=${PROJECT}/" .env && rm -f .env.bak

echo ">> starting containers for project '$PROJECT'"
docker compose up -d --build

echo ">> waiting for HDFS to leave safe mode"
docker compose exec -T namenode hdfs dfsadmin -safemode wait
docker compose exec -T namenode hdfs dfs -mkdir -p /bigdata/upi
docker compose exec -T namenode hdfs dfsadmin -report | grep -E "Live datanodes|^Name:" || true

echo ">> creating HBase tables"
docker compose exec -T dashboard python scripts/create_tables.py

cat <<MSG

Cluster is up.
  Dashboard       http://localhost:5000
  HDFS NameNode   http://localhost:9870
  Spark master    http://localhost:8080
  HBase master    http://localhost:16010

Now start the streaming job (keep it running in its own terminal):
  ./scripts/run_stream.sh
MSG
