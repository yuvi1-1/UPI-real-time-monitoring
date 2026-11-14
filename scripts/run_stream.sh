#!/usr/bin/env bash
# Submit the Spark Structured Streaming job to the Spark master (Docker version).
#   ./scripts/run_stream.sh
set -euo pipefail
cd "$(dirname "$0")/.."
PROJECT=upi
docker compose exec spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 \
  --conf spark.jars.ivy=/tmp/.ivy2 \
  --conf spark.executor.memory=768m \
  --conf spark.cores.max=4 \
  /app/${PROJECT}/stream_job.py
