#!/usr/bin/env bash
# Run the batch analytics job over the full history stored in HDFS (Docker version).
set -euo pipefail
cd "$(dirname "$0")/.."
PROJECT=upi
docker compose exec spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --conf spark.jars.ivy=/tmp/.ivy2 \
  --conf spark.executor.memory=768m \
  /app/${PROJECT}/batch_report.py
