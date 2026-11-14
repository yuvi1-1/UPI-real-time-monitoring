#!/usr/bin/env bash
# ./scripts/native/run_batch.sh     -> batch report over all data in HDFS
source "$(dirname "$0")/env.sh"
cd "$PYTHONPATH"
spark-submit --master $SPARK_MASTER_URL $SPARK_DRIVER_OPTS --conf spark.executor.memory=1g upi/batch_report.py
