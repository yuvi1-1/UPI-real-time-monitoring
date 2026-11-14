#!/usr/bin/env bash
# ./scripts/native/run_stream.sh    -> streaming job on the Spark cluster (master + 2 workers)
source "$(dirname "$0")/env.sh"
cd "$PYTHONPATH"
spark-submit --master $SPARK_MASTER_URL $SPARK_DRIVER_OPTS \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 \
  --conf spark.executor.memory=1g --conf spark.cores.max=4 \
  --conf spark.executorEnv.PYTHONPATH=$PYTHONPATH \
  upi/stream_job.py
