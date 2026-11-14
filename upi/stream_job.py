"""Spark Structured Streaming job for the UPI project.

Submit on the cluster (see scripts/run_stream.sh):
    spark-submit --master spark://<master>:7077 \
      --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 upi/stream_job.py
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from common.engine import run_stream  # noqa: E402
from upi import logic  # noqa: E402

if __name__ == "__main__":
    run_stream("upi", logic)
