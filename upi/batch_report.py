"""Batch analytics over the full UPI history stored in HDFS (Parquet).

    spark-submit --master spark://<master>:7077 upi/batch_report.py
Results are written to HDFS (CSV) and to HBase table upi_reports, which the dashboard shows.
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from common.batch import run_batch  # noqa: E402
from upi import logic  # noqa: E402

if __name__ == "__main__":
    run_batch("upi", logic)
