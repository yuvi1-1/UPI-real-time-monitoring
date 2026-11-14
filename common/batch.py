"""Shared runner for the batch (historical) analytics job."""
from datetime import datetime

from pyspark.sql import SparkSession

from common import config
from common.store import get_store


def collect_reports(reports, limit=50):
    return {name: [r.asDict() for r in df.limit(limit).collect()] for name, df in reports.items()}


def run_batch(project, module):
    spark = (SparkSession.builder.appName(f"{project}-batch-report")
             .config("spark.sql.shuffle.partitions", "12").getOrCreate())
    spark.sparkContext.setLogLevel("WARN")
    src = config.hdfs_path(project, "raw")
    df = spark.read.parquet(src).cache()
    total = df.count()
    print(f"Read {total} archived events from {src}")

    reports = module.build_reports(df)
    store = get_store(project)
    store.create_tables()
    for name, rdf in reports.items():
        out = config.hdfs_path(project, f"reports/{name}")
        rdf.coalesce(1).write.mode("overwrite").option("header", True).csv(out)
        print(f"\n=== {name}  (saved to {out}) ===")
        rdf.show(15, truncate=False)

    data = collect_reports(reports)
    data["_meta"] = [{"events_analysed": total, "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S")}]
    for name, rows in data.items():
        store.put_report(name, rows)
    print("Reports stored in HBase table", f"{project}_reports")
    spark.stop()
