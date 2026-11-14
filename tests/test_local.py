"""Local smoke test - no Kafka/HBase/HDFS needed.

Generates events with the real generators, runs them through the SAME Spark streaming code
(file source instead of Kafka, in-memory store instead of HBase), then runs the batch reports.

    STORE=memory python tests/test_local.py
"""
import json
import os
import shutil
import sys
import tempfile
import time

os.environ["STORE"] = "memory"
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pyspark.sql import SparkSession  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402

from common.batch import collect_reports  # noqa: E402
from common.engine import make_batch_processor  # noqa: E402
from common.store import get_store  # noqa: E402
import upi.generator as ug  # noqa: E402
import upi.logic as ul  # noqa: E402


def write_events(folder, gen, n_files=4, per_file=600):
    os.makedirs(folder, exist_ok=True)
    for i in range(n_files):
        events = []
        while len(events) < per_file:
            events.extend(gen.next_events())
        with open(os.path.join(folder, f"part{i}.json"), "w") as f:
            f.write("\n".join(json.dumps(e) for e in events))


def run_project(spark, name, gen, logic, tmp):
    src = os.path.join(tmp, name, "in")
    write_events(src, gen)
    store = get_store(name)
    # same parsing as Kafka path: JSON string -> schema -> timestamps
    stream = (spark.readStream.format("text").option("maxFilesPerTrigger", 1).load(src)
              .select(F.from_json("value", logic.SCHEMA).alias("e")).select("e.*")
              .withColumn("event_time", F.to_timestamp("event_time"))
              .withColumn("event_date", F.to_date("event_time")))
    q = (stream.writeStream.foreachBatch(make_batch_processor(name, logic, store))
         .option("checkpointLocation", os.path.join(tmp, name, "chk")).trigger(availableNow=True).start())
    q.awaitTermination()

    total = store.scan_metrics("total")["all"]
    print(f"\n[{name}] totals: {total}")
    for dim, _ in logic.DIMENSIONS[:2]:
        print(f"[{name}] {dim}: {dict(list(store.scan_metrics(dim).items())[:5])}")
    print(f"[{name}] alert types: {store.scan_metrics('alert_type')}")
    for a in store.latest_alerts(3):
        print(f"[{name}] alert: {a}")
    assert total["count"] == 4 * 600 or total["count"] > 2400, total
    assert store.latest_alerts(1), "expected at least one alert"

    # batch reports on the same data
    df = (spark.read.text(src).select(F.from_json("value", logic.SCHEMA).alias("e")).select("e.*")
          .withColumn("event_time", F.to_timestamp("event_time")))
    reports = collect_reports(logic.build_reports(df))
    for rname, rows in reports.items():
        store.put_report(rname, rows)
        print(f"[{name}] report {rname}: {rows[:2]}")
    return store


if __name__ == "__main__":
    t0 = time.time()
    spark = SparkSession.builder.master("local[2]").appName("local-test").config("spark.ui.enabled", "false").config("spark.sql.shuffle.partitions", "4").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    tmp = tempfile.mkdtemp()
    try:
        run_project(spark, "upi", ug, ul, tmp)
    finally:
        shutil.rmtree(tmp, ignore_errors=True)
    print(f"\nALL OK in {time.time() - t0:.0f}s")
