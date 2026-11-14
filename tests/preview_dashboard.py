"""Preview the dashboard on ONE laptop without Kafka/HBase/HDFS.

Runs the real generator + the real Spark aggregation/alert code in local mode, keeps the results
in memory instead of HBase, and serves the real dashboard. Useful to check the UI before the cluster is up.

    python tests/preview_dashboard.py      # then open http://localhost:5000
"""
import json
import os
import random
import sys
import threading
import time
from datetime import datetime, timedelta

project = "upi"
os.environ["STORE"] = "memory"
os.environ["PROJECT"] = project
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pyspark.sql import SparkSession  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402

from common.batch import collect_reports  # noqa: E402
from common.engine import make_batch_processor  # noqa: E402
from common.store import get_store  # noqa: E402
from dashboard.app import app  # noqa: E402

import upi.generator as gen  # noqa: E402
import upi.logic as logic  # noqa: E402

spark = (SparkSession.builder.master("local[2]").appName("dashboard-preview")
         .config("spark.ui.enabled", "false").config("spark.sql.shuffle.partitions", "2").getOrCreate())
spark.sparkContext.setLogLevel("ERROR")
store = get_store(project)
process = make_batch_processor(project, logic, store)
history = []


def to_df(events):
    df = spark.createDataFrame([(json.dumps(e),) for e in events], ["value"])
    return (df.select(F.from_json("value", logic.SCHEMA).alias("e")).select("e.*")
            .withColumn("event_time", F.to_timestamp("event_time")))


def events_at(ts, n):
    out = []
    while len(out) < n:
        for e in gen.next_events():
            e["event_time"] = (ts + timedelta(seconds=random.uniform(0, 3))).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3]
            out.append(e)
    return out


# back-fill the last 30 minutes so the timeline has a shape
now = datetime.now()
batch_id = 0
backfill = []
for m in range(30, 0, -1):
    for s in range(0, 60, 15):
        backfill += events_at(now - timedelta(minutes=m, seconds=-s), int(random.uniform(15, 30)))
history += backfill
process(to_df(backfill), batch_id)
for name, rows in collect_reports(logic.build_reports(to_df(history))).items():
    store.put_report(name, rows)
store.put_report("_meta", [{"events_analysed": len(history), "generated_at": now.strftime("%Y-%m-%d %H:%M:%S")}])


def live_loop():
    global batch_id
    while True:
        batch_id += 1
        evs = events_at(datetime.now(), random.randint(25, 45))
        process(to_df(evs), batch_id)
        time.sleep(3)


threading.Thread(target=live_loop, daemon=True).start()
print(f"\nPreview for '{project}' running -> http://localhost:5000\n")
app.run(host="0.0.0.0", port=int(os.getenv("PORT", "5000")))
