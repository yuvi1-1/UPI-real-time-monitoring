"""Shared Spark Structured Streaming engine used by both projects.

Pipeline:
    Kafka topic --> Spark Structured Streaming --+--> HDFS (raw events, Parquet, partitioned by date)
                                                 |
                                                 +--> foreachBatch every few seconds:
                                                        * group-by aggregates  --> HBase counters (<project>_metrics)
                                                        * anomaly / alert rules --> HBase rows    (<project>_alerts)

The project package (upi/) provides:
    SCHEMA      - Spark schema of one JSON event
    DIMENSIONS  - list of (name, SQL expression) to group by
    SUMS        - dict of counter name -> SQL expression to sum
    detect_alerts(df) -> DataFrame of alerts
"""
import time

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from common import config
from common.store import get_store

ALWAYS_DIMENSIONS = [
    ("total", "'all'"),
    ("minute", "date_format(event_time, 'yyyyMMddHHmm')"),
]


def aggregate(df: DataFrame, dimensions, sums):
    """Return list of (row_key, {counter: value}) for every dimension value seen in this micro-batch."""
    out = []
    agg_exprs = [F.count(F.lit(1)).alias("count")] + [
        F.sum(F.expr(expr)).cast("long").alias(name) for name, expr in sums.items()
    ]
    for dim, expr in ALWAYS_DIMENSIONS + list(dimensions):
        for r in df.groupBy(F.expr(expr).alias("k")).agg(*agg_exprs).collect():
            if r["k"] is None:
                continue
            out.append((f"{dim}|{r['k']}", {c: int(r[c] or 0) for c in ["count", *sums]}))
    return out


def alerts_to_dicts(alerts_df: DataFrame, limit=500):
    # format the time inside Spark so it shows exactly the wall-clock time the event carried
    alerts_df = (alerts_df
                 .withColumn("ts_ms", (F.unix_timestamp("event_time") * 1000).cast("long"))
                 .withColumn("event_time", F.date_format("event_time", "yyyy-MM-dd HH:mm:ss")))
    rows = []
    for r in alerts_df.limit(limit).collect():
        d = {k: v for k, v in r.asDict().items() if v is not None}
        d.setdefault("ts_ms", int(time.time() * 1000))
        rows.append(d)
    return rows


def make_batch_processor(project, module, store=None):
    """Builds the foreachBatch function. Runs on the Spark driver; heavy work is done by executors."""

    holder = {"store": store}

    def get():
        if holder["store"] is None:
            holder["store"] = get_store(project)  # one HBase connection, reused across batches
        return holder["store"]

    def process(batch_df: DataFrame, batch_id: int):
        batch_df = batch_df.persist()
        n = batch_df.count()
        if n == 0:
            batch_df.unpersist()
            return
        rows = aggregate(batch_df, module.DIMENSIONS, module.SUMS)
        alerts = alerts_to_dicts(module.detect_alerts(batch_df))
        if alerts:
            rows.append(("total|all", {"alerts": len(alerts)}))
            for a in alerts:
                rows.append((f"alert_type|{a['alert_type']}", {"count": 1}))
        try:
            get().incr_many(rows)
            get().put_alerts(alerts)
        except Exception as e:  # e.g. Thrift connection dropped: reconnect once and retry
            print(f"[batch {batch_id}] HBase write failed ({e}), reconnecting", flush=True)
            holder["store"] = None
            get().incr_many(rows)
            get().put_alerts(alerts)
        batch_df.unpersist()
        print(f"[batch {batch_id}] events={n} metric_rows={len(rows)} alerts={len(alerts)}", flush=True)

    return process


def parse_kafka(spark: SparkSession, project, schema):
    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", config.KAFKA_BOOTSTRAP)
        .option("subscribe", config.topic(project))
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
    )
    return (
        raw.select(F.from_json(F.col("value").cast("string"), schema).alias("e"))
        .select("e.*")
        .withColumn("event_time", F.to_timestamp("event_time"))
        .withColumn("event_date", F.to_date("event_time"))
    )


def run_stream(project, module, trigger_seconds=5):
    spark = (SparkSession.builder.appName(f"{project}-realtime-analytics")
             .config("spark.sql.shuffle.partitions", "6")  # small micro-batches: few partitions = low latency
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")
    get_store(project).create_tables()

    events = parse_kafka(spark, project, module.SCHEMA)

    # 1) archive every raw event in HDFS (Parquet, partitioned by date) for batch analytics later
    (
        events.writeStream.format("parquet")
        .option("path", config.hdfs_path(project, "raw"))
        .option("checkpointLocation", config.hdfs_path(project, "checkpoints/raw"))
        .partitionBy("event_date")
        .trigger(processingTime="30 seconds")
        .start()
    )

    # 2) live metrics + alerts into HBase
    (
        events.writeStream.foreachBatch(make_batch_processor(project, module))
        .option("checkpointLocation", config.hdfs_path(project, "checkpoints/metrics"))
        .trigger(processingTime=f"{trigger_seconds} seconds")
        .start()
    )

    print(f"Streaming '{config.topic(project)}' -> HDFS + HBase. Spark UI: http://<master>:4040", flush=True)
    spark.streams.awaitAnyTermination()
