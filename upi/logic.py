"""UPI transaction analytics: schema, what to aggregate, and the anomaly rules."""
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, StringType, StructField, StructType

SCHEMA = StructType([
    StructField("txn_id", StringType()),
    StructField("event_time", StringType()),
    StructField("payer_vpa", StringType()),
    StructField("payee_vpa", StringType()),
    StructField("payer_bank", StringType()),
    StructField("payee_bank", StringType()),
    StructField("city", StringType()),
    StructField("app", StringType()),
    StructField("txn_type", StringType()),       # P2P or P2M
    StructField("category", StringType()),       # merchant category (P2M) or "Transfer"
    StructField("amount", DoubleType()),
    StructField("status", StringType()),         # SUCCESS / FAILED
])

# group-by dimensions -> one HBase row per value, e.g. "bank|SBI"
DIMENSIONS = [
    ("bank", "payer_bank"),
    ("city", "city"),
    ("app", "app"),
    ("category", "category"),
    ("type", "txn_type"),
    ("status", "status"),
    ("hour", "lpad(hour(event_time), 2, '0')"),
]

# counters summed for every dimension value (amount stored in paise so it fits a 64-bit integer counter)
SUMS = {
    "amount": "cast(round(amount * 100) as long)",
    "success": "cast(status = 'SUCCESS' as int)",
    "failed": "cast(status = 'FAILED' as int)",
}

# ---------------- anomaly rules ----------------
HIGH_AMOUNT = 100_000      # single payment of 1 lakh or more
RAPID_REPEAT = 5           # 5+ payments by one payer inside one micro-batch (~5 seconds)
FAILED_BURST = 3           # 3+ failed attempts by one payer inside one micro-batch


def detect_alerts(df: DataFrame) -> DataFrame:
    high = df.where(F.col("amount") >= HIGH_AMOUNT).select(
        F.col("txn_id").alias("id"),
        F.lit("HIGH_AMOUNT").alias("alert_type"),
        "event_time",
        F.col("payer_vpa").alias("payer"),
        "city",
        F.round("amount", 2).alias("amount"),
        F.concat(F.lit("Single payment of Rs "), F.format_number("amount", 0), F.lit(" to "), "payee_vpa").alias("detail"),
    )

    per_payer = df.groupBy("payer_vpa").agg(
        F.count(F.lit(1)).alias("n"),
        F.sum("amount").alias("total"),
        F.sum(F.when(F.col("status") == "FAILED", 1).otherwise(0)).alias("failed"),
        F.max("event_time").alias("event_time"),
        F.first("city").alias("city"),
        F.first("txn_id").alias("first_txn"),
    )

    rapid = per_payer.where(F.col("n") >= RAPID_REPEAT).select(
        F.concat(F.lit("R-"), "first_txn").alias("id"),
        F.lit("RAPID_REPEAT").alias("alert_type"),
        "event_time",
        F.col("payer_vpa").alias("payer"),
        "city",
        F.round("total", 2).alias("amount"),
        F.concat(F.col("n").cast("string"), F.lit(" payments within a few seconds")).alias("detail"),
    )

    failed = per_payer.where(F.col("failed") >= FAILED_BURST).select(
        F.concat(F.lit("F-"), "first_txn").alias("id"),
        F.lit("FAILED_BURST").alias("alert_type"),
        "event_time",
        F.col("payer_vpa").alias("payer"),
        "city",
        F.round("total", 2).alias("amount"),
        F.concat(F.col("failed").cast("string"), F.lit(" failed attempts in a row (possible PIN guessing)")).alias("detail"),
    )

    return high.unionByName(rapid).unionByName(failed)


# ---------------- batch reports (run on the full history in HDFS) ----------------
def build_reports(df: DataFrame) -> dict:
    """Return {report_name: DataFrame}. Used by upi/batch_report.py on all archived data."""
    r2 = lambda c: F.round(c, 2)  # noqa: E731
    return {
        "bank_summary": df.groupBy(F.col("payer_bank").alias("bank")).agg(
            F.count(F.lit(1)).alias("transactions"),
            r2(F.sum("amount")).alias("total_value"),
            r2(F.avg("amount")).alias("avg_value"),
            r2(F.avg(F.when(F.col("status") == "FAILED", 100.0).otherwise(0.0))).alias("failure_rate_pct"),
        ).orderBy(F.desc("total_value")),
        "city_summary": df.groupBy("city").agg(
            F.count(F.lit(1)).alias("transactions"),
            r2(F.sum("amount")).alias("total_value"),
            r2(F.avg("amount")).alias("avg_value"),
            F.sum(F.when(F.col("amount") >= HIGH_AMOUNT, 1).otherwise(0)).alias("high_value_txns"),
        ).orderBy(F.desc("total_value")),
        "category_share": df.where(F.col("txn_type") == "P2M").groupBy("category").agg(
            F.count(F.lit(1)).alias("transactions"),
            r2(F.sum("amount")).alias("total_value"),
        ).orderBy(F.desc("total_value")),
        "app_share": df.groupBy("app").agg(
            F.count(F.lit(1)).alias("transactions"),
            r2(F.sum("amount")).alias("total_value"),
        ).orderBy(F.desc("transactions")),
        "largest_transactions": df.orderBy(F.desc("amount")).select(
            "txn_id", F.date_format("event_time", "yyyy-MM-dd HH:mm:ss").alias("time"),
            "payer_vpa", "city", r2("amount").alias("amount"), "status",
        ).limit(10),
    }
