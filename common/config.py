"""Central configuration. Every value can be overridden with an environment variable,
so the same code runs inside Docker and on the real 3-PC cluster."""
import os

PROJECT = os.getenv("PROJECT", "upi")

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
HBASE_HOST = os.getenv("HBASE_HOST", "hbase")
HBASE_PORT = int(os.getenv("HBASE_PORT", "9090"))  # HBase Thrift server
HDFS_URL = os.getenv("HDFS_URL", "hdfs://namenode:9000")
SPARK_MASTER = os.getenv("SPARK_MASTER_URL", "spark://spark-master:7077")

# Store backend: "hbase" (real) or "memory" (only for local tests without a cluster)
STORE = os.getenv("STORE", "hbase")

TOPICS = {"upi": "upi_transactions"}


def topic(project: str = PROJECT) -> str:
    return TOPICS[project]


def hdfs_path(project: str, kind: str) -> str:
    """e.g. hdfs://namenode:9000/bigdata/upi/raw"""
    return f"{HDFS_URL}/bigdata/{project}/{kind}"
