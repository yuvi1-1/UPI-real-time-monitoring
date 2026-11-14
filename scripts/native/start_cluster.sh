#!/usr/bin/env bash
# Run on the MASTER PC. Starts HDFS (namenode + datanodes on workers over SSH), Spark (master + workers),
# Kafka (KRaft) and HBase (+ Thrift server).
set -e
source "$(dirname "$0")/env.sh"

echo ">> HDFS";   start-dfs.sh
hdfs dfsadmin -safemode wait
hdfs dfs -mkdir -p /bigdata/upi

echo ">> Spark";  start-master.sh && start-workers.sh

echo ">> Kafka";  kafka-server-start.sh -daemon $KAFKA_HOME/config/kraft/server.properties
sleep 8
for t in upi_transactions; do
  kafka-topics.sh --bootstrap-server $KAFKA_BOOTSTRAP --create --if-not-exists --topic $t --partitions 3 --replication-factor 1
done

echo ">> HBase";  start-hbase.sh && sleep 8 && hbase-daemon.sh start thrift
sleep 5
python3 "$PYTHONPATH/scripts/create_tables.py"

"$(dirname "$0")/check_cluster.sh"
