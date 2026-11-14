#!/usr/bin/env bash
# Shows that every component is alive. Good to run at the start of the demo.
source "$(dirname "$0")/env.sh"
echo "===== Java processes on master (jps) ====="; jps
echo; echo "===== HDFS: live datanodes ====="; hdfs dfsadmin -report | grep -E "Live datanodes|^Name:|DFS Used%"
echo; echo "===== Spark workers =====";  curl -s http://$MASTER:8080/json/ | python3 -c "import json,sys; d=json.load(sys.stdin); [print(w['host'], w['state'], w['cores'], 'cores') for w in d['workers']]"
echo; echo "===== Kafka topics ====="; kafka-topics.sh --bootstrap-server $KAFKA_BOOTSTRAP --list
echo; echo "===== HBase tables ====="; echo "list" | hbase shell -n 2>/dev/null | grep -E "_(metrics|alerts|reports)"
