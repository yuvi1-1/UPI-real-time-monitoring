#!/usr/bin/env bash
source "$(dirname "$0")/env.sh"
hbase-daemon.sh stop thrift; stop-hbase.sh
kafka-server-stop.sh
stop-workers.sh; stop-master.sh
stop-dfs.sh
