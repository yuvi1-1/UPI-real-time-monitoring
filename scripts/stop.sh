#!/usr/bin/env bash
# Stop everything. Add  -v  to also delete HDFS data:  ./scripts/stop.sh -v
cd "$(dirname "$0")/.." && docker compose down "$@"
