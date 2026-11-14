#!/usr/bin/env bash
# ./scripts/native/run_generator.sh 20      (events per second)
source "$(dirname "$0")/env.sh"
cd "$PYTHONPATH" && python3 -m upi.generator --rate ${1:-20}
