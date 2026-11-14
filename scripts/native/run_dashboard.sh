#!/usr/bin/env bash
# ./scripts/native/run_dashboard.sh   -> http://<master-ip>:5000
source "$(dirname "$0")/env.sh"
cd "$PYTHONPATH" && PROJECT=upi python3 -m dashboard.app
