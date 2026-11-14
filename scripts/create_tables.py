"""Create the HBase tables for the UPI project (waits until the HBase Thrift server is up)."""
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from common import config  # noqa: E402
from common.store import HBaseStore  # noqa: E402

for attempt in range(40):
    try:
        HBaseStore("upi").create_tables()
        break
    except Exception as e:
        print(f"HBase not ready at {config.HBASE_HOST}:{config.HBASE_PORT} ({e.__class__.__name__}), retry {attempt + 1}/40")
        time.sleep(5)
else:
    sys.exit("HBase did not come up")
