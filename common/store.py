"""Thin wrapper around HBase (via the Thrift server + happybase).

HBase layout (per project, e.g. project = "upi"):

  upi_metrics   column family m   row key "<dimension>|<value>"   e.g. "bank|SBI", "minute|202611131430"
                                  columns are 64-bit counters: m:count, m:amount, ...
  upi_alerts    column family a   row key "<reverse timestamp>|<id>"  (newest rows come first in a scan)
  upi_reports   column family r   row key "<report name>"  column r:json  (output of the batch job)
"""
import json
import struct
import time

from common import config


def tables(project):
    return {
        f"{project}_metrics": "m",
        f"{project}_alerts": "a",
        f"{project}_reports": "r",
    }


def reverse_ts(ts_ms=None):
    ts_ms = int(ts_ms if ts_ms is not None else time.time() * 1000)
    return f"{10**13 - ts_ms:013d}"


class HBaseStore:
    def __init__(self, project, host=None, port=None):
        import happybase  # imported lazily so tests can run without it

        self.project = project
        self.conn = happybase.Connection(host or config.HBASE_HOST, port or config.HBASE_PORT, timeout=20000)
        self.conn.open()

    # ---------- admin ----------
    def create_tables(self):
        existing = {t.decode() for t in self.conn.tables()}
        for name, cf in tables(self.project).items():
            if name not in existing:
                self.conn.create_table(name, {cf: dict(max_versions=1)})
                print(f"created HBase table {name}")
            else:
                print(f"HBase table {name} already exists")

    # ---------- writes ----------
    def incr_many(self, rows):
        """rows: list of (row_key, {column: int})"""
        t = self.conn.table(f"{self.project}_metrics")
        for row_key, cols in rows:
            for col, val in cols.items():
                if val:
                    t.counter_inc(row_key.encode(), f"m:{col}".encode(), int(val))

    def put_alerts(self, alerts):
        t = self.conn.table(f"{self.project}_alerts")
        with t.batch() as b:
            for a in alerts:
                key = f"{reverse_ts(a.get('ts_ms'))}|{a.get('id', '')}"
                b.put(key.encode(), {f"a:{k}".encode(): str(v).encode() for k, v in a.items()})

    def put_report(self, name, data):
        t = self.conn.table(f"{self.project}_reports")
        t.put(name.encode(), {b"r:json": json.dumps(data, default=str).encode()})

    # ---------- reads ----------
    def scan_metrics(self, prefix):
        t = self.conn.table(f"{self.project}_metrics")
        out = {}
        for key, data in t.scan(row_prefix=f"{prefix}|".encode()):
            out[key.decode().split("|", 1)[1]] = {
                k.decode().split(":", 1)[1]: struct.unpack(">q", v)[0] for k, v in data.items()
            }
        return out

    def latest_alerts(self, limit=25):
        t = self.conn.table(f"{self.project}_alerts")
        return [
            {k.decode().split(":", 1)[1]: v.decode() for k, v in data.items()}
            for _, data in t.scan(limit=limit)
        ]

    def reports(self):
        t = self.conn.table(f"{self.project}_reports")
        return {k.decode(): json.loads(d[b"r:json"]) for k, d in t.scan()}


class MemoryStore:
    """Same interface, kept in memory. Used only by tests / UI preview without a cluster."""

    def __init__(self, project):
        self.project = project
        self.metrics, self.alerts, self._reports = {}, [], {}

    def create_tables(self):
        pass

    def incr_many(self, rows):
        for row_key, cols in rows:
            d = self.metrics.setdefault(row_key, {})
            for c, v in cols.items():
                if v:
                    d[c] = d.get(c, 0) + int(v)

    def put_alerts(self, alerts):
        for a in alerts:
            self.alerts.append((f"{reverse_ts(a.get('ts_ms'))}|{a.get('id', '')}", {k: str(v) for k, v in a.items()}))
        self.alerts.sort(key=lambda x: x[0])

    def put_report(self, name, data):
        self._reports[name] = json.loads(json.dumps(data, default=str))

    def scan_metrics(self, prefix):
        p = f"{prefix}|"
        return {k[len(p):]: dict(v) for k, v in sorted(self.metrics.items()) if k.startswith(p)}

    def latest_alerts(self, limit=25):
        return [a for _, a in self.alerts[:limit]]

    def reports(self):
        return dict(self._reports)


_memory_singletons = {}


def get_store(project=None):
    project = project or config.PROJECT
    if config.STORE == "memory":
        return _memory_singletons.setdefault(project, MemoryStore(project))
    return HBaseStore(project)
