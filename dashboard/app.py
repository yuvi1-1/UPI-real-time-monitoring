"""Live dashboard (Flask). Reads the aggregates the Spark job writes into HBase and serves them
as JSON to the browser, which refreshes the charts every few seconds.

    python -m dashboard.app      -> http://localhost:5000
"""
import os
import sys
import time
from datetime import datetime, timedelta

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from flask import Flask, jsonify, render_template  # noqa: E402

from common import config  # noqa: E402
from common.store import get_store  # noqa: E402

app = Flask(__name__)
app.json.sort_keys = False  # keep report columns in the order Spark produced them


# ---------------- formatting helpers ----------------
def inr(paise):
    r = (paise or 0) / 100
    if r >= 1e7:
        return f"₹{r / 1e7:.2f} Cr"
    if r >= 1e5:
        return f"₹{r / 1e5:.2f} L"
    return f"₹{r:,.0f}"


def ratio(a, b, scale=1.0):
    return round(scale * a / b, 2) if b else 0


def num(n):
    return f"{n:,}"


# ---------------- per-project dashboard definitions ----------------
PROJECTS = {
    "upi": {
        "title": "Real-Time UPI Transaction Monitoring",
        "subtitle": "Kafka → Spark Streaming → HDFS + HBase",
        "kpis": lambda t: [
            ("Transactions", num(t.get("count", 0)), "processed"),
            ("Total value", inr(t.get("amount", 0)), "moved"),
            ("Success rate", f"{ratio(t.get('success', 0), t.get('count', 0), 100)}%", f"{num(t.get('failed', 0))} failed"),
            ("Avg ticket", inr(ratio(t.get("amount", 0), t.get("count", 0))), "per payment"),
            ("Alerts", num(t.get("alerts", 0)), "suspicious"),
        ],
        "charts": [
            dict(id="bank", title="Value by payer bank", dim="bank", type="bar", value="amount_rs", top=11),
            dict(id="app", title="Transactions by app", dim="app", type="doughnut", value="count"),
            dict(id="city", title="Transactions by city", dim="city", type="bar", value="count", top=10),
            dict(id="category", title="Merchant spend by category", dim="category", type="doughnut",
                 value="amount_rs", exclude=["Transfer"]),
            dict(id="failrate", title="Failure rate by bank (%)", dim="bank", type="bar", value="pct:failed/count", top=11),
        ],
        "timeline_value_label": "Value (₹)",
        "alert_columns": ["event_time", "alert_type", "payer", "city", "amount", "detail"],
    },
}


def chart_value(cols, how):
    if how == "count":
        return cols.get("count", 0)
    if how == "amount_rs":
        return round(cols.get("amount", 0) / 100, 2)
    kind, expr = how.split(":")
    a, b = expr.split("/")
    return ratio(cols.get(a, 0), cols.get(b, 0), 100 if kind == "pct" else 1)


def build_chart(store, spec):
    data = store.scan_metrics(spec["dim"])
    items = [(k, chart_value(v, spec["value"])) for k, v in data.items() if k not in spec.get("exclude", [])]
    items.sort(key=lambda kv: kv[1], reverse=True)
    if spec.get("top"):
        items = items[: spec["top"]]
    return {**{k: spec[k] for k in ("id", "title", "type")},
            "labels": [k for k, _ in items], "values": [v for _, v in items]}


def build_timeline(store, minutes=30):
    data = store.scan_metrics("minute")
    now = datetime.now().replace(second=0, microsecond=0)
    keys = [(now - timedelta(minutes=i)).strftime("%Y%m%d%H%M") for i in range(minutes - 1, -1, -1)]
    # if the clock of the data differs from this machine (e.g. demo data), fall back to the latest minutes we have
    if data and not any(k in data for k in keys):
        keys = sorted(data)[-minutes:]
    return {
        "labels": [f"{k[8:10]}:{k[10:12]}" for k in keys],
        "counts": [data.get(k, {}).get("count", 0) for k in keys],
        "values": [round(data.get(k, {}).get("amount", 0) / 100, 2) for k in keys],
    }


@app.route("/")
def index():
    p = PROJECTS[config.PROJECT]
    return render_template("index.html", title=p["title"], subtitle=p["subtitle"], project=config.PROJECT)


@app.route("/api/summary")
def summary():
    p = PROJECTS[config.PROJECT]
    t0 = time.time()
    try:
        store = get_store(config.PROJECT)
        total = store.scan_metrics("total").get("all", {})
        payload = {
            "ok": True,
            "kpis": [dict(label=a, value=b, sub=c) for a, b, c in p["kpis"](total)],
            "charts": [build_chart(store, c) for c in p["charts"]],
            "timeline": build_timeline(store),
            "timeline_value_label": p["timeline_value_label"],
            "alert_types": {k: v.get("count", 0) for k, v in store.scan_metrics("alert_type").items()},
            "alerts": store.latest_alerts(25),
            "alert_columns": p["alert_columns"],
            "reports": store.reports(),
        }
    except Exception as e:  # show the problem in the UI instead of a blank page
        payload = {"ok": False, "error": f"{e.__class__.__name__}: {e}"}
    payload["query_ms"] = int((time.time() - t0) * 1000)
    payload["server_time"] = datetime.now().strftime("%H:%M:%S")
    return jsonify(payload)


@app.route("/api/health")
def health():
    return {"project": config.PROJECT, "store": config.STORE, "hbase": f"{config.HBASE_HOST}:{config.HBASE_PORT}"}


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", "5000")), debug=False)
