"""Simulates a live stream of UPI payments and publishes them to Kafka.

    python -m upi.generator --rate 20            # 20 transactions / second
    python -m upi.generator --rate 20 --dry-run  # print instead of sending

About 1-2 % of the traffic is deliberately suspicious so the anomaly rules have something to catch:
huge single payments, one payer firing many payments in a few seconds, and repeated failed attempts.
"""
import argparse
import json
import random
import sys
import time
import uuid
from datetime import datetime

BANKS = {"SBI": 26, "HDFC": 16, "ICICI": 13, "Axis": 9, "Kotak": 7, "PNB": 7, "Bank of Baroda": 6,
         "Canara": 5, "Union Bank": 4, "Yes Bank": 3, "IDFC First": 4}
HANDLES = {"SBI": "oksbi", "HDFC": "okhdfcbank", "ICICI": "okicici", "Axis": "okaxis", "Kotak": "kotak",
           "PNB": "pnb", "Bank of Baroda": "barodampay", "Canara": "cnrb", "Union Bank": "unionbank",
           "Yes Bank": "yesbank", "IDFC First": "idfcbank"}
CITIES = {"Delhi": 18, "Mumbai": 17, "Bengaluru": 15, "Hyderabad": 10, "Chennai": 9, "Pune": 8,
          "Kolkata": 8, "Ahmedabad": 6, "Jaipur": 5, "Lucknow": 4}
APPS = {"PhonePe": 47, "Google Pay": 36, "Paytm": 8, "BHIM": 2, "Bank App": 4, "Others": 3}
CATEGORIES = {"Grocery": 24, "Food & Dining": 20, "Shopping": 14, "Fuel": 9, "Bills & Recharge": 12,
              "Travel": 7, "Entertainment": 5, "Healthcare": 5, "Education": 4}
NAMES = ["aarav", "vivaan", "aditya", "rohan", "arjun", "kabir", "ishaan", "priya", "ananya", "diya",
         "sneha", "kavya", "riya", "neha", "rahul", "amit", "pooja", "vikram", "sanya", "yuvraj"]


def pick(weights):
    return random.choices(list(weights), weights=list(weights.values()))[0]


def new_payer():
    bank = pick(BANKS)
    return f"{random.choice(NAMES)}{random.randint(10, 9999)}@{HANDLES[bank]}", bank, pick(CITIES)


def normal_amount(txn_type):
    # log-normal: most UPI payments are small, a long tail is large
    median = 180 if txn_type == "P2M" else 900
    return round(min(max(random.lognormvariate(0, 1.1) * median, 1), 95_000), 2)


def make_txn(payer=None, amount=None, status=None):
    vpa, bank, city = payer or new_payer()
    txn_type = "P2M" if random.random() < 0.62 else "P2P"
    payee_bank = pick(BANKS)
    if txn_type == "P2M":
        category = pick(CATEGORIES)
        payee = f"merchant{random.randint(1000, 99999)}@{HANDLES[payee_bank]}"
    else:
        category = "Transfer"
        payee = f"{random.choice(NAMES)}{random.randint(10, 9999)}@{HANDLES[payee_bank]}"
    return {
        "txn_id": "UPI" + uuid.uuid4().hex[:12].upper(),
        "event_time": datetime.now().strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3],
        "payer_vpa": vpa,
        "payee_vpa": payee,
        "payer_bank": bank,
        "payee_bank": payee_bank,
        "city": city,
        "app": pick(APPS),
        "txn_type": txn_type,
        "category": category,
        "amount": amount if amount is not None else normal_amount(txn_type),
        "status": status or ("FAILED" if random.random() < 0.03 else "SUCCESS"),
    }


def next_events():
    """Usually one normal transaction; sometimes an injected suspicious pattern."""
    r = random.random()
    if r < 0.006:   # huge single payment
        return [make_txn(amount=round(random.uniform(1_00_000, 5_00_000), 2))]
    if r < 0.010:   # one payer firing many payments quickly
        payer = new_payer()
        return [make_txn(payer=payer, amount=round(random.uniform(2_000, 9_999), 2)) for _ in range(random.randint(5, 9))]
    if r < 0.013:   # repeated failed attempts (wrong PIN / insufficient balance)
        payer = new_payer()
        return [make_txn(payer=payer, status="FAILED") for _ in range(random.randint(3, 6))]
    return [make_txn()]


def connect_producer(bootstrap):
    from kafka import KafkaProducer

    for attempt in range(30):
        try:
            return KafkaProducer(bootstrap_servers=bootstrap, value_serializer=lambda v: json.dumps(v).encode(),
                                 key_serializer=lambda k: k.encode(), linger_ms=50)
        except Exception as e:  # Kafka may still be starting
            print(f"waiting for Kafka at {bootstrap} ({e.__class__.__name__}), retry {attempt + 1}/30", flush=True)
            time.sleep(5)
    sys.exit("could not connect to Kafka")


def main():
    from common import config

    ap = argparse.ArgumentParser()
    ap.add_argument("--rate", type=float, default=20, help="transactions per second")
    ap.add_argument("--bootstrap", default=config.KAFKA_BOOTSTRAP)
    ap.add_argument("--topic", default=config.topic("upi"))
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    producer = None if args.dry_run else connect_producer(args.bootstrap)
    print(f"sending ~{args.rate}/s UPI transactions to '{args.topic}'", flush=True)
    sent, next_log = 0, 500
    while True:
        for e in next_events():
            if producer:
                producer.send(args.topic, key=e["payer_vpa"], value=e)
            else:
                print(json.dumps(e))
            sent += 1
        if sent >= next_log:
            next_log += 500
            print(f"sent {sent} transactions", flush=True)
        time.sleep(random.expovariate(args.rate))


if __name__ == "__main__":
    main()
