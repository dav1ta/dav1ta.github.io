"""
EDA demo (stdlib-only)

Simulates two services:
- PaymentIngestionService (producer): validates and publishes to a queue with manual retry/backoff and DLQ.
- PaymentProcessingService (consumer): processes events; must be idempotent to tolerate duplicate delivery.

This is a small, single-file simulation to illustrate failure modes and the
developer work needed around retries/idempotency.
"""

from __future__ import annotations

import json
import queue
import random
import threading
import time
from dataclasses import dataclass
from typing import Dict, List, Optional


# ----------------------------------------------------------------------------
# Message Queue (simulated)
# ----------------------------------------------------------------------------

class InMemoryQueue:
    def __init__(self, maxsize: int = 1024):
        self.q = queue.Queue(maxsize=maxsize)

    def publish(self, topic: str, message: dict) -> bool:
        # 10% chance to simulate publish failure (network/queue hiccup)
        if random.random() < 0.10:
            print(f"!!! ERROR: failed to publish to {topic}")
            return False
        try:
            self.q.put_nowait((topic, message))
            print(f"-> Published to {topic}: {message['id']}")
            return True
        except queue.Full:
            print("!!! ERROR: queue is full")
            return False

    def consume(self, timeout: float = 0.1) -> Optional[tuple[str, dict]]:
        try:
            return self.q.get(timeout=timeout)
        except queue.Empty:
            return None


# ----------------------------------------------------------------------------
# Data layer (simulated) — use idempotency to avoid double apply
# ----------------------------------------------------------------------------

class AccumulationDB:
    def __init__(self):
        self._processed_ids: set[str] = set()

    def record_payment(self, payment: dict) -> None:
        # 5% chance to simulate DB hiccup
        if random.random() < 0.05:
            raise ConnectionError("database timeout")
        pid = payment["id"]
        if pid in self._processed_ids:
            print(f"(idempotent) already recorded {pid}")
            return
        # simulate write latency
        time.sleep(0.05)
        self._processed_ids.add(pid)
        print(f"DB: recorded {pid}")


# ----------------------------------------------------------------------------
# Producer — Payment ingestion with manual retry/backoff and DLQ
# ----------------------------------------------------------------------------

def ingest_payment(mq: InMemoryQueue, payment: dict, topic: str = "payments") -> None:
    if payment.get("amount", 0) <= 0:
        print(f"--- FAILURE: validation failed for {payment['id']}")
        return

    max_retries = 3
    backoff_base = 0.1
    for attempt in range(max_retries):
        if mq.publish(topic, payment):
            print(f"*** SUCCESS: ingested {payment['id']}")
            return
        sleep = min(backoff_base * (2 ** attempt) + random.random() * 0.1, 1.0)
        print(f"retrying publish {payment['id']} in {sleep:.2f}s")
        time.sleep(sleep)
    print(f"!!! CRITICAL: {payment['id']} failed after {max_retries} retries → DLQ/manual")


# ----------------------------------------------------------------------------
# Consumer — Payment processing; must be idempotent
# ----------------------------------------------------------------------------

def processing_loop(mq: InMemoryQueue, db: AccumulationDB, *, crash_after_db_write: bool = False, run_seconds: float = 3.0) -> None:
    start = time.time()
    while time.time() - start < run_seconds:
        msg = mq.consume(timeout=0.1)
        if not msg:
            continue
        topic, event = msg
        pid = event["id"]
        print(f"consume: {topic}:{pid}")
        try:
            # write to DB — must be idempotent
            db.record_payment(event)
            # simulate crash window after write but before ack
            if crash_after_db_write and random.random() < 0.3:
                raise RuntimeError("crash after db write, before ack")
            print(f"processed {pid}")
        except Exception as e:
            print(f"!!! ERROR: consumer failed for {pid}: {e}")
            # requeue to simulate at-least-once
            mq.publish(topic, event)


# ----------------------------------------------------------------------------
# Demo
# ----------------------------------------------------------------------------

def run_demo() -> None:
    print("--- EDA demo start ---")
    mq = InMemoryQueue()
    db = AccumulationDB()

    payments = [
        {"id": "tx-1", "amount": 100.0, "user": "Alice"},
        {"id": "tx-2", "amount": 0.0, "user": "Bob"},       # invalid
        {"id": "tx-3", "amount": 50.0, "user": "Carol"},
    ]

    for p in payments:
        ingest_payment(mq, p)

    # inject a duplicate delivery for tx-1 to show idempotency effect
    mq.publish("payments", payments[0])

    processing_loop(mq, db, crash_after_db_write=True, run_seconds=3.0)
    print("--- EDA demo end ---")


if __name__ == "__main__":
    random.seed(42)
    run_demo()

