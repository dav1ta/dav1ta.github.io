"""
Single-file FastAPI job queue service demonstrating:
- [Backpressure] bounded asyncio queue + 429 when full
- [Concurrency] fixed worker pool with asyncio Tasks
- [Backoff+Jitter] exponential retry without blocking workers (delayed re-enqueue)
- [DLQ] dead-letter for max-attempt failures + endpoints to view/requeue
- [RateLimit] global and per-client token-bucket on /enqueue
- [Idempotency] optional idempotency key to dedupe enqueues
- [Observability] counters via /stats and Prometheus /metrics

Notes
- This is intentionally minimal and in-memory (MVP). No external deps besides FastAPI/Pydantic.
- Run with uvicorn: uvicorn job-queue-service.src.fastapi_app:app --reload
"""

import asyncio
import os
import sqlite3
import random
import time
from typing import Any, Dict, Optional, List

from fastapi import FastAPI, HTTPException, Request, Response
from pydantic import BaseModel


# ----------------------------
# Config (tweak for experiments)
# ----------------------------

Q_MAX = 1000                 # [Backpressure] queue capacity
WORKERS = 4                  # [Concurrency] number of worker tasks
MAX_ATTEMPTS = 3             # [DLQ] max retry attempts before DLQ
BACKOFF_BASE = 0.1           # [Backoff+Jitter] base seconds
BACKOFF_CAP = 1.0            # [Backoff+Jitter] max delay seconds
JITTER = 0.2                 # [Backoff+Jitter] added 0..JITTER seconds

RATE_CAPACITY = 50.0         # [RateLimit] max tokens in bucket
RATE_REFILL_PER_SEC = 25.0   # [RateLimit] tokens refilled per second

# Optional per-client rate limiting (by X-Client-Id or remote IP)
CLIENT_RATE_CAPACITY = 10.0
CLIENT_RATE_REFILL_PER_SEC = 5.0

IDEMP_TTL = 600.0            # [Idempotency] seconds to keep keys
IDEMP_BACKEND = os.getenv("IDEMP_BACKEND", "memory").lower()  # memory | sqlite
IDEMP_SQLITE_PATH = os.getenv("IDEMP_SQLITE_PATH", "idem.db")


# ----------------------------
# State (in-memory, guarded by async lock)
# ----------------------------

app = FastAPI()

queue_immediate: asyncio.Queue = asyncio.Queue(maxsize=Q_MAX)  # [Backpressure]

processed: int = 0       # [Observability]
failures: int = 0        # [Observability]
enqueued: int = 0        # [Observability]
in_progress: int = 0     # [Observability]
dlq: List[Dict[str, Any]] = []  # [DLQ]

# [Idempotency] key -> {status: "accepted"|"completed", result: Any|None, expires_at: float}
idempotency: Dict[str, Dict[str, Any]] = {}

# [RateLimit] token bucket
tokens: float = RATE_CAPACITY
last_refill: float = time.time()

# [RateLimit] per-client token buckets: client_id -> {tokens, last}
client_buckets: Dict[str, Dict[str, float]] = {}

lock = asyncio.Lock()  # guard counters, dlq, idempotency, ratelimiter
worker_tasks: List[asyncio.Task] = []
bg_tasks: List[asyncio.Task] = []

# [Idempotency] optional sqlite backend
_idem_db: Optional[sqlite3.Connection] = None


# ----------------------------
# Models
# ----------------------------

class EnqueueBody(BaseModel):
    # Free-form payload for experimentation
    payload: Optional[Dict[str, Any]] = None
    # [Failure Simulation] first N attempts will fail, then succeed
    fail_times: int = 0
    # Optional caller-supplied id
    id: Optional[str] = None


# ----------------------------
# Helpers
# ----------------------------

def now() -> float:
    return time.time()


async def rl_consume(n: float = 1.0) -> bool:
    """[RateLimit] consume n tokens if available; refill based on elapsed time."""
    global tokens, last_refill
    async with lock:
        t = now()
        elapsed = max(0.0, t - last_refill)
        last_refill = t
        tokens = min(RATE_CAPACITY, tokens + elapsed * RATE_REFILL_PER_SEC)
        if tokens >= n:
            tokens -= n
            return True
        return False


async def rl_consume_client(client_id: str, n: float = 1.0) -> bool:
    """[RateLimit] Per-client token bucket with simple refill."""
    async with lock:
        now_ts = now()
        b = client_buckets.get(client_id)
        if b is None:
            b = {"tokens": CLIENT_RATE_CAPACITY, "last": now_ts}
            client_buckets[client_id] = b
        elapsed = max(0.0, now_ts - b["last"])
        b["last"] = now_ts
        b["tokens"] = min(CLIENT_RATE_CAPACITY, b["tokens"] + elapsed * CLIENT_RATE_REFILL_PER_SEC)
        if b["tokens"] >= n:
            b["tokens"] -= n
            return True
        return False


async def delayed_requeue(item: Dict[str, Any], delay: float) -> None:
    """[Backoff+Jitter] non-blocking retry: wait, then re-enqueue or DLQ if still full."""
    await asyncio.sleep(delay)
    # Backpressure interaction: avoid indefinite backpressure causing retry storms.
    try:
        queue_immediate.put_nowait(item)
    except asyncio.QueueFull:
        try:
            await asyncio.wait_for(queue_immediate.put(item), timeout=1.0)
        except asyncio.TimeoutError:
            async with lock:
                dlq.append(item)


async def process_job(item: Dict[str, Any]) -> None:
    """Core job logic with retries, jitter, and DLQ."""
    global processed, failures, in_progress
    attempt = int(item.get("attempt", 0))
    fail_times = int(item.get("fail_times", 0))

    try:
        async with lock:
            in_progress += 1
        # [Failure Simulation] fail first `fail_times` attempts
        if attempt < fail_times:
            raise RuntimeError("simulated failure")

        # Place real work here (I/O, CPU, etc.)
        # await asyncio.sleep(0)  # no-op yield

        async with lock:
            processed += 1

        # [Idempotency] mark completed if key exists
        key = item.get("idempotency_key")
        if key:
            await idem_set_completed(key)

    except Exception:
        async with lock:
            failures += 1

        # Retry or DLQ
        next_attempt = attempt + 1
        if next_attempt < MAX_ATTEMPTS:
            item["attempt"] = next_attempt
            base = BACKOFF_BASE * (2 ** (next_attempt - 1))
            delay = min(base + random.uniform(0, JITTER), BACKOFF_CAP)
            await delayed_requeue(item, delay)
        else:
            async with lock:
                dlq.append(item)
    
    
async def worker_loop(idx: int) -> None:
    """[Concurrency] worker consumes from immediate queue and processes jobs."""
    global in_progress
    while True:
        item = await queue_immediate.get()
        try:
            await process_job(item)
        finally:
            queue_immediate.task_done()
            async with lock:
                in_progress = max(0, in_progress - 1)


# ----------------------------
# Idempotency backend (memory or sqlite)
# ----------------------------

def _idem_sqlite_init() -> None:
    global _idem_db
    _idem_db = sqlite3.connect(IDEMP_SQLITE_PATH, check_same_thread=False)
    _idem_db.execute(
        "CREATE TABLE IF NOT EXISTS idem (k TEXT PRIMARY KEY, status TEXT, expires REAL, result TEXT)"
    )
    _idem_db.commit()


async def idem_get_status(key: str) -> Optional[str]:
    if not key:
        return None
    if IDEMP_BACKEND == "sqlite" and _idem_db is not None:
        t = now()
        row = _idem_db.execute(
            "SELECT status, expires FROM idem WHERE k=?", (key,)
        ).fetchone()
        if not row:
            return None
        status, expires = row
        return status if expires and float(expires) > t else None
    # memory
    async with lock:
        rec = idempotency.get(key)
        if not rec:
            return None
        return rec["status"] if rec.get("expires_at", 0) > now() else None


async def idem_set_accepted(key: str) -> None:
    if not key:
        return
    expires = now() + IDEMP_TTL
    if IDEMP_BACKEND == "sqlite" and _idem_db is not None:
        _idem_db.execute(
            "INSERT OR REPLACE INTO idem(k,status,expires,result) VALUES(?,?,?,NULL)",
            (key, "accepted", expires),
        )
        _idem_db.commit()
        return
    async with lock:
        idempotency[key] = {"status": "accepted", "result": None, "expires_at": expires}


async def idem_set_completed(key: str) -> None:
    if not key:
        return
    expires = now() + IDEMP_TTL
    if IDEMP_BACKEND == "sqlite" and _idem_db is not None:
        _idem_db.execute(
            "UPDATE idem SET status=?, expires=? WHERE k=?",
            ("completed", expires, key),
        )
        _idem_db.commit()
        return
    async with lock:
        rec = idempotency.get(key)
        if rec:
            rec["status"] = "completed"
            rec["expires_at"] = expires


# ----------------------------
# FastAPI endpoints
# ----------------------------

@app.on_event("startup")
async def startup() -> None:
    # [Concurrency] spawn a fixed pool of workers
    for i in range(WORKERS):
        worker_tasks.append(asyncio.create_task(worker_loop(i)))
    # Idem backend init
    if IDEMP_BACKEND == "sqlite":
        _idem_sqlite_init()
    # TTL sweeper for idempotency
    async def _idem_sweeper():
        while True:
            await asyncio.sleep(30)
            t = now()
            if IDEMP_BACKEND == "sqlite" and _idem_db is not None:
                _idem_db.execute("DELETE FROM idem WHERE expires<=?", (t,))
                _idem_db.commit()
            else:
                async with lock:
                    for k in list(idempotency.keys()):
                        if idempotency[k].get("expires_at", 0) <= t:
                            idempotency.pop(k, None)
    bg_tasks.append(asyncio.create_task(_idem_sweeper()))


@app.on_event("shutdown")
async def shutdown() -> None:
    # Cancel workers gracefully
    for t in worker_tasks + bg_tasks:
        t.cancel()
    await asyncio.gather(*worker_tasks, *bg_tasks, return_exceptions=True)


@app.get("/health")
async def health() -> Dict[str, Any]:
    return {"status": "ok"}


@app.get("/stats")
async def stats() -> Dict[str, Any]:
    # [Observability]
    async with lock:
        return {
            "queue_depth": queue_immediate.qsize(),
            "queue_max": Q_MAX,
            "processed": processed,
            "failures": failures,
            "enqueued": enqueued,
            "in_progress": in_progress,
            "workers": WORKERS,
            "dlq": len(dlq),
            "rate_tokens": tokens,
            "client_buckets": len(client_buckets),
        }


@app.get("/metrics")
async def metrics() -> Response:
    """Prometheus exposition format with a few counters/gauges."""
    async with lock:
        lines = []
        def add(name: str, mtype: str, help_text: str, value: Any):
            lines.append(f"# HELP {name} {help_text}")
            lines.append(f"# TYPE {name} {mtype}")
            lines.append(f"{name} {value}")
        add("job_enqueued_total", "counter", "Total enqueued jobs", enqueued)
        add("job_processed_total", "counter", "Total processed jobs", processed)
        add("job_failures_total", "counter", "Total failed processing attempts", failures)
        add("job_queue_depth", "gauge", "Current queue depth", queue_immediate.qsize())
        add("job_dlq_size", "gauge", "Current DLQ size", len(dlq))
        add("job_in_progress", "gauge", "Jobs currently being processed", in_progress)
        add("rate_tokens", "gauge", "Global rate limiter tokens", tokens)
        add("rate_client_buckets", "gauge", "Number of client buckets", len(client_buckets))
        body = "\n".join(lines) + "\n"
    return Response(content=body, media_type="text/plain; version=0.0.4; charset=utf-8")


@app.get("/dlq")
async def dlq_list(limit: int = 50) -> Dict[str, Any]:
    async with lock:
        return {"count": len(dlq), "items": dlq[-limit:]}


@app.post("/dlq/requeue")
async def dlq_requeue(limit: int = 10) -> Dict[str, Any]:
    moved = 0
    async with lock:
        while dlq and moved < limit:
            item = dlq.pop(0)
            moved += 1
            item["attempt"] = int(item.get("attempt", 0))  # keep attempt as is
            try:
                queue_immediate.put_nowait(item)
            except asyncio.QueueFull:
                dlq.insert(0, item)
                break
    return {"requeued": moved}


@app.post("/enqueue")
async def enqueue(body: EnqueueBody, request: Request) -> Dict[str, Any]:
    # [RateLimit] global bucket
    if not await rl_consume(1.0):
        raise HTTPException(status_code=429, detail="rate_limited")
    # [RateLimit] per-client bucket
    client_id = request.headers.get("X-Client-Id") or (request.client.host if request.client else "unknown")
    if not await rl_consume_client(client_id, 1.0):
        raise HTTPException(status_code=429, detail="rate_limited_client")

    # [Idempotency] via headers: X-Idempotency-Key or Idempotency-Key
    idem_key = request.headers.get("X-Idempotency-Key") or request.headers.get("Idempotency-Key")
    if idem_key:
        status = await idem_get_status(idem_key)
        if status:
            return {"status": status, "idempotent": True}

    # [Backpressure] reject when queue is full
    if queue_immediate.full():
        raise HTTPException(status_code=429, detail="queue_full")

    item: Dict[str, Any] = {
        "payload": body.payload or {},
        "fail_times": body.fail_times,
        "attempt": 0,
    }
    if idem_key:
        item["idempotency_key"] = idem_key

    try:
        queue_immediate.put_nowait(item)
    except asyncio.QueueFull:
        raise HTTPException(status_code=429, detail="queue_full")

    if idem_key:
        await idem_set_accepted(idem_key)
    async with lock:
        global enqueued
        enqueued += 1

    return {"status": "enqueued", "idempotent": bool(idem_key)}
