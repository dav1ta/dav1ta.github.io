"""
Minimal job queue service (stdlib-only) demonstrating:
- [Backpressure] bounded queue + 429 on full
- [Concurrency] fixed worker pool
- [Backoff+Jitter] exponential retry with jitter
- [DLQ] dead-letter on max attempts
- [Observability] basic counters via /stats

Edit and extend for experiments (idempotency, rate limiting, etc.).
"""

import json
import queue
import threading
import time
import random
from http.server import BaseHTTPRequestHandler, HTTPServer
from urllib.parse import urlparse

Q_MAX = 1000  # [Backpressure] bounded queue capacity; 429 when full
WORKERS = 4   # [Concurrency] number of worker threads

# [Backoff+Jitter] retry tuning (MVP)
MAX_ATTEMPTS = 3          # [DLQ] after this many failures, drop to DLQ
BACKOFF_BASE = 0.1        # base backoff seconds (2^(attempt-1) multiplier)
BACKOFF_CAP = 1.0         # upper bound on backoff delay
JITTER = 0.2              # random 0..JITTER seconds added to spread retries

job_q = queue.Queue(maxsize=Q_MAX)  # [Backpressure]
processed = 0      # [Observability] success counter
failures = 0       # [Observability] failure counter
dlq = []           # [DLQ] in-memory dead-letter collection (MVP)
lock = threading.Lock()  # guard counters and DLQ list


def worker():
    """Background worker: pulls jobs, retries with backoff+jitter, DLQ on max attempts."""
    global processed, failures
    while True:
        item = job_q.get()  # blocks until job available
        if item is None:
            job_q.task_done()  # sentinel for graceful shutdown
            break
        try:
            # Minimal simulated work (replace with real logic)
            # [Failure Simulation] If client sends {"fail_times": N}, first N attempts fail.
            attempt = int(item.get("attempt", 0)) if isinstance(item, dict) else 0
            fail_times = int(item.get("fail_times", 0)) if isinstance(item, dict) else 0
            if attempt < fail_times:
                raise RuntimeError("simulated failure")

            with lock:
                processed += 1
        except Exception:
            # [Backoff+Jitter] on failure, retry with exponential backoff + jitter
            with lock:
                failures += 1

            if not isinstance(item, dict):
                item = {"payload": item}
            item_attempt = int(item.get("attempt", 0)) + 1

            if item_attempt < MAX_ATTEMPTS:
                item["attempt"] = item_attempt
                # [Backoff+Jitter] exponential backoff + jitter (core logic)
                base = BACKOFF_BASE * (2 ** (item_attempt - 1))
                delay = min(base + random.uniform(0, JITTER), BACKOFF_CAP)
                time.sleep(delay)  # MVP: blocks worker; later: delayed/visibility queue
                try:
                    job_q.put_nowait(item)  # re-enqueue for retry
                except queue.Full:
                    # [Backpressure] queue still full; block briefly then DLQ to avoid storms
                    try:
                        job_q.put(item, timeout=1)
                    except queue.Full:
                        # still full: push to DLQ (shed load)
                        with lock:
                            dlq.append(item)
            else:
                # [DLQ] exceeded attempts -> send to dead letter
                with lock:
                    dlq.append(item)
        finally:
            job_q.task_done()


class Handler(BaseHTTPRequestHandler):
    """HTTP endpoints: /enqueue (POST), /stats (GET)."""
    def _send(self, code=200, body=None):
        b = b""
        if body is not None:
            b = json.dumps(body).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(b)))
        self.end_headers()
        if b:
            self.wfile.write(b)

    def do_POST(self):
        parsed = urlparse(self.path)
        if parsed.path == "/enqueue":
            length = int(self.headers.get("Content-Length", 0))
            payload = self.rfile.read(length) if length else b"{}"
            try:
                data = json.loads(payload or b"{}")
            except Exception:
                return self._send(400, {"error": "invalid json"})

            if job_q.full():
                # [Backpressure] refuse when queue at capacity
                return self._send(429, {"error": "queue_full"})
            try:
                job_q.put_nowait(data)
            except queue.Full:
                # [Backpressure] race: also respond 429 here
                return self._send(429, {"error": "queue_full"})
            return self._send(202, {"status": "enqueued"})
        return self._send(404, {"error": "not_found"})

    def do_GET(self):
        parsed = urlparse(self.path)
        if parsed.path == "/stats":
            body = {
                "queue_depth": job_q.qsize(),  # [Observability]
                "queue_max": Q_MAX,            # [Observability]
                "processed": processed,        # [Observability]
                "failures": failures,          # [Observability]
                "workers": WORKERS,            # [Observability]
                "dlq": len(dlq),               # [DLQ]
            }
            return self._send(200, body)
        return self._send(404, {"error": "not_found"})


def main(host="127.0.0.1", port=8080):
    # [Concurrency] spawn a fixed pool of worker threads
    threads = [threading.Thread(target=worker, daemon=True) for _ in range(WORKERS)]
    for t in threads:
        t.start()

    server = HTTPServer((host, port), Handler)
    try:
        server.serve_forever()  # serve /enqueue and /stats
    except KeyboardInterrupt:
        pass
    finally:
        server.shutdown()
        for _ in threads:
            job_q.put(None)
        for t in threads:
            t.join()


if __name__ == "__main__":
    main()
