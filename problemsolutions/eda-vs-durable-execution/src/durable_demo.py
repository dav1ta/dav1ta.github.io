"""
Durable Execution demo (pseudo-framework, stdlib-only)

Illustrates a workflow that runs sequential business logic while the framework
handles retries, records durable step completion, and supports resume after a crash.

Key ideas:
- Keep workflow code deterministic; do side-effects in Activities.
- Framework executes Activities with retries and records results per step.
- On resume, already-completed steps are skipped based on recorded history.
"""

from __future__ import annotations

import random
import time
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, Optional


# ----------------------------------------------------------------------------
# Activities (side effects) — can fail and will be retried by the engine
# ----------------------------------------------------------------------------

def validate_payment_activity(payment: dict) -> str:
    if payment.get("amount", 0) <= 0:
        raise ValueError("invalid amount")
    print(f"Activity: validated {payment['id']}")
    return "ok"


def record_payment_to_db_activity(payment: dict) -> str:
    # 10% chance transient DB failure
    if random.random() < 0.10:
        raise ConnectionError("temporary db failure")
    # pretend to write; idempotency assumed at the DB layer
    time.sleep(0.05)
    print(f"Activity: recorded {payment['id']} to DB")
    return "ok"


def clear_payment_activity(batch: list[dict]) -> str:
    # 5% chance external API flake
    if random.random() < 0.05:
        raise RuntimeError("clearing service unstable")
    time.sleep(0.05)
    print(f"Activity: cleared {len(batch)} payments")
    return "ok"


# ----------------------------------------------------------------------------
# Minimal durable engine — records step results; retries activities; resumes
# ----------------------------------------------------------------------------

StepFn = Callable[..., Any]


@dataclass
class DurableHistory:
    steps: Dict[str, Any] = field(default_factory=dict)

    def has(self, key: str) -> bool:
        return key in self.steps

    def get(self, key: str) -> Any:
        return self.steps.get(key)

    def set(self, key: str, value: Any) -> None:
        self.steps[key] = value


class DurableEngine:
    def __init__(self, history: Optional[DurableHistory] = None):
        self.history = history or DurableHistory()

    def execute_activity(self, step_key: str, fn: StepFn, *args, max_retries: int = 5, backoff: float = 0.1) -> Any:
        if self.history.has(step_key):
            print(f"[engine] skip {step_key} (already recorded)")
            return self.history.get(step_key)
        attempt = 0
        while True:
            try:
                result = fn(*args)
                self.history.set(step_key, result)
                return result
            except Exception as e:
                attempt += 1
                if attempt >= max_retries:
                    print(f"[engine] {step_key} failed after {attempt}: {e}")
                    raise
                delay = min(backoff * (2 ** (attempt - 1)) + random.random() * 0.05, 1.0)
                print(f"[engine] retry {step_key} in {delay:.2f}s: {e}")
                time.sleep(delay)

    def durable_sleep(self, step_key: str, seconds: float) -> None:
        # In a real engine, this would set a durable timer and return immediately;
        # here we record the intention and sleep briefly to keep the demo fast.
        if self.history.has(step_key):
            print(f"[engine] skip {step_key} (timer already satisfied)")
            return
        print(f"[engine] set durable timer {seconds}s")
        # compress time for demo
        time.sleep(min(seconds, 0.2))
        self.history.set(step_key, "done")


# ----------------------------------------------------------------------------
# Workflow (deterministic) — business logic
# ----------------------------------------------------------------------------

def payment_workflow(engine: DurableEngine, payment: dict) -> str:
    print(f"Workflow: start {payment['id']}")
    engine.execute_activity("validate", validate_payment_activity, payment)
    engine.execute_activity("record", record_payment_to_db_activity, payment)
    engine.durable_sleep("wait_1d", seconds=1.0)  # compressed
    engine.execute_activity("clear", clear_payment_activity, [payment])
    print(f"Workflow: complete {payment['id']}")
    return "Done"


# ----------------------------------------------------------------------------
# Demo — run workflow, simulate crash, then resume from durable state
# ----------------------------------------------------------------------------

def run_demo() -> None:
    print("--- Durable demo start ---")
    history = DurableHistory()
    engine = DurableEngine(history)
    payment = {"id": "tx-123", "amount": 100.0, "user": "Alice"}

    try:
        # Execute up to record step, then simulate crash
        engine.execute_activity("validate", validate_payment_activity, payment)
        engine.execute_activity("record", record_payment_to_db_activity, payment)
        print("[demo] CRASH happens now (before timer)")
        raise SystemExit("simulated crash")
    except SystemExit:
        pass

    # Resume: the workflow re-runs from the top, but engine skips done steps
    print("[demo] RESUME workflow")
    engine2 = DurableEngine(history)
    result = payment_workflow(engine2, payment)
    print("Result:", result)
    print("--- Durable demo end ---")


if __name__ == "__main__":
    random.seed(7)
    run_demo()

