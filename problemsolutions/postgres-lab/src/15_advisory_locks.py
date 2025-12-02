"""
Advisory locks: cross-process mutex keyed by integers.

Usage:
  export DATABASE_URL=postgresql://user:pass@host:5432/db
  python postgres-lab/src/15_advisory_locks.py
"""

import os, sys

try:
    import psycopg
except Exception as e:  # pragma: no cover
    print("Install psycopg (v3) to run: pip install psycopg[binary]", file=sys.stderr)
    raise


def run():
    dsn = os.getenv("DATABASE_URL")
    if not dsn:
        print("Set DATABASE_URL", file=sys.stderr)
        return 2
    a = psycopg.connect(dsn)
    b = psycopg.connect(dsn)
    with a, a.cursor() as ca, b, b.cursor() as cb:
        ca.execute("select pg_try_advisory_lock(42)"); print("A lock:", ca.fetchone()[0])
        cb.execute("select pg_try_advisory_lock(42)"); print("B lock:", cb.fetchone()[0])
        ca.execute("select pg_advisory_unlock(42)")


if __name__ == "__main__":
    run()

