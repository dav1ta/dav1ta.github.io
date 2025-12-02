"""
Parallel query knobs and plan inspection.

Usage:
  export DATABASE_URL=postgresql://user:pass@host:5432/db
  python postgres-lab/src/14_parallel_query_knobs.py
"""

import os, sys

try:
    import psycopg
except Exception as e:  # pragma: no cover
    print("Install psycopg (v3) to run: pip install psycopg[binary]", file=sys.stderr)
    raise


def run(n=200000):
    dsn = os.getenv("DATABASE_URL")
    if not dsn:
        print("Set DATABASE_URL", file=sys.stderr)
        return 2
    with psycopg.connect(dsn) as conn, conn.cursor() as cur:
        cur.execute("set max_parallel_workers_per_gather=2")
        cur.execute("drop table if exists big")
        cur.execute("create table big(x int)")
        cur.execute("insert into big select generate_series(1,%s)", (n,))
        cur.execute("explain select sum(x) from big")
        print("\n".join(r[0] for r in cur.fetchall()))


if __name__ == "__main__":
    run()

