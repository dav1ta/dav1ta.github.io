"""
Partial index targeting hot subset (e.g., active rows only).

Usage:
  export DATABASE_URL=postgresql://user:pass@host:5432/db
  python postgres-lab/src/12_partial_index_hotset.py
"""

import os, sys, random

try:
    import psycopg
except Exception as e:  # pragma: no cover
    print("Install psycopg (v3) to run: pip install psycopg[binary]", file=sys.stderr)
    raise


def run(n=2000):
    dsn = os.getenv("DATABASE_URL")
    if not dsn:
        print("Set DATABASE_URL", file=sys.stderr)
        return 2
    with psycopg.connect(dsn) as conn, conn.cursor() as cur:
        cur.execute("drop table if exists e")
        cur.execute("create table e(id serial, active boolean, ts timestamptz default now(), v int)")
        data = [ (i % 5 == 0, i) for i in range(n) ]
        cur.executemany("insert into e(active,v) values (%s,%s)", data)
        cur.execute("create index if not exists e_ts_active_idx on e(ts) where active")
        cur.execute("explain select * from e where active and ts > now() - interval '1 day'")
        print("\n".join(r[0] for r in cur.fetchall()))


if __name__ == "__main__":
    random.seed(42)
    run()

