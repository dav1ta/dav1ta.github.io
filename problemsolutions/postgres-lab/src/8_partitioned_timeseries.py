"""
Declarative partitioned tables for time-series pruning and easier maintenance.

Usage:
  export DATABASE_URL=postgresql://user:pass@host:5432/db
  python postgres-lab/src/8_partitioned_timeseries.py
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
    with psycopg.connect(dsn) as conn, conn.cursor() as cur:
        cur.execute("drop table if exists ev cascade")
        cur.execute("create table ev(dt date, user_id int, amount numeric) partition by range (dt)")
        cur.execute("create table ev_2024_10 partition of ev for values from ('2024-10-01') to ('2024-11-01')")
        cur.execute("create table ev_2024_11 partition of ev for values from ('2024-11-01') to ('2024-12-01')")
        cur.execute("explain select * from ev where dt >= '2024-11-10' and dt < '2024-11-15'")
        print("\n".join(r[0] for r in cur.fetchall()))


if __name__ == "__main__":
    run()

