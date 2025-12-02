"""
Window functions for running totals and ranks.

Usage:
  export DATABASE_URL=postgresql://user:pass@host:5432/db
  python postgres-lab/src/16_window_rollups.py
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
        cur.execute("drop table if exists tx")
        cur.execute("create table tx(id serial, user_id int, amount int)")
        cur.executemany("insert into tx(user_id,amount) values (%s,%s)", [(1,10),(1,5),(2,7),(1,3),(2,6)])
        cur.execute("select id,user_id,amount, sum(amount) over (partition by user_id order by id) as running from tx")
        print(cur.fetchall())


if __name__ == "__main__":
    run()

