"""
Materialized view with concurrent refresh to avoid read downtime.

Usage:
  export DATABASE_URL=postgresql://user:pass@host:5432/db
  python postgres-lab/src/13_materialized_views_refresh.py
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
        cur.execute("drop table if exists sales")
        cur.execute("create table sales(id serial primary key, amt int)")
        cur.execute("insert into sales(amt) select 1 from generate_series(1,1000)")
        cur.execute("drop materialized view if exists sales_mv")
        cur.execute("create materialized view sales_mv as select 1 as k, count(*) cnt from sales")
        cur.execute("create unique index if not exists sales_mv_k_idx on sales_mv(k)")
        cur.execute("refresh materialized view concurrently sales_mv")
        cur.execute("select * from sales_mv")
        print(cur.fetchall())


if __name__ == "__main__":
    run()

