"""
Generated columns to index JSON keys cleanly.

Usage:
  export DATABASE_URL=postgresql://user:pass@host:5432/db
  python postgres-lab/src/17_generated_columns_json.py
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
        cur.execute("drop table if exists items")
        cur.execute("create table items(data jsonb, sku text generated always as ((data->>'sku')) stored)")
        cur.execute("create unique index if not exists items_sku_idx on items(sku)")
        cur.execute("insert into items(data) values (%s)", (psycopg.adapters.Json({"sku":"A1","name":"x"}),))
        cur.execute("select sku from items where sku='A1'")
        print(cur.fetchall())


if __name__ == "__main__":
    run()

