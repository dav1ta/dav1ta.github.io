"""
Observe I/O with pg_stat_io (16+). Falls back gracefully if unavailable.

Usage:
  export DATABASE_URL=postgresql://user:pass@host:5432/db
  python postgres-lab/src/5_io_telemetry.py
"""

import os, sys

try:
    import psycopg
except Exception as e:  # pragma: no cover
    print("Install psycopg (v3) to run: pip install psycopg[binary]", file=sys.stderr)
    raise


def run(limit: int = 10):
    dsn = os.getenv("DATABASE_URL")
    if not dsn:
        print("Set DATABASE_URL", file=sys.stderr)
        return 2
    with psycopg.connect(dsn) as conn:
        # feature probe: does the view exist?
        has_view = False
        with conn.cursor() as cur:
            cur.execute(
                """
                select 1
                from pg_catalog.pg_class c
                join pg_catalog.pg_namespace n on n.oid = c.relnamespace
                where c.relkind in ('v','m') and n.nspname = 'pg_catalog' and c.relname = 'pg_stat_io'
                """
            )
            has_view = cur.fetchone() is not None
        if not has_view:
            print({"pg_stat_io": False, "note": "requires PostgreSQL 16+"})
            return 0
        with conn.cursor() as cur:
            cur.execute("select backend_type, object, reads, writes from pg_stat_io order by reads desc limit %s", (limit,))
            rows = cur.fetchall()
            print({"pg_stat_io": True, "sample": rows})


if __name__ == "__main__":
    run()
