"""
Full‑text search with GIN on tsvector.

Usage:
  export DATABASE_URL=postgresql://user:pass@host:5432/db
  python postgres-lab/src/9_full_text_search.py
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
        cur.execute("drop table if exists docs")
        cur.execute("create table docs(id serial primary key, body text)")
        cur.execute("create index if not exists docs_fts on docs using gin(to_tsvector('english', body))")
        cur.executemany("insert into docs(body) values (%s)", [
            ("Postgres search is great",),
            ("Transactions and indexes",),
            ("Searching text with tsquery",),
        ])
        cur.execute("select id from docs where to_tsvector('english',body) @@ plainto_tsquery('english', %s)", ("search",))
        print(cur.fetchall())


if __name__ == "__main__":
    run()

