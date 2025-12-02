"""
Push JSON filtering to SQL using JSONPath (12+).

Usage:
  export DATABASE_URL=postgresql://user:pass@host:5432/db
  python postgres-lab/src/4_jsonpath_filter.py
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
    doc = {"users": [{"id": 1, "role": "admin"}, {"id": 2, "role": "user"}, {"id": 3, "role": "user"}]}
    expr = "$.users[*] ? (@.role == 'user').id"
    with psycopg.connect(dsn) as conn:
        with conn.cursor() as cur:
            cur.execute("select jsonb_path_query(%s::jsonb, %s)", (psycopg.adapters.Json(doc), expr))
            rows = [r[0] for r in cur.fetchall()]
            print({"jsonpath": expr, "results": rows})


if __name__ == "__main__":
    run()
