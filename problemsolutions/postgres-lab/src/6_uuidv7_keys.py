"""
Time-ordered IDs with uuidv7() (18+). Falls back if unavailable.

Usage:
  export DATABASE_URL=postgresql://user:pass@host:5432/db
  python postgres-lab/src/6_uuidv7_keys.py
"""

import os, sys

try:
    import psycopg
except Exception as e:  # pragma: no cover
    print("Install psycopg (v3) to run: pip install psycopg[binary]", file=sys.stderr)
    raise


def run(n: int = 5):
    dsn = os.getenv("DATABASE_URL")
    if not dsn:
        print("Set DATABASE_URL", file=sys.stderr)
        return 2
    with psycopg.connect(dsn) as conn:
        with conn.cursor() as cur:
            try:
                cur.execute("select uuidv7() from generate_series(1,%s)", (n,))
                print({"uuidv7_available": True, "sample": [r[0] for r in cur.fetchall()]})
            except Exception as e:
                print({"uuidv7_available": False, "note": "requires PostgreSQL 18+", "error": str(e)[:120]})


if __name__ == "__main__":
    run()
