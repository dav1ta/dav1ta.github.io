"""
Bulk ingest using COPY FROM STDIN (text format).

Usage:
  export DATABASE_URL=postgresql://user:pass@host:5432/db
  python postgres-lab/src/1_copy_ingest.py
"""

import os, sys, time, random

try:
    import psycopg
except Exception as e:  # pragma: no cover
    print("Install psycopg (v3) to run: pip install psycopg[binary]", file=sys.stderr)
    raise


def run(rows: int = 20000):
    dsn = os.getenv("DATABASE_URL")
    if not dsn:
        print("Set DATABASE_URL", file=sys.stderr)
        return 2
    with psycopg.connect(dsn) as conn:
        with conn.cursor() as cur:
            cur.execute("drop table if exists copy_ingest_demo")
            cur.execute("create table copy_ingest_demo(k int, v text)")

        start = time.time()
        sql = psycopg.sql.SQL("COPY {} ({}) FROM STDIN").format(
            psycopg.sql.Identifier("copy_ingest_demo"),
            psycopg.sql.SQL("k,v"),
        )
        n = 0
        with conn.cursor() as cur:
            with cur.copy(sql) as cp:
                for i in range(rows):
                    # small text format protocol: \N for NULL; escape tabs/newlines
                    k = str(i)
                    v = f"val_{i}_{int(random.random()*1e6)}"
                    v = v.replace("\\", "\\\\").replace("\n", "\\n").replace("\t", "\\t")
                    cp.write(k + "\t" + v + "\n")
                    n += 1
        conn.commit()
        took = time.time() - start
        print({"rows": n, "seconds": round(took, 3), "rows_per_sec": int(n / max(took, 1e-6))})


if __name__ == "__main__":
    random.seed(42)
    run()
