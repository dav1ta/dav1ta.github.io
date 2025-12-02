"""
Latency-bound small writes reduced with libpq pipeline mode (14+).

Usage:
  export DATABASE_URL=postgresql://user:pass@host:5432/db
  python postgres-lab/src/2_pipeline_inserts.py
"""

import os, sys, time

try:
    import psycopg
except Exception as e:  # pragma: no cover
    print("Install psycopg (v3) to run: pip install psycopg[binary]", file=sys.stderr)
    raise


def run(n_rows: int = 5000, batch: int = 1000):
    dsn = os.getenv("DATABASE_URL")
    if not dsn:
        print("Set DATABASE_URL", file=sys.stderr)
        return 2
    with psycopg.connect(dsn) as conn:
        with conn.cursor() as cur:
            cur.execute("drop table if exists pipeline_demo")
            cur.execute("create table pipeline_demo(id int primary key, note text)")

        insert_sql = psycopg.sql.SQL("insert into {}(id,note) values (%s,%s)").format(
            psycopg.sql.Identifier("pipeline_demo")
        )

        start = time.time()
        # gate: pipeline exists on psycopg v3 with libpq 14+
        if hasattr(conn, "pipeline"):
            left = n_rows
            id_base = 0
            while left > 0:
                sz = min(batch, left)
                with conn.pipeline() as p:  # type: ignore[attr-defined]
                    with p.cursor() as cur:
                        for i in range(sz):
                            cur.execute(insert_sql, (id_base + i, f"piped_{id_base+i}"))
                left -= sz
                id_base += sz
        else:  # fallback: executemany
            rows = [(i, f"plain_{i}") for i in range(n_rows)]
            with conn.cursor() as cur:
                cur.executemany(insert_sql.as_string(conn), rows)
        conn.commit()
        took = time.time() - start
        print({"rows": n_rows, "batch": batch, "seconds": round(took, 3), "rows_per_sec": int(n_rows / max(took, 1e-6))})


if __name__ == "__main__":
    run()
