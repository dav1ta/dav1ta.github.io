"""
Show EXPLAIN plans that can benefit from B-tree skip scans (18+).

Usage:
  export DATABASE_URL=postgresql://user:pass@host:5432/db
  python postgres-lab/src/7_skip_scans_explain.py
"""

import os, sys, random

try:
    import psycopg
except Exception as e:  # pragma: no cover
    print("Install psycopg (v3) to run: pip install psycopg[binary]", file=sys.stderr)
    raise


def run(n: int = 20000):
    dsn = os.getenv("DATABASE_URL")
    if not dsn:
        print("Set DATABASE_URL", file=sys.stderr)
        return 2
    with psycopg.connect(dsn) as conn:
        with conn.cursor() as cur:
            cur.execute("drop table if exists skip_demo")
            cur.execute("create table skip_demo(a int, b int, filler text)")
            cur.execute("create index on skip_demo(a,b)")
            # load data quickly with COPY
            sql = psycopg.sql.SQL("COPY {} (a,b,filler) FROM STDIN").format(psycopg.sql.Identifier("skip_demo"))
            with cur.copy(sql) as cp:
                for i in range(n):
                    a = random.randint(0, 100)
                    b = random.randint(0, 100)
                    s = f"x{i}"
                    s = s.replace("\\", "\\\\")
                    cp.write(f"{a}\t{b}\t{s}\n")
            conn.commit()
            # Query without a predicate on the leading column 'a'; PG18 may use skip scan
            cur.execute("EXPLAIN SELECT * FROM skip_demo WHERE b = 42 ORDER BY a LIMIT 10")
            plan = "\n".join(r[0] for r in cur.fetchall())
            print(plan)


if __name__ == "__main__":
    random.seed(42)
    run()
