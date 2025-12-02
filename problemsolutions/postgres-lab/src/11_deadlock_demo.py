"""
Demonstrate a deadlock between two transactions.

Usage:
  export DATABASE_URL=postgresql://user:pass@host:5432/db
  python postgres-lab/src/11_deadlock_demo.py
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
    a = psycopg.connect(dsn)
    b = psycopg.connect(dsn)
    with a, a.cursor() as ca, b, b.cursor() as cb:
        ca.execute("drop table if exists d")
        ca.execute("create table d(id int primary key, v int)")
        ca.executemany("insert into d values (%s,%s)", [(1,1),(2,2)])
        a.commit()
        ca.execute("begin"); cb.execute("begin")
        ca.execute("update d set v=3 where id=1")
        cb.execute("update d set v=4 where id=2")
        try:
            cb.execute("update d set v=5 where id=1")
            ca.execute("update d set v=6 where id=2")
        except Exception as e:
            print("deadlock detected:", str(e)[:200])


if __name__ == "__main__":
    run()

