"""
Simulate lock contention and observe timeout.

Usage:
  export DATABASE_URL=postgresql://user:pass@host:5432/db
  python postgres-lab/src/10_lock_contention.py
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
    with a, a.cursor() as ca:
        ca.execute("drop table if exists t")
        ca.execute("create table t(id int primary key, v int)")
        ca.execute("insert into t values (1,10)")
        a.commit()
        ca.execute("begin")
        ca.execute("update t set v=11 where id=1")  # hold row lock
        with b, b.cursor() as cb:
            cb.execute("set lock_timeout='300ms'")
            try:
                cb.execute("update t set v=12 where id=1")
            except Exception as e:
                print("blocked->timeout:", str(e)[:120])


if __name__ == "__main__":
    run()

