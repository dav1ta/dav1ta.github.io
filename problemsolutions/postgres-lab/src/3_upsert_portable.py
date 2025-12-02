"""
Portable UPSERT: MERGE (15+, with RETURNING 17+) or ON CONFLICT fallback.

Usage:
  export DATABASE_URL=postgresql://user:pass@host:5432/db
  python postgres-lab/src/3_upsert_portable.py
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
    with psycopg.connect(dsn) as conn:
        with conn.cursor() as cur:
            cur.execute("drop table if exists upsert_demo")
            cur.execute("create table upsert_demo(k text primary key, v int, note text)")

        # feature detection: version and simple probe for MERGE returning
        with conn.cursor() as cur:
            cur.execute("show server_version_num")
            vnum = int(cur.fetchone()[0])
        has_merge = (vnum // 10000) >= 15
        merge_returning = (vnum // 10000) >= 17

        rows = [
            {"k": "a", "v": 1, "note": "first"},
            {"k": "b", "v": 2, "note": "second"},
            {"k": "a", "v": 9, "note": "updated"},
        ]

        if has_merge:
            values = psycopg.sql.SQL(",").join(
                psycopg.sql.SQL("({})").format(psycopg.sql.SQL(",").join(psycopg.sql.Placeholder() for _ in ("k","v","note")))
                for _ in rows
            )
            stmt = psycopg.sql.SQL(
                """
                MERGE INTO {t} AS t
                USING (VALUES {values}) AS s(k,v,note)
                ON t.k = s.k
                WHEN MATCHED THEN UPDATE SET v = s.v, note = s.note
                WHEN NOT MATCHED THEN INSERT (k,v,note) VALUES (s.k,s.v,s.note)
                {ret}
                """
            ).format(
                t=psycopg.sql.Identifier("upsert_demo"),
                values=values,
                ret=psycopg.sql.SQL("RETURNING action, k, v, note").if_(merge_returning)  # type: ignore[attr-defined]
            )
            params = []
            for r in rows:
                params.extend([r["k"], r["v"], r["note"]])
            with conn.cursor() as cur:
                try:
                    cur.execute(stmt, params)
                except Exception as e:
                    # some servers might not support RETURNING on MERGE yet
                    stmt_no_ret = psycopg.sql.SQL(str(stmt)).replace(" RETURNING action, k, v, note", "")
                    cur.execute(stmt_no_ret, params)
                try:
                    print("merge_result:", cur.fetchall())
                except Exception:
                    print("merge_result: (no RETURNING)")
        else:
            insert = psycopg.sql.SQL(
                "INSERT INTO {t}(k,v,note) VALUES (%s,%s,%s) ON CONFLICT (k) DO UPDATE SET v=EXCLUDED.v, note=EXCLUDED.note RETURNING k,v,note"
            ).format(t=psycopg.sql.Identifier("upsert_demo"))
            out = []
            with conn.cursor() as cur:
                for r in rows:
                    cur.execute(insert, (r["k"], r["v"], r["note"]))
                    out.append(cur.fetchone())
            print("upsert_result:", out)

        conn.commit()


if __name__ == "__main__":
    run()
