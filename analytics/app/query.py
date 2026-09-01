"""Historical reports over the Parquet archive.

    docker compose run --rm dispatcher python -m app.query monthly
    docker compose run --rm dispatcher python -m app.query tenant --year 2025
    docker compose run --rm dispatcher python -m app.query user --tenant 3 --user 42
    docker compose run --rm dispatcher python -m app.query export --tenant 3 --year 2025
    docker compose run --rm dispatcher python -m app.query verify

Two rules every query here follows:

  * Name the columns. `SELECT *` on a columnar archive reads columns the report
    never uses.
  * Filter on year/month, not only created_at. Those come from the Hive-style
    directory names, so the filter prunes whole directories before any file is
    opened. A created_at filter alone still has to open every file to check its
    row-group statistics.
"""
import argparse
import os
import sys

from .clients import archive_glob, duck, read_archive, s3_client, setup_logging
from .config import cfg

log = setup_logging("query")


def _scope(year, month):
    """Turn optional year/month into a partition predicate + bound parameters."""
    clauses, params = [], []
    if year:
        clauses.append("year = ?")
        params.append(int(year))
    if month:
        clauses.append("month = ?")
        params.append(int(month))
    return (" WHERE " + " AND ".join(clauses) if clauses else ""), params


def run(con, sql, params=None):
    rel = con.execute(sql, params or [])
    rows = rel.fetchall()
    cols = [d[0] for d in rel.description]
    widths = [
        max(len(str(c)), *(len(str(r[i])) for r in rows)) if rows else len(str(c))
        for i, c in enumerate(cols)
    ]
    print("  ".join(str(c).ljust(w) for c, w in zip(cols, widths)))
    print("  ".join("-" * w for w in widths))
    for r in rows:
        print("  ".join(str(v).ljust(w) for v, w in zip(r, widths)))
    print(f"\n({len(rows)} rows)")
    return rows


def cmd_total(con, a):
    where, params = _scope(a.year, a.month)
    run(con, f"SELECT COUNT(*) AS total_events FROM {read_archive()}{where}", params)


def cmd_actions(con, a):
    where, params = _scope(a.year, a.month)
    run(con, f"""
        SELECT action, COUNT(*) AS total
          FROM {read_archive()}{where}
         GROUP BY action
         ORDER BY total DESC
    """, params)


def cmd_monthly(con, a):
    where, params = _scope(a.year, None)
    run(con, f"""
        SELECT year, month, COUNT(*) AS total,
               COUNT(DISTINCT tenant_id) AS tenants,
               MIN(created_at) AS first_event,
               MAX(created_at) AS last_event
          FROM {read_archive()}{where}
         GROUP BY year, month
         ORDER BY year, month
    """, params)


def cmd_tenant(con, a):
    where, params = _scope(a.year, a.month)
    run(con, f"""
        SELECT tenant_id, COUNT(*) AS total,
               COUNT(DISTINCT user_id) AS users
          FROM {read_archive()}{where}
         GROUP BY tenant_id
         ORDER BY total DESC
    """, params)


def cmd_user(con, a):
    if not a.tenant:
        raise SystemExit("--tenant is required (tenant isolation is not optional)")
    where, params = _scope(a.year, a.month)
    joiner = " AND " if where else " WHERE "
    sql = f"""
        SELECT created_at, action, subject_type, subject_id, properties
          FROM {read_archive()}{where}{joiner}tenant_id = ?
    """
    params.append(int(a.tenant))
    if a.user:
        sql += " AND user_id = ?"
        params.append(int(a.user))
    sql += " ORDER BY created_at DESC LIMIT ?"
    params.append(int(a.limit))
    run(con, sql, params)


def cmd_json(con, a):
    """JSON properties stay queryable without becoming typed columns."""
    where, params = _scope(a.year, a.month)
    joiner = " AND " if where else " WHERE "
    run(con, f"""
        SELECT json_extract_string(properties, '$.new.name') AS new_name,
               COUNT(*) AS total
          FROM {read_archive()}{where}{joiner}action = 'contact.updated'
         GROUP BY 1
         ORDER BY total DESC
         LIMIT 20
    """, params)


def cmd_export(con, a):
    """Write an export, then hand it to object storage. In production Laravel
    returns a short-lived signed URL to this object - never the bucket itself."""
    if not a.tenant:
        raise SystemExit("--tenant is required")
    where, params = _scope(a.year, a.month)
    joiner = " AND " if where else " WHERE "
    out_dir = os.path.join(cfg.work_dir, "exports")
    os.makedirs(out_dir, exist_ok=True)
    name = f"activity-t{a.tenant}-{a.year or 'all'}.csv"
    local = os.path.join(out_dir, name)

    params.append(int(a.tenant))
    con.execute(f"""
        COPY (
            SELECT event_id, tenant_id, user_id, action, subject_type,
                   subject_id, created_at
              FROM {read_archive()}{where}{joiner}tenant_id = ?
             ORDER BY created_at
        ) TO '{local}' (FORMAT CSV, HEADER, DELIMITER ',')
    """, params)

    key = f"exports/{name}"
    with open(local, "rb") as fh:
        s3_client().put_object(Bucket=cfg.s3_bucket, Key=key, Body=fh,
                               ContentType="text/csv")
    url = s3_client().generate_presigned_url(
        "get_object", Params={"Bucket": cfg.s3_bucket, "Key": key}, ExpiresIn=900
    )
    size = os.path.getsize(local)
    print(f"exported {size / 1024:.1f} KiB -> s3://{cfg.s3_bucket}/{key}")
    print(f"signed URL (15 min): {url}")


def cmd_verify(con, a):
    """Restore test: can the archive actually be read back, and does it agree
    with what MySQL still thinks it archived?

    §17 of the guide asks for this periodically. Object storage holding bytes is
    not the same thing as those bytes being queryable.
    """
    from .clients import mysql_conn

    rows = run(con, f"""
        SELECT year, month, COUNT(*) AS parquet_rows,
               MIN(created_at) AS lo, MAX(created_at) AS hi
          FROM {read_archive()}
         GROUP BY year, month ORDER BY year, month
    """)
    actual = {(r[0], r[1]): r[2] for r in rows}

    conn = mysql_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT year, month, SUM(rows_written) AS n, COUNT(*) AS objects
                  FROM archive_manifests GROUP BY year, month ORDER BY year, month
            """)
            claimed = cur.fetchall()
    finally:
        conn.close()

    print("\nmanifest vs archive")
    print("year  month  manifest_rows  parquet_rows  objects  status")
    bad = 0
    for c in claimed:
        got = actual.get((c["year"], c["month"]))
        ok = got == int(c["n"])
        bad += 0 if ok else 1
        print(f"{c['year']}  {c['month']:>5}  {int(c['n']):>13}  "
              f"{got if got is not None else 'MISSING':>12}  {c['objects']:>7}  "
              f"{'OK' if ok else 'MISMATCH'}")
    if bad:
        print(f"\n{bad} month(s) do not match. Do NOT drop MySQL partitions.")
        sys.exit(1)
    print("\nall archived months verified.")


COMMANDS = {
    "total": cmd_total, "actions": cmd_actions, "monthly": cmd_monthly,
    "tenant": cmd_tenant, "user": cmd_user, "json": cmd_json,
    "export": cmd_export, "verify": cmd_verify,
}


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("command", choices=sorted(COMMANDS))
    ap.add_argument("--year", type=int)
    ap.add_argument("--month", type=int)
    ap.add_argument("--tenant", type=int)
    ap.add_argument("--user", type=int)
    ap.add_argument("--limit", type=int, default=100)
    a = ap.parse_args()

    con = duck()
    try:
        log.info("scanning %s", archive_glob())
        COMMANDS[a.command](con, a)
    finally:
        con.close()


if __name__ == "__main__":
    main()
