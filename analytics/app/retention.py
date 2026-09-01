"""MySQL hot-window cleanup: drop partitions that are fully archived.

    docker compose run --rm dispatcher python -m app.retention --keep-months 12
    docker compose run --rm dispatcher python -m app.retention --keep-months 12 --apply

Dry-run by default. `--apply` is the only way to drop anything.

Three conditions must all hold before a partition is dropped, in this order:

  1. The partition is entirely older than the hot window.
  2. Every outbox row in that range is 'archived' - nothing pending or claimed.
  3. The archive itself, read back through DuckDB right now, contains at least
     as many rows for that range as MySQL does.

Condition 3 is the one that matters. A manifest row says an upload once
succeeded; only a live read proves the data is still there and still readable.
"""
import argparse
from datetime import datetime, timedelta, timezone

from .clients import duck, mysql_conn, read_archive, setup_logging
from .config import cfg

log = setup_logging("retention")


def partitions(conn) -> list[dict]:
    with conn.cursor() as cur:
        cur.execute("""
            SELECT partition_name, partition_description, table_rows
              FROM information_schema.partitions
             WHERE table_schema = %s AND table_name = 'activity_logs'
               AND partition_name IS NOT NULL
             ORDER BY partition_ordinal_position
        """, (cfg.mysql_db,))
        return cur.fetchall()


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--keep-months", type=int, default=12)
    ap.add_argument("--apply", action="store_true")
    a = ap.parse_args()

    cutoff = (datetime.now(timezone.utc).replace(day=1, hour=0, minute=0,
                                                 second=0, microsecond=0)
              - timedelta(days=31 * a.keep_months)).replace(day=1)
    log.info("hot window keeps everything from %s onward", cutoff.date())

    conn = mysql_conn()
    con = duck()
    dropped = 0
    try:
        for p in partitions(conn):
            name = p["partition_name"]
            desc = (p["partition_description"] or "").strip("'")
            if desc in ("MAXVALUE", ""):
                continue
            try:
                upper = datetime.fromisoformat(desc).replace(tzinfo=timezone.utc)
            except ValueError:
                log.warning("%s: cannot parse bound %r, skipping", name, desc)
                continue
            if upper > cutoff:
                continue

            lower = (upper - timedelta(days=1)).replace(day=1)

            with conn.cursor() as cur:
                cur.execute("""SELECT COUNT(*) AS n FROM outbox_events
                                WHERE created_at >= %s AND created_at < %s
                                  AND status <> 'archived'""", (lower, upper))
                unarchived = cur.fetchone()["n"]
                cur.execute("""SELECT COUNT(*) AS n FROM activity_logs
                                WHERE created_at >= %s AND created_at < %s""",
                            (lower, upper))
                mysql_rows = cur.fetchone()["n"]

            if unarchived:
                log.warning("%s: %s events not archived yet - KEEPING", name, unarchived)
                continue

            archived_rows = con.execute(
                f"""SELECT COUNT(*) FROM {read_archive()}
                     WHERE created_at >= ? AND created_at < ?""",
                [lower, upper],
            ).fetchone()[0]

            if archived_rows < mysql_rows:
                log.error("%s: archive has %s rows, MySQL has %s - KEEPING",
                          name, archived_rows, mysql_rows)
                continue

            if not a.apply:
                log.info("%s: would drop (%s rows, %s in archive)",
                         name, mysql_rows, archived_rows)
                continue

            with conn.cursor() as cur:
                # Partition names are read from information_schema, not user input.
                cur.execute(f"ALTER TABLE activity_logs DROP PARTITION `{name}`")
                cur.execute("""DELETE FROM outbox_events
                                WHERE created_at >= %s AND created_at < %s""",
                            (lower, upper))
            conn.commit()
            dropped += 1
            log.info("%s: dropped, %s rows released (archive verified at %s)",
                     name, mysql_rows, archived_rows)
    finally:
        con.close()
        conn.close()

    if not a.apply:
        log.info("dry run - nothing changed. Re-run with --apply to drop.")
    else:
        log.info("dropped %s partition(s)", dropped)


if __name__ == "__main__":
    main()
