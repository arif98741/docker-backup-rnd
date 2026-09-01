"""Outbox sweeper.

Two jobs, on a loop:

  1. Reclaim - outbox rows stuck in 'claimed' past RECLAIM_AFTER go back to
     'pending'. This is what makes a worker crash survivable.
  2. Enqueue - one archive job per (year, month) that still has pending rows.
     A short-lived Redis lease per month stops a slow month being queued twice,
     and expires on its own if the worker holding it dies.
"""
import json
import signal
import time

from .clients import mysql_conn, redis_conn, setup_logging
from .config import cfg

log = setup_logging("dispatcher")
_running = True


def _stop(signum, _frame):
    global _running
    log.info("signal %s received, exiting", signum)
    _running = False


def reclaim(conn) -> int:
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE outbox_events
               SET status = 'pending', claimed_at = NULL
             WHERE status = 'claimed'
               AND claimed_at < UTC_TIMESTAMP(6) - INTERVAL %s SECOND
            """,
            (cfg.reclaim_after,),
        )
        n = cur.rowcount
    conn.commit()
    return n


def pending_buckets(conn) -> list[dict]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT YEAR(created_at) AS y, MONTH(created_at) AS m, COUNT(*) AS n
              FROM outbox_events
             WHERE status = 'pending'
             GROUP BY 1, 2
             ORDER BY 1, 2
            """
        )
        return cur.fetchall()


def main() -> None:
    signal.signal(signal.SIGTERM, _stop)
    signal.signal(signal.SIGINT, _stop)
    r = redis_conn()
    log.info("sweeping outbox every %ss", cfg.dispatch_interval)

    while _running:
        try:
            conn = mysql_conn()
            try:
                released = reclaim(conn)
                if released:
                    log.warning("reclaimed %s stuck events", released)

                for b in pending_buckets(conn):
                    bucket_id = f"{b['y']}-{int(b['m']):02d}"
                    # nx=True: only one dispatcher wins the lease for this month.
                    # ex=ttl: a dead worker's lease expires and the month retries.
                    if not r.set(cfg.inflight_prefix + bucket_id, "1",
                                 nx=True, ex=cfg.inflight_ttl):
                        continue
                    job = {
                        "year": int(b["y"]),
                        "month": int(b["m"]),
                        "batch": cfg.archive_batch_size,
                    }
                    r.rpush(cfg.queue_key, json.dumps(job))
                    log.info("queued %s (%s pending events)", bucket_id, b["n"])
            finally:
                conn.close()
        except Exception as exc:
            log.error("sweep failed: %s", exc)

        for _ in range(cfg.dispatch_interval):
            if not _running:
                break
            time.sleep(1)

    log.info("stopped")


if __name__ == "__main__":
    main()
