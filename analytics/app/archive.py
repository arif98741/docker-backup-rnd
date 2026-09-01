"""Archive worker: MySQL batch -> Parquet -> checksum -> object storage -> manifest.

Ordering is the whole point, and it is deliberate:

    claim -> write -> verify locally -> upload -> manifest -> verify read-back
          -> mark archived

Nothing is marked archived until the object is durable and has been read back
successfully. A partially written file is never published, and a crash at any
step leaves the batch re-runnable.

Idempotency: the object key is derived from a hash of the event_ids in the
batch, so replaying the same batch overwrites the identical object instead of
appending a duplicate part.
"""
import hashlib
import io
import json
import os
import signal
import time
from datetime import datetime, timezone

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from .clients import duck, mysql_conn, redis_conn, s3_client, setup_logging
from .config import SCHEMA_VERSION, cfg
from .schema import ARCHIVE_SCHEMA

log = setup_logging("worker")
_running = True


def _stop(signum, _frame):
    global _running
    log.info("signal %s received, finishing current job then exiting", signum)
    _running = False


def month_bounds(year: int, month: int) -> tuple[datetime, datetime]:
    start = datetime(year, month, 1, tzinfo=timezone.utc)
    end = (
        datetime(year + 1, 1, 1, tzinfo=timezone.utc)
        if month == 12
        else datetime(year, month + 1, 1, tzinfo=timezone.utc)
    )
    return start, end


def claim_batch(conn, year: int, month: int, limit: int) -> list[str]:
    """Move up to `limit` pending outbox rows for this month into 'claimed'.

    Claiming inside a transaction stops two workers taking the same events. A
    row left stuck in 'claimed' is released by the dispatcher's reclaim sweep.
    """
    start, end = month_bounds(year, month)
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE outbox_events
               SET status = 'claimed', claimed_at = UTC_TIMESTAMP(6),
                   attempts = attempts + 1
             WHERE status = 'pending' AND created_at >= %s AND created_at < %s
             ORDER BY created_at, event_id
             LIMIT %s
            """,
            (start, end, limit),
        )
        if cur.rowcount == 0:
            conn.commit()
            return []
        cur.execute(
            """
            SELECT event_id FROM outbox_events
             WHERE status = 'claimed' AND created_at >= %s AND created_at < %s
             ORDER BY created_at, event_id
             LIMIT %s
            """,
            (start, end, limit),
        )
        ids = [r["event_id"] for r in cur.fetchall()]
    conn.commit()
    return ids


def fetch_events(conn, event_ids: list[str]) -> pd.DataFrame:
    """Load the claimed events.

    Placeholders are generated, values are bound. Data never reaches SQL by
    string interpolation - the shape of the query is fixed, the values are not.
    """
    placeholders = ", ".join(["%s"] * len(event_ids))
    sql = f"""
        SELECT event_id, tenant_id, user_id, action, subject_type, subject_id,
               properties, ip_address, created_at
          FROM activity_logs
         WHERE event_id IN ({placeholders})
         ORDER BY created_at, event_id
    """
    with conn.cursor() as cur:
        cur.execute(sql, event_ids)
        rows = cur.fetchall()
    return normalise(rows)


def normalise(rows: list[dict]) -> pd.DataFrame:
    """Coerce to the archive schema's types before Parquet ever sees the data."""
    df = pd.DataFrame(rows, columns=[f.name for f in ARCHIVE_SCHEMA])
    if df.empty:
        return df
    df["created_at"] = pd.to_datetime(df["created_at"], utc=True)
    df["tenant_id"] = df["tenant_id"].astype("int32")
    for col in ("user_id", "subject_id"):
        df[col] = df[col].astype("Int64")
    for col in ("event_id", "action", "subject_type", "properties", "ip_address"):
        df[col] = df[col].astype("string")
    return df


def batch_key(year: int, month: int, event_ids: list[str]) -> str:
    """Deterministic object key: same events in, same key out."""
    joined = chr(10).join(sorted(event_ids)).encode()
    digest = hashlib.sha256(joined).hexdigest()[:16]
    return f"{cfg.archive_prefix}/year={year}/month={month:02d}/part-{digest}.parquet"


def write_parquet(df: pd.DataFrame, path: str) -> None:
    table = pa.Table.from_pandas(df, schema=ARCHIVE_SCHEMA, preserve_index=False)
    pq.write_table(
        table,
        path,
        compression="zstd",
        compression_level=6,
        row_group_size=128_000,      # keeps row-group statistics useful for pruning
        use_dictionary=True,
        write_statistics=True,
    )


def sha256_of(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        while True:
            chunk = fh.read(1024 * 1024)
            if not chunk:
                break
            h.update(chunk)
    return h.hexdigest()


def process(job: dict) -> int:
    year, month = int(job["year"]), int(job["month"])
    limit = int(job.get("batch", cfg.archive_batch_size))
    conn = mysql_conn()
    s3 = s3_client()
    staging = os.path.join(cfg.work_dir, "staging")
    os.makedirs(staging, exist_ok=True)
    local = None

    try:
        event_ids = claim_batch(conn, year, month, limit)
        if not event_ids:
            return 0

        df = fetch_events(conn, event_ids)
        if len(df) != len(event_ids):
            # An outbox row without its activity row means the two writes were
            # not in one transaction. Loud failure beats a silent gap.
            raise RuntimeError(
                f"claimed {len(event_ids)} events but found {len(df)} in activity_logs"
            )

        key = batch_key(year, month, event_ids)
        local = os.path.join(staging, os.path.basename(key))
        write_parquet(df, local)

        # 1. verify the staged file before it ever leaves this host
        con = duck()
        staged = con.execute(
            "SELECT COUNT(*), MIN(created_at), MAX(created_at) "
            f"FROM read_parquet('{local}')"
        ).fetchone()
        con.close()
        if staged[0] != len(df):
            raise RuntimeError(
                f"staged parquet has {staged[0]} rows, expected {len(df)}"
            )

        checksum = sha256_of(local)
        size = os.path.getsize(local)

        # 2. upload only a complete file
        with open(local, "rb") as fh:
            s3.put_object(
                Bucket=cfg.s3_bucket,
                Key=key,
                Body=fh,
                ContentType="application/vnd.apache.parquet",
                Metadata={"sha256": checksum, "schema-version": str(SCHEMA_VERSION)},
            )

        manifest = {
            "object": key,
            "bucket": cfg.s3_bucket,
            "rows": int(len(df)),
            "bytes": int(size),
            "min_created_at": staged[1].isoformat(),
            "max_created_at": staged[2].isoformat(),
            "schema_version": SCHEMA_VERSION,
            "sha256": checksum,
            "written_at": datetime.now(timezone.utc).isoformat(),
        }
        s3.put_object(
            Bucket=cfg.s3_bucket,
            Key=f"manifests/year={year}/month={month:02d}/{os.path.basename(key)}.json",
            Body=io.BytesIO(json.dumps(manifest, indent=2).encode()),
            ContentType="application/json",
        )

        # 3. read the object back out of storage before trusting it
        con = duck()
        remote = con.execute(
            f"SELECT COUNT(*) FROM read_parquet('s3://{cfg.s3_bucket}/{key}')"
        ).fetchone()[0]
        con.close()
        if remote != len(df):
            raise RuntimeError(f"read-back returned {remote} rows, expected {len(df)}")

        # 4. only now is the batch archived
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO archive_manifests
                    (object_key, bucket, year, month, rows_written, bytes_written,
                     min_created_at, max_created_at, schema_version, sha256,
                     verified_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, UTC_TIMESTAMP(6))
                ON DUPLICATE KEY UPDATE
                    rows_written  = VALUES(rows_written),
                    bytes_written = VALUES(bytes_written),
                    sha256        = VALUES(sha256),
                    verified_at   = VALUES(verified_at)
                """,
                (
                    key, cfg.s3_bucket, year, month, len(df), size,
                    df["created_at"].min().to_pydatetime(),
                    df["created_at"].max().to_pydatetime(),
                    SCHEMA_VERSION, checksum,
                ),
            )
            placeholders = ", ".join(["%s"] * len(event_ids))
            cur.execute(
                f"""UPDATE outbox_events
                       SET status = 'archived', archived_at = UTC_TIMESTAMP(6)
                     WHERE event_id IN ({placeholders})""",
                event_ids,
            )
        conn.commit()

        log.info(
            "archived %s rows -> s3://%s/%s (%.1f KiB)",
            len(df), cfg.s3_bucket, key, size / 1024,
        )
        return len(df)

    except Exception:
        conn.rollback()
        raise
    finally:
        if local and os.path.exists(local):
            os.remove(local)
        conn.close()


def main() -> None:
    signal.signal(signal.SIGTERM, _stop)
    signal.signal(signal.SIGINT, _stop)
    r = redis_conn()
    log.info("waiting on %s", cfg.queue_key)

    while _running:
        item = r.blpop(cfg.queue_key, timeout=5)
        if item is None:
            continue
        _, payload = item
        job = json.loads(payload)
        bucket_id = f"{job['year']}-{int(job['month']):02d}"
        lease = cfg.inflight_prefix + bucket_id
        requeued = False
        try:
            written = process(job)
            # More rows may remain for this month; re-queue so the next batch
            # runs, and push the lease out so the dispatcher does not duplicate it.
            if written >= int(job.get("batch", cfg.archive_batch_size)):
                r.expire(lease, cfg.inflight_ttl)
                r.rpush(cfg.queue_key, payload)
                requeued = True
        except Exception as exc:
            log.exception("job %s failed: %s", bucket_id, exc)
            time.sleep(2)
        finally:
            if not requeued:
                r.delete(lease)

    log.info("stopped")


if __name__ == "__main__":
    main()
