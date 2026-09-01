"""Compaction: merge a month's small Parquet parts into one large file.

    docker compose run --rm dispatcher python -m app.compact --year 2025 --month 3
    docker compose run --rm dispatcher python -m app.compact --all --min-parts 4

Why this job exists at all: the archive worker writes one part per batch, so an
actively-archiving month accumulates many small files. Small files are both a
query problem (one S3 GET each, poor row-group statistics) and a cost problem
(object stores bill per request). Left alone, a busy month becomes thousands of
parts and every historical query pays for it.

It doubles as the late-arrival mechanism. Parquet objects are immutable, so an
event that shows up after its month was sealed simply lands in a new part; the
next compaction folds it into the month's file. That is the only safe way to
"append" to an immutable archive.

Safety: the merged object is written, verified and manifested BEFORE any source
part is deleted, and deletion only happens once the merged row count matches
the sum of the parts it replaced.
"""
import argparse
import hashlib
import io
import json
import os
from datetime import datetime, timezone

from .clients import duck, mysql_conn, s3_client, setup_logging
from .config import SCHEMA_VERSION, cfg

log = setup_logging("compact")


def list_parts(s3, year: int, month: int) -> list[dict]:
    prefix = f"{cfg.archive_prefix}/year={year}/month={month:02d}/"
    out, token = [], None
    while True:
        kw = {"Bucket": cfg.s3_bucket, "Prefix": prefix}
        if token:
            kw["ContinuationToken"] = token
        resp = s3.list_objects_v2(**kw)
        for o in resp.get("Contents", []):
            if o["Key"].endswith(".parquet") and "/compacted-" not in o["Key"]:
                out.append(o)
        if not resp.get("IsTruncated"):
            return out
        token = resp["NextContinuationToken"]


def months_with_parts(s3) -> list[tuple[int, int]]:
    found, token = set(), None
    while True:
        kw = {"Bucket": cfg.s3_bucket, "Prefix": f"{cfg.archive_prefix}/"}
        if token:
            kw["ContinuationToken"] = token
        resp = s3.list_objects_v2(**kw)
        for o in resp.get("Contents", []):
            parts = o["Key"].split("/")
            if len(parts) >= 4 and parts[1].startswith("year=") and parts[2].startswith("month="):
                found.add((int(parts[1][5:]), int(parts[2][6:])))
        if not resp.get("IsTruncated"):
            return sorted(found)
        token = resp["NextContinuationToken"]


def compact(year: int, month: int, min_parts: int, dry_run: bool) -> bool:
    s3 = s3_client()
    parts = list_parts(s3, year, month)
    if len(parts) < min_parts:
        log.info("%s-%02d: %s part(s), below --min-parts %s, skipping",
                 year, month, len(parts), min_parts)
        return False

    src_glob = f"s3://{cfg.s3_bucket}/{cfg.archive_prefix}/year={year}/month={month:02d}/*.parquet"
    staging = os.path.join(cfg.work_dir, "staging")
    os.makedirs(staging, exist_ok=True)
    local = os.path.join(staging, f"compacted-{year}{month:02d}.parquet")

    con = duck()
    try:
        expected = con.execute(
            f"SELECT COUNT(*) FROM read_parquet('{src_glob}', union_by_name = true)"
        ).fetchone()[0]
        log.info("%s-%02d: merging %s parts / %s rows", year, month, len(parts), expected)
        if dry_run:
            return False

        # Sorted by time so row-group min/max statistics are tight and a
        # created_at filter can skip most of the file.
        con.execute(f"""
            COPY (
                SELECT * EXCLUDE (year, month)
                  FROM read_parquet('{src_glob}', hive_partitioning = true,
                                    union_by_name = true)
                 ORDER BY created_at, event_id
            ) TO '{local}'
              (FORMAT PARQUET, COMPRESSION ZSTD, COMPRESSION_LEVEL 6,
               ROW_GROUP_SIZE 128000)
        """)
        merged = con.execute(f"SELECT COUNT(*), MIN(created_at), MAX(created_at) "
                             f"FROM read_parquet('{local}')").fetchone()
    finally:
        con.close()

    if merged[0] != expected:
        os.remove(local)
        raise RuntimeError(f"{year}-{month:02d}: merged {merged[0]} rows, expected {expected}")

    h = hashlib.sha256()
    with open(local, "rb") as fh:
        while True:
            chunk = fh.read(1024 * 1024)
            if not chunk:
                break
            h.update(chunk)
    checksum = h.hexdigest()
    size = os.path.getsize(local)
    key = f"{cfg.archive_prefix}/year={year}/month={month:02d}/compacted-{checksum[:16]}.parquet"

    with open(local, "rb") as fh:
        s3.put_object(Bucket=cfg.s3_bucket, Key=key, Body=fh,
                      ContentType="application/vnd.apache.parquet",
                      Metadata={"sha256": checksum, "schema-version": str(SCHEMA_VERSION)})

    # Verify the merged object in place before removing anything.
    con = duck()
    try:
        remote = con.execute(
            f"SELECT COUNT(*) FROM read_parquet('s3://{cfg.s3_bucket}/{key}')"
        ).fetchone()[0]
    finally:
        con.close()
    if remote != expected:
        s3.delete_object(Bucket=cfg.s3_bucket, Key=key)
        os.remove(local)
        raise RuntimeError(f"{year}-{month:02d}: read-back {remote}, expected {expected}")

    manifest = {
        "object": key, "bucket": cfg.s3_bucket, "rows": int(expected),
        "bytes": int(size), "min_created_at": merged[1].isoformat(),
        "max_created_at": merged[2].isoformat(), "schema_version": SCHEMA_VERSION,
        "sha256": checksum, "compacted_from": [p["Key"] for p in parts],
        "written_at": datetime.now(timezone.utc).isoformat(),
    }
    s3.put_object(
        Bucket=cfg.s3_bucket,
        Key=f"manifests/year={year}/month={month:02d}/{os.path.basename(key)}.json",
        Body=io.BytesIO(json.dumps(manifest, indent=2).encode()),
        ContentType="application/json",
    )

    conn = mysql_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO archive_manifests
                    (object_key, bucket, year, month, rows_written, bytes_written,
                     min_created_at, max_created_at, schema_version, sha256, verified_at)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s, UTC_TIMESTAMP(6))
                ON DUPLICATE KEY UPDATE rows_written = VALUES(rows_written),
                    bytes_written = VALUES(bytes_written), sha256 = VALUES(sha256),
                    verified_at = VALUES(verified_at)
            """, (key, cfg.s3_bucket, year, month, expected, size,
                  merged[1].replace(tzinfo=None), merged[2].replace(tzinfo=None),
                  SCHEMA_VERSION, checksum))
            # Only now do the superseded parts leave the manifest table.
            for p in parts:
                cur.execute("DELETE FROM archive_manifests WHERE object_key = %s", (p["Key"],))
        conn.commit()
    finally:
        conn.close()

    s3.delete_objects(
        Bucket=cfg.s3_bucket,
        Delete={"Objects": [{"Key": p["Key"]} for p in parts], "Quiet": True},
    )
    os.remove(local)
    log.info("%s-%02d: %s parts -> 1 object, %s rows, %.1f MiB",
             year, month, len(parts), expected, size / 1024 / 1024)
    return True


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--year", type=int)
    ap.add_argument("--month", type=int)
    ap.add_argument("--all", action="store_true", help="every month in the archive")
    ap.add_argument("--min-parts", type=int, default=2)
    ap.add_argument("--dry-run", action="store_true")
    a = ap.parse_args()

    if a.all:
        targets = months_with_parts(s3_client())
    elif a.year and a.month:
        targets = [(a.year, a.month)]
    else:
        raise SystemExit("give --year and --month, or --all")

    done = 0
    for y, m in targets:
        try:
            done += 1 if compact(y, m, a.min_parts, a.dry_run) else 0
        except Exception as exc:
            log.error("%s-%02d failed, sources left intact: %s", y, m, exc)
    log.info("compacted %s month(s)", done)


if __name__ == "__main__":
    main()
