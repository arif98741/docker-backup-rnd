"""Connection factories for MySQL, Redis, S3 and DuckDB."""
import logging
import os
import sys

import boto3
import duckdb
import pymysql
import redis
from botocore.config import Config as BotoConfig

from .config import cfg


def setup_logging(name: str) -> logging.Logger:
    logging.basicConfig(
        level=os.getenv("LOG_LEVEL", "INFO"),
        format="%(asctime)s %(levelname)-5s [" + name + "] %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S",
        stream=sys.stdout,
    )
    return logging.getLogger(name)


def mysql_conn(autocommit: bool = False) -> pymysql.connections.Connection:
    conn = pymysql.connect(
        host=cfg.mysql_host,
        port=cfg.mysql_port,
        user=cfg.mysql_user,
        password=cfg.mysql_password,
        database=cfg.mysql_db,
        charset="utf8mb4",
        cursorclass=pymysql.cursors.DictCursor,
        autocommit=autocommit,
    )
    # Every session speaks UTC. Without this, DATETIME comparisons silently
    # shift and the Parquet partition an event lands in depends on the host TZ.
    with conn.cursor() as cur:
        cur.execute("SET time_zone = '+00:00'")
    if not autocommit:
        conn.commit()
    return conn


def redis_conn() -> redis.Redis:
    return redis.Redis(
        host=cfg.redis_host,
        port=cfg.redis_port,
        password=cfg.redis_password or None,
        decode_responses=True,
        socket_keepalive=True,
    )


def s3_client():
    return boto3.client(
        "s3",
        endpoint_url=f"{cfg.s3_scheme}://{cfg.s3_endpoint}",
        aws_access_key_id=cfg.s3_key,
        aws_secret_access_key=cfg.s3_secret,
        region_name=cfg.s3_region,
        config=BotoConfig(signature_version="s3v4", s3={"addressing_style": "path"}),
    )


def _q(v: str) -> str:
    """Escape a single-quoted SQL literal (config values only, never user input)."""
    return str(v).replace("'", "''")


def duck() -> duckdb.DuckDBPyConnection:
    """In-memory DuckDB wired to the object store, with bounded memory + spill.

    DuckDB holds no permanent state here: object storage is the archive, this is
    only the engine that reads it.
    """
    con = duckdb.connect(database=":memory:")
    con.execute(f"SET memory_limit='{cfg.duckdb_memory_limit}'")
    con.execute(f"SET threads={int(cfg.duckdb_threads)}")
    con.execute(f"SET temp_directory='{cfg.work_dir}/tmp'")
    con.execute("SET preserve_insertion_order=false")

    con.execute("INSTALL httpfs; LOAD httpfs;")
    con.execute("INSTALL json;  LOAD json;")

    # Modern credential mechanism (DuckDB >= 0.10). CREATE SECRET is DDL and
    # does not accept bound parameters, so values are quote-escaped instead.
    # They come from the container environment, never from an HTTP request.
    con.execute(
        f"""
        CREATE OR REPLACE SECRET object_store (
            TYPE       S3,
            KEY_ID     '{_q(cfg.s3_key)}',
            SECRET     '{_q(cfg.s3_secret)}',
            REGION     '{_q(cfg.s3_region)}',
            ENDPOINT   '{_q(cfg.s3_endpoint)}',
            URL_STYLE  'path',
            USE_SSL    {'true' if cfg.s3_use_ssl else 'false'}
        )
        """
    )
    return con


def archive_glob(year: int | None = None, month: int | None = None) -> str:
    """Path to read. Hive-style directories are only pruned when the reader is
    told they are Hive-style - see read_archive()."""
    base = f"s3://{cfg.s3_bucket}/{cfg.archive_prefix}"
    if year is not None and month is not None:
        return f"{base}/year={year}/month={month:02d}/*.parquet"
    if year is not None:
        return f"{base}/year={year}/**/*.parquet"
    return f"{base}/**/*.parquet"


def read_archive(path: str | None = None) -> str:
    """SQL fragment for scanning the archive.

    hive_partitioning exposes year/month as real columns so a WHERE on them
    prunes whole directories instead of opening every file.
    union_by_name lets files written under different schema_versions coexist.
    """
    return (
        f"read_parquet('{path or archive_glob()}', "
        "hive_partitioning = true, union_by_name = true)"
    )
