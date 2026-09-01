"""Single source of truth for runtime configuration. Everything comes from env."""
import os
from dataclasses import dataclass

# Bump when the Parquet column set changes. Recorded in every manifest so a
# reader can tell which files predate a change.
SCHEMA_VERSION = 1


def _int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, default))
    except (TypeError, ValueError):
        return default


@dataclass(frozen=True)
class Config:
    mysql_host: str = os.getenv("MYSQL_HOST", "mysql")
    mysql_port: int = _int("MYSQL_INTERNAL_PORT", 3306)
    mysql_db: str = os.getenv("MYSQL_DATABASE", "activity")
    mysql_user: str = os.getenv("MYSQL_USER", "activity")
    mysql_password: str = os.getenv("MYSQL_PASSWORD", "")

    redis_host: str = os.getenv("REDIS_HOST", "redis")
    redis_port: int = _int("REDIS_INTERNAL_PORT", 6379)
    redis_password: str = os.getenv("REDIS_PASSWORD", "")

    s3_endpoint: str = os.getenv("S3_ENDPOINT", "minio:9000")
    s3_bucket: str = os.getenv("MINIO_BUCKET", "activity-archive")
    s3_region: str = os.getenv("MINIO_REGION", "us-east-1")
    s3_key: str = os.getenv("MINIO_ROOT_USER", "")
    s3_secret: str = os.getenv("MINIO_ROOT_PASSWORD", "")
    s3_use_ssl: bool = os.getenv("S3_USE_SSL", "false").lower() == "true"

    archive_batch_size: int = _int("ARCHIVE_BATCH_SIZE", 50_000)
    dispatch_interval: int = _int("DISPATCH_INTERVAL", 15)
    reclaim_after: int = _int("RECLAIM_AFTER", 300)

    duckdb_memory_limit: str = os.getenv("DUCKDB_MEMORY_LIMIT", "4GB")
    duckdb_threads: int = _int("DUCKDB_THREADS", 4)

    work_dir: str = os.getenv("WORK_DIR", "/var/lib/analytics")

    # Redis keys
    queue_key: str = "archive:jobs"
    inflight_key: str = "archive:inflight"

    @property
    def archive_prefix(self) -> str:
        return "activity_logs"

    @property
    def s3_scheme(self) -> str:
        return "https" if self.s3_use_ssl else "http"


cfg = Config()
