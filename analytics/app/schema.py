"""The archive's Parquet schema, declared explicitly.

Inferring the schema from a DataFrame is how `created_at` ends up as a string
column that `date_trunc()` then refuses to accept. Every archive file is written
against this schema, so the column types are stable across years of files.

Evolution rule: additive, nullable columns only. Bump SCHEMA_VERSION in
config.py, and readers keep working because reads use union_by_name = true.
"""
import pyarrow as pa

ARCHIVE_SCHEMA = pa.schema(
    [
        pa.field("event_id", pa.string(), nullable=False),
        pa.field("tenant_id", pa.int32(), nullable=False),
        pa.field("user_id", pa.int64(), nullable=True),
        pa.field("action", pa.string(), nullable=False),
        pa.field("subject_type", pa.string(), nullable=True),
        pa.field("subject_id", pa.int64(), nullable=True),
        # Kept as a JSON string: queried with json_extract_string(), rare enough
        # not to justify a struct that every schema change would have to migrate.
        pa.field("properties", pa.string(), nullable=True),
        pa.field("ip_address", pa.string(), nullable=True),
        # Microsecond UTC. Not a string, not a naive local timestamp.
        pa.field("created_at", pa.timestamp("us", tz="UTC"), nullable=False),
    ]
)

# Columns pulled from MySQL, in schema order.
SELECT_COLUMNS = [f.name for f in ARCHIVE_SCHEMA]
