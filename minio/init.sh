#!/bin/sh
# Idempotent: safe to re-run on every `docker compose up`.
set -eu

echo "[minio-init] waiting for ${MINIO_ALIAS_URL}"
until mc alias set local "${MINIO_ALIAS_URL}" "${MINIO_ROOT_USER}" "${MINIO_ROOT_PASSWORD}" >/dev/null 2>&1; do
  sleep 2
done

mc mb --ignore-existing "local/${MINIO_BUCKET}"

# The archive is private. Nothing is served publicly; Laravel hands out signed URLs.
mc anonymous set none "local/${MINIO_BUCKET}" >/dev/null 2>&1 || true

# Versioning: recovers from an accidental overwrite or delete of an archive object.
mc version enable "local/${MINIO_BUCKET}" >/dev/null 2>&1 || \
  echo "[minio-init] versioning unavailable on this backend, continuing"

echo "[minio-init] bucket ready: ${MINIO_BUCKET}"
mc ls local
