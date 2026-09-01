-- ---------------------------------------------------------------------------
-- Hot store schema.
--
-- activity_logs is RANGE-partitioned on created_at from day one. The archive
-- lifecycle ends with "drop the old MySQL partition", and retrofitting
-- partitioning onto a large live table later is painful - so it is here now.
--
-- MySQL requires every unique key to contain the partitioning column, which is
-- why the PK is (id, created_at) and the natural key is (event_id, created_at).
-- ---------------------------------------------------------------------------
SET time_zone = '+00:00';

CREATE TABLE IF NOT EXISTS activity_logs (
  id            BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  event_id      CHAR(26)        NOT NULL,              -- ULID, time-sortable
  tenant_id     INT UNSIGNED    NOT NULL,
  user_id       BIGINT UNSIGNED NULL,
  action        VARCHAR(64)     NOT NULL,
  subject_type  VARCHAR(64)     NULL,
  subject_id    BIGINT UNSIGNED NULL,
  properties    JSON            NULL,
  ip_address    VARCHAR(45)     NULL,
  created_at    DATETIME(6)     NOT NULL,              -- UTC, always
  PRIMARY KEY (id, created_at),
  UNIQUE KEY uq_event (event_id, created_at),
  KEY idx_tenant_created (tenant_id, created_at),
  KEY idx_action_created (action, created_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
PARTITION BY RANGE COLUMNS (created_at) (
  PARTITION p2024m01 VALUES LESS THAN ('2024-02-01'),
  PARTITION p2024m02 VALUES LESS THAN ('2024-03-01'),
  PARTITION p2024m03 VALUES LESS THAN ('2024-04-01'),
  PARTITION p2024m04 VALUES LESS THAN ('2024-05-01'),
  PARTITION p2024m05 VALUES LESS THAN ('2024-06-01'),
  PARTITION p2024m06 VALUES LESS THAN ('2024-07-01'),
  PARTITION p2024m07 VALUES LESS THAN ('2024-08-01'),
  PARTITION p2024m08 VALUES LESS THAN ('2024-09-01'),
  PARTITION p2024m09 VALUES LESS THAN ('2024-10-01'),
  PARTITION p2024m10 VALUES LESS THAN ('2024-11-01'),
  PARTITION p2024m11 VALUES LESS THAN ('2024-12-01'),
  PARTITION p2024m12 VALUES LESS THAN ('2025-01-01'),
  PARTITION p2025m01 VALUES LESS THAN ('2025-02-01'),
  PARTITION p2025m02 VALUES LESS THAN ('2025-03-01'),
  PARTITION p2025m03 VALUES LESS THAN ('2025-04-01'),
  PARTITION p2025m04 VALUES LESS THAN ('2025-05-01'),
  PARTITION p2025m05 VALUES LESS THAN ('2025-06-01'),
  PARTITION p2025m06 VALUES LESS THAN ('2025-07-01'),
  PARTITION p2025m07 VALUES LESS THAN ('2025-08-01'),
  PARTITION p2025m08 VALUES LESS THAN ('2025-09-01'),
  PARTITION p2025m09 VALUES LESS THAN ('2025-10-01'),
  PARTITION p2025m10 VALUES LESS THAN ('2025-11-01'),
  PARTITION p2025m11 VALUES LESS THAN ('2025-12-01'),
  PARTITION p2025m12 VALUES LESS THAN ('2026-01-01'),
  PARTITION p2026m01 VALUES LESS THAN ('2026-02-01'),
  PARTITION p2026m02 VALUES LESS THAN ('2026-03-01'),
  PARTITION p2026m03 VALUES LESS THAN ('2026-04-01'),
  PARTITION p2026m04 VALUES LESS THAN ('2026-05-01'),
  PARTITION p2026m05 VALUES LESS THAN ('2026-06-01'),
  PARTITION p2026m06 VALUES LESS THAN ('2026-07-01'),
  PARTITION p2026m07 VALUES LESS THAN ('2026-08-01'),
  PARTITION p2026m08 VALUES LESS THAN ('2026-09-01'),
  PARTITION p2026m09 VALUES LESS THAN ('2026-10-01'),
  PARTITION p2026m10 VALUES LESS THAN ('2026-11-01'),
  PARTITION p2026m11 VALUES LESS THAN ('2026-12-01'),
  PARTITION p2026m12 VALUES LESS THAN ('2027-01-01'),
  PARTITION p_future VALUES LESS THAN (MAXVALUE)
);

-- ---------------------------------------------------------------------------
-- Transactional outbox. Written in the SAME transaction as the activity row,
-- so an event can never exist without an archive obligation.
-- Not partitioned: small, high-churn, fully drained.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS outbox_events (
  event_id    CHAR(26)     NOT NULL PRIMARY KEY,
  created_at  DATETIME(6)  NOT NULL,                   -- copy of the event time
  status      ENUM('pending','claimed','archived') NOT NULL DEFAULT 'pending',
  claimed_at  DATETIME(6)  NULL,
  archived_at DATETIME(6)  NULL,
  attempts    SMALLINT UNSIGNED NOT NULL DEFAULT 0,
  KEY idx_status_created (status, created_at),
  KEY idx_status_claimed (status, claimed_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- ---------------------------------------------------------------------------
-- One row per published archive object. This is the record that authorises
-- dropping a MySQL partition - never drop one just because a file exists.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS archive_manifests (
  object_key      VARCHAR(512) NOT NULL PRIMARY KEY,
  bucket          VARCHAR(128) NOT NULL,
  year            SMALLINT UNSIGNED NOT NULL,
  month           TINYINT UNSIGNED  NOT NULL,
  rows_written    BIGINT UNSIGNED   NOT NULL,
  bytes_written   BIGINT UNSIGNED   NOT NULL,
  min_created_at  DATETIME(6)  NOT NULL,
  max_created_at  DATETIME(6)  NOT NULL,
  schema_version  INT UNSIGNED NOT NULL,
  sha256          CHAR(64)     NOT NULL,
  verified_at     DATETIME(6)  NULL,                   -- set after read-back check
  created_at      DATETIME(6)  NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
  KEY idx_year_month (year, month)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Convenience view: what is still owed to the archive, by month.
CREATE OR REPLACE VIEW v_archive_backlog AS
SELECT YEAR(created_at)  AS year,
       MONTH(created_at) AS month,
       status,
       COUNT(*)          AS events,
       MIN(created_at)   AS min_created_at,
       MAX(created_at)   AS max_created_at
FROM outbox_events
WHERE status <> 'archived'
GROUP BY 1, 2, 3;
