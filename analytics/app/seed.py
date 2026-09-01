"""Generate test activity across a date range.

    docker compose run --rm dispatcher python -m app.seed --events 200000 \
        --from 2024-01-01 --to 2026-08-31 --tenants 5

The activity row and its outbox row are written in ONE transaction. That is the
outbox pattern's entire guarantee: an event cannot exist without an archive
obligation, and an obligation cannot exist without an event.
"""
import argparse
import json
import random
import secrets
import time
from datetime import datetime, timedelta, timezone

from .clients import mysql_conn, setup_logging

log = setup_logging("seed")

CROCKFORD = "0123456789ABCDEFGHJKMNPQRSTVWXYZ"

ACTIONS = [
    ("contact.created", "Contact"),
    ("contact.updated", "Contact"),
    ("contact.deleted", "Contact"),
    ("invoice.created", "Invoice"),
    ("invoice.paid", "Invoice"),
    ("user.login", "User"),
    ("user.logout", "User"),
    ("report.exported", "Report"),
    ("settings.changed", "Setting"),
]

FIRST = ["John", "Ayesha", "Marco", "Yuki", "Fatima", "Lars", "Priya", "Diego"]
LAST = ["Smith", "Rahman", "Rossi", "Tanaka", "Haddad", "Nilsson", "Iyer", "Silva"]


def ulid(ts: datetime) -> str:
    """Time-sortable 26-char id. Real deployments should use a proper ULID lib."""
    ms = int(ts.timestamp() * 1000)
    out = []
    for _ in range(10):
        out.append(CROCKFORD[ms & 31])
        ms >>= 5
    head = "".join(reversed(out))
    tail = "".join(CROCKFORD[secrets.randbelow(32)] for _ in range(16))
    return head + tail


def make_event(ts: datetime, tenants: int, users: int) -> tuple:
    action, subject_type = random.choice(ACTIONS)
    name = f"{random.choice(FIRST)} {random.choice(LAST)}"
    props = {"old": {"name": name}, "new": {"name": name + " Jr"}} \
        if action.endswith(".updated") else {"source": random.choice(["web", "api", "cli"])}
    return (
        ulid(ts),
        random.randint(1, tenants),
        random.randint(1, users),
        action,
        subject_type,
        random.randint(1, 500_000),
        json.dumps(props),
        f"10.10.{random.randint(0, 255)}.{random.randint(1, 254)}",
        ts.replace(tzinfo=None),      # column is DATETIME(6); session is UTC
    )


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--events", type=int, default=100_000)
    ap.add_argument("--from", dest="date_from", default="2024-01-01")
    ap.add_argument("--to", dest="date_to", default=None, help="default: today")
    ap.add_argument("--tenants", type=int, default=5)
    ap.add_argument("--users", type=int, default=200)
    ap.add_argument("--chunk", type=int, default=5_000)
    args = ap.parse_args()

    start = datetime.fromisoformat(args.date_from).replace(tzinfo=timezone.utc)
    end = (
        datetime.fromisoformat(args.date_to).replace(tzinfo=timezone.utc)
        if args.date_to
        else datetime.now(timezone.utc)
    )
    if end <= start:
        raise SystemExit("--to must be after --from")
    span = int((end - start).total_seconds())

    conn = mysql_conn()
    written = 0
    t0 = time.time()
    try:
        while written < args.events:
            n = min(args.chunk, args.events - written)
            rows = [
                make_event(start + timedelta(seconds=random.randint(0, span)),
                           args.tenants, args.users)
                for _ in range(n)
            ]
            rows.sort(key=lambda r: r[-1])
            with conn.cursor() as cur:
                cur.executemany(
                    """INSERT INTO activity_logs
                         (event_id, tenant_id, user_id, action, subject_type,
                          subject_id, properties, ip_address, created_at)
                       VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
                    rows,
                )
                cur.executemany(
                    "INSERT INTO outbox_events (event_id, created_at) VALUES (%s, %s)",
                    [(r[0], r[-1]) for r in rows],
                )
            conn.commit()          # both tables, one transaction
            written += n
            log.info("seeded %s/%s", written, args.events)
    finally:
        conn.close()

    log.info("done: %s events in %.1fs", written, time.time() - t0)


if __name__ == "__main__":
    main()
