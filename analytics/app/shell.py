"""Interactive DuckDB shell, pre-wired to the archive.

    docker compose run --rm duckdb

A view named `activity` is created over the whole archive with Hive
partitioning on, so `year` and `month` are real, prunable columns:

    SELECT year, month, COUNT(*) FROM activity GROUP BY 1,2 ORDER BY 1,2;
    SELECT action, COUNT(*) FROM activity WHERE year = 2025 GROUP BY 1;

Meta commands: \\d <table>  describe    \\t  list tables/views    \\q  quit
Statements are terminated by a semicolon; blank input runs the buffer.
"""
import sys

from .clients import archive_glob, duck, read_archive
from .config import cfg

BANNER = f"""
DuckDB analytics shell
  archive : s3://{cfg.s3_bucket}/{cfg.archive_prefix}/
  view    : activity  (hive_partitioning + union_by_name)
  memory  : {cfg.duckdb_memory_limit}   threads: {cfg.duckdb_threads}
  \\t tables   \\d <name> describe   \\q quit
"""


def render(rel) -> None:
    rows = rel.fetchall()
    cols = [d[0] for d in rel.description]
    widths = [
        max(len(str(c)), *(len(str(r[i])) for r in rows)) if rows else len(str(c))
        for i, c in enumerate(cols)
    ]
    print("  ".join(str(c).ljust(w) for c, w in zip(cols, widths)))
    print("  ".join("-" * w for w in widths))
    for r in rows[:200]:
        print("  ".join(str(v).ljust(w) for v, w in zip(r, widths)))
    extra = f" (showing first 200)" if len(rows) > 200 else ""
    print(f"({len(rows)} rows){extra}")


def main() -> None:
    con = duck()
    con.execute(f"CREATE OR REPLACE VIEW activity AS SELECT * FROM {read_archive()}")
    print(BANNER)

    buf = []
    while True:
        try:
            line = input("duckdb> " if not buf else "   ...> ").strip()
        except (EOFError, KeyboardInterrupt):
            print()
            break

        if not buf and line in ("\\q", "quit", "exit"):
            break
        if not buf and line == "\\t":
            render(con.execute("SELECT table_name, table_type FROM information_schema.tables ORDER BY 1"))
            continue
        if not buf and line.startswith("\\d "):
            name = line[3:].strip().strip(";")
            try:
                render(con.execute(f"DESCRIBE SELECT * FROM {name}"))
            except Exception as exc:
                print(f"error: {exc}", file=sys.stderr)
            continue
        if not line and not buf:
            continue

        buf.append(line)
        if not line.endswith(";"):
            continue

        sql = " ".join(buf).rstrip(";")
        buf = []
        try:
            render(con.execute(sql))
        except Exception as exc:
            print(f"error: {exc}", file=sys.stderr)

    con.close()
    print("bye")


if __name__ == "__main__":
    main()
