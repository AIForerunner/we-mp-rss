#!/usr/bin/env python3
import argparse
import os
import signal
import sqlite3
import sys
import time
from datetime import datetime


def process_exists(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except OSError:
        return False
    return True


def get_counts(db_path: str) -> tuple[int, int]:
    conn = sqlite3.connect(db_path)
    try:
        cur = conn.cursor()
        cur.execute("select count(*) from feeds where id!='MP_WXS_FEATURED_ARTICLES'")
        feeds_total = cur.fetchone()[0]
        cur.execute(
            "select count(*) from feeds f "
            "where f.id!='MP_WXS_FEATURED_ARTICLES' "
            "and not exists (select 1 from articles a where a.mp_id=f.id)"
        )
        feeds_no_article = cur.fetchone()[0]
        return feeds_total, feeds_no_article
    finally:
        conn.close()


def log_line(message: str) -> None:
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{ts}] {message}", flush=True)


def main() -> int:
    parser = argparse.ArgumentParser(description="Watch backfill progress")
    parser.add_argument("pid", type=int, help="PID of the backfill worker")
    parser.add_argument("--db", default="data/db.db", help="SQLite db path")
    parser.add_argument("--interval", type=int, default=30, help="Polling interval in seconds")
    args = parser.parse_args()

    if not os.path.exists(args.db):
        log_line(f"ERROR db not found: {args.db}")
        return 1

    last_no_article = None
    log_line(f"WATCH start pid={args.pid} db={args.db} interval={args.interval}s")

    while True:
        feeds_total, feeds_no_article = get_counts(args.db)
        status = "RUNNING" if process_exists(args.pid) else "DONE"

        if last_no_article is None:
            delta = "init"
        else:
            delta_value = last_no_article - feeds_no_article
            delta = f"delta={delta_value:+d}"

        log_line(
            f"status={status} feeds_total={feeds_total} feeds_no_article={feeds_no_article} {delta}"
        )
        last_no_article = feeds_no_article

        if status == "DONE":
            break

        time.sleep(args.interval)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())