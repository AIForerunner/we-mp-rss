#!/usr/bin/env python3
import argparse
import os
import random
import sqlite3
import time
from datetime import datetime

from core.db import DB
from core.models.feed import Feed
from core.models.article import Article
from core.wx import WxGather
from jobs.article import UpdateArticle


def process_exists(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except OSError:
        return False
    return True


def log(message: str) -> None:
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{ts}] {message}", flush=True)


def load_target_names(csv_path: str) -> list[str]:
    with open(csv_path, "r", encoding="utf-8") as file_obj:
        return [line.strip() for line in file_obj if line.strip()]


def find_candidate_feed_ids(db_path: str, names: list[str], cutoff_ts: int) -> list[str]:
    conn = sqlite3.connect(db_path)
    try:
        cur = conn.cursor()
        placeholders = ",".join("?" for _ in names)
        sql = (
            "select f.id, f.mp_name, min(a.publish_time) as min_publish_time, count(a.id) as article_count "
            "from feeds f "
            "join articles a on a.mp_id = f.id "
            f"where f.id != 'MP_WXS_FEATURED_ARTICLES' and f.mp_name in ({placeholders}) "
            "group by f.id, f.mp_name "
            "having article_count > 0 and min_publish_time >= ? "
            "order by f.mp_name"
        )
        rows = cur.execute(sql, [*names, cutoff_ts]).fetchall()
        for feed_id, mp_name, min_publish_time, article_count in rows:
            log(
                f"CANDIDATE mp_name={mp_name} feed_id={feed_id} article_count={article_count} min_publish_time={min_publish_time}"
            )
        return [row[0] for row in rows]
    finally:
        conn.close()


def main() -> int:
    parser = argparse.ArgumentParser(description="Backfill second page after first pass completes")
    parser.add_argument("wait_pid", type=int, help="PID to wait for before starting")
    parser.add_argument("--csv", default="refactor/wechat_account_list.csv", help="Target CSV path")
    parser.add_argument("--db", default="data/db.db", help="SQLite db path")
    parser.add_argument("--cutoff", default="2026-04-20", help="Earliest desired publish date, format YYYY-MM-DD")
    parser.add_argument("--poll-interval", type=int, default=20, help="Wait interval in seconds")
    args = parser.parse_args()

    cutoff_ts = int(datetime.strptime(args.cutoff, "%Y-%m-%d").timestamp())
    log(f"WAIT start pid={args.wait_pid} cutoff={args.cutoff} cutoff_ts={cutoff_ts}")

    while process_exists(args.wait_pid):
        log(f"WAIT pid={args.wait_pid} still running")
        time.sleep(args.poll_interval)

    log(f"WAIT pid={args.wait_pid} completed, start selecting candidates")
    names = load_target_names(args.csv)
    candidate_ids = find_candidate_feed_ids(args.db, names, cutoff_ts)
    log(f"CANDIDATE total={len(candidate_ids)}")

    session = DB.get_session()
    try:
        ok = 0
        no_new = 0
        fail = 0

        for index, feed_id in enumerate(candidate_ids, 1):
            feed = session.query(Feed).filter(Feed.id == feed_id).first()
            if feed is None:
                continue

            try:
                before = session.query(Article).filter(Article.mp_id == feed.id).count()
                log(f"[{index}/{len(candidate_ids)}] PAGE2 mp_name={feed.mp_name} feed_id={feed.id}")
                wx = WxGather().Model()
                wx.get_Articles(
                    feed.faker_id,
                    CallBack=UpdateArticle,
                    Mps_id=feed.id,
                    Mps_title=feed.mp_name,
                    start_page=1,
                    MaxPage=2,
                    interval=3,
                )
                after = session.query(Article).filter(Article.mp_id == feed.id).count()
                if after > before:
                    log(f"  OK added={after - before}")
                    ok += 1
                else:
                    log("  NO_NEW")
                    no_new += 1
                sleep_seconds = random.uniform(2.5, 5.0)
                log(f"  Sleeping {sleep_seconds:.1f}s")
                time.sleep(sleep_seconds)
            except Exception as err:
                fail += 1
                message = str(err)
                log(f"  FAIL {feed.mp_name}: {message}")
                if "frequency" in message.lower() or "frequencey control" in message.lower():
                    sleep_seconds = random.uniform(20.0, 40.0)
                    log(f"  COOL_DOWN {sleep_seconds:.1f}s")
                    time.sleep(sleep_seconds)
                else:
                    time.sleep(5)

        log(f"DONE ok={ok} no_new={no_new} fail={fail}")
    finally:
        session.close()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())