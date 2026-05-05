#!/usr/bin/env python3
import argparse
import datetime as dt
import json
import os
import re
import sys
import sqlite3
import time
import threading
from typing import Any, Dict, List
from urllib import error, request

ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT_DIR not in sys.path:
    sys.path.insert(0, ROOT_DIR)

from core.content_format import format_content

# Global lock for synchronized printing
print_lock = threading.Lock()


DEFAULT_API_URL = "https://api.coze.cn/v1/workflow/stream_run"
DEFAULT_CONFIG_PATH = os.path.join(ROOT_DIR, "config.yaml")


def replace_env_vars(data: Any) -> Any:
    if isinstance(data, dict):
        return {key: replace_env_vars(value) for key, value in data.items()}
    if isinstance(data, list):
        return [replace_env_vars(item) for item in data]
    if isinstance(data, str):
        pattern = re.compile(r'\$\{([^}:]+)(?::-([^}]*))?\}')

        def replace_match(match: re.Match[str]) -> str:
            var_name = match.group(1)
            default_value = match.group(2)
            if default_value is not None:
                return os.getenv(var_name, default_value)
            return os.getenv(var_name, "")

        return pattern.sub(replace_match, data)
    return data


def load_request_config(config_path: str) -> Dict[str, Any]:
    if not os.path.exists(config_path):
        return {}

    try:
        import yaml
    except ImportError:
        return {}

    with open(config_path, "r", encoding="utf-8") as file:
        config = replace_env_vars(yaml.safe_load(file) or {})

    request_config = ((config.get("weekly_collector") or {}).get("request") or {})
    body_template_json = request_config.get("body_template_json") or ""
    body_template = {}
    if body_template_json:
        try:
            body_template = json.loads(body_template_json)
        except Exception:
            body_template = {}

    return {
        "api_url": request_config.get("url") or DEFAULT_API_URL,
        "api_token": request_config.get("api_token") or "",
        "timeout": int(request_config.get("timeout") or 60),
        "workflow_id": body_template.get("workflow_id") or "",
        "app_id": body_template.get("app_id") or "",
    }


def parse_args() -> argparse.Namespace:
    config_defaults = load_request_config(DEFAULT_CONFIG_PATH)
    parser = argparse.ArgumentParser(
        description="Send collected articles to Coze workflow endpoint."
    )
    parser.add_argument("--config", default=DEFAULT_CONFIG_PATH, help="Path to config.yaml")
    parser.add_argument("--db", default="data/db.db", help="Path to sqlite db file")
    parser.add_argument("--api-url", default=config_defaults.get("api_url") or DEFAULT_API_URL, help="Coze API url")
    parser.add_argument("--api-token", default=os.getenv("COZE_API_TOKEN", "") or config_defaults.get("api_token", ""), help="Coze API token")
    parser.add_argument("--workflow-id", default=os.getenv("COZE_WORKFLOW_ID", "") or config_defaults.get("workflow_id", ""), help="Coze workflow id")
    parser.add_argument("--app-id", default=os.getenv("COZE_APP_ID", "") or config_defaults.get("app_id", ""), help="Coze app id")
    parser.add_argument("--limit", type=int, default=100, help="Max number of articles to send")
    parser.add_argument("--offset", type=int, default=0, help="Offset for paging")
    parser.add_argument("--timeout", type=int, default=config_defaults.get("timeout") or 60, help="HTTP timeout in seconds")
    parser.add_argument("--retry", type=int, default=2, help="Retry times for each article")
    parser.add_argument("--mark-exported", action="store_true", help="Mark article as exported on success")
    parser.add_argument("--include-exported", action="store_true", help="Include rows where is_export = 1")
    parser.add_argument("--article-id", default="", help="Only send one specific article id")
    parser.add_argument("--publish-time-start", default="", help="Inclusive publish time start, format: YYYY-MM-DD or YYYY-MM-DD HH:MM:SS")
    parser.add_argument("--publish-time-end", default="", help="Inclusive publish time end, format: YYYY-MM-DD or YYYY-MM-DD HH:MM:SS")
    parser.add_argument("--dry-run", action="store_true", help="Only print what would be sent")
    return parser.parse_args()


def parse_publish_time(value: str, end_of_day: bool = False) -> int:
    text = (value or "").strip()
    if not text:
        return 0

    formats = ["%Y-%m-%d %H:%M:%S", "%Y-%m-%d"]
    parsed = None
    for time_format in formats:
        try:
            parsed = dt.datetime.strptime(text, time_format)
            break
        except ValueError:
            continue

    if parsed is None:
        raise ValueError(f"Invalid publish time format: {value}")

    if len(text) == 10 and end_of_day:
        parsed = parsed.replace(hour=23, minute=59, second=59)

    return int(parsed.timestamp())


def fetch_articles(
    conn: sqlite3.Connection,
    limit: int,
    offset: int,
    include_exported: bool,
    article_id: str,
    publish_time_start: int,
    publish_time_end: int,
) -> List[sqlite3.Row]:
    where_parts = [
        "a.status != 1000",
        "a.has_content = 1",
        "(COALESCE(a.content, '') != '' OR COALESCE(a.content_html, '') != '')",
    ]
    params: List[Any] = []
    if not include_exported:
        where_parts.append("COALESCE(a.is_export, 0) != 1")
    if article_id:
        where_parts.append("a.id = ?")
        params.append(article_id)
    if publish_time_start:
        where_parts.append("a.publish_time >= ?")
        params.append(publish_time_start)
    if publish_time_end:
        where_parts.append("a.publish_time <= ?")
        params.append(publish_time_end)

    sql = f"""
    SELECT
        a.id,
        a.mp_id,
        a.title,
        a.url,
        a.content,
        a.content_html,
        a.publish_time,
        f.mp_name,
        f.mp_cover
    FROM articles a
    LEFT JOIN feeds f ON f.id = a.mp_id
    WHERE {' AND '.join(where_parts)}
    ORDER BY a.publish_time DESC
    LIMIT ? OFFSET ?
    """
    params.extend([limit, offset])
    return conn.execute(sql, tuple(params)).fetchall()


def build_payload(row: sqlite3.Row, workflow_id: str, app_id: str) -> Dict[str, Any]:
    html_content = row["content_html"] or row["content"] or ""
    content = format_content(html_content, "markdown")
    account = row["mp_name"] or row["mp_id"] or ""
    publish_time_sec = int(row["publish_time"] or 0)
    create_time = (
        dt.datetime.fromtimestamp(publish_time_sec).strftime("%Y-%m-%d %H:%M:%S")
        if publish_time_sec > 0
        else ""
    )
    return {
        "workflow_id": workflow_id,
        "app_id": app_id,
        "parameters": {
            "url": row["url"] or "",
            "title": row["title"] or "",
            "content": content,
            "account": account,
            "follow_avatar": row["mp_cover"] or "",
            "create_time": create_time,
        },
    }


def send_payload(
    api_url: str,
    api_token: str,
    payload: Dict[str, Any],
    timeout: int,
) -> Dict[str, Any]:
    headers = {"Content-Type": "application/json"}
    if api_token:
        headers["Authorization"] = f"Bearer {api_token}"

    data = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    req = request.Request(api_url, data=data, headers=headers, method="POST")
    with request.urlopen(req, timeout=timeout) as resp:
        return {
            "status": resp.status,
        }


def mark_exported(conn: sqlite3.Connection, article_id: str) -> None:
    conn.execute("UPDATE articles SET is_export = 1 WHERE id = ?", (article_id,))


def send_article_async(
    idx: int,
    total: int,
    row: sqlite3.Row,
    workflow_id: str,
    app_id: str,
    api_url: str,
    api_token: str,
    timeout: int,
    db_path: str,
    mark_exported: bool,
) -> None:
    payload = build_payload(row, workflow_id, app_id)
    try:
        send_payload(api_url, api_token, payload, timeout)
        if mark_exported:
            conn = sqlite3.connect(db_path)
            conn.execute("UPDATE articles SET is_export = 1 WHERE id = ?", (row["id"],))
            conn.commit()
            conn.close()
        with print_lock:
            print(f"[SENT] {idx}/{total} id={row['id']} title={row['title']}", flush=True)
    except error.HTTPError as e:
        with print_lock:
            print(f"[FAIL] {idx}/{total} id={row['id']} title={row['title']} reason=HTTPError {e.code}", flush=True)
    except Exception as e:  # noqa: BLE001
        with print_lock:
            print(f"[FAIL] {idx}/{total} id={row['id']} title={row['title']} reason={e}", flush=True)


def main() -> int:
    args = parse_args()

    if args.config != DEFAULT_CONFIG_PATH:
        config_defaults = load_request_config(args.config)
        if not args.api_url and config_defaults.get("api_url"):
            args.api_url = config_defaults["api_url"]
        if not args.api_token and config_defaults.get("api_token"):
            args.api_token = config_defaults["api_token"]
        if not args.workflow_id and config_defaults.get("workflow_id"):
            args.workflow_id = config_defaults["workflow_id"]
        if not args.app_id and config_defaults.get("app_id"):
            args.app_id = config_defaults["app_id"]
        if args.timeout == 60 and config_defaults.get("timeout"):
            args.timeout = config_defaults["timeout"]

    if not args.workflow_id or not args.app_id:
        print("ERROR: workflow-id and app-id are required.")
        return 2

    try:
        publish_time_start = parse_publish_time(args.publish_time_start, end_of_day=False)
        publish_time_end = parse_publish_time(args.publish_time_end, end_of_day=True)
    except ValueError as exc:
        print(f"ERROR: {exc}")
        return 2

    if not os.path.exists(args.db):
        print(f"ERROR: db file not found: {args.db}")
        return 2

    conn = sqlite3.connect(args.db)
    conn.row_factory = sqlite3.Row

    rows = fetch_articles(
        conn,
        args.limit,
        args.offset,
        args.include_exported,
        args.article_id,
        publish_time_start,
        publish_time_end,
    )
    if not rows:
        print("No articles to send.")
        conn.close()
        return 0

    print(
        f"Start sending total={len(rows)} offset={args.offset} limit={args.limit} "
        f"article_id={args.article_id or '-'} publish_time_start={args.publish_time_start or '-'} "
        f"publish_time_end={args.publish_time_end or '-'} mark_exported={args.mark_exported}",
        flush=True,
    )

    threads = []
    for idx, row in enumerate(rows, 1):
        if args.dry_run:
            with print_lock:
                print(f"[DRY RUN] {idx}/{len(rows)} id={row['id']} title={row['title']}", flush=True)
            time.sleep(1)
            continue

        with print_lock:
            dispatch_at = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            print(f"[DISPATCH] {idx}/{len(rows)} at={dispatch_at} id={row['id']} title={row['title']}", flush=True)

        thread = threading.Thread(
            target=send_article_async,
            args=(
                idx,
                len(rows),
                row,
                args.workflow_id,
                args.app_id,
                args.api_url,
                args.api_token,
                args.timeout,
                args.db,
                args.mark_exported,
            ),
            daemon=False,
        )
        thread.start()
        threads.append(thread)
        
        # Sleep 2 seconds before sending next article
        if idx < len(rows):
            time.sleep(2)

    for thread in threads:
        thread.join()

    conn.close()

    print(f"Done. total={len(rows)}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
