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
from typing import Any, Dict, List, Tuple
from urllib import error, request
from queue import Queue

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
    return {
        "workflow_id": workflow_id,
        "app_id": app_id,
        "parameters": {
            "url": row["url"] or "",
            "title": row["title"] or "",
            "content": content,
            "account": account,
            "follow_avatar": row["mp_cover"] or "",
            "create_time": str(row["publish_time"] or ""),
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
        body = resp.read().decode("utf-8", errors="replace")
        return {
            "status": resp.status,
            "body": body,
        }

def _evaluate_response(status: int, body: str) -> Tuple[bool, str]:
    if not (200 <= status < 300):
        return False, f"HTTP {status}: {body[:300]}"

    content = (body or "").strip()
    if not content:
        return True, ""

    # 1) Non-stream JSON response, usually includes code/msg.
    try:
        payload = json.loads(content)
        if isinstance(payload, dict):
            code = payload.get("code")
            if code is not None and code != 0:
                return False, f"code={code}, msg={payload.get('msg', '')}"
            event = str(payload.get("event", "")).lower()
            if event == "error":
                return False, f"event=Error, data={payload.get('data', '')}"
        return True, ""
    except Exception:
        pass

    # 2) Stream response (SSE), check for event/data pairs.
    current_event = ""
    saw_error = ""
    saw_any_data = False
    for line in content.splitlines():
        stripped = line.strip()
        if stripped.lower().startswith("event:"):
            current_event = stripped.split(":", 1)[1].strip()
            if current_event.lower() == "error":
                saw_error = "event: Error"
        elif stripped.lower().startswith("data:"):
            saw_any_data = True
            data_raw = stripped.split(":", 1)[1].strip()
            if current_event.lower() == "error":
                try:
                    err = json.loads(data_raw)
                    msg = err.get("error_message") or err.get("msg") or data_raw
                except Exception:
                    msg = data_raw
                return False, f"event=Error, {msg[:300]}"

    if saw_error:
        return False, saw_error

    # If stream has data and no explicit error event, treat as success.
    if saw_any_data:
        return True, ""

    # Fallback for unknown but non-empty body without explicit error markers.
    if "event: Error" in content or '"event":"Error"' in content:
        return False, content[:300]
    return True, ""


def mark_exported(conn: sqlite3.Connection, article_id: str) -> None:
    conn.execute("UPDATE articles SET is_export = 1 WHERE id = ?", (article_id,))


def send_article_async(
    idx: int,
    row: sqlite3.Row,
    workflow_id: str,
    app_id: str,
    api_url: str,
    api_token: str,
    timeout: int,
    retry: int,
    db_path: str,
    mark_exported: bool,
    total: int,
) -> None:
    """Send a single article in a thread."""
    payload = build_payload(row, workflow_id, app_id)
    
    attempt = 0
    sent = False
    last_error = ""
    
    while attempt <= retry and not sent:
        attempt += 1
        try:
            result = send_payload(api_url, api_token, payload, timeout)
            status = result["status"]
            ok_resp, reason = _evaluate_response(status, result.get("body", ""))
            if ok_resp:
                sent = True
                break
            last_error = reason
        except error.HTTPError as e:
            body = e.read().decode("utf-8", errors="replace")
            last_error = f"HTTPError {e.code}: {body[:300]}"
        except Exception as e:  # noqa: BLE001
            last_error = str(e)
        
        if not sent and attempt <= retry:
            time.sleep(1.5)
    
    if sent:
        if mark_exported:
            conn = sqlite3.connect(db_path)
            conn.execute("UPDATE articles SET is_export = 1 WHERE id = ?", (row["id"],))
            conn.commit()
            conn.close()
        with print_lock:
            print(f"[OK] {idx}/{total} id={row['id']} title={row['title']}", flush=True)
    else:
        with print_lock:
            print(f"[FAIL] {idx}/{total} id={row['id']} title={row['title']} reason={last_error}", flush=True)


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
        
        # Launch async thread for this article
        thread = threading.Thread(
            target=send_article_async,
            args=(
                idx,
                row,
                args.workflow_id,
                args.app_id,
                args.api_url,
                args.api_token,
                args.timeout,
                args.retry,
                args.db,
                args.mark_exported,
                len(rows),
            ),
            daemon=False,
        )
        thread.start()
        threads.append(thread)
        
        # Sleep 2 seconds before sending next article
        if idx < len(rows):
            time.sleep(2)
    
    # Wait for all threads to complete
    for thread in threads:
        thread.join()
    
    conn.close()

    print(f"Done. total={len(rows)}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
