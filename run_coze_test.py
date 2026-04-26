import sqlite3
import yaml
import json
import requests
import re
import os

def resolve_env_vars(data):
    if isinstance(data, dict):
        return {k: resolve_env_vars(v) for k, v in data.items()}
    elif isinstance(data, list):
        return [resolve_env_vars(v) for v in data]
    elif isinstance(data, str):
        # Match ${VAR:-default} or ${VAR}
        pattern = re.compile(r'\$\{(?P<var>[^:-]+)(?::-(?P<default>[^}]*))?\}')
        def replace(match):
            var = match.group('var')
            default = match.group('default')
            return os.environ.get(var, default if default is not None else match.group(0))
        return pattern.sub(replace, data)
    return data

def run():
    try:
        with open('config.yaml', 'r') as f:
            raw_config = yaml.safe_load(f)
            config = resolve_env_vars(raw_config)
    except Exception as e:
        print(f"Error reading config.yaml: {e}")
        return

    wc_req = config.get('weekly_collector', {}).get('request', {})
    url = wc_req.get('url')
    api_token = wc_req.get('api_token')
    body_template_json = wc_req.get('body_template_json')
    headers = json.loads(wc_req.get('custom_headers_json', '{}'))

    if not url or not body_template_json:
        print("Missing url or body_template_json in config.yaml")
        return

    try:
        conn = sqlite3.connect('data/db.db')
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        query = "SELECT id, title, url, content, content_html, publish_time, pic_url FROM articles WHERE has_content = 1 LIMIT 1"
        row = cursor.execute(query).fetchone()
        conn.close()
    except Exception as e:
        print(f"Error reading database: {e}")
        return

    if not row:
        print("No articles with content found in database.")
        return

    article = dict(row)
    article['mp_name'] = "Unknown"
    article['mp_cover'] = article.get('pic_url', '')

    body_str = body_template_json
    body_str = body_str.replace("{{url}}", article['url'] or "")
    body_str = body_str.replace("{{title}}", article['title'] or "")
    content_escaped = (article['content'] or article['content_html'] or "").replace('\\', '\\\\').replace('"', '\\"').replace('\n', '\\n').replace('\r', '\\r').replace('\t', '\\t')
    body_str = body_str.replace("{{content}}", content_escaped)
    body_str = body_str.replace("{{account}}", article['mp_name'])
    body_str = body_str.replace("{{follow_avatar}}", article['mp_cover'])
    body_str = body_str.replace("{{create_time}}", str(article['publish_time']))

    try:
        body = json.loads(body_str)
    except Exception as e:
        # print(f"Error parsing body_template_json after replacement: {e}")
        # If content has issues, try a shorter version
        content_escaped = "Content truncated due to encoding issues."
        body_str = body_template_json
        body_str = body_str.replace("{{url}}", article['url'] or "")
        body_str = body_str.replace("{{title}}", article['title'] or "")
        body_str = body_str.replace("{{content}}", content_escaped)
        body_str = body_str.replace("{{account}}", article['mp_name'])
        body_str = body_str.replace("{{follow_avatar}}", article['mp_cover'])
        body_str = body_str.replace("{{create_time}}", str(article['publish_time']))
        body = json.loads(body_str)

    if api_token and 'Authorization' not in headers:
        headers['Authorization'] = f"Bearer {api_token}"

    try:
        response = requests.post(url, json=body, headers=headers, stream=True, timeout=120)
        
        print(f"article_id: {article['id']}")
        print(f"title: {article['title']}")
        print(f"http_status: {response.status_code}")

        content_type = response.headers.get('Content-Type', '')
        is_sse = 'text/event-stream' in content_type
        
        if is_sse:
            print("Response (SSE format, first 20 lines):")
            line_count = 0
            for line in response.iter_lines():
                if line_count >= 20:
                    break
                if line:
                    decoded_line = line.decode('utf-8')
                    if decoded_line.startswith('event:') or decoded_line.startswith('data:'):
                        print(decoded_line)
                        line_count += 1
        else:
            print("Response body (first 1500 chars):")
            print(response.text[:1500])

    except Exception as e:
        print(f"Request failed: {e}")

if __name__ == "__main__":
    run()
