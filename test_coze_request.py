import sqlite3
import yaml
import json
import requests
import sys

def run():
    # 1. Read config.yaml
    with open('config.yaml', 'r') as f:
        config = yaml.safe_load(f)
    
    wc_req = config.get('weekly_collector', {}).get('request', {})
    url = wc_req.get('url')
    api_token = wc_req.get('api_token')
    body_template_json = wc_req.get('body_template_json')
    headers = json.loads(wc_req.get('custom_headers_json', '{}'))

    if not url or not body_template_json:
        print("Missing url or body_template_json in config.yaml")
        return

    # 2. Extract article from data/db.db
    conn = sqlite3.connect('data/db.db')
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    # Using fields: id, title, url, content, content_html, publish_time, pic_url (as mp_cover)
    # mp_name is not in articles table directly, maybe in extinfo or another table. 
    # Let's try to get it from extinfo or just use placeholder.
    query = "SELECT id, title, url, content, content_html, publish_time, pic_url, extinfo FROM articles WHERE has_content = 1 LIMIT 1"
    row = cursor.execute(query).fetchone()
    conn.close()

    if not row:
        print("No articles with content found in database.")
        return

    article = dict(row)
    # Basic mp_name extraction if possible
    article['mp_name'] = "Unknown"
    article['mp_cover'] = article.get('pic_url', '')

    # 3. Fill body_template_json
    # Template placeholders: {{url}} {{title}} {{content}} {{account}} {{follow_avatar}} {{create_time}}
    body_str = body_template_json
    body_str = body_str.replace("{{url}}", article['url'] or "")
    body_str = body_str.replace("{{title}}", article['title'] or "")
    body_str = body_str.replace("{{content}}", (article['content'] or article['content_html'] or "").replace('"', '\\"').replace('\n', '\\n'))
    body_str = body_str.replace("{{account}}", article['mp_name'])
    body_str = body_str.replace("{{follow_avatar}}", article['mp_cover'])
    body_str = body_str.replace("{{create_time}}", str(article['publish_time']))

    try:
        body = json.loads(body_str)
    except Exception as e:
        print(f"Error parsing body_template_json after replacement: {e}")
        # Fallback: simple manual build if template replacement failed
        body = {
            "workflow_id": "7630469077471281204",
            "parameters": {
                "url": article['url'],
                "title": article['title'],
                "content": article['content']
            }
        }

    # 4. Handle Authentication
    if api_token and 'Authorization' not in headers:
        headers['Authorization'] = f"Bearer {api_token}"
    
    has_auth = 'yes' if 'Authorization' in headers else 'no'

    # 5. Make POST request
    try:
        response = requests.post(url, json=body, headers=headers, stream=True, timeout=60)
        status_code = response.status_code
        
        print(f"article_id: {article['id']}")
        print(f"title: {article['title']}")
        print(f"请求URL: {url}")
        print(f"是否带Authorization: {has_auth}")
        print(f"HTTP状态码: {status_code}")

        # 6. Handle SSE/stream or normal response
        content_preview = ""
        is_sse = 'text/event-stream' in response.headers.get('Content-Type', '')
        
        if is_sse:
            print("Response is SSE/stream. Key segments:")
            count = 0
            for line in response.iter_lines():
                if line:
                    decoded_line = line.decode('utf-8')
                    if decoded_line.startswith('data:'):
                        data_part = decoded_line[5:].strip()
                        try:
                            data_json = json.loads(data_part)
                            # Look for error fields or result fields
                            if 'msg' in data_json or 'error' in data_json or 'code' in data_json:
                                print(f"  {data_part[:200]}")
                        except:
                            pass
                    if count < 5:
                         content_preview += decoded_line + "\n"
                    count += 1
            print(f"原始响应前1000字符 (Preview):\n{content_preview[:1000]}")
        else:
            content_preview = response.text[:1000]
            print(f"原始响应前1000字符:\n{content_preview}")

    except Exception as e:
        print(f"Request failed: {e}")

if __name__ == "__main__":
    run()
