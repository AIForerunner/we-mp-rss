import urllib.request
import json
import time

def run_test(token=None):
    url = "https://api.coze.cn/v1/workflow/stream_run"
    workflow_id = "7630469077471281204"
    app_id = "7630113285274877961"
    
    payload = {
        "workflow_id": workflow_id,
        "app_id": app_id,
        "parameters": {
            "url": "https://mp.weixin.qq.com/s/dJiIi3CP-iLmDrcqg_lSmA",
            "title": "鹅厂员工，最近看的一本书是什么？",
            "content": "TEST CONTENT",
            "account": "腾讯技术工程",
            "follow_avatar": "http://mmbiz.qpic.cn/avatar/...",
            "create_time": "1777003345"
        }
    }
    
    headers = {
        "Content-Type": "application/json"
    }
    if token:
        headers["Authorization"] = f"Bearer {token}"
        print(f"\n--- Testing WITH Token ({token[:6]}...{token[-4:]}) ---")
    else:
        print("\n--- Testing WITHOUT Token ---")
        
    req = urllib.request.Request(url, data=json.dumps(payload).encode('utf-8'), headers=headers, method='POST')
    
    try:
        with urllib.request.urlopen(req) as response:
            status = response.getcode()
            body = response.read(300).decode('utf-8')
            print(f"Status: {status}")
            print(f"Body (300 chars): {body}")
            try:
                # Stream response might not be pure JSON, but let's see
                json_data = json.loads(body)
                print(f"JSON Parsable: Yes")
                print(f"Code: {json_data.get('code')}, Msg: {json_data.get('msg')}")
            except:
                print(f"JSON Parsable: No (Stream response or error)")
    except urllib.error.HTTPError as e:
        status = e.code
        body = e.read(300).decode('utf-8')
        print(f"Status: {status}")
        print(f"Body (300 chars): {body}")
        try:
            json_data = json.loads(body)
            print(f"JSON Parsable: Yes")
            print(f"Code: {json_data.get('code')}, Msg: {json_data.get('msg')}")
        except:
            print(f"JSON Parsable: No")

token = "sat_iM6Ck3XV5PkLo3AfgV4i7kN8gjktSXHD4enIWecQnSJL5ljI9CbDb70s38eWyPaU"
run_test(None)
run_test(token)
