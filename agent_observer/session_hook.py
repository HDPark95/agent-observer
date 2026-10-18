"""Register the exact transcript supplied by a trusted Codex lifecycle hook."""
import json
import sys
import urllib.request


def registration(event):
    if not isinstance(event, dict) or event.get('hook_event_name') not in ('SessionStart', 'UserPromptSubmit'):
        return None
    if event.get('hook_event_name') == 'SessionStart' and event.get('source') == 'fork':
        return None
    path, sid = event.get('transcript_path'), event.get('session_id')
    if not isinstance(path, str) or not isinstance(sid, str):
        return None
    return {'source':path, 'sessionId':sid, 'metadataOnly':True}


def main():
    try:
        raw = sys.stdin.read(100001)
        if len(raw) > 100000:
            return
        payload = registration(json.loads(raw))
        if not payload:
            return
        request = urllib.request.Request('http://127.0.0.1:8767/api/sessions',
            data=json.dumps(payload).encode(), headers={'Content-Type':'application/json'}, method='POST')
        with urllib.request.urlopen(request, timeout=.8) as response:
            response.read(1024)
    except (ValueError, OSError):
        print(json.dumps({'systemMessage':'Observer 세션 자동 등록을 완료하지 못했습니다. 수동 세션 등록을 사용할 수 있습니다.'}))


if __name__ == '__main__':
    main()
