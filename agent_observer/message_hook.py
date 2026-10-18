"""Codex PostToolUse hook: forward send_message input only to loopback."""
import json
import os
import re
import tempfile
from datetime import datetime, timezone
from pathlib import Path
import sys
import urllib.request
import urllib.error


def encrypted_input(event):
    if not isinstance(event, dict):
        return False
    args = event.get('tool_input')
    message = args.get('message') if isinstance(args, dict) else None
    return isinstance(message, str) and re.fullmatch(r'gAAAAA[A-Za-z0-9_=-]{80,}', message) is not None


def capture_payload(event):
    if not isinstance(event, dict) or event.get('hook_event_name') != 'PostToolUse':
        return None
    if event.get('tool_name') not in ('send_message', 'collaboration.send_message', 'collaboration/send_message', 'collaborationsend_message'):
        return None
    args = event.get('tool_input')
    if not isinstance(args, dict) or not isinstance(args.get('message'), str):
        return None
    if len(args['message']) > 12000:
        return None
    if encrypted_input(event):
        return None
    return dict(sessionId=event.get('session_id'), callId=event.get('tool_use_id'),
                target=args.get('target'), body=args['message'])


def record_status(directory, status, event=None, http_status=None):
    """Record delivery diagnostics without message text or other tool inputs."""
    report = {'at': datetime.now(timezone.utc).isoformat(), 'status': status}
    if isinstance(event, dict):
        for source, target in (('hook_event_name', 'hookEvent'), ('tool_name', 'toolName'),
                               ('session_id', 'sessionId'), ('tool_use_id', 'callId')):
            value = event.get(source)
            if isinstance(value, str) and re.fullmatch(r'[A-Za-z0-9_./:-]{1,200}', value):
                report[target] = value
    if http_status is not None:
        report['httpStatus'] = http_status
    temporary = None
    try:
        directory = Path(directory)
        if directory.is_symlink():
            return
        directory.mkdir(mode=0o700, exist_ok=True)
        with tempfile.NamedTemporaryFile(mode='w', dir=directory, prefix='.hook-status-', delete=False) as sink:
            temporary = sink.name
            json.dump(report, sink)
        os.replace(temporary, directory / 'message-hook-status.json')
        temporary = None
    except OSError:
        pass
    finally:
        if temporary:
            try:
                os.unlink(temporary)
            except OSError:
                pass


def main():
    status_dir = Path(__file__).resolve().parents[1] / '.local'
    event = None
    try:
        raw = sys.stdin.read(100001)
        if len(raw) > 100000:
            record_status(status_dir, 'input-too-large')
            return
        event = json.loads(raw)
        payload = capture_payload(event)
        if payload is None:
            record_status(status_dir, 'encrypted-input' if encrypted_input(event) else 'ignored-input', event)
            return
        request = urllib.request.Request('http://127.0.0.1:8767/api/message-hooks',
            data=json.dumps(payload).encode(), headers={'Content-Type': 'application/json'}, method='POST')
        with urllib.request.urlopen(request, timeout=.8) as response:
            result = json.loads(response.read(1024))
            state = result.get('status') if isinstance(result, dict) else None
            record_status(status_dir, state if state in ('attached', 'pending') else 'unexpected-response', event, response.status)
    except urllib.error.HTTPError as error:
        record_status(status_dir, 'rejected', event, error.code)
        print(json.dumps({'systemMessage': 'Observer 본문 자동 연결을 완료하지 못했습니다. 메시지는 재전송하지 않습니다.'}))
    except (ValueError, OSError):
        record_status(status_dir, 'delivery-failed', event)
        # Observation must not block, rewrite, or resend the actual message.
        print(json.dumps({'systemMessage': 'Observer 본문 자동 연결을 완료하지 못했습니다. 메시지는 재전송하지 않습니다.'}))


if __name__ == '__main__':
    main()
