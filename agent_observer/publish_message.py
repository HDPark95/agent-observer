"""After send_message, attach the same stdin text to its unique local event."""
import argparse
import json
import sys
import time
import urllib.request
import urllib.parse


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--session', help='Required when observing multiple sessions')
    parser.add_argument('--sender', required=True)
    parser.add_argument('--target', required=True)
    parser.add_argument('--event-id', help='Required if multiple unmatched calls exist')
    parser.add_argument('--port', type=int, default=8767)
    args = parser.parse_args()
    body = sys.stdin.read(12001)
    if len(body) > 12000:
        parser.error('본문은 12000자 이하여야 합니다.')
    base = f'http://127.0.0.1:{args.port}'
    identity = lambda path: 'agent' if path == '/root' else 'subagent:' + path
    source, target = identity(args.sender), identity(args.target)
    if not args.session:
        with urllib.request.urlopen(base + '/api/sessions', timeout=3) as response:
            if len(json.load(response)['sessions']) != 1:
                parser.error('여러 세션 중 --session으로 지정하세요.')
    initial_turn = None
    for _ in range(20):
        with urllib.request.urlopen(base + '/api/state' + ('?session=' + urllib.parse.quote(args.session) if args.session else ''), timeout=3) as response:
            state = json.load(response)
        if initial_turn is None:
            initial_turn = state['turn']
        if initial_turn != state['turn']:
            parser.error('턴이 바뀌어 연결을 중단합니다.')
        candidates = [n for n in state['nodes'] if n['kind'] == 'a2a'
                      and n.get('source') == source and n.get('target') == target
                      and (n['id'] == args.event_id if args.event_id else n.get('bodySource') != 'sender-local')]
        if len(candidates) > 1:
            parser.error('송신 기록이 여러 개입니다. --event-id로 지정하세요.')
        if candidates:
            payload = dict(sessionId=state.get('sessionId'), turn=initial_turn, eventId=candidates[0]['id'], source=source, target=target, body=body)
            request = urllib.request.Request(base + '/api/messages', data=json.dumps(payload).encode(),
                                             headers={'Content-Type': 'application/json'}, method='POST')
            with urllib.request.urlopen(request, timeout=3) as response:
                print(response.read().decode())
            return
        time.sleep(.5)
    parser.error('제한 시간 내 실제 송신 기록을 찾지 못했습니다.')


if __name__ == '__main__':
    main()
