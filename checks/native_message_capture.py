"""Verify one real native send; never count a mailbox or supplied body as success."""
import argparse
import json
from pathlib import Path
import sys
import urllib.error
import urllib.parse
import urllib.request


def verify(state, args, expected, hook):
    if state.get('sessionId') != args.session or state.get('turn') != args.turn:
        raise ValueError('Selected session or turn changed')
    nodes = [n for n in state.get('nodes', []) if n.get('id') == args.event_id]
    if len(nodes) != 1:
        raise ValueError('Exactly one selected native event must exist')
    node = nodes[0]
    if (node.get('kind'), node.get('source'), node.get('target')) != (
            'a2a', args.sender, args.recipient):
        raise ValueError('Event kind or endpoints do not match')
    origin = node.get('bodySource')
    exact = node.get('body') == expected
    automatic = origin in (None, 'native-hook')
    passed = exact and automatic
    report = {
        'sessionId': args.session, 'turn': args.turn, 'eventId': args.event_id,
        'nativeAutomaticBody': 'passed' if passed else 'failed',
        'bodyPresent': node.get('body') is not None, 'exactBodyMatch': exact,
        'bodySource': origin, 'bodyUnavailable': node.get('bodyUnavailable'),
        'separateBodyPublication': not automatic,
    }
    call_id = args.event_id.rsplit(':', 1)[-1]
    if hook:
        matched = (hook.get('sessionId'), hook.get('callId')) == (args.session, call_id)
        report['hookMatchesEvent'] = matched
        if matched:
            report['hookStatus'] = hook.get('status')
            report['hookAt'] = hook.get('at')
    return report, passed


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--session', required=True)
    parser.add_argument('--turn', required=True)
    parser.add_argument('--event-id', required=True)
    parser.add_argument('--sender', required=True)
    parser.add_argument('--recipient', default='agent')
    parser.add_argument('--port', type=int, default=8767)
    parser.add_argument('--hook-status', type=Path)
    args = parser.parse_args()
    if not args.event_id.startswith('a2a:') or not 1 <= args.port <= 65535:
        parser.error('A native a2a event and a valid loopback port are required')
    expected = sys.stdin.read(12001)
    if not expected or len(expected) > 12000:
        parser.error('Provide the exact nonempty probe body (at most 12000 characters) on stdin')
    try:
        query = urllib.parse.urlencode({'session': args.session})
        with urllib.request.urlopen(f'http://127.0.0.1:{args.port}/api/state?{query}', timeout=5) as response:
            raw = response.read(32 * 1024 * 1024 + 1)
        if len(raw) > 32 * 1024 * 1024:
            raise ValueError('Snapshot exceeds diagnostic limit')
        hook = json.loads(args.hook_status.read_text()) if args.hook_status else None
        report, passed = verify(json.loads(raw), args, expected, hook)
    except (OSError, ValueError, TypeError, AttributeError):
        # Raw responses, body text and credentials never enter diagnostic output.
        print(json.dumps({'nativeAutomaticBody': 'error', 'reason': 'Unable to validate the exact selected event'}))
        return 2
    print(json.dumps(report, ensure_ascii=False, indent=2))
    return 0 if passed else 1


if __name__ == '__main__':
    sys.exit(main())
