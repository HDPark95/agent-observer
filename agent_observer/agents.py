"""Read local A2A messages from explicitly selected agent transcripts."""
import json
import re
from pathlib import PurePosixPath


def encrypted_message(value):
    return isinstance(value, str) and re.fullmatch(r"gAAAAA[A-Za-z0-9_=-]{80,}", value) is not None


def message_body(value):
    """Bound local display text and redact common credential formats."""
    if not isinstance(value, str) or encrypted_message(value):
        return None
    value = re.sub(r'(?i)\b(authorization\s*[:=]\s*bearer\s+|(?:api[_-]?key|token|password|secret)\s*[:=]\s*)[^\s,;]+', r'\1[가림]', value)
    value = re.sub(r'\b(?:sk-[A-Za-z0-9_-]{16,}|gh[pousr]_[A-Za-z0-9_]{16,}|xox[baprs]-[A-Za-z0-9-]+)\b', '[가림]', value)
    return value[:12000] + ('\n[긴 메시지 생략]' if len(value) > 12000 else '')


def agent_path(value):
    if isinstance(value, str) and re.fullmatch(r'/root(?:/[A-Za-z0-9_-]+)*', value):
        return value
    return None


def agent_id(path):
    return 'agent' if path == '/root' else 'subagent:' + path


class AgentMessages:
    def __init__(self, source, expected=None):
        self.source = source
        self.expected = expected
        self.identity = None
        self.reset()

    def reset(self):
        self.offset = 0
        self.path = None
        self.thread = None
        self.floor = None
        self.calls = {}
        self.activities = {}
        self.first = True

    def consume(self, row):
        p = row.get('payload', {})
        if self.first:
            self.first = False
            if row.get('type') == 'session_meta':
                self.path = agent_path(p.get('agent_path'))
                self.thread = p.get('id')
                self.floor = p.get('subagent_history_start_ordinal')
                if self.expected and (self.thread, self.path, p.get('parent_thread_id')) != self.expected:
                    self.path = None
            return
        ordinal = row.get('ordinal')
        if not self.path or not isinstance(self.floor, int) or not isinstance(ordinal, int) or ordinal < self.floor:
            return
        if row.get('type') == 'event_msg' and p.get('type') == 'item_completed':
            self.consume_activity(row)
            return
        if row.get('type') != 'response_item':
            return
        call_id = p.get('call_id')
        if not isinstance(call_id, str) or not re.fullmatch(r'[\w-]{1,100}', call_id):
            return
        if p.get('type') == 'function_call' and p.get('namespace') == 'collaboration' and p.get('name') == 'send_message':
            try:
                args = json.loads(p.get('arguments', '{}'))
            except (ValueError, TypeError):
                return
            if not isinstance(args, dict):
                return
            target = args.get('target')
            if isinstance(target, str) and not target.startswith('/'):
                target = str(PurePosixPath(self.path).parent / target)
            target = agent_path(target)
            if target:
                self.calls[call_id] = {'id': 'a2a:' + self.path + ':' + call_id,
                    'sender': self.path, 'recipient': target, 'at': row.get('timestamp'),
                    'returned': False, 'body': message_body(args.get('message')),
                    'bodyUnavailable': 'encrypted' if encrypted_message(args.get('message')) else None}
        elif p.get('type') == 'function_call_output' and call_id in self.calls:
            self.calls[call_id]['returned'] = True

    def consume_activity(self, row):
        # Reuse the same metadata allowlist as the parent, without retaining raw rows.
        from .events import Observer
        payload = row.get('payload', {})
        item = payload.get('item', {})
        if item.get('type') not in ('CommandExecution', 'McpToolCall', 'FileChange', 'ImageView', 'WebSearch'):
            return
        if not isinstance(item.get('id'), str) or not isinstance(self.thread, str):
            return
        normalizer = Observer(self.source)
        normalizer.turn = payload.get('turn_id') or 'child-event'
        normalizer.consume(dict(row, payload=dict(payload, turn_id=normalizer.turn)))
        prefix = 'child:' + self.thread + ':'
        for node in normalizer.nodes.values():
            node = dict(node)
            node['id'] = prefix + node['id']
            node['parent'] = agent_id(self.path) if node['parent'] == 'agent' else prefix + node['parent']
            node['ownerAgent'] = agent_id(self.path)
            node['ownerLabel'] = self.path.rsplit('/', 1)[-1]
            node['childActivity'] = True
            self.activities[node['id']] = node

    def read(self):
        stat = self.source.stat()
        identity = (stat.st_dev, stat.st_ino)
        if identity != self.identity or stat.st_size < self.offset:
            self.reset()
            self.identity = identity
        with self.source.open('rb') as stream:
            stream.seek(self.offset)
            while True:
                line = stream.readline()
                if not line or not line.endswith(b'\n'):
                    break
                self.offset = stream.tell()
                try:
                    self.consume(json.loads(line))
                    if not self.path:
                        return []
                except (ValueError, TypeError, AttributeError):
                    continue
        return list(self.calls.values())


def child_candidates(source, thread, stamp):
    """List only filenames matching an observed UUID in known date directories."""
    if not isinstance(thread, str) or not re.fullmatch(r'[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}', thread):
        return []
    directories = {source.parent}
    day = str(stamp or '')[:10]
    current = source.parent
    if (re.fullmatch(r'\d{2}', current.name) and re.fullmatch(r'\d{2}', current.parent.name)
            and re.fullmatch(r'\d{4}', current.parent.parent.name)
            and re.fullmatch(r'\d{4}-\d{2}-\d{2}', day)):
        directories.add(current.parent.parent.parent.joinpath(*day.split('-')))
    return [path for directory in sorted(directories)
            for path in sorted(directory.glob('rollout-*-' + thread + '.jsonl'))
            if not path.is_symlink() and path.is_file()]


def discover_child(source, thread, path, parent, stamp):
    matches = []
    for candidate in child_candidates(source, thread, stamp):
        try:
            with candidate.open('rb') as stream:
                line = stream.readline(1024 * 1024)
            if not line.endswith(b'\n'):
                continue
            row = json.loads(line)
            meta = row.get('payload', {})
            if (row.get('type') == 'session_meta' and meta.get('id') == thread
                    and meta.get('agent_path') == path and meta.get('parent_thread_id') == parent
                    and isinstance(meta.get('subagent_history_start_ordinal'), int)):
                matches.append(candidate)
        except (OSError, ValueError, AttributeError):
            continue
    if len(matches) == 1:
        return AgentMessages(matches[0], expected=(thread, path, parent))
    return None
