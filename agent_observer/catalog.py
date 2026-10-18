"""Bounded recent-session catalog: reads session_meta headers, never prompts."""
import json
import re
import time
from datetime import datetime, timezone
from pathlib import Path


class Catalog:
    def __init__(self, root):
        self.root = Path(root) if root else None
        self.entries = {}
        self.refreshed = float('-inf')

    def read(self):
        if self.root is None or time.monotonic() - self.refreshed < 10:
            return self.entries
        found = {}
        duplicates = set()
        if self.root.is_dir() and not self.root.is_symlink():
            days = sorted(self.root.glob('[0-9][0-9][0-9][0-9]/[0-9][0-9]/[0-9][0-9]'), reverse=True)
            examined = 0
            for day in days:
                if any(p.is_symlink() for p in (day, day.parent, day.parent.parent)):
                    continue
                for source in sorted(day.glob('rollout-*.jsonl'), reverse=True):
                    examined += 1
                    item = self.header(source)
                    if item:
                        sid = item['sessionId']
                        if sid in found:
                            duplicates.add(sid)
                        found[sid] = item
                    if examined >= 1000 or len(found) >= 100:
                        break
                if examined >= 1000 or len(found) >= 100:
                    break
        self.entries = {sid: item for sid, item in found.items() if sid not in duplicates}
        self.refreshed = time.monotonic()
        return self.entries

    @staticmethod
    def header(source):
        try:
            if source.is_symlink() or not source.is_file():
                return None
            with source.open('rb') as stream:
                line = stream.readline(65537)
            if len(line) > 65536:
                return None
            row = json.loads(line)
            meta = row.get('payload', {})
            sid = meta.get('id')
            if row.get('type') != 'session_meta' or not isinstance(sid, str) or not re.fullmatch(r'[\w-]{1,100}', sid):
                return None
            if meta.get('parent_thread_id') or meta.get('agent_path') not in (None, '/root'):
                return None
            project = Path(meta.get('cwd') or '').name or 'Codex'
            return dict(sessionId=sid, label=project, project=project, source=source,
                        updatedAt=datetime.fromtimestamp(source.stat().st_mtime, timezone.utc).isoformat(),
                        registered=False)
        except (OSError, ValueError, TypeError, AttributeError):
            return None
