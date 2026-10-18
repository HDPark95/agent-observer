"""Read only Codex's title index; never derive titles from message bodies."""
import json


class Titles:
    def __init__(self, path):
        self.path = path
        self.signature = None
        self.names = {}

    def read(self):
        if self.path is None:
            return {}
        try:
            if self.path.is_symlink():
                return {}
            stat = self.path.stat()
            signature = (stat.st_ino, stat.st_size, stat.st_mtime_ns)
            if signature == self.signature:
                return self.names
            names = {}
            with self.path.open('rb') as stream:
                # Bound memory and read the newest index records on large installations.
                offset = max(0, stat.st_size - 4 * 1024 * 1024)
                stream.seek(offset)
                if offset:
                    stream.readline()
                for line in stream:
                    try:
                        row = json.loads(line)
                        sid, title = row.get('id'), row.get('thread_name')
                        if isinstance(sid, str) and isinstance(title, str) and title.strip():
                            names[sid] = title.strip()[:500]
                    except (ValueError, AttributeError):
                        continue
            self.names, self.signature = names, signature
            return names
        except OSError:
            return {}
