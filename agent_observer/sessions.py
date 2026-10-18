"""Explicit session registry with an optional metadata-only recent catalog."""
import json
import re
import threading
from pathlib import Path
from .events import Observer
from .message_capture import MessageCapture
from .catalog import Catalog
from .titles import Titles


class Sessions:
    def __init__(self, catalog_root=None):
        self.lock = threading.RLock()
        self.entries = {}
        self.offered = {}
        self.catalog = Catalog(catalog_root)
        self.titles = Titles(Path(catalog_root).parent / "session_index.jsonl" if catalog_root else None)

    def open_session(self, payload):
        if not isinstance(payload, dict) or not isinstance(payload.get('sessionId'), str):
            raise ValueError('세션 ID가 필요합니다.')
        sid = payload['sessionId']
        with self.lock:
            if sid in self.entries:
                return {'sessionId': sid, 'label': self.display_label(sid, self.entries[sid])}
            item = self.offered.get(sid) or self.catalog.read().get(sid)
            if not item:
                raise ValueError('목록에 없는 세션입니다.')
            return self.register({'source':str(item['source']), 'sessionId':sid, 'label':item.get('customLabel')})

    def register(self, payload, agent_sources=()):
        if not isinstance(payload, dict) or not isinstance(payload.get('source'), str):
            raise ValueError('대화 기록 파일 경로가 필요합니다.')
        source = Path(payload['source']).expanduser()
        if not source.is_absolute() or source.suffix != '.jsonl' or source.is_symlink() or not source.is_file():
            raise ValueError('절대 경로의 JSONL 파일이 필요합니다.')
        source = source.resolve()
        with source.open('rb') as stream:
            header = stream.readline(65537)
        if len(header) > 65536:
            raise ValueError('잘못된 세션 헤더')
        row = json.loads(header)
        meta = row.get('payload', {})
        session_id = meta.get('id')
        if row.get('type') != 'session_meta' or not isinstance(session_id, str) or not re.fullmatch(r'[\w-]{1,100}', session_id):
            raise ValueError('Codex 세션 헤더가 없습니다.')
        if meta.get('parent_thread_id') or meta.get('agent_path') not in (None, '/root'):
            raise ValueError('자식 기록은 부모 세션에서 자동 연결합니다.')
        if payload.get('sessionId') and payload['sessionId'] != session_id:
            raise ValueError('세션 ID와 기록 파일이 일치하지 않습니다.')
        project = Path(meta.get('cwd') or '').name or 'Codex'
        label = payload.get('label') or project
        if not isinstance(label, str) or len(label) > 100:
            raise ValueError('세션 이름은 100자 이하여야 합니다.')
        with self.lock:
            if payload.get('metadataOnly') and session_id not in self.entries:
                if len(self.offered) >= 100 and session_id not in self.offered:
                    raise ValueError('세션 목록 한도')
                item = Catalog.header(source)
                if not item:
                    raise ValueError('잘못된 세션 헤더')
                self.offered[session_id] = dict(item, label=label, customLabel=payload.get('label') or None)
                return {'sessionId':session_id, 'label':label, 'registered':False}
            previous = self.entries.get(session_id)
            if previous:
                if previous['source'] != source:
                    raise ValueError('이미 다른 경로로 등록된 세션입니다.')
                if payload.get('label'):
                    previous['label'] = label
                    previous['customLabel'] = label
            else:
                if len(self.entries) >= 32:
                    raise ValueError('최대 32개 세션을 연결할 수 있습니다.')
                self.entries[session_id] = dict(source=source, label=label, project=project, customLabel=payload.get("label") or None,
                    capture=MessageCapture(Observer(source, agent_sources)))
            return {'sessionId': session_id, 'label': self.display_label(session_id, self.entries[session_id])}

    def capture(self, session_id):
        with self.lock:
            if session_id not in self.entries:
                raise ValueError('등록되지 않은 세션입니다.')
            return self.entries[session_id]['capture']

    def state(self, session_id=None):
        with self.lock:
            session_id = session_id or next(iter(self.entries), None)
            if session_id is None:
                return dict(sessionId=None, turn=None, revision='empty', nodes=[], error=None)
            capture = self.capture(session_id)
            label = self.display_label(session_id, self.entries[session_id])
        return dict(capture.read(), sessionId=session_id, sessionLabel=label)

    def listing(self):
        with self.lock:
            entries = list(self.entries.items())
            result = {sid: {k:v for k,v in item.items() if k != 'source'}
                      for sid,item in dict(self.catalog.read(), **self.offered).items()}
        for sid, entry in entries:
            state = entry['capture'].read()
            result[sid] = dict(result.get(sid, {}), sessionId=sid, label=entry['label'], project=entry['project'], customLabel=entry.get('customLabel'), registered=True,
                               error=state.get('error'), lastEvent=state.get('lastEvent'), nodes=len(state['nodes']))
        names = self.titles.read()
        for sid, item in result.items():
            item['label'] = self.display_label(sid, item, names)
            item.pop('customLabel', None)
        return {'sessions': sorted(result.values(), key=lambda item:item.get('updatedAt') or item.get('lastEvent') or '', reverse=True)}

    def display_label(self, sid, item, names=None):
        names = self.titles.read() if names is None else names
        return item.get('customLabel') or names.get(sid) or ('제목 없는 대화 · ' + sid[-8:])

    def publish(self, payload, hook=False):
        if not isinstance(payload, dict):
            raise ValueError('JSON 객체가 필요합니다.')
        sid = payload.get('sessionId')
        with self.lock:
            if not sid and not hook and len(self.entries) == 1:
                sid = next(iter(self.entries))
        capture = self.capture(sid)
        return capture.capture_hook(payload) if hook else capture.publish(payload)
