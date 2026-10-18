"""Ephemeral, explicit sender-provided bodies attached to observed A2A calls."""
import threading
import time
import re
from pathlib import PurePosixPath
from .agents import message_body


class MessageCapture:
    def __init__(self, observer):
        self.observer = observer
        self.lock = threading.RLock()
        self.turn = None
        self.bodies = {}
        self.revision = 0
        self.pending = {}
        self.origins = {}

    def read(self):
        with self.lock:
            state = self.observer.read()
            if state['turn'] != self.turn:
                self.turn = state['turn']
                self.bodies.clear()
                self.pending.clear()
                self.origins.clear()
                self.revision += 1
            self.match_pending(state)
            nodes = []
            for original in state['nodes']:
                node = dict(original)
                if node['id'] in self.bodies and node['kind'] == 'a2a':
                    node.update(body=self.bodies[node['id']], bodyUnavailable=None,
                                bodySource=self.origins.get(node['id'], 'sender-local'))
                nodes.append(node)
            return dict(state, nodes=nodes, revision=f"{state['revision']}:{self.revision}")

    def match_pending(self, state):
        for call_id, item in list(self.pending.items()):
            if time.monotonic() - item['received'] > 120:
                del self.pending[call_id]
                continue
            matches = [n for n in state['nodes'] if n['kind'] == 'a2a' and n['id'].endswith(':' + call_id)]
            if len(matches) != 1:
                continue
            node = matches[0]
            target = item['target']
            if not target.startswith('/'):
                target = str(PurePosixPath(node['sender']).parent / target)
            if target == node['recipient'] and node['id'] not in self.bodies:
                self.bodies[node['id']] = item['body']
                self.origins[node['id']] = 'native-hook'
                self.revision += 1
            del self.pending[call_id]

    def capture_hook(self, payload):
        """Queue by native call id; never guess by sender order or time."""
        with self.lock:
            self.read()
            if not isinstance(payload, dict) or payload.get('sessionId') != self.observer.root_thread:
                raise ValueError('관찰 중인 세션이 아닙니다.')
            call_id, body, target = (payload.get(k) for k in ('callId', 'body', 'target'))
            if not isinstance(call_id, str) or not re.fullmatch(r'[\w-]{1,100}', call_id):
                raise ValueError('호출 ID가 필요합니다.')
            if not isinstance(body, str) or len(body) > 12000 or message_body(body) is None:
                raise ValueError('본문 형식 오류')
            if not isinstance(target, str) or not re.fullmatch(r'(?:/root/)?[\w/-]{1,200}', target):
                raise ValueError('수신자 형식 오류')
            body = message_body(body)
            existing = next((v for k, v in self.bodies.items() if k.endswith(':' + call_id)), None)
            previous = self.pending.get(call_id)
            if existing is not None:
                if existing != body:
                    raise ValueError('본문 충돌')
                return {'status': 'attached'}
            if previous and (previous['body'], previous['target']) != (body, target):
                raise ValueError('본문 충돌')
            if len(self.pending) >= 256 and not previous:
                raise ValueError('대기열 한도')
            self.pending[call_id] = dict(body=body, target=target, received=time.monotonic())
            self.read()
            return {'status': 'pending' if call_id in self.pending else 'attached'}

    def publish(self, payload):
        with self.lock:
            if not isinstance(payload, dict) or not isinstance(payload.get('body'), str):
                raise ValueError('본문 문자열이 필요합니다.')
            body = payload['body']
            if len(body) > 12000 or message_body(body) is None:
                raise ValueError('본문 형식 또는 길이가 올바르지 않습니다.')
            state = self.read()
            if payload.get('turn') != state['turn']:
                raise ValueError('현재 턴과 일치하지 않습니다.')
            node = next((n for n in state['nodes'] if n['id'] == payload.get('eventId') and n['kind'] == 'a2a'), None)
            if not node or node.get('source') != payload.get('source') or node.get('target') != payload.get('target'):
                raise ValueError('일치하는 실제 메시지 송신 기록이 없습니다.')
            body = message_body(body)
            previous = self.bodies.get(node['id'])
            if previous is not None and previous != body:
                raise ValueError('이미 연결된 본문과 다릅니다.')
            if previous is None:
                self.bodies[node['id']] = body
                self.revision += 1
            return {'eventId': node['id'], 'status': 'attached'}
