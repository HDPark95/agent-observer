"""Normalize only approved metadata from a Codex transcript."""
import json
import re
import shlex
import threading
import time
from datetime import datetime
from pathlib import Path
from .agents import AgentMessages, agent_path, agent_id, discover_child
from .browser_events import browser_metadata

def display_name(value):
    name = Path(str(value)).name
    if not re.fullmatch(r'[\w.-]{1,64}', name) or name.startswith('.'):
        return '파일'
    return name


def simple_command_words(source):
    """Decline shell control and expansion rather than hide mixed product work."""
    quote = None
    escaped = False
    for char in source.strip():
        if escaped:
            if char == '\n':
                return []
            escaped = False
            continue
        if char == '\\' and quote != "'":
            escaped = True
            continue
        if quote != "'" and char in '$`':
            return []
        if quote:
            if char == quote:
                quote = None
            continue
        if char in "'\"":
            quote = char
        elif char in ';&|()<>\n#':
            return []
    try:
        return shlex.split(source)
    except ValueError:
        return []


def unwrap_uv(words):
    if len(words) < 2 or Path(words[0]).name != 'uv' or words[1] != 'run':
        return words
    words = words[2:]
    while words and words[0] in ('--offline', '--no-sync'):
        words = words[1:]
    return words


def mailbox_stdin_command(source):
    """Recognize only mailbox send's single body-file input redirection."""
    quote = None
    escaped = False
    for index, char in enumerate(source):
        if escaped:
            escaped = False
            continue
        if char == '\\' and quote != "'":
            escaped = True
            continue
        if quote:
            if char == quote:
                quote = None
            continue
        if char in "'\"":
            quote = char
            continue
        if char != '<':
            continue
        command = simple_command_words(source[:index])
        body_file = simple_command_words(source[index + 1:])
        if (len(command) < 4 or Path(command[0]).name != 'agent-observer'
                or command[1:3] != ['message', 'send'] or '--body-stdin' not in command[3:]
                or len(body_file) != 1):
            return []
        return command
    return []


def go_test_label(args):
    """Use explicit packages and selectors, never test arguments."""
    packages = []
    selection = ''
    value_flags = {
        '-bench', '-benchtime', '-blockprofile', '-blockprofilerate', '-count',
        '-covermode', '-coverpkg', '-coverprofile', '-cpu', '-cpuprofile', '-exec',
        '-fuzz', '-fuzzminimizetime', '-fuzztime', '-list', '-memprofile',
        '-memprofilerate', '-mod', '-modfile', '-mutexprofile', '-mutexprofilefraction',
        '-o', '-outputdir', '-overlay', '-p', '-parallel', '-shuffle', '-skip',
        '-tags', '-timeout', '-trace', '-vet',
    }
    i = 0
    while i < len(args):
        arg = args[i]
        i += 1
        if arg == '-args':
            break
        flag, inline, value = arg.partition('=')
        if flag == '-run':
            if not inline and i < len(args):
                value = args[i]
                i += 1
            selection = value
            continue
        if arg.startswith('-'):
            if not inline and flag in value_flags:
                i += 1
            continue
        packages.append(arg.removesuffix('/...').removeprefix('./').removeprefix('agent-observer/'))
    if 'SSE' in selection or 'Stream' in selection:
        return 'SSE 테스트 실행'
    if packages and all(p in ('internal/messaging', 'internal/messagecli', 'internal/messages') for p in packages):
        return '메시지 테스트 실행'
    return 'Go 테스트 실행'


def task_label(words):
    """Recognize explicit entry points, not strings inside code or arguments."""
    words = unwrap_uv(words)
    if not words:
        return None
    executable = Path(words[0]).name
    if executable in ('python', 'python3') and words[1:2] == ['-m']:
        modules = {'unittest': '테스트 실행', 'pytest': '테스트 실행',
                   'checks.reset_contract': '기록 초기화 동작 검사'}
        return modules.get(words[2]) if len(words) > 2 else None
    if executable == 'pytest' and '--version' not in words and '--help' not in words:
        return '테스트 실행'
    if executable == 'go' and words[1:2] == ['test']:
        return go_test_label(words[2:])
    if executable == 'agent-observer' and words[1:2] == ['message'] and len(words) > 2:
        return {'send': '메시지 보내기', 'receive': '메시지 읽기',
                'ack': '메시지 수신 확인', 'enroll': '에이전트 메시지 연결'}.get(words[2])
    if executable == 'node' and len(words) > 1:
        script = Path(words[1]).as_posix()
        if script == 'checks/initial_render.cjs' or script.endswith('/checks/initial_render.cjs'):
            return '화면 동작 검사'
        if words[1] == '--test':
            return '테스트 실행'
    if executable in ('npm', 'pnpm', 'yarn'):
        rest = words[1:]
        if rest[:1] == ['run']:
            rest = rest[1:]
        actions = {'test': '테스트 실행', 'build': '빌드 실행',
                   'lint': '코드 스타일 검사', 'typecheck': '타입 검사'}
        return actions.get(rest[0]) if rest else None
    return None


def observation_label(words):
    words = unwrap_uv(words)
    if len(words) < 3 or Path(words[0]).name not in ('python', 'python3') or words[1] != '-m':
        return None
    module = words[2]
    if module not in ('tools.skill_observation', 'tools.observation_workflow',
                      'tools.task_observation', 'tools.task_measurement'):
        return None
    actions = {'start': '시작', 'read': '읽기 기록', 'end': '종료', 'finish': '마감',
               'deliver': '전송', 'resume': '재개', 'lookup': '조회 기록',
               'agent': '에이전트 기록', 'mark': '진행 기록', 'question': '질문 기록',
               'answer': '응답 기록', 'rework': '재작업 기록', 'summary': '요약'}
    action = actions.get(words[3], '실행') if len(words) > 3 else '실행'
    prefix = '스킬 관찰 · ' if module == 'tools.skill_observation' else '작업 관찰 · '
    return prefix + action, module


def document_label(reads):
    names = {
        'messaging.md': '메시지 문서 검토', 'messaging-design-proposal.md': '메시지 문서 검토',
        'harness-observability-usage.md': '하네스 관측 문서 읽기',
        'skill-observation-usage.md': '하네스 관측 문서 읽기',
        'task-friction-usage.md': '하네스 관측 문서 읽기',
    }
    labels = {names.get(Path(p.get('path') or p.get('name') or '').name) for p in reads}
    return next(iter(labels)) if len(labels) == 1 and None not in labels else None


def command_identity(item):
    """Describe observed command structure; never expose arbitrary arguments."""
    parsed = item.get('parsed_cmd', [])
    command = item.get('command', [])
    source = command[-1] if isinstance(command, list) and command else ''
    simple = simple_command_words(source) or mailbox_stdin_command(source)
    words = simple
    if not words:
        try:
            words = shlex.split(source.splitlines()[0] if source else '')
        except ValueError:
            words = []
    executable = Path(words[0]).name if words else ''
    reader = executable if executable in ('cat', 'sed', 'head', 'tail', 'rg') else '파일 읽기'
    for part in parsed:
        path = part.get('path', '')
        if part.get('type') == 'read' and path.endswith('/SKILL.md'):
            return '스킬 읽기 · ' + display_name(Path(path).parent), reader
    label = task_label(simple)
    if label:
        return label, executable
    observation = observation_label(simple)
    if observation:
        return observation
    if simple and all(p.get('type') == 'read' for p in parsed):
        label = document_label(parsed)
        if label:
            return label, reader
    for part in parsed:
        if part.get('type') == 'read':
            count = sum(p.get('type') == 'read' for p in parsed)
            suffix = f' 외 {count-1}개' if count > 1 else ''
            return '파일 읽기 · ' + display_name(part.get('path') or part.get('name') or '파일') + suffix, reader
    if executable in ('rg', 'grep'):
        return ('파일 목록 검색' if '--files' in words else '내용 검색') + ' · ' + executable, executable
    if executable in ('ls', 'find'):
        return '파일 목록 · ' + executable, executable
    if executable in ('python', 'python3', 'node', 'uv'):
        if '--version' in words:
            return '버전 확인 · ' + executable, executable
        script = words[1] if len(words)>1 and re.fullmatch(r'[\w./-]+\.(py|js|mjs|cjs)', words[1]) else None
        return ('스크립트 실행 · ' + display_name(script) if script else '코드 실행 · ' + executable), executable
    if executable in ('git', 'npm', 'pnpm', 'pytest', 'cat', 'sed', 'curl', 'mkdir', 'wc', 'codex', 'claude'):
        action = words[1] if len(words)>1 and words[1] in ('status','diff','test','build','--version','--help') else ''
        return '명령 실행 · ' + executable + (' ' + action if action else ''), executable
    return '셸 명령 실행', 'shell'

class Observer:
    def __init__(self, source, agent_sources=()):
        self.source = source
        self.agent_sources = [AgentMessages(path) for path in agent_sources]
        self.agent_errors = []
        self.root_thread = None
        self.auto_sources = {}
        self.auto_pending = 0
        self.offset = 0
        self.identity = None
        self.nodes = {}
        self.turn = None
        self.started = None
        self.revision = 0
        self.last_event = None
        self.error = None
        self.lock = threading.Lock()

    def node(self, id, kind, label, stamp, status='완료 관측', parent='agent', **extra):
        old = self.nodes.get(id, {})
        self.nodes[id] = {**old, 'id': id, 'kind': kind, 'label': label,
            'at': old.get('at', stamp), 'seen': time.time(), 'status': status,
            'parent': parent, **extra}

    def consume_agent(self, item, stamp):
        path = agent_path(item.get('agent_path'))
        if not path or path == '/root':
            return
        statuses = {'started': '생성 관측', 'interacted': '추가 작업 관측', 'completed': '완료 관측'}
        status = statuses.get(item.get('kind'))
        if not status:
            return
        parent = path.rsplit('/', 1)[0]
        self.node(agent_id(path), 'subagent', path.rsplit('/', 1)[-1], stamp,
                  status, agent_id(parent), agentPath=path, threadId=item.get('agent_thread_id'),
                  updatedAt=stamp, evidence='SubAgentActivity · 에이전트 상태 기록')

    def connect_children(self):
        active = {n['threadId']: n for n in self.nodes.values()
                  if n['kind'] == 'subagent' and isinstance(n.get('threadId'), str)}
        self.auto_sources = {key: reader for key, reader in self.auto_sources.items()
                             if key in active and reader.source.is_file()}
        self.auto_pending = 0
        for thread, node in active.items():
            parent_path = node['agentPath'].rsplit('/', 1)[0]
            parent = self.root_thread if parent_path == '/root' else self.nodes.get(agent_id(parent_path), {}).get('threadId')
            expected = (thread, node['agentPath'], parent)
            reader = self.auto_sources.get(thread)
            if reader and reader.expected != expected:
                del self.auto_sources[thread]
                reader = None
            if not reader and parent:
                reader = discover_child(self.source, thread, node['agentPath'], parent, node['at'])
                if reader:
                    self.auto_sources[thread] = reader
            if not reader:
                self.auto_pending += 1

    def merge_messages(self):
        self.agent_errors = []
        expected = {}
        self.connect_children()
        readers = {reader.source: reader for reader in [*self.agent_sources, *self.auto_sources.values()]}
        for reader in readers.values():
            try:
                messages = reader.read()
            except OSError:
                self.agent_errors.append('선택한 자식 기록을 읽을 수 없습니다.')
                continue
            if reader.expected and not reader.path:
                self.auto_sources.pop(reader.expected[0], None)
                self.auto_pending += 1
                continue
            owner = self.nodes.get(agent_id(reader.path)) if reader.path else None
            if not owner or owner.get('threadId') != reader.thread:
                continue
            for activity in reader.activities.values():
                if activity.get('at') and self.started and activity['at'] >= self.started:
                    expected[activity['id']] = activity
            for message in messages:
                if not message['at'] or not self.started or message['at'] < self.started:
                    continue
                sender, recipient = agent_id(message['sender']), agent_id(message['recipient'])
                if sender not in self.nodes or recipient not in self.nodes:
                    continue
                expected[message['id']] = dict(message, kind='a2a',
                    label=self.nodes[sender]['label'] + ' → ' + self.nodes[recipient]['label'],
                    parent=sender, source=sender, target=recipient,
                    status='호출 반환 관측' if message['returned'] else '송신 호출 관측',
                    evidence='선택한 자식 기록의 collaboration.send_message · 송수신 경로',
                    scope='호출 반환은 상대의 수신·이해 확인과 다릅니다. 본문은 선택한 기록에서 읽어 로컬에 표시합니다.')
        old = {key: value for key, value in self.nodes.items() if value['kind'] == 'a2a' or value.get('childActivity')}
        if old != expected:
            for key in old:
                del self.nodes[key]
            self.nodes.update(sorted(expected.items(), key=lambda pair: (pair[1]['at'], pair[0])))
            self.revision += 1

    def consume(self, row):
        p = row.get('payload', {})
        stamp = row.get('timestamp')
        typ = p.get('type')
        if row.get('type') == 'session_meta' and self.root_thread is None:
            self.root_thread = p.get('id')
        if row.get('type') == 'event_msg' and typ == 'task_started':
            self.turn = p.get('turn_id')
            self.started = stamp
            self.nodes = {}
            self.node('request', 'request', '사용자 요청', stamp, '요청 관측', None)
            self.node('agent', 'agent', 'Codex 에이전트', stamp, '작업 중', 'request')
        if not self.turn:
            return
        if row.get('type') == 'event_msg' and typ == 'task_complete':
            if p.get('turn_id') == self.turn:
                self.node('agent', 'agent', 'Codex 에이전트', stamp, '턴 종료', 'request')
        if row.get('type') == 'response_item':
            if typ in ('custom_tool_call', 'function_call'):
                # Only the wrapper name is exposed. Script input may contain secrets.
                name = str(p.get('name', 'tool'))
                name = name if re.fullmatch(r'[\w.:_-]{1,100}', name) else 'tool'
                self.node(p.get('call_id'), 'call', name, stamp, '반환 대기', evidence='도구 호출 시작 기록', scope='호출 묶음 · 내부 도구와 중복 집계 금지')
            elif typ in ('custom_tool_call_output', 'function_call_output'):
                id = p.get('call_id')
                if id in self.nodes:
                    n = self.nodes[id]
                    self.node(id, n['kind'], n['label'], stamp, '반환 관측', ended=stamp)
        if row.get('type') == 'event_msg' and typ == 'item_completed':
            if p.get('turn_id') != self.turn:
                return
            item = p.get('item', {})
            kind = item.get('type')
            id = item.get('id')
            extra = {'evidence': 'item_completed · 완료 후 관측', 'startMs': p.get('started_at_ms'), 'endMs': p.get('completed_at_ms')}
            if kind == 'SubAgentActivity':
                self.consume_agent(item, stamp)
            elif kind == 'CommandExecution':
                code = item.get('exit_code')
                label, tool = command_identity(item)
                self.node(id, 'tool', label, stamp, '오류' if code else '완료 관측', toolName=tool, exitCode=code, **extra)
                # Extract skill names only from the native parsed read commands.
                for cmd in item.get('parsed_cmd', []):
                    path = cmd.get('path', '')
                    if cmd.get('type') == 'read' and path.endswith('/SKILL.md'):
                        name = Path(path).parent.name
                        if re.fullmatch(r'[\w.-]{1,100}', name):
                            self.node(id + ':skill:' + name, 'skill', name, stamp, '읽기 시도', id, evidence='parsed_cmd.type=read · 개별 읽기 결과 미수집 · 실제 적용 여부 미판정')
            elif kind == 'McpToolCall':
                browser = browser_metadata(item)
                if browser:
                    label = browser.pop('label')
                    self.node(id, 'tool', label, stamp,
                              '오류' if item.get('status') == 'failed' or (isinstance(item.get('result'), dict) and item['result'].get('isError')) else '호출 완료 관측',
                              **(extra | browser))
                    self.last_event = stamp
                    self.revision += 1
                    return
                label = str(item.get('server', 'mcp')) + ' / ' + str(item.get('tool', 'tool'))
                if not re.fullmatch(r'[\w ./:-]{1,160}', label):
                    label = 'MCP 도구'
                self.node(id, 'tool', label, stamp, str(item.get('status', '완료 관측')), **extra)
            elif kind in ('ImageView', 'FileChange', 'WebSearch'):
                self.node(id, 'tool', kind, stamp, **extra)
            elif kind == 'AgentMessage':
                self.node(id, 'message', '최종 답변' if item.get('phase') == 'final' else '진행 메시지', stamp, evidence='메시지 메타데이터 · 원문 미전송')
        self.last_event = stamp
        self.revision += 1

    def read(self):
        with self.lock:
            try:
                stat = self.source.stat()
                identity = (stat.st_dev, stat.st_ino)
                if identity != self.identity or stat.st_size < self.offset:
                    if self.identity is not None:
                        self.revision += 1
                    self.root_thread = None
                    self.auto_sources = {}
                    self.offset = 0
                    self.nodes = {}
                    self.turn = None
                    self.started = None
                    self.last_event = None
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
                        except (ValueError, TypeError):
                            continue
                self.error = None
            except OSError:
                self.error = '현재 대화 기록을 읽을 수 없습니다.'
            self.merge_messages()
            return {'autoAgents': {'connected': len(self.auto_sources), 'pending': self.auto_pending}, 'agentSourceErrors': self.agent_errors, 'turn': self.turn, 'started': self.started, 'revision': self.revision,
                'nodes': list(self.nodes.values()), 'lastEvent': self.last_event,
                'checkedAt': datetime.now().astimezone().isoformat(), 'error': self.error,
                'mode': '실제 transcript 증분 읽기 · 0.5초 폴링'}
