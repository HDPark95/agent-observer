import json
import tempfile
import unittest
from pathlib import Path

from agent_observer.events import Observer, command_identity


class EventTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.path = Path(self.temp.name) / 'source.jsonl'
        self.path.touch()
        self.observer = Observer(self.path)
        self.append('event_msg', type='task_started', turn_id='turn-a')

    def append(self, event, **payload):
        row = {'timestamp': '2026-10-15T00:00:00Z', 'type': event, 'payload': payload}
        with self.path.open('a') as output:
            output.write(json.dumps(row) + '\n')

    def test_partial_and_idempotent(self):
        original = self.observer.read()
        row = json.dumps({'type': 'response_item', 'payload': {'type': 'custom_tool_call', 'name': 'exec', 'call_id': 'a'}})
        with self.path.open('a') as output:
            output.write(row)
        self.assertEqual(original, self.observer.read() | {'checkedAt': original['checkedAt']})
        with self.path.open('a') as output:
            output.write('\n')
        current = self.observer.read()
        self.assertEqual(len(current['nodes']), 3)
        self.assertEqual(current['revision'], self.observer.read()['revision'])

    def test_call_update_and_privacy(self):
        self.append('response_item', type='custom_tool_call', call_id='a', name='exec', input='PRIVATE-INPUT')
        self.assertEqual(self.observer.read()['nodes'][-1]['status'], '반환 대기')
        self.append('response_item', type='custom_tool_call_output', call_id='a', output='PRIVATE-OUTPUT')
        state = self.observer.read()
        self.assertEqual(len(state['nodes']), 3)
        self.assertEqual(state['nodes'][-1]['status'], '반환 관측')
        self.assertNotIn('PRIVATE', json.dumps(state))

    def test_skill_parent(self):
        self.append('event_msg', type='item_completed', turn_id='turn-a', item={
            'type': 'CommandExecution', 'id': 'read-1', 'command': ['sh', '-c', 'cat /skills/demo/SKILL.md'],
            'exit_code': 0, 'parsed_cmd': [{'type': 'read', 'path': '/skills/demo/SKILL.md'}]})
        nodes = self.observer.read()['nodes']
        self.assertEqual(nodes[-1]['parent'], 'read-1')
        self.assertEqual(nodes[-1]['label'], 'demo')
        self.assertEqual(nodes[-2]['label'], '스킬 읽기 · demo')

    def test_command_exit_does_not_prove_individual_skill_read(self):
        for command in ['cat /skills/demo/SKILL.md',
                        'cat /skills/demo/SKILL.md || true',
                        'false && cat /skills/demo/SKILL.md; true',
                        'cat /skills/demo/SKILL.md /missing']:
            for code in [0, 1, None]:
                with self.subTest(command=command, code=code):
                    self.append('event_msg', type='item_completed', turn_id='turn-a', item={
                        'type': 'CommandExecution', 'id': 'read', 'command': ['sh', '-c', command],
                        'exit_code': code, 'parsed_cmd': [{'type': 'read', 'path': '/skills/demo/SKILL.md'}]})
                    node = next(n for n in self.observer.read()['nodes'] if n['id'] == 'read:skill:demo')
                    self.assertEqual(node['status'], '읽기 시도')
                    self.assertEqual(node['parent'], 'read')
                    self.assertIn('개별 읽기 결과 미수집', node['evidence'])

    def test_turn_and_rotation(self):
        self.observer.read()
        self.append('event_msg', type='task_started', turn_id='turn-b')
        self.assertEqual(self.observer.read()['turn'], 'turn-b')
        replacement = self.path.with_suffix('.new')
        replacement.write_text(json.dumps({'type':'event_msg','payload':{'type':'task_started','turn_id':'turn-c'}})+'\n')
        replacement.replace(self.path)
        state = self.observer.read()
        self.assertEqual(state['turn'], 'turn-c')
        self.assertEqual(len(state['nodes']), 2)

    def test_command_names(self):
        label, _ = command_identity({'command':['sh','-c','curl -H "Authorization: PRIVATE" https://example.test']})
        self.assertEqual(label, '명령 실행 · curl')
        label, _ = command_identity({'command':['sh','-c',"python3 - <<'PY'\nprint('private.py')\nPY"]})
        self.assertEqual(label, '코드 실행 · python3')
        label, _ = command_identity({'command':['sh','-c','rg --files .']})
        self.assertEqual(label, '파일 목록 검색 · rg')

    def test_task_labels(self):
        cases = {
            'node checks/initial_render.cjs': '화면 동작 검사',
            'node /workspace/checks/initial_render.cjs': '화면 동작 검사',
            'python3 -m unittest discover -s tests -v': '테스트 실행',
            '.venv/bin/python -m pytest': '테스트 실행',
            'uv run --offline --no-sync python -m unittest': '테스트 실행',
            'pytest tests': '테스트 실행',
            'node --test': '테스트 실행',
            'python3 -m checks.reset_contract': '기록 초기화 동작 검사',
            'npm test': '테스트 실행',
            'pnpm run build': '빌드 실행',
            'npm run lint': '코드 스타일 검사',
            'yarn typecheck': '타입 검사',
            'go test -race ./...': 'Go 테스트 실행',
            "go test ./internal/httpapi -run 'TestStreamFilesystemReconnectAndIsolation|TestMissingDiscoveryDirectory' -count=1": 'SSE 테스트 실행',
            'go test ./internal/httpapi -run=TestMailboxSSEWithoutTranscriptWrite': 'SSE 테스트 실행',
            'go test -race ./internal/messaging ./internal/messagecli ./internal/messages': '메시지 테스트 실행',
            'go test ./internal/messaging ./internal/httpapi': 'Go 테스트 실행',
            'go test ./... -args -run TestStreamPrivate': 'Go 테스트 실행',
            'go test ./... -run TestStreamPrivate -run TestOther': 'Go 테스트 실행',
            'go test -coverprofile ./internal/messages ./...': 'Go 테스트 실행',
            './bin/agent-observer message send --body-stdin --message-id PRIVATE < /tmp/private-message.txt': '메시지 보내기',
            "./bin/agent-observer message send --body-stdin<'/tmp/PRIVATE; message.txt'": '메시지 보내기',
            './bin/agent-observer message receive --wait 1s': '메시지 읽기',
            './bin/agent-observer message ack --message-id PRIVATE': '메시지 수신 확인',
            './bin/agent-observer message enroll --token PRIVATE': '에이전트 메시지 연결',
        }
        for command, expected in cases.items():
            with self.subTest(command=command):
                label, _ = command_identity({'command': ['sh', '-c', command]})
                self.assertEqual(label, expected)
        for command in ('node -e "checks/initial_render.cjs"',
                        'python3 -c "pytest PRIVATE"', 'echo pytest',
                        'npm install test', 'node unknown.cjs'):
            label, _ = command_identity({'command': ['sh', '-c', command]})
            self.assertNotIn(label, cases.values())
            self.assertNotIn('PRIVATE', label)

    def test_observation_command_structure(self):
        cases = (
            ('uv run --offline --no-sync python -m tools.task_observation agent --run-id PRIVATE',
             '작업 관찰 · 에이전트 기록', 'tools.task_observation'),
            ('.venv/bin/python -m tools.task_measurement rework --reason PRIVATE',
             '작업 관찰 · 재작업 기록', 'tools.task_measurement'),
            ('python3 -m tools.task_measurement unknown --reason start',
             '작업 관찰 · 실행', 'tools.task_measurement'),
        )
        for command, label, tool in cases:
            self.assertEqual(command_identity({'command': ['sh', '-c', command]}), (label, tool))
        for command in (
            'echo -m tools.skill_observation start',
            'echo "python3 -m tools.skill_observation start"',
            'python3 -c "print(\'-m tools.observation_workflow start\')"',
            'python3 -m tools.skill_observation start;go test ./...',
            'python3 -m tools.observation_workflow start && go test ./...',
            'python3 -m tools.observation_workflow start\ngo test ./...',
            'python3 -m tools.skill_observation start --run-id "$(go test ./...)"',
            'python3 -m tools.skill_observation start --run-id `go test ./...`',
            'uv run --offline python3 -m tools.skill_observation start | cat',
        ):
            with self.subTest(command=command):
                label, tool = command_identity({'command': ['sh', '-c', command]})
                self.assertNotIn('관찰', label)
                self.assertFalse(tool.startswith('tools.'))

    def test_mailbox_label_rejects_compound_and_substitution(self):
        for command in (
            './bin/agent-observer message send --body-stdin < /tmp/body; go test ./...',
            './bin/agent-observer message send --body-stdin < $(cat /tmp/path)',
            './bin/agent-observer message send --body-stdin <<EOF\nPRIVATE\nEOF',
            './bin/agent-observer message send --body-stdin < /tmp/body | cat',
        ):
            with self.subTest(command=command):
                label, _ = command_identity({'command': ['sh', '-c', command]})
                self.assertNotEqual(label, '메시지 보내기')

    def test_known_document_labels_require_all_reads(self):
        cases = (
            (['/project/docs/messaging.md'], '메시지 문서 검토'),
            (['/project/docs/messaging.md', '/project/docs/messaging-design-proposal.md'], '메시지 문서 검토'),
            (['/harness/docs/skill-observation-usage.md'], '하네스 관측 문서 읽기'),
            (['/harness/docs/skill-observation-usage.md', '/project/docs/product.md'], '파일 읽기 · skill-observation-usage.md 외 1개'),
        )
        for paths, expected in cases:
            with self.subTest(paths=paths):
                result = command_identity({'command': ['sh', '-c', 'cat documents'],
                                           'parsed_cmd': [{'type': 'read', 'path': path} for path in paths]})
                self.assertEqual(result, (expected, 'cat'))
        observation = {'type': 'read', 'path': '/harness/docs/skill-observation-usage.md'}
        for command, parsed in (
            ('cat docs/skill-observation-usage.md; go test ./...', [observation]),
            ('cat docs/skill-observation-usage.md', [observation, {'type': 'unknown'}]),
        ):
            result = command_identity({'command': ['sh', '-c', command], 'parsed_cmd': parsed})
            self.assertEqual(result, ('파일 읽기 · skill-observation-usage.md', 'cat'))

    def test_task_label_keeps_tool_and_hides_arguments(self):
        self.append('event_msg', type='item_completed', turn_id='turn-a', item={
            'type': 'CommandExecution', 'id': 'test-task',
            'command': ['sh', '-c', 'python3 -m unittest PRIVATE'], 'exit_code': 0})
        node = next(n for n in self.observer.read()['nodes'] if n['id'] == 'test-task')
        self.assertEqual(node['label'], '테스트 실행')
        self.assertEqual(node['toolName'], 'python3')
        self.assertNotIn('PRIVATE', json.dumps(node))
