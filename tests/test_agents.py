import json
import tempfile
import unittest
from pathlib import Path
from agent_observer.events import Observer


class AgentTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.child = Path(self.temp.name) / 'child.jsonl'
        self.main = Path(self.temp.name) / 'main.jsonl'
        self.main.touch()
        self.child.write_text('')
        self.observer = Observer(self.main, [self.child])
        self.observer.consume({'type': 'event_msg', 'timestamp': '2026-10-15T01:00:00Z',
            'payload': {'type': 'task_started', 'turn_id': 'turn'}})
        for name in ('sender', 'receiver'):
            self.activity(name, 'started')

    def activity(self, name, kind):
        self.observer.consume({'type': 'event_msg', 'timestamp': '2026-10-15T01:00:01Z',
            'payload': {'type': 'item_completed', 'turn_id': 'turn', 'item': {
                'type': 'SubAgentActivity', 'agent_path': '/root/' + name,
                'agent_thread_id': name, 'kind': kind}}})

    def write_child(self, target='/root/receiver', thread='sender'):
        rows = [{'type': 'session_meta', 'ordinal': 0, 'payload': {
            'agent_path': '/root/sender', 'id': thread, 'subagent_history_start_ordinal': 5}}]
        for ordinal, call in ((2, 'inherited'), (5, 'actual')):
            rows.append({'type': 'response_item', 'ordinal': ordinal, 'timestamp': '2026-10-15T01:00:02Z',
                'payload': {'type': 'function_call', 'name': 'send_message', 'namespace': 'collaboration',
                    'call_id': call, 'arguments': json.dumps({'target': target, 'message': 'PRIVATE'})}})
        rows.append({'type': 'response_item', 'ordinal': 6, 'payload': {
            'type': 'function_call_output', 'call_id': 'actual', 'output': 'PRIVATE'}})
        self.child.write_text(''.join(json.dumps(r)+'\n' for r in rows))

    def test_lifecycle_stable_identity(self):
        self.activity('sender', 'completed')
        self.assertEqual(self.observer.nodes['subagent:/root/sender']['status'], '완료 관측')
        self.activity('sender', 'interacted')
        self.assertEqual(self.observer.nodes['subagent:/root/sender']['status'], '추가 작업 관측')
        self.assertEqual(sum(n['kind']=='subagent' for n in self.observer.nodes.values()), 2)

    def test_message_identity_privacy_and_idempotence(self):
        self.write_child(); self.observer.merge_messages()
        messages=[n for n in self.observer.nodes.values() if n['kind']=='a2a']
        self.assertEqual(len(messages), 1)
        self.assertEqual(messages[0]['source'], 'subagent:/root/sender')
        self.assertEqual(messages[0]['target'], 'subagent:/root/receiver')
        self.assertEqual(messages[0]['status'], '호출 반환 관측')
        self.assertEqual(messages[0]['body'], 'PRIVATE')
        before=self.observer.revision; self.observer.merge_messages()
        self.assertEqual(before, self.observer.revision)

    def test_unknown_target_and_thread_mismatch_omitted(self):
        self.write_child(target='/root/unknown');self.observer.merge_messages()
        self.assertFalse(any(n['kind']=='a2a' for n in self.observer.nodes.values()))
        self.write_child(thread='other');self.observer.merge_messages()
        self.assertFalse(any(n['kind']=='a2a' for n in self.observer.nodes.values()))

    def test_relative_target_and_reset(self):
        self.write_child(target='receiver');self.observer.merge_messages()
        self.assertTrue(any(n['kind']=='a2a' for n in self.observer.nodes.values()))
        self.child.write_text('');self.observer.merge_messages()
        self.assertFalse(any(n['kind']=='a2a' for n in self.observer.nodes.values()))

    def test_child_tools_skills_namespacing_and_reset(self):
        self.write_child()
        item={'type':'CommandExecution','id':'shared-id','exit_code':0,
            'command':['sh','-c','cat /skills/demo/SKILL.md'],
            'parsed_cmd':[{'type':'read','path':'/skills/demo/SKILL.md'}], 'output':'PRIVATE'}
        with self.child.open('a') as stream:
            for ordinal in (3, 7):
                stream.write(json.dumps({'type':'event_msg','ordinal':ordinal,'timestamp':'2026-10-15T01:00:03Z',
                    'payload':{'type':'item_completed','turn_id':'child-turn','item':item}})+'\n')
        self.observer.merge_messages()
        nodes=[n for n in self.observer.nodes.values() if n.get('childActivity')]
        self.assertEqual(len(nodes),2)
        tool=next(n for n in nodes if n['kind']=='tool');skill=next(n for n in nodes if n['kind']=='skill')
        self.assertEqual(tool['parent'],'subagent:/root/sender')
        self.assertEqual(skill['parent'],tool['id'])
        self.assertEqual(skill['label'],'demo')
        self.assertTrue(tool['id'].startswith('child:sender:'))
        self.assertEqual(tool['ownerAgent'],'subagent:/root/sender')
        self.assertNotIn('PRIVATE',json.dumps(nodes))
        revision=self.observer.revision;self.observer.merge_messages();self.assertEqual(revision,self.observer.revision)
        self.child.write_text('');self.observer.merge_messages()
        self.assertFalse(any(n.get('childActivity') for n in self.observer.nodes.values()))

    def test_message_body_bounds_and_credentials(self):
        from agent_observer.agents import message_body
        self.assertIsNone(message_body({'message': 'invalid'}))
        self.assertEqual(message_body('첫 줄\n둘째 줄 <script>'), '첫 줄\n둘째 줄 <script>')
        self.assertNotIn('abc123', message_body('token=abc123'))
        self.assertLess(len(message_body('x' * 13000)), 12100)

    def test_encrypted_body_is_not_displayed(self):
        from agent_observer.agents import message_body
        encrypted='gAAAAA'+'a'*120
        self.assertIsNone(message_body(encrypted))
        self.write_child()
        self.child.write_text(self.child.read_text().replace('PRIVATE', encrypted))
        self.observer.merge_messages()
        message=next(n for n in self.observer.nodes.values() if n['kind']=='a2a')
        self.assertEqual(message['bodyUnavailable'], 'encrypted')
        self.assertIsNone(message['body'])
        self.assertNotIn(encrypted, json.dumps(message))
