import unittest
from agent_observer.message_capture import MessageCapture

class Source:
    def __init__(self):
        self.state={'turn':'t','revision':1,'nodes':[{'id':'m','kind':'a2a','source':'s','target':'r','body':None}]}
    def read(self): return self.state

class CaptureTests(unittest.TestCase):
    def setUp(self):
        self.source=Source();self.capture=MessageCapture(self.source)
        self.payload=dict(turn='t',eventId='m',source='s',target='r',body='UX 제안\n다음 줄')
    def test_attach_idempotence_and_source_unchanged(self):
        before=self.capture.read()['revision']
        self.capture.publish(self.payload)
        state=self.capture.read()
        self.assertEqual(state['nodes'][0]['body'],self.payload['body'])
        self.assertIsNone(self.source.state['nodes'][0]['body'])
        self.assertNotEqual(before,state['revision'])
        self.capture.publish(self.payload)
        self.assertEqual(state['revision'],self.capture.read()['revision'])
    def test_wrong_identity_turn_and_conflict(self):
        for key in ('eventId','turn','source','target'):
            with self.assertRaises(ValueError): self.capture.publish(dict(self.payload,**{key:'wrong'}))
        self.capture.publish(self.payload)
        with self.assertRaises(ValueError): self.capture.publish(dict(self.payload,body='different'))
    def test_reset_and_redaction(self):
        self.capture.publish(dict(self.payload,body='token=abc123'))
        self.assertNotIn('abc123',self.capture.read()['nodes'][0]['body'])
        self.source.state['turn']='new'
        self.assertIsNone(self.capture.read()['nodes'][0]['body'])
        self.assertEqual(self.capture.bodies,{})

class HookCaptureTests(unittest.TestCase):
    def setUp(self):
        self.source=Source();self.source.root_thread='session'
        self.source.state['nodes']=[]
        self.capture=MessageCapture(self.source)
        self.payload=dict(sessionId='session',callId='call-1',target='dx',body='자동 수집\n본문')
    def add_message(self, call='call-1', recipient='/root/dx'):
        self.source.state['nodes'].append(dict(id='a2a:/root/ux:'+call,kind='a2a',sender='/root/ux',recipient=recipient,body=None))
    def test_hook_before_transcript_and_idempotence(self):
        self.assertEqual(self.capture.capture_hook(self.payload)['status'],'pending')
        self.add_message()
        state=self.capture.read()
        self.assertEqual(state['nodes'][0]['body'],self.payload['body'])
        self.assertEqual(state['nodes'][0]['bodySource'],'native-hook')
        self.assertEqual(self.capture.capture_hook(self.payload)['status'],'attached')
        self.assertEqual(state['revision'],self.capture.read()['revision'])
    def test_session_call_target_and_turn_isolation(self):
        with self.assertRaises(ValueError): self.capture.capture_hook(dict(self.payload,sessionId='other'))
        self.capture.capture_hook(self.payload)
        self.add_message(call='different');self.assertIsNone(self.capture.read()['nodes'][0]['body'])
        self.add_message(recipient='/root/other');self.assertIsNone(self.capture.read()['nodes'][1]['body'])
        self.capture.capture_hook(dict(self.payload,callId='later'))
        self.source.state['turn']='new';self.capture.read();self.assertEqual(self.capture.pending,{})
    def test_out_of_order_calls_keep_exact_bodies(self):
        self.capture.capture_hook(self.payload)
        self.capture.capture_hook(dict(self.payload,callId='call-2',body='두 번째'))
        self.add_message(call='call-2');self.add_message()
        nodes=self.capture.read()['nodes']
        self.assertEqual([n['body'] for n in nodes],['두 번째',self.payload['body']])
    def test_expiry_and_conflict(self):
        self.capture.capture_hook(self.payload)
        with self.assertRaises(ValueError):self.capture.capture_hook(dict(self.payload,body='changed'))
        self.capture.pending['call-1']['received']-=121
        self.add_message();self.assertIsNone(self.capture.read()['nodes'][0]['body'])
    def test_hook_selects_only_message_input(self):
        from agent_observer.message_hook import capture_payload
        event=dict(hook_event_name='PostToolUse',tool_name='send_message',session_id='session',tool_use_id='call-1',tool_input=dict(message='본문',target='dx',other='omit'),tool_response='omit')
        self.assertEqual(set(capture_payload(event)),{'sessionId','callId','target','body'})
        for name in ('send_message', 'collaboration.send_message', 'collaboration/send_message', 'collaborationsend_message'):
            with self.subTest(tool=name):
                payload = capture_payload(dict(event, tool_name=name))
                self.assertIsNotNone(payload)
                self.assertEqual(payload['body'], '본문')
        for name in ('Bash','send_email','mcp__slack__send_message', 'collaborationsend_message_extra', 'othersend_message'):
            self.assertIsNone(capture_payload(dict(event,tool_name=name)))

class HookStatusTests(unittest.TestCase):
    def test_encrypted_hook_input_is_identified_without_forwarding(self):
        from agent_observer.message_hook import capture_payload, encrypted_input
        event = dict(hook_event_name='PostToolUse', tool_name='collaborationsend_message',
                     tool_input=dict(target='/root', message='gAAAAA' + 'x' * 80))
        self.assertTrue(encrypted_input(event))
        self.assertIsNone(capture_payload(event))
        self.assertFalse(encrypted_input(None))

    def test_status_is_private_and_contains_no_message_input(self):
        import json
        import os
        from pathlib import Path
        import tempfile
        from agent_observer.message_hook import record_status
        with tempfile.TemporaryDirectory() as root:
            directory = Path(root) / 'diagnostics'
            event = dict(hook_event_name='PostToolUse', tool_name='send_message',
                         session_id='selected-session', tool_use_id='call-one',
                         tool_input=dict(message='private message', target='dx'),
                         tool_response='private output')
            record_status(directory, 'rejected', event, 400)
            path = directory / 'message-hook-status.json'
            data = json.loads(path.read_text())
            self.assertEqual(data['status'], 'rejected')
            self.assertEqual(data['httpStatus'], 400)
            self.assertEqual(data['callId'], 'call-one')
            self.assertEqual(set(data), {'at', 'status', 'httpStatus', 'hookEvent', 'toolName', 'sessionId', 'callId'})
            self.assertNotIn('private', path.read_text())
            self.assertEqual(os.stat(path).st_mode & 0o777, 0o600)
            record_status(directory, 'attached', event, 200)
            self.assertEqual(json.loads(path.read_text())['status'], 'attached')
            self.assertEqual(len(list(directory.iterdir())), 1)

    def test_status_does_not_follow_a_directory_symlink(self):
        from pathlib import Path
        import tempfile
        from agent_observer.message_hook import record_status
        with tempfile.TemporaryDirectory() as root:
            outside = Path(root) / 'outside'
            outside.mkdir()
            link = Path(root) / 'link'
            link.symlink_to(outside, target_is_directory=True)
            record_status(link, 'attached')
            self.assertEqual(list(outside.iterdir()), [])
