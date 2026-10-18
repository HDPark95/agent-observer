"""Guards for the diagnostic, not proof of native capture support."""
import argparse
import unittest

from checks.native_message_capture import verify


class NativeProbeTests(unittest.TestCase):
    def setUp(self):
        self.args = argparse.Namespace(session='session', turn='turn',
            event_id='a2a:/root/probe:call-one', sender='subagent:/root/probe', recipient='agent')
        self.node = dict(id=self.args.event_id, kind='a2a', source=self.args.sender,
            target=self.args.recipient, body='known probe', bodySource='native-hook')
        self.state = dict(sessionId='session', turn='turn', nodes=[self.node])

    def test_supplied_body_cannot_pass_as_native_capture(self):
        for origin, expected in [('native-hook', True), (None, True),
                                 ('sender-local', False), ('observer-mailbox', False)]:
            with self.subTest(origin=origin):
                self.node['bodySource'] = origin
                report, passed = verify(self.state, self.args, 'known probe', None)
                self.assertEqual(passed, expected)
                self.assertNotIn('body', report)

    def test_missing_and_wrong_bodies_fail(self):
        for body in [None, '', 'other probe']:
            with self.subTest(body=body):
                self.node['body'] = body
                self.assertFalse(verify(self.state, self.args, 'known probe', None)[1])

    def test_different_event_scope_is_rejected(self):
        for owner, key in [(self.state, 'sessionId'), (self.state, 'turn'),
                           (self.node, 'id'), (self.node, 'source'), (self.node, 'target')]:
            original = owner[key]
            owner[key] = 'other'
            with self.subTest(key=key), self.assertRaises(ValueError):
                verify(self.state, self.args, 'known probe', None)
            owner[key] = original

    def test_unrelated_hook_cannot_be_used_as_evidence(self):
        hook = dict(sessionId='session', callId='other', status='encrypted-input')
        report, _ = verify(self.state, self.args, 'known probe', hook)
        self.assertFalse(report['hookMatchesEvent'])
        self.assertNotIn('hookStatus', report)
        hook['callId'] = 'call-one'
        report, _ = verify(self.state, self.args, 'known probe', hook)
        self.assertTrue(report['hookMatchesEvent'])
        self.assertEqual(report['hookStatus'], 'encrypted-input')


if __name__ == '__main__':
    unittest.main()
