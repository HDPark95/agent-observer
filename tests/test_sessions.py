import json
import tempfile
import threading
import unittest
import urllib.request
import urllib.error
from pathlib import Path
from agent_observer.sessions import Sessions
from agent_observer.server import make_server
from agent_observer.session_hook import registration

class SessionTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory();self.addCleanup(self.tmp.cleanup)
        self.registry=Sessions()
        self.a=self.fixture('session-a');self.b=self.fixture('session-b')
    def fixture(self,sid,parent=None):
        p=Path(self.tmp.name)/(sid+'.jsonl')
        p.write_text(json.dumps({'type':'session_meta','payload':{'id':sid,'parent_thread_id':parent,'cwd':'/work/demo'}})+'\n'+json.dumps({'type':'event_msg','timestamp':'2026-10-15T01:00:00Z','payload':{'type':'task_started','turn_id':'same-turn'}})+'\n')
        return p
    def test_register_dedupe_and_isolation(self):
        for path in (self.a,self.b):self.registry.register({'source':str(path)})
        self.registry.register({'source':str(self.a),'label':'UX session'})
        self.assertEqual(len(self.registry.listing()['sessions']),2)
        a=self.registry.capture('session-a');b=self.registry.capture('session-b')
        a.capture_hook(dict(sessionId='session-a',callId='shared',body='A only',target='dx'))
        self.assertEqual(b.pending,{})
        self.assertEqual(self.registry.state('session-b')['sessionId'],'session-b')
        with self.assertRaises(ValueError):self.registry.publish({'turn':'same-turn','body':'ambiguous'})
        with self.assertRaises(ValueError):self.registry.capture('unregistered')
    def test_reject_wrong_identity_child_and_non_transcript(self):
        with self.assertRaises(ValueError):self.registry.register({'source':str(self.a),'sessionId':'wrong'})
        child=self.fixture('child',parent='session-a')
        with self.assertRaises(ValueError):self.registry.register({'source':str(child)})
        invalid=Path(self.tmp.name)/'invalid.jsonl';invalid.write_text('{}\n')
        with self.assertRaises(ValueError):self.registry.register({'source':str(invalid)})
        alias=Path(self.tmp.name)/'alias.jsonl';alias.symlink_to(self.a)
        with self.assertRaises(ValueError):self.registry.register({'source':str(alias)})
    def test_empty_server_and_unknown_session(self):
        self.assertEqual(self.registry.state()['nodes'],[])
        self.assertEqual(self.registry.listing(),{'sessions':[]})
        with self.assertRaises(ValueError):self.registry.state('missing')
    def test_registration_hook_ignores_prompt_and_child(self):
        event=dict(hook_event_name='UserPromptSubmit',session_id='session-a',transcript_path=str(self.a),prompt='not collected')
        self.assertEqual(registration(event),{'source':str(self.a),'sessionId':'session-a','metadataOnly':True})
        self.assertIsNone(registration(dict(event,hook_event_name='SessionStart',source='fork')))
    def test_http_two_sessions_and_tab_selection(self):
        # Ephemeral loopback integration server; only synthetic transcripts are registered.
        server=make_server(port=0)
        self.addCleanup(server.server_close)
        worker=threading.Thread(target=server.serve_forever,daemon=True);worker.start()
        self.addCleanup(server.shutdown)
        base='http://127.0.0.1:'+str(server.server_port)
        def request(path,data=None,origin=None):
            headers={'Content-Type':'application/json'}
            if origin:headers['Origin']=origin
            req=urllib.request.Request(base+path,data=json.dumps(data).encode() if data is not None else None,headers=headers)
            with urllib.request.urlopen(req,timeout=3) as r:return json.load(r)
        for p in (self.a,self.b):request('/api/sessions',{'source':str(p)},base)
        self.assertEqual(len(request('/api/sessions')['sessions']),2)
        self.assertEqual(request('/api/state?session=session-a')['sessionId'],'session-a')
        self.assertEqual(request('/api/state?session=session-b')['sessionId'],'session-b')
        self.assertEqual(request('/api/state?session=session-a')['sessionId'],'session-a')
        with self.assertRaises(urllib.error.HTTPError) as error:request('/api/sessions',{'source':str(self.a)},'https://outside.example')
        self.assertEqual(error.exception.code,403)
