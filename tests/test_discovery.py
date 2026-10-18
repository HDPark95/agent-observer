import json
import tempfile
import unittest
from pathlib import Path
from agent_observer.agents import discover_child, child_candidates
from agent_observer.events import Observer

THREAD='11111111-2222-3333-4444-555555555555'
PARENT='aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee'
STAMP='2026-10-15T01:00:00Z'


class DiscoveryTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory();self.addCleanup(self.temp.cleanup)
        self.directory=Path(self.temp.name)
        self.source=self.directory/'parent.jsonl';self.source.touch()
        self.child=self.directory/('rollout-test-'+THREAD+'.jsonl')

    def write_child(self, parent=PARENT, path='/root/child', destination=None):
        rows=[{'type':'session_meta','ordinal':0,'payload':{'id':THREAD,'parent_thread_id':parent,
            'agent_path':path,'subagent_history_start_ordinal':1}},
            {'type':'response_item','ordinal':1,'timestamp':STAMP,'payload':{'type':'function_call',
            'name':'send_message','namespace':'collaboration','call_id':'call-1',
            'arguments':json.dumps({'target':'/root','message':'PRIVATE'})}}]
        (destination or self.child).write_text(''.join(json.dumps(r)+'\n' for r in rows))

    def find(self):
        return discover_child(self.source,THREAD,'/root/child',PARENT,STAMP)

    def test_parent_and_path_binding(self):
        self.write_child(parent='wrong');self.assertIsNone(self.find())
        self.write_child(path='/root/other');self.assertIsNone(self.find())
        self.write_child();self.assertIsNotNone(self.find())

    def test_ambiguous_and_symlink_rejected(self):
        self.write_child()
        other=self.directory/('rollout-other-'+THREAD+'.jsonl')
        self.write_child(destination=other);self.assertIsNone(self.find())
        other.unlink();self.child.unlink();other.write_text('{}\n');self.child.symlink_to(other)
        self.assertIsNone(self.find())

    def test_replacement_identity_checked_again(self):
        self.write_child();reader=self.find();self.assertEqual(len(reader.read()),1)
        replacement=self.directory/'replacement';self.write_child(parent='wrong',destination=replacement)
        replacement.replace(self.child);self.assertEqual(reader.read(),[])

    def test_late_file_auto_connect_and_new_turn_prune(self):
        observer=Observer(self.source)
        observer.consume({'type':'session_meta','payload':{'id':PARENT}})
        observer.consume({'type':'event_msg','timestamp':STAMP,'payload':{'type':'task_started','turn_id':'turn'}})
        observer.consume_agent({'agent_path':'/root/child','agent_thread_id':THREAD,'kind':'started'},STAMP)
        observer.merge_messages();self.assertEqual(observer.auto_pending,1)
        self.write_child();observer.merge_messages()
        self.assertEqual(len(observer.auto_sources),1)
        self.assertEqual(sum(n['kind']=='a2a' for n in observer.nodes.values()),1)
        self.assertEqual(next(n for n in observer.nodes.values() if n['kind']=='a2a')['body'], 'PRIVATE')
        observer.consume({'type':'event_msg','timestamp':STAMP,'payload':{'type':'task_started','turn_id':'new'}})
        observer.merge_messages();self.assertEqual(observer.auto_sources,{})

    def test_date_boundary_and_invalid_ids(self):
        source=self.directory/'2026/10/15/parent.jsonl';source.parent.mkdir(parents=True);source.touch()
        dest=self.directory/('2026/10/16/rollout-test-'+THREAD+'.jsonl');dest.parent.mkdir(parents=True)
        self.write_child(destination=dest)
        self.assertEqual(child_candidates(source,THREAD,'2026-10-16T00:00:01Z'),[dest])
        self.assertEqual(child_candidates(source,'../../*',STAMP),[])
