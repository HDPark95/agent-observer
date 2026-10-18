import json
import tempfile
import unittest
from pathlib import Path
from agent_observer.catalog import Catalog
from agent_observer.sessions import Sessions

class CatalogTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory();self.addCleanup(self.tmp.cleanup)
        self.root=Path(self.tmp.name);self.day=self.root/'2026/10/15';self.day.mkdir(parents=True)
    def write(self,sid,**meta):
        p=self.day/('rollout-'+sid+'.jsonl')
        p.write_text(json.dumps({'type':'session_meta','payload':dict(id=sid,cwd='/work/demo',**meta)})+'\nPRIVATE BODY NOT JSON\n')
        return p
    def test_catalog_reads_only_headers_and_opens_on_selection(self):
        self.write('session-a');self.write('session-b');self.write('child',agent_path='/root/ux',parent_thread_id='session-a')
        registry=Sessions(self.root)
        items=registry.listing()['sessions']
        self.assertEqual(len(items),2)
        self.assertEqual(registry.entries,{})
        self.assertNotIn('PRIVATE',json.dumps(items))
        self.assertNotIn('source',items[0])
        registry.open_session({'sessionId':'session-a'})
        self.assertEqual(set(registry.entries),{'session-a'})
        self.assertEqual(registry.state('session-a')['sessionId'],'session-a')
    def test_revalidate_header_on_open(self):
        p=self.write('session-a');registry=Sessions(self.root);registry.listing()
        p.write_text(json.dumps({'type':'session_meta','payload':{'id':'wrong'}})+'\n')
        with self.assertRaises(ValueError):registry.open_session({'sessionId':'session-a'})
        self.assertEqual(registry.entries,{})
    def test_symlinks_and_malformed_headers_ignored(self):
        original=self.write('real')
        (self.day/'rollout-alias.jsonl').symlink_to(original)
        (self.day/'rollout-invalid.jsonl').write_text('invalid')
        self.assertEqual(set(Catalog(self.root).read()),{'real'})
    def test_duplicate_identity_is_not_guessed(self):
        p=self.write('same');(self.day/'rollout-copy.jsonl').write_bytes(p.read_bytes())
        self.assertEqual(Catalog(self.root).read(),{})
    def test_missing_directory(self):
        self.assertEqual(Catalog(self.root/'missing').read(),{})

    def test_hook_registration_stays_metadata_only_until_clicked(self):
        p=self.write('hook-session');registry=Sessions()
        registry.register({'source':str(p),'sessionId':'hook-session','metadataOnly':True})
        self.assertEqual(registry.entries,{})
        self.assertFalse(registry.listing()['sessions'][0]['registered'])
        with self.assertRaises(ValueError): registry.state('hook-session')
        registry.open_session({'sessionId':'hook-session'})
        self.assertEqual(set(registry.entries),{'hook-session'})

    def test_titles_refresh_without_opening_bodies_and_manual_name_wins(self):
        p=self.write('session-a');self.write('session-b')
        index=self.root.parent/(self.root.name+'-index.jsonl')
        self.addCleanup(index.unlink, missing_ok=True)
        registry=Sessions(self.root)
        registry.titles.path=index
        index.write_text(json.dumps({'id':'session-a','thread_name':'화면 동작 검사'})+'\ninvalid\n')
        items={s['sessionId']:s for s in registry.listing()['sessions']}
        self.assertEqual(items['session-a']['label'],'화면 동작 검사')
        self.assertEqual(items['session-a']['project'],'demo')
        self.assertEqual(items['session-b']['label'],'제목 없는 대화 · ession-b')
        self.assertEqual(registry.entries,{})
        registry.open_session({'sessionId':'session-a'})
        self.assertEqual(registry.state('session-a')['sessionLabel'],'화면 동작 검사')
        with index.open('a') as f:f.write(json.dumps({'id':'session-a','thread_name':'변경된 작업 제목'})+'\n')
        self.assertEqual(registry.state('session-a')['sessionLabel'],'변경된 작업 제목')
        registry.register({'source':str(p),'label':'직접 정한 이름'})
        self.assertEqual(registry.state('session-a')['sessionLabel'],'직접 정한 이름')
        self.assertEqual(next(s for s in registry.listing()['sessions'] if s['sessionId']=='session-a')['label'],'직접 정한 이름')
