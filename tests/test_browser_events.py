import json
import unittest
from pathlib import Path
from agent_observer.browser_events import browser_metadata, browser_actions
from agent_observer.events import Observer


class BrowserEventTests(unittest.TestCase):
    def item(self, host='docs.example.test', wrapped=False):
        result={'content':[{'type':'text','text':f'Browser tab: 12, Title: "PRIVATE title", URL: "https://{host}/PRIVATE?token=SECRET".\n0 AXWebArea PRIVATE body https://unrelated.test/'}]}
        if wrapped:
            result={'content':[{'type':'text','text':json.dumps(result)}]}
        return {'type':'McpToolCall','id':'browser-1','server':'cua_repl','tool':'js',
                'arguments':{'code':"await tab.getAXState();",'title':'PRIVATE title'},
                'status':'completed','result':result}

    def test_dynamic_hosts_and_wrapped_results(self):
        for host in ['app.slack.com','app.clickup.com','docs.example.test']:
            for wrapped in [False,True]:
                m=browser_metadata(self.item(host,wrapped))
                self.assertEqual(m['label'],host+' · 화면 읽기')
                self.assertNotIn('PRIVATE',json.dumps(m))
                self.assertNotIn('SECRET',json.dumps(m))
                self.assertNotIn('unrelated',json.dumps(m))

    def test_operations_are_not_guessed_from_titles_or_literals(self):
        self.assertEqual(browser_actions('await tab.click(3);await tab.getAXState();'),['클릭','화면 읽기'])
        self.assertEqual(browser_actions('const s="tab.click()"; // tab.goto()\n'),[])
        item=self.item();item['arguments']={'title':'ClickUp 문서 읽기'};item['result']={}
        self.assertEqual(browser_metadata(item)['label'],'브라우저 작업')

    def test_failed_call_and_parent_normalization(self):
        observer=Observer(Path('/unused'));observer.turn='t'
        item=self.item();item['status']='failed'
        observer.consume({'type':'event_msg','timestamp':'2026-10-15T00:00:00Z',
                          'payload':{'type':'item_completed','turn_id':'t','item':item}})
        node=observer.nodes['browser-1']
        self.assertEqual(node['kind'],'tool');self.assertEqual(node['parent'],'agent')
        self.assertEqual(node['status'],'오류');self.assertEqual(node['toolName'],'cua_repl.js')
        self.assertNotIn('PRIVATE',json.dumps(node))

    def test_non_browser_mcp_unchanged(self):
        item=self.item();item['server']='other'
        self.assertIsNone(browser_metadata(item))
