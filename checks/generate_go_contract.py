"""Generate synthetic compatibility fixtures from the retained Python reference."""
import json
import sys
import tempfile
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from agent_observer.events import Observer, command_identity

STAMP='2026-10-15T01:00:00Z'
def row(kind, **payload):return dict(type=kind,timestamp=STAMP,payload=payload)
def line(r):return json.dumps(r,ensure_ascii=False)+'\n'
def item(item_type,id,**kw):return row('event_msg',type='item_completed',turn_id='turn',item=dict(type=item_type,id=id,**kw))
def clean(v):
 if isinstance(v,dict):return {k:clean(x) for k,x in v.items() if k not in ('seen','checkedAt','revision')}
 if isinstance(v,list):return [clean(x) for x in v]
 return v
steps=[]
with tempfile.TemporaryDirectory() as tmp:
 root=Path(tmp)/'root.jsonl';child=Path(tmp)/'child.jsonl';root.touch();child.touch();o=Observer(root,[child])
 def step(name,target,content,mode='append'):
  p=root if target=='root' else child
  with p.open('a' if mode=='append' else 'w') as f:f.write(content)
  state=clean(o.read());steps.append(dict(name=name,target=target,content=content,mode=mode,expected=state))
 step('start','root',line(row('session_meta',id='root-thread'))+line(row('event_msg',type='task_started',turn_id='turn')))
 for name in ('ux','dx'):
  step('spawn '+name,'root',line(item('SubAgentActivity','spawn-'+name,agent_path='/root/'+name,agent_thread_id=name,kind='started')))
 commands=['python3 -m unittest discover -s tests -v','uv run --offline --no-sync python -m tools.observation_workflow finish','node checks/initial_render.cjs','pnpm run typecheck','rg --files','ls','python3 file.py','git status','echo SECRET','cat /skills/demo/SKILL.md']
 for i,cmd in enumerate(commands):
  step('command '+str(i),'root',line(item('CommandExecution','cmd-'+str(i),command=['sh','-c',cmd],exit_code=0,parsed_cmd=[dict(type='read',path='/skills/demo/SKILL.md')] if i==9 else [])))
 for i,kind in enumerate(['FileChange','ImageView','WebSearch','AgentMessage']):step(kind,'root',line(item(kind,'generic-'+str(i),phase='final')))
 step('browser','root',line(item('McpToolCall','browse',server='cua_repl',tool='js',arguments={'code':"await tab.goto('https://app.slack.com/x'); await tab.getAXState();"},result={'content':[{'type':'text','text':'Browser tab: 1, Title: "PRIVATE", URL: "https://app.slack.com/x".\nBODY'}]},status='completed')))
 step('generic mcp','root',line(item('McpToolCall','mcp',server='service',tool='read',status='completed')))
 step('call starts','root',line(row('response_item',type='function_call',call_id='wrap',name='functions.exec',arguments='SECRET')))
 step('call returns','root',line(row('response_item',type='function_call_output',call_id='wrap',output='SECRET')))
 child_header=row('session_meta',id='ux',agent_path='/root/ux',parent_thread_id='root-thread',subagent_history_start_ordinal=5)
 inherited=row('response_item',type='function_call',namespace='collaboration',name='send_message',call_id='inherited',arguments=json.dumps({'target':'dx','message':'inherited'}));inherited['ordinal']=2
 actual=row('response_item',type='function_call',namespace='collaboration',name='send_message',call_id='hello',arguments=json.dumps({'target':'dx','message':'안녕\n검사 완료 token=private'}));actual['ordinal']=5
 step('child message','child',line(child_header)+line(inherited)+line(actual))
 activity=item('CommandExecution','child-tool',command=['sh','-c','cat /skills/demo/SKILL.md'],exit_code=0,parsed_cmd=[dict(type='read',path='/skills/demo/SKILL.md')]);activity['ordinal']=6
 step('child tool skill','child',line(activity))
 ret=row('response_item',type='function_call_output',call_id='hello',output='SECRET');ret['ordinal']=7
 step('message return','child',line(ret))
 encrypted=dict(actual);encrypted['ordinal']=8;encrypted['payload']=dict(actual['payload'],call_id='encrypted',arguments=json.dumps({'target':'dx','message':'gAAAAA'+'a'*120}))
 step('encrypted body','child',line(encrypted))
 step('idle','root','')
 step('child truncate','child','',mode='replace')
 partial=line(item('CommandExecution','partial',command=['sh','-c','node --test'],exit_code=1))
 step('partial line','root',partial[:-1]);step('completed line','root','\n')
 step('complete agent','root',line(item('SubAgentActivity','done',agent_path='/root/ux',agent_thread_id='ux',kind='completed')))
 step('turn reset','root',line(row('event_msg',type='task_started',turn_id='new-turn')))
 step('file truncate','root',line(row('session_meta',id='root-thread'))+line(row('event_msg',type='task_started',turn_id='replaced')),mode='replace')
Path('internal/observer/testdata').mkdir(parents=True,exist_ok=True)
Path('internal/observer/testdata/python_contract.json').write_text(json.dumps(steps,ensure_ascii=False,indent=2))
print('synthetic contract phases:',len(steps))
