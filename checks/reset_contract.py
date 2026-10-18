"""Frozen acceptance contract for transcript reset recovery."""
import json
import tempfile
from pathlib import Path
from agent_observer.events import Observer

for replace in (False, True):
    with tempfile.TemporaryDirectory() as temporary:
        path = Path(temporary) / 'events.jsonl'
        row = {'timestamp':'2026-10-15T00:00:00Z','type':'event_msg','payload':{'type':'task_started','turn_id':'before'}}
        path.write_text(json.dumps(row)+'\n')
        observer = Observer(path)
        before = observer.read()
        if replace:
            replacement = path.with_suffix('.new')
            replacement.write_text('')
            replacement.replace(path)
        else:
            path.write_text('')
        after = observer.read()
        assert after['nodes'] == [] and after['turn'] is None
        assert after['started'] is None and after['lastEvent'] is None
        assert after['revision'] > before['revision'], 'UI must see an empty-source reset'
        assert observer.read()['revision'] == after['revision'], 'empty polling must be stable'
        row['payload']['turn_id'] = 'after'
        raw = json.dumps(row)
        path.write_text(raw)
        assert observer.read()['nodes'] == []
        with path.open('a') as output:
            output.write('\n')
        assert observer.read()['turn'] == 'after'
print('PASS: truncate, replace, empty polling, partial-line recovery')
