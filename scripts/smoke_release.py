"""Isolated binary smoke using only owned synthetic records and an explicit config."""
import argparse
import json
import shutil
import sys
from pathlib import Path
import socket
import subprocess
import tempfile
import time
import urllib.request
import urllib.error


def request(port, route, payload=None):
    data = None if payload is None else json.dumps(payload).encode()
    req = urllib.request.Request('http://127.0.0.1:%d%s' % (port, route), data=data, headers={'Content-Type': 'application/json'})
    with urllib.request.urlopen(req, timeout=3) as response:
        return json.load(response)


def run(binary, *args, **kwargs):
    return subprocess.run([str(binary), *args], capture_output=True, text=True, timeout=8, **kwargs)


def free_port():
    with socket.socket() as sock:
        sock.bind(('127.0.0.1', 0))
        return sock.getsockname()[1]


def await_server(process, port):
    for _ in range(80):
        if process.poll() is not None:
            raise RuntimeError('candidate server exited during startup')
        try:
            if request(port, '/healthz')['product'] == 'agent-observer':
                return
        except (OSError, ValueError):
            time.sleep(.05)
    raise RuntimeError('candidate did not start')


def stop(process):
    process.terminate()
    try:
        process.communicate(timeout=5)
    except subprocess.TimeoutExpired:
        process.kill()
        process.communicate()
        raise RuntimeError('candidate failed graceful shutdown')


def smoke(binary):
    with tempfile.TemporaryDirectory(prefix='observer-smoke-') as temporary:
        root = Path(temporary)
        config = root / 'private' / 'config.json'
        port = free_port()
        result = run(binary, 'init', '--user-config', str(config), '--port', str(port))
        assert result.returncode == 0, result.stderr
        assert run(binary, 'init', '--user-config', str(config)).returncode != 0, 'init overwrote config'
        assert run(binary, 'version').returncode == 0
        assert run(binary, 'demo', '--source', str(root / 'forbidden.jsonl')).returncode == 2
        process = subprocess.Popen([str(binary), '--user-config', str(config)], cwd=temporary, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        try:
            await_server(process, port)
            assert request(port, '/api/sessions')['sessions'] == [], 'fresh startup selected real records'
            assert run(binary, 'doctor', '--user-config', str(config), '--connect').returncode == 0
            assert run(binary, '--user-config', str(config)).returncode == 1, 'port collision did not fail'
            for name in ('alpha', 'beta'):
                transcript = root / (name + '.jsonl')
                rows = [
                    {'type':'session_meta','payload':{'id':name}},
                    {'type':'event_msg','timestamp':'2025-01-01T00:00:00Z','payload':{'type':'task_started','turn_id':name+'-turn'}},
                    {'type':'event_msg','payload':{'type':'item_completed','turn_id':name+'-turn','item':{'type':'CommandExecution','id':name+'-tool','command':['echo',name],'exit_code':0}}},
                ]
                transcript.write_text(''.join(json.dumps(row)+'\n' for row in rows))
                payload = {'hook_event_name':'SessionStart','session_id':name,'transcript_path':str(transcript)}
                assert run(binary, 'hook', '--user-config', str(config), input=json.dumps(payload)).returncode == 0
                listing = request(port, '/api/sessions')['sessions']
                assert any(item['sessionId']==name for item in listing), 'shared-port hook registration missing'
                request(port, '/api/sessions/open', {'sessionId':name})
            for name in ('alpha','beta','alpha'):
                state = request(port, '/api/state?session='+name)
                assert state['sessionId']==name and state['turn']==name+'-turn', 'session state mixed'
                assert any(name+'-tool' in n['id'] for n in state['nodes']), 'synthetic tool missing'
                assert all(('beta' if name=='alpha' else 'alpha')+'-tool' not in n['id'] for n in state['nodes'])
        finally:
            stop(process)
        demo_port = free_port()
        demo = subprocess.Popen([str(binary), 'demo', '--port', str(demo_port)], cwd=temporary, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        try:
            await_server(demo, demo_port)
            assert request(demo_port, '/api/sessions')['sessions']==[]
            with urllib.request.urlopen('http://127.0.0.1:%d/?demo=1'%demo_port) as response:
                assert b'syntheticSnapshot' in response.read(), 'embedded sample missing'
        finally:
            stop(demo)
    print('PASS: isolated init/version/doctor, occupied port, shared-port hook, two-session isolation, demo and shutdown')


def first_install(binary):
    """Exercise default profile discovery without inheriting host application settings.

    This is a process-level isolated profile, not a fresh OS or Gatekeeper test.
    The normal repository checks keep their intended environment unchanged.
    """
    if sys.platform not in ('darwin', 'linux'):
        raise RuntimeError('clean-profile verification supports macOS and Linux only')
    with tempfile.TemporaryDirectory(prefix='observer-first-install-') as temporary:
        root = Path(temporary)
        profile = root / 'profile'
        install = root / 'install'
        scratch = root / 'tmp'
        for directory in (profile, install, scratch):
            directory.mkdir()
        installed = install / 'agent-observer'
        shutil.copy2(binary, installed)
        # This child-only HOME retains its standard meaning: an empty user profile.
        # No shell profile, Codex credentials or inherited app/Go/proxy settings.
        env = {'HOME': str(profile), 'PATH': '/usr/bin:/bin', 'TMPDIR': str(scratch), 'LANG': 'C'}
        config_root = profile / ('Library/Application Support' if sys.platform == 'darwin' else '.config')
        config = config_root / 'agent-observer/config.json'
        port = free_port()
        def command(*args):
            return run(installed, *args, env=env, cwd=install)
        assert command('version').returncode == 0
        help_text = command('--help')
        assert help_text.returncode == 0 and 'demo' in help_text.stderr
        assert command('doctor').returncode == 0
        assert not config.exists(), 'read-only doctor created settings'
        assert command('init', '--port', str(port)).returncode == 0
        original = config.read_bytes()
        assert json.loads(original) == {'version': 1, 'port': port}
        assert config.stat().st_mode & 0o777 == 0o600
        assert config.parent.stat().st_mode & 0o777 == 0o700
        assert command('init', '--port', str(free_port())).returncode != 0
        assert config.read_bytes() == original, 'repeat init modified existing config'
        assert command('doctor', '--connect').returncode != 0, 'doctor accepted stopped server'
        transcript = root / 'session with spaces.jsonl'
        row = {'type': 'session_meta', 'payload': {'id': 'first-install'}}
        transcript.write_text(json.dumps(row) + '\n')
        initial = transcript.read_bytes()
        for attempt in range(2):
            process = subprocess.Popen([str(installed)], cwd=install, env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            try:
                await_server(process, port)
                assert request(port, '/api/sessions')['sessions'] == [], 'startup discovered real records'
                assert command('doctor', '--connect').returncode == 0
                collision = command()
                assert collision.returncode == 1 and str(port) in collision.stderr
                try:
                    request(port, '/api/sessions', {'source': str(root/'missing.jsonl')})
                except urllib.error.HTTPError as error:
                    with error:
                        assert error.code == 400
                else:
                    raise AssertionError('missing transcript accepted')
                registered = request(port, '/api/sessions', {'source': str(transcript), 'label': 'First install'})
                assert registered['sessionId'] == 'first-install'
                assert request(port, '/api/state?session=first-install')['sessionId'] == 'first-install'
                assert transcript.read_bytes() == initial, 'registration modified source'
            finally:
                stop(process)
            assert command('doctor', '--connect').returncode != 0
            assert config.read_bytes() == original
        assert not (profile/'.codex').exists(), 'unexpected Codex profile created'
        assert not (profile/'.claude').exists(), 'unexpected Claude profile created'
    print('PASS: clean default profile, system-only PATH, local install, init permissions/no overwrite, saved-port restart, manual connection, source unchanged and stop')


if __name__ == '__main__':
    if not __debug__:
        raise SystemExit('smoke verification requires assertions; remove -O / PYTHONOPTIMIZE')
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('binary',type=Path)
    parser.add_argument('--clean-profile', action='store_true', help='verify a fresh default user profile instead of the explicit-config smoke')
    args=parser.parse_args()
    if args.clean_profile:
        first_install(args.binary.resolve())
    else:
        smoke(args.binary.resolve())
