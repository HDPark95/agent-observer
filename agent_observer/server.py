"""Loopback-only HTTP adapter for Agent Observer."""
import json
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import urlsplit, parse_qs
from .sessions import Sessions

ROOT = Path(__file__).parent

def make_server(sources=(), port=8767, agent_sources=(), catalog_root=None):
    sessions = Sessions(catalog_root)
    if isinstance(sources, Path):
        sources = [sources]
    for source in sources:
        sessions.register({"source": str(source)}, agent_sources)
    class Handler(BaseHTTPRequestHandler):
        def do_POST(self):
            allowed = (f'127.0.0.1:{port}', f'localhost:{port}')
            if self.headers.get('Host') not in allowed or self.headers.get('Origin') not in (None, f'http://127.0.0.1:{port}', f'http://localhost:{port}'):
                self.send_error(403)
                return
            if self.path not in ('/api/messages', '/api/message-hooks', '/api/sessions', '/api/sessions/open'):
                self.send_error(404)
                return
            try:
                size = int(self.headers.get('Content-Length', '0'))
                if not 0 < size <= 80000 or self.headers.get('Content-Type') != 'application/json':
                    raise ValueError('잘못된 요청 형식')
                payload = json.loads(self.rfile.read(size))
                if self.path == '/api/sessions/open':
                    result = sessions.open_session(payload)
                elif self.path == '/api/sessions':
                    result = sessions.register(payload)
                else:
                    result = sessions.publish(payload, hook=self.path == '/api/message-hooks')
                content = json.dumps(result).encode()
            except (ValueError, UnicodeError, OSError, TypeError):
                self.send_error(400, 'Invalid message attachment')
                return
            self.send_response(200)
            self.send_header('Content-Type', 'application/json')
            self.send_header('Cache-Control', 'no-store')
            self.end_headers()
            self.wfile.write(content)

        def do_GET(self):
            if self.headers.get('Host') not in (f'127.0.0.1:{port}', f'localhost:{port}'):
                self.send_error(403)
                return
            route = urlsplit(self.path)
            if route.path in ('/api/state', '/api/sessions'):
                try:
                    sid = parse_qs(route.query).get('session', [None])[0]
                    result = sessions.listing() if route.path == '/api/sessions' else sessions.state(sid)
                    content = json.dumps(result, ensure_ascii=False).encode()
                except ValueError:
                    self.send_error(404, 'Unknown session')
                    return
                mime = 'application/json; charset=utf-8'
            elif route.path == '/':
                content = (ROOT / 'index.html').read_bytes()
                mime = 'text/html; charset=utf-8'
            else:
                self.send_error(404)
                return
            self.send_response(200)
            self.send_header('Content-Type', mime)
            self.send_header('Cache-Control', 'no-store')
            self.send_header('X-Content-Type-Options', 'nosniff')
            self.send_header('Content-Security-Policy', "default-src 'self'; script-src 'unsafe-inline'; style-src 'unsafe-inline'; connect-src 'self'; frame-ancestors 'none'")
            self.end_headers()
            self.wfile.write(content)
        def log_message(self, *args):
            pass
    server = ThreadingHTTPServer(('127.0.0.1', port), Handler)
    port = server.server_port
    server.sessions = sessions
    return server


def serve(sources=(), port=8767, agent_sources=(), catalog_root=None):
    server = make_server(sources, port, agent_sources, catalog_root)
    print(f'Agent Observer: http://127.0.0.1:{port}', flush=True)
    server.serve_forever()
