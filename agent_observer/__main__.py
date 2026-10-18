import argparse
from pathlib import Path
from .server import serve


def main():
    parser = argparse.ArgumentParser(description='Agent Observer: local Codex event graph')
    parser.add_argument('--source', action='append', default=[], type=Path, help='Explicit transcript; repeat for multiple sessions, or register later')
    parser.add_argument('--port', type=int, default=8767)
    parser.add_argument('--agent-source', action='append', type=Path, default=[], help='Explicit child transcript for A2A metadata; repeatable')
    parser.add_argument('--catalog-root', type=Path, default=None, help='Recent session headers to list; no transcript body read until opened')
    args = parser.parse_args()
    if any(not path.is_file() for path in args.agent_source):
        parser.error('--agent-source must be an existing transcript file')
    if any(not path.is_file() for path in args.source):
        parser.error('--source must be an existing transcript file')
    if not 1024 <= args.port <= 65535:
        parser.error('--port must be between 1024 and 65535')
    serve([path.resolve() for path in args.source], args.port, [path.resolve() for path in args.agent_source], args.catalog_root)


if __name__ == '__main__':
    main()
