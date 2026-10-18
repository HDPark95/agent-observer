"""Register one explicit Codex transcript with the running shared Observer."""
import argparse
import json
import urllib.request
from pathlib import Path


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--source', required=True, type=Path)
    parser.add_argument('--label')
    args = parser.parse_args()
    payload = dict(source=str(args.source.expanduser().absolute()), label=args.label)
    request = urllib.request.Request('http://127.0.0.1:8767/api/sessions',
        data=json.dumps(payload).encode(), headers={'Content-Type':'application/json'}, method='POST')
    with urllib.request.urlopen(request, timeout=5) as response:
        result = json.load(response)
    print('http://127.0.0.1:8767/?session=' + result['sessionId'])


if __name__ == '__main__':
    main()
