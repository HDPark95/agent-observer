"""Build local release candidates from an explicit source allowlist, never Git history."""
import argparse
import gzip
import hashlib
import io
import json
import os
import platform
from pathlib import Path
import re
import stat
import subprocess
import tarfile
import tempfile

ROOT = Path(__file__).resolve().parents[1]
MAX_FILE = 32 * 1024 * 1024
MAX_TOTAL = 96 * 1024 * 1024
MAX_ARCHIVE = 100 * 1024 * 1024
MAX_ENTRIES = 10000
MANIFEST = 'SOURCE-MANIFEST.json'
FORBIDDEN = {'.git', '.local', 'bin', '__pycache__', '.venv', 'dist', 'node_modules', '.codex', '.claude', '.ssh', '.aws', '.runs', '.state'}
PRIVATE_SUFFIXES = ('.jsonl', '.token', '.pem', '.key', '.p12', '.pfx', '.log')
EMAIL = re.compile(rb'(?i)\b[a-z0-9._%+-]+@([a-z0-9.-]+\.[a-z]{2,})\b')


def digest(data):
    return hashlib.sha256(data).hexdigest()


def safe_name(name):
    if not isinstance(name, str) or not name or '\\' in name or any(p in ('', '.', '..') for p in name.split('/')):
        raise ValueError('invalid package path')
    return name


def validate_metadata(meta, generated=False):
    fields = {'schema_version', 'version', 'repository', 'license_spdx', 'license_sha256', 'supported_targets'}
    if generated:
        fields |= {'publication_status', 'source_sha256'}
    if not isinstance(meta, dict) or set(meta) - fields or meta.get('schema_version') != 1:
        raise ValueError('invalid release schema')
    version = meta.get('version')
    if not isinstance(version, str) or not re.fullmatch(r'[0-9]+\.[0-9]+\.[0-9]+(?:-[a-z0-9.]+)?', version):
        raise ValueError('invalid release version')
    for key, pattern in [('repository', r'[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+'), ('license_spdx', r'[A-Za-z0-9.-]+'), ('license_sha256', r'[a-f0-9]{64}'), ('source_sha256', r'[a-f0-9]{64}')]:
        value = meta.get(key)
        if value is not None and (not isinstance(value, str) or not re.fullmatch(pattern, value)):
            raise ValueError('invalid release setting: ' + key)
    if meta.get('supported_targets') != ['darwin/arm64']:
        raise ValueError('unverified release target')
    if generated and meta.get('publication_status') not in ('eligible-for-review', 'blocked-owner-license-pending', 'blocked-repository-pending', 'blocked-license-pending'):
        raise ValueError('invalid publication status')
    scan_source('release metadata', json.dumps(meta).encode())
    return meta


def release_metadata(root, purpose, files=None):
    data = files['release/release.json'][0] if files is not None else read_source(root, 'release/release.json')
    meta = validate_metadata(json.loads(data))
    status = publication_status(meta, files)
    if purpose == 'publish' and status != 'eligible-for-review':
        raise ValueError('publication blocked: repository and reviewed, allowlisted LICENSE with matching SHA-256 required')
    return dict(meta, publication_status=status)


def publication_status(meta, files):
    licensed = bool(meta.get('license_spdx') and files is not None and 'LICENSE' in files and digest(files['LICENSE'][0]) == meta.get('license_sha256'))
    if not meta.get('repository'):
        return 'blocked-repository-pending' if licensed else 'blocked-owner-license-pending'
    return 'eligible-for-review' if licensed else 'blocked-license-pending'


def source_paths(root):
    names = [line.strip() for line in (root / 'release/source-files.txt').read_text().splitlines() if line.strip() and not line.startswith('#')]
    if not names or len(names)>MAX_ENTRIES or len(set(n.casefold() for n in names)) != len(names):
        raise ValueError('empty or duplicate source allowlist')
    for name in names:
        safe_name(name)
        normalized = name.casefold()
        if normalized.startswith('docs/') and not normalized.startswith('docs/public/'):
            raise ValueError('private document subtree: ' + name)
        if normalized.startswith('internal/') and normalized.endswith('.json') and normalized != 'internal/observer/testdata/event_contract.json':
            raise ValueError('unreviewed fixture: ' + name)
        parts = normalized.split('/')
        if normalized == MANIFEST.casefold() or any(p in FORBIDDEN or p.startswith('.env') for p in parts) or normalized.endswith(PRIVATE_SUFFIXES):
            raise ValueError('forbidden source file: ' + name)
    return sorted(names)


def check_public_tree(root):
    """Check a public checkout's tracked files, not private development history."""
    top = subprocess.check_output(['git', 'rev-parse', '--show-toplevel'], cwd=root, text=True).strip()
    if Path(top).resolve() != root.resolve():
        raise ValueError('public inventory check must run at its Git repository root')
    tracked = subprocess.check_output(['git', 'ls-files', '--cached', '-z'], cwd=root).decode().split('\0')
    actual = set(tracked) - {''}
    expected = set(source_paths(root))
    if actual != expected:
        raise ValueError(f'public inventory mismatch: {len(actual - expected)} unlisted, {len(expected - actual)} missing files; review release/source-files.txt')
    return len(source_snapshot(root))


def read_source(root, name):
    current = root
    for part in name.split('/'):
        current = current / part
        if current.is_symlink():
            raise ValueError('source symlink: ' + name)
    info = current.stat()
    if not stat.S_ISREG(info.st_mode) or info.st_size > MAX_FILE:
        raise ValueError('invalid source file: ' + name)
    with current.open('rb') as handle:
        data = handle.read(MAX_FILE + 1)
    if len(data) > MAX_FILE:
        raise ValueError('source file too large: ' + name)
    return data


def scan_source(name, data):
    # Heuristics complement review; matches are intentionally not printed.
    patterns = [rb'-----BEGIN (?:RSA |EC |OPENSSH )?PRIVATE KEY-----', rb'(?i)(?:ghp_|github_pat_)[a-z0-9_]{25,}', rb'/(?:Users|home)/(?!<|example/|demo/|test/)[A-Za-z0-9._-]+/', rb'/(?:private/)?var/folders/']
    if any(re.search(pattern, data) for pattern in patterns) or contains_unreviewed_email(data):
        raise ValueError('sensitive-source review required: ' + name)


def contains_unreviewed_email(data):
    """Reserved documentation domains are synthetic; other addresses need review."""
    for match in EMAIL.finditer(data):
        domain = match.group(1).lower()
        reserved = domain.endswith((b'.example', b'.test', b'.invalid')) or any(
            domain == base or domain.endswith(b'.' + base)
            for base in (b'example.com', b'example.org', b'example.net'))
        if not reserved:
            return True
    return False


def source_snapshot(root):
    files = {}
    total = 0
    for name in source_paths(root):
        data = read_source(root, name)
        total += len(data)
        if total > MAX_TOTAL:
            raise ValueError('source package too large')
        scan_source(name, data)
        files[name] = (data, 0o644)
    return files


def archive_bytes(files, metadata):
    validate_metadata(metadata, generated=True)
    if len(files)>MAX_ENTRIES or any(len(data)>MAX_FILE for data, _ in files.values()):
        raise ValueError('package exceeds file limits')
    manifest = dict(release=metadata, files={name: {'sha256': digest(data), 'size': len(data), 'mode': mode} for name, (data, mode) in sorted(files.items())})
    entries = dict(files)
    entries[MANIFEST] = (json.dumps(manifest, sort_keys=True, indent=2).encode() + b'\n', 0o644)
    if sum(len(data) for data, _ in entries.values()) > MAX_TOTAL:
        raise ValueError('package exceeds expanded size limit')
    output = io.BytesIO()
    prefix = 'agent-observer-' + metadata['version'] + '/'
    with gzip.GzipFile(fileobj=output, mode='wb', filename='', mtime=0) as gz:
        with tarfile.open(fileobj=gz, mode='w', format=tarfile.USTAR_FORMAT) as archive:
            for name, (data, mode) in sorted(entries.items()):
                safe_name(name)
                entry = tarfile.TarInfo(prefix + name)
                entry.size, entry.mode, entry.mtime = len(data), mode, 0
                archive.addfile(entry, io.BytesIO(data))
    data = output.getvalue()
    if len(data)>MAX_ARCHIVE:
        raise ValueError("package exceeds compressed size limit")
    return data


def write_artifact(output, name, data):
    output.mkdir(parents=True, exist_ok=True)
    path = output / name
    checksum = output / (name + '.sha256')
    if path.exists() or checksum.exists():
        raise ValueError('artifact exists; use a new output directory')
    with path.open('xb') as handle:
        handle.write(data)
    with checksum.open('x') as handle:
        handle.write(digest(data) + '  ' + name + '\n')
    return path


def verify_archive(path):
    if path.stat().st_size > MAX_ARCHIVE:
        raise ValueError('archive too large')
    data = path.read_bytes()
    expected = path.with_name(path.name + '.sha256').read_text().strip()
    if expected != digest(data) + '  ' + path.name:
        raise ValueError('archive checksum mismatch')
    entries, prefix, total, seen = {}, None, 0, set()
    with tarfile.open(fileobj=io.BytesIO(data), mode='r:gz') as archive:
        for item in archive:
            safe_name(item.name)
            head, sep, name = item.name.partition('/')
            if not sep or not item.isfile() or item.size > MAX_FILE or item.mode not in (0o644, 0o755):
                raise ValueError('invalid archive member')
            if prefix is not None and head != prefix:
                raise ValueError('mixed archive prefixes')
            prefix = head
            if name.casefold() in seen or len(seen)>MAX_ENTRIES:
                raise ValueError('duplicate archive member')
            seen.add(name.casefold())
            total += item.size
            if total > MAX_TOTAL:
                raise ValueError('expanded archive too large')
            entries[name] = (archive.extractfile(item).read(), item.mode)
    manifest = json.loads(entries.pop(MANIFEST)[0])
    if not isinstance(manifest, dict) or set(manifest)!={'release','files'} or not isinstance(manifest['files'],dict):
        raise ValueError('invalid manifest schema')
    validate_metadata(manifest['release'], generated=True)
    if prefix != 'agent-observer-' + manifest['release']['version'] or set(entries) != set(manifest['files']):
        raise ValueError('archive membership mismatch')
    for name, (data, mode) in entries.items():
        if manifest['files'][name] != {'sha256': digest(data), 'size': len(data), 'mode': mode}:
            raise ValueError('archive file mismatch: ' + name)
    return manifest, entries


def unpack(path, destination):
    manifest, entries = verify_archive(path)
    destination.mkdir(parents=True, exist_ok=False)
    for name, (data, mode) in entries.items():
        target = destination / name
        target.parent.mkdir(parents=True, exist_ok=True)
        with target.open('xb') as handle:
            handle.write(data)
        target.chmod(mode)
    return manifest


def build_binary(source, output):
    # Build only a validated curated snapshot; no ignored host configuration or
    # checkout history can be added to the artifact by this command.
    with tempfile.TemporaryDirectory(prefix='observer-build-') as temporary:
        root = Path(temporary) / 'source'
        manifest = unpack(source, root)
        if platform.system()!='Darwin' or platform.machine()!='arm64':
            raise ValueError('binary candidate must be built on native macOS arm64')
        build_env = dict(os.environ, GOWORK='off', GOENV='off', GOFLAGS='', GOTOOLCHAIN='local')
        target = subprocess.check_output(['go', 'env', 'GOOS', 'GOARCH'], cwd=root, env=build_env, text=True).split()
        if target != ['darwin', 'arm64']:
            raise ValueError('binary release currently targets darwin/arm64 only')
        meta = dict(manifest['release'], source_sha256=digest(source.read_bytes()))
        version = meta['version']
        binary = Path(temporary) / 'agent-observer'
        subprocess.run(['go', 'build', '-trimpath', '-mod=readonly', '-buildvcs=false', '-ldflags', '-X agent-observer/internal/buildinfo.Version=' + version, '-o', str(binary), './cmd/agent-observer'], cwd=root, env=build_env, check=True)
        files = {'agent-observer': (binary.read_bytes(), 0o755)}
        for name in ['README.md', 'CONTRIBUTING.md', 'SECURITY.md', 'THIRD_PARTY_NOTICES.md', 'docs/public/getting-started.md', 'docs/public/macos-install-ko.md', 'docs/public/privacy.md', 'docs/public/managed-workers.md', 'docs/public/releases.md']:
            files[name] = (read_source(root, name), 0o644)
        if (root / 'LICENSE').exists():
            files['LICENSE'] = (read_source(root, 'LICENSE'), 0o644)
        return write_artifact(output, 'agent-observer-' + version + '-darwin-arm64.tar.gz', archive_bytes(files, meta))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest='command', required=True)
    sub.add_parser('check-tree', help='verify the public Git checkout matches the source allowlist')
    build = sub.add_parser('source')
    build.add_argument('--purpose', choices=['local-rc', 'publish'], default='local-rc')
    build.add_argument('--output', type=Path, required=True)
    verify = sub.add_parser('verify')
    verify.add_argument('archive', type=Path)
    extract = sub.add_parser('unpack')
    extract.add_argument('archive', type=Path)
    extract.add_argument('--destination', type=Path, required=True)
    binary = sub.add_parser('binary')
    binary.add_argument('archive', type=Path)
    binary.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    try:
        if args.command == 'check-tree':
            print(json.dumps({'verified_files': check_public_tree(ROOT)}, sort_keys=True))
        elif args.command == 'source':
            files = source_snapshot(ROOT)
            metadata = release_metadata(ROOT, args.purpose, files)
            print(write_artifact(args.output, 'agent-observer-' + metadata['version'] + '-source.tar.gz', archive_bytes(files, metadata)))
        elif args.command == 'verify':
            manifest, _ = verify_archive(args.archive)
            print(json.dumps({'verified_files': len(manifest['files']), 'release': manifest['release']}, sort_keys=True))
        elif args.command == 'unpack':
            unpack(args.archive, args.destination)
            print(args.destination)
        else:
            print(build_binary(args.archive, args.output))
    except (OSError, ValueError, KeyError, TypeError, AttributeError, tarfile.TarError, subprocess.SubprocessError) as error:
        parser.exit(1, str(error) + '\n')


if __name__ == '__main__':
    main()
