import importlib.util
import json
from pathlib import Path
import subprocess
import tempfile
import unittest

SPEC = importlib.util.spec_from_file_location('package_release', Path(__file__).resolve().parents[1] / 'scripts' / 'package_release.py')
package = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(package)


class ReleasePackageTests(unittest.TestCase):
    def test_reproducible_and_verified_unpack(self):
        files = {'readme.md': (b'synthetic fixture\n', 0o644), 'nested/file.txt': (b'test', 0o644)}
        metadata = {'schema_version':1,'supported_targets':['darwin/arm64'],'version':'0.1.0-rc1','publication_status':'blocked-owner-license-pending'}
        first = package.archive_bytes(files, metadata)
        self.assertEqual(first, package.archive_bytes(files, metadata))
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            path = package.write_artifact(root, 'source.tar.gz', first)
            manifest, actual = package.verify_archive(path)
            self.assertEqual(actual, files)
            self.assertEqual(manifest['release'], metadata)
            package.unpack(path, root / 'extracted')
            self.assertEqual((root/'extracted/nested/file.txt').read_bytes(), b'test')
            with self.assertRaises(ValueError):
                package.write_artifact(root, 'source.tar.gz', first)
            path.write_bytes(first + b'corrupted')
            with self.assertRaises(ValueError):
                package.verify_archive(path)

    def test_paths_and_state_are_excluded(self):
        for name in ['../x','/tmp/x','a//b','a/./b','a/../b','a\\b']:
            with self.subTest(name=name), self.assertRaises(ValueError):
                package.safe_name(name)
        with tempfile.TemporaryDirectory() as temporary:
            root=Path(temporary)
            (root/'release').mkdir()
            for name in ['.local/token','bin/observer','.env','session.jsonl','a\nA',
                         '.ENV.local', '.GiT/config', '.CoDeX/auth.json', '.claude/settings.json',
                         '.aws/credentials', '.ssh/config', '.runs/event.json', '.state/output.json',
                         'private.KEY', 'identity.P12', 'identity.pfx', 'server.LOG', 'trace.JSONL',
                         'Docs/private.md', 'INTERNAL/private.JSON']:
                (root/'release/source-files.txt').write_text(name)
                with self.subTest(name=name), self.assertRaises(ValueError):
                    package.source_paths(root)
            (root/'real').write_text('fixture')
            (root/'link').symlink_to(root/'real')
            with self.assertRaises(ValueError):
                package.read_source(root,'link')
            with self.assertRaises(FileNotFoundError):
                package.read_source(root,'missing')

    def test_source_scan_flags_personal_data_without_echoing_values(self):
        # Construct synthetic candidates so the test source contains no literal
        # home path or address that the public-source scan would flag itself.
        values = [b'/Users/' + b'sample-person/project', b'/home/' + b'sample-person/project',
                  b'/var/' + b'folders/random/output', b'person@' + b'synthetic-company.org',
                  b'ghp_' + b'A' * 30]
        for data in values:
            with self.subTest(category='synthetic private data'):
                with self.assertRaises(ValueError) as error:
                    package.scan_source('fixture.txt', data)
                self.assertEqual(str(error.exception), 'sensitive-source review required: fixture.txt')

    def test_source_scan_allows_reserved_example_addresses(self):
        for domain in [b'example.com', b'sub.example.org', b'EXAMPLE.NET', b'example.test', b'example.invalid', b'project.example']:
            with self.subTest(domain=domain):
                package.scan_source('fixture.txt', b'person@' + domain)

    def test_public_tree_requires_exact_tracked_inventory(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            subprocess.run(['git', 'init', '--quiet', str(root)], check=True)
            (root / 'release').mkdir()
            (root / 'README.md').write_text('Synthetic public source.\n')
            allowlist = root / 'release/source-files.txt'
            allowlist.write_text('README.md\nrelease/source-files.txt\n')
            subprocess.run(['git', 'add', 'README.md', 'release/source-files.txt'], cwd=root, check=True)
            self.assertEqual(package.check_public_tree(root), 2)

            # An unlisted tracked file is rejected, even though source packaging
            # would otherwise omit it and appear clean.
            (root / 'unexpected.txt').write_text('Synthetic unlisted fixture.\n')
            subprocess.run(['git', 'add', 'unexpected.txt'], cwd=root, check=True)
            with self.assertRaisesRegex(ValueError, '1 unlisted, 0 missing'):
                package.check_public_tree(root)
            subprocess.run(['git', 'rm', '--cached', 'unexpected.txt'], cwd=root, check=True, capture_output=True)

            allowlist.write_text('README.md\nrelease/source-files.txt\nmissing.txt\n')
            with self.assertRaisesRegex(ValueError, '0 unlisted, 1 missing'):
                package.check_public_tree(root)

    def test_public_tree_rejects_nested_non_repository_directory(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            subprocess.run(['git', 'init', '--quiet', str(root)], check=True)
            nested = root / 'export'
            nested.mkdir()
            with self.assertRaisesRegex(ValueError, 'repository root'):
                package.check_public_tree(nested)

    def test_publication_requires_owner_and_license(self):
        with tempfile.TemporaryDirectory() as temporary:
            root=Path(temporary)
            (root/'release').mkdir()
            (root/'release/release.json').write_text(json.dumps({'schema_version':1,'supported_targets':['darwin/arm64'],'version':'0.1.0-rc1','repository':None,'license_spdx':None}))
            self.assertEqual(package.release_metadata(root,'local-rc')['publication_status'],'blocked-owner-license-pending')
            with self.assertRaises(ValueError):
                package.release_metadata(root,'publish')

    def test_unvalidated_release_metadata_is_rejected(self):
        meta={'schema_version':1,'supported_targets':['darwin/arm64'],'version':'0.1.0-rc1','publication_status':'blocked-owner-license-pending'}
        for invalid in [dict(meta, version='../invalid'), dict(meta, private_notes='unreviewed'), dict(meta, repository=42), dict(meta, supported_targets=['windows/amd64'])]:
            with self.subTest(invalid=invalid), self.assertRaises(ValueError):
                package.archive_bytes({'readme':(b'test',0o644)}, invalid)

    def test_license_selection_does_not_imply_repository_or_publication(self):
        license_text = b'synthetic license fixture\n'
        meta = {'schema_version': 1, 'supported_targets': ['darwin/arm64'], 'version': '0.1.0-rc1', 'repository': None, 'license_spdx': 'MIT', 'license_sha256': package.digest(license_text)}
        files = {'release/release.json': (json.dumps(meta).encode(), 0o644), 'LICENSE': (license_text, 0o644)}
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            release = package.release_metadata(root, 'local-rc', files)
            self.assertEqual(release['publication_status'], 'blocked-repository-pending')
            with self.assertRaises(ValueError):
                package.release_metadata(root, 'publish', files)
            path = package.write_artifact(root, 'source.tar.gz', package.archive_bytes(files, release))
            manifest, actual = package.verify_archive(path)
            self.assertEqual(actual['LICENSE'][0], license_text)
            self.assertEqual(manifest['release']['license_sha256'], package.digest(actual['LICENSE'][0]))

    def test_publication_requires_matching_allowlisted_license(self):
        license_text = b'synthetic license fixture\n'
        meta = {'schema_version': 1, 'supported_targets': ['darwin/arm64'], 'version': '0.1.0-rc1', 'repository': 'example/agent-observer', 'license_spdx': 'MIT', 'license_sha256': package.digest(license_text)}
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            for license_entry in [None, b'changed license', license_text]:
                with self.subTest(license_entry=license_entry):
                    files = {'release/release.json': (json.dumps(meta).encode(), 0o644)}
                    if license_entry is not None:
                        files['LICENSE'] = (license_entry, 0o644)
                    if license_entry == license_text:
                        self.assertEqual(package.release_metadata(root, 'publish', files)['publication_status'], 'eligible-for-review')
                    else:
                        self.assertEqual(package.release_metadata(root, 'local-rc', files)['publication_status'], 'blocked-license-pending')
                        with self.assertRaises(ValueError):
                            package.release_metadata(root, 'publish', files)
