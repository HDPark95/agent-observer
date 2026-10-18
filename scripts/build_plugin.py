"""Build a self-contained, local Codex marketplace from reviewed source inputs."""
import argparse
import json
from pathlib import Path
import shutil
import subprocess
import tempfile

from package_release import ROOT, source_snapshot


def write_json(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2) + '\n')


def build_plugin(output):
    if output.exists():
        raise ValueError('output exists; select a new --output directory')
    files = source_snapshot(ROOT)
    with tempfile.TemporaryDirectory(prefix='observer-plugin-build-') as temporary:
        source = Path(temporary) / 'source'
        for name, (data, _) in files.items():
            target = source / name
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes(data)
        stage = Path(temporary) / 'marketplace'
        plugin = stage / 'plugins/agent-observer'
        shutil.copytree(source / 'plugins/agent-observer', plugin)
        shutil.copyfile(source / 'LICENSE', plugin / 'LICENSE')
        (plugin / 'bin').mkdir()
        subprocess.run(['go', 'build', '-trimpath', '-buildvcs=false', '-o', str(plugin / 'bin/agent-observer'), './cmd/agent-observer'], cwd=source, check=True)
        add_compatibility_manifests(plugin)
        write_json(stage / '.agents/plugins/marketplace.json', {
            'name': 'agent-observer-local',
            'interface': {'displayName': 'Agent Observer Local'},
            'plugins': [{
                'name': 'agent-observer',
                'source': {'source': 'local', 'path': './plugins/agent-observer'},
                'policy': {'installation': 'AVAILABLE', 'authentication': 'ON_INSTALL'},
                'category': 'Developer tools',
            }],
        })
        output.parent.mkdir(parents=True, exist_ok=True)
        shutil.copytree(stage, output)


def add_compatibility_manifests(plugin):
    # Generate the compatibility view from the portable source of truth.
    manifest = json.loads((plugin / 'plugin.json').read_text())
    extension = manifest['extensions']['com.openai']
    write_json(plugin / '.codex-plugin/plugin.json', {
        'name': manifest['name'], 'version': manifest['version'],
        'description': manifest['description'], 'skills': './skills/',
        'mcpServers': './.mcp.json', 'requires_local_executor': True,
        'interface': extension['interface'],
    })
    # Older Codex clients resolve cwd relative to the plugin but do not
    # expand portable placeholders in MCP commands or cwd.
    portable = (plugin / 'mcp.json').read_text()
    servers = json.loads(portable.replace('${PLUGIN_ROOT}', '.'))['mcpServers']
    write_json(plugin / '.mcp.json', {'mcpServers': {
        name: {key: value for key, value in server.items() if key != 'type'}
        for name, server in servers.items()
    }})


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', type=Path, default=ROOT / 'dist/codex-plugin')
    args = parser.parse_args()
    try:
        build_plugin(args.output.resolve())
    except (OSError, ValueError, subprocess.SubprocessError) as error:
        parser.exit(1, str(error) + '\n')
    print(args.output.resolve())


if __name__ == '__main__':
    main()
