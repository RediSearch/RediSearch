#!/usr/bin/env python3
# Copyright (c) 2006-Present, Redis Ltd. All rights reserved.
# Licensed under your choice of RSALv2, SSPLv1, or AGPLv3.
"""Export a pinned local Git tree into a fresh, history-free repository."""

import argparse
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import re
import shutil
import subprocess


def environment():
    """Do not inherit a caller's Git directory, identity, or filter settings."""
    env = {k: v for k, v in os.environ.items() if not k.startswith('GIT_')}
    env.update(GIT_CONFIG_NOSYSTEM='1', GIT_CONFIG_GLOBAL=os.devnull,
               GIT_TERMINAL_PROMPT='0', GIT_NO_REPLACE_OBJECTS='1',
               GIT_NO_LAZY_FETCH='1', GIT_ALLOW_PROTOCOL='')
    return env


def git(repo, *args, input=None):
    return subprocess.run(
        ['git', '-c', 'core.fsmonitor=false', '-c', 'core.hooksPath=/dev/null',
         '-C', str(repo), *args], input=input, stdout=subprocess.PIPE,
        stderr=subprocess.PIPE, check=True, env=environment()).stdout


def collect(repo, sha, prefix='', ancestry=()):
    """Resolve gitlinks at their pinned commits, never at submodule HEAD."""
    repo = repo.resolve()
    if repo in ancestry:
        raise ValueError(f'Recursive submodule repository: {prefix}')
    top = Path(os.fsdecode(git(repo, 'rev-parse', '--show-toplevel')).strip()).resolve()
    if top != repo:
        raise ValueError(f'Submodule is not initialized as its own checkout: {prefix}')
    resolved = git(repo, 'rev-parse', '--verify', f'{sha}^{{commit}}').decode().strip()
    if resolved != sha:
        raise ValueError('A full commit SHA is required')
    entries, modules = [], {}
    for record in git(repo, 'ls-tree', '-rz', '--full-tree', sha).split(b'\0'):
        if not record:
            continue
        header, raw_path = record.split(b'\t', 1)
        mode, kind, oid = header.decode().split()
        path = os.fsdecode(raw_path)
        parts = PurePosixPath(path).parts
        if path.startswith('/') or '..' in parts or any(p.lower() == '.git' for p in parts):
            raise ValueError(f'Unsafe tracked path: {prefix}{path}')
        relative = prefix + path
        if mode == '160000':
            nested, pins = collect(repo / path, oid, relative + '/', ancestry + (repo,))
            entries.extend(nested)
            modules[relative] = oid
            modules.update(pins)
        elif kind == 'blob' and mode in {'100644', '100755', '120000'}:
            entries.append((relative, mode, oid, repo))
        else:
            raise ValueError(f'Unsupported tree entry: {relative} ({mode})')
    return entries, modules


def export(entries, destination):
    digest = hashlib.sha256()
    for relative, mode, oid, repo in sorted(entries):
        content = git(repo, 'cat-file', 'blob', oid)
        if content.startswith(b'version https://git-lfs.github.com/spec/v1\n'):
            raise ValueError(f'LFS pointer needs trusted materialization before use: {relative}')
        path = destination / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        if mode == '120000':
            target = os.fsdecode(content)
            # Resolving rejects a link through an earlier link that escapes the export.
            if os.path.isabs(target) or not (path.parent / target).resolve().is_relative_to(destination):
                raise ValueError(f'Symlink escapes snapshot: {relative}')
            path.symlink_to(target)
        else:
            path.write_bytes(content)
            path.chmod(0o755 if mode == '100755' else 0o644)
        row = [relative, mode, hashlib.sha256(content).hexdigest()]
        digest.update(json.dumps(row, ensure_ascii=True, separators=(',', ':')).encode() + b'\n')
    # A later-created symlink can change the resolution of an earlier link.
    for relative, mode, _, _ in entries:
        if mode == '120000' and not (destination / relative).resolve().is_relative_to(destination):
            raise ValueError(f'Symlink chain escapes snapshot: {relative}')
    return digest.hexdigest()


def prepare(repo, sha, output):
    if not re.fullmatch(r'[0-9a-f]{40}|[0-9a-f]{64}', sha):
        raise ValueError('--sha must be a full lowercase commit SHA, not a branch or tag')
    output = output.absolute()
    if output.exists() or output.is_symlink():
        raise ValueError(f'Output already exists: {output}')
    entries, modules = collect(repo, sha)
    if not output.parent.is_dir():
        raise ValueError('Output parent must already exist')
    # Reserve the output name before work; remove only our own files on failure.
    output.mkdir()
    try:
        source = output / 'source'
        source.mkdir()
        digest = export(entries, source)
        git(source, 'init', '--template=', '--initial-branch=migration')
        # Write raw blobs into the new object database; avoid attributes/filters
        # changing source bytes or executing commands during staging.
        index = bytearray()
        for relative, mode, _, _ in sorted(entries):
            path = source / relative
            content = os.fsencode(os.readlink(path)) if mode == '120000' else path.read_bytes()
            oid = git(source, 'hash-object', '-w', '--stdin', input=content).strip()
            index.extend(mode.encode() + b' ' + oid + b'\t' + os.fsencode(relative) + b'\0')
        git(source, 'update-index', '-z', '--index-info', input=bytes(index))
        env = environment()
        env.update(GIT_AUTHOR_DATE='2000-01-01T00:00:00+00:00',
                   GIT_COMMITTER_DATE='2000-01-01T00:00:00+00:00')
        subprocess.run(['git', '-C', str(source), '-c', 'core.hooksPath=/dev/null',
                        '-c', 'commit.gpgsign=false', '-c', 'user.name=Migration snapshot',
                        '-c', 'user.email=migration-snapshot@example.invalid',
                        'commit', '--allow-empty', '-m', 'Migration source baseline'],
                       check=True, env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        baseline = git(source, 'rev-parse', 'HEAD').decode().strip()
        provenance = dict(format_version=1, starting_sha=sha, submodules=modules,
                          snapshot_sha256=digest, file_count=len(entries),
                          local_baseline_commit=baseline,
                          script_sha256=hashlib.sha256(Path(__file__).read_bytes()).hexdigest())
        (output / 'provenance.json').write_text(json.dumps(provenance, indent=2) + '\n')
        if (git(source, 'rev-list', '--count', 'HEAD').strip() != b'1'
                or git(source, 'remote').strip()
                or (source / '.git/objects/info/alternates').exists()):
            raise ValueError('Snapshot history/remotes isolation check failed')
        return provenance
    except BaseException:
        shutil.rmtree(output)
        raise


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--repo', type=Path, required=True, help='Trusted local checkout with pinned submodules available')
    parser.add_argument('--sha', required=True, help='Full source commit SHA')
    parser.add_argument('--output', type=Path, required=True, help='New directory containing source/ and provenance.json')
    args = parser.parse_args()
    try:
        result = prepare(args.repo, args.sha, args.output)
    except (ValueError, OSError, RuntimeError, subprocess.CalledProcessError) as exc:
        detail = exc.stderr.decode(errors='replace') if isinstance(exc, subprocess.CalledProcessError) else str(exc)
        parser.exit(1, f'Snapshot preparation failed: {detail}\n')
    print(json.dumps(result, indent=2))


if __name__ == '__main__':
    main()
