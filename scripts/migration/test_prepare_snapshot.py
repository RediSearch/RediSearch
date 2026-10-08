# Copyright (c) 2006-Present, Redis Ltd. All rights reserved.
# Licensed under your choice of RSALv2, SSPLv1, or AGPLv3.
"""Exercise pinned exports and isolation against real temporary Git repositories."""

import os
from pathlib import Path
import subprocess
import tempfile
import unittest

from prepare_snapshot import git, prepare


class SnapshotTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.repo = self.root / 'original'
        self.repo.mkdir()
        git(self.repo, 'init', '--template=')
        (self.repo / 'value').write_text('baseline\n')
        self.start = self.commit(self.repo)

    def commit(self, repo):
        git(repo, 'add', '--all')
        git(repo, '-c', 'user.name=Test', '-c', 'user.email=test@example.invalid',
            '-c', 'commit.gpgsign=false', 'commit', '-m', 'fixture')
        return git(repo, 'rev-parse', 'HEAD').decode().strip()

    def test_exact_source_no_future_history_and_repeatable_identity(self):
        (self.repo / 'value').write_text('future\n')
        (self.repo / 'future-answer').write_text('solution')
        future = self.commit(self.repo)
        (self.repo / 'untracked').write_text('private')
        a = prepare(self.repo, self.start, self.root / 'a')
        b = prepare(self.repo, self.start, self.root / 'b')
        source = self.root / 'a/source'
        self.assertEqual((source / 'value').read_text(), 'baseline\n')
        self.assertFalse((source / 'future-answer').exists())
        self.assertFalse((source / 'untracked').exists())
        self.assertEqual(git(source, 'rev-list', '--count', 'HEAD').strip(), b'1')
        self.assertEqual(git(source, 'remote'), b'')
        with self.assertRaises(subprocess.CalledProcessError):
            git(source, 'cat-file', '-e', future)
        self.assertEqual(a, b)
        self.assertEqual(git(self.repo, 'rev-parse', 'HEAD').decode().strip(), future)

    def test_pinned_submodule_not_its_current_head(self):
        sub = self.repo / 'dep'
        sub.mkdir()
        git(sub, 'init', '--template=')
        (sub / 'library').write_text('pinned')
        pinned = self.commit(sub)
        git(self.repo, 'update-index', '--add', '--cacheinfo', f'160000,{pinned},dep')
        git(self.repo, '-c', 'user.name=Test', '-c', 'user.email=test@example.invalid',
            '-c', 'commit.gpgsign=false', 'commit', '-m', 'pin')
        sha = git(self.repo, 'rev-parse', 'HEAD').decode().strip()
        (sub / 'library').write_text('future')
        self.commit(sub)
        result = prepare(self.repo, sha, self.root / 'output')
        self.assertEqual(result['submodules'], {'dep': pinned})
        self.assertEqual((self.root / 'output/source/dep/library').read_text(), 'pinned')
        self.assertFalse((self.root / 'output/source/dep/.git').exists())
        sub.rename(self.repo / 'unavailable')
        with self.assertRaises((ValueError, subprocess.CalledProcessError)):
            prepare(self.repo, sha, self.root / 'missing')
        self.assertFalse((self.root / 'missing').exists())

    def test_preserves_modes_links_and_export_ignored_files(self):
        (self.repo / 'run').write_text('#!/bin/sh\n')
        (self.repo / 'run').chmod(0o755)
        (self.repo / 'link').symlink_to('value')
        (self.repo / '.gitattributes').write_text('value export-ignore\n')
        sha = self.commit(self.repo)
        prepare(self.repo, sha, self.root / 'output')
        source = self.root / 'output/source'
        self.assertEqual((source / 'value').read_text(), 'baseline\n')
        self.assertEqual(os.readlink(source / 'link'), 'value')
        self.assertTrue((source / 'run').stat().st_mode & 0o111)

    def test_escaping_link_fails_without_output(self):
        (self.repo / 'link').symlink_to('../../private')
        sha = self.commit(self.repo)
        with self.assertRaisesRegex(ValueError, 'Symlink escapes'):
            prepare(self.repo, sha, self.root / 'output')
        self.assertFalse((self.root / 'output').exists())

    def test_lfs_pointer_fails_explicitly(self):
        (self.repo / 'asset').write_text('version https://git-lfs.github.com/spec/v1\noid sha256:abc\nsize 1\n')
        sha = self.commit(self.repo)
        with self.assertRaisesRegex(ValueError, 'LFS pointer'):
            prepare(self.repo, sha, self.root / 'output')
        self.assertFalse((self.root / 'output').exists())

    def test_refuses_branch_names_and_existing_output(self):
        with self.assertRaises(ValueError):
            prepare(self.repo, 'HEAD', self.root / 'output')
        out = self.root / 'existing'
        out.mkdir()
        (out / 'keep').write_text('keep')
        with self.assertRaises(ValueError):
            prepare(self.repo, self.start, out)
        self.assertEqual((out / 'keep').read_text(), 'keep')


if __name__ == '__main__':
    unittest.main()
