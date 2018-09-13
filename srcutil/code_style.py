#!/usr/bin/env python
import glob
import os
import os.path
import sys
import shutil
from argparse import ArgumentParser
from subprocess import Popen, PIPE

RED = '\033[0;31m'
GREEN = '\033[0;32m'
NC = '\033[0m' # No Color
CLANG_ARGS = ['-style=file', '-fallback-style=none']
GIT_STATUS_PATTERNS = [
    'src/*.[ch]',
    'src/*.[ch]pp'
]

ap = ArgumentParser()
ap.add_argument('-f', '--path',
    help="Manual path or glob to format", metavar="FILE_OR_DIR")
ap.add_argument('-n', '--dry-run',
    help="Show which files would have been modified and exit",
    action='store_true')
ap.add_argument('--install',
    help="Install this script as a Git hook", action='store_true')

options = ap.parse_args()

if options.install:
    # Copy this file as a git hook
    if not os.path.exists('.git'):
        raise Exception('Must be run from source directory (no .git directory found)')
    script_target = '.git/hooks/pre-commit'
    script_text = """
#!/bin/bash
./srcutil/code_style.py -n
"""
    with open(script_target, 'w') as f:
        f.write(script_text)
        f.close()
    os.chmod(script_target, 0o755)
    sys.exit(0)

if options.path:
    files = glob.glob(options.path)
else:
    po = Popen(['git', 'status', '--porcelain'] + GIT_STATUS_PATTERNS, stdout=PIPE)
    output, _ = po.communicate()
    lines = [line for line in output.split('\n') if line]
    files = []
    for line in lines:
        # Check the two letter status
        status = line[0:2]
        if status[0] == ' ':
            continue
        if status[0] in ('C', 'R'):
            # [C]opy or [R]ename
            # TODO: This can theoretically break if there are spaces in
            # the filename. Needs to be tested.
            src, delim, dst, = line[3:].split(' ')
            files.append(dst)
        elif status[0] in ('M', 'A'):
            files.append(line[3:])

has_error = False
for f in files:

    po = Popen(['clang-format'] + CLANG_ARGS + ['-output-replacements-xml', f], stdout=PIPE)
    output, _ = po.communicate()
    rv = po.wait()
    if rv != 0:
        print 'Warning: Could not analyze {}'.format(f)
    count = len([line for line in output.split('\n') if line])
    has_changes = count > 3
    if options.dry_run:
        if has_changes:
            print RED + f + ' [FAIL]' + NC
            has_error = True
        else:
            print GREEN + f + ' [OK]' + NC
    else:
        if has_changes:
            print 'Reformatting ' + f
            po = Popen(['clang-format'] + CLANG_ARGS + ['-i', f])
            po.communicate()
            if po.wait() != 0:
                print "Warning: Couldn't reformat!"

if has_error:
    print(
        'Some files were not properly formatted. Run this script ({}) to '
        'format them'.format(__file__))
    if os.environ.get('CODE_STYLE_IGNORE'):
        print 'Ignoring error because of CODE_STYLE_IGNORE in the environment'
    else:
        sys.exit(1)