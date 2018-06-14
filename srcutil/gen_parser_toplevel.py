#!/usr/bin/env python
import argparse
import sys

"""
This script generates a source file suitable for compilation. Because our
parser generator (Lemon) always outputs the same symbols, we need a way to
namespace them so that they don't crash. The approach we use will leave the
file as-is, but generate an include wrapper, so that the symbols are changed
before the actual source file is included, and then compiled with the macro
definition instead.

This script writes to stdout; the output may be captured and redirected to
another file.
"""

ap = argparse.ArgumentParser()
ap.add_argument('-p', '--prefix', help='Prefix for function names', required=True)
ap.add_argument('-i', '--include', help='Next-include for actual parser code',
        default='parser.c.inc')
options = ap.parse_args()

fp = sys.stdout
NAMES = (
        'Parse', 'ParseTrace', 'ParseAlloc', 'ParseFree', 'ParseInit',
        'ParseFinalize',
        'ParseStackPeack')


for name in NAMES:
    fp.write('#define {name} {prefix}_{name}\n'.format(name=name, prefix=options.prefix))
    fp.flush()

fp.write('#include "{}"\n'.format(options.include))
fp.flush()