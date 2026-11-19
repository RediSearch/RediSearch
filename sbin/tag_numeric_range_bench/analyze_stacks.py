#!/usr/bin/env python3
# Copyright (c) 2006-Present, Redis Ltd.
# All rights reserved.
#
# Licensed under your choice of the Redis Source Available License 2.0
# (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
# GNU Affero General Public License v3 (AGPLv3).
"""Summarise the stacks written by gdb_sampler.py for the thread running the query.

usage: analyze_stacks.py <stacks file> <folded output file>

Prints the share of samples per cost category and per innermost function, and writes the
collapsed stacks, one "frame;frame;... count" line each, which is the input format of
flamegraph.pl.
"""
import collections
import re
import sys

# The function that drives the result pipeline. Other threads, and the query thread while
# it parses or replies, are not part of the cost being measured.
PIPELINE_ROOT = "startPipelineCommon"
FRAME = re.compile(r"^#\d+\s+(?:0x[0-9a-f]+ in )?(.*)$")


def strip_generics(name):
    out, depth = [], 0
    for ch in name:
        if ch == "<":
            depth += 1
        elif ch == ">":
            depth = max(0, depth - 1)
        elif depth == 0:
            out.append(ch)
    return "".join(out)


def short(name):
    parts = [p for p in name.split("::") if p]
    return "::".join(parts[-3:])


def category(frames):
    """Cost category of one sample.

    Matches on module paths of the iterator implementation. A sample that matches none of
    them is reported as "other" rather than guessed, so a rename shows up as a growing
    "other" share instead of a wrong attribution.
    """
    def has(sub):
        return any(sub in f for f in frames)

    if has("::union_"):
        if has("::min_heap::"):
            return "union: heap maintenance"
        if has("inverted_index::codec::"):
            return "union child: decoding entries inside a block"
        if has("inverted_index::") or has("numeric_range_tree::"):
            return "union child: locating the block"
        return "union: advancing lagging children"
    if has("DocTable_"):
        return "outside the union: document table lookup"
    return "outside the union: other"


def main():
    samples = open(sys.argv[1]).read().split("=== sample\n")
    total = in_query = 0
    by_category, by_self, folded = (collections.Counter() for _ in range(3))
    for sample in samples:
        if not sample.strip():
            continue
        total += 1
        for block in re.split(r"\nThread \d+ ", "\n" + sample):
            frames = []
            for line in block.split("\n"):
                m = FRAME.match(line)
                if m:
                    frames.append(strip_generics(m.group(1).split(" (", 1)[0]).strip())
            if PIPELINE_ROOT not in frames:
                continue
            frames = frames[: frames.index(PIPELINE_ROOT) + 1]
            in_query += 1
            by_category[category(frames)] += 1
            by_self[short(frames[0])] += 1
            folded[";".join(short(f) for f in reversed(frames))] += 1
            break
    with open(sys.argv[2], "w") as f:
        for stack, count in folded.most_common():
            f.write("%s %d\n" % (stack, count))
    print("samples: %d, inside the query pipeline: %d" % (total, in_query))
    if not in_query:
        return
    print("-- share of samples by category")
    for name, count in by_category.most_common():
        print("  %5.1f%%  %s" % (100.0 * count / in_query, name))
    print("-- share of samples by innermost function, top 12")
    for name, count in by_self.most_common(12):
        print("  %5.1f%%  %s" % (100.0 * count / in_query, name))


main()
