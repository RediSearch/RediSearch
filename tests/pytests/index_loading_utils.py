# Copyright (c) 2006-Present, Redis Ltd.
# All rights reserved.
#
# Licensed under your choice of the Redis Source Available License 2.0
# (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
# GNU Affero General Public License v3 (AGPLv3).

"""Helpers shared by the index-loading (`$as`) flow tests."""

from functools import wraps

from common import run_command_on_all_shards, to_dict


def set_unstable_features(env, on):
    run_command_on_all_shards(env, 'CONFIG', 'SET', 'search-enable-unstable-features',
                              'yes' if on else 'no')


def with_unstable_features(test):
    """Runs `test` with unstable features on, and turns them back off after it, so no other test
    in the file inherits them."""
    @wraps(test)
    def wrapper(env, *args, **kwargs):
        set_unstable_features(env, True)
        try:
            return test(env, *args, **kwargs)
        finally:
            set_unstable_features(env, False)
    return wrapper


def aggregate_rows(env, query, *args):
    """FT.AGGREGATE rows of `idx` as dicts, after the leading count (which a cluster may report
    differently)."""
    res = env.cmd('FT.AGGREGATE', 'idx', query, *args)
    return [to_dict(row) for row in res[1:]]


def _without_time(node):
    if not isinstance(node, list):
        return node
    out, i = [], 0
    while i < len(node):
        if node[i] == 'Time':
            i += 2
            continue
        out.append(_without_time(node[i]))
        i += 1
    return out


def iterators_profile(reply):
    """The iterator tree of an FT.PROFILE reply, without its timings, or None when it has none."""
    if isinstance(reply, list):
        for i, item in enumerate(reply):
            if item == 'Iterators profile':
                return _without_time(reply[i + 1])
            found = iterators_profile(item)
            if found is not None:
                return found
    return None
