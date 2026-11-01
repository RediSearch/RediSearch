# Copyright (c) 2006-Present, Redis Ltd.
# All rights reserved.
#
# Licensed under your choice of the Redis Source Available License 2.0
# (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
# GNU Affero General Public License v3 (AGPLv3).

"""The query memory stop matrix: every query shape under every ON_OOM x ON_TIMEOUT x WORKERS
combination, in both protocols, standalone and in a cluster with pressure on every shard but the
one in the coordinator's process, on the coordinator's process only, or on every process.

Pressure comes from `_FT.DEBUG MEMORY_PRESSURE AT_CALL <k>`, swept over the checkpoint calls the
query makes (measured per cell, never hard-coded, so new checkpoints do not break the sweep). Exact
outcomes per stop site are the per-stack tests' job; here every reply is checked against the
invariants every site must keep, whatever the site:
- ON_OOM IGNORE: the complete result.
- Otherwise: no error but the OOM error (only under ON_OOM FAIL, or at client cursor admission on
  the coordinator's process), never a timeout; no duplicate rows, no rows outside the complete
  result (FT.HYBRID: outside the index, since fusion re-ranks a cut subquery), sorted shapes still
  sorted; a reply that differs from the complete one carries an OOM warning wherever the protocol
  has a warning channel.
- The sweep stops the query somewhere, under ON_OOM FAIL with the OOM error at least once; the next
  unpressured run is complete again, without an error or a warning, and no cursor is left open.

The call counter is per process. Where the armed process runs more than one pipeline at once (the
coordinator's process, which also hosts a shard, or any process with workers), call k is not always
the same checkpoint from run to run: a failure names the cell, and its `at_call` may need a few runs
to reproduce."""

from common import *

OOM_QUERY_ERROR = "Not enough memory available to execute the query"
OOM_WARNINGS = {
    "Coordinator failed to execute the query due to insufficient memory",
    "One or more shards failed to execute the query due to insufficient memory",
}
N_DOCS = 24
N_GROUPS = 4
# Arms the call counter without firing.
NEVER = 1_000_000
# Larger sweeps sample: every early call (build stages and first rows), then evenly up to the last.
MAX_POSITIONS = 24
EARLY_POSITIONS = 12
CURSOR_COUNT = 5
MAX_CURSOR_READS = 2 * N_DOCS

ON_OOM_POLICIES = ('return', 'fail', 'ignore')
ON_TIMEOUT_POLICIES = ('RETURN', 'FAIL', 'RETURN-STRICT')
WORKER_COUNTS = (0, 2)

ORIGIN = np.array([0.0, 0.0], dtype=np.float32).tobytes()
# L2 is squared: docs 0..15 lie within the radius of the origin.
RADIUS_DOCS = 15
RADIUS = 2 * RADIUS_DOCS ** 2


class Shape:
    """A query shape. `kind` selects the reply parser; `key` names the row identity; `order`
    is the order the complete result is sorted in (by `n`), if any."""
    def __init__(self, kind, query, key, order=None):
        self.kind, self.query, self.key, self.order = kind, query, key, order


SHAPES = {
    'search_score': Shape('search', ['FT.SEARCH', 'idx', 'hello', 'RETURN', 1, 'n',
                                     'LIMIT', 0, N_DOCS], 'id'),
    'search_sortby': Shape('search', ['FT.SEARCH', 'idx', '*', 'SORTBY', 'n', 'DESC',
                                      'RETURN', 1, 'n', 'LIMIT', 0, N_DOCS], 'id', 'desc'),
    'aggregate_stream': Shape('aggregate', ['FT.AGGREGATE', 'idx', '*', 'LOAD', 1, '@n'], 'n'),
    'aggregate_sortby': Shape('aggregate', ['FT.AGGREGATE', 'idx', '*', 'LOAD', 2, '@n', '@name',
                                            'SORTBY', 2, '@n', 'DESC', 'MAX', N_DOCS],
                              'n', 'desc'),
    'aggregate_groupby': Shape('groupby', ['FT.AGGREGATE', 'idx', '*', 'GROUPBY', 1, '@g',
                                           'REDUCE', 'COUNT', 0, 'AS', 'c'], 'g'),
    'cursor': Shape('cursor', ['FT.AGGREGATE', 'idx', '*', 'LOAD', 1, '@n',
                               'WITHCURSOR', 'COUNT', CURSOR_COUNT], 'n'),
    # Every doc in both lists and in the reply: the text scores all tie, so a narrower window or
    # LIMIT would pick an arbitrary subset of them on every run.
    'hybrid': Shape('hybrid', ['FT.HYBRID', 'idx', 'SEARCH', 'hello', 'VSIM', '@v', '$BLOB',
                               'KNN', 2, 'K', N_DOCS, 'COMBINE', 'RRF', 2, 'WINDOW', N_DOCS,
                               'LIMIT', 0, N_DOCS, 'PARAMS', 2, 'BLOB', ORIGIN], '__key'),
    # SORTBY: without it the cluster reply is not in distance order.
    'knn': Shape('search', ['FT.SEARCH', 'idx', f'*=>[KNN {N_DOCS} @v $b AS d]', 'PARAMS', 2,
                            'b', ORIGIN, 'SORTBY', 'd', 'RETURN', 1, 'n', 'LIMIT', 0, N_DOCS],
                 'id', 'asc'),
    'range': Shape('search', ['FT.SEARCH', 'idx', '@v:[VECTOR_RANGE $r $b]=>{$YIELD_DISTANCE_AS: d}',
                              'PARAMS', 4, 'b', ORIGIN, 'r', RADIUS, 'SORTBY', 'd',
                              'RETURN', 1, 'n', 'LIMIT', 0, N_DOCS], 'id', 'asc'),
}


def _pairs(flat):
    return {flat[i]: flat[i + 1] for i in range(0, len(flat), 2)}


def _parse(kind, res):
    """(rows as dicts, warnings or None where the protocol has no warning channel)."""
    resp3 = isinstance(res, dict)
    if kind == 'hybrid':
        if resp3:
            return [dict(r) for r in res['results']], list(res['warnings'])
        rows = res[res.index('results') + 1]
        return [_pairs(r) for r in rows], list(res[res.index('warnings') + 1])
    if resp3:
        if kind == 'search':
            rows = [{'id': r['id'], **r.get('extra_attributes', {})} for r in res['results']]
        else:
            rows = [dict(r['extra_attributes']) for r in res['results']]
        return rows, list(res['warning'])
    if kind == 'search':
        return [{'id': res[i], **_pairs(res[i + 1])} for i in range(1, len(res), 2)], None
    return [_pairs(r) for r in res[1:]], None


def _search_total(res):
    return res['total_results'] if isinstance(res, dict) else res[0]

class Stopped:
    """One reply of a pressured run: rows, warnings, or the error it ended with. `total` is the
    reply's total count, kept for FT.SEARCH only: the other shapes' totals differ by shape and
    topology (cursor chunks, hybrid fusion, the coordinator's WITHCOUNT)."""
    def __init__(self, rows=(), warnings=None, error=None, first_command_error=False, total=None):
        self.rows, self.warnings, self.error, self.total = list(rows), warnings, error, total
        self.first_command_error = first_command_error


def _run(env, shape):
    """Runs `shape` to its end (every cursor read) and gathers its rows and warnings."""
    if shape.kind != 'cursor':
        try:
            res = env.cmd(*shape.query)
        except redis_exceptions.ResponseError as e:
            return Stopped(error=str(e), first_command_error=True)
        rows, warnings = _parse(shape.kind, res)
        return Stopped(rows, warnings, total=_search_total(res) if shape.kind == 'search' else None)

    rows, warnings = [], [] if env.protocol == 3 else None
    try:
        res, cid = env.cmd(*shape.query)
    except redis_exceptions.ResponseError as e:
        return Stopped(error=str(e), first_command_error=True)
    for _ in range(MAX_CURSOR_READS):
        chunk, chunk_warnings = _parse('aggregate', res)
        rows += chunk
        if warnings is not None:
            warnings += chunk_warnings
        if cid == 0:
            return Stopped(rows, warnings)
        try:
            res, cid = env.cmd('FT.CURSOR', 'READ', 'idx', cid)
        except redis_exceptions.ResponseError as e:
            return Stopped(rows, warnings, error=str(e))
    env.assertTrue(False, message=('cursor did not end', shape.query))
    return Stopped(rows, warnings)


def _targets(env, where):
    """Connections whose process gets the pressure. The coordinator is the process `env.cmd`
    talks to; it also hosts a shard."""
    if not env.isCluster() or where == 'all':
        return env.getOSSMasterNodesConnectionList()
    if where == 'coordinator':
        return [env.getConnection(1)]
    return [env.getConnection(i) for i in range(2, env.shardsCount + 1)]


def _pressure(conns, *args):
    for conn in conns:
        conn.execute_command(debug_cmd(), 'MEMORY_PRESSURE', *args)


def _checkpoint_calls(env, conns, shape):
    """The most checkpoint calls any target process makes for `shape` when nothing fires."""
    _pressure(conns, 'AT_CALL', NEVER)
    try:
        _run(env, shape)
        return max(conn.execute_command(debug_cmd(), 'MEMORY_PRESSURE', 'STATUS')[5]
                   for conn in conns)
    finally:
        _pressure(conns, 'RESET')


def _positions(calls):
    if calls <= MAX_POSITIONS:
        return list(range(1, calls + 1))
    rest = MAX_POSITIONS - EARLY_POSITIONS
    late = {EARLY_POSITIONS + round(i * (calls - EARLY_POSITIONS) / rest) for i in range(1, rest + 1)}
    return sorted(set(range(1, EARLY_POSITIONS + 1)) | late)


def _key_of(shape, row):
    return str(row.get(shape.key))


def _n_of(row):
    return float(row['n'])


class Cell:
    """Checks the replies of one matrix cell against the complete result."""
    def __init__(self, env, shape, where, complete, label):
        self.env, self.shape, self.label = env, shape, label
        # Client cursors are admitted on the process the client talks to, never on a shard.
        self.admission_pressured = shape.kind == 'cursor' and where != 'shards'
        self.complete = complete
        self.complete_keys = [_key_of(shape, r) for r in complete.rows]
        self.complete_by_key = {_key_of(shape, r): r for r in complete.rows}
        self.complete_counts = {_key_of(shape, r): int(r['c']) for r in complete.rows} \
            if shape.kind == 'groupby' else None
        # Fusion re-ranks: a cut subquery lets docs outside the complete top rows in.
        self.universe = {f'doc{i}' for i in range(N_DOCS)} if shape.kind == 'hybrid' \
            else set(self.complete_keys)

    def _is_cut(self, got):
        if self.shape.kind == 'groupby':
            counts = {_key_of(self.shape, r): int(r['c']) for r in got.rows}
            return counts != self.complete_counts
        if self.shape.kind == 'hybrid':
            # Tied fusion scores may come back in another order without anything being cut.
            return {_key_of(self.shape, r) for r in got.rows} != set(self.complete_keys)
        return len(got.rows) < len(self.complete.rows)

    def check(self, got, on_oom, at_call):
        """Returns 'error' or 'cut' when the reply shows the stop (a warning counts as a cut),
        None otherwise."""
        env, msg = self.env, (self.label, at_call, got.error, got.rows, got.warnings)
        if on_oom == 'ignore':
            self.expect_complete(got, msg)
            return None

        if got.error is not None:
            env.assertContains(OOM_QUERY_ERROR, got.error, message=msg)
            # RETURN replies an OOM error only where it cannot keep rows: a client cursor
            # refused at admission.
            env.assertTrue(on_oom == 'fail' or (self.admission_pressured and
                                                got.first_command_error), message=msg)
            # Cursor reads before the failing one keep the invariants too.
            self._check_rows(got, msg)
            return 'error'

        self._check_rows(got, msg)
        cut = self._is_cut(got)
        if got.warnings is not None and cut:
            env.assertNotEqual(got.warnings, [], message=('cut without a warning',) + msg)
        return 'cut' if cut or got.warnings else None

    def _check_rows(self, got, msg):
        """The invariants of any reply, cut or not: known rows once each, in order, with no
        warning but an OOM one."""
        env = self.env
        keys = [_key_of(self.shape, r) for r in got.rows]
        env.assertEqual(len(keys), len(set(keys)), message=('duplicate rows',) + msg)
        env.assertTrue(set(keys) <= self.universe, message=('unknown rows',) + msg)
        env.assertLessEqual(len(keys), len(self.complete_keys), message=msg)
        if self.shape.kind == 'groupby':
            for r in got.rows:
                env.assertLessEqual(int(r['c']),
                                    self.complete_counts.get(_key_of(self.shape, r), 0),
                                    message=msg)
        if self.shape.order:
            ns = [_n_of(r) for r in got.rows]
            env.assertEqual(ns, sorted(ns, reverse=self.shape.order == 'desc'), message=msg)
        # A kept row is the complete result's row for its key; GROUPBY counts may be partial and
        # hybrid fusion scores change with what a cut subquery kept.
        if self.shape.kind not in ('groupby', 'hybrid'):
            for r in got.rows:
                env.assertEqual(r, self.complete_by_key[_key_of(self.shape, r)],
                                message=('row payload',) + msg)

        if got.warnings is not None:
            env.assertTrue(set(got.warnings) <= OOM_WARNINGS, message=('non-OOM warning',) + msg)

    def expect_complete(self, got, msg):
        env = self.env
        env.assertEqual(got.error, None, message=msg)
        if self.shape.kind == 'hybrid':
            # Tied fusion scores may come back in another order; their payloads are scores.
            env.assertEqual(sorted(self.complete_keys),
                            sorted(_key_of(self.shape, r) for r in got.rows), message=msg)
        elif self.shape.order:
            env.assertEqual(got.rows, self.complete.rows, message=msg)
        else:
            def by_key(rows):
                return sorted(rows, key=lambda r: _key_of(self.shape, r))
            env.assertEqual(by_key(got.rows), by_key(self.complete.rows), message=msg)
        if got.warnings is not None:
            env.assertEqual(got.warnings, [], message=msg)
        if got.total is not None:
            env.assertEqual(got.total, self.complete.total, message=('total_results',) + msg)


CONFIGS = ('ON_OOM', 'ON_TIMEOUT', 'WORKERS', 'TIMEOUT', '_MAX_FOREGROUND_TIMEOUT_LIMIT')


def _get_config(env, name):
    res = env.cmd(config_cmd(), 'GET', name)
    return res[name] if isinstance(res, dict) else res[0][1]


def _set_policies(env, on_oom, on_timeout):
    verify_command_OK_on_all_shards(env, config_cmd(), 'SET', 'ON_OOM', on_oom)
    verify_command_OK_on_all_shards(env, config_cmd(), 'SET', 'ON_TIMEOUT', on_timeout)


def _create_index(env):
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'name', 'TEXT', 'n', 'NUMERIC', 'SORTABLE',
               'g', 'NUMERIC', 'SORTABLE',
               'v', 'VECTOR', 'FLAT', 6, 'TYPE', 'FLOAT32', 'DIM', 2, 'DISTANCE_METRIC', 'L2').ok()
    conn = getConnectionByEnv(env)
    for i in range(N_DOCS):
        conn.execute_command('HSET', f'doc{i}', 'name', 'hello', 'n', i, 'g', i % N_GROUPS,
                             'v', np.array([float(i), float(i)], dtype=np.float32).tobytes())


def _open_cursors(env):
    """Client and coordinator-to-shard cursors on `idx` (the coordinator's FT.INFO sums the
    shards)."""
    info = env.cmd('FT.INFO', 'idx')
    stats = info['cursor_stats'] if isinstance(info, dict) else to_dict(to_dict(info)['cursor_stats'])
    return int(stats['index_total']) + int(stats['index_total_internal'])


def _expect_no_open_cursors(env, label):
    # A coordinator frees its shard cursors asynchronously after the client's cursor ends.
    with TimeLimit(10, f'cursor left open: {label}'):
        while _open_cursors(env):
            time.sleep(0.05)


def _matrix(env, where):
    """Sweeps every shape x ON_OOM x ON_TIMEOUT x WORKERS cell with pressure on `where`."""
    _create_index(env)
    saved = {name: _get_config(env, name) for name in CONFIGS}
    # No deadline: a timeout warning is then always a mislabelled stop. Without the limit
    # override, TIMEOUT 0 with WORKERS 0 is capped with a warning of its own.
    conns = _targets(env, where)
    try:
        verify_command_OK_on_all_shards(env, config_cmd(), 'SET', '_MAX_FOREGROUND_TIMEOUT_LIMIT', 0)
        verify_command_OK_on_all_shards(env, config_cmd(), 'SET', 'TIMEOUT', 0)
        for workers in WORKER_COUNTS:
            set_workers(env, workers)
            for on_timeout in ON_TIMEOUT_POLICIES:
                for on_oom in ON_OOM_POLICIES:
                    _set_policies(env, on_oom, on_timeout)
                    for name, shape in SHAPES.items():
                        label = (where, name, f'ON_OOM {on_oom}', f'ON_TIMEOUT {on_timeout}',
                                 f'WORKERS {workers}', f'RESP{env.protocol}')
                        _sweep(env, conns, where, shape, on_oom, label)
    finally:
        _pressure(env.getOSSMasterNodesConnectionList(), 'RESET')
        set_workers(env, saved['WORKERS'])
        for name in CONFIGS:
            if name != 'WORKERS':
                verify_command_OK_on_all_shards(env, config_cmd(), 'SET', name, saved[name])


def _expect_seeded(env, shape, complete, label):
    """The complete result is the oracle of every pressured run, so it must match the seeded data
    (see _create_index) rather than merely be nonempty."""
    rows = complete.rows
    if shape.kind == 'groupby':
        env.assertEqual({int(float(r['g'])): int(r['c']) for r in rows},
                        {g: N_DOCS // N_GROUPS for g in range(N_GROUPS)}, message=label)
        return
    keys = [_key_of(shape, r) for r in rows]
    env.assertEqual(len(keys), len(set(keys)), message=('duplicate rows',) + label)
    if shape.kind in ('search', 'hybrid'):
        docs = [int(k[len('doc'):]) for k in keys]
    else:
        docs = [int(_n_of(r)) for r in rows]
    # VECTOR_RANGE sits on the radius boundary, so only its interior is pinned.
    if shape is SHAPES['range']:
        env.assertTrue(set(range(RADIUS_DOCS)) <= set(docs) <= set(range(RADIUS_DOCS + 1)),
                       message=('range rows',) + label)
    else:
        env.assertEqual(sorted(docs), list(range(N_DOCS)), message=label)
    if shape.kind == 'search':
        for r, doc in zip(rows, docs):
            env.assertEqual(int(_n_of(r)), doc, message=('row payload',) + label)
        env.assertEqual(complete.total, len(rows), message=('total_results',) + label)

def _sweep(env, conns, where, shape, on_oom, label):
    complete = _run(env, shape)
    env.assertEqual(complete.error, None, message=label)
    _expect_seeded(env, shape, complete, label)
    cell = Cell(env, shape, where, complete, label)

    calls = _checkpoint_calls(env, conns, shape)
    env.assertGreater(calls, 0, message=label)
    outcomes = set()
    for at_call in _positions(calls):
        _pressure(conns, 'AT_CALL', at_call)
        try:
            got = _run(env, shape)
        finally:
            _pressure(conns, 'RESET')
        outcomes.add(cell.check(got, on_oom, at_call))
    if on_oom != 'ignore':
        env.assertTrue(outcomes - {None}, message=('no position stopped the query',) + label)
    if on_oom == 'fail':
        # The build stages decide FAIL before any row is produced, so some position must error.
        env.assertContains('error', outcomes, message=('FAIL never failed',) + label)

    cell.expect_complete(_run(env, shape), ('state left behind',) + label)
    _expect_no_open_cursors(env, label)


@skip(cluster=True)
@env_spec(protocol=3)
def test_matrix_standalone_resp3(env):
    """Standalone, RESP3: replies carry warnings, so every cut must be warned."""
    _matrix(env, 'all')


@skip(cluster=True)
@env_spec(protocol=2)
def test_matrix_standalone_resp2(env):
    """Standalone, RESP2: no warning channel outside FT.HYBRID; cuts are checked for shape only."""
    _matrix(env, 'all')


@skip(cluster=False, min_shards=2)
@env_spec(protocol=3)
def test_matrix_shards_resp3(env):
    """Every shard but the coordinator's process stops: the coordinator must merge the rest and warn."""
    _matrix(env, 'shards')


@skip(cluster=False, min_shards=2)
@env_spec(protocol=2)
def test_matrix_shards_resp2(env):
    """As test_matrix_shards_resp3, in RESP2."""
    _matrix(env, 'shards')


@skip(cluster=False, min_shards=2)
@env_spec(protocol=3)
def test_matrix_coordinator_resp3(env):
    """Only the coordinator's process stops (its own pipelines and its co-located shard)."""
    _matrix(env, 'coordinator')


@skip(cluster=False, min_shards=2)
@env_spec(protocol=2)
def test_matrix_coordinator_resp2(env):
    """As test_matrix_coordinator_resp3, in RESP2."""
    _matrix(env, 'coordinator')


@skip(cluster=False)
@env_spec(protocol=3)
def test_matrix_every_process_resp3(env):
    """Every process stops at the same call position of its own."""
    _matrix(env, 'all')


@skip(cluster=False)
@env_spec(protocol=2)
def test_matrix_every_process_resp2(env):
    """As test_matrix_every_process_resp3, in RESP2."""
    _matrix(env, 'all')
