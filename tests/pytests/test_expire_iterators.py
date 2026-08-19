"""Field-expiry (`HEXPIRE`) behaviour, per query iterator.

Every iterator that consults the expiration checker is reached here by its own
query shape, and each one is put through the same assertions. The point is
comparison: where two iterators treat the same expiry situation differently, the
matrix reports it as one failing row rather than burying it in an aggregate.

Each case asserts, through `FT.PROFILE`, that the query really did reach the
iterator the case names — a matrix that silently exercises the wrong iterator is
worse than no matrix at all.

Expiry is driven by `_FT.DEBUG MOCK_QUERY_TIME`, which shifts the instant a query
evaluates TTLs against. That keeps the tests off the wall clock: TTLs are set far
in the future and the clock is moved past them on demand.
"""

from common import *

# Offsets used to place the query instant either side of the TTLs below. Both are
# far enough from `TTL_MS` that a coarse clock cannot land between them.
TTL_MS = 100_000
BEFORE_EXPIRY_MS = 0
AFTER_EXPIRY_MS = 200_000

# A vector close to `doc:1`'s, so KNN ordering is by construction rather than by
# tie-break.
VEC_QUERY = 'aaaaaaaa'


class IteratorCase:
    """How to reach one iterator, and how to prove the query got there.

    `docs` maps document id to its full field set; `ttl_field` is the field given
    a TTL on `doc:1`, so the two documents differ only in whether the queried
    field has lapsed.
    """

    def __init__(self, name, schema, docs, query, ttl_field, profile_type, params=None,
                 dialect=None, yield_order=('doc:1', 'doc:2')):
        self.name = name
        self.schema = schema
        self.docs = docs
        self.query = query
        self.ttl_field = ttl_field
        self.profile_type = profile_type
        self.params = params or []
        self.dialect = dialect
        # The order the iterator yields its two documents in. Ascending doc id for
        # everything that walks the index; nearest-first for the vector iterator,
        # which orders by score instead.
        self.yield_order = yield_order


TEXT_CASE = IteratorCase(
    name='text',
    schema=('t', 'TEXT'),
    docs={'doc:1': {'t': 'hello'}, 'doc:2': {'t': 'hello'}},
    query='@t:hello',
    ttl_field='t',
    profile_type='TEXT',
)

TAG_CASE = IteratorCase(
    name='tag',
    schema=('tg', 'TAG'),
    docs={'doc:1': {'tg': 'x'}, 'doc:2': {'tg': 'x'}},
    query='@tg:{x}',
    ttl_field='tg',
    profile_type='TAG',
)

NUMERIC_CASE = IteratorCase(
    name='numeric',
    schema=('n', 'NUMERIC'),
    docs={'doc:1': {'n': '1'}, 'doc:2': {'n': '2'}},
    query='@n:[0 100]',
    ttl_field='n',
    profile_type='NUMERIC',
)

GEO_CASE = IteratorCase(
    name='geo',
    schema=('g', 'GEO'),
    docs={'doc:1': {'g': '1,1'}, 'doc:2': {'g': '1.001,1.001'}},
    query='@g:[1 1 100 km]',
    ttl_field='g',
    profile_type='GEO',
)

GEOSHAPE_CASE = IteratorCase(
    name='geoshape',
    schema=('geom', 'GEOSHAPE', 'FLAT'),
    docs={'doc:1': {'geom': 'POLYGON((1 1, 1 100, 100 100, 100 1, 1 1))'},
          'doc:2': {'geom': 'POLYGON((1 1, 1 120, 120 120, 120 1, 1 1))'}},
    query='@geom:[within $poly]',
    ttl_field='geom',
    profile_type='GEO-SHAPE',
    params=['PARAMS', 2, 'poly', 'POLYGON((0 0, 0 150, 150 150, 150 0, 0 0))'],
    dialect=3,
)

VECTOR_CASE = IteratorCase(
    name='vector',
    schema=('v', 'VECTOR', 'FLAT', '6', 'TYPE', 'FLOAT32', 'DIM', '2',
            'DISTANCE_METRIC', 'L2', 't', 'TEXT'),
    docs={'doc:1': {'v': 'bababaca', 't': 'hello'},
          'doc:2': {'v': 'babababa', 't': 'hello'}},
    query='@t:hello=>[KNN 2 @v $vec]',
    ttl_field='v',
    profile_type='VECTOR',
    params=['PARAMS', 2, 'vec', VEC_QUERY],
    dialect=3,
    yield_order=('doc:2', 'doc:1'),
)

ALL_CASES = [TEXT_CASE, TAG_CASE, NUMERIC_CASE, GEO_CASE, GEOSHAPE_CASE, VECTOR_CASE]


def profile_iterator_types(node):
    """Every `Type` value in an `FT.PROFILE` reply, in traversal order.

    Walks the reply rather than indexing into it, so nesting (an intersection
    above a leaf) and the extra `Shard ID` key in enterprise replies do not
    change what it finds.
    """
    found = []
    if isinstance(node, dict):
        for key, value in node.items():
            if key == 'Type' and isinstance(value, str):
                found.append(value)
            else:
                found.extend(profile_iterator_types(value))
    elif isinstance(node, (list, tuple)):
        for i, value in enumerate(node):
            if value == 'Type' and i + 1 < len(node) and isinstance(node[i + 1], str):
                found.append(node[i + 1])
            else:
                found.extend(profile_iterator_types(value))
    return found


def setup_case(env, case, ttl_doc='doc:1'):
    """Create the index for `case`, load its documents, and give `ttl_doc` a TTL
    on the queried field. The two documents then differ only in whether that field
    has lapsed."""
    conn = getConnectionByEnv(env)
    env.flush()
    conn.execute_command('DEBUG', 'SET-ACTIVE-EXPIRE', '0')
    env.expect('FT.CREATE', 'idx', 'SCHEMA', *case.schema).ok()
    env.cmd(debug_cmd(), 'SET_MONITOR_EXPIRATION', 'idx', 'fields')
    for doc_id, fields in case.docs.items():
        conn.execute_command('HSET', doc_id, *chain_fields(fields))
    if ttl_doc is not None:
        conn.execute_command('HPEXPIRE', ttl_doc, str(TTL_MS), 'FIELDS', '1', case.ttl_field)
    return conn


def chain_fields(fields):
    for key, value in fields.items():
        yield key
        yield value


def search_args(case, *extra):
    args = ['idx', case.query, *extra, 'NOCONTENT', *case.params]
    if case.dialect is not None:
        args += ['DIALECT', case.dialect]
    return args


def search_ids(env, case):
    """`(total, sorted ids)` for the case's query. Sorted so score order — which
    differs between the vector case and the rest — does not enter the assertion."""
    res = env.cmd('FT.SEARCH', *search_args(case))
    return res[0], sorted(res[1:])


def assert_reaches_iterator(env, case):
    res = env.cmd('FT.PROFILE', 'idx', 'SEARCH', 'QUERY', *search_args(case)[1:])
    types = profile_iterator_types(res)
    env.assertContains(case.profile_type, types,
                       message=f'{case.name}: profile types were {types}')


def set_query_time(env, offset_ms):
    env.cmd(debug_cmd(), 'MOCK_QUERY_TIME', str(offset_ms))


def run_baseline(env, case):
    """A field TTL hides its document from the queried iterator, and only that one.

    Both documents match the query; only `doc:1` carries a TTL on the queried
    field. Before the query instant reaches it both are returned; after, only
    `doc:2` is.
    """
    setup_case(env, case)
    try:
        assert_reaches_iterator(env, case)

        set_query_time(env, BEFORE_EXPIRY_MS)
        env.assertEqual(search_ids(env, case), (2, ['doc:1', 'doc:2']),
                        message=f'{case.name}: before expiry')

        set_query_time(env, AFTER_EXPIRY_MS)
        env.assertEqual(search_ids(env, case), (1, ['doc:2']),
                        message=f'{case.name}: after expiry')
    finally:
        env.cmd(debug_cmd(), 'MOCK_QUERY_TIME', 'disable')


@skip(cluster=True, redis_less_than='7.4')
def test_expiry_baseline_text(env):
    run_baseline(env, TEXT_CASE)


@skip(cluster=True, redis_less_than='7.4')
def test_expiry_baseline_tag(env):
    run_baseline(env, TAG_CASE)


@skip(cluster=True, redis_less_than='7.4')
def test_expiry_baseline_numeric(env):
    run_baseline(env, NUMERIC_CASE)


@skip(cluster=True, redis_less_than='7.4')
def test_expiry_baseline_geo(env):
    run_baseline(env, GEO_CASE)


@skip(cluster=True, redis_less_than='8.0')
def test_expiry_baseline_geoshape(env):
    run_baseline(env, GEOSHAPE_CASE)


@skip(cluster=True, redis_less_than='8.0')
def test_expiry_baseline_vector(env):
    run_baseline(env, VECTOR_CASE)


def aggregate_cursor(env, case, count):
    """Open a cursor over the case's query, loading each document's key."""
    args = ['idx', case.query, 'LOAD', 1, '@__key', *case.params]
    if case.dialect is not None:
        args += ['DIALECT', case.dialect]
    args += ['WITHCURSOR', 'COUNT', count]
    return env.cmd('FT.AGGREGATE', *args)


def rows_to_keys(rows):
    """The document keys in one `FT.AGGREGATE` chunk."""
    return [to_dict(row)['__key'] for row in rows[1:]]


def drain_cursor(env, cursor):
    """Read a cursor to exhaustion, returning every key it still yields."""
    keys = []
    while cursor:
        rows, cursor = env.cmd('FT.CURSOR', 'READ', 'idx', cursor)
        keys += rows_to_keys(rows)
    return keys


def run_liveness_across_cursor_reads(env, case):
    """A field that lapses between two cursor reads stops being yielded.

    Each `FT.CURSOR READ` re-stamps the instant the round evaluates TTLs against,
    so a document still live when the cursor opened must disappear once the clock
    has moved past its TTL — unless the iterator decided its filtering strategy
    when the cursor was built and never revisited it.

    The TTL goes on the document yielded *second*, so the clock moves while it is
    still in flight. The first assertion pins that: if it comes out of the opening
    chunk instead, the shift lands after it was already returned and the rest of
    the test proves nothing.
    """
    early_doc, late_doc = case.yield_order
    setup_case(env, case, ttl_doc=late_doc)
    try:
        set_query_time(env, BEFORE_EXPIRY_MS)
        rows, cursor = aggregate_cursor(env, case, 1)
        seen_before = rows_to_keys(rows)

        env.assertEqual(seen_before, [early_doc],
                        message=f'{case.name}: opening chunk was {seen_before}, so the clock '
                                f'shift cannot land while the TTL document is in flight')

        set_query_time(env, AFTER_EXPIRY_MS)
        seen_after = drain_cursor(env, cursor)

        env.assertEqual(late_doc in seen_after, False,
                        message=f'{case.name}: expired doc yielded after the clock moved, '
                                f'after={seen_after}')
    finally:
        env.cmd(debug_cmd(), 'MOCK_QUERY_TIME', 'disable')


def run_ttl_table_emptied_mid_cursor(env, case):
    """Removing the last document carrying a field TTL mid-cursor is survivable.

    The TTL table is freed when its last entry leaves, while iterators built
    before that may still hold a decision that assumed it was there. Draining the
    cursor afterwards must still yield the live document.
    """
    conn = setup_case(env, case)
    try:
        set_query_time(env, BEFORE_EXPIRY_MS)
        rows, cursor = aggregate_cursor(env, case, 1)
        seen = rows_to_keys(rows)

        conn.execute_command('DEL', 'doc:1')
        seen += drain_cursor(env, cursor)

        env.assertContains('doc:2', seen,
                           message=f'{case.name}: live doc lost after the TTL table emptied, '
                                   f'seen={seen}')
    finally:
        env.cmd(debug_cmd(), 'MOCK_QUERY_TIME', 'disable')


@skip(cluster=True, redis_less_than='7.4')
def test_expiry_liveness_text(env):
    run_liveness_across_cursor_reads(env, TEXT_CASE)


@skip(cluster=True, redis_less_than='7.4')
def test_expiry_liveness_tag(env):
    run_liveness_across_cursor_reads(env, TAG_CASE)


@skip(cluster=True, redis_less_than='7.4')
def test_expiry_liveness_numeric(env):
    run_liveness_across_cursor_reads(env, NUMERIC_CASE)


@skip(cluster=True, redis_less_than='7.4')
def test_expiry_liveness_geo(env):
    run_liveness_across_cursor_reads(env, GEO_CASE)


@skip(cluster=True, redis_less_than='8.0')
def test_expiry_liveness_geoshape(env):
    run_liveness_across_cursor_reads(env, GEOSHAPE_CASE)


@skip(cluster=True, redis_less_than='8.0')
def test_expiry_liveness_vector(env):
    run_liveness_across_cursor_reads(env, VECTOR_CASE)


@skip(cluster=True, redis_less_than='7.4')
def test_ttl_table_emptied_mid_cursor_text(env):
    run_ttl_table_emptied_mid_cursor(env, TEXT_CASE)


@skip(cluster=True, redis_less_than='7.4')
def test_ttl_table_emptied_mid_cursor_numeric(env):
    run_ttl_table_emptied_mid_cursor(env, NUMERIC_CASE)


@skip(cluster=True, redis_less_than='8.0')
def test_ttl_table_emptied_mid_cursor_vector(env):
    run_ttl_table_emptied_mid_cursor(env, VECTOR_CASE)


def run_gate_armed_mid_cursor(env, case):
    """A field TTL created after the cursor opened is honoured on later reads.

    With no TTL anywhere in the index the expiration gate is off, and an iterator
    is free to commit to a no-filtering read path when it is built. Arming the
    gate mid-cursor asks whether that decision is ever revisited: the document is
    yielded second, so it is still in flight when its TTL appears and lapses.
    """
    early_doc, late_doc = case.yield_order
    conn = setup_case(env, case, ttl_doc=None)
    try:
        set_query_time(env, BEFORE_EXPIRY_MS)
        rows, cursor = aggregate_cursor(env, case, 1)
        seen_before = rows_to_keys(rows)

        env.assertEqual(seen_before, [early_doc],
                        message=f'{case.name}: opening chunk was {seen_before}, so the gate '
                                f'cannot be armed while the TTL document is in flight')

        conn.execute_command('HPEXPIRE', late_doc, str(TTL_MS), 'FIELDS', '1', case.ttl_field)
        set_query_time(env, AFTER_EXPIRY_MS)
        seen_after = drain_cursor(env, cursor)

        env.assertEqual(late_doc in seen_after, False,
                        message=f'{case.name}: document expired by a TTL set after the cursor '
                                f'opened was still yielded, after={seen_after}')
    finally:
        env.cmd(debug_cmd(), 'MOCK_QUERY_TIME', 'disable')


@skip(cluster=True, redis_less_than='7.4')
def test_gate_armed_mid_cursor_text(env):
    run_gate_armed_mid_cursor(env, TEXT_CASE)


@skip(cluster=True, redis_less_than='7.4')
def test_gate_armed_mid_cursor_tag(env):
    run_gate_armed_mid_cursor(env, TAG_CASE)


@skip(cluster=True, redis_less_than='7.4')
def test_gate_armed_mid_cursor_numeric(env):
    run_gate_armed_mid_cursor(env, NUMERIC_CASE)


@skip(cluster=True, redis_less_than='7.4')
def test_gate_armed_mid_cursor_geo(env):
    run_gate_armed_mid_cursor(env, GEO_CASE)


@skip(cluster=True, redis_less_than='8.0')
def test_gate_armed_mid_cursor_geoshape(env):
    run_gate_armed_mid_cursor(env, GEOSHAPE_CASE)


@skip(cluster=True, redis_less_than='8.0')
def test_gate_armed_mid_cursor_vector(env):
    run_gate_armed_mid_cursor(env, VECTOR_CASE)


@skip(cluster=True, redis_less_than='7.4')
def test_hexpire_on_iterated_numeric_field_mid_cursor(env):
    """`HPEXPIRE` on the numeric field a suspended cursor is iterating.

    Reduced from `test_gate_armed_mid_cursor_numeric`, and reached with no debug
    command in the sequence. Narrowing, for whoever picks this up:

    - a plain `HSET` on the same field mid-cursor is fine;
    - `HPEXPIRE` on a field absent from the schema is fine;
    - `HPEXPIRE` on the field under iteration is fine too, if the index already
      held a field TTL when the cursor opened;
    - only the index acquiring its *first* field TTL mid-cursor is not.

    Written up in `.vscode/docs/expiry_tests/numeric-cursor-abort.md`.
    """
    conn = getConnectionByEnv(env)
    conn.execute_command('DEBUG', 'SET-ACTIVE-EXPIRE', '0')
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'n', 'NUMERIC').ok()
    conn.execute_command('HSET', 'doc:1', 'n', '1')
    conn.execute_command('HSET', 'doc:2', 'n', '2')

    rows, cursor = env.cmd('FT.AGGREGATE', 'idx', '@n:[0 100]',
                           'LOAD', 1, '@__key', 'WITHCURSOR', 'COUNT', 1)
    env.assertEqual(rows_to_keys(rows), ['doc:1'], message=rows)

    conn.execute_command('HPEXPIRE', 'doc:2', str(TTL_MS), 'FIELDS', '1', 'n')
    seen = drain_cursor(env, cursor)

    env.assertEqual(seen, ['doc:2'], message=seen)


@skip(cluster=True, redis_less_than='7.4')
def test_first_field_ttl_mid_cursor_drops_matching_documents(env):
    """The index acquiring its first field TTL mid-cursor loses matching documents.

    Same trigger as `test_hexpire_on_iterated_numeric_field_mid_cursor`, at an
    index size where the damaged read lands on an entry boundary instead of
    mid-entry: no error, no crash, just a short answer. The expired document is
    given a far-future TTL, so every one of the documents is still live and all
    of them are expected back.

    Controls, all of which return the full set:
    - no `HPEXPIRE` at all;
    - `HPEXPIRE` before the cursor opens, none during;
    - a TTL already present when the cursor opens, plus one during.

    Only the index going from *no* field TTL to its first one mid-cursor loses
    documents. Written up in
    `.vscode/docs/expiry_tests/numeric-cursor-lost-documents.md`.
    """
    n_docs = 200
    conn = getConnectionByEnv(env)
    conn.execute_command('DEBUG', 'SET-ACTIVE-EXPIRE', '0')
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'n', 'NUMERIC').ok()
    for i in range(1, n_docs + 1):
        conn.execute_command('HSET', f'doc:{i}', 'n', str(i))

    rows, cursor = env.cmd('FT.AGGREGATE', 'idx', '@n:[0 100000]',
                           'LOAD', 1, '@__key', 'WITHCURSOR', 'COUNT', 1)
    seen = rows_to_keys(rows)

    conn.execute_command('HPEXPIRE', 'doc:2', str(TTL_MS), 'FIELDS', '1', 'n')
    seen += drain_cursor(env, cursor)

    env.assertEqual(len(seen), n_docs,
                    message=f'returned {len(seen)} of {n_docs}, '
                            f'{n_docs - len(seen)} matching documents lost')
