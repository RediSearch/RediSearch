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
                 dialect=None):
        self.name = name
        self.schema = schema
        self.docs = docs
        self.query = query
        self.ttl_field = ttl_field
        self.profile_type = profile_type
        self.params = params or []
        self.dialect = dialect


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


def setup_case(env, case):
    """Create the index for `case`, load its documents, and give `doc:1` a TTL."""
    conn = getConnectionByEnv(env)
    env.flush()
    conn.execute_command('DEBUG', 'SET-ACTIVE-EXPIRE', '0')
    env.expect('FT.CREATE', 'idx', 'SCHEMA', *case.schema).ok()
    env.cmd(debug_cmd(), 'SET_MONITOR_EXPIRATION', 'idx', 'fields')
    for doc_id, fields in case.docs.items():
        conn.execute_command('HSET', doc_id, *chain_fields(fields))
    conn.execute_command('HPEXPIRE', 'doc:1', str(TTL_MS), 'FIELDS', '1', case.ttl_field)
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
