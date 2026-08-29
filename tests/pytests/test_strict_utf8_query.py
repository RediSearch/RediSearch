import os
import struct

from common import *

"""
Tests for what search-_enable-next-major-breaking-changes does to queries.

Off (the default) a query carrying bytes that are not well-formed UTF-8 is parsed and run as it
always has been. On, such a query is refused, so that the bytes never reach the index and never
travel back to the client inside a result or an error.

The verdict is recorded on the term the client sent, before unescaping and case folding get to
reinterpret whatever they cannot decode. It is acted on once the term's field is known: the
setting governs TEXT values, so a term under a TAG field stays queryable with whatever bytes the
tag index accepted. The config is immutable, so each setting needs its own server.
"""

CONFIG_NAME = 'search-_enable-next-major-breaking-changes'

# 'caf\xe9' is Latin-1, so the term is ill-formed while the query around it is not.
ILL_FORMED = b'caf\xe9'


def _startEnv(name, strict):
    conf = f'/tmp/{name}.conf'
    if os.path.isfile(conf):
        os.unlink(conf)
    with open(conf, 'w') as f:
        f.write(f'{CONFIG_NAME} {"yes" if strict else "no"}\n')
        f.write('search-default-dialect 2\n')

    env = Env(noDefaultModuleArgs=True, redisConfigFile=conf)
    if env.env == 'existing-env':
        env.skip()
    env.expect('FT.CREATE', 'idx', 'SCHEMA',
               't', 'TEXT',
               'tag', 'TAG',
               'v', 'VECTOR', 'FLAT', '6', 'TYPE', 'FLOAT32', 'DIM', '2', 'DISTANCE_METRIC', 'L2').ok()
    return env


def _expectRefused(env, *query):
    env.expect(*query).error().contains('Invalid UTF-8 value in query')


@skip(cluster=True, redis_less_than='7.9.227')
def test_ill_formed_query_runs_when_off():
    env = _startEnv('strict_utf8_query_off', strict=False)

    # The term matches nothing, but the query itself is accepted: the default behaviour is
    # unchanged by the feature existing.
    env.expect('FT.SEARCH', 'idx', ILL_FORMED, 'NOCONTENT').equal([0])
    env.expect('FT.SEARCH', 'idx', b'@t:' + ILL_FORMED, 'NOCONTENT').equal([0])
    env.stop()


@skip(cluster=True, redis_less_than='7.9.227')
def test_ill_formed_text_terms_refused():
    env = _startEnv('strict_utf8_query_text', strict=True)

    # Every shape a TEXT term can take reaches its node through a different constructor, so each
    # one is worth pinning separately.
    _expectRefused(env, 'FT.SEARCH', 'idx', ILL_FORMED)                        # bare term
    _expectRefused(env, 'FT.SEARCH', 'idx', b'@t:' + ILL_FORMED)               # field-scoped
    _expectRefused(env, 'FT.SEARCH', 'idx', ILL_FORMED + b'*')                 # prefix
    _expectRefused(env, 'FT.SEARCH', 'idx', b'*' + ILL_FORMED)                 # suffix
    _expectRefused(env, 'FT.SEARCH', 'idx', b'%' + ILL_FORMED + b'%')          # fuzzy
    _expectRefused(env, 'FT.SEARCH', 'idx', b"w'" + ILL_FORMED + b"*'")        # wildcard
    _expectRefused(env, 'FT.SEARCH', 'idx', b'"alpha ' + ILL_FORMED + b'"')    # inside a phrase
    _expectRefused(env, 'FT.SEARCH', 'idx', b'alpha | ' + ILL_FORMED)          # under a union
    _expectRefused(env, 'FT.SEARCH', 'idx', b'-' + ILL_FORMED)                 # negated
    env.stop()


@skip(cluster=True, redis_less_than='7.9.227')
def test_ill_formed_param_refused():
    # A parameter's value is unknown at parse time, so it is checked once the parameters are
    # bound instead.
    env = _startEnv('strict_utf8_query_param', strict=True)

    _expectRefused(env, 'FT.SEARCH', 'idx', '@t:$p', 'PARAMS', '2', 'p', ILL_FORMED)
    _expectRefused(env, 'FT.SEARCH', 'idx', '@t:$p*', 'PARAMS', '2', 'p', ILL_FORMED)
    env.stop()


@skip(cluster=True, redis_less_than='7.9.227')
def test_ill_formed_refused_across_commands():
    # Every command that parses a query goes through the same check, including the ones that only
    # describe the plan and never touch the index.
    env = _startEnv('strict_utf8_query_commands', strict=True)

    _expectRefused(env, 'FT.SEARCH', 'idx', ILL_FORMED)
    _expectRefused(env, 'FT.AGGREGATE', 'idx', ILL_FORMED)
    _expectRefused(env, 'FT.EXPLAIN', 'idx', ILL_FORMED)
    _expectRefused(env, 'FT.EXPLAINCLI', 'idx', ILL_FORMED)
    _expectRefused(env, 'FT.PROFILE', 'idx', 'SEARCH', 'QUERY', ILL_FORMED)
    _expectRefused(env, 'FT.SPELLCHECK', 'idx', ILL_FORMED)
    _expectRefused(env, 'FT.SPELLCHECK', 'idx', ILL_FORMED)
    env.stop()


@skip(cluster=True, redis_less_than='7.9.227')
def test_ill_formed_tag_term_accepted():
    # Indexing only refuses TEXT values, so an ill-formed TAG value is in the index and has to
    # stay reachable.
    env = _startEnv('strict_utf8_query_tag', strict=True)
    env.cmd('HSET', 'doc1', 'tag', ILL_FORMED)

    env.expect('FT.SEARCH', 'idx', b'@tag:{' + ILL_FORMED + b'}', 'NOCONTENT').equal([1, 'doc1'])
    env.expect('FT.SEARCH', 'idx', b'@tag:{' + ILL_FORMED + b'*}', 'NOCONTENT').equal([1, 'doc1'])
    env.expect('FT.SEARCH', 'idx', '@tag:{$p}', 'NOCONTENT',
               'PARAMS', '2', 'p', ILL_FORMED).equal([1, 'doc1'])
    env.stop()


@skip(cluster=True, redis_less_than='7.9.227')
def test_vector_blob_param_accepted():
    # A KNN blob is raw floats, which are almost never well-formed UTF-8. Rejecting it would make
    # vector search unusable, so parameter values are checked by the type the query gives them.
    env = _startEnv('strict_utf8_query_vector', strict=True)
    env.cmd('HSET', 'doc1', 'v', struct.pack('<2f', 1.0, 1.0))
    blob = struct.pack('<2f', 0.9, 0.9)

    env.expect('FT.SEARCH', 'idx', '*=>[KNN 1 @v $b]', 'NOCONTENT',
               'PARAMS', '2', 'b', blob).equal([1, 'doc1'])
    env.stop()


@skip(cluster=True, redis_less_than='7.9.227')
def test_well_formed_multibyte_query_accepted():
    env = _startEnv('strict_utf8_query_valid', strict=True)
    env.cmd('HSET', 'doc1', 't', 'café 日本語'.encode())

    env.expect('FT.SEARCH', 'idx', 'café', 'NOCONTENT').equal([1, 'doc1'])
    env.expect('FT.SEARCH', 'idx', '日本語', 'NOCONTENT').equal([1, 'doc1'])
    env.expect('FT.SEARCH', 'idx', '@t:$p', 'NOCONTENT',
               'PARAMS', '2', 'p', 'café').equal([1, 'doc1'])
    env.stop()
