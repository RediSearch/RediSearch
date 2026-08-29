import os

from common import *

"""
Tests for what search-_enable-next-major-breaking-changes does to indexing.

Off (the default) the engine indexes whatever bytes arrive, as it always has. On, a document
carrying a TEXT value that is not well-formed UTF-8 is refused whole, and the refusal surfaces
as an FT.INFO indexing error.

The check is made on the whole field value before tokenization, not on the tokens it yields: a
token carrying the ill-formed bytes can disappear inside the tokenizer — as a stopword, or once
normalization has re-encoded what it could not decode — and a document that must not be indexed
cannot rely on that token surviving. The config is immutable, so each setting needs its own
server.
"""

CONFIG_NAME = 'search-_enable-next-major-breaking-changes'

# 'caf\xe9' is Latin-1, so the middle token is ill-formed while the ones around it are not. The
# well-formed neighbours are the point: refusing takes them down with the document.
ILL_FORMED = b'alpha caf\xe9 omega'


def _startEnv(name, strict):
    conf = f'/tmp/{name}.conf'
    if os.path.isfile(conf):
        os.unlink(conf)
    with open(conf, 'w') as f:
        f.write(f'{CONFIG_NAME} {"yes" if strict else "no"}\n')

    env = Env(noDefaultModuleArgs=True, redisConfigFile=conf)
    if env.env == 'existing-env':
        env.skip()
    return env


def _createAndAdd(env, value, key='doc1'):
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 't', 'TEXT').ok()
    env.cmd('HSET', key, 't', value)


@skip(cluster=True, redis_less_than='7.9.227')
def test_ill_formed_indexed_when_off():
    env = _startEnv('strict_utf8_off', strict=False)
    _createAndAdd(env, ILL_FORMED)

    # All three tokens indexed: the default behaviour is unchanged by the feature existing.
    assertInfoField(env, 'idx', 'num_terms', 3)
    assertInfoField(env, 'idx', 'num_docs', 1)
    env.assertEqual(int(index_errors(env)['indexing failures']), 0)
    env.stop()


@skip(cluster=True, redis_less_than='7.9.227')
def test_ill_formed_document_rejected():
    env = _startEnv('strict_utf8_reject', strict=True)
    _createAndAdd(env, ILL_FORMED)

    # Nothing of the document survives, not even its well-formed tokens.
    assertInfoField(env, 'idx', 'num_docs', 0)
    assertInfoField(env, 'idx', 'num_terms', 0)
    errs = index_errors(env)
    env.assertEqual(int(errs['indexing failures']), 1)
    env.assertContains('Invalid UTF-8 value', errs['last indexing error'])
    env.assertContains('t', errs['last indexing error'])
    env.expect('FT.SEARCH', 'idx', 'alpha', 'NOCONTENT').equal([0])
    env.stop()


@skip(cluster=True, redis_less_than='7.9.227')
def test_ill_formed_field_rejects_sibling_fields():
    # The unit refused is the document, so a well-formed field indexes nothing either.
    env = _startEnv('strict_utf8_sibling', strict=True)
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 't', 'TEXT', 'u', 'TEXT').ok()
    env.cmd('HSET', 'doc1', 't', ILL_FORMED, 'u', 'sibling')

    assertInfoField(env, 'idx', 'num_docs', 0)
    env.assertEqual(int(index_errors(env)['indexing failures']), 1)
    env.expect('FT.SEARCH', 'idx', 'sibling', 'NOCONTENT').equal([0])
    env.stop()


@skip(cluster=True, redis_less_than='7.9.227')
def test_well_formed_multibyte_kept():
    env = _startEnv('strict_utf8_valid', strict=True)
    _createAndAdd(env, 'café 日本語 😀'.encode())

    assertInfoField(env, 'idx', 'num_terms', 3)
    assertInfoField(env, 'idx', 'num_docs', 1)
    env.assertEqual(int(index_errors(env)['indexing failures']), 0)
    env.expect('FT.SEARCH', 'idx', 'café', 'NOCONTENT').equal([1, 'doc1'])
    env.stop()


@skip(cluster=True, redis_less_than='7.9.227', no_json=True)
def test_multi_value_of_well_formed_values():
    # Multi-value TEXT comes from JSON only, and JSON.SET refuses ill-formed bytes outright
    # ("Couldn't parse as UTF-8 string"), so this path can only ever carry well-formed values.
    # What is worth pinning is that the check, which runs once per value, does not disturb it.
    env = _startEnv('strict_utf8_multi', strict=True)
    env.expect('FT.CREATE', 'idx', 'ON', 'JSON', 'SCHEMA', '$.t[*]', 'AS', 't', 'TEXT').ok()
    env.cmd('JSON.SET', 'doc1', '$', '{"t":["alpha","café"]}')

    assertInfoField(env, 'idx', 'num_terms', 2)
    assertInfoField(env, 'idx', 'num_docs', 1)
    env.assertEqual(int(index_errors(env)['indexing failures']), 0)
    env.stop()
