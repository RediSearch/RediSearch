from RLTest import Env

from common import *
from includes import *
import numpy as np

MAX_DIALECT = 0

def set_max_dialect(env):
  global MAX_DIALECT
  if MAX_DIALECT == 0:
    info = env.cmd('INFO', 'MODULES')
    prefix = 'search_dialect_'
    MAX_DIALECT = max([int(key.replace(prefix, '')) for key in info.keys() if prefix in key])

@skip(cluster=True)
def test_dialect_config_get_set_from_default(env):
    # skip when default MODARGS for pytest is DEFAULT_DIALECT 2. RediSearch>=2.4 is loading with dialect v1 as default.
    skipOnDialect(env, 2)
    set_max_dialect(env)
    env.expect("FT.CONFIG GET DEFAULT_DIALECT").equal([['DEFAULT_DIALECT', '1']] )
    env.expect("FT.CONFIG SET DEFAULT_DIALECT 2").ok()
    env.expect("FT.CONFIG GET DEFAULT_DIALECT").equal([['DEFAULT_DIALECT', '2']] )
    env.expect("FT.CONFIG SET DEFAULT_DIALECT 0").error()
    env.expect("FT.CONFIG SET DEFAULT_DIALECT -1").error()
    env.expect("FT.CONFIG SET DEFAULT_DIALECT {}".format(MAX_DIALECT + 1)).error()

@skip(cluster=True)
def test_dialect_config_get_set_from_config(env):
    env = Env(moduleArgs = 'DEFAULT_DIALECT 2')
    set_max_dialect(env)
    env.expect("FT.CONFIG GET DEFAULT_DIALECT").equal([['DEFAULT_DIALECT', '2']] )
    env.expect("FT.CONFIG SET DEFAULT_DIALECT 1").ok()
    env.expect("FT.CONFIG GET DEFAULT_DIALECT").equal([['DEFAULT_DIALECT', '1']] )
    env.expect("FT.CONFIG SET DEFAULT_DIALECT 0").error()
    env.expect("FT.CONFIG SET DEFAULT_DIALECT -1").error()
    env.expect("FT.CONFIG SET DEFAULT_DIALECT {}".format(MAX_DIALECT + 1)).error()

def test_dialect_query_errors(env):
    conn = getConnectionByEnv(env)
    set_max_dialect(env)
    env.expect("FT.CREATE idx SCHEMA t TEXT").ok()
    conn.execute_command("HSET", "h", "t", "hello")
    env.expect("FT.SEARCH idx 'hello' DIALECT").error().contains("Need an argument for DIALECT")
    env.expect("FT.SEARCH idx 'hello' DIALECT 0").error().contains("DIALECT requires a non negative integer >=1 and <= {}".format(MAX_DIALECT))
    env.expect("FT.SEARCH idx 'hello' DIALECT 5").error().contains("DIALECT requires a non negative integer >=1 and <= {}".format(MAX_DIALECT))

def test_v1_vs_v2(env):
    env.expect("FT.CREATE idx SCHEMA title TAG t1 TEXT t2 TEXT t3 TEXT num NUMERIC v VECTOR HNSW 6 TYPE FLOAT32 DIM 1 DISTANCE_METRIC COSINE").ok()
    env.expect('FT.EXPLAIN', 'idx', '(*)', 'DIALECT', 1).error().contains('Syntax error')
    env.expect('FT.EXPLAIN', 'idx', '(*)', 'DIALECT', 2).contains('WILDCARD')


    env.expect('FT.EXPLAIN', 'idx', '$hello', 'DIALECT', 1).error().contains('Syntax error')
    env.expect('FT.EXPLAIN', 'idx', '$hello', 'DIALECT', 2, 'PARAMS', 2, 'hello', 'hello').contains('UNION {\n  hello\n  +hello(expanded)\n}\n')

    env.expect('FT.EXPLAIN', 'idx', '"$hello"', 'DIALECT', 1).error().contains('Syntax error')
    env.expect('FT.EXPLAIN', 'idx', '"$hello"', 'DIALECT', 2).contains('$hello')

    env.expect('FT.EXPLAIN', 'idx', '@title:@num:[0 10]', 'DIALECT', 1).contains("NUMERIC {0.000000 <= @num <= 10.000000}")
    env.expect('FT.EXPLAIN', 'idx', '@title:@num:[0 10]', 'DIALECT', 2).error().contains('Syntax error')

    env.expect('FT.EXPLAIN', 'idx', '@title:(@num:[0 10])', 'DIALECT', 1).contains('NUMERIC {0.000000 <= @num <= 10.000000}\n')
    env.expect('FT.EXPLAIN', 'idx', '@title:(@num:[0 10])', 'DIALECT', 2).error().contains('Syntax error')

    env.expect('FT.EXPLAIN', 'idx', '@t1:@t2:@t3:hello', 'DIALECT', 1).contains('@NULL:UNION {\n  @NULL:hello\n  @NULL:+hello(expanded)\n}\n')
    env.expect('FT.EXPLAIN', 'idx', '@t1:@t2:@t3:hello', 'DIALECT', 2).error().contains('Syntax error')

    env.expect('FT.EXPLAIN', 'idx', '@title:{foo}}}}}', 'DIALECT', 1).contains('TAG:@title {\n  foo\n}\n')
    env.expect('FT.EXPLAIN', 'idx', '@title:{foo}}}}}', 'DIALECT', 2).error().contains('Syntax error')

    env.expect('FT.EXPLAIN', 'idx', '*=>[KNN 10 @v $BLOB]', 'PARAMS', 2, 'BLOB', np.full((1), 1, dtype = np.float32).tobytes(), 'DIALECT', 1).contains("Syntax error")
    env.expect('FT.EXPLAIN', 'idx', '*=>[KNN 10 @v $BLOB]', 'PARAMS', 2, 'BLOB', np.full((1), 1, dtype = np.float32).tobytes(), 'DIALECT', 2).contains("{K=10 nearest vector")
    env.expect('FT.EXPLAIN', 'idx', '*=>[knn $K @vec_field $BLOB as score]', 'PARAMS', 2, 'BLOB', np.full((1), 1, dtype = np.float32).tobytes(), 'DIALECT', 1).contains("Syntax error")
    env.expect('FT.EXPLAIN', 'idx', '*=>[knn $K @vec_field $BLOB as score]', 'PARAMS', 4, 'K', 10, 'BLOB', np.full((1), 1, dtype = np.float32).tobytes(), 'DIALECT', 2).contains("{K=10 nearest vector")

def test_spell_check_dialect_errors(env):
    env.cmd('ft.create', 'idx', 'SCHEMA', 't', 'text')
    set_max_dialect(env)
    env.expect('FT.SPELLCHECK', 'idx', 'Tooni toque kerfuffle', 'DIALECT').error().contains("Need an argument for DIALECT")
    env.expect('FT.SPELLCHECK', 'idx', 'Tooni toque kerfuffle', 'DIALECT', 0).error().contains("DIALECT requires a non negative integer >=1 and <= {}".format(MAX_DIALECT))
    env.expect('FT.SPELLCHECK', 'idx', 'Tooni toque kerfuffle', 'DIALECT', "{}".format(MAX_DIALECT + 1)).error().contains("DIALECT requires a non negative integer >=1 and <= {}".format(MAX_DIALECT))

def test_dialect_aggregate(env):
    conn = getConnectionByEnv(env)

    env.expect("FT.CREATE idx SCHEMA t1 TEXT t2 TEXT").ok()
    conn.execute_command("HSET", "h1", "t1", "James Brown", "t2", "Jimi Hendrix")
    conn.execute_command("HSET", "h2", "t1", "James", "t2", "Brown")
    
    # In dialect 2, both documents are returned ("James" in t1 and "Brown" in any field)
    res = conn.execute_command('FT.AGGREGATE', 'idx', '@t1:James Brown', 'GROUPBY', '2', '@t1', '@t2', 'DIALECT', 1)
    env.assertEqual(res[0], 1)
    res = conn.execute_command('FT.AGGREGATE', 'idx', '@t1:James Brown', 'GROUPBY', '2', '@t1', '@t2', 'DIALECT', 2)
    env.assertEqual(res[0], 2)

def check_info_module_results(env, module_expect):
  info = env.cmd('INFO', 'MODULES')
  env.assertEqual(int(info['search_dialect_1']), module_expect[0])
  env.assertEqual(int(info['search_dialect_2']), module_expect[1])
  env.assertEqual(int(info['search_dialect_3']), module_expect[2])

def check_info_results(env, command, idx1_expect, idx2_expect, should_succeed):
  env.cmd(command) if should_succeed else env.expect(command).error()
  info = index_info(env, 'idx1')
  env.assertEqual(info['dialect_stats'], ['dialect_1', idx1_expect[0], 'dialect_2', idx1_expect[1], 'dialect_3', idx1_expect[2], 'dialect_4', idx1_expect[3]])
  info = index_info(env, 'idx2')
  env.assertEqual(info['dialect_stats'], ['dialect_1', idx2_expect[0], 'dialect_2', idx2_expect[1], 'dialect_3', idx2_expect[2], 'dialect_4', idx2_expect[3]])
  check_info_module_results(env, [x or y for x, y in zip(idx1_expect, idx2_expect)])

def test_dialect_info(env):
  conn = getConnectionByEnv(env)
  config = ("_" if env.isCluster() else "") + "FT.CONFIG SET DEFAULT_DIALECT 1"
  env.expect(config).ok()
  info = env.cmd('INFO', 'MODULES')

  env.cmd('FT.CREATE', 'idx1', 'SCHEMA', 'business', 'TEXT')
  env.cmd('FT.CREATE', 'idx2', 'SCHEMA', 'country', 'TEXT')
  conn.execute_command('HSET', 'addr:1', 'business', 'foo', 'country', 'USA')

  check_info_results(env, "FT.SEARCH idx1 * DIALECT 3", [0,0,1,0], [0,0,0,0], True)        # add dialect 3 to idx 1
  check_info_results(env, "FT.SEARCH idx1 * SLOP DIALECT 1", [0,0,1,0], [0,0,0,0], False)  # should fail. don't update dialects.
  check_info_results(env, "FT.AGGREGATE idx2 *", [0,0,1,0], [1,0,0,0], True)               # add default dialect to idx2
  check_info_results(env, "FT.AGGREGATE idx1 * FILTER", [0,0,1,0], [1,0,0,0], False)       # should fail. don't update dialects.
  check_info_results(env, "FT.SPELLCHECK idx1 adr", [1,0,1,0], [1,0,0,0], True)            # add default dialect to idx1
  check_info_results(env, "FT.SPELLCHECK idx2 * DISTANCE", [1,0,1,0], [1,0,0,0], False)    # should fail. don't update dialects.
  check_info_results(env, "FT.EXPLAIN idx2 * DIALECT 2", [1,0,1,0], [1,0,0,0], True)       # not a real query, does not add
  check_info_results(env, "FT.SEARCH idx2 * DIALECT 4", [1,0,1,0], [1,0,0,1], True)        # add dialect 4 to idx 2
  if not env.isCluster():                                                                       # FT.EXPLAINCLI is not supported on cluster
    check_info_results(env, "FT.EXPLAINCLI idx1 * DIALECT 2", [1,0,1,0], [1,0,0,1], True)  # not a real query, does not add

  env.flush()
  check_info_module_results(env, [0,0,0])
