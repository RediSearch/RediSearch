from RLTest import Env

from common import *
from includes import *
import numpy as np

MAX_DIALECT = -1

def test_dialect_config_get_set_from_default(env):
    env.skipOnCluster()
    # skip when default MODARGS for pytest is DEFAULT_DIALECT 2. RediSearch>=2.4 is loading with dialect v1 as default.
    skipOnDialect(env, 2)
    global MAX_DIALECT
    if MAX_DIALECT == -1:
        info = env.cmd('INFO', 'MODULES')
        MAX_DIALECT = int(info['search_max_dialect_version'])
    env.expect("FT.CONFIG GET DEFAULT_DIALECT").equal([['DEFAULT_DIALECT', '1']] )
    env.expect("FT.CONFIG SET DEFAULT_DIALECT 2").ok()
    env.expect("FT.CONFIG GET DEFAULT_DIALECT").equal([['DEFAULT_DIALECT', '2']] )
    env.expect("FT.CONFIG SET DEFAULT_DIALECT 0").error()
    env.expect("FT.CONFIG SET DEFAULT_DIALECT -1").error()
    env.expect("FT.CONFIG SET DEFAULT_DIALECT {}".format(MAX_DIALECT + 1)).error()

def test_dialect_config_get_set_from_config(env):
    env.skipOnCluster()
    env = Env(moduleArgs = 'DEFAULT_DIALECT 2')
    global MAX_DIALECT
    if MAX_DIALECT == -1:
        info = env.cmd('INFO', 'MODULES')
        MAX_DIALECT = int(info['search_max_dialect_version'])
    env.expect("FT.CONFIG GET DEFAULT_DIALECT").equal([['DEFAULT_DIALECT', '2']] )
    env.expect("FT.CONFIG SET DEFAULT_DIALECT 1").ok()
    env.expect("FT.CONFIG GET DEFAULT_DIALECT").equal([['DEFAULT_DIALECT', '1']] )
    env.expect("FT.CONFIG SET DEFAULT_DIALECT 0").error()
    env.expect("FT.CONFIG SET DEFAULT_DIALECT -1").error()
    env.expect("FT.CONFIG SET DEFAULT_DIALECT {}".format(MAX_DIALECT + 1)).error()

def test_dialect_query_errors(env):
    conn = getConnectionByEnv(env)
    global MAX_DIALECT
    if MAX_DIALECT == -1:
        info = env.cmd('INFO', 'MODULES')
        MAX_DIALECT = int(info['search_max_dialect_version'])
    env.expect("FT.CREATE idx SCHEMA t TEXT").ok()
    conn.execute_command("HSET", "h", "t", "hello")
    env.expect("FT.SEARCH idx 'hello' DIALECT").error().contains("Need an argument for DIALECT")
    env.expect("FT.SEARCH idx 'hello' DIALECT 0").error().contains("DIALECT requires a non negative integer >=1 and <= {}".format(MAX_DIALECT))
    env.expect("FT.SEARCH idx 'hello' DIALECT 4").error().contains("DIALECT requires a non negative integer >=1 and <= {}".format(MAX_DIALECT))

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
    global MAX_DIALECT
    if MAX_DIALECT == -1:
        info = env.cmd('INFO', 'MODULES')
        MAX_DIALECT = int(info['search_max_dialect_version'])
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
    
def test_dialect_info(env):
  conn = getConnectionByEnv(env)

  env.expect('FT.CONFIG', 'SET', 'DEFAULT_DIALECT', 1).ok()
  info = env.cmd('INFO', 'MODULES')
  env.assertEqual(int(info['search_min_dialect_version']), 1)
  env.assertEqual(int(info['search_max_dialect_version']), 3)
  env.cmd('FT.CREATE', 'idx1', 'SCHEMA', 'business', 'TEXT')
  env.cmd('FT.CREATE', 'idx2', 'SCHEMA', 'country', 'TEXT')
  conn.execute_command('HSET', 'addr:1', 'business', 'foo', 'country', 'USA')

  env.cmd('FT.SEARCH', 'idx1', '*', 'NOCONTENT', 'DIALECT', 3)
  info = index_info(env, 'idx1')
  env.assertEqual(info['dialect_stats'], ['dialect_1', 0, 'dialect_2', 0, 'dialect_3', 1])
  info = index_info(env, 'idx2')
  env.assertEqual(info['dialect_stats'], ['dialect_1', 0, 'dialect_2', 0, 'dialect_3', 0])
  info = env.cmd('INFO', 'MODULES')
  env.assertEqual(int(info['search_dialect_1']), 0)
  env.assertEqual(int(info['search_dialect_2']), 0)
  env.assertEqual(int(info['search_dialect_3']), 1)

  env.cmd('FT.SEARCH', 'idx2', '*', 'NOCONTENT')
  info = index_info(env, 'idx1')
  env.assertEqual(info['dialect_stats'], ['dialect_1', 0, 'dialect_2', 0, 'dialect_3', 1])
  info = index_info(env, 'idx2')
  env.assertEqual(info['dialect_stats'], ['dialect_1', 1, 'dialect_2', 0, 'dialect_3', 0])
  info = env.cmd('INFO', 'MODULES')
  env.assertEqual(int(info['search_dialect_1']), 1)
  env.assertEqual(int(info['search_dialect_2']), 0)
  env.assertEqual(int(info['search_dialect_3']), 1)

  env.flush()
  info = env.cmd('INFO', 'MODULES')
  env.assertEqual(int(info['search_dialect_1']), 0)
  env.assertEqual(int(info['search_dialect_2']), 0)
  env.assertEqual(int(info['search_dialect_3']), 0)
