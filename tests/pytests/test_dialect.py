from RLTest import Env

from common import *
from includes import *
import numpy as np

def test_dialect_config_get_set_from_default(env):
    env.skipOnCluster()
    env.expect("FT.CONFIG GET DEFAULT_DIALECT").equal([['DEFAULT_DIALECT', '1']] )
    env.expect("FT.CONFIG SET DEFAULT_DIALECT 2").ok()
    env.expect("FT.CONFIG GET DEFAULT_DIALECT").equal([['DEFAULT_DIALECT', '2']] )
    env.expect("FT.CONFIG SET DEFAULT_DIALECT 0").error()
    env.expect("FT.CONFIG SET DEFAULT_DIALECT -1").error()
    env.expect("FT.CONFIG SET DEFAULT_DIALECT 3").error()

def test_dialect_config_get_set_from_config(env):
    env.skipOnCluster()
    env = Env(moduleArgs = 'DEFAULT_DIALECT 2')
    env.expect("FT.CONFIG GET DEFAULT_DIALECT").equal([['DEFAULT_DIALECT', '2']] )
    env.expect("FT.CONFIG SET DEFAULT_DIALECT 1").ok()
    env.expect("FT.CONFIG GET DEFAULT_DIALECT").equal([['DEFAULT_DIALECT', '1']] )
    env.expect("FT.CONFIG SET DEFAULT_DIALECT 0").error()
    env.expect("FT.CONFIG SET DEFAULT_DIALECT -1").error()
    env.expect("FT.CONFIG SET DEFAULT_DIALECT 3").error()

def test_dialect_query_errors(env):
    conn = getConnectionByEnv(env)
    env.expect("FT.CREATE idx SCHEMA t TEXT").ok()
    conn.execute_command("HSET", "h", "t", "hello")
    env.expect("FT.SEARCH idx 'hello' DIALECT").error().contains("Need an argument for DIALECT")
    env.expect("FT.SEARCH idx 'hello' DIALECT 0").error().contains("DIALECT requires a non negative integer >=1 and <= 2")
    env.expect("FT.SEARCH idx 'hello' DIALECT 3").error().contains("DIALECT requires a non negative integer >=1 and <= 2")

def test_v1_vs_v2(env):
    env = Env(moduleArgs = 'DEFAULT_DIALECT 1')
    conn = getConnectionByEnv(env)
    env.expect("FT.CREATE idx SCHEMA title TAG t1 TEXT t2 TEXT t3 TEXT num NUMERIC v VECTOR HNSW 6 TYPE FLOAT32 DIM 1 DISTANCE_METRIC COSINE").ok()
    # v1
    env.expect('FT.EXPLAIN', 'idx', '(*)').error().contains('Syntax error')
    env.expect('FT.EXPLAIN', 'idx', '(*)','DIALECT', 2).contains('WILDCARD')


    env.expect('FT.EXPLAIN', 'idx', '$hello').error().contains('Syntax error')
    env.expect('FT.EXPLAIN', 'idx', '$hello', 'DIALECT', 2, 'PARAMS', 2, 'hello', 'hello').contains('UNION {\n  hello\n  +hello(expanded)\n}\n')

    env.expect('FT.EXPLAIN', 'idx', '"$hello"').error().contains('Syntax error')
    env.expect('FT.EXPLAIN', 'idx', '"$hello"', 'DIALECT', 2).contains('$hello')

    env.expect('FT.EXPLAIN', 'idx', '@title:@num:[0 10]').contains("NUMERIC {0.000000 <= @num <= 10.000000}")
    env.expect('FT.EXPLAIN', 'idx', '@title:@num:[0 10]', 'DIALECT', 2).error().contains('Syntax error')

    env.expect('FT.EXPLAIN', 'idx', '@title:(@num:[0 10])').contains('NUMERIC {0.000000 <= @num <= 10.000000}\n')
    env.expect('FT.EXPLAIN', 'idx', '@title:(@num:[0 10])', 'DIALECT', 2).error().contains('Syntax error')

    env.expect('FT.EXPLAIN', 'idx', '@t1:@t2:@t3:hello').contains('@NULL:UNION {\n  @NULL:hello\n  @NULL:+hello(expanded)\n}\n')
    env.expect('FT.EXPLAIN', 'idx', '@t1:@t2:@t3:hello', 'DIALECT', 2).error().contains('Syntax error')

    env.expect('FT.EXPLAIN', 'idx', '@title:{foo}}}}}').contains('TAG:@title {\n  foo\n}\n')
    env.expect('FT.EXPLAIN', 'idx', '@title:{foo}}}}}', 'DIALECT', 2).error().contains('Syntax error')

    env.expect('FT.EXPLAIN', 'idx', '*=>[KNN 10 @v $BLOB]', 'PARAMS', 2, 'BLOB', np.full((1), 1, dtype = np.float32).tobytes()).contains("Syntax error")
    env.expect('FT.EXPLAIN', 'idx', '*=>[KNN 10 @v $BLOB]', 'PARAMS', 2, 'BLOB', np.full((1), 1, dtype = np.float32).tobytes(),'DIALECT', 2).contains("{K=10 nearest vector")
    env.expect('FT.EXPLAIN', 'idx', '*=>[knn $K @vec_field $BLOB as score]', 'PARAMS', 2, 'BLOB', np.full((1), 1, dtype = np.float32).tobytes()).contains("Syntax error")
    env.expect('FT.EXPLAIN', 'idx', '*=>[knn $K @vec_field $BLOB as score]', 'PARAMS', 4, 'K', 10, 'BLOB', np.full((1), 1, dtype = np.float32).tobytes(),'DIALECT', 2).contains("{K=10 nearest vector")
