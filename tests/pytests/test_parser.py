from includes import *
from common import *

def test_and_or(env):
    conn = getConnectionByEnv(env)
    env.assertOk(conn.execute_command('FT.CREATE', 'idx', 'SCHEMA', 't', 'TEXT', 'SORTABLE'))

    res = conn.execute_command('FT.EXPLAIN', 'idx', 'hello world | goodbye moon')
    exp_res = '\
UNION {\n\
  INTERSECT {\n\
    UNION {\n\
      hello\n\
      +hello(expanded)\n\
    }\n\
    UNION {\n\
      world\n\
      +world(expanded)\n\
    }\n\
  }\n\
  INTERSECT {\n\
    UNION {\n\
      goodbye\n\
      +goodby(expanded)\n\
      goodby(expanded)\n\
    }\n\
    UNION {\n\
      moon\n\
      +moon(expanded)\n\
    }\n\
  }\n\
}\n'
    env.assertEqual(exp_res, res)

    res = conn.execute_command('FT.EXPLAIN', 'idx', 'hello world | "goodbye" moon')
    exp_res = '\
UNION {\n\
  INTERSECT {\n\
    UNION {\n\
      hello\n\
      +hello(expanded)\n\
    }\n\
    UNION {\n\
      world\n\
      +world(expanded)\n\
    }\n\
  }\n\
  INTERSECT {\n\
    goodbye\n\
    UNION {\n\
      moon\n\
      +moon(expanded)\n\
    }\n\
  }\n\
}\n'
    env.assertEqual(exp_res, res)

    res = conn.execute_command('FT.EXPLAIN', 'idx', 'hello world | goodbye "moon"')
    exp_res = '\
UNION {\n\
  INTERSECT {\n\
    UNION {\n\
      hello\n\
      +hello(expanded)\n\
    }\n\
    UNION {\n\
      world\n\
      +world(expanded)\n\
    }\n\
  }\n\
  INTERSECT {\n\
    UNION {\n\
      goodbye\n\
      +goodby(expanded)\n\
      goodby(expanded)\n\
    }\n\
    moon\n\
  }\n\
}\n'
    env.assertEqual(exp_res, res)

    res = conn.execute_command('FT.EXPLAIN', 'idx', '"hello" "world" | "goodbye" "moon"')
    exp_res = '\
UNION {\n\
  INTERSECT {\n\
    hello\n\
    world\n\
  }\n\
  INTERSECT {\n\
    goodbye\n\
    moon\n\
  }\n\
}\n'
    env.assertEqual(exp_res, res)

    res = conn.execute_command('FT.EXPLAIN', 'idx', '("hello" "world")|(("hello" "world")|("hallo" "world"|"werld") | "hello" "world" "werld")')
    exp_res = '\
UNION {\n\
  INTERSECT {\n\
    hello\n\
    world\n\
  }\n\
  UNION {\n\
    INTERSECT {\n\
      hello\n\
      world\n\
    }\n\
    UNION {\n\
      INTERSECT {\n\
        hallo\n\
        world\n\
      }\n\
      werld\n\
    }\n\
    INTERSECT {\n\
      hello\n\
      world\n\
      werld\n\
    }\n\
  }\n\
}\n'
    env.assertEqual(exp_res, res)

def test_modifier(env):
    conn = getConnectionByEnv(env)
    env.assertOk(conn.execute_command('FT.CREATE', 'idx', 'SCHEMA', 't1', 'TEXT', 'NOSTEM', 't2', 'TEXT', 'SORTABLE', 'v', 'VECTOR', 'FLAT', '6', 'TYPE', 'FLOAT32', 'DIM', '2','DISTANCE_METRIC', 'L2'))

    res = conn.execute_command('FT.EXPLAIN', 'idx', '@t1:hello world @t2:howdy')
    exp_res = '\
INTERSECT {\n\
  @t1:UNION {\n\
    @t1:hello\n\
    @t1:+hello(expanded)\n\
  }\n\
  UNION {\n\
    world\n\
    +world(expanded)\n\
  }\n\
  @t2:UNION {\n\
    @t2:howdy\n\
    @t2:+howdi(expanded)\n\
    @t2:howdi(expanded)\n\
  }\n\
}\n'
    env.assertEqual(exp_res, res)

    res = conn.execute_command('FT.EXPLAIN', 'idx', '@t1:(hello|world|mars)')
    exp_res = '\
@t1:UNION {\n\
  @t1:UNION {\n\
    @t1:hello\n\
    @t1:+hello(expanded)\n\
  }\n\
  @t1:UNION {\n\
    @t1:world\n\
    @t1:+world(expanded)\n\
  }\n\
  @t1:UNION {\n\
    @t1:mars\n\
    @t1:+mar(expanded)\n\
    @t1:mar(expanded)\n\
  }\n\
}\n'
    env.assertEqual(exp_res, res)

    res1 = conn.execute_command('FT.EXPLAIN', 'idx', '@t1:hello world')
    res2 = conn.execute_command('FT.EXPLAIN', 'idx', '@t1:(hello) world')
    env.assertEqual(res1, res2)

    res1 = conn.execute_command('FT.EXPLAIN', 'idx', '@t1:hello=>{$weight:5} world')
    res2 = conn.execute_command('FT.EXPLAIN', 'idx', '@t1:(hello=>{$weight:5}) world')
    env.assertEqual(res1, res2)

    env.expect('FT.EXPLAIN', 'idx', '@t1:hello world=>[KNN 10 @v $B]', 'PARAMS', 2, 'B', '#blob#').error().contains('Syntax error')

    res = conn.execute_command('FT.EXPLAIN', 'idx', '@t1:(hello world)=>[KNN 10 @v $B]', 'PARAMS', 2, 'B', '#blob#')
    exp_res = '\
VECTOR {\n\
  @t1:INTERSECT {\n\
    @t1:hello\n\
    @t1:world\n\
  }\n\
} => {K=10 nearest vectors to `$B` in @v, AS `__v_score`}\n'
    env.assertEqual(exp_res, res)

def test_filters(env):
    conn = getConnectionByEnv(env)
    env.assertOk(conn.execute_command('FT.CREATE', 'idx', 'SCHEMA', 't', 'TEXT', 't2', 'TAG', 'n', 'NUMERIC', 'g', 'GEO', 'v', 'VECTOR', 'FLAT', '6', 'TYPE', 'FLOAT32', 'DIM', '2','DISTANCE_METRIC', 'L2'))

    res = conn.execute_command('FT.EXPLAIN', 'idx', 'very simple | @t:hello @t2:{ free\ world } (@n:[1 2]|@n:[3 4]) (@g:[1.5 0.5 0.5 km] -@g:[2.5 1.5 0.5 km])')
    exp_res = '\
UNION {\n\
  INTERSECT {\n\
    UNION {\n\
      very\n\
      +veri(expanded)\n\
      veri(expanded)\n\
    }\n\
    UNION {\n\
      simple\n\
      +simpl(expanded)\n\
      simpl(expanded)\n\
    }\n\
  }\n\
  INTERSECT {\n\
    @t:UNION {\n\
      @t:hello\n\
      @t:+hello(expanded)\n\
    }\n\
    TAG:@t2 {\n\
      free\\ world\n\
    }\n\
    UNION {\n\
      NUMERIC {1.000000 <= @n <= 2.000000}\n\
      NUMERIC {3.000000 <= @n <= 4.000000}\n\
    }\n\
    INTERSECT {\n\
      GEO g:{1.500000,0.500000 --> 0.500000 km}\n\
      NOT{\n\
        GEO g:{2.500000,1.500000 --> 0.500000 km}\n\
      }\n\
    }\n\
  }\n\
}\n'
    env.assertEqual(exp_res, res)

def test_combinations(env):
    conn = getConnectionByEnv(env)
    env.assertOk(conn.execute_command('FT.CREATE', 'idx', 'SCHEMA', 't', 'TEXT', 't2', 'TAG', 'n', 'NUMERIC', 'g', 'GEO', 'v', 'VECTOR', 'FLAT', '6', 'TYPE', 'FLOAT32', 'DIM', '2','DISTANCE_METRIC', 'L2'))

    res = conn.execute_command('FT.EXPLAIN', 'idx', 'hello | "world" again', 'PARAMS', 2, 'B', '#blob#')
    exp_res = '\
UNION {\n\
  UNION {\n\
    hello\n\
    +hello(expanded)\n\
  }\n\
  INTERSECT {\n\
    world\n\
    UNION {\n\
      again\n\
      +again(expanded)\n\
    }\n\
  }\n\
}\n'
    env.assertEqual(exp_res, res)

    res = conn.execute_command('FT.EXPLAIN', 'idx', 'hello | -"world" again', 'PARAMS', 2, 'B', '#blob#')
    exp_res = '\
UNION {\n\
  UNION {\n\
    hello\n\
    +hello(expanded)\n\
  }\n\
  INTERSECT {\n\
    NOT{\n\
      world\n\
    }\n\
    UNION {\n\
      again\n\
      +again(expanded)\n\
    }\n\
  }\n\
}\n'
    env.assertEqual(exp_res, res)

    res = conn.execute_command('FT.EXPLAIN', 'idx', 'hello ~-"world" ~again', 'PARAMS', 2, 'B', '#blob#')
    exp_res = '\
INTERSECT {\n\
  UNION {\n\
    hello\n\
    +hello(expanded)\n\
  }\n\
  OPTIONAL{\n\
    NOT{\n\
      world\n\
    }\n\
  }\n\
  OPTIONAL{\n\
    again\n\
  }\n\
}\n'
    env.assertEqual(exp_res, res)
