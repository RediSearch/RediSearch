from includes import *
from common import *

def test_and_or(env):
    conn = getConnectionByEnv(env)
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 't', 'TEXT', 'SORTABLE').ok()

    res = env.execute_command('FT.EXPLAIN', 'idx', 'hello world | goodbye moon')
    exp_res = r'''
UNION {
  INTERSECT {
    UNION {
      hello
      +hello(expanded)
    }
    UNION {
      world
      +world(expanded)
    }
  }
  INTERSECT {
    UNION {
      goodbye
      +goodby(expanded)
      goodby(expanded)
    }
    UNION {
      moon
      +moon(expanded)
    }
  }
}
'''[1:]
    env.assertEqual(exp_res, res)

    res = env.execute_command('FT.EXPLAIN', 'idx', 'hello world | "goodbye" moon')
    exp_res = r'''
UNION {
  INTERSECT {
    UNION {
      hello
      +hello(expanded)
    }
    UNION {
      world
      +world(expanded)
    }
  }
  INTERSECT {
    goodbye
    UNION {
      moon
      +moon(expanded)
    }
  }
}
'''[1:]
    env.assertEqual(exp_res, res)

    res = env.execute_command('FT.EXPLAIN', 'idx', 'hello world | goodbye "moon"')
    exp_res = r'''
UNION {
  INTERSECT {
    UNION {
      hello
      +hello(expanded)
    }
    UNION {
      world
      +world(expanded)
    }
  }
  INTERSECT {
    UNION {
      goodbye
      +goodby(expanded)
      goodby(expanded)
    }
    moon
  }
}
'''[1:]
    env.assertEqual(exp_res, res)

    res = env.execute_command('FT.EXPLAIN', 'idx', '"hello" "world" | "goodbye" "moon"')
    exp_res = r'''
UNION {
  INTERSECT {
    hello
    world
  }
  INTERSECT {
    goodbye
    moon
  }
}
'''[1:]
    env.assertEqual(exp_res, res)

    res = env.execute_command('FT.EXPLAIN', 'idx', '("hello" "world")|(("hello" "world")|("hallo" "world"|"werld") | "hello" "world" "werld")')
    exp_res = r'''
UNION {
  INTERSECT {
    hello
    world
  }
  UNION {
    INTERSECT {
      hello
      world
    }
    UNION {
      INTERSECT {
        hallo
        world
      }
      werld
    }
    INTERSECT {
      hello
      world
      werld
    }
  }
}
'''[1:]
    env.assertEqual(exp_res, res)

def test_modifier(env):
    conn = getConnectionByEnv(env)
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 't1', 'TEXT', 'NOSTEM', 't2', 'TEXT', 'SORTABLE', 'v', 'VECTOR', 'FLAT', '6', 'TYPE', 'FLOAT32', 'DIM', '2','DISTANCE_METRIC', 'L2').ok()

    res = env.execute_command('FT.EXPLAIN', 'idx', '@t1:hello world @t2:howdy')
    exp_res = r'''
INTERSECT {
  @t1:UNION {
    @t1:hello
    @t1:+hello(expanded)
  }
  UNION {
    world
    +world(expanded)
  }
  @t2:UNION {
    @t2:howdy
    @t2:+howdi(expanded)
    @t2:howdi(expanded)
  }
}
'''[1:]
    env.assertEqual(exp_res, res)

    res = env.execute_command('FT.EXPLAIN', 'idx', '@t1:(hello|world|mars)')
    exp_res = r'''
@t1:UNION {
  @t1:UNION {
    @t1:hello
    @t1:+hello(expanded)
  }
  @t1:UNION {
    @t1:world
    @t1:+world(expanded)
  }
  @t1:UNION {
    @t1:mars
    @t1:+mar(expanded)
    @t1:mar(expanded)
  }
}
'''[1:]
    env.assertEqual(exp_res, res)

    res1 = env.execute_command('FT.EXPLAIN', 'idx', '@t1:hello world')
    res2 = env.execute_command('FT.EXPLAIN', 'idx', '@t1:(hello) world')
    env.assertEqual(res1, res2)

    res1 = env.execute_command('FT.EXPLAIN', 'idx', '@t1:hello=>{$weight:5} world')
    res2 = env.execute_command('FT.EXPLAIN', 'idx', '@t1:(hello=>{$weight:5}) world')
    env.assertEqual(res1, res2)

    env.expect('FT.EXPLAIN', 'idx', '@t1:hello world=>[KNN 10 @v $B]', 'PARAMS', 2, 'B', '#blob#').error().contains('Syntax error')

    res = env.execute_command('FT.EXPLAIN', 'idx', '@t1:(hello world)=>[KNN 10 @v $B]', 'PARAMS', 2, 'B', '#blob#')
    exp_res = r'''
VECTOR {
  @t1:INTERSECT {
    @t1:hello
    @t1:world
  }
} => {K=10 nearest vectors to `$B` in @v, AS `__v_score`}
'''[1:]
    env.assertEqual(exp_res, res)

def test_filters(env):
    conn = getConnectionByEnv(env)
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 't', 'TEXT', 't2', 'TAG', 'n', 'NUMERIC', 'g', 'GEO', 'v', 'VECTOR', 'FLAT', '6', 'TYPE', 'FLOAT32', 'DIM', '2','DISTANCE_METRIC', 'L2').ok()

    res = env.execute_command('FT.EXPLAIN', 'idx', 'very simple | @t:hello @t2:{ free\ world } (@n:[1 2]|@n:[3 4]) (@g:[1.5 0.5 0.5 km] -@g:[2.5 1.5 0.5 km])')
    exp_res = r'''
UNION {
  INTERSECT {
    UNION {
      very
      +veri(expanded)
      veri(expanded)
    }
    UNION {
      simple
      +simpl(expanded)
      simpl(expanded)
    }
  }
  INTERSECT {
    @t:UNION {
      @t:hello
      @t:+hello(expanded)
    }
    TAG:@t2 {
      free\ world
    }
    UNION {
      NUMERIC {1.000000 <= @n <= 2.000000}
      NUMERIC {3.000000 <= @n <= 4.000000}
    }
    INTERSECT {
      GEO g:{1.500000,0.500000 --> 0.500000 km}
      NOT{
        GEO g:{2.500000,1.500000 --> 0.500000 km}
      }
    }
  }
}
'''[1:]
    env.assertEqual(exp_res, res)

def test_combinations(env):
    conn = getConnectionByEnv(env)
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 't', 'TEXT', 't2', 'TAG', 'n', 'NUMERIC', 'g', 'GEO', 'v', 'VECTOR', 'FLAT', '6', 'TYPE', 'FLOAT32', 'DIM', '2','DISTANCE_METRIC', 'L2').ok()

    res = env.execute_command('FT.EXPLAIN', 'idx', 'hello | "world" again', 'PARAMS', 2, 'B', '#blob#')
    exp_res = r'''
UNION {
  UNION {
    hello
    +hello(expanded)
  }
  INTERSECT {
    world
    UNION {
      again
      +again(expanded)
    }
  }
}
'''[1:]
    env.assertEqual(exp_res, res)

    res = env.execute_command('FT.EXPLAIN', 'idx', 'hello | -"world" again', 'PARAMS', 2, 'B', '#blob#')
    exp_res = r'''
UNION {
  UNION {
    hello
    +hello(expanded)
  }
  INTERSECT {
    NOT{
      world
    }
    UNION {
      again
      +again(expanded)
    }
  }
}
'''[1:]
    env.assertEqual(exp_res, res)

    res = env.execute_command('FT.EXPLAIN', 'idx', 'hello ~-"world" ~again', 'PARAMS', 2, 'B', '#blob#')
    exp_res = r'''
INTERSECT {
  UNION {
    hello
    +hello(expanded)
  }
  OPTIONAL{
    NOT{
      world
    }
  }
  OPTIONAL{
    again
  }
}
'''[1:]
    env.assertEqual(exp_res, res)

def nest_exp(modifier, term, is_and, i):
    if i == 1:
        return '(@' + modifier + ':' + term + str(i) + ')'
    return '(' + term + str(i) + (' ' if is_and else '|') + nest_exp(modifier, term, is_and, i - 1) + ')'

def testUnsupportedNesting(env):
    nest_level = 200
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'mod', 'TEXT').ok()

    and_exp = nest_exp('mod', 'a', True, nest_level)
    or_exp = nest_exp('mod', 'a', False, nest_level)
    # env.debugPrint(and_exp, force=True)
    # env.debugPrint(or_exp, force=True)
    env.expect('ft.search', 'idx', and_exp).error().contains('Parser stack overflow.')
    env.expect('ft.search', 'idx', or_exp).error().contains('Parser stack overflow.')

def testSupportedNesting(env):
    nest_level = 84
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'mod', 'TEXT').ok()

    and_exp = nest_exp('mod', 'a', True, nest_level)
    or_exp = nest_exp('mod', 'a', False, nest_level)
    # env.debugPrint(and_exp, force=True)
    # env.debugPrint(or_exp, force=True)
    env.expect('ft.search', 'idx', and_exp).equal([0])
    env.expect('ft.search', 'idx', or_exp).equal([0])
