from includes import *
from common import *
from RLTest import Env

def test_and_or_v1():
    env = Env(moduleArgs = 'DEFAULT_DIALECT 1')
    conn = getConnectionByEnv(env)
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 't', 'TEXT', 'SORTABLE').ok()

    env.expect('FT.EXPLAIN', 'idx', 'hello world | goodbye moon').equal(r'''
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
'''[1:])

    env.expect('FT.EXPLAIN', 'idx', 'hello world | "goodbye" moon').equal(r'''
INTERSECT {
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
    goodbye
  }
  UNION {
    moon
    +moon(expanded)
  }
}
'''[1:])

    env.expect('FT.EXPLAIN', 'idx', 'hello world | goodbye "moon"').equal(r'''
INTERSECT {
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
    UNION {
      goodbye
      +goodby(expanded)
      goodby(expanded)
    }
  }
  moon
}
'''[1:])

    env.expect('FT.EXPLAIN', 'idx', '"hello" "world" | "goodbye" "moon"').equal(r'''
INTERSECT {
  hello
  UNION {
    world
    goodbye
  }
  moon
}
'''[1:])

    env.expect('FT.EXPLAIN', 'idx', '("hello" "world")|(("hello" "world")|("hallo" "world"|"werld") | "hello" "world" "werld")').equal(r'''
UNION {
  INTERSECT {
    hello
    world
  }
  INTERSECT {
    UNION {
      INTERSECT {
        hello
        world
      }
      INTERSECT {
        hallo
        UNION {
          world
          werld
        }
      }
      hello
    }
    world
    werld
  }
}
'''[1:])

def test_and_or_v2():
    env = Env(moduleArgs = 'DEFAULT_DIALECT 2')
    conn = getConnectionByEnv(env)
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 't', 'TEXT', 'SORTABLE').ok()

    env.expect('FT.EXPLAIN', 'idx', 'hello world | goodbye moon').equal(r'''
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
'''[1:])

    env.expect('FT.EXPLAIN', 'idx', 'hello world | "goodbye" moon').equal(r'''
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
'''[1:])

    env.expect('FT.EXPLAIN', 'idx', 'hello world | goodbye "moon"').equal(r'''
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
'''[1:])

    env.expect('FT.EXPLAIN', 'idx', '"hello" "world" | "goodbye" "moon"').equal(r'''
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
'''[1:])

    env.expect('FT.EXPLAIN', 'idx', '("hello" "world")|(("hello" "world")|("hallo" "world"|"werld") | "hello" "world" "werld")').equal(r'''
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
'''[1:])

def test_modifier_v1():
    env = Env(moduleArgs = 'DEFAULT_DIALECT 1')
    conn = getConnectionByEnv(env)
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 't1', 'TEXT', 'NOSTEM', 't2', 'TEXT', 'SORTABLE', 'v', 'VECTOR', 'FLAT', '6', 'TYPE', 'FLOAT32', 'DIM', '2','DISTANCE_METRIC', 'L2').ok()

    env.expect('FT.EXPLAIN', 'idx', '@t1:hello world @t2:howdy').equal(r'''
INTERSECT {
  @t1:INTERSECT {
    @t1:UNION {
      @t1:hello
      @t1:+hello(expanded)
    }
    @t1:UNION {
      @t1:world
      @t1:+world(expanded)
    }
  }
  @t2:UNION {
    @t2:howdy
    @t2:+howdi(expanded)
    @t2:howdi(expanded)
  }
}
'''[1:])

    env.expect('FT.EXPLAIN', 'idx', '@t1:(hello|world|mars)').equal(r'''
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
'''[1:])

    env.expect('FT.EXPLAIN', 'idx', '@t1:hello world').equal(env.expect('FT.EXPLAIN', 'idx', '@t1:(hello world)').res)

    env.expect('FT.EXPLAIN', 'idx', '@t1:hello=>{$weight:5} world').equal(env.expect('FT.EXPLAIN', 'idx', '@t1:(hello=>{$weight:5}) world').res)

    env.expect('FT.EXPLAIN', 'idx', '@t1:hello world=>[KNN 10 @v $B]', 'PARAMS', 2, 'B', '#blob#').error().contains('Syntax error')
    env.expect('FT.EXPLAIN', 'idx', '@t1:(hello world)=>[KNN 10 @v $B]', 'PARAMS', 2, 'B', '#blob#').error().contains('Syntax error')

def test_modifier_v2(env):
    env = Env(moduleArgs = 'DEFAULT_DIALECT 2')
    conn = getConnectionByEnv(env)
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 't1', 'TEXT', 'NOSTEM', 't2', 'TEXT', 'SORTABLE', 'v', 'VECTOR', 'FLAT', '6', 'TYPE', 'FLOAT32', 'DIM', '2','DISTANCE_METRIC', 'L2').ok()

    env.expect('FT.EXPLAIN', 'idx', '@t1:hello world @t2:howdy').equal(r'''
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
'''[1:])

    env.expect('FT.EXPLAIN', 'idx', '@t1:(hello|world|mars)').equal('''
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
'''[1:])

    env.expect('FT.EXPLAIN', 'idx', '@t1:hello world').equal(env.expect('FT.EXPLAIN', 'idx', '@t1:(hello) world').res)
    env.expect('FT.EXPLAIN', 'idx', '@t1:hello=>{$weight:5} world').equal(env.expect('FT.EXPLAIN', 'idx', '@t1:(hello=>{$weight:5}) world').res)
    env.expect('FT.EXPLAIN', 'idx', '@t1:hello world=>[KNN 10 @v $B]', 'PARAMS', 2, 'B', '#blob#').error().contains('Syntax error')

    env.expect('FT.EXPLAIN', 'idx', '@t1:(hello world)=>[KNN 10 @v $B]', 'PARAMS', 2, 'B', '#blob#').equal(r'''
VECTOR {
  @t1:INTERSECT {
    @t1:hello
    @t1:world
  }
} => {K=10 nearest vectors to `$B` in @v, AS `__v_score`}
'''[1:])

def test_filters_v1():
    env = Env(moduleArgs = 'DEFAULT_DIALECT 1')
    conn = getConnectionByEnv(env)
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 't', 'TEXT', 't2', 'TAG', 'n', 'NUMERIC', 'g', 'GEO', 'v', 'VECTOR', 'FLAT', '6', 'TYPE', 'FLOAT32', 'DIM', '2','DISTANCE_METRIC', 'L2').ok()

    env.expect('FT.EXPLAIN', 'idx', 'very simple | @t:hello @t2:{ free\ world } (@n:[1 2]|@n:[3 4]) (@g:[1.5 0.5 0.5 km] -@g:[2.5 1.5 0.5 km])').equal(r'''
INTERSECT {
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
    @t:UNION {
      @t:hello
      @t:+hello(expanded)
    }
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
'''[1:])

def test_filters_v2():
    env = Env(moduleArgs = 'DEFAULT_DIALECT 2')
    conn = getConnectionByEnv(env)
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 't', 'TEXT', 't2', 'TAG', 'n', 'NUMERIC', 'g', 'GEO', 'v', 'VECTOR', 'FLAT', '6', 'TYPE', 'FLOAT32', 'DIM', '2','DISTANCE_METRIC', 'L2').ok()

    env.expect('FT.EXPLAIN', 'idx', 'very simple | @t:hello @t2:{ free\ world } (@n:[1 2]|@n:[3 4]) (@g:[1.5 0.5 0.5 km] -@g:[2.5 1.5 0.5 km])').equal(r'''
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
'''[1:])

def test_combinations_v1():
    env = Env(moduleArgs = 'DEFAULT_DIALECT 1')
    conn = getConnectionByEnv(env)
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 't', 'TEXT', 't2', 'TAG', 'n', 'NUMERIC', 'g', 'GEO', 'v', 'VECTOR', 'FLAT', '6', 'TYPE', 'FLOAT32', 'DIM', '2','DISTANCE_METRIC', 'L2').ok()

    env.expect('FT.EXPLAIN', 'idx', 'hello | "world" again', 'PARAMS', 2, 'B', '#blob#').equal(r'''
INTERSECT {
  UNION {
    UNION {
      hello
      +hello(expanded)
    }
    world
  }
  UNION {
    again
    +again(expanded)
  }
}
'''[1:])

    env.expect('FT.EXPLAIN', 'idx', 'hello | -"world" again', 'PARAMS', 2, 'B', '#blob#').equal(r'''
UNION {
  UNION {
    hello
    +hello(expanded)
  }
  NOT{
    INTERSECT {
      world
      again
    }
  }
}
'''[1:])

    env.expect('FT.EXPLAIN', 'idx', 'hello ~-"world" ~again', 'PARAMS', 2, 'B', '#blob#').equal(r'''
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
'''[1:])

def test_combinations_v2():
    env = Env(moduleArgs = 'DEFAULT_DIALECT 2')
    conn = getConnectionByEnv(env)
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 't', 'TEXT', 't2', 'TAG', 'n', 'NUMERIC', 'g', 'GEO', 'v', 'VECTOR', 'FLAT', '6', 'TYPE', 'FLOAT32', 'DIM', '2','DISTANCE_METRIC', 'L2').ok()

    env.expect('FT.EXPLAIN', 'idx', 'hello | "world" again', 'PARAMS', 2, 'B', '#blob#').equal(r'''
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
'''[1:])

    env.expect('FT.EXPLAIN', 'idx', 'hello | -"world" again', 'PARAMS', 2, 'B', '#blob#').equal(r'''
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
'''[1:])

    env.expect('FT.EXPLAIN', 'idx', 'hello ~-"world" ~again', 'PARAMS', 2, 'B', '#blob#').equal(r'''
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
'''[1:])

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
    env.expect('ft.search', 'idx', and_exp, 'DIALECT', 1).error().contains('Syntax error at offset')
    env.expect('ft.search', 'idx', and_exp, 'DIALECT', 2).error().contains('Parser stack overflow.')
    env.expect('ft.search', 'idx', or_exp, 'DIALECT', 1).error().contains('Syntax error at offset')
    env.expect('ft.search', 'idx', or_exp, 'DIALECT', 2).error().contains('Parser stack overflow.')

def testSupportedNesting_v1():
    env = Env(moduleArgs = 'DEFAULT_DIALECT 1')
    nest_level = 30
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'mod', 'TEXT').ok()

    and_exp = nest_exp('mod', 'a', True, nest_level)
    or_exp = nest_exp('mod', 'a', False, nest_level)
    # env.debugPrint(and_exp, force=True)
    # env.debugPrint(or_exp, force=True)
    env.expect('ft.search', 'idx', and_exp).equal([0])
    env.expect('ft.search', 'idx', or_exp).equal([0])

def testSupportedNesting_v2():
    env = Env(moduleArgs = 'DEFAULT_DIALECT 2')
    nest_level = 84
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'mod', 'TEXT').ok()

    and_exp = nest_exp('mod', 'a', True, nest_level)
    or_exp = nest_exp('mod', 'a', False, nest_level)
    # env.debugPrint(and_exp, force=True)
    # env.debugPrint(or_exp, force=True)
    env.expect('ft.search', 'idx', and_exp).equal([0])
    env.expect('ft.search', 'idx', or_exp).equal([0])

def testLongUnionList(env):
    env.expect('FT.CREATE', 'idx1', 'SCHEMA', 'n', 'NUMERIC', 't', 'TEXT').ok()
    env.expect('FT.CREATE', 'idx2', 'SCHEMA', 'n', 'NUMERIC', 't', 'TAG').ok()
    conn = getConnectionByEnv(env)
    num_args = 300
    for i in range(1, num_args+1):
        conn.execute_command('HSET', f'doc{i}', 't', f't{i}', 'n', i)
    arg = '|'.join([f't{i}' for i in range(1, num_args+1)])
    env.expect('ft.search', 'idx1', f'@t:({arg})', 'SORTBY', 'n', 'NOCONTENT').equal([num_args, *[f'doc{i}' for i in range(1, 11)]])
    env.expect('ft.search', 'idx2', f'@t:{{{arg}}}', 'SORTBY', 'n', 'NOCONTENT').equal([num_args, *[f'doc{i}' for i in range(1, 11)]])

    # Make sure we get a single union node of all the args, and not a deep tree
    exact_arg = '|'.join([f'"t{i}"' for i in range(1, num_args+1)])
    env.expect('FT.EXPLAIN', 'idx1', f'@t:({exact_arg})').equal('@t:UNION {\n' + '\n'.join([f'  @t:t{i}' for i in range(1, num_args+1)]) + '\n}\n')

def testModifierList(env):
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 't1', 'TEXT', 't2', 'TEXT').ok()
    env.expect('FT.EXPLAIN', 'idx', '@t1|t2:(text value)').equal(r'''
@t1|t2:INTERSECT {
  @t1|t2:UNION {
    @t1|t2:text
    @t1|t2:+text(expanded)
  }
  @t1|t2:UNION {
    @t1|t2:value
    @t1|t2:+valu(expanded)
    @t1|t2:valu(expanded)
  }
}
'''[1:])
