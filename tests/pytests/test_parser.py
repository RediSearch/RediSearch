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
    EXACT {
      goodbye
    }
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
    EXACT {
      moon
    }
  }
}
'''[1:])

    env.expect('FT.EXPLAIN', 'idx', '"hello" "world" | "goodbye" "moon"').equal(r'''
UNION {
  INTERSECT {
    EXACT {
      hello
    }
    EXACT {
      world
    }
  }
  INTERSECT {
    EXACT {
      goodbye
    }
    EXACT {
      moon
    }
  }
}
'''[1:])

    env.expect('FT.EXPLAIN', 'idx', '("hello" "world")|(("hello" "world")|("hallo" "world"|"werld") | "hello" "world" "werld")').equal(r'''
UNION {
  INTERSECT {
    EXACT {
      hallo
    }
    EXACT {
      world
    }
  }
  EXACT {
    werld
  }
  INTERSECT {
    EXACT {
      hello
    }
    EXACT {
      world
    }
  }
  INTERSECT {
    EXACT {
      hello
    }
    EXACT {
      world
    }
    EXACT {
      werld
    }
  }
  INTERSECT {
    EXACT {
      hello
    }
    EXACT {
      world
    }
  }
}
'''[1:])

    env.expect('FT.EXPLAIN', 'idx', '(\'hello\' \'world\')').equal(r'''
INTERSECT {
  EXACT {
    hello
  }
  EXACT {
    world
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
    @t1:hello
    @t1:world
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
  @t1:hello
  @t1:world
  @t1:mars
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
  @t1:hello
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
  @t1:hello
  @t1:world
  @t1:mars
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
} => {K=10 nearest vectors to `$B` in vector index associated with field @v, yields distance as `__v_score`}
'''[1:])

def test_filters_v1():
    env = Env(moduleArgs = 'DEFAULT_DIALECT 1')
    conn = getConnectionByEnv(env)
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 't', 'TEXT', 't2', 'TAG', 'n', 'NUMERIC', 'g', 'GEO', 'v', 'VECTOR', 'FLAT', '6', 'TYPE', 'FLOAT32', 'DIM', '2','DISTANCE_METRIC', 'L2').ok()

    env.expect('FT.EXPLAIN', 'idx', 'very simple | @t:hello @t2:{ free\\ world } (@n:[1 2]|@n:[3 4]) (@g:[1.5 0.5 0.5 km] -@g:[2.5 1.5 0.5 km])').equal(r'''
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

    env.expect('FT.EXPLAIN', 'idx', 'very simple | @t:hello @t2:{ free\\ world } (@n:[1 2]|@n:[3 4]) (@g:[1.5 0.5 0.5 km] -@g:[2.5 1.5 0.5 km])').equal(r'''
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
      UNION {
        again
        +again(expanded)
      }
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
    UNION {
      again
      +again(expanded)
    }
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
    EXACT {
      world
    }
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
      EXACT {
        world
      }
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
      EXACT {
        world
      }
    }
  }
  OPTIONAL{
    UNION {
      again
      +again(expanded)
    }
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
    # env.debugPrint(and_exp, force=TEST_DEBUG)
    # env.debugPrint(or_exp, force=TEST_DEBUG)
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
    # env.debugPrint(and_exp, force=TEST_DEBUG)
    # env.debugPrint(or_exp, force=TEST_DEBUG)
    env.expect('ft.search', 'idx', and_exp).equal([0])
    env.expect('ft.search', 'idx', or_exp).equal([0])

def testSupportedNesting_v2():
    env = Env(moduleArgs = 'DEFAULT_DIALECT 2')
    nest_level = 84
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'mod', 'TEXT').ok()

    and_exp = nest_exp('mod', 'a', True, nest_level)
    or_exp = nest_exp('mod', 'a', False, nest_level)
    # env.debugPrint(and_exp, force=TEST_DEBUG)
    # env.debugPrint(or_exp, force=TEST_DEBUG)
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
    dialect = env.cmd(config_cmd(), "GET", "DEFAULT_DIALECT")[0][1]
    if (dialect == 1):
      env.expect('FT.EXPLAIN', 'idx1', f'@t:({exact_arg})').equal('@t:UNION {\n' + '\n'.join([f'  @t:t{i}' for i in range(1, num_args+1)]) + '\n}\n')
    elif (dialect == 2):
      env.expect('FT.EXPLAIN', 'idx1', f'@t:({exact_arg})').equal('@t:UNION {\n' + '\n'.join([f'  @t:EXACT {{\n    @t:t{i}\n  }}' for i in range(1, num_args+1)]) + '\n}\n')

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

def testQuotes_v2():
    env = Env(moduleArgs = 'DEFAULT_DIALECT 2')
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 't1', 'TEXT', 't2', 'TAG', 'INDEXEMPTY').ok()
    query_to_explain = {
        '@t1:("hello")':
            'EXACT {\n  t1\n  hello\n}\n',
        '@t1:("hello world")':
            'EXACT {\n  t1\n  hello\n  world\n}\n',
        '@t1:("$param")':
            'EXACT {\n  t1\n  $param\n}\n',
        '@t2:{"hello world"}':
            'EXACT {\n  t2\n  hello\n  world\n}\n',
        '@t2:{"" world}':
            'EXACT {\n  t2\n  world\n}\n',
        r'@t2:{"$param\!"}': # Hits the quote attribute quote parser syntax
            'EXACT {\n  t2\n  $param!\n}\n',
    }
    for query, expected in query_to_explain.items():
        env.expect('FT.EXPLAIN', 'idx', f'\'{query}\'').equal(expected)
        squote_query = query.replace('"', '\'')
        env.expect('FT.EXPLAIN', 'idx', f'"{squote_query}"').equal(expected)

def testTagQueryWithStopwords_V2(env):
    env = Env(moduleArgs = 'DEFAULT_DIALECT 2')
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'tag', 'TAG').ok()
    conn = getConnectionByEnv(env)
    conn.execute_command('HSET', 'doc1', 'tag', 'as is the with by')
    conn.execute_command('HSET', 'doc2', 'tag', 'as,is,the,with,by')
    env.expect('FT.EXPLAIN', 'idx', '@tag:{as is the with by}').equal(r'''
TAG:@tag {
  INTERSECT {
    as
    is
    the
    with
    by
  }
}
'''[1:])
    env.expect('FT.SEARCH', 'idx', '@tag:{as is the with by}', 'NOCONTENT').equal([1, 'doc1'])

    conn.execute_command('HSET', 'doc3', 'tag', 'cat dog')
    conn.execute_command('HSET', 'doc4', 'tag', 'cat with dog')
    env.expect('FT.EXPLAIN', 'idx', '@tag:{cat with dog}').equal(r'''
TAG:@tag {
  INTERSECT {
    cat
    with
    dog
  }
}
'''[1:])
    env.expect('FT.SEARCH', 'idx', '@tag:{cat with dog}', 'NOCONTENT').equal([1, 'doc4'])

    env.expect('FT.CREATE', 'custom_idx', 'STOPWORDS', 2, 'foo', 'bar', 'SCHEMA', 'tag', 'TAG').ok()
    conn.execute_command('HSET', 'doc5', 'tag', 'foo bar')
    conn.execute_command('HSET', 'doc7', 'tag', 'cat foo dog')
    env.expect('FT.EXPLAIN', 'custom_idx', '@tag:{foo bar}').equal(r'''
TAG:@tag {
  INTERSECT {
    foo
    bar
  }
}
'''[1:])
    env.expect('FT.SEARCH', 'custom_idx', '@tag:{foo bar}', 'NOCONTENT').equal([1, 'doc5'])
    env.expect('FT.SEARCH', 'idx', '@tag:{cat foo dog}', 'NOCONTENT').equal([1, 'doc7'])

def testTagQueryWithOR_V2(env):
  env = Env(moduleArgs = 'DEFAULT_DIALECT 2')
  env.expect('FT.CREATE', 'idx', 'SCHEMA', 'tag', 'TAG').ok()
  conn = getConnectionByEnv(env)
  conn.execute_command('HSET', 'doc1', 'tag', 'x y')
  conn.execute_command('HSET', 'doc2', 'tag', 'apple')
  conn.execute_command('HSET', 'doc3', 'tag', 'banana')

 # tag_list ::= taglist OR affix (affix is suffix)
  env.expect('FT.EXPLAIN', 'idx', '@tag:{x y | *ple }').equal(r'''
TAG:@tag {
  INTERSECT {
    x
    y
  }
  SUFFIX{*ple}
}
'''[1:])
  env.expect('FT.SEARCH', 'idx', '@tag:{x y | *ple }').equal([2, 'doc1', ['tag', 'x y'], 'doc2', ['tag', 'apple']])

  # tag_list ::= taglist OR affix (affix is prefix)
  env.expect('FT.EXPLAIN', 'idx', '@tag:{x y | ba* }').equal(r'''
TAG:@tag {
  INTERSECT {
    x
    y
  }
  PREFIX{ba*}
}
'''[1:])
  env.expect('FT.SEARCH', 'idx', '@tag:{x y | ba* }').equal([2, 'doc1', ['tag', 'x y'], 'doc3', ['tag', 'banana']])

 # tag_list ::= taglist OR affix (affix is contains)
  env.expect('FT.EXPLAIN', 'idx', '@tag:{x y | *pl* }').equal(r'''
TAG:@tag {
  INTERSECT {
    x
    y
  }
  INFIX{*pl*}
}
'''[1:])
  env.expect('FT.SEARCH', 'idx', '@tag:{x y | *pl* }').equal([2, 'doc1', ['tag', 'x y'], 'doc2', ['tag', 'apple']])

# taglist OR param_term_case
  env.expect('FT.EXPLAIN', 'idx', '@tag:{x y | banana }').equal(r'''
TAG:@tag {
  INTERSECT {
    x
    y
  }
  banana
}
'''[1:])
  env.expect('FT.SEARCH', 'idx', '@tag:{x y | banana }').equal([2, 'doc1', ['tag', 'x y'], 'doc3', ['tag', 'banana']])

def testTagQueryWithStopwords_V1(env):
    env = Env(moduleArgs = 'DEFAULT_DIALECT 1')
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'tag', 'TAG').ok()
    conn = getConnectionByEnv(env)
    conn.execute_command('HSET', 'doc1', 'tag', 'cat')
    conn.execute_command('HSET', 'doc2', 'tag', 'dog')
    env.expect('FT.EXPLAIN', 'idx', '@tag:{cat dog}').equal(r'''
TAG:@tag {
  INTERSECT {
    cat
    dog
  }
}
'''[1:])
    env.expect('FT.SEARCH', 'idx', '@tag:{cat dog}', 'NOCONTENT').equal([0])
    env.expect('FT.SEARCH', 'idx', '@tag:{cat}', 'NOCONTENT').equal([1, 'doc1'])

    # error when contain stopwords
    env.expect('FT.SEARCH', 'idx', '@tag:{with dog}').error().contains('Syntax error')