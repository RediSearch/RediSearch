from RLTest import Env

from common import *
from includes import *
import numpy as np

@skip(cluster=True)
def test_dialect_config_get_set_from_default(env):
    # skip when default MODARGS for pytest is DEFAULT_DIALECT 2. RediSearch>=2.4 is loading with dialect v1 as default.
    skipOnDialect(env, 2)
    MAX_DIALECT = set_max_dialect(env)
    env.expect("FT.CONFIG GET DEFAULT_DIALECT").equal([['DEFAULT_DIALECT', '1']] )
    env.expect("FT.CONFIG SET DEFAULT_DIALECT 2").ok()
    env.expect("FT.CONFIG GET DEFAULT_DIALECT").equal([['DEFAULT_DIALECT', '2']] )
    env.expect("FT.CONFIG SET DEFAULT_DIALECT 0").error()
    env.expect("FT.CONFIG SET DEFAULT_DIALECT -1").error()
    env.expect("FT.CONFIG SET DEFAULT_DIALECT {}".format(MAX_DIALECT + 1)).error()

@skip(cluster=True)
def test_dialect_config_get_set_from_config(env):
    env = Env(moduleArgs = 'DEFAULT_DIALECT 2')
    MAX_DIALECT = set_max_dialect(env)
    env.expect("FT.CONFIG GET DEFAULT_DIALECT").equal([['DEFAULT_DIALECT', '2']] )
    env.expect("FT.CONFIG SET DEFAULT_DIALECT 1").ok()
    env.expect("FT.CONFIG GET DEFAULT_DIALECT").equal([['DEFAULT_DIALECT', '1']] )
    env.expect("FT.CONFIG SET DEFAULT_DIALECT 0").error()
    env.expect("FT.CONFIG SET DEFAULT_DIALECT -1").error()
    env.expect("FT.CONFIG SET DEFAULT_DIALECT {}".format(MAX_DIALECT + 1)).error()

def test_dialect_query_errors(env):
    conn = getConnectionByEnv(env)
    MAX_DIALECT = set_max_dialect(env)
    env.expect("FT.CREATE idx SCHEMA t TEXT").ok()
    conn.execute_command("HSET", "h", "t", "hello")
    env.expect("FT.SEARCH idx 'hello' DIALECT").error().contains("Need an argument for DIALECT")
    env.expect("FT.SEARCH idx 'hello' DIALECT 0").error().contains("DIALECT requires a non negative integer >=1 and <= {}".format(MAX_DIALECT))
    env.expect("FT.SEARCH idx 'hello' DIALECT 6").error().contains("DIALECT requires a non negative integer >=1 and <= {}".format(MAX_DIALECT))

def test_v1_vs_v2_vs_v5(env):
    env.expect("FT.CREATE idx SCHEMA title TAG t1 TEXT t2 TEXT t3 TEXT num NUMERIC v VECTOR HNSW 6 TYPE FLOAT32 DIM 1 DISTANCE_METRIC COSINE").ok()
    env.expect('FT.EXPLAIN', 'idx', '(*)', 'DIALECT', 1).error().contains('Syntax error')
    env.expect('FT.EXPLAIN', 'idx', '(*)', 'DIALECT', 2).contains('WILDCARD')
    env.expect('FT.EXPLAIN', 'idx', '(*)', 'DIALECT', 5).contains('WILDCARD')

    env.expect('FT.EXPLAIN', 'idx', '$hello', 'DIALECT', 1).error().contains('Syntax error')
    env.expect('FT.EXPLAIN', 'idx', '$hello', 'DIALECT', 2, 'PARAMS', 2, 'hello', 'hello').contains('UNION {\n  hello\n  +hello(expanded)\n}\n')
    env.expect('FT.EXPLAIN', 'idx', '$hello', 'DIALECT', 5, 'PARAMS', 2, 'hello', 'hello').contains('UNION {\n  hello\n  +hello(expanded)\n}\n')

    env.expect('FT.EXPLAIN', 'idx', '"$hello"', 'DIALECT', 1).error().contains('Syntax error')
    env.expect('FT.EXPLAIN', 'idx', '"$hello"', 'DIALECT', 2).contains('$hello')
    env.expect('FT.EXPLAIN', 'idx', '"$hello"', 'DIALECT', 5).contains('$hello')

    env.expect('FT.EXPLAIN', 'idx', '@title:@num:[0 10]', 'DIALECT', 1).contains("NUMERIC {0.000000 <= @num <= 10.000000}")
    env.expect('FT.EXPLAIN', 'idx', '@title:@num:[0 10]', 'DIALECT', 2).error().contains('Syntax error')
    env.expect('FT.EXPLAIN', 'idx', '@title:@num:[0 10]', 'DIALECT', 5).error().contains('Syntax error')

    env.expect('FT.EXPLAIN', 'idx', '@title:(@num:[0 10])', 'DIALECT', 1).contains('NUMERIC {0.000000 <= @num <= 10.000000}\n')
    env.expect('FT.EXPLAIN', 'idx', '@title:(@num:[0 10])', 'DIALECT', 2).error().contains('Syntax error')
    env.expect('FT.EXPLAIN', 'idx', '@title:(@num:[0 10])', 'DIALECT', 5).error().contains('Syntax error')

    env.expect('FT.EXPLAIN', 'idx', '@num:[0 .1]', 'DIALECT', 1).contains('NUMERIC {0.000000 <= @num <= 1.000000}\n')
    env.expect('FT.EXPLAIN', 'idx', '@num:[0 .1]', 'DIALECT', 2).contains('NUMERIC {0.000000 <= @num <= 1.000000}\n')
    env.expect('FT.EXPLAIN', 'idx', '@num:[0 .1]', 'DIALECT', 5).contains('NUMERIC {0.000000 <= @num <= 0.100000}\n')

    env.expect('FT.EXPLAIN', 'idx', '@t1:@t2:@t3:hello', 'DIALECT', 1).contains('@NULL:UNION {\n  @NULL:hello\n  @NULL:+hello(expanded)\n}\n')
    env.expect('FT.EXPLAIN', 'idx', '@t1:@t2:@t3:hello', 'DIALECT', 2).error().contains('Syntax error')
    env.expect('FT.EXPLAIN', 'idx', '@t1:@t2:@t3:hello', 'DIALECT', 5).error().contains('Syntax error')

    env.expect('FT.EXPLAIN', 'idx', '@title:{foo}}}}}', 'DIALECT', 1).contains('TAG:@title {\n  foo\n}\n')
    env.expect('FT.EXPLAIN', 'idx', '@title:{foo}}}}}', 'DIALECT', 2).error().contains('Syntax error')
    env.expect('FT.EXPLAIN', 'idx', '@title:{foo}}}}}', 'DIALECT', 5).error().contains('Syntax error')

    env.expect('FT.EXPLAIN', 'idx', '*=>[KNN 10 @v $BLOB]', 'PARAMS', 2, 'BLOB', np.full((1), 1, dtype = np.float32).tobytes(), 'DIALECT', 1).contains("Syntax error")
    env.expect('FT.EXPLAIN', 'idx', '*=>[KNN 10 @v $BLOB]', 'PARAMS', 2, 'BLOB', np.full((1), 1, dtype = np.float32).tobytes(), 'DIALECT', 2).contains("{K=10 nearest vector")
    env.expect('FT.EXPLAIN', 'idx', '*=>[KNN 10 @v $BLOB]', 'PARAMS', 2, 'BLOB', np.full((1), 1, dtype = np.float32).tobytes(), 'DIALECT', 5).contains("{K=10 nearest vector")
    env.expect('FT.EXPLAIN', 'idx', '*=>[knn $K @vec_field $BLOB as score]', 'PARAMS', 2, 'BLOB', np.full((1), 1, dtype = np.float32).tobytes(), 'DIALECT', 1).contains("Syntax error")
    env.expect('FT.EXPLAIN', 'idx', '*=>[knn $K @vec_field $BLOB as score]', 'PARAMS', 4, 'K', 10, 'BLOB', np.full((1), 1, dtype = np.float32).tobytes(), 'DIALECT', 2).contains("{K=10 nearest vector")
    env.expect('FT.EXPLAIN', 'idx', '*=>[knn $K @vec_field $BLOB as score]', 'PARAMS', 4, 'K', 10, 'BLOB', np.full((1), 1, dtype = np.float32).tobytes(), 'DIALECT', 5).contains("{K=10 nearest vector")

    env.expect('FT.EXPLAIN', 'idx', '@t1:(a-b-*)', 'DIALECT', 1).error().contains('Syntax error')
    env.expect('FT.EXPLAIN', 'idx', '@t1:(a-b-*)', 'DIALECT', 2).error().contains('Syntax error')
    env.expect('FT.EXPLAIN', 'idx', '@t1:(a-b-*)', 'DIALECT', 5).error().contains('Syntax error')

    env.expect('FT.EXPLAIN', 'idx', '@t1:(*a-b-*)', 'DIALECT', 1).error().contains('Syntax error')
    env.expect('FT.EXPLAIN', 'idx', '@t1:(*a-b-*)', 'DIALECT', 2).error().contains('Syntax error')
    env.expect('FT.EXPLAIN', 'idx', '@t1:(*a-b-*)', 'DIALECT', 5).error().contains('Syntax error')

    env.expect('FT.EXPLAIN', 'idx', '@t1:(*a-b-)', 'DIALECT', 1).error().contains('Syntax error')
    env.expect('FT.EXPLAIN', 'idx', '@t1:(*a-b-)', 'DIALECT', 2).error().contains('Syntax error')
    env.expect('FT.EXPLAIN', 'idx', '@t1:(*a-b-)', 'DIALECT', 5).error().contains('Syntax error')

    env.expect('FT.EXPLAIN', 'idx', '@num:[-inf  inf]', 'DIALECT', 1).contains('NUMERIC {-inf <= @num <= inf}\n')
    env.expect('FT.EXPLAIN', 'idx', '@num:[-inf  inf]', 'DIALECT', 2).contains('NUMERIC {-inf <= @num <= inf}\n')
    env.expect('FT.EXPLAIN', 'idx', '@num:[-inf  inf]', 'DIALECT', 5).contains('NUMERIC {-inf <= @num <= inf}\n')

    env.expect('FT.EXPLAIN', 'idx', "@t1:(w'abc?')", 'DIALECT', 1).contains('INTERSECT')
    env.expect('FT.EXPLAIN', 'idx', "@t1:(w'abc?')", 'DIALECT', 2).contains('@t1:WILDCARD{abc?}')
    env.expect('FT.EXPLAIN', 'idx', "@t1:(w'abc?')", 'DIALECT', 5).contains('@t1:WILDCARD{abc?}')

    env.expect('FT.EXPLAIN', 'idx', "@t1:(*a\\w's')", 'DIALECT', 1).contains('@t1:INTERSECT {\n  @t1:SUFFIX')
    env.expect('FT.EXPLAIN', 'idx', "@t1:(*a\\w's')", 'DIALECT', 2).contains('@t1:INTERSECT {\n  @t1:SUFFIX')
    env.expect('FT.EXPLAIN', 'idx', "@t1:(*a\\w's')", 'DIALECT', 5).error().contains('Syntax error')

    env.expect('FT.EXPLAIN', 'idx', "@t1:(w'*')", 'DIALECT', 1).error().contains('Syntax error')
    env.expect('FT.EXPLAIN', 'idx', "@t1:(w'*')", 'DIALECT', 2).contains('@t1:WILDCARD{*}')
    env.expect('FT.EXPLAIN', 'idx', "@t1:(w'*')", 'DIALECT', 5).contains('@t1:WILDCARD{*}')

    env.expect('FT.EXPLAIN', 'idx', "*-*", 'DIALECT', 1).error().contains('Syntax error')
    env.expect('FT.EXPLAIN', 'idx', "*-*", 'DIALECT', 2).error().contains('Syntax error')
    env.expect('FT.EXPLAIN', 'idx', "*-*", 'DIALECT', 5).error().contains('Syntax error')

    env.expect('FT.EXPLAIN', 'idx', "*1*", 'DIALECT', 1).contains('INFIX{*1*}')
    env.expect('FT.EXPLAIN', 'idx', "*1*", 'DIALECT', 2).contains('INFIX{*1*}')
    env.expect('FT.EXPLAIN', 'idx', "*1*", 'DIALECT', 5).contains('INFIX{*1*}')

    env.expect('FT.EXPLAIN', 'idx', "abc!", 'DIALECT', 1).contains('UNION {\n  abc\n  +abc(expanded)\n}\n')
    env.expect('FT.EXPLAIN', 'idx', "abc!", 'DIALECT', 2).contains('UNION {\n  abc\n  +abc(expanded)\n}\n')
    env.expect('FT.EXPLAIN', 'idx', "abc!", 'DIALECT', 5).error().contains('Syntax error')
    env.expect('FT.EXPLAIN', 'idx', "abc\!", 'DIALECT', 5).contains('UNION {\n  abc!\n  +abc!(expanded)\n}\n')

    res = env.cmd('FT.EXPLAINCLI', 'idx', "1.2e+3", 'DIALECT', 1)
    expected = [
      'INTERSECT {',                                                                                                                                 
      '  UNION {',
      '    1.2',
      '    +1.2(expanded)',
      '  }',
      '  UNION {',
      '    e',
      '    +e(expanded)',
      '  }',
      '  UNION {',
      '    3',
      '    +3(expanded)',
      '  }',
      '}',
      ''
    ]
    env.assertEqual(res, expected)
    res = env.cmd('FT.EXPLAINCLI', 'idx', "1.2e+3", 'DIALECT', 2)
    expected = [
      'INTERSECT {',
      '  UNION {',
      '    1.2',
      '    +1.2(expanded)',
      '  }',
      '  INTERSECT {',
      '    UNION {',
      '      e',
      '      +e(expanded)',
      '    }',
      '    UNION {',
      '      3',
      '      +3(expanded)',
      '    }',
      '  }',
      '}',
      ''
    ]
    env.assertEqual(res, expected)
    res = env.cmd('FT.EXPLAINCLI', 'idx', "1.2e+3", 'DIALECT', 5)
    expected = ['1.2e+3', '']
    env.assertEqual(res, expected)

    res = env.cmd('FT.EXPLAINCLI', 'idx', "1.e+3", 'DIALECT', 1)
    expected = [
      'INTERSECT {',
      '  UNION {',
      '    1',
      '    +1(expanded)',
      '  }',
      '  UNION {',
      '    e',
      '    +e(expanded)',
      '  }',
      '  UNION {',
      '    3',
      '    +3(expanded)',
      '  }',
      '}',
      ''
    ]
    env.assertEqual(res, expected)
    res = env.cmd('FT.EXPLAINCLI', 'idx', "1.e+3", 'DIALECT', 2)
    expected = [
      'INTERSECT {',
      '  UNION {',
      '    1',
      '    +1(expanded)',
      '  }',
      '  INTERSECT {',
      '    UNION {',
      '      e',
      '      +e(expanded)',
      '    }',
      '    UNION {',
      '      3',
      '      +3(expanded)',
      '    }',
      '  }',
      '}',
      ''
    ]
    env.assertEqual(res, expected)
    env.expect('FT.EXPLAINCLI', 'idx', "1.e+3", 'DIALECT', 5).error()\
      .contains('Syntax error')

    # From DIALECT 5, numbers are not expanded
    res = env.cmd('FT.EXPLAINCLI', 'idx', "705", 'DIALECT', 1)
    expected = ['UNION {', '  705', '  +705(expanded)', '}', '']
    env.assertEqual(res, expected)
    res = env.cmd('FT.EXPLAINCLI', 'idx', "705", 'DIALECT', 2)
    env.assertEqual(res, expected)
    res = env.cmd('FT.EXPLAINCLI', 'idx', "705", 'DIALECT', 5)
    expected = ['705', '']
    env.assertEqual(res, expected)

def test_spell_check_dialect_errors(env):
    env.cmd('ft.create', 'idx', 'SCHEMA', 't', 'text')
    MAX_DIALECT = set_max_dialect(env)
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
  env.assertEqual(int(info['search_dialect_4']), module_expect[3])
  env.assertEqual(int(info['search_dialect_5']), module_expect[4])

def check_info_results(env, command, idx1_expect, idx2_expect, should_succeed):
  env.cmd(command) if should_succeed else env.expect(command).error()
  info = index_info(env, 'idx1')
  env.assertEqual(info['dialect_stats'], ['dialect_1', idx1_expect[0],
                                          'dialect_2', idx1_expect[1],
                                          'dialect_3', idx1_expect[2],
                                          'dialect_4', idx1_expect[3],
                                          'dialect_5', idx1_expect[4]])
  info = index_info(env, 'idx2')
  env.assertEqual(info['dialect_stats'], ['dialect_1', idx2_expect[0],
                                          'dialect_2', idx2_expect[1],
                                          'dialect_3', idx2_expect[2],
                                          'dialect_4', idx2_expect[3],
                                          'dialect_5', idx2_expect[4]])
  check_info_module_results(env, [x or y for x, y in zip(idx1_expect, idx2_expect)])

def test_dialect_info(env):
  conn = getConnectionByEnv(env)
  config = ("_" if env.isCluster() else "") + "FT.CONFIG SET DEFAULT_DIALECT 1"
  env.expect(config).ok()
  info = env.cmd('INFO', 'MODULES')

  env.cmd('FT.CREATE', 'idx1', 'SCHEMA', 'business', 'TEXT')
  env.cmd('FT.CREATE', 'idx2', 'SCHEMA', 'country', 'TEXT')
  conn.execute_command('HSET', 'addr:1', 'business', 'foo', 'country', 'USA')

  check_info_results(env, "FT.SEARCH idx1 * DIALECT 3", [0,0,1,0,0], [0,0,0,0,0], True)        # add dialect 3 to idx 1
  check_info_results(env, "FT.SEARCH idx1 * SLOP DIALECT 1", [0,0,1,0,0], [0,0,0,0,0], False)  # should fail. don't update dialects.
  check_info_results(env, "FT.AGGREGATE idx2 *", [0,0,1,0,0], [1,0,0,0,0], True)               # add default dialect to idx2
  check_info_results(env, "FT.AGGREGATE idx1 * FILTER", [0,0,1,0,0], [1,0,0,0,0], False)       # should fail. don't update dialects.
  check_info_results(env, "FT.SPELLCHECK idx1 adr", [1,0,1,0,0], [1,0,0,0,0], True)            # add default dialect to idx1
  check_info_results(env, "FT.SPELLCHECK idx2 * DISTANCE", [1,0,1,0,0], [1,0,0,0,0], False)    # should fail. don't update dialects.
  check_info_results(env, "FT.EXPLAIN idx2 * DIALECT 2", [1,0,1,0,0], [1,0,0,0,0], True)       # not a real query, does not add
  check_info_results(env, "FT.SEARCH idx2 * DIALECT 4", [1,0,1,0,0], [1,0,0,1,0], True)        # add dialect 4 to idx 2
  if not env.isCluster():                                                                       # FT.EXPLAINCLI is not supported on cluster
    check_info_results(env, "FT.EXPLAINCLI idx1 * DIALECT 2", [1,0,1,0,0], [1,0,0,1,0], True)  # not a real query, does not add

  env.flush()
  check_info_module_results(env, [0,0,0,0,0])

def test_dialect5_punct_chars():
  env = Env(moduleArgs="DEFAULT_DIALECT 5")

  env.expect('FT.CREATE', 'idx', 'SCHEMA', 'text', 'TEXT', 'text2', 'TEXT').ok()

  def _expect_valid_syntax(query, verbose=False):
      if verbose:
        print('Testing valid query:', query)
      env.expect('FT.SEARCH', 'idx', query).equal([0])

  def _expect_invalid_syntax(query, verbose=False, err_msg='Syntax error'):
      if verbose:
        print('Testing invalid query:', query)
      env.expect('FT.SEARCH', 'idx', query).error().contains(err_msg)

  def _validate_syntax(c, unaryOperator = False, binaryOperator = False,
                       validInFieldName = False, validBeforeText = False,
                       validInText = False, validAfterText = False,
                       verbose=False):

    if verbose:
      print('--------------------------------------------------')
      print('Testing char:', c)

    err_msg = 'Syntax error at offset'

    # test character before field name
    queries = [f'{c}@text:(abc)', f'{c}@text|text2:(abc)']
    for query in queries:
      if unaryOperator:
        _expect_valid_syntax(query, verbose)
      else:
        _expect_invalid_syntax(query, verbose, err_msg)

    # test character in field name
    queries = [f'@{c}text:(abc)', f'@text{c}:(abc)',
               f'@{c}text|text2:(abc)', f'@text{c}|text2:(abc)']
    for query in queries:
      if validInFieldName:
        _expect_valid_syntax(query, verbose)
      else:
        _expect_invalid_syntax(query, verbose, err_msg)

    # test character before between fieldname (or fielname list) and expression
    queries = [f'@text:{c}(abc)', f'@text|text2:{c}(abc)']
    for query in queries:
      _expect_invalid_syntax(query, verbose, err_msg)

    queries = [f'@text:{c}abc', f'@text|text2:{c}abc']
    for query in queries:
      if unaryOperator:
        _expect_invalid_syntax(query, verbose, err_msg)
      elif validBeforeText:
        if c == '$':
          env.expect('FT.SEARCH', 'idx', query, 'PARAMS', 2, 'abc', 'abc').equal([0])
        else:
          _expect_valid_syntax(query, verbose)
      else:
        _expect_invalid_syntax(query, verbose, err_msg)

    queries = [f'{c}(abc)']
    for query in queries:
      if unaryOperator:
        _expect_valid_syntax(query, verbose)
      else:
        _expect_invalid_syntax(query, verbose, err_msg)

    # test character before text
    queries = [f'@text:({c}abc)', f'@text|text2:({c}abc)',
               f'@text:({c}abc def)', f'@text|text2:({c}abc def)',
               f'@text:({c}abc | def)', f'@text:(def | {c}abc)', f'@text:({c}abc | {c}abc)',
               f'@text|text2:({c}abc | def)', f'@text|text2:(def | {c}abc)', f'@text|text2:({c}abc | {c}abc)',
               f'({c}abc)',
               f'({c}abc def)',
               f'({c}abc | def)', f'(def | {c}abc)',
               f'{c}abc',
               f'{c}abc def',
               f'{c}abc | def', f'def | {c}abc',]
    for query in queries:
      if validBeforeText:
        if c == '$':
          env.expect('FT.SEARCH', 'idx', query, 'PARAMS', 2, 'abc', 'abc').equal([0])
        else:
          _expect_valid_syntax(query, verbose)
      else:
        _expect_invalid_syntax(query, verbose, err_msg)

    # test character enclosing text
    queries = [f'@text:({c}abc {c}abc)', f'@text|text2:({c}abc {c}abc)', f'({c}abc {c}abc)', f'{c}abc {c}abc']
    for query in queries:
      if validBeforeText:
        if c == '$':
          env.expect('FT.SEARCH', 'idx', query, 'PARAMS', 2, 'abc', 'abc').equal([0])
        else:
          _expect_valid_syntax(query, verbose)
      else:
        if c == '%':
          # TODO: This should be invalid
          _expect_valid_syntax(query, verbose)
        else:
          _expect_invalid_syntax(query, verbose, err_msg)

    # test character in the middle of the text
    queries = [f'@text:(ab{c}c)', f'@text|text2:(ab{c}c)', f'(ab{c}c)', f'ab{c}c',
               f'@text:(ab {c}c)', f'@text|text2:(ab {c}c)', f'(ab {c}c)', f'ab {c}c']
    for query in queries:
      if validInText or unaryOperator or binaryOperator:
        if c == '$':
          if verbose:
            print('Testing valid query:', query)
          env.expect('FT.SEARCH', 'idx', query, 'PARAMS', 2, 'c', 'c').equal([0])
        else:
          _expect_valid_syntax(query, verbose)
      else:
          _expect_invalid_syntax(query, verbose, err_msg)

    # test character at the end of the text
    queries = [f'@text:(abc{c})', f'@text|text2:(abc{c})',
               f'(abc{c})', f'abc{c}']
    for query in queries:
      if validAfterText:
        _expect_valid_syntax(query, verbose)
      else:
        _expect_invalid_syntax(query, verbose, err_msg)

    # test character at the end of the query
    queries = [f'@text:(abc){c}', f'@text:(abc) {c}',
               f'@text|text2:(abc){c}', f'@text|text2:(abc) {c}',
               f'(abc){c}', f'(abc) {c}', f'abc {c}']
    for query in queries:
      _expect_invalid_syntax(query, verbose, err_msg)

  #-----------------------------------------------------------------------------
  # Test invalid syntax caused by punctuation chars
  for c in [ '!', '?', '^', '=', '@', '&', '#', '+', '`', '{', '}', '[',
            ']', '(', ')', '<', '>', ':', ';', ',', '.', '/']:
    _validate_syntax(c)

  # '*' is the prefix/infix/suffix operator, it can part of the text
  _validate_syntax('*', validBeforeText=True, validInText=True, validAfterText=True)

  # '$' is used for parameters
  _validate_syntax('$', validBeforeText=True, validInText=True)

  # '|' is the OR operator
  _validate_syntax('|', binaryOperator=True)

  # '_' is valid for field names, and can be part of the text
  _validate_syntax('_', validInFieldName=True, validBeforeText=True,
                  validInText=True, validAfterText=True)

  # '-', and '~' can be part of the text or used before the field name
  _validate_syntax('-', unaryOperator=True, validBeforeText=True, validInText=True)
  _validate_syntax('~', unaryOperator=True, validBeforeText=True, validInText=True)

  # TODO: The following queries should be invalid:
  # @text:(%abc %abc), (%abc %abc), %abc %abc
  # '%' is used for fuzzy search
  _validate_syntax('%')
