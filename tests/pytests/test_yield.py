from includes import *
from common import *
from RLTest import Env


def testYieldSingle(env):
  conn = getConnectionByEnv(env)
  env.expect('FT.CREATE', 'idx', 'SCHEMA', 'txt1', 'TEXT', 'txt2', 'TEXT').ok()

  res = conn.execute_command('HSET', 'doc1', 'txt1', 'foo')
  env.assertEqual(res, 2)

  #######################################################################################
  ###                             Current behavior                                    ###
  #######################################################################################

  # FT.SEARCH with content
  env.expect('FT.SEARCH', 'idx', 'foo').equal([1L, 'doc1', ['txt1', 'foo']])

  # FT.SEARCH without content
  env.expect('FT.SEARCH', 'idx', 'foo', 'NOCONTENT').equal([1L, 'doc1'])

  #######################################################################################
  ###                            Sanity test - Single                                 ###
  #######################################################################################

  # Return value without content
  env.expect('FT.SEARCH', 'idx', '(foo)=>{yield_value:"value"}', 'NOCONTENT') \
    .equal([1L, 'doc1', ['value', 'foo']])

  # Return exists without content
  env.expect('FT.SEARCH', 'idx', '(foo)=>{yield_exists:"exists"}', 'NOCONTENT') \
    .equal([1L, 'doc1', ['exists', 'true']])

  # Return distance without content
  env.expect('FT.SEARCH', 'idx', '(foo)=>{yield_distance:"distance"}', 'NOCONTENT') \
    .equal([1L, 'doc1', ['distance', '0']])

  #######################################################################################
  ###                            Sanity test - not exist                              ###
  #######################################################################################

  # Yield source does not exist
  env.expect('FT.SEARCH', 'idx', '(@not_exist:foo)=>{yield_value:"value"}') \
    .equal([0L])

  # Yield source does not exist in intersection, no results
  env.expect('FT.SEARCH', 'idx', 'foo & (@not_exist:foo)=>{yield_value:"value"}', 'NOCONTENT') \
    .equal([0L])

  # Yield source does not exist in union, if not exist, ignore
  env.expect('FT.SEARCH', 'idx', 'foo | (@not_exist:foo)=>{yield_value:"value"}', 'NOCONTENT') \
    .equal([1L, 'doc1'])

  #######################################################################################
  ###                            Sanity test - Multiple                               ###
  #######################################################################################

  # Add the word 'bar' to the document
  res = conn.execute_command('HSET', 'doc2', 'txt2', 'bar')
  env.assertEqual(res, 2)

  # Return two documents with loaded content
  env.expect('FT.SEARCH', 'idx', '(foo)=>{yield_value:"yield_foo" | (bar)=>{yield_value:"yield_bar"}') \
    .equal([2L, 'doc1', ['txt1', 'foo', 'yield_foo', 'foo'], 'doc2', ['txt2', 'bar', 'yield_bar', 'bar']])

  # Return two documents without loaded content
  env.expect('FT.SEARCH', 'idx', '(foo)=>{yield_exists:"exist_foo" | (bar)=>{yield_exists:"exist_bar"}', 'NOCONTENT') \
    .equal([2L, 'doc1', ['txt1', 'foo', 'exist_foo', 'true', 'exist_bar', 'false'],
                'doc2', ['txt2', 'bar', 'exist_foo', 'false', 'exist_bar', 'true']])

  #######################################################################################
  ###                              Error handling                                     ###
  #######################################################################################

  # Fail on non-supported - prototype for failing yield function which is not supported 
  # return an error as the parser fails on an unsupported function 
  env.expect('FT.SEARCH', 'idx', '(foo)=>{yield_not_supported:"not_supported"}') \
    .error().contains('yield_not_supported is not supported for TEXT fields')
  env.expect('FT.SEARCH', 'idx', '(@geo:[12.34 4.54 100 KM])=>{yield_not_supported:"not_supported"}') \
    .error().contains('yield_not_supported is not supported for GEO fields')

  # Fail on duplicate 
  # return an error as the parser fails on duplicate yield names
  env.expect('FT.SEARCH', 'idx', '(foo)=>{yield_value:"foo"}|(geo2)=>{yield_distance:"foo"}') \
    .error().contains('duplicate yield name: "foo"')

  # Fail if one yielder fails
  # return an error as the parser fails on duplicate yield names
  env.expect('FT.SEARCH', 'idx', '(foo)=>{yield_value:"txt_value"} |' +
                                 '(foo)=>{yield_not_supported:"not_supported"}') \
    .error().contains('yield_not_supported is not supported for TEXT fields')

  # Fail yield on multi filters
  env.expect('FT.SEARCH', 'idx', '(foo | @num[0 100])=>{yield_value:"multi_value"}') \
    .error().contains('value of multi fields is not supported')
  env.expect('FT.SEARCH', 'idx', '(foo & @num[0 100])=>{yield_value:"multi_value"}') \
    .error().contains('value of multi fields is not supported')

  #######################################################################################
  ###                                   TEXT                                          ###
  #######################################################################################

def testYieldTypeText(env):
  conn = getConnectionByEnv(env)
  env.expect('FT.CREATE', 'idx', 'SCHEMA', 'txt', 'TEXT').ok()

  res = conn.execute_command('HSET', 'doc', 'txt', 'foo')
  env.assertEqual(res, 2)

  # Current
  env.expect('FT.SEARCH', 'idx', 'foo').equal([1L, 'doc', ['txt', 'foo']])
  env.expect('FT.SEARCH', 'idx', 'not_exist').equal([0L])

  # Return value
  env.expect('FT.SEARCH', 'idx', '(foo)=>{yield_value:"txt_value"}') \
    .equal([1L, 'doc', ['txt_value', 'foo']])
  env.expect('FT.SEARCH', 'idx', 'foo | (not_exist)=>{yield_value:"txt_value"}') \
    .equal([1L, 'doc'])

  # Return exists or not
  env.expect('FT.SEARCH', 'idx', '(foo)=>{yield_exists:"txt_exist"}') \
    .equal([1L, 'doc', ['txt_exist', 'true']])
  env.expect('FT.SEARCH', 'idx', 'foo | (not_exist)=>{yield_exists:"not_exist"}') \
    .equal([1L, 'doc', ['txt_exist', 'false']])

  # return distance as '1' is exists, else '0' 
  env.expect('FT.SEARCH', 'idx', '(foo)=>{yield_distance:"txt_distance"}') \
    .equal([1L, 'doc', ['txt_distance', '0']])
  env.expect('FT.SEARCH', 'idx', 'foo | (not_exist)=>{yield_distance:"txt_distance"}') \
    .equal([1L, 'doc', ['txt_distance', 'inf']])

  #######################################################################################
  ###                                   TAG                                           ###
  #######################################################################################

def testYieldTypeTag(env):
  conn = getConnectionByEnv(env)
  env.expect('FT.CREATE', 'idx', 'SCHEMA', 'tag', 'TAG').ok()

  res = conn.execute_command('HSET', 'doc', 'txt', 'foo', 'tag', 'bar', 'num', 42, 'geo', '1.414 3.1415')
  env.assertEqual(res, 2)

  # Current
  env.expect('FT.SEARCH', 'idx', '@tag:{bar}').equal([1L, 'doc', ['tag', 'bar')
  env.expect('FT.SEARCH', 'idx', '@tag:{not_exist}').equal([0L])

  # Return value
  env.expect('FT.SEARCH', 'idx', '(@tag:{bar})=>{yield_value:"tag_value"}') \
    .equal([1L, 'doc', ['tag_value', 'bar']])
  env.expect('FT.SEARCH', 'idx', '(foo | @tag:{not_exist})=>{yield_value:"tag_value"}') \
    .equal([1L, 'doc'])

  # Return exists or not
  env.expect('FT.SEARCH', 'idx', '(@tag:{bar})=>{yield_exists:"tag_exist"}') \
    .equal([1L, 'doc', ['tag_exist', 'true']])
  env.expect('FT.SEARCH', 'idx', '(foo | @tag:{not_exist})=>{yield_exists:"tag_exist"}') \
    .equal([1L, 'doc', ['tag_exist', 'false']])

  # return distance as '1' is exists, else '0' 
  env.expect('FT.SEARCH', 'idx', '(@tag:{bar})=>{yield_distance:"tag_distance"}') \
    .equal([1L, 'doc', ['tag_distance', '0']])
  env.expect('FT.SEARCH', 'idx', '(foo | @tag:{not_exist})=>{yield_distance:"tag_distance"}') \
    .equal([1L, 'doc', ['tag_distance', 'inf']])

  #######################################################################################
  ###                                   NUMERIC                                       ###
  #######################################################################################

def testYieldTypeNumeric(env):
  conn = getConnectionByEnv(env)
  env.expect('FT.CREATE', 'idx', 'SCHEMA', 'num', 'NUMERIC').ok()

  res = conn.execute_command('HSET', 'doc', 'num', '42')
  env.assertEqual(res, 2)

  # Current
  env.expect('FT.SEARCH', 'idx', '@num[0 100]}').equal([1L, 'doc', ['num', '42']])
  env.expect('FT.SEARCH', 'idx', '@num[0 10]}').equal([0L])

  # Return value
  env.expect('FT.SEARCH', 'idx', '@num[0 100]=>{yield_value:"num_value"}') \
    .equal([1L, 'doc', ['num', '42', 'num_value', '42']])
  env.expect('FT.SEARCH', 'idx', 'foo | @num[0 10]=>{yield_value:"num_value"}') \
    .equal([1L, 'doc', ['num', '42']])

  # Return exists or not
  env.expect('FT.SEARCH', 'idx', '(@num[0 100])=>{yield_exists:"num_value"}') \
    .equal([1L, 'doc', ['num', '42', 'num_value', 'true']])
  env.expect('FT.SEARCH', 'idx', '(foo | @num[0 10])=>{yield_exists:"num_value"}') \
    .equal([1L, 'doc', ['num', '42', 'num_value', 'false']])

  # Return percentile in range or 'inf' if out of range
  env.expect('FT.SEARCH', 'idx', '(@num[10 60])=>{yield_distance:"num_distance"}') \
    .equal([1L, 'doc', ['num', '42', 'num_distance', '0.64']]) # = (42 - 10) / (60 - 10)
  env.expect('FT.SEARCH', 'idx', '(foo | @num[0 10])=>{yield_distance:"num_distance"}') \
    .equal([1L, 'doc', ['num', '42', 'num_distance', 'inf']])


  #######################################################################################
  ###                                     GEO                                         ###
  #######################################################################################

def testYieldTypeGeo(env):
  conn = getConnectionByEnv(env)
  env.expect('FT.CREATE', 'idx', 'SCHEMA', 'geo', 'GEO').ok()

  res = conn.execute_command('HSET', 'doc', 'geo', '1.414 3.1415')
  env.assertEqual(res, 2)

  # Current
  env.expect('FT.SEARCH', 'idx', '@geo[1.414 3.1414 100 M]}') \
    .equal([1L, 'doc', ['geo', '1.414 3.1415')
  env.expect('FT.SEARCH', 'idx', 'foo | @geo[0 0 100 M]') \
    .equal([0L])

  # Return value
  env.expect('FT.SEARCH', 'idx', '@geo[1.414 3.1414 100 M]=>{yield_value:"geo_value"}') \
    .equal([1L, 'doc', ['geo', '1.414 3.1415', 'geo_value', '1.414 3.1415']])
  env.expect('FT.SEARCH', 'idx', 'foo | @geo[0 0 100 M]=>{yield_value:"geo_value"}') \
    .equal([1L, 'doc', ['geo', '1.414 3.1415'])

  # Return exists or not
  env.expect('FT.SEARCH', 'idx', '(@geo[1.414 3.1414 100 M])=>{yield_exists:"geo_exist"}') \
    .equal([1L, 'doc', ['geo', '1.414 3.1415', 'geo_exist', 'true']])
  env.expect('FT.SEARCH', 'idx', '(foo | @geo[0 0 100 M])=>{yield_exists:"geo_exist"}') \
    .equal([1L, 'doc', ['geo', '1.414 3.1415', 'geo_exist', 'false']])

  # Return distance or 'inf' if out of range
  env.expect('FT.SEARCH', 'idx', '(@geo[1.414 3.1414 100 M])=>{yield_distance:"geo_distance"}') \
    .equal([1L, 'doc', ['geo', '1.414 3.1415', 'geo_distance', '321.123']]) # distance between (1.414 3.1414) and (1.414 3.1415)
  env.expect('FT.SEARCH', 'idx', '(foo | @geo[0 0 100 M])=>{yield_distance:"geo_distance"}') \
    .equal([1L, 'doc', ['geo', '1.414 3.1415', 'geo_distance', 'inf']])

  #######################################################################################
  ###                                  VECSIM KNN                                     ###
  #######################################################################################

def testYieldTypeVectorKnn(env):
  conn = getConnectionByEnv(env)

  vecsim_type = ['FLAT', 'HNSW']
  for vs_type in vecsim_type:
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'vecsim', 'VECTOR', vs_type, '6', 'TYPE', 'FLOAT32', 'DIM', '2','DISTANCE_METRIC', 'L2').ok()

    res = conn.execute_command('HSET', 'doc', 'vecsim', 'aaaaaaaa')
    env.assertEqual(res, 2)

    # Current
    env.expect('FT.SEARCH', 'idx', '(*=>[KNN 4 @v aaaaaaab])') \
      .equal([1L, 'doc', ['vecsim', 'aaaaaaaa']])
    # KNN 0 mimics no results
    env.expect('FT.SEARCH', 'idx', '*=>[KNN 0 @v aaaaaaab]') \
      .equal([0L])

    # Return value
    env.expect('FT.SEARCH', 'idx', '(*=>[KNN 4 @v aaaaaaab])=>{yield_value:"vector_value"}') \
      .equal([1L, 'doc', ['vecsim', 'aaaaaaaa', 'vector_value', 'aaaaaaab']])
    env.expect('FT.SEARCH', 'idx', 'foo | (*=>[KNN 0 @v aaaaaaab])=>{yield_value:"vector_value"}') \
      .equal([1L, 'doc', ['vecsim', 'aaaaaaaa']])

    # Return exists or not
    env.expect('FT.SEARCH', 'idx', '(*=>[KNN 4 @v aaaaaaaa])=>{yield_exists:"vector_exist"}') \
      .equal([1L, 'doc', ['vector_exist', 'true']])
    env.expect('FT.SEARCH', 'idx', 'foo | (*=>[KNN 0 @v aaaaaaab])=>{yield_exists:"vector_exist"}') \
      .equal([1L, 'doc', ['vector_exist', 'false']])

    # Return distance or 'inf' if out of range
    env.expect('FT.SEARCH', 'idx', '(*=>[KNN 4 @v aaaaaaab])=>{yield_distance:"vector_distance"}') \
      .equal([1L, 'doc', ['vector_distance', '3.141']])
    env.expect('FT.SEARCH', 'idx', 'foo | (*=>[KNN 0 @v aaaaaaab])=>{yield_distance:"vector_distance"}') \
      .equal([1L, 'doc', ['vector_distance', 'inf']])

  #######################################################################################
  ###                                  VECSIM RANGE                                   ###
  #######################################################################################

def testYieldTypeVectorRange(env):
  conn = getConnectionByEnv(env)

  vecsim_type = ['FLAT', 'HNSW']
  for vs_type in vecsim_type:
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'vecsim', 'VECTOR', vs_type, '6', 'TYPE', 'FLOAT32', 'DIM', '2','DISTANCE_METRIC', 'L2').ok()

    res = conn.execute_command('HSET', 'doc', 'vecsim', 'aaaaaaaa')
    env.assertEqual(res, 2)

    # Current
    env.expect('FT.SEARCH', 'idx', '([VECTOR_RANGE aaaaaaaa 0.1])') \
      .equal([1L, 'doc', ['vecsim', 'aaaaaaaa']])
    env.expect('FT.SEARCH', 'idx', '([VECTOR_RANGE zzzzzzzz 0.1])') \
      .equal([0L])

    # Return value
    env.expect('FT.SEARCH', 'idx', '([VECTOR_RANGE aaaaaaab 0.1])=>{yield_value:"vector_value"}') \
      .equal([1L, 'doc', ['vecsim', 'aaaaaaaa', 'vector_value', 'aaaaaaaa']])
    env.expect('FT.SEARCH', 'idx', '[VECTOR_RANGE aaaaaaab 0.1] | ([VECTOR_RANGE zzzzzzzz 0.1])=>{yield_value:"vector_value"}') \
      .equal([1L, 'doc', ['vecsim', 'aaaaaaaa']])

    # Return exists or not
    env.expect('FT.SEARCH', 'idx', '([VECTOR_RANGE aaaaaaab 0.1])=>{yield_exists:"vector_exist"}') \
      .equal([1L, 'doc', ['vecsim', 'aaaaaaaa', 'vector_exist', 'true']])
    env.expect('FT.SEARCH', 'idx', '[VECTOR_RANGE aaaaaaab 0.1] | ([VECTOR_RANGE zzzzzzzz 0.1])=>{yield_exist:"vector_exist"}') \
      .equal([1L, 'doc', ['vecsim', 'aaaaaaaa', 'vector_exist', 'false']])

    # Return distance or 'inf' if out of range
    env.expect('FT.SEARCH', 'idx', '([VECTOR_RANGE aaaaaaab 0.1])=>{yield_distance:"vector_distance"}') \
      .equal([1L, 'doc', ['vecsim', 'aaaaaaaa', 'vector_distance', '3.141']])
    env.expect('FT.SEARCH', 'idx', '[VECTOR_RANGE aaaaaaab 0.1] | ([VECTOR_RANGE zzzzzzzz 0.1])=>{yield_distance:"vector_distance"}') \
      .equal([1L, 'doc', ['vecsim', 'aaaaaaaa', 'vector_distance', 'inf']])
