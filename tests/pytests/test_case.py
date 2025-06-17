from common import *

def testWithVectorRange(env):
    conn = getConnectionByEnv(env)
    dim = 4
    env.expect('FT.CREATE', 'idx', 'SCHEMA',
               'v', 'VECTOR', 'FLAT', '6', 'DIM', dim, 'DISTANCE_METRIC', 'L2', 'TYPE', 'FLOAT32',
               't', 'TEXT').ok()

    conn.execute_command('HSET', 'doc1', 'v', create_np_array_typed([1.1] * dim).tobytes(), 't', 'pizza')
    conn.execute_command('HSET', 'doc2', 'v', create_np_array_typed([1.4] * dim).tobytes())
    conn.execute_command('HSET', 'doc3', 't', 'pizzas')

    res = conn.execute_command(
        'FT.AGGREGATE', 'idx', '@v:[VECTOR_RANGE .7 $vector]=>{$YIELD_DISTANCE_AS: vector_distance} | @t:(pizza)',
        'ADDSCORES', 'SCORER', 'BM25STD',
        'PARAMS', '2', 'vector', create_np_array_typed([1.2] * dim).tobytes(),
        'LOAD', '3', '@vector_distance', '@__key', 't',
        # 'APPLY', 'case(exists(@vector_distance), (@__score * 0.3 + @vector_distance * 0.7), (@__score * 0.3))', 'AS', 'final_score',
        # 'APPLY', 'case(!exists(@vector_distance), (@__score * 0.3), (@__score * 0.3 + @vector_distance * 0.7))', 'AS', 'final_score',
        # 'APPLY', 'case(exists(@vector_distance), @vector_distance, 0)', 'AS', 'final_vector_distance',
        # 'APPLY', 'case(exists(@__score), @__score, 0)', 'AS', 'final_score',
        # 'APPLY', 'vector_distance*0.7 + score*0.3' 'AS', 'final_score',

        # CRASH!!! if using negation and invalid function name
        # 'APPLY', 'case(!exist(@vector_distance), (@__score * 0.3), (@__score * 0.3 + @vector_distance * 0.7))', 'AS', 'final_score',
        'SORTBY', '4', '@__score', 'DESC', '@vector_distance', 'DESC',
        'DIALECT', '2')
    print(res)
    # env.assertEqual(res[1:], [['vector_distance', '0', 't', 'pizza']])

