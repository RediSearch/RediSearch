from common import *
import json

EMPTY_RESULT = [0]

def testNullValidations():
    """Validates the edge-cases of the `ISNULL` field option"""

    env = Env(moduleArgs="DEFAULT_DIALECT 2")

    for fieldType in ["TAG", "TEXT", "NUMERIC", "GEO", "GEOSHAPE"]:

        # JSON index field with ISNULL is valid
        env.expect('FT.CREATE', f'idx_{fieldType}' , 'ON', 'JSON', 'SCHEMA',
                   'field', fieldType, 'ISNULL').ok()
        
        # HASH index field with ISNULL is invalid
        env.expect('FT.CREATE', f'idx_{fieldType}_hash1' , 'ON', 'HASH',
                   'SCHEMA', 'field', fieldType, 'ISNULL').error().\
                    contains("`ISNULL` is only valid for JSON index")
        
        env.expect('FT.CREATE', f'idx_{fieldType}_hash2' , 'SCHEMA',
                   'field', fieldType, 'ISNULL').error().\
                    contains("`ISNULL` is only valid for JSON index")

def testNullInfo():
    """Tests that the `FT.INFO` command returns the correct information
    regarding the indexing of null values for a field"""

    env = Env(moduleArgs="DEFAULT_DIALECT 2")

    for fieldType in ["TAG", "TEXT", "NUMERIC", "GEO", "GEOSHAPE"]:
        env.expect('FT.CREATE', f'idx_{fieldType}' , 'ON', 'JSON', 'SCHEMA',
                   'field', fieldType, 'ISNULL').ok()

        info = index_info(env, f'idx_{fieldType}')
        field_info = info['attributes'][0]
        env.assertEqual(field_info[-1], 'ISNULL')

    # Tes FT.INFO for a VECTOR field
    for data_type in ['FLOAT32', 'FLOAT64']:
        env.expect(
            'FT.CREATE', f'idx_{data_type}' , 'ON', 'JSON', 'SCHEMA',
            '$.v', 'AS', 'vec', 'VECTOR', 'FLAT', '6', 'TYPE', data_type,
            'DIM', '2','DISTANCE_METRIC', 'L2', 'ISNULL').ok()
        info = index_info(env, f'idx_{data_type}')
        field_info = info['attributes'][0]
        env.assertEqual(field_info[-1], 'ISNULL')

def testNullBasic():
    """Tests the basic functionality of null values indexing."""

    env = Env(moduleArgs='DEFAULT_DIALECT 2')
    conn = getConnectionByEnv(env)
    pass

