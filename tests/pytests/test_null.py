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

        env.expect('FT.INFO', f'idx_{fieldType}').equal([f'idx_{fieldType}', 'json', '1', 'field', fieldType, 'ISNULL'])


def testNullBasic():
    """Tests the basic functionality of null values indexing."""

    env = Env(moduleArgs='DEFAULT_DIALECT 2')
    conn = getConnectionByEnv(env)
    pass

