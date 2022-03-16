from RLTest import Env

from common import *
from includes import *

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
    env.expect("FT.SEARCH idx 'hello' DIALECT").error().contains("Need argument for DIALECT")
    env.expect("FT.SEARCH idx 'hello' DIALECT 0").error().contains("DIALECT requires a non negative integer >=1 and <= 2")
    env.expect("FT.SEARCH idx 'hello' DIALECT 3").error().contains("DIALECT requires a non negative integer >=1 and <= 2")
