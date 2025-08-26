from RLTest import Env
from includes import *
from common import *

def setup_index(env):
    dim = 2
    conn = env.getClusterConnectionIfNeeded()
    env.expect('FT.CREATE idx SCHEMA description TEXT embedding VECTOR FLAT 6 TYPE FLOAT32 DIM 2 DISTANCE_METRIC L2').ok
    conn.execute_command('HSET', 'doc:1', 'description', "red shoes", 'embedding', np.random.rand(dim).astype(np.float32).tobytes())
    conn.execute_command('HSET', 'doc:2', 'description', "red running shoes", 'embedding', np.random.rand(dim).astype(np.float32).tobytes())
    conn.execute_command('HSET', 'doc:3', 'description', "running gear", 'embedding', np.random.rand(dim).astype(np.float32).tobytes())
    conn.execute_command('HSET', 'doc:4', 'description', "blue shoes", 'embedding', np.random.rand(dim).astype(np.float32).tobytes())



def test_hybrid_timeout_policy_fail():
    env = Env(enableDebugCommand=True, moduleArgs='ON_TIMEOUT FAIL')
    setup_index(env)
    env.expect('_FT.DEBUG', 'FT.HYBRID', 'idx', 'SEARCH', 'running', 'VSIM', '@embedding', '$BLOB', 'PARAMS', '2', 'BLOB', np.random.rand(2).astype(np.float32).tobytes(), 'TIMEOUT_AFTER_N_SEARCH', '1', 'DEBUG_PARAMS_COUNT', '2').error().contains('Timeout limit was reached')
    env.expect('_FT.DEBUG', 'FT.HYBRID', 'idx', 'SEARCH', 'running', 'VSIM', '@embedding', '$BLOB', 'PARAMS', '2', 'BLOB', np.random.rand(2).astype(np.float32).tobytes(), 'TIMEOUT_AFTER_N_VSIM', '1', 'DEBUG_PARAMS_COUNT', '2').error().contains('Timeout limit was reached')
    env.expect('_FT.DEBUG', 'FT.HYBRID', 'idx', 'SEARCH', 'running', 'VSIM', '@embedding', '$BLOB', 'PARAMS', '2', 'BLOB', np.random.rand(2).astype(np.float32).tobytes(), 'TIMEOUT_AFTER_N_SEARCH', '1','TIMEOUT_AFTER_N_VSIM', '2', 'DEBUG_PARAMS_COUNT', '4').error().contains('Timeout limit was reached')



def test_hybrid_timeout_policy_return(env):
    env = Env(enableDebugCommand=True, moduleArgs='ON_TIMEOUT RETURN')
    setup_index(env)
    def get_warnings(response):
        warnings_index = recursive_index(response, 'warning')
        warnings_index[-1] += 1
        return access_nested_list(response, warnings_index)
    response = env.cmd('_FT.DEBUG', 'FT.HYBRID', 'idx', 'SEARCH', 'running', 'VSIM', '@embedding', '$BLOB', 'PARAMS', '2', 'BLOB', np.random.rand(2).astype(np.float32).tobytes(), 'TIMEOUT_AFTER_N_SEARCH', '1', 'DEBUG_PARAMS_COUNT', '2')
    env.assertTrue('Timeout limit was reached' in get_warnings(response))
    response = env.cmd('_FT.DEBUG', 'FT.HYBRID', 'idx', 'SEARCH', 'running', 'VSIM', '@embedding', '$BLOB', 'PARAMS', '2', 'BLOB', np.random.rand(2).astype(np.float32).tobytes(), 'TIMEOUT_AFTER_N_VSIM', '1', 'DEBUG_PARAMS_COUNT', '2')
    env.assertTrue('Timeout limit was reached' in get_warnings(response))
    response = env.cmd('_FT.DEBUG', 'FT.HYBRID', 'idx', 'SEARCH', 'running', 'VSIM', '@embedding', '$BLOB', 'PARAMS', '2', 'BLOB', np.random.rand(2).astype(np.float32).tobytes(), 'TIMEOUT_AFTER_N_SEARCH', '1','TIMEOUT_AFTER_N_VSIM', '2', 'DEBUG_PARAMS_COUNT', '4')
    env.assertTrue('Timeout limit was reached' in get_warnings(response))