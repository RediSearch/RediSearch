from includes import *
try:
    from collections.abc import Iterable
except ImportError:
    from collections import Iterable
import time
from packaging import version
from functools import wraps
import signal
import platform
import itertools
from redis.client import NEVER_DECODE
from redis import exceptions as redis_exceptions
import RLTest
from typing import Any, Callable, List
from RLTest import Env
from RLTest.env import Query
import numpy as np
from scipy import spatial
from pprint import pprint as pp
from deepdiff import DeepDiff
from unittest.mock import ANY, _ANY
from unittest import SkipTest
import inspect
import subprocess
import math

BASE_RDBS_URL = 'https://dev.cto.redis.s3.amazonaws.com/RediSearch/rdbs/'
REDISEARCH_CACHE_DIR = '/tmp/redisearch-rdbs/'
VECSIM_DATA_TYPES = ['FLOAT32', 'FLOAT64', 'FLOAT16', 'BFLOAT16']
VECSIM_ALGOS = ['FLAT', 'HNSW']

class TimeLimit(object):
    """
    A context manager that fires a TimeExpired exception if it does not
    return within the specified amount of time.
    """

    def __init__(self, timeout, message='operation timeout exceeded'):
        self.timeout = timeout
        self.message = message

    def __enter__(self):
        signal.signal(signal.SIGALRM, self.handler)
        signal.setitimer(signal.ITIMER_REAL, self.timeout, 0)

    def __exit__(self, exc_type, exc_value, traceback):
        signal.setitimer(signal.ITIMER_REAL, 0)
        signal.signal(signal.SIGALRM, signal.SIG_DFL)

    def handler(self, signum, frame):
        raise Exception(f'Timeout: {self.message}')

class DialectEnv(Env):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.dialect = None

    def set_dialect(self, dialect):
        self.dialect = dialect
        result = run_command_on_all_shards(self, config_cmd(), 'SET', 'DEFAULT_DIALECT', dialect)
        expected_result = ['OK'] * self.shardsCount
        self.assertEqual(result, expected_result, message=f"Failed to set dialect to {dialect} on all shards")

    def get_dialect(self):
        return self.dialect

    def assertEqual(self, first, second, depth=0, message=None):
        if self.dialect is not None:
            if message is None:
                message = f'Dialect {self.dialect}'
            else:
                message = f'Dialect {self.dialect}, {message}'
        super().assertEqual(first, second, depth=depth+1, message=message)

def getConnectionByEnv(env):
    conn = None
    if env.env == 'oss-cluster':
        conn = env.envRunner.getClusterConnection()
    else:
        conn = env.getConnection()
    return conn

def waitForIndex(env, idx = 'idx'):
    waitForRdbSaveToFinish(env)
    while True:
        res = env.cmd('ft.info', idx)
        try:
            if res[res.index('indexing') + 1] == 0:
                break
        except:
            # RESP3
            if res['indexing'] == 0:
                break
        time.sleep(0.1)

def waitForNoCleanup(env, idx, max_wait=30):
    ''' Wait for the index to finish cleanup

    Parameters:
        max_wait - max duration in seconds to wait
    '''
    waitForRdbSaveToFinish(env)
    retry_wait = 0.1
    max_wait = max(max_wait, retry_wait)
    while max_wait >= 0:
        res = env.cmd('ft.info', idx)
        if int(res[res.index('cleaning') + 1]) == 0:
            break
        time.sleep(retry_wait)
        max_wait -= retry_wait

def py2sorted(x):
    it = iter(x)
    groups = [[next(it)]]
    for item in it:
        for group in groups:
            try:
                item < group[0]  # exception if not comparable
                group.append(item)
                break
            except TypeError:
                continue
        else:  # did not break, make new group
            groups.append([item])
    # print(groups)  # for debugging
    return list(itertools.chain.from_iterable(sorted(group) for group in groups))

def toSortedFlatList(res):
    if isinstance(res, str):
        return [res]
    if isinstance(res, Iterable):
        finalList = []
        for e in res:
            finalList += toSortedFlatList(e)

        return py2sorted(finalList)
    return [res]

def assertInfoField(env, idx, field, expected, delta=None):
    d = index_info(env, idx)
    if delta is None:
        env.assertEqual(d[field], expected)
    else:
        env.assertAlmostEqual(float(d[field]), float(expected), delta=delta)

def sortedResults(res):
    n = res[0]
    res = res[1:]

    y = []
    data = []
    for x in res:
        y.append(x)
        if len(y) == 2:
            data.append(y)
            y = []

    data = py2sorted(data)
    res = [n] + [item for sublist in data for item in sublist]
    return res

def slice_at(v, val):
    try:
        i = v.index(val)
        return v[i+1:]
    except:
        return []

def numver_to_version(numver):
    v = numver
    v = "%d.%d.%d" % (int(v/10000), int(v/100)%100, v%100)
    return version.parse(v)

def arch_int_bits():
  arch = platform.machine()
  if arch == 'x86_64':
    return 128
  elif arch == 'aarch64':
    return 128
  elif arch == 'arm64':
    return 128
  else:
    return 64

module_ver = None
def module_version_at_least(env, ver):
    global module_ver
    if module_ver is None:
        v = env.cmd('MODULE LIST')[0][3]
        module_ver = numver_to_version(v)
    if not isinstance(ver, version.Version):
        ver = version.parse(ver)
    return module_ver >= ver

def module_version_less_than(env, ver):
    return not module_version_at_least(env, ver)

server_ver = None
def server_version_at_least(env: Env, ver):
    global server_ver
    if server_ver is None:
        v = env.cmd('INFO')['redis_version']
        server_ver = version.parse(v)
    if not isinstance(ver, version.Version):
        ver = version.parse(ver)
    return server_ver >= ver

def server_version_less_than(env: Env, ver):
    return not server_version_at_least(env, ver)

def server_version_is_at_least(ver):
    global server_ver
    if server_ver is None:
        import subprocess
        # Expecting something like "Redis server v=7.2.3 sha=******** malloc=jemalloc-5.3.0 bits=64 build=***************"
        v = subprocess.run([Defaults.binary, '--version'], stdout=subprocess.PIPE).stdout.decode().split()[2].split('=')[1]
        server_ver = version.parse(v)
    if not isinstance(ver, version.Version):
        ver = version.parse(ver)
    return server_ver >= ver

def server_version_is_less_than(ver):
    return not server_version_is_at_least(ver)

def index_info(env, idx='idx'):
    res = env.cmd('FT.INFO', idx)
    return to_dict(res)


def dump_numeric_index_tree(env, idx, numeric_field):
    tree_dump = env.cmd(debug_cmd(), 'DUMP_NUMIDXTREE', idx, numeric_field)
    return to_dict(tree_dump)


def dump_numeric_index_tree_root(env, idx, numeric_field):
    tree_root_stats = dump_numeric_index_tree(env, idx, numeric_field)['root']
    root_dump = {tree_root_stats[i]: tree_root_stats[i + 1]
                 for i in range(0, len(tree_root_stats), 2)}
    return root_dump

def numeric_tree_summary(env, idx, numeric_field):
    tree_summary = env.cmd(debug_cmd(), 'NUMIDX_SUMMARY', idx, numeric_field)
    return to_dict(tree_summary)


def getWorkersThpoolStats(env):
    return to_dict(env.cmd(debug_cmd(), "WORKERS", "stats"))

def getWorkersThpoolNumThreads(env):
    return env.cmd(debug_cmd(), "WORKERS", "n_threads")


def getWorkersThpoolStatsFromShard(shard_conn):
    return to_dict(shard_conn.execute_command(debug_cmd(), "WORKERS", "stats"))


def skipOnExistingEnv(env):
    if 'existing' in env.env:
        env.skip()

def SkipOnNonCluster(env):
    if not env.isCluster():
        env.skip()

def skipOnCrdtEnv(env):
    if len([a for a in env.cmd('module', 'list') if a[1] == 'crdt']) > 0:
        env.skip()

def skipOnDialect(env, dialect):
    server_dialect = int(env.expect(config_cmd(), 'GET', 'DEFAULT_DIALECT').res[0][1])
    if dialect == server_dialect:
        env.skip()

def waitForRdbSaveToFinish(env):
    if env.isCluster():
        conns = env.getOSSMasterNodesConnectionList()
    else:
        conns = [env.getConnection()]

    # Busy wait until all connection are done rdb bgsave
    check_bgsave = True
    while check_bgsave:
        check_bgsave = False
        for conn in conns:
            if conn.execute_command('info', 'Persistence')['rdb_bgsave_in_progress']:
                check_bgsave = True
                break


def countKeys(env, pattern='*'):
    if not env.isCluster():
        return len(env.keys(pattern))
    keys = 0
    for shard in range(0, env.shardsCount):
        conn = env.getConnection(shard)
        keys += len(conn.keys(pattern))
    return keys

def collectKeys(env, pattern='*'):
    if not env.isCluster():
        return sorted(env.keys(pattern))
    keys = []
    for shard in range(0, env.shardsCount):
        conn = env.getConnection(shard)
        keys.extend(conn.keys(pattern))
    return sorted(keys)


def debug_cmd():
    return '_FT.DEBUG'

def config_cmd():
    return '_FT.CONFIG'

def enable_unstable_features(env):
    run_command_on_all_shards(env, 'CONFIG', 'SET', 'search-enable-unstable-features', 'yes')

def run_command_on_all_shards(env, *args):
    return [con.execute_command(*args) for con in env.getOSSMasterNodesConnectionList()]

def verify_command_OK_on_all_shards(env, *args):
    res = run_command_on_all_shards(env, *args)
    env.assertEqual(res, ['OK'] * env.shardsCount)

def get_vecsim_debug_dict(env, index_name, vector_field):
    return to_dict(env.cmd(debug_cmd(), "VECSIM_INFO", index_name, vector_field))

def forceInvokeGC(env, idx='idx', timeout=None):
    waitForRdbSaveToFinish(env)
    if timeout is not None:
        # Note: timeout==0 means infinite (no timeout)
        env.cmd(debug_cmd(), 'GC_FORCEINVOKE', idx, timeout)
    else:
        env.cmd(debug_cmd(), 'GC_FORCEINVOKE', idx)

def forceBGInvokeGC(env, idx='idx'):
    waitForRdbSaveToFinish(env)
    env.cmd(debug_cmd(), 'GC_FORCEBGINVOKE', idx)

def no_msan(f):
    @wraps(f)
    def wrapper(env, *args, **kwargs):
        if SANITIZER == 'memory':
            fname = f.__name__
            env.debugPrint(f"skipping {fname} due to memory sanitizer", force=True)
            env.skip()
            return
        return f(env, *args, **kwargs)
    return wrapper

def unstable(f):
    @wraps(f)
    def wrapper(env, *args, **kwargs):
        if UNSTABLE == True:
            fname = f.__name__
            env.debugPrint(f"skipping {fname} because it is unstable", force=True)
            env.skip()
            return
        return f(env, *args, **kwargs)
    return wrapper

# Wraps the decorator `skip` for calling from within a test function
def skipTest(**kwargs):
    skip(**kwargs)(lambda: None)()

def skip(cluster=None, macos=False, asan=False, msan=False, redis_less_than=None, redis_greater_equal=None, min_shards=None, arch=None, gc_no_fork=None, no_json=False):
    def decorate(f):
        def wrapper():
            if not ((cluster is not None) or macos or asan or msan or redis_less_than or redis_greater_equal or min_shards or no_json):
                raise SkipTest()
            if cluster == CLUSTER:
                raise SkipTest()
            if macos and OS == 'macos':
                raise SkipTest()
            if arch == platform.machine():
                raise SkipTest()
            if asan and SANITIZER == 'address':
                raise SkipTest()
            if msan and SANITIZER == 'memory':
                raise SkipTest()
            if redis_less_than and server_version_is_less_than(redis_less_than):
                raise SkipTest()
            if redis_greater_equal and server_version_is_at_least(redis_greater_equal):
                raise SkipTest()
            if min_shards and Defaults.num_shards < min_shards:
                raise SkipTest()
            if gc_no_fork and Env().cmd(config_cmd(), 'GET', 'GC_POLICY')[0][1] != 'fork':
                raise SkipTest()
            if no_json and not REJSON:
                raise SkipTest()
            if len(inspect.signature(f).parameters) > 0:
                env = Env()
                return f(env)
            else:
                return f()
        return wrapper
    return decorate

def to_dict(res):
    if type(res) == dict:
        return res
    d = {res[i]: res[i + 1] for i in range(0, len(res), 2)}
    return d

def to_list(input_dict: dict):
    return [item for pair in input_dict.items() for item in pair]

def get_redis_memory_in_mb(env):
    return float(env.cmd('info', 'memory')['used_memory'])/0x100000

MAX_DIALECT = 0
def set_max_dialect(env):
    global MAX_DIALECT
    if MAX_DIALECT == 0:
        info = env.cmd('INFO', 'MODULES')
        prefix = 'search_dialect_'
        MAX_DIALECT = max([int(key.replace(prefix, '')) for key in info.keys() if prefix in key])
    return MAX_DIALECT

def get_redisearch_index_memory(env, index_key):
    return float(index_info(env, index_key)["inverted_sz_mb"])

def get_redisearch_vector_index_memory(env, index_key):
    return float(index_info(env, index_key)["vector_index_sz_mb"])

def module_ver_filter(env, module_name, ver_filter):
    info = env.getConnection().info()
    for module in info['modules']:
        if module['name'] == module_name:
            ver = int(module['ver'])
            return ver_filter(ver)
    return False

def has_json_api_v2(env):
    return module_ver_filter(env, 'ReJSON', lambda ver: True if ver == 999999 or ver >= 20200 else False)

# A very simple implementation of a bfloat16 array type.
# wrap a numpy array (for basic operations) and override `tobytes` to convert to bfloat16
# This saves us the need to install a new package for bfloat16 support (e.g. tensorflow, torch, bfloat16 numpy extension)
# and deal with dependencies and compatibility issues.
class Bfloat16Array(np.ndarray):
    offset = 2 if sys.byteorder == 'little' else 0
    def __new__(cls, input_array):
        return np.asarray(input_array).view(cls)

    def tobytes(self):
        b32 = np.ndarray.tobytes(self.astype(np.float32))
        # Generate a byte string from every other pair of bytes in b32
        return b''.join(b32[i:i+2] for i in range(Bfloat16Array.offset, len(b32), 4))

# Helper function to create numpy array vector with a specific type
def create_np_array_typed(data, data_type='FLOAT32'):
    if data_type.upper() == 'BFLOAT16':
        return Bfloat16Array(data)
    return np.array(data, dtype=data_type.lower())

def compare_lists_rec(var1, var2, delta):
    if type(var1) != type(var2):
        return False
    try:
        if type(var1) is not str and len(var1) != len(var2):
            return False
    except:
        pass

    if isinstance(var1, list):
        #print("compare_lists_rec: list {}".format(var1))
        for i in range(len(var1)):
            #print("compare_lists_rec: list: i = {}".format(i))
            res = compare_lists_rec(var1[i], var2[i], delta)
            #print("list: var1 = {}, var2 = {}, res = {}".format(var1[i], var2[i], res))
            if res is False:
                return False

    elif isinstance(var1, dict):
        for k in var1:
            res = compare_lists_rec(var1[k], var2[k], delta)
            if res is False:
                return False

    elif isinstance(var1, set):
        for v in var1:
            if v not in var2:
                return False

    elif isinstance(var1, tuple):
        for i in range(len(var1)):
            compare_lists_rec(var1[i], var2[i], delta)
            if res is False:
                return False

    elif isinstance(var1, float):
        diff = var1 - var2
        if diff < 0:
            diff = -diff
        #print("diff {} delta {}".format(diff, delta))
        return diff <= delta

    elif isinstance(var1, str): # float as string
        try:
            diff = float(var1) - float(var2)
            if diff < 0:
                diff = -diff
        except:
            return var1 == var2

        #print("var1 {} var2 {} diff {} delta {}".format(var1, var2, diff, delta))
        return diff <= delta

    else: # int() | bool() | None:
        return var1 == var2

    return True

def compare_lists(env, list1, list2, delta=0.01, _assert=True):
    res = compare_lists_rec(list1, list2, delta + 0.000001)
    if res:
        if _assert:
            env.assertTrue(True, message=f'{str(list1)} ~ {str(list2)}')
        return True
    else:
        if _assert:
            env.assertTrue(False, message=f'{str(list1)} ~ {str(list2)}')
        return False

class ConditionalExpected:
    def __init__(self, env, cond):
        self.env = env
        self.cond_val = cond(env)
        self.query = None

    def call(self, *query):
        self.query = query
        return self

    def expect_when(self, cond_val, func: Callable[[Query], Any]):
        if cond_val == self.cond_val:
            func(self.env.expect(*self.query))
        return self

def load_vectors_to_redis(env, n_vec, query_vec_index, vec_size, data_type='FLOAT32', ids_offset=0, seed=10):
    conn = getConnectionByEnv(env)
    np.random.seed(seed)
    for i in range(n_vec):
        vector = create_np_array_typed(np.random.rand(vec_size), data_type)
        if i == query_vec_index:
            query_vec = vector
        conn.execute_command('HSET', ids_offset + i, 'vector', vector.tobytes())
    return query_vec

def sortResultByKeyName(res, start_index=1):
  '''
    Sorts the result by NAMEs
    res = [<COUNT>, '<NAME_1>, '<VALUE_1>', '<NAME_2>, '<VALUE_2>', ...]

    If VALUEs are lists, they are sorted by name as well
  '''
  # Sort name and value pairs by name
  pairs = [(name,sortResultByKeyName(value, 0) if isinstance(value, list) else value) for name,value in zip(res[start_index::2], res[start_index+1::2])]
  pairs = [i for i in sorted(pairs, key=lambda x: x[0])]
  # Flatten the sorted pairs to a list
  pairs = [i for pair in pairs for i in pair]
  if start_index == 1:
    # Bring the COUNT back to the beginning
    res = [res[0], *pairs]
  else:
    res = [*pairs]
  return res

def dict_diff(res, exp, show=False, ignore_order=True, significant_digits=7,
              ignore_numeric_type_changes=True, exclude_paths=None,
              exclude_regex_paths=None):
    dd = DeepDiff(res, exp, exclude_types={_ANY}, ignore_order=ignore_order, significant_digits=significant_digits,
                  ignore_numeric_type_changes=ignore_numeric_type_changes, exclude_paths=exclude_paths,
                  exclude_regex_paths=exclude_regex_paths)
    if dd != {} and show:
        pp(dd)
    return dd

def number_to_ordinal(n: int) -> str:
    if 11 <= (n % 100) <= 13:
        suffix = 'th'
    else:
        suffix = ['th', 'st', 'nd', 'rd', 'th'][min(n % 10, 4)]
    return str(n) + suffix

def populate_db(env: Env, idx_name: str = 'idx', text: bool = False, numeric: bool = False, tag: bool = False, n_per_shard=10000):
    """
    Creates a simple index called `idx`, and populates the database with
    `n * n_shards` matching documents.
    The names of the fields will be 'text1', 'numeric1', 'tag1' corresponding to
    the field type.

    Parameters:
    -----------
        env (Env): Environment to populate.
        idx_name: The name of the index to create.
        text (bool): Whether to create a text field in the index.
        numeric (bool): Whether to create a numeric field in the index.
        tag (bool): Whether to create a tag field in the index.
        n_per_shard (int): Number of documents to create per shard.

    Returns:
    -----------
        None
    """
    conn = getConnectionByEnv(env)
    text_f = 'text1 TEXT' if text else ''
    numeric_f = 'numeric1 NUMERIC' if numeric else ''
    tag_f = 'tag1 TAG' if tag else ''

    index_creation = f'FT.CREATE {idx_name} SCHEMA'
    if text:
        index_creation += f' {text_f}'
    if numeric:
        index_creation += f' {numeric_f}'
    if tag:
        index_creation += f' {tag_f}'

    conn.execute_command(*index_creation.split(' '))

    num_docs = n_per_shard * env.shardsCount
    pipeline = conn.pipeline(transaction=False)
    for i in range(num_docs):
        population_command = f'HMSET doc:{i}'
        if text:
            population_command += f' text1 lala:{i}'
        if numeric:
            population_command += f' numeric1 {i}'
        if tag:
            population_command += f' tag1 MOVIE'

        pipeline.execute_command(*population_command.split(' '))
        if i % 1000 == 0:
            pipeline.execute()
            pipeline = conn.pipeline(transaction=False)
    pipeline.execute()

def get_TLS_args():
    root = os.environ.get('ROOT', None)
    if root is None:
        root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))) # go up 3 levels from common.py

    cert_file       = os.path.join(root, 'bin', 'tls', 'redis.crt')
    key_file        = os.path.join(root, 'bin', 'tls', 'redis.key')
    ca_cert_file    = os.path.join(root, 'bin', 'tls', 'ca.crt')
    passphrase_file = os.path.join(root, 'bin', 'tls', '.passphrase')

    with_pass = server_version_is_at_least('6.2')

    # If any of the files are missing, generate them
    import subprocess
    subprocess.run([os.path.join(root, 'sbin', 'gen-test-certs'), str(1 if with_pass else 0)]).check_returncode()

    def get_passphrase():
        with open(passphrase_file, 'r') as f:
            return f.read()

    passphrase = get_passphrase() if with_pass else None

    return cert_file, key_file, ca_cert_file, passphrase

# Dispatch a command to make sure that the module is loaded and initialized
# We need to dispatch a command that will activate the topology updater, by
# sending a command to the shards. Otherwise the cluster.refresh command will
# not be effective, due to lazy initialization of the topology updater.
# Thus we dispatch a command that does not have an index, as it is not stopped
# in the coordinator level.
def verify_shard_init(shard):
    # One of the following errors can be raised (timing), yet they
    # mean the same thing in this case - the command was dispatched
    # to the shards before the connections were ready. Continue to
    # try until success\timeout.
    uninitialized_errors = [
        'ERRCLUSTER Uninitialized cluster state, could not perform command',
        'Could not distribute command'
    ]
    # The following error means that the cluster is initialized, as it was
    # returned from the shards.
    initialized_error = 'Alias does not exist'

    with TimeLimit(5, 'Failed to verify shard initialization'):
        while True:
            try:
                shard.execute_command('FT.ALIASDEL', 'non-existing-alias')
                break
            except redis_exceptions.ResponseError as e:
                if any([err in str(e) for err in uninitialized_errors]):
                    continue
                elif initialized_error in str(e):
                    break
                # Unexpected error, raise it.
                raise

def cmd_assert(env, cmd, res, message=None):
    db_res = env.cmd(*cmd)
    env.assertEqual(db_res, res, message=message)

# fields should be in capital letters
def getInvertedIndexInitialSize(env, fields, depth=0):
    total_size = 0
    for field in fields:
        if field in ['GEO', 'NUMERIC']:
            block_size = 48
            initial_block_cap = 6
            inverted_index_meta_data = 40
            total_size += (block_size + initial_block_cap + inverted_index_meta_data)
            continue
        env.assertTrue(field in ['TEXT', 'TAG', 'GEOMETRY', 'VECTOR'], message=f"type {field} is not supported", depth=depth+1)

    return total_size

# fields should be in capital letters
def getInvertedIndexInitialSize_MB(env, fields, depth=0) -> float:
    return getInvertedIndexInitialSize(env, fields, depth=depth+1) / float(1024 * 1024)

def check_index_info(env, idx, exp_num_records, exp_inv_idx_size, msg="", depth=0):
    d = index_info(env, idx)
    env.assertEqual(float(d['num_records']), exp_num_records, message=msg + ", num_records", depth=depth+1)

    if(exp_inv_idx_size != None):
        env.assertEqual(float(d['inverted_sz_mb']), exp_inv_idx_size, message=msg + ", inverted_sz_mb", depth=depth+1)

# Iterates items in d1 and compare their keys[value] with d2
# asserts when a key is missing in d2
# For simplicity, all values are compared as floats
def compare_numeric_dicts(env, d1, d2, d1_name="d1", d2_name="d2", msg="", _assert=True, depth=0):
    for key, value in d1.items():
        try:
            res = float(d2[key]) == float(value)
            if _assert:
                env.assertTrue(res, message=msg + " value is different in key: " + key, depth=depth+1)
            else:
                if res == False:
                    return False
        except KeyError:
            if _assert:
                env.assertTrue(False, message=msg + f" key {key} exists in {d1_name} but doesn't exist in {d2_name}")
            else:
                raise KeyError
    return True

def compare_index_info_dict(env, idx, expected_info_dict, msg="", depth=0):
    d = index_info(env, idx)
    compare_numeric_dicts(env, expected_info_dict, d, "expected_info_dict", "index_info", msg, depth=depth+1)

# expected info for index that was initialized and *emptied*
def check_index_info_empty(env, idx, fields, msg="after delete all and gc", depth=0):
    expected_size = getInvertedIndexInitialSize_MB(env, fields, depth=depth+1)
    check_index_info(env, idx, exp_num_records=0, exp_inv_idx_size=expected_size, msg=msg, depth=depth+1)

def recursive_index(lst, target):
    for i, element in enumerate(lst):
        if isinstance(element, list):
            sublist_index = recursive_index(element, target)
            if sublist_index != -1:
                return [i] + sublist_index
        elif element == target:
            return [i]
    return -1

def recursive_contains(lst, target):
    return recursive_index(lst, target) != -1

def access_nested_list(lst, index):
    result = lst
    for entry in index:
        result = result[entry]
    return result

def downloadFile(env, file_name, depth=0):
    path = os.path.join(REDISEARCH_CACHE_DIR, file_name)
    path_dir = os.path.dirname(path)
    os.makedirs(path_dir, exist_ok=True)  # create dir if not exists
    if not os.path.exists(path):
        env.debugPrint(f"downloading {file_name}", force=True)
        try:
            subprocess.run(
                [
                    "wget",
                    "--no-check-certificate",
                    BASE_RDBS_URL + file_name,
                    "-O",
                    path,
                    "-q",
                ],
                check=True,
            )
        except subprocess.CalledProcessError as e:
            env.assertTrue(
                False,
                message=f"Failed to download {BASE_RDBS_URL + file_name}. Return code: {e.returncode}, output: {e.output}, stderr: {e.stderr}",
                depth=depth + 1,
            )
            try:
                os.remove(path)
                env.debugPrint(f"Partially downloaded file {path}. Removing it.", force=True)
            except OSError:
                env.debugPrint(f"Failed to remove {path}", force=True)
                pass
            return False
    if not os.path.exists(path):
        env.assertTrue(
            False,
            message=f"{path} does not exist after download",
            depth=depth + 1,
        )
        return False
    return True


def downloadFiles(env, rdbs=None, depth=0):
    if rdbs is None:
        return False

    for f in rdbs:
        if not downloadFile(env, f, depth=depth + 1):
            return False
    return True

def index_errors(env, idx = 'idx'):
    return to_dict(index_info(env, idx)['Index Errors'])
def field_errors(env, idx = 'idx', fld_index = 0):
    return to_dict(to_dict(to_dict(index_info(env, idx)['field statistics'][fld_index]))['Index Errors'])

def VerifyTimeoutWarningResp3(env, res, message="", depth=0):
    env.assertTrue(res['warning'], message=message + " expected warning", depth=depth+1)
    if (res['warning']):
        env.assertContains("Timeout", res["warning"][0], message=message + " expected timeout warning", depth=depth+1)

def runDebugQueryCommand(env, query_cmd, debug_params):
    return env.cmd(debug_cmd(), *query_cmd, *debug_params, 'DEBUG_PARAMS_COUNT', len(debug_params))

def runDebugQueryCommandTimeoutAfterN(env, query_cmd, timeout_res_count, internal_only=False):
    debug_params = ['TIMEOUT_AFTER_N', timeout_res_count]
    if internal_only:
        debug_params.append("INTERNAL_ONLY")
    return runDebugQueryCommand(env, query_cmd, debug_params)

def runDebugQueryCommandAndCrash(env, query_cmd):
    debug_params = ['CRASH']
    return env.expect(debug_cmd(), *query_cmd, *debug_params, 'DEBUG_PARAMS_COUNT', len(debug_params)).error()

def shardsConnections(env):
  for s in range(1, env.shardsCount + 1):
      yield env.getConnection(shardId=s)

def waitForIndexFinishScan(env, idx = 'idx'):
    while index_info(env, idx)['percent_indexed'] != '1':
        time.sleep(0.1)

def bgScanCommand():
    return debug_cmd() + ' BG_SCAN_CONTROLLER'

def getDebugScannerStatus(env, idx = 'idx'):
    return env.cmd(bgScanCommand(), 'GET_DEBUG_SCANNER_STATUS', idx)

def checkDebugScannerError(env, idx = 'idx', expected_error = ''):
    env.expect(bgScanCommand(), 'GET_DEBUG_SCANNER_STATUS', idx).error() \
        .contains(expected_error)

def set_tight_maxmemory_for_oom(env, memory_limit_per = 1.0):
    # Get current memory consumption value
    memory_usage = env.cmd('INFO', 'MEMORY')['used_memory']
    # Set memory limit to less then memory limit
    required_memory = memory_usage * (1/memory_limit_per)
    # Round up and add 1
    new_memory = math.ceil(required_memory) + 1

    env.expect('config', 'set', 'maxmemory',new_memory).ok()

def set_unlimited_maxmemory_for_oom(env):
    env.expect('config', 'set', 'maxmemory', 0).ok()


def waitForIndexStatus(env, status, idx='idx'):
    while getDebugScannerStatus(env, idx) != status:
        time.sleep(0.1)

def waitForIndexPauseScan(env,idx = 'idx'):
    waitForIndexStatus(env,'PAUSED', idx)

def shard_getDebugScannerStatus(env, shardId, idx = 'idx'):
    return env.getConnection(shardId).execute_command(bgScanCommand(), 'GET_DEBUG_SCANNER_STATUS', idx)

def shard_waitForIndexStatus(env, shardId, status, idx='idx'):
    while shard_getDebugScannerStatus(env, shardId, idx) != status:
        time.sleep(0.1)

def shard_waitForIndexPauseScan(env, shardId, idx = 'idx'):
    shard_waitForIndexStatus(env, shardId, 'PAUSED', idx)

def allShards_waitForIndexPauseScan(env, idx = 'idx'):
    for shardId in range(1, env.shardsCount + 1):
        shard_waitForIndexPauseScan(env, shardId, idx)

def allShards_waitForIndexStatus(env, status, idx='idx'):
    for shardId in range(1, env.shardsCount + 1):
        shard_waitForIndexStatus(env, shardId, status, idx)

def shard_waitForIndexFinishScan(env, shardId, idx = 'idx'):
    while index_info(env, idx)['percent_indexed'] != '1':
        time.sleep(0.1)

def allShards_waitForIndexFinishScan(env, idx = 'idx'):
    for shardId in range(1, env.shardsCount + 1):
        shard_waitForIndexFinishScan(env, shardId, idx)

def shard_set_tight_maxmemory_for_oom(env, shardId, memory_limit_per = 1.0):
    # Get current memory consumption value
    memory_usage = env.getConnection(shardId).execute_command('INFO', 'MEMORY')['used_memory']
    # Set memory limit to less then memory limit
    required_memory = memory_usage * (1/memory_limit_per)
    # Round up and add 1
    new_memory = math.ceil(required_memory) + 1
    res = env.getConnection(shardId).execute_command('config', 'set', 'maxmemory', new_memory)
    env.assertEqual(res, 'OK')

def allShards_set_tight_maxmemory_for_oom(env, memory_limit_per = 1.0):
    for shardId in range(1, env.shardsCount + 1):
        shard_set_tight_maxmemory_for_oom(env, shardId, memory_limit_per)

def shard_set_unlimited_maxmemory_for_oom(env, shardId):
    res = env.getConnection(shardId).execute_command('config', 'set', 'maxmemory', 0)
    env.assertEqual(res, 'OK')

def allShards_set_unlimited_maxmemory_for_oom(env):
    for shardId in range(1, env.shardsCount + 1):
        shard_set_unlimited_maxmemory_for_oom(env, shardId)

def assertEqual_dicts_on_intersection(env, d1, d2, message=None, depth=0):
    for k in d1:
        if k in d2:
            env.assertEqual(d1[k], d2[k], message=message, depth=depth+1)
