
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
import RLTest
from typing import Any, Callable
from RLTest import Env
from RLTest.env import Query
import numpy as np
from scipy import spatial
from pprint import pprint as pp
from deepdiff import DeepDiff
from unittest.mock import ANY, _ANY
from unittest import SkipTest
import inspect


BASE_RDBS_URL = 'https://s3.amazonaws.com/redismodules/redisearch-oss/rdbs/'
VECSIM_DATA_TYPES = ['FLOAT32', 'FLOAT64']
VECSIM_ALGOS = ['FLAT', 'HNSW']

class TimeLimit(object):
    """
    A context manager that fires a TimeExpired exception if it does not
    return within the specified amount of time.
    """

    def __init__(self, timeout):
        self.timeout = timeout

    def __enter__(self):
        signal.signal(signal.SIGALRM, self.handler)
        signal.setitimer(signal.ITIMER_REAL, self.timeout, 0)

    def __exit__(self, exc_type, exc_value, traceback):
        signal.setitimer(signal.ITIMER_REAL, 0)
        signal.signal(signal.SIGALRM, signal.SIG_DFL)

    def handler(self, signum, frame):
        raise Exception('timeout')


def getConnectionByEnv(env):
    conn = None
    if env.env == 'oss-cluster':
        conn = env.envRunner.getClusterConnection()
    else:
        conn = env.getConnection()
    return conn

def waitForIndex(env, idx):
    waitForRdbSaveToFinish(env)
    while True:
        res = env.execute_command('ft.info', idx)
        try:
            if int(res[res.index('indexing') + 1]) == 0:
                break
        except:
            # RESP3
            if int(res['indexing']) == 0:
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
        res = env.execute_command('ft.info', idx)
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
    if not env.isCluster():
        res = env.cmd('ft.info', idx)
        d = {res[i]: res[i + 1] for i in range(0, len(res), 2)}
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
    return 64
  else:
    return 64

module_ver = None
def module_version_at_least(env, ver):
    global module_ver
    if module_ver is None:
        v = env.execute_command('MODULE LIST')[0][3]
        module_ver = numver_to_version(v)
    if not isinstance(ver, version.Version):
        ver = version.parse(ver)
    return module_ver >= ver

def module_version_less_than(env, ver):
    return not module_version_at_least(env, ver)

server_ver = None
def server_version_at_least(env, ver):
    global server_ver
    if server_ver is None:
        v = env.execute_command('INFO')['redis_version']
        server_ver = version.parse(v)
    if not isinstance(ver, version.Version):
        ver = version.parse(ver)
    return server_ver >= ver

def server_version_less_than(env, ver):
    return not server_version_at_least(env, ver)

def index_info(env, idx):
    res = env.cmd('FT.INFO', idx)
    res = {res[i]: res[i + 1] for i in range(0, len(res), 2)}
    return res


def dump_numeric_index_tree(env, idx, numeric_field):
    res = env.cmd('FT.DEBUG', 'DUMP_NUMIDXTREE', idx, numeric_field)
    tree_dump = {res[i]: res[i + 1] for i in range(0, len(res), 2)}
    return tree_dump


def dump_numeric_index_tree_root(env, idx, numeric_field):
    tree_root_stats = dump_numeric_index_tree(env, idx, numeric_field)['root']
    root_dump = {tree_root_stats[i]: tree_root_stats[i + 1]
                 for i in range(0, len(tree_root_stats), 2)}
    return root_dump


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
    server_dialect = int(env.expect('FT.CONFIG', 'GET', 'DEFAULT_DIALECT').res[0][1])
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
    if not env.is_cluster():
        return len(env.keys(pattern))
    keys = 0
    for shard in range(0, env.shardsCount):
        conn = env.getConnection(shard)
        keys += len(conn.keys(pattern))
    return keys

def collectKeys(env, pattern='*'):
    if not env.is_cluster():
        return sorted(env.keys(pattern))
    keys = []
    for shard in range(0, env.shardsCount):
        conn = env.getConnection(shard)
        keys.extend(conn.keys(pattern))
    return sorted(keys)

def ftDebugCmdName(env):
    return '_ft.debug' if env.isCluster() else 'ft.debug'


def get_vecsim_debug_dict(env, index_name, vector_field):
    return to_dict(env.cmd(ftDebugCmdName(env), "VECSIM_INFO", index_name, vector_field))


def forceInvokeGC(env, idx):
    waitForRdbSaveToFinish(env)
    env.cmd(ftDebugCmdName(env), 'GC_FORCEINVOKE', idx)

def no_msan(f):
    @wraps(f)
    def wrapper(env, *args, **kwargs):
        if SANITIZER == 'memory':
            fname = f.__name__
            env.debugPrint("skipping {} due to memory sanitizer".format(fname), force=True)
            env.skip()
            return
        return f(env, *args, **kwargs)
    return wrapper

def unstable(f):
    @wraps(f)
    def wrapper(env, *args, **kwargs):
        if UNSTABLE == True:
            fname = f.__name__
            env.debugPrint("skipping {} because it is unstable".format(fname), force=True)
            env.skip()
            return
        return f(env, *args, **kwargs)
    return wrapper

def skip(cluster=False, macos=False, asan=False, msan=False, noWorkers=False):
    def decorate(f):
        if len(inspect.signature(f).parameters) == 0:
            @wraps(f)
            def wrapper(*args, **kwargs):
                if not (cluster or macos or asan or msan or noWorkers):
                    raise SkipTest()
                if macos and OS == 'macos':
                    raise SkipTest()
                if asan and SANITIZER == 'address':
                    raise SkipTest()
                if msan and SANITIZER == 'memory':
                    raise SkipTest()
                if noWorkers and not MT_BUILD:
                    raise SkipTest()

                return f(*args, **kwargs)
        else:
            @wraps(f)
            def wrapper(x, *args, **kwargs):
                env = x if isinstance(x, Env) else x.env
                if not (cluster or macos or asan or msan or noWorkers):
                    env.skip()
                if cluster and env.isCluster():
                    env.skip()
                if macos and OS == 'macos':
                    env.skip()
                if asan and SANITIZER == 'address':
                    env.skip()
                if msan and SANITIZER == 'memory':
                    env.skip()
                if noWorkers and not MT_BUILD:
                    env.skip()
                return f(x, *args, **kwargs)
        return wrapper
    return decorate

def to_dict(res):
    if type(res) == dict:
        return res
    d = {res[i]: res[i + 1] for i in range(0, len(res), 2)}
    return d


def get_redis_memory_in_mb(env):
    return float(env.cmd('info', 'memory')['used_memory'])/0x100000

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

# Helper function to create numpy array vector with a specific type
def create_np_array_typed(data, data_type='FLOAT32'):
    if data_type == 'FLOAT32':
        return np.array(data, dtype=np.float32)
    if data_type == 'FLOAT64':
        return np.array(data, dtype=np.float64)
    return None

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
            env.assertTrue(True, message='%s ~ %s' % (str(list1), str(list2)))
        return True
    else:
        if _assert:
            env.assertTrue(False, message='%s ~ %s' % (str(list1), str(list2)))
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

def load_vectors_to_redis(env, n_vec, query_vec_index, vec_size, data_type='FLOAT32', ids_offset=0):
    conn = getConnectionByEnv(env)
    np.random.seed(10)
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
