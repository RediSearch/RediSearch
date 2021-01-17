from collections import Iterable
import time
from packaging import version

def getConnectionByEnv(env):
    conn = None
    if env.env == 'oss-cluster':
        conn = env.envRunner.getClusterConnection()
    else:
        conn = env.getConnection()
    return conn

def waitForIndex(env, idx):
    while True:
        res = env.execute_command('ft.info', idx)
        if int(res[res.index('indexing') + 1]) == 0:
            break
        time.sleep(0.1)

def toSortedFlatList(res):
    if isinstance(res, str):
        return [res]    
    if isinstance(res, Iterable):
        finalList = []
        for e in res:
            finalList += toSortedFlatList(e)
        return sorted(finalList)
    return [res]

def assertInfoField(env, idx, field, expected):
    if not env.isCluster():
        res = env.cmd('ft.info', idx)
        d = {res[i]: res[i + 1] for i in range(0, len(res), 2)}
        env.assertEqual(d[field], expected)

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

    data = sorted(data)
    res = [n] + [item for sublist in data for item in sublist]
    return res

module_v = 0
def check_module_version(env, version):
    global module_v
    if module_v == 0:
        module_v = env.execute_command('MODULE LIST')[0][3]
    if int(module_v) >= int(version):
        return True
    return False

server_v = 0
def check_server_version(env, ver):
    global server_v
    if server_v == 0:
        server_v = env.execute_command('INFO')['redis_version']
    u = version.parse(server_v) 
    if version.parse(server_v) >= version.parse(ver):
        return True
    return False
