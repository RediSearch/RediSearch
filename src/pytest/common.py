
import time

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

def sortedResults(res):
    n = res[0]
    res = res[1:]

    y = []
    data = []
    i = 0
    for x in res:
        y.append(x)
        if len(y) == 2:
            data.append(y)
            y = []

    data = sorted(data)
    res = [n] + [item for sublist in data for item in sublist]
    return res
