import time

def to_dict(r):
    return {r[i]: r[i + 1] for i in range(0, len(r), 2)}


def index_info(e, idx):
    rv = e.cmd('ft.info', idx)
    return to_dict(rv)


def is_synced(e, idx=None):
    if idx:
        info = index_info(e, idx)
        return info['sync_status'].upper() == 'SYNCED'
    else:
        idxs = e.cmd('ft._list')
        for idx in idxs:
            if not is_synced(e, idx):
                return False
        return True

def wait_sync(e):
    while not is_synced(e):
        time.sleep(0.01)

def restart_and_reload(e):
    e.restart_and_reload()
    wait_sync(e)

def dump_and_reload(e):
    e.dump_and_reload()
    wait_sync(e)


def reloading_iterator(env):
    for x in env.reloading_iterator():
        wait_sync(env)
        yield x
