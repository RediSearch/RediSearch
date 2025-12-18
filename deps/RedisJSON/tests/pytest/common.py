import signal
from contextlib import contextmanager
from functools import wraps
from includes import *
from packaging import version
from unittest import SkipTest
from RLTest import Env
import inspect

@contextmanager
def TimeLimit(timeout):
    def handler(signum, frame):
        raise Exception('TimeLimit timeout')

    signal.signal(signal.SIGALRM, handler)
    signal.setitimer(signal.ITIMER_REAL, timeout, 0)
    try:
        yield
    finally:
        signal.setitimer(signal.ITIMER_REAL, 0)
        signal.signal(signal.SIGALRM, signal.SIG_DFL)

def skipOnExistingEnv(env):
    if 'existing' in env.env:
        env.skip()

def skipOnCrdtEnv(env):
    if len([a for a in env.cmd('module', 'list') if a[1] == 'crdt']) > 0:
        env.skip()

def skip(f, on_cluster=False):
    @wraps(f)
    def wrapper(env, *args, **kwargs):
        if not on_cluster or env.isCluster():
            env.skip()
            return
        return f(env, *args, **kwargs)
    return wrapper

def no_san(f):
    @wraps(f)
    def wrapper(env, *args, **kwargs):
        if SANITIZER != '':
            fname = f.__name__
            env.debugPrint("skipping {} due to sanitizer".format(fname), force=True)
            env.skip()
            return
        return f(env, *args, **kwargs)
    return wrapper

def skip_redis_less_than(redis_less_than=None):
    def decorate(f):
        def wrapper():
            if redis_less_than and server_version_is_less_than(redis_less_than):
                raise SkipTest()
            if len(inspect.signature(f).parameters) > 0:
                env = Env()
                return f(env)
            else:
                return f()
        return wrapper
    return decorate


server_ver = None
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
