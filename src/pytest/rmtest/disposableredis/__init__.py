from contextlib import contextmanager
import subprocess
import socket
import tempfile
import redis
import time
import os
import sys
import itertools


def get_random_port():
    sock = socket.socket()
    sock.listen(0)
    _, port = sock.getsockname()
    sock.close()

    return port


class Client(redis.StrictRedis):

    def __init__(self, disposable_redis, port):

        redis.StrictRedis.__init__(self, port=port)
        self.dr = disposable_redis

    def retry_with_rdb_reload(self):

        yield 1
        self.dr.dump_and_reload()
        yield 2


class DisposableRedis(object):
    """
    Disposable redis loader
    """

    def __init__(self, port=None, path='redis-server', **extra_args):
        """
        :param port: port number to start the redis server on. Specify none to automatically
        generate
        :type port: int|None
        :param extra_args: any extra arguments kwargs will be passed to redis server as --key val
        """

        self.port = port if port is not None else get_random_port()
        self.dumpfile = 'dump.%s.rdb' % self.port


        self.extra_args = list(itertools.chain(
            *(('--%s' % k, v) for k, v in extra_args.items())
        ))
        self.path = path
        self.dumped = False
        self.errored = False
        self.process = None
        self.args = [self.path,
                     '--port', str(self.port),
                     '--dir', tempfile.gettempdir(),
                     '--save', '',
                     '--dbfilename', self.dumpfile] + self.extra_args


    def _start_rocess(self):
        """
        Start the actual redis subprocess
        """

        self.process = subprocess.Popen(
            self.args,
            stdin=subprocess.PIPE,
            stderr=subprocess.PIPE,
            stdout=subprocess.PIPE, #open(os.devnull, 'w')
        )

        while True:
            try:
                self.client().ping()
                break
            except redis.ConnectionError:
                self.process.poll()
                if self.process.returncode is not None:
                    #print "Error:" self.process.communicate()[1]
                    raise RuntimeError(
                        "Process has exited with code {}\n. Redis output: {}"
                        .format(self.process.returncode, self.process.stdout.read()))
                time.sleep(0.1)

    def __enter__(self):
        self._start_rocess()
        return self.client()

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.process.terminate()
        if exc_val or self.errored:
            sys.stderr.write("Redis output: {}\n".format(self.process.stdout.read()))
        if self.dumped:
            try:
                os.unlink(os.path.join(tempfile.gettempdir(), self.dumpfile))
            except OSError:
                pass

    def dump_and_reload(self):
        """
        Dump the rdb and reload it, to test for serialization errors
        """

        conn = self.client()
        conn.save()
        self.dumped = True
        try:
            conn.execute_command('DEBUG', 'RELOAD')
        except redis.RedisError as err:
            self.errored = True
            raise err

    def client(self):
        """
        :rtype: redis.StrictRedis
        """

        return Client(self, self.port)
