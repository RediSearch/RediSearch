import subprocess
import socket
import tempfile
import redis
import time
import os
import itertools


def get_random_port():
    sock = socket.socket()
    sock.listen(0)
    _, port = sock.getsockname()
    sock.close()

    return port


class DisposableRedis(object):

    def __init__(self, port=None, path='redis-server', **extra_args):
        """
        :param port: port number to start the redis server on. Specify none to automatically generate
        :type port: int|None
        :param extra_args: any extra arguments kwargs will be passed to redis server as --key val
        """

        self._port = port

        # this will hold the actual port the redis is listening on. It's equal to `_port` unless `_port` is None
        # in that case `port` is randomly generated
        self.port = None
        self.extra_args = list(itertools.chain(
            *(('--%s' % k, v) for k, v in extra_args.items())
        ))
        self.path = path

    def __enter__(self):
        if self._port is None:
            self.port = get_random_port()
        else:
            self.port = self._port
        args = [self.path,
                '--port', str(self.port),
                '--dir', tempfile.gettempdir(),
                '--save', ''] + self.extra_args

        self.process = subprocess.Popen(
            args,
            stdin=subprocess.PIPE,
            stdout=open(os.devnull, 'w')
        )

        while True:
            try:
                self.client().ping()
                break
            except redis.ConnectionError:
                self.process.poll()
                if self.process.returncode is not None:
                    raise RuntimeError(
                        "Process has exited with code {}".format(self.process.returncode))
                time.sleep(0.1)

        return self.client()

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.process.terminate()

    def client(self):
        """
        :rtype: redis.StrictRedis
        """

        return redis.StrictRedis(port=self.port)
