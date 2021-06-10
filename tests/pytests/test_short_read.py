import gevent.server
import gevent.queue
import gevent.socket
import signal
import time
from RLTest import Env
from common import TimeLimit
from includes import *


class Connection(object):
    def __init__(self, sock, bufsize=4096, underlying_sock=None):
        self.sock = sock
        self.sockf = sock.makefile('rwb', bufsize)
        self.closed = False
        self.peer_closed = False
        self.underlying_sock = underlying_sock

    def close(self):
        if not self.closed:
            self.closed = True
            self.sockf.close()
            self.sock.close()
            self.sockf = None

    def is_close(self, timeout=2):
        if self.closed:
            return True
        try:
            with TimeLimit(timeout):
                return self.read(1) == ''
        except Exception:
            return False

    def flush(self):
        self.sockf.flush()

    def get_address(self):
        return self.sock.getsockname()[0]

    def get_port(self):
        return self.sock.getsockname()[1]

    def read(self, bytes):
        return self.sockf.read(bytes)

    def read_at_most(self, bytes, timeout=0.01):
        self.sock.settimeout(timeout)
        return self.sock.recv(bytes)

    def send(self, data):
        self.sockf.write(data)
        self.sockf.flush()

    def readline(self):
        return self.sockf.readline()

    def send_bulk_header(self, data_len):
        self.sockf.write('$%d\r\n' % data_len)
        self.sockf.flush()

    def send_bulk(self, data):
        self.sockf.write('$%d\r\n%s\r\n' % (len(data), data))
        self.sockf.flush()

    def send_status(self, data):
        self.sockf.write('+%s\r\n' % data)
        self.sockf.flush()

    def send_error(self, data):
        self.sockf.write('-%s\r\n' % data)
        self.sockf.flush()

    def send_integer(self, data):
        self.sockf.write(':%u\r\n' % data)
        self.sockf.flush()

    def send_mbulk(self, data):
        self.sockf.write('*%d\r\n' % len(data))
        for elem in data:
            self.sockf.write('$%d\r\n%s\r\n' % (len(elem), elem))
        self.sockf.flush()

    def read_mbulk(self, args_count=None):
        if args_count is None:
            line = self.readline()
            if not line:
                self.peer_closed = True
            if not line or line[0] != '*':
                self.close()
                return None
            try:
                args_count = int(line[1:])
            except ValueError:
                raise Exception('Invalid mbulk header: %s' % line)
        data = []
        for arg in range(args_count):
            data.append(self.read_response())
        return data

    def read_request(self):
        line = self.readline()
        if not line:
            self.peer_closed = True
            self.close()
            return None
        if line[0] != '*':
            return line.rstrip().split()
        try:
            args_count = int(line[1:])
        except ValueError:
            raise Exception('Invalid mbulk request: %s' % line)
        return self.read_mbulk(args_count)

    def read_request_and_reply_status(self, status):
        req = self.read_request()
        if not req:
            return
        self.current_request = req
        self.send_status(status)

    def wait_until_writable(self, timeout=None):
        try:
            gevent.socket.wait_write(self.sockf.fileno(), timeout)
        except gevent.socket.error:
            return False
        return True

    def wait_until_readable(self, timeout=None):
        if self.closed:
            return False
        try:
            gevent.socket.wait_read(self.sockf.fileno(), timeout)
        except gevent.socket.error:
            return False
        return True

    def read_response(self):
        line = self.readline()
        if not line:
            self.peer_closed = True
            self.close()
            return None
        if line[0] == '+':
            return line.rstrip()
        elif line[0] == ':':
            try:
                return int(line[1:])
            except ValueError:
                raise Exception('Invalid numeric value: %s' % line)
        elif line[0] == '-':
            return line.rstrip()
        elif line[0] == '$':
            try:
                bulk_len = int(line[1:])
            except ValueError:
                raise Exception('Invalid bulk response: %s' % line)
            if bulk_len == -1:
                return None
            data = self.sockf.read(bulk_len + 2)
            if len(data) < bulk_len:
                self.peer_closed = True
                self.close()
            return data[:bulk_len]
        elif line[0] == '*':
            try:
                args_count = int(line[1:])
            except ValueError:
                raise Exception('Invalid mbulk response: %s' % line)
            return self.read_mbulk(args_count)
        else:
            raise Exception('Invalid response: %s' % line)


class ShardMock():
    def __init__(self, env):
        self.env = env
        self.new_conns = gevent.queue.Queue()

    def _handle_conn(self, sock, client_addr):
        conn = Connection(sock)
        self.new_conns.put(conn)

    def __enter__(self):
        self.stream_server = gevent.server.StreamServer(('localhost', 10000), self._handle_conn)
        self.stream_server.start()
        return self

    def __exit__(self, type, value, traceback):
        self.stream_server.stop()

    def GetConnection(self, timeout=None):
        conn = self.new_conns.get(block=True, timeout=timeout)
        return conn

    def GetCleanConnection(self):
        return self.new_conns.get(block=True, timeout=None)

    def StopListening(self):
        self.stream_server.stop()

    def StartListening(self):
        self.stream_server = gevent.server.StreamServer(('localhost', 10000), self._handle_conn)
        self.stream_server.start()


def testShortRead(env):
    env.skipOnCluster()
    with open("short_read.rdb_full", mode='rb') as f:
        full_rdb = f.read()
    total_len = len(full_rdb)
    for b in range(0, total_len + 1):
        rdb = full_rdb[0:b]
        runShortRead(env, rdb, total_len)

def runShortRead(env, data, total_len):
    env.skipOnCluster()
    with ShardMock(env) as shardMock:
        print('runShortRead: %d out of %d \n' % (len(data), total_len)) # TODO: remove print?

        # Notice: Do not use env.expect in this test
        # (since it is sending commands to redis and in this test we need to follow strict hand-shaking)
        res = env.cmd('slaveof', 'localhost', '10000')
        env.expect(res).true()
        conn = shardMock.GetConnection()
        # Perform hand-shake with slave

        res = conn.read_request();
        env.assertEqual(res, ['PING'])

        conn.send_status('PONG')
        max_attempt = 100
        while max_attempt > 0:
            if res[0] == 'REPLCONF':
                conn.send_status('OK')
            else:
                break
            max_attempt = max_attempt - 1
            res = conn.read_request()

        # Send RDB to slave
        guid = 'af4e30b5d14dce9f96fbb7769d0ec794cdc0bbcc'
        conn.send_status('FULLRESYNC ' + guid + ' 0')
        conn.send('$%d\r\n%s\r\n' % (total_len, data))

        if total_len != len(data):
            # Close during slave is waiting for more RDB data
            conn.close()
        # Make sure slave did not crash
        res = env.cmd('PING')
        env.assertEqual(res, True)
        conn = shardMock.GetConnection(timeout=3)
        env.assertNotEqual(conn, None)

        # Exit (avoid read-only exception with flush on slave)
        env.assertEqual(env.cmd('slaveof', 'no', 'one'), True)

