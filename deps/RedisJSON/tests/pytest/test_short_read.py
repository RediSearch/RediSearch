# encoding=utf-8
import json
import os
import random
import subprocess
import tempfile
import zipfile
from itertools import chain

import gevent.queue
import gevent.server
import gevent.socket
import time
from RLTest import Defaults
from enum import Enum, auto
from common import TimeLimit


Defaults.decode_responses = True
Defaults.no_log = True

CREATE_INDICES_TARGET_DIR = '/tmp/test'
BASE_RDBS_URL = 'https://dev.cto.redis.s3.amazonaws.com/RediSearch/rdbs/'

SHORT_READ_BYTES_DELTA = int(os.getenv('SHORT_READ_BYTES_DELTA', '1'))
OS = os.getenv('OS')

RDBS = ['short-reads/rejson_keys_2.0.0.rdb.zip']

# LOCALHOST = 'localhost'
LOCALHOST = '127.0.0.1'
# LOCALHOST = '[::1]'


def unzip(zip_path, to_dir):
    if not zipfile.is_zipfile(zip_path):
        return False
    with zipfile.ZipFile(zip_path, 'r') as db_zip:
        for info in db_zip.infolist():
            if not os.path.exists(db_zip.extract(info, to_dir)):
                return False
    return True


def downloadFiles(target_dir):
    for f in RDBS:
        path = os.path.join(target_dir, f)
        path_dir = os.path.dirname(path)
        if not os.path.exists(path_dir):
            os.makedirs(path_dir)
        if not os.path.exists(path):
            subprocess.call(['wget', '--no-check-certificate', '-q', BASE_RDBS_URL + f, '-O', path])
            _, ext = os.path.splitext(f)
            if ext == '.zip':
                if not unzip(path, path_dir):
                    return False
            else:
                if not os.path.exists(path) or os.path.getsize(path) == 0:
                    return False
    return True


def rand_name(k):
    # rand alphabetic string with between 2 to k chars
    return ''.join([chr(random.randint(ord('a'), ord('z'))) for _ in range(0, random.randint(2, max(2, k)))])
    # return random.choices(''.join(string.ascii_letters), k=k)


def rand_num(k):
    # rand positive number with between 2 to k digits
    return chr(random.randint(ord('1'), ord('9'))) + ''.join(
        [chr(random.randint(ord('0'), ord('9'))) for _ in range(0, random.randint(1, max(1, k)))])
    # return random.choices(''.join(string.digits, k=k))


def rand_bool():
    return False if not random.randint(0, 1) else True


def create_keys(env, rdbFileName):
    env.flush()
    add_keys(env, 40, 'content:key')

    # Save the rdb
    env.assertOk(env.cmd('config', 'set', 'dbfilename', rdbFileName))
    dbFileName = env.cmd('config', 'get', 'dbfilename')[1]
    dbDir = env.cmd('config', 'get', 'dir')[1]
    dbFilePath = os.path.join(dbDir, dbFileName)

    env.assertTrue(env.cmd('save'))
    # Copy to avoid truncation of rdb due to RLTest flush and save
    dbCopyFilePath = os.path.join(CREATE_INDICES_TARGET_DIR, dbFileName)
    dbCopyFileDir = os.path.dirname(dbCopyFilePath)
    if not os.path.exists(dbCopyFileDir):
        os.makedirs(dbCopyFileDir)
    with zipfile.ZipFile(dbCopyFilePath + '.zip', 'w') as db_zip:
        db_zip.write(dbFilePath, os.path.join(os.path.curdir, os.path.basename(dbCopyFilePath)))


def get_identifier(name, isHash):
    return '$.' + name if not isHash else name


def add_keys(env, num_keys, prefix):
    for i in range(1, num_keys + 1):
        json_val = random_json_value(0, random.randint(0, 5))
        cmd = ['json.set', prefix + ':' + str(i), '$', json.dumps(json_val)]
        env.assertCmdOk(*cmd)


class JSON_Value_Kind(Enum):
    FIRST = auto()
    NULL = FIRST
    INT = auto()
    FLOAT = auto()
    STRING = auto()
    BOOL = auto()
    ARRAY = auto()
    OBJECT = auto()
    LAST = OBJECT


# class JSON_String_Value_Kind(Enum):
#     FIRST = auto()
#     NONE_ESCAPED = FIRST
#     QUOTE = auto()
#     REVERSE_SOLIDUS = auto()
#     SOLIDUS = auto()
#     BACKSPACE = auto()
#     FORMFEED = auto()
#     LINEFEED = auto()
#     CR = auto()
#     HORIZ_TAB = auto()
#     HEX = auto()
#     LAST = HEX


def random_json_value(nesting_level, max_nesting_level):
    # Favor a top-level object/array
    if nesting_level == 0 and nesting_level != max_nesting_level and not random.randint(0, 5):
        kind = JSON_Value_Kind(random.randint(JSON_Value_Kind.ARRAY.value, JSON_Value_Kind.OBJECT.value))
    else:
        kind = JSON_Value_Kind(random.randint(JSON_Value_Kind.FIRST.value, JSON_Value_Kind.LAST.value))

    if kind == JSON_Value_Kind.NULL:
        # null
        return None
    elif kind == JSON_Value_Kind.INT:
        # int
        return int(rand_num(3))
    elif kind == JSON_Value_Kind.FLOAT:
        # float
        return float(rand_num(3) + '.' + rand_num(2))
    elif kind == JSON_Value_Kind.STRING:
        # string
        # TODO: Allow also escaped sequenced
        return rand_name(6)
    elif kind == JSON_Value_Kind.BOOL:
        # bool
        return rand_bool()
    elif kind == JSON_Value_Kind.ARRAY:
        # array
        arr = []
        count = 0 if nesting_level == max_nesting_level else random.randint(1, 10)
        for i in range(0, count):
            arr.append(random_json_value(nesting_level + 1, max_nesting_level))
        return arr
    elif kind == JSON_Value_Kind.OBJECT:
        # object
        obj = {}
        count = 0 if nesting_level == max_nesting_level else random.randint(1, 10)
        for i in range(0, count):
            obj[rand_name(4)] = random_json_value(nesting_level + 1, max_nesting_level)
        return obj
    return None


def testCreateKeysRdbFile(env):
    env.skipOnVersionSmaller('6.2')  # Another alternative is to set env var SHORT_READ_BYTES_DELTA to be greater than 1 (in redis 6.0)
    if os.environ.get('CI'):
        env.skip()
    create_keys(env, 'rejson_keys_2.0.0.rdb')


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
        return self.decoder(self.sockf.read(bytes))

    def read(self, size):
        return self.decoder(self.sockf.read(size))

    def read_at_most(self, bytes, timeout=0.01):
        self.sock.settimeout(timeout)
        return self.decoder(self.sock.recv(bytes))

    def send(self, data):
        self.sockf.write(self.encoder(data))
        self.sockf.flush()

    def encoder(self, value):
        if isinstance(value, str):
            return value.encode('utf-8')
        else:
            return value

    def decoder(self, value):
        if isinstance(value, bytes):
            return value.decode('utf-8')
        else:
            return value

    def readline(self):
        return self.decoder(self.sockf.readline())

    def send_bulk(self, data):
        data = self.encoder(data)
        binary_data = b'$%d\r\n%s\r\n' % (len(data), data)
        self.sockf.write(binary_data)
        self.sockf.flush()

    def send_status(self, data):
        binary_data = b'+%s\r\n' % self.encoder(data)
        self.sockf.write(binary_data)
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
            data = self.read(bulk_len + 2)
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


class ShardMock:
    def __init__(self, env):
        self.env = env
        self.new_conns = gevent.queue.Queue()

    def _handle_conn(self, sock, client_addr):
        conn = Connection(sock)
        self.new_conns.put(conn)

    def __enter__(self):
        self.server_port = None
        if not self.StartListening(port=0, attempts=10) and not self.StartListening(port=random.randint(55000, 57000)):
            raise Exception("%s StartListening failed" % self.__class__.__name__)
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
        self.server_port = None

    def StartListening(self, port, attempts=1):
        for i in range(1, attempts + 1):
            self.stream_server = gevent.server.StreamServer((LOCALHOST, port), self._handle_conn)
            try:
                self.stream_server.start()
            except Exception as e:
                self.env.assertEqual(self.server_port, None, message='%s: StartListening(%d/%d) %d -> %s' % (
                    self.__class__.__name__, i, attempts, port, e.strerror))
                continue
            self.server_port = self.stream_server.address[1]
            self.env.assertNotEqual(self.server_port, None, message='%s: StartListening(%d/%d) %d -> %d' % (
                self.__class__.__name__, i, attempts, port, self.server_port))
            return True
        else:
            return False


class Debug:
    def __init__(self, enabled=False):
        self.enabled = enabled
        self.clear()

    def clear(self):
        self.dbg_str = ''
        self.dbg_ndx = -1

    def __call__(self, f):
        if self.enabled:
            def f_with_debug(*args, **kwds):
                self.print_bytes_incremental(args[0], args[1], args[2], f.__name__)
                if len(args[1]) == args[2]:
                    self.clear()
                f(*args, **kwds)

            return f_with_debug
        else:
            return f

    def print_bytes_incremental(self, env, data, total_len, name):
        # For debugging: print the binary content before it is sent
        byte_count_width = len(str(total_len))
        if len(data):
            ch = data[self.dbg_ndx]
            printable_ch = ch
            if ch < 32 or ch == 127:
                printable_ch = '\\?'
            else:
                printable_ch = chr(printable_ch)
        else:
            ch = '\0'
            printable_ch = '\\!'  # no data (zero length)
        self.dbg_str = '{} {:=0{}n}:{:<2}({:<3})'.format(self.dbg_str, self.dbg_ndx, byte_count_width, printable_ch, ch)
        if not (self.dbg_ndx + 1) % 10:
            self.dbg_str = self.dbg_str + "\n"
        self.dbg_ndx = self.dbg_ndx + 1

        env.debugPrint(name + ': %d out of %d \n%s' % (self.dbg_ndx, total_len, self.dbg_str))


def testShortReadJson(env):
    env.skipOnVersionSmaller('6.2')  # Another alternative is to set env var SHORT_READ_BYTES_DELTA to be greater than 1 (in redis 6.0)
    env.skipOnCluster()
    if env.env.endswith('existing-env') and os.environ.get('CI'):
        env.skip()

    if env.useAof or env.useSlaves:
        env.skip()

    if OS == 'macos':
        env.skip()

    env.envRunner.setTerminateRetries(retries=3, seconds=2)

    seed = str(time.time())
    env.assertNotEqual(seed, None, message='random seed ' + seed)
    random.seed(seed)

    with tempfile.TemporaryDirectory(prefix="short-read_") as temp_dir:
        if not downloadFiles(temp_dir):
            env.assertTrue(False, message="downloadFiles failed")

        for f in RDBS:
            name, ext = os.path.splitext(f)
            if ext == '.zip':
                f = name
            fullfilePath = os.path.join(temp_dir, f)
            env.assertNotEqual(fullfilePath, None, message='testShortReadJson')
            sendShortReads(env, fullfilePath)


def sendShortReads(env, rdb_file):
    # Add some initial content (keys) to test backup/restore/discard when short read fails
    env.assertCmdOk('replicaof', 'no', 'one')
    env.flush()
    add_keys(env, 10, 'backup:key')
    env.assertIsNotNone(env.cmd('json.get', 'backup:key:1', '$'))
    env.assertIsNotNone(env.cmd('json.get', 'backup:key:10', '$'))

    with open(rdb_file, mode='rb') as f:
        full_rdb = f.read()
    total_len = len(full_rdb)

    env.assertGreater(total_len, SHORT_READ_BYTES_DELTA)
    r = range(0, total_len + 1, SHORT_READ_BYTES_DELTA)
    if (total_len % SHORT_READ_BYTES_DELTA) != 0:
        r = chain(r,range(total_len, total_len + 1))

    try:
        for b in r:
            rdb = full_rdb[0:b]
            runShortRead(env, rdb, total_len)
    except:
        pass


@Debug(False)
def runShortRead(env, data, total_len):
    with ShardMock(env) as shardMock:

        # For debugging: if adding breakpoints in redis,
        # In order to avoid closing the connection, uncomment the following line
        # res = env.cmd('CONFIG', 'SET', 'timeout', '0')

        # Notice: Do not use env.expect in this test
        # (since it is sending commands to redis and in this test we need to follow strict hand-shaking)
        res = env.cmd('CONFIG', 'SET', 'repl-diskless-load', 'swapdb')
        env.assertTrue(res)
        res = env.cmd('replicaof', LOCALHOST, shardMock.server_port)
        env.assertTrue(res)
        conn = None
        try:
            conn = shardMock.GetConnection()
        except:
            # Avoid hang if connection cannot be established
            env.assertCmdOk('replicaof', 'no', 'one')
        if conn is None:
            env.assertTrue(False, message="Cannot connect to server")
            raise Exception("Cannot connect to server")

        # Perform hand-shake with replica
        res = conn.read_request()
        env.assertEqual(res, ['PING'])
        conn.send_status('PONG')

        max_attempt = 100
        res = conn.read_request()
        while max_attempt > 0:
            if res[0] == 'REPLCONF':
                conn.send_status('OK')
            else:
                break
            max_attempt = max_attempt - 1
            res = conn.read_request()

        # Send RDB to replica
        some_guid = 'c43cf134fd16468b1e26a3d000f2053fef1c5f8c'
        conn.send_status('FULLRESYNC ' + some_guid + ' 0')
        is_shortread = total_len != len(data)
        if is_shortread:
            # Send without the trailing '\r\n' (send data not according to RESP protocol)
            binary_data = b'$%d\r\n%s' % (total_len, data)
            conn.send(binary_data)
        else:
            # Allow to succeed with a full read (send data according to RESP protocol)
            conn.send_bulk(data)
        conn.flush()

        # Close during replica is waiting for more RDB data (so replica will re-connect to master)
        conn.close()

        # Make sure replica did not crash
        res = env.cmd('PING')
        env.assertEqual(res, True)
        conn = shardMock.GetConnection(timeout=3)
        env.assertNotEqual(conn, None)

        if is_shortread:
            # Verify original data, that existed before the failed attempt to short-read, is restored
            env.assertIsNotNone(env.cmd('json.get', 'backup:key:1', '$'))
            env.assertIsNotNone(env.cmd('json.get', 'backup:key:10', '$'))

            env.assertIsNone(env.cmd('json.get', 'content:key:1', '$'))
            env.assertIsNone(env.cmd('json.get', 'content:key:2', '$'))
            env.assertIsNone(env.cmd('json.get', 'content:key:39', '$'))
            env.assertIsNone(env.cmd('json.get', 'content:key:40', '$'))
        else:
            # Verify new data was loaded and the backup was discarded
            # TODO: How to verify internal backup was indeed discarded
            env.assertIsNotNone(env.cmd('json.get', 'content:key:1', '$'))
            env.assertIsNotNone(env.cmd('json.get', 'content:key:2', '$'))
            env.assertIsNotNone(env.cmd('json.get', 'content:key:39', '$'))
            env.assertIsNotNone(env.cmd('json.get', 'content:key:40', '$'))

            env.assertIsNone(env.cmd('json.get', 'backup:key:1', '$'))
            env.assertIsNone(env.cmd('json.get', 'backup:key:10', '$'))

        # Exit (avoid read-only exception with flush on replica)
        env.assertCmdOk('replicaof', 'no', 'one')
