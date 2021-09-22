# coding=utf-8
import collections
import os
import random
import re
import shutil
import subprocess
import tempfile
import zipfile

import gevent.queue
import gevent.server
import gevent.socket
import time

from common import TimeLimit
from common import waitForIndex

CREATE_INDICES_TARGET_DIR = '/tmp/test'
BASE_RDBS_URL = 'https://s3.amazonaws.com/redismodules/redisearch-enterprise/rdbs/'

IS_SANITIZER = int(os.getenv('SANITIZER', '0'))
IS_CODE_COVERAGE = int(os.getenv('CODE_COVERAGE', '0'))
SHORT_READ_BYTES_DELTA = int(os.getenv('SHORT_READ_BYTES_DELTA', '1'))
IS_SHORT_READ_FULL_TEST = int(os.getenv('SHORT_READ_FULL_TEST', '0'))
OS = os.getenv('OS')

RDBS_SHORT_READS = [
    'short-reads/redisearch_2.2.0.rdb.zip',
    'short-reads/rejson_2.0.0.rdb.zip',
    'short-reads/redisearch_2.2.0_rejson_2.0.0.rdb.zip',
]
RDBS_COMPATIBILITY = [
    'redisearch_2.0.9.rdb',
]

ExpectedIndex = collections.namedtuple('ExpectedIndex', ['count', 'pattern', 'search_result_count'])
RDBS_EXPECTED_INDICES = [
                         ExpectedIndex(2, 'shortread_idxSearch_[1-9]', [20, 55]),
                         ExpectedIndex(2, 'shortread_idxJson_[1-9]', [55, 20]),  # TODO: why order of indices is first _2 then _1
                         ExpectedIndex(2, 'shortread_idxSearchJson_[1-9]', [10, 35])
                         ]

RDBS = []
RDBS.extend(RDBS_SHORT_READS)
if (not IS_CODE_COVERAGE) and (not IS_SANITIZER) and IS_SHORT_READ_FULL_TEST:
    RDBS.extend(RDBS_COMPATIBILITY)
    RDBS_EXPECTED_INDICES.append(ExpectedIndex(1, 'idx', [1000]))


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
            subprocess.call(['wget', '-q', BASE_RDBS_URL + f, '-O', path])
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
    return chr(random.randint(ord('1'), ord('9'))) + ''.join([chr(random.randint(ord('0'), ord('9'))) for _ in range(0, random.randint(1, max(1, k)))])
    # return random.choices(''.join(string.digits, k=k))


def create_indices(env, rdbFileName, idxNameStem, isHash, isJson):
    env.flush()
    idxNameStem = 'shortread_' + idxNameStem + '_'
    if isHash and isJson:
        # 1 Hash index and 1 Json index
        add_index(env, True, idxNameStem + '1', 'c', 10, 20)
        add_index(env, False, idxNameStem + '2', 'd', 35, 40)
    elif isHash:
        # 2 Hash indices
        add_index(env, True, idxNameStem + '1', 'e', 10, 20)
        add_index(env, True, idxNameStem + '2', 'f', 35, 40)
    elif isJson:
        # 2 Json indices
        add_index(env, False, idxNameStem + '1', 'g', 10, 20)
        add_index(env, False, idxNameStem + '2', 'h', 35, 40)
    else:
        env.assertTrue(False, "should not reach here")

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


def add_index(env, isHash, index_name, key_suffix, num_prefs, num_keys):
    ''' Cover most of the possible options of an index

    FT.CREATE {index}
    [ON {structure}]
    [PREFIX {count} {prefix} [{prefix} ..]
    [FILTER {filter}]
    [LANGUAGE {default_lang}]
    [LANGUAGE_FIELD {lang_field}]
    [SCORE {default_score}]
    [SCORE_FIELD {score_field}]
    [PAYLOAD_FIELD {payload_field}]
    [MAXTEXTFIELDS] [TEMPORARY {seconds}] [NOOFFSETS] [NOHL] [NOFIELDS] [NOFREQS] [SKIPINITIALSCAN]
    [STOPWORDS {num} {stopword} ...]
    SCHEMA {field} [TEXT [NOSTEM] [WEIGHT {weight}] [PHONETIC {matcher}] | NUMERIC | GEO | TAG [SEPARATOR {sep}] ] [SORTABLE][NOINDEX] ...
    '''

    # Create the index
    cmd_create = ['ft.create', index_name, 'ON', 'HASH' if isHash else 'JSON']
    # With prefixes
    if num_prefs > 0:
        cmd_create.extend(['prefix', num_prefs])
        for i in range(1, num_prefs + 1):
            cmd_create.append('pref' + str(i) + ":")
    # With filter
    cmd_create.extend(['filter', 'startswith(@__key' + ', "p")'])
    # With language
    cmd_create.extend(['language', 'spanish'])
    # With language field
    cmd_create.extend(['language_field', 'myLang'])
    # With score field
    cmd_create.extend(['score_field', 'myScore'])
    # With payload field
    cmd_create.extend(['payload_field', 'myPayload'])
    # With maxtextfields
    cmd_create.append('maxtextfields')
    # With stopwords
    cmd_create.extend(['stopwords', 3, 'stop', 'being', 'silly'])
    # With schema
    cmd_create.extend(['schema',
                       get_identifier('field1', isHash), 'as', 'f1', 'text', 'nostem', 'weight', '0.2', 'phonetic',
                       'dm:es', 'sortable',
                       get_identifier('field2', isHash), 'as', 'f2', 'numeric', 'sortable',
                       get_identifier('field3', isHash), 'as', 'f3', 'geo',
                       get_identifier('field4', isHash), 'as', 'f4', 'tag', 'separator', ';',
                       get_identifier('field11', isHash), 'text', 'nostem',
                       get_identifier('field12', isHash), 'numeric', 'noindex',
                       get_identifier('field13', isHash), 'geo',
                       get_identifier('field14', isHash), 'tag', 'noindex',
                       get_identifier('myLang', isHash), 'text',
                       get_identifier('myScore', isHash), 'numeric',
                       ])
    env.assertOk(env.cmd(*cmd_create))
    waitForIndex(env, index_name)
    env.assertOk(env.cmd('ft.synupdate', index_name, 'syngrp1', 'pelota', 'bola', 'balón'))
    env.assertOk(env.cmd('ft.synupdate', index_name, 'syngrp2', 'jugar', 'tocar'))

    # Add keys
    for i in range(1, num_keys + 1):
        if isHash:
            cmd = ['hset', 'pref' + str(i) + ":k" + str(i) + '_' + rand_num(5) + key_suffix, 'a' + rand_name(5), rand_num(2), 'b' + rand_name(5), rand_num(3)]
            env.assertEqual(env.cmd(*cmd), 2L)
        else:
            cmd = ['json.set', 'pref' + str(i) + ":k" + str(i) + '_' + rand_num(5) + key_suffix, '$', r'{"field1":"' + rand_name(5) + r'", "field2":' + rand_num(3) + r'}']
            env.assertOk(env.cmd(*cmd))


def testCreateIndexRdbFiles(env):
    if os.environ.get('CI'):
        env.skip()
    create_indices(env, 'redisearch_2.2.0.rdb', 'idxSearch', True, False)
    create_indices(env, 'rejson_2.0.0.rdb', 'idxJson', False, True)
    create_indices(env, 'redisearch_2.2.0_rejson_2.0.0.rdb', 'idxSearchJson', True, True)


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
            self.stream_server = gevent.server.StreamServer(('localhost', port), self._handle_conn)
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
            if ord(ch) < 32 or ord(ch) == 127:
                printable_ch = '\?'
        else:
            ch = '\0'
            printable_ch = '\!'  # no data (zero length)
        self.dbg_str = '{} {:=0{}n}:{:<2}({:<3})'.format(self.dbg_str, self.dbg_ndx, byte_count_width, printable_ch, ord(ch))
        if not (self.dbg_ndx + 1) % 10:
            self.dbg_str = self.dbg_str + "\n"
        self.dbg_ndx = self.dbg_ndx + 1

        env.debugPrint(name + ': %d out of %d \n%s' % (self.dbg_ndx, total_len, self.dbg_str))


def testShortReadSearch(env):

    if IS_CODE_COVERAGE:
        env.skip()  # FIXME: enable coverage test

    env.skipOnCluster()
    if env.env.endswith('existing-env') and os.environ.get('CI'):
        env.skip()

    if OS == 'macos':
        env.skip()

    seed = str(time.time())
    env.assertNotEqual(seed, None, message='random seed ' + seed)
    random.seed(seed)

    try:
        temp_dir = tempfile.mkdtemp(prefix="short-read_")
        # TODO: In python3 use "with tempfile.TemporaryDirectory()"
        if not downloadFiles(temp_dir):
            env.assertTrue(False, "downloadFiles failed")

        for f, expected_index in zip(RDBS, RDBS_EXPECTED_INDICES):
            name, ext = os.path.splitext(f)
            if ext == '.zip':
                f = name
            fullfilePath = os.path.join(temp_dir, f)
            env.assertNotEqual(fullfilePath, None, message='testShortReadSearch')
            sendShortReads(env, fullfilePath, expected_index)
    finally:
        shutil.rmtree(temp_dir)



def sendShortReads(env, rdb_file, expected_index):
    # Add some initial content (index+keys) to test backup/restore/discard when short read fails
    # When entire rdb is successfully sent and loaded (from swapdb) - backup should be discarded
    env.assertCmdOk('replicaof', 'no', 'one')
    env.flush()
    add_index(env, True,  'idxBackup1', 'a', 5, 10)
    add_index(env, False, 'idxBackup2', 'b', 5, 10)

    res = env.cmd('ft.search ', 'idxBackup1', '*', 'limit', '0', '0')
    env.assertEqual(res[0], 5L)
    res = env.cmd('ft.search ', 'idxBackup2', '*', 'limit', '0', '0')
    env.assertEqual(res[0], 5L)

    with open(rdb_file, mode='rb') as f:
        full_rdb = f.read()
    total_len = len(full_rdb)

    env.assertGreater(total_len, SHORT_READ_BYTES_DELTA)
    r = range(0, total_len + 1, SHORT_READ_BYTES_DELTA)
    if (total_len % SHORT_READ_BYTES_DELTA) != 0:
        r = r + range(total_len, total_len + 1)
    for b in r:
        rdb = full_rdb[0:b]
        runShortRead(env, rdb, total_len, expected_index)


@Debug(False)
def runShortRead(env, data, total_len, expected_index):
    with ShardMock(env) as shardMock:

        # For debugging: if adding breakpoints in redis,
        # In order to avoid closing the connection, uncomment the following line
        # res = env.cmd('CONFIG', 'SET', 'timeout', '0')

        # Notice: Do not use env.expect in this test
        # (since it is sending commands to redis and in this test we need to follow strict hand-shaking)
        res = env.cmd('CONFIG', 'SET', 'repl-diskless-load', 'swapdb')
        res = env.cmd('replicaof', 'localhost', shardMock.server_port)
        env.assertTrue(res)
        conn = shardMock.GetConnection()
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
        some_guid = 'af4e30b5d14dce9f96fbb7769d0ec794cdc0bbcc'
        conn.send_status('FULLRESYNC ' + some_guid + ' 0')
        is_shortread = total_len != len(data)
        if is_shortread:
            conn.send('$%d\r\n%s' % (total_len, data))
        else:
            # Allow to succeed with a full read (protocol expects a trailing '\r\n')
            conn.send('$%d\r\n%s\r\n' % (total_len, data))
        conn.flush()

        # Close during replica is waiting for more RDB data (so replica will re-connect to master)
        conn.close()

        # Make sure replica did not crash
        res = env.cmd('PING')
        env.assertEqual(res, True)
        conn = shardMock.GetConnection(timeout=3)
        env.assertNotEqual(conn, None)

        res = env.cmd('ft._list')
        if is_shortread:
            # Verify original data, that existed before the failed attempt to short-read, is restored
            env.assertEqual(res, ['idxBackup2', 'idxBackup1'])
            res = env.cmd('ft.search ', 'idxBackup1', '*', 'limit', '0', '0')
            env.assertEqual(res[0], 5L)
            res = env.cmd('ft.search ', 'idxBackup2', '*', 'limit', '0', '0')
            env.assertEqual(res[0], 5L)
        else:
            # Verify new data was loaded and the backup was discarded
            # TODO: How to verify internal backup was indeed discarded
            env.assertEqual(len(res), expected_index.count)
            r = re.compile(expected_index.pattern)
            expected_indices = list(filter(lambda x: r.match(x), res))
            env.assertEqual(len(expected_indices), expected_index.count)
            for ind, expected_result_count in zip(expected_indices, expected_index.search_result_count):
                res = env.cmd('ft.search ', ind, '*', 'limit', '0', '0')
                env.assertEqual(res[0], expected_result_count)

        # Exit (avoid read-only exception with flush on replica)
        env.assertCmdOk('replicaof', 'no', 'one')


