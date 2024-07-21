# coding=utf-8

import collections
import random
import re
import shutil
import subprocess
import tempfile
import zipfile
from itertools import chain
import tempfile

import gevent.queue
import gevent.server
import gevent.socket

from common import *
from includes import *


SHORT_READ_BYTES_DELTA = int(os.getenv('SHORT_READ_BYTES_DELTA', '1'))
SHORT_READ_FULL_TEST = int(os.getenv('SHORT_READ_FULL_TEST', '0'))

ExpectedIndex = collections.namedtuple('ExpectedIndex', ['count', 'pattern', 'search_result_count'])

RDBS_SHORT_READS = {
    'short-reads/redisearch_2.2.0.rdb.zip'             : ExpectedIndex(2, 'shortread_idxSearch_[1-9]', [20, 55]),
    'short-reads/rejson_2.0.0.rdb.zip'                 : ExpectedIndex(2, 'shortread_idxJson_[1-9]', [20, 55]),
    'short-reads/redisearch_2.2.0_rejson_2.0.0.rdb.zip': ExpectedIndex(2, 'shortread_idxSearchJson_[1-9]', [10, 35]),
    'short-reads/redisearch_2.8.0.rdb.zip'             : ExpectedIndex(2, 'shortread_idxSearch_with_geom_[1-9]', [20, 60]),
    'short-reads/redisearch_2.8.4.rdb.zip'             : ExpectedIndex(2, 'shortread_idxSearch_with_geom_[1-9]', [20, 60]),
    'short-reads/redisearch_2.10.3.rdb.zip'            : ExpectedIndex(2, 'shortread_idxSearch_[1-9]', [10, 35]),
    'short-reads/redisearch_2.10.3_missing.rdb.zip'    : ExpectedIndex(2, 'shortread_idxSearch_[1-9]', [20, 55]),
}
RDBS_COMPATIBILITY = {
    'redisearch_2.0.9.rdb': ExpectedIndex(1, 'idx', [1000]),
}

RDBS = RDBS_SHORT_READS.copy()
if not CODE_COVERAGE and SANITIZER == '' and SHORT_READ_FULL_TEST:
    RDBS.update(RDBS_COMPATIBILITY)

def unzip(zip_path, to_dir):
    if not zipfile.is_zipfile(zip_path):
        return False
    with zipfile.ZipFile(zip_path, 'r') as db_zip:
        for info in db_zip.infolist():
            if not os.path.exists(db_zip.extract(info, to_dir)):
                return False
    return True

def rand_name(k):
    # rand alphabetic string with between 2 to k chars
    return ''.join([chr(random.randint(ord('a'), ord('z'))) for _ in range(0, random.randint(2, max(2, k)))])
    # return random.choices(''.join(string.ascii_letters), k=k)


def rand_num(k):
    # rand positive number with between 2 to k digits
    return chr(random.randint(ord('1'), ord('9'))) + ''.join([chr(random.randint(ord('0'), ord('9'))) for _ in range(0, random.randint(1, max(1, k-1)))])
    # return random.choices(''.join(string.digits, k=k))


def create_indices(env, rdbFileName, idxNameStem, isHash, isJson, num_geometry_keys=0):
    env.flush()
    idxNameStem = 'shortread_' + idxNameStem + '_'
    if isHash and isJson:
        # 1 Hash index and 1 Json index
        add_index(env, True, idxNameStem + '1', 'c', 10, 20, num_geometry_keys)
        add_index(env, False, idxNameStem + '2', 'd', 35, 40, num_geometry_keys)
    elif isHash:
        # 2 Hash indices
        add_index(env, True, idxNameStem + '1', 'e', 10, 20, num_geometry_keys)
        add_index(env, True, idxNameStem + '2', 'f', 35, 40, num_geometry_keys)
    elif isJson:
        # 2 Json indices
        add_index(env, False, idxNameStem + '1', 'g', 10, 20, num_geometry_keys)
        add_index(env, False, idxNameStem + '2', 'h', 35, 40, num_geometry_keys)
    else:
        env.assertTrue(False, "should not reach here")

    # Save the rdb
    env.assertOk(env.cmd('config', 'set', 'dbfilename', rdbFileName))
    dbFileName = env.cmd('config', 'get', 'dbfilename')[1]
    dbDir = env.cmd('config', 'get', 'dir')[1]
    dbFilePath = os.path.join(dbDir, dbFileName)

    env.assertTrue(env.cmd('save'))
    # Copy to avoid truncation of rdb due to RLTest flush and save
    tempdir = tempfile.TemporaryDirectory(prefix='test_')
    dbCopyFilePath = os.path.join(tempdir.name, dbFileName)
    dbCopyFileDir = os.path.dirname(dbCopyFilePath)
    os.makedirs(dbCopyFileDir, exist_ok=True)
    zipFilePath = dbCopyFilePath + '.zip'
    with zipfile.ZipFile(zipFilePath, 'w') as db_zip:
        db_zip.write(dbFilePath, dbFileName)

def get_identifier(name, isHash):
    return '$.' + name if not isHash else name

def get_polygon(x, y, i):
    return 'POLYGON({} {}, {} {}, {} {}, {} {}, {} {})'.format(
        x, y,
        x + 15*i, y,
        x + 15*i, y + 17.2*i,
        x, y + 17.2*i,
        x, y)

def add_index(env, isHash, index_name, key_suffix, num_prefs, num_keys, num_geometry_keys=0):
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
                       get_identifier('field6', isHash), 'as', 'f6', 'tag', 'INDEXEMPTY',
                       get_identifier('field7', isHash), 'as', 'f7', 'tag', 'INDEXEMPTY', 'SORTABLE',
                       get_identifier('field8', isHash), 'as', 'f8', 'TEXT', 'INDEXEMPTY',
                       get_identifier('field9', isHash), 'as', 'f9', 'TEXT', 'INDEXEMPTY', 'SORTABLE',
                       get_identifier('field10', isHash), 'as', 'f10', 'tag', 'INDEXEMPTY', 'SORTABLE',
                       get_identifier('field15', isHash), 'as', 'f15', 'tag', 'INDEXMISSING',
                       get_identifier('field16', isHash), 'as', 'f16', 'tag', 'INDEXMISSING', 'SORTABLE',
                       get_identifier('field17', isHash), 'as', 'f17', 'text', 'INDEXMISSING',
                       get_identifier('field18', isHash), 'as', 'f18', 'text', 'INDEXMISSING', 'SORTABLE',

                       get_identifier('field11', isHash), 'text', 'nostem',
                       get_identifier('field12', isHash), 'numeric', 'noindex',
                       get_identifier('field13', isHash), 'geo',
                       get_identifier('field14', isHash), 'tag', 'noindex',

                       get_identifier('myLang', isHash), 'text',
                       get_identifier('myScore', isHash), 'numeric',
                       ])
    if num_geometry_keys > 0:
        cmd_create.extend([
            get_identifier('field5', isHash), 'as', 'geom', 'geoshape', 'flat',
            get_identifier('field15', isHash), 'geoshape', 'spherical',
        ])

    conn = getConnectionByEnv(env)
    env.expect(*cmd_create).ok()
    waitForIndex(env, index_name)
    env.expect('ft.synupdate', index_name, 'syngrp1', 'pelota', 'bola', 'balón').ok()
    env.expect('ft.synupdate', index_name, 'syngrp2', 'jugar', 'tocar').ok()

    # Add keys
    for i in range(1, num_keys + 1):
        if isHash:
            cmd = ['hset', 'pref' + str(i) + ":k" + str(i) + '_' + rand_num(5) + key_suffix, 'a' + rand_name(5), rand_num(2), 'b' + rand_name(5), rand_num(3), 'field6', '', 'field7', '']
            env.assertEqual(conn.execute_command(*cmd), 4)
        else:
            cmd = ['json.set', 'pref' + str(i) + ":k" + str(i) + '_' + rand_num(5) + key_suffix, '$', r'{"field1":"' + rand_name(5) + r'", "field2":' + rand_num(3) + r', "field6":"", "field7":""}']
            env.assertOk(conn.execute_command(*cmd))

    for i in range(num_keys + 1, num_keys + num_geometry_keys + 1):
        geom_wkt = get_polygon(int(rand_num(3)), int(rand_num(3)), i)
        if isHash:
            cmd = ['hset', 'pref' + str(i) + ":k" + str(i) + '_' + rand_num(5) + key_suffix, 'field5', geom_wkt, 'field15', geom_wkt]
            env.assertEqual(conn.execute_command(*cmd), 2)
        else:
            cmd = ['json.set', 'pref' + str(i) + ":k" + str(i) + '_' + rand_num(5) + key_suffix, '$', r'{"field5":"' + geom_wkt + r'", "field15":"' + geom_wkt + r'"}']
            env.assertOk(conn.execute_command(*cmd))

def _testCreateIndexRdbFiles(env):
    if not server_version_at_least(env, "6.2.0"):
        env.skip()
    create_indices(env, 'redisearch_2.10.3.rdb', 'idxSearch', True, False)

def _testCreateIndexRdbFilesWithJSON(env):
    if not server_version_at_least(env, "6.2.0"):
        env.skip()
    if OS == 'macos':
        env.skip()
    create_indices(env, 'rejson_2.0.0.rdb', 'idxJson', False, True)
    create_indices(env, 'redisearch_2.8.12_rejson_2.0.0.rdb', 'idxSearchJson', True, True)

def _testCreateIndexRdbFilesWithGeometry(env):
    if not server_version_at_least(env, "6.2.0"):
        env.skip()
    if OS == 'macos':
        env.skip()
    create_indices(env, 'redisearch_2.8.4.rdb', 'idxSearch_with_geom', True, False, 5)

# def _testCreateIndexRdbFilesWithGeometryWithJSON(env):
#     if not server_version_at_least(env, "6.2.0"):
#         env.skip()
#     if OS == 'macos':
#         env.skip()
#     create_indices(env, 'redisearch_2.8.4_rejson_2.0.0.rdb', 'idxSearchJson_with_geom', True, True, 5)


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
    server_port = 0
    def __init__(self, env):
        self.env = env
        self.new_conns = gevent.queue.Queue()

    def _handle_conn(self, sock, client_addr):
        conn = Connection(sock)
        self.new_conns.put(conn)

    def __enter__(self):
        try:
            self.StartListening(port=ShardMock.server_port, attempts=10)
        except Exception as e1:
            try:
                self.StartListening(port=random.randint(55000, 57000))
            except Exception as e2:
                # If both failed - raise both exceptions
                raise Exception([e1,e2])
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

    def StartListening(self, port, attempts=1):
        error_msgs = []
        for i in range(1, attempts + 1):
            self.stream_server = gevent.server.StreamServer(('127.0.0.1', port), self._handle_conn)
            try:
                self.stream_server.start()
            except Exception as e:
                msg = '(%d/%d) %d -> %s' % (i, attempts, port, e.strerror)
                error_msgs.append(msg)
                self.env.assertEqual(ShardMock.server_port, None, message=msg)
                continue
            ShardMock.server_port = self.stream_server.address[1]
            self.env.assertNotEqual(ShardMock.server_port, None, message='%s: StartListening(%d/%d) %d -> %d' % (
                self.__class__.__name__, i, attempts, port, ShardMock.server_port))
            break
        else:
            raise Exception("%s StartListening failed: %s" % (self.__class__.__name__, '\n'.join(error_msgs)))


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

def sendShortReads(env, rdb_file, expected_index):
    # Add some initial content (index+keys) to test backup/restore/discard when short read fails
    # When entire rdb is successfully sent and loaded (from swapdb) - backup should be discarded
    env.assertCmdOk('replicaof', 'no', 'one')
    env.flush()
    add_index(env, True,  'idxBackup1', 'a', 5, 10, 5)
    add_index(env, False, 'idxBackup2', 'b', 5, 10, 5)

    res = env.cmd('ft.search ', 'idxBackup1', '*', 'limit', '0', '0')
    env.assertEqual(res[0], 5)
    res = env.cmd('ft.search ', 'idxBackup2', '*', 'limit', '0', '0')
    env.assertEqual(res[0], 5)

    with open(rdb_file, mode='rb') as f:
        full_rdb = f.read()
    total_len = len(full_rdb)

    env.assertGreater(total_len, SHORT_READ_BYTES_DELTA)
    r = range(0, total_len + 1, SHORT_READ_BYTES_DELTA)
    if (total_len % SHORT_READ_BYTES_DELTA) != 0:
        r = chain(r, range(total_len, total_len + 1))
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
        env.assertTrue(res)
        res = env.cmd('replicaof', '127.0.0.1', shardMock.server_port)
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

        if server_version_less_than(env, '7.0.0'):
            # Async load in 'swapdb' mode is supported in redis < 7.
            res = env.cmd('ft._list')
            if is_shortread:
                # Verify original data, that existed before the failed attempt to short-read, is restored
                env.assertEqual(res, ['idxBackup2', 'idxBackup1'])
                res = env.cmd('ft.search ', 'idxBackup1', '*', 'limit', '0', '0')
                env.assertEqual(res[0], 5)
                res = env.cmd('ft.search ', 'idxBackup2', '*', 'limit', '0', '0')
                env.assertEqual(res[0], 5)
            else:
                # Verify new data was loaded and the backup was discarded
                # TODO: How to verify internal backup was indeed discarded
                res.sort()
                env.assertEqual(len(res), expected_index.count)
                r = re.compile(expected_index.pattern)
                expected_indices = list(filter(lambda x: r.match(x), res))
                env.assertEqual(len(expected_indices), expected_index.count)
                for ind, expected_result_count in zip(expected_indices, expected_index.search_result_count):
                    res = env.cmd('ft.search ', ind, '*', 'limit', '0', '0')
                    env.assertEqual(res[0], expected_result_count)

        # Exit (avoid read-only exception with flush on replica)
        env.assertCmdOk('replicaof', 'no', 'one')

seed = str(time.time())
random.seed(seed)

def downloadFile(file_name):
    path = os.path.join(REDISEARCH_CACHE_DIR, file_name)
    path_dir = os.path.dirname(path)
    os.makedirs(path_dir, exist_ok=True)
    if not os.path.exists(path):
        subprocess.run(["wget", "--no-check-certificate", BASE_RDBS_URL + file_name, "-O", path, "-q"])
        if os.path.splitext(path)[-1] == '.zip':
            return unzip(path, path_dir)
        else:
            return os.path.exists(path) and os.path.getsize(path) > 0
    return True

def doTest(env: Env, test_name, rdb_name, expected_index):
    env.debugPrint(f'random seed for {test_name}: {seed}', force=True)
    env.assertTrue(downloadFile(rdb_name), message='Failed to download ' + rdb_name)
    name, ext = os.path.splitext(rdb_name)
    fullPath = os.path.join(REDISEARCH_CACHE_DIR, name if ext == '.zip' else rdb_name)

    if MT_BUILD:
        env.cmd(config_cmd(), 'SET', 'MIN_OPERATION_WORKERS', '0') # test without MT
        sendShortReads(env, fullPath, expected_index)
        if server_version_at_least(env, "7.0.0"):
            env.cmd(config_cmd(), 'SET', 'MIN_OPERATION_WORKERS', '2') # test with MT
            sendShortReads(env, fullPath, expected_index)
    else:
        # test without MT (no need to change configuration)
        sendShortReads(env, fullPath, expected_index)

# Dynamically create a test function for each rdb file
@skip(cluster=True, redis_less_than='6.2.0', macos=True, asan=True, arch='aarch64')
def register_tests():
    test_func = lambda test, rdb, idx: lambda env: doTest(env, test, rdb, idx)
    for rdb_name, expected_index in RDBS.items():
        test_name = 'test_' + rdb_name.replace('/', '_').replace('.', '_')
        globals()[test_name] = test_func(test_name, rdb_name, expected_index)
try:
    register_tests()
except SkipTest:
    pass
