# coding=utf-8
import random
import string

import gevent.queue
import gevent.server
import gevent.socket
import os
import shutil

from common import TimeLimit
from common import waitForIndex

def rand_name(k):
    return ''.join([chr(random.randint(ord('a'), ord('z'))) for _ in range(0, random.randint(1, k))])
    #return random.choices(''.join(string.ascii_letters), k=k)

def rand_num(k):
    return ''.join([chr(random.randint(ord('0'), ord('9'))) for _ in range(0, random.randint(1, k))])
    #return random.choices(''.join(string.digits, k=k))

def create_indices(env):
    env.skipOnCluster()

    '''FT.CREATE {index}
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
    cmd_create = ['ft.create', 'idxSearch', 'ON', 'HASH']
    # With prefixes
    cmd_create.extend(['prefix', '34'])
    for i in range(1, 35):
        cmd_create.append('pref' + str(i) + ":")
    # With filter
    cmd_create.extend(['filter', 'startswith(@__key, "r")'])
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
                'field1', 'as', 'f1', 'text', 'nostem', 'weight', '0.2', 'phonetic', 'dm:es', 'sortable',
                'field2', 'as', 'f2', 'numeric', 'sortable',
                'field3', 'as', 'f3', 'geo',
                'field4', 'as', 'f4', 'tag', 'separator', ';',
                'field11', 'text', 'nostem',
                'field12', 'numeric', 'noindex',
                'field13', 'geo',
                'field14', 'tag', 'noindex',
                'myLang', 'text',
                'myScore', 'numeric',
                ])
    env.assertOk(env.cmd(*cmd_create))
    waitForIndex(env, 'idxSearch')
    env.assertOk(env.cmd('ft.synupdate', 'idxSearch', 'syngrp1', 'pelota', 'bola', 'balón'))
    env.assertOk(env.cmd('ft.synupdate', 'idxSearch', 'syngrp2', 'jugar', 'tocar'))

    # Add keys
    for i in range(1, 50):
        cmd = ['hset', 'pref' + str(i) + ":k" + str(i), rand_name(5), rand_num(2), rand_name(5), rand_num(3)]
        env.assertEqual(env.cmd(*cmd), 2L)

    # Save the rdb
    env.assertOk(env.cmd('config', 'set', 'dbfilename', 'test_short_read_search.rdb'))
    dbFileName = env.cmd('config', 'get', 'dbfilename')[1]
    dbDir = env.cmd('config', 'get', 'dir')[1]
    dbFilePath = os.path.join(dbDir, dbFileName)

    env.assertTrue(env.cmd('save'))
    # Copy to avoid truncation of rdb due to RLTest flush and save
    dbCopyFilePath = os.path.join('/tmp/', dbFileName)
    shutil.copyfile(dbFilePath, dbCopyFilePath)

def testCreateIndexRdbFiles(env):
    create_indices(env)

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


def testShortReadSearch(env):
    env.skipOnCluster()
    # sendShortReads(env, "short_read.rdb_searchOnly")
    sendShortReads(env, "/tmp/test_short_read_search.rdb")


def testShortReadJSON(env):
    env.skipOnCluster()
    sendShortReads(env, "short_read.rdb_jsonOnly")


def testShortReadSearchJSON(env):
    env.skipOnCluster()
    sendShortReads(env, "short_read.rdb_searchjson")


def sendShortReads(env, rdb_file):
    with open(rdb_file, mode='rb') as f:
        full_rdb = f.read()
    total_len = len(full_rdb)
    dbg_str = ''
    dbg_ndx = -1
    for b in range(0, total_len + 1):
        rdb = full_rdb[0:b]

        # # For debugging: print the binary content before it is sent
        # if len(rdb):
        #     ch = rdb[dbg_ndx]
        #     printable_ch = ch
        #     if ord(ch) < 32 or ord(ch) == 127:
        #         printable_ch = '\?'
        # else:
        #     ch = '\0'
        #     printable_ch = '\!'   # no data (zero length)
        # dbg_str = '{} {:=03n}:{:<2}({:<3})'.format(dbg_str, dbg_ndx, printable_ch, ord(ch))
        # if not (dbg_ndx+1) % 10:
        #     dbg_str = dbg_str + "\n"
        # dbg_ndx = dbg_ndx + 1

        env.debugPrint('runShortRead: %d out of %d \n%s' % (b, total_len, dbg_str))
        runShortRead(env, rdb, total_len)


def runShortRead(env, data, total_len):
    with ShardMock(env) as shardMock:

        # For debugging: if adding breakpoints in redis, in order to avoid closing the connection, uncomment the following line
        # res = env.cmd('CONFIG', 'SET', 'timeout', '0')

        # Notice: Do not use env.expect in this test
        # (since it is sending commands to redis and in this test we need to follow strict hand-shaking)
        res = env.cmd('CONFIG', 'SET', 'repl-diskless-load', 'swapdb')
        res = env.cmd('slaveof', 'localhost', '10000')
        env.assertTrue(res)
        conn = shardMock.GetConnection()
        # Perform hand-shake with slave
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

        # Send RDB to slave
        some_guid = 'af4e30b5d14dce9f96fbb7769d0ec794cdc0bbcc'
        conn.send_status('FULLRESYNC ' + some_guid + ' 0')
        if total_len != len(data):
            conn.send('$%d\r\n%s' % (total_len, data))
            # Close during slave is waiting for more RDB data
            conn.close()
        else:
            # Allow to succeed with a full read (protocol expects a trailing '\r\n')
            conn.send('$%d\r\n%s\r\n' % (total_len, data))
        conn.flush()

        # Make sure slave did not crash
        res = env.cmd('PING')
        env.assertEqual(res, True)
        conn = shardMock.GetConnection(timeout=3)
        env.assertNotEqual(conn, None)

        # Exit (avoid read-only exception with flush on slave)
        env.assertEqual(env.cmd('slaveof', 'no', 'one'), True)
