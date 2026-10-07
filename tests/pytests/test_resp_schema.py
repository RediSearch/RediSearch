# Copyright (c) 2006-Present, Redis Ltd.
# All rights reserved.
#
# Licensed under your choice of the Redis Source Available License 2.0
# (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
# GNU Affero General Public License v3 (AGPLv3).

import json
import threading
from contextlib import contextmanager
from common import *

CONFIG = 'search-internal-resp-schema'
TAG = '__resp_schema_v1'


@contextmanager
def schema_mode(env, enabled, config=CONFIG):
    conns = env.getOSSMasterNodesConnectionList()
    previous = [c.execute_command('CONFIG', 'GET', config) for c in conns]
    try:
        for c in conns:
            c.execute_command('CONFIG', 'SET', config, enabled)
        yield
    finally:
        for c, old in zip(conns, previous):
            c.execute_command('CONFIG', 'SET', config,
                              old[config] if isinstance(old, dict) else old[1])


def rows(env, reply):
    if env.protocol == 3:
        return [r['extra_attributes'] for r in reply['results']]
    return [dict(zip(r[::2], r[1::2])) for r in reply[1:]]

def payload(env, reply):
    return reply['results'] if env.protocol == 3 else reply


def wire_rows(env, chunk):
    return chunk[1] if env.protocol == 3 else chunk[1][1:]


def decode_schema(env, chunk):
    decoded = []
    for mask, values in wire_rows(env, chunk):
        names = chunk[2] if mask is None else [
            name for name, bit in zip(chunk[2], mask) if bit == '1']
        if mask is not None:
            env.assertEqual(mask.count('1'), len(values))
        decoded.append(dict(zip(names, values)))
    return decoded


def compare_modes(env, *command):
    with schema_mode(env, 'no'):
        legacy = env.cmd(*command)
    with schema_mode(env, 'yes'):
        compact = env.cmd(*command)
    env.assertEqual(compact, legacy, message=command)
    return compact


def load_sparse(env, count, key_pattern):
    conn = getConnectionByEnv(env)
    for i in range(count):
        fields = ['id', i] + (['optional', 'value'] if i % 2 else [])
        conn.execute_command('HSET', key_pattern.format(i), *fields)


@skip(cluster=False)
def test_resp_schema_format_selection():
    """Verify internal opt-in and coordinator authority across flag settings in both protocols."""
    for protocol in (2, 3):
        env = Env(protocol=protocol)
        env.expect('FT.CREATE', 'idx', 'SCHEMA', 'id', 'NUMERIC', 'SORTABLE').ok()
        conn = getConnectionByEnv(env)
        for i in range(1, 4):
            conn.execute_command('HSET', f'{{docs}}:{i}', 'id', i)
        shard = next(c for c in env.getOSSMasterNodesConnectionList()
                     if c.execute_command('DBSIZE'))
        shard.execute_command('DEBUG', 'MARK-INTERNAL-CLIENT')
        command = ['_FT.AGGREGATE', 'idx', '*', 'LOAD', 1, '@id', 'TIMEOUT', 0]
        for mode in ('no', 'yes'):
            with schema_mode(env, mode):
                for opt_in in ([], ['_RESP_SCHEMA']):
                    raw = payload(env, shard.execute_command(*command, *opt_in))
                    env.assertEqual(raw[0] == TAG, bool(opt_in), message=raw)
        # The coordinator chooses the format even when every other shard differs.
        command = ['FT.AGGREGATE', 'idx', '*', 'LOAD', 1, '@id', 'TIMEOUT', 0]
        coordinator = env.getConnection()
        for shard_mode, coordinator_mode in (('no', 'yes'), ('yes', 'no')):
            with schema_mode(env, shard_mode):
                coordinator.execute_command('CONFIG', 'SET', CONFIG, coordinator_mode)
                env.assertEqual(rows(env, env.cmd(*command)),
                                [{'id': '1'}, {'id': '2'}, {'id': '3'}])


@skip(cluster=False)
def test_resp_schema_wire():
    """Exercise dense/sparse schema replies and late LOAD * fields in both protocols."""
    for protocol in (2, 3):
        env = Env(protocol=protocol)
        env.expect('FT.CREATE', 'idx', 'SCHEMA', 'id', 'NUMERIC', 'SORTABLE').ok()
        for c in env.getOSSMasterNodesConnectionList():
            config = c.execute_command('CONFIG', 'GET', CONFIG)
            env.assertEqual(config[CONFIG] if isinstance(config, dict) else config[1], 'no')
        env.expect('FT.AGGREGATE', 'idx', '*', '_RESP_SCHEMA').error().contains('Unknown argument')
        conn = getConnectionByEnv(env)
        conn.execute_command('HSET', '{docs}:1', 'id', 1, 'first', 'a')
        conn.execute_command('HSET', '{docs}:2', 'id', 2, 'late', 'b')
        conn.execute_command('HSET', '{docs}:3', 'id', 3, 'first', 'c', 'late', 'd')
        with schema_mode(env, 'yes'):
            shard = next(c for c in env.getOSSMasterNodesConnectionList()
                         if c.execute_command('DBSIZE'))
            shard.execute_command('DEBUG', 'MARK-INTERNAL-CLIENT')
            try:
                shard.execute_command('_FT.SEARCH', 'idx', '*', '_RESP_SCHEMA')
                env.assertTrue(False, message='SEARCH accepted aggregate schema opt-in')
            except redis.ResponseError as error:
                env.assertContains('Unknown argument', str(error))
            reply = shard.execute_command('_FT.AGGREGATE', 'idx', '*',
                                          'LOAD', '*',
                                          'WITHCURSOR', 'COUNT', 2, '_RESP_SCHEMA')
            chunk = payload(env, reply[0])
            env.assertEqual(chunk[0], TAG)
            env.assertEqual(len(chunk), 3)
            names = chunk[2]
            env.assertEqual(set(names), {'id', 'first', 'late'})
            records = wire_rows(env, chunk)
            env.assertEqual(len(records), 2)
            env.assertEqual(records[0][0], None)
            env.assertEqual(len(records[0][1]), 2)
            env.assertEqual(records[1][0].count('0'), 1)
            recovered = decode_schema(env, chunk)
            env.assertEqual(recovered, [{'id': '1', 'first': 'a'}, {'id': '2', 'late': 'b'}])
            env.assertNotEqual(reply[1], 0)
            tail = shard.execute_command('_FT.CURSOR', 'READ', 'idx', reply[1], 'COUNT', 2)
            tail_payload = payload(env, tail[0])
            env.assertEqual(tail_payload[0], TAG)
            env.assertEqual(tail[1], 0)
            for workers in (0, 2):
                with schema_mode(env, workers, 'search-workers'):
                    for policy in ('return', 'fail', 'return-strict'):
                        with schema_mode(env, policy, 'search-on-timeout'):
                            command = ['_FT.AGGREGATE', 'idx', '*', 'ADDSCORES', 'LOAD', 1, '@id']
                            expected = rows(env, shard.execute_command(*command))
                            scored = shard.execute_command(*command, '_RESP_SCHEMA')
                            scored = payload(env, scored)
                            env.assertEqual(scored[0], TAG)
                            scored_rows = wire_rows(env, scored)
                            env.assertEqual(len(scored_rows), 3)
                            env.assertEqual(set(scored[2]), {'id', '__score'})
                            for i, (mask, values) in enumerate(scored_rows):
                                env.assertEqual(mask, None)
                                env.assertEqual(len(values), 2)
                                row = dict(zip(scored[2], values))
                                env.assertEqual(row['id'], expected[i]['id'])
                                env.assertEqual(str(row['__score']), str(expected[i]['__score']))
            metadata = ['_FT.AGGREGATE', 'idx', '*', 'WITHRAWIDS', 'LOAD', 1, '@id']
            legacy = shard.execute_command(*metadata)
            fallback = shard.execute_command(*metadata, '_RESP_SCHEMA')
            env.assertEqual(fallback, legacy)


@skip(cluster=False)
def test_resp_schema_results():
    """Compare public replies with schema enabled across sparse loads, counts and cursors."""
    for protocol in (2, 3):
        env = Env(protocol=protocol)
        env.expect('FT.CREATE', 'idx', 'SCHEMA', 'id', 'NUMERIC', 'SORTABLE',
                   'text', 'TEXT').ok()
        conn = getConnectionByEnv(env)
        for i in range(12):
            args = ['id', i, 'text', 'hello', 'binary', 'a\x00b']
            if i % 2:
                args += ['sparse', 'value']
            conn.execute_command('HSET', f'{{doc{i}}}:1', *args)
        queries = [
            ['LOAD', 3, '@id', '@sparse', '@binary', 'SORTBY', 2, '@id', 'ASC'],
            ['LOAD', '*', 'SORTBY', 2, '@id', 'ASC'],
            ['WITHCOUNT', 'LOAD', 1, '@id', 'SORTBY', 2, '@id', 'ASC', 'LIMIT', 0, 3],
            ['WITHCOUNT', 'LIMIT', 0, 0],
            ['ADDSCORES', 'LOAD', 1, '@id', 'SORTBY', 2, '@id', 'ASC'],
            ['FILTER', '@id < 0'],
            ['GROUPBY', 0, 'REDUCE', 'COUNT', 0, 'AS', 'count'],
        ]
        for policy in ('return', 'fail', 'return-strict'):
            with schema_mode(env, policy, 'search-on-timeout'):
                for query in queries:
                    compare_modes(env, 'FT.AGGREGATE', 'idx', '*', *query)
        query = ['LOAD', '*', 'SORTBY', 2, '@id', 'ASC', 'LIMIT', 0, 12, 'WITHCURSOR', 'COUNT', 3]
        all_modes = []
        for mode in ('no', 'yes'):
            with schema_mode(env, mode):
                reply, cursor = env.cmd('FT.AGGREGATE', 'idx', '*', *query)
                collected = rows(env, reply)
                while cursor:
                    reply, cursor = env.cmd('FT.CURSOR', 'READ', 'idx', cursor, 'COUNT', 3)
                    collected += rows(env, reply)
                all_modes.append(collected)
        env.assertEqual(all_modes[0], all_modes[1])
        env.assertEqual(len(all_modes[1]), 12)

@skip(cluster=False, no_json=True)
def test_resp_schema_json():
    """Preserve JSON nulls, missing fields, nested values and multi-value selection."""
    for protocol in (2, 3):
        env = Env(protocol=protocol)
        env.expect('FT.CREATE', 'idx', 'ON', 'JSON', 'SCHEMA', '$.id', 'AS', 'id',
                   'NUMERIC', 'SORTABLE', '$.tags[*]', 'AS', 'tags', 'TAG').ok()
        conn = getConnectionByEnv(env)
        for i in range(12):
            doc = dict(id=i, tags=['a', 'b'], nested=[{'num': 1.5, 'null': None}, ['x', 2]])
            if i % 2:
                doc['optional'] = None
            conn.execute_command('JSON.SET', f'{{json{i}}}:1', '$', json.dumps(doc))
        for dialect in (2, 3):
            formats = ('STRING', 'EXPAND') if protocol == 3 else ('STRING',)
            for fmt in formats:
                args = ['LOAD', 8, '@id', '@tags', '$.optional', 'AS', 'optional',
                        '$.nested', 'AS', 'nested', 'SORTBY', 2, '@id', 'ASC',
                        'LIMIT', 0, 12, 'DIALECT', dialect]
                if protocol == 3:
                    args += ['FORMAT', fmt]
                compact = compare_modes(env, 'FT.AGGREGATE', 'idx', '*', *args)
                result = rows(env, compact)
                env.assertEqual(len(result), 12)
                env.assertFalse('optional' in result[0], message=result[0])
                env.assertTrue('optional' in result[1], message=result[1])

@skip(cluster=False)
def test_resp_schema_profile_and_timeout():
    """Preserve full profiles, LIMIT drain counters and deterministic RETURN timeout chunks."""
    for protocol in (2, 3):
        env = Env(protocol=protocol)
        env.expect('FT.CREATE', 'idx', 'SCHEMA', 'id', 'NUMERIC').ok()
        load_sparse(env, 60, '{{time{}}}:1')
        with schema_mode(env, 'yes'), schema_mode(env, 'return', 'search-on-timeout'):
            profile = env.cmd('FT.PROFILE', 'idx', 'AGGREGATE', 'QUERY', '*', 'LOAD', 1, '@id')
            result = profile['Results'] if protocol == 3 else profile[0]
            env.assertEqual(len(rows(env, result)), 60)
            shards = env.getOSSMasterNodesConnectionList()
            for shard in shards:
                env.assertGreater(shard.execute_command('DBSIZE'), 0)
            reply = env.cmd('FT.PROFILE', 'idx', 'AGGREGATE', 'QUERY', '*',
                            'LOAD', 2, '@id', '@optional', 'LIMIT', 0, 1, 'TIMEOUT', 0)
            result = reply['Results'] if protocol == 3 else reply[0]
            env.assertEqual(len(rows(env, result)), 1)
            profile = reply['Profile'] if protocol == 3 else to_dict(reply[1])
            coordinator = profile['Coordinator']
            if protocol == 2:
                coordinator = to_dict(coordinator)
            processors = coordinator['Result processors profile']
            if protocol == 2:
                processors = [to_dict(processor) for processor in processors]
            network = next(processor for processor in processors if processor['Type'] == 'Network')
            # All shard profiles with just one converted row require draining the other chunks.
            env.assertEqual(network['Shard replies'], len(shards))
            env.assertLess(network['Results processed'], 2)
            env.assertTrue(network['Fields converted'] in (1, 2), message=network)
            env.assertEqual(len(profile['Shards']), len(shards))
            command = ['FT.AGGREGATE', 'idx', '*', 'LOAD', 1, '@id', 'LIMIT', 0, 60]
            reply = runDebugQueryCommandTimeoutAfterN(env, command, 5, internal_only=True)
            result = rows(env, reply)
            env.assertEqual(len(result), 5 if protocol == 3 else 60)
            for row in result:
                env.assertEqual(set(row), {'id'})
            if protocol == 3:
                VerifyTimeoutWarningResp3(env, reply)


@skip(cluster=False)
def test_resp_schema_multiple_internal_chunks():
    """Resolve schemas afresh across interleaved shards and their internal cursor reads."""
    for protocol in (2, 3):
        env = Env(protocol=protocol)
        env.expect('FT.CREATE', 'idx', 'SCHEMA', 'id', 'NUMERIC', 'SORTABLE').ok()
        count = 4000
        load_sparse(env, count, '{{chunk{}}}:1')
        for shard in env.getOSSMasterNodesConnectionList():
            env.assertGreater(shard.execute_command('DBSIZE'), 1000)
        command = ['FT.AGGREGATE', 'idx', '*', 'WITHCOUNT', 'LOAD', 2, '@id', '@optional',
                   'SORTBY', 2, '@id', 'ASC', 'LIMIT', 0, count]
        compact = compare_modes(env, *command)
        result = rows(env, compact)
        env.assertEqual(len(result), count)
        env.assertEqual(result[0], {'id': '0'})
        env.assertEqual(result[-1], {'id': str(count - 1), 'optional': 'value'})


@skip(cluster=False)
def test_resp_schema_buffered_timeout():
    """Force buffered shard timeouts after sparse rows, including a live config change."""
    for protocol in (2, 3):
        env = Env(protocol=protocol)
        skipIfNoEnableAssert(env)
        env.expect('FT.CREATE', 'idx', 'SCHEMA', 'id', 'NUMERIC').ok()
        load_sparse(env, 6, '{{buffered}}:{}')
        shard = next(c for c in env.getOSSMasterNodesConnectionList()
                     if c.execute_command('DBSIZE'))
        with schema_mode(env, 'yes'), schema_mode(env, 2, 'search-workers'):
            for policy in ('return-strict', 'fail'):
                with schema_mode(env, policy, 'search-on-timeout'):
                    # Pin one connection so CLIENT UNBLOCK targets this exact request.
                    query = redis.Redis(single_connection_client=True,
                                        **shard.connection_pool.connection_kwargs)
                    query.execute_command('DEBUG', 'MARK-INTERNAL-CLIENT')
                    client_id = query.execute_command('CLIENT', 'ID')
                    results = []

                    def execute():
                        try:
                            results.append(query.execute_command(
                                '_FT.AGGREGATE', 'idx', '*', 'LOAD', 2, '@id', '@optional',
                                'TIMEOUT', 0, 'WITHCURSOR', 'COUNT', 6, '_RESP_SCHEMA'))
                        except Exception as error:
                            results.append(error)

                    setPauseAfterAggregateResult(shard, 3)
                    thread = threading.Thread(target=execute, daemon=True)
                    thread.start()
                    try:
                        wait_for_condition(
                            lambda: (getIsAggregateResultsPaused(shard) == 1, {}),
                            'Shard did not buffer three rows', timeout=5)
                        # The request must retain its format after CONFIG changes on main.
                        shard.execute_command('CONFIG', 'SET', CONFIG, 'no')
                        env.assertEqual(shard.execute_command('CLIENT', 'UNBLOCK',
                                                              client_id, 'TIMEOUT'), 1)
                        thread.join(timeout=5)
                        env.assertFalse(thread.is_alive(), message=results)
                        env.assertEqual(len(results), 1, message=results)
                        if policy == 'fail':
                            env.assertTrue(isinstance(results[0], redis.ResponseError),
                                           message=results)
                            env.assertContains('Timeout limit was reached', str(results[0]))
                            continue
                        reply, cursor = results[0]
                        env.assertEqual(cursor, 0)
                        chunk = payload(env, reply)
                        env.assertEqual(chunk[0], TAG)
                        decoded = decode_schema(env, chunk)
                        env.assertEqual(decoded, [{'id': '0'},
                                                  {'id': '1', 'optional': 'value'},
                                                  {'id': '2'}])
                        if protocol == 3:
                            env.assertEqual(reply['warning'], ['Timeout limit was reached'])
                    finally:
                        if thread.is_alive():
                            shard.execute_command('CLIENT', 'UNBLOCK', client_id, 'TIMEOUT')
                            thread.join(timeout=5)
                        # FAIL replies before its worker leaves the self-releasing pause.
                        shard.execute_command(debug_cmd(), 'WORKERS', 'drain')
                        resetAggregateResultsDebug(shard)
                        shard.execute_command('CONFIG', 'SET', CONFIG, 'yes')
                        query.close()
                        query.connection_pool.disconnect()
