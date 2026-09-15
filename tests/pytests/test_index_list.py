# Copyright (c) 2006-Present, Redis Ltd.
# All rights reserved.
#
# Licensed under your choice of the Redis Source Available License 2.0
# (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
# GNU Affero General Public License v3 (AGPLv3).

import socket
import socketserver
import threading

from common import *

# Every non-"ok" warning opens with this marker, shared with the FT.INFO error on a
# diverged index so that one grep finds both.
INCONSISTENT = 'Inconsistent index state'

UNKNOWN_ARG = 'SEARCH_ARG_UNRECOGNIZED Unknown argument'

# The cluster tests pin @env_spec(shardsCount=3) rather than taking the suite default:
# the warnings they assert name the shard count ("2 of 3 reporting shards"), so the
# expected text would go stale if the count floated with SHARDS=.


def cluster_state(env, token='WITHCLUSTERSTATE'):
    """`{index name: entry}` from `FT._LIST WITHCLUSTERSTATE` on the serving shard.

    A non-"ok" status is unflattened too, so RESP2 and RESP3 replies read alike. The
    shard-id lists are sorted: the reply carries them as a set of shards, and their
    order follows reply arrival, which is not stable between two fanouts.
    """
    entries = {}
    for entry in env.cmd('FT._LIST', token):
        entry = to_dict(entry)
        if entry['status'] != 'ok':
            entry['status'] = to_dict(entry['status'])
            for key in ('missing_from_shards', 'unreachable_shards'):
                if key in entry['status']:
                    entry['status'][key] = sorted(entry['status'][key])
        entries[entry['index']] = entry
    return entries


def shard_node_ids(env):
    """Wait for every shard to have a topology, then return their Redis Cluster node
    ids in shard order — the ids the reply names shards by.

    Until a shard has a topology it reports an empty node id, which the reducer counts
    as a shard that did not report, so every assertion on a status map needs this first.
    """
    ids = []
    for shardId in range(1, env.shardsCount + 1):
        con = env.getConnection(shardId)
        verify_shard_init(con)
        ids.append(con.execute_command('CLUSTER', 'MYID'))
    return ids


def internal_payload(env, shardId=1):
    """One shard's internal `_FT._LIST WITHCLUSTERSTATE` payload:
    `[node id, fingerprint recipe, index encoding version,
    [[index name, fingerprint], ...]]`.
    """
    con = env.getConnection(shardId)
    con.execute_command('DEBUG', 'MARK-INTERNAL-CLIENT')
    return con.execute_command('_FT._LIST', 'WITHCLUSTERSTATE')


def local_fingerprint(env, idx, shardId=1):
    """One shard's schema fingerprint for `idx`."""
    return dict(internal_payload(env, shardId)[3])[idx]


@skip(cluster=True)
def test_list_rejects_unknown_arguments(env):
    """FT._LIST takes at most the WITHCLUSTERSTATE token."""
    env.expect('FT._LIST', 'BOGUS').error().equal(UNKNOWN_ARG)
    env.expect('FT._LIST', 'BOGUS', 'EXTRA').error().contains('wrong number of arguments')


@skip(cluster=True)
def test_list_command_metadata_accepts_optional_token(env):
    """RAMP discovers public command arity and key positions from COMMAND INFO."""
    info = next(iter(env.cmd('COMMAND', 'INFO', 'FT._LIST').values()))
    env.assertEqual(info['arity'], -1)
    env.assertEqual([info['first_key_pos'], info['last_key_pos'], info['step_count']], [0, 0, 0])


@skip(cluster=True)
def test_internal_list_rejects_unknown_arguments(env):
    """The shard-side _FT._LIST validates its own arguments, although the coordinator
    never sends it anything but WITHCLUSTERSTATE."""
    env.expect('DEBUG', 'MARK-INTERNAL-CLIENT').ok()
    env.expect('_FT._LIST', 'BOGUS').error().equal(UNKNOWN_ARG)
    env.expect('_FT._LIST', 'BOGUS', 'EXTRA').error().contains('wrong number of arguments')


@skip(cluster=False)
@env_spec(shardsCount=3)
def test_list_without_the_token_stays_local(env):
    """Without the token the reply is the serving shard's own list, so a shard that
    alone holds an index lists it and its peers do not."""
    conns = create_diverged_index(env, 'idx_diverged')
    conns[0].execute_command('_FT.CREATE', 'idx_shard1_only', 'SCHEMA', 't', 'TEXT')

    env.assertEqual(sorted(conns[0].execute_command('FT._LIST')),
                    ['idx_diverged', 'idx_shard1_only'])
    for shardId, con in enumerate(conns[1:], start=2):
        env.assertEqual(con.execute_command('FT._LIST'), ['idx_diverged'],
                        message=f'shard {shardId}')

    # The public and the internal command answer the same list on the same shard.
    env.assertEqual(sorted(conns[0].execute_command('_FT._LIST')),
                    sorted(conns[0].execute_command('FT._LIST')))


@skip(cluster=False)
@env_spec(shardsCount=3)
def test_consistent_index_is_ok(env):
    """An index every shard holds with the same schema reports the plain string "ok",
    and nothing else."""
    shard_node_ids(env)
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 't', 'TEXT').ok()
    env.assertEqual(cluster_state(env), {'idx': {'index': 'idx', 'status': 'ok'}})


@skip(cluster=False)
@env_spec(shardsCount=3)
def test_diverged_schemas_are_counted_never_attributed(env):
    """Divergence is counted and no shard is named for it: without a majority there is
    no defensible way to say which shards hold the wrong schema."""
    shard_node_ids(env)
    create_diverged_index(env, 'idx')

    status = cluster_state(env)['idx']['status']
    env.assertEqual(set(status.keys()), {'warning'})
    env.assertEqual(status['warning'],
                    INCONSISTENT + ': the shards that have it hold 3 different schemas.'
                    ' Drop the index and recreate it so that all shards agree.')


@skip(cluster=False)
@env_spec(shardsCount=3)
def test_shards_missing_an_index_are_named(env):
    """Shards that reported without the index are both counted and named."""
    node_ids = shard_node_ids(env)
    con = env.getConnection(1)
    con.execute_command('DEBUG', 'MARK-INTERNAL-CLIENT')
    con.execute_command('_FT.CREATE', 'idx', 'SCHEMA', 't', 'TEXT')

    status = cluster_state(env)['idx']['status']
    env.assertEqual(set(status.keys()), {'warning', 'missing_from_shards'})
    env.assertEqual(status['missing_from_shards'], sorted(node_ids[1:]))
    env.assertEqual(status['warning'],
                    INCONSISTENT + ': index is missing from 2 of 3 reporting shards.'
                    ' Drop the index and recreate it so that all shards agree.')


@skip(cluster=False)
@env_spec(shardsCount=3)
def test_index_both_missing_and_diverged(env):
    """Both causes are reported: one clause must not hide the other."""
    node_ids = shard_node_ids(env)
    for shardId, extra in ((1, []), (2, ['b', 'TEXT'])):
        con = env.getConnection(shardId)
        con.execute_command('DEBUG', 'MARK-INTERNAL-CLIENT')
        con.execute_command('_FT.CREATE', 'idx', 'SCHEMA', 'a', 'TEXT', *extra)

    status = cluster_state(env)['idx']['status']
    env.assertEqual(set(status.keys()), {'warning', 'missing_from_shards'})
    env.assertEqual(status['missing_from_shards'], [node_ids[-1]])
    env.assertEqual(status['warning'],
                    INCONSISTENT + ': index is missing from 1 of 3 reporting shards, and the'
                    ' shards that have it hold 2 different schemas.'
                    ' Drop the index and recreate it so that all shards agree.')


@skip(cluster=False)
@env_spec(shardsCount=3)
def test_reply_covers_every_index_in_the_cluster(env):
    """One entry per index in the union of the shards' lists, however few shards hold it."""
    shard_node_ids(env)
    conns = create_diverged_index(env, 'idx_diverged')
    conns[0].execute_command('_FT.CREATE', 'idx_shard1_only', 'SCHEMA', 't', 'TEXT')
    env.expect('FT.CREATE', 'idx_everywhere', 'SCHEMA', 't', 'TEXT').ok()

    state = cluster_state(env)
    env.assertEqual(set(state.keys()),
                    {'idx_diverged', 'idx_shard1_only', 'idx_everywhere'})
    env.assertEqual(state['idx_everywhere']['status'], 'ok')
    env.assertNotEqual(state['idx_diverged']['status'], 'ok')
    env.assertNotEqual(state['idx_shard1_only']['status'], 'ok')

    env.assertEqual(cluster_state(env, 'withclusterstate'), cluster_state(env))


@skip(cluster=False)
@env_spec(shardsCount=3)
def test_internal_payload_reports_this_shards_schemas(env):
    """The payload the reducer consumes: the shard's own identity, its comparability
    gates, and one fingerprint per local index."""
    node_ids = shard_node_ids(env)
    conns = create_diverged_index(env, 'idx_diverged')
    for con in conns:
        con.execute_command('_FT.CREATE', 'idx_same', 'SCHEMA', 't', 'TEXT')
    conns[0].execute_command('_FT.CREATE', 'idx_shard1_only', 'SCHEMA', 't', 'TEXT')

    payloads = [con.execute_command('_FT._LIST', 'WITHCLUSTERSTATE') for con in conns]

    # Each shard must self-identify, since fanout replies arrive unattributed.
    env.assertEqual(len(set(node_ids)), env.shardsCount)
    env.assertEqual([p[0] for p in payloads], node_ids)

    # The initial fingerprint version is shared by every shard.
    env.assertEqual([p[1] for p in payloads], [1] * env.shardsCount)
    env.assertEqual(len(set(p[2] for p in payloads)), 1)

    fps = [dict(p[3]) for p in payloads]
    # Equal schemas must hash equal across processes, and unequal ones apart.
    env.assertEqual(len(set(d['idx_same'] for d in fps)), 1)
    env.assertEqual(len(set(d['idx_diverged'] for d in fps)), env.shardsCount)
    env.assertEqual([('idx_shard1_only' in d) for d in fps], [True, False, False])


@skip(cluster=False)
@env_spec(shardsCount=3, protocol=3)
def test_internal_payload_shape_is_protocol_independent(env):
    """The payload is arrays, strings and integers only, so RESP3 clients see the same
    structure RESP2 ones do and the reducer parses one shape."""
    shard_node_ids(env)
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 't', 'TEXT').ok()

    payload = internal_payload(env)
    env.assertIsInstance(payload, list, message=payload)
    env.assertEqual(len(payload), 4, message=payload)
    node_id, recipe, index_version, entries = payload
    env.assertIsInstance(node_id, str, message=payload)
    for gate in (recipe, index_version):
        env.assertIsInstance(gate, int, message=payload)
    env.assertIsInstance(entries, list, message=entries)
    env.assertEqual(len(entries), 1, message=entries)
    name, fingerprint = entries[0]
    env.assertEqual(name, 'idx')
    env.assertIsInstance(fingerprint, int, message=entries)


@skip(cluster=False)
@env_spec(shardsCount=3)
def test_alter_moves_the_fingerprint(env):
    """FT.ALTER redefines the schema, so it must move the fingerprint - and, reaching
    every shard, must leave the index consistent."""
    shard_node_ids(env)
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 't', 'TEXT').ok()
    before = local_fingerprint(env, 'idx')

    env.expect('FT.ALTER', 'idx', 'SCHEMA', 'ADD', 'n', 'NUMERIC').ok()
    env.assertNotEqual(local_fingerprint(env, 'idx'), before)
    env.assertEqual(cluster_state(env)['idx']['status'], 'ok')


@skip(cluster=False)
@env_spec(shardsCount=3)
def test_synonyms_move_the_fingerprint(env):
    """Synonyms are part of the schema. That the index still reads consistent afterwards
    is the cross-process half of the claim: the synonym dict iterates in per-process
    order, so its hash must not."""
    shard_node_ids(env)
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 't', 'TEXT').ok()
    before = local_fingerprint(env, 'idx')

    env.expect('FT.SYNUPDATE', 'idx', 'g1', 'hello', 'hi', 'shalom').ok()
    env.assertNotEqual(local_fingerprint(env, 'idx'), before)
    env.assertEqual(cluster_state(env)['idx']['status'], 'ok')


@skip(cluster=False)
@env_spec(shardsCount=3)
def test_divergence_and_uncertainty_are_reported_together(env):
    """Proven divergence outranks uncertainty but does not replace it: the warning
    carries both clauses, so a silent shard cannot mask a divergence the shards that
    did answer demonstrated."""
    node_ids = shard_node_ids(env)
    create_diverged_index(env, 'idx')

    env.expect(debug_cmd(), 'PAUSE_TOPOLOGY_UPDATER').ok()
    try:
        with stopped_shard(env, env.shardsCount):
            status = cluster_state(env)['idx']['status']
            env.assertEqual(set(status.keys()), {'warning', 'unreachable_shards'})
            env.assertEqual(status['unreachable_shards'], [node_ids[-1]])
            # Divergence is proven by the two shards that answered, and the third
            # shard's silence is reported alongside it rather than instead of it.
            env.assertEqual(status['warning'],
                            INCONSISTENT + ': the shards that have it hold 2 different'
                            ' schemas. Drop the index and recreate it so that all shards'
                            ' agree. The rest of the picture cannot be determined:'
                            ' 1 of 3 shards did not reply.')
    finally:
        env.expect(debug_cmd(), 'RESUME_TOPOLOGY_UPDATER').ok()


@skip(cluster=False)
@env_spec(shardsCount=3)
def test_unreachable_shard_is_uncertainty_not_absence(env):
    """A shard that did not reply is named as unreachable and never as missing the
    index: absence can only be proven by a shard that answered."""
    node_ids = shard_node_ids(env)
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 't', 'TEXT').ok()

    # Freeze the serving shard's topology: once the cluster drops the stopped node it is
    # no longer expected to reply, and the index legitimately reads consistent again.
    env.expect(debug_cmd(), 'PAUSE_TOPOLOGY_UPDATER').ok()
    try:
        with stopped_shard(env, env.shardsCount):
            status = cluster_state(env)['idx']['status']
            env.assertEqual(set(status.keys()), {'warning', 'unreachable_shards'})
            env.assertEqual(status['unreachable_shards'], [node_ids[-1]])
            env.assertEqual(status['warning'], INCONSISTENT + ' cannot be determined:'
                            ' 1 of 3 shards did not reply.')
    finally:
        env.expect(debug_cmd(), 'RESUME_TOPOLOGY_UPDATER').ok()


@skip(cluster=False)
@env_spec(shardsCount=3)
def test_divergence_is_proven_within_a_gate_group(env):
    """A shard whose gates differ cannot mask a divergence between the shards whose
    gates agree. Fingerprints are compared inside each group of gate-agreeing shards,
    so the answer does not depend on which shard's reply arrived first."""
    shard_node_ids(env)
    create_diverged_index(env, 'idx')

    # Emulate a peer with a different fingerprint version.
    if env.useTLS:
        env.skip()
    node_id, _, version, entries = internal_payload(env, env.shardsCount)
    fingerprint = dict(entries)['idx']
    response = (f'*4\r\n${len(node_id)}\r\n{node_id}\r\n'
                f':2\r\n:{version}\r\n*1\r\n*2\r\n$3\r\nidx\r\n:{fingerprint}\r\n').encode()
    env.expect(debug_cmd(), 'PAUSE_TOPOLOGY_UPDATER').ok()
    try:
        with rejecting_shard(env, env.shardsCount, response):
            expected = {'warning':
                        INCONSISTENT + ': the shards that have it hold 2 different'
                        ' schemas. Drop the index and recreate it so that all shards'
                        ' agree. The rest of the picture cannot be determined: shards'
                        ' are running incompatible versions or configurations.'}

            def has_different_version():
                status = cluster_state(env)['idx']['status']
                return status == expected, status

            wait_for_condition(has_different_version, 'coordinator did not receive the different fingerprint version')
            env.assertEqual(cluster_state(env)['idx']['status'], expected)
    finally:
        env.expect(debug_cmd(), 'RESUME_TOPOLOGY_UPDATER').ok()


@skip(cluster=False)
@env_spec(shardsCount=3)
def test_rdbcompression_does_not_affect_fingerprints(env):
    """Equal schemas remain comparable with different persistence compression settings."""
    shard_node_ids(env)
    env.expect('FT.CREATE', 'idx', 'SCHEMA',
               'a_field_name_well_over_twenty_bytes_long', 'TEXT').ok()
    before = [local_fingerprint(env, 'idx', i) for i in range(1, env.shardsCount + 1)]
    con = env.getConnection(env.shardsCount)
    prior = to_dict(con.execute_command('CONFIG', 'GET', 'rdbcompression'))['rdbcompression']
    con.execute_command('CONFIG', 'SET', 'rdbcompression', 'no' if prior == 'yes' else 'yes')
    try:
        env.assertEqual([local_fingerprint(env, 'idx', i) for i in range(1, env.shardsCount + 1)], before)
        env.assertEqual(cluster_state(env), {'idx': {'index': 'idx', 'status': 'ok'}})
    finally:
        con.execute_command('CONFIG', 'SET', 'rdbcompression', prior)


@skip(cluster=False)
@env_spec(shardsCount=3)
def test_only_withclusterstate_needs_to_block(env):
    """The token makes the command fan out, so only that form is refused where blocking
    is denied; the plain form answers locally before any such check."""
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 't', 'TEXT').ok()
    denied = 'Cannot perform `FT._LIST`: Cannot block'

    env.expect('MULTI').ok()
    env.expect('FT._LIST').equal('QUEUED')
    env.expect('FT._LIST', 'WITHCLUSTERSTATE').equal('QUEUED')
    res = env.cmd('EXEC')
    env.assertEqual(res[0], ['idx'])
    env.assertIsInstance(res[1], redis_exceptions.ResponseError)
    env.assertEqual(str(res[1]), denied)

    env.expect('EVAL', "return redis.call('FT._LIST')", '0').equal(['idx'])
    env.expect('EVAL', "return redis.call('FT._LIST', 'WITHCLUSTERSTATE')", '0') \
       .error().contains(denied)


@skip(cluster=False)
@env_spec(shardsCount=3, protocol=3)
def test_cluster_state_resp3(env):
    """RESP3: the entry and a non-"ok" status are real maps, an "ok" status is still the
    plain string."""
    shard_node_ids(env)
    create_diverged_index(env, 'idx_diverged')
    env.expect('FT.CREATE', 'idx_same', 'SCHEMA', 't', 'TEXT').ok()

    entries = {entry['index']: entry for entry in env.cmd('FT._LIST', 'WITHCLUSTERSTATE')}
    env.assertEqual(entries['idx_same'], {'index': 'idx_same', 'status': 'ok'})
    status = entries['idx_diverged']['status']
    env.assertIsInstance(status, dict, message=status)
    env.assertEqual(set(status.keys()), {'warning'})
    env.assertTrue(status['warning'].startswith(INCONSISTENT), message=status)


@skip(cluster=True)
def test_standalone_reports_every_index_ok(env):
    """A single-shard deployment answers locally: there is no second shard to disagree."""
    env.expect('FT.CREATE', 'idx1', 'SCHEMA', 't', 'TEXT').ok()
    env.expect('FT.CREATE', 'idx2', 'SCHEMA', 'v', 'NUMERIC').ok()

    env.assertEqual(cluster_state(env), {'idx1': {'index': 'idx1', 'status': 'ok'},
                                         'idx2': {'index': 'idx2', 'status': 'ok'}})
    env.assertEqual(env.cmd('FT._LIST', 'withclusterstate'),
                    env.cmd('FT._LIST', 'WITHCLUSTERSTATE'))


@skip(cluster=True)
@env_spec(protocol=3)
def test_standalone_reports_every_index_ok_resp3(env):
    """RESP3: the single-shard path renders the same map the reducer does."""
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 't', 'TEXT').ok()
    env.assertEqual(env.cmd('FT._LIST', 'WITHCLUSTERSTATE'),
                    [{'index': 'idx', 'status': 'ok'}])


@skip(cluster=False)
@env_spec(shardsCount=1)
def test_single_shard_cluster_reports_every_index_ok(env):
    """The single-shard cluster path answers locally, including inside MULTI and Lua."""
    shard_node_ids(env)
    env.expect('FT.CREATE', 'idx1', 'SCHEMA', 't', 'TEXT').ok()
    env.expect('FT.CREATE', 'idx2', 'SCHEMA', 'n', 'NUMERIC').ok()
    expected = {'idx1': {'index': 'idx1', 'status': 'ok'},
                'idx2': {'index': 'idx2', 'status': 'ok'}}
    env.assertEqual(cluster_state(env), expected)
    reply = env.cmd('FT._LIST', 'WITHCLUSTERSTATE')
    env.expect('MULTI').ok()
    env.expect('FT._LIST', 'WITHCLUSTERSTATE').equal('QUEUED')
    env.assertEqual(env.cmd('EXEC'), [reply])
    env.expect('EVAL', "return redis.call('FT._LIST', 'WITHCLUSTERSTATE')", '0').equal(reply)


@skip(cluster=False)
@env_spec(shardsCount=1, protocol=3)
def test_single_shard_cluster_resp3(env):
    """A single-shard cluster exposes the same RESP3 maps as standalone."""
    shard_node_ids(env)
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 't', 'TEXT').ok()
    env.assertEqual(env.cmd('FT._LIST', 'WITHCLUSTERSTATE'),
                    [{'index': 'idx', 'status': 'ok'}])


@skip(cluster=False)
@env_spec(shardsCount=3)
def test_empty_union_with_unreachable_shard_is_an_error(env):
    """An index only on the silent shard must not appear to be a confirmed empty list."""
    shard_node_ids(env)
    con = env.getConnection(env.shardsCount)
    con.execute_command('DEBUG', 'MARK-INTERNAL-CLIENT')
    con.execute_command('_FT.CREATE', 'idx', 'SCHEMA', 't', 'TEXT')
    env.expect(debug_cmd(), 'PAUSE_TOPOLOGY_UPDATER').ok()
    try:
        with stopped_shard(env, env.shardsCount):
            env.expect('FT._LIST', 'WITHCLUSTERSTATE').error().equal(
                INCONSISTENT + ' cannot be determined: incomplete shard reports; '
                'the index list may be incomplete.')
    finally:
        env.expect(debug_cmd(), 'RESUME_TOPOLOGY_UPDATER').ok()


@skip(cluster=False)
@env_spec(shardsCount=3)
def test_empty_cluster_returns_empty_list(env):
    """A complete set of empty shard reports confirms there are no indexes."""
    shard_node_ids(env)
    env.expect('FT._LIST', 'WITHCLUSTERSTATE').equal([])


@contextmanager
def rejecting_shard(env, shard_id, response=b"-ERR unknown command '_FT._LIST'\r\n"):
    """Replace a stopped shard with an endpoint that rejects the new internal command.

    A real internal connection bypasses ACLs, so revoking a user's permission would
    not exercise rejection. This RESP endpoint models an older shard that can
    authenticate but does not recognize _FT._LIST.
    """
    address = env.getConnection(shard_id).connection_pool.connection_kwargs
    connections = []

    class Handler(socketserver.StreamRequestHandler):
        def handle(self):
            try:
                while header := self.rfile.readline():
                    if not header.startswith(b'*'):
                        return
                    args = []
                    for _ in range(int(header[1:])):
                        length = int(self.rfile.readline()[1:])
                        args.append(self.rfile.read(length))
                        self.rfile.read(2)
                    command = args[0].upper()
                    if command == b'_FT._LIST':
                        self.wfile.write(response)
                    elif command == b'PING':
                        self.wfile.write(b'+PONG\r\n')
                    else:
                        self.wfile.write(b'+OK\r\n')
            except (ConnectionError, OSError):
                pass

    class Server(socketserver.ThreadingTCPServer):
        allow_reuse_address = True
        daemon_threads = True

        def get_request(self):
            request, address = super().get_request()
            connections.append(request)
            return request, address

    with stopped_shard(env, shard_id):
        with Server((address['host'], address['port']), Handler) as server:
            thread = threading.Thread(target=server.serve_forever)
            thread.start()
            try:
                yield
            finally:
                server.shutdown()
                thread.join()
                for connection in connections:
                    try:
                        connection.shutdown(socket.SHUT_RDWR)
                    except OSError:
                        pass


@skip(cluster=False)
@env_spec(shardsCount=3)
def test_rejecting_shard_is_not_named_unreachable(env):
    """Unattributed rejection errors suppress shard IDs, including mixed failures."""
    if env.useTLS:
        env.skip()  # The synthetic older-shard endpoint speaks plain RESP.
    shard_node_ids(env)
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 't', 'TEXT').ok()
    env.expect(debug_cmd(), 'PAUSE_TOPOLOGY_UPDATER').ok()
    try:
        with rejecting_shard(env, 2):
            def has_rejection():
                status = cluster_state(env)['idx']['status']
                return (isinstance(status, dict) and
                        'rejected the request' in status['warning']), status

            wait_for_condition(has_rejection, 'coordinator did not reconnect to rejecting shard')
            status = cluster_state(env)['idx']['status']
            env.assertEqual(status, {'warning': INCONSISTENT + ' cannot be determined: '
                "1 of 3 shards rejected the request (ERR unknown command '_FT._LIST')."})
            with stopped_shard(env, 3):
                status = cluster_state(env)['idx']['status']
                env.assertEqual(status, {'warning': INCONSISTENT + ' cannot be determined: '
                    '1 of 3 shards did not reply; '
                    "1 of 3 shards rejected the request (ERR unknown command '_FT._LIST')."})
    finally:
        env.expect(debug_cmd(), 'RESUME_TOPOLOGY_UPDATER').ok()


@skip(cluster=False)
@env_spec(shardsCount=3)
def test_malformed_shard_payload_is_not_an_empty_report(env):
    """An invalid entry cannot count as proof that an index is missing on that shard."""
    if env.useTLS:
        env.skip()
    ids = shard_node_ids(env)
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 't', 'TEXT').ok()
    env.expect(debug_cmd(), 'PAUSE_TOPOLOGY_UPDATER').ok()
    # The envelope is valid, but an integer cannot be an index name.
    response = (f'*4\r\n${len(ids[1])}\r\n{ids[1]}\r\n'
                ':3\r\n:1\r\n*1\r\n*2\r\n:42\r\n:1\r\n').encode()
    try:
        with rejecting_shard(env, 2, response):
            expected = {'warning': INCONSISTENT +
                        ' cannot be determined: 1 of 3 shards did not reply.',
                        'unreachable_shards': [ids[1]]}

            def has_incomplete_report():
                status = cluster_state(env)['idx']['status']
                return status == expected, status

            wait_for_condition(has_incomplete_report, 'malformed report was not excluded')
            env.assertEqual(cluster_state(env)['idx']['status'], expected)
    finally:
        env.expect(debug_cmd(), 'RESUME_TOPOLOGY_UPDATER').ok()


@skip(cluster=False)
@env_spec(shardsCount=3)
def test_long_index_names_do_not_collide(env):
    """Exact-name lookup retains lengths beyond the former trie-map key width."""
    shard_node_ids(env)
    names = ['x' * 65536 + 'a', 'x' * 65536 + 'b']
    for name in names:
        env.expect('FT.CREATE', name, 'SCHEMA', 't', 'TEXT').ok()
    env.assertEqual(cluster_state(env),
                    {name: {'index': name, 'status': 'ok'} for name in names})
