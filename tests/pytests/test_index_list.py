# Copyright (c) 2006-Present, Redis Ltd.
# All rights reserved.
#
# Licensed under your choice of the Redis Source Available License 2.0
# (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
# GNU Affero General Public License v3 (AGPLv3).

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

    # The gates - fingerprint recipe and index encoding version - agree here, and the
    # recipe carries the shard's own rdbcompression setting.
    env.assertEqual(len(set((p[1], p[2]) for p in payloads)), 1)
    compression = to_dict(conns[0].execute_command('CONFIG', 'GET', 'rdbcompression'))
    env.assertEqual(payloads[0][1] % 2, 1 if compression['rdbcompression'] == 'yes' else 0)

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

    # Put the last shard in a gate group of its own. The other two still agree with each
    # other, and their schemas differ, so that divergence stays provable.
    con = env.getConnection(env.shardsCount)
    prior = to_dict(con.execute_command('CONFIG', 'GET', 'rdbcompression'))['rdbcompression']
    con.execute_command('CONFIG', 'SET', 'rdbcompression',
                        'no' if prior == 'yes' else 'yes')
    try:
        status = cluster_state(env)['idx']['status']
        env.assertEqual(set(status.keys()), {'warning'})
        env.assertEqual(status['warning'],
                        INCONSISTENT + ': the shards that have it hold 2 different'
                        ' schemas. Drop the index and recreate it so that all shards'
                        ' agree. The rest of the picture cannot be determined: shards'
                        ' are running incompatible versions or configurations.')
    finally:
        con.execute_command('CONFIG', 'SET', 'rdbcompression', prior)


@skip(cluster=False)
@env_spec(shardsCount=3)
def test_rdbcompression_skew_reads_as_undetermined(env):
    """Shards that disagree on rdbcompression cannot have their fingerprints compared,
    and must read as undetermined rather than as a schema mismatch."""
    shard_node_ids(env)
    # The long field name is incidental: what makes the shards incomparable is the gate
    # itself, not a proven difference in the bytes the fingerprint is taken over.
    env.expect('FT.CREATE', 'idx', 'SCHEMA',
               'a_field_name_well_over_twenty_bytes_long', 'TEXT').ok()

    con = env.getConnection(env.shardsCount)
    prior = to_dict(con.execute_command('CONFIG', 'GET', 'rdbcompression'))['rdbcompression']
    con.execute_command('CONFIG', 'SET', 'rdbcompression', 'no')
    try:
        status = cluster_state(env)['idx']['status']
        env.assertEqual(set(status.keys()), {'warning'})
        env.assertEqual(status['warning'], INCONSISTENT + ' cannot be determined: shards are'
                        ' running incompatible versions or configurations.')
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
