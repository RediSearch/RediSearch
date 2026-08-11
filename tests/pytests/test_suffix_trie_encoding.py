# -*- coding: utf-8 -*-

from common import *

'''
Behavioral tests for how the WITHSUFFIXTRIE suffix trie of a TEXT field
handles the full spectrum of byte inputs arriving through document indexing.

The store is spec->suffix, a rune-keyed Trie (src/suffix.c: addSuffixTrie).
Every term is decoded into uint16 runes, and one entry is inserted per suffix
of the rune sequence. The PAYLOAD (suffixData) holds the original term BYTES,
so candidates handed back to the inverted index escape the rune round-trip --
but the KEYS are runes and are therefore lossy above U+FFFF, exactly like
spec->terms.

Current semantics demonstrated here (readout via FT.DEBUG DUMP_SUFFIX_TRIE,
which replies with the runesToStr-re-encoded trie KEYS, and via
contains/suffix queries):
  * Keys are truncated above U+FFFF while the payload keeps the real bytes:
    contains and suffix queries reach an astral term that prefix queries
    (terms-trie driven) cannot, and a query for the truncated BMP codepoint
    reaches it too.
  * That divergence is one-sided. Suffix_CB_Wildcard deliberately skips
    candidates containing a supplementary codepoint so the suffix-trie path
    stays result-identical with the brute-force scan; Suffix_IterateContains
    has no such filter, so contains/suffix return documents the brute-force
    path never returns.
  * Terms are identified inside the suffix trie by their RUNE key, so two
    byte-distinct terms that truncate to the same runes collide: the term
    indexed second is dropped from the store entirely, and every contains or
    suffix query answers with the first one. Both remain exactly searchable
    through the (byte-keyed) inverted index.
  * The insert path applies its length gate to the suffixes only, so the
    full-word entry is stored at any length. A long term is therefore
    reachable by a query spelling it out in full, and by shorter suffixes of
    it, but not by the suffixes in between.
  * The query-side minimum length (MINPREFIX) counts BYTES, so a one-character
    pattern is rejected or accepted depending on how that character encodes.
'''

# U+1F600 GRINNING FACE. Valid UTF-8, four bytes, above the uint16 rune range.
ASTRAL = '\U0001F600'
# The rune the suffix trie actually stores for ASTRAL: the low 16 bits.
BMP_TWIN = chr(0xF600)

# 300 runes of plain ASCII: past the 256-rune insert gate, well under the
# 512-byte one, so the rune gate is the only thing that fires.
LONG = ''.join(chr(ord('a') + (i * 7) % 26) for i in range(300))
# 255 runes of the same alphabet: the longest term the terms trie accepts.
LONG_255 = LONG[:255]


def create_index(env, with_suffix_trie=True):
    env.skipOnCluster()
    schema = ['t', 'TEXT', 'NOSTEM'] + (['WITHSUFFIXTRIE'] if with_suffix_trie else [])
    env.expect('ft.create', 'idx', 'ON', 'HASH', 'SCHEMA', *schema).ok()

def add_doc(env, key, value):
    with env.getClusterConnectionIfNeeded() as r:
        env.assertEqual(r.execute_command('hset', key, 't', value), 1)
    waitForIndex(env, 'idx')

def dump_suffix_raw(env):
    '''DUMP_SUFFIX_TRIE without response decoding: keys may be arbitrary bytes.'''
    return sorted(env.cmd(debug_cmd(), 'DUMP_SUFFIX_TRIE', 'idx',
                          **{NEVER_DECODE: []}))

def ids(env, query):
    return sorted(env.cmd('ft.search', 'idx', query, 'NOCONTENT')[1:])

def num_results(env, query):
    return env.cmd('ft.search', 'idx', query, 'NOCONTENT')[0]


# ============ RUNE KEYS, BYTE PAYLOAD (valid UTF-8 input) ============
# The suffix trie stores every term twice over: as rune keys, and as the
# original bytes inside the payload. Above U+FFFF the two disagree.

def testSuffixKeysAreRunesPayloadIsOriginalBytes(env):
    create_index(env)
    add_doc(env, 'doc1', 'w' + ASTRAL + 'y')
    # One entry per suffix of the 3-rune term, each key re-encoded from the
    # truncated runes.
    env.assertEqual(dump_suffix_raw(env),
                    sorted([('w' + BMP_TWIN + 'y').encode(),
                            (BMP_TWIN + 'y').encode(),
                            b'y']))
    # The payload kept the real bytes, so the candidate looked up in the
    # inverted index is the term as indexed, not the re-encoded key.
    env.assertEqual(ids(env, '*' + ASTRAL + '*'), ['doc1'])
    env.assertEqual(ids(env, '*' + ASTRAL + 'y'), ['doc1'])
    # Prefix queries resolve candidates through the terms trie instead, and
    # that store has no byte-preserving payload to fall back on.
    env.assertEqual(num_results(env, 'w' + ASTRAL + '*'), 0)

def testTruncatedRuneQueryReachesAstralTerm(env):
    create_index(env)
    add_doc(env, 'doc1', 'w' + ASTRAL + 'y')
    # The query pattern is converted to runes by the same lossy path, so a
    # query for U+F600 lands on the node holding U+1F600 and returns a
    # document that does not contain the requested codepoint at all.
    env.assertEqual(ids(env, '*' + BMP_TWIN + '*'), ['doc1'])
    env.assertEqual(ids(env, '*' + BMP_TWIN + 'y'), ['doc1'])

def testSuffixTrieChangesResultsForAstralTerms(env):
    create_index(env, with_suffix_trie=False)
    add_doc(env, 'doc1', 'w' + ASTRAL + 'y')
    # Without the suffix trie, contains and suffix queries brute-force the
    # terms trie and re-encode each candidate, so they miss the real key.
    # WITHSUFFIXTRIE is documented as an optimization, but for these terms it
    # decides whether the document is found at all.
    env.assertEqual(num_results(env, '*' + ASTRAL + '*'), 0)
    env.assertEqual(num_results(env, '*' + BMP_TWIN + '*'), 0)
    env.assertEqual(num_results(env, 'w' + ASTRAL + 'y'), 1)
    # Wildcard is the half of the divergence that does NOT change: these same
    # queries answer 0 with the suffix trie too.
    env.assertEqual(num_results(env, "w'*" + ASTRAL + "*'"), 0)
    env.assertEqual(num_results(env, "w'*" + BMP_TWIN + "*'"), 0)

def testWildcardSkipsAstralCandidates(env):
    create_index(env)
    add_doc(env, 'doc1', 'w' + ASTRAL + 'y')
    # Suffix_CB_Wildcard drops any candidate holding a supplementary
    # codepoint, matching the no-suffix-trie answers pinned in
    # testSuffixTrieChangesResultsForAstralTerms. Suffix_IterateContains has
    # no such filter, hence the split verdict below.
    env.assertEqual(num_results(env, "w'*" + ASTRAL + "*'"), 0)
    env.assertEqual(num_results(env, "w'*" + BMP_TWIN + "*'"), 0)
    env.assertEqual(num_results(env, '*' + ASTRAL + '*'), 1)


# ======================== RUNE-KEY COLLISIONS ========================
# addSuffixTrie asks the trie whether the term's rune key already carries a
# term and returns early if it does. The check cannot distinguish two terms
# that truncate to the same runes.

def testTwoTermsShareSuffixNodeWithoutCollision(env):
    create_index(env)
    add_doc(env, 'doc_a', 'zqay')
    add_doc(env, 'doc_b', 'zqby')
    # Baseline: distinct rune keys, so both terms are stored and a shared
    # suffix node reports both documents.
    env.assertEqual(ids(env, '*zq*'), ['doc_a', 'doc_b'])

def testRuneCollisionDropsSecondTerm(env):
    create_index(env)
    add_doc(env, 'doc_bmp', 'x' + BMP_TWIN + 'y')
    add_doc(env, 'doc_astral', 'x' + ASTRAL + 'y')
    # The second term found a node that already carries a term and returned
    # without adding itself -- not even to that node's candidate array. The
    # store looks exactly as it would with the first document alone.
    env.assertEqual(dump_suffix_raw(env),
                    sorted([('x' + BMP_TWIN + 'y').encode(),
                            (BMP_TWIN + 'y').encode(),
                            b'y']))
    # Every contains or suffix query therefore answers with the first term,
    # whichever of the two codepoints was asked for.
    env.assertEqual(ids(env, '*' + ASTRAL + '*'), ['doc_bmp'])
    env.assertEqual(ids(env, '*' + BMP_TWIN + '*'), ['doc_bmp'])
    # Both documents stay exactly searchable: the inverted index is keyed by
    # the term bytes and never collapses the pair.
    env.assertEqual(ids(env, 'x' + ASTRAL + 'y'), ['doc_astral'])
    env.assertEqual(ids(env, 'x' + BMP_TWIN + 'y'), ['doc_bmp'])

def testRuneCollisionSurvivorIsTheTermIndexedFirst(env):
    create_index(env)
    add_doc(env, 'doc_astral', 'x' + ASTRAL + 'y')
    add_doc(env, 'doc_bmp', 'x' + BMP_TWIN + 'y')
    # Same pair, reversed indexing order: now the astral term is the one
    # stored, so the same two queries answer with the other document. Which
    # document a contains query returns depends on indexing order alone --
    # including the order documents are re-indexed in after a reload.
    env.assertEqual(dump_suffix_raw(env),
                    sorted([('x' + BMP_TWIN + 'y').encode(),
                            (BMP_TWIN + 'y').encode(),
                            b'y']))
    env.assertEqual(ids(env, '*' + ASTRAL + '*'), ['doc_astral'])
    env.assertEqual(ids(env, '*' + BMP_TWIN + '*'), ['doc_astral'])

def testRuneCollisionDeletingWinnerHidesLoser(env):
    create_index(env)
    add_doc(env, 'doc_bmp', 'x' + BMP_TWIN + 'y')
    add_doc(env, 'doc_astral', 'x' + ASTRAL + 'y')
    with env.getClusterConnectionIfNeeded() as r:
        env.assertEqual(r.execute_command('del', 'doc_bmp'), 1)
    # Suffix entries are removed by the fork GC.
    forceInvokeGC(env, 'idx')
    # The deleted document's term was the only one ever stored, so the whole
    # trie goes with it -- and the surviving document, whose term was dropped
    # at indexing time, is now unreachable by contains or suffix while still
    # being indexed and exactly searchable.
    env.assertEqual(dump_suffix_raw(env), [])
    env.assertEqual(num_results(env, '*' + ASTRAL + '*'), 0)
    env.assertEqual(ids(env, 'x' + ASTRAL + 'y'), ['doc_astral'])


# ===================== INSERT-SIDE LENGTH GATES ======================
# addSuffixTrie inserts the full word with Trie_InsertRuneNoSize (no length
# guard) and every proper suffix with Trie_InsertRune (guarded at
# TRIE_INITIAL_STRING_LEN runes). Reachability is therefore not monotonic in
# the length of the queried pattern.
#
# None of these tests dump the trie: TrieIterator caps the key it rebuilds at
# TRIE_INITIAL_STRING_LEN runes, and a term this long walks straight into
# that cap.

def testLongTermReachableOnlyThroughTheSuffixTrie(env):
    create_index(env)
    add_doc(env, 'doc1', LONG)
    add_doc(env, 'doc2', LONG_255)
    # The terms trie takes the 255-rune term and rejects the 300-rune one, so
    # prefix expansion sees only the shorter document.
    env.assertEqual(ids(env, LONG[:20] + '*'), ['doc2'])
    # Both are exactly searchable, and the suffix trie reaches both.
    env.assertEqual(ids(env, LONG), ['doc1'])
    env.assertEqual(ids(env, '*' + LONG[-10:]), ['doc1'])
    env.assertEqual(ids(env, '*' + LONG_255[-10:]), ['doc2'])

def testSuffixEntriesStopBelowTheFullWordEntry(env):
    create_index(env)
    add_doc(env, 'doc1', LONG)
    # Suffixes up to the insert limit are stored...
    env.assertEqual(num_results(env, '*' + LONG[-254:]), 1)
    env.assertEqual(num_results(env, '*' + LONG[-255:]), 1)
    # ...longer ones are silently dropped...
    env.assertEqual(num_results(env, '*' + LONG[-256:]), 0)
    env.assertEqual(num_results(env, '*' + LONG[-260:]), 0)
    # ...yet the full word, inserted through the unguarded entry point, is
    # there: asking for a longer suffix succeeds where a shorter one failed.
    env.assertEqual(num_results(env, '*' + LONG), 1)

def testContainsWalksIntoTheFullWordEntry(env):
    create_index(env)
    add_doc(env, 'doc1', LONG)
    # A contains pattern is matched against key prefixes rather than whole
    # keys, so it reaches the full-word entry and is unaffected by the gate
    # that hides the long suffixes.
    env.assertEqual(num_results(env, '*' + LONG[140:150] + '*'), 1)
    env.assertEqual(num_results(env, '*' + LONG[:260] + '*'), 1)


# =============== QUERY-SIDE MINIMUM LENGTH (IN BYTES) ================
# Contains and suffix patterns shorter than MINPREFIX are answered with an
# empty result rather than an error. The comparison is against the pattern's
# byte length, so it is really a limit on encoded size.

def testMinimumPatternLengthCountsBytesNotCharacters(env):
    create_index(env)
    add_doc(env, 'doc1', 'wéz x日y')
    # One ASCII character is one byte: below the default MINPREFIX of 2.
    env.assertEqual(num_results(env, '*z'), 0)
    env.assertEqual(num_results(env, '*x*'), 0)
    # One non-ASCII character is two or three bytes, so the identical query
    # shape passes the gate.
    env.assertEqual(ids(env, '*é*'), ['doc1'])
    env.assertEqual(ids(env, '*日*'), ['doc1'])


# ================= RDB RELOAD (RE-INDEXING) =========================
# The suffix trie is not serialized: it is rebuilt from the documents by the
# same indexing path that filled it originally. What survives a reload is
# therefore whatever that path is deterministic about.

def testGatesReproduceAcrossReload(env):
    create_index(env)
    add_doc(env, 'doc1', LONG)
    env.dumpAndReload()
    env.assertEqual(num_results(env, '*' + LONG[-10:]), 1)
    env.assertEqual(num_results(env, '*' + LONG[-256:]), 0)
    env.assertEqual(num_results(env, '*' + LONG), 1)

def testCollisionSurvivesReloadWithoutAgreeingOnAWinner(env):
    create_index(env)
    add_doc(env, 'doc_bmp', 'x' + BMP_TWIN + 'y')
    add_doc(env, 'doc_astral', 'x' + ASTRAL + 'y')
    env.dumpAndReload()
    # Documents are re-indexed in keyspace order, which is not the order they
    # were added in, so which term wins the collision is not pinned here --
    # only that one of them does, and that both queries agree on it.
    winner = ids(env, '*' + ASTRAL + '*')
    env.assertEqual(len(winner), 1)
    env.assertEqual(ids(env, '*' + BMP_TWIN + '*'), winner)
    # Exact search is unaffected by which one that is.
    env.assertEqual(ids(env, 'x' + ASTRAL + 'y'), ['doc_astral'])
    env.assertEqual(ids(env, 'x' + BMP_TWIN + 'y'), ['doc_bmp'])
