# -*- coding: utf-8 -*-

from common import *

'''
Behavioral tests for how the index term trie (spec->terms) handles the full
spectrum of byte inputs arriving through document indexing.

Pipeline under test:
  HSET value --toksep (src/toksep.h)--> raw tokens
             --DefaultNormalize (src/tokenize.c): strips isblank/iscntrl-->
             --unicode_tolower (src/util/strconv.h): blind nu_utf8_read decode
               to uint32 codepoints, lowercase, re-encode as UTF-8-->
             normalized term bytes, which land in TWO stores:
               1. the inverted index, keyed by the term BYTES (lossless), and
               2. spec->terms via IndexSpec_AddTerm -> Trie_InsertStringBuffer
                  (src/spec.c) -- re-decoded into uint16 runes, which is LOSSY
                  above U+FFFF and silently drops over-long terms.

Current semantics demonstrated here (readout via FT.DEBUG DUMP_TERMS, which
re-encodes trie runes with runesToStr, and via FT.SEARCH):
  * toksep iterates with a C-string loop: tokenization STOPS at the first
    NUL byte; the rest of the field value is silently never indexed.
  * Other control bytes are stripped from tokens by DefaultNormalize.
  * Invalid UTF-8 is never rejected: unicode_tolower blind-decodes it and
    re-encodes valid UTF-8, so garbage bytes are "laundered" into real
    codepoints consistently in both the inverted index and the terms trie.
  * Lone surrogates survive laundering (blind decode+encode are inverses),
    so both stores can hold invalid UTF-8, and replies built from either
    (DUMP_TERMS, FT.SPELLCHECK suggestions) can be invalid UTF-8.
  * Astral codepoints (valid UTF-8!) diverge between the two stores: the
    inverted index keeps the real 4-byte sequence, the trie truncates each
    codepoint to uint16. Exact search works (index lookup by bytes) while
    expansion driven by the terms trie (prefix, fuzzy, wildcard) rebuilds the
    WRONG bytes and finds nothing. WITHSUFFIXTRIE is the exception: the
    suffix trie carries the original term bytes as a payload, so contains and
    suffix queries survive what prefix queries lose
    (test_suffix_trie_encoding.py).
  * Terms longer than the trie's insert limit are dropped from the trie but
    still indexed: another way for a searchable term to be invisible to
    every trie-driven expansion.
'''


def dump_terms_raw(env, idx='idx'):
    '''DUMP_TERMS without response decoding: replies may be arbitrary bytes.'''
    return sorted(env.cmd(debug_cmd(), 'DUMP_TERMS', idx, **{NEVER_DECODE: []}))

def create_index(env):
    env.skipOnCluster()
    env.expect('ft.create', 'idx', 'ON', 'HASH',
               'SCHEMA', 't', 'TEXT', 'NOSTEM').ok()

def add_doc(env, key, value):
    with env.getClusterConnectionIfNeeded() as r:
        env.assertEqual(r.execute_command('hset', key, 't', value), 1)
    waitForIndex(env, 'idx')

def num_results(env, query):
    return env.cmd('ft.search', 'idx', query, 'NOCONTENT')[0]


# ============================ VALID UTF-8 ============================
# Field values that decode cleanly: every codepoint <= U+FFFF round-trips.

def testValidTermsCaseFoldedRoundtrip(env):
    create_index(env)
    add_doc(env, 'doc1', 'Hello Café 日本語')
    # Terms are case-folded on the way in, by unicode_tolower.
    env.assertEqual(dump_terms_raw(env),
                    sorted(['café'.encode(), b'hello', '日本語'.encode()]))
    env.assertEqual(num_results(env, 'hello'), 1)
    env.assertEqual(num_results(env, 'café'), 1)
    env.assertEqual(num_results(env, '日本語'), 1)


# ==================== ASTRAL PLANE (valid UTF-8) ====================
# The normalized term bytes keep the real 4-byte sequences (unicode_tolower
# works on uint32 codepoints), so the inverted index key is intact. But the
# terms-trie insert truncates each codepoint to uint16, so the trie holds
# U+F600 instead of U+1F600 -- and every query path that goes through the
# trie rebuilds the wrong bytes.

def testAstralExactSearchWorks(env):
    create_index(env)
    add_doc(env, 'doc1', '\U0001F600\U0001F600')
    # Exact term search hits the inverted index by bytes: lossless.
    env.assertEqual(num_results(env, '\U0001F600\U0001F600'), 1)

def testAstralTermTrieTruncated(env):
    create_index(env)
    add_doc(env, 'doc1', '\U0001F600\U0001F600')
    # The trie stored truncated runes: DUMP_TERMS shows U+F600 U+F600.
    env.assertEqual(dump_terms_raw(env), ['\uf600\uf600'.encode()])

def testAstralPrefixSearchFindsNothing(env):
    create_index(env)
    add_doc(env, 'doc1', '\U0001F600\U0001F600')
    # Prefix expansion walks the trie (truncated runes match fine), then
    # re-encodes each candidate with runesToStr -> U+F600 bytes -> inverted
    # index lookup misses the real U+1F600 key. Valid-UTF-8 input, zero hits.
    env.assertEqual(num_results(env, '\U0001F600*'), 0)
    # Same divergence in reverse: querying by the truncated rune the trie
    # actually stores DOES match in the trie, but the re-encoded candidate
    # term has no inverted-index entry either.
    env.assertEqual(num_results(env, '\uf600*'), 0)

def testAstralFuzzyAndWildcardFindNothing(env):
    create_index(env)
    add_doc(env, 'doc1', '\U0001F600\U0001F600')
    # Prefix is not a special case: every expansion that resolves candidates
    # through the terms trie re-encodes them with runesToStr and misses the
    # real inverted-index key the same way.
    env.assertEqual(num_results(env, '%\U0001F600\U0001F600%'), 0)
    env.assertEqual(num_results(env, "w'\U0001F600*'"), 0)


# =================== CURRENT LENIENT BEHAVIOR =======================
# Invalid bytes and NULs in document field values: never rejected, always
# rewritten into whatever the decoder makes of them.

def testNulStopsTokenization(env):
    create_index(env)
    # toksep's scan loop is NUL-terminated: 'bar' after the NUL is never seen.
    add_doc(env, 'doc1', b'foo\x00bar')
    env.assertEqual(dump_terms_raw(env), [b'foo'])
    env.assertEqual(num_results(env, 'foo'), 1)
    env.assertEqual(num_results(env, 'bar'), 0)

def testControlCharsStrippedFromToken(env):
    create_index(env)
    # In contrast to NUL, other control bytes are stripped by
    # DefaultNormalize's iscntrl() filter and tokenization continues:
    # 'a\x01b' indexes as a single term 'ab'.
    add_doc(env, 'doc1', b'a\x01b')
    env.assertEqual(dump_terms_raw(env), [b'ab'])
    env.assertEqual(num_results(env, 'ab'), 1)

def testInvalidBytesLaunderedIntoValidTerm(env):
    create_index(env)
    # 0xC3 0xC3 is invalid (0xC3 is not a continuation byte). The blind
    # decoder reads it as (0xC3 & 0x03) << 6 | (0xC3 & 0x3F) = U+00C3 'Ã',
    # which then case-folds to 'ã'. Both the inverted index and the trie
    # agree on the laundered term, so it is searchable -- but only by bytes
    # the user never wrote.
    add_doc(env, 'doc1', b'\xc3\xc3')
    env.assertEqual(dump_terms_raw(env), ['ã'.encode()])
    env.assertEqual(num_results(env, 'ã'), 1)

def testSurrogateBytesIndexedVerbatim(env):
    create_index(env)
    # 0xED 0xA0 0x80 (lone surrogate U+D800, invalid UTF-8) survives the
    # normalize pipeline verbatim: blind decode to 0xD800, no case mapping,
    # re-encode to the same three bytes. Both stores hold invalid UTF-8.
    add_doc(env, 'doc1', b'\xed\xa0\x80')
    env.assertEqual(dump_terms_raw(env), [b'\xed\xa0\x80'])

def testSurrogateTermSurfacesInSpellCheckReply(env):
    create_index(env)
    # FT.SPELLCHECK suggestions come from spec->terms (no dictionaries
    # involved): the surrogate term is one edit from 'x', its re-encoded
    # bytes hit the inverted index (so it gets a real score), and the reply
    # contains invalid UTF-8.
    add_doc(env, 'doc1', b'\xed\xa0\x80')
    res = env.cmd('ft.spellcheck', 'idx', 'x', **{NEVER_DECODE: []})
    env.assertEqual(res, [[b'TERM', b'x', [[b'1', b'\xed\xa0\x80']]]])


# ================= INVALID UTF-8 ABOVE THE BMP ======================
# The blind decoder sizes a sequence from its lead byte alone, so invalid
# bytes can decode ABOVE U+FFFF and hit the astral truncation from the
# other side. There the damage compounds: F5 80 80 80 decodes to U+140000,
# whose low 16 bits are ZERO, and strToRunesN stops at a zero codepoint.
# The trie entry is therefore CUT SHORT at the offending character, leaving
# a phantom prefix behind that no document ever contained -- unlike the
# valid-astral case, where the trie holds a wrong term of the right length.

def testAboveBmpInvalidBytesCutTermShortInTrie(env):
    create_index(env)
    add_doc(env, 'doc1', b'caf\xf5\x80\x80\x80e')
    # Everything from the truncated codepoint onwards is gone from the trie.
    env.assertEqual(dump_terms_raw(env), [b'caf'])
    # The inverted index is keyed by the laundered bytes, which are intact,
    # so exact search - which never consults the trie - still finds the doc.
    env.assertEqual(num_results(env, b'caf\xf5\x80\x80\x80e'), 1)

def testAboveBmpPhantomTermMatchesNothing(env):
    create_index(env)
    add_doc(env, 'doc1', b'caf\xf5\x80\x80\x80e')
    # 'caf' is in the trie but names no inverted index entry, so it is not
    # a searchable term, and prefix expansion resolves to it and nothing else.
    env.assertEqual(num_results(env, 'caf'), 0)
    env.assertEqual(num_results(env, 'caf*'), 0)

def testAboveBmpPrefixReachesOnlyBmpSibling(env):
    create_index(env)
    add_doc(env, 'doc1', b'caf\xf5\x80\x80\x80e')
    # E9 41 42 is invalid too, but decodes to U+9042 - inside the BMP, so it
    # survives the rune round-trip and stays reachable.
    add_doc(env, 'doc2', b'caf\xe9\x41\x42')
    env.assertEqual(dump_terms_raw(env), [b'caf', 'caf遂'.encode()])
    # One prefix, two trie hits, one reachable document.
    env.assertEqual(num_results(env, 'caf*'), 1)


# ================ SUFFIX TRIE KEEPS THE ORIGINAL BYTES ==============
# WITHSUFFIXTRIE keeps the original term bytes, so contains and suffix queries
# escape the astral loss the prefix path suffers. Only that contrast with
# spec->terms is pinned here; test_suffix_trie_encoding.py owns the suffix
# store's own characteristics.

def testSuffixQueriesReachAstralTerm(env):
    env.skipOnCluster()
    env.expect('ft.create', 'idx', 'ON', 'HASH',
               'SCHEMA', 't', 'TEXT', 'NOSTEM', 'WITHSUFFIXTRIE').ok()
    add_doc(env, 'doc1', 'x\U0001F600y')
    # The terms trie holds the truncated rune, as it does without a suffix trie.
    env.assertEqual(dump_terms_raw(env), ['x\uf600y'.encode()])
    # Contains and suffix queries resolve candidates from the suffix trie
    # payload and hit the real inverted-index key...
    env.assertEqual(num_results(env, '*\U0001F600*'), 1)
    env.assertEqual(num_results(env, '*\U0001F600y'), 1)
    # ...while the prefix query for the same document still finds nothing:
    # prefixes are resolved through the terms trie, which lost the codepoint.
    env.assertEqual(num_results(env, 'x\U0001F600*'), 0)


# ========================== LENGTH LIMITS ===========================
# The inverted index has no length gate, the terms trie does: terms above the
# limit are indexed and exactly searchable, yet absent from the trie. Same
# observable shape as the above-BMP case, reached without any invalid input.

def testLongTermIndexedButMissingFromTrie(env):
    create_index(env)
    add_doc(env, 'doc1', 'a' * 255)
    # Trie_InsertStringBuffer rejects terms longer than 512 bytes and
    # IndexSpec_AddTerm discards that rejection without logging or counting it.
    add_doc(env, 'doc2', 'b' * 600)
    env.assertEqual(dump_terms_raw(env), [b'a' * 255])
    env.assertEqual(num_results(env, 'b' * 600), 1)
    env.assertEqual(num_results(env, 'bbb*'), 0)

def testLongMultibyteTermGateIsBytes(env):
    create_index(env)
    # The gate counts raw bytes, not runes, so a multibyte term is dropped at
    # well under 256 runes: 200 three-byte characters are 600 bytes.
    add_doc(env, 'doc1', '日' * 200)
    env.assertEqual(dump_terms_raw(env), [])
    env.assertEqual(num_results(env, '日' * 200), 1)


# ===================== RDB RELOAD (RE-INDEXING) =====================
# spec->terms of a regular in-memory index is NOT serialized to RDB
# (IndexSpec_RdbSave only writes the terms trie for
# disk/SST-backed specs; the other TrieType_GenericLoad site is the legacy
# pre-2.0 path). On load, documents fire 'loaded' keyspace notifications and
# every doc re-runs the full tokenize -> normalize -> trie pipeline. The
# reload equality below therefore holds because that pipeline is
# DETERMINISTIC, not because the trie bytes round-trip: truncated-astral and
# surrogate terms are re-created from the (binary-safe) hash values.

def testRdbRoundtripTermsTrie(env):
    create_index(env)
    add_doc(env, 'doc1', 'Hello')
    add_doc(env, 'doc2', '\U0001F600\U0001F600')
    add_doc(env, 'doc3', b'\xed\xa0\x80')
    before = dump_terms_raw(env)
    env.assertEqual(len(before), 3)
    env.dumpAndReload()
    waitForIndex(env, 'idx')
    env.assertEqual(dump_terms_raw(env), before)
    # Re-indexing also rebuilds the bytes-keyed inverted index: exact astral
    # search still works, and the trie/index divergence is reproduced too.
    env.assertEqual(num_results(env, '\U0001F600\U0001F600'), 1)
    env.assertEqual(num_results(env, '\U0001F600*'), 0)
