# -*- coding: utf-8 -*-

from common import *

'''
Behavioral tests for how the spellcheck dictionary (FT.DICTADD / FT.DICTDEL /
FT.DICTDUMP) handles the full spectrum of byte inputs.

Pipeline under test (src/dictionary.c -> src/trie/trie.c -> src/trie/rune_util.c):
  DICTADD:  bytes --strToRunesN/nu_utf8_read--> uint16 runes --> rune trie
  DICTDUMP: runes --runesToStr/nu_utf8_write--> UTF-8 bytes

Current (lenient) semantics demonstrated here:
  * The UTF-8 decoder (libnu nu_utf8_read) performs NO validation. Invalid
    sequences are decoded blindly from the lead byte's bit pattern; nothing
    is ever rejected.
  * Runes are uint16: codepoints above U+FFFF (astral plane) are silently
    truncated to their low 16 bits, so distinct emoji can collide.
  * Decoding stops at the first NUL codepoint: embedded NULs truncate terms.
  * DICTDUMP re-encodes stored runes as UTF-8, so for any non-canonical input
    the dumped bytes differ from the added bytes. The conversion is
    deterministic, so DICTDEL with the same original bytes still deletes.
  * DICTDUMP can even emit invalid UTF-8 (lone surrogates survive the
    round-trip), so clients must treat replies as binary.

Forward-looking note (strict UTF-8 mode): when input validation lands,
the tests below split into two groups:
  * "valid" tests (testValid*) pin behavior that must keep working unchanged.
  * "lenient" tests (everything under the CURRENT LENIENT BEHAVIOR banner)
    pin today's garbage-in behavior. Under strict mode each DICTADD/DICTDEL
    call with invalid UTF-8 should instead be asserted to reply with an
    error and leave the dictionary untouched, and the mangled-round-trip
    assertions disappear (valid input always round-trips byte-identically).
    Astral-plane truncation (testAstral*) is the interesting middle case:
    the input IS valid UTF-8, so strict validation alone does not fix it --
    either runes widen to uint32 (then these become round-trip tests) or
    supplementary-plane input must be explicitly rejected.
'''


def dump_raw(env, dict_name):
    '''DICTDUMP without response decoding: replies may be arbitrary bytes.'''
    return sorted(env.cmd('ft.dictdump', dict_name, **{NEVER_DECODE: []}))


# ============================ VALID UTF-8 ============================
# These pin behavior that must survive a future strict-UTF-8 mode as-is.

def testValidAsciiRoundtrip(env):
    env.expect('ft.dictadd', 'dict', 'hello', 'world').equal(2)
    env.assertEqual(dump_raw(env, 'dict'), [b'hello', b'world'])
    env.expect('ft.dictdel', 'dict', 'hello', 'world').equal(2)

def testValidBmpMultibyteRoundtrip(env):
    # 2-byte (é), 3-byte (日) sequences: all codepoints <= U+FFFF round-trip.
    terms = ['café', 'über', '日本語', 'привет']
    env.expect('ft.dictadd', 'dict', *terms).equal(4)
    env.assertEqual(dump_raw(env, 'dict'), sorted(t.encode() for t in terms))
    env.expect('ft.dictdel', 'dict', *terms).equal(4)

def testValidCaseSensitive(env):
    # The dictionary trie does not case-fold: FOO and foo are distinct.
    env.expect('ft.dictadd', 'dict', 'FOO', 'foo').equal(2)
    env.assertEqual(dump_raw(env, 'dict'), [b'FOO', b'foo'])

def testValidDuplicateCounting(env):
    env.expect('ft.dictadd', 'dict', 'term').equal(1)
    env.expect('ft.dictadd', 'dict', 'term').equal(0)


# ==================== ASTRAL PLANE (valid UTF-8) ====================
# Valid input, mangled anyway: runes are uint16, codepoints > U+FFFF are
# truncated to their low 16 bits (strToRunesN's `(rune)cp` cast).
# Strict-mode note: input validation alone does NOT fix these.

def testAstralCodepointTruncated(env):
    # U+1F600 (grinning face) is stored as U+F600: DICTDUMP returns a
    # different (BMP, private-use) character than what was added.
    env.expect('ft.dictadd', 'dict', '\U0001F600').equal(1)
    env.assertEqual(dump_raw(env, 'dict'), ['\uf600'.encode()])

def testAstralCodepointCollision(env):
    # U+1F600 and U+2F600 share low 16 bits -> same trie entry.
    env.expect('ft.dictadd', 'dict', '\U0001F600').equal(1)
    env.expect('ft.dictadd', 'dict', '\U0002F600').equal(0)
    # Deleting via either spelling removes the single shared entry.
    env.expect('ft.dictdel', 'dict', '\U0002F600').equal(1)
    env.expect('ft.dictdump', 'dict').equal([])


# =================== CURRENT LENIENT BEHAVIOR =======================
# Everything below feeds bytes that are NOT valid UTF-8 (or contain NULs).
# Under a future strict-UTF-8 mode, each DICTADD/DICTDEL below should be
# replaced with .error() assertions (input rejected, dictionary untouched).

def testEmbeddedNulTruncates(env):
    # Decoding stops at the first NUL codepoint: 'foo\0bar' is stored as 'foo'.
    env.expect('ft.dictadd', 'dict', b'foo\x00bar').equal(1)
    env.assertEqual(dump_raw(env, 'dict'), [b'foo'])
    # Everything after the NUL is ignored, so 'foo\0baz' is a duplicate...
    env.expect('ft.dictadd', 'dict', b'foo\x00baz').equal(0)
    # ...and any 'foo\0<suffix>' deletes the shared 'foo' entry.
    env.expect('ft.dictdel', 'dict', b'foo\x00qux').equal(1)
    env.expect('ft.dictdump', 'dict').equal([])

def testLoneNulNotAdded(env):
    # A term that decodes to zero runes is silently dropped (reply 0, no error).
    env.expect('ft.dictadd', 'dict', b'\x00').equal(0)
    env.expect('ft.dictdump', 'dict').equal([])

def testOverlongNulTruncates(env):
    # 0xC0 0x80 is an (invalid) overlong encoding of NUL. The decoder does not
    # reject it; it decodes to codepoint 0 and truncates, like a real NUL.
    env.expect('ft.dictadd', 'dict', b'ab\xc0\x80cd').equal(1)
    env.assertEqual(dump_raw(env, 'dict'), [b'ab'])

def testInvalid2ByteSequenceDecodedBlindly(env):
    # 0xC3 0x28 is invalid ('(' is not a continuation byte). The decoder
    # combines the payload bits anyway: (0xC3 & 0x03) << 6 | (0x28 & 0x3F)
    # = U+00E8 'è'. Both bytes are consumed as one bogus codepoint.
    env.expect('ft.dictadd', 'dict', b'\xc3\x28').equal(1)
    env.assertEqual(dump_raw(env, 'dict'), ['è'.encode()])
    # The conversion is deterministic, so the original bytes still delete it.
    env.expect('ft.dictdel', 'dict', b'\xc3\x28').equal(1)

def testLoneContinuationByteSwallowsNextByte(env):
    # 0x80 is a bare continuation byte. The decoder misreads it as a 2-byte
    # lead and consumes the following 'a' with it: (0x80 & 0x03) << 6 |
    # (0x61 & 0x3F) = U+0021 '!'. Input '\x80abc' is stored as '!bc'.
    env.expect('ft.dictadd', 'dict', b'\x80abc').equal(1)
    env.assertEqual(dump_raw(env, 'dict'), [b'!bc'])

def testUndefinedLeadByteSwallowsThreeBytes(env):
    # 0xFF is not a legal lead byte; the decoder treats anything >= 0xF0 as a
    # 4-byte sequence, consuming 0xFF plus 'abc' as one codepoint (which is
    # then also > U+FFFF and truncated to uint16). Only 'd' survives intact.
    env.expect('ft.dictadd', 'dict', b'\xffabcd').equal(1)
    dumped = dump_raw(env, 'dict')
    env.assertEqual(len(dumped), 1)
    # One mangled leading character followed by the surviving 'd'.
    env.assertTrue(dumped[0].endswith(b'd'))
    env.assertNotEqual(dumped[0], b'\xffabcd')

def testTruncatedSequenceAtEndOfTerm(env):
    # A 2-byte lead as the last byte makes the decoder read one byte past the
    # term (hitting the sds NUL terminator): 0xC3 0x00 decodes to U+00C0 'À'.
    env.expect('ft.dictadd', 'dict', b'ab\xc3').equal(1)
    env.assertEqual(dump_raw(env, 'dict'), ['abÀ'.encode()])

def testSurrogateMakesDumpReplyInvalidUtf8(env):
    # 0xED 0xA0 0x80 encodes lone surrogate U+D800 (invalid UTF-8). It is
    # stored as rune 0xD800 and DICTDUMP re-encodes it verbatim: the reply
    # itself is invalid UTF-8. Clients decoding replies as UTF-8 will fail.
    env.expect('ft.dictadd', 'dict', b'\xed\xa0\x80').equal(1)
    dumped = dump_raw(env, 'dict')
    env.assertEqual(dumped, [b'\xed\xa0\x80'])
    try:
        dumped[0].decode('utf-8')
        env.assertTrue(False, message='expected invalid UTF-8 in DICTDUMP reply')
    except UnicodeDecodeError:
        pass


# ========================= LENGTH LIMITS ============================
# Independent of encoding, but part of the same silent-drop surface:
# over-long terms are neither added nor reported as errors.

def testTermLengthLimits(env):
    # Trie_InsertStringBuffer drops terms > 512 bytes (TRIE_INITIAL_STRING_LEN
    # * sizeof(rune)) and Trie_InsertRune drops terms >= 256 runes.
    env.expect('ft.dictadd', 'dict', 'a' * 255).equal(1)
    env.expect('ft.dictadd', 'dict', 'b' * 256).equal(0)   # 256 runes: dropped
    env.expect('ft.dictadd', 'dict', 'c' * 600).equal(0)   # 600 bytes: dropped
    env.assertEqual(dump_raw(env, 'dict'), [b'a' * 255])


# ==================== RDB PERSISTENCE ROUND-TRIP ====================
# Aux save (SpellCheckDictAuxSave -> TrieType_GenericSave) re-encodes the
# stored runes to UTF-8 via runesToStr; aux load decodes them back with the
# same blind decoder. Encoding and decoding are exact inverses for every
# uint16 rune value -- including surrogates -- so whatever the trie holds
# after DICTADD survives an RDB cycle byte-identically. All mangling happens
# at DICTADD time; persistence adds no further loss.
# Strict-mode note: if RDB load ever starts validating, dictionaries
# persisted by older versions with surrogate/mangled entries would fail to
# load -- these tests pin the compatibility surface such a change must handle.

def testRdbRoundtripValidTerms(env):
    terms = ['hello', 'café', '日本語', 'FOO', 'foo']
    env.expect('ft.dictadd', 'dict', *terms).equal(5)
    before = dump_raw(env, 'dict')
    env.dumpAndReload()
    env.assertEqual(dump_raw(env, 'dict'), before)
    env.expect('ft.dictdel', 'dict', *terms).equal(5)

def testRdbRoundtripMangledTerms(env):
    # Stored-state round-trip for every lenient-decode shape above: truncated
    # astral rune (U+F600), blindly-decoded invalid sequences, and an entry
    # truncated by an embedded NUL.
    env.expect('ft.dictadd', 'dict', '\U0001F600').equal(1)  # -> U+F600
    env.expect('ft.dictadd', 'dict', b'\xc3\x28').equal(1)   # -> 'è'
    env.expect('ft.dictadd', 'dict', b'\x80abc').equal(1)    # -> '!bc'
    env.expect('ft.dictadd', 'dict', b'foo\x00bar').equal(1) # -> 'foo'
    before = dump_raw(env, 'dict')
    env.assertEqual(len(before), 4)
    env.dumpAndReload()
    env.assertEqual(dump_raw(env, 'dict'), before)
    # Entries are still addressable post-reload via the original raw bytes.
    env.expect('ft.dictdel', 'dict', b'\xc3\x28').equal(1)
    env.expect('ft.dictdel', 'dict', b'foo\x00baz').equal(1)

def testRdbRoundtripSurrogateRune(env):
    # The nastiest case: rune 0xD800 serializes to RDB as the same invalid
    # UTF-8 bytes (ED A0 80) that DICTDUMP emits, and the blind loader
    # restores it verbatim. Persistence of invalid UTF-8 is thus durable
    # across restarts, not just an in-memory artifact.
    env.expect('ft.dictadd', 'dict', b'\xed\xa0\x80').equal(1)
    env.dumpAndReload()
    env.assertEqual(dump_raw(env, 'dict'), [b'\xed\xa0\x80'])
    env.expect('ft.dictdel', 'dict', b'\xed\xa0\x80').equal(1)


# ================== SPELLCHECK USES THE SAME TRIE ====================

def testSpellCheckSuggestionFromMultibyteDict(env):
    # Suggestions surface through the same runesToStr re-encoding, so valid
    # BMP dictionary terms come back byte-identical via FT.SPELLCHECK too.
    env.cmd('ft.create', 'idx', 'ON', 'HASH', 'SCHEMA', 't', 'TEXT')
    env.cmd('ft.dictadd', 'dict', 'café')
    res = env.cmd('ft.spellcheck', 'idx', 'cafe', 'TERMS', 'INCLUDE', 'dict')
    env.assertEqual(res, [['TERM', 'cafe', [['0', 'café']]]])

def testSpellCheckIncludeSurfacesMangledEntry(env):
    # DICTADD b'\x80abc' stores '!bc' (lone continuation byte swallows 'a').
    # A fuzzy INCLUDE match against query term 'abc' (one substitution away)
    # surfaces the mangled entry as a suggestion: the user-visible correction
    # never matches the bytes that were added to the dictionary.
    env.cmd('ft.create', 'idx', 'ON', 'HASH', 'SCHEMA', 't', 'TEXT')
    env.cmd('ft.dictadd', 'dict', b'\x80abc')
    res = env.cmd('ft.spellcheck', 'idx', 'abc', 'TERMS', 'INCLUDE', 'dict')
    env.assertEqual(res, [['TERM', 'abc', [['0', '!bc']]]])

def testSpellCheckIncludeSurrogateMakesReplyInvalidUtf8(env):
    # A lone-surrogate dictionary entry (rune 0xD800, one edit away from any
    # single-char query term) is re-encoded verbatim into the FT.SPELLCHECK
    # reply: like DICTDUMP, the reply itself becomes invalid UTF-8.
    env.cmd('ft.create', 'idx', 'ON', 'HASH', 'SCHEMA', 't', 'TEXT')
    env.cmd('ft.dictadd', 'dict', b'\xed\xa0\x80')
    res = env.cmd('ft.spellcheck', 'idx', 'x', 'TERMS', 'INCLUDE', 'dict',
                  **{NEVER_DECODE: []})
    env.assertEqual(res, [[b'TERM', b'x', [[b'0', b'\xed\xa0\x80']]]])

def testSpellCheckExcludeMatchesNulTruncatedEntry(env):
    # DICTADD b'foo\x00bar' stores 'foo'. The EXCLUDE lookup is an exact
    # (distance-0) match on the stored runes, so query term 'foo' -- which the
    # user never added -- is treated as correct and omitted from the reply.
    env.cmd('ft.create', 'idx', 'ON', 'HASH', 'SCHEMA', 't', 'TEXT')
    env.cmd('ft.dictadd', 'dict', b'foo\x00bar')
    env.expect('ft.spellcheck', 'idx', 'foo', 'TERMS', 'EXCLUDE', 'dict').equal([])
    # Sanity: without the exclude dict the term does get spellcheck treatment.
    res = env.cmd('ft.spellcheck', 'idx', 'foo')
    env.assertEqual(res, [['TERM', 'foo', []]])
