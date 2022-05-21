import os.path
from includes import *


GENTEXT = os.path.dirname(os.path.abspath(__file__)) + '/../tests/genesis.txt'


def setupGenesis(env):
    txt = open(GENTEXT, 'r').read()
    env.cmd('ft.create', 'idx', 'schema', 'txt', 'text')
    env.cmd('ft.add', 'idx', 'gen1', 1.0, 'fields', 'txt', txt)

def testSummarization(env):
    # Load the file
    setupGenesis(env)
    res = env.cmd('FT.SEARCH', 'idx', 'abraham isaac jacob',
                   'SUMMARIZE', 'FIELDS', 1, 'txt', 'LEN', 20,
                   'HIGHLIGHT', 'FIELDS', 1, 'txt', 'TAGS', '<b>', '</b>')
    env.assertEqual(1, res[0])
    # print res
    res_txt = res[2][1]
    # print res_txt

    env.assertTrue("<b>Abraham</b>" in res_txt)
    env.assertTrue("<b>Isaac</b>" in res_txt)
    env.assertTrue("<b>Jacob</b>" in res_txt)

    res = env.cmd('FT.SEARCH', 'idx', 'abraham isaac jacob',
                   'HIGHLIGHT', 'fields', 1, 'txt', 'TAGS', '<i>', '</i>')
    res_txt = res[2][1]
    env.assertGreaterEqual(len(res_txt), 160000)

    res = env.cmd('FT.SEARCH', 'idx', 'abraham isaac jacob',
                   'SUMMARIZE', 'FIELDS', 1, 'txt', 'FRAGS', 10000)
    # print res

    res_list = res[2][1]
    # env.assertIsInstance(res_list, list)

    # Search with custom separator
    res = env.cmd('FT.SEARCH', 'idx', 'isaac',
                   'SUMMARIZE', 'FIELDS', 1, 'txt',
                   'SEPARATOR', '\r\n',
                   'FRAGS', 4, 'LEN', 3)
    env.assertEqual([1L, 'gen1', [
                     'txt', 'name Isaac: and\r\nwith Isaac,\r\nIsaac. {21:4} And Abraham circumcised his son Isaac\r\nson Isaac was\r\n']], res)

    # Attempt a query which doesn't have a corresponding matched term
    res = env.cmd('FT.SEARCH', 'idx', '-blah', 'SUMMARIZE', 'LEN', 3)
    env.assertEqual(
        [1L, 'gen1', ['txt', ' The First Book of Moses, called Genesis {1:1} In']], res)

    # Try the same, but attempting to highlight
    res = env.cmd('FT.SEARCH', 'idx', '-blah', 'HIGHLIGHT')
    env.assertEqual(214894, len(res[2][1]))

def testPrefixExpansion(env):
    # Search with prefix
    setupGenesis(env)
    res = env.cmd('FT.SEARCH', 'idx', 'begi*',
                   'HIGHLIGHT', 'FIELDS', 1, 'txt', 'TAGS', '<b>', '</b>',
                   'SUMMARIZE', 'FIELDS', 1, 'txt', 'LEN', 20)

    # Prefix expansion uses "early exit" strategy, so the term highlighted won't necessarily be the
    # best term
    env.assertEqual([1L, 'gen1', [
                     'txt', 'is] one, and they have all one language; and this they <b>begin</b> to do: and now nothing will be restrained from them, which... ']], res)
    # env.assertEqual([1L, 'gen1', ['txt', 'First Book of Moses, called Genesis {1:1} In the <b>beginning</b> God created the heaven and the earth. {1:2} And the earth... the mighty hunter before the LORD. {10:10} And the <b>beginning</b> of his kingdom was Babel, and Erech, and Accad, and Calneh... is] one, and they have all one language; and this they <b>begin</b> to do: and now nothing will be restrained from them, which... ']], res)

def testSummarizationMultiField(env):
    p1 = "Redis is an open-source in-memory database project implementing a networked, in-memory key-value store with optional durability. Redis supports different kinds of abstract data structures, such as strings, lists, maps, sets, sorted sets, hyperloglogs, bitmaps and spatial indexes. The project is mainly developed by Salvatore Sanfilippo and is currently sponsored by Redis Labs.[4] Redis Labs creates and maintains the official Redis Enterprise Pack."
    p2 = "Redis typically holds the whole dataset in memory. Versions up to 2.4 could be configured to use what they refer to as virtual memory[19] in which some of the dataset is stored on disk, but this feature is deprecated. Persistence is now achieved in two different ways: one is called snapshotting, and is a semi-persistent durability mode where the dataset is asynchronously transferred from memory to disk from time to time, written in RDB dump format. Since version 1.1 the safer alternative is AOF, an append-only file (a journal) that is written as operations modifying the dataset in memory are processed. Redis is able to rewrite the append-only file in the background in order to avoid an indefinite growth of the journal."

    env.cmd('FT.CREATE', 'idx', 'SCHEMA', 'txt1', 'TEXT', 'txt2', 'TEXT')
    env.cmd('FT.ADD', 'idx', 'redis', 1.0,
             'FIELDS', 'txt1', p1, 'txt2', p2)

    # Now perform the multi-field search
    env.cmd('FT.SEARCH', 'idx', 'memory persistence salvatore',
             'HIGHLIGHT', 'TAGS', '<b>', '</b>',
             'SUMMARIZE', 'LEN', 5,
             'RETURN', 2, 'txt1', 'txt2')

    # Now perform the multi-field search
    res = env.cmd('FT.SEARCH', 'idx', 'memory persistence salvatore',
                   'SUMMARIZE', 'FIELDS', 2, 'txt1', 'txt2', 'LEN', 5)

    env.assertEqual(1L, res[0])
    env.assertEqual('redis', res[1])
    for term in ['txt1', 'memory database project implementing a networked, in-memory ... by Salvatore Sanfilippo... ', 'txt2',
                 'dataset in memory. Versions... as virtual memory[19] in... persistent durability mode where the dataset is asynchronously transferred from memory... ']:
        env.assertIn(term, res[2])


def testSummarizationDisabled(env):
    env.cmd('FT.CREATE', 'idx', 'NOOFFSETS', 'SCHEMA', 'body', 'TEXT')
    env.cmd('FT.ADD', 'idx', 'doc', 1.0, 'FIELDS', 'body', 'hello world')
    with env.assertResponseError():
        res = env.cmd('FT.SEARCH', 'idx', 'hello',
                       'SUMMARIZE', 'FIELDS', 1, 'body')

    env.cmd('FT.CREATE', 'idx2', 'NOHL', 'SCHEMA', 'body', 'TEXT')
    env.cmd('FT.ADD', 'idx2', 'doc', 1.0, 'FIELDS', 'body', 'hello world')
    with env.assertResponseError():
        res = env.cmd('FT.SEARCH', 'idx2', 'hello',
                       'SUMMARIZE', 'FIELDS', 1, 'body')

def testSummarizationNoSave(env):
    env.cmd('FT.CREATE', 'idx', 'SCHEMA', 'body', 'TEXT')
    env.cmd('FT.ADD', 'idx', 'doc', 1.0, 'NOSAVE',
             'fields', 'body', 'hello world')
    res = env.cmd('FT.SEARCH', 'idx', 'hello',
                   'SUMMARIZE', 'RETURN', 1, 'body')
    # print res
    env.assertEqual([1L, 'doc', []], res)

def testSummarizationMeta(env):
    env.cmd('ft.create', 'idx', 'schema', 'foo',
             'text', 'bar', 'text', 'baz', 'text')
    env.cmd('ft.add', 'idx', 'doc1', 1.0, 'fields', 'foo',
             'pill', 'bar', 'pillow', 'baz', 'piller')

    # Now, return the fields:
    res = env.cmd('ft.search', 'idx', 'pill pillow piller',
                   'RETURN', 1, 'baz', 'SUMMARIZE', 'FIELDS', 2, 'foo', 'bar')
    env.assertEqual(1, res[0])
    result = res[2]
    names = [x[0] for x in grouper(result, 2)]

    # RETURN restricts the number of fields
    env.assertEqual(set(('baz',)), set(names))

    res = env.cmd('ft.search', 'idx', 'pill pillow piller',
                   'RETURN', 3, 'foo', 'bar', 'baz', 'SUMMARIZE')
    env.assertEqual([1L, 'doc1', ['foo', 'pill... ', 'bar',
                                   'pillow... ', 'baz', 'piller... ']], res)


def testOverflow1(env):
    #"FT.CREATE" "netflix" "SCHEMA" "title" "TEXT" "WEIGHT" "1" "rating" "TEXT" "WEIGHT" "1" "level" "TEXT" "WEIGHT" "1" "description" "TEXT" "WEIGHT" "1" "year" "NUMERIC" "uscore" "NUMERIC" "usize" "NUMERIC"
    #FT.ADD" "netflix" "15ad80086ccc7f" "1" "FIELDS" "title" "The Vampire Diaries" "rating" "TV-14" "level" "Parents strongly cautioned. May be unsuitable for children ages 14 and under." "description" "90" "year" "2017" "uscore" "91" "usize" "80"
    env.cmd('FT.CREATE', 'netflix', 'SCHEMA', 'title', 'TEXT', 'rating', 'TEXT', 'leve', 'TEXT', 'description', 'TEXT', 'year', 'NUMERIC', 'uscore', 'NUMERIC', 'usize', 'NUMERIC')
    env.cmd('FT.ADD', "netflix", "15ad80086ccc7f", "1.0", "FIELDS", "title", "The Vampire Diaries", "rating", "TV-14", "level",
        "Parents strongly cautioned. May be unsuitable for children ages 14 and under.",
        "description", "90", "year", "2017", "uscore", "91", "usize", "80")
    res = env.cmd('ft.search', 'netflix', 'vampire', 'highlight')
    env.assertEqual(1L, res[0])
    env.assertEqual('15ad80086ccc7f', res[1])
    for term in ['title', 'The <b>Vampire</b> Diaries', 'rating', 'TV-14', 'level', 'Parents strongly cautioned. May be unsuitable for children ages 14 and under.', 'description', '90', 'year', '2017', 'uscore', '91', 'usize', '80']:
        env.assertIn(term, res[2])
    
def testIssue364(env):
    # FT.CREATE testset "SCHEMA" "permit_timestamp" "NUMERIC" "SORTABLE" "job_category" "TEXT" "NOSTEM" "address" "TEXT" "NOSTEM"  "neighbourhood" "TAG" "SORTABLE" "description" "TEXT"  "building_type" "TEXT" "WEIGHT" "20" "NOSTEM" "SORTABLE"     "work_type" "TEXT" "NOSTEM" "SORTABLE"     "floor_area" "NUMERIC" "SORTABLE"     "construction_value" "NUMERIC" "SORTABLE"     "zoning" "TAG"     "units_added" "NUMERIC" "SORTABLE"     "location" "GEO"
    # ft.add testset 109056573-002 1 fields building_type "Retail and Shops" description "To change the use from a Restaurant to a Personal Service Shop (Great Clips)"
    # FT.SEARCH testset retail RETURN 1 description SUMMARIZE LIMIT 0 1
    env.cmd('ft.create', 'idx', 'SCHEMA', 'building_type', 'TEXT', 'description', 'TEXT')
    env.cmd('ft.add', 'idx', 'doc1', '1.0', 'FIELDS',
             'building_type', 'Retail and Shops',
             'description', 'To change the use from a Restaurant to a Personal Service Shop (Great Clips)')
    
    env.cmd('ft.add', 'idx', 'doc2', '1.0', 'FIELDS',
             'building_type', 'Retail and Shops',
             'description', 'To change the use from a Restaurant to a Personal Service Shop (Great Clips) at the end')

    ret = env.cmd('FT.SEARCH', 'idx', 'retail', 'RETURN', 1, 'description', 'SUMMARIZE')
    expected = [2L, 'doc2', ['description', 'To change the use from a Restaurant to a Personal Service Shop (Great Clips) at the'], 'doc1', ['description', 'To change the use from a Restaurant to a Personal Service Shop (Great Clips)']]
    env.assertEqual(expected, ret)

def grouper(iterable, n, fillvalue=None):
    "Collect data into fixed-length chunks or blocks"
    from itertools import izip_longest
    # grouper('ABCDEFG', 3, 'x') --> ABC DEF Gxx
    args = [iter(iterable)] * n
    return izip_longest(fillvalue=fillvalue, *args)

def testFailedHighlight(env):
    #test NOINDEX
    env.cmd('ft.create', 'idx', 'SCHEMA', 'f1', 'TEXT', 'f2', 'TEXT', 'f3', 'TEXT', 'NOINDEX')
    env.cmd('ft.add', 'idx', 'doc1', '1.0', 'FIELDS', 'f1', 'foo foo foo', 'f2', 'bar bar bar', 'f3', 'baz baz baz')
    env.assertEqual([1L, 'doc1', ['f1', 'foo foo foo', 'f2', 'bar bar bar', 'f3', 'baz baz baz']],
        env.cmd('ft.search idx foo'))
    env.assertEqual([1L, 'doc1', ['f1', '<b>foo</b> <b>foo</b> <b>foo</b>', 'f2', 'bar bar bar', 'f3', 'baz baz baz']],
        env.cmd('ft.search', 'idx', 'foo', 'highlight', 'fields', '1', 'f1'))
    env.assertEqual([1L, 'doc1', ['f2', 'bar bar bar', 'f1', 'foo foo foo', 'f3', 'baz baz baz']],
        env.cmd('ft.search idx foo highlight fields 1 f2'))
    env.assertEqual([1L, 'doc1', ['f3', 'baz baz baz', 'f1', 'foo foo foo', 'f2', 'bar bar bar']],
        env.cmd('ft.search idx foo highlight fields 1 f3'))

    #test empty string
    env.cmd('ft.create idx2 SCHEMA f1 TEXT f2 TEXT f3 TEXT')
    env.cmd('ft.add', 'idx2', 'doc2', '1.0', 'FIELDS', 'f1', 'foo foo foo', 'f2', '', 'f3', 'baz baz baz')
    env.assertEqual([1L, 'doc2', ['f1', '<b>foo</b> <b>foo</b> <b>foo</b>', 'f2', '', 'f3', 'baz baz baz']],
        env.cmd('ft.search idx2 foo highlight fields 1 f1'))
    env.assertEqual([1L, 'doc2', ['f2', '', 'f1', 'foo foo foo', 'f3', 'baz baz baz']],
        env.cmd('ft.search idx2 foo highlight fields 1 f2'))
    env.assertEqual([1L, 'doc2', ['f3', 'baz baz baz', 'f1', 'foo foo foo', 'f2', '']],
        env.cmd('ft.search idx2 foo highlight fields 1 f3'))

    #test stop word list
    env.cmd('ft.create idx3 SCHEMA f1 TEXT f2 TEXT f3 TEXT')
    env.cmd('ft.add', 'idx3', 'doc3', '1.0', 'FIELDS', 'f1', 'foo foo foo', 'f2', 'not a', 'f3', 'baz baz baz')
    env.assertEqual([1L, 'doc3', ['f1', '<b>foo</b> <b>foo</b> <b>foo</b>', 'f2', 'not a', 'f3', 'baz baz baz']],
        env.cmd('ft.search idx3 foo highlight fields 1 f1'))
    env.assertEqual([1L, 'doc3', ['f2', 'not a', 'f1', 'foo foo foo', 'f3', 'baz baz baz']],
        env.cmd('ft.search idx3 foo highlight fields 1 f2'))
    env.assertEqual([1L, 'doc3', ['f3', 'baz baz baz', 'f1', 'foo foo foo', 'f2', 'not a']],
        env.cmd('ft.search idx3 foo highlight fields 1 f3')) 