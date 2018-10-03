from RLTest import Env
from RLTest.utils import Colors


def printWithPrefix(val, prefix='\t'):
    print prefix + str(val)


class testShow:
    def __init__(self):
        self.env = Env()
        self.env.expect('ft.create', 'imdb_db_idx', 'SCHEMA', 'primaryName', 'TEXT', 'SORTABLE',
                        'birthYear', 'NUMERIC', 'SORTABLE',
                        'primaryProfession', 'TEXT', 'SORTABLE',
                        'knownForTitles', 'TEXT', 'SORTABLE')
        self.env.expect('ft.add', 'imdb_db_idx', 'doc1', '1.0', 'FIELDS',
                        'primaryName', 'Meir Ariel',
                        'birthYear', '1942',
                        'primaryProfession', 'soundtrack,writer,composer',
                        'knownForTitles', 'tt1934269,tt0102919,tt0313126,tt1830273')
        self.env.expect('ft.add', 'imdb_db_idx', 'doc2', '1.0', 'FIELDS',
                        'primaryName', 'Meir Banai',
                        'birthYear', '1961',
                        'primaryProfession', 'soundtrack,actor',
                        'knownForTitles', 'tt0152427,tt0089661,tt6791802,tt0380126')
        self.env.expect('ft.add', 'imdb_db_idx', 'doc3', '1.0', 'FIELDS',
                        'primaryName', 'Meir Kahane',
                        'birthYear', '1932',
                        'primaryProfession', '',
                        'knownForTitles', 'tt0104131')
        self.env.expect('ft.add', 'imdb_db_idx', 'doc4', '1.0', 'FIELDS',
                        'primaryName', 'Golda Meir',
                        'birthYear', '1898',
                        'primaryProfession', 'actress,soundtrack',
                        'knownForTitles', 'tt2372900,tt0096697,tt5650700,tt0133408')
        self.env.expect('ft.add', 'imdb_db_idx', 'doc5', '1.0', 'FIELDS',
                        'primaryName', 'Meir Rosenne',
                        'birthYear', '1931',
                        'primaryProfession', '',
                        'knownForTitles', 'tt1450633')
        self.env.expect('ft.add', 'imdb_db_idx', 'doc6', '1.0', 'FIELDS',
                        'primaryName', 'Gabriel Ben Meir',
                        'birthYear', '1945',
                        'primaryProfession', '',
                        'knownForTitles', 'tt3011976,tt0123338')
        self.env.expect('ft.add', 'imdb_db_idx', 'doc7', '1.0', 'FIELDS',
                        'primaryName', 'Meir Dagan',
                        'birthYear', '1945',
                        'primaryProfession', '',
                        'knownForTitles', 'tt3011976,tt0123338')
        self.env.expect('ft.add', 'imdb_db_idx', 'doc8', '1.0', 'FIELDS',
                        'primaryName', 'Meir Einstein',
                        'birthYear', '1951',
                        'primaryProfession', 'actor',
                        'knownForTitles', 'tt2635882,tt0385411,tt3421738,tt6143126')
        self.env.expect('ft.add', 'imdb_db_idx', 'doc9', '1.0', 'FIELDS',
                        'primaryName', 'Meir Amit',
                        'birthYear', '1921',
                        'primaryProfession', '',
                        'knownForTitles', 'tt0250764')
        self.aggregationQueries = [
            {
                'query': 'FT.AGGREGATE imdb_db_idx @primaryName:meir LOAD 2 @primaryName @birthYear',
                'summary': '\tsimple query that just return the result',
                'prettyPtint': '\tFT.AGGREGATE imdb_db_idx @primaryName:meir LOAD 2 @primaryName @birthYear'
            },
            {
                'query': 'FT.AGGREGATE imdb_db_idx @primaryName:meir LOAD 2 @primaryName @birthYear '
                         'FILTER @birthYear>1900',
                'summary': '\texample for filter query',
                'prettyPtint': '\tFT.AGGREGATE imdb_db_idx @primaryName:meir LOAD 2 @primaryName @birthYear\r\n'
                               '\t\t\tFILTER @birthYear>1900'
            },
            {
                'query': 'FT.AGGREGATE imdb_db_idx @primaryName:meir LOAD 1 @primaryName '
                         'FILTER @birthYear>1900 '
                         'APPLY @birthYear-(@birthYear%10) as decade',
                'summary': '\texample for apply query',
                'prettyPtint': '\tFT.AGGREGATE imdb_db_idx @primaryName:meir LOAD 1 @primaryName\r\n'
                               '\t\t\tFILTER @birthYear>1900\r\n'
                               '\t\t\tAPPLY @birthYear-(@birthYear%10) as decade'
            },
            {
                'query': 'FT.AGGREGATE imdb_db_idx @primaryName:meir LOAD 1 @primaryName '
                         'FILTER @birthYear>1900 '
                         'APPLY @birthYear-(@birthYear%10) as decade '
                         'SORTBY 2 @decade DESC ',
                'summary': '\texample for sortby query',
                'prettyPtint': '\tFT.AGGREGATE imdb_db_idx @primaryName:meir LOAD 1 @primaryName\r\n'
                               '\t\t\tFILTER @birthYear>1900\r\n'
                               '\t\t\tAPPLY @birthYear-(@birthYear%10) as decade\r\n'
                               '\t\t\tSORTBY 2 @decade DESC'
            },
            {
                'query': 'FT.AGGREGATE imdb_db_idx @primaryName:meir LOAD 1 @primaryProfession',
                'summary': '\texample for group by',
                'prettyPtint': '\tFT.AGGREGATE imdb_db_idx @primaryName:meir LOAD 1 @primaryProfession\r\n'
            },
            {
                'query': 'FT.AGGREGATE imdb_db_idx @primaryName:meir LOAD 1 @primaryProfession '
                         'FILTER @primaryProfession',
                'summary': '\texample for group by',
                'prettyPtint': '\tFT.AGGREGATE imdb_db_idx @primaryName:meir LOAD 1 @primaryProfession\r\n'
                               '\t\t\tFILTER @primaryProfession\r\n'
            },
            {
                'query': 'FT.AGGREGATE imdb_db_idx @primaryName:meir '
                         'FILTER @primaryProfession '
                         'APPLY split(@primaryProfession) as professions',
                'summary': '\texample for group by',
                'prettyPtint': '\tFT.AGGREGATE imdb_db_idx @primaryName:meir\r\n'
                               '\t\t\tFILTER @primaryProfession\r\n'
                               '\t\t\tAPPLY split(@primaryProfession) as professions\r\n'
            },
            {
                'query': 'FT.AGGREGATE imdb_db_idx @primaryName:meir '
                         'FILTER @primaryProfession '
                         'APPLY split(@primaryProfession) as professions '
                         'GROUPBY 1 @professions REDUCE COUNT 0 as count',
                'summary': '\texample for group by',
                'prettyPtint': '\tFT.AGGREGATE imdb_db_idx @primaryName:meir\r\n'
                               '\t\t\tFILTER @primaryProfession\r\n'
                               '\t\t\tAPPLY split(@primaryProfession) as professions\r\n'
                               '\t\t\tGROUPBY 1 @professions REDUCE COUNT 0 as count\r\n'
            }
        ]

        self.spellCheckQueries = [
            {
                'query': 'FT.SEARCH imdb_db_idx actrass',
                'summary': '\tsimple search query',
                'prettyPtint': '\tFT.SEARCH imdb_db_idx actrass'
            },
            {
                'query': 'FT.SPELLCHECK imdb_db_idx actrass',
                'summary': '\tsimple spell check example',
                'prettyPtint': '\tFT.SPELLCHECK imdb_db_idx actrass'
            }
            # ,
            # {
            #     'query': 'FT.DICTADD imdb_db_idx_names miir',
            #     'summary': '\tadding "miir" to dictionary',
            #     'prettyPtint': '\tFT.DICTADD imdb_db_idx_names miir'
            # },
            # {
            #     'query': 'FT.SPELLCHECK imdb_db_idx miir '
            #              'TERMS EXCLUDE imdb_db_idx_names',
            #     'summary': '\tsimple spell check example with exclude dictionary',
            #     'prettyPtint': '\tFT.SPELLCHECK imdb_db_idx miir\r\n'
            #                    '\t\t\tTERMS EXCLUDE imdb_db_idx_names'
            # },
            # {
            #     'query': 'FT.DICTADD strange_names inventedname',
            #     'summary': '\tadding "inventedname" to "strange_names" dictionary',
            #     'prettyPtint': '\tFT.DICTADD strange_names inventedname'
            # },
            # {
            #     'query': 'FT.SPELLCHECK imdb_db_idx inventdname',
            #     'summary': '\tsimple spell check example on none existing term',
            #     'prettyPtint': '\tFT.SPELLCHECK imdb_db_idx inventdname'
            # },
            # {
            #     'query': 'FT.SPELLCHECK imdb_db_idx inventdname '
            #              'TERMS INCLUDE strange_names',
            #     'summary': '\tsimple spell check example on none existing term with include dictionary',
            #     'prettyPtint': '\tFT.SPELLCHECK imdb_db_idx inventdname\r\n'
            #                    '\t\t\tTERMS INCLUDE strange_names'
            # }
        ]

        self.phoneticsQueries = [
            {
                'query': 'FT.CREATE phonetics_example SCHEMA name TEXT PHONETIC dm:en SORTABLE',
                'summary': '\tcreating index with phonetic field',
                'prettyPtint': '\tFT.CREATE phonetics_example SCHEMA name TEXT PHONETIC dm:en SORTABLE'
            },
            {
                'query': 'FT.ADD phonetics_example phonetics_doc1 1.0 FIELDS name felix',
                'summary': '\tadding felix to phonetics_example index',
                'prettyPtint': '\tFT.ADD phonetics_example phonetics_doc1 1.0 FIELDS name felix'
            },
            {
                'query': 'FT.SEARCH phonetics_example phelix',
                'summary': '\tsearching for phelix',
                'prettyPtint': '\tFT.SEARCH phonetics_example phelix'
            },
            {
                'query': 'FT.SEARCH phonetics_example phelix=>{$phonetic:false;}',
                'summary': '\tsearching for phelix, disabling phonetics',
                'prettyPtint': '\tFT.SEARCH phonetics_example phelix=>{$phonetic:false;}'
            }
        ]

    def doQuery(self, query, summary, prettyPrint):
        print ''
        printWithPrefix(Colors.Green('summary : ') + Colors.Bold(str(summary)))
        printWithPrefix(Colors.Green('query : ') + Colors.Bold(str(prettyPrint)))
        print ''
        raw_input(Colors.Bold('\tpress any button to continue ...'))
        print ''
        self.env.expect(*query.split()).prettyPrint()

    def showOf(self, queries):
        for i, q in enumerate(queries):
            print ''
            print Colors.Bold('\t[EXAMPLE %d]' % (i + 1))
            self.doQuery(q['query'], q['summary'], q['prettyPtint'])
            print ''
            raw_input(Colors.Bold('\tnext example ...'))

    def testAggregationShowOf(self):
        self.showOf(self.aggregationQueries)

        print ''
        print raw_input(Colors.Bold('\tThats it for aggregations :), any questions?'))

    def testSpellCheckShowOf(self):
        self.showOf(self.spellCheckQueries)

        print ''
        print raw_input(Colors.Bold('\tThats it for spell check :), any questions?'))

    def testPhoneticsShowOf(self):
        self.showOf(self.phoneticsQueries)

        print ''
        print raw_input(Colors.Bold('\tThats it for phonetics :), any questions?'))
