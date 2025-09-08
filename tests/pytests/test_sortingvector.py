from common import *
from random import shuffle

def test_sortingvector_utf_casefolding_for_sharp_s():
    """
    This tests if searchable values with case-folded utf-8 can be 
    introduced, to be searchable ß should be replaced with ss.

    Tries to reproduce a benchmark crash that has been introduced by
    RSSortingVector swapping code
    """

    env = Env(moduleArgs='DEFAULT_DIALECT 2')
    conn = getConnectionByEnv(env)

    # Create an index that is similar to the crashing benchmarks
    env.expect('FT.CREATE enwiki_pages ON HASH SCHEMA title text SORTABLE text text SORTABLE comment text SORTABLE username tag SORTABLE timestamp numeric SORTABLE').ok()

    # Set a data field that makes sense 
    conn.execute_command("HSET", "doc:7ea0e6788f6a4e62af1caaadc1fe967b:0", "title", "Battle of Monte Cassino with ß", "text", "some text", "username", "alice", "timestamp", "1234556789")
    
    res = env.cmd("FT.SEARCH", "enwiki_pages", "Cassino")
    env.assertEqual(res[0], 1)

    res = env.cmd("FT.SEARCH", "enwiki_pages", "with ß")
    env.assertEqual(res[0], 1)

def test_sortingvector_ft_dot_info():
    """
    In the benchmarks we get a Traceback when using the command 'r.execute_command("ft.info {}".format(fts_indexname))' we try to reproduce this with this test
    """

    env = Env(moduleArgs='DEFAULT_DIALECT 2')
    conn = getConnectionByEnv(env)
    
    # Set a data field that makes sense 
    conn.execute_command("HSET", "doc:7ea0e6788f6a4e62af1caaadc1fe967b:0", "title", "Battle of Monte Cassino with ß", "text", "some text", "username", "alice", "timestamp", "1234556789")
   
    # Create an index that is similar to the crashing benchmarks
    env.expect('FT.CREATE enwiki_pages ON HASH SCHEMA title text SORTABLE text text SORTABLE comment text SORTABLE username tag SORTABLE timestamp numeric SORTABLE').ok()

    # Get FT.INFO
    res = env.cmd("FT.INFO enwiki_pages")

def test_sortingvector_ft_dot_info_with_uninon_iterator():
    """
    In the benchmarks we get a Traceback when using the command 'r.execute_command("ft.info {}".format(fts_indexname))' 
    we try to reproduce this with this test.

    In the logs we see it is related to 370K-docs-union-iterators.idx174.commands.SETUP.csv and we found a
    FT.CREATE and a FT.SEARCH command that have been used. 

    The crash happens after FT.INFO but either the setup or creating the index brings the redis server in the
    unsound state.

    This test skips if the source file cannot be found.
    """

    env = Env(moduleArgs='DEFAULT_DIALECT 2')
    conn = getConnectionByEnv(env)

    fp = "../../.vscode/370K-docs-union-iterators.idx174.commands.SETUP.csv"
    create_command = ['FT.CREATE', 'idx174', 'PREFIX', '1', 'idx174:', 'SCHEMA', 'field1', 'NUMERIC', 'field2', 'TEXT', 'field3', 'TEXT', 'field4', 'NUMERIC', 'field5', 'NUMERIC', 'field6', 'NUMERIC', 'field7', 'NUMERIC', 'field8', 'NUMERIC', 'field9', 'NUMERIC']
    search_command = ['FT.SEARCH', 'idx174', "@field4: [-inf 18053372 ] @field5: [18053372 +inf ] @field8: [-inf 0.01 ] @field9:[0.01 +inf ] ( @field3: (1685)|(1876)|(1880)|(1882)|(1883)|(2257)|(2258) @field6: [-inf 0.325 ] @field7: [0.325 +inf ] )| ( @field3: (1866)|(1868)|(1874)|(1878)|(1879)|(2227)|(2610) @field6: [-inf 3.794 ] @field7: [3.794 +inf ] )| ( @field3: (1867)|(1869)|(1872) @field6: [-inf 4.743 ] @field7: [4.743 +inf ] )| ( @field3: (1873)|(1887)|(2215) @field6: [-inf 5.692 ] @field7: [5.692 +inf ] )| ( @field3: (1881)|(1888) @field6: [-inf 3.168 ] @field7: [3.168 +inf ] )| ( @field3: (2008)|(2221) @field6: [-inf 3.605 ] @field7: [3.605 +inf ] )"];

    # open csv file and iterate over lines, create columns vector
    import csv
    import os

    #conn.execute_command(*create_command)

    if os.path.exists(fp):
        with open(fp, 'r', encoding='utf-8') as csvfile:
            reader = csv.reader(csvfile)
            columns = []
            for row in reader:
                columns.append(row)
        

        # Setup the CSV data like in the benchmark
        end_idx = len(columns) - 1
        for column_data in columns[:end_idx]:  # Limit to first 10 rows for testing
            if len(column_data) > 0:
                num_fields = 34
                end_field = 3+35*2
                cmd = column_data[3:end_field]
                if end_idx <= 10:
                    print(cmd)
                conn.execute_command(*cmd)

        # Run create after setting up the db
        conn.execute_command(*create_command)

        # Run search command as in the benchmarks
        conn.execute_command(*search_command)

        # Run ft.info
        conn.execute_command("FT.INFO", "idx174")
    else:
        print("Cannot find: " + fp + " skipping test")

def test_sortingvector_info_modules():
    """
    r.execute_command("info modules") 'r.execute_command("info modules")' we try to reproduce this with this test
    """

    env = Env(moduleArgs='DEFAULT_DIALECT 2')
    conn = getConnectionByEnv(env)

    ## Execute the command that has been mentioned in the traceback:
    res = conn.execute_command('info', 'modules')

def test_sortingvector_get_is_set():
    """
    This tests if the string written is the same as received. This shall catch bugs
    when creating RSvalue Strings from the Rust Side.
    """

    env = Env(moduleArgs='DEFAULT_DIALECT 2')
    conn = getConnectionByEnv(env)

    # Create an index that is similar to the crashing benchmarks
    env.expect('FT.CREATE enwiki_pages ON HASH SCHEMA title text SORTABLE text text SORTABLE comment text SORTABLE username tag SORTABLE timestamp numeric SORTABLE').ok()

    # Set a data field that makes sense 
    conn.execute_command("HSET", "doc:7ea0e6788f6a4e62af1caaadc1fe967b:0", "title", "Battle of Monte Cassino with ß", "text", "some text", "username", "alice", "timestamp", "1234556789")
    
    res = conn.execute_command("HGET", "doc:7ea0e6788f6a4e62af1caaadc1fe967b:0", "title")
    env.assertEqual(res, "Battle of Monte Cassino with ß")
