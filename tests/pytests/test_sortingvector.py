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
