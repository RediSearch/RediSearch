from common import *
from random import shuffle

def test_sortingvector_utf_casefolding_for_sharp_s():
    """
    This tests if searchable values with case-folded utf-8 can be 
    introduced, to be searchable ß should be replaced with ss.

    Tries to reproduce a benchmark crash that has been introduced by
    RSSortingVector swaping code
    """

    env = Env(moduleArgs='DEFAULT_DIALECT 2')
    conn = getConnectionByEnv(env)

    ## Execute the command that has been mentioned in the traceback:
    conn.execute_command('info', 'modules')


    # Create an index that is similar to the crashing benchmarks
    env.expect('FT.CREATE enwiki_pages ON HASH SCHEMA title text SORTABLE text text SORTABLE comment text SORTABLE username tag SORTABLE timestamp numeric SORTABLE').ok()

    # Set a 
    conn.execute_command("HSET doc:7ea0e6788f6a4e62af1caaadc1fe967b:0 title", "Battle of Monte Cassino with ß")
    
    res = env.cmd("FT.SEARCH", "enwiki_pages", "Cassino with ß")
    env.assertEqual(res[0], 1)
