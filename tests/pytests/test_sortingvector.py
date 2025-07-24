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

def test_sortingvector_ft_dot_info():
    """
    In the benchmarks we get a Traceback when using the command 'r.execute_command("ft.info {}".format(fts_indexname))' we try to reproduce this with this test

    Before the actual traceback the following CSV has been loaded:
    2025-08-05T16:58:06.5633706Z INFO:root:Executing remote command "wget https://s3.amazonaws.com/benchmarks.redislabs/redisearch/tools/ftsb/ftsb_redisearch_linux_amd64 -q -O /tmp/ftsb_redisearch"
    2025-08-05T16:58:06.5634831Z 2025-08-05 16:58:06,562 INFO Executing remote command "wget https://s3.amazonaws.com/benchmarks.redislabs/redisearch/tools/ftsb/ftsb_redisearch_linux_amd64 -q -O /tmp/ftsb_redisearch"
    2025-08-05T16:58:06.8931377Z INFO:root:Executing remote command "wget https://s3.amazonaws.com/benchmarks.redislabs/redisearch/datasets/1M-nyc_taxis-hashes/1M-nyc_taxis-hashes.redisearch.commands.ALL.csv -q -O /tmp/input.data"
    2025-08-05T16:58:06.8933323Z 2025-08-05 16:58:06,892 INFO Executing remote command "wget https://s3.amazonaws.com/benchmarks.redislabs/redisearch/datasets/1M-nyc_taxis-hashes/1M-nyc_taxis-hashes.redisearch.commands.ALL.csv -q -O /tmp/input.data"
    2025-08-05T16:58:14.6626877Z INFO:root:Executing remote command "chmod 755 /tmp/ftsb_redisearch"
    2025-08-05T16:58:14.6627975Z 2025-08-05 16:58:14,662 INFO Executing remote command "chmod 755 /tmp/ftsb_redisearch"
    2025-08-05T17:01:00.2903148Z INFO:root:Finished up remote tool ftsb_redisearch requirements
    2025-08-05T17:01:00.2904194Z 2025-08-05 17:01:00,290 INFO Finished up remote tool ftsb_redisearch requirements
    2025-08-05T17:01:00.2905241Z INFO:root:Reusing cached remote file (located at /tmp/input.data ).
    2025-08-05T17:01:00.2906280Z 2025-08-05 17:01:00,290 INFO Reusing cached remote file (located at /tmp/input.data ).
    2025-08-05T17:01:00.2908747Z INFO:root:Running the benchmark with the following parameters:
    2025-08-05T17:01:00.2910332Z 	Args array: ['/tmp/ftsb_redisearch', '--host', '10.3.0.14:6379', '--workers', '64', '--reporting-period', '1s', '--input', '/tmp/input.data', '--json-out-file', '/tmp/benchmark-result-ftsb-10K-enwiki_abstract-hashes-term-prefix_2025-08-05-17-00-53.out']
    2025-08-05T17:01:00.2911962Z 	Args str: /tmp/ftsb_redisearch --host 10.3.0.14:6379 --workers 64 --reporting-period 1s --input /tmp/input.data --json-out-file /tmp/benchmark-result-ftsb-10K-enwiki_abstract-hashes-term-prefix_2025-08-05-17-00-53.out
    2025-08-05T17:01:00.2912874Z 2025-08-05 17:01:00,290 INFO Running the benchmark with the following parameters:
    2025-08-05T17:01:00.2913812Z 	Args array: ['/tmp/ftsb_redisearch', '--host', '10.3.0.14:6379', '--workers', '64', '--reporting-period', '1s', '--input', '/tmp/input.data', '--json-out-file', '/tmp/benchmark-result-ftsb-10K-enwiki_abstract-hashes-term-prefix_2025-08-05-17-00-53.out']
    2025-08-05T17:01:00.2915179Z 	Args str: /tmp/ftsb_redisearch --host 10.3.0.14:6379 --workers 64 --reporting-period 1s --input /tmp/input.data --json-out-file /tmp/benchmark-result-ftsb-10K-enwiki_abstract-hashes-term-prefix_2025-08-05-17-00-53.out
    """

    env = Env(moduleArgs='DEFAULT_DIALECT 2')
    conn = getConnectionByEnv(env)
    
    # Set a data field that makes sense 
    conn.execute_command("HSET", "doc:7ea0e6788f6a4e62af1caaadc1fe967b:0", "title", "Battle of Monte Cassino with ß", "text", "some text", "username", "alice", "timestamp", "1234556789")
   
    # Create an index that is similar to the crashing benchmarks
    env.expect('FT.CREATE enwiki_pages ON HASH SCHEMA title text SORTABLE text text SORTABLE comment text SORTABLE username tag SORTABLE timestamp numeric SORTABLE').ok()

    # Get FT.INFO
    res = conn.execute_command("FT.INFO enwiki_pages")
    

def test_sortingvector_info_modules():
    """
    r.execute_command("info modules") 'r.execute_command("info modules")' we try to reproduce this with this test

    Before the actual traceback the following CSVs have been loaded:
    2025-08-05T17:00:59.3144359Z INFO:root:Executing remote command "wget https://s3.amazonaws.com/benchmarks.redislabs/redisearch/tools/ftsb/ftsb_redisearch_linux_amd64 -q -O /tmp/ftsb_redisearch"
    2025-08-05T17:00:59.3145866Z 2025-08-05 17:00:59,313 INFO Executing remote command "wget https://s3.amazonaws.com/benchmarks.redislabs/redisearch/tools/ftsb/ftsb_redisearch_linux_amd64 -q -O /tmp/ftsb_redisearch"
    2025-08-05T17:00:59.7571491Z INFO:root:Executing remote command "wget https://s3.amazonaws.com/benchmarks.redislabs/redisearch/datasets/enwiki_abstract-hashes-wildcard/enwiki_abstract-hashes-wildcard.redisearch.commands.SETUP.csv -q -O /tmp/input.data"
    2025-08-05T17:00:59.7573198Z 2025-08-05 17:00:59,756 INFO Executing remote command "wget https://s3.amazonaws.com/benchmarks.redislabs/redisearch/datasets/enwiki_abstract-hashes-wildcard/enwiki_abstract-hashes-wildcard.redisearch.commands.SETUP.csv -q -O /tmp/input.data"
    2025-08-05T17:01:00.1796853Z INFO:root:Executing remote command "chmod 755 /tmp/ftsb_redisearch"
    2025-08-05T17:01:00.1797812Z 2025-08-05 17:01:00,179 INFO Executing remote command "chmod 755 /tmp/ftsb_redisearch"
    2025-08-05T17:01:00.2903148Z INFO:root:Finished up remote tool ftsb_redisearch requirements
    2025-08-05T17:01:00.2904194Z 2025-08-05 17:01:00,290 INFO Finished up remote tool ftsb_redisearch requirements
    2025-08-05T17:01:00.2905241Z INFO:root:Reusing cached remote file (located at /tmp/input.data ).
    2025-08-05T17:01:00.2906280Z 2025-08-05 17:01:00,290 INFO Reusing cached remote file (located at /tmp/input.data ).
    2025-08-05T17:01:00.2908747Z INFO:root:Running the benchmark with the following parameters:
    2025-08-05T17:01:00.2910332Z 	Args array: ['/tmp/ftsb_redisearch', '--host', '10.3.0.14:6379', '--workers', '64', '--reporting-period', '1s', '--input', '/tmp/input.data', '--json-out-file', '/tmp/benchmark-result-ftsb-10K-enwiki_abstract-hashes-term-prefix_2025-08-05-17-00-53.out']
    2025-08-05T17:01:00.2911962Z 	Args str: /tmp/ftsb_redisearch --host 10.3.0.14:6379 --workers 64 --reporting-period 1s --input /tmp/input.data --json-out-file /tmp/benchmark-result-ftsb-10K-enwiki_abstract-hashes-term-prefix_2025-08-05-17-00-53.out
    2025-08-05T17:01:00.2912874Z 2025-08-05 17:01:00,290 INFO Running the benchmark with the following parameters:
    2025-08-05T17:01:00.2913812Z 	Args array: ['/tmp/ftsb_redisearch', '--host', '10.3.0.14:6379', '--workers', '64', '--reporting-period', '1s', '--input', '/tmp/input.data', '--json-out-file', '/tmp/benchmark-result-ftsb-10K-enwiki_abstract-hashes-term-prefix_2025-08-05-17-00-53.out']
    2025-08-05T17:01:00.2915179Z 	Args str: /tmp/ftsb_redisearch --host 10.3.0.14:6379 --workers 64 --reporting-period 1s --input /tmp/input.data --json-out-file /tmp/benchmark-result-ftsb-10K-enwiki_abstract-hashes-term-prefix_2025-08-05-17-00-53.out
    """

    env = Env(moduleArgs='DEFAULT_DIALECT 2')
    conn = getConnectionByEnv(env)

    ## Execute the command that has been mentioned in the traceback:
    res = conn.execute_command('info', 'modules')
    #print(res)
    