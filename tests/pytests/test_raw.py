from RLTest import Env
from common import *
from includes import *

def test_async_cmd(env):

    promise = env.async_cmd(bla bla bla)
    env.cmd('bla bla bla')
    env.cmd('bla bla bla')
    res = promise.read_response()

@SearchTest(readers = 1)
def test(SearchEnv):
    reader = SearchEnv.attach_reader(1) # how to get the reader thread id?
    gc = SearchEnv.attach_to_gc()
    async_res = SearchEnv.execute_command_async('FT.SEARCH ....')
    reader.run_to_label("before_background_search")
    SearchEnv.execute_command('FT.DROP ...')
    reader.cont()
    async_res.read_response().equal('Unknown index')