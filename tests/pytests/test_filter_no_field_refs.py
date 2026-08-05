# -*- coding: utf-8 -*-
#
# A FILTER expression that references no schema fields leaves the rule's
# filter-field arrays empty; writes to a matching prefix must still be
# indexed or rejected without crashing the server.

from includes import *
from common import *
from RLTest import Env


def testFilterConstantBoolean(env):
    """Constant FILTER expression with no @field references must not crash
    Redis on a matching key write."""
    conn = getConnectionByEnv(env)

    env.cmd('FT.CREATE', 'idx_const', 'ON', 'HASH',
            'PREFIX', '1', 'doc:',
            'FILTER', '1 == 1',
            'SCHEMA', 'name', 'TEXT')

    conn.execute_command('HSET', 'doc:1', 'name', 'hello')

    env.assertTrue(env.isUp(), message='server died after HSET into a prefix with a constant FILTER')

    env.expect('FT.SEARCH', 'idx_const', 'hello') \
       .equal([1, 'doc:1', ['name', 'hello']])


def testFilterLiteralOnlyFunction(env):
    """FILTER over a function call whose arguments are all literals also has
    zero property references and hits the same NULL filter_fields path."""
    conn = getConnectionByEnv(env)

    env.cmd('FT.CREATE', 'idx_litfn', 'ON', 'HASH',
            'PREFIX', '1', 'doc:',
            'FILTER', 'startswith("foo", "fo")',
            'SCHEMA', 'name', 'TEXT')

    conn.execute_command('HSET', 'doc:2', 'name', 'world')

    env.assertTrue(env.isUp(), message='server died after HSET into a prefix with a literal-only FILTER function')

    env.expect('FT.SEARCH', 'idx_litfn', 'world') \
       .equal([1, 'doc:2', ['name', 'world']])


def testFilterAlwaysFalseConstant(env):
    """A constant FILTER that evaluates to false should reject the doc without
    crashing -- exercises the same NULL slice construction on the reject path."""
    conn = getConnectionByEnv(env)

    env.cmd('FT.CREATE', 'idx_false', 'ON', 'HASH',
            'PREFIX', '1', 'doc:',
            'FILTER', '1 == 2',
            'SCHEMA', 'name', 'TEXT')

    conn.execute_command('HSET', 'doc:3', 'name', 'ignored')

    env.assertTrue(env.isUp(), message='server died after HSET when constant FILTER evaluated to false')

    env.expect('FT.SEARCH', 'idx_false', 'ignored').equal([0])
