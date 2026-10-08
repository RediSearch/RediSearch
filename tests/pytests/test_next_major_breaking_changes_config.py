# Copyright (c) 2006-Present, Redis Ltd.
# All rights reserved.
#
# Licensed under your choice of the Redis Source Available License 2.0
# (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
# GNU Affero General Public License v3 (AGPLv3).

"""
Tests for the search-_enable-next-major-breaking-changes module config.

The config opts into behavior changes staged for the next major release, such as refusing
TEXT values that are not well-formed UTF-8. It is immutable (load-time only) and defaults
to 'no'.

It is registered as a modern CONFIG parameter only: there is no FT.CONFIG alias and no legacy
module-arguments spelling, so 'CONFIG SET' at startup — via redis.conf or MODULE LOADEX — is
the only way to turn it on. The 'moduleArgs=' pattern other config tests use does not reach it.
"""

import os

from common import *
# Reused rather than duplicated: _removeModuleArgs pokes at RLTest internals, and that
# knowledge belongs in one place.
from test_config import _getRDBFilePath, _removeModuleArgs

CONFIG_NAME = 'search-_enable-next-major-breaking-changes'


@skip(cluster=True, redis_less_than='7.9.227')
def test_next_major_default():
    """A server started without the config reports it as off."""
    env = Env(noDefaultModuleArgs=True)
    if env.env == 'existing-env':
        env.skip()
    env.expect('CONFIG', 'GET', CONFIG_NAME).equal([CONFIG_NAME, 'no'])


@skip(cluster=True, redis_less_than='7.9.227')
def test_next_major_immutable(env):
    """CONFIG SET at runtime is refused in both directions: the config is registered immutable."""
    env.expect('CONFIG', 'SET', CONFIG_NAME, 'yes').error()
    env.expect('CONFIG', 'SET', CONFIG_NAME, 'no').error()


@skip(cluster=True, redis_less_than='7.9.227')
def test_next_major_not_exposed_via_ft_config(env):
    """FT.CONFIG GET matches nothing and FT.CONFIG SET errors: the config has no legacy alias."""
    # FT.CONFIG dispatches through its own table of legacy names, which this config is
    # deliberately absent from.
    env.expect(config_cmd(), 'GET', CONFIG_NAME).equal([])
    env.expect(config_cmd(), 'SET', CONFIG_NAME, 'yes').error()


@skip(cluster=True, redis_less_than='7.9.227')
def test_next_major_no_legacy_module_args():
    """MODULE LOADEX ... ARGS with the legacy uppercase spelling fails the load: no legacy module-arguments entry exists."""
    env = Env(noDefaultModuleArgs=True)
    if env.env == 'existing-env':
        env.skip()

    rdbFilePath = _getRDBFilePath(env)
    env.stop()
    os.unlink(rdbFilePath)

    redisearch_module_path = env.envRunner.modulePath[0]
    _removeModuleArgs(env)

    env.start()
    # The uppercase name follows the legacy-args convention of sibling configs like
    # _FREE_RESOURCE_ON_THREAD; ReadConfig must not recognize it, failing the load.
    env.expect('MODULE', 'LOADEX', redisearch_module_path, 'ARGS',
               '_ENABLE_NEXT_MAJOR_BREAKING_CHANGES', 'true').error()
    env.stop()
