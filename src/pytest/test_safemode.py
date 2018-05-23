# -*- coding: utf-8 -*-
from test import SearchTestCase
import os

class SafemodeTestCase(SearchTestCase):
    # TODO: Implement a proper API in rmtest and expose this correctly
    _loadmodule_args = (
        os.environ.get('REDIS_MODULE_PATH', '../redisearch.so'), 'SAFEMODE',)