# -*- coding: utf-8 -*-
from test import SearchTestCase

class SafemodeTestCase(SearchTestCase):
    # TODO: Implement a proper API in rmtest and expose this correctly
    _loadmodule_args = ('../redisearch.so', 'SAFEMODE',)