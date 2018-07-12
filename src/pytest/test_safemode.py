# -*- coding: utf-8 -*-
from test import SearchTestCase


class SafemodeTestCase(SearchTestCase):
    @classmethod
    def get_module_args(cls):
        return super(SafemodeTestCase, cls).get_module_args() + ['SAFEMODE']
