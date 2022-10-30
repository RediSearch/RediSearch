from rmtest2 import BaseModuleTestCase
import redis
import os


class FTBaseCaseMethods(object):
    def ftcreate(self, idx, *args):
        return self.execute_command('ft.create', idx, *args)

    def search(self, idx, *args):
        if not isinstance(idx, basestring):
            raise Exception("OOPS!")
        return self.cmd('ft.search', idx, *args)

    def ftexists(self, idx, doc):
        try:
            self.ftget(idx, doc)
            return True

        except redis.ResponseError:
            return False

    def ftget(self, idx, doc):
        return self.execute_command('ft.get', idx, doc)

    def ftadd(self, idx, docid, weight=1.0, **fields):
        cmd = ['FT.ADD', idx, docid, weight, 'FIELDS']
        for k, v in fields.items():
            cmd += [k, v]
        return self.cmd(*cmd)


class BaseSearchTestCase(BaseModuleTestCase, FTBaseCaseMethods):
    @classmethod
    def get_module_args(cls):
        rv = super(BaseSearchTestCase, cls).get_module_args()
        if os.environ.get('RS_TEST_SAFEMODE'):
            rv += ['SAFEMODE']
        if os.environ.get('GC_POLICY_FORK'):
            rv += ['GC_POLICY', 'FORK']
        return rv
