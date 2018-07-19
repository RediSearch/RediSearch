from rmtest2 import BaseModuleTestCase
import redis


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


class BaseSearchTestCase(BaseModuleTestCase, FTBaseCaseMethods):
    pass
