from rmtest import BaseModuleTestCase


class BaseSearchTestCase(BaseModuleTestCase):
    def search(self, idx, *args):
        if not isinstance(idx, basestring):
            raise Exception("OOPS!")
        return self.cmd('ft.search', idx, *args)
