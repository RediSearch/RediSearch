from rmtest import ModuleTestCase


class BaseSearchTestCase(ModuleTestCase('../src/module-oss.so')):

    def setUp(self):
        self.flushdb()

    def search(self, *args):
        return self.cmd('ft.search', *args)

    def flushdb(self):
        self.cmd('flushdb')
