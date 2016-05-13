from rmtest import ModuleTestCase
import redis
import unittest

class SearchTestCase(ModuleTestCase('../module.so')):
    
    
    
    def _addDoc(r, id, body, title, score=1.0):
    
        r.execute_command('ft.add', )
        
    def assertOk(self, x):
    
        self.assertEquals("OK", x)
        
    def assertExists(self, r, key):
        self.assertTrue(r.exists(key))
            
    def testAdd(self):
        with self.redis() as r:
            
            self.assertOk(r.execute_command('ft.create', 'idx', 'title', 1.0, 'body', 1.0))
            self.assertTrue(r.exists('idx:idx'))
            self.assertOk(r.execute_command('ft.add', 'idx', 'doc1', 1.0, 'fields',
                                 'title', 'hello world',
                                 'body', 'lorem ist ipsum'))
            
            for prefix in ('ft', 'si', 'ss'):
                self.assertExists(r, prefix+':idx/hello')
                self.assertExists(r, prefix+':idx/world')
                self.assertExists(r, prefix+':idx/lorem')
               
    def testSearch(self):
        with self.redis() as r:
            self.assertOk(r.execute_command('ft.create', 'idx', 'title', 10.0, 'body', 1.0))
            self.assertOk(r.execute_command('ft.add', 'idx', 'doc1', 0.5, 'fields',
                                 'title', 'hello world',
                                 'body', 'lorem ist ipsum'))
            self.assertOk(r.execute_command('ft.add', 'idx', 'doc2', 1.0, 'fields',
                                 'title', 'hello another world',
                                 'body', 'lorem ist ipsum lorem lorem'))
                                 
            res = r.execute_command('ft.search', 'idx', 'hello')                   
            
            self.assertTrue(len(res) == 5)
            self.assertEqual(res[0], 2L)
            self.assertEqual(res[1], "doc2")
        
        
        
if __name__ == '__main__':

    unittest.main(verbosity=100)