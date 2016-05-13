from rmtest import ModuleTestCase
import redis
import unittest

class SearchTestCase(ModuleTestCase('../module.so')):
    
    def _addDoc(r, id, body, title, score=1.0):
    
        r.execute_command('ft.add', )
        
    
            
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
            self.assertTrue(isinstance(res[2], list))
            self.assertTrue('title' in res[2])
            self.assertTrue('hello another world' in res[2])
            self.assertEqual(res[3], "doc1")
            self.assertTrue('hello world' in res[4])

            # Test searching with no content
            res = r.execute_command('ft.search', 'idx', 'hello', 'nocontent')  
            self.assertTrue(len(res) == 3)
            self.assertEqual(res[0], 2L)
            self.assertEqual(res[1], "doc2")
            self.assertEqual(res[2], "doc1") 
            
    def testExact(self):
        with self.redis(port=9979) as r:
            self.assertOk(r.execute_command('ft.create', 'idx', 'title', 10.0, 'body', 1.0))
            self.assertOk(r.execute_command('ft.add', 'idx', 'doc1', 0.5, 'fields',
                                 'title', 'hello world',
                                 'body', 'lorem ist ipsum'))
            self.assertOk(r.execute_command('ft.add', 'idx', 'doc2', 1.0, 'fields',
                                 'title', 'hello another world',
                                 'body', 'lorem ist ipsum lorem lorem'))
                                 
            res = r.execute_command('ft.search', 'idx', '"hello world"') 
            self.assertEqual(3, len(res))     
            self.assertEqual(1, res[0])
            self.assertEqual("doc1", res[1])
            
            res = r.execute_command('ft.search', 'idx', "hello \"another world\"") 
            self.assertEqual(3, len(res))     
            self.assertEqual(1, res[0])
            self.assertEqual("doc2", res[1])
            
            res = r.execute_command('ft.search', 'idx', 'hello "another world"') 
            self.assertEqual(3, len(res))     
            self.assertEqual(1, res[0])
            self.assertEqual("doc2", res[1])
                 
     
            
            
if __name__ == '__main__':

    unittest.main()