from rmtest import ModuleTestCase
import redis
import unittest

class SearchTestCase(ModuleTestCase('../module.so')):
    
            
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

            # Test searching WITHSCORES
            res = r.execute_command('ft.search', 'idx', 'hello', 'WITHSCORES')
            self.assertEqual(len(res), 7)
            self.assertEqual(res[0], 2L)
            self.assertEqual(res[1], "doc2")
            self.assertTrue(float(res[2]) > 0)
            self.assertEqual(res[4], "doc1")
            self.assertTrue(float(res[5]) > 0)

            # Test searching WITHSCORES NOCONTENT
            res = r.execute_command('ft.search', 'idx', 'hello', 'WITHSCORES', 'NOCONTENT')
            self.assertEqual(len(res), 5)
            self.assertEqual(res[0], 2L)
            self.assertEqual(res[1], "doc2")
            self.assertTrue(float(res[2]) > 0)
            self.assertEqual(res[3], "doc1")
            self.assertTrue(float(res[4]) > 0)
            
            
    def testExact(self):
        with self.redis() as r:
            r.flushdb()
            self.assertOk(r.execute_command('ft.create', 'idx', 'title', 10.0, 'body', 1.0))
            self.assertOk(r.execute_command('ft.add', 'idx', 'doc1', 0.5, 'fields',
                                 'title', 'hello world',
                                 'body', 'lorem ist ipsum'))
            self.assertOk(r.execute_command('ft.add', 'idx', 'doc2', 1.0, 'fields',
                                 'title', 'hello another world',
                                 'body', 'lorem ist ipsum lorem lorem'))
                                 
            res = r.execute_command('ft.search', 'idx', '"hello world"', 'verbatim')
            self.assertEqual(3, len(res))     
            self.assertEqual(1, res[0])
            self.assertEqual("doc1", res[1])
            
            # res = r.execute_command('ft.search', 'idx', "hello \"another world\"", 'verbatim') 
            # self.assertEqual(3, len(res))     
            # self.assertEqual(1, res[0])
            # self.assertEqual("doc2", res[1])
            
           
            
    
    def testInfields(self):
        with self.redis() as r:
            self.assertOk(r.execute_command('ft.create', 'idx', 'title', 10.0, 'body', 1.0))
            self.assertOk(r.execute_command('ft.add', 'idx', 'doc1', 0.5, 'fields',
                                 'title', 'hello world',
                                 'body', 'lorem ipsum'))

            self.assertOk(r.execute_command('ft.add', 'idx', 'doc2', 1.0, 'fields',
                                 'title', 'hello world lorem ipsum',
                                 'body', 'hello world'))
                                 
            res = r.execute_command('ft.search', 'idx', 'hello world', 'verbatim', "infields", 1, "title", "nocontent") 
            self.assertEqual(3, len(res))     
            self.assertEqual(2, res[0])
            self.assertEqual("doc2", res[1])
            self.assertEqual("doc1", res[2])
            
            res = r.execute_command('ft.search', 'idx', 'hello world', 'verbatim', "infields", 1, "body", "nocontent") 
            self.assertEqual(2, len(res))     
            self.assertEqual(1, res[0])
            self.assertEqual("doc2", res[1])
            
            res = r.execute_command('ft.search', 'idx', 'hello', 'verbatim', "infields", 1, "body", "nocontent") 
            self.assertEqual(2, len(res))     
            self.assertEqual(1, res[0])
            self.assertEqual("doc2", res[1])
            
            res = r.execute_command('ft.search', 'idx',  '\"hello world\"', 'verbatim', "infields", 1, "body", "nocontent")
            print res 
            self.assertEqual(2, len(res))     
            self.assertEqual(1, res[0])
            self.assertEqual("doc2", res[1])
            
            res = r.execute_command('ft.search', 'idx', '\"lorem ipsum\"', 'verbatim', "infields", 1, "body", "nocontent") 
            self.assertEqual(2, len(res))     
            self.assertEqual(1, res[0])
            self.assertEqual("doc1", res[1])
            
            res = r.execute_command('ft.search', 'idx', 'lorem ipsum', "infields", 2, "body", "title", "nocontent")
            self.assertEqual(3, len(res))     
            self.assertEqual(2, res[0])
            self.assertEqual("doc2", res[1])
            self.assertEqual("doc1", res[2])
            
            
    def testStemming(self):
        with self.redis() as r:
            self.assertOk(r.execute_command('ft.create', 'idx', 'title', 10.0))
            self.assertOk(r.execute_command('ft.add', 'idx', 'doc1', 0.5, 'fields',
                                 'title', 'hello kitty'))
            self.assertOk(r.execute_command('ft.add', 'idx', 'doc2', 1.0, 'fields',
                                 'title', 'hello kitties'))
                                                
            res = r.execute_command('ft.search', 'idx', 'hello kitty', "nocontent")
            self.assertEqual(3, len(res))     
            self.assertEqual(2, res[0])      
            
            res = r.execute_command('ft.search', 'idx', 'hello kitty', "nocontent", "verbatim")
            self.assertEqual(2, len(res))     
            self.assertEqual(1, res[0])    
            
            
    def testNumericRange(self):
        
        with self.redis() as r:
        
            self.assertOk(r.execute_command('ft.create', 'idx', 'title', 10.0, 'score', 'numeric'))
            for i in xrange(100):
                self.assertOk(r.execute_command('ft.add', 'idx', 'doc%d' % i, 1, 'fields',
                                    'title', 'hello kitty', 'score', i))
            
            res = r.execute_command('ft.search', 'idx', 'hello kitty', "nocontent", 
                                    "filter", "score", 0, 100 )
            
            self.assertEqual(11, len(res))     
            self.assertEqual(100, res[0])
            
            res = r.execute_command('ft.search', 'idx', 'hello kitty', "nocontent", 
                                    "filter", "score", 0, 50 )
            self.assertEqual(51, res[0])                      
            res = r.execute_command('ft.search', 'idx', 'hello kitty', "nocontent", 
                                    "filter", "score", "(0", "(50" )
            self.assertEqual(49, res[0])
            res = r.execute_command('ft.search', 'idx', 'hello kitty', "nocontent", 
                                    "filter", "score", "-inf", "+inf" )
            self.assertEqual(100, res[0])
                                                
                                                                                   
                                    
    def testSuggestions(self):
        
        with self.redis() as r:
        
            self.assertEqual(1, r.execute_command('ft.SUGADD', 'ac', 'hello world', 1))
            self.assertEqual(1, r.execute_command('ft.SUGADD', 'ac', 'hello world', 1, 'INCR'))

            res = r.execute_command("FT.SUGGET", "ac", "hello")
            self.assertEqual(1, len(res))
            self.assertEqual("hello world", res[0])

            terms = ["hello werld", "hallo world", "yellow world", "wazzup", "herp", "derp"]
            sz = 2
            for term in terms:
                self.assertEqual(sz, r.execute_command('ft.SUGADD', 'ac', term, sz-1))
                sz+=1      
            
            
            self.assertEqual(7, r.execute_command('ft.SUGLEN', 'ac'))

            # search not fuzzy
            self.assertEqual(["hello world", "hello werld"], r.execute_command("ft.SUGGET", "ac", "hello"))
            
            #print  r.execute_command("ft.SUGGET", "ac", "hello", "FUZZY", "MAX", "1", "WITHSCORES")
            # search fuzzy - shuold yield more results
            self.assertEqual(['hello world', 'hello werld', 'yellow world', 'hallo world'], 
                             r.execute_command("ft.SUGGET", "ac", "hello", "FUZZY"))

            # search fuzzy with limit of 1 
            self.assertEqual(['hello world'], 
                             r.execute_command("ft.SUGGET", "ac", "hello", "FUZZY", "MAX", "1"))
     
            # scores should return on WITHSCORES
            rc = r.execute_command("ft.SUGGET", "ac", "hello", "WITHSCORES")
            self.assertEqual(4, len(rc))
            self.assertTrue(float(rc[1]) > 0)
            self.assertTrue(float(rc[3]) > 0)
            
if __name__ == '__main__':

    unittest.main()