import unittest
from .disposableredis import DisposableRedis
import os

def ModuleTestCase(module_path, redis_path = 'redis-server'):

    class _ModuleTestCase(unittest.TestCase):
        _module_path = os.path.abspath(os.path.join(os.getcwd(), module_path))
        _redis_path =  redis_path
        
        def redis(self, port=None):
            return DisposableRedis(port=port, path = self._redis_path, loadmodule = self._module_path)  
            
        def assertOk(self, x):
            self.assertEquals("OK", x)
        
        def assertExists(self, r, key):
            self.assertTrue(r.exists(key))  
    
    return _ModuleTestCase
    
   