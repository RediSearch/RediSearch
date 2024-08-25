from common import *

class TestACL():
    def __init__(self):
        self.env = Env()

    def test_acl_category(self):
        """Test that the `search` category was added appropriately in module
        load"""
        res = self.env.cmd('ACL', 'CAT')
        self.env.assertTrue('search' in res)

    def test_acl_search_commands(self):
        """Tests that the RediSearch commands are registered to the `search`
        ACL category"""
        res = self.env.cmd('ACL', 'CAT', 'search')
        # Verify that we don't get an empty array back
        commands = [
            'FT._LIST', '_FT.DEBUG', 'FT.EXPLAIN', '_FT.ALIASUPDATE',
            '_FT.SPELLCHECK', '_FT.ALIASDEL', '_FT.SYNUPDATE', '_FT.ALIASADD',
            '_FT.SUGLEN', '_FT.AGGREGATE', '_FT.CREATE', '_FT.SUGDEL',
            '_FT.ALTER', 'FT.DICTDUMP', '_FT.INFO', '_FT.SUGADD', '_FT.CURSOR',
            'FT.EXPLAINCLI', '_FT.SUGGET', '_FT.DICTDEL', '_FT.DICTADD',
            '_FT.PROFILE', 'FT.SYNDUMP'
        ]
        # Use a set since the order of the response is not consistent.
        commands = set(commands)
        self.env.assertEqual(set(res), commands)
