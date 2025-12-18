from docs_utils import *


class testCommandDocsAndHelp:
    def __init__(self):
        self.env = Env(decodeResponses=True)

    def test_command_docs_json_arrappend(self):
        env = self.env
        if server_version_is_less_than("7.0.0"):
            env.skip()
        assert_docs(
            env,
            "json.arrappend",
            summary="Append the JSON values into the array at path after the last element in it",
            complexity="O(1) when path is evaluated to a single value, O(N) when path is evaluated to multiple values, where N is the size of the key",
            arity=-3,
            since="1.0.0",
            group="module",
        )

    def test_command_docs_json_arrindex(self):
        env = self.env
        if server_version_is_less_than("7.0.0"):
            env.skip()
        assert_docs(
            env,
            "json.arrindex",
            summary="Search for the first occurrence of a JSON value in an array",
            complexity="O(N) when path is evaluated to a single value where N is the size of the array, O(N) when path is evaluated to multiple values, where N is the size of the key",
            arity=-4,
            since="1.0.0",
            group="module",
        )

    def test_command_docs_json_arrinsert(self):
        env = self.env
        if server_version_is_less_than("7.0.0"):
            env.skip()
        assert_docs(
            env,
            "json.arrinsert",
            summary="Insert the json values into the array at path before the index (shifts to the right)",
            complexity="O(N) when path is evaluated to a single value where N is the size of the array, O(N) when path is evaluated to multiple values, where N is the size of the key",
            arity=-5,
            since="1.0.0",
            group="module",
        )

    def test_command_docs_json_arrlen(self):
        env = self.env
        if server_version_is_less_than("7.0.0"):
            env.skip()
        assert_docs(
            env,
            "json.arrlen",
            summary="Report the length of the JSON array at path in key",
            complexity="O(1) where path is evaluated to a single value, O(N) where path is evaluated to multiple values, where N is the size of the key",
            arity=-2,
            since="1.0.0",
            group="module",
        )

    def test_command_docs_json_arrpop(self):
        env = self.env
        if server_version_is_less_than("7.0.0"):
            env.skip()
        assert_docs(
            env,
            "json.arrpop",
            summary="Remove and return the element at the specified index in the array at path",
            complexity="O(N) when path is evaluated to a single value where N is the size of the array and the specified index is not the last element, O(1) when path is evaluated to a single value and the specified index is the last element, or O(N) when path is evaluated to multiple values, where N is the size of the key",
            arity=-2,
            since="1.0.0",
            group="module",
        )

    def test_command_docs_json_arrtrim(self):
        env = self.env
        if server_version_is_less_than("7.0.0"):
            env.skip()
        assert_docs(
            env,
            "json.arrtrim",
            summary="Trim an array so that it contains only the specified inclusive range of elements",
            complexity="O(N) when path is evaluated to a single value where N is the size of the array, O(N) when path is evaluated to multiple values, where N is the size of the key",
            arity=5,
            since="1.0.0",
            group="module",
        )
    
    def test_command_docs_json_clear(self):
        env = self.env
        if server_version_is_less_than("7.0.0"):
            env.skip()
        assert_docs(
            env,
            "json.clear",
            summary="Clear container values (arrays/objects) and set numeric values to 0",
            complexity="O(N) when path is evaluated to a single value where N is the size of the values, O(N) when path is evaluated to multiple values, where N is the size of the key",
            arity=-2,
            since="2.0.0",
            group="module",
        )

    def test_command_docs_json_debug(self):
        env = self.env
        if server_version_is_less_than("7.0.0"):
            env.skip()
        assert_docs(
            env,
            "json.debug",
            summary="This is a container command for debugging related tasks",
            complexity="N/A",
            arity=-2,
            since="1.0.0",
            group="module",
        )

    def test_command_docs_json_del(self):
        env = self.env
        if server_version_is_less_than("7.0.0"):
            env.skip()
        assert_docs(
            env,
            "json.del",
            summary="Delete a value",
            complexity="O(N) when path is evaluated to a single value where N is the size of the deleted value, O(N) when path is evaluated to multiple values, where N is the size of the key",
            arity=-2,
            since="1.0.0",
            group="module",
        )
    
    def test_command_docs_json_forget(self):
        env = self.env
        if server_version_is_less_than("7.0.0"):
            env.skip()
        assert_docs(
            env,
            "json.forget",
            summary="Delete a value",
            complexity="O(N) when path is evaluated to a single value where N is the size of the deleted value, O(N) when path is evaluated to multiple values, where N is the size of the key",
            arity=-2,
            since="1.0.0",
            group="module",
        )
    
    def test_command_docs_json_get(self):
        env = self.env
        if server_version_is_less_than("7.0.0"):
            env.skip()
        assert_docs(
            env,
            "json.get",
            summary="Get JSON value at path",
            complexity="O(N) where N is the size of the JSON",
            arity=-2,
            since="1.0.0",
            group="module",
        )
    
    def test_command_docs_json_merge(self):
        env = self.env
        if server_version_is_less_than("7.0.0"):
            env.skip()
        assert_docs(
            env,
            "json.merge",
            summary="Merge a given JSON value into matching paths. Consequently, JSON values at matching paths are updated, deleted, or expanded with new children",
            complexity="O(M+N) when path is evaluated to a single value where M is the size of the original value (if it exists) and N is the size of the new value, O(M+N) when path is evaluated to multiple values where M is the size of the key and N is the size of the new value * the number of original values in the key",
            arity=-4,
            since="2.6.0",
            group="module",
        )

    def test_command_docs_json_mget(self):
        env = self.env
        if server_version_is_less_than("7.0.0"):
            env.skip()
        assert_docs(
            env,
            "json.mget",
            summary="Return the values at path from multiple key arguments",
            complexity="O(M*N) when path is evaluated to a single value where M is the number of keys and N is the size of the value, O(N1+N2+...+Nm) when path is evaluated to multiple values where m is the number of keys and Ni is the size of the i-th key",
            arity=-3,
            since="1.0.0",
            group="module",
        )
    
    def test_command_docs_json_mset(self):
        env = self.env
        if server_version_is_less_than("7.0.0"):
            env.skip()
        assert_docs(
            env,
            "json.mset",
            summary="Set or update one or more JSON values according to the specified key-path-value triplets",
            complexity="O(K*(M+N)) where k is the number of keys in the command, when path is evaluated to a single value where M is the size of the original value (if it exists) and N is the size of the new value, or O(K*(M+N)) when path is evaluated to multiple values where M is the size of the key and N is the size of the new value * the number of original values in the key",
            arity=-4,
            since="2.6.0",
            group="module",
        )
    
    def test_command_docs_json_numincrby(self):
        env = self.env
        if server_version_is_less_than("7.0.0"):
            env.skip()
        assert_docs(
            env,
            "json.numincrby",
            summary="Increment the number value stored at path by number",
            complexity="O(1) when path is evaluated to a single value, O(N) when path is evaluated to multiple values, where N is the size of the key",
            arity=4,
            since="1.0.0",
            group="module",
        )
    
    def test_command_docs_json_nummultby(self):
        env = self.env
        if server_version_is_less_than("7.0.0"):
            env.skip()
        assert_docs(
            env,
            "json.nummultby",
            summary="Multiply the number value stored at path by number",
            complexity="O(1) when path is evaluated to a single value, O(N) when path is evaluated to multiple values, where N is the size of the key",
            arity=4,
            since="1.0.0",
            group="module",
        )
    
    def test_command_docs_json_numpowby(self):
        env = self.env
        if server_version_is_less_than("7.0.0"):
            env.skip()
        assert_docs(
            env,
            "json.numpowby",
            summary="Raise the number value stored at path to the power of number",
            complexity="O(1) when path is evaluated to a single value, O(N) when path is evaluated to multiple values, where N is the size of the key",
            arity=4,
            since="1.0.0",
            group="module",
        )

    def test_command_docs_json_objkeys(self):
        env = self.env
        if server_version_is_less_than("7.0.0"):
            env.skip()
        assert_docs(
            env,
            "json.objkeys",
            summary="Return the keys in the object that's referenced by path",
            complexity="O(1) when path is evaluated to a single value, O(N) when path is evaluated to multiple values, where N is the size of the key",
            arity=-2,
            since="1.0.0",
            group="module",
        )

    def test_command_docs_json_objlen(self):
        env = self.env
        if server_version_is_less_than("7.0.0"):
            env.skip()
        assert_docs(
            env,
            "json.objlen",
            summary="Report the number of keys in the JSON object at path in key",
            complexity="O(1) when path is evaluated to a single value, O(N) when path is evaluated to multiple values, where N is the size of the key",
            arity=-2,
            since="1.0.0",
            group="module",
        )

    def test_command_docs_json_resp(self):
        env = self.env
        if server_version_is_less_than("7.0.0"):
            env.skip()
        assert_docs(
            env,
            "json.resp",
            summary="Return the JSON in key in Redis serialization protocol specification form",
            complexity="O(N) when path is evaluated to a single value, where N is the size of the value, O(N) when path is evaluated to multiple values, where N is the size of the key",
            arity=-2,
            since="1.0.0",
            group="module",
        )

    def test_command_docs_json_set(self):
        env = self.env
        if server_version_is_less_than("7.0.0"):
            env.skip()
        assert_docs(
            env,
            "json.set",
            summary="Set the JSON value at path in key",
            complexity="O(M+N) where M is the size of the original value (if it exists) and N is the size of the new value",
            arity=-4,
            since="1.0.0",
            group="module",
        )

    def test_command_docs_json_strappend(self):
        env = self.env
        if server_version_is_less_than("7.0.0"):
            env.skip()
        assert_docs(
            env,
            "json.strappend",
            summary="Append the json-string values to the string at path",
            complexity="O(1) when path is evaluated to a single value, O(N) when path is evaluated to multiple values, where N is the size of the key",
            arity=-3,
            since="1.0.0",
            group="module",
        )

    def test_command_docs_json_strlen(self):
        env = self.env
        if server_version_is_less_than("7.0.0"):
            env.skip()
        assert_docs(
            env,
            "json.strlen",
            summary="Report the length of the JSON String at path in key",
            complexity="O(1) when path is evaluated to a single value, O(N) when path is evaluated to multiple values, where N is the size of the key",
            arity=-2,
            since="1.0.0",
            group="module",
        )

    def test_command_docs_json_toggle(self):
        env = self.env
        if server_version_is_less_than("7.0.0"):
            env.skip()
        assert_docs(
            env,
            "json.toggle",
            summary="Toggle the boolean value stored at path",
            complexity="O(1) when path is evaluated to a single value, O(N) when path is evaluated to multiple values, where N is the size of the key",
            arity=3,
            since="2.0.0",
            group="module",
        )

    def test_command_docs_json_type(self):
        env = self.env
        if server_version_is_less_than("7.0.0"):
            env.skip()
        assert_docs(
            env,
            "json.type",
            summary="Report the type of JSON value at path",
            complexity="O(1) when path is evaluated to a single value, O(N) when path is evaluated to multiple values, where N is the size of the key",
            arity=-2,
            since="1.0.0",
            group="module",
        )