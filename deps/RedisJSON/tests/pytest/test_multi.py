# -*- coding: utf-8 -*-

import sys
import os
import redis
import json
from RLTest import Env

from common import *
from includes import *

from RLTest import Defaults

from functools import reduce

Defaults.decode_responses = True

# ----------------------------------------------------------------------------------------------

# Path to JSON test case files
HERE = os.path.abspath(os.path.dirname(__file__))
ROOT = os.path.abspath(os.path.join(HERE, "../.."))
TESTS_ROOT = os.path.abspath(os.path.join(HERE, ".."))
JSON_PATH = os.path.join(TESTS_ROOT, 'files')

nested_large_key = r'{"jkra":[154,4472,[8567,false,363.84,5276,"ha","rizkzs",93],false],"hh":20.77,"mr":973.217,"ihbe":[68,[true,{"lqe":[486.363,[true,{"mp":{"ory":"rj","qnl":"tyfrju","hf":null},"uooc":7418,"xela":20,"bt":7014,"ia":547,"szec":68.73},null],3622,"iwk",null],"fepi":19.954,"ivu":{"rmnd":65.539,"bk":98,"nc":"bdg","dlb":{"hw":{"upzz":[true,{"nwb":[4259.47],"nbt":"yl"},false,false,65,[[[],629.149,"lvynqh","hsk",[],2011.932,true,[]],null,"ymbc",null],"aj",97.425,"hc",58]},"jq":true,"bi":3333,"hmf":"pl","mrbj":[true,false]}},"hfj":"lwk","utdl":"aku","alqb":[74,534.389,7235,[null,false,null]]},null,{"lbrx":{"vm":"ubdrbb"},"tie":"iok","br":"ojro"},70.558,[{"mmo":null,"dryu":null}]],true,null,false,{"jqun":98,"ivhq":[[[675.936,[520.15,1587.4,false],"jt",true,{"bn":null,"ygn":"cve","zhh":true,"aak":9165,"skx":true,"qqsk":662.28},{"eio":9933.6,"agl":null,"pf":false,"kv":5099.631,"no":null,"shly":58},[null,["uiundu",726.652,false,94.92,259.62,{"ntqu":null,"frv":null,"rvop":"upefj","jvdp":{"nhx":[],"bxnu":{},"gs":null,"mqho":null,"xp":65,"ujj":{}},"ts":false,"kyuk":[false,58,{},"khqqif"]},167,true,"bhlej",53],64,{"eans":"wgzfo","zfgb":431.67,"udy":[{"gnt":[],"zeve":{}},{"pg":{},"vsuc":{},"dw":19,"ffo":"uwsh","spk":"pjdyam","mc":[],"wunb":{},"qcze":2271.15,"mcqx":null},"qob"],"wo":"zy"},{"dok":null,"ygk":null,"afdw":[7848,"ah",null],"foobar":3.141592,"wnuo":{"zpvi":{"stw":true,"bq":{},"zord":true,"omne":3061.73,"bnwm":"wuuyy","tuv":7053,"lepv":null,"xap":94.26},"nuv":false,"hhza":539.615,"rqw":{"dk":2305,"wibo":7512.9,"ytbc":153,"pokp":null,"whzd":null,"judg":[],"zh":null},"bcnu":"ji","yhqu":null,"gwc":true,"smp":{"fxpl":75,"gc":[],"vx":9352.895,"fbzf":4138.27,"tiaq":354.306,"kmfb":{},"fxhy":[],"af":94.46,"wg":{},"fb":null}},"zvym":2921,"hhlh":[45,214.345],"vv":"gqjoz"},["uxlu",null,"utl",64,[2695],[false,null,["cfcrl",[],[],562,1654.9,{},null,"sqzud",934.6],{"hk":true,"ed":"lodube","ye":"ziwddj","ps":null,"ir":{},"heh":false},true,719,50.56,[99,6409,null,4886,"esdtkt",{},null],[false,"bkzqw"]],null,6357],{"asvv":22.873,"vqm":{"drmv":68.12,"tmf":140.495,"le":null,"sanf":[true,[],"vyawd",false,76.496,[],"sdfpr",33.16,"nrxy","antje"],"yrkh":662.426,"vxj":true,"sn":314.382,"eorg":null},"bavq":[21.18,8742.66,{"eq":"urnd"},56.63,"fw",[{},"pjtr",null,"apyemk",[],[],false,{}],{"ho":null,"ir":124,"oevp":159,"xdrv":6705,"ff":[],"sx":false},true,null,true],"zw":"qjqaap","hr":{"xz":32,"mj":8235.32,"yrtv":null,"jcz":"vnemxe","ywai":[null,564,false,"vbr",54.741],"vw":82,"wn":true,"pav":true},"vxa":881},"bgt","vuzk",857]]],null,null,{"xyzl":"nvfff"},true,13],"npd":null,"ha":[["du",[980,{"zdhd":[129.986,["liehns",453,{"fuq":false,"dxpn":{},"hmpx":49,"zb":"gbpt","vdqc":null,"ysjg":false,"gug":7990.66},"evek",[{}],"dfywcu",9686,null]],"gpi":{"gt":{"qe":7460,"nh":"nrn","czj":66.609,"jwd":true,"rb":"azwwe","fj":{"csn":true,"foobar":1.61803398875,"hm":"efsgw","zn":"vbpizt","tjo":138.15,"teo":{},"hecf":[],"ls":false}},"xlc":7916,"jqst":48.166,"zj":"ivctu"},"jl":369.27,"mxkx":null,"sh":[true,373,false,"sdis",6217,{"ernm":null,"srbo":90.798,"py":677,"jgrq":null,"zujl":null,"odsm":{"pfrd":null,"kwz":"kfvjzb","ptkp":false,"pu":null,"xty":null,"ntx":[],"nq":48.19,"lpyx":[]},"ff":null,"rvi":["ych",{},72,9379,7897.383,true,{},999.751,false]},true],"ghe":[24,{"lpr":true,"qrs":true},true,false,7951.94,true,2690.54,[93,null,null,"rlz",true,"ky",true]],"vet":false,"olle":null},"jzm",true],null,null,19.17,7145,"ipsmk"],false,{"du":6550.959,"sps":8783.62,"nblr":{"dko":9856.616,"lz":{"phng":"dj"},"zeu":766,"tn":"dkr"},"xa":"trdw","gn":9875.687,"dl":null,"vuql":null},{"qpjo":null,"das":{"or":{"xfy":null,"xwvs":4181.86,"yj":206.325,"bsr":["qrtsh"],"wndm":{"ve":56,"jyqa":true,"ca":null},"rpd":9906,"ea":"dvzcyt"},"xwnn":9272,"rpx":"zpr","srzg":{"beo":325.6,"sq":null,"yf":null,"nu":[377,"qda",true],"sfz":"zjk"},"kh":"xnpj","rk":null,"hzhn":[null],"uio":6249.12,"nxrv":1931.635,"pd":null},"pxlc":true,"mjer":false,"hdev":"msr","er":null},"ug",null,"yrfoix",503.89,563],"tcy":300,"me":459.17,"tm":[134.761,"jcoels",null],"iig":945.57,"ad":"be"},"ltpdm",null,14.53],"xi":"gxzzs","zfpw":1564.87,"ow":null,"tm":[46,876.85],"xejv":null}'

# FIXME: Test all multi-path options (dot notation and bracket notation):
#  Recursive descent, e.g., $..leaf_val
#  Wildcard (in key and in index), e.g., $.*[*]
#  Array slice [start:end:step], e.g., $.arr[2:4]
#  Union, e.g., $.arr[1,2,4] and  $.[field1, field5]
#  Boolean filter, e.g., $.arr[?(@.field>3 && @.id==null)]

def testDelCommand(env):
    """Test REJSON.DEL command"""
    r = env

    r.assertOk(r.execute_command('JSON.SET', 'doc1', '$', '{"a": 1, "nested": {"a": 2, "b": 3}}'))
    res = r.execute_command('JSON.DEL', 'doc1', '$..a')
    r.assertEqual(res, 2)
    res = r.execute_command('JSON.GET', 'doc1', '$')
    r.assertEqual(res, '[{"nested":{"b":3}}]')

    # Test deletion of nested hierarchy - only higher hierarchy is deleted
    r.assertOk(r.execute_command('JSON.SET', 'doc2', '$', '{"a": {"a": 2, "b": 3}, "b": ["a", "b"], "nested": {"b":[true, "a","b"]}}'))
    res = r.execute_command('JSON.DEL', 'doc2', '$..a')
    r.assertEqual(res, 1)
    res = r.execute_command('JSON.GET', 'doc2', '$')
    r.assertEqual(res, '[{"nested":{"b":[true,"a","b"]},"b":["a","b"]}]')

    r.assertOk(r.execute_command('JSON.SET', 'doc3', '$', '[{"ciao":["non ancora"],"nested":[{"ciao":[1,"a"]}, {"ciao":[2,"a"]}, {"ciaoc":[3,"non","ciao"]}, {"ciao":[4,"a"]}, {"e":[5,"non","ciao"]}]}]'))
    res = r.execute_command('JSON.DEL', 'doc3', '$.[0]["nested"]..ciao')
    r.assertEqual(res, 3)
    res = r.execute_command('JSON.GET', 'doc3', '$')
    r.assertEqual(res, '[[{"ciao":["non ancora"],"nested":[{},{},{"ciaoc":[3,"non","ciao"]},{},{"e":[5,"non","ciao"]}]}]]')

    # Test default path
    res = r.execute_command('JSON.DEL', 'doc3')
    r.assertEqual(res, 1)
    res = r.execute_command('JSON.GET', 'doc3', '$')
    r.assertEqual(res, None)

    # Test missing key
    res = r.execute_command('JSON.DEL', 'non_existing_doc', '..a')
    r.assertEqual(res, 0)

    r.assertOk(r.execute_command('JSON.SET', 'doc2', '$', '[1, 2, 3]'))
    res = r.execute_command('JSON.DEL', 'doc2', '$[*]')
    r.assertEqual(res, 3)

    r.assertOk(r.execute_command('JSON.SET', 'doc2', '$', '[1, 2, 3]'))
    res = r.execute_command('JSON.DEL', 'doc2', '$[2,1,0]')
    r.assertEqual(res, 3)

    r.assertOk(r.execute_command('JSON.SET', 'doc2', '$', '[1, 2, 3]'))
    res = r.execute_command('JSON.DEL', 'doc2', '$[1,2,0]')
    r.assertEqual(res, 3)

    r.assertOk(r.execute_command('JSON.SET', 'doc2', '$', '{"b": [1,2,3], "a": {"b": [1, 2, 3], "c": [1, 2, 3]}, "x": {"b": [1, 2, 3], "c": [1, 2, 3]}}'))
    res = r.execute_command('JSON.DEL', 'doc2', '$..x.b[*]')
    r.assertEqual(res, 3)
    res = r.execute_command('JSON.GET', 'doc2', '$')
    r.assertEqual(json.loads(res), [{"b": [1, 2, 3], "a": {"b": [1, 2, 3], "c": [1, 2, 3]}, "x": {"b": [], "c": [1, 2, 3]}}])

    r.assertOk(r.execute_command('JSON.SET', 'doc2', '$', '{"b": [1,2,3], "a": {"b": [1, 2, 3], "c": [1, 2, 3]}, "x": {"b": [1, 2, 3], "c": [1, 2, 3]}}'))
    res = r.execute_command('JSON.DEL', 'doc2', '$..x.b[1,0,2]')
    r.assertEqual(res, 3)
    res = r.execute_command('JSON.GET', 'doc2', '$')
    r.assertEqual(json.loads(res), [{"b": [1, 2, 3], "a": {"b": [1, 2, 3], "c": [1, 2, 3]}, "x": {"b": [], "c": [1, 2, 3]}}])

    # Test deleting a null value
    r.assertOk(r.execute_command('JSON.SET', 'doc2', '$', '[ true, { "answer": 42}, null ]'))
    res = r.execute_command('JSON.DEL', 'doc2', '[-1]')
    r.assertEqual(res, 1)
    res = r.execute_command('JSON.GET', 'doc2', '$')
    r.assertEqual(json.loads(res), [[True, {"answer": 42}]])

def testDelCommand_issue529(env):
    r = env
    r.assertOk(r.execute_command('JSON.SET', 'doc1', '$', '[{"a00": [{"a00": "a00_00"}, {"a01": "a00_01"}, {"a02": "a00_02"}, {"a03": "a00_03"}]}, {"a01": [{"a00": "a01_00"}, {"a01": "a01_01"}, {"a02": "a01_02"}, {"a03": "a01_03"}]}, {"a02": [{"a00": "a02_00"}, {"a01": "a02_01"}, {"a02": "a02_02"}, {"a03": "a02_03"}]}, {"a03": [{"a00": "a03_00"}, {"a01": "a03_01"}, {"a02": "a03_02"}, {"a03": "a03_03"}]}]'))
    res = r.execute_command('JSON.DEL', 'doc1', '$..[2]')
    r.assertEqual(res, 4)
    res = r.execute_command('JSON.ARRLEN', 'doc1', '$.*[*]')
    r.assertEqual(res, [3, 3, 3])

def testDelCommand_issue754(env):
    r = env
    r.assertOk(r.execute_command('JSON.SET', 'doc1', '$', '[[1],[1,2,3]]'))
    res = r.execute_command('JSON.DEL', 'doc1', '$..[0]')
    # The array `[1]` is deleted and its nested element `1` is not counted as deleted
    r.assertEqual(res, 2)
    res = r.execute_command('JSON.GET', 'doc1', '$')
    r.assertEqual(json.loads(res), [[[2,3]]])

    r.assertOk(r.execute_command('JSON.SET', 'doc1', '$', '{"a":[[1],[1,2,3,[4,5,[{"a":6},7,8]]], [10,{"11":11}], ["12","13"]], "b":[[1,2],{"a":[3,4,5]}]}'))
    res = r.execute_command('JSON.DEL', 'doc1', '$..[0]')
    r.assertEqual(res, 8)
    res = r.execute_command('JSON.GET', 'doc1', '$')
    r.assertEqual(json.loads(res), [{"a":[[2,3,[5,[7,8]]],[{"11":11}],["13"]],"b":[{"a":[4,5]}]}])


def testForgetCommand(env):
    """Test REJSON.FORGET command"""
    """Alias of REJSON.DEL"""
    r = env

    r.assertOk(r.execute_command('JSON.SET', 'doc1', '$', '{"a": 1, "nested": {"a": 2, "b": 3}}'))
    res = r.execute_command('JSON.FORGET', 'doc1', '$..a')
    r.assertEqual(res, 2)
    res = r.execute_command('JSON.GET', 'doc1', '$')
    r.assertEqual(res, '[{"nested":{"b":3}}]')

    # Test deletion of nested hierarchy - only higher hierarchy is deleted
    r.assertOk(r.execute_command('JSON.SET', 'doc2', '$', '{"a": {"a": 2, "b": 3}, "b": ["a", "b"], "nested": {"b":[true, "a","b"]}}'))
    res = r.execute_command('JSON.FORGET', 'doc2', '$..a')
    r.assertEqual(res, 1)
    res = r.execute_command('JSON.GET', 'doc2', '$')
    r.assertEqual(res, '[{"nested":{"b":[true,"a","b"]},"b":["a","b"]}]')

    r.assertOk(r.execute_command('JSON.SET', 'doc3', '$', '[{"ciao":["non ancora"],"nested":[{"ciao":[1,"a"]}, {"ciao":[2,"a"]}, {"ciaoc":[3,"non","ciao"]}, {"ciao":[4,"a"]}, {"e":[5,"non","ciao"]}]}]'))
    res = r.execute_command('JSON.FORGET', 'doc3', '$.[0]["nested"]..ciao')
    r.assertEqual(res, 3)
    res = r.execute_command('JSON.GET', 'doc3', '$')
    r.assertEqual(res, '[[{"ciao":["non ancora"],"nested":[{},{},{"ciaoc":[3,"non","ciao"]},{},{"e":[5,"non","ciao"]}]}]]')

    # Test default path
    res = r.execute_command('JSON.FORGET', 'doc3')
    r.assertEqual(res, 1)
    res = r.execute_command('JSON.GET', 'doc3', '$')
    r.assertEqual(res, None)

    # Test missing key
    res = r.execute_command('JSON.FORGET', 'non_existing_doc', '..a')
    r.assertEqual(res, 0)


def testSetAndGetCommands(env):
    """Test REJSON.SET command"""
    """Test REJSON.GET command"""

    r = env
    # Test set and get on large nested key
    r.assertIsNone(r.execute_command('JSON.SET', 'doc1', '$', nested_large_key, 'XX'))
    r.assertOk(r.execute_command('JSON.SET', 'doc1', '$', nested_large_key, 'NX'))
    res = r.execute_command('JSON.GET', 'doc1', '$')
    r.assertEqual(res, '[' + nested_large_key + ']')
    r.assertIsNone(r.execute_command('JSON.SET', 'doc1', '$', nested_large_key, 'NX'))
    # Test single path
    res = r.execute_command('JSON.GET', 'doc1', '$..tm')
    r.assertEqual(res, '[[46,876.85],[134.761,"jcoels",null]]')

    # Test multi get and set
    res = r.execute_command('JSON.GET', 'doc1', '$..foobar')
    r.assertEqual(res, '[3.141592,1.61803398875]')
    # Set multi existing values
    res = r.execute_command('JSON.SET', 'doc1', '$..foobar', '"new_val"')
    res = r.execute_command('JSON.GET', 'doc1', '$..foobar')
    r.assertEqual(res, '["new_val","new_val"]')

    # Test multi set and get on small nested key
    nested_simple_key = r'{"a":1,"nested":{"a":2,"b":3}}'
    r.assertOk(r.execute_command('JSON.SET', 'doc2', '$', nested_simple_key))
    res = r.execute_command('JSON.GET', 'doc2', '$')
    r.assertEqual(res, '[' + nested_simple_key + ']')
    # Set multi existing values
    r.assertOk(r.execute_command('JSON.SET', 'doc2', '$..a', '4.2'))
    res = r.execute_command('JSON.GET', 'doc2', '$')
    r.assertEqual(res, '[{"a":4.2,"nested":{"a":4.2,"b":3}}]')


    # Test multi paths
    res = r.execute_command('JSON.GET', 'doc1', '$..tm', '$..nu')
    r.assertEqual(json.loads(res), {"$..tm": [[46, 876.85], [134.761, "jcoels", None]], "$..nu": [[377, "qda", True]]})
    # Test multi paths - if one path is none-legacy - result format is not legacy
    res = r.execute_command('JSON.GET', 'doc1', '..tm', '$..nu')
    r.assertEqual(json.loads(res), {"..tm": [[46, 876.85], [134.761, "jcoels", None]], "$..nu": [[377, "qda", True]]})
    # Test multi paths with formatting (using the same path in order to get a map and still avoid failure due to undefined ordering between map keys)
    res = r.execute_command('JSON.GET', 'doc2', 'INDENT', '\\t', 'NEWLINE', '\\n', 'SPACE', ' ', '$..a', '$..a')
    r.assertEqual(res, '{\\n\\t"$..a": [\\n\\t\\t4.2,\\n\\t\\t4.2\\n\\t]\\n}')

    # Test missing key
    r.assertIsNone(r.execute_command('JSON.GET', 'docX', '..tm', '$..nu'))
    # Test missing path
    res = r.execute_command('JSON.GET', 'doc1', '..tm', '$..back_in_nov')
    r.assertEqual(json.loads(res), {"$..back_in_nov": [], "..tm": [[46, 876.85], [134.761, "jcoels", None]]})
    res = r.execute_command('JSON.GET', 'doc2', '..a', '..b', '$.back_in_nov')
    r.assertEqual(json.loads(res), {"$.back_in_nov": [], "..a": [4.2, 4.2], "..b": [3]})

    # Test legacy multi path (all paths are legacy)
    res = r.execute_command('JSON.GET', 'doc1', '..nu', '..tm')
    r.assertEqual(json.loads(res), json.loads('{"..nu":[377,"qda",true],"..tm":[46,876.85]}'))
    # Test multi paths with formatting (using the same path in order to get a map and still avoid failure due to undefined ordering between map keys)
    res = r.execute_command('JSON.GET', 'doc2', 'INDENT', '\\t', 'NEWLINE', '\\n', 'SPACE', ' ', '..a', '..a')
    r.assertEqual(res, '{\\n\\t"..a": 4.2\\n}')
    # Test legacy single path
    res = r.execute_command('JSON.GET', 'doc1', '..tm')
    r.assertEqual(res, '[46,876.85]')

    # Test missing legacy path (should return an error for a missing path)
    r.assertOk(r.execute_command('JSON.SET', 'doc2', '$.nested.b', 'null'))
    r.expect('JSON.GET', 'doc2', '.a', '.nested.b', '.back_in_nov', '.ttyl').raiseError()
    r.expect('JSON.GET', 'doc2', '.back_in_nov').raiseError()

    # Test missing path (defaults to root)
    r.assertOk(r.execute_command('JSON.SET', 'doc1', '$', '"inizio"'))
    res = r.execute_command('JSON.GET', 'doc1')
    r.assertEqual(res, '"inizio"')


def testMGetCommand(env):
    """Test REJSON.MGET command"""
    r = env
    # Test mget with multi paths
    r.assertOk(r.execute_command('JSON.SET', '{doc}:1', '$', '{"a":1, "b": 2, "nested1": {"a": 3}, "c": null, "nested2": {"a": null}}'))
    r.assertOk(r.execute_command('JSON.SET', '{doc}:2', '$', '{"a":4, "b": 5, "nested3": {"a": 6}, "c": null, "nested4": {"a": [null]}}'))
    # Compare also to single JSON.GET
    res1 = r.execute_command('JSON.GET', '{doc}:1', '$..a')
    res2 = r.execute_command('JSON.GET', '{doc}:2', '$..a')
    r.assertEqual(res1, '[1,3,null]')
    r.assertEqual(res2, '[4,6,[null]]')

    r.assertTrue(r.execute_command('SET', '{doc}:wrong_key_type', 'not a json key'))

    # Test mget with single path
    res = r.execute_command('JSON.MGET', '{doc}:1', '$..a')
    r.assertEqual([res1], res)

    # Test mget with multi path
    res = r.execute_command('JSON.MGET', '{doc}:1', '{doc}:wrong_key_type', '{doc}:2', '$..a')
    r.assertEqual(res, [res1, None, res2])

    # Test missing/wrong key / missing path
    res = r.execute_command('JSON.MGET', '{doc}:1', '{doc}:missing', '$..a')
    r.assertEqual(res, [res1, None])
    res = r.execute_command('JSON.MGET', '{doc}:1', '{doc}:2', '{doc}:wrong_key_type', '{doc}:missing', '$.nested1.a')
    r.assertEqual(res, [json.dumps([json.loads(res1)[1]]), '[]', None, None])
    res = r.execute_command('JSON.MGET', '{doc}:missing1', '{doc}:missing2', '$..a')
    r.assertEqual(res, [None, None])

    # Test missing path
    res = r.execute_command('JSON.MGET', '{doc}:1', '{doc}:wrong_key_type', '{doc}:missing2', '$..niente')
    r.assertEqual(res, ['[]', None, None])

    # Test legacy (for each path only the first value is returned as a json string)
    # Test mget with single path
    res = r.execute_command('JSON.MGET', '{doc}:1', '..a')
    r.assertEqual(res, [json.dumps(json.loads(res1)[0])])
    # Test mget with multi path
    res = r.execute_command('JSON.MGET', '{doc}:1', '{doc}:2', '..a')
    r.assertEqual(res, [json.dumps(json.loads(res1)[0]), json.dumps(json.loads(res2)[0])])

    # Test wrong key
    res = r.execute_command('JSON.MGET', '{doc}:1', '{doc}:wrong_key_type', '{doc}:2', '..a')
    r.assertEqual(res, [json.dumps(json.loads(res1)[0]), None, json.dumps(json.loads(res2)[0])])

    # Test missing key/path
    res = r.execute_command('JSON.MGET', '{doc}:1', '{doc}:2', '{doc}:wrong_key_type', '{doc}:missing', '.nested1.a')
    r.assertEqual(res, [json.dumps(json.loads(res1)[1]), None, None, None])
    res = r.execute_command('JSON.MGET', '{doc}:missing1', '{doc}:missing2', '..a')
    r.assertEqual(res, [None, None])

    # Test missing path
    res = r.execute_command('JSON.MGET', '{doc}:1', '{doc}:wrong_key_type', '{doc}:missing2', '.niente')
    r.assertEqual(res, [None, None, None])


def testNumByCommands(env):
    """
    Test REJSON.NUMINCRBY command
    Test REJSON.NUMMULTBY command
    Test REJSON.NUMPOWBY command
    """
    r = env

    # Test NUMINCRBY
    r.assertOk(r.execute_command('JSON.SET', 'doc1', '$', '{"a":"b","b":[{"a":2}, {"a":5.0}, {"a":"c"}]}'))
    # Test multi
    res = r.execute_command('JSON.NUMINCRBY', 'doc1', '$..a', '2')
    r.assertEqual(json.loads(res), [None, 4, 7.0, None])
    res = r.execute_command('JSON.NUMINCRBY', 'doc1', '$..a', '2.5')
    r.assertEqual(json.loads(res), [None, 6.5, 9.5, None])
    # Test single
    res = r.execute_command('JSON.NUMINCRBY', 'doc1', '$.b[1].a', '2')
    #  Avoid json.loads to verify the underlying type (integer/float)
    r.assertEqual(res, '[11.5]')
    res = r.execute_command('JSON.NUMINCRBY', 'doc1', '$.b[2].a', '2')
    r.assertEqual(res, '[null]')
    res = r.execute_command('JSON.NUMINCRBY', 'doc1', '$.b[1].a', '3.5')
    r.assertEqual(res, '[15.0]')

    # Test NUMMULTBY
    r.assertOk(r.execute_command('JSON.SET', 'doc1', '$', '{"a":"b","b":[{"a":2}, {"a":5.0}, {"a":"c"}]}'))
    # Test multi
    res = r.execute_command('JSON.NUMMULTBY', 'doc1', '$..a', '2')
    r.assertEqual(json.loads(res), [None, 4, 10, None])
    res = r.execute_command('JSON.NUMMULTBY', 'doc1', '$..a', '2.5')
    #  Avoid json.loads to verify the underlying type (integer/float)
    r.assertEqual(res, '[null,10.0,25.0,null]')
    # Test single
    res = r.execute_command('JSON.NUMMULTBY', 'doc1', '$.b[1].a', '2')
    r.assertEqual(res, '[50.0]')
    res = r.execute_command('JSON.NUMMULTBY', 'doc1', '$.b[2].a', '2')
    r.assertEqual(res, '[null]')
    res = r.execute_command('JSON.NUMMULTBY', 'doc1', '$.b[1].a', '3')
    r.assertEqual(res, '[150.0]')

    # Test NUMPOWBY
    r.assertOk(r.execute_command('JSON.SET', 'doc1', '$', '{"a":"b","b":[{"a":2}, {"a":5.0}, {"a":"c"}]}'))
    # Test multi
    res = r.execute_command('JSON.NUMPOWBY', 'doc1', '$..a', '2')
    r.assertEqual(json.loads(res), [None, 4, 25, None])
    #  Avoid json.loads to verify the underlying type (integer/float)
    res = r.execute_command('JSON.NUMPOWBY', 'doc1', '$..a', '2')
    r.assertEqual(res, '[null,16,625.0,null]')
    # Test single
    res = r.execute_command('JSON.NUMPOWBY', 'doc1', '$.b[1].a', '2')
    r.assertEqual(res, '[390625.0]')
    res = r.execute_command('JSON.NUMPOWBY', 'doc1', '$.b[2].a', '2')
    r.assertEqual(res, '[null]')
    res = r.execute_command('JSON.NUMPOWBY', 'doc1', '$.b[1].a', '3')
    r.assertEqual(res, '[5.960464477539062e16]')

    # Test missing key
    r.expect('JSON.NUMINCRBY', 'non_existing_doc', '$..a', '2').raiseError()
    r.expect('JSON.NUMMULTBY', 'non_existing_doc', '$..a', '2').raiseError()
    r.expect('JSON.NUMPOWBY', 'non_existing_doc', '$..a', '2').raiseError()

    # Test legacy NUMINCRBY
    r.assertOk(r.execute_command('JSON.SET', 'doc1', '$', '{"a":"b","b":[{"a":2}, {"a":5.0}, {"a":"c"}]}'))
    res = r.execute_command('JSON.NUMINCRBY', 'doc1', '.b[0].a', '3')
    r.assertEqual(res, '5')

    # Test legacy NUMMULTBY
    r.assertOk(r.execute_command('JSON.SET', 'doc1', '$', '{"a":"b","b":[{"a":2}, {"a":5.0}, {"a":"c"}]}'))
    res = r.execute_command('JSON.NUMMULTBY', 'doc1', '.b[0].a', '3')
    r.assertEqual(res, '6')

    # Test legacy NUMPOWBY
    r.assertOk(r.execute_command('JSON.SET', 'doc1', '$', '{"a":"b","b":[{"a":2}, {"a":5.0}, {"a":"c"}]}'))
    res = r.execute_command('JSON.NUMPOWBY', 'doc1', '.b[0].a', '4')
    r.assertEqual(res, '16')

def testStrAppendCommand(env):
    """
    Test REJSON.STRAPPEND command
    """
    r = env

    r.assertOk(r.execute_command('JSON.SET', 'doc1', '$', '{"a":"foo", "nested1": {"a": "hello"}, "nested2": {"a": 31}}'))
    # Test multi
    res = r.execute_command('JSON.STRAPPEND', 'doc1', '$..a', '"bar"')
    r.assertEqual(res, [6, 8, None])
    res = r.execute_command('JSON.GET', 'doc1', '$')
    r.assertEqual(res, '[{"a":"foobar","nested1":{"a":"hellobar"},"nested2":{"a":31}}]')
    # Test single
    res = r.execute_command('JSON.STRAPPEND', 'doc1', '$.nested1.a', '"baz"')
    r.assertEqual(res, [11])
    res = r.execute_command('JSON.GET', 'doc1', '$')
    r.assertEqual(res, '[{"a":"foobar","nested1":{"a":"hellobarbaz"},"nested2":{"a":31}}]')

    # Test missing key
    r.expect('JSON.STRAPPEND', 'non_existing_doc', '$..a', '"err"').raiseError()

    # Test legacy
    r.assertOk(r.execute_command('JSON.SET', 'doc1', '$', '{"a":"foo", "nested1": {"a": "hello"}, "nested2": {"a": 31}}'))
    # Test multi
    res = r.execute_command('JSON.STRAPPEND', 'doc1', '.*.a', '"bar"')
    r.assertEqual(res, 8)
    res = r.execute_command('JSON.GET', 'doc1', '$')
    r.assertEqual(res, '[{"a":"foo","nested1":{"a":"hellobar"},"nested2":{"a":31}}]')

    # Test missing path (defaults to root)
    r.assertOk(r.execute_command('JSON.SET', 'doc1', '$', '"abcd"'))
    res = r.execute_command('JSON.STRAPPEND', 'doc1', '"piu"')
    r.assertEqual(res, 7)


def testStrLenCommand(env):
    """
    Test REJSON.STRLEN command
    """
    r = env

    # Test multi
    r.assertOk(r.execute_command('JSON.SET', 'doc1', '$', '{"a":"foo", "nested1": {"a": "hello"}, "nested2": {"a": 31}}'))
    res1 = r.execute_command('JSON.STRLEN', 'doc1', '$..a')
    r.assertEqual(res1, [3, 5, None])
    res2 = r.execute_command('JSON.STRAPPEND', 'doc1', '$..a', '"bar"')
    r.assertEqual(res2, [6, 8, None])
    res1 = r.execute_command('JSON.STRLEN', 'doc1', '$..a')
    r.assertEqual(res1, res2)

    # Test single
    res = r.execute_command('JSON.STRLEN', 'doc1', '$.nested1.a')
    r.assertEqual(res, [8])
    res = r.execute_command('JSON.STRLEN', 'doc1', '$.nested2.a')
    r.assertEqual(res, [None])

    # Test missing key
    r.expect('JSON.STRLEN', 'non_existing_doc', '$..a').raiseError()

    # Test legacy
    res1 = r.execute_command('JSON.STRLEN', 'doc1', '..a')
    r.assertEqual(res1, 6)

    # Test missing path
    r.assertOk(r.execute_command('JSON.SET', 'doc1', '$', '"kantele"'))
    res = r.execute_command('JSON.STRLEN', 'doc1')
    r.assertEqual(res, 7)


def testArrAppendCommand(env):
    """
    Test REJSON.ARRAPPEND command
    """
    r = env

    r.assertOk(r.execute_command('JSON.SET', 'doc1', '$', '{"a":["foo"], "nested1": {"a": ["hello", null, "world"]}, "nested2": {"a": 31}}'))
    # Test multi
    res = r.execute_command('JSON.ARRAPPEND', 'doc1', '$..a', '"bar"', '"racuda"')
    r.assertEqual(res, [3, 5, None])
    res = r.execute_command('JSON.GET', 'doc1', '$')
    r.assertEqual(json.loads(res), [{"a": ["foo", "bar", "racuda"], "nested1": {"a": ["hello", None, "world", "bar", "racuda"]}, "nested2": {"a": 31}}])
    # Test single
    res = r.execute_command('JSON.ARRAPPEND', 'doc1', '$.nested1.a', '"baz"')
    r.assertEqual(res, [6])
    res = r.execute_command('JSON.GET', 'doc1', '$')
    r.assertEqual(json.loads(res), [{"a": ["foo", "bar", "racuda"], "nested1": {"a": ["hello", None, "world", "bar", "racuda", "baz"]}, "nested2": {"a": 31}}])

    # Test missing key
    r.expect('JSON.ARRAPPEND', 'non_existing_doc', '$..a').raiseError()

    # Test legacy
    r.assertOk(r.execute_command('JSON.SET', 'doc1', '$', '{"a":["foo"], "nested1": {"a": ["hello", null, "world"]}, "nested2": {"a": 31}}'))
    # Test multi (all paths are updated, but return result of last path)
    res = r.execute_command('JSON.ARRAPPEND', 'doc1', '..a', '"bar"', '"racuda"')
    r.assertEqual(res, 5)
    res = r.execute_command('JSON.GET', 'doc1', '$')
    r.assertEqual(json.loads(res), [{"a": ["foo", "bar", "racuda"], "nested1": {"a": ["hello", None, "world", "bar", "racuda"]}, "nested2": {"a": 31}}])
    # Test single
    res = r.execute_command('JSON.ARRAPPEND', 'doc1', '.nested1.a', '"baz"')
    r.assertEqual(res, 6)
    res = r.execute_command('JSON.GET', 'doc1', '$')
    r.assertEqual(json.loads(res), [{"a": ["foo", "bar", "racuda"], "nested1": {"a": ["hello", None, "world", "bar", "racuda", "baz"]}, "nested2": {"a": 31}}])

    # Test missing key
    r.expect('JSON.ARRAPPEND', 'non_existing_doc', '..a').raiseError()


def testArrInsertCommand(env):
    """
    Test REJSON.ARRINSERT command
    """
    r = env

    r.assertOk(r.execute_command('JSON.SET', 'doc1', '$', '{"a":["foo"], "nested1": {"a": ["hello", null, "world"]}, "nested2": {"a": 31}}'))
    # Test multi
    res = r.execute_command('JSON.ARRINSERT', 'doc1', '$..a', '1', '"bar"', '"racuda"')
    r.assertEqual(res, [3, 5, None])
    res = r.execute_command('JSON.GET', 'doc1', '$')
    r.assertEqual(json.loads(res), [{"a": ["foo", "bar", "racuda"], "nested1": {"a": ["hello", "bar", "racuda", None, "world"]}, "nested2": {"a": 31}}])
    # Test single
    res = r.execute_command('JSON.ARRINSERT', 'doc1', '$.nested1.a', -2, '"baz"')
    r.assertEqual(res, [6])
    res = r.execute_command('JSON.GET', 'doc1', '$')
    r.assertEqual(json.loads(res), [{"a": ["foo", "bar", "racuda"], "nested1": {"a": ["hello", "bar", "racuda", "baz", None, "world"]}, "nested2": {"a": 31}}])

    # Test missing key
    r.expect('JSON.ARRINSERT', 'non_existing_doc', '$..a', '0').raiseError()

    # Test legacy
    r.assertOk(r.execute_command('JSON.SET', 'doc1', '$', '{"a":["foo"], "nested1": {"a": ["hello", null, "world"]}, "nested2": {"a": 31}}'))
    # Test multi (all paths are updated, but return result of last path)
    res = r.execute_command('JSON.ARRINSERT', 'doc1', '..a', '1', '"bar"', '"racuda"')
    r.assertEqual(res, 5)
    res = r.execute_command('JSON.GET', 'doc1', '$')
    r.assertEqual(json.loads(res), [{"a": ["foo", "bar", "racuda"], "nested1": {"a": ["hello", "bar", "racuda", None, "world"]}, "nested2": {"a": 31}}])
    # Test single
    res = r.execute_command('JSON.ARRINSERT', 'doc1', '.nested1.a', -2, '"baz"')
    r.assertEqual(res, 6)
    res = r.execute_command('JSON.GET', 'doc1', '$')
    r.assertEqual(json.loads(res), [{"a": ["foo", "bar", "racuda"], "nested1": {"a": ["hello", "bar", "racuda", "baz", None, "world"]}, "nested2": {"a": 31}}])

    # Test missing key
    r.expect('JSON.ARRINSERT', 'non_existing_doc', '..a').raiseError()


def testArrLenCommand(env):
    """
    Test REJSON.ARRLEN command
    """
    r = env

    r.assertOk(r.execute_command('JSON.SET', 'doc1', '$', '{"a":["foo"], "nested1": {"a": ["hello", null, "world"]}, "nested2": {"a": 31}}'))
    # Test multi
    res = r.execute_command('JSON.ARRLEN', 'doc1', '$..a')
    r.assertEqual(res, [1, 3, None])
    res = r.execute_command('JSON.ARRAPPEND', 'doc1', '$..a', '"non"', '"abba"', '"stanza"')
    r.assertEqual(res, [4, 6, None])
    r.execute_command('JSON.CLEAR', 'doc1', '$.a')
    res = r.execute_command('JSON.ARRLEN', 'doc1', '$..a')
    r.assertEqual(res, [0, 6, None])
    # Test single
    res = r.execute_command('JSON.ARRLEN', 'doc1', '$.nested1.a')
    r.assertEqual(res, [6])

    # Test missing key
    r.expect('JSON.ARRLEN', 'non_existing_doc', '$..a').raiseError()

    # Test legacy
    r.assertOk(r.execute_command('JSON.SET', 'doc1', '$', '{"a":["foo"], "nested1": {"a": ["hello", null, "world"]}, "nested2": {"a": 31}}'))
    # Test multi (return result of last path)
    res = r.execute_command('JSON.ARRLEN', 'doc1', '$..a')
    r.assertEqual(res, [1, 3, None])
    res = r.execute_command('JSON.ARRAPPEND', 'doc1', '..a', '"non"', '"abba"', '"stanza"')
    r.assertEqual(res, 6)
    # Test single
    res = r.execute_command('JSON.ARRLEN', 'doc1', '.nested1.a')
    r.assertEqual(res, 6)

    # Test missing key
    r.assertEqual(r.execute_command('JSON.ARRLEN', 'non_existing_doc', '..a'), None)

    # Test missing path (defaults to root)
    r.assertOk(r.execute_command('JSON.SET', 'doc1', '$', '[0, 1, 2, 3, 4]'))
    res = r.execute_command('JSON.ARRLEN', 'doc1')
    r.assertEqual(res, 5)

def testArrPopCommand(env):
    """
    Test REJSON.ARRPOP command
    """
    r = env

    r.assertOk(r.execute_command('JSON.SET', 'doc1', '$', '{"a":["foo"], "nested1": {"a": ["hello", null, "world"]}, "nested2": {"a": 31}}'))
    # Test multi
    res = r.execute_command('JSON.ARRPOP', 'doc1', '$..a', '1')
    r.assertEqual(res, ['"foo"', 'null', None])
    res = r.execute_command('JSON.GET', 'doc1', '$')
    r.assertEqual(json.loads(res), [{"a": [], "nested1": {"a": ["hello", "world"]}, "nested2": {"a": 31}}])

    res = r.execute_command('JSON.ARRPOP', 'doc1', '$..a', '-1')
    r.assertEqual(res, [None, '"world"', None])
    res = r.execute_command('JSON.GET', 'doc1', '$')
    r.assertEqual(json.loads(res), [{"a": [], "nested1": {"a": ["hello"]}, "nested2": {"a": 31}}])
    # Test single
    res = r.execute_command('JSON.ARRPOP', 'doc1', '$.nested1.a', -2)
    r.assertEqual(res, ['"hello"'])
    res = r.execute_command('JSON.GET', 'doc1', '$')
    r.assertEqual(json.loads(res), [{"a": [], "nested1": {"a": []}, "nested2": {"a": 31}}])

    # Test missing key
    r.expect('JSON.ARRPOP', 'non_existing_doc', '$..a', '0').raiseError()

    # Test legacy
    r.assertOk(r.execute_command('JSON.SET', 'doc1', '$', '{"a":["foo"], "nested1": {"a": ["hello", null, "world"]}, "nested2": {"a": 31}}'))
    # Test multi (all paths are updated, but return result of last path)
    res = r.execute_command('JSON.ARRPOP', 'doc1', '..a', '1')
    r.assertEqual(res, 'null')
    res = r.execute_command('JSON.GET', 'doc1', '$')
    r.assertEqual(json.loads(res), [{"a": [], "nested1": {"a": ["hello", "world"]}, "nested2": {"a": 31}}])
    # Test single
    res = r.execute_command('JSON.ARRPOP', 'doc1', '.nested1.a', -2)
    r.assertEqual(res, '"hello"')
    res = r.execute_command('JSON.GET', 'doc1', '$')
    r.assertEqual(json.loads(res), [{"a": [], "nested1": {"a": ["world"]}, "nested2": {"a": 31}}])

    # Test missing key
    r.expect('JSON.ARRPOP', 'non_existing_doc', '..a').raiseError()

    # Test default path/index
    r.assertOk(r.execute_command('JSON.SET', 'doc1', '$', '[0, 1, 2]'))
    res = r.execute_command('JSON.ARRPOP', 'doc1')
    r.assertEqual(res, '2')
    res = r.execute_command('JSON.ARRPOP', 'doc1', '$')
    r.assertEqual(res, ['1'])
    res = r.execute_command('JSON.ARRPOP', 'doc1', '.')
    r.assertEqual(res, '0')



def testArrTrimCommand(env):
    """
    Test REJSON.ARRTRIM command
    """
    r = env

    r.assertOk(r.execute_command('JSON.SET', 'doc1', '$', '{"a":["foo"], "nested1": {"a": ["hello", null, "world"]}, "nested2": {"a": 31}}'))
    # Test multi
    res = r.execute_command('JSON.ARRTRIM', 'doc1', '$..a', '1', -1)
    r.assertEqual(res, [0, 2, None])
    res = r.execute_command('JSON.GET', 'doc1', '$')
    r.assertEqual(json.loads(res), [{"a": [], "nested1": {"a": [None, "world"]}, "nested2": {"a": 31}}])

    res = r.execute_command('JSON.ARRTRIM', 'doc1', '$..a', '1', '1')
    r.assertEqual(res, [0, 1, None])
    res = r.execute_command('JSON.GET', 'doc1', '$')
    r.assertEqual(json.loads(res), [{"a": [], "nested1": {"a": ["world"]}, "nested2": {"a": 31}}])
    # Test single
    res = r.execute_command('JSON.ARRTRIM', 'doc1', '$.nested1.a', 1, 0)
    r.assertEqual(res, [0])
    res = r.execute_command('JSON.GET', 'doc1', '$')
    r.assertEqual(json.loads(res), [{"a": [], "nested1": {"a": []}, "nested2": {"a": 31}}])

    # Test missing key
    r.expect('JSON.ARRTRIM', 'non_existing_doc', '$..a', '0').raiseError()

    # Test legacy
    r.assertOk(r.execute_command('JSON.SET', 'doc1', '$', '{"a":["foo"], "nested1": {"a": ["hello", null, "world"]}, "nested2": {"a": 31}}'))
    # Test multi (all paths are updated, but return result of last path)
    res = r.execute_command('JSON.ARRTRIM', 'doc1', '..a', '1', '-1')
    r.assertEqual(res, 2)
    res = r.execute_command('JSON.GET', 'doc1', '$')
    r.assertEqual(json.loads(res), [{"a": [], "nested1": {"a": [None, "world"]}, "nested2": {"a": 31}}])
    # Test single
    res = r.execute_command('JSON.ARRTRIM', 'doc1', '.nested1.a', '1', '1')
    r.assertEqual(res, 1)
    res = r.execute_command('JSON.GET', 'doc1', '$')
    r.assertEqual(json.loads(res), [{"a": [], "nested1": {"a": ["world"]}, "nested2": {"a": 31}}])

    # Test missing key
    r.expect('JSON.ARRTRIM', 'non_existing_doc', '..a').raiseError()

def testObjKeysCommand(env):
    """Test JSON.OBJKEYS command"""
    r = env

    r.assertOk(r.execute_command('JSON.SET', 'doc1', '$', '{"nested1": {"a": {"foo": 10, "bar": 20}}, "a":["foo"], "nested2": {"a": {"baz":50}}}'))
    # Test multi
    res = r.execute_command('JSON.OBJKEYS', 'doc1', '$..a')
    r.assertEqual(res, [None, ["foo", "bar"], ["baz"]])
    # Test single
    res = r.execute_command('JSON.OBJKEYS', 'doc1', '$.nested1.a')
    r.assertEqual(res, [["foo", "bar"]])

    # Test legacy
    res = r.execute_command('JSON.OBJKEYS', 'doc1', '.*.a')
    r.assertEqual(res, ["foo", "bar"])
    # Test single
    res = r.execute_command('JSON.OBJKEYS', 'doc1', '.nested2.a')
    r.assertEqual(res, ["baz"])

    # Test missing key
    res = r.execute_command('JSON.OBJKEYS', 'non_existing_doc', '..a')
    r.assertEqual(res, None)

    # Test missing key
    r.assertEqual(r.execute_command('JSON.OBJKEYS', 'doc1', '$.nowhere'), [])

    # Test default path
    res = r.execute_command('JSON.OBJKEYS', 'doc1')
    r.assertEqual(res, ["nested1", "a", "nested2"])


def testObjLenCommand(env):
    """Test JSON.OBJLEN command"""
    r = env

    r.assertOk(r.execute_command('JSON.SET', 'doc1', '$', '{"nested1": {"a": {"foo": 10, "bar": 20}}, "a":["foo"], "nested2": {"a": {"baz":50}}}'))
    # Test multi
    res = r.execute_command('JSON.OBJLEN', 'doc1', '$..a')
    r.assertEqual(set(res), set([2, None, 1]))
    # Test single
    res = r.execute_command('JSON.OBJLEN', 'doc1', '$.nested1.a')
    r.assertEqual(res, [2])

    # Test missing key
    r.expect('JSON.OBJLEN', 'non_existing_doc', '$..a').raiseError().contains("does not exist")

    # Test missing path
    r.assertEqual(r.execute_command('JSON.OBJLEN', 'doc1', '$.nowhere'), [])


    # Test legacy
    res = r.execute_command('JSON.OBJLEN', 'doc1', '.*.a')
    r.assertEqual(res, 2)
    # Test single
    res = r.execute_command('JSON.OBJLEN', 'doc1', '.nested2.a')
    r.assertEqual(res, 1)

    # Test missing key
    res = r.execute_command('JSON.OBJLEN', 'non_existing_doc', '..a')
    r.assertEqual(res, None)

    # Test missing path
    res = r.execute_command('JSON.OBJLEN', 'doc1', '.nowhere')
    r.assertEqual(res, None)

    # Test default path
    res = r.execute_command('JSON.OBJLEN', 'doc1')
    r.assertEqual(res, 3)


def load_types_data(nested_key_name):
    types_data = {
        'object':   {},
        'array':    [],
        'string':   'str',
        'integer':  42,
        'number':   1.2,
        'boolean':  False,
        'null':     None,

    }
    jdata = {}
    types = []
    for i, (k, v) in zip(range(1, len(types_data) + 1), iter(types_data.items())):
        jdata["nested" + str(i)] = {nested_key_name: v}
        types.append(k)

    return jdata, types


def testTypeCommand(env):
    """Test JSON.TYPE command"""

    jdata, jtypes = load_types_data('a')
    r = env

    r.assertOk(r.execute_command('JSON.SET', 'doc1', '$', json.dumps(jdata)))
    # Test multi
    res = r.execute_command('JSON.TYPE', 'doc1', '$..a')
    r.assertEqual(res, jtypes)
    # Test single
    res = r.execute_command('JSON.TYPE', 'doc1', '$.nested2.a')
    r.assertEqual(res, [jtypes[1]])

    # Test legacy
    res = r.execute_command('JSON.TYPE', 'doc1', '..a')
    r.assertEqual(res, jtypes[0])
    # Test missing path (defaults to root)
    res = r.execute_command('JSON.TYPE', 'doc1')
    r.assertEqual(res, 'object')

    # Test missing key
    res = r.execute_command('JSON.TYPE', 'non_existing_doc', '..a')
    r.assertEqual(res, None)

def testClearCommand(env):
    """Test JSON.CLEAR command"""
    r = env

    r.assertOk(r.execute_command('JSON.SET', 'doc1', '$', '{"nested1": {"a": {"foo": 10, "bar": 20}}, "a":["foo"], "nested2": {"a": "claro"}, "nested3": {"a": {"baz":50}}}'))
    # Test multi
    res = r.execute_command('JSON.CLEAR', 'doc1', '$..a')
    r.assertEqual(res, 3)
    res = r.execute_command('JSON.GET', 'doc1', '$')
    r.assertEqual(json.loads(res), [{"nested1": {"a": {}}, "a": [], "nested2": {"a": "claro"}, "nested3": {"a": {}}}])
    # Not clearing already cleared values
    res = r.execute_command('JSON.CLEAR', 'doc1', '$..a')
    r.assertEqual(res, 0)

    # Test single
    r.assertOk(r.execute_command('JSON.SET', 'doc1', '$', '{"nested1": {"a": {"foo": 10, "bar": 20}}, "a":["foo"], "nested2": {"a": "claro"}, "nested3": {"a": {"baz":50}}}'))
    res = r.execute_command('JSON.CLEAR', 'doc1', '$.nested1.a')
    r.assertEqual(res, 1)
    res = r.execute_command('JSON.GET', 'doc1', '$')
    r.assertEqual(json.loads(res), [{"nested1": {"a": {}}, "a": ["foo"], "nested2": {"a": "claro"}, "nested3": {"a": {"baz": 50}}}])
    # Not clearing already cleared values
    res = r.execute_command('JSON.CLEAR', 'doc1', '$.nested1.a')
    r.assertEqual(res, 0)

    # Test missing path (defaults to root)
    res = r.execute_command('JSON.CLEAR', 'doc1')
    r.assertEqual(res, 1)
    res = r.execute_command('JSON.GET', 'doc1', '$')
    r.assertEqual(json.loads(res), [{}])

    # Test missing key
    r.expect('JSON.CLEAR', 'non_existing_doc', '$..a').raiseError()


def testToggleCommand(env):
    """
    Test REJSON.TOGGLE command
    """
    r = env

    r.assertOk(r.execute_command('JSON.SET', 'doc1', '$', '{"a":["foo"], "nested1": {"a": false}, "nested2": {"a": 31}, "nested3": {"a": true}}'))
    # Test multi
    res = r.execute_command('JSON.TOGGLE', 'doc1', '$..a')
    r.assertEqual(res, [None, 1, None, 0])
    res = r.execute_command('JSON.GET', 'doc1', '$')
    r.assertEqual(json.loads(res), [{"a": ["foo"], "nested1": {"a": True}, "nested2": {"a": 31}, "nested3": {"a": False}}])

    # Test single
    res = r.execute_command('JSON.TOGGLE', 'doc1', '$.nested1.a')
    r.assertEqual(res, [0])
    res = r.execute_command('JSON.GET', 'doc1', '$')
    r.assertEqual(json.loads(res), [{"a": ["foo"], "nested1": {"a": False}, "nested2": {"a": 31}, "nested3": {"a": False}}])

    # Test missing key
    r.expect('JSON.TOGGLE', 'non_existing_doc', '$..a').raiseError()

# TODO adapt test to run on Mac
# @no_san
# def testMemoryUsage(env):
#     """
#     Test MEMORY USAGE key
#     """
#     r = env
#     jdata, jtypes = load_types_data('a')
#     r.assertOk(r.execute_command('JSON.SET', 'doc1', '$', json.dumps(jdata)))
#     res = r.execute_command('MEMORY', 'USAGE', 'doc1')
#     r.assertEqual(res, 211)

#     jdata, jtypes = load_types_data('verylongfieldname')
#     r.assertOk(r.execute_command('JSON.SET', 'doc2', '$', json.dumps(jdata)))
#     res = r.execute_command('MEMORY', 'USAGE', 'doc2')
#     r.assertEqual(res, 323)

@no_san
def testDebugCommand(env):
    """
    Test REJSON.DEBUG MEMORY command
    """
    r = env
    jdata, jtypes = load_types_data('a')

    r.assertOk(r.execute_command('JSON.SET', 'doc1', '$', json.dumps(jdata)))

    # Test multi
    # json.get a $..a ==> "[{},[],\"str\",42,1.2,false,null]"
    res = r.execute_command('JSON.DEBUG', 'MEMORY', 'doc1', '$..a')
    r.assertEqual(res, [8, 8, 8, 8, 16, 8, 8])

    # Test single
    res = r.execute_command('JSON.DEBUG', 'MEMORY', 'doc1', '$.nested2.a')
    r.assertEqual(res, [8])

    # Test legacy
    res = r.execute_command('JSON.DEBUG', 'MEMORY', 'doc1', '..a')
    r.assertEqual(res, 8)

    # Test missing path (defaults to root)
    res = r.execute_command('JSON.DEBUG', 'MEMORY', 'doc1')
    r.assertEqual(res, 1080)

    # Test missing subcommand
    r.expect('JSON.DEBUG', 'non_existing_doc', '$..a').raiseError()

def testRespCommand(env):
    """Test REJSON.RESP command"""
    r = env
    data = {
        'L1': {
            'a': {
                'A1_B1': 10,
                'A1_B2': False,
                'A1_B3': {
                    'A1_B3_C1': None,
                    'A1_B3_C2': [ 'A1_B3_C2_D1_1', 'A1_B3_C2_D1_2', -19.5, 'A1_B3_C2_D1_4', 'A1_B3_C2_D1_5', {
                        'A1_B3_C2_D1_6_E1': True
                        }
                    ],
                    'A1_B3_C3': [1]
                },
                'A1_B4': {
                    'A1_B4_C1': "foo",
                }
            },
        },
        'L2': {
            'a': {
                'A2_B1': 20,
                'A2_B2': False,
                'A2_B3': {
                    'A2_B3_C1': None,
                    'A2_B3_C2': [ 'A2_B3_C2_D1_1', 'A2_B3_C2_D1_2', -37.5, 'A2_B3_C2_D1_4', 'A2_B3_C2_D1_5', {
                        'A2_B3_C2_D1_6_E1': False
                        }
                    ],
                    'A2_B3_C3': [2]
                },
                'A2_B4': {
                    'A2_B4_C1': "bar",
                }
            },
        },
    }
    r.assertOk(r.execute_command('JSON.SET', 'doc1', '$', json.dumps(data)))
    # Test multi
    res = r.execute_command('JSON.RESP', 'doc1', '$..a')
    r.assertEqual(res, [['{', 'A1_B1', 10, 'A1_B2', 'false', 'A1_B3', ['{', 'A1_B3_C1', None, 'A1_B3_C2', ['[', 'A1_B3_C2_D1_1', 'A1_B3_C2_D1_2', '-19.5', 'A1_B3_C2_D1_4', 'A1_B3_C2_D1_5', ['{', 'A1_B3_C2_D1_6_E1', 'true']], 'A1_B3_C3', ['[', 1]], 'A1_B4', ['{', 'A1_B4_C1', 'foo']], ['{', 'A2_B1', 20, 'A2_B2', 'false', 'A2_B3', ['{', 'A2_B3_C1', None, 'A2_B3_C2', ['[', 'A2_B3_C2_D1_1', 'A2_B3_C2_D1_2', '-37.5', 'A2_B3_C2_D1_4', 'A2_B3_C2_D1_5', ['{', 'A2_B3_C2_D1_6_E1', 'false']], 'A2_B3_C3', ['[', 2]], 'A2_B4', ['{', 'A2_B4_C1', 'bar']]])

    # Test single
    resSingle = r.execute_command('JSON.RESP', 'doc1', '$.L1.a')
    r.assertEqual(resSingle, [['{', 'A1_B1', 10, 'A1_B2', 'false', 'A1_B3', ['{', 'A1_B3_C1', None, 'A1_B3_C2', ['[', 'A1_B3_C2_D1_1', 'A1_B3_C2_D1_2', '-19.5', 'A1_B3_C2_D1_4', 'A1_B3_C2_D1_5', ['{', 'A1_B3_C2_D1_6_E1', 'true']], 'A1_B3_C3', ['[', 1]], 'A1_B4', ['{', 'A1_B4_C1', 'foo']]])

    # Test missing path
    r.assertEqual(r.execute_command('JSON.RESP', 'doc1', '$.nowhere'), [])

    # Test missing key
    res = r.execute_command('JSON.RESP', 'non_existing_doc', '$..a')
    r.assertEqual(res, None)

    # Test legacy
    res = r.execute_command('JSON.RESP', 'doc1', '.L1.a')
    r.assertEqual([res], resSingle)

    # Test default path
    r.assertOk(r.execute_command('JSON.SET', 'doc1', '$', '[[1],[2]]'))
    res = r.execute_command('JSON.RESP', 'doc1')
    r.assertEqual(res, ['[', ['[', 1], ['[', 2]])

def testArrIndexCommand(env):
    """Test JSON.ARRINDEX command"""
    r = env
    # Test index of int scalar in multi values using filter expression
    r.assertOk(r.execute_command('JSON.SET',
                                 'store',
                                 '$',
                                 '{"store":{"book":[{"category":"reference","author":"Nigel Rees","title":"Sayings of the Century","price":8.95,"size":[10,20,30,40]},{"category":"fiction","author":"Evelyn Waugh","title":"Sword of Honour","price":12.99,"size":[50,60,70,80]},{"category":"fiction","author":"Herman Melville","title":"Moby Dick","isbn":"0-553-21311-3","price":8.99,"size":[5,10,20,30]},{"category":"fiction","author":"J. R. R. Tolkien","title":"The Lord of the Rings","isbn":"0-395-19395-8","price":22.99,"size":[5,6,7,8]}],"bicycle":{"color":"red","price":19.95}}}'))

    res = r.execute_command('JSON.GET',
                            'store',
                            '$.store.book[?(@.price<10)].size')
    r.assertEqual(res, '[[10,20,30,40],[5,10,20,30]]')
    res = r.execute_command('JSON.ARRINDEX',
                            'store',
                            '$.store.book[?(@.price<10)].size',
                            '20')
    r.assertEqual(res, [1, 2])

    # Test index of int scalar in multi values
    r.assertOk(r.execute_command('JSON.SET', 'test_num',
                                 '.',
                                 '[{"arr":[0,1,3.0,3,2,1,0,3]},{"nested1_found":{"arr":[5,4,3,2,1,0,1,2,3.0,2,4,5]}},{"nested2_not_found":{"arr":[2,4,6]}},{"nested3_scalar":{"arr":"3"}},[{"nested41_not_arr":{"arr_renamed":[1,2,3]}},{"nested42_empty_arr":{"arr":[]}}]]'))

    res = r.execute_command('JSON.GET', 'test_num', '$..arr')
    r.assertEqual(res, '[[0,1,3.0,3,2,1,0,3],[5,4,3,2,1,0,1,2,3.0,2,4,5],[2,4,6],"3",[]]')

    res = r.execute_command('JSON.ARRINDEX', 'test_num', '$..arr', 3)
    r.assertEqual(res, [3, 2, -1, None, -1])

    # Test index of double scalar in multi values
    res = r.execute_command('JSON.ARRINDEX', 'test_num', '$..arr', 3.0)
    r.assertEqual(res, [2, 8, -1, None, -1])

    # Test index of string scalar in multi values
    r.assertOk(r.execute_command('JSON.SET', 'test_string',
                                 '.',
                                 '[{"arr":["bazzz","bar",2,"baz",2,"ba","baz",3]},{"nested1_found":{"arr":[null,"baz2","buzz",2,1,0,1,"2","baz",2,4,5]}},{"nested2_not_found":{"arr":["baz2",4,6]}},{"nested3_scalar":{"arr":"3"}},[{"nested41_arr":{"arr_renamed":[1,"baz",3]}},{"nested42_empty_arr":{"arr":[]}}]]'))
    res = r.execute_command('JSON.GET', 'test_string', '$..arr')
    r.assertEqual(res, '[["bazzz","bar",2,"baz",2,"ba","baz",3],[null,"baz2","buzz",2,1,0,1,"2","baz",2,4,5],["baz2",4,6],"3",[]]')

    res = r.execute_command('JSON.ARRINDEX', 'test_string', '$..arr', '"baz"')
    r.assertEqual(res, [3, 8, -1, None, -1])

    res = r.execute_command('JSON.ARRINDEX', 'test_string', '$..arr', '"baz"', 2)
    r.assertEqual(res, [3, 8, -1, None, -1])
    res = r.execute_command('JSON.ARRINDEX', 'test_string', '$..arr', '"baz"', 4)
    r.assertEqual(res, [6, 8, -1, None, -1])
    res = r.execute_command('JSON.ARRINDEX', 'test_string', '$..arr', '"baz"', -5)
    r.assertEqual(res, [3, 8, -1, None, -1])
    res = r.execute_command('JSON.ARRINDEX', 'test_string', '$..arr', '"baz"', 4, 7)
    r.assertEqual(res, [6, -1, -1, None, -1])
    res = r.execute_command('JSON.ARRINDEX', 'test_string', '$..arr', '"baz"', 4, -1)
    r.assertEqual(res, [6, 8, -1, None, -1])
    res = r.execute_command('JSON.ARRINDEX', 'test_string', '$..arr', '"baz"', 4, 0)
    r.assertEqual(res, [6, 8, -1, None, -1])
    res = r.execute_command('JSON.ARRINDEX', 'test_string', '$..arr', '5', 7, -1)
    r.assertEqual(res, [-1, -1, -1, None, -1])
    res = r.execute_command('JSON.ARRINDEX', 'test_string', '$..arr', '5', 7, 0)
    r.assertEqual(res, [-1, 11, -1, None, -1])

    # Test index of null scalar in multi values
    r.assertOk(r.execute_command('JSON.SET', 'test_null',
                                 '.',
                                 '[{"arr":["bazzz","null",2,null,2,"ba","baz",3]},{"nested1_found":{"arr":["zaz","baz2","buzz",2,1,0,1,"2",null,2,4,5]}},{"nested2_not_found":{"arr":["null",4,6]}},{"nested3_scalar":{"arr":null}},[{"nested41_arr":{"arr_renamed":[1,null,3]}},{"nested42_empty_arr":{"arr":[]}}]]'))
    res = r.execute_command('JSON.GET', 'test_null', '$..arr')
    r.assertEqual(res, '[["bazzz","null",2,null,2,"ba","baz",3],["zaz","baz2","buzz",2,1,0,1,"2",null,2,4,5],["null",4,6],null,[]]')

    res = r.execute_command('JSON.ARRINDEX', 'test_null', '$..arr', 'null')
    r.assertEqual(res, [3, 8, -1, None, -1])

    # Search none-scalar value
    res = r.execute_command('JSON.ARRINDEX', 'test_null', '$.[4][1].nested42_empty_arr.arr', '{"arr":[]}')
    r.assertEqual(res, [-1])

    res = r.execute_command('JSON.ARRINDEX', 'test_null', '.[4][1].nested42_empty_arr.arr', '{"arr":[]}')
    r.assertEqual(res, -1)

    # Test legacy (path begins with dot)
    # Test index of int scalar in single value
    r.assertEqual(r.execute_command('JSON.ARRINDEX', 'test_num', '.[0].arr', 3), 3)
    r.assertEqual(r.execute_command('JSON.ARRINDEX', 'test_num', '.[0].arr', 9), -1)
    r.expect('JSON.ARRINDEX', 'test_num', '.[0].arr_not', 3).raiseError()
    # Test index of string scalar in single value
    r.assertEqual(r.execute_command('JSON.ARRINDEX', 'test_string', '.[0].arr', '"baz"'), 3)
    r.assertEqual(r.execute_command('JSON.ARRINDEX', 'test_string', '.[0].arr', '"faz"'), -1)
    # Test index of null scalar in single value
    r.assertEqual(r.execute_command('JSON.ARRINDEX', 'test_null', '.[0].arr', 'null'), 3)
    r.assertEqual(r.execute_command('JSON.ARRINDEX', 'test_null', '..nested2_not_found.arr', 'null'), -1)

def testErrorMessage(env):

    r = env
    types_data = {
        'object':   {},
        'array':    [],
        'string':   'str',
        'integer':  42,
        'number':   1.2,
        'boolean':  False,
        'null': None
    }
    r.assertOk(r.execute_command('JSON.SET', 'doc1', '$', json.dumps(types_data)))
    res = r.execute_command('JSON.GET', 'doc1', '$')
    r.assertEqual([types_data], json.loads(res))

    r.assertEqual(r.execute_command('HSET', 'hash_key', 'a', '1', 'b', '2'), 2)

    # Notice: redis client is parsing error responses and trimming prefixes such as 'ERR'

    # ARRAPPEND
    r.assertEqual(r.execute_command('JSON.ARRAPPEND', 'doc1', '$.string', '"abc"'), [None])
    r.assertEqual(r.execute_command('JSON.ARRAPPEND', 'doc1', '$.nowhere', '"abc"'), [])
    r.expect('JSON.ARRAPPEND', 'doc_none', '$.string', '"abc"').raiseError().contains("doesn't exist")
    r.expect('JSON.ARRAPPEND', 'hash_key', '$.string', '"abc"').raiseError().contains("wrong Redis type")

    r.expect('JSON.ARRAPPEND', 'doc1', '.string', '"abc"').raiseError().contains("not an array")
    r.expect('JSON.ARRAPPEND', 'doc1', '.nowhere', '"abc"').raiseError().contains("does not exist")
    r.expect('JSON.ARRAPPEND', 'doc_none', '.string', '"abc"').raiseError().contains("doesn't exist")
    """ Legacy 1.0.8
    json.set doc1 .  '{"a":[0, 1, 2, 3, 4, 5], "b":{"x":100}}'
    OK
    json.arrappend doc1 .b '1'
    (error) ERR wrong type of path value - expected array but found object
    json.arrappend doc1 .bzzz '1'
    (error) ERR key 'bzzz' does not exist at level 0 in path
    json.arrappend doc1zzz .b '1'
    (error) WRONGTYPE Operation against a key holding the wrong kind of value
    """

    # ARRPOP
    r.assertEqual(r.execute_command('JSON.ARRPOP', 'doc1', '$.string', '"abc"'), [None])
    r.assertEqual(r.execute_command('JSON.ARRPOP', 'doc1', '$.nowhere', '"abc"'), [])
    r.expect('JSON.ARRPOP', 'doc_none', '$..string', '"abc"').raiseError().contains("doesn't exist")
    r.expect('JSON.ARRPOP', 'hash_key', '$..string', '"abc"').raiseError().contains("wrong Redis type")

    r.expect('JSON.ARRPOP', 'doc1', '.string', '"abc"').raiseError().contains("not an array")
    r.expect('JSON.ARRPOP', 'doc1', '.nowhere', '"abc"').raiseError().contains("does not exist")
    r.expect('JSON.ARRPOP', 'doc_none', '.string', '"abc"').raiseError().contains("doesn't exist")
    """ Legacy 1.0.8:
    json.set doc1 .  '{"a":[0, 1, 2, 3, 4, 5], "b":{"x":100}}'
    OK
    json.arrpop doc1 .b 1
    (error) ERR wrong type of path value - expected array but found object
    json.arrpop doc1 .bzzz 1
    (error) ERR key 'bzzz' does not exist at level 0 in path
    json.arrpop doc1zzz .b 1
    (error) WRONGTYPE Operation against a key holding the wrong kind of value
    """

    # ARRINDEX
    r.assertEqual(r.execute_command('JSON.ARRINDEX', 'doc1', '$.number', '"abc"'), [None])
    r.assertEqual(r.execute_command('JSON.ARRINDEX', 'doc1', '$.nowhere', '"abc"'), [])
    r.expect('JSON.ARRINDEX', 'doc_none', '$.number', '"abc"').raiseError().contains("does not exist")
    r.expect('JSON.ARRINDEX', 'hash_key', '$.number', '"abc"').raiseError().contains("wrong Redis type")

    r.expect('JSON.ARRINDEX', 'doc1', '.number', '"abc"').raiseError().contains("expected array")
    r.expect('JSON.ARRINDEX', 'doc1', '.nowhere', '"abc"').raiseError().contains("does not exist")
    r.expect('JSON.ARRINDEX', 'doc_none', '.number', '"abc"').raiseError().contains("does not exist")
    """ Legacy 1.0.8
    json.set doc1 .  '{"a":[0, 1, 2, 3, 4, 5], "b":{"x":100}}'
    OK
    json.arrindex doc1 .b '1'
    (error) ERR wrong type of path value - expected array but found object
    json.arrindex doc1 .bzzz '1'
    (error) ERR key 'bzzz' does not exist at level 0 in path
    json.arrindex doc1zzz .b '1'
    (error) WRONGTYPE Operation against a key holding the wrong kind of value
    """

    # ARRINSERT
    r.assertEqual(r.execute_command('JSON.ARRINSERT', 'doc1', '$.string', 0, '"abc"'), [None])
    r.assertEqual(r.execute_command('JSON.ARRINSERT', 'doc1', '$.nowhere', 0, '"abc"'), [])
    r.expect('JSON.ARRINSERT', 'doc_none', '$.string', 0, '"abc"').raiseError().contains("doesn't exist")
    r.expect('JSON.ARRINSERT', 'hash_key', '$.string', 0, '"abc"').raiseError().contains("wrong Redis type")

    r.expect('JSON.ARRINSERT', 'doc1', '.string', 0, '"abc"').raiseError().contains("not an array")
    r.expect('JSON.ARRINSERT', 'doc1', '.nowhere', 0, '"abc"').raiseError().contains("does not exist")
    r.expect('JSON.ARRINSERT', 'doc_none', '.string', 0, '"abc"').raiseError().contains("doesn't exist")
    """ Legacy 1.0.8:
    json.set doc1 .  '{"a":[0, 1, 2, 3, 4, 5], "b":{"x":100}}'
    OK
    json.arrinsert doc1 .b 0 '1'
    (error) ERR wrong type of path value - expected array but found object
    json.arrinsert doc1 .bzzz 0 '1'
    (error) ERR key 'bzzz' does not exist at level 0 in path
    json.arrinsert doc1zzz .b 0 '1'
    (error) WRONGTYPE Operation against a key holding the wrong kind of value
    """

    # ARRLEN
    r.assertEqual(r.execute_command('JSON.ARRLEN', 'doc1', '$.string', '"abc"'), [None])
    r.assertEqual(r.execute_command('JSON.ARRLEN', 'doc1', '$.nowhere', '"abc"'), [])
    r.expect('JSON.ARRLEN', 'doc_none', '$.string', '"abc"').raiseError().contains("doesn't exist")
    r.expect('JSON.ARRLEN', 'hash_key', '$.string', '"abc"').raiseError().contains("wrong Redis type")

    r.expect('JSON.ARRLEN', 'doc1', '.string', '"abc"').raiseError().contains("not an array")
    r.expect('JSON.ARRLEN', 'doc1', '.nowhere', '"abc"').raiseError().contains("does not exist")
    r.assertEqual(r.execute_command('JSON.ARRLEN', 'doc_none', '.string', '"abc"'), None)
    """ Legacy 1.0.8:
    json.set doc1 .  '{"a":[0, 1, 2, 3, 4, 5], "b":{"x":100}}'
    OK
    json.arrlen doc1 .b
    (error) ERR wrong type of path value - expected array but found object
    json.arrlen doc1 .zzz
    (error) ERR key 'zzz' does not exist at level 0 in path
    json.arrlen doc1zz .zzz
    (nil)
    """

# ARRTRIM
    r.assertEqual(r.execute_command('JSON.ARRTRIM', 'doc1', '$.string', 0, 1), [None])
    r.assertEqual(r.execute_command('JSON.ARRTRIM', 'doc1', '$.nowhere', 0, 1), [])
    r.expect('JSON.ARRTRIM', 'doc_none', '$.string', 0, 1).raiseError().contains("doesn't exist")
    r.expect('JSON.ARRTRIM', 'hash_key', '$.string', 0, 1).raiseError().contains("wrong Redis type")

    r.expect('JSON.ARRTRIM', 'doc1', '.string', 0, 1).raiseError().contains("not an array")
    r.expect('JSON.ARRTRIM', 'doc1', '.nowhere', 0, 1).raiseError().contains("not an array")
    r.expect('JSON.ARRTRIM', 'doc_none', '.string', 0, 1).raiseError().contains("doesn't exist")
    """ Legacy 1.0.8:
    json.set doc1 .  '{"a":[0, 1, 2, 3, 4, 5], "b":{"x":100}}'
    OK
    json.arrtrim doc1 .b 0 3
    (error) ERR wrong type of path value - expected array but found object
    json.arrtrim doc1 .bzzz 0 3
    (error) ERR key 'bzzz' does not exist at level 0 in path
    json.arrtrim doc1zzz .b 0 3
    (error) WRONGTYPE Operation against a key holding the wrong kind of value
    """

    # OBJKEYS
    r.assertEqual(r.execute_command('JSON.OBJKEYS', 'doc1', '$.string'), [None])
    r.assertEqual(r.execute_command('JSON.OBJKEYS', 'doc1', '$.nowhere'), [])
    r.expect('JSON.OBJKEYS', 'doc_none', '$.string').raiseError().contains("doesn't exist")
    r.expect('JSON.OBJKEYS', 'hash_key', '$.string').raiseError().contains("wrong Redis type")

    r.expect('JSON.OBJKEYS', 'doc1', '.string').raiseError().contains("not an object")
    r.assertEqual(r.execute_command('JSON.OBJKEYS', 'doc1', '.nowhere'), None)
    r.assertEqual(r.execute_command('JSON.OBJKEYS', 'doc_none', '.string'), None)
    """ Legacy 1.0.8:
    json.set doc1 .  '{"a":[0, 1, 2, 3, 4, 5], "b":{"x":100}}'
    OK
    json.objkeys doc1 .a
    (error) ERR wrong type of path value - expected object but found array
    json.objkeys doc1 .azzz
    (nil)
    json.objkeys doc1zzz .a
    (nil)
    """

    # OBJLEN
    r.assertEqual(r.execute_command('JSON.OBJLEN', 'doc1', '$.string'), [None])
    r.assertEqual(r.execute_command('JSON.OBJLEN', 'doc1', '$.nowhere'), [])
    r.expect('JSON.OBJLEN', 'doc_none', '$.string').raiseError().contains("does not exist")
    r.expect('JSON.OBJLEN', 'hash_key', '.string').raiseError().contains("wrong Redis type")

    r.expect('JSON.OBJLEN', 'doc1', '.boolean').raiseError().contains("expected object but found boolean")
    r.assertEqual(r.execute_command('JSON.OBJLEN', 'doc1', '.nowhere'), None)
    r.assertEqual(r.execute_command('JSON.OBJLEN', 'doc_none', '.string'), None)
    """ Legacy 1.0.8:
   json.set doc1 .  '{"a":[0, 1, 2, 3, 4, 5], "b":{"x":100}}'
   OK
   json.objkeys doc1 .a
   (error) ERR wrong type of path value - expected object but found array
   json.objkeys doc1 .azzz
   (nil)
   json.objkeys doc1zzz .a
   (nil)
   """

    # NUMINCRBY
    r.assertEqual(r.execute_command('JSON.NUMINCRBY', 'doc1', '$.string', 3), '[null]')
    r.assertEqual(r.execute_command('JSON.NUMINCRBY', 'doc1', '$.nowhere', 3), '[]')
    r.expect('JSON.NUMINCRBY', 'doc_none', '$.string', 3).raiseError().contains("doesn't exist")
    r.expect('JSON.NUMINCRBY', 'hash_key', '$.string', 3).raiseError().contains("wrong Redis type")

    r.expect('JSON.NUMINCRBY', 'doc1', '.string', 3).raiseError().contains("does not contains a number")
    r.expect('JSON.NUMINCRBY', 'doc1', '.nowhere', 3).raiseError().contains("does not contains a number")
    r.expect('JSON.NUMINCRBY', 'doc_none', '.string', 3).raiseError().contains("doesn't exist")
    """ Legacy 1.0.8:
     json.set doc1 .  '{"a":[0, 1, 2, 3, 4, 5], "b":{"x":100}}'
    OK
    json.numincrby doc1 .b 1
    (error) ERR wrong type of path value - expected a number but found object
    json.numincrby doc1 .bzzz 1
    (error) ERR key 'bzzz' does not exist at level 0 in path
    json.numincrby doc1zzz .b 1
    (error) ERR could not perform this operation on a key that doesn't exist
    """


    # NUMMULTBY
    r.assertEqual(r.execute_command('JSON.NUMMULTBY', 'doc1', '$.string', 3), '[null]')
    r.assertEqual(r.execute_command('JSON.NUMMULTBY', 'doc1', '$.nowhere', 3), '[]')
    r.expect('JSON.NUMMULTBY', 'doc_none', '$.string', 3).raiseError().contains("doesn't exist")
    r.expect('JSON.NUMMULTBY', 'hash_key', '$.string', 3).raiseError().contains("wrong Redis type")

    r.expect('JSON.NUMMULTBY', 'doc1', '.string', 3).raiseError().contains("does not contains a number")
    r.expect('JSON.NUMMULTBY', 'doc1', '.nowhere', 3).raiseError().contains("does not contains a number")
    r.expect('JSON.NUMMULTBY', 'doc_none', '.string', 3).raiseError().contains("doesn't exist")
    """ Legacy 1.0.8:
     json.set doc1 .  '{"a":[0, 1, 2, 3, 4, 5], "b":{"x":100}}'
    OK
    json.nummultby doc1 .b 1
    (error) ERR wrong type of path value - expected a number but found object
    json.nummultby doc1 .bzzz 1
    (error) ERR key 'bzzz' does not exist at level 0 in path
    json.nummultby doc1zzz .b 1
    (error) ERR could not perform this operation on a key that doesn't exist
    """

    # STRAPPEND
    r.assertEqual(r.execute_command('JSON.STRAPPEND', 'doc1', '$.number', '"abc"'), [None])
    r.assertEqual(r.execute_command('JSON.STRAPPEND', 'doc1', '$.nowhere', '"abc"'), [])
    r.expect('JSON.STRAPPEND', 'doc_none', '$.number', '"abc"').raiseError().contains("doesn't exist")
    r.expect('JSON.STRAPPEND', 'hash_key', '$.number', '"abc"').raiseError().contains("wrong Redis type")

    r.expect('JSON.STRAPPEND', 'doc1', '.number', '"abc"').raiseError().contains("not a string")
    r.expect('JSON.STRAPPEND', 'doc1', '.nowhere', '"abc"').raiseError().contains("does not exist")
    r.expect('JSON.STRAPPEND', 'doc_none', '.number', '"abc"').raiseError().contains("doesn't exist")
    """ Legacy 1.0.8:
     json.set doc1 .  '{"a":[0, 1, 2, 3, 4, 5], "b":{"x":100}}'
    OK
    json.strappend doc1 .b '"abc"'
    (error) ERR wrong type of path value - expected string but found object
    json.strappend doc1 .bzzz '"abc"'
    (error) ERR key 'bzzz' does not exist at level 0 in path
    json.strappend doc1zzz .b '"abc"'
    (error) WRONGTYPE Operation against a key holding the wrong kind of value
    """


    # STRLEN
    r.assertEqual(r.execute_command('JSON.STRLEN', 'doc1', '$.object', '"abc"'), [None])
    r.assertEqual(r.execute_command('JSON.STRLEN', 'doc1', '$.nowhere', '"abc"'), [])
    r.expect('JSON.STRLEN', 'doc_none', '$.object', '"abc"').raiseError().contains("doesn't exist")
    r.expect('JSON.STRLEN', 'hash_key', '$.object', '"abc"').raiseError().contains("wrong Redis type")

    r.expect('JSON.STRLEN', 'doc1', '.object', '"abc"').raiseError().contains("expected string but found object")
    r.expect('JSON.STRLEN', 'doc1', '.nowhere', '"abc"').raiseError().contains("does not exist")
    r.assertEqual(r.execute_command('JSON.STRLEN', 'doc_none', '.object', '"abc"'), None)
    """ Legacy 1.0.8:
    json.set doc1 .  '{"a":[0, 1, 2, 3, 4, 5], "b":{"x":100}}'
    OK
    json.strlen doc1 .b
    (error) ERR wrong type of path value - expected string but found object
    json.strlen doc1 .bzzz
    (error) ERR key 'bzzz' does not exist at level 0 in path
    json.strlen doc1zzz .b
    (nil)
    """

    # TOGGLE
    r.assertEqual(r.execute_command('JSON.TOGGLE', 'doc1', '$.object'), [None])
    r.assertEqual(r.execute_command('JSON.TOGGLE', 'doc1', '$.nowhere'), [])
    r.expect('JSON.TOGGLE', 'doc_none', '$.object').raiseError().contains("doesn't exist")
    r.expect('JSON.TOGGLE', 'hash_key', '$.object').raiseError().contains("wrong Redis type")

    r.expect('JSON.TOGGLE', 'doc1', '.object').raiseError().contains("not a bool")
    r.expect('JSON.TOGGLE', 'doc1', '.nowhere').raiseError().contains("not a bool")
    r.expect('JSON.TOGGLE', 'doc_none', '.object').raiseError().contains("doesn't exist")
    """ Legacy 1.0.8: not relevant (only since 2.0) """

    # CLEAR
    r.assertEqual(r.execute_command('JSON.CLEAR', 'doc1', '$.null'), 0)
    r.assertEqual(r.execute_command('JSON.CLEAR', 'doc1', '$.nowhere'), 0)
    r.expect('JSON.CLEAR', 'doc_none', '$.string').raiseError().contains("doesn't exist")
    r.expect('JSON.CLEAR', 'hash_key', '$.string').raiseError().contains("wrong Redis type")

    r.assertEqual(r.execute_command('JSON.CLEAR', 'doc1', '.null'), 0)
    r.assertEqual(r.execute_command('JSON.CLEAR', 'doc1', '.nowhere'), 0)
    r.expect('JSON.CLEAR', 'doc_none', '.string').raiseError().contains("doesn't exist")
    """ Legacy 1.0.8: not relevant (only since 2.0) """

    # Commands that operate on all json types

    # DEL
    r.assertEqual(r.execute_command('JSON.DEL', 'doc1', '$.nowhere'), 0)
    r.assertEqual(r.execute_command('JSON.DEL', 'doc_none', '$.object', '"abc"'), 0)
    r.expect('JSON.DEL', 'hash_key', '$.object', '"abc"').raiseError().contains("wrong Redis type")

    r.assertEqual(r.execute_command('JSON.DEL', 'doc1', '.nowhere'), 0)
    r.assertEqual(r.execute_command('JSON.DEL', 'doc_none', '.object'), 0)

    # DEBUG
    r.assertEqual(r.execute_command('JSON.DEBUG', 'MEMORY', 'doc1', '$.nowhere'), [])
    r.assertEqual(r.execute_command('JSON.DEBUG', 'MEMORY', 'doc_none', '$.object'), [])
    r.expect('JSON.DEBUG', 'MEMORY', 'hash_key', '$.object').raiseError().contains("wrong Redis type")

    r.expect('JSON.DEBUG', 'MEMORY', 'doc1', '.nowhere').raiseError().contains("does not exist")
    r.assertEqual(r.execute_command('JSON.DEBUG', 'MEMORY', 'doc_none', '.object'), 0)
    """ Legacy 1.0.8:
    json.set doc1 .  '{"a":[0, 1, 2, 3, 4, 5], "b":{"x":100}}'
    OK
    json.debug memory doc1 .bzzz
    (error) ERR key 'bzzz' does not exist at level 0 in path
    json.debug memory doc1zzz .b
    (nil)
    """

    # RESP
    r.assertEqual(r.execute_command('JSON.RESP', 'doc1', '$.nowhere'), [])
    r.assertEqual(r.execute_command('JSON.RESP', 'doc_none', '$.object'), None)
    r.assertEqual(r.execute_command('JSON.RESP', 'doc_none', '$.object'), None)
    r.expect('JSON.RESP', 'hash_key', '$.object').raiseError().contains("wrong Redis type")

    r.expect('JSON.RESP', 'doc1', '.nowhere').raiseError().contains("does not exist")
    r.assertEqual(r.execute_command('JSON.RESP', 'doc_none', '.object'), None)
    """ Legacy 1.0.8:
    json.set doc1 .  '{"a":[0, 1, 2, 3, 4, 5], "b":{"x":100}}'
    OK
    json.resp doc1 .bzzz
    (error) ERR key 'bzzz' does not exist at level 0 in path
    json.resp doc1zzz .b
    (nil)
    """

def testFilterDup_issue667(env):
    """Test issue #667 """
    r = env

    r.assertOk(r.execute_command('JSON.SET',
                                 'test',
                                 '$',
                                 '[{"name":{"first":"Markss","middle":"S","last":"Pronto"},"rank":1},{"name":{"first":"A","middle":"A","last":"Pronto"},"rank":8},{"name":{"first":"A","middle":"A","last":"Pronto"},"rank":90}]'))

    # Should not get duplicated results
    res = r.execute_command('JSON.GET',
                            'test',
                            '$.[?(@.name.first=="A")]')
    r.assertEqual(res, '[{"name":{"first":"A","middle":"A","last":"Pronto"},"rank":8},{"name":{"first":"A","middle":"A","last":"Pronto"},"rank":90}]')


