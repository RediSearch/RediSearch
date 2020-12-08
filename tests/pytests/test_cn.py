# -*- coding: utf-8 -*-

import redis
import unittest
import os
from includes import *
from common import waitForIndex


SRCTEXT=os.path.join(os.path.dirname(__file__), '..', 'ctests', 'cn_sample.txt')
GENTXT=os.path.join(os.path.dirname(__file__), '..', 'ctests', 'genesis.txt')

GEN_CN_S = """
太初，上帝创造了天地。 那时，大地空虚混沌，还没有成形，黑暗笼罩着深渊，上帝的灵运行在水面上。 上帝说：“要有光！”就有了光。 上帝看光是好的，就把光和暗分开， 称光为昼，称暗为夜。晚上过去，早晨到来，这是第一天。 上帝说：“水与水之间要有穹苍，把水分开。” 果然如此。上帝开辟了穹苍，用穹苍将水上下分开。 上帝称穹苍为天空。晚上过去，早晨到来，这是第二天。
"""

GEN_CN_T = """
太初，上帝創造了天地。 那時，大地空虛混沌，還沒有成形，黑暗籠罩著深淵，上帝的靈運行在水面上。 上帝說：「要有光！」就有了光。 上帝看光是好的，就把光和暗分開， 稱光為晝，稱暗為夜。晚上過去，早晨到來，這是第一天。 上帝說：「水與水之間要有穹蒼，把水分開。」 果然如此。上帝開闢了穹蒼，用穹蒼將水上下分開。 上帝稱穹蒼為天空。晚上過去，早晨到來，這是第二天。
"""


def testCn(env):
    text = open(SRCTEXT).read()
    env.cmd('ft.create', 'idx', 'ON', 'HASH', 'LANGUAGE', 'CHINESE', 'schema', 'txt', 'text')
    waitForIndex(env, 'idx')
    env.cmd('ft.add', 'idx', 'doc1', 1.0, 'FIELDS', 'txt', text)
    res = env.cmd('ft.search', 'idx', '之旅', 'SUMMARIZE', 'HIGHLIGHT', 'LANGUAGE', 'chinese')
    cn = '2009\xe5\xb9\xb4\xef\xbc\x98\xe6\x9c\x88\xef\xbc\x96\xe6\x97\xa5\xe5\xbc\x80\xe5\xa7\x8b\xe5\xa4\xa7\xe5\xad\xa6<b>\xe4\xb9\x8b\xe6\x97\x85</b>\xef\xbc\x8c\xe5\xb2\xb3\xe9\x98\xb3\xe4\xbb\x8a\xe5\xa4\xa9\xe7\x9a\x84\xe6\xb0\x94\xe6\xb8\xa9\xe4\xb8\xba38.6\xe2\x84\x83, \xe4\xb9\x9f\xe5\xb0\xb1\xe6\x98\xaf101.48\xe2\x84\x89... \xef\xbc\x8c \xe5\x8d\x95\xe4\xbd\x8d \xe5\x92\x8c \xe5\x85\xa8\xe8\xa7\x92 : 2009\xe5\xb9\xb4 8\xe6\x9c\x88 6\xe6\x97\xa5 \xe5\xbc\x80\xe5\xa7\x8b \xe5\xa4\xa7\xe5\xad\xa6 <b>\xe4\xb9\x8b\xe6\x97\x85</b> \xef\xbc\x8c \xe5\xb2\xb3\xe9\x98\xb3 \xe4\xbb\x8a\xe5\xa4\xa9 \xe7\x9a\x84 \xe6\xb0\x94\xe6\xb8\xa9 \xe4\xb8\xba 38.6\xe2\x84\x83 , \xe4\xb9\x9f\xe5\xb0\xb1\xe6\x98\xaf 101... '
    env.assertContains(cn, res[2])

    res = env.cmd('ft.search', 'idx', 'hacker', 'summarize', 'highlight')
    cn = ' visit http://code.google.com/p/jcseg, we all admire the <b>hacker</b> spirit!\xe7\x89\xb9\xe6\xae\x8a\xe6\x95\xb0\xe5\xad\x97: \xe2\x91\xa0 \xe2\x91\xa9 \xe2\x91\xbd \xe3\x88\xa9. ... p / jcseg , we all admire appreciate like love enjoy the <b>hacker</b> spirit mind ! \xe7\x89\xb9\xe6\xae\x8a \xe6\x95\xb0\xe5\xad\x97 : \xe2\x91\xa0 \xe2\x91\xa9 \xe2\x91\xbd \xe3\x88\xa9 . ~~~ ... '
    env.assertContains(cn, res[2])

    # Check that we can tokenize english with friso (sub-optimal, but don't want gibberish)
    gentxt = open(GENTXT).read()
    env.cmd('ft.add', 'idx', 'doc2', 1.0, 'FIELDS', 'txt', gentxt)
    res = env.cmd('ft.search', 'idx', 'abraham', 'summarize', 'highlight')
    cn = 'thy name any more be called Abram, but thy name shall be <b>Abraham</b>; for a father of many nations have I made thee. {17:6} And... and I will be their God. {17:9} And God said unto <b>Abraham</b>, Thou shalt keep my covenant therefore, thou, and thy seed... hath broken my covenant. {17:15} And God said unto <b>Abraham</b>, As for Sarai thy wife, thou shalt not call her name Sarai... '
    env.assertContains(cn, res[2])

    # Add an empty document. Hope we don't crash!
    env.cmd('ft.add', 'idx', 'doc3', 1.0, 'fields', 'txt1', '')

    # Check splitting. TODO - see how to actually test for matches
    env.cmd('ft.search', 'idx', 'redis客户端', 'language', 'chinese')
    env.cmd('ft.search', 'idx', '简介Redisson 是一个高级的分布式协调Redis客户端', 'language', 'chinese')

def testMixedHighlight(env):
    txt = r"""
Redis支持主从同步。数据可以从主服务器向任意数量的从服务器上同步，从服务器可以是关联其他从服务器的主服务器。这使得Redis可执行单层树复制。从盘可以有意无意的对数据进行写操作。由于完全实现了发布/订阅机制，使得从数据库在任何地方同步树时，可订阅一个频道并接收主服务器完整的消息发布记录。同步对读取操作的可扩展性和数据冗余很有帮助。[8]
"""
    env.cmd('ft.create', 'idx', 'ON', 'HASH', 'LANGUAGE_FIELD', 'chinese', 'schema', 'txt', 'text')
    waitForIndex(env, 'idx')
    env.cmd('ft.add', 'idx', 'doc1', 1.0, 'language', 'chinese', 'fields', 'txt', txt)
    # Should not crash!
    env.cmd('ft.search', 'idx', 'redis', 'highlight')

def testTradSimp(env):
    # Ensure that traditional chinese characters get converted to their simplified variants
    env.cmd('ft.create', 'idx', 'ON', 'HASH', 'LANGUAGE_FIELD', '__language', 'schema', 'txt', 'text')
    waitForIndex(env, 'idx')
    env.cmd('ft.add', 'idx', 'genS', 1.0, 'language', 'chinese', 'fields', 'txt', GEN_CN_S)
    env.cmd('ft.add', 'idx', 'genT', 1.0, 'language', 'chinese', 'fields', 'txt', GEN_CN_T)

    res = env.cmd('ft.search', 'idx', '那时', 'language', 'chinese', 'highlight', 'summarize')
    env.assertContains('<b>\xe9\x82\xa3\xe6\x97\xb6</b>\xef... ', res[2])
    env.assertContains('<b>\xe9\x82\xa3\xe6\x99\x82</b>\xef... ', res[4])

    # The variants should still show up as different, so as to not modify
    res1 = {res[2][i]:res[2][i + 1] for i in range(0, len(res[2]), 2)}
    res2 = {res[4][i]:res[4][i + 1] for i in range(0, len(res[4]), 2)}
    env.assertTrue('那时' in res1['txt'])
    env.assertTrue('那時' in res2['txt'])

    # Ensure that searching in traditional still gives us the proper results:
    res = env.cmd('ft.search', 'idx', '那時', 'language', 'chinese', 'highlight')
    res1 = {res[2][i]:res[2][i + 1] for i in range(0, len(res[2]), 2)}
    res2 = {res[4][i]:res[4][i + 1] for i in range(0, len(res[4]), 2)}
    env.assertTrue('那时' in res1['txt'])
    env.assertTrue('那時' in res2['txt'])

def testMixedEscapes(env):
    env.cmd('ft.create', 'idx', 'ON', 'HASH', 'LANGUAGE_FIELD', '__language', 'schema', 'txt', 'text')
    waitForIndex(env, 'idx')
    env.cmd('ft.add', 'idx', 'doc1', 1.0, 'language', 'chinese', 'fields', 'txt', 'hello\\-world 那时')
    env.cmd('ft.add', 'idx', 'doc2', 1.0, 'fields', 'txt', 'hello\\-world')
    env.cmd('ft.add', 'idx', 'doc3', 1.0, 'language', 'chinese', 'fields', 'txt', 'one \\:\\:hello two 器上同步 \\-hello world\\- two 器上同步')

    r = env.cmd('ft.search', 'idx', 'hello\\-world')
    env.assertEqual(2, r[0])
    env.assertIn('doc1', r)
    env.assertIn('doc2', r)
    r = env.cmd('ft.search', 'idx', '\\:\\:hello')
    env.assertEqual('doc3', r[1])
    r = env.cmd('ft.search', 'idx', '\\-hello')
    env.assertEqual('doc3', r[1])
    r = env.cmd('ft.search', 'idx', 'two')
    env.assertEqual('doc3', r[1])
    r = env.cmd('ft.search', 'idx', 'world\\-')
    env.assertEqual('doc3', r[1])
