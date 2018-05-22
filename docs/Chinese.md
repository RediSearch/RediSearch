# Chinese support in RediSearch

Support for adding documents in Chinese is available starting at version 0.99.0.

Chinese support allows Chinese documents to be added and tokenized using segmentation
rather than simple tokenization using whitespace and/or punctuation.

Indexing a Chinese document is different than indexing a document in most other
languages because of how tokens are extracted. While most languages can have
their tokens distinguished by separation characters and whitespace, this
is not common in Chinese.

Chinese tokenization is done by scanning the input text and checking every
character or sequence of characters against a dictionary of predefined terms
and determining the most likely (based on the surrounding terms and characters)
match.

RediSearch makes use of the [Friso](https://github.com/lionsoul2014/friso)
chinese tokenization library for this purpose. This is largely transparent to
the user and often no additional configuration is required.

## Example: Using Chinese in RediSearch

In pseudo-code:

```
FT.CREATE idx SCHEMA txt TEXT
FT.ADD idx docCn 1.0 LANGUAGE chinese FIELDS txt "Redis支持主从同步。数据可以从主服务器向任意数量的从服务器上同步，从服务器可以是关联其他从服务器的主服务器。这使得Redis可执行单层树复制。从盘可以有意无意的对数据进行写操作。由于完全实现了发布/订阅机制，使得从数据库在任何地方同步树时，可订阅一个频道并接收主服务器完整的消息发布记录。同步对读取操作的可扩展性和数据冗余很有帮助。[8]"
FT.SEARCH idx "数据" LANGUAGE chinese HIGHLIGHT SUMMARIZE
# Outputs:
# <b>数据</b>?... <b>数据</b>进行写操作。由于完全实现了发布... <b>数据</b>冗余很有帮助。[8...
```

Using the Python Client:

```
# -*- coding: utf-8 -*-

from redisearch.client import Client, Query
from redisearch import TextField

client = Client('idx')
try:
    client.drop_index()
except:
    pass

client.create_index([TextField('txt')])

# Add a document
client.add_document('docCn1',
                    txt='Redis支持主从同步。数据可以从主服务器向任意数量的从服务器上同步从服务器可以是关联其他从服务器的主服务器。这使得Redis可执行单层树复制。从盘可以有意无意的对数据进行写操作。由于完全实现了发布/订阅机制，使得从数据库在任何地方同步树时，可订阅一个频道并接收主服务器完整的消息发布记录。同步对读取操作的可扩展性和数据冗余很有帮助。[8]',
                    language='chinese')
print client.search(Query('数据').summarize().highlight().language('chinese')).docs[0].txt
```

Prints:

```
<b>数据</b>?... <b>数据</b>进行写操作。由于完全实现了发布... <b>数据</b>冗余很有帮助。[8... 
```

## Using custom dictionaries

If you wish to use a custom dictionary, you can do so at the module level when
loading the module. The `FRISOINI` setting can point to the location of a
`friso.ini` file which contains the relevant settings and paths to the dictionary
files.

Note that there is no "default" friso.ini file location. RediSearch comes with
its own `friso.ini` and dictionary files which are compiled into the module
binary at build-time.