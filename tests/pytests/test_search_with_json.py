from redisearch import Client, TextField, IndexDefinition, Query
import json

# Creating a client with a given index name
ndxName = "myIndex"
client = Client(ndxName)
indices = client.redis.execute_command('FT._LIST')
if client.index_name in indices:
    client.drop_index()

# IndexDefinition is available for RediSearch 2.0+
definition = IndexDefinition(prefix=['doc:', 'article:'])

# Creating the index definition and schema
client.create_index((TextField("title", weight=5.0), TextField("body")), definition=definition)

client.redis.hset('doc:1',
                  mapping={
                      'title': 'RediSearch',
                      'body': 'Redisearch implements a search engine on top of redis'
                  })

data = {
    'foo': 'bar'
}

client.redis.execute_command('JSON.SET', 'doc:3', '.', json.dumps(data))
reply = json.loads(client.redis.execute_command('JSON.GET', 'doc:3'))

print("simple search 1:")
res = client.search("search engine")
print("simple search result#: " + str(res.total))
for doc in res.docs:
    print(doc.id)

client.drop_index()
client.create_index((TextField("title", weight=5.0), TextField("body")), definition=definition)
client.redis.hset('doc:1',
                  mapping={
                      'title': 'RediSearch',
                      'body': 'Redisearch implements a search index on top of redis'
                  })

print("simple search 1:")
res = client.search("search engine")
print("simple search result#: " + str(res.total))
for doc in res.docs:
    print(doc.id)
