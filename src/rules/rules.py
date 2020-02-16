import redis
import time

redis = redis.Redis(host='localhost', port=6379, db=0)
redis_pipe = redis.pipeline()

print'start'

redis.execute_command('flushall')
redis.execute_command("ft.create", "idx", "WITHRULES", "SCHEMA", "f1", "text")
redis.execute_command("ft.ruleadd", "idx", "rule1", "PREFIX", "user:", "INDEX")
redis.execute_command("ft.ruleadd", "idx", "rule2", "EXPR", "@year>2015", "INDEX")
redis.execute_command("hset", "user:mnunberg", "foo", "bar")
redis.execute_command("hset", "user:mnunberg", "f1", "hello world")

time.sleep(1)
print redis.execute_command('FT.SEARCH idx hello')
print redis.execute_command('keys *')

redis.execute_command('hset', 'someDoc', 'year', '2019')
redis.execute_command('hset', 'someDoc', 'name', 'mark')
redis.execute_command('hset', 'someDoc', 'f1', 'goodbye')

time.sleep(1)
print redis.execute_command('ft.search', 'idx', 'goodbye')
