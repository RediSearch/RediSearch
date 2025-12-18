-- Takes a JSON value and sets it to the root

local js = ARGV[1]
redis.call('SET', KEYS[1], js)
return 'OK'
