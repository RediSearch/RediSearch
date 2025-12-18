-- Sets a JSON value to a msgpacked key
-- Path to value is given by ARGV

-- Get the stored value and unpack it
local mp = redis.call('GET', KEYS[1])
local v = cmsgpack.unpack(mp)

-- Parse the path
local p = v
local c = v
local token, n
while #ARGV > 1 do
  p = c
  token = table.remove(ARGV,1)
  n = tonumber(token)
  if n then
    c = p[n+1]
  else
    c = p[token]
  end
end

-- Get the new value
local nv = cjson.decode(ARGV[1])

-- Replace the value in the parent
  if n then
    p[n+1] = nv
  else
    p[token] = nv
  end

-- Pack the value
local nmp = cmsgpack.pack(v)

-- Store it
redis.call('SET', KEYS[1], nmp)

return 'OK'