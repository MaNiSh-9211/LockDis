-- Reentrant release: decrement hold count; delete only at zero.
-- KEYS[1] = lock hash
-- ARGV[1] = owner token, ARGV[2] = ttl ms (refreshed on partial release)
-- Returns remaining holds, or -1 if we no longer own the lock.

if redis.call('EXISTS', KEYS[1]) == 0 then
  return -1
end

if redis.call('HGET', KEYS[1], 'owner') ~= ARGV[1] then
  return -1
end

local count = redis.call('HINCRBY', KEYS[1], 'count', -1)
if count <= 0 then
  redis.call('DEL', KEYS[1])
  return 0
end

if tonumber(ARGV[2]) > 0 then
  redis.call('PEXPIRE', KEYS[1], ARGV[2])
end
return count
