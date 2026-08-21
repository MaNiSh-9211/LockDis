-- Read release: decrement reader count; delete at zero.
-- KEYS[1] = rw hash, ARGV[1] = ttl ms (refreshed while readers remain)
-- Returns remaining readers, or -1 if not in read mode (never was / lost).

if redis.call('EXISTS', KEYS[1]) == 0 then
  return -1
end

if redis.call('HGET', KEYS[1], 'mode') ~= 'r' then
  return -1
end

local readers = redis.call('HINCRBY', KEYS[1], 'readers', -1)
if readers <= 0 then
  redis.call('DEL', KEYS[1])
  return 0
end

redis.call('PEXPIRE', KEYS[1], ARGV[1])
return readers
