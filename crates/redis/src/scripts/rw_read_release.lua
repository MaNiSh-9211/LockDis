-- Read release: only the REGISTERED reader token may decrement; a stale
-- reader (lease expired, slot taken over) finds no field and reports loss
-- without touching live accounting.
-- KEYS[1] = rw hash, ARGV[1] = token, ARGV[2] = ttl ms (refresh while readers remain)
-- Returns remaining readers, or -1 if this token held no read slot.

if redis.call('EXISTS', KEYS[1]) == 0 then
  return -1
end

if redis.call('HGET', KEYS[1], 'mode') ~= 'r' then
  return -1
end

if redis.call('HEXISTS', KEYS[1], 'r:' .. ARGV[1]) == 0 then
  return -1
end

redis.call('HDEL', KEYS[1], 'r:' .. ARGV[1])
local readers = redis.call('HINCRBY', KEYS[1], 'readers', -1)
if readers <= 0 then
  redis.call('DEL', KEYS[1])
  return 0
end

redis.call('PEXPIRE', KEYS[1], ARGV[2])
return readers
