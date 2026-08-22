-- Read grant: shared while no writer holds the structure.
-- KEYS[1] = rw hash, KEYS[2] = fence counter
-- ARGV[1] = token, ARGV[2] = ttl ms, ARGV[3] = fence ttl ms
-- Returns {status, fence}: 1 granted, 0 denied (writer holds).

if redis.call('EXISTS', KEYS[1]) == 0 then
  redis.call('HSET', KEYS[1], 'mode', 'r', 'readers', 1)
  redis.call('PEXPIRE', KEYS[1], ARGV[2])
  return {1, redis.call('INCR', KEYS[2])}
end

if redis.call('HGET', KEYS[1], 'mode') == 'r' then
  redis.call('HINCRBY', KEYS[1], 'readers', 1)
  redis.call('PEXPIRE', KEYS[1], ARGV[2])
  return {1, redis.call('INCR', KEYS[2])}
end

return {0, 0}
