-- Write grant: exclusive; same-token reentry refreshes the lease.
-- KEYS[1] = rw hash, KEYS[2] = fence counter
-- ARGV[1] = token, ARGV[2] = ttl ms, ARGV[3] = fence ttl ms
-- Returns {status, fence}: 1 fresh write lock, 2 reentry, 0 denied.

if redis.call('EXISTS', KEYS[1]) == 0 then
  redis.call('HSET', KEYS[1], 'mode', 'w', 'owner', ARGV[1])
  redis.call('PEXPIRE', KEYS[1], ARGV[2])
  return {1, redis.call('INCR', KEYS[2])}
end

if redis.call('HGET', KEYS[1], 'mode') == 'w'
   and redis.call('HGET', KEYS[1], 'owner') == ARGV[1] then
  redis.call('PEXPIRE', KEYS[1], ARGV[2])
  return {2, redis.call('INCR', KEYS[2])}
end

return {0, 0}
