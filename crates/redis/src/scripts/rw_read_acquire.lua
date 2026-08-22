-- Read grant: shared while no writer holds the structure. Each reader
-- registers its own token so a STALE reader's late release can never
-- corrupt the live count (enterprise hardening, see EDGE_CASES.md #1).
-- KEYS[1] = rw hash, KEYS[2] = fence counter
-- ARGV[1] = token, ARGV[2] = ttl ms, ARGV[3] = fence ttl ms
-- Returns {status, fence}: 1 granted, 0 denied (writer holds).

if redis.call('EXISTS', KEYS[1]) == 0 then
  redis.call('HSET', KEYS[1], 'mode', 'r', 'readers', 1, 'r:' .. ARGV[1], '1')
  redis.call('PEXPIRE', KEYS[1], ARGV[2])
  return {1, redis.call('INCR', KEYS[2])}
end

if redis.call('HGET', KEYS[1], 'mode') == 'r'
   and redis.call('HEXISTS', KEYS[1], 'r:' .. ARGV[1]) == 0 then
  redis.call('HINCRBY', KEYS[1], 'readers', 1)
  redis.call('HSET', KEYS[1], 'r:' .. ARGV[1], '1')
  redis.call('PEXPIRE', KEYS[1], ARGV[2])
  return {1, redis.call('INCR', KEYS[2])}
end

return {0, 0}
