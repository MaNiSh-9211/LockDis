-- Semaphore release: return our permit; clean up when empty.
-- KEYS[1] = zset, ARGV[1] = token
-- Returns 1 released, 0 if we held no permit.

if redis.call('ZREM', KEYS[1], ARGV[1]) == 1 then
  if redis.call('ZCARD', KEYS[1]) == 0 then
    redis.call('DEL', KEYS[1])
  end
  return 1
end
return 0
