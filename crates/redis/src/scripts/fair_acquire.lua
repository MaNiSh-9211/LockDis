-- Fair acquire: direct grant only when no one is queued ahead of us;
-- otherwise enqueue (once) and refresh our heartbeat. The queue is FIFO:
-- newest waiters LPUSH to the head side, the oldest waiter sits at the
-- tail and is served first by fair_release or by self-grant here.
--
-- KEYS[1] = lock key, KEYS[2] = queue list, KEYS[3] = fence counter,
-- KEYS[4] = this waiter's heartbeat key
-- ARGV[1] = token, ARGV[2] = ttl ms, ARGV[3] = fence ttl ms, ARGV[4] = heartbeat ttl ms
-- Returns {status, fence}: 2 handed off to us earlier, 1 granted now,
-- 0 still waiting (queued).

if redis.call('GET', KEYS[1]) == ARGV[1] then
  redis.call('PEXPIRE', KEYS[1], ARGV[2])
  return {2, redis.call('INCR', KEYS[3])}
end

local oldest = redis.call('LRANGE', KEYS[2], -1, -1)[1]
if redis.call('EXISTS', KEYS[1]) == 0 and (not oldest or oldest == ARGV[1]) then
  if oldest then
    redis.call('LREM', KEYS[2], 1, ARGV[1])
  end
  redis.call('SET', KEYS[1], ARGV[1], 'PX', ARGV[2])
  return {1, redis.call('INCR', KEYS[3])}
end

if redis.call('LPOS', KEYS[2], ARGV[1]) == false then
  redis.call('LPUSH', KEYS[2], ARGV[1])
end
redis.call('SET', KEYS[4], '1', 'PX', ARGV[4])
redis.call('PEXPIRE', KEYS[2], tonumber(ARGV[2]) * 10)
return {0, 0}
