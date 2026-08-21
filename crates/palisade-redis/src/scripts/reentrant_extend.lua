-- Reentrant extend: refresh TTL only if we still own the hold count.
-- KEYS[1] = lock hash, ARGV[1] = token, ARGV[2] = new ttl ms
-- Returns 1 extended, -1 if we no longer own it.

if redis.call('EXISTS', KEYS[1]) == 0 then
  return -1
end

if redis.call('HGET', KEYS[1], 'owner') == ARGV[1] then
  redis.call('PEXPIRE', KEYS[1], ARGV[2])
  return 1
end
return -1
