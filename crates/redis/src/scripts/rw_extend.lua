-- Read-write extend: any live reader or the writer may refresh the TTL.
-- KEYS[1] = rw hash, ARGV[1] = token, ARGV[2] = new ttl ms
-- Returns 1 extended, -1 if the structure is gone or owned otherwise.

if redis.call('EXISTS', KEYS[1]) == 0 then
  return -1
end

if redis.call('HGET', KEYS[1], 'mode') == 'r' then
  redis.call('PEXPIRE', KEYS[1], ARGV[2])
  return 1
end

if redis.call('HGET', KEYS[1], 'mode') == 'w'
   and redis.call('HGET', KEYS[1], 'owner') == ARGV[1] then
  redis.call('PEXPIRE', KEYS[1], ARGV[2])
  return 1
end
return -1
