-- Write release: ownership-checked delete of the whole rw structure.
-- KEYS[1] = rw hash, ARGV[1] = token
-- Returns 1 released, -1 if we don't own the write lock.

if redis.call('EXISTS', KEYS[1]) == 0 then
  return -1
end

if redis.call('HGET', KEYS[1], 'mode') == 'w'
   and redis.call('HGET', KEYS[1], 'owner') == ARGV[1] then
  redis.call('DEL', KEYS[1])
  return 1
end

return -1
