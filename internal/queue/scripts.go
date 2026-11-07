package queue

import "github.com/redis/go-redis/v9"

// scheduleRetry moves a failed message into the retry set and removes it from
// the stream in one atomic step, so a crash can never leave it in neither.
//
// KEYS[1] stream, KEYS[2] retry zset
// ARGV[1] group, ARGV[2] message id, ARGV[3] run-at ms, ARGV[4] job id
var scheduleRetryScript = redis.NewScript(`
redis.call('ZADD', KEYS[2], ARGV[3], ARGV[4])
redis.call('XACK', KEYS[1], ARGV[1], ARGV[2])
redis.call('XDEL', KEYS[1], ARGV[2])
return 1
`)

// promoteRetries moves due job IDs from the retry set back onto the stream.
// ZREM guards the XADD, so two schedulers racing never enqueue twice.
//
// KEYS[1] retry zset, KEYS[2] stream; ARGV job ids
var promoteRetriesScript = redis.NewScript(`
local moved = 0
for _, id in ipairs(ARGV) do
  if redis.call('ZREM', KEYS[1], id) == 1 then
    redis.call('XADD', KEYS[2], '*', 'job_id', id)
    moved = moved + 1
  end
end
return moved
`)

// extendLease resets a message's idle time, but only if this consumer still
// owns it. Used as a heartbeat for long jobs so XAUTOCLAIM leaves them alone.
//
// KEYS[1] stream; ARGV[1] group, ARGV[2] consumer, ARGV[3] message id
var extendLeaseScript = redis.NewScript(`
local p = redis.call('XPENDING', KEYS[1], ARGV[1], ARGV[3], ARGV[3], 1)
if #p == 0 or p[1][2] ~= ARGV[2] then
  return 0
end
redis.call('XCLAIM', KEYS[1], ARGV[1], ARGV[2], 0, ARGV[3], 'JUSTID')
return 1
`)
