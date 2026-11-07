package scheduler

import "github.com/redis/go-redis/v9"

func redisZ(member string) redis.Z { return redis.Z{Score: 0, Member: member} }
