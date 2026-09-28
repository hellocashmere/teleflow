package main

import (
	"context"
	"errors"
	"time"

	"github.com/hellocashmere/teleflow/storage"
	"github.com/redis/go-redis/v9"
)

// redisStorage is an application-owned adapter. Teleflow itself does not
// depend on go-redis and only sees the storage.Storage interface.
type redisStorage struct {
	client *redis.Client
	prefix string
}

var _ storage.Storage = (*redisStorage)(nil)

var compareAndSwapScript = redis.NewScript(`
local current = redis.call("GET", KEYS[1])
if ARGV[1] == "0" then
  if current then return 0 end
elseif not current or current ~= ARGV[2] then
  return 0
end
local ttl = tonumber(ARGV[4])
if ttl > 0 then
  redis.call("SET", KEYS[1], ARGV[3], "PX", ttl)
else
  redis.call("SET", KEYS[1], ARGV[3])
end
return 1
`)

var compareAndDeleteScript = redis.NewScript(`
local current = redis.call("GET", KEYS[1])
if not current or current ~= ARGV[1] then return 0 end
redis.call("DEL", KEYS[1])
return 1
`)

func newRedisStorage(client *redis.Client, prefix string) *redisStorage {
	return &redisStorage{client: client, prefix: prefix}
}

func (s *redisStorage) Get(ctx context.Context, key string) ([]byte, error) {
	value, err := s.client.Get(ctx, s.key(key)).Bytes()
	if errors.Is(err, redis.Nil) {
		return nil, storage.ErrKeyNotFound
	}
	return value, err
}

func (s *redisStorage) Set(ctx context.Context, key string, value []byte, expiration time.Duration) error {
	return s.client.Set(ctx, s.key(key), value, expiration).Err()
}

func (s *redisStorage) Delete(ctx context.Context, key string) error {
	return s.client.Del(ctx, s.key(key)).Err()
}

func (s *redisStorage) CompareAndSwap(
	ctx context.Context,
	key string,
	expected []byte,
	value []byte,
	expiration time.Duration,
) (bool, error) {
	expectedExists := "1"
	expectedValue := expected
	if expected == nil {
		expectedExists = "0"
		expectedValue = []byte{}
	}
	ttl := expiration.Milliseconds()
	if expiration > 0 && ttl == 0 {
		ttl = 1
	}

	result, err := compareAndSwapScript.Run(
		ctx,
		s.client,
		[]string{s.key(key)},
		expectedExists,
		expectedValue,
		value,
		ttl,
	).Int()
	return result == 1, err
}

func (s *redisStorage) CompareAndDelete(ctx context.Context, key string, expected []byte) (bool, error) {
	result, err := compareAndDeleteScript.Run(
		ctx,
		s.client,
		[]string{s.key(key)},
		expected,
	).Int()
	return result == 1, err
}

func (s *redisStorage) key(key string) string {
	return s.prefix + key
}
