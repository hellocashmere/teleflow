package redis

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/hellocashmere/teleflow/storage"
	redissdk "github.com/redis/go-redis/v9"
)

const defaultPingTimeout = 10 * time.Second

// API is the Redis client contract used by Store.
type API interface {
	Get(ctx context.Context, key string) *redissdk.StringCmd
	Set(ctx context.Context, key string, value any, expiration time.Duration) *redissdk.StatusCmd
	Del(ctx context.Context, keys ...string) *redissdk.IntCmd
	Ping(ctx context.Context) *redissdk.StatusCmd
	Close() error
}

// Store implements storage.Storage with Redis.
type Store struct {
	api    API
	prefix string
}

// New creates a Redis client and verifies it with PING.
//
// The supplied prefix is prepended to every storage key, and the initial PING is bounded by ten seconds and the deadline of ctx.
func New(ctx context.Context, options *redissdk.Options, prefix string) (*Store, error) {
	if options == nil {
		return nil, fmt.Errorf("redis: options are required")
	}

	client := redissdk.NewClient(options)

	pingCtx, cancel := context.WithTimeout(ctx, defaultPingTimeout)
	defer cancel()

	if err := client.Ping(pingCtx).Err(); err != nil {
		pingErr := fmt.Errorf("redis: ping: %w", err)
		if closeErr := client.Close(); closeErr != nil {
			return nil, errors.Join(
				pingErr,
				fmt.Errorf("redis: close after ping: %w", closeErr),
			)
		}

		return nil, pingErr
	}

	return &Store{
		api:    client,
		prefix: prefix,
	}, nil
}

// Close closes the Redis client.
//
// It is safe to call on a nil or uninitialized Store.
func (s *Store) Close() error {
	if s == nil || s.api == nil {
		return nil
	}

	if err := s.api.Close(); err != nil {
		return fmt.Errorf("redis: close: %w", err)
	}

	return nil
}

// Ping verifies the Redis connection.
func (s *Store) Ping(ctx context.Context) error {
	if err := s.ready(); err != nil {
		return err
	}

	if err := s.api.Ping(ctx).Err(); err != nil {
		return fmt.Errorf("redis: ping: %w", err)
	}

	return nil
}

// Get returns the value associated with key.
//
// It maps a missing Redis key to storage.ErrKeyNotFound.
func (s *Store) Get(ctx context.Context, key string) ([]byte, error) {
	if err := s.ready(); err != nil {
		return nil, err
	}

	value, err := s.api.Get(ctx, s.key(key)).Bytes()
	if errors.Is(err, redissdk.Nil) {
		return nil, storage.ErrKeyNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("redis: get: %w", err)
	}

	return value, nil
}

// Set stores value under key and replaces any existing value.
//
// A zero or negative expiration disables expiry, while a positive expiration below one millisecond is rounded up to one millisecond.
func (s *Store) Set(ctx context.Context, key string, value []byte, expiration time.Duration) error {
	if err := s.ready(); err != nil {
		return err
	}

	err := s.api.Set(ctx, s.key(key), value, redisExpiration(expiration)).Err()
	if err != nil {
		return fmt.Errorf("redis: set: %w", err)
	}

	return nil
}

// Delete removes key and returns nil when it is absent.
func (s *Store) Delete(ctx context.Context, key string) error {
	if err := s.ready(); err != nil {
		return err
	}

	if err := s.api.Del(ctx, s.key(key)).Err(); err != nil {
		return fmt.Errorf("redis: delete: %w", err)
	}

	return nil
}

func (s *Store) ready() error {
	if s == nil || s.api == nil {
		return fmt.Errorf("redis: store is nil")
	}

	return nil
}

func (s *Store) key(key string) string {
	return s.prefix + key
}

func redisExpiration(expiration time.Duration) time.Duration {
	if expiration <= 0 {
		return 0
	}
	if expiration < time.Millisecond {
		return time.Millisecond
	}

	return expiration
}
