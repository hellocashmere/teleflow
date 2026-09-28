package storage

import (
	"context"
	"time"
)

type Storage interface {
	// Get returns a value or ErrKeyNotFound.
	Get(ctx context.Context, key string) ([]byte, error)

	// Set stores a value with optional expiration.
	Set(ctx context.Context, key string, value []byte, expiration time.Duration) error

	// Delete removes a value and ignores missing keys.
	Delete(ctx context.Context, key string) error

	// CompareAndSwap replaces expected or creates a missing key when expected is nil.
	CompareAndSwap(
		ctx context.Context,
		key string,
		expected []byte,
		value []byte,
		expiration time.Duration,
	) (bool, error)

	// CompareAndDelete removes a key only when its value matches expected.
	CompareAndDelete(ctx context.Context, key string, expected []byte) (bool, error)
}
