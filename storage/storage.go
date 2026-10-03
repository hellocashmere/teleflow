package storage

import (
	"context"
	"time"
)

// Storage persists opaque session state and must be safe for concurrent use.
//
// Implementations do not need compare-and-swap or distributed locking because a Bus serializes updates locally.
type Storage interface {
	// Get returns the value associated with key.
	//
	// A missing or expired key must return an error that matches ErrKeyNotFound with errors.Is.
	Get(ctx context.Context, key string) ([]byte, error)

	// Set stores value under key and replaces any existing value.
	//
	// A positive expiration sets a time to live, while a zero or negative expiration keeps the value until it is replaced or deleted.
	Set(ctx context.Context, key string, value []byte, expiration time.Duration) error

	// Delete removes key and returns nil when it is absent.
	Delete(ctx context.Context, key string) error
}
