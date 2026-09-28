//go:build integration

package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hellocashmere/teleflow/storage"
	"github.com/redis/go-redis/v9"
)

func TestCompareAndSwapAndDelete(t *testing.T) {
	tests := []struct {
		name string
		test func(t *testing.T)
	}{
		{
			name: "compare_and_swap_and_delete",
			test: func(t *testing.T) {
				store, _ := newRedisStorageForTest(t)

				swapped, err := store.CompareAndSwap(t.Context(), "user", nil, []byte("old"), 0)
				if err != nil {
					t.Fatalf("create CompareAndSwap() error = %v", err)
				}
				if !swapped {
					t.Fatal("create CompareAndSwap() swapped = false, want true")
				}

				swapped, err = store.CompareAndSwap(t.Context(), "user", nil, []byte("other"), 0)
				if err != nil {
					t.Fatalf("second create CompareAndSwap() error = %v", err)
				}
				if swapped {
					t.Fatal("second create CompareAndSwap() swapped = true, want false")
				}

				swapped, err = store.CompareAndSwap(t.Context(), "user", []byte("other"), []byte("new"), 0)
				if err != nil {
					t.Fatalf("mismatched CompareAndSwap() error = %v", err)
				}
				if swapped {
					t.Fatal("mismatched CompareAndSwap() swapped = true, want false")
				}

				swapped, err = store.CompareAndSwap(t.Context(), "user", []byte("old"), []byte("new"), 0)
				if err != nil {
					t.Fatalf("matching CompareAndSwap() error = %v", err)
				}
				if !swapped {
					t.Fatal("matching CompareAndSwap() swapped = false, want true")
				}

				got, err := store.Get(t.Context(), "user")
				if err != nil {
					t.Fatalf("Get() error = %v", err)
				}
				if !bytes.Equal(got, []byte("new")) {
					t.Fatalf("Get() = %q, want %q", got, "new")
				}

				deleted, err := store.CompareAndDelete(t.Context(), "user", []byte("old"))
				if err != nil {
					t.Fatalf("mismatched CompareAndDelete() error = %v", err)
				}
				if deleted {
					t.Fatal("mismatched CompareAndDelete() deleted = true, want false")
				}

				deleted, err = store.CompareAndDelete(t.Context(), "user", []byte("new"))
				if err != nil {
					t.Fatalf("matching CompareAndDelete() error = %v", err)
				}
				if !deleted {
					t.Fatal("matching CompareAndDelete() deleted = false, want true")
				}
				if _, err := store.Get(t.Context(), "user"); !errors.Is(err, storage.ErrKeyNotFound) {
					t.Fatalf("Get() after delete error = %v, want %v", err, storage.ErrKeyNotFound)
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, tt.test)
	}
}

func TestEmptyValueIsNotMissing(t *testing.T) {
	tests := []struct {
		name string
		test func(t *testing.T)
	}{
		{
			name: "empty_value_is_not_missing",
			test: func(t *testing.T) {
				store, _ := newRedisStorageForTest(t)

				if err := store.Set(t.Context(), "user", []byte{}, 0); err != nil {
					t.Fatalf("Set() error = %v", err)
				}
				swapped, err := store.CompareAndSwap(t.Context(), "user", nil, []byte("new"), 0)
				if err != nil {
					t.Fatalf("nil expected CompareAndSwap() error = %v", err)
				}
				if swapped {
					t.Fatal("nil expected CompareAndSwap() swapped = true, want false")
				}

				swapped, err = store.CompareAndSwap(t.Context(), "user", []byte{}, []byte("new"), 0)
				if err != nil {
					t.Fatalf("empty expected CompareAndSwap() error = %v", err)
				}
				if !swapped {
					t.Fatal("empty expected CompareAndSwap() swapped = false, want true")
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, tt.test)
	}
}

func TestCompareAndSwapExpiration(t *testing.T) {
	tests := []struct {
		name string
		test func(t *testing.T)
	}{
		{
			name: "compare_and_swap_expiration",
			test: func(t *testing.T) {
				store, client := newRedisStorageForTest(t)

				swapped, err := store.CompareAndSwap(t.Context(), "user", nil, []byte("old"), 2*time.Second)
				if err != nil {
					t.Fatalf("expiring CompareAndSwap() error = %v", err)
				}
				if !swapped {
					t.Fatal("expiring CompareAndSwap() swapped = false, want true")
				}

				ttl, err := client.PTTL(t.Context(), store.key("user")).Result()
				if err != nil {
					t.Fatalf("PTTL() error = %v", err)
				}
				if ttl <= 0 || ttl > 2*time.Second {
					t.Fatalf("PTTL() = %v, want positive value up to 2s", ttl)
				}

				swapped, err = store.CompareAndSwap(t.Context(), "user", []byte("old"), []byte("new"), 0)
				if err != nil {
					t.Fatalf("persistent CompareAndSwap() error = %v", err)
				}
				if !swapped {
					t.Fatal("persistent CompareAndSwap() swapped = false, want true")
				}

				ttl, err = client.PTTL(t.Context(), store.key("user")).Result()
				if err != nil {
					t.Fatalf("persistent PTTL() error = %v", err)
				}
				if ttl != -time.Nanosecond {
					t.Fatalf("persistent PTTL() = %v, want %v", ttl, -time.Nanosecond)
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, tt.test)
	}
}

func TestCanceledContextDoesNotMutate(t *testing.T) {
	tests := []struct {
		name string
		test func(t *testing.T)
	}{
		{
			name: "canceled_context_does_not_mutate",
			test: func(t *testing.T) {
				store, _ := newRedisStorageForTest(t)
				if err := store.Set(t.Context(), "user", []byte("old"), 0); err != nil {
					t.Fatalf("Set() error = %v", err)
				}

				ctx, cancel := context.WithCancel(t.Context())
				cancel()
				swapped, err := store.CompareAndSwap(ctx, "user", []byte("old"), []byte("new"), 0)
				if !errors.Is(err, context.Canceled) {
					t.Fatalf("CompareAndSwap() error = %v, want %v", err, context.Canceled)
				}
				if swapped {
					t.Fatal("CompareAndSwap() swapped = true, want false")
				}

				got, err := store.Get(t.Context(), "user")
				if err != nil {
					t.Fatalf("Get() error = %v", err)
				}
				if !bytes.Equal(got, []byte("old")) {
					t.Fatalf("Get() = %q, want %q", got, "old")
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, tt.test)
	}
}

func TestConcurrentWritersHaveOneWinner(t *testing.T) {
	tests := []struct {
		name string
		test func(t *testing.T)
	}{
		{
			name: "concurrent_writers_have_one_winner",
			test: func(t *testing.T) {
				firstStore, firstClient := newRedisStorageForTest(t)
				secondClient := redis.NewClient(firstClient.Options())
				t.Cleanup(func() {
					_ = secondClient.Close()
				})
				secondStore := newRedisStorage(secondClient, firstStore.prefix)

				if err := firstStore.Set(t.Context(), "user", []byte("old"), 0); err != nil {
					t.Fatalf("Set() error = %v", err)
				}

				const writers = 32
				start := make(chan struct{})
				var ready sync.WaitGroup
				var done sync.WaitGroup
				var winners atomic.Int32
				errCh := make(chan error, writers)

				ready.Add(writers)
				done.Add(writers)
				for i := range writers {
					go func() {
						defer done.Done()
						ready.Done()
						<-start

						store := firstStore
						if i%2 == 1 {
							store = secondStore
						}
						swapped, err := store.CompareAndSwap(
							t.Context(),
							"user",
							[]byte("old"),
							[]byte(fmt.Sprintf("writer_%d", i)),
							0,
						)
						if err != nil {
							errCh <- err
							return
						}
						if swapped {
							winners.Add(1)
						}
					}()
				}

				ready.Wait()
				close(start)
				done.Wait()
				close(errCh)

				for err := range errCh {
					t.Errorf("CompareAndSwap() error = %v", err)
				}
				if got := winners.Load(); got != 1 {
					t.Fatalf("successful CompareAndSwap() calls = %d, want 1", got)
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, tt.test)
	}
}

func newRedisStorageForTest(t *testing.T) (*redisStorage, *redis.Client) {
	t.Helper()

	options := redisOptionsForTest(t)
	client := redis.NewClient(options)
	if err := client.Ping(t.Context()).Err(); err != nil {
		_ = client.Close()
		t.Skipf("Redis is unavailable: %v", err)
	}

	prefix := fmt.Sprintf("teleflow:integration:%d:", time.Now().UnixNano())
	store := newRedisStorage(client, prefix)
	t.Cleanup(func() {
		iterator := client.Scan(context.Background(), 0, prefix+"*", 0).Iterator()
		keys := []string{}
		for iterator.Next(context.Background()) {
			keys = append(keys, iterator.Val())
		}
		if len(keys) > 0 {
			_ = client.Del(context.Background(), keys...).Err()
		}
		_ = client.Close()
	})

	return store, client
}

func redisOptionsForTest(t *testing.T) *redis.Options {
	t.Helper()

	if rawURL := os.Getenv("REDIS_URL"); rawURL != "" {
		options, err := redis.ParseURL(rawURL)
		if err != nil {
			t.Fatalf("redis.ParseURL() error = %v", err)
		}
		return options
	}

	if address := os.Getenv("REDIS_ADDR"); address != "" {
		return &redis.Options{Addr: address}
	}

	t.Skip("set REDIS_URL or REDIS_ADDR to run Redis integration tests")
	return nil
}
