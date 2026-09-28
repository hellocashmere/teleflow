package storage

import (
	"bytes"
	"context"
	"errors"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"
)

func TestNewMemory(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		new  func() Storage
	}{
		{
			name: "constructor",
			new:  NewMemory,
		},
		{
			name: "zero_value",
			new: func() Storage {
				return &Memory{}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			store := tt.new()
			if store == nil {
				t.Fatal("storage is nil")
			}
			if err := store.Set(t.Context(), "user", []byte("state"), time.Minute); err != nil {
				t.Fatalf("Set() error = %v", err)
			}
			got, err := store.Get(t.Context(), "user")
			if err != nil {
				t.Fatalf("Get() error = %v", err)
			}
			if !bytes.Equal(got, []byte("state")) {
				t.Fatalf("Get() = %q, want %q", got, "state")
			}
		})
	}
}

func TestGet(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		stored     bool
		value      []byte
		expiration time.Duration
		advance    time.Duration
		canceled   bool
		mutate     bool
		want       []byte
		wantErr    error
	}{
		{
			name:   "existing_value",
			stored: true,
			value:  []byte("state"),
			want:   []byte("state"),
		},
		{
			name:   "empty_value",
			stored: true,
			value:  []byte{},
			want:   []byte{},
		},
		{
			name:   "nil_value",
			stored: true,
			value:  nil,
			want:   nil,
		},
		{
			name:    "missing_key",
			wantErr: ErrKeyNotFound,
		},
		{
			name:       "before_expiration",
			stored:     true,
			value:      []byte("state"),
			expiration: time.Minute,
			advance:    time.Minute - time.Nanosecond,
			want:       []byte("state"),
		},
		{
			name:       "at_expiration",
			stored:     true,
			value:      []byte("state"),
			expiration: time.Minute,
			advance:    time.Minute,
			wantErr:    ErrKeyNotFound,
		},
		{
			name:       "after_expiration",
			stored:     true,
			value:      []byte("state"),
			expiration: time.Minute,
			advance:    2 * time.Minute,
			wantErr:    ErrKeyNotFound,
		},
		{
			name:   "returns_a_copy",
			stored: true,
			value:  []byte("state"),
			mutate: true,
			want:   []byte("state"),
		},
		{
			name:     "canceled_context",
			stored:   true,
			value:    []byte("state"),
			canceled: true,
			wantErr:  context.Canceled,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				store := &Memory{}
				if tt.stored {
					if err := store.Set(t.Context(), "user", tt.value, tt.expiration); err != nil {
						t.Fatalf("Set() error = %v", err)
					}
				}
				if tt.advance > 0 {
					time.Sleep(tt.advance)
					synctest.Wait()
				}

				ctx := t.Context()
				if tt.canceled {
					var cancel context.CancelFunc
					ctx, cancel = context.WithCancel(ctx)
					cancel()
				}
				got, err := store.Get(ctx, "user")
				if !errors.Is(err, tt.wantErr) {
					t.Fatalf("Get() error = %v, want %v", err, tt.wantErr)
				}
				if tt.wantErr != nil {
					return
				}
				if !bytes.Equal(got, tt.want) {
					t.Fatalf("Get() = %q, want %q", got, tt.want)
				}

				if tt.mutate {
					got[0] = 'X'
					again, err := store.Get(t.Context(), "user")
					if err != nil {
						t.Fatalf("second Get() error = %v", err)
					}
					if !bytes.Equal(again, tt.want) {
						t.Fatalf("second Get() = %q, want %q", again, tt.want)
					}
				}
			})
		})
	}
}

func TestSet(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		key         string
		value       []byte
		expiration  time.Duration
		hasPrevious bool
		previousTTL time.Duration
		beforeSet   time.Duration
		advance     time.Duration
		mutateInput bool
		canceled    bool
		want        []byte
		wantSetErr  error
		wantGetErr  error
	}{
		{
			name:  "value",
			key:   "user",
			value: []byte("state"),
			want:  []byte("state"),
		},
		{
			name:  "empty_value",
			key:   "user",
			value: []byte{},
			want:  []byte{},
		},
		{
			name:  "nil_value",
			key:   "user",
			value: nil,
			want:  nil,
		},
		{
			name:  "empty_key",
			key:   "",
			value: []byte("state"),
			want:  []byte("state"),
		},
		{
			name:        "copies_input",
			key:         "user",
			value:       []byte("state"),
			mutateInput: true,
			want:        []byte("state"),
		},
		{
			name:       "positive_expiration",
			key:        "user",
			value:      []byte("state"),
			expiration: time.Minute,
			advance:    time.Minute,
			wantGetErr: ErrKeyNotFound,
		},
		{
			name:    "zero_expiration",
			key:     "user",
			value:   []byte("state"),
			advance: 24 * time.Hour,
			want:    []byte("state"),
		},
		{
			name:       "negative_expiration",
			key:        "user",
			value:      []byte("state"),
			expiration: -time.Minute,
			advance:    24 * time.Hour,
			want:       []byte("state"),
		},
		{
			name:        "overwrite_removes_expiration",
			key:         "user",
			value:       []byte("new"),
			hasPrevious: true,
			previousTTL: time.Minute,
			beforeSet:   30 * time.Second,
			advance:     2 * time.Minute,
			want:        []byte("new"),
		},
		{
			name:        "overwrite_renews_expiration",
			key:         "user",
			value:       []byte("new"),
			expiration:  time.Minute,
			hasPrevious: true,
			previousTTL: time.Minute,
			beforeSet:   30 * time.Second,
			advance:     45 * time.Second,
			want:        []byte("new"),
		},
		{
			name:        "overwrite_applies_expiration",
			key:         "user",
			value:       []byte("new"),
			expiration:  time.Minute,
			hasPrevious: true,
			previousTTL: time.Minute,
			beforeSet:   30 * time.Second,
			advance:     time.Minute,
			wantGetErr:  ErrKeyNotFound,
		},
		{
			name:        "canceled_context_keeps_previous_value",
			key:         "user",
			value:       []byte("new"),
			hasPrevious: true,
			canceled:    true,
			want:        []byte("old"),
			wantSetErr:  context.Canceled,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				store := &Memory{}
				if tt.hasPrevious {
					if err := store.Set(t.Context(), tt.key, []byte("old"), tt.previousTTL); err != nil {
						t.Fatalf("prepare Set() error = %v", err)
					}
				}
				if tt.beforeSet > 0 {
					time.Sleep(tt.beforeSet)
					synctest.Wait()
				}
				ctx := t.Context()
				if tt.canceled {
					var cancel context.CancelFunc
					ctx, cancel = context.WithCancel(ctx)
					cancel()
				}

				if err := store.Set(ctx, tt.key, tt.value, tt.expiration); !errors.Is(err, tt.wantSetErr) {
					t.Fatalf("Set() error = %v, want %v", err, tt.wantSetErr)
				}
				if tt.mutateInput {
					tt.value[0] = 'X'
				}
				if tt.advance > 0 {
					time.Sleep(tt.advance)
					synctest.Wait()
				}

				got, err := store.Get(t.Context(), tt.key)
				if !errors.Is(err, tt.wantGetErr) {
					t.Fatalf("Get() error = %v, want %v", err, tt.wantGetErr)
				}
				if tt.wantGetErr == nil && !bytes.Equal(got, tt.want) {
					t.Fatalf("Get() = %q, want %q", got, tt.want)
				}
			})
		})
	}
}

func TestDelete(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		stored     bool
		canceled   bool
		wantErr    error
		want       []byte
		wantGetErr error
	}{
		{
			name:       "existing_key",
			stored:     true,
			wantGetErr: ErrKeyNotFound,
		},
		{
			name:       "missing_key",
			wantGetErr: ErrKeyNotFound,
		},
		{
			name:     "canceled_context_keeps_value",
			stored:   true,
			canceled: true,
			wantErr:  context.Canceled,
			want:     []byte("state"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			store := NewMemory()
			if tt.stored {
				if err := store.Set(t.Context(), "user", []byte("state"), 0); err != nil {
					t.Fatalf("Set() error = %v", err)
				}
			}
			ctx := t.Context()
			if tt.canceled {
				var cancel context.CancelFunc
				ctx, cancel = context.WithCancel(ctx)
				cancel()
			}

			if err := store.Delete(ctx, "user"); !errors.Is(err, tt.wantErr) {
				t.Fatalf("Delete() error = %v, want %v", err, tt.wantErr)
			}
			got, err := store.Get(t.Context(), "user")
			if !errors.Is(err, tt.wantGetErr) {
				t.Fatalf("Get() error = %v, want %v", err, tt.wantGetErr)
			}
			if tt.wantGetErr == nil && !bytes.Equal(got, tt.want) {
				t.Fatalf("Get() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestCompareAndSwap(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		stored      bool
		storedValue []byte
		storedTTL   time.Duration
		beforeSwap  time.Duration
		expected    []byte
		value       []byte
		expiration  time.Duration
		afterSwap   time.Duration
		canceled    bool
		mutateInput bool
		wantSwapped bool
		wantSwapErr error
		want        []byte
		wantGetErr  error
	}{
		{
			name:        "creates_missing_key",
			value:       []byte("new"),
			wantSwapped: true,
			want:        []byte("new"),
		},
		{
			name:       "does_not_create_with_expected_value",
			expected:   []byte("old"),
			value:      []byte("new"),
			wantGetErr: ErrKeyNotFound,
		},
		{
			name:        "does_not_replace_when_expected_is_nil",
			stored:      true,
			storedValue: []byte("old"),
			value:       []byte("new"),
			want:        []byte("old"),
		},
		{
			name:        "replaces_matching_value",
			stored:      true,
			storedValue: []byte("old"),
			expected:    []byte("old"),
			value:       []byte("new"),
			wantSwapped: true,
			want:        []byte("new"),
		},
		{
			name:        "keeps_mismatched_value",
			stored:      true,
			storedValue: []byte("old"),
			expected:    []byte("other"),
			value:       []byte("new"),
			want:        []byte("old"),
		},
		{
			name:        "matches_empty_value",
			stored:      true,
			storedValue: []byte{},
			expected:    []byte{},
			value:       []byte("new"),
			wantSwapped: true,
			want:        []byte("new"),
		},
		{
			name:        "copies_input",
			value:       []byte("new"),
			mutateInput: true,
			wantSwapped: true,
			want:        []byte("new"),
		},
		{
			name:        "expired_value_is_missing",
			stored:      true,
			storedValue: []byte("old"),
			storedTTL:   time.Minute,
			beforeSwap:  time.Minute,
			value:       []byte("new"),
			wantSwapped: true,
			want:        []byte("new"),
		},
		{
			name:        "replacement_removes_expiration",
			stored:      true,
			storedValue: []byte("old"),
			storedTTL:   time.Minute,
			beforeSwap:  30 * time.Second,
			expected:    []byte("old"),
			value:       []byte("new"),
			afterSwap:   time.Minute,
			wantSwapped: true,
			want:        []byte("new"),
		},
		{
			name:        "replacement_applies_expiration",
			stored:      true,
			storedValue: []byte("old"),
			expected:    []byte("old"),
			value:       []byte("new"),
			expiration:  time.Minute,
			afterSwap:   time.Minute,
			wantSwapped: true,
			wantGetErr:  ErrKeyNotFound,
		},
		{
			name:        "replacement_renews_expiration",
			stored:      true,
			storedValue: []byte("old"),
			storedTTL:   time.Minute,
			beforeSwap:  30 * time.Second,
			expected:    []byte("old"),
			value:       []byte("new"),
			expiration:  time.Minute,
			afterSwap:   45 * time.Second,
			wantSwapped: true,
			want:        []byte("new"),
		},
		{
			name:        "mismatch_preserves_expiration",
			stored:      true,
			storedValue: []byte("old"),
			storedTTL:   time.Minute,
			beforeSwap:  30 * time.Second,
			expected:    []byte("other"),
			value:       []byte("new"),
			afterSwap:   30 * time.Second,
			wantGetErr:  ErrKeyNotFound,
		},
		{
			name:        "canceled_context_keeps_value",
			stored:      true,
			storedValue: []byte("old"),
			expected:    []byte("old"),
			value:       []byte("new"),
			canceled:    true,
			wantSwapErr: context.Canceled,
			want:        []byte("old"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			synctest.Test(t, func(t *testing.T) {
				store := &Memory{}
				if tt.stored {
					if err := store.Set(t.Context(), "user", tt.storedValue, tt.storedTTL); err != nil {
						t.Fatalf("prepare Set() error = %v", err)
					}
				}

				if tt.beforeSwap > 0 {
					time.Sleep(tt.beforeSwap)
					synctest.Wait()
				}

				ctx := t.Context()
				if tt.canceled {
					var cancel context.CancelFunc
					ctx, cancel = context.WithCancel(ctx)
					cancel()
				}

				swapped, err := store.CompareAndSwap(
					ctx,
					"user",
					tt.expected,
					tt.value,
					tt.expiration,
				)
				if !errors.Is(err, tt.wantSwapErr) {
					t.Fatalf("CompareAndSwap() error = %v, want %v", err, tt.wantSwapErr)
				}
				if swapped != tt.wantSwapped {
					t.Fatalf("CompareAndSwap() swapped = %t, want %t", swapped, tt.wantSwapped)
				}

				if tt.mutateInput {
					tt.value[0] = 'X'
				}
				if tt.afterSwap > 0 {
					time.Sleep(tt.afterSwap)
					synctest.Wait()
				}

				got, err := store.Get(t.Context(), "user")
				if !errors.Is(err, tt.wantGetErr) {
					t.Fatalf("Get() error = %v, want %v", err, tt.wantGetErr)
				}
				if tt.wantGetErr == nil && !bytes.Equal(got, tt.want) {
					t.Fatalf("Get() = %q, want %q", got, tt.want)
				}
			})
		})
	}
}

func TestCompareAndSwapConcurrentWriters(t *testing.T) {
	tests := []struct {
		name string
		test func(t *testing.T)
	}{
		{
			name: "compare_and_swap_concurrent_writers",
			test: func(t *testing.T) {
				t.Parallel()

				const writers = 32

				store := NewMemory()
				if err := store.Set(t.Context(), "user", []byte("old"), 0); err != nil {
					t.Fatalf("prepare Set() error = %v", err)
				}

				start := make(chan struct{})
				var ready sync.WaitGroup
				var done sync.WaitGroup
				var winners atomic.Int32

				ready.Add(writers)
				done.Add(writers)
				for i := range writers {
					go func() {
						defer done.Done()
						ready.Done()
						<-start

						swapped, err := store.CompareAndSwap(
							t.Context(),
							"user",
							[]byte("old"),
							[]byte(strconv.Itoa(i)),
							0,
						)
						if err != nil {
							t.Errorf("CompareAndSwap() error = %v", err)
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

func TestCompareAndDelete(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		stored      bool
		storedValue []byte
		storedTTL   time.Duration
		advance     time.Duration
		expected    []byte
		canceled    bool
		wantDeleted bool
		wantErr     error
		want        []byte
		wantGetErr  error
	}{
		{
			name:        "deletes_matching_value",
			stored:      true,
			storedValue: []byte("old"),
			expected:    []byte("old"),
			wantDeleted: true,
			wantGetErr:  ErrKeyNotFound,
		},
		{
			name:        "keeps_mismatched_value",
			stored:      true,
			storedValue: []byte("old"),
			expected:    []byte("other"),
			want:        []byte("old"),
		},
		{
			name:       "missing_key",
			expected:   []byte("old"),
			wantGetErr: ErrKeyNotFound,
		},
		{
			name:        "expired_key",
			stored:      true,
			storedValue: []byte("old"),
			storedTTL:   time.Minute,
			advance:     time.Minute,
			expected:    []byte("old"),
			wantGetErr:  ErrKeyNotFound,
		},
		{
			name:        "deletes_empty_value",
			stored:      true,
			storedValue: []byte{},
			expected:    []byte{},
			wantDeleted: true,
			wantGetErr:  ErrKeyNotFound,
		},
		{
			name:        "canceled_context_keeps_value",
			stored:      true,
			storedValue: []byte("old"),
			expected:    []byte("old"),
			canceled:    true,
			wantErr:     context.Canceled,
			want:        []byte("old"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			synctest.Test(t, func(t *testing.T) {
				store := &Memory{}
				if tt.stored {
					if err := store.Set(t.Context(), "user", tt.storedValue, tt.storedTTL); err != nil {
						t.Fatalf("prepare Set() error = %v", err)
					}
				}

				if tt.advance > 0 {
					time.Sleep(tt.advance)
					synctest.Wait()
				}

				ctx := t.Context()
				if tt.canceled {
					var cancel context.CancelFunc
					ctx, cancel = context.WithCancel(ctx)
					cancel()
				}

				deleted, err := store.CompareAndDelete(ctx, "user", tt.expected)
				if !errors.Is(err, tt.wantErr) {
					t.Fatalf("CompareAndDelete() error = %v, want %v", err, tt.wantErr)
				}
				if deleted != tt.wantDeleted {
					t.Fatalf("CompareAndDelete() deleted = %t, want %t", deleted, tt.wantDeleted)
				}

				got, err := store.Get(t.Context(), "user")
				if !errors.Is(err, tt.wantGetErr) {
					t.Fatalf("Get() error = %v, want %v", err, tt.wantGetErr)
				}
				if tt.wantGetErr == nil && !bytes.Equal(got, tt.want) {
					t.Fatalf("Get() = %q, want %q", got, tt.want)
				}
			})
		})
	}
}

func TestCompareAndDeleteConcurrentCallers(t *testing.T) {
	tests := []struct {
		name string
		test func(t *testing.T)
	}{
		{
			name: "compare_and_delete_concurrent_callers",
			test: func(t *testing.T) {
				t.Parallel()

				const callers = 32

				store := NewMemory()
				if err := store.Set(t.Context(), "user", []byte("old"), 0); err != nil {
					t.Fatalf("prepare Set() error = %v", err)
				}

				start := make(chan struct{})
				var ready sync.WaitGroup
				var done sync.WaitGroup
				var winners atomic.Int32

				ready.Add(callers)
				done.Add(callers)
				for range callers {
					go func() {
						defer done.Done()
						ready.Done()
						<-start

						deleted, err := store.CompareAndDelete(t.Context(), "user", []byte("old"))
						if err != nil {
							t.Errorf("CompareAndDelete() error = %v", err)
							return
						}
						if deleted {
							winners.Add(1)
						}
					}()
				}

				ready.Wait()
				close(start)
				done.Wait()

				if got := winners.Load(); got != 1 {
					t.Fatalf("successful CompareAndDelete() calls = %d, want 1", got)
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, tt.test)
	}
}
