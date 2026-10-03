package storage

import (
	"bytes"
	"context"
	"errors"
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
