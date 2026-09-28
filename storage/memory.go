package storage

import (
	"bytes"
	"context"
	"errors"
	"sync"
	"time"
)

var (
	ErrKeyNotFound = errors.New("key not found")
)

// Memory stores values in process memory.
type Memory struct {
	data sync.Map
}

type raw struct {
	value     []byte
	expiresAt time.Time
	timerMu   sync.Mutex
	timer     *time.Timer
	isRemoved bool
}

var _ Storage = (*Memory)(nil)

// NewMemory creates in-memory storage.
func NewMemory() Storage {
	return &Memory{}
}

// Get returns a copied value by key.
func (m *Memory) Get(ctx context.Context, key string) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	value, ok := m.data.Load(key)
	if !ok {
		return nil, ErrKeyNotFound
	}

	v := value.(*raw)
	if m.isExpired(v) {
		if m.data.CompareAndDelete(key, v) {
			v.stopTimer()
		}
		return nil, ErrKeyNotFound
	}

	return append([]byte(nil), v.value...), nil
}

// Set stores a copy of value and applies a positive expiration.
func (m *Memory) Set(ctx context.Context, key string, value []byte, expiration time.Duration) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	v := m.newRaw(value, expiration)
	previous, replaced := m.data.Swap(key, v)
	v.startTimer(m, key, expiration)
	if replaced {
		previous.(*raw).stopTimer()
	}
	return nil
}

// Delete removes a value by key.
func (m *Memory) Delete(ctx context.Context, key string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if value, ok := m.data.LoadAndDelete(key); ok {
		value.(*raw).stopTimer()
	}
	return nil
}

// CompareAndSwap conditionally stores a copied value.
func (m *Memory) CompareAndSwap(
	ctx context.Context,
	key string,
	expected []byte,
	value []byte,
	expiration time.Duration,
) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}

	for {
		current, exists := m.data.Load(key)
		if !exists {
			if expected != nil {
				return false, nil
			}

			next := m.newRaw(value, expiration)
			if _, loaded := m.data.LoadOrStore(key, next); loaded {
				continue
			}
			next.startTimer(m, key, expiration)
			return true, nil
		}

		stored := current.(*raw)
		if m.isExpired(stored) {
			if m.data.CompareAndDelete(key, stored) {
				stored.stopTimer()
			}
			continue
		}
		if expected == nil || !bytes.Equal(stored.value, expected) {
			return false, nil
		}

		next := m.newRaw(value, expiration)
		if !m.data.CompareAndSwap(key, stored, next) {
			continue
		}
		next.startTimer(m, key, expiration)
		stored.stopTimer()
		return true, nil
	}
}

// CompareAndDelete conditionally removes a value.
func (m *Memory) CompareAndDelete(ctx context.Context, key string, expected []byte) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}

	for {
		current, exists := m.data.Load(key)
		if !exists {
			return false, nil
		}

		stored := current.(*raw)
		if m.isExpired(stored) {
			if m.data.CompareAndDelete(key, stored) {
				stored.stopTimer()
			}
			return false, nil
		}
		if !bytes.Equal(stored.value, expected) {
			return false, nil
		}
		if !m.data.CompareAndDelete(key, stored) {
			continue
		}
		stored.stopTimer()
		return true, nil
	}
}

func (m *Memory) newRaw(value []byte, expiration time.Duration) *raw {
	v := &raw{value: append([]byte(nil), value...)}
	if expiration > 0 {
		v.expiresAt = time.Now().Add(expiration)
	}
	return v
}

func (m *Memory) isExpired(v *raw) bool {
	return !v.expiresAt.IsZero() && !time.Now().Before(v.expiresAt)
}

func (v *raw) startTimer(m *Memory, key string, expiration time.Duration) {
	if expiration <= 0 {
		return
	}

	v.timerMu.Lock()
	defer v.timerMu.Unlock()
	if v.isRemoved {
		return
	}
	v.timer = time.AfterFunc(expiration, func() {
		if m.data.CompareAndDelete(key, v) {
			v.stopTimer()
		}
	})
}

func (v *raw) stopTimer() {
	v.timerMu.Lock()
	defer v.timerMu.Unlock()
	v.isRemoved = true
	if v.timer != nil {
		v.timer.Stop()
		v.timer = nil
	}
}
